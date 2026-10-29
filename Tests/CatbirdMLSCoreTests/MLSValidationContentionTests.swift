import Foundation
import GRDB
import XCTest
@testable import CatbirdMLSCore

final class MLSValidationContentionTests: XCTestCase {
  private var provider: MLSGRDBManager!
  private var directory: URL!
  private var did: String!
  private var databaseURL: URL!
  private var key: Data!
  private var salt: Data!

  override func setUp() async throws {
    did = "did:plc:validation-contention-\(UUID().uuidString.lowercased())"
    directory = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSValidationContentionTests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(directory)
    MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())
    provider = MLSGRDBManager()
    _ = try await provider.getDatabasePool(for: did)
    await provider.closeAllDatabases()
    databaseURL = try MLSStorageCoordinator.shared.databaseURL(for: .swiftGRDB, userDID: did)
    key = try await MLSSQLCipherEncryption.shared.getKey(for: did)
    salt = try await MLSSQLCipherEncryption.shared.getSalt(for: did)
  }

  override func tearDown() async throws {
    MLSConversationManager.resetSuspensionStateForTesting()
    MLSCoreContext.clearSuspensionFlag()
    MLSClient.clearSuspensionFlag(reason: "validation contention cleanup")
    NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
    await provider?.shutdownAllDatabases()
    provider = nil
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
  }

  /// Exclusive WAL locking reproduces the actual SQLITE_BUSY raised by GRDB's
  /// format query during DatabaseQueue initialization, before quick_check runs.
  private func exclusiveWriter() throws -> DatabaseQueue {
    let keyHex = key.map { String(format: "%02x", $0) }.joined()
    let saltHex = salt.map { String(format: "%02x", $0) }.joined()
    var configuration = Configuration()
    configuration.allowsUnsafeTransactions = true
    configuration.prepareDatabase { db in
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF")
      try db.execute(sql: "PRAGMA key = \"x'\(keyHex)\(saltHex)'\"")
      try db.execute(sql: "PRAGMA cipher_page_size = 4096")
      try db.execute(sql: "PRAGMA kdf_iter = 256000")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512")
    }
    let queue = try DatabaseQueue(path: databaseURL.path, configuration: configuration)
    try queue.writeWithoutTransaction { db in
      try db.execute(sql: "PRAGMA locking_mode = EXCLUSIVE")
      try db.execute(sql: "BEGIN EXCLUSIVE")
      try db.execute(sql: "UPDATE MLSConversationModel SET needsReset = needsReset")
    }
    return queue
  }

  private static func release(_ queue: DatabaseQueue) throws {
    try queue.writeWithoutTransaction { db in try db.execute(sql: "ROLLBACK") }
    try queue.close()
  }

  func testValidationSucceedsWhenRealExclusiveLockIsReleasedWithinBudget() async throws {
    let locked = try exclusiveWriter()
    let release = Task {
      try await Task.sleep(for: .milliseconds(125))
      try Self.release(locked)
    }
    defer { release.cancel() }
    do {
      try await provider.validateExistingGRDBDatabase(at: databaseURL, encryptionKey: key, salt: salt)
      try await release.value
    } catch {
      try? await release.value
      throw error
    }
    XCTAssertEqual(MLSGRDBManager.activeEmergencyPoolTokenCount(for: did), 0,
      "Read-only validation must not publish a pool")
  }

  func testSecondProviderReopensAfterCompetingWriterReleasesLock() async throws {
    let firstPool = try await provider.getDatabasePool(for: did)
    try await firstPool.writeWithoutTransaction { db in
      try db.execute(sql: "PRAGMA journal_mode = DELETE")
    }
    let locked = try exclusiveWriter()
    let secondProvider = MLSGRDBManager()
    let release = Task {
      try await Task.sleep(for: .milliseconds(125))
      try Self.release(locked)
    }
    do {
      let secondPool = try await secondProvider.getDatabasePool(for: did)
      try await release.value
      XCTAssertFalse(secondPool === firstPool)
      XCTAssertEqual(MLSGRDBManager.activeEmergencyPoolTokenCount(for: did), 2)
      let tableCount = try await secondPool.read { db in
        try Int.fetchOne(db, sql: "SELECT count(*) FROM sqlite_master")
      }
      XCTAssertGreaterThan(tableCount ?? 0, 0)
    } catch {
      try? await release.value
      await secondProvider.shutdownAllDatabases()
      throw error
    }
    await secondProvider.shutdownAllDatabases()
  }

  func testCachedPoolSurvivesBusyBudgetExhaustion() async throws {
    let originalPool = try await provider.getDatabasePool(for: did)
    try await originalPool.writeWithoutTransaction { db in
      try db.execute(sql: "PRAGMA journal_mode = DELETE")
    }
    let locked = try exclusiveWriter()
    let startedAt = ContinuousClock.now
    do {
      _ = try await provider.getDatabasePool(for: did)
      XCTFail("Cache validation must report the unreleased lock")
    } catch {
      XCTAssertTrue(error.localizedDescription.contains("locked"), "\(error)")
      XCTAssertLessThan(startedAt.duration(to: .now), .seconds(2))
    }
    try Self.release(locked)
    XCTAssertEqual(MLSGRDBManager.activeEmergencyPoolTokenCount(for: did), 1,
      "Contention must not retire the live pool registration or its admission lease")
    let currentPool = try await provider.getDatabasePool(for: did)
    XCTAssertTrue(currentPool === originalPool)
    // Restore the production journal mode before GRDB creates a new reader;
    // its read-only connection cannot perform this fixture's DELETE -> WAL change.
    try await originalPool.writeWithoutTransaction { db in
      try db.execute(sql: "PRAGMA journal_mode = WAL")
    }
    let tableCount = try await originalPool.read { db in
      try Int.fetchOne(db, sql: "SELECT count(*) FROM sqlite_master")
    }
    XCTAssertGreaterThan(tableCount ?? 0, 0)
  }

  /// A suspension close racing foreground resume can close the cached pool
  /// underneath the manager. The writer probe then fails with SQLITE_MISUSE,
  /// whose SQLite message reads "out of memory". Classified as resource
  /// exhaustion, it suspended storage for 8 s, so the immediate reopen that
  /// `rebindDatabaseIfClosed` relies on failed as well.
  func testClosedCachedPoolIsEvictedAndReopenedWithoutSuspendingStorage() async throws {
    let closedPool = try await provider.getDatabasePool(for: did)
    try closedPool.close()
    do {
      _ = try await provider.getDatabasePool(for: did)
      XCTFail("A closed cached pool must not pass validation")
    } catch let error as DatabaseError {
      XCTAssertEqual(error.resultCode, .SQLITE_MISUSE, "\(error)")
    }
    XCTAssertEqual(MLSGRDBManager.activeEmergencyPoolTokenCount(for: did), 0,
      "Evicting the closed pool must retire its registration and admission lease")
    let reopened = try await provider.getDatabasePool(for: did)
    XCTAssertFalse(reopened === closedPool)
    let tableCount = try await reopened.read { db in
      try Int.fetchOne(db, sql: "SELECT count(*) FROM sqlite_master")
    }
    XCTAssertGreaterThan(tableCount ?? 0, 0)
  }

  func testSuspensionAbandonsCoalescedOpenAndReleasesActualAdmissionLock() async throws {
    let locked = try exclusiveWriter()
    defer { try? Self.release(locked) }
    let provider = provider!
    let did = did!
    let opener = Task { try await provider.getDatabasePool(for: did) }
    let coalescedWaiter = Task { try await provider.getDatabasePool(for: did) }
    try await Task.sleep(for: .milliseconds(100))
    XCTAssertFalse(try canAcquireExclusiveAdmissionLease(), "Open attempt must own admission")
    MLSClient.markSuspensionInProgress(reason: "coalesced validation suspension")
    MLSCoreContext.markSuspensionInProgress()
    let initialClose = MLSGRDBManager.closeAllDatabasesForSuspension()
    XCTAssertEqual(initialClose.inFlightOpenCount, 1)
    // A later foreground may reopen flags before this owner wakes. Its captured
    // generation still must reject both callers and release the original lease.
    MLSCoreContext.clearSuspensionFlag()
    MLSClient.clearSuspensionFlag(reason: "later foreground overtakes pending validation")
    let suspendedAt = ContinuousClock.now
    do {
      _ = try await opener.value
      XCTFail("The shared open owner must not publish after suspension")
    } catch let error as MLSSQLCipherError {
      guard case .storageUnavailable = error else {
        return XCTFail("Expected suspension failure, got \(error)")
      }
    }
    do {
      _ = try await coalescedWaiter.value
      XCTFail("Coalesced waiter must not receive a pool from the old generation")
    } catch let error as MLSSQLCipherError {
      guard case .storageUnavailable = error else {
        return XCTFail("Expected shared owner suspension failure, got \(error)")
      }
    }
    XCTAssertLessThan(suspendedAt.duration(to: .now), .milliseconds(250))
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    XCTAssertTrue(try canAcquireExclusiveAdmissionLease(),
      "Independent process must acquire the actual flock after the open owner exits")
  }

  private func canAcquireExclusiveAdmissionLease() throws -> Bool {
    #if os(macOS)
    let lease = try MLSStorageCoordinator.shared.leaseURL(
      for: .swiftGRDB, userDID: did, type: "admission")
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/python3")
    process.arguments = ["-c", """
      import fcntl, os, sys
      fd = os.open(sys.argv[1], os.O_RDWR)
      try:
          fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
      except BlockingIOError:
          sys.exit(3)
      os.close(fd)
      """, lease.path]
    try process.run()
    process.waitUntilExit()
    XCTAssertTrue(process.terminationStatus == 0 || process.terminationStatus == 3)
    return process.terminationStatus == 0
    #else
    throw XCTSkip("Independent admission lock probe requires macOS")
    #endif
  }

  func testValidationStopsOnCancellationWhileDatabaseRemainsLocked() async throws {
    let locked = try exclusiveWriter()
    defer { try? Self.release(locked) }
    let provider = provider!
    let databaseURL = databaseURL!
    let key = key!
    let salt = salt!
    let validator = Task {
      try await provider.validateExistingGRDBDatabase(
        at: databaseURL, encryptionKey: key, salt: salt)
    }
    try await Task.sleep(for: .milliseconds(100))
    let cancelledAt = ContinuousClock.now
    validator.cancel()
    do {
      try await validator.value
      XCTFail("Cancelled validation must not succeed")
    } catch is CancellationError {
      XCTAssertLessThan(cancelledAt.duration(to: .now), .milliseconds(250))
    } catch {
      XCTFail("Cancellation must survive validation wrapping: \(error)")
    }
  }

  func testValidationStopsOnSuspensionWhileDatabaseRemainsLocked() async throws {
    let locked = try exclusiveWriter()
    defer { try? Self.release(locked) }
    let provider = provider!
    let databaseURL = databaseURL!
    let key = key!
    let salt = salt!
    let validator = Task {
      try await provider.validateExistingGRDBDatabase(
        at: databaseURL, encryptionKey: key, salt: salt)
    }
    try await Task.sleep(for: .milliseconds(100))
    MLSClient.markSuspensionInProgress(reason: "validation retry regression")
    MLSCoreContext.markSuspensionInProgress()
    _ = MLSGRDBManager.closeAllDatabasesForSuspension()
    do {
      try await validator.value
      XCTFail("Suspension must abandon pending validation")
    } catch let error as MLSSQLCipherError {
      guard case .storageUnavailable = error else {
        return XCTFail("Expected suspension storage gate, got \(error)")
      }
    } catch {
      XCTFail("Suspension must survive validation wrapping: \(error)")
    }
  }

  func testValidationExhaustsBudgetWithoutMutatingLockedDatabase() async throws {
    let locked = try exclusiveWriter()
    defer { try? Self.release(locked) }
    let before = try Data(contentsOf: databaseURL)
    let startedAt = ContinuousClock.now
    do {
      try await provider.validateExistingGRDBDatabase(at: databaseURL, encryptionKey: key, salt: salt)
      XCTFail("An unreleased lock must fail within the retry budget")
    } catch {
      let elapsed = startedAt.duration(to: .now)
      XCTAssertGreaterThanOrEqual(elapsed, .milliseconds(900))
      XCTAssertLessThan(elapsed, .seconds(2))
      XCTAssertTrue(error.localizedDescription.contains("locked"), "\(error)")
    }
    XCTAssertEqual(try Data(contentsOf: databaseURL), before)
    XCTAssertEqual(MLSGRDBManager.activeEmergencyPoolTokenCount(for: did), 0)
  }

  func testWrongKeyAndMissingSchemaFailWithoutRetryOrRepair() async throws {
    let before = try Data(contentsOf: databaseURL)
    let startedAt = ContinuousClock.now
    do {
      try await provider.validateExistingGRDBDatabase(at: databaseURL,
        encryptionKey: Data(repeating: 0x83, count: 32), salt: salt)
      XCTFail("Wrong key must fail closed")
    } catch {
      XCTAssertLessThan(startedAt.duration(to: .now), .milliseconds(500))
    }
    XCTAssertEqual(try Data(contentsOf: databaseURL), before)
    let pool = try await provider.getDatabasePool(for: did)
    try await pool.write { db in try db.execute(sql: "DROP TABLE MLSMessageAppendLedgerV1") }
    await provider.closeAllDatabases()
    let damaged = try Data(contentsOf: databaseURL)
    let schemaStartedAt = ContinuousClock.now
    do {
      try await provider.validateExistingGRDBDatabase(at: databaseURL, encryptionKey: key, salt: salt)
      XCTFail("Missing required table must fail closed")
    } catch {
      XCTAssertTrue(error.localizedDescription.contains("missing required table"), "\(error)")
      XCTAssertLessThan(schemaStartedAt.duration(to: .now), .milliseconds(500))
    }
    XCTAssertEqual(try Data(contentsOf: databaseURL), damaged)
  }
}
