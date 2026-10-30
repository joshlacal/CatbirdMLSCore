import GRDB
import XCTest
@testable import CatbirdMLSCore

final class MLSSuspensionCloseTests: XCTestCase {
  private var manager: MLSGRDBManager!
  private var directory: URL!

  override func setUp() async throws {
    directory = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSSuspensionCloseTests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(directory)
    MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())
    manager = MLSGRDBManager()
  }

  override func tearDown() async throws {
    MLSStorageCoordinator.shared.testPrePublicationHook = nil
    MLSStorageCoordinator.shared.testBarrierHook = nil
    MLSGRDBManager.afterDrainCheckpointForTesting = nil
    MLSCoreContext.clearSuspensionFlag()
    MLSClient.clearSuspensionFlag(reason: "suspension-close test cleanup")
    NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
    await manager.shutdownAllDatabases()
    manager = nil
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
    // Keep disposable test files for diagnosis; no user storage is touched.
  }

  func testEmergencyCloseRetriesFailedPoolAndReleasesLease() async throws {
    let did = "did:plc:suspension-close-\(UUID().uuidString.lowercased())"
    let pool = try await manager.getDatabasePool(for: did)
    let holder = StatementHolder()
    try await pool.writeWithoutTransaction { db in
      holder.statement = try db.makeStatement(sql: "SELECT 1")
    }

    MLSGRDBManager.emergencyCloseAllDatabases()
    XCTAssertTrue(MLSStorageCoordinator.shared.hasActiveAdmissionLease(for: .swiftGRDB, userDID: did))
    holder.statement = nil

    MLSGRDBManager.emergencyCloseAllDatabases()
    XCTAssertFalse(MLSStorageCoordinator.shared.hasActiveAdmissionLease(for: .swiftGRDB, userDID: did),
      "A subsequent suspension close must retry previously failed survivors and release their lease")
  }
  func testSuspensionClosesAllManagersAndQueuesThenReopensPersistedData() async throws {
    let did = "did:plc:suspension-shared-\(UUID().uuidString.lowercased())"
    let otherManager = MLSGRDBManager()
    try await manager.nseWrite(for: did) { db in
      try db.execute(sql: "CREATE TABLE suspension_probe (value INTEGER); INSERT INTO suspension_probe VALUES (42)")
    }
    let pool = try await otherManager.getDatabasePool(for: did)
    let cipherVersion = try await pool.read { try String.fetchOne($0, sql: "PRAGMA cipher_version") }
    let sqliteVersion = try await pool.read { try String.fetchOne($0, sql: "SELECT sqlite_version()") }
    print("Suspension close runtime: cipher_version=\(cipherVersion ?? "unavailable"), sqlite_version=\(sqliteVersion ?? "unavailable")")
    XCTAssertNotNil(cipherVersion, "The actual manager fixture requires SQLCipher")
    let trace = SQLTrace()
    try await pool.writeWithoutTransaction { db in
      db.trace { trace.record($0.description) }
    }
    XCTAssertEqual(MLSStorageCoordinator.shared.activeAdmissionLeaseCount(for: .swiftGRDB), 2)
    XCTAssertFalse(try canAcquireExclusiveLease(did))

    MLSCoreContext.markSuspensionInProgress()
    NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
    let result = MLSGRDBManager.closeAllDatabasesForSuspension()

    XCTAssertTrue(result.isComplete)
    XCTAssertEqual(result.closedPoolCount, 1)
    XCTAssertEqual(result.closedQueueCount, 1)
    XCTAssertEqual(result.remainingAdmissionLeaseCount, 0)
    XCTAssertTrue(try canAcquireExclusiveLease(did))
    XCTAssertFalse(trace.containsCheckpoint)
    do {
      _ = try await pool.read { try Int.fetchOne($0, sql: "SELECT 1") }
      XCTFail("A previously returned pool must be closed")
    } catch { }

    MLSCoreContext.clearSuspensionFlag()
    NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
    let reopened = try await otherManager.getDatabasePool(for: did)
    XCTAssertFalse(reopened === pool)
    let value = try await reopened.read { try Int.fetchOne($0, sql: "SELECT value FROM suspension_probe") }
    XCTAssertEqual(value, 42)
    XCTAssertFalse(try canAcquireExclusiveLease(did))
    let queueValue = try await manager.nseRead(for: did) { try Int.fetchOne($0, sql: "SELECT value FROM suspension_probe") }
    XCTAssertEqual(queueValue, 42)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    XCTAssertTrue(try canAcquireExclusiveLease(did))
    await otherManager.shutdownAllDatabases()
  }

  func testFailedCloseRetainsLeaseUntilSuccessfulRetry() async throws {
    let did = "did:plc:suspension-failure-\(UUID().uuidString.lowercased())"
    let pool = try await manager.getDatabasePool(for: did)
    let holder = StatementHolder()
    try await pool.writeWithoutTransaction { holder.statement = try $0.makeStatement(sql: "SELECT 1") }
    MLSCoreContext.markSuspensionInProgress()

    let failed = MLSGRDBManager.closeAllDatabasesForSuspension()
    XCTAssertFalse(failed.isComplete)
    XCTAssertEqual(failed.failedPoolCount, 1)
    XCTAssertEqual(failed.remainingPoolCount, 1)
    XCTAssertEqual(failed.remainingAdmissionLeaseCount, 1)
    XCTAssertFalse(try canAcquireExclusiveLease(did))

    holder.statement = nil
    let retried = MLSGRDBManager.closeAllDatabasesForSuspension()
    XCTAssertTrue(retried.isComplete)
    XCTAssertEqual(retried.closedPoolCount, 1)
    XCTAssertTrue(try canAcquireExclusiveLease(did))
    XCTAssertEqual(MLSGRDBManager.closeAllDatabasesForSuspension().closedPoolCount, 0)
  }

  func testFailedQueueKeepsSharedLeaseAfterSiblingPoolCloses() async throws {
    let did = "did:plc:suspension-queue-\(UUID().uuidString.lowercased())"
    let otherManager = MLSGRDBManager()
    let holder = StatementHolder()
    try await manager.nseWrite(for: did) { holder.statement = try $0.makeStatement(sql: "SELECT 1") }
    _ = try await otherManager.getDatabasePool(for: did)
    XCTAssertEqual(MLSStorageCoordinator.shared.activeAdmissionLeaseCount(for: .swiftGRDB), 2)
    MLSCoreContext.markSuspensionInProgress()

    let partial = MLSGRDBManager.closeAllDatabasesForSuspension()
    XCTAssertFalse(partial.isComplete)
    XCTAssertEqual(partial.closedPoolCount, 1)
    XCTAssertEqual(partial.failedQueueCount, 1)
    XCTAssertEqual(partial.remainingQueueCount, 1)
    XCTAssertEqual(partial.remainingAdmissionLeaseCount, 1)
    XCTAssertFalse(try canAcquireExclusiveLease(did))
    holder.statement = nil

    let retried = MLSGRDBManager.closeAllDatabasesForSuspension()
    XCTAssertTrue(retried.isComplete)
    XCTAssertEqual(retried.closedQueueCount, 1)
    XCTAssertEqual(retried.closedPoolCount, 0)
    XCTAssertTrue(try canAcquireExclusiveLease(did))
    await otherManager.shutdownAllDatabases()
  }

  func testPendingOpenCannotPublishAfterSuspensionOrReportCompleteEarly() async throws {
    let did = "did:plc:suspension-pending-\(UUID().uuidString.lowercased())"
    let reachedPublication = expectation(description: "Open holds admission lease before publication")
    let releaseOpen = DispatchSemaphore(value: 0)
    MLSStorageCoordinator.shared.testPrePublicationHook = {
      reachedPublication.fulfill()
      _ = releaseOpen.wait(timeout: .now() + 5)
    }
    let manager = try XCTUnwrap(manager)
    let open = Task.detached { try await manager.getDatabasePool(for: did) }
    await fulfillment(of: [reachedPublication], timeout: 5)
    defer { releaseOpen.signal() }
    MLSCoreContext.markSuspensionInProgress()

    let pending = MLSGRDBManager.closeAllDatabasesForSuspension()
    XCTAssertFalse(pending.isComplete)
    XCTAssertEqual(pending.inFlightOpenCount, 1)
    XCTAssertFalse(try canAcquireExclusiveLease(did))
    MLSStorageCoordinator.shared.testPrePublicationHook = nil
    releaseOpen.signal()
    do {
      _ = try await open.value
      XCTFail("An open from the previous generation must be rejected")
    } catch { }

    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    XCTAssertEqual(MLSGRDBManager.activeEmergencyPoolTokenCount(for: did), 0)
    XCTAssertTrue(try canAcquireExclusiveLease(did))
    // The gate rejects newly requested opens before they acquire any admission lock.
    do {
      _ = try await manager.getDatabasePool(for: did)
      XCTFail("New opens must remain blocked during suspension")
    } catch { }
    XCTAssertTrue(try canAcquireExclusiveLease(did))
    MLSCoreContext.clearSuspensionFlag()
    _ = try await manager.getDatabasePool(for: did)
    XCTAssertFalse(try canAcquireExclusiveLease(did))
  }

  func testConcurrentCloserReportsOutstandingCloseUntilLeaseReleased() async throws {
    let did = "did:plc:suspension-concurrent-\(UUID().uuidString.lowercased())"
    let pool = try await manager.getDatabasePool(for: did)
    let writerEntered = expectation(description: "Writer occupies connection")
    let releaseWriter = DispatchSemaphore(value: 0)
    let write = Task.detached {
      try pool.writeWithoutTransaction { _ in
        writerEntered.fulfill()
        _ = releaseWriter.wait(timeout: .now() + 5)
      }
    }
    await fulfillment(of: [writerEntered], timeout: 5)
    defer { releaseWriter.signal() }
    MLSCoreContext.markSuspensionInProgress()
    let firstClose = Task.detached { MLSGRDBManager.closeAllDatabasesForSuspension() }
    for _ in 0..<100 where MLSGRDBManager.activeEmergencyPoolTokenCount(for: did) != 0 {
      try await Task.sleep(for: .milliseconds(10))
    }
    XCTAssertEqual(MLSGRDBManager.activeEmergencyPoolTokenCount(for: did), 0)

    let secondClose = MLSGRDBManager.closeAllDatabasesForSuspension()
    XCTAssertFalse(secondClose.isComplete)
    XCTAssertEqual(secondClose.remainingPoolCount, 1)
    XCTAssertFalse(try canAcquireExclusiveLease(did))
    releaseWriter.signal()
    try await write.value
    let firstResult = await firstClose.value
    XCTAssertTrue(firstResult.isComplete)
    XCTAssertTrue(try canAcquireExclusiveLease(did))
  }

  func testCachedReadAndWriteRoutesReopenAfterSuspension() async throws {
    let did = "did:plc:suspension-routes-\(UUID().uuidString.lowercased())"
    for route in ["read", "write", "lightweightRead", "lightweightWrite"] {
      _ = try await manager.getDatabasePool(for: did)
      XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
      do {
        let value: Int?
        switch route {
        case "read": value = try await manager.read(for: did) { try Int.fetchOne($0, sql: "SELECT 42") }
        case "write": value = try await manager.write(for: did) { try Int.fetchOne($0, sql: "SELECT 42") }
        case "lightweightRead": value = try await manager.performLightweightRead(for: did) { try Int.fetchOne($0, sql: "SELECT 42") }
        default: value = try await manager.performLightweightWrite(for: did) { try Int.fetchOne($0, sql: "SELECT 42") }
        }
        XCTAssertEqual(value, 42, route)
      } catch {
        XCTFail("\(route) reused a closed pool after suspension: \(error)")
      }
    }
  }

  func testOldAsynchronousDrainCannotReleaseReopenedPoolsLease() async throws {
    let did = "did:plc:suspension-stale-drain-\(UUID().uuidString.lowercased())"
    _ = try await manager.getDatabasePool(for: did)
    let checkpointFinished = expectation(description: "Old drain has captured its pool and finished checkpoint")
    let barrier = DrainBarrier()
    defer {
      MLSGRDBManager.afterDrainCheckpointForTesting = nil
      Task { await barrier.release() }
    }
    MLSGRDBManager.afterDrainCheckpointForTesting = {
      await barrier.pause(checkpointFinished)
    }
    let manager = try XCTUnwrap(manager)
    let oldDrain = Task { await manager.closeDatabaseAndDrain(for: did) }
    await fulfillment(of: [checkpointFinished], timeout: 5)

    MLSCoreContext.markSuspensionInProgress()
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    MLSCoreContext.clearSuspensionFlag()
    let reopened = try await manager.getDatabasePool(for: did)
    XCTAssertFalse(try canAcquireExclusiveLease(did))
    MLSGRDBManager.afterDrainCheckpointForTesting = nil
    await barrier.release()
    let drained = await oldDrain.value
    XCTAssertFalse(drained, "The old generation no longer owns current storage")

    let stillReadable = try await reopened.read { try Int.fetchOne($0, sql: "SELECT 42") }
    XCTAssertEqual(stillReadable, 42)
    XCTAssertFalse(try canAcquireExclusiveLease(did), "Stale drain must not release the new pool's admission lease")
    XCTAssertEqual(MLSGRDBManager.activeEmergencyPoolTokenCount(for: did), 1)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    try reopened.close()
  }

  func testForegroundPreparationWorksBeforeGeneralMLSOperationsResume() async throws {
    let did = "did:plc:foreground-preparation-\(UUID().uuidString.lowercased())"
    _ = try await manager.getDatabasePool(for: did)
    MLSClient.markSuspensionInProgress(reason: "foreground preparation regression")
    MLSCoreContext.markSuspensionInProgress()
    NotificationCenter.default.post(name: Database.suspendNotification, object: nil)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    // Foreground restores GRDB first, but general MLS and Rust admission stay closed
    // until preparation has established that the existing database is readable.
    NotificationCenter.default.post(name: Database.resumeNotification, object: nil)

    let permit = MLSGRDBManager.ForegroundResumePermit()
    _ = try await manager.prepareForForegroundResume(for: did, permit: permit)

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(try canAcquireExclusiveLease(did), "Preparatory probe must close before return")
    XCTAssertEqual(MLSGRDBManager.activeEmergencyPoolTokenCount(for: did), 0)
    do {
      _ = try await manager.getDatabasePool(for: did)
      XCTFail("A preparation permit must not authorize ordinary database opens")
    } catch { }
    XCTAssertTrue(try canAcquireExclusiveLease(did))
  }

  func testForegroundPreparationRequiresPermitWhileGeneralGatesRemainClosed() async throws {
    let did = "did:plc:foreground-no-permit-\(UUID().uuidString.lowercased())"
    _ = try await manager.getDatabasePool(for: did)
    MLSClient.markSuspensionInProgress(reason: "missing foreground permit")
    MLSCoreContext.markSuspensionInProgress()
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    do {
      _ = try await manager.prepareForForegroundResume(for: did)
      XCTFail("Closed-gate preparation requires explicit foreground authority")
    } catch { }
    let revoked = MLSGRDBManager.ForegroundResumePermit()
    revoked.revoke()
    XCTAssertFalse(revoked.isValid)
    do {
      _ = try await manager.prepareForForegroundResume(for: did, permit: revoked)
      XCTFail("Revoked foreground authority must be rejected")
    } catch { }
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    XCTAssertTrue(try canAcquireExclusiveLease(did))
  }

  func testForegroundPermitRevocationDuringAdmissionUnwindsLease() async throws {
    try await assertForegroundPreparationAbortsDuringAdmission(revokePermit: true)
  }

  func testNewSuspensionDuringForegroundAdmissionReportsPendingUntilCleanup() async throws {
    try await assertForegroundPreparationAbortsDuringAdmission(revokePermit: false)
  }

  private func assertForegroundPreparationAbortsDuringAdmission(revokePermit: Bool) async throws {
    let did = "did:plc:foreground-admission-race-\(UUID().uuidString.lowercased())"
    _ = try await manager.getDatabasePool(for: did)
    MLSClient.markSuspensionInProgress(reason: "foreground admission race")
    MLSCoreContext.markSuspensionInProgress()
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    let permit = MLSGRDBManager.ForegroundResumePermit()
    let admitted = expectation(description: "Preparatory admission lease is held before opening")
    let releaseAdmission = DispatchSemaphore(value: 0)
    MLSStorageCoordinator.shared.testBarrierHook = {
      admitted.fulfill()
      _ = releaseAdmission.wait(timeout: .now() + 5)
    }
    defer {
      MLSStorageCoordinator.shared.testBarrierHook = nil
      releaseAdmission.signal()
    }
    let manager = try XCTUnwrap(manager)
    let preparation = Task.detached { try await manager.prepareForForegroundResume(for: did, permit: permit) }
    await fulfillment(of: [admitted], timeout: 5)
    XCTAssertFalse(try canAcquireExclusiveLease(did))
    if revokePermit {
      // Prove immediate permit revocation works without a later close/generation bump.
      permit.revoke()
    } else {
      let result = MLSGRDBManager.closeAllDatabasesForSuspension()
      XCTAssertFalse(result.isComplete)
      XCTAssertEqual(result.inFlightOpenCount, 1)
      XCTAssertTrue(permit.isValid, "Generation rejection is independent of permit revocation")
    }
    MLSStorageCoordinator.shared.testBarrierHook = nil
    releaseAdmission.signal()
    do {
      _ = try await preparation.value
      XCTFail("Superseded foreground preparation must not report ready")
    } catch { }
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    XCTAssertTrue(try canAcquireExclusiveLease(did))
  }

  func testForegroundPreparationDoesNotInitializeAbsentStore() async throws {
    let did = "did:plc:foreground-absent-\(UUID().uuidString.lowercased())"
    MLSClient.markSuspensionInProgress(reason: "absent foreground storage")
    MLSCoreContext.markSuspensionInProgress()
    let permit = MLSGRDBManager.ForegroundResumePermit()
    _ = try await manager.prepareForForegroundResume(for: did, permit: permit)
    let url = try MLSStorageCoordinator.shared.databaseURL(for: .swiftGRDB, userDID: did)
    XCTAssertFalse(FileManager.default.fileExists(atPath: url.path))
    let key = try await MLSSQLCipherEncryption.shared.getKey(for: did)
    XCTAssertNil(key)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    XCTAssertTrue(try canAcquireExclusiveLease(did))
  }

  private func canAcquireExclusiveLease(_ did: String) throws -> Bool {
    let url = try MLSStorageCoordinator.shared.leaseURL(for: .swiftGRDB, userDID: did, type: "admission")
    #if os(macOS)
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
      """, url.path]
    try process.run()
    process.waitUntilExit()
    XCTAssertTrue(process.terminationStatus == 0 || process.terminationStatus == 3)
    return process.terminationStatus == 0
    #else
    throw XCTSkip("Separate-process admission probe requires the macOS test host")
    #endif
  }

}

private final class StatementHolder: @unchecked Sendable {
  var statement: Statement?
}

private final class SQLTrace: @unchecked Sendable {
  private let lock = NSLock()
  private var statements: [String] = []
  func record(_ sql: String) { lock.withLock { statements.append(sql) } }
  var containsCheckpoint: Bool {
    lock.withLock { statements.contains { $0.lowercased().contains("wal_checkpoint") } }
  }
}

private actor DrainBarrier {
  private var continuation: CheckedContinuation<Void, Never>?
  private var released = false
  func pause(_ reached: XCTestExpectation) async {
    guard !released else { return }
    await withCheckedContinuation { continuation in
      self.continuation = continuation
      reached.fulfill()
    }
  }
  func release() {
    released = true
    continuation?.resume()
    continuation = nil
  }
}
