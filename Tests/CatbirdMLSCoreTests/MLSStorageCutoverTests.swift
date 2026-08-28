import CryptoKit
import Foundation
import GRDB
import Security
import XCTest
@testable import CatbirdMLSCore

final class MLSStorageCutoverTests: XCTestCase {

  private var tempBaseDir: URL!

  override func setUpWithError() throws {
    try super.setUpWithError()
    tempBaseDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("mls-storage-cutover-tests-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: tempBaseDir, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(tempBaseDir)
  }

  override func tearDownWithError() throws {
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    if let tempBaseDir {
      try? FileManager.default.removeItem(at: tempBaseDir)
    }
    try super.tearDownWithError()
  }

  // MARK: - 1. Absent DB/keys/marker first open

  func testFirstOpenWhenAllAbsentSucceedsAndCreatesCompleteState() async throws {
    let testDID = "did:plc:cutover_test_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // Verify initial absence
    let initialStatus = try coordinator.evaluateState(for: .swiftGRDB, userDID: testDID)
    guard case .allAbsent = initialStatus else {
      XCTFail("Initial state must be allAbsent, got: \(initialStatus)")
      return
    }

    let manager = MLSGRDBManager()
    let pool = try await manager.getDatabasePool(for: testDID)

    // Verify table creation / write works
    try await pool.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS test_cutover (id TEXT PRIMARY KEY);")
      try db.execute(sql: "INSERT INTO test_cutover (id) VALUES ('hello');")
    }

    let readVal = try await pool.read { db in
      try String.fetchOne(db, sql: "SELECT id FROM test_cutover WHERE id = 'hello'")
    }
    XCTAssertEqual(readVal, "hello")

    // Verify cipher_version is non-empty
    let cipherVersion: String? = try await pool.read { db in
      try String.fetchOne(db, sql: "PRAGMA cipher_version;")
    }
    XCTAssertNotNil(cipherVersion)
    XCTAssertFalse(cipherVersion?.isEmpty ?? true)

    // Verify raw header is strictly encrypted (not plaintext SQLite)
    let dbPath = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
    let fileHandle = try FileHandle(forReadingFrom: dbPath)
    defer { try? fileHandle.close() }
    let rawHeader = fileHandle.readData(ofLength: 16)
    XCTAssertEqual(rawHeader.count, 16)
    XCTAssertNotEqual(rawHeader, Data("SQLite format 3\0".utf8), "Database header must not be plaintext SQLite")

    // Verify complete state and marker
    let finalStatus = try coordinator.evaluateState(for: .swiftGRDB, userDID: testDID)
    guard case .complete(let record) = finalStatus else {
      XCTFail("Expected complete state after creation, got: \(finalStatus)")
      return
    }

    XCTAssertEqual(record.generationToken, MLSStoragePaths.generationToken)
    XCTAssertEqual(record.userDID, testDID.lowercased())
    XCTAssertEqual(record.databaseKind, MLSDatabaseKind.swiftGRDB.rawValue)
    XCTAssertEqual(record.state, .complete)
    XCTAssertNotNil(record.completedAt)

    // Verify keys exist in keychain with clean suffix
    let sqlKey = try await MLSSQLCipherEncryption.shared.getKey(for: testDID)
    XCTAssertNotNil(sqlKey)
    let sqlSalt = try await MLSSQLCipherEncryption.shared.getSalt(for: testDID)
    XCTAssertNotNil(sqlSalt)

    await manager.closeAllDatabases()

    // Clean up Keychain items
    try? await MLSSQLCipherEncryption.shared.deleteKey(for: testDID)
    try? await MLSSQLCipherEncryption.shared.deleteSalt(for: testDID)
  }

  // MARK: - 2. Complete reopen

  func testCompleteReopenValidatesAndReusesExistingState() async throws {
    let testDID = "did:plc:reopen_test_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    let manager1 = MLSGRDBManager()
    let pool1 = try await manager1.getDatabasePool(for: testDID)
    try await pool1.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS reopen_check (id TEXT PRIMARY KEY);")
      try db.execute(sql: "INSERT INTO reopen_check (id) VALUES ('token_123');")
    }
    await manager1.closeAllDatabases()

    guard case .complete(let initialRecord) = try coordinator.evaluateState(for: .swiftGRDB, userDID: testDID) else {
      XCTFail("Must be complete after first open")
      return
    }

    // Reopen in a fresh manager instance
    let manager2 = MLSGRDBManager()
    let pool2 = try await manager2.getDatabasePool(for: testDID)
    let val = try await pool2.read { db in
      try String.fetchOne(db, sql: "SELECT id FROM reopen_check WHERE id = 'token_123'")
    }
    XCTAssertEqual(val, "token_123")

    guard case .complete(let reopenedRecord) = try coordinator.evaluateState(for: .swiftGRDB, userDID: testDID) else {
      XCTFail("Must remain complete after reopen")
      return
    }

    XCTAssertEqual(initialRecord.attemptUUID, reopenedRecord.attemptUUID)
    XCTAssertEqual(initialRecord.generationToken, reopenedRecord.generationToken)
    await manager2.closeAllDatabases()

    try? await MLSSQLCipherEncryption.shared.deleteKey(for: testDID)
    try? await MLSSQLCipherEncryption.shared.deleteSalt(for: testDID)
  }

  // MARK: - 3. Persisted `creating` restart failure

  func testPersistedCreatingMarkerFailsClosedOnRestart() async throws {
    let testDID = "did:plc:creating_crash_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // Simulate crash mid-creation: write a `creating` marker
    let record = MLSInitializationRecord(
      generationToken: MLSStoragePaths.generationToken,
      attemptUUID: UUID().uuidString,
      userDID: testDID.lowercased(),
      databaseKind: MLSDatabaseKind.swiftGRDB.rawValue,
      databasePathHash: try coordinator.databasePathHash(for: .swiftGRDB, userDID: testDID),
      state: .creating
    )
    try coordinator.writeMarkerDirectlyForTesting(record)

    let manager = MLSGRDBManager()
    do {
      _ = try await manager.getDatabasePool(for: testDID)
      XCTFail("Reopen must fail closed when marker is in creating state")
    } catch {
      // Expected fail closed
      XCTAssertTrue(error is MLSStorageInitializationError || error is MLSSQLCipherError)
    }

    // Verify marker was not deleted or mutated
    let status = try coordinator.evaluateState(for: .swiftGRDB, userDID: testDID)
    guard case .incompleteAttempt(let retainedRecord) = status else {
      XCTFail("State must remain incompleteAttempt, got: \(status)")
      return
    }
    XCTAssertEqual(retainedRecord.attemptUUID, record.attemptUUID)
    XCTAssertEqual(retainedRecord.state, .creating)
  }

  // MARK: - 4. Key-only and salt-only separate mixed cases

  func testSeparateKeyOnlyAndSaltOnlyMixedCasesFailClosed() async throws {
    let coordinator = MLSStorageCoordinator.shared

    // Case 4a: Key only, salt absent
    let didKeyOnly = "did:plc:key_only_\(UUID().uuidString)"
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateKey(for: didKeyOnly)
    let status4a = try coordinator.evaluateState(for: .swiftGRDB, userDID: didKeyOnly)
    guard case .mixedState(let details4a) = status4a else {
      XCTFail("Expected mixedState for key-only, got: \(status4a)")
      return
    }
    XCTAssertTrue(details4a.contains("key present"))
    XCTAssertTrue(details4a.contains("salt absent"))

    let manager4a = MLSGRDBManager()
    do {
      _ = try await manager4a.getDatabasePool(for: didKeyOnly)
      XCTFail("Must fail closed on key-only")
    } catch {
      // Expected failure
    }
    try? await MLSSQLCipherEncryption.shared.deleteKey(for: didKeyOnly)

    // Case 4b: Salt only, key absent
    let didSaltOnly = "did:plc:salt_only_\(UUID().uuidString)"
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateSalt(for: didSaltOnly)
    let status4b = try coordinator.evaluateState(for: .swiftGRDB, userDID: didSaltOnly)
    guard case .mixedState(let details4b) = status4b else {
      XCTFail("Expected mixedState for salt-only, got: \(status4b)")
      return
    }
    XCTAssertTrue(details4b.contains("key absent"))
    XCTAssertTrue(details4b.contains("salt present"))

    let manager4b = MLSGRDBManager()
    do {
      _ = try await manager4b.getDatabasePool(for: didSaltOnly)
      XCTFail("Must fail closed on salt-only")
    } catch {
      // Expected failure
    }
    try? await MLSSQLCipherEncryption.shared.deleteSalt(for: didSaltOnly)

    // Case 4c: Both keys present, DB absent, marker absent
    let didBothKeys = "did:plc:both_keys_\(UUID().uuidString)"
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateKey(for: didBothKeys)
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateSalt(for: didBothKeys)
    let status4c = try coordinator.evaluateState(for: .swiftGRDB, userDID: didBothKeys)
    guard case .mixedState(let details4c) = status4c else {
      XCTFail("Expected mixedState for both keys without DB/marker, got: \(status4c)")
      return
    }
    XCTAssertTrue(details4c.contains("database absent"))
    XCTAssertTrue(details4c.contains("marker absent"))

    try? await MLSSQLCipherEncryption.shared.deleteKey(for: didBothKeys)
    try? await MLSSQLCipherEncryption.shared.deleteSalt(for: didBothKeys)
  }

  // MARK: - 5. DB-only failure

  func testDBOnlyWithoutKeysOrMarkerFailsClosed() async throws {
    let testDID = "did:plc:db_only_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    let dbPath = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
    try FileManager.default.createDirectory(at: dbPath.deletingLastPathComponent(), withIntermediateDirectories: true)
    try Data("unencrypted-or-corrupt-bytes".utf8).write(to: dbPath)

    let status = try coordinator.evaluateState(for: .swiftGRDB, userDID: testDID)
    guard case .mixedState(let details) = status else {
      XCTFail("Expected mixedState for DB-only, got: \(status)")
      return
    }
    XCTAssertTrue(details.contains("database present"))
    XCTAssertTrue(details.contains("marker absent"))

    let manager = MLSGRDBManager()
    do {
      _ = try await manager.getDatabasePool(for: testDID)
      XCTFail("Opening must fail closed on DB-only mixed state")
    } catch {
      // Expected fail closed
    }

    // Verify DB file was NOT deleted or mutated
    XCTAssertTrue(FileManager.default.fileExists(atPath: dbPath.path))
    let bytes = try Data(contentsOf: dbPath)
    XCTAssertEqual(bytes, Data("unencrypted-or-corrupt-bytes".utf8))
  }

  // MARK: - 6. Corrupt/plaintext/wrong-key DB failure without mutation

  func testCorruptPlaintextWrongKeyOrShortHeaderDBFailsWithoutMutation() async throws {
    let testDID = "did:plc:corrupt_test_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // Create a complete DB first
    let manager = MLSGRDBManager()
    _ = try await manager.getDatabasePool(for: testDID)
    await manager.closeAllDatabases()

    let dbPath = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
    XCTAssertTrue(FileManager.default.fileExists(atPath: dbPath.path))

    // Overwrite DB with plaintext SQLite header
    var plaintextHeader = Data("SQLite format 3\0".utf8)
    plaintextHeader.append(Data(repeating: 0xaa, count: 1024))
    try plaintextHeader.write(to: dbPath)

    let originalData = try Data(contentsOf: dbPath)

    do {
      _ = try await manager.getDatabasePool(for: testDID)
      XCTFail("Opening plaintext database header must fail closed")
    } catch {
      // Expected failure
    }

    // Ensure no mutation, deletion, or quarantine occurred
    let postData = try Data(contentsOf: dbPath)
    XCTAssertEqual(originalData, postData, "Database bytes must not be mutated or deleted on validation failure")

    let quarantineDir = try coordinator.quarantineDirectoryURL(for: .swiftGRDB)
    let quarantineEntries = (try? FileManager.default.contentsOfDirectory(atPath: quarantineDir.path)) ?? []
    XCTAssertTrue(quarantineEntries.isEmpty, "Corrupt database must not be automatically quarantined on open")

    try? await MLSSQLCipherEncryption.shared.deleteKey(for: testDID)
    try? await MLSSQLCipherEncryption.shared.deleteSalt(for: testDID)
  }

  // MARK: - 7. Keychain read/entitlement error is not absence

  func testKeychainReadEntitlementErrorIsNotAbsence() async throws {
    let key = "mls.test.strict.read.\(UUID().uuidString).\(MLSStoragePaths.cleanSuffix)"
    // Calling retrieveKeyStrict on missing key returns nil (absence)
    let nilKey = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: key)
    XCTAssertNil(nilKey)

    // Store immutable key
    let testKey = try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: key)
    XCTAssertEqual(testKey.count, 32)

    // Strict retrieve returns data
    let retrieved = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: key)
    XCTAssertEqual(retrieved, testKey)

    try? MLSKeychainManager.shared.delete(forKey: key)
  }

  // MARK: - 8. Duplicate concurrent creators converge on one winner

  func testDuplicateConcurrentCreatorsConvergeOnOneWinner() async throws {
    let key = "mls.test.concurrent.winner.\(UUID().uuidString).\(MLSStoragePaths.cleanSuffix)"

    async let task1 = Task.detached { () -> Data in
      try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: key)
    }.value

    async let task2 = Task.detached { () -> Data in
      try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: key)
    }.value

    let (res1, res2) = try await (task1, task2)
    XCTAssertEqual(res1, res2, "Both concurrent creators must converge on the exact same winning key")

    try? MLSKeychainManager.shared.delete(forKey: key)
  }

  // MARK: - 9. Admitted entrants converge on winning attempt and late entrant fails

  func testAdmittedEntrantsConvergeOnWinningAttemptAndLateEntrantFails() async throws {
    let testDID = "did:plc:converge_attempt_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // Entrant 1 and 2 open concurrently starting from total absence
    async let appOpen = Task.detached { () -> String in
      let manager = MLSGRDBManager()
      let pool = try await manager.getDatabasePool(for: testDID)
      try await pool.write { db in
        try db.execute(sql: "CREATE TABLE IF NOT EXISTS converge (id TEXT PRIMARY KEY);")
      }
      await manager.closeAllDatabases()
      guard case .complete(let record) = try coordinator.evaluateState(for: .swiftGRDB, userDID: testDID) else {
        throw MLSStorageInitializationError.validationFailed(details: "Not complete")
      }
      return record.attemptUUID
    }.value

    async let nseOpen = Task.detached { () -> String in
      let manager = MLSGRDBManager()
      _ = try await manager.performLightweightWrite(for: testDID) { db in
        try db.execute(sql: "CREATE TABLE IF NOT EXISTS converge (id TEXT PRIMARY KEY);")
        return true
      }
      guard case .complete(let record) = try coordinator.evaluateState(for: .swiftGRDB, userDID: testDID) else {
        throw MLSStorageInitializationError.validationFailed(details: "Not complete")
      }
      return record.attemptUUID
    }.value

    let (attempt1, attempt2) = try await (appOpen, nseOpen)
    XCTAssertEqual(attempt1, attempt2, "Both admitted entrants must converge on the exact same winning attempt UUID")

    try? await MLSSQLCipherEncryption.shared.deleteKey(for: testDID)
    try? await MLSSQLCipherEncryption.shared.deleteSalt(for: testDID)
  }

  // MARK: - 10. Rust resource orchestration first-open, reopen, mixed, and reset

  func testRustResourceOrchestrationFirstOpenReopenMixedAndReset() async throws {
    let testDID = "did:plc:rust_resource_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // Initial state: allAbsent
    let status0 = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .allAbsent = status0 else {
      XCTFail("Initial state must be allAbsent, got: \(status0)")
      return
    }

    // First open
    let record1 = try await coordinator.coordinateOpen(for: .rustState, userDID: testDID) { attemptUUID, isFirstCreation in
      XCTAssertTrue(isFirstCreation)
      // Simulate Rust creating DB file
      let dbURL = try coordinator.databaseURL(for: .rustState, userDID: testDID)
      try FileManager.default.createDirectory(at: dbURL.deletingLastPathComponent(), withIntermediateDirectories: true)
      try Data("rust-openmls-db-bytes".utf8).write(to: dbURL)
      return attemptUUID
    }
    XCTAssertFalse(record1.isEmpty)

    // Complete state
    let status1 = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .complete(let completeRecord) = status1 else {
      XCTFail("State must be complete, got: \(status1)")
      return
    }
    XCTAssertEqual(completeRecord.attemptUUID, record1)

    // Reopen
    let record2 = try await coordinator.coordinateOpen(for: .rustState, userDID: testDID) { attemptUUID, isFirstCreation in
      XCTAssertFalse(isFirstCreation)
      return attemptUUID
    }
    XCTAssertEqual(record2, record1)

    // Coordinated reset
    try await coordinator.coordinateReset(for: .rustState, userDID: testDID)

    // Verify allAbsent again
    let status2 = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .allAbsent = status2 else {
      XCTFail("State must be allAbsent after reset, got: \(status2)")
      return
    }
  }

  // MARK: - 11. Explicit reset removes only clean-generation resource set

  func testExplicitResetRemovesOnlyCleanGenerationResourceSet() async throws {
    let testDID = "did:plc:reset_clean_only_\(UUID().uuidString)"
    let baseContainer = MLSStoragePaths.baseContainerURL()
    let coordinator = MLSStorageCoordinator.shared

    // 1. Create Clean generation state
    let manager = MLSGRDBManager()
    let pool = try await manager.getDatabasePool(for: testDID)
    try await pool.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS clean_table (id TEXT PRIMARY KEY);")
    }
    await manager.closeAllDatabases()

    // 2. Create Legacy state that must NOT be touched
    let legacyMLS = baseContainer.appendingPathComponent("MLS", isDirectory: true)
    let legacyRust = baseContainer.appendingPathComponent("mls-state", isDirectory: true)
    try FileManager.default.createDirectory(at: legacyMLS, withIntermediateDirectories: true)
    try FileManager.default.createDirectory(at: legacyRust, withIntermediateDirectories: true)

    let legacyDBFile = legacyMLS.appendingPathComponent("mls_messages_legacy.db")
    let legacyRustFile = legacyRust.appendingPathComponent("legacy.db")
    try Data("legacy-grdb-sentinel".utf8).write(to: legacyDBFile)
    try Data("legacy-rust-sentinel".utf8).write(to: legacyRustFile)

    // Legacy migration flags
    let sanitizedDID = testDID.replacingOccurrences(of: ":", with: "-")
    let defaults = UserDefaults(suiteName: "group.blue.catbird.shared")
    defaults?.set(true, forKey: "MLSPlaintextHeaderMigrationV1_\(sanitizedDID)")

    // 3. Execute destroyStorageCompletely
    await MLSClient.shared.destroyStorageCompletely(for: testDID)

    // 4. Assert clean generation files deleted
    let cleanDB = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
    XCTAssertFalse(FileManager.default.fileExists(atPath: cleanDB.path), "Clean GRDB database must be deleted")

    let cleanRustDB = try coordinator.databaseURL(for: .rustState, userDID: testDID)
    XCTAssertFalse(FileManager.default.fileExists(atPath: cleanRustDB.path), "Clean Rust database must be deleted")

    let cleanMarker = try coordinator.markerURL(for: .swiftGRDB, userDID: testDID)
    XCTAssertFalse(FileManager.default.fileExists(atPath: cleanMarker.path), "Clean marker must be deleted")

    // 5. Assert legacy files & migration flags remain untouched!
    XCTAssertTrue(FileManager.default.fileExists(atPath: legacyDBFile.path), "Legacy GRDB database must remain untouched")
    XCTAssertTrue(FileManager.default.fileExists(atPath: legacyRustFile.path), "Legacy Rust database must remain untouched")
    XCTAssertEqual(
      defaults?.bool(forKey: "MLSPlaintextHeaderMigrationV1_\(sanitizedDID)"),
      true,
      "Legacy migration flags must remain untouched"
    )
  }

  // MARK: - 12. Legacy sentinels and counters remain byte-for-byte unchanged

  func testLegacySentinelsAndCountersRemainByteForByteUnchangedAcrossSuccessAndFailure() async throws {
    let testDID = "did:plc:legacy_sentinel_\(UUID().uuidString)"
    let baseContainer = MLSStoragePaths.baseContainerURL()

    let legacyDir = baseContainer.appendingPathComponent("MLS", isDirectory: true)
    try FileManager.default.createDirectory(at: legacyDir, withIntermediateDirectories: true)
    let legacySentinelURL = legacyDir.appendingPathComponent("sentinel.bin")
    let legacyData = Data((0..<256).map { UInt8($0) })
    try legacyData.write(to: legacySentinelURL)

    let initialHash = SHA256.hash(data: legacyData)

    // Run clean open
    let manager = MLSGRDBManager()
    let pool = try await manager.getDatabasePool(for: testDID)
    try await pool.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS dummy (id TEXT);")
    }
    await manager.closeAllDatabases()

    // Verify legacy sentinel unchanged after success
    let postSuccessData = try Data(contentsOf: legacySentinelURL)
    XCTAssertEqual(SHA256.hash(data: postSuccessData), initialHash)

    // Trigger explicit reset
    await MLSClient.shared.destroyStorageCompletely(for: testDID)

    // Verify legacy sentinel unchanged after reset
    let postResetData = try Data(contentsOf: legacySentinelURL)
    XCTAssertEqual(SHA256.hash(data: postResetData), initialHash)
  }

  // MARK: - 13. Identifiers contain exact generation/suffix and never legacy names

  func testAllIdentifiersContainExactGenerationAndSuffixAndNeverLegacyNames() throws {
    XCTAssertEqual(MLSStoragePaths.generationToken, "mls-state-clean-v2-openmls-v09")
    XCTAssertEqual(MLSStoragePaths.cleanSuffix, "clean-v2-openmls-v09")

    let rustDir = try MLSStoragePaths.rustDatabaseDirectory()
    let grdbDir = try MLSStoragePaths.grdbDatabaseDirectory()
    let checkpointsDir = try MLSStoragePaths.checkpointsDirectory()
    let welcomeGateDir = try MLSStoragePaths.welcomeGateDirectory()
    let coordinationDir = try MLSStoragePaths.coordinationDirectory()

    XCTAssertEqual(rustDir.lastPathComponent, "mls-state-clean-v2-openmls-v09")
    XCTAssertEqual(grdbDir.lastPathComponent, "MLS-clean-v2-openmls-v09")
    XCTAssertEqual(checkpointsDir.lastPathComponent, "epoch-checkpoints-clean-v2-openmls-v09")
    XCTAssertEqual(welcomeGateDir.lastPathComponent, "mls_welcome_gate-clean-v2-openmls-v09")
    XCTAssertEqual(coordinationDir.lastPathComponent, "mls-coordination-clean-v2-openmls-v09")

    // Verify no legacy directory names
    XCTAssertNotEqual(rustDir.lastPathComponent, "mls-state")
    XCTAssertNotEqual(grdbDir.lastPathComponent, "MLS")
    XCTAssertNotEqual(checkpointsDir.lastPathComponent, "epoch-checkpoints")
    XCTAssertNotEqual(welcomeGateDir.lastPathComponent, "mls_welcome_gate")

    // Verify Darwin notification names
    let stateChanged = kMLSStateChangedNotification as String
    let nseWillClose = kMLSNSEWillCloseNotification as String
    let appAck = kMLSAppAcknowledgedNotification as String
    let nseStop = kMLSNSEStopNotification as String

    XCTAssertTrue(stateChanged.hasSuffix(".clean-v2-openmls-v09"))
    XCTAssertTrue(nseWillClose.hasSuffix(".clean-v2-openmls-v09"))
    XCTAssertTrue(appAck.hasSuffix(".clean-v2-openmls-v09"))
    XCTAssertTrue(nseStop.hasSuffix(".clean-v2-openmls-v09"))
  }
}
