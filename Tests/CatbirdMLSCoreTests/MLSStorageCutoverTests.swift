import CryptoKit
import Foundation
import GRDB
import Security
import XCTest
@testable import CatbirdMLSCore

final class MLSStorageCutoverTests: XCTestCase {

  private var tempBaseDir: URL!
  private var fakeKeychain: MLSKeychainFakeStorage!

  override func setUpWithError() throws {
    try super.setUpWithError()
    tempBaseDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("mls-storage-cutover-tests-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: tempBaseDir, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(tempBaseDir)

    fakeKeychain = MLSKeychainFakeStorage()
    MLSKeychainManager.setFakeStorageOverrideForTesting(fakeKeychain)
  }

  override func tearDownWithError() throws {
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
    if let tempBaseDir {
      try? FileManager.default.removeItem(at: tempBaseDir)
    }
    try super.tearDownWithError()
  }

  // MARK: - 1. Absent DB/keys/marker first open

  func test01_AllAbsentSwiftFirstOpenCreatesKeySaltDBCompleteMarkerAndEncryptedHeader() async throws {
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

    // Verify raw header is strictly encrypted (16 bytes, not plaintext SQLite)
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

    // Verify keys exist in keychain with clean suffix and exact lengths
    let sqlKey = try await MLSSQLCipherEncryption.shared.getKey(for: testDID)
    XCTAssertEqual(sqlKey?.count, 32)
    let sqlSalt = try await MLSSQLCipherEncryption.shared.getSalt(for: testDID)
    XCTAssertEqual(sqlSalt?.count, 16)

    await manager.closeAllDatabases()
  }

  // MARK: - 2. Complete reopen

  func test02_CompleteReopenPreservesAttemptKeyBytesAndRevalidates() async throws {
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

    let initialKey = try await MLSSQLCipherEncryption.shared.getKey(for: testDID)

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

    let reopenedKey = try await MLSSQLCipherEncryption.shared.getKey(for: testDID)

    XCTAssertEqual(initialRecord.attemptUUID, reopenedRecord.attemptUUID)
    XCTAssertEqual(initialRecord.generationToken, reopenedRecord.generationToken)
    XCTAssertEqual(initialKey, reopenedKey)

    await manager2.closeAllDatabases()
  }

  // MARK: - 3. Persisted `creating` restart failure

  func test03_PersistedCreatingMarkerFailsClosedOnRestartWithoutMutation() async throws {
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

    let markerURL = try coordinator.markerURL(for: .swiftGRDB, userDID: testDID)
    let markerBytesBefore = try Data(contentsOf: markerURL)

    let manager = MLSGRDBManager()
    do {
      _ = try await manager.getDatabasePool(for: testDID)
      XCTFail("Reopen must fail closed when marker is in creating state")
    } catch {
      // Expected fail closed
    }

    // Verify marker was not deleted or mutated
    let markerBytesAfter = try Data(contentsOf: markerURL)
    XCTAssertEqual(markerBytesBefore, markerBytesAfter)

    let status = try coordinator.evaluateState(for: .swiftGRDB, userDID: testDID)
    guard case .incompleteAttempt(let retainedRecord) = status else {
      XCTFail("State must remain incompleteAttempt, got: \(status)")
      return
    }
    XCTAssertEqual(retainedRecord.attemptUUID, record.attemptUUID)
    XCTAssertEqual(retainedRecord.state, .creating)
  }

  // MARK: - 4. Key-only, salt-only, both-keys-only, DB-only, sidecar-only, wrong-length key fail closed

  func test04_MixedStatesAndWrongLengthKeysFailClosed() async throws {
    let coordinator = MLSStorageCoordinator.shared

    // 4a. Key only
    let didKeyOnly = "did:plc:key_only_\(UUID().uuidString)"
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateKey(for: didKeyOnly)
    let status4a = try coordinator.evaluateState(for: .swiftGRDB, userDID: didKeyOnly)
    guard case .mixedState = status4a else {
      XCTFail("Expected mixedState for key-only, got: \(status4a)")
      return
    }

    // 4b. Salt only
    let didSaltOnly = "did:plc:salt_only_\(UUID().uuidString)"
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateSalt(for: didSaltOnly)
    let status4b = try coordinator.evaluateState(for: .swiftGRDB, userDID: didSaltOnly)
    guard case .mixedState = status4b else {
      XCTFail("Expected mixedState for salt-only, got: \(status4b)")
      return
    }

    // 4c. Both keys only
    let didBothKeys = "did:plc:both_keys_\(UUID().uuidString)"
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateKey(for: didBothKeys)
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateSalt(for: didBothKeys)
    let status4c = try coordinator.evaluateState(for: .swiftGRDB, userDID: didBothKeys)
    guard case .mixedState = status4c else {
      XCTFail("Expected mixedState for both keys only, got: \(status4c)")
      return
    }

    // 4d. DB only
    let didDBOnly = "did:plc:db_only_\(UUID().uuidString)"
    let dbPath = try coordinator.databaseURL(for: .swiftGRDB, userDID: didDBOnly)
    try FileManager.default.createDirectory(at: dbPath.deletingLastPathComponent(), withIntermediateDirectories: true)
    try Data("db-content".utf8).write(to: dbPath)
    let status4d = try coordinator.evaluateState(for: .swiftGRDB, userDID: didDBOnly)
    guard case .mixedState = status4d else {
      XCTFail("Expected mixedState for DB only, got: \(status4d)")
      return
    }

    // 4e. Sidecar only (orphan WAL/SHM)
    let didSidecarOnly = "did:plc:sidecar_only_\(UUID().uuidString)"
    let scDBPath = try coordinator.databaseURL(for: .swiftGRDB, userDID: didSidecarOnly)
    let scWAL = URL(fileURLWithPath: scDBPath.path + "-wal")
    try FileManager.default.createDirectory(at: scWAL.deletingLastPathComponent(), withIntermediateDirectories: true)
    try Data("orphan-wal".utf8).write(to: scWAL)
    let status4e = try coordinator.evaluateState(for: .swiftGRDB, userDID: didSidecarOnly)
    guard case .mixedState(let details) = status4e else {
      XCTFail("Expected mixedState for orphan sidecar, got: \(status4e)")
      return
    }
    XCTAssertTrue(details.contains("Orphan database sidecars"))

    // 4f. Wrong length key (e.g. 31 bytes instead of 32)
    let didWrongLen = "did:plc:wrong_len_\(UUID().uuidString)"
    let keyAcc = MLSStoragePaths.grdbKeyAccount(for: didWrongLen)
    _ = try fakeKeychain.add(service: "blue.catbird.mls", account: keyAcc, data: Data(repeating: 0x42, count: 31))
    XCTAssertThrowsError(
      try MLSKeychainManager.shared.retrieveKeyStrict(forKey: keyAcc, expectedLength: 32)
    )
  }

  // MARK: - 5. Corrupt / plaintext / wrong key fail without mutation

  func test05_CorruptPlaintextWrongKeyOrShortHeaderDBFailsWithoutMutation() async throws {
    let coordinator = MLSStorageCoordinator.shared

    enum CorruptionCase: String, CaseIterable {
      case plaintextHeader
      case randomCorruptBytes
      case shortHeader
      case wrongKey
      case markerMismatch
    }

    for testCase in CorruptionCase.allCases {
      let testDID = "did:plc:corrupt_\(testCase.rawValue)_\(UUID().uuidString)"
      let manager = MLSGRDBManager()

      // 1. First create a valid database
      let pool = try await manager.getDatabasePool(for: testDID)
      try await pool.write { db in
        try db.execute(sql: "CREATE TABLE IF NOT EXISTS data_table (id TEXT PRIMARY KEY, val TEXT);")
        try db.execute(sql: "INSERT INTO data_table (id, val) VALUES ('k1', 'v1');")
      }
      await manager.closeAllDatabases()

      let dbPath = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
      let walPath = URL(fileURLWithPath: dbPath.path + "-wal")
      let shmPath = URL(fileURLWithPath: dbPath.path + "-shm")
      let markerURL = try coordinator.markerURL(for: .swiftGRDB, userDID: testDID)

      XCTAssertTrue(FileManager.default.fileExists(atPath: dbPath.path))

      // 2. Apply corruption according to test case
      switch testCase {
      case .plaintextHeader:
        var plaintextHeader = Data("SQLite format 3\0".utf8)
        plaintextHeader.append(Data(repeating: 0xaa, count: 1024))
        try plaintextHeader.write(to: dbPath)

      case .randomCorruptBytes:
        let corruptData = Data((0..<2048).map { _ in UInt8.random(in: 0...255) })
        try corruptData.write(to: dbPath)

      case .shortHeader:
        let shortData = Data([0x01, 0x02, 0x03, 0x04])
        try shortData.write(to: dbPath)

      case .wrongKey:
        let keyAcc = MLSStoragePaths.grdbKeyAccount(for: testDID)
        try fakeKeychain.delete(service: "blue.catbird.mls", account: keyAcc)
        _ = try fakeKeychain.add(
          service: "blue.catbird.mls",
          account: keyAcc,
          data: Data(repeating: 0x99, count: 32)
        )

      case .markerMismatch:
        let tamperedRecord = MLSInitializationRecord(
          generationToken: MLSStoragePaths.generationToken,
          attemptUUID: UUID().uuidString,
          userDID: testDID.lowercased(),
          databaseKind: MLSDatabaseKind.swiftGRDB.rawValue,
          databasePathHash: "tampered-hash-does-not-match",
          state: .complete,
          completedAt: Date().timeIntervalSince1970
        )
        try coordinator.writeMarkerDirectlyForTesting(tamperedRecord)
      }

      // Snapshot state after corruption
      let corruptedDBData = try? Data(contentsOf: dbPath)
      let corruptedWALData = try? Data(contentsOf: walPath)
      let corruptedSHMData = try? Data(contentsOf: shmPath)
      let corruptedMarkerData = try? Data(contentsOf: markerURL)

      // 3. Attempt open -> must fail closed
      do {
        _ = try await manager.getDatabasePool(for: testDID)
        XCTFail("Opening corrupted database must fail closed for case: \(testCase.rawValue)")
      } catch {
        // Expected fail-closed error
      }

      // 4. Assert byte-for-byte immutability (no automatic repair, deletion, or modification)
      let postDBData = try? Data(contentsOf: dbPath)
      let postWALData = try? Data(contentsOf: walPath)
      let postSHMData = try? Data(contentsOf: shmPath)
      let postMarkerData = try? Data(contentsOf: markerURL)

      XCTAssertEqual(corruptedDBData, postDBData, "DB file must be byte-for-byte unchanged on open failure (\(testCase.rawValue))")
      XCTAssertEqual(corruptedWALData, postWALData, "WAL file must be unchanged on open failure (\(testCase.rawValue))")
      XCTAssertEqual(corruptedSHMData, postSHMData, "SHM file must be unchanged on open failure (\(testCase.rawValue))")
      XCTAssertEqual(corruptedMarkerData, postMarkerData, "Marker file must be unchanged on open failure (\(testCase.rawValue))")

      let quarantineDir = try coordinator.quarantineDirectoryURL(for: .swiftGRDB)
      let quarantineEntries = (try? FileManager.default.contentsOfDirectory(atPath: quarantineDir.path)) ?? []
      XCTAssertTrue(quarantineEntries.isEmpty, "No automatic quarantine must occur on open failure (\(testCase.rawValue))")

      await manager.closeAllDatabases()
    }
  }

  // MARK: - 6. Injected Keychain read/entitlement error throws without creating marker/DB

  func test06_InjectedKeychainErrorThrowsWithoutCreatingState() async throws {
    let testDID = "did:plc:injected_err_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // Inject entitlement error
    fakeKeychain.setInjectedError(errSecAuthFailed)

    let manager = MLSGRDBManager()
    do {
      _ = try await manager.getDatabasePool(for: testDID)
      XCTFail("Must fail when Keychain throws entitlement/auth error")
    } catch {
      // Expected failure
    }

    // Reset error injection to check disk state
    fakeKeychain.setInjectedError(nil)

    let dbPath = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
    XCTAssertFalse(FileManager.default.fileExists(atPath: dbPath.path), "DB must not be created on Keychain error")

    let markerURL = try coordinator.markerURL(for: .swiftGRDB, userDID: testDID)
    XCTAssertFalse(FileManager.default.fileExists(atPath: markerURL.path), "Marker must not exist after failure")
  }

  // MARK: - 7. Concurrent Add-only candidates converge on one winner

  func test07_DuplicateConcurrentCreatorsConvergeOnOneWinningValue() async throws {
    let key = "mls.test.concurrent.winner.\(UUID().uuidString).\(MLSStoragePaths.cleanSuffix)"

    async let task1 = Task.detached { () -> Data in
      try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: key, length: 32)
    }.value

    async let task2 = Task.detached { () -> Data in
      try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: key, length: 32)
    }.value

    let (res1, res2) = try await (task1, task2)
    XCTAssertEqual(res1, res2, "Both concurrent creators must converge on the exact same winning key")
    XCTAssertEqual(res1.count, 32)
  }

  // MARK: - 8. Admitted entrants converge on winning attempt and late entrant fails

  func test08_AdmittedEntrantsConvergeOnWinningAttemptAndLateEntrantFails() async throws {
    let testDID = "did:plc:converge_attempt_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // PART A: Two admitted entrants both observe absence and converge on one attemptUUID
    let prePubCondition = NSCondition()
    var arrivedAtPrePub = 0
    let totalEntrants = 2

    coordinator.testPrePublicationHook = {
      prePubCondition.lock()
      arrivedAtPrePub += 1
      if arrivedAtPrePub < totalEntrants {
        while arrivedAtPrePub < totalEntrants {
          prePubCondition.wait()
        }
      } else {
        prePubCondition.broadcast()
      }
      prePubCondition.unlock()
    }

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
    coordinator.testPrePublicationHook = nil

    XCTAssertEqual(attempt1, attempt2, "Both admitted entrants must converge on the exact same winning attempt UUID")

    // PART B: Late entrant observing a creating marker fails closed
    let lateDID = "did:plc:late_entrant_\(UUID().uuidString)"
    let lateCreatingRecord = MLSInitializationRecord(
      generationToken: MLSStoragePaths.generationToken,
      attemptUUID: UUID().uuidString,
      userDID: lateDID.lowercased(),
      databaseKind: MLSDatabaseKind.swiftGRDB.rawValue,
      databasePathHash: try coordinator.databasePathHash(for: .swiftGRDB, userDID: lateDID),
      state: .creating
    )
    try coordinator.writeMarkerDirectlyForTesting(lateCreatingRecord)

    let lateManager = MLSGRDBManager()
    do {
      _ = try await lateManager.getDatabasePool(for: lateDID)
      XCTFail("Late entrant observing creating marker must fail closed")
    } catch let error as MLSStorageInitializationError {
      if case .incompleteAttempt = error {
        // Expected fail-closed incompleteAttempt error
      } else {
        XCTFail("Expected incompleteAttempt error, got: \(error)")
      }
    } catch {
      // Also acceptable
    }
  }

  // MARK: - 9. Rust coordinator open, reopen, mixed, and reset

  func test09_RustResourceOrchestrationFirstOpenReopenMixedAndReset() async throws {
    let testDID = "did:plc:rust_real_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // Initial state: allAbsent
    let status0 = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .allAbsent = status0 else {
      XCTFail("Initial state must be allAbsent, got: \(status0)")
      return
    }

    // First open using MLSCoreContext
    let context = try await MLSCoreContext.shared.getContext(for: testDID)
    let diskVersion = MLSStateVersionManager.shared.getDiskVersion(for: testDID)
    XCTAssertGreaterThanOrEqual(diskVersion, 0)

    // Complete state verification
    let status1 = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .complete(let completeRecord) = status1 else {
      XCTFail("State must be complete after Rust first open, got: \(status1)")
      return
    }
    XCTAssertEqual(completeRecord.generationToken, MLSStoragePaths.generationToken)
    XCTAssertEqual(completeRecord.userDID, testDID.lowercased())

    // MEK and content root must exist in Keychain
    let mek = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: MLSStoragePaths.rustMEKAccount(for: testDID), expectedLength: 32)
    XCTAssertNotNil(mek)
    let contentRoot = try MLSKeychainManager.shared.retrieveKeyStrict(
      forKey: MLSStoragePaths.contentRootAccount(for: testDID),
      service: "blue.catbird.mls.content",
      expectedLength: 32
    )
    XCTAssertNotNil(contentRoot)

    // Reopen context
    try await MLSCoreContext.shared.reloadContext(for: testDID)
    let statusReopen = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .complete(let reopenedRecord) = statusReopen else {
      XCTFail("State must remain complete after reopen, got: \(statusReopen)")
      return
    }
    XCTAssertEqual(completeRecord.attemptUUID, reopenedRecord.attemptUUID)

    // Coordinated reset via clearStorage
    try await MLSClient.shared.clearStorage(for: testDID)

    // Verify allAbsent again
    let status2 = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .allAbsent = status2 else {
      XCTFail("State must be allAbsent after reset, got: \(status2)")
      return
    }
  }

  // MARK: - 10. Explicit reset removes only clean-generation resource set and propagates failure

  func test10_ExplicitResetRemovesCleanResourceSetAndPropagatesFailure() async throws {
    let testDID = "did:plc:reset_full_manifest_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // 1. Seed complete manifest:
    // Swift DB, WAL, SHM
    let manager = MLSGRDBManager()
    let pool = try await manager.getDatabasePool(for: testDID)
    try await pool.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS manifest_table (id TEXT PRIMARY KEY);")
      try db.execute(sql: "INSERT INTO manifest_table (id) VALUES ('seed');")
    }
    await manager.closeAllDatabases()

    // Rust state via coordinator
    _ = try await coordinator.coordinateOpen(for: .rustState, userDID: testDID) { attemptUUID, isFirstCreation in
      let rustDB = try coordinator.databaseURL(for: .rustState, userDID: testDID)
      try FileManager.default.createDirectory(at: rustDB.deletingLastPathComponent(), withIntermediateDirectories: true)
      try Data("rust-data".utf8).write(to: rustDB)
      let rustWAL = URL(fileURLWithPath: rustDB.path + "-wal")
      try Data("rust-wal".utf8).write(to: rustWAL)
      return attemptUUID
    }

    // Required and optional Keychain slots
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateKey(for: testDID)
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateSalt(for: testDID)
    _ = try MLSContentRootKey.loadOrCreate(for: testDID)
    _ = try MLSKeychainManager.shared.getOrCreateImmutableKey(
      forKey: MLSStoragePaths.rustMEKAccount(for: testDID),
      length: 32
    )
    _ = try MLSKeychainManager.shared.getOrCreateImmutableKey(
      forKey: MLSStoragePaths.identityBackupAccount(for: testDID),
      length: 32
    )
    let adapter = MLSOrchestratorCredentialAdapter()
    try adapter.storeSigningKey(userDid: testDID, keyData: Data(repeating: 0x77, count: 64))
    try adapter.storeMlsDid(userDid: testDID, mlsDid: "did:key:zManifest")
    try adapter.storeDeviceUuid(userDid: testDID, uuid: UUID().uuidString)
    let bridge = MLSKeychainAccessBridge(userDID: testDID)
    try await bridge.write(key: "hybrid_slot_1", value: Data("hybrid-val".utf8))

    // Quarantine and CAS temp entries
    let quarantineDir = try coordinator.quarantineDirectoryURL(for: .swiftGRDB)
    let qTag = MLSStoragePaths.quarantineTag(for: testDID)
    let userQuarantineDir = quarantineDir.appendingPathComponent("2026-08-28T00:00:00Z_\(qTag)", isDirectory: true)
    try FileManager.default.createDirectory(at: userQuarantineDir, withIntermediateDirectories: true)
    try Data("quarantine-file".utf8).write(to: userQuarantineDir.appendingPathComponent("corrupt.db"))

    let markerURL = try coordinator.markerURL(for: .swiftGRDB, userDID: testDID)
    let markerDir = markerURL.deletingLastPathComponent()
    let tempMarker = markerDir.appendingPathComponent("tmp_test_attempt_12345.json")
    try Data("temp-marker".utf8).write(to: tempMarker)

    // Verify setup
    let swiftDB = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
    XCTAssertTrue(FileManager.default.fileExists(atPath: swiftDB.path))
    XCTAssertTrue(FileManager.default.fileExists(atPath: userQuarantineDir.path))

    // 2. Injected failure test: custom callback throws -> marker MUST be retained
    struct InjectedDeletionError: Error {}
    do {
      try await coordinator.coordinateReset(for: .swiftGRDB, userDID: testDID) {
        throw InjectedDeletionError()
      }
      XCTFail("Reset must propagate injected deletion failure")
    } catch is InjectedDeletionError {
      // Verify marker was RETAINED on failure
      XCTAssertTrue(
        FileManager.default.fileExists(atPath: markerURL.path),
        "Marker must be retained when reset fails"
      )
    }

    // 3. Successful complete reset via clearStorage
    try await MLSClient.shared.clearStorage(for: testDID)

    // 4. Assert full manifest is strictly deleted
    XCTAssertFalse(FileManager.default.fileExists(atPath: swiftDB.path), "Swift DB must be deleted")
    XCTAssertFalse(FileManager.default.fileExists(atPath: userQuarantineDir.path), "User quarantine must be deleted")
    XCTAssertFalse(FileManager.default.fileExists(atPath: tempMarker.path), "Temp marker must be deleted")
    XCTAssertFalse(FileManager.default.fileExists(atPath: markerURL.path), "Marker must be deleted last")

    let rustDB = try coordinator.databaseURL(for: .rustState, userDID: testDID)
    XCTAssertFalse(FileManager.default.fileExists(atPath: rustDB.path), "Rust DB must be deleted")

    let postKey = try await MLSSQLCipherEncryption.shared.getKey(for: testDID)
    XCTAssertNil(postKey, "SQLCipher key must be deleted")
    let postSalt = try await MLSSQLCipherEncryption.shared.getSalt(for: testDID)
    XCTAssertNil(postSalt, "SQLCipher salt must be deleted")
    let postContent = try? MLSContentRootKey.loadStrict(for: testDID)
    XCTAssertNil(postContent, "Content root key must be deleted")
    let postMEK = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: MLSStoragePaths.rustMEKAccount(for: testDID))
    XCTAssertNil(postMEK, "MEK must be deleted")
    let postBackup = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: MLSStoragePaths.identityBackupAccount(for: testDID))
    XCTAssertNil(postBackup, "Identity backup must be deleted")
    let postSigner = try adapter.getSigningKey(userDid: testDID)
    XCTAssertNil(postSigner, "Orchestrator signer must be deleted")
    let postMlsDid = try adapter.getMlsDid(userDid: testDID)
    XCTAssertNil(postMlsDid, "MLS DID must be deleted")
    let postDeviceUuid = try adapter.getDeviceUuid(userDid: testDID)
    XCTAssertNil(postDeviceUuid, "Device UUID must be deleted")
    let postHybrid = try await bridge.read(key: "hybrid_slot_1")
    XCTAssertNil(postHybrid, "Hybrid signer slot must be deleted")
  }

  // MARK: - 11. Legacy sentinels and counters remain byte-for-byte unchanged

  func test11_LegacySentinelsAndCountersRemainByteForByteUnchanged() async throws {
    let testDID = "did:plc:legacy_sentinel_\(UUID().uuidString)"
    let baseContainer = MLSStoragePaths.baseContainerURL()

    let legacyDir = baseContainer.appendingPathComponent("MLS", isDirectory: true)
    try FileManager.default.createDirectory(at: legacyDir, withIntermediateDirectories: true)
    let legacySentinelURL = legacyDir.appendingPathComponent("sentinel.bin")
    let legacyWAL = legacyDir.appendingPathComponent("sentinel.db-wal")
    let legacySHM = legacyDir.appendingPathComponent("sentinel.db-shm")

    let legacyData = Data((0..<256).map { UInt8($0) })
    try legacyData.write(to: legacySentinelURL)
    try Data("legacy-wal-content".utf8).write(to: legacyWAL)
    try Data("legacy-shm-content".utf8).write(to: legacySHM)

    // Legacy keychain sentinels
    _ = try fakeKeychain.add(service: "blue.catbird.mls", account: "mls.legacy.key.\(testDID)", data: Data("legacy-key".utf8))
    _ = try fakeKeychain.add(service: "blue.catbird.mls.signature", account: "blue.catbird.mls.sig.\(testDID)", data: Data("legacy-sig".utf8))

    let initialHash = SHA256.hash(data: legacyData)
    let initialWALHash = SHA256.hash(data: try Data(contentsOf: legacyWAL))
    let initialSHMHash = SHA256.hash(data: try Data(contentsOf: legacySHM))

    // Run clean open
    let manager = MLSGRDBManager()
    let pool = try await manager.getDatabasePool(for: testDID)
    try await pool.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS dummy (id TEXT);")
    }
    await manager.closeAllDatabases()

    // Verify legacy sentinels unchanged after clean open
    XCTAssertEqual(SHA256.hash(data: try Data(contentsOf: legacySentinelURL)), initialHash)
    XCTAssertEqual(SHA256.hash(data: try Data(contentsOf: legacyWAL)), initialWALHash)
    XCTAssertEqual(SHA256.hash(data: try Data(contentsOf: legacySHM)), initialSHMHash)
    let legacyKey = try fakeKeychain.get(service: "blue.catbird.mls", account: "mls.legacy.key.\(testDID)")
    XCTAssertEqual(legacyKey, Data("legacy-key".utf8))
    let legacySig = try fakeKeychain.get(service: "blue.catbird.mls.signature", account: "blue.catbird.mls.sig.\(testDID)")
    XCTAssertEqual(legacySig, Data("legacy-sig".utf8))

    // Trigger explicit clean reset
    await MLSClient.shared.destroyStorageCompletely(for: testDID)

    // Verify legacy sentinels completely untouched after clean reset
    XCTAssertEqual(SHA256.hash(data: try Data(contentsOf: legacySentinelURL)), initialHash)
    XCTAssertEqual(SHA256.hash(data: try Data(contentsOf: legacyWAL)), initialWALHash)
    XCTAssertEqual(SHA256.hash(data: try Data(contentsOf: legacySHM)), initialSHMHash)
    let postResetLegacyKey = try fakeKeychain.get(service: "blue.catbird.mls", account: "mls.legacy.key.\(testDID)")
    XCTAssertEqual(postResetLegacyKey, Data("legacy-key".utf8))
    let postResetLegacySig = try fakeKeychain.get(service: "blue.catbird.mls.signature", account: "blue.catbird.mls.sig.\(testDID)")
    XCTAssertEqual(postResetLegacySig, Data("legacy-sig".utf8))
  }

  // MARK: - 12. Identifiers contain exact generation/suffix and never legacy names

  func test12_AllIdentifiersContainExactGenerationAndSuffixAndNeverLegacyNames() throws {
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

    // Darwin notifications
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
