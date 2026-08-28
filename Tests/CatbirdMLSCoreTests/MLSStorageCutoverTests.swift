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
    final class SendableBarrier: @unchecked Sendable {
      let condition = NSCondition()
      var arrived = 0
      let total: Int

      init(total: Int) {
        self.total = total
      }

      func arriveAndWait() {
        condition.lock()
        arrived += 1
        if arrived < total {
          while arrived < total {
            condition.wait()
          }
        } else {
          condition.broadcast()
        }
        condition.unlock()
      }
    }

    let barrier = SendableBarrier(total: 2)

    coordinator.testPrePublicationHook = {
      barrier.arriveAndWait()
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
    try await MLSCoreContext.shared.ensureContext(for: testDID)
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
    MLSStorageCoordinator.shared.releaseAdmissionLease(for: .rustState, userDID: testDID)
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
    let userQuarantineDir = quarantineDir.appendingPathComponent("quarantine_\(MLSDatabaseKind.swiftGRDB.rawValue)_\(qTag)_test", isDirectory: true)
    try FileManager.default.createDirectory(at: userQuarantineDir, withIntermediateDirectories: true)
    try Data("quarantine-file".utf8).write(to: userQuarantineDir.appendingPathComponent("corrupt.db"))

    let markerURL = try coordinator.markerURL(for: .swiftGRDB, userDID: testDID)
    let markerDir = markerURL.deletingLastPathComponent()
    let pathHash = try coordinator.databasePathHash(for: .swiftGRDB, userDID: testDID)
    let tempPrefix = "tmp_\(MLSDatabaseKind.swiftGRDB.rawValue)_\(pathHash)_"
    let tempMarker = markerDir.appendingPathComponent("\(tempPrefix)test_attempt_12345.json")
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
    // Re-seed Swift manifest (DB, sidecars, quarantine, temp marker, key/salt) so complete reset tests real deletion
    try Data("swift-db-reseed".utf8).write(to: swiftDB)
    try Data("swift-wal-reseed".utf8).write(to: URL(fileURLWithPath: swiftDB.path + "-wal"))
    try FileManager.default.createDirectory(at: userQuarantineDir, withIntermediateDirectories: true)
    try Data("quarantine-file-2".utf8).write(to: userQuarantineDir.appendingPathComponent("corrupt2.db"))
    try Data("temp-marker-2".utf8).write(to: tempMarker)
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateKey(for: testDID)
    _ = try await MLSSQLCipherEncryption.shared.getOrCreateSalt(for: testDID)

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

  // MARK: - 13. Coordination store fails closed without process-local fallback

  func test13_CoordinationStoreFailsClosedOnCorruptState() throws {
    let store = MLSCoordinationStore.shared
    let coordinationDir = try MLSStoragePaths.coordinationDirectory()
    let fileURL = coordinationDir.appendingPathComponent("coordination_state.\(MLSStoragePaths.cleanSuffix).json")

    // Write corrupt JSON
    try FileManager.default.createDirectory(at: coordinationDir, withIntermediateDirectories: true)
    try Data("corrupted-not-json".utf8).write(to: fileURL, options: .atomic)

    // Strict fetch must throw unreadableState, not return State.initial
    XCTAssertThrowsError(try store.fetchState()) { error in
      guard case MLSStorageInitializationError.unreadableState = error else {
        XCTFail("Expected unreadableState for corrupt JSON, got \(error)")
        return
      }
    }

    // Non-throwing getState must fail closed (generation -1, phase .closed)
    let state = store.getState()
    XCTAssertEqual(state.coordinationGeneration, -1)
    XCTAssertEqual(state.phase, .closed)
    XCTAssertEqual(store.currentGeneration, -1)

    // validateGeneration must throw for corrupt state
    XCTAssertThrowsError(try store.validateGeneration(1))
  }

  // MARK: - 14. Access group caching retries on nil

  func test14_AccessGroupResolutionDoesNotPermanentlyCacheNil() {
    MLSKeychainManager.resetAccessGroupResolutionForTesting()
    // Calling sharedAccessGroup() when probe returns nil (e.g. before unlock / test environment)
    // must not permanently latch didResolveGroup if resolution was nil.
    _ = MLSKeychainManager.sharedAccessGroup()
    // Calling again should retry resolution rather than assuming permanent failure
    _ = MLSKeychainManager.sharedAccessGroup()
  }

  // MARK: - 15. Keychain keys include cleanSuffix

  func test15_KeychainConversationAndInviteKeysContainCleanSuffix() throws {
    let conversationID = "convo_12345"
    let inviteID = "inv_67890"
    let userDID = "did:plc:user12345"

    let groupStateKey = MLSKeychainManager.KeychainKey.groupState(conversationID: conversationID).key
    XCTAssertEqual(
      groupStateKey,
      "mls.groupstate.\(conversationID).\(MLSStoragePaths.cleanSuffix)",
      "Group state key must match expected clean cutover format"
    )
    XCTAssertTrue(groupStateKey.hasSuffix(".\(MLSStoragePaths.cleanSuffix)"))

    let inviteKey = MLSKeychainManager.KeychainKey.invitePSK(inviteID: inviteID).key
    XCTAssertEqual(
      inviteKey,
      "mls.invite.psk.\(inviteID).\(MLSStoragePaths.cleanSuffix)",
      "Invite PSK key must match expected clean cutover format"
    )
    XCTAssertTrue(inviteKey.hasSuffix(".\(MLSStoragePaths.cleanSuffix)"))

    let rejoinKey = MLSKeychainManager.KeychainKey.rejoinPSK(
      conversationID: conversationID,
      userDID: userDID
    ).key
    XCTAssertEqual(
      rejoinKey,
      "mls.rejoin.psk.\(conversationID).\(userDID).\(MLSStoragePaths.cleanSuffix)",
      "Rejoin PSK key must match expected clean cutover format"
    )
    XCTAssertTrue(rejoinKey.hasSuffix(".\(MLSStoragePaths.cleanSuffix)"))
  }

  // MARK: - 16. Colliding DIDs resolve to distinct URLs and independent reset

  func test16_CollidingDIDsResolveToDistinctDatabaseURLsAndResetDoesNotDeleteCollidingDID() async throws {
    let coordinator = MLSStorageCoordinator.shared
    let didColon = "did:example:alice"
    let didHyphen = "did-example-alice"

    let grdbColonURL = try coordinator.databaseURL(for: .swiftGRDB, userDID: didColon)
    let grdbHyphenURL = try coordinator.databaseURL(for: .swiftGRDB, userDID: didHyphen)
    XCTAssertNotEqual(grdbColonURL.path, grdbHyphenURL.path, "Colon vs hyphen DIDs must produce distinct GRDB URLs")

    let longPrefix = "did:plc:abcdefghijklmnopqrstuvwxyz0123456789abcdefghij"
    let didLong1 = "\(longPrefix)1"
    let didLong2 = "\(longPrefix)2"
    let rustLong1URL = try coordinator.databaseURL(for: .rustState, userDID: didLong1)
    let rustLong2URL = try coordinator.databaseURL(for: .rustState, userDID: didLong2)
    XCTAssertNotEqual(rustLong1URL.path, rustLong2URL.path, "Long DIDs with same 48-byte prefix must produce distinct Rust URLs")

    // Create both databases via coordinator fixtures without violating active-user policy
    _ = try await coordinator.coordinateOpen(for: .swiftGRDB, userDID: didColon) { _, _ in
      try FileManager.default.createDirectory(at: grdbColonURL.deletingLastPathComponent(), withIntermediateDirectories: true)
      try Data("colon-data".utf8).write(to: grdbColonURL)
    }
    coordinator.releaseAdmissionLease(for: .swiftGRDB, userDID: didColon)

    _ = try await coordinator.coordinateOpen(for: .swiftGRDB, userDID: didHyphen) { _, _ in
      try FileManager.default.createDirectory(at: grdbHyphenURL.deletingLastPathComponent(), withIntermediateDirectories: true)
      try Data("hyphen-data".utf8).write(to: grdbHyphenURL)
    }
    coordinator.releaseAdmissionLease(for: .swiftGRDB, userDID: didHyphen)
    // Reset didColon
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: didColon)

    // Verify didHyphen DB and keys remain intact
    XCTAssertFalse(FileManager.default.fileExists(atPath: grdbColonURL.path), "Colon DB must be deleted")
    XCTAssertTrue(FileManager.default.fileExists(atPath: grdbHyphenURL.path), "Hyphen DB must NOT be deleted by colon reset")
    let hyphenKey = try await MLSSQLCipherEncryption.shared.getKey(for: didHyphen)
    XCTAssertNotNil(hyphenKey, "Hyphen key must remain in Keychain")

    // Clean up didHyphen
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: didHyphen)
  }

  // MARK: - 17. Simultaneous signer and credential loser adoption

  func test17_SimultaneousSignerAndCredentialLoserAdoption() async throws {
    let testDID = "did:plc:loser_adopt_\(UUID().uuidString)"
    let adapter = MLSOrchestratorCredentialAdapter()

    let winnerSigner = Data(repeating: 0x11, count: 64)
    let loserSigner = Data(repeating: 0x22, count: 64)
    let winnerMlsDid = "did:key:zWinner123"
    let loserMlsDid = "did:key:zLoser456"
    let winnerDeviceUuid = UUID().uuidString
    let loserDeviceUuid = UUID().uuidString

    // Winner stores credentials first
    try adapter.storeSigningKey(userDid: testDID, keyData: winnerSigner)
    try adapter.storeMlsDid(userDid: testDID, mlsDid: winnerMlsDid)
    try adapter.storeDeviceUuid(userDid: testDID, uuid: winnerDeviceUuid)

    // Loser stores different credentials concurrently -> must adopt the winner (no throw, no overwrite)
    try adapter.storeSigningKey(userDid: testDID, keyData: loserSigner)
    try adapter.storeMlsDid(userDid: testDID, mlsDid: loserMlsDid)
    try adapter.storeDeviceUuid(userDid: testDID, uuid: loserDeviceUuid)

    // Re-reading must strictly return the winner's credentials
    let loadedSigner = try adapter.getSigningKey(userDid: testDID)
    XCTAssertEqual(loadedSigner, winnerSigner, "Must adopt winning signer")

    let loadedMlsDid = try adapter.getMlsDid(userDid: testDID)
    XCTAssertEqual(loadedMlsDid, winnerMlsDid, "Must adopt winning MLS DID")

    let loadedDeviceUuid = try adapter.getDeviceUuid(userDid: testDID)
    XCTAssertEqual(loadedDeviceUuid, winnerDeviceUuid, "Must adopt winning device UUID")

    // clearAll must fail closed without deleting
    XCTAssertThrowsError(try adapter.clearAll(userDid: testDID)) { error in
      guard case MLSStorageInitializationError.validationFailed = error else {
        XCTFail("Expected validationFailed on clearAll, got \(error)")
        return
      }
    }

    // Verify credentials remain in Keychain after rejected clearAll
    XCTAssertEqual(try adapter.getSigningKey(userDid: testDID), winnerSigner)
  }

  // MARK: - 18. Corrupt handshake counter and indexed record fail closed

  func test18_CorruptHandshakeCounterAndIndexedRecordFailClosed() throws {
    let testDID = "did:plc:corrupt_handshake_\(UUID().uuidString)"
    let store = MLSAppGroupHandshakeStore.shared
    let defaults = UserDefaults(suiteName: "group.blue.catbird.shared")!

    // Seed corrupt non-numeric counter
    let counterDigest = SHA256.hash(data: Data(testDID.utf8)).compactMap { String(format: "%02x", $0) }.joined()
    let counterKey = "mls_handshake_counter.\(counterDigest.prefix(16)).\(MLSStoragePaths.cleanSuffix)"
    defaults.set("corrupt-string-value", forKey: counterKey)
    defaults.synchronize()

    // Assert issueWillCloseRequest throws unreadableState instead of resetting to token 1
    XCTAssertThrowsError(try store.issueWillCloseRequest(for: testDID)) { error in
      guard case MLSStorageInitializationError.unreadableState = error else {
        XCTFail("Expected unreadableState for corrupt counter, got \(error)")
        return
      }
    }

    // Clear corrupt counter and issue valid request
    defaults.removeObject(forKey: counterKey)
    let validReq = try store.issueWillCloseRequest(for: testDID)
    XCTAssertEqual(validReq.token, 1)

    let requestsBefore = try store.allRequests()
    XCTAssertEqual(requestsBefore.count, 1)
    XCTAssertEqual(requestsBefore.first?.userDID, testDID)

    // Corrupt request record in defaults
    let reqDigest = SHA256.hash(data: Data(testDID.utf8)).compactMap { String(format: "%02x", $0) }.joined()
    let requestKey = "mls_handshake_request.\(reqDigest.prefix(16)).\(MLSStoragePaths.cleanSuffix)"
    defaults.set(Data("corrupt-undecodable-json".utf8), forKey: requestKey)
    defaults.synchronize()

    // Assert allRequests throws unreadableState without skipping or mutating
    XCTAssertThrowsError(try store.allRequests()) { error in
      guard case MLSStorageInitializationError.unreadableState = error else {
        XCTFail("Expected unreadableState for corrupt request record, got \(error)")
        return
      }
    }

    try store.clearAll(for: testDID)
  }

  // MARK: - 19. Validate-only reopen byte immutability and schema validation

  func test19_ValidateOnlyReopenByteImmutabilityAndSchemaValidation() async throws {
    let testDID = "did:plc:reopen_immutability_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    let manager1 = MLSGRDBManager()
    let pool1 = try await manager1.getDatabasePool(for: testDID)
    try await pool1.write { db in
      try db.execute(
        sql: """
        INSERT INTO MLSConversationModel (conversationID, currentUserDID, groupID, createdAt, updatedAt, needsReset, isUnrecoverable)
        VALUES ('c1', ?, ?, datetime('now'), datetime('now'), 0, 0);
        """,
        arguments: [testDID, Data([0x01, 0x02, 0x03])]
      )
    }
    await manager1.closeAllDatabases()

    let dbURL = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
    let key = try await MLSSQLCipherEncryption.shared.getKey(for: testDID)!
    let salt = try await MLSSQLCipherEncryption.shared.getSalt(for: testDID)!

    let initialBytes = try Data(contentsOf: dbURL)
    let initialHash = SHA256.hash(data: initialBytes)

    // Call read-only validator directly and assert strict byte immutability
    try await manager1.validateExistingGRDBDatabase(at: dbURL, encryptionKey: key, salt: salt)
    let postValidationBytes = try Data(contentsOf: dbURL)
    let postValidationHash = SHA256.hash(data: postValidationBytes)
    XCTAssertEqual(initialHash, postValidationHash, "Database bytes must remain immutable across read-only validation")

    // Separately prove normal reopen succeeds and reads stored conversation
    let manager2 = MLSGRDBManager()
    let pool2 = try await manager2.getDatabasePool(for: testDID)
    let readConvo = try await pool2.read { db in
      try String.fetchOne(db, sql: "SELECT conversationID FROM MLSConversationModel WHERE conversationID = 'c1'")
    }
    XCTAssertEqual(readConvo, "c1")
    await manager2.closeAllDatabases()

    await MLSClient.shared.destroyStorageCompletely(for: testDID)
  }

  // MARK: - 20. Partial optional Rust credential slots fail closed on complete reopen

  func test20_PartialOptionalRustCredentialSlotsFailClosedOnCompleteReopen() async throws {
    let testDID = "did:plc:partial_rust_creds_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // 1. Create complete Rust state via coordinator
    _ = try await coordinator.coordinateOpen(for: .rustState, userDID: testDID) { attemptUUID, isFirstCreation in
      let dbURL = try coordinator.databaseURL(for: .rustState, userDID: testDID)
      try FileManager.default.createDirectory(at: dbURL.deletingLastPathComponent(), withIntermediateDirectories: true)
      let header = Data("SQLite format 3\0".utf8) + Data(repeating: 0x00, count: 16)
      try header.write(to: dbURL)
    }
    coordinator.releaseAdmissionLease(for: .rustState, userDID: testDID)

    // State without optional slots -> complete
    let state0 = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .complete = state0 else {
      XCTFail("Expected complete when 0 optional slots present, got \(state0)")
      return
    }

    let adapter = MLSOrchestratorCredentialAdapter()

    // Store 1 slot (mlsDid) -> partial -> mixedState
    try adapter.storeMlsDid(userDid: testDID, mlsDid: "did:key:zTest")
    let state1 = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .mixedState = state1 else {
      XCTFail("Expected mixedState when 1 of 3 optional slots present, got \(state1)")
      return
    }

    // Store 2nd slot (deviceUuid) -> partial -> mixedState
    try adapter.storeDeviceUuid(userDid: testDID, uuid: UUID().uuidString)
    let state2 = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .mixedState = state2 else {
      XCTFail("Expected mixedState when 2 of 3 optional slots present, got \(state2)")
      return
    }

    // Store 3rd slot (signer) -> all 3 present -> complete
    try adapter.storeSigningKey(userDid: testDID, keyData: Data(repeating: 0x33, count: 64))
    let state3 = try coordinator.evaluateState(for: .rustState, userDID: testDID)
    guard case .complete = state3 else {
      XCTFail("Expected complete when all 3 optional slots present, got \(state3)")
      return
    }

    try await coordinator.coordinateReset(for: .rustState, userDID: testDID)
  }

  // MARK: - 21. Reset blocked by live admission lease

  func test21_ResetBlockedByLiveAdmissionLease() async throws {
    let testDID = "did:plc:lease_reset_block_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    let manager = MLSGRDBManager()
    let pool = try await manager.getDatabasePool(for: testDID)
    try await pool.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS test_lease (id TEXT PRIMARY KEY);")
    }

    // While database is open, coordinator holds active admission lease (LOCK_SH)
    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID))

    // Attempting exclusive reset lease in parallel should fail / be blocked
    let leaseURL = try coordinator.leaseURL(for: .swiftGRDB, userDID: testDID, type: "admission")
    let testFd = open(leaseURL.path, O_RDWR, 0o666)
    XCTAssertGreaterThanOrEqual(testFd, 0)
    defer { close(testFd) }

    // Non-blocking exclusive lock must fail because handle holds LOCK_SH
    let lockResult = flock(testFd, LOCK_EX | LOCK_NB)
    XCTAssertEqual(lockResult, -1, "LOCK_EX must be denied while live handle holds admission lease")
    XCTAssertEqual(errno, EWOULDBLOCK)

    // Close database pool -> releases admission lease
    await manager.closeAllDatabases()
    XCTAssertFalse(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID))

    // Now coordinateReset succeeds cleanly
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: testDID)
    let dbURL = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
    XCTAssertFalse(FileManager.default.fileExists(atPath: dbURL.path), "Database must be deleted after reset")
  }

  // MARK: - 22. NSE Database Owner Mapping Contract

  func test22_NSEDatabaseOwnerMappingContract() throws {
    // Canonical method-specific DID with uppercase character spelling (e.g. did:key:z6MkuV...)
    let canonicalDID = "did:key:z6MkuV8w3qFzYy4X1v8"
    let exactDigest = SHA256.hash(data: Data(canonicalDID.utf8)).compactMap { String(format: "%02x", $0) }.joined()

    // Key must bind to exact canonical bytes hash
    let key = MLSStoragePaths.databaseOwnerMappingKey(for: canonicalDID)
    XCTAssertTrue(key.contains(exactDigest))

    // Publish mapping
    try MLSStoragePaths.publishDatabaseOwnerMapping(for: canonicalDID)

    // Resolve via exact hash (lowercase) -> returns exact canonical DID spelling
    let resolvedLower = try MLSStoragePaths.resolveDatabaseOwnerDID(forNormalizedDIDHash: exactDigest)
    XCTAssertEqual(resolvedLower, canonicalDID)

    // Resolve via uppercase hex query -> returns exact canonical DID spelling
    let resolvedUpper = try MLSStoragePaths.resolveDatabaseOwnerDID(forNormalizedDIDHash: exactDigest.uppercased())
    XCTAssertEqual(resolvedUpper, canonicalDID)

    // Unrelated hash returns nil
    let fakeHash = String(repeating: "f", count: 64)
    let resolvedNil = try MLSStoragePaths.resolveDatabaseOwnerDID(forNormalizedDIDHash: fakeHash)
    XCTAssertNil(resolvedNil)

    // Clean up via remove
    try MLSStoragePaths.removeDatabaseOwnerMapping(for: canonicalDID)
    let postRemove = try MLSStoragePaths.resolveDatabaseOwnerDID(forNormalizedDIDHash: exactDigest)
    XCTAssertNil(postRemove)
  }

  // MARK: - 23. Owner mapping failure unwinds lease without leak

  func test23_OwnerMappingFailureUnwindsLeaseWithoutLeak() async throws {
    let testDID = "did:plc:mapping_fail_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared
    let defaults = UserDefaults(suiteName: "group.blue.catbird.shared")!

    // Seed corrupt owner mapping
    let mappingKey = MLSStoragePaths.databaseOwnerMappingKey(for: testDID)
    defaults.set(Data("corrupt-non-string".utf8), forKey: mappingKey)
    defaults.synchronize()

    // coordinateOpen should fail on owner mapping publish
    do {
      _ = try await coordinator.coordinateOpen(for: .swiftGRDB, userDID: testDID) { _, _ in
        let dbURL = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
        try FileManager.default.createDirectory(at: dbURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("dummy".utf8).write(to: dbURL)
      }
      XCTFail("coordinateOpen must fail on corrupt owner mapping")
    } catch {
      // Assert admission lease was NOT leaked
      XCTAssertFalse(
        coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID),
        "Admission lease must be unwound on owner mapping failure"
      )
    }

    // Clean up mapping and reset
    defaults.removeObject(forKey: mappingKey)
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: testDID)
  }

  // MARK: - 24. Admission lease refcounts balanced across multiple handles and evictions

  func test24_AdmissionLeaseRefcountsBalancedAcrossMultipleHandlesAndEvictions() async throws {
    let testDID = "did:plc:refcount_balance_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared
    let manager = MLSGRDBManager()

    // Open 1: Heavy DatabasePool -> refcount = 1
    let pool = try await manager.getDatabasePool(for: testDID)
    try await pool.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS balance_test (id TEXT PRIMARY KEY);")
    }
    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID))

    // Open 2: Second handle / coordinateOpen -> refcount = 2
    _ = try await coordinator.coordinateOpen(for: .swiftGRDB, userDID: testDID) { _, _ in
      return true
    }
    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID))

    // Release second handle -> refcount = 1 (lease must STILL be held by pool!)
    coordinator.releaseAdmissionLease(for: .swiftGRDB, userDID: testDID)
    XCTAssertTrue(
      coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID),
      "Lease must still be held while pool is alive"
    )

    // Open 3: Lightweight queue read -> refcount = 2
    let val = try await manager.read(for: testDID) { db in
      try String.fetchOne(db, sql: "SELECT 'ok';")
    }
    XCTAssertEqual(val, "ok")
    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID))

    // Evict lightweight queue -> refcount = 1
    await manager.closeAllLightweightQueues()
    XCTAssertTrue(
      coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID),
      "Lease must still be held while pool is alive after lightweight eviction"
    )

    // Close pool -> refcount = 0 -> lease released completely
    await manager.closeDatabase(for: testDID)
    XCTAssertFalse(
      coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID),
      "Lease must be completely released when all handles close"
    )

    // coordinateReset succeeds immediately without block
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: testDID)
  }

  // MARK: - 25. Concurrent DIDs handshake index serialization

  func test25_ConcurrentDIDsHandshakeIndexSerialization() async throws {
    let store = MLSAppGroupHandshakeStore.shared
    let dids = (0..<5).map { "did:plc:concurrent_user_\($0)_\(UUID().uuidString)" }

    // Issue concurrent requests from multiple asynchronous tasks
    await withTaskGroup(of: Void.self) { group in
      for did in dids {
        group.addTask {
          _ = try? store.issueWillCloseRequest(for: did)
        }
      }
    }

    // Verify all DIDs are present in the active request index and decodable without loss
    let allRequests = try store.allRequests()
    let foundDIDs = Set(allRequests.map(\.userDID))
    for did in dids {
      XCTAssertTrue(foundDIDs.contains(did), "Concurrent DID \(did) must not be lost from active index")
      try store.clearAll(for: did)
    }
  }

  // MARK: - 26. Rust missing table fails closed without disk mutation

  func test26_RustMissingTableFailsClosedWithoutDiskMutation() async throws {
    let testDID = "did:plc:rust_missing_table_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared
    let dbURL = try coordinator.databaseURL(for: .rustState, userDID: testDID)
    try FileManager.default.createDirectory(at: dbURL.deletingLastPathComponent(), withIntermediateDirectories: true)

    // Create SQLCipher database with 32-byte plaintext header but missing required tables
    let keyData = try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: MLSStoragePaths.rustMEKAccount(for: testDID), length: 32)
    let encryptionKey = keyData.hexEncodedString()
    let keyHash = SHA256.hash(data: Data(encryptionKey.utf8))
    let saltHex = Array(keyHash.prefix(16)).map { String(format: "%02x", $0) }.joined()

    var config = GRDB.Configuration()
    config.prepareDatabase { db in
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")
      try db.execute(sql: "PRAGMA key = '\(encryptionKey)';")
      try db.execute(sql: "PRAGMA cipher_plaintext_header_size = 32;")
      try db.execute(sql: "PRAGMA cipher_salt = \"x'\(saltHex)'\";")
      try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
      try db.execute(sql: "PRAGMA kdf_iter = 256000;")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
    }
    let queue = try DatabaseQueue(path: dbURL.path, configuration: config)
    try await queue.write { db in
      try db.execute(sql: "CREATE TABLE mls_manifests (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
      try db.execute(sql: "CREATE TABLE mls_key_package_bundles (hash_ref TEXT PRIMARY KEY, bundle_b64 TEXT NOT NULL, created_at INTEGER NOT NULL);")
      try db.execute(sql: "CREATE TABLE mls_own_echo_proofs (canonical_entry_sha256 BLOB PRIMARY KEY, accepted_request_sha256 BLOB NOT NULL, conversation_id TEXT NOT NULL, group_id BLOB NOT NULL, server_entry_id TEXT NOT NULL, mls_epoch INTEGER NOT NULL, aad_sha256 BLOB NOT NULL, ciphertext_sha256 BLOB NOT NULL);")
      try db.execute(sql: "CREATE TABLE openmls_encryption_keys (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_epoch_keys_pairs (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_key_packages (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_own_leaf_nodes (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_proposals (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_psks (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_signature_keys (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE vc_emulation_group_secrets (epoch_id BLOB, secret_type TEXT, PRIMARY KEY (epoch_id, secret_type));")
      try db.execute(sql: "CREATE TABLE vc_emulation_bindings (group_id BLOB PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE vc_operation_trees (epoch_id BLOB PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE vc_retained_key_package_material (key_package_ref BLOB PRIMARY KEY, epoch_id BLOB);")
      try db.execute(sql: "CREATE INDEX vc_retained_key_package_material_epoch_id ON vc_retained_key_package_material(epoch_id);")
      try db.execute(sql: "CREATE TABLE registered_vc_emulation_epochs (group_id BLOB PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_sqlite_storage_migrations (version INTEGER PRIMARY KEY);")
      for v in 1...6 {
        try db.execute(sql: "INSERT INTO openmls_sqlite_storage_migrations (version) VALUES (\(v));")
      }
      // Deliberately omit openmls_group_data
    }
    try queue.close()
    let initialBytes = try Data(contentsOf: dbURL)
    let initialHash = SHA256.hash(data: initialBytes)

    do {
      try await MLSCoreContext.shared.validateExistingRustDatabase(at: dbURL.path, encryptionKey: encryptionKey)
      XCTFail("Must fail closed on missing openmls_group_data table")
    } catch {
      guard case MLSStorageInitializationError.validationFailed(let details) = error else {
        XCTFail("Expected validationFailed, got \(error)")
        return
      }
      XCTAssertTrue(details.contains("openmls_group_data"), "Error details must mention missing table: \(details)")
    }
    let postValidationBytes = try Data(contentsOf: dbURL)
    let postValidationHash = SHA256.hash(data: postValidationBytes)
    XCTAssertEqual(initialHash, postValidationHash, "Corrupt schema validation must not mutate disk bytes")

    // Part B: Test database with tables but missing refinery migration version (only version 1, missing 2..6)
    let testDID2 = "did:plc:rust_missing_migration_\(UUID().uuidString)"
    let dbURL2 = try coordinator.databaseURL(for: .rustState, userDID: testDID2)
    try FileManager.default.createDirectory(at: dbURL2.deletingLastPathComponent(), withIntermediateDirectories: true)
    let keyData2 = try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: MLSStoragePaths.rustMEKAccount(for: testDID2), length: 32)
    let encKey2 = keyData2.hexEncodedString()
    let keyHash2 = SHA256.hash(data: Data(encKey2.utf8))
    let saltHex2 = Array(keyHash2.prefix(16)).map { String(format: "%02x", $0) }.joined()

    var config2 = GRDB.Configuration()
    config2.prepareDatabase { db in
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")
      try db.execute(sql: "PRAGMA key = '\(encKey2)';")
      try db.execute(sql: "PRAGMA cipher_plaintext_header_size = 32;")
      try db.execute(sql: "PRAGMA cipher_salt = \"x'\(saltHex2)'\";")
      try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
      try db.execute(sql: "PRAGMA kdf_iter = 256000;")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
    }
    let queue2 = try DatabaseQueue(path: dbURL2.path, configuration: config2)
    try await queue2.write { db in
      try db.execute(sql: "CREATE TABLE mls_manifests (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
      try db.execute(sql: "CREATE TABLE mls_key_package_bundles (hash_ref TEXT PRIMARY KEY, bundle_b64 TEXT NOT NULL, created_at INTEGER NOT NULL);")
      try db.execute(sql: "CREATE TABLE mls_own_echo_proofs (canonical_entry_sha256 BLOB PRIMARY KEY, accepted_request_sha256 BLOB NOT NULL, conversation_id TEXT NOT NULL, group_id BLOB NOT NULL, server_entry_id TEXT NOT NULL, mls_epoch INTEGER NOT NULL, aad_sha256 BLOB NOT NULL, ciphertext_sha256 BLOB NOT NULL);")
      try db.execute(sql: "CREATE TABLE openmls_encryption_keys (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_epoch_keys_pairs (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_group_data (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_key_packages (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_own_leaf_nodes (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_proposals (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_psks (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_signature_keys (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE vc_emulation_group_secrets (epoch_id BLOB, secret_type TEXT, PRIMARY KEY (epoch_id, secret_type));")
      try db.execute(sql: "CREATE TABLE vc_emulation_bindings (group_id BLOB PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE vc_operation_trees (epoch_id BLOB PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE vc_retained_key_package_material (key_package_ref BLOB PRIMARY KEY, epoch_id BLOB);")
      try db.execute(sql: "CREATE INDEX vc_retained_key_package_material_epoch_id ON vc_retained_key_package_material(epoch_id);")
      try db.execute(sql: "CREATE TABLE registered_vc_emulation_epochs (group_id BLOB PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_sqlite_storage_migrations (version INTEGER PRIMARY KEY);")
      try db.execute(sql: "INSERT INTO openmls_sqlite_storage_migrations (version) VALUES (1);")
    }
    let initialBytes2 = try Data(contentsOf: dbURL2)
    let initialHash2 = SHA256.hash(data: initialBytes2)

    do {
      try await MLSCoreContext.shared.validateExistingRustDatabase(at: dbURL2.path, encryptionKey: encKey2)
      XCTFail("Must fail closed on incomplete refinery migration version history [1]")
    } catch {
      guard case MLSStorageInitializationError.validationFailed(let details) = error else {
        XCTFail("Expected validationFailed, got \(error)")
        return
      }
      XCTAssertTrue(details.contains("migration versions must exactly match"), "Error details: \(details)")
    }

    let postValidationBytes2 = try Data(contentsOf: dbURL2)
    let postValidationHash2 = SHA256.hash(data: postValidationBytes2)
    XCTAssertEqual(initialHash2, postValidationHash2, "Incomplete refinery migration validation must not mutate disk bytes")

    // Part C: Test database with extra foreign/future migration version [1..7] -> must fail closed
    let testDID3 = "did:plc:rust_future_migration_\(UUID().uuidString)"
    let dbURL3 = try coordinator.databaseURL(for: .rustState, userDID: testDID3)
    try FileManager.default.createDirectory(at: dbURL3.deletingLastPathComponent(), withIntermediateDirectories: true)
    let keyData3 = try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: MLSStoragePaths.rustMEKAccount(for: testDID3), length: 32)
    let encKey3 = keyData3.hexEncodedString()
    let keyHash3 = SHA256.hash(data: Data(encKey3.utf8))
    let saltHex3 = Array(keyHash3.prefix(16)).map { String(format: "%02x", $0) }.joined()

    var config3 = GRDB.Configuration()
    config3.prepareDatabase { db in
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")
      try db.execute(sql: "PRAGMA key = '\(encKey3)';")
      try db.execute(sql: "PRAGMA cipher_plaintext_header_size = 32;")
      try db.execute(sql: "PRAGMA cipher_salt = \"x'\(saltHex3)'\";")
      try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
      try db.execute(sql: "PRAGMA kdf_iter = 256000;")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
    }
    let queue3 = try DatabaseQueue(path: dbURL3.path, configuration: config3)
    try await queue3.write { db in
      try db.execute(sql: "CREATE TABLE mls_manifests (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
      try db.execute(sql: "CREATE TABLE mls_key_package_bundles (hash_ref TEXT PRIMARY KEY, bundle_b64 TEXT NOT NULL, created_at INTEGER NOT NULL);")
      try db.execute(sql: "CREATE TABLE mls_own_echo_proofs (canonical_entry_sha256 BLOB PRIMARY KEY, accepted_request_sha256 BLOB NOT NULL, conversation_id TEXT NOT NULL, group_id BLOB NOT NULL, server_entry_id TEXT NOT NULL, mls_epoch INTEGER NOT NULL, aad_sha256 BLOB NOT NULL, ciphertext_sha256 BLOB NOT NULL);")
      try db.execute(sql: "CREATE TABLE openmls_encryption_keys (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_epoch_keys_pairs (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_group_data (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_key_packages (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_own_leaf_nodes (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_proposals (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_psks (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_signature_keys (id TEXT PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE vc_emulation_group_secrets (epoch_id BLOB, secret_type TEXT, PRIMARY KEY (epoch_id, secret_type));")
      try db.execute(sql: "CREATE TABLE vc_emulation_bindings (group_id BLOB PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE vc_operation_trees (epoch_id BLOB PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE vc_retained_key_package_material (key_package_ref BLOB PRIMARY KEY, epoch_id BLOB);")
      try db.execute(sql: "CREATE INDEX vc_retained_key_package_material_epoch_id ON vc_retained_key_package_material(epoch_id);")
      try db.execute(sql: "CREATE TABLE registered_vc_emulation_epochs (group_id BLOB PRIMARY KEY);")
      try db.execute(sql: "CREATE TABLE openmls_sqlite_storage_migrations (version INTEGER PRIMARY KEY);")
      for v in 1...7 {
        try db.execute(sql: "INSERT INTO openmls_sqlite_storage_migrations (version) VALUES (\(v));")
      }
    }
    let initialBytes3 = try Data(contentsOf: dbURL3)
    let initialHash3 = SHA256.hash(data: initialBytes3)

    do {
      try await MLSCoreContext.shared.validateExistingRustDatabase(at: dbURL3.path, encryptionKey: encKey3)
      XCTFail("Must fail closed on future/foreign migration version 7")
    } catch {
      guard case MLSStorageInitializationError.validationFailed(let details) = error else {
        XCTFail("Expected validationFailed, got \(error)")
        return
      }
      XCTAssertTrue(details.contains("migration versions must exactly match"), "Error details: \(details)")
    }
    let postValidationBytes3 = try Data(contentsOf: dbURL3)
    let postValidationHash3 = SHA256.hash(data: postValidationBytes3)
    XCTAssertEqual(initialHash3, postValidationHash3, "Foreign refinery version validation must not mutate disk bytes")
  }
  // MARK: - 27. Owner mapping publication failure leaves creating state without lease leak

  func test27_OwnerMappingPublicationFailureLeavesCreatingStateWithoutLeaseLeak() async throws {
    let testDID = "did:plc:crash_order_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared
    let defaults = UserDefaults(suiteName: "group.blue.catbird.shared")!

    // Seed conflicting owner mapping
    let mappingKey = MLSStoragePaths.databaseOwnerMappingKey(for: testDID)
    defaults.set("did:plc:conflicting_owner", forKey: mappingKey)
    defaults.synchronize()

    // Attempt first creation
    do {
      _ = try await coordinator.coordinateOpen(for: .swiftGRDB, userDID: testDID) { _, _ in
        let dbURL = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
        try FileManager.default.createDirectory(at: dbURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("uncompleted-data".utf8).write(to: dbURL)
      }
      XCTFail("coordinateOpen must fail when owner mapping publication conflicts")
    } catch {
      // Assert marker is NOT complete (remains creating or missing)
      let marker = try coordinator.readMarker(for: .swiftGRDB, userDID: testDID)
      XCTAssertNotEqual(marker?.state, .complete, "Marker must NOT be completed if owner mapping publication fails")

      // Assert admission lease was NOT leaked
      XCTAssertFalse(
        coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID),
        "Admission lease must not be held after publication failure"
      )
    }

    // Clean up
    defaults.removeObject(forKey: mappingKey)
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: testDID)
  }

  // MARK: - 28. GRDB missing table or migration fails closed without disk mutation

  func test28_GRDBMissingTableOrMigrationFailsClosedWithoutDiskMutation() async throws {
    let testDID = "did:plc:grdb_missing_v33_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // 1. Create valid complete GRDB database through standard first open
    let manager1 = MLSGRDBManager()
    let pool1 = try await manager1.getDatabasePool(for: testDID)
    try await pool1.write { db in
      try db.execute(
        sql: """
        INSERT INTO MLSConversationModel (conversationID, currentUserDID, groupID, createdAt, updatedAt, needsReset, isUnrecoverable)
        VALUES ('c1', ?, ?, datetime('now'), datetime('now'), 0, 0);
        """,
        arguments: [testDID, Data([0x01, 0x02, 0x03])]
      )
    }

    // 2. Remove v33 migration row
    try await pool1.write { db in
      try db.execute(sql: "DELETE FROM grdb_migrations WHERE identifier = 'v33_conversation_description';")
    }
    await manager1.closeAllDatabases()

    let dbURL = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
    let initialBytes = try Data(contentsOf: dbURL)
    let initialHash = SHA256.hash(data: initialBytes)

    // 3. Reopen in fresh manager must fail closed on missing required v33 migration
    let manager2 = MLSGRDBManager()
    do {
      _ = try await manager2.getDatabasePool(for: testDID)
      XCTFail("Must fail closed on missing v33 migration")
    } catch {
      guard case MLSStorageInitializationError.validationFailed(let details) = error else {
        XCTFail("Expected validationFailed, got \(error)")
        return
      }
      XCTAssertTrue(
        details.contains("GRDB migrations mismatch") || details.contains("v33"),
        "Validation error must fail on migration mismatch: \(details)"
      )
    }

    // 4. Assert disk bytes were not mutated by failed validation
    let postValidationBytes = try Data(contentsOf: dbURL)
    let postValidationHash = SHA256.hash(data: postValidationBytes)
    XCTAssertEqual(initialHash, postValidationHash, "Missing migration validation must not mutate disk bytes")

    await MLSClient.shared.destroyStorageCompletely(for: testDID)
  }

  // MARK: - 29. Rust emergency close lease release

  func test29_RustEmergencyCloseLeaseRelease() async throws {
    let testDID = "did:plc:rust_emergency_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // Open context via MLSCoreContext (performs first creation, OpenMLS migrations, registers for emergency close, and holds admission lease)
    try await MLSCoreContext.shared.ensureContext(for: testDID)
    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .rustState, userDID: testDID))

    // Emergency close releases the lease on success
    MLSCoreContext.emergencyCloseAllContexts()
    XCTAssertFalse(
      coordinator.hasActiveAdmissionLease(for: .rustState, userDID: testDID),
      "Emergency close must release admission lease"
    )

    // Reset succeeds immediately without blocking
    try await coordinator.coordinateReset(for: .rustState, userDID: testDID)

    // Clear the global suspension flag set by emergencyCloseAllContexts so it does not
    // pollute subsequent tests (the stricter pool/queue publication guard requires it).
    MLSCoreContext.clearSuspensionFlag()
  }

  // MARK: - 30. Welcome gate stale marker remains pending and reset cleans up

  func test30_WelcomeGateStaleMarkerRemainsPendingAndResetCleansUp() async throws {
    let testDID = "did:plc:welcome_gate_\(UUID().uuidString)"
    let convoID = "convo_gate_test_123"
    let coordinator = MLSStorageCoordinator.shared
    let gate = MLSWelcomeGate.shared

    // Begin welcome processing -> creates marker
    try await gate.beginWelcomeProcessing(for: convoID, userDID: testDID)
    let isPendingInitial = await gate.hasPendingWelcome(for: convoID, userDID: testDID)
    XCTAssertTrue(isPendingInitial)
    // Backdate marker file by 60 seconds
    let markerURL = await gate.markerURL(conversationID: convoID, userDID: testDID)
    XCTAssertTrue(FileManager.default.fileExists(atPath: markerURL.path))
    let oldDate = Date().addingTimeInterval(-60)
    try FileManager.default.setAttributes([.modificationDate: oldDate], ofItemAtPath: markerURL.path)

    // Check pending -> must fail closed (remain true) and NOT delete marker
    let isPendingStale = await gate.hasPendingWelcome(for: convoID, userDID: testDID)
    XCTAssertTrue(isPendingStale, "Stale welcome marker >30s must fail closed and remain pending")
    XCTAssertTrue(FileManager.default.fileExists(atPath: markerURL.path), "Stale welcome marker must not be auto-deleted")

    // Explicit reset must delete clean welcome marker
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: testDID)
    XCTAssertFalse(FileManager.default.fileExists(atPath: markerURL.path), "Explicit reset must delete welcome gate marker")
  }

  // MARK: - 31. GRDB manager deallocation releases leases

  func test31_GRDBManagerDeallocationReleasesLeases() async throws {
    let testDID = "did:plc:dealloc_test_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    let localManager = MLSGRDBManager()
    let pool = try await localManager.getDatabasePool(for: testDID)
    try await pool.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS dealloc_tbl (id TEXT PRIMARY KEY);")
    }
    let readVal = try await localManager.read(for: testDID) { db in
      try String.fetchOne(db, sql: "SELECT 'ok';")
    }
    XCTAssertEqual(readVal, "ok")
    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID))

    // Explicit shutdown releases all admission leases on proven close success
    await localManager.shutdownAllDatabases()
    XCTAssertFalse(
      coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID),
      "Shutdown must release all admission leases"
    )

    // Reset succeeds immediately
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: testDID)
  }

  // MARK: - 32. Inactive DID operations reuse single tracked pool

  func test32_RepeatedInactiveDIDOperationsReuseSingleTrackedPool() async throws {
    let activeDID = "did:plc:active_\(UUID().uuidString)"
    let inactiveDID = "did:plc:inactive_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared
    let manager = MLSGRDBManager()

    // 1. Establish active user
    _ = try await manager.getDatabasePool(for: activeDID)
    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: activeDID))

    // 2. Perform first ephemeral operation on inactive user
    let pool1 = try await manager.getEphemeralDatabasePool(for: inactiveDID)
    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: inactiveDID))

    // 3. Perform second ephemeral operation on same inactive user -> must reuse pool1 without leaking lease refcount
    let pool2 = try await manager.getEphemeralDatabasePool(for: inactiveDID)
    XCTAssertTrue(pool1 === pool2, "Repeated ephemeral access for inactive user must reuse tracked uncached pool")

    // 4. Closing the inactive database must release its single admission lease immediately
    let closed = await manager.closeDatabaseAndDrain(for: inactiveDID)
    XCTAssertTrue(closed)
    XCTAssertFalse(
      coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: inactiveDID),
      "Closing inactive pool must release its admission lease without refcount leak"
    )

    // Cleanup active user
    _ = await manager.closeDatabaseAndDrain(for: activeDID)
  }

  // MARK: - 33. Normal repeated context and pool getters do not close or release

  func test33_NormalRepeatedContextAndPoolGettersDoNotCloseOrRelease() async throws {
    let testDID = "did:plc:normal_getters_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // 1. Initial creation
    let manager = MLSGRDBManager()
    let pool1 = try await manager.getDatabasePool(for: testDID)
    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID))

    // 2. Normal repeated calls to getDatabasePool -> must return exact same pool and maintain lease
    let pool2 = try await manager.getDatabasePool(for: testDID)
    XCTAssertTrue(pool1 === pool2, "Repeated getDatabasePool calls must return same active pool without closing")
    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID))

    // Cleanup
    _ = await manager.closeDatabaseAndDrain(for: testDID)
    XCTAssertFalse(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID))
  }

  // MARK: - 34. Corrupt complete reopen does not publish owner mapping

  func test34_CorruptCompleteReopenDoesNotPublishOwnerMapping() async throws {
    let testDID = "did:plc:corrupt_no_mapping_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared
    let dbURL = try coordinator.databaseURL(for: .swiftGRDB, userDID: testDID)
    try FileManager.default.createDirectory(at: dbURL.deletingLastPathComponent(), withIntermediateDirectories: true)

    // Write corrupt database content
    let garbage = Data("not-a-sqlite-db".utf8)
    try garbage.write(to: dbURL)
    let initialBytes = try Data(contentsOf: dbURL)
    let initialHash = SHA256.hash(data: initialBytes)
    let initialAttrs = try FileManager.default.attributesOfItem(atPath: dbURL.path)
    let initialMtime = initialAttrs[.modificationDate] as? Date

    // Write complete marker directly
    let marker = MLSInitializationRecord(
      generationToken: MLSStoragePaths.generationToken,
      attemptUUID: UUID().uuidString,
      userDID: testDID.lowercased(),
      databaseKind: MLSDatabaseKind.swiftGRDB.rawValue,
      databasePathHash: try coordinator.databasePathHash(for: .swiftGRDB, userDID: testDID),
      state: .complete
    )
    try coordinator.writeMarkerDirectlyForTesting(marker)

    // Remove any existing owner mapping
    try? MLSStoragePaths.removeDatabaseOwnerMapping(for: testDID)

    // Attempting to coordinate open on complete state with missing/unvalidated owner mapping or corrupt DB must fail closed
    do {
      _ = try await coordinator.coordinateOpen(for: .swiftGRDB, userDID: testDID) { _, _ in
        XCTFail("Should not reach creation closure")
        throw MLSStorageInitializationError.validationFailed(details: "unreachable")
      }
      XCTFail("Must fail closed on reopen of complete marker without pre-existing valid owner mapping or valid DB")
    } catch {
      // Expected failure
    }

    // Verify disk bytes and mtime are untouched
    let postBytes = try Data(contentsOf: dbURL)
    let postHash = SHA256.hash(data: postBytes)
    let postAttrs = try FileManager.default.attributesOfItem(atPath: dbURL.path)
    let postMtime = postAttrs[.modificationDate] as? Date

    XCTAssertEqual(initialHash, postHash, "Corrupt complete reopen must not mutate disk bytes")
    XCTAssertEqual(initialMtime, postMtime, "Corrupt complete reopen must not update disk modification time")

    // Verify NO new owner mapping was written
    let digest = SHA256.hash(data: Data(testDID.utf8)).compactMap { String(format: "%02x", $0) }.joined()
    let resolved = try MLSStoragePaths.resolveDatabaseOwnerDID(forNormalizedDIDHash: digest)
    XCTAssertNil(resolved, "Corrupt complete reopen must not publish new owner mapping")

    // Cleanup
    try? FileManager.default.removeItem(at: dbURL)
    let markerURL = try coordinator.markerURL(for: .swiftGRDB, userDID: testDID)
    try? FileManager.default.removeItem(at: markerURL)
  }

  // MARK: - 35. Collision-resistant full DID isolation

  func test35_CollisionResistantFullDIDIsolation() async throws {
    // Two DIDs that share a long common prefix
    let prefix = "did:plc:shared_prefix_1234567890abcdef_"
    let didA = "\(prefix)userA_\(UUID().uuidString)"
    let didB = "\(prefix)userB_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    // Initialize both accounts cleanly
    let managerA = MLSGRDBManager()
    let poolA = try await managerA.getDatabasePool(for: didA)
    try await poolA.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS data_a (id TEXT PRIMARY KEY, value TEXT);")
      try db.execute(sql: "INSERT INTO data_a VALUES ('key1', 'payload_A');")
    }

    let managerB = MLSGRDBManager()
    let poolB = try await managerB.getDatabasePool(for: didB)
    try await poolB.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS data_b (id TEXT PRIMARY KEY, value TEXT);")
      try db.execute(sql: "INSERT INTO data_b VALUES ('key1', 'payload_B');")
    }

    // Capture B's database state and hash
    let dbURL_B = try coordinator.databaseURL(for: .swiftGRDB, userDID: didB)
    let bytesB_before = try Data(contentsOf: dbURL_B)
    let hashB_before = SHA256.hash(data: bytesB_before)

    // Drain and close account A
    _ = await managerA.closeDatabaseAndDrain(for: didA)

    // Reset account A completely
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: didA)

    // Assert account A is completely removed
    let dbURL_A = try coordinator.databaseURL(for: .swiftGRDB, userDID: didA)
    XCTAssertFalse(FileManager.default.fileExists(atPath: dbURL_A.path), "Account A database must be deleted")

    // Assert account B is completely untouched (bytes, marker, keys, admission)
    XCTAssertTrue(FileManager.default.fileExists(atPath: dbURL_B.path), "Account B database must still exist")
    let bytesB_after = try Data(contentsOf: dbURL_B)
    let hashB_after = SHA256.hash(data: bytesB_after)
    XCTAssertEqual(hashB_before, hashB_after, "Resetting account A must not mutate account B database bytes")

    let readValB = try await managerB.read(for: didB) { db in
      try String.fetchOne(db, sql: "SELECT value FROM data_b WHERE id='key1';")
    }
    XCTAssertEqual(readValB, "payload_B", "Account B data must remain readable and correct")

    // Cleanup account B
    _ = await managerB.closeDatabaseAndDrain(for: didB)
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: didB)
  }
  // MARK: - 36. Two managers same DID drain all handles before ACK/reset

  func test36_TwoManagersSameDIDDrainAllBeforeAck() async throws {
    let testDID = "did:plc:two_mgr_\(UUID().uuidString)"
    let coordinator = MLSStorageCoordinator.shared

    let manager1 = MLSGRDBManager()
    let pool1 = try await manager1.getDatabasePool(for: testDID)
    try await pool1.write { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS shared_tbl (id TEXT PRIMARY KEY);")
    }

    let manager2 = MLSGRDBManager()
    let pool2 = try await manager2.getDatabasePool(for: testDID)
    try await pool2.write { db in
      try db.execute(sql: "INSERT OR REPLACE INTO shared_tbl VALUES ('1');")
    }

    XCTAssertTrue(coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID))

    // closeAndDrainAllManagers must close BOTH manager1 and manager2 pools
    let drained = await MLSGRDBManager.closeAndDrainAllManagers(for: testDID)
    XCTAssertTrue(drained, "All managers must drain successfully")

    // Assert admission lease is released after both close
    XCTAssertFalse(
      coordinator.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID),
      "Admission lease must be fully released only after all managers close"
    )

    // Reset succeeds immediately without blocking
    try await coordinator.coordinateReset(for: .swiftGRDB, userDID: testDID)
  }
}
