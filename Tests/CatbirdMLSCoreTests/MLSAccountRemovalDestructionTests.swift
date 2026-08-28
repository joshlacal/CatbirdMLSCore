import CryptoKit
import Foundation
import XCTest
@testable import CatbirdMLSCore

final class MLSAccountRemovalDestructionTests: XCTestCase {

  private var tempBaseDir: URL!
  private var fakeKeychain: MLSKeychainFakeStorage!

  override func setUpWithError() throws {
    try super.setUpWithError()
    tempBaseDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("mls-account-removal-tests-\(UUID().uuidString)", isDirectory: true)
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

  func testDestroyStorageCompletelyRemovesAllArtifacts() async throws {
    let testDID = "did:plc:remove_test_\(UUID().uuidString)"
    let grdbDir = try MLSStoragePaths.grdbDatabaseDirectory()
    let coordinator = MLSStorageCoordinator.shared

    // 1. Create simulated Swift SQLCipher files
    try FileManager.default.createDirectory(at: grdbDir, withIntermediateDirectories: true)
    let sanitizedDID = testDID
      .replacingOccurrences(of: ":", with: "-")
      .replacingOccurrences(of: "/", with: "-")
      .replacingOccurrences(of: "#", with: "-")
      .replacingOccurrences(of: "?", with: "-")
    let swiftDB = grdbDir.appendingPathComponent("mls_messages_\(sanitizedDID).db")
    let swiftWAL = URL(fileURLWithPath: swiftDB.path + "-wal")
    let swiftSHM = URL(fileURLWithPath: swiftDB.path + "-shm")
    try Data("swift-db".utf8).write(to: swiftDB)
    try Data("swift-wal".utf8).write(to: swiftWAL)
    try Data("swift-shm".utf8).write(to: swiftSHM)

    // Create marker
    let marker = MLSInitializationRecord(
      generationToken: MLSStoragePaths.generationToken,
      attemptUUID: UUID().uuidString,
      userDID: testDID.lowercased(),
      databaseKind: MLSDatabaseKind.swiftGRDB.rawValue,
      databasePathHash: try coordinator.databasePathHash(for: .swiftGRDB, userDID: testDID),
      state: .complete
    )
    try coordinator.writeMarkerDirectlyForTesting(marker)

    // 2. Create simulated Rust state files
    let rustDB = try coordinator.databaseURL(for: .rustState, userDID: testDID)
    try FileManager.default.createDirectory(at: rustDB.deletingLastPathComponent(), withIntermediateDirectories: true)
    let rustWAL = URL(fileURLWithPath: rustDB.path + "-wal")
    let rustSHM = URL(fileURLWithPath: rustDB.path + "-shm")
    try Data("rust-db".utf8).write(to: rustDB)
    try Data("rust-wal".utf8).write(to: rustWAL)
    try Data("rust-shm".utf8).write(to: rustSHM)
    // 3. Create Keychain items
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
    try MLSOrchestratorCredentialAdapter().storeSigningKey(userDid: testDID, keyData: Data(repeating: 0x55, count: 64))
    try MLSOrchestratorCredentialAdapter().storeMlsDid(userDid: testDID, mlsDid: "did:key:zTest")
    try MLSOrchestratorCredentialAdapter().storeDeviceUuid(userDid: testDID, uuid: UUID().uuidString)

    // 4. Set legacy migration flags (must NOT be cleared by clean reset)
    let legacyDidHash = testDID.data(using: .utf8)?.base64EncodedString()
      .replacingOccurrences(of: "/", with: "_")
      .replacingOccurrences(of: "+", with: "-")
      .replacingOccurrences(of: "=", with: "")
      .prefix(64).description ?? "default"
    let appGroupDefaults = UserDefaults(suiteName: "group.blue.catbird.shared")
    appGroupDefaults?.set(true, forKey: "MLSPlaintextHeaderMigrationV1_\(sanitizedDID)")
    appGroupDefaults?.set(true, forKey: "MLSRustFFIMigrationV1_\(legacyDidHash)")
    // Verify setup
    XCTAssertTrue(FileManager.default.fileExists(atPath: swiftDB.path))
    XCTAssertTrue(FileManager.default.fileExists(atPath: rustDB.path))

    // 5. Execute Complete Destruction
    await MLSClient.shared.destroyStorageCompletely(for: testDID)

    // 6. Assert files are deleted
    XCTAssertFalse(FileManager.default.fileExists(atPath: swiftDB.path), "Swift DB must be deleted")
    XCTAssertFalse(FileManager.default.fileExists(atPath: swiftWAL.path), "Swift WAL must be deleted")
    XCTAssertFalse(FileManager.default.fileExists(atPath: swiftSHM.path), "Swift SHM must be deleted")
    XCTAssertFalse(FileManager.default.fileExists(atPath: rustDB.path), "Rust DB must be deleted")
    XCTAssertFalse(FileManager.default.fileExists(atPath: rustWAL.path), "Rust WAL must be deleted")
    XCTAssertFalse(FileManager.default.fileExists(atPath: rustSHM.path), "Rust SHM must be deleted")

    // 7. Assert Keychain items are deleted
    let sqlKey = try? await MLSSQLCipherEncryption.shared.getKey(for: testDID)
    XCTAssertNil(sqlKey, "SQLCipher key must be deleted")
    let sqlSalt = try? await MLSSQLCipherEncryption.shared.getSalt(for: testDID)
    XCTAssertNil(sqlSalt, "SQLCipher salt must be deleted")
    
    let encryptionKey = try? MLSKeychainManager.shared.retrieveKeyStrict(forKey: MLSStoragePaths.rustMEKAccount(for: testDID))
    XCTAssertNil(encryptionKey, "Encryption key must be deleted")

    let identityKey = try? MLSKeychainManager.shared.retrieveKeyStrict(forKey: MLSStoragePaths.identityBackupAccount(for: testDID))
    XCTAssertNil(identityKey, "Identity key must be deleted")
    let mlsDidCred = try? MLSKeychainManager.shared.retrieveKeyStrict(forKey: MLSStoragePaths.mlsDidAccount(for: testDID))
    XCTAssertNil(mlsDidCred, "MLS DID credential must be deleted")

    let deviceUuidCred = try? MLSKeychainManager.shared.retrieveKeyStrict(forKey: MLSStoragePaths.deviceUuidAccount(for: testDID))
    XCTAssertNil(deviceUuidCred, "Device UUID credential must be deleted")

    let signingKey = try? MLSOrchestratorCredentialAdapter().getSigningKey(userDid: testDID)
    XCTAssertNil(signingKey, "Orchestrator signing key must be deleted")

    // 8. Assert Migration preferences are untouched
    let swiftMigration = appGroupDefaults?.object(forKey: "MLSPlaintextHeaderMigrationV1_\(sanitizedDID)")
    XCTAssertEqual(swiftMigration as? Bool, true, "Swift legacy migration flag must remain untouched")
    let rustMigration = appGroupDefaults?.object(forKey: "MLSRustFFIMigrationV1_\(legacyDidHash)")
    XCTAssertEqual(rustMigration as? Bool, true, "Rust legacy migration flag must remain untouched")
    let gateState = await MLSDatabaseGate.shared.gateState(for: testDID)
    XCTAssertEqual(gateState, .open, "Database gate must be left open for clean re-add")

    // Clean up test suite keys
    appGroupDefaults?.removeObject(forKey: "MLSPlaintextHeaderMigrationV1_\(sanitizedDID)")
    appGroupDefaults?.removeObject(forKey: "MLSRustFFIMigrationV1_\(legacyDidHash)")
}
}
