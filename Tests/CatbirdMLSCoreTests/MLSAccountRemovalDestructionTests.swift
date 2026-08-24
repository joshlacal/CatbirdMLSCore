import XCTest
@testable import CatbirdMLSCore

final class MLSAccountRemovalDestructionTests: XCTestCase {

  func testDestroyStorageCompletelyRemovesAllArtifacts() async throws {
    let testDID = "did:plc:remove_test_\(UUID().uuidString)"
    let baseContainer = MLSStoragePaths.baseContainerURL()
    
    // 1. Create simulated Swift SQLCipher files
    let mlsDir = baseContainer.appendingPathComponent("MLS", isDirectory: true)
    try FileManager.default.createDirectory(at: mlsDir, withIntermediateDirectories: true)
    let sanitizedDID = testDID
      .replacingOccurrences(of: ":", with: "-")
      .replacingOccurrences(of: "/", with: "-")
      .replacingOccurrences(of: "#", with: "-")
      .replacingOccurrences(of: "?", with: "-")
    let swiftDB = mlsDir.appendingPathComponent("mls_messages_\(sanitizedDID).db")
    let swiftWAL = URL(fileURLWithPath: swiftDB.path + "-wal")
    let swiftSHM = URL(fileURLWithPath: swiftDB.path + "-shm")
    try Data("swift-db".utf8).write(to: swiftDB)
    try Data("swift-wal".utf8).write(to: swiftWAL)
    try Data("swift-shm".utf8).write(to: swiftSHM)

    // 2. Create simulated Rust state files
    let rustDir = baseContainer.appendingPathComponent("mls-state", isDirectory: true)
    try FileManager.default.createDirectory(at: rustDir, withIntermediateDirectories: true)
    let didHash = testDID.data(using: .utf8)?.base64EncodedString()
      .replacingOccurrences(of: "/", with: "_")
      .replacingOccurrences(of: "+", with: "-")
      .replacingOccurrences(of: "=", with: "")
      .prefix(64).description ?? "default"
    let rustDB = rustDir.appendingPathComponent("\(didHash).db")
    let rustWAL = URL(fileURLWithPath: rustDB.path + "-wal")
    let rustSHM = URL(fileURLWithPath: rustDB.path + "-shm")
    try Data("rust-db".utf8).write(to: rustDB)
    try Data("rust-wal".utf8).write(to: rustWAL)
    try Data("rust-shm".utf8).write(to: rustSHM)

    // 3. Create Keychain items
    _ = try? await MLSSQLCipherEncryption.shared.getOrCreateKey(for: testDID)
    _ = try? await MLSSQLCipherEncryption.shared.getOrCreateSalt(for: testDID)
    _ = try? MLSKeychainManager.shared.getOrCreateEncryptionKey(forUserDID: testDID)
    _ = try? MLSContentRootKey.loadOrCreate(for: testDID)
    try? MLSKeychain.storeSignatureKey(Data(repeating: 0x55, count: 64), forIdentity: testDID)
    try? MLSKeychainManager.shared.store(Data("identity-key".utf8), forKey: "mls_identity_key_\(testDID)")
    try? MLSKeychainManager.shared.store(Data("mls-did".utf8), forKey: "mls.credential.mlsDid.\(testDID)")
    try? MLSKeychainManager.shared.store(Data("device-uuid".utf8), forKey: "mls.credential.deviceUuid.\(testDID)")

    // 4. Set migration flags
    let appGroupDefaults = UserDefaults(suiteName: "group.blue.catbird.shared")
    appGroupDefaults?.set(true, forKey: "MLSPlaintextHeaderMigrationV1_\(sanitizedDID)")
    appGroupDefaults?.set(true, forKey: "MLSRustFFIMigrationV1_\(didHash)")

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
    
    let encryptionKey = try? MLSKeychainManager.shared.retrieve(forKey: "mls.encryption.key.\(testDID)")
    XCTAssertNil(encryptionKey, "Encryption key must be deleted")

    let identityKey = try? MLSKeychainManager.shared.retrieve(forKey: "mls_identity_key_\(testDID)")
    XCTAssertNil(identityKey, "Identity key must be deleted")

    let mlsDidCred = try? MLSKeychainManager.shared.retrieve(forKey: "mls.credential.mlsDid.\(testDID)")
    XCTAssertNil(mlsDidCred, "MLS DID credential must be deleted")

    let deviceUuidCred = try? MLSKeychainManager.shared.retrieve(forKey: "mls.credential.deviceUuid.\(testDID)")
    XCTAssertNil(deviceUuidCred, "Device UUID credential must be deleted")

    // 8. Assert Migration preferences are cleared
    let swiftMigration = appGroupDefaults?.object(forKey: "MLSPlaintextHeaderMigrationV1_\(sanitizedDID)")
    XCTAssertNil(swiftMigration, "Swift migration flag must be cleared")
    let rustMigration = appGroupDefaults?.object(forKey: "MLSRustFFIMigrationV1_\(didHash)")
    XCTAssertNil(rustMigration, "Rust migration flag must be cleared")

    // 9. Assert gate is open for future re-registration
    let gateState = await MLSDatabaseGate.shared.gateState(for: testDID)
    XCTAssertEqual(gateState, .open, "Database gate must be left open for clean re-add")
  }
}
