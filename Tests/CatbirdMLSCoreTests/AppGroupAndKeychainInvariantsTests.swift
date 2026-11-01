import CatbirdMLS
@testable import CatbirdMLSCore
import Darwin
import Foundation
import Petrel
import Security
import XCTest

final class RecordingKeychainBackend: KeychainBackend, @unchecked Sendable {
  private let lock = NSLock()
  private var items: [String: [String: Any]] = [:]
  private(set) var recordedAddCalls: [[String: Any]] = []

  private func compositeKey(for dict: [String: Any]) -> String {
    let service = dict[kSecAttrService as String] as? String ?? ""
    let account = dict[kSecAttrAccount as String] as? String ?? ""
    return "\(service)|\(account)"
  }

  func add(_ attributes: [String: Any]) -> OSStatus {
    lock.lock()
    defer { lock.unlock() }
    recordedAddCalls.append(attributes)
    let key = compositeKey(for: attributes)
    if items[key] != nil {
      return errSecDuplicateItem
    }
    items[key] = attributes
    return errSecSuccess
  }

  func copyMatching(_ query: [String: Any]) -> (OSStatus, AnyObject?) {
    lock.lock()
    defer { lock.unlock() }
    let key = compositeKey(for: query)
    guard let stored = items[key],
          let data = stored[kSecValueData as String] as? Data
    else {
      return (errSecItemNotFound, nil)
    }
    return (errSecSuccess, data as NSData)
  }

  func update(_ query: [String: Any], _ attributesToUpdate: [String: Any]) -> OSStatus {
    lock.lock()
    defer { lock.unlock() }
    let key = compositeKey(for: query)
    guard var stored = items[key] else {
      return errSecItemNotFound
    }
    for (k, v) in attributesToUpdate {
      stored[k] = v
    }
    items[key] = stored
    return errSecSuccess
  }

  func delete(_ query: [String: Any]) -> OSStatus {
    lock.lock()
    defer { lock.unlock() }
    if query[kSecAttrAccount as String] is String {
      let key = compositeKey(for: query)
      return items.removeValue(forKey: key) != nil ? errSecSuccess : errSecItemNotFound
    }
    let service = query[kSecAttrService as String] as? String ?? ""
    let matching = items.keys.filter { $0.hasPrefix("\(service)|") }
    guard !matching.isEmpty else { return errSecItemNotFound }
    for k in matching {
      items.removeValue(forKey: k)
    }
    return errSecSuccess
  }

  var storedAccounts: [String] {
    lock.lock()
    defer { lock.unlock() }
    return items.values.compactMap { $0[kSecAttrAccount as String] as? String }
  }
}

private final class StubPlatformTransport: PlatformTransport, @unchecked Sendable {
  func executeHttp(request: PlatformHttpRequest) throws -> PlatformHttpResponse {
    PlatformHttpResponse(status: 200, headers: [], body: Data("{}".utf8))
  }
  func connectRealtimeStream(homeDsEndpoint: String, cursorSeq: Int64?) throws {}
  func disconnectRealtimeStream() throws {}
}

final class AppGroupAndKeychainInvariantsTests: XCTestCase {
  private var tempRoot: URL!

  override func setUpWithError() throws {
    try super.setUpWithError()
    tempRoot = FileManager.default.temporaryDirectory
      .appendingPathComponent("CatbirdMLSCleanSlate-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)
  }

  override func tearDownWithError() throws {
    if let tempRoot {
      let trashRoot = FileManager.default.temporaryDirectory
        .appendingPathComponent("_trash-CatbirdMLSCleanSlate", isDirectory: true)
      try? FileManager.default.createDirectory(at: trashRoot, withIntermediateDirectories: true)
      try? FileManager.default.moveItem(
        at: tempRoot,
        to: trashRoot.appendingPathComponent(tempRoot.lastPathComponent)
      )
    }
    tempRoot = nil
    try super.tearDownWithError()
  }

  func testI8LegacyAppGroupWipeAndFileProtectionAttributes() async throws {
    let mlsSubdir = tempRoot.appendingPathComponent("catbird-mls", isDirectory: true)
    try FileManager.default.createDirectory(at: mlsSubdir, withIntermediateDirectories: true)
    try Data("legacy-accounts".utf8).write(to: tempRoot.appendingPathComponent("accounts.json"))
    try Data("legacy-accounts-2".utf8).write(to: mlsSubdir.appendingPathComponent("accounts.json"))
    try Data("legacy-lease".utf8).write(to: mlsSubdir.appendingPathComponent("user.lease"))
    try Data("legacy-sqlite".utf8).write(to: tempRoot.appendingPathComponent("mls_messages.sqlite"))
    try Data("legacy-sqlite-2".utf8).write(to: mlsSubdir.appendingPathComponent("old.sqlite"))

    let backend = RecordingKeychainBackend()
    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.test.\(UUID().uuidString)",
      backend: backend
    )
    let accountDid = "  DID:PLC:AliceCleanSlate123  "
    let expectedHash = try MLSAppGroupStorage.accountDidHash(for: accountDid)

    let storageRootPath = tempRoot.path
    let config = CatbirdMlsClientConfig(
      storageRoot: storageRootPath,
      accountDid: accountDid,
      deviceId: "dev-1",
      homeDsDid: "did:web:chat.catbird.blue",
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://pds.catbird.blue"
    )

    let coordinator = try await MainActor.run {
      try MLSClientCoordinator(
        config: config,
        keystore: keystore,
        transport: StubPlatformTransport()
      )
    }

    XCTAssertFalse(FileManager.default.fileExists(atPath: tempRoot.appendingPathComponent("accounts.json").path))
    XCTAssertFalse(FileManager.default.fileExists(atPath: mlsSubdir.appendingPathComponent("accounts.json").path))
    XCTAssertFalse(FileManager.default.fileExists(atPath: mlsSubdir.appendingPathComponent("user.lease").path))
    XCTAssertFalse(FileManager.default.fileExists(atPath: tempRoot.appendingPathComponent("mls_messages.sqlite").path))
    XCTAssertFalse(FileManager.default.fileExists(atPath: mlsSubdir.appendingPathComponent("old.sqlite").path))

    let entries = try FileManager.default.contentsOfDirectory(atPath: mlsSubdir.path)
    XCTAssertFalse(entries.isEmpty)
    for entry in entries {
      let allowed = entry == "\(expectedHash).db"
        || entry == "\(expectedHash).db-wal"
        || entry == "\(expectedHash).db-shm"
      XCTAssertTrue(allowed, "Unexpected file in <AppGroup>/catbird-mls/: \(entry)")
    }

    let dbURL = try MLSAppGroupStorage.databaseURL(storageRoot: tempRoot, accountDid: accountDid)
    XCTAssertEqual(dbURL.lastPathComponent, "\(expectedHash).db")
    let resourceValues = try dbURL.resourceValues(forKeys: [.isExcludedFromBackupKey])
    XCTAssertEqual(resourceValues.isExcludedFromBackup, true)

    await coordinator.close()
  }

  func testI8KeychainPlatformKeyStoreUsesAfterFirstUnlockThisDeviceOnlyAndDidHash() throws {
    let backend = RecordingKeychainBackend()
    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.test.\(UUID().uuidString)",
      backend: backend
    )
    let rawDid = "DID:PLC:KeychainUser99"
    let didHash = try MLSAppGroupStorage.accountDidHash(for: rawDid)

    let dbKey1 = try keystore.loadOrCreateDbKey(accountDidHash: didHash)
    XCTAssertEqual(dbKey1.count, 32)
    let dbKey2 = try keystore.loadDbKey(accountDidHash: didHash)
    XCTAssertEqual(dbKey1, dbKey2)

    let sigBytes = Data(repeating: 0xAB, count: 64)
    try keystore.storeSignatureKeypair(keyId: "\(didHash):dev-1", keypairBytes: sigBytes)
    XCTAssertEqual(try keystore.loadSignatureKeypair(keyId: "\(didHash):dev-1"), sigBytes)

    let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
    try keystore.storeCachedChatDpopToken(
      accountDidHash: didHash,
      token: "valid-dpop-jwt",
      expiresAtMs: nowMs + 60_000
    )
    XCTAssertEqual(try keystore.loadCachedChatDpopToken(accountDidHash: didHash), "valid-dpop-jwt")

    try keystore.storeCachedChatDpopToken(
      accountDidHash: didHash,
      token: "expired-dpop-jwt",
      expiresAtMs: nowMs - 1_000
    )
    XCTAssertNil(try keystore.loadCachedChatDpopToken(accountDidHash: didHash))

    XCTAssertGreaterThanOrEqual(backend.recordedAddCalls.count, 3)
    for addCall in backend.recordedAddCalls {
      let accessible = addCall[kSecAttrAccessible as String] as? String
      XCTAssertEqual(
        accessible,
        kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly as String,
        "Keychain item must use AfterFirstUnlockThisDeviceOnly from creation (A-8)"
      )
      let accountAttr = addCall[kSecAttrAccount as String] as? String ?? ""
      XCTAssertFalse(accountAttr.lowercased().contains("did:plc:"), "Keychain account must never contain raw DID")
      XCTAssertTrue(accountAttr.contains(didHash), "Keychain account must be keyed by sha256(normalized_did)")
    }

    try keystore.deleteAccountKeys(accountDidHash: didHash)
    XCTAssertNil(try keystore.loadSignatureKeypair(keyId: "\(didHash):dev-1"))
    XCTAssertNil(try keystore.loadCachedChatDpopToken(accountDidHash: didHash))
    XCTAssertThrowsError(try keystore.loadDbKey(accountDidHash: didHash))
  }

  func testColdRestartDeleteAccountKeysWipesEnrolledMlsSignerKeypair() throws {
    let backend = RecordingKeychainBackend()
    let serviceName = "blue.catbird.mls.test.coldrestart.\(UUID().uuidString)"
    let didHash = try MLSAppGroupStorage.accountDidHash(for: "did:plc:coldrestartwipeuser")

    // Process launch #1: Rust CatbirdMlsClient.enroll_device stores mls-signer:<didHash>
    let keystoreLaunch1 = KeychainPlatformKeyStore(service: serviceName, backend: backend)
    let signerBytes = Data("{\"public\":[1,2,3],\"private\":[4,5,6]}".utf8)
    try keystoreLaunch1.storeSignatureKeypair(keyId: "mls-signer:\(didHash)", keypairBytes: signerBytes)
    _ = try keystoreLaunch1.loadOrCreateDbKey(accountDidHash: didHash)
    XCTAssertEqual(try keystoreLaunch1.loadSignatureKeypair(keyId: "mls-signer:\(didHash)"), signerBytes)

    // Process launch #2 (cold restart -> empty in-memory knownKeyIdsByAccountHash): sign-out wipe!
    let keystoreLaunch2 = KeychainPlatformKeyStore(service: serviceName, backend: backend)
    try keystoreLaunch2.deleteAccountKeys(accountDidHash: didHash)
    XCTAssertNil(
      try keystoreLaunch2.loadSignatureKeypair(keyId: "mls-signer:\(didHash)"),
      "Cold-restart deleteAccountKeys must deterministically wipe mls-signer:<didHash> from Keychain"
    )
    XCTAssertThrowsError(try keystoreLaunch2.loadDbKey(accountDidHash: didHash))
  }

  func testI9PrepareForBackgroundClosesSqliteHandleAndResumesCleanly() async throws {
    let backend = RecordingKeychainBackend()
    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.test.\(UUID().uuidString)",
      backend: backend
    )
    let accountDid = "did:plc:backgroundtest"
    let storageRootPath = tempRoot.path
    let config = CatbirdMlsClientConfig(
      storageRoot: storageRootPath,
      accountDid: accountDid,
      deviceId: "dev-bg",
      homeDsDid: "did:web:chat.catbird.blue",
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://pds.catbird.blue"
    )

    let coordinator = try await MainActor.run {
      try MLSClientCoordinator(
        config: config,
        keystore: keystore,
        transport: StubPlatformTransport()
      )
    }

    let dbPath = try MLSAppGroupStorage.databaseURL(storageRoot: tempRoot, accountDid: accountDid)
      .standardizedFileURL.path
    _ = try await coordinator.createGroup(
      conversationId: "convo-bg-1",
      sequencerDid: "did:web:chat.catbird.blue",
      title: "Background Safe Group",
      description: nil
    )

    try await coordinator.prepareForBackground()
    let closedFlag = await MainActor.run { coordinator.isHandleClosed }
    XCTAssertTrue(closedFlag, "I9: prepareForBackground must close the SQLite handle")
    XCTAssertEqual(
      openFileDescriptorPaths(matching: dbPath),
      [],
      "I9: zero open SQLite file descriptors may remain after prepareForBackground()"
    )

    try await coordinator.resumeFromBackground()
    let reopenedFlag = await MainActor.run { coordinator.isHandleClosed }
    XCTAssertFalse(reopenedFlag)
    let convos = try await coordinator.listConversations(limit: 10)
    XCTAssertEqual(convos.map(\.conversationId), ["convo-bg-1"])

    await coordinator.close()
  }

  func testI15SpacesEligibilityRequiresPdsSupportAndFullLabyrinthScopes() {
    let allScopes = MLSAppGroupStorage.requiredSpacesScopes

    let unsupported = MLSClientCoordinator.evaluateEligibility(
      pdsSupportsSpaces: false,
      grantedScopes: allScopes
    )
    XCTAssertFalse(unsupported.eligible)
    XCTAssertTrue(unsupported.blueskyDmsAvailable, "I15: Standard Bluesky DMs must remain available")
    XCTAssertEqual(unsupported.reason, .pdsDoesNotSupportSpaces)

    let missingSpaceScope = MLSClientCoordinator.evaluateEligibility(
      pdsSupportsSpaces: true,
      grantedScopes: Array(allScopes.dropLast())
    )
    XCTAssertFalse(missingSpaceScope.eligible)
    XCTAssertTrue(missingSpaceScope.blueskyDmsAvailable)
    XCTAssertEqual(
      missingSpaceScope.reason,
      .missingOAuthScopes(missingScopes: ["space:blue.catbird.labyrinth.conversation?authority=*"])
    )

    let ready = MLSClientCoordinator.evaluateEligibility(
      pdsSupportsSpaces: true,
      grantedScopes: allScopes
    )
    XCTAssertTrue(ready.eligible)
    XCTAssertEqual(ready.reason, .available)
  }

  func testA3AndA5OutboundHTTPValidatorRejectsSSRFAndDisallowedNSIDs() throws {
    let blockedEndpoints = [
      "https://127.0.0.1",
      "https://192.168.1.1",
      "https://10.0.0.1",
      "https://169.254.169.254",
      "https://[::1]",
      "https://localhost",
      "https://metadata.internal",
      "https://pds.example.com:8443",
      "https://pds.example.com#fragment",
      "http://pds.example.com",
      "https://user:pass@pds.example.com",
    ]
    for raw in blockedEndpoints {
      if let url = URL(string: raw) {
        XCTAssertThrowsError(try MLSOutboundHTTPValidator.validatePublicHTTPSServiceEndpoint(url))
      }
    }
    XCTAssertNoThrow(
      try MLSOutboundHTTPValidator.validatePublicHTTPSServiceEndpoint(URL(string: "https://chat.catbird.blue")!)
    )
    XCTAssertNoThrow(
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID("com.atproto.space.getBlob")
    )
    XCTAssertNoThrow(
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID("blue.catbird.push.registerKey")
    )
    for allowedRepoNSID in [
      "com.atproto.repo.uploadBlob",
      "com.atproto.repo.putRecord",
      "com.atproto.repo.getRecord",
      "com.atproto.repo.listRecords",
    ] {
      XCTAssertNoThrow(
        try MLSOutboundHTTPValidator.validatePreparedRequestNSID(allowedRepoNSID)
      )
    }
    XCTAssertThrowsError(
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID("com.atproto.server.createAppPassword")
    )
    XCTAssertThrowsError(
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID("com.atproto.repo.deleteRecord")
    )
    XCTAssertThrowsError(
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID("com.atproto.repo.applyWrites")
    )
  }

  private func openFileDescriptorPaths(matching targetPath: String) -> [String] {
    var matches: [String] = []
    for fd in 0..<1024 {
      var buffer = [UInt8](repeating: 0, count: Int(MAXPATHLEN))
      let rc = buffer.withUnsafeMutableBytes { rawBuf in
        fcntl(Int32(fd), F_GETPATH, rawBuf.baseAddress!)
      }
      if rc != -1 {
        let prefix = buffer.prefix { $0 != 0 }
        let path = String(decoding: prefix, as: UTF8.self)
        if path == targetPath || path.hasPrefix(targetPath + "-") {
          matches.append(path)
        }
      }
    }
    return matches
  }
}
