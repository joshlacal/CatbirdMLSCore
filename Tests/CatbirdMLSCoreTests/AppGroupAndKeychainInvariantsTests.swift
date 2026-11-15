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
      deviceId: "5a0f1c2e-3b4d-4e6f-8a7b-9c0d1e2f3a41",
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

  /// Live path (no offline create fallback): bare `did:plc`, UUIDv4 ids, an enrolled device, and
  /// DS-signed responses from `NativeArchiveMockNetwork`.
  func testI9PrepareForBackgroundClosesSqliteHandleAndResumesCleanly() async throws {
    NativeArchiveMockNetwork.shared.reset()
    let boundedSession = MLSBoundedURLSession(protocolClasses: [NativeArchiveMockURLProtocol.self])
    defer { boundedSession.invalidateAndCancel() }
    let backend = RecordingKeychainBackend()
    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.test.\(UUID().uuidString)",
      backend: backend
    )
    let accountDid = "did:plc:backgroundtestaaaaaaaaaa"
    let conversationId = "5a0f1c2e-3b4d-4e6f-8a7b-9c0d1e2f3b01"
    let storageRootPath = tempRoot.path
    let config = CatbirdMlsClientConfig(
      storageRoot: storageRootPath,
      accountDid: accountDid,
      deviceId: "5a0f1c2e-3b4d-4e6f-8a7b-9c0d1e2f3a42",
      homeDsDid: NativeArchiveMockNetwork.dsDid,
      homeDsEndpoint: NativeArchiveMockNetwork.dsEndpoint,
      pdsEndpoint: NativeArchiveMockNetwork.pdsEndpoint(for: accountDid)
    )
    let transport = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: NativeArchiveMockNetwork.dsEndpoint),
      ownPdsEndpoint: URL(string: NativeArchiveMockNetwork.pdsEndpoint(for: accountDid)),
      boundedSession: boundedSession
    )

    let coordinator = try await MainActor.run {
      try MLSClientCoordinator(
        config: config,
        keystore: keystore,
        transport: transport
      )
    }

    let dbPath = try MLSAppGroupStorage.databaseURL(storageRoot: tempRoot, accountDid: accountDid)
      .standardizedFileURL.path
    _ = try await coordinator.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    _ = try await coordinator.createGroup(
      conversationId: conversationId,
      sequencerDid: NativeArchiveMockNetwork.dsDid,
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
    XCTAssertEqual(convos.map(\.conversationId), [conversationId])

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
      .missingOAuthScopes(missingScopes: [
        "space:blue.catbird.labyrinth.conversation?action=create&action=delete&action=read&action=update&authority=*&collection=blue.catbird.labyrinth.attachmentManifest&collection=blue.catbird.labyrinth.authorRedaction&collection=blue.catbird.labyrinth.batch&collection=blue.catbird.labyrinth.mek",
      ])
    )

    // A session granted only the retired Labyrinth Space scopes must re-authorize.
    let oldScopeSession = MLSClientCoordinator.evaluateEligibility(
      pdsSupportsSpaces: true,
      grantedScopes: Array(allScopes.prefix(4)) + [
        "space:blue.catbird.labyrinth.mailbox?manage=create&manage=delete",
        "space:blue.catbird.labyrinth.conversation?authority=*",
      ]
    )
    XCTAssertFalse(oldScopeSession.eligible)
    XCTAssertEqual(oldScopeSession.reason, .missingOAuthScopes(missingScopes: Array(allScopes.suffix(3))))

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
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID("com.atproto.identity.resolveDid")
    )
    XCTAssertNoThrow(
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID(
        "com.atproto.identity.resolveDid",
        method: "GET",
        requestURL: URL(string: "https://pds.catbird.blue/xrpc/com.atproto.identity.resolveDid?did=did%3Aplc%3Aabc")!,
        ownPDSOrigin: URL(string: "https://pds.catbird.blue")!
      )
    )
    XCTAssertThrowsError(
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID("com.atproto.server.createAppPassword")
    )
    XCTAssertThrowsError(
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID("com.atproto.repo.deleteRecord")
    )
    XCTAssertThrowsError(
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID("com.atproto.repo.applyWrites")
    )

    // deleteRecord passes only as the native Labyrinth mailboxKeys purge shape.
    let repo = "did:plc:alicecleanuseraaaaaaaaaa"
    func deletion(_ fields: [String: Any], method: String = "POST") throws -> String {
      try MLSOutboundHTTPValidator.mailboxKeysDeletionRepo(
        method: method,
        body: try JSONSerialization.data(withJSONObject: fields)
      )
    }
    let purge: [String: Any] = [
      "repo": repo, "collection": "blue.catbird.labyrinth.mailboxKeys", "rkey": "self", "swapRecord": "bafyreiswap",
    ]
    XCTAssertEqual(try deletion(purge), repo)
    XCTAssertEqual(try deletion(purge.merging(["rkey": "000000000003"]) { $1 }), repo)
    for rejected: [String: Any] in [
      purge.merging(["collection": "app.bsky.feed.post"]) { $1 },
      purge.merging(["rkey": "3l2hx5kz7ab2c"]) { $1 },
      purge.merging(["rkey": "00000000003"]) { $1 },
      purge.merging(["swapRecord": ""]) { $1 },
      purge.merging(["validate": false]) { $1 },
      purge.filter { $0.key != "swapRecord" },
      purge.merging(["repo": "self"]) { $1 },
    ] {
      XCTAssertThrowsError(try deletion(rejected), "\(rejected)")
    }
    XCTAssertThrowsError(try deletion(purge, method: "GET"))
    XCTAssertThrowsError(try MLSOutboundHTTPValidator.mailboxKeysDeletionRepo(method: "POST", body: nil))
  }

  /// Even the exact purge shape needs the signed-in PDS session to bind `repo`.
  func testMailboxKeysDeletionWithoutSignedInSessionIsRejected() async throws {
    let transport = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      boundedSession: MLSBoundedURLSession(protocolClasses: [])
    )
    let body = try JSONSerialization.data(withJSONObject: [
      "repo": "did:plc:alicecleanuseraaaaaaaaaa", "collection": "blue.catbird.labyrinth.mailboxKeys",
      "rkey": "self", "swapRecord": "bafyreiswap",
    ])
    let request = PlatformHttpRequest(
      method: "POST",
      url: "https://pds.catbird.blue/xrpc/com.atproto.repo.deleteRecord",
      headers: [PlatformHttpHeader(name: "content-type", value: "application/json")],
      body: body
    )
    let rawOutcome = await Task.detached { Result { try transport.executeHttpRaw(request: request) } }.value
    XCTAssertThrowsError(try rawOutcome.get()) { error in
      XCTAssertEqual(
        error as? MLSOutboundHTTPValidator.ValidationError,
        .disallowedNSID("com.atproto.repo.deleteRecord")
      )
    }
    let ffiOutcome = await Task.detached { Result { try transport.executeHttp(request: request) } }.value
    XCTAssertThrowsError(try ffiOutcome.get()) { error in
      guard case .InvalidInput = (error as? CatbirdMLS.MlsError) else {
        XCTFail("Expected FFI-safe MlsError.InvalidInput, got \(error)")
        return
      }
    }
  }

  /// `P0-7`: `MLSClientCoordinator.open` runs SQLCipher/FFI initialization off `@MainActor`,
  /// and foreign Swift callback errors map to `CatbirdMLS.MlsError` instead of panicking in Rust
  /// via UniFFI `0.28.3` `flat_error` `CALL_UNEXPECTED_ERROR`.
  func testP07AsyncOpenRunsOffMainActorAndCallbackThrowNeverTrapsAcrossUniFFI() async throws {
    struct ArbitraryForeignCallbackError: Error, LocalizedError {
      var errorDescription: String? { "simulated foreign callback throw" }
    }
    final class ThreadRecordingKeystoreProxy: PlatformKeyStore, @unchecked Sendable {
      let inner: KeychainPlatformKeyStore
      private let lock = NSLock()
      private(set) var sawMainThreadDuringOpen = false
      init(inner: KeychainPlatformKeyStore) { self.inner = inner }
      func loadOrCreateDbKey(accountDidHash: String) throws -> Data {
        lock.lock()
        if Thread.isMainThread { sawMainThreadDuringOpen = true }
        lock.unlock()
        return try inner.loadOrCreateDbKey(accountDidHash: accountDidHash)
      }
      func loadDbKey(accountDidHash: String) throws -> Data {
        try inner.loadDbKey(accountDidHash: accountDidHash)
      }
      func loadSignatureKeypair(keyId: String) throws -> Data? {
        try inner.loadSignatureKeypair(keyId: keyId)
      }
      func storeSignatureKeypair(keyId: String, keypairBytes: Data) throws {
        try inner.storeSignatureKeypair(keyId: keyId, keypairBytes: keypairBytes)
      }
      func deleteAccountKeys(accountDidHash: String) throws {
        try inner.deleteAccountKeys(accountDidHash: accountDidHash)
      }
      func loadCachedChatDpopToken(accountDidHash: String) throws -> String? {
        try inner.loadCachedChatDpopToken(accountDidHash: accountDidHash)
      }
      func storeCachedChatDpopToken(accountDidHash: String, token: String, expiresAtMs: Int64) throws {
        try inner.storeCachedChatDpopToken(accountDidHash: accountDidHash, token: token, expiresAtMs: expiresAtMs)
      }
    }

    let innerKeystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.test.p07.\(UUID().uuidString)",
      backend: RecordingKeychainBackend()
    )
    let proxyKeystore = ThreadRecordingKeystoreProxy(inner: innerKeystore)
    let accountDid = "did:plc:p07asyncopenuseraaaaaaaa"
    let transport = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      ownPdsEndpoint: URL(string: "https://pds.catbird.blue"),
      boundedSession: MLSBoundedURLSession(protocolClasses: []),
      httpOverride: { _ in
        throw ArbitraryForeignCallbackError()
      }
    )
    let config = CatbirdMlsClientConfig(
      storageRoot: tempRoot.path,
      accountDid: accountDid,
      deviceId: "5a0f1c2e-3b4d-4e6f-8a7b-9c0d1e2f3a99",
      homeDsDid: "did:web:chat.catbird.blue",
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://pds.catbird.blue"
    )

    let coordinator = try await MainActor.run {
      Task {
        try await MLSClientCoordinator.open(
          config: config,
          keystore: proxyKeystore,
          transport: transport
        )
      }
    }.value

    XCTAssertFalse(
      proxyKeystore.sawMainThreadDuringOpen,
      "P0-7: MLSClientCoordinator.open must open SQLCipher/Rust FFI off the main thread"
    )

    do {
      _ = try await coordinator.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
      XCTFail("Expected enrollDevice to fail when transport throws ArbitraryForeignCallbackError")
    } catch let mlsError as CatbirdMLS.MlsError {
      XCTAssertFalse(String(describing: mlsError).isEmpty)
    } catch {
      XCTFail("Expected CatbirdMLS.MlsError across UniFFI, got \(error)")
    }

    await coordinator.close()
  }

  /// `P0-8`: When `atProtoClient` is configured and `getServiceAuth` fails on a `homeDs`
  /// `blue.catbird.chat.*` XRPC call, `PetrelPlatformTransport` MUST fail closed and never
  /// fall through to an unauthenticated HTTP request against `homeDs`.
  func testP08GetServiceAuthFailureFailsClosedWithoutUnauthenticatedHomeDsFallback() async throws {
    final class HomeDsSpyProtocol: URLProtocol {
      private static let lock = NSLock()
      nonisolated(unsafe) private static var observedURLs: [String] = []
      static func reset() {
        lock.lock()
        observedURLs = []
        lock.unlock()
      }
      static func snapshot() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return observedURLs
      }
      override class func canInit(with request: URLRequest) -> Bool { true }
      override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
      override func startLoading() {
        if let u = request.url?.absoluteString {
          Self.lock.lock()
          Self.observedURLs.append(u)
          Self.lock.unlock()
        }
        let resp = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: "HTTP/1.1", headerFields: [:])!
        client?.urlProtocol(self, didReceive: resp, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: Data("{}".utf8))
        client?.urlProtocolDidFinishLoading(self)
      }
      override func stopLoading() {}
    }

    HomeDsSpyProtocol.reset()
    let spySession = MLSBoundedURLSession(protocolClasses: [HomeDsSpyProtocol.self])
    defer { spySession.invalidateAndCancel() }

    // Unauthenticated ATProtoClient bound to local loopback/invalid port so getServiceAuth fails immediately.
    let unauthedClient = await ATProtoClient(baseURL: URL(string: "http://127.0.0.1:1")!)
    let transport = PetrelPlatformTransport(
      atProtoClient: unauthedClient,
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      homeDsDid: "did:web:chat.catbird.blue",
      ownPdsEndpoint: URL(string: "https://pds.catbird.blue"),
      boundedSession: spySession
    )
    let request = PlatformHttpRequest(
      method: "GET",
      url: "https://chat.catbird.blue/xrpc/blue.catbird.chat.getConversationState?conversationId=5a0f1c2e-3b4d-4e6f-8a7b-9c0d1e2f3b01",
      headers: [],
      body: nil
    )

    let outcome = await Task.detached { Result { try transport.executeHttp(request: request) } }.value
    XCTAssertThrowsError(try outcome.get()) { error in
      XCTAssertNotNil(error as? CatbirdMLS.MlsError, "Expected MlsError on getServiceAuth failure, got \(error)")
    }
    XCTAssertEqual(
      HomeDsSpyProtocol.snapshot(),
      [],
      "P0-8: getServiceAuth failure must fail closed with zero unauthenticated requests sent to homeDs"
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
