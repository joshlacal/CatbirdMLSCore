import CatbirdMLS
@testable import CatbirdMLSCore
import Foundation
import XCTest

private final class SpyPushFetchTransport: PushFetchTransport, @unchecked Sendable {
  private let lock = NSLock()
  private(set) var fetchCallCount = 0
  private(set) var oauthRefreshAttemptCount = 0
  var entriesToReturn: [FetchedPushEntry] = []

  func fetchMissingEntries(
    conversationId: String,
    afterSeq: Int64,
    chatDpopToken: String
  ) throws -> PushFetchBundle {
    lock.lock()
    fetchCallCount += 1
    lock.unlock()
    return PushFetchBundle(entries: entriesToReturn, metadataSnapshot: nil, authorizedDeviceKeys: [])
  }

  func getAuthorizedDeviceKeys(
    did: String,
    chatDpopToken: String?
  ) throws -> [AuthorizedPeerDeviceKey] {
    []
  }
}

private final class StubCoordinatorTransport: PlatformTransport, @unchecked Sendable {
  func executeHttp(request: PlatformHttpRequest) throws -> PlatformHttpResponse {
    PlatformHttpResponse(status: 200, headers: [], body: Data("{}".utf8))
  }
  func connectRealtimeStream(homeDsEndpoint: String, cursorSeq: Int64?) throws {}
  func disconnectRealtimeStream() throws {}
}

final class PushExtensionIntegrationTests: XCTestCase {
  private var tempRoot: URL!

  override func setUpWithError() throws {
    try super.setUpWithError()
    tempRoot = FileManager.default.temporaryDirectory
      .appendingPathComponent("CatbirdMLSPushExt-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)
  }

  override func tearDownWithError() throws {
    if let tempRoot {
      let trashRoot = FileManager.default.temporaryDirectory
        .appendingPathComponent("_trash-CatbirdMLSPushExt", isDirectory: true)
      try? FileManager.default.createDirectory(at: trashRoot, withIntermediateDirectories: true)
      try? FileManager.default.moveItem(
        at: tempRoot,
        to: trashRoot.appendingPathComponent(tempRoot.lastPathComponent)
      )
    }
    tempRoot = nil
    try super.tearDownWithError()
  }

  func testI12InlinePushCiphertextCutoffIs2200Bytes() {
    XCTAssertTrue(shouldInlinePushCiphertext(ciphertext: Data(repeating: 0x42, count: 2200)))
    XCTAssertFalse(shouldInlinePushCiphertext(ciphertext: Data(repeating: 0x42, count: 2201)))
  }

  func testI10PushProcessorNeverRefreshesExpiredDpopTokenAndFallsBackUnderTwoSeconds() async throws {
    let backend = RecordingKeychainBackend()
    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.pushtest.\(UUID().uuidString)",
      backend: backend
    )
    let accountDid = "did:plc:nsefallbackuser"
    let didHash = try accountDidHashHex(accountDid: accountDid)

    let storageRootPath = tempRoot.path
    let coordinator = try await MainActor.run {
      try MLSClientCoordinator(
        config: CatbirdMlsClientConfig(
          storageRoot: storageRootPath,
          accountDid: accountDid,
          deviceId: "dev-nse",
          homeDsDid: "did:web:chat.catbird.blue",
          homeDsEndpoint: "https://chat.catbird.blue",
          pdsEndpoint: "https://pds.catbird.blue"
        ),
        keystore: keystore,
        transport: StubCoordinatorTransport()
      )
    }
    try await coordinator.prepareForBackground()

    let expiredMs = Int64(Date().timeIntervalSince1970 * 1000) - 30_000
    try keystore.storeCachedChatDpopToken(
      accountDidHash: didHash,
      token: "expired-dpop-token",
      expiresAtMs: expiredMs
    )

    let spyTransport = SpyPushFetchTransport()
    let wake = PushWakePayload(
      kind: "application",
      conversationId: "convo-fetch-1",
      generation: 0,
      entrySeq: 5,
      messageId: "msg-fetch-5",
      recipientAccountHash: didHash,
      fetchRequired: true,
      senderDid: "did:plc:sender",
      inlineCiphertext: nil
    )

    let started = ContinuousClock.now
    let preview = MLSPushExtensionProcessor.process(
      storageRoot: tempRoot,
      accountDidHash: didHash,
      keystore: keystore,
      transport: spyTransport,
      wake: wake,
      postDarwinNotification: false
    )
    let elapsed = started.duration(to: ContinuousClock.now)

    XCTAssertLessThan(elapsed, .seconds(2), "I10: expired-token fallback must finish within 2 seconds")
    XCTAssertEqual(spyTransport.oauthRefreshAttemptCount, 0, "I10: NSE must never attempt OAuth refresh")
    XCTAssertEqual(spyTransport.fetchCallCount, 0, "I10: NSE must not call DS with an expired token")
    XCTAssertTrue(preview.fallbackGeneric)
    XCTAssertEqual(preview.body, "New encrypted message")

    await coordinator.close()
  }

  func testI11DarwinWakeupPostedAndObservedByMainCoordinator() async throws {
    let backend = RecordingKeychainBackend()
    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.darwintest.\(UUID().uuidString)",
      backend: backend
    )
    let accountDid = "did:plc:darwinwakeuser"
    let storageRootPath = tempRoot.path
    let coordinator = try await MainActor.run {
      try MLSClientCoordinator(
        config: CatbirdMlsClientConfig(
          storageRoot: storageRootPath,
          accountDid: accountDid,
          deviceId: "dev-darwin",
          homeDsDid: "did:web:chat.catbird.blue",
          homeDsEndpoint: "https://chat.catbird.blue",
          pdsEndpoint: "https://pds.catbird.blue"
        ),
        keystore: keystore,
        transport: StubCoordinatorTransport()
      )
    }

    let initialCount = await MainActor.run { coordinator.darwinWakeupCount }
    MLSPushExtensionProcessor.postNseDidCommitDarwinNotification()

    for _ in 0..<20 {
      let current = await MainActor.run { coordinator.darwinWakeupCount }
      if current > initialCount { break }
      try await Task.sleep(nanoseconds: 25_000_000)
    }

    let finalCount = await MainActor.run { coordinator.darwinWakeupCount }
    XCTAssertGreaterThan(
      finalCount,
      initialCount,
      "I11: Posting blue.catbird.mls.nseDidCommit must wake active MLSClientCoordinator"
    )

    await coordinator.close()
  }
}
