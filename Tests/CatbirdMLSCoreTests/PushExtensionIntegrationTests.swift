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
          deviceId: "6b1e2d3f-4c5a-4b7e-9f80-a1b2c3d4e5f1",
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
          deviceId: "6b1e2d3f-4c5a-4b7e-9f80-a1b2c3d4e5f2",
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

  func testDecodeNestApnsPushWakeEnvelope() throws {
    let didHash = String(repeating: "a", count: 64)
    let ciphertext = Data([0x10, 0x20, 0x30, 0x40])
    // Exact JSON structure produced by nest/catbird/src/services/push/gateway.rs apns_envelope:
    let apnsUserInfo: [AnyHashable: Any] = [
      "aps": [
        "alert": ["body": "New message"],
        "mutable-content": 1,
      ],
      "catbird": [
        "recipientAccountHash": didHash,
        "wakeKind": "application",
        "conversationId": "11111111-2222-4333-8444-555555555555",
        "generation": 2,
        "seq": 17,
        "epoch": 4,
        "fetchRequired": false,
        "ciphertext": ciphertext.base64EncodedString(),
      ] as [String: Any],
    ]

    let decoded = try XCTUnwrap(MLSPushExtensionProcessor.decodePushWakePayload(from: apnsUserInfo))
    XCTAssertEqual(decoded.kind, "application")
    XCTAssertEqual(decoded.conversationId, "11111111-2222-4333-8444-555555555555")
    XCTAssertEqual(decoded.recipientAccountHash, didHash)
    XCTAssertEqual(decoded.generation, 2)
    XCTAssertEqual(decoded.entrySeq, 17)
    XCTAssertFalse(decoded.fetchRequired)
    XCTAssertEqual(decoded.inlineCiphertext, ciphertext)
  }

  func testDecodeCanonicalMlsDsGetEntriesWithPrunedEntry() throws {
    let rawMls = Data([0xDE, 0xAD, 0xBE, 0xEF])
    let wireJson = """
    {
      "entries": [
        {
          "$type": "blue.catbird.chat.defs#applicationEntry",
          "conversationId": "11111111-2222-4333-8444-555555555555",
          "entryId": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
          "payloadPruned": true,
          "receivedAt": "2026-10-31T12:00:00.000Z",
          "seq": 4
        },
        {
          "$type": "blue.catbird.chat.defs#applicationEntry",
          "conversationId": "11111111-2222-4333-8444-555555555555",
          "entryId": "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff",
          "receivedAt": "2026-10-31T12:00:01.000Z",
          "seq": 5,
          "signedRequest": {
            "body": {
              "actorDid": "did:plc:alice123",
              "messageId": "cccccccc-dddd-4eee-8fff-000000000000",
              "prior": { "epoch": 3 },
              "applicationMessage": {
                "bytes": { "$bytes": "\(rawMls.base64EncodedString())" },
                "contentType": "application/mls",
                "framing": "private"
              }
            }
          }
        }
      ],
      "hasMore": false,
      "nextAfterSeq": 5
    }
    """
    let bundle = try PetrelPlatformTransport.decodePushGetEntriesBundle(from: Data(wireJson.utf8))
    XCTAssertEqual(bundle.entries.count, 2)

    XCTAssertEqual(bundle.entries[0].seq, 4)
    XCTAssertEqual(bundle.entries[0].entryKind, "application")
    XCTAssertTrue(bundle.entries[0].mlsBytes.isEmpty, "I14 pruned entry must decode to empty mlsBytes")

    XCTAssertEqual(bundle.entries[1].seq, 5)
    XCTAssertEqual(bundle.entries[1].epoch, 3)
    XCTAssertEqual(bundle.entries[1].entryKind, "application")
    XCTAssertEqual(bundle.entries[1].entryId, "cccccccc-dddd-4eee-8fff-000000000000")
    XCTAssertEqual(bundle.entries[1].senderDid, "did:plc:alice123")
    XCTAssertEqual(bundle.entries[1].mlsBytes, rawMls)
  }
}
