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

  func testReceiptlessPreviewDoesNotPostDidCommitNotification() {
    let preview = NotificationPreview(
      messageId: "inline-preview",
      conversationId: "conversation",
      senderDid: "did:plc:sender",
      senderDisplayName: nil,
      groupTitle: "Nest",
      body: "Authenticated inline preview",
      epoch: 3,
      seq: 9,
      fromCache: false,
      fallbackGeneric: false,
      darwinNotificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification,
      receiptVerified: false,
      isOwnSiblingSender: false
    )
    XCTAssertFalse(MLSPushExtensionProcessor.shouldPostDidCommit(for: preview, requested: true))

    let verified = NotificationPreview(
      messageId: preview.messageId,
      conversationId: preview.conversationId,
      senderDid: preview.senderDid,
      senderDisplayName: preview.senderDisplayName,
      groupTitle: preview.groupTitle,
      body: preview.body,
      epoch: preview.epoch,
      seq: preview.seq,
      fromCache: false,
      fallbackGeneric: false,
      darwinNotificationName: preview.darwinNotificationName,
      receiptVerified: true,
      isOwnSiblingSender: false
    )
    XCTAssertTrue(MLSPushExtensionProcessor.shouldPostDidCommit(for: verified, requested: true))
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

  /// The same wake as `testDecodeNestApnsPushWakeEnvelope`, sealed as version 2
  /// (`docs/security/2026-11-09-sealed-push-wake-envelope.md`): the provider-visible object
  /// holds only `v`, `kid` and `sealed`, and decoding yields the identical `PushWakePayload`.
  func testDecodeNestApnsSealedPushWakeEnvelopeMatchesVersion1() throws {
    let accountDid = "did:plc:pushwakev2user0000000000"
    let didHash = try accountDidHashHex(accountDid: accountDid)
    let ciphertext = Data([0x10, 0x20, 0x30, 0x40])
    let pushKey = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
    let store = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.test.pushwake",
      accessGroup: nil,
      backend: RecordingKeychainBackend()
    )
    try store.storePushWakeKey(pushKey: pushKey, appId: "blue.catbird", accountDid: accountDid)

    let version1: [AnyHashable: Any] = [
      "aps": ["alert": ["body": "New message"], "mutable-content": 1],
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
    let header = Data(
      #"{"wakeKind":"application","conversationId":"11111111-2222-4333-8444-555555555555","generation":2,"seq":17,"epoch":4,"fetchRequired":false}"#
        .utf8
    )
    let sealed = try PushWakeEnvelope.seal(
      header: header,
      ciphertext: ciphertext,
      keys: PushWakeEnvelope.Keys(pushKey: pushKey),
      appId: "blue.catbird"
    )
    XCTAssertEqual(Set(sealed.keys), ["v", "kid", "sealed"])
    let version2: [AnyHashable: Any] = [
      "aps": ["alert": ["body": "New message"], "mutable-content": 1],
      "catbird": sealed,
    ]

    let fromV1 = try XCTUnwrap(
      MLSPushExtensionProcessor.decodePushWakePayload(from: version1, wakeKeys: store)
    )
    let fromV2 = try XCTUnwrap(
      MLSPushExtensionProcessor.decodePushWakePayload(from: version2, wakeKeys: store)
    )
    XCTAssertEqual(fromV2.kind, fromV1.kind)
    XCTAssertEqual(fromV2.conversationId, fromV1.conversationId)
    XCTAssertEqual(fromV2.generation, fromV1.generation)
    XCTAssertEqual(fromV2.entrySeq, fromV1.entrySeq)
    XCTAssertEqual(fromV2.messageId, fromV1.messageId)
    XCTAssertEqual(fromV2.recipientAccountHash, didHash)
    XCTAssertEqual(fromV2.recipientAccountHash, fromV1.recipientAccountHash)
    XCTAssertEqual(fromV2.fetchRequired, fromV1.fetchRequired)
    XCTAssertEqual(fromV2.senderDid, fromV1.senderDid)
    XCTAssertEqual(fromV2.inlineCiphertext, ciphertext)
    XCTAssertEqual(fromV2.inlineCiphertext, fromV1.inlineCiphertext)

    // A device that does not hold the key leaves the notification untouched.
    let empty = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.test.pushwake",
      accessGroup: nil,
      backend: RecordingKeychainBackend()
    )
    XCTAssertNil(MLSPushExtensionProcessor.decodePushWakePayload(from: version2, wakeKeys: empty))
  }

  /// `T7-SWIFT-PUSH-RECEIPT`: Live end-to-end verification through `PetrelPlatformTransport`,
  /// `MLSPushExtensionProcessor.processAsync`, and `NativeArchiveMockNetwork`:
  /// - A tampered/forged `sequencerReceipt` in `getEntries` fails verification (`receiptVerified == false`),
  ///   returns generic fallback without leaking plaintext, and suppresses Darwin wakeup.
  /// - A genuine DS-signed `sequencerReceipt` verifies (`receiptVerified == true`), decrypts the
  ///   plaintext body, and gates `shouldPostDidCommit == true`.
  func testT7SwiftPushReceiptLiveNseVerifiesSequencerReceiptAndRejectsForgedReceipt() async throws {
    let network = NativeArchiveMockNetwork.shared
    network.reset()
    let boundedSession = MLSBoundedURLSession(protocolClasses: [NativeArchiveMockURLProtocol.self])
    defer {
      network.forgeReceipts(messageIds: [])
      boundedSession.invalidateAndCancel()
    }

    let aliceRoot = tempRoot.appendingPathComponent("alice", isDirectory: true)
    let bobRoot = tempRoot.appendingPathComponent("bob", isDirectory: true)
    try FileManager.default.createDirectory(at: aliceRoot, withIntermediateDirectories: true)
    try FileManager.default.createDirectory(at: bobRoot, withIntermediateDirectories: true)

    let aliceDid = "did:plc:t7pushreceiptaliceaaaaaa"
    let bobDid = "did:plc:t7pushreceiptbobbbbbbbbb"
    let aliceDeviceId = "6b1e2d3f-4c5a-4b7e-9f80-a1b2c3d4e5a1"
    let bobDeviceId = "6b1e2d3f-4c5a-4b7e-9f80-a1b2c3d4e5b1"
    let conversationId = "7a2d3b4c-5e6f-4a70-8b81-c2d3e4f5a799"
    let forgedMsgId = "7a2d3b4c-5e6f-4a70-8b81-c2d3e4f5a701"
    let genuineMsgId = "7a2d3b4c-5e6f-4a70-8b81-c2d3e4f5a702"

    let aliceKeystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.t7push.alice.\(UUID().uuidString)",
      backend: RecordingKeychainBackend()
    )
    let bobKeystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.t7push.bob.\(UUID().uuidString)",
      backend: RecordingKeychainBackend()
    )
    let aliceTransport = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: NativeArchiveMockNetwork.dsEndpoint),
      homeDsDid: NativeArchiveMockNetwork.dsDid,
      ownPdsEndpoint: URL(string: NativeArchiveMockNetwork.pdsEndpoint(for: aliceDid)),
      actorDeviceId: aliceDeviceId,
      boundedSession: boundedSession
    )
    let bobTransport = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: NativeArchiveMockNetwork.dsEndpoint),
      homeDsDid: NativeArchiveMockNetwork.dsDid,
      ownPdsEndpoint: URL(string: NativeArchiveMockNetwork.pdsEndpoint(for: bobDid)),
      actorDeviceId: bobDeviceId,
      boundedSession: boundedSession
    )

    let alice = try await MLSClientCoordinator.open(
      config: CatbirdMlsClientConfig(
        storageRoot: aliceRoot.path,
        accountDid: aliceDid,
        deviceId: aliceDeviceId,
        homeDsDid: NativeArchiveMockNetwork.dsDid,
        homeDsEndpoint: NativeArchiveMockNetwork.dsEndpoint,
        pdsEndpoint: NativeArchiveMockNetwork.pdsEndpoint(for: aliceDid)
      ),
      keystore: aliceKeystore,
      transport: aliceTransport
    )
    let bob = try await MLSClientCoordinator.open(
      config: CatbirdMlsClientConfig(
        storageRoot: bobRoot.path,
        accountDid: bobDid,
        deviceId: bobDeviceId,
        homeDsDid: NativeArchiveMockNetwork.dsDid,
        homeDsEndpoint: NativeArchiveMockNetwork.dsEndpoint,
        pdsEndpoint: NativeArchiveMockNetwork.pdsEndpoint(for: bobDid)
      ),
      keystore: bobKeystore,
      transport: bobTransport
    )

    _ = try await bob.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    _ = try await alice.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    _ = try await alice.createGroup(
      conversationId: conversationId,
      sequencerDid: NativeArchiveMockNetwork.dsDid,
      memberDids: [bobDid],
      title: "Receipt Gate Group",
      description: nil
    )
    _ = try await bob.sync()
    _ = try await alice.sync()
    _ = try await bob.sync()

    let bobDidHash = try MLSAppGroupStorage.accountDidHash(for: bobDid)
    let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
    try bobKeystore.storeCachedChatDpopToken(
      accountDidHash: bobDidHash,
      token: "valid-nse-chat-dpop-token",
      expiresAtMs: nowMs + 120_000
    )
    _ = try await bob.prepareForBackground()

    // 1. Forged I13 sequencerReceipt must be rejected before rendering or Darwin wakeup.
    let sentForged = try await alice.sendMessage(
      conversationId: conversationId,
      messageId: forgedMsgId,
      text: "forged-secret-must-not-render"
    )
    network.forgeReceipts(messageIds: [forgedMsgId])

    let forgedWake = PushWakePayload(
      kind: "application",
      conversationId: conversationId,
      generation: 0,
      entrySeq: sentForged.seq,
      messageId: forgedMsgId,
      recipientAccountHash: bobDidHash,
      fetchRequired: true,
      senderDid: aliceDid,
      inlineCiphertext: nil
    )
    let forgedPreview = await MLSPushExtensionProcessor.processAsync(
      storageRoot: bobRoot,
      accountDidHash: bobDidHash,
      keystore: bobKeystore,
      transport: bobTransport,
      wake: forgedWake,
      postDarwinNotification: false
    )
    XCTAssertFalse(forgedPreview.receiptVerified, "Forged sequencerReceipt must fail verification")
    XCTAssertTrue(forgedPreview.fallbackGeneric, "Forged sequencerReceipt must degrade to generic fallback")
    XCTAssertFalse(forgedPreview.body.contains("forged-secret-must-not-render"))
    XCTAssertFalse(MLSPushExtensionProcessor.shouldPostDidCommit(for: forgedPreview, requested: true))

    // 2. Restore genuine receipts: NSE catch-up verifies both entries and produces verified preview.
    network.forgeReceipts(messageIds: [])
    let sentGenuine = try await alice.sendMessage(
      conversationId: conversationId,
      messageId: genuineMsgId,
      text: "genuine-receipt-verified-body"
    )
    let genuineWake = PushWakePayload(
      kind: "application",
      conversationId: conversationId,
      generation: 0,
      entrySeq: sentGenuine.seq,
      messageId: genuineMsgId,
      recipientAccountHash: bobDidHash,
      fetchRequired: true,
      senderDid: aliceDid,
      inlineCiphertext: nil
    )
    let genuinePreview = await MLSPushExtensionProcessor.processAsync(
      storageRoot: bobRoot,
      accountDidHash: bobDidHash,
      keystore: bobKeystore,
      transport: bobTransport,
      wake: genuineWake,
      postDarwinNotification: false
    )
    XCTAssertTrue(genuinePreview.receiptVerified, "Genuine DS-signed sequencerReceipt must verify")
    XCTAssertFalse(genuinePreview.fallbackGeneric)
    XCTAssertTrue(genuinePreview.body.contains("genuine-receipt-verified-body"))
    XCTAssertFalse(genuinePreview.isOwnSiblingSender)
    XCTAssertTrue(MLSPushExtensionProcessor.shouldPostDidCommit(for: genuinePreview, requested: true))

    await alice.close()
    await bob.close()
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
    XCTAssertEqual(bundle.entries[1].entryId, "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff")
    XCTAssertEqual(bundle.entries[1].senderDid, "did:plc:alice123")
    XCTAssertEqual(bundle.entries[1].mlsBytes, rawMls)
  }

  func testDecodeGetEntriesCarriesCommitRequestAndSequencerReceipt() throws {
    let conversationId = "11111111-2222-4333-8444-555555555555"
    let entryId = "dddddddd-eeee-4fff-8000-111111111111"
    let commit = Data([0x01, 0x02, 0x03, 0x04])
    let hash = ["$bytes": Data(repeating: 0x22, count: 32).base64EncodedString()]
    let prior: [String: Any] = [
      "conversationId": conversationId,
      "generation": 0,
      "groupId": hash,
      "epoch": 3,
      "stateVersion": 3,
      "groupContextHash": hash,
      "confirmationTag": hash,
      "lifecycle": "active",
    ]
    var next = prior
    next["epoch"] = 4
    next["stateVersion"] = 4
    let signedRequest: [String: Any] = [
      "$type": "blue.catbird.chat.defs#signedCommitTransition",
      "body": [
        "$type": "blue.catbird.chat.defs#commitTransitionBody",
        "conversationId": conversationId,
        "transitionId": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "actorDid": "did:plc:alice123",
        "actorDeviceId": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "prior": prior,
        "next": next,
        "commit": ["bytes": ["$bytes": commit.base64EncodedString()]],
      ] as [String: Any],
      "signature": ["$bytes": Data(repeating: 0x11, count: 64).base64EncodedString()],
    ]
    let sequencerReceipt: [String: Any] = [
      "conversationId": conversationId,
      "generation": 0,
      "groupId": hash,
      "epoch": 4,
      "entryId": entryId,
      "seq": 9,
      "senderDid": "did:plc:alice123",
      "senderDeviceId": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
      "archiveCommitment": hash,
      "issuedAtMs": 1_799_000_000_000,
      "sequencerDid": "did:web:chat.catbird.blue",
      "kid": "did:web:chat.catbird.blue#mls-receipt-1",
      "signature": ["$bytes": Data(repeating: 0x33, count: 64).base64EncodedString()],
    ]
    let wire: [String: Any] = [
      "entries": [[
        "$type": "blue.catbird.chat.defs#commitEntry",
        "conversationId": conversationId,
        "entryId": entryId,
        "seq": 9,
        "signedRequest": signedRequest,
        "sequencerReceipt": sequencerReceipt,
        "receivedAt": "2026-11-11T00:00:00.000Z",
      ] as [String: Any]],
      "hasMore": false,
      "nextAfterSeq": 9,
    ]

    let data = try JSONSerialization.data(withJSONObject: wire)
    let entry = try XCTUnwrap(
      PetrelPlatformTransport.decodePushGetEntriesBundle(from: data).entries.first
    )
    XCTAssertEqual(entry.entryKind, "commit")
    XCTAssertEqual(entry.seq, 9, "the outer sequence must reach Rust beside its receipt")
    XCTAssertEqual(entry.mlsBytes, commit)
    XCTAssertEqual(
      try JSONSerialization.jsonObject(with: try XCTUnwrap(entry.signedRequest)) as? NSDictionary,
      signedRequest as NSDictionary
    )
    XCTAssertEqual(
      try JSONSerialization.jsonObject(with: try XCTUnwrap(entry.sequencerReceipt)) as? NSDictionary,
      sequencerReceipt as NSDictionary
    )

    let controls: [String: Any] = [
      "entries": [
        ["$type": "blue.catbird.chat.defs#leafRecoveryFulfillmentEntry", "seq": 10],
        ["$type": "blue.catbird.chat.defs#leaveCommitFulfillmentEntry", "seq": 11],
      ],
    ]
    let controlEntries = try PetrelPlatformTransport.decodePushGetEntriesBundle(
      from: JSONSerialization.data(withJSONObject: controls)
    ).entries
    XCTAssertEqual(
      controlEntries.map(\.entryKind),
      ["leafRecoveryFulfillment", "leaveCommitFulfillment"]
    )
  }
}
