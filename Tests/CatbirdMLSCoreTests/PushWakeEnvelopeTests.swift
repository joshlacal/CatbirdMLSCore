import CatbirdMLS
@testable import CatbirdMLSCore
import CryptoKit
import Foundation
import Testing

/// Sealed push wake envelope version 2, checked against the shared test vector in
/// `docs/security/2026-11-09-sealed-push-wake-envelope.md` (asserted on the gateway by
/// `known_answer_vector` in `nest/catbird/src/services/push/wake_envelope.rs`).
@Suite("Sealed push wake envelope v2")
struct PushWakeEnvelopeTests {
  static let pushKey = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
  static let appId = "blue.catbird"
  static let conversationId = "3f0e6f0a-6f0b-4c5e-9d55-0a4e4f6c2b11"
  static let headerJSON = Data(
    #"{"wakeKind":"applicationMessage","conversationId":"3f0e6f0a-6f0b-4c5e-9d55-0a4e4f6c2b11","generation":0,"seq":7,"epoch":3,"fetchRequired":false}"#
      .utf8
  )
  static let ciphertext = Data([0xDE, 0xAD, 0xBE, 0xEF])
  static let nonce = Data(repeating: 0x11, count: 12)

  static func hex(_ bytes: some Sequence<UInt8>) -> String {
    bytes.map { String(format: "%02x", $0) }.joined()
  }

  static func vectorEnvelope() throws -> [String: Any] {
    try PushWakeEnvelope.seal(
      header: headerJSON,
      ciphertext: ciphertext,
      keys: PushWakeEnvelope.Keys(pushKey: pushKey),
      appId: appId,
      nonce: nonce
    )
  }

  /// Seals arbitrary plaintext bytes under the vector key, for malformed-plaintext cases.
  static func sealRaw(_ plain: Data, appId: String = appId) throws -> [String: Any] {
    let keys = PushWakeEnvelope.Keys(pushKey: pushKey)
    let box = try AES.GCM.seal(
      plain,
      using: keys.key,
      nonce: AES.GCM.Nonce(data: nonce),
      authenticating: Data("catbird-push-wake-v2|\(keys.kid)|\(appId)".utf8)
    )
    var raw = nonce
    raw.append(box.ciphertext)
    raw.append(box.tag)
    return ["v": 2, "kid": keys.kid, "sealed": PushWakeEnvelope.encodeBase64URLNoPad(raw)]
  }

  static func plaintext(header: Data, ciphertext: Data, length: Int = 2560, padByte: UInt8 = 0) -> Data {
    var plain = Data([UInt8(header.count >> 8), UInt8(header.count & 0xFF)])
    plain.append(header)
    plain.append(contentsOf: [UInt8(ciphertext.count >> 8), UInt8(ciphertext.count & 0xFF)])
    plain.append(ciphertext)
    plain.append(Data(repeating: padByte, count: max(0, length - plain.count)))
    return plain
  }

  static func openError(_ envelope: [String: Any], appId: String = appId) -> PushWakeEnvelope.OpenError? {
    do {
      _ = try PushWakeEnvelope.open(envelope, keys: PushWakeEnvelope.Keys(pushKey: pushKey), appId: appId)
      return nil
    } catch {
      return error as? PushWakeEnvelope.OpenError
    }
  }

  static func rawOpenError(_ plain: Data) -> PushWakeEnvelope.OpenError? {
    guard let envelope = try? sealRaw(plain) else { return .malformedEnvelope }
    return openError(envelope)
  }

  @Test("Key and kid derivation match the spec vector")
  func derivationMatchesVector() {
    let keys = PushWakeEnvelope.Keys(pushKey: Self.pushKey)
    #expect(
      keys.key.withUnsafeBytes { Self.hex($0) }
        == "2cabdf528c8de4de63eb1450b059454b100d32b8d27bc6ec72c7913435c37bba"
    )
    #expect(keys.kid == "147038276a8a4cab")
  }

  @Test("Sealing the vector inputs with the fixed nonce reproduces the gateway's AES-GCM output, and it opens")
  func sealedVectorMatchesGatewayAndOpens() throws {
    let envelope = try Self.vectorEnvelope()
    let sealed = try #require(envelope["sealed"] as? String)
    #expect(sealed.count == 3451)
    #expect(sealed.prefix(48) == "ERERERERERERERERbo1f3fz0vDFIWBf2fNbQdl6ql0646ibw")
    #expect(
      Self.hex(SHA256.hash(data: Data(sealed.utf8)))
        == "9064f0f4ba65680f4e952537d3fefae8df1eb17814affa3446dbc988d9d96e3b"
    )

    let opened = try PushWakeEnvelope.open(
      envelope,
      keys: PushWakeEnvelope.Keys(pushKey: Self.pushKey),
      appId: Self.appId
    )
    #expect(
      opened.header == PushWakeEnvelope.Header(
        wakeKind: "applicationMessage",
        conversationId: Self.conversationId,
        generation: 0,
        seq: 7,
        epoch: 3,
        fetchRequired: false
      )
    )
    #expect(opened.ciphertext == Self.ciphertext)
  }

  @Test("A fetch-only wake opens with no ciphertext and is the same size on the wire")
  func fetchOnlyWake() throws {
    let header = Data(
      #"{"wakeKind":"applicationMessage","conversationId":"c","generation":1,"seq":2,"epoch":3,"fetchRequired":true}"#
        .utf8
    )
    let keys = PushWakeEnvelope.Keys(pushKey: Self.pushKey)
    let envelope = try PushWakeEnvelope.seal(header: header, ciphertext: nil, keys: keys, appId: Self.appId)
    #expect((envelope["sealed"] as? String)?.count == 3451)
    let opened = try PushWakeEnvelope.open(envelope, keys: keys, appId: Self.appId)
    #expect(opened.header.fetchRequired)
    #expect(opened.ciphertext == nil)
  }

  @Test("Wrong key, wrong appId, wrong kid, tampering, wrong length and wrong version are rejected")
  func rejectsEnvelopeLevelFailures() throws {
    let envelope = try Self.vectorEnvelope()
    let sealed = try #require(envelope["sealed"] as? String)

    #expect(Self.openError(envelope, appId: "blue.catbird.android") == .authenticationFailed)

    let otherKeys = PushWakeEnvelope.Keys(pushKey: "a-different-push-key-of-sufficient-length")
    #expect(throws: PushWakeEnvelope.OpenError.kidMismatch) {
      try PushWakeEnvelope.open(envelope, keys: otherKeys, appId: Self.appId)
    }

    var tampered = Array(sealed)
    tampered[100] = tampered[100] == "A" ? "B" : "A"
    #expect(Self.openError(["v": 2, "kid": "147038276a8a4cab", "sealed": String(tampered)]) == .authenticationFailed)

    #expect(
      Self.openError(["v": 2, "kid": "147038276a8a4cab", "sealed": String(sealed.dropLast(4))])
        == .sealedWrongLength
    )
    #expect(
      Self.openError(["v": 2, "kid": "147038276a8a4cab", "sealed": sealed + "AAAA"]) == .sealedWrongLength
    )
    #expect(
      Self.openError(["v": 2, "kid": "147038276a8a4cab", "sealed": String(sealed.dropLast(1)) + "+"])
        == .sealedNotBase64URL
    )
    #expect(Self.openError(["v": 3, "kid": "147038276a8a4cab", "sealed": sealed]) == .notVersion2)
    #expect(Self.openError(["kid": "147038276a8a4cab", "sealed": sealed]) == .notVersion2)
    #expect(
      Self.openError(["v": 2, "kid": "147038276a8a4cab", "sealed": sealed, "extra": 1]) == .malformedEnvelope
    )
  }

  @Test("Non-zero padding, unknown or missing header members, mistyped members and fetchRequired mismatches are rejected")
  func rejectsMalformedPlaintext() throws {
    #expect(
      Self.rawOpenError((Self.plaintext(header: Self.headerJSON, ciphertext: Self.ciphertext, padByte: 1)))
        == .paddingNotZero
    )
    var lastByteSet = Self.plaintext(header: Self.headerJSON, ciphertext: Self.ciphertext)
    lastByteSet[lastByteSet.count - 1] = 0x80
    #expect(Self.rawOpenError((lastByteSet)) == .paddingNotZero)

    let extraMember = Data(
      #"{"wakeKind":"a","conversationId":"c","generation":0,"seq":7,"epoch":3,"fetchRequired":false,"recipientAccountHash":"x"}"#
        .utf8
    )
    #expect(
      Self.rawOpenError((Self.plaintext(header: extraMember, ciphertext: Self.ciphertext)))
        == .headerInvalid
    )
    let missingMember = Data(#"{"wakeKind":"a","conversationId":"c","generation":0,"seq":7,"fetchRequired":false}"#.utf8)
    #expect(
      Self.rawOpenError((Self.plaintext(header: missingMember, ciphertext: Self.ciphertext)))
        == .headerInvalid
    )
    let mistyped = Data(
      #"{"wakeKind":"a","conversationId":"c","generation":0,"seq":"7","epoch":3,"fetchRequired":0}"#.utf8
    )
    #expect(
      Self.rawOpenError((Self.plaintext(header: mistyped, ciphertext: Self.ciphertext)))
        == .headerInvalid
    )

    // fetchRequired false with no ciphertext, and true with one.
    #expect(
      Self.rawOpenError((Self.plaintext(header: Self.headerJSON, ciphertext: Data())))
        == .fetchRequiredMismatch
    )
    let fetchTrue = Data(
      #"{"wakeKind":"a","conversationId":"c","generation":0,"seq":7,"epoch":3,"fetchRequired":true}"#.utf8
    )
    #expect(
      Self.rawOpenError((Self.plaintext(header: fetchTrue, ciphertext: Self.ciphertext)))
        == .fetchRequiredMismatch
    )

    // An authentic blob whose plaintext is not exactly 2560 bytes.
    #expect(
      Self.rawOpenError((Self.plaintext(header: Self.headerJSON, ciphertext: Self.ciphertext, length: 2559)))
        == .sealedWrongLength
    )
    // Declared lengths that run past the plaintext.
    var overrun = Self.plaintext(header: Self.headerJSON, ciphertext: Self.ciphertext)
    overrun[0] = 0xFF
    overrun[1] = 0xFF
    #expect(Self.rawOpenError((overrun)) == .headerOverrun)
  }

  @Test("The keychain store indexes kid to account, replaces on a new key, and clears on delete and sign-out")
  func keychainStoreLifecycle() throws {
    let store = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.test.pushwake",
      accessGroup: nil,
      backend: RecordingKeychainBackend()
    )
    let alice = "did:plc:alicepushwake0000000000"
    let bob = "did:plc:bobpushwake000000000000"
    let aliceFirst = PushWakeEnvelope.Keys(pushKey: Self.pushKey).kid
    #expect(store.pushWakeKey(forKid: aliceFirst) == nil)

    try store.storePushWakeKey(pushKey: Self.pushKey, appId: Self.appId, accountDid: alice)
    try store.storePushWakeKey(pushKey: "bob-push-key-bob-push-key-bob-push-key-1234", appId: Self.appId, accountDid: bob)
    let bobKid = PushWakeEnvelope.Keys(pushKey: "bob-push-key-bob-push-key-bob-push-key-1234").kid
    #expect(store.pushWakeKey(forKid: aliceFirst)?.accountDid == alice)
    #expect(store.pushWakeKey(forKid: aliceFirst)?.pushKey == Self.pushKey)
    #expect(store.pushWakeKey(forKid: bobKid)?.accountDid == bob)
    #expect(store.pushWakeKey(forKid: "not-a-kid") == nil)

    // registerKey returned a new key for Alice: the old kid stops resolving.
    let rotated = "alice-rotated-push-key-alice-rotated-push-1"
    try store.storePushWakeKey(pushKey: rotated, appId: Self.appId, accountDid: alice)
    #expect(store.pushWakeKey(forKid: aliceFirst) == nil)
    #expect(store.pushWakeKey(forKid: PushWakeEnvelope.Keys(pushKey: rotated).kid)?.pushKey == rotated)

    // Chat pushes disabled for Alice.
    try store.deletePushWakeKey(accountDid: alice)
    #expect(store.pushWakeKey(forKid: PushWakeEnvelope.Keys(pushKey: rotated).kid) == nil)
    #expect(store.pushWakeKey(forKid: bobKid) != nil)

    // Bob signs out: the account-wide wipe removes the key and its index entry.
    try store.deleteAccountKeys(accountDidHash: MLSAppGroupStorage.accountDidHash(for: bob))
    #expect(store.pushWakeKey(forKid: bobKid) == nil)
  }

  @Test("decodePushWakePayload opens a v2 wake into the same PushWakePayload, and returns nil for an unknown kid or a failed open")
  func decodeSealedWake() throws {
    let store = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.test.pushwake",
      accessGroup: nil,
      backend: RecordingKeychainBackend()
    )
    let account = "did:plc:alicepushwake0000000000"
    let envelope = try Self.vectorEnvelope()
    let userInfo: [AnyHashable: Any] = [
      "aps": ["alert": ["body": "New message"], "mutable-content": 1],
      "catbird": envelope,
    ]

    // Unknown kid: nothing stored yet.
    #expect(MLSPushExtensionProcessor.decodePushWakePayload(from: userInfo, wakeKeys: store) == nil)

    try store.storePushWakeKey(pushKey: Self.pushKey, appId: Self.appId, accountDid: account)
    let wake = try #require(MLSPushExtensionProcessor.decodePushWakePayload(from: userInfo, wakeKeys: store))
    #expect(wake.kind == "applicationMessage")
    #expect(wake.conversationId == Self.conversationId)
    #expect(wake.generation == 0)
    #expect(wake.entrySeq == 7)
    #expect(wake.messageId == "msg-7")
    #expect(!wake.fetchRequired)
    #expect(wake.inlineCiphertext == Self.ciphertext)
    #expect(wake.senderDid == nil)
    #expect(wake.recipientAccountHash == Self.hex(SHA256.hash(data: Data(account.utf8))))
    let storageHash = try MLSAppGroupStorage.accountDidHash(for: account)
    #expect(wake.recipientAccountHash == storageHash)

    // FCM-style: the catbird object arrives as a JSON string.
    let asString = try #require(
      String(data: try JSONSerialization.data(withJSONObject: envelope), encoding: .utf8)
    )
    #expect(
      MLSPushExtensionProcessor.decodePushWakePayload(from: ["catbird": asString], wakeKeys: store)?
        .conversationId == Self.conversationId
    )

    // Known kid, but registered under another appId: the open fails, so nil.
    try store.storePushWakeKey(pushKey: Self.pushKey, appId: "blue.catbird.android", accountDid: account)
    #expect(MLSPushExtensionProcessor.decodePushWakePayload(from: userInfo, wakeKeys: store) == nil)

    // An unsupported envelope version is not parsed as version 1.
    let future: [AnyHashable: Any] = [
      "catbird": [
        "v": 3,
        "recipientAccountHash": String(repeating: "a", count: 64),
        "conversationId": Self.conversationId,
      ] as [String: Any],
    ]
    #expect(MLSPushExtensionProcessor.decodePushWakePayload(from: future, wakeKeys: store) == nil)
  }
}
