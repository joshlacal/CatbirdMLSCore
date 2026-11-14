import CatbirdMLS
@testable import CatbirdMLSCore
import Foundation
import Testing

/// Inert transport: Circle operations that reach the network fail closed.
private final class OfflineCircleTransport: PlatformTransport, @unchecked Sendable {
  func executeHttp(request: PlatformHttpRequest) throws -> PlatformHttpResponse {
    PlatformHttpResponse(status: 503, headers: [], body: Data("{}".utf8))
  }

  func connectRealtimeStream(homeDsEndpoint: String, cursorSeq: Int64?) throws {}
  func disconnectRealtimeStream() throws {}
}

/// A real, unenrolled `CatbirdMlsClient` in a throwaway storage root. Enough for
/// the pure Circle operations (key generation, per-post key derivation) and for
/// input validation; it has no Labyrinth session.
private func makeUnenrolledClient() throws -> (CatbirdMlsClient, URL) {
  let root = FileManager.default.temporaryDirectory
    .appendingPathComponent("CatbirdCircleCrypto-\(UUID().uuidString)", isDirectory: true)
  try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
  let keystore = KeychainPlatformKeyStore(
    service: "blue.catbird.mls.test.circles.\(UUID().uuidString)",
    backend: RecordingKeychainBackend()
  )
  let config = CatbirdMlsClientConfig(
    storageRoot: root.path,
    accountDid: "did:plc:circlecryptotest0000000",
    deviceId: "11111111-1111-4111-8111-111111111111",
    homeDsDid: "did:web:chat.catbird.blue",
    homeDsEndpoint: "https://chat.catbird.blue",
    pdsEndpoint: "https://pds.catbird.blue"
  )
  let client = try CatbirdMlsClient.open(
    config: config,
    keystore: keystore,
    transport: OfflineCircleTransport(),
    observer: nil
  )
  return (client, root)
}

private func trash(_ root: URL) {
  let fm = FileManager.default
  let trashRoot = fm.temporaryDirectory.appendingPathComponent("_trash-CatbirdCircleCrypto", isDirectory: true)
  try? fm.createDirectory(at: trashRoot, withIntermediateDirectories: true)
  try? fm.moveItem(at: root, to: trashRoot.appendingPathComponent(root.lastPathComponent))
}

@Suite("CircleCrypto")
struct CircleCryptoTests {
  @Test func postKeyIsDeterministic() async throws {
    let (client, root) = try makeUnenrolledClient()
    defer { trash(root) }
    let crypto = FFICircleCrypto(client: client)

    let circleKey = Data((0 ..< 32).map { UInt8($0) })
    let postURI = "at://did:plc:owner/space/blue.catbird.circle.encrypted/c1/blue.catbird.circle.post/3k"
    let a = try crypto.postKey(circleKey: circleKey, circleID: "c1", postURI: postURI)
    let b = try crypto.postKey(circleKey: circleKey, circleID: "c1", postURI: postURI)
    #expect(a.count == 32)
    #expect(a == b)
    #expect(a != circleKey)
    // Domain-separated by post URI and Circle id.
    #expect(try crypto.postKey(circleKey: circleKey, circleID: "c1", postURI: postURI + "x") != a)
    #expect(try crypto.postKey(circleKey: circleKey, circleID: "c2", postURI: postURI) != a)
  }

  @Test func generatedCircleKeysAreFresh32Bytes() async throws {
    let (client, root) = try makeUnenrolledClient()
    defer { trash(root) }
    let crypto = FFICircleCrypto(client: client)
    let k1 = try await crypto.generateCircleKey()
    let k2 = try await crypto.generateCircleKey()
    #expect(k1.count == 32)
    #expect(k2.count == 32)
    #expect(k1 != k2)
  }

  /// Replaces `openCircleKeyReturnsNilForStranger`: this package has no cheap
  /// enrolled-client fixture (enrollment needs the full mock Space server of the
  /// Labyrinth E2E suite), so this covers the open path's failure mode instead.
  @Test func openCircleKeyRejectsMalformedRecord() async throws {
    let (client, root) = try makeUnenrolledClient()
    defer { trash(root) }
    let crypto = FFICircleCrypto(client: client)
    await #expect(throws: CircleCryptoError.self) {
      _ = try await crypto.openCircleKey(
        recordJSON: "{not json",
        spaceURI: "at://did:plc:owner/space/blue.catbird.circle.encrypted/c1",
        ownerDID: "did:plc:owner"
      )
    }
  }

  /// The Rust layer refuses an owner that is not the DID authority of the
  /// Space URI before opening the session (no directory lookup of that DID);
  /// the wrapper surfaces it as `.untrusted`.
  @Test func openCircleKeyRefusesOwnerThatIsNotTheSpaceAuthority() async throws {
    let (client, root) = try makeUnenrolledClient()
    defer { trash(root) }
    let crypto = FFICircleCrypto(client: client)
    do {
      _ = try await crypto.openCircleKey(
        recordJSON: "{}",
        spaceURI: "at://did:plc:realowner/space/blue.catbird.circle.encrypted/3m5circle2aa",
        ownerDID: "did:plc:bogusowner"
      )
      Issue.record("open with a bogus owner must throw")
    } catch let error as CircleCryptoError {
      guard case let .untrusted(reason) = error else {
        Issue.record("expected .untrusted, got \(error)")
        return
      }
      #expect(reason.contains("space authority"))
    }
  }

  @Test func ffiErrorsMapOneToOne() {
    #expect(CircleCryptoError(CircleFfiError.NotOpenable(reason: "a")) == .notOpenable("a"))
    #expect(CircleCryptoError(CircleFfiError.Untrusted(reason: "b")) == .untrusted("b"))
    #expect(CircleCryptoError(CircleFfiError.Malformed(reason: "c")) == .malformed("c"))
    #expect(CircleCryptoError(CircleFfiError.Unavailable(reason: "d")) == .unavailable("d"))
    #expect(CircleCryptoError.from(CircleFfiError.Untrusted(reason: "b")) == .untrusted("b"))
    #expect(CircleCryptoError.from(CircleCryptoError.malformed("x")) == .malformed("x"))
    #expect(CircleCryptoError.untrusted("why").reason == "why")
  }

  @Test func nonCircleErrorsMapToUnavailable() {
    struct Other: Error {}
    guard case .unavailable = CircleCryptoError.from(Other()) else {
      Issue.record("non-Circle error must map to .unavailable")
      return
    }
    guard case .unavailable = CircleCryptoError.from(MlsError.Internal(message: "secret detail")) else {
      Issue.record("MlsError must map to .unavailable")
      return
    }
  }

  /// `OpenedCircleKey` carries the authenticated id/version from the FFI
  /// result verbatim and never prints the key.
  @Test func openedCircleKeyCarriesAuthenticatedIdentity() {
    let key = Data(repeating: 0xAB, count: 32)
    let opened = OpenedCircleKey(
      CatbirdMLS.CircleKeyOpenResult(circleKey: key, circleId: "3m5circle2aa", keyVersion: 7)
    )
    #expect(opened.circleKey == key)
    #expect(opened.circleID == "3m5circle2aa")
    #expect(opened.keyVersion == 7)
    #expect(!opened.description.contains("ab"))
    #expect(!String(reflecting: opened).contains("171"))
    #expect(opened.description.contains("<redacted>"))
  }

  @Test func postKeyRejectsWrongLengthKey() async throws {
    let (client, root) = try makeUnenrolledClient()
    defer { trash(root) }
    let crypto = FFICircleCrypto(client: client)
    do {
      _ = try crypto.postKey(circleKey: Data(repeating: 7, count: 31), circleID: "c1", postURI: "at://did:plc:owner/p/1")
      Issue.record("a 31-byte key must be rejected")
    } catch let error as CircleCryptoError {
      guard case .malformed = error else {
        Issue.record("expected .malformed, got \(error)")
        return
      }
    }
  }

  @Test func createdAtMatchesATProtocolDateFormat() {
    let date = Date(timeIntervalSince1970: 1_762_473_600.25)
    let s = CircleCryptoFormat.createdAt(date)
    #expect(s == "2025-11-07T00:00:00.250Z")
    #expect(CircleCryptoFormat.createdAt(Date(timeIntervalSince1970: 0)) == "1970-01-01T00:00:00.000Z")
  }

  @Test func exclusionsZipDIDsWithReasons() {
    let result = CircleKeyWrapResult(
      recordJson: "{}",
      rkey: "1",
      excludedDids: ["did:plc:a", "did:plc:b"],
      excludedReasons: ["no epoch", "unverified"]
    )
    let outcome = CircleKeyWrapOutcome(result)
    #expect(outcome.recordJSON == "{}")
    #expect(outcome.rkey == "1")
    #expect(outcome.excluded.map(\.did) == ["did:plc:a", "did:plc:b"])
    #expect(outcome.excluded.map(\.reason) == ["no epoch", "unverified"])
  }
}
