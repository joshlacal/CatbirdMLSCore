import Foundation
import XCTest
import CatbirdMLS
import Synchronization
@testable import CatbirdMLSCore

final class MLSDeviceAuthorizationTests: XCTestCase {
  let did = "did:plc:oq3qa6f332ergklpj2dvd3up"
  let device = "00000000-0000-4000-8000-000000000001"
  let key = Data(repeating: 7, count: 32)

  func page(uri: String? = nil, algorithm: String = "ed25519", bytes: Data? = nil, type: String = "blue.catbird.chat.device", cursor: String? = nil) throws -> Data {
    var value: [String: Any] = ["records": [[
      "uri": uri ?? "at://\(did)/blue.catbird.chat.device/\(device)",
      "value": ["$type": type, "algorithm": algorithm,
                "mlsSignaturePublicKey": ["$bytes": (bytes ?? key).base64EncodedString()],
                "createdAt": "2026-09-04T00:00:00.000Z"]]]]
    if let cursor { value["cursor"] = cursor }
    return try JSONSerialization.data(withJSONObject: value)
  }

  func testStrictRepoAuthorizationValidatesProofShapeAndDoesNotReturnEmptyOnMalformedInput() throws {
    XCTAssertEqual(try MLSPublicPDSReader.decodeAuthorizedDevicePage(page(), did: did).records.map(\.publicKey), [key])
    let unpadded = String(decoding: try page(), as: UTF8.self).replacingOccurrences(of: key.base64EncodedString(), with: key.base64EncodedString().replacingOccurrences(of: "=", with: ""))
    XCTAssertEqual(try MLSPublicPDSReader.decodeAuthorizedDevicePage(Data(unpadded.utf8), did: did).records.map(\.publicKey), [key])
    for data in [
      try page(uri: "at://did:plc:other/blue.catbird.chat.device/\(device)"),
      try page(algorithm: "secp256k1"), try page(bytes: Data([1])),
      try page(type: "app.bsky.feed.post"), Data("not-json".utf8), Data("{}".utf8)
    ] {
      XCTAssertThrowsError(try MLSPublicPDSReader.decodeAuthorizedDevicePage(data, did: did))
    }
    XCTAssertEqual(try MLSPublicPDSReader.decodeAuthorizedDevicePage(Data("{\"records\":[]}".utf8), did: did).records, [])
  }

  func testRepositoryLookupReadsEveryPageAndRejectsCursorLoopAndHttpFailure() async throws {
    let config = URLSessionConfiguration.ephemeral
    config.protocolClasses = [AuthorizationURLProtocol.self]
    let session = URLSession(configuration: config)
    defer { session.invalidateAndCancel(); AuthorizationURLProtocol.handler = nil }
    let first = try page(cursor: "next")
    AuthorizationURLProtocol.handler = { request in
      let next = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)!.queryItems!.contains { $0.name == "cursor" && $0.value == "next" }
      return (200, next ? Data("{\"records\":[]}".utf8) : first)
    }
    let resolved = try await MLSPublicPDSReader.fetchAuthorizedDeviceSignatureKeys(
      did: did, resolvePDS: { _ in URL(string: "https://pds.example")! }, session: session)
    XCTAssertEqual(resolved, [key])
    AuthorizationURLProtocol.handler = { _ in (200, Data("{\"records\":[],\"cursor\":\"repeat\"}".utf8)) }
    do {
      _ = try await MLSPublicPDSReader.fetchAuthorizedDeviceSignatureKeys(did: did, resolvePDS: { _ in URL(string: "https://pds.example")! }, session: session)
      XCTFail("Repeated cursor must not produce a partial key set")
    } catch {}
    AuthorizationURLProtocol.handler = { _ in (503, Data()) }
    do {
      _ = try await MLSPublicPDSReader.fetchAuthorizedDeviceSignatureKeys(did: did, resolvePDS: { _ in URL(string: "https://pds.example")! }, session: session)
      XCTFail("Network failure must not become unsupported or empty authorization")
    } catch {}
  }

  func testPublisherIsIdempotentAndNeverOverwritesAnotherKey() async throws {
    let uri = "at://\(did)/blue.catbird.chat.device/\(device)"
    var writes = 0
    try await MLSDeviceRecordPublication.ensure(userDid: did, deviceId: device, publicKey: key,
      read: { [.init(uri: uri, publicKey: self.key)] }, create: { writes += 1 })
    XCTAssertEqual(writes, 0)
    do {
      try await MLSDeviceRecordPublication.ensure(userDid: did, deviceId: device, publicKey: key,
        read: { [.init(uri: uri, publicKey: Data(repeating: 8, count: 32))] }, create: { writes += 1 })
      XCTFail("Conflicting authorization must not be replaced")
    } catch {}
    XCTAssertEqual(writes, 0)
  }

  func testPublisherCreatesAbsentRecordAndRequiresFreshProofAfterAmbiguousAcknowledgement() async throws {
    struct LostAcknowledgement: Error {}
    let uri = "at://\(did)/blue.catbird.chat.device/\(device)"
    var records: [MLSPublicPDSReader.AuthorizedDeviceRecord] = []
    var writes = 0
    try await MLSDeviceRecordPublication.ensure(userDid: did, deviceId: device, publicKey: key,
      read: { records }, create: { writes += 1; records = [.init(uri: uri, publicKey: self.key)]; throw LostAcknowledgement() })
    XCTAssertEqual(writes, 1)
    records = []
    do {
      try await MLSDeviceRecordPublication.ensure(userDid: did, deviceId: device, publicKey: key,
        read: { records }, create: { throw LostAcknowledgement() })
      XCTFail("Unconfirmed publication must remain an error")
    } catch { XCTAssertTrue(error is LostAcknowledgement) }
  }

  func testFreshDIDAuthorityResolutionObservesPDSMigration() async throws {
    let config = URLSessionConfiguration.ephemeral
    config.protocolClasses = [AuthorizationURLProtocol.self]
    let session = URLSession(configuration: config)
    defer { session.invalidateAndCancel(); AuthorizationURLProtocol.handler = nil }
    func document(_ host: String) throws -> Data {
      try JSONSerialization.data(withJSONObject: ["id": did, "service": [[
        "id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://\(host)"
      ]]])
    }
    let responses = Mutex([try document("old.example"), try document("new.example")])
    AuthorizationURLProtocol.handler = { request in
      XCTAssertEqual(request.cachePolicy, .reloadIgnoringLocalCacheData)
      return (200, responses.withLock { $0.removeFirst() })
    }
    let old = try await MLSPublicPDSReader.resolveCurrentPDS(did: did, session: session)
    let new = try await MLSPublicPDSReader.resolveCurrentPDS(did: did, session: session)
    XCTAssertEqual(old.host, "old.example")
    XCTAssertEqual(new.host, "new.example", "A forced native refresh must not remain pinned to the previous PDS")
  }

  func testFailedPublicationDeniesSyncAndSendThenRetriesForSameSession() async throws {
    struct FailedPublication: Error {}
    let gate = MLSDeviceAuthorizationGate()
    let attempts = Mutex(0)
    var enteredProtocol = 0
    for _ in ["sync", "send"] {
      do {
        try await gate.ensure(scope: "did/device/session") { attempts.withLock { $0 += 1 }; throw FailedPublication() }
        enteredProtocol += 1
        XCTFail("Failed publication cannot admit protocol work")
      } catch { XCTAssertTrue(error is FailedPublication) }
    }
    XCTAssertEqual(enteredProtocol, 0)
    try await gate.ensure(scope: "did/device/session") { attempts.withLock { $0 += 1 } }
    enteredProtocol += 1
    try await gate.ensure(scope: "did/device/session") { XCTFail("Same completed session is already authorized") }
    XCTAssertEqual(enteredProtocol, 1)
    XCTAssertEqual(attempts.withLock { $0 }, 3)
    try await gate.ensure(scope: "different-did/device/session") { attempts.withLock { $0 += 1 } }
    XCTAssertEqual(attempts.withLock { $0 }, 4, "Authorization cannot transfer to another account/device scope")
  }

  func testConcurrentPublicationWaitersShareAttemptAndCancelledWaiterCannotProceed() async throws {
    let gate = MLSDeviceAuthorizationGate()
    let suspension = AuthorizationSuspension()
    let started = expectation(description: "Publication started")
    let first = Task {
      try await gate.ensure(scope: "same") { started.fulfill(); await suspension.wait() }
    }
    await fulfillment(of: [started], timeout: 1)
    let waiter = Task { try await gate.ensure(scope: "same") { XCTFail("Must share existing publication") } }
    waiter.cancel()
    await suspension.resume()
    try await first.value
    do { try await waiter.value; XCTFail("Cancelled waiter cannot enter protocol work") }
    catch { XCTAssertTrue(error is CancellationError) }
    try await gate.ensure(scope: "same") { XCTFail("Successful owner retained publication proof") }
  }

  func testRepoResolutionAndBindingErrorsHaveHumanDescriptions() {
    for error in [
      OrchestratorBridgeError.Credential(message: "device_authorization_unavailable: lookup failed"),
      OrchestratorBridgeError.InvalidInput(message: "application DID/device credential binding rejected during send: unauthorized device key")
    ] {
      let mapped = MLSConversationLifecycleError.presentingDeviceAuthorization(error)
      XCTAssertTrue(mapped.localizedDescription.contains("cannot verify"))
      XCTAssertFalse(mapped.localizedDescription.contains("OrchestratorBridgeError"))
      XCTAssertFalse(mapped.localizedDescription.contains("device_authorization_unavailable:"))
    }
  }

  func testCredentialCallbackPropagatesRepositoryResolutionFailure() throws {
    struct ResolutionFailed: Error {}
    let adapter = MLSOrchestratorCredentialAdapter { _ in throw ResolutionFailed() }
    XCTAssertThrowsError(try adapter.getAuthorizedDeviceKeys(userDid: did)) {
      XCTAssertTrue($0 is ResolutionFailed)
    }
  }
}

private final class AuthorizationURLProtocol: URLProtocol {
  private static let state = Mutex<(@Sendable (URLRequest) throws -> (Int, Data))?>(nil)
  static var handler: (@Sendable (URLRequest) throws -> (Int, Data))? {
    get { state.withLock { $0 } }
    set { state.withLock { $0 = newValue } }
  }
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    do {
      let (status, data) = try Self.handler!(request)
      let response = HTTPURLResponse(url: request.url!, statusCode: status, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
      client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
      client?.urlProtocol(self, didLoad: data)
      client?.urlProtocolDidFinishLoading(self)
    } catch { client?.urlProtocol(self, didFailWithError: error) }
  }
  override func stopLoading() {}
}

private actor AuthorizationSuspension {
  var released = false
  var continuation: CheckedContinuation<Void, Never>?
  func wait() async {
    if released { return }
    await withCheckedContinuation { continuation = $0 }
  }
  func resume() { released = true; continuation?.resume(); continuation = nil }
}
