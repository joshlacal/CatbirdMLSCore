import Foundation
import CatbirdMLS
import Petrel
import XCTest
@testable import CatbirdMLSCore

final class MLSLabyrinthTransportTests: XCTestCase {
  func testGeneratedBindingsMatchSelectedNativeArtifact() {
    // Any generated native call validates the complete UniFFI checksum table.
    // Callback documentation can change checksums without changing signatures.
    XCTAssertGreaterThan(securityStorageCapabilitiesVersion(), 0)
  }

  func testSpaceCapabilityRequiresVerifiedLocalNativeArtifact() {
    #if CATBIRD_MLS_VERIFIED_SPACE_FFI
    XCTAssertTrue(MLSAPIClient.labyrinthSpaceTransportAvailable)
    #else
    XCTAssertFalse(MLSAPIClient.labyrinthSpaceTransportAvailable)
    #endif
  }
  private typealias Client = MLSAPIClient
  private typealias TransportError = MLSAPIClient.LabyrinthTransportError

  // MARK: - Request validation

  func testAcceptsLabyrinthProcedureAndQueryPaths() throws {
    XCTAssertNoThrow(try Client.validateLabyrinthRequest(
      method: "POST", nsid: "blue.catbird.labyrinth.appendBatch",
      path: "/xrpc/blue.catbird.labyrinth.appendBatch"))
    XCTAssertNoThrow(try Client.validateLabyrinthRequest(
      method: "GET", nsid: "blue.catbird.labyrinth.listBatches",
      path: "/xrpc/blue.catbird.labyrinth.listBatches?actorDeviceId=abc&limit=10"))
  }

  func testRejectsNonLabyrinthNSIDs() {
    for nsid in ["blue.catbird.chat.sendMessage", "blue.catbird.labyrinth.", "com.atproto.repo.putRecord"] {
      XCTAssertThrowsError(try Client.validateLabyrinthRequest(
        method: "POST", nsid: nsid, path: "/xrpc/\(nsid)"), nsid)
    }
  }

  func testRejectsPathThatDoesNotMatchTheNSID() {
    XCTAssertThrowsError(try Client.validateLabyrinthRequest(
      method: "POST", nsid: "blue.catbird.labyrinth.appendBatch",
      path: "/xrpc/blue.catbird.labyrinth.deleteAccountData"))
    XCTAssertThrowsError(try Client.validateLabyrinthRequest(
      method: "POST", nsid: "blue.catbird.labyrinth.appendBatch",
      path: "https://evil.example/xrpc/blue.catbird.labyrinth.appendBatch"))
    XCTAssertThrowsError(try Client.validateLabyrinthRequest(
      method: "POST", nsid: "blue.catbird.labyrinth.appendBatch",
      path: "/xrpc/blue.catbird.labyrinth.appendBatchX"))
  }

  func testRejectsUnexpectedMethods() {
    XCTAssertThrowsError(try Client.validateLabyrinthRequest(
      method: "DELETE", nsid: "blue.catbird.labyrinth.appendBatch",
      path: "/xrpc/blue.catbird.labyrinth.appendBatch"))
  }

  // MARK: - Space credential reuse (MLSDS-01)

  /// Petrel caches credentials on the manager. A new manager per foreign
  /// read exchanged a fresh credential (delegation token, getSpaceCredential,
  /// authority policy call) for every record.
  func testForeignConversationReadsReuseOneCredentialManagerPerAccount() async {
    let atProto = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let api = await MLSAPIClient(client: atProto)
    let alice = api.labyrinthSpaceCredentialManager(for: "did:plc:alice")
    XCTAssertTrue(alice === api.labyrinthSpaceCredentialManager(for: "did:plc:alice"))

    let bob = api.labyrinthSpaceCredentialManager(for: "did:plc:bob")
    XCTAssertFalse(alice === bob, "another account must not reuse the first account's credentials")
    XCTAssertTrue(bob === api.labyrinthSpaceCredentialManager(for: "did:plc:bob"))
    XCTAssertFalse(alice === api.labyrinthSpaceCredentialManager(for: "did:plc:alice"),
                   "switching back must not resurrect the earlier account's credentials")

    let current = api.labyrinthSpaceCredentialManager(for: "did:plc:alice")
    api.clearLabyrinthSpaceCredentials()
    XCTAssertFalse(current === api.labyrinthSpaceCredentialManager(for: "did:plc:alice"))
  }

  // MARK: - One DID document per foreign read (CM-5)

  /// Each foreign record read fetched the repo's DID document twice: once
  /// for its PDS and once in Petrel's default author KEM key provider.
  func testForeignReadTakesPDSAndKEMKeyFromOneDocumentFetch() async throws {
    let repo = "did:plc:author"
    let fetches = DIDFetchCounter()
    let documents = LabyrinthDIDDocumentCache(fetch: { did in
      fetches.record(did)
      return labyrinthAuthorDocument(did)
    })
    let document = try await documents.document(for: repo)
    XCTAssertEqual(try MLSPublicPDSReader.currentPDS(in: document, did: repo), URL(string: "https://pds.example"))
    let key = try await Client.labyrinthAuthorKEMKey(repo: repo, forceRefresh: false, documents: documents)
    XCTAssertEqual(key, "did:key:" + labyrinthTestKEMMultibase)
    XCTAssertEqual(fetches.count(repo), 1, "the PDS and the KEM key must share one fetch")

    // Petrel retries once with forceRefresh after a MAC mismatch: always refetch.
    _ = try await Client.labyrinthAuthorKEMKey(repo: repo, forceRefresh: true, documents: documents)
    XCTAssertEqual(fetches.count(repo), 2)
    _ = try await documents.document(for: repo)
    XCTAssertEqual(fetches.count(repo), 2, "the refreshed document replaces the cached one")
  }

  func testDIDDocumentCacheIsShortLivedBoundedAndNeverKeepsFailures() async throws {
    let clock = DIDCacheTestClock()
    let fetches = DIDFetchCounter()
    let documents = LabyrinthDIDDocumentCache(lifetime: 60, capacity: 2, now: { clock.now }, fetch: { did in
      fetches.record(did)
      if did == "did:plc:unreachable" { throw URLError(.timedOut) }
      if did == "did:plc:impostor" { return labyrinthAuthorDocument("did:plc:someoneelse") }
      return labyrinthAuthorDocument(did)
    })

    _ = try await documents.document(for: "did:plc:a")
    clock.advance(59)
    _ = try await documents.document(for: "did:plc:a")
    XCTAssertEqual(fetches.count("did:plc:a"), 1)
    clock.advance(2)
    _ = try await documents.document(for: "did:plc:a")
    XCTAssertEqual(fetches.count("did:plc:a"), 2, "an entry older than its lifetime is refetched")

    // Capacity 2: a third DID evicts the entry closest to expiry.
    clock.advance(1)
    _ = try await documents.document(for: "did:plc:b")
    clock.advance(1)
    _ = try await documents.document(for: "did:plc:c")
    _ = try await documents.document(for: "did:plc:c")
    XCTAssertEqual(fetches.count("did:plc:c"), 1)
    _ = try await documents.document(for: "did:plc:a")
    XCTAssertEqual(fetches.count("did:plc:a"), 3, "the oldest entry was evicted")

    for did in ["did:plc:unreachable", "did:plc:impostor"] {
      for _ in 0..<2 {
        do {
          _ = try await documents.document(for: did)
          XCTFail("\(did) must not resolve")
        } catch {}
      }
      XCTAssertEqual(fetches.count(did), 2, "\(did): a failed or mismatched fetch is not cached")
    }

    documents.clear()
    _ = try await documents.document(for: "did:plc:c")
    XCTAssertEqual(fetches.count("did:plc:c"), 2)
  }

  func testEnvironmentSwitchDropsCachedSpaceCredentials() async {
    let atProto = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let api = await MLSAPIClient(client: atProto)
    let before = api.labyrinthSpaceCredentialManager(for: "did:plc:alice")
    await api.switchEnvironment(.custom(serviceDID: "did:web:example.com#atproto_mls"))
    XCTAssertFalse(before === api.labyrinthSpaceCredentialManager(for: "did:plc:alice"))
  }

  // MARK: - Credential failures as raw responses (X3)

  /// The synthesized response as catbird-mls's `decode_response` reads it.
  private func rawResponse(
    _ error: SpaceCredentialError
  ) throws -> (status: Int, error: String?, message: String?) {
    let url = try XCTUnwrap(URL(string: "https://pds.example/xrpc/com.atproto.space.getRecord"))
    let (data, response) = try Client.labyrinthSpaceCredentialResponse(for: error, url: url)
    XCTAssertEqual(response.value(forHTTPHeaderField: "Content-Type"), "application/json")
    let body = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: String])
    XCTAssertNotNil(body["message"])
    return (response.statusCode, body["error"], body["message"])
  }

  /// Labyrinth maps `SpaceDeleted` and 403 to its terminal `Forbidden`. As
  /// untyped bridge errors these reached it as status 0 with no error name.
  func testAuthorityRefusalsReachLabyrinthAsTypedResponses() throws {
    var raw = try rawResponse(.spaceDeleted(host: "authority.example", message: "Space has been deleted"))
    XCTAssertEqual(raw.status, 400)
    XCTAssertEqual(raw.error, "SpaceDeleted")
    XCTAssertEqual(raw.message, "Space has been deleted")

    raw = try rawResponse(.spaceDeleted(host: "authority.example", message: nil))
    XCTAssertEqual(raw.status, 400)
    XCTAssertEqual(raw.error, "SpaceDeleted")

    for name in ["NotAuthorized", "UserNotAuthorized", "AppNotAuthorized"] {
      raw = try rawResponse(.authorizationRefused(host: "authority.example", error: name, message: nil))
      XCTAssertEqual(raw.status, 403, name)
      XCTAssertEqual(raw.error, name)
    }
    // Petrel names a nameless 403 "HTTP 403"; that is not an atproto error name.
    raw = try rawResponse(.authorizationRefused(host: "authority.example", error: "HTTP 403", message: "denied"))
    XCTAssertEqual(raw.status, 403)
    XCTAssertNil(raw.error)
    XCTAssertEqual(raw.message, "denied")

    raw = try rawResponse(.tokenRejected(
      host: "authority.example", error: "InvalidDelegationToken", message: "token expired"))
    XCTAssertEqual(raw.status, 400)
    XCTAssertEqual(raw.error, "InvalidDelegationToken")
    XCTAssertEqual(raw.message, "token expired")
  }

  /// Petrel reports an authority's 400 SpaceNotFound as
  /// `authorizationRefused(error: "SpaceNotFound")`. It is not a refusal of
  /// this reader, so it keeps the authority's 400: labyrinth maps the name to
  /// NotFound("space"), and nothing that reads the status first sees the 403
  /// it turns into Forbidden.
  func testSpaceNotFoundKeepsTheAuthority400() throws {
    var raw = try rawResponse(.authorizationRefused(
      host: "authority.example", error: "SpaceNotFound", message: "Space not found"))
    XCTAssertEqual(raw.status, 400)
    XCTAssertEqual(raw.error, "SpaceNotFound")
    XCTAssertEqual(raw.message, "Space not found")

    let bare = SpaceCredentialError.authorizationRefused(
      host: "authority.example", error: "SpaceNotFound", message: nil)
    raw = try rawResponse(bare)
    XCTAssertEqual(raw.status, 400)
    XCTAssertEqual(raw.error, "SpaceNotFound")
    XCTAssertEqual(raw.message, bare.errorDescription)
    // Petrel's description says this is not a deletion.
    XCTAssertTrue(raw.message?.contains("does not mean it was deleted") == true)
  }

  /// Petrel reports an invalid client attestation (400 or 401) as
  /// `tokenRejected(error: "InvalidClientAttestation")`. It stays a 400 with
  /// its name, which labyrinth keeps retryable (Backend), not Forbidden.
  func testInvalidClientAttestationStaysRetryable() throws {
    let error = SpaceCredentialError.tokenRejected(
      host: "authority.example", error: "InvalidClientAttestation", message: nil)
    let raw = try rawResponse(error)
    XCTAssertEqual(raw.status, 400)
    XCTAssertEqual(raw.error, "InvalidClientAttestation")
    XCTAssertEqual(raw.message, error.errorDescription)
    XCTAssertTrue(raw.message?.contains("client attestation") == true)
  }

  func testExchangeFailuresKeepTheAuthorityStatusAndErrorName() throws {
    // Swan answers 404 {"error":"SpaceNotFound"} with no message, so Petrel
    // keeps the raw body as the message.
    var raw = try rawResponse(.exchangeFailed(statusCode: 404, message: #"{"error":"SpaceNotFound"}"#))
    XCTAssertEqual(raw.status, 404)
    XCTAssertEqual(raw.error, "SpaceNotFound")

    raw = try rawResponse(.exchangeFailed(statusCode: 503, message: "conversation policy unavailable"))
    XCTAssertEqual(raw.status, 503)
    XCTAssertNil(raw.error)
    XCTAssertEqual(raw.message, "conversation policy unavailable")

    // Never synthesize a success.
    raw = try rawResponse(.exchangeFailed(statusCode: 200, message: "unexpected"))
    XCTAssertEqual(raw.status, 502)
    XCTAssertEqual(raw.error, "UpstreamFailure")
  }

  func testLocalCredentialFailuresStayRetryable() throws {
    let expected: [(SpaceCredentialError, Int, String)] = [
      (.missingDelegationToken, 502, "UpstreamFailure"),
      (.invalidToken("Credential has expired"), 502, "UpstreamFailure"),
      (.invalidResponse, 502, "UpstreamFailure"),
      (.insecureURL("http://pds.example"), 502, "UpstreamFailure"),
      (.invalidKey, 500, "InternalServerError"),
      (.invalidSpaceRef("did:invalid"), 400, "InvalidRequest"),
    ]
    for (error, status, name) in expected {
      let raw = try rawResponse(error)
      XCTAssertEqual(raw.status, status, "\(error)")
      XCTAssertEqual(raw.error, name, "\(error)")
      XCTAssertEqual(raw.message, error.errorDescription, "\(error)")
    }
  }

  // MARK: - Service resolution

  func testOwnerSpaceCallbackUsesOnlyItsPersonalMailbox() throws {
    let did = "did:plc:alice"
    let nsid = "com.atproto.space.listRecords"
    let prefix = "/xrpc/\(nsid)?space=at://\(did)/space/blue.catbird.labyrinth.mailbox/self&repo="
    let suffix = "&collection=blue.catbird.labyrinth.epoch&limit=50"
    let query = try Client.validateLabyrinthSpaceRequest(
      did: did, method: "GET", nsid: nsid, path: prefix + did + suffix, body: nil)
    XCTAssertEqual(query?.first(where: { $0.name == "repo" })?.value, did)
    for path in [prefix + "did:plc:bob" + suffix,
                 prefix + did + suffix + "&repo=did:plc:bob",
                 prefix + did + suffix + "#fragment",
                 prefix.replacingOccurrences(of: "mailbox/self", with: "conversation/thread") + did + suffix] {
      XCTAssertThrowsError(try Client.validateLabyrinthSpaceRequest(
        did: did, method: "GET", nsid: nsid, path: path, body: nil))
    }
    XCTAssertThrowsError(try Client.validateLabyrinthSpaceRequest(
      did: did, method: "GET", nsid: "com.atproto.space.getRepo",
      path: "/xrpc/com.atproto.space.getRepo", body: nil))
  }

  func testOwnerSpaceWritesCannotEscapeMailboxCollections() throws {
    let did = "did:plc:alice"
    let nsid = "com.atproto.space.applyWrites"
    func body(collection: String, repo: String = "did:plc:alice") throws -> Data {
      try JSONSerialization.data(withJSONObject: [
        "space": "at://\(did)/space/blue.catbird.labyrinth.mailbox/self", "repo": repo,
        "writes": [["$type": "com.atproto.space.applyWrites#create", "collection": collection,
                    "rkey": "0", "value": ["$type": collection]]],
      ])
    }
    XCTAssertNoThrow(try Client.validateLabyrinthSpaceRequest(
      did: did, method: "POST", nsid: nsid, path: "/xrpc/\(nsid)",
      body: body(collection: "blue.catbird.labyrinth.epoch")))
    for data in [try body(collection: "app.bsky.feed.post"),
                 try body(collection: "blue.catbird.labyrinth.epoch", repo: "did:plc:bob")] {
      XCTAssertThrowsError(try Client.validateLabyrinthSpaceRequest(
        did: did, method: "POST", nsid: nsid, path: "/xrpc/\(nsid)", body: data))
    }
  }

  func testConversationReadsBindCanonicalGenerationAndBatchCapability() throws {
    let space = "at://did:plc:authority/space/blue.catbird.labyrinth.conversation/11111111-1111-4111-8111-111111111111"
    let nsid = "com.atproto.space.getRecord"
    let path = "/xrpc/\(nsid)?space=\(space)&repo=did:plc:bob&collection=blue.catbird.labyrinth.batch&rkey=immutable"
    let parsed = try XCTUnwrap(Client.conversationSpaceRequest(
      did: "did:plc:alice", method: "GET", nsid: nsid, path: path, body: nil))
    XCTAssertEqual(parsed.repo, "did:plc:bob")
    for bad in [path + "&repo=did:plc:alice", path + "&unexpected=1", path + "#fragment",
                path.replacingOccurrences(of: "blue.catbird.labyrinth.batch", with: "app.bsky.feed.post"),
                path.replacingOccurrences(of: "11111111-1111-4111-8111-111111111111", with: "old") ] {
      XCTAssertThrowsError(try Client.conversationSpaceRequest(
        did: "did:plc:alice", method: "GET", nsid: nsid, path: bad, body: nil))
    }
  }

  func testConversationCleanupCannotReadOrDeleteForeignOwnedRecords() throws {
    let space = "at://did:plc:authority/space/blue.catbird.labyrinth.conversation/11111111-1111-4111-8111-111111111111"
    for repo in ["did:plc:alice", "did:plc:bob"] {
      let nsid = "com.atproto.space.listRecords"
      let path = "/xrpc/\(nsid)?space=\(space)&repo=\(repo)&collection=blue.catbird.labyrinth.batch&limit=100&excludeValues=true"
      let body = try JSONSerialization.data(withJSONObject: ["space": space, "repo": repo,
        "collection": "blue.catbird.labyrinth.batch", "rkey": "owned"])
      if repo == "did:plc:alice" {
        XCTAssertNotNil(try Client.conversationSpaceRequest(
          did: repo, method: "GET", nsid: nsid, path: path, body: nil))
        XCTAssertNotNil(try Client.conversationSpaceRequest(did: repo, method: "POST",
          nsid: "com.atproto.space.deleteRecord", path: "/xrpc/com.atproto.space.deleteRecord", body: body))
      } else {
        XCTAssertThrowsError(try Client.conversationSpaceRequest(
          did: "did:plc:alice", method: "GET", nsid: nsid, path: path, body: nil))
        XCTAssertThrowsError(try Client.conversationSpaceRequest(did: "did:plc:alice", method: "POST",
          nsid: "com.atproto.space.deleteRecord", path: "/xrpc/com.atproto.space.deleteRecord", body: body))
      }
    }
  }

  func testConversationInventoryIsRestrictedToHistorySpaces() throws {
    let path = "/xrpc/com.atproto.space.listSpaces?spaceType=blue.catbird.labyrinth.conversation&limit=100"
    XCTAssertNoThrow(try Client.validateConversationSpaceInventory(method: "GET", path: path, body: nil))
    for bad in [path + "&did=did:plc:bob", path + "&spaceType=app.bsky.feed", path + "#fragment",
                path.replacingOccurrences(of: "blue.catbird.labyrinth.conversation", with: "app.bsky.feed")] {
      XCTAssertThrowsError(try Client.validateConversationSpaceInventory(method: "GET", path: bad, body: nil))
    }
  }

  func testSplitsServiceDIDIntoAudienceAndFragment() {
    let split = Client.splitServiceDID("did:web:chat.catbird.blue#atproto_mls")
    XCTAssertEqual(split.did, "did:web:chat.catbird.blue")
    XCTAssertEqual(split.fragment, "#atproto_mls")
    XCTAssertEqual(Client.splitServiceDID("did:web:ds.example").fragment, "#atproto_mls")
  }

  private func document(_ json: String) throws -> DIDDocument {
    try JSONDecoder().decode(DIDDocument.self, from: Data(json.utf8))
  }

  func testSelectsTheMLSServiceEndpoint() throws {
    let doc = try document("""
      {"id":"did:web:chat.catbird.blue","service":[
        {"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://pds.example"},
        {"id":"#atproto_mls","type":"AtprotoMlsDeliveryService","serviceEndpoint":"https://chat.catbird.blue"}]}
      """)
    let endpoint = try Client.labyrinthServiceEndpoint(
      in: doc, did: "did:web:chat.catbird.blue", fragment: "#atproto_mls")
    XCTAssertEqual(endpoint.absoluteString, "https://chat.catbird.blue")
  }

  func testRejectsMissingDuplicateOrInsecureServiceEndpoints() throws {
    let did = "did:web:chat.catbird.blue"
    let missing = try document(#"{"id":"did:web:chat.catbird.blue","service":[]}"#)
    let insecure = try document("""
      {"id":"did:web:chat.catbird.blue","service":[
        {"id":"#atproto_mls","type":"x","serviceEndpoint":"http://chat.catbird.blue"}]}
      """)
    let duplicate = try document("""
      {"id":"did:web:chat.catbird.blue","service":[
        {"id":"#atproto_mls","type":"x","serviceEndpoint":"https://a.example"},
        {"id":"did:web:chat.catbird.blue#atproto_mls","type":"x","serviceEndpoint":"https://b.example"}]}
      """)
    let wrongId = try document("""
      {"id":"did:web:other.example","service":[
        {"id":"#atproto_mls","type":"x","serviceEndpoint":"https://chat.catbird.blue"}]}
      """)
    for doc in [missing, insecure, duplicate, wrongId] {
      XCTAssertThrowsError(try Client.labyrinthServiceEndpoint(in: doc, did: did, fragment: "#atproto_mls"))
    }
  }

  // MARK: - getRecord mapping

  func testGetRecordReturnsTheValueObject() throws {
    let body = Data(#"{"uri":"at://did:plc:x/blue.catbird.labyrinth.mailboxKeys/self","cid":"c","value":{"$type":"blue.catbird.labyrinth.mailboxKeys","epoch":3}}"#.utf8)
    let value = try XCTUnwrap(Client.mailboxKeysValue(fromGetRecord: body, status: 200))
    let object = try XCTUnwrap(JSONSerialization.jsonObject(with: value) as? [String: Any])
    XCTAssertEqual(object["$type"] as? String, "blue.catbird.labyrinth.mailboxKeys")
    XCTAssertEqual(object["epoch"] as? Int, 3)
  }

  func testGetRecordMapsAbsentRecordsAndReposToNil() throws {
    XCTAssertNil(try Client.mailboxKeysValue(
      fromGetRecord: Data(#"{"error":"RecordNotFound","message":"x"}"#.utf8), status: 400))
    XCTAssertNil(try Client.mailboxKeysValue(
      fromGetRecord: Data(#"{"error":"RepoNotFound"}"#.utf8), status: 400))
  }

  func testGetRecordSurfacesOtherFailures() {
    XCTAssertThrowsError(try Client.mailboxKeysValue(
      fromGetRecord: Data(#"{"error":"InvalidRequest"}"#.utf8), status: 400))
    XCTAssertThrowsError(try Client.mailboxKeysValue(fromGetRecord: Data(), status: 500))
    XCTAssertThrowsError(try Client.mailboxKeysValue(fromGetRecord: Data("{}".utf8), status: 200))
  }
}

// MARK: - DID document cache fixtures

private final class DIDFetchCounter: @unchecked Sendable {
  private let lock = NSLock()
  private var counts: [String: Int] = [:]
  func record(_ did: String) { lock.lock(); counts[did, default: 0] += 1; lock.unlock() }
  func count(_ did: String) -> Int { lock.lock(); defer { lock.unlock() }; return counts[did, default: 0] }
}

private final class DIDCacheTestClock: @unchecked Sendable {
  private let lock = NSLock()
  private var current = Date(timeIntervalSince1970: 1_800_000_000)
  var now: Date { lock.lock(); defer { lock.unlock() }; return current }
  func advance(_ seconds: TimeInterval) { lock.lock(); current += seconds; lock.unlock() }
}

/// A P-256 Multikey from Petrel's KEM key tests.
private let labyrinthTestKEMMultibase = "zDnaembgSGUhZULN2Caob4HLJPaxBh92N7rtH21TErzqf8HQo"

private func labyrinthAuthorDocument(_ did: String) -> DIDDocument {
  DIDDocument(
    id: did,
    verificationMethod: [VerificationMethod(
      id: "#atproto_kem", type: "Multikey", controller: did, publicKeyMultibase: labyrinthTestKEMMultibase)],
    service: [Service(id: "#atproto_pds", type: "AtprotoPersonalDataServer", serviceEndpoint: "https://pds.example")])
}
