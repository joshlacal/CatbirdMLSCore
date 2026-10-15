import Foundation
import Petrel
import XCTest
@testable import CatbirdMLSCore

final class MLSLabyrinthTransportTests: XCTestCase {
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

  // MARK: - Service resolution

  func testSplitsServiceDIDIntoAudienceAndFragment() {
    let split = Client.splitServiceDID("did:web:chat.catbird.blue#atproto_mls")
    XCTAssertEqual(split.did, "did:web:chat.catbird.blue")
    XCTAssertEqual(split.fragment, "#atproto_mls")
    XCTAssertEqual(Client.splitServiceDID("did:web:ds.example").fragment, "#atproto_mls")
  }

  func testServiceAuthAudienceKeepsTheServiceFragment() {
    XCTAssertEqual(
      Client.serviceAuthAudience("did:web:chat.catbird.blue#atproto_mls"),
      "did:web:chat.catbird.blue#atproto_mls")
    XCTAssertEqual(Client.serviceAuthAudience("did:web:ds.example"), "did:web:ds.example#atproto_mls")
  }

  // MARK: - Record listing

  func testListingFollowsTheCursorPastAnEmptyPage() throws {
    var seen = Set<String>()
    XCTAssertEqual(
      try Client.nextListRecordsCursor(["records": [[String: Any]](), "cursor": "page2"], seen: &seen), "page2")
    XCTAssertNil(try Client.nextListRecordsCursor(["records": [[String: Any]]()], seen: &seen))
    XCTAssertNil(try Client.nextListRecordsCursor(["records": [[String: Any]](), "cursor": ""], seen: &seen))
  }

  func testListingRefusesARepeatedCursor() throws {
    var seen = Set<String>()
    _ = try Client.nextListRecordsCursor(["cursor": "a"], seen: &seen)
    _ = try Client.nextListRecordsCursor(["cursor": "b"], seen: &seen)
    XCTAssertThrowsError(try Client.nextListRecordsCursor(["cursor": "a"], seen: &seen))
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
    let absent = [
      (400, #"{"error":"RecordNotFound","message":"x"}"#),
      // A PDS with no local account for the DID (deleted, deactivated, taken down).
      (400, #"{"error":"InvalidRequest","message":"Could not locate record"}"#),
      // The AppView a PDS pipes that request to.
      (400, #"{"error":"InvalidRequest","message":"Could not find repo: did:plc:x"}"#),
      (400, #"{"error":"RepoDeactivated"}"#),
      (404, ""),
    ]
    for (status, body) in absent {
      XCTAssertNil(try Client.mailboxKeysValue(fromGetRecord: Data(body.utf8), status: status), body)
    }
  }

  func testGetRecordSurfacesOtherFailures() {
    XCTAssertThrowsError(try Client.mailboxKeysValue(fromGetRecord: Data(), status: 500))
    XCTAssertThrowsError(try Client.mailboxKeysValue(
      fromGetRecord: Data(#"{"error":"RateLimitExceeded"}"#.utf8), status: 429))
    XCTAssertThrowsError(try Client.mailboxKeysValue(fromGetRecord: Data("{}".utf8), status: 200))
  }

  func testGetRecordURLKeepsThePDSBasePath() throws {
    let url = try XCTUnwrap(Client.mailboxKeysGetRecordURL(
      pds: try XCTUnwrap(URL(string: "https://pds.example/base/")), did: "did:plc:x", rkey: "self"))
    let components = try XCTUnwrap(URLComponents(url: url, resolvingAgainstBaseURL: false))
    XCTAssertEqual(components.host, "pds.example")
    XCTAssertEqual(components.path, "/base/xrpc/com.atproto.repo.getRecord")
    XCTAssertEqual(components.queryItems, [
      URLQueryItem(name: "repo", value: "did:plc:x"),
      URLQueryItem(name: "collection", value: "blue.catbird.labyrinth.mailboxKeys"),
      URLQueryItem(name: "rkey", value: "self"),
    ])
    XCTAssertNil(Client.mailboxKeysGetRecordURL(
      pds: try XCTUnwrap(URL(string: "http://pds.example")), did: "did:plc:x", rkey: "self"))
  }

  func testGoneOrPDSlessIdentitiesReadAsAbsent() {
    XCTAssertTrue(Client.isAbsentPDS(MLSPublicPDSReader.ReaderError.invalidEndpoint))
    XCTAssertTrue(Client.isAbsentPDS(SpaceHostResolutionError.networkError("HTTP status 404")))
    XCTAssertTrue(Client.isAbsentPDS(SpaceHostResolutionError.networkError("HTTP status 410")))
    XCTAssertFalse(Client.isAbsentPDS(SpaceHostResolutionError.networkError("HTTP status 503")))
    XCTAssertFalse(Client.isAbsentPDS(SpaceHostResolutionError.networkError("The request timed out.")))
    XCTAssertFalse(Client.isAbsentPDS(MLSPublicPDSReader.ReaderError.invalidResponse))
    XCTAssertFalse(Client.isAbsentPDS(URLError(.notConnectedToInternet)))
  }
}
