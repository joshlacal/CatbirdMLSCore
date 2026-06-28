import Foundation
import Testing
@testable import CatbirdMLSCore

/// W6: foreign-repo device-record reads must hit the target DID's hosting PDS directly
/// (unauthenticated), not the BFF — which only routes to the session user's PDS and returns
/// 400 "Could not find repo" for a foreign repo. These cover the pure, device-free units:
/// URL construction and the listRecords JSON → signature-key decode.
struct MLSPublicPDSReaderTests {

  // MARK: deviceRecordsListURL

  @Test func buildsListRecordsURLForHostingPDS() throws {
    let pds = URL(string: "https://pds.example.com")!
    let url = try #require(
      MLSPublicPDSReader.deviceRecordsListURL(pds: pds, did: "did:plc:abc123")
    )
    let comps = try #require(URLComponents(url: url, resolvingAgainstBaseURL: false))
    #expect(comps.scheme == "https")
    #expect(comps.host == "pds.example.com")
    #expect(comps.path == "/xrpc/com.atproto.repo.listRecords")
    let items = Dictionary(uniqueKeysWithValues: (comps.queryItems ?? []).map { ($0.name, $0.value) })
    #expect(items["repo"] == "did:plc:abc123")
    #expect(items["collection"] == "blue.catbird.mlsChat.device")
    #expect(items["limit"] == "50")
  }

  @Test func rejectsNonHTTPSEndpoint() {
    // Defense against downgraded/SSRF endpoints — never read records over plaintext.
    let pds = URL(string: "http://pds.example.com")!
    #expect(MLSPublicPDSReader.deviceRecordsListURL(pds: pds, did: "did:plc:abc") == nil)
  }

  @Test func handlesPDSEndpointWithTrailingSlash() throws {
    let pds = URL(string: "https://pds.example.com/")!
    let url = try #require(
      MLSPublicPDSReader.deviceRecordsListURL(pds: pds, did: "did:plc:abc")
    )
    #expect(url.path == "/xrpc/com.atproto.repo.listRecords")
  }

  // MARK: decodeDeviceSignatureKeys

  @Test func decodesDeviceSignatureKeysFromListRecordsJSON() {
    // "AQID" base64 == bytes [0x01, 0x02, 0x03]
    let json = """
    {"records":[
      {"uri":"at://did:plc:abc/blue.catbird.mlsChat.device/3k1",
       "cid":"bafyreiabc",
       "value":{"$type":"blue.catbird.mlsChat.device",
                "mlsSignaturePublicKey":{"$bytes":"AQID"},
                "algorithm":"ed25519",
                "createdAt":"2026-06-27T00:00:00.000Z"}}
    ]}
    """.data(using: .utf8)!

    let keys = MLSPublicPDSReader.decodeDeviceSignatureKeys(fromListRecordsJSON: json)
    #expect(keys.count == 1)
    #expect(keys.first == Data([0x01, 0x02, 0x03]))
  }

  @Test func skipsMalformedRecordsButKeepsValidOnes() {
    let json = """
    {"records":[
      {"uri":"at://did:plc:abc/blue.catbird.mlsChat.device/bad",
       "value":{"$type":"app.bsky.feed.post","text":"not a device record"}},
      {"uri":"at://did:plc:abc/blue.catbird.mlsChat.device/good",
       "value":{"$type":"blue.catbird.mlsChat.device",
                "mlsSignaturePublicKey":{"$bytes":"BAUG"},
                "algorithm":"ed25519",
                "createdAt":"2026-06-27T00:00:00.000Z"}}
    ]}
    """.data(using: .utf8)!

    let keys = MLSPublicPDSReader.decodeDeviceSignatureKeys(fromListRecordsJSON: json)
    // "BAUG" base64 == [0x04, 0x05, 0x06]
    #expect(keys == [Data([0x04, 0x05, 0x06])])
  }

  @Test func returnsEmptyForNoRecordsOrGarbage() {
    let empty = #"{"records":[]}"#.data(using: .utf8)!
    #expect(MLSPublicPDSReader.decodeDeviceSignatureKeys(fromListRecordsJSON: empty).isEmpty)

    let garbage = "not json".data(using: .utf8)!
    #expect(MLSPublicPDSReader.decodeDeviceSignatureKeys(fromListRecordsJSON: garbage).isEmpty)
  }
}
