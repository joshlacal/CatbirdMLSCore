@testable import CatbirdMLSCore
import Foundation
import Testing

/// `com.atproto.identity.resolveDid` is admitted only as a GET to the
/// signed-in account's own PDS origin.
@Suite("MLSOutboundHTTPValidator own-PDS resolveDid")
struct MLSOutboundOwnPDSResolveDidTests {
  private let nsid = "com.atproto.identity.resolveDid"
  private let ownPDS = URL(string: "https://pds.catbird.blue")!

  private func url(_ raw: String) -> URL { URL(string: raw)! }

  @Test func directsPersonalProofToTheAccountPDS() throws {
    let did = "did:plc:fixture"
    let gateway = url("https://gateway.example")
    let source = url("https://gateway.example/xrpc/com.atproto.space.getRecord?repo=did%3Aplc%3Afixture&space=test")
    var request = URLRequest(url: source)
    request.httpMethod = "GET"
    request.setValue(did, forHTTPHeaderField: "Atproto-Space-Audience")
    let direct = try PetrelPlatformTransport.personalSpaceReadURL(for: request, accountDID: did, sessionOrigin: gateway, pds: ownPDS)
    #expect(direct.absoluteString == "https://pds.catbird.blue/xrpc/com.atproto.space.getRecord?repo=did%3Aplc%3Afixture&space=test")
    #expect(throws: (any Error).self) {
      try PetrelPlatformTransport.personalSpaceReadURL(for: request, accountDID: "did:plc:other", sessionOrigin: gateway, pds: ownPDS)
    }
  }

  @Test func homeServiceAuthUsesExactBackupOrChatMethod() {
    let home = url("https://delivery.example")
    for method in ["blue.catbird.chat.getOwnDevices", "blue.catbird.labyrinth.getRecoveryShareCapabilities",
                   "blue.catbird.labyrinth.deleteAccountData"] {
      let request = home.appendingPathComponent("xrpc").appendingPathComponent(method)
      #expect(PetrelPlatformTransport.deliveryServiceMethod(for: request, home: home) == method)
      #expect(PetrelPlatformTransport.deliveryServiceMethod(for: request, home: ownPDS) == nil)
    }
    #expect(PetrelPlatformTransport.deliveryServiceMethod(
      for: home.appendingPathComponent("xrpc/com.atproto.repo.getRecord"), home: home) == nil)
  }

  @Test func resolveDidToOwnPDSIsAllowed() throws {
    try MLSOutboundHTTPValidator.validatePreparedRequestNSID(
      nsid,
      method: "GET",
      requestURL: url("https://pds.catbird.blue/xrpc/com.atproto.identity.resolveDid?did=did%3Aplc%3Aabc"),
      ownPDSOrigin: ownPDS
    )
    // Explicit default port and a base URL with a path are still the same origin.
    try MLSOutboundHTTPValidator.validatePreparedRequestNSID(
      nsid,
      method: "get",
      requestURL: url("https://PDS.catbird.blue:443/xrpc/com.atproto.identity.resolveDid?did=did%3Aplc%3Aabc"),
      ownPDSOrigin: url("https://pds.catbird.blue/some/base")
    )
  }

  @Test func resolveDidToForeignHostIsRejected() {
    for raw in [
      "https://evil.example.com/xrpc/com.atproto.identity.resolveDid?did=did%3Aplc%3Aabc",
      "https://pds.catbird.blue.evil.example/xrpc/com.atproto.identity.resolveDid",
      "https://catbird.blue/xrpc/com.atproto.identity.resolveDid",
      "https://pds.catbird.blue:8443/xrpc/com.atproto.identity.resolveDid",
      "http://pds.catbird.blue/xrpc/com.atproto.identity.resolveDid",
    ] {
      #expect(throws: MLSOutboundHTTPValidator.ValidationError.disallowedNSID(nsid)) {
        try MLSOutboundHTTPValidator.validatePreparedRequestNSID(
          nsid, method: "GET", requestURL: url(raw), ownPDSOrigin: ownPDS
        )
      }
    }
  }

  @Test func resolveDidWithoutKnownOwnPDSOrNonGetIsRejected() {
    let target = url("https://pds.catbird.blue/xrpc/com.atproto.identity.resolveDid")
    #expect(throws: MLSOutboundHTTPValidator.ValidationError.disallowedNSID(nsid)) {
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID(
        nsid, method: "GET", requestURL: target, ownPDSOrigin: nil
      )
    }
    #expect(throws: MLSOutboundHTTPValidator.ValidationError.disallowedNSID(nsid)) {
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID(
        nsid, method: "POST", requestURL: target, ownPDSOrigin: ownPDS
      )
    }
  }

  /// The route-blind check (used for every other prepared request) still
  /// rejects resolveDid, and the route-aware check does not widen anything else.
  @Test func routeBlindCheckStillRejectsResolveDid() throws {
    #expect(throws: MLSOutboundHTTPValidator.ValidationError.disallowedNSID(nsid)) {
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID(nsid)
    }
    let other = "com.atproto.identity.updateHandle"
    #expect(throws: MLSOutboundHTTPValidator.ValidationError.disallowedNSID(other)) {
      try MLSOutboundHTTPValidator.validatePreparedRequestNSID(
        other,
        method: "GET",
        requestURL: url("https://pds.catbird.blue/xrpc/\(other)"),
        ownPDSOrigin: ownPDS
      )
    }
    try MLSOutboundHTTPValidator.validatePreparedRequestNSID(
      "com.atproto.repo.getRecord",
      method: "GET",
      requestURL: url("https://elsewhere.example.com/xrpc/com.atproto.repo.getRecord"),
      ownPDSOrigin: ownPDS
    )
  }
}
