import Foundation
import CryptoKit
import XCTest

@testable import CatbirdMLSCore

final class MLSOrchestratorCredentialAdapterTests: XCTestCase {
  func testAuthorizedDeviceKeysReturnsNilWhenNoResolverIsConfigured() throws {
    let adapter = MLSOrchestratorCredentialAdapter()

    XCTAssertNil(try adapter.getAuthorizedDeviceKeys(userDid: "did:plc:alice"))
  }

  func testAuthorizedDeviceKeysUsesSynchronousResolver() throws {
    let authorizedKey = Data([0x01, 0x02, 0x03])
    let adapter = MLSOrchestratorCredentialAdapter { userDid in
      userDid == "did:plc:alice" ? [authorizedKey] : []
    }

    XCTAssertEqual(
      try adapter.getAuthorizedDeviceKeys(userDid: "did:plc:alice"),
      [authorizedKey]
    )
    XCTAssertEqual(
      try adapter.getAuthorizedDeviceKeys(userDid: "did:plc:bob"),
      []
    )
  }

  func testCleanChatTranscriptUsesDeviceSignerWithoutReturningPrivateKey() throws {
    let identity = "did:plc:signing-test-\(UUID().uuidString.lowercased())"
    let privateKey = Curve25519.Signing.PrivateKey()
    try MLSKeychain.storeSignatureKey(privateKey.rawRepresentation, forIdentity: identity)
    defer { try? MLSKeychain.deleteSignatureKey(forIdentity: identity) }

    let transcript = Data("clean-chat-transcript".utf8)
    let keyID = MLSOrchestratorCredentialAdapter.keyIdentifier(
      forPublicKey: privateKey.publicKey.rawRepresentation
    )
    let adapter = MLSOrchestratorCredentialAdapter(
      signingBindingResolver: { did in
        did == identity
          ? .init(deviceId: "device-1", dpopJkt: "jkt-1", authGeneration: 3)
          : nil
      }
    )

    let authority = try XCTUnwrap(
      try adapter.signCleanChatTranscript(
        userDid: identity,
        transcript: transcript,
        keyId: keyID
      )
    )

    XCTAssertEqual(authority.publicKey, privateKey.publicKey.rawRepresentation)
    let publicKey = try Curve25519.Signing.PublicKey(rawRepresentation: authority.publicKey)
    XCTAssertTrue(publicKey.isValidSignature(authority.signature, for: transcript))
    XCTAssertNotEqual(authority.signature, privateKey.rawRepresentation)
    XCTAssertEqual(authority.deviceId, "device-1")
    XCTAssertEqual(authority.dpopJkt, "jkt-1")
    XCTAssertEqual(authority.authGeneration, 3)
  }
}
