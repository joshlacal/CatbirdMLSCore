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
    XCTAssertEqual(authority.authGeneration, 3)
  }

  func testCleanChatSignerFailsClosedAcrossAuthorityRotationDuringSigning() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/Callbacks/MLSOrchestratorCredentialAdapter.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(signature: "public func signCleanChatTranscript(", from: source)
    )

    let bindingBefore = try XCTUnwrap(
      body.range(of: "bindingBeforeSignature = signingBindingResolver?(userDid)")
    )
    let publicKeyBefore = try XCTUnwrap(
      body.range(of: "let publicKeyBeforeSignature = signingPublicKeyResolver?(userDid)")
    )
    let signer = try XCTUnwrap(body.range(of: "signature = try transcriptSigner(userDid, transcript)"))
    let publicKeyAfter = try XCTUnwrap(
      body.range(of: "let publicKeyAfterSignature = signingPublicKeyResolver?(userDid)")
    )
    let bindingAfter = try XCTUnwrap(
      body.range(of: "bindingAfterSignature = signingBindingResolver?(userDid)")
    )

    XCTAssertLessThan(bindingBefore.lowerBound, signer.lowerBound)
    XCTAssertLessThan(publicKeyBefore.lowerBound, signer.lowerBound)
    XCTAssertLessThan(signer.lowerBound, publicKeyAfter.lowerBound)
    XCTAssertLessThan(publicKeyAfter.lowerBound, bindingAfter.lowerBound)
    XCTAssertTrue(body.contains("publicKeyAfterSignature == publicKeyBeforeSignature"))
    XCTAssertTrue(body.contains("bindingAfterSignature == bindingBeforeSignature"))
    XCTAssertTrue(body.contains("isValidSignature(signature, for: transcript)"))
  }
  private final class TestAtomicCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var _count = 0
    func increment() -> Int {
      lock.lock()
      defer { lock.unlock() }
      _count += 1
      return _count
    }
  }

  func testAtomicAuthoritySnapshotRotationRejectsWholeSignature() throws {
    let identity = "did:plc:atomic-\(UUID().uuidString.lowercased())"
    let key = Curve25519.Signing.PrivateKey()
    let counter = TestAtomicCounter()
    let adapter = MLSOrchestratorCredentialAdapter(
      signingAuthorityResolver: { did in
        let calls = counter.increment()
        guard did == identity else { return nil }
        return .init(
          actorDid: did,
          deviceId: "device-1",
          dpopJkt: "jkt-1",
          authGeneration: calls == 1 ? 1 : 2,
          signerHandle: "signer-v\(calls)",
          publicKey: key.publicKey.rawRepresentation,
          signer: { _, payload in try key.signature(for: payload) }
        )
      }
    )

    XCTAssertNil(
      try adapter.signCleanChatTranscript(
        userDid: identity,
        transcript: Data("atomic".utf8),
        keyId: MLSOrchestratorCredentialAdapter.keyIdentifier(
          forPublicKey: key.publicKey.rawRepresentation
        )
      )
    )
  }

  private func sourceFileURL(relativePath: String) -> URL {
    let testsDirectory = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
    let packageRoot = testsDirectory.deletingLastPathComponent().deletingLastPathComponent()
    return packageRoot.appendingPathComponent(relativePath)
  }

  private func extractFunctionBody(signature: String, from source: String) -> String? {
    guard let signatureRange = source.range(of: signature),
          let bodyStart = source[signatureRange.upperBound...].firstIndex(of: "{")
    else {
      return nil
    }

    var depth = 0
    var currentIndex = bodyStart
    while currentIndex < source.endIndex {
      let character = source[currentIndex]
      if character == "{" {
        depth += 1
      } else if character == "}" {
        depth -= 1
        if depth == 0 {
          return String(source[bodyStart...currentIndex])
        }
      }
      currentIndex = source.index(after: currentIndex)
    }
    return nil
  }
}
