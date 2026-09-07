import Foundation
import CryptoKit
import XCTest

@testable import CatbirdMLSCore

final class MLSOrchestratorCredentialAdapterTests: XCTestCase {
  private var fakeKeychain: MLSKeychainFakeStorage!

  override func setUpWithError() throws {
    try super.setUpWithError()
    fakeKeychain = MLSKeychainFakeStorage()
    MLSKeychainManager.setFakeStorageOverrideForTesting(fakeKeychain)
  }

  override func tearDownWithError() throws {
    MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
    try super.tearDownWithError()
  }

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
    let adapter = MLSOrchestratorCredentialAdapter(
      signingBindingResolver: { did in
        did == identity
          ? .init(deviceId: "device-1", dpopJkt: "jkt-1", authGeneration: 3)
          : nil
      }
    )
    try adapter.storeSigningKey(userDid: identity, keyData: privateKey.rawRepresentation)
    defer { try? adapter.deleteSigningKey(userDid: identity) }

    let transcript = Data("clean-chat-transcript".utf8)
    let keyID = MLSOrchestratorCredentialAdapter.keyIdentifier(
      forPublicKey: privateKey.publicKey.rawRepresentation
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
    func value() -> Int {
      lock.lock()
      defer { lock.unlock() }
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
  func testDeviceUUIDCachingInvariants() throws {
    let aliceDid = "did:plc:alice-\(UUID().uuidString.lowercased())"
    let bobDid = "did:plc:bob-\(UUID().uuidString.lowercased())"
    let aliceUUID1 = UUID().uuidString.lowercased()
    let aliceUUID2 = UUID().uuidString.lowercased()
    let bobUUID1 = UUID().uuidString.lowercased()

    let cache = MLSDeviceUUIDCache()
    let generationBox = TestAtomicCounter()
    _ = generationBox.increment() // currentGen = 1

    let adapter = MLSOrchestratorCredentialAdapter(
      deviceUuidCache: cache,
      generationProvider: { generationBox.value() }
    )
    // 1. Initial store and resolution for Alice and Bob
    try adapter.storeDeviceUuid(userDid: aliceDid, uuid: aliceUUID1)
    try adapter.storeDeviceUuid(userDid: bobDid, uuid: bobUUID1)


    // Both accounts should resolve their own stored UUIDs
    let resolvedAlice1 = try adapter.getDeviceUuid(userDid: aliceDid)
    XCTAssertEqual(resolvedAlice1, aliceUUID1)

    let resolvedBob1 = try adapter.getDeviceUuid(userDid: bobDid)
    XCTAssertEqual(resolvedBob1, bobUUID1)
    // 2. Cache hit invariant: delete from underlying keychain, repeated resolution still succeeds
    let aliceKey = MLSStoragePaths.deviceUuidAccount(for: aliceDid)
    try MLSKeychainManager.shared.deleteStrict(forKey: aliceKey)
    XCTAssertNil(try MLSKeychainManager.shared.retrieveKeyStrict(forKey: aliceKey))

    let cachedAlice = try adapter.getDeviceUuid(userDid: aliceDid)
    XCTAssertEqual(cachedAlice, aliceUUID1, "Repeated resolution MUST hit the in-memory cache without keychain read")

    // 3. Per-account isolation invariant: Bob's resolution never returns Alice's value
    let cachedBob = try adapter.getDeviceUuid(userDid: bobDid)
    XCTAssertEqual(cachedBob, bobUUID1, "Account resolution MUST be isolated by user DID and never return other account's value")
    XCTAssertNotEqual(cachedBob, cachedAlice)

    // 4. Generation change invariant: changing generation forces fresh resolution from keychain
    // Re-populate Alice in keychain with a new UUID
    let aliceData2 = try XCTUnwrap(aliceUUID2.data(using: .utf8))
    _ = try MLSKeychainManager.shared.storeOrAdoptImmutableKey(aliceData2, forKey: aliceKey)

    // Bump generation
    let newGen = generationBox.increment()
    let adapterNewGen = MLSOrchestratorCredentialAdapter(
      deviceUuidCache: cache,
      generationProvider: { newGen }
    )

    let resolvedAliceGen2 = try adapterNewGen.getDeviceUuid(userDid: aliceDid)
    XCTAssertEqual(resolvedAliceGen2, aliceUUID2, "Generation change MUST force fresh resolution and purge stale cached value")

    // 5. Account switch / invalidation invariant
    cache.invalidate(userDid: aliceDid)
    XCTAssertNil(cache.entry(for: aliceDid), "Explicit invalidation for user DID must purge that account's entry")
    XCTAssertNotNil(cache.entry(for: bobDid), "Invalidating one account must not invalidate another account")

    cache.invalidate()
    XCTAssertNil(cache.entry(for: bobDid), "Invalidate all must clear all entries")
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
