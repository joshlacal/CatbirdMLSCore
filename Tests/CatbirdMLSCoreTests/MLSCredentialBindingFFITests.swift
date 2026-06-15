import XCTest

@testable import CatbirdMLS
@testable import CatbirdMLSCore

final class MLSCredentialBindingFFITests: XCTestCase {
  private var ctx: MlsContext!
  private var tempStorageDir: URL!

  override func setUp() async throws {
    try await super.setUp()
    tempStorageDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSCredentialBindingFFITests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: tempStorageDir, withIntermediateDirectories: true)
    ctx = try MlsContext(
      storagePath: tempStorageDir.appendingPathComponent("ctx.db").path,
      encryptionKey: String(repeating: "cd", count: 32),
      keychain: InMemoryKeychainAccess()
    )
  }

  override func tearDown() async throws {
    if let ctx {
      try? ctx.flushAndPrepareClose()
    }
    ctx = nil
    if let tempStorageDir {
      try? FileManager.default.removeItem(at: tempStorageDir)
    }
    tempStorageDir = nil
    try await super.tearDown()
  }

  func testClassifiesMatchingKeyPackageIdentityAndSigningKey() throws {
    let package = try ctx.createKeyPackage(identityBytes: Data("did:plc:alice#device-1".utf8))

    let classification = MLSCredentialBinding.classifyKeyPackageBinding(
      expectedDID: "did:plc:alice",
      keyPackageData: package.keyPackageData,
      authorizedSignatureKeys: [package.signaturePublicKey]
    )

    XCTAssertEqual(classification.status, .verified)
    XCTAssertTrue(classification.identityMatches)
    XCTAssertEqual(classification.signingKeyMatches, true)
    XCTAssertEqual(classification.claimedIdentity, "did:plc:alice#device-1")
  }

  func testClassifiesKeyPackageIdentityMismatch() throws {
    let package = try ctx.createKeyPackage(identityBytes: Data("did:plc:alice#device-1".utf8))

    let classification = MLSCredentialBinding.classifyKeyPackageBinding(
      expectedDID: "did:plc:bob",
      keyPackageData: package.keyPackageData,
      authorizedSignatureKeys: [package.signaturePublicKey]
    )

    XCTAssertEqual(classification.status, .identityMismatch)
    XCTAssertFalse(classification.identityMatches)
    XCTAssertEqual(classification.signingKeyMatches, true)
    XCTAssertTrue(classification.reason?.contains("does not match expected DID") == true)
  }

  func testClassifiesKeyPackageSigningKeyMismatch() throws {
    let package = try ctx.createKeyPackage(identityBytes: Data("did:plc:alice#device-1".utf8))

    let classification = MLSCredentialBinding.classifyKeyPackageBinding(
      expectedDID: "did:plc:alice",
      keyPackageData: package.keyPackageData,
      authorizedSignatureKeys: [Data(repeating: 0x42, count: 32)]
    )

    XCTAssertEqual(classification.status, .signingKeyMismatch)
    XCTAssertTrue(classification.identityMatches)
    XCTAssertEqual(classification.signingKeyMatches, false)
  }

  func testDeviceRegistrationUsesKeyPackageLeafSignatureKey() throws {
    let package = try ctx.createKeyPackage(identityBytes: Data("did:plc:alice".utf8))
    let legacyDeviceKey = Data(repeating: 0x42, count: 32)

    let selectedKey = try MLSDeviceManager.registrationSignaturePublicKey(
      keyPackageSignatureKeys: [package.signaturePublicKey],
      fallbackSignaturePublicKey: legacyDeviceKey
    )

    XCTAssertEqual(selectedKey, package.signaturePublicKey)
    XCTAssertNotEqual(selectedKey, legacyDeviceKey)
  }

  func testDeviceRegistrationRejectsMixedKeyPackageLeafSignatureKeys() throws {
    let package = try ctx.createKeyPackage(identityBytes: Data("did:plc:alice".utf8))
    let otherKey = Data(repeating: 0x24, count: package.signaturePublicKey.count)

    XCTAssertThrowsError(
      try MLSDeviceManager.registrationSignaturePublicKey(
        keyPackageSignatureKeys: [package.signaturePublicKey, otherKey],
        fallbackSignaturePublicKey: Data(repeating: 0x42, count: 32)
      )
    )
  }
}
