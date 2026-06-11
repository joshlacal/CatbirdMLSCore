import XCTest

@testable import CatbirdMLSCore

/// N44e: Swift twin of the Rust `credential_binding` unit tests
/// (`catbird-mls/src/orchestrator/credential_binding.rs`) — the two
/// implementations must agree on DID-root extraction and comparison rules.
final class MLSCredentialBindingTests: XCTestCase {

  func testRootDIDBare() {
    XCTAssertEqual(
      MLSCredentialBinding.credentialRootDID("did:plc:alice"), "did:plc:alice")
  }

  func testRootDIDWithDeviceFragment() {
    XCTAssertEqual(
      MLSCredentialBinding.credentialRootDID("did:plc:alice#device-1"), "did:plc:alice")
  }

  func testRootDIDEmptyFragmentRoot() {
    XCTAssertEqual(MLSCredentialBinding.credentialRootDID("#device-1"), "")
  }

  func testClaimCheckMatchesBareDID() {
    XCTAssertEqual(
      MLSCredentialBinding.checkIdentityClaim(
        expectedDID: "did:plc:alice", claimedIdentity: "did:plc:alice"),
      .verified)
  }

  func testClaimCheckMatchesDeviceRootedIdentity() {
    XCTAssertEqual(
      MLSCredentialBinding.checkIdentityClaim(
        expectedDID: "did:plc:alice", claimedIdentity: "did:plc:alice#device-1"),
      .verified)
  }

  func testClaimCheckIsCaseInsensitive() {
    XCTAssertEqual(
      MLSCredentialBinding.checkIdentityClaim(
        expectedDID: "did:web:Example.Com", claimedIdentity: "did:web:example.com"),
      .verified)
  }

  func testClaimCheckRejectsMismatchedRoot() {
    let result = MLSCredentialBinding.checkIdentityClaim(
      expectedDID: "did:plc:alice", claimedIdentity: "did:plc:mallory#device-1")
    XCTAssertEqual(
      result,
      .didMismatch(
        expectedDID: "did:plc:alice",
        claimedIdentity: "did:plc:mallory#device-1",
        claimedRootDID: "did:plc:mallory"
      ))
  }

  func testClaimCheckRejectsEmptyIdentity() {
    let result = MLSCredentialBinding.checkIdentityClaim(
      expectedDID: "did:plc:alice", claimedIdentity: "")
    guard case .didMismatch = result else {
      return XCTFail("expected didMismatch, got \(result)")
    }
  }
}
