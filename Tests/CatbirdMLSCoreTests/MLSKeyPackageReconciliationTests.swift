import XCTest
@testable import CatbirdMLSCore

final class MLSKeyPackageReconciliationTests: XCTestCase {

  func testConsumedWelcomeHashConfirmedByReconcileIsNotEvicted() {
    let toEvict = MLSClient.localKeyPackageHashesToEvict(
      localHashes: ["available-hash", "consumed-welcome-hash"],
      legacyServerHashes: ["available-hash"],
      reconcileLocalOnly: []
    )

    XCTAssertEqual(toEvict, [])
  }

  func testFallsBackToLegacyServerHashesWhenReconcileUnavailable() {
    let toEvict = MLSClient.localKeyPackageHashesToEvict(
      localHashes: ["available-hash", "stale-hash"],
      legacyServerHashes: ["available-hash"],
      reconcileLocalOnly: nil
    )

    XCTAssertEqual(toEvict, ["stale-hash"])
  }

  func testUnverifiedDeviceDoesNotEvictLocalPackages() {
    let toEvict = MLSClient.localKeyPackageHashesToEvict(
      localHashes: ["local-a", "local-b"],
      legacyServerHashes: [],
      reconcileLocalOnly: ["local-a", "local-b"],
      reconcileDeviceVerified: false
    )

    XCTAssertEqual(toEvict, [])
  }
}
