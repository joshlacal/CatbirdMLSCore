import XCTest
@testable import CatbirdMLSCore

final class MLSAPIClientGroupInfoVerificationTests: XCTestCase {

  func testDispositionVerifiesBytesWhenEpochsMatch() {
    let disposition = MLSAPIClient.groupInfoVerificationDisposition(
      uploadedEpoch: 12,
      storedEpoch: 12
    )

    XCTAssertEqual(disposition, .verifyStoredBytes)
  }

  func testDispositionAcceptsConcurrentAdvanceWhenServerIsAhead() {
    let disposition = MLSAPIClient.groupInfoVerificationDisposition(
      uploadedEpoch: 12,
      storedEpoch: 13
    )

    XCTAssertEqual(disposition, .acceptConcurrentAdvance(serverEpoch: 13))
  }

  func testDispositionRetriesWhenVerificationReadsStaleEpoch() {
    let disposition = MLSAPIClient.groupInfoVerificationDisposition(
      uploadedEpoch: 12,
      storedEpoch: 11
    )

    XCTAssertEqual(disposition, .retryStaleRead(serverEpoch: 11))
  }
}
