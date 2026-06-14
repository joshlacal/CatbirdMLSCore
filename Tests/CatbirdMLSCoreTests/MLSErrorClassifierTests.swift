import XCTest

@testable import CatbirdMLS
@testable import CatbirdMLSCore

final class MLSErrorClassifierTests: XCTestCase {
  func testInvalidCommitIsPeerBadWhenLocalEpochIsNotBehind() {
    let classification = MLSErrorClassifier.classifyPeerBad(
      MlsError.InvalidCommit(message: "Invalid commit"),
      localEpoch: 7,
      messageEpoch: 7
    )

    XCTAssertTrue(classification.peerBad)
    XCTAssertTrue(classification.quarantineTriggerEligible)
    XCTAssertFalse(classification.wrongEpoch)
  }

  func testInvalidCommitIsNotPeerBadWhenLocalEpochIsBehind() {
    let classification = MLSErrorClassifier.classifyPeerBad(
      MlsError.InvalidCommit(message: "Invalid commit"),
      localEpoch: 6,
      messageEpoch: 7
    )

    XCTAssertFalse(classification.peerBad)
    XCTAssertFalse(classification.quarantineTriggerEligible)
  }

  func testWrongEpochIsNotPeerBad() {
    let classification = MLSErrorClassifier.classifyPeerBad(
      MlsError.OpenMls(message: "ValidationError(WrongEpoch)"),
      localEpoch: 7,
      messageEpoch: 7
    )

    XCTAssertFalse(classification.peerBad)
    XCTAssertFalse(classification.quarantineTriggerEligible)
    XCTAssertTrue(classification.wrongEpoch)
  }
}
