import XCTest
@testable import CatbirdMLSCore

final class WelcomeRecoveryDecisionTests: XCTestCase {
  func testNoMatchingKeyPackageRequestsReissueBeforeLimit() {
    let decision = WelcomeRecoveryPolicy.decide(
      welcomeFailure: .noMatchingKeyPackage,
      priorReissueAttempts: 2,
      hasCurrentAdmin: true,
      groupInfoUnrecoverable: false,
      externalCommitAlreadyFailed: false,
      lastSeenEpoch: 7
    )

    XCTAssertEqual(
      decision,
      .requestReissue(reason: "NoMatchingKeyPackage", nextAttempt: 3)
    )
  }

  func testNoMatchingKeyPackageEscalatesToExternalCommitAfterThreeAttempts() {
    let decision = WelcomeRecoveryPolicy.decide(
      welcomeFailure: .noMatchingKeyPackage,
      priorReissueAttempts: 3,
      hasCurrentAdmin: true,
      groupInfoUnrecoverable: false,
      externalCommitAlreadyFailed: false,
      lastSeenEpoch: 9
    )

    XCTAssertEqual(decision, .externalCommitWithHistoryGap(lastSeenEpoch: 9))
  }

  func testNoMatchingKeyPackageEscalatesToExternalCommitWhenNoAdminCanReissue() {
    let decision = WelcomeRecoveryPolicy.decide(
      welcomeFailure: .noMatchingKeyPackage,
      priorReissueAttempts: 0,
      hasCurrentAdmin: false,
      groupInfoUnrecoverable: false,
      externalCommitAlreadyFailed: false,
      lastSeenEpoch: 11
    )

    XCTAssertEqual(decision, .externalCommitWithHistoryGap(lastSeenEpoch: 11))
  }

  func testUnrecoverableGroupInfoSurrendersInsteadOfExternalCommitLoop() {
    let decision = WelcomeRecoveryPolicy.decide(
      welcomeFailure: .welcomeExpired,
      priorReissueAttempts: 3,
      hasCurrentAdmin: true,
      groupInfoUnrecoverable: true,
      externalCommitAlreadyFailed: false,
      lastSeenEpoch: 12
    )

    XCTAssertEqual(
      decision,
      .surrender(reason: "GroupInfo unrecoverable", retryAfter: nil)
    )
  }

  func testExternalCommitFailureAfterExhaustedReissueSurrenders() {
    let decision = WelcomeRecoveryPolicy.decide(
      welcomeFailure: .noMatchingKeyPackage,
      priorReissueAttempts: 3,
      hasCurrentAdmin: true,
      groupInfoUnrecoverable: false,
      externalCommitAlreadyFailed: true,
      lastSeenEpoch: 13
    )

    XCTAssertEqual(
      decision,
      .surrender(reason: "External Commit recovery failed", retryAfter: nil)
    )
  }
}
