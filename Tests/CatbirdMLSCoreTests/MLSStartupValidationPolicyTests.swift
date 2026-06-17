import XCTest

@testable import CatbirdMLSCore

final class MLSStartupValidationPolicyTests: XCTestCase {
  func testMassGroupNotFoundDefersDestructiveRecovery() {
    let decision = MLSStartupValidationPolicy.decision(
      totalConversations: 44,
      validatedCount: 1,
      corruptedCount: 43
    )

    XCTAssertEqual(decision, .deferDestructiveRecovery)
  }

  func testSmallNumberOfCorruptGroupsCanBeMarkedForRejoin() {
    let decision = MLSStartupValidationPolicy.decision(
      totalConversations: 12,
      validatedCount: 10,
      corruptedCount: 2
    )

    XCTAssertEqual(decision, .markCorruptedGroupsForRejoin)
  }

  func testThresholdSizedTotalFailureDefersDestructiveRecovery() {
    let decision = MLSStartupValidationPolicy.decision(
      totalConversations: 5,
      validatedCount: 0,
      corruptedCount: 5
    )

    XCTAssertEqual(decision, .deferDestructiveRecovery)
  }

  func testBelowThresholdTotalFailureCanBeMarkedForRejoin() {
    let decision = MLSStartupValidationPolicy.decision(
      totalConversations: 4,
      validatedCount: 0,
      corruptedCount: 4
    )

    XCTAssertEqual(decision, .markCorruptedGroupsForRejoin)
  }

  func testEmptyValidationDoesNothing() {
    let decision = MLSStartupValidationPolicy.decision(
      totalConversations: 0,
      validatedCount: 0,
      corruptedCount: 0
    )

    XCTAssertEqual(decision, .noCorruption)
  }
}
