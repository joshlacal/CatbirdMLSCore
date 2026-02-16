import XCTest
@testable import CatbirdMLSCore

final class MLSRejoinReliabilityTests: XCTestCase {
  func testRejoinGateSuppressesConcurrentAttempts() {
    let decision = MLSConversationManager.evaluateRejoinGate(
      now: Date(),
      lastAttempt: nil,
      cooldownSeconds: 60,
      isRejoinInProgress: true,
      activeConversationID: "active-convo"
    )

    XCTAssertEqual(decision, .inProgress(activeConversationID: "active-convo"))
  }

  func testRejoinGateAppliesCooldownForRapidRetry() {
    let now = Date()
    let decision = MLSConversationManager.evaluateRejoinGate(
      now: now,
      lastAttempt: now.addingTimeInterval(-5),
      cooldownSeconds: 60,
      isRejoinInProgress: false,
      activeConversationID: nil
    )

    guard case .cooldown(let remainingSeconds) = decision else {
      return XCTFail("Expected cooldown suppression")
    }

    XCTAssertGreaterThanOrEqual(remainingSeconds, 54)
    XCTAssertLessThanOrEqual(remainingSeconds, 55)
  }

  func testRejoinGateAllowsSyncPathAfterCooldownExpires() {
    let now = Date()
    let decision = MLSConversationManager.evaluateRejoinGate(
      now: now,
      lastAttempt: now.addingTimeInterval(-120),
      cooldownSeconds: 60,
      isRejoinInProgress: false,
      activeConversationID: nil
    )

    XCTAssertEqual(decision, .allow)
  }
}

final class MLSRecoveryBackoffReliabilityTests: XCTestCase {
  func testRecoveryBackoffSchedule() {
    XCTAssertEqual(MLSRecoveryManager.cooldownForAttempts(0), 0)
    XCTAssertEqual(MLSRecoveryManager.cooldownForAttempts(1), 30)
    XCTAssertEqual(MLSRecoveryManager.cooldownForAttempts(2), 120)
    XCTAssertEqual(MLSRecoveryManager.cooldownForAttempts(3), 600)
    XCTAssertEqual(MLSRecoveryManager.cooldownForAttempts(4), 3600)
  }

  func testRecoveryBackoffGatesImmediateRetry() {
    let now = Date()
    let inCooldown = MLSRecoveryManager.isInBackoffCooldown(
      attempts: 2,
      lastAttempt: now.addingTimeInterval(-10),
      now: now
    )

    XCTAssertTrue(inCooldown)
  }

  func testRecoveryBackoffAllowsRetryAfterWindow() {
    let now = Date()
    let inCooldown = MLSRecoveryManager.isInBackoffCooldown(
      attempts: 1,
      lastAttempt: now.addingTimeInterval(-35),
      now: now
    )

    XCTAssertFalse(inCooldown)
  }
}
