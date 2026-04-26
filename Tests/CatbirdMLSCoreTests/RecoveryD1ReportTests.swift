import Foundation
import XCTest

@testable import CatbirdMLSCore

/// Tests for Phase 2 Stage 3 trifecta detection (spec §8.6 / ADR-008 D1 —
/// `docs/superpowers/specs/2026-04-26-mls-auto-reset-phase2-design.md`).
///
/// The fast-path D1 dispatch fires a Mode B
/// (`group_state_unrecoverable`) report on the operationally-unrecoverable
/// trifecta:
///   1. Local FFI state absent (implicit at the EC convergence point —
///      Lifecycle deletes stale local state before EC runs)
///   2. Welcome fetch returned 200 with no welcome blob (sentinel
///      `MLSConversationManager.trifectaMissingWelcomeMarker`)
///   3. External Commit failed with HTTP 409 (epoch race) OR stale
///      GroupInfo
///
/// These tests exercise the pure helpers that classify each leg + the
/// pure threshold evaluator. Following the established package convention
/// (see `MLSRejoinReliabilityTests` / `MLSRecoveryStateMachineTests`),
/// we test the algorithm — not a mocked actor — to keep the test surface
/// fast and resilient to refactors of the recovery loop.
@available(iOS 18.0, macOS 13.0, *)
final class RecoveryD1ReportTests: XCTestCase {

  // MARK: - Pure threshold evaluator

  func testThirdHitWithinWindowDispatchesAndClearsWindow() {
    let now = Date()
    var window: [Date] = []

    // Hit #1
    let r1 = MLSConversationManager.evaluateTrifectaThreshold(
      window: window,
      now: now.addingTimeInterval(-300)
    )
    XCTAssertFalse(r1.shouldDispatch)
    XCTAssertEqual(r1.newWindow.count, 1)
    window = r1.newWindow

    // Hit #2
    let r2 = MLSConversationManager.evaluateTrifectaThreshold(
      window: window,
      now: now.addingTimeInterval(-150)
    )
    XCTAssertFalse(r2.shouldDispatch)
    XCTAssertEqual(r2.newWindow.count, 2)
    window = r2.newWindow

    // Hit #3 — should dispatch
    let r3 = MLSConversationManager.evaluateTrifectaThreshold(
      window: window,
      now: now
    )
    XCTAssertTrue(r3.shouldDispatch)
    XCTAssertEqual(r3.newWindow.count, 3)
  }

  func testHitsOutsideWindowAreEvictedAndDoNotCountTowardThreshold() {
    let now = Date()
    // Two stale entries (older than 600s window) and one fresh entry should
    // count as 1, not 3.
    let window: [Date] = [
      now.addingTimeInterval(-1200),  // expired
      now.addingTimeInterval(-700),   // expired
    ]

    let result = MLSConversationManager.evaluateTrifectaThreshold(window: window, now: now)
    XCTAssertFalse(result.shouldDispatch)
    XCTAssertEqual(result.newWindow.count, 1, "Stale entries must be pruned before append")
  }

  func testThresholdRespectsCustomParameters() {
    let now = Date()
    // Custom: threshold=2 within 60s — second hit should fire.
    let firstHit = MLSConversationManager.evaluateTrifectaThreshold(
      window: [],
      now: now.addingTimeInterval(-30),
      windowSeconds: 60,
      threshold: 2
    )
    XCTAssertFalse(firstHit.shouldDispatch)

    let secondHit = MLSConversationManager.evaluateTrifectaThreshold(
      window: firstHit.newWindow,
      now: now,
      windowSeconds: 60,
      threshold: 2
    )
    XCTAssertTrue(secondHit.shouldDispatch)
  }

  func testEmptyWindowSingleHitDoesNotDispatch() {
    let now = Date()
    let result = MLSConversationManager.evaluateTrifectaThreshold(window: [], now: now)
    XCTAssertFalse(result.shouldDispatch)
    XCTAssertEqual(result.newWindow, [now])
  }

  // MARK: - Welcome reason classification

  func testMissingWelcomeIn200IsTrifectaWelcomeReason() {
    XCTAssertTrue(
      MLSConversationManager.isTrifectaWelcomeReason(
        "Welcome unavailable (missing welcome in 200 response)"
      ),
      "The exact sentinel from classifyWelcomeRejoinFailure must be recognized"
    )
  }

  func testHttp404WelcomeFailureIsNotTrifectaWelcomeReason() {
    XCTAssertFalse(
      MLSConversationManager.isTrifectaWelcomeReason("Welcome unavailable (HTTP 404)"),
      "404 is a device-sync signal, not a missing-welcome bug"
    )
  }

  func testHttp410WelcomeFailureIsNotTrifectaWelcomeReason() {
    XCTAssertFalse(
      MLSConversationManager.isTrifectaWelcomeReason("Welcome expired (HTTP 410)")
    )
  }

  func testTransientWelcomeFailureIsNotTrifectaWelcomeReason() {
    XCTAssertFalse(
      MLSConversationManager.isTrifectaWelcomeReason("transient Welcome error (HTTP 503)")
    )
  }

  // MARK: - External Commit error classification

  func testHttp409EcErrorIsTrifectaEcError() {
    let err = MLSAPIError.httpError(statusCode: 409, message: "epoch mismatch")
    XCTAssertTrue(MLSConversationManager.isTrifectaExternalCommitError(err))
  }

  func testStaleGroupInfoEcErrorIsTrifectaEcError() {
    struct FakeError: LocalizedError {
      let errorDescription: String?
    }
    let stale = FakeError(errorDescription: "GroupInfo expired during external commit")
    XCTAssertTrue(MLSConversationManager.isTrifectaExternalCommitError(stale))
  }

  func testStaleKeywordEcErrorIsTrifectaEcError() {
    struct FakeError: LocalizedError {
      let errorDescription: String?
    }
    let stale = FakeError(errorDescription: "stale data on server")
    XCTAssertTrue(MLSConversationManager.isTrifectaExternalCommitError(stale))
  }

  func testHttp403EcErrorIsNotTrifectaEcError() {
    let err = MLSAPIError.httpError(statusCode: 403, message: "external commit forbidden")
    XCTAssertFalse(
      MLSConversationManager.isTrifectaExternalCommitError(err),
      "403 is the readdition path, distinct from the trifecta"
    )
  }

  func testHttp500EcErrorIsNotTrifectaEcError() {
    let err = MLSAPIError.httpError(statusCode: 500, message: "internal error")
    XCTAssertFalse(MLSConversationManager.isTrifectaExternalCommitError(err))
  }

  func testGenericTimeoutErrorIsNotTrifectaEcError() {
    struct FakeError: LocalizedError {
      let errorDescription: String?
    }
    let err = FakeError(errorDescription: "request timed out")
    XCTAssertFalse(MLSConversationManager.isTrifectaExternalCommitError(err))
  }

  // MARK: - Combined trifecta gating

  /// Partial trifecta: the Welcome arrived (404, not the 200-empty sentinel)
  /// but EC 409'd. Counter must NOT increment — different bug, different
  /// recovery policy.
  func testPartial404WelcomeAndEc409IsNotTrifecta() {
    let welcomeReason = "Welcome unavailable (HTTP 404)"
    let ecErr = MLSAPIError.httpError(statusCode: 409, message: "")

    let isCandidate = MLSConversationManager.isTrifectaWelcomeReason(welcomeReason)
    let isEcMatch = MLSConversationManager.isTrifectaExternalCommitError(ecErr)

    XCTAssertFalse(isCandidate, "404 disqualifies — that's device-sync, not missing-welcome")
    // EC error matches in isolation, but the Welcome leg gates the counter.
    XCTAssertTrue(isEcMatch)
    // Combined gate (mirrors the call-site check in
    // attemptRejoinWithWelcomeFallback): both must be true.
    XCTAssertFalse(isCandidate && isEcMatch)
  }

  /// Partial trifecta: missing-welcome sentinel was observed, but EC
  /// succeeded (or failed with a non-trifecta error). Counter must NOT
  /// increment.
  func testMissingWelcomeAndNonTrifectaEcFailureIsNotTrifecta() {
    let welcomeReason = "Welcome unavailable (missing welcome in 200 response)"
    let ecErr = MLSAPIError.httpError(statusCode: 500, message: "internal")

    let isCandidate = MLSConversationManager.isTrifectaWelcomeReason(welcomeReason)
    let isEcMatch = MLSConversationManager.isTrifectaExternalCommitError(ecErr)

    XCTAssertTrue(isCandidate)
    XCTAssertFalse(isEcMatch)
    XCTAssertFalse(isCandidate && isEcMatch)
  }

  /// Full trifecta: missing-welcome sentinel + EC 409. Both legs match,
  /// caller will call recordTrifectaFailure(convoId:).
  func testMissingWelcomeAndEc409IsFullTrifecta() {
    let welcomeReason = "Welcome unavailable (missing welcome in 200 response)"
    let ecErr = MLSAPIError.httpError(statusCode: 409, message: "epoch race")

    let isCandidate = MLSConversationManager.isTrifectaWelcomeReason(welcomeReason)
    let isEcMatch = MLSConversationManager.isTrifectaExternalCommitError(ecErr)

    XCTAssertTrue(isCandidate)
    XCTAssertTrue(isEcMatch)
    XCTAssertTrue(isCandidate && isEcMatch)
  }
}
