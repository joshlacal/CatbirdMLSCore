//
//  MLSRecoveryManagerGateTests.swift
//
//  Regression tests for the 2026-05-02 post-reset deadlock fix.
//
//  Pinning the contract for `MLSRecoveryManager.extractRustGateRemainingSec`:
//  it must recognize the Rust orchestrator's `RecoveryFailed("Rejoin
//  suppressed for X: <kind> (Ns remaining)")` shape so the iOS recovery
//  driver can route those errors through `recordRejoinOutcome` instead of
//  `recordFailedRejoin`. Without this, a Rust-side gate suppression
//  (no real network attempt happened) gets stamped as if it were a real
//  failure, doubling the gate window and creating the gate-thrash deadlock
//  observed in production.
//
//  Source of truth for the matched message shapes:
//    catbird-mls/src/orchestrator/recovery.rs:551
//      "Rejoin suppressed for {convo_id}: cooldown active ({N}s remaining)"
//    catbird-mls/src/orchestrator/recovery.rs:572
//      "Rejoin suppressed for {convo_id}: minimum interval ({N}s remaining)"
//

import XCTest

@testable import CatbirdMLSCore

private struct TestError: Error, CustomStringConvertible {
  let description: String
}

final class MLSRecoveryManagerGateTests: XCTestCase {

  func testExtractsCooldownActiveRemainingSeconds() {
    let err = TestError(
      description:
        "RecoveryFailed: Rejoin suppressed for convo-abc: cooldown active (29s remaining)"
    )
    let remaining = MLSRecoveryManager.extractRustGateRemainingSec(err)
    XCTAssertEqual(
      remaining,
      29,
      "Must parse the (29s remaining) shape from a 'cooldown active' Rust gate error"
    )
  }

  func testExtractsMinimumIntervalRemainingSeconds() {
    let err = TestError(
      description:
        "RecoveryFailed: Rejoin suppressed for convo-abc: minimum interval (15s remaining)"
    )
    let remaining = MLSRecoveryManager.extractRustGateRemainingSec(err)
    XCTAssertEqual(
      remaining,
      15,
      "Must parse the (15s remaining) shape from a 'minimum interval' Rust gate error"
    )
  }

  func testExtractsVerboseSecondsForm() {
    // Defensive future-proofing: if Rust ever emits the more verbose
    // `(N seconds remaining)` form, parser should still match.
    let err = TestError(
      description:
        "Some wrapper: Rejoin suppressed for X: minimum interval (45 seconds remaining)"
    )
    let remaining = MLSRecoveryManager.extractRustGateRemainingSec(err)
    XCTAssertEqual(remaining, 45, "Must also accept '(45 seconds remaining)' verbose shape")
  }

  func testReturnsNilForUnrelatedNetworkError() {
    let err = TestError(description: "ServerError: status=500 body=internal server error")
    XCTAssertNil(
      MLSRecoveryManager.extractRustGateRemainingSec(err),
      "Generic server error must not match the 'Rejoin suppressed' Rust gate signature"
    )
  }

  func testReturnsNilFor429RateLimit() {
    // 429 rate limits are handled by a SEPARATE existing path (the server
    // tells us a Retry-After). The Rust gate extractor must not steal those.
    let err = TestError(
      description: "Api: status code: 429, body: Retry after 60 seconds."
    )
    XCTAssertNil(
      MLSRecoveryManager.extractRustGateRemainingSec(err),
      "429 with Retry-After must not match the Rust gate extractor — that's a different path"
    )
  }

  func testReturnsNilForRejoinSuppressedButNoRemaining() {
    // The "max attempts reached" suppression has the same prefix but no
    // (Ns remaining) suffix. We can't project the gate without a duration,
    // so return nil and let the caller fall through to recordFailedRejoin.
    let err = TestError(
      description: "RecoveryFailed: Rejoin suppressed for convo-abc: max attempts reached"
    )
    XCTAssertNil(
      MLSRecoveryManager.extractRustGateRemainingSec(err),
      "Suppression message without parenthesized remaining time must return nil"
    )
  }

  func testReturnsNilForEmptyError() {
    let err = TestError(description: "")
    XCTAssertNil(MLSRecoveryManager.extractRustGateRemainingSec(err))
  }
}
