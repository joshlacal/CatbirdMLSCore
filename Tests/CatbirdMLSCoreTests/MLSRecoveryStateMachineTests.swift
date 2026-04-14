import Foundation
import XCTest

@testable import CatbirdMLSCore

/// Tests for `ConversationRecoveryState` and its transition graph.
///
/// Cites `MLS_CLIENT_PROTOCOL.md §8.1` (state definitions) and `§8.2` (state
/// transitions). Also covers the S1/S1.1 invariants from
/// `docs/program/01-INVARIANTS.md`: every documented transition must be
/// exercised here so that a missing test blocks a merge (invariant E1: "TDD
/// for recovery paths").
///
/// These tests are intentionally pure — they avoid constructing the full
/// `MLSRecoveryManager` actor (which requires `MLSClient` and `MLSAPIClient`)
/// by testing:
///   * the state enum's pure helper methods
///   * `MLSConversationModel.persistedRecoveryState` (no actor required)
///   * `MLSRecoveryManager` static methods (cooldown table)
///
/// If this file ever needs actor-level integration tests, add a separate
/// `MLSRecoveryStateMachineIntegrationTests` target so the unit tests stay
/// cheap.
@available(iOS 18.0, macOS 13.0, *)
final class MLSRecoveryStateMachineTests: XCTestCase {

  // MARK: - §8.1 Classification

  func testHealthyIsTheOnlyHealthyState() {
    XCTAssertTrue(ConversationRecoveryState.healthy.isHealthy)
    for state in ConversationRecoveryState.allCases where state != .healthy {
      XCTAssertFalse(state.isHealthy, "\(state) should not be .isHealthy")
    }
  }

  func testPersistedStatesMatchSpecTable() {
    // Spec §8.1 "Persisted?" column:
    //   HEALTHY               — no
    //   EPOCH_BEHIND          — no (transient)
    //   GROUP_MISSING         — no (detected at runtime)
    //   NEEDS_REJOIN          — yes
    //   RECOVERING            — no (transient)
    //   UNRECOVERABLE_LOCAL   — yes
    //   RESET_PENDING         — yes
    XCTAssertFalse(ConversationRecoveryState.healthy.isPersisted)
    XCTAssertFalse(ConversationRecoveryState.epochBehind.isPersisted)
    XCTAssertFalse(ConversationRecoveryState.groupMissing.isPersisted)
    XCTAssertTrue(ConversationRecoveryState.needsRejoin.isPersisted)
    XCTAssertFalse(ConversationRecoveryState.recovering.isPersisted)
    XCTAssertTrue(ConversationRecoveryState.unrecoverableLocal.isPersisted)
    XCTAssertTrue(ConversationRecoveryState.resetPending.isPersisted)
  }

  func testAllowsRecoveryAttemptGatesDeferredHook() {
    // Used by the post-sync deferred recovery hook to filter candidates:
    //   eligible for attempt: NEEDS_REJOIN, RESET_PENDING, GROUP_MISSING, EPOCH_BEHIND
    //   not eligible: HEALTHY (nothing to recover), RECOVERING (already in flight),
    //                 UNRECOVERABLE_LOCAL (terminal, needs server intervention)
    XCTAssertFalse(ConversationRecoveryState.healthy.allowsRecoveryAttempt)
    XCTAssertTrue(ConversationRecoveryState.epochBehind.allowsRecoveryAttempt)
    XCTAssertTrue(ConversationRecoveryState.groupMissing.allowsRecoveryAttempt)
    XCTAssertTrue(ConversationRecoveryState.needsRejoin.allowsRecoveryAttempt)
    XCTAssertFalse(ConversationRecoveryState.recovering.allowsRecoveryAttempt)
    XCTAssertFalse(ConversationRecoveryState.unrecoverableLocal.allowsRecoveryAttempt)
    XCTAssertTrue(ConversationRecoveryState.resetPending.allowsRecoveryAttempt)
  }

  // MARK: - §8.2 Transitions — HEALTHY

  func testHealthyTransitionsMatchSpecDiagram() {
    // HEALTHY
    //   ├─ 5 consecutive commit failures  ──→ NEEDS_REJOIN
    //   ├─ 3 decryption failures         ──→ NEEDS_REJOIN
    //   ├─ group not found locally       ──→ GROUP_MISSING
    //   └─ server reset event            ──→ RESET_PENDING
    // Additionally, .healthy → .epochBehind is allowed as an intermediate
    // counting state before the threshold is reached.
    XCTAssertTrue(ConversationRecoveryState.healthy.canTransition(to: .epochBehind))
    XCTAssertTrue(ConversationRecoveryState.healthy.canTransition(to: .needsRejoin))
    XCTAssertTrue(ConversationRecoveryState.healthy.canTransition(to: .groupMissing))
    XCTAssertTrue(ConversationRecoveryState.healthy.canTransition(to: .resetPending))

    // Not directly reachable from HEALTHY:
    XCTAssertFalse(ConversationRecoveryState.healthy.canTransition(to: .recovering))
    XCTAssertFalse(ConversationRecoveryState.healthy.canTransition(to: .unrecoverableLocal))
  }

  // MARK: - §8.2 Transitions — EPOCH_BEHIND

  func testEpochBehindReturnsToHealthyOnCatchUp() {
    // Implicit in §8.2: the transient counting state clears when the client
    // catches up via sync. This is the "cheapest recovery" rung of S1.1.
    XCTAssertTrue(ConversationRecoveryState.epochBehind.canTransition(to: .healthy))
  }

  func testEpochBehindEscalatesToNeedsRejoin() {
    XCTAssertTrue(ConversationRecoveryState.epochBehind.canTransition(to: .needsRejoin))
  }

  func testEpochBehindCannotSkipToRecoveringOrUnrecoverable() {
    XCTAssertFalse(ConversationRecoveryState.epochBehind.canTransition(to: .recovering))
    XCTAssertFalse(ConversationRecoveryState.epochBehind.canTransition(to: .unrecoverableLocal))
    XCTAssertFalse(ConversationRecoveryState.epochBehind.canTransition(to: .resetPending))
    XCTAssertFalse(ConversationRecoveryState.epochBehind.canTransition(to: .groupMissing))
  }

  // MARK: - §8.2 Transitions — GROUP_MISSING

  func testGroupMissingTransitionsMatchSpecDiagram() {
    // GROUP_MISSING
    //   ├─ Welcome succeeds              ──→ HEALTHY
    //   ├─ External Commit succeeds      ──→ HEALTHY
    //   └─ recovery fails                ──→ NEEDS_REJOIN
    XCTAssertTrue(ConversationRecoveryState.groupMissing.canTransition(to: .healthy))
    XCTAssertTrue(ConversationRecoveryState.groupMissing.canTransition(to: .needsRejoin))
    // And GROUP_MISSING can enter the in-flight RECOVERING phase while an
    // External Commit / Welcome fetch is active.
    XCTAssertTrue(ConversationRecoveryState.groupMissing.canTransition(to: .recovering))

    XCTAssertFalse(ConversationRecoveryState.groupMissing.canTransition(to: .unrecoverableLocal))
    XCTAssertFalse(ConversationRecoveryState.groupMissing.canTransition(to: .resetPending))
    XCTAssertFalse(ConversationRecoveryState.groupMissing.canTransition(to: .epochBehind))
  }

  // MARK: - §8.2 Transitions — NEEDS_REJOIN

  func testNeedsRejoinTransitionsMatchSpecDiagram() {
    // NEEDS_REJOIN
    //   ├─ External Commit succeeds      ──→ HEALTHY
    //   ├─ External Commit fails (< max) ──→ NEEDS_REJOIN (attempt counter++)
    //   └─ attempts >= MAX_REJOIN_ATTEMPTS──→ UNRECOVERABLE_LOCAL
    //
    // The "RECOVERING" intermediate state is allowed — it's how the hook
    // signals "attempt in flight" before the terminal outcome.
    XCTAssertTrue(ConversationRecoveryState.needsRejoin.canTransition(to: .recovering))
    XCTAssertTrue(ConversationRecoveryState.needsRejoin.canTransition(to: .healthy))
    XCTAssertTrue(ConversationRecoveryState.needsRejoin.canTransition(to: .unrecoverableLocal))
    XCTAssertTrue(ConversationRecoveryState.needsRejoin.canTransition(to: .needsRejoin))
    // RESET_PENDING is reachable: a server reset event can preempt a stuck
    // rejoin (spec §8.5 Phase 1 / §8.6 quorum reset).
    XCTAssertTrue(ConversationRecoveryState.needsRejoin.canTransition(to: .resetPending))

    XCTAssertFalse(ConversationRecoveryState.needsRejoin.canTransition(to: .epochBehind))
    XCTAssertFalse(ConversationRecoveryState.needsRejoin.canTransition(to: .groupMissing))
  }

  // MARK: - §8.2 Transitions — RECOVERING

  func testRecoveringTransitionsMatchSpecDiagram() {
    // RECOVERING is the in-flight state for an External Commit / Welcome
    // fetch. Terminal outcomes:
    //   success                           ──→ HEALTHY
    //   retryable failure                 ──→ NEEDS_REJOIN (for next cycle)
    //   max attempts reached              ──→ UNRECOVERABLE_LOCAL
    XCTAssertTrue(ConversationRecoveryState.recovering.canTransition(to: .healthy))
    XCTAssertTrue(ConversationRecoveryState.recovering.canTransition(to: .needsRejoin))
    XCTAssertTrue(ConversationRecoveryState.recovering.canTransition(to: .unrecoverableLocal))

    XCTAssertFalse(ConversationRecoveryState.recovering.canTransition(to: .epochBehind))
    XCTAssertFalse(ConversationRecoveryState.recovering.canTransition(to: .groupMissing))
    XCTAssertFalse(ConversationRecoveryState.recovering.canTransition(to: .resetPending))
  }

  // MARK: - §8.2 Transitions — UNRECOVERABLE_LOCAL

  func testUnrecoverableLocalIsStickyExceptForServerReset() {
    // UNRECOVERABLE_LOCAL
    //   ├─ report to server               ──→ (await quorum reset)
    //   └─ server reset event             ──→ RESET_PENDING
    //
    // The ONLY legal out-bound transition is RESET_PENDING, triggered by a
    // server-side event. Local code MUST NOT transition out of
    // UNRECOVERABLE_LOCAL on its own (spec §8.2 / §8.6).
    XCTAssertTrue(ConversationRecoveryState.unrecoverableLocal.canTransition(to: .resetPending))

    XCTAssertFalse(ConversationRecoveryState.unrecoverableLocal.canTransition(to: .healthy))
    XCTAssertFalse(ConversationRecoveryState.unrecoverableLocal.canTransition(to: .epochBehind))
    XCTAssertFalse(ConversationRecoveryState.unrecoverableLocal.canTransition(to: .groupMissing))
    XCTAssertFalse(ConversationRecoveryState.unrecoverableLocal.canTransition(to: .needsRejoin))
    XCTAssertFalse(ConversationRecoveryState.unrecoverableLocal.canTransition(to: .recovering))
  }

  // MARK: - §8.2 Transitions — RESET_PENDING

  func testResetPendingTransitionsMatchSpecDiagram() {
    // RESET_PENDING
    //   ├─ rejoin at epoch 0 succeeds    ──→ HEALTHY
    //   └─ rejoin fails                  ──→ NEEDS_REJOIN (reset attempt counter)
    XCTAssertTrue(ConversationRecoveryState.resetPending.canTransition(to: .healthy))
    XCTAssertTrue(ConversationRecoveryState.resetPending.canTransition(to: .needsRejoin))

    XCTAssertFalse(ConversationRecoveryState.resetPending.canTransition(to: .recovering))
    XCTAssertFalse(ConversationRecoveryState.resetPending.canTransition(to: .unrecoverableLocal))
    XCTAssertFalse(ConversationRecoveryState.resetPending.canTransition(to: .epochBehind))
    XCTAssertFalse(ConversationRecoveryState.resetPending.canTransition(to: .groupMissing))
  }

  // MARK: - Self-transitions

  func testAllStatesAllowSelfTransition() {
    // Self-transitions are always legal — the implementation may rewrite the
    // same state to update counters/timestamps without violating the
    // machine. Covers e.g. `NEEDS_REJOIN → NEEDS_REJOIN (attempts++)`.
    for state in ConversationRecoveryState.allCases {
      XCTAssertTrue(
        state.canTransition(to: state),
        "Self-transition should be legal for \(state)")
    }
  }

  // MARK: - Regression: Threshold Paths (§8.2 arrows)

  /// Regression test for the "5 consecutive commit failures → NEEDS_REJOIN"
  /// path from §8.2. This is the most common recovery trigger in production.
  func testCommitFailureThresholdPathIsLegal() {
    // Path: HEALTHY → (counting via transient EPOCH_BEHIND) → NEEDS_REJOIN
    XCTAssertTrue(ConversationRecoveryState.healthy.canTransition(to: .epochBehind))
    XCTAssertTrue(ConversationRecoveryState.epochBehind.canTransition(to: .needsRejoin))
    // Direct skip path (when the failure arrives as a single large burst):
    XCTAssertTrue(ConversationRecoveryState.healthy.canTransition(to: .needsRejoin))
  }

  /// Regression test for the "3 decryption failures → NEEDS_REJOIN" path.
  func testDecryptionFailureThresholdPathIsLegal() {
    // Decryption failures use the same terminal transition as commit failures,
    // just counted separately. The state transition is identical.
    XCTAssertTrue(ConversationRecoveryState.healthy.canTransition(to: .needsRejoin))
  }

  /// Regression test for "group not found locally → GROUP_MISSING".
  func testGroupNotFoundPathIsLegal() {
    XCTAssertTrue(ConversationRecoveryState.healthy.canTransition(to: .groupMissing))
  }

  /// Regression test for "server reset event → RESET_PENDING".
  func testServerResetEventPathIsLegal() {
    XCTAssertTrue(ConversationRecoveryState.healthy.canTransition(to: .resetPending))
    // Also reachable from every live state (server can reset at any time):
    XCTAssertTrue(ConversationRecoveryState.needsRejoin.canTransition(to: .resetPending))
    XCTAssertTrue(ConversationRecoveryState.unrecoverableLocal.canTransition(to: .resetPending))
  }

  // MARK: - Codable round-trip

  func testCodableRoundTrip() throws {
    let encoder = JSONEncoder()
    let decoder = JSONDecoder()
    for state in ConversationRecoveryState.allCases {
      let data = try encoder.encode(state)
      let decoded = try decoder.decode(ConversationRecoveryState.self, from: data)
      XCTAssertEqual(decoded, state)
    }
  }

  // MARK: - MLSConversationModel.persistedRecoveryState

  func testPersistedRecoveryStateDefaultsToHealthy() {
    let model = makeModel()
    XCTAssertEqual(model.persistedRecoveryState, .healthy)
  }

  func testPersistedRecoveryStateMapsNeedsRejoin() {
    let model = makeModel(needsRejoin: true)
    XCTAssertEqual(model.persistedRecoveryState, .needsRejoin)
  }

  func testPersistedRecoveryStateMapsUnrecoverable() {
    let model = makeModel(isUnrecoverable: true)
    XCTAssertEqual(model.persistedRecoveryState, .unrecoverableLocal)
  }

  func testPersistedRecoveryStateMapsResetPending() {
    let model = makeModel(needsReset: true)
    XCTAssertEqual(model.persistedRecoveryState, .resetPending)
  }

  func testPersistedRecoveryStatePrecedenceResetDominates() {
    // All three flags set at once — resetPending has highest precedence
    // because a server-issued reset supersedes any local flag.
    let model = makeModel(
      needsRejoin: true,
      needsReset: true,
      isUnrecoverable: true
    )
    XCTAssertEqual(model.persistedRecoveryState, .resetPending)
  }

  func testPersistedRecoveryStatePrecedenceUnrecoverableBeatsNeedsRejoin() {
    // Without needsReset, unrecoverable dominates needsRejoin so we don't
    // bounce a max-attempts-exhausted conversation back into the retry path.
    let model = makeModel(needsRejoin: true, isUnrecoverable: true)
    XCTAssertEqual(model.persistedRecoveryState, .unrecoverableLocal)
  }

  func testPersistedRecoveryStateAllEightBooleanCombinations() {
    // Exhaustive coverage of the 2^3 = 8 boolean combinations. Matches the
    // precedence rules: needsReset → unrecoverable → needsRejoin → healthy.
    let combos: [(needsRejoin: Bool, needsReset: Bool, isUnrecoverable: Bool, expected: ConversationRecoveryState)] = [
      (false, false, false, .healthy),
      (true,  false, false, .needsRejoin),
      (false, false, true,  .unrecoverableLocal),
      (true,  false, true,  .unrecoverableLocal),
      (false, true,  false, .resetPending),
      (true,  true,  false, .resetPending),
      (false, true,  true,  .resetPending),
      (true,  true,  true,  .resetPending),
    ]

    for combo in combos {
      let model = makeModel(
        needsRejoin: combo.needsRejoin,
        needsReset: combo.needsReset,
        isUnrecoverable: combo.isUnrecoverable
      )
      XCTAssertEqual(
        model.persistedRecoveryState,
        combo.expected,
        "Combo (rejoin=\(combo.needsRejoin), reset=\(combo.needsReset), unrec=\(combo.isUnrecoverable)) expected \(combo.expected) got \(model.persistedRecoveryState)")
    }
  }

  // MARK: - MLSRecoveryManager static methods (spec §10 backoff table)

  func testBackoffScheduleMatchesSpec() {
    // Spec §10: REJOIN_BACKOFF = [30s, 2m, 10m] indexed by attempt (1-based).
    XCTAssertEqual(MLSRecoveryManager.cooldownForAttempts(0), 0)
    XCTAssertEqual(MLSRecoveryManager.cooldownForAttempts(1), 30)
    XCTAssertEqual(MLSRecoveryManager.cooldownForAttempts(2), 120)
    XCTAssertEqual(MLSRecoveryManager.cooldownForAttempts(3), 600)
  }

  func testBackoffCooldownIsActiveBeforeWindowElapses() {
    let now = Date()
    XCTAssertTrue(
      MLSRecoveryManager.isInBackoffCooldown(
        attempts: 1,
        lastAttempt: now.addingTimeInterval(-5),
        now: now))
    XCTAssertTrue(
      MLSRecoveryManager.isInBackoffCooldown(
        attempts: 2,
        lastAttempt: now.addingTimeInterval(-30),
        now: now))
  }

  func testBackoffCooldownClearsAfterWindow() {
    let now = Date()
    XCTAssertFalse(
      MLSRecoveryManager.isInBackoffCooldown(
        attempts: 1,
        lastAttempt: now.addingTimeInterval(-31),
        now: now))
    XCTAssertFalse(
      MLSRecoveryManager.isInBackoffCooldown(
        attempts: 2,
        lastAttempt: now.addingTimeInterval(-121),
        now: now))
  }

  func testBackoffCooldownIgnoresZeroAttempts() {
    // attempts == 0 means "never failed" — no cooldown applies.
    let now = Date()
    XCTAssertFalse(
      MLSRecoveryManager.isInBackoffCooldown(
        attempts: 0,
        lastAttempt: now,
        now: now))
  }

  // MARK: - Helpers

  private func makeModel(
    needsRejoin: Bool = false,
    needsReset: Bool = false,
    isUnrecoverable: Bool = false
  ) -> MLSConversationModel {
    MLSConversationModel(
      conversationID: "test-convo-\(UUID().uuidString)",
      currentUserDID: "did:plc:test",
      groupID: Data(repeating: 0xAB, count: 16),
      epoch: 1,
      joinMethod: .welcome,
      joinEpoch: 0,
      title: nil,
      avatarURL: nil,
      avatarImageData: nil,
      createdAt: Date(),
      updatedAt: Date(),
      lastMessageAt: nil,
      lastMembershipChangeAt: nil,
      unacknowledgedMemberChanges: 0,
      isActive: true,
      needsRejoin: needsRejoin,
      needsReset: needsReset,
      isUnrecoverable: isUnrecoverable,
      rejoinRequestedAt: nil,
      lastRecoveryAttempt: nil,
      consecutiveFailures: 0,
      isPlaceholder: false,
      requestState: .none,
      mutedUntil: nil)
  }
}
