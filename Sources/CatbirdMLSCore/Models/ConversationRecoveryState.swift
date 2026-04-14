//
//  ConversationRecoveryState.swift
//  CatbirdMLSCore
//
//  Conversation-level recovery state machine (per MLS_CLIENT_PROTOCOL.md §8.1).
//
//  Implements S1 ("Welcome-first. External Commit is the escape hatch, never the
//  hot path") and S1.1 ("Recovery precedence pyramid") from the 2026 Q2 Ship
//  Program invariants. Every conversation must be in exactly one of these seven
//  states at any given time.
//

import Foundation

/// The recovery state of a single MLS conversation.
///
/// This enum is the public surface that `MLSRecoveryManager` and friends use to
/// communicate conversation health. Internal ad-hoc counters (attempt numbers,
/// cooldown timestamps) are implementation details and MUST NOT leak into API
/// boundaries — callers reason about state, not counts.
///
/// Cites protocol spec `MLS_CLIENT_PROTOCOL.md §8.1` and invariants `S1` /
/// `S1.1` in `docs/program/01-INVARIANTS.md`.
///
/// ### Layering: single enum, split persistence
///
/// The spec marks three states as "persisted" (survive app restart) and four
/// as "transient" (derived at runtime). Rather than splitting this into two
/// enums, we use a single enum and split the storage:
///
/// - **Persisted states** (`.needsRejoin`, `.unrecoverableLocal`, `.resetPending`)
///   are derived from three existing `MLSConversationModel` boolean columns
///   (`needsRejoin`, `isUnrecoverable`, `needsReset`). No new GRDB column is
///   introduced — the mapping is a computed property on the model
///   (`MLSConversationModel.persistedRecoveryState`). This avoids a schema
///   migration and keeps a single source of truth.
///
/// - **Transient states** (`.epochBehind`, `.groupMissing`, `.recovering`) live
///   only in an in-memory dictionary inside `MLSRecoveryManager`. They are
///   wiped on app restart, which is the spec-intended behavior: a fresh sync
///   re-derives them from first principles.
///
/// - **The resolved state** is produced by combining both sources. Transient
///   state takes precedence over persisted state for a given conversation,
///   because a currently-running recovery (`.recovering`) supersedes whatever
///   persisted flag triggered it.
///
/// This decision is documented in the Cluster B spec status log at
/// `docs/program/specs/cluster-B-spec-formalization.md`.
@available(iOS 18.0, macOS 13.0, *)
public enum ConversationRecoveryState: String, Codable, Equatable, Sendable, CaseIterable {
  /// Default. Everything works. No failures tracked, no pending recovery.
  ///
  /// Transient. Recomputed at runtime as the absence of any other state.
  case healthy

  /// Commits are failing with `WrongEpoch`. The client is behind the group.
  ///
  /// Transient. Detected when a send or process fails with `WrongEpoch`; moves
  /// to `.needsRejoin` after `COMMIT_FAILURE_THRESHOLD` consecutive failures
  /// (spec §8.2: "5 consecutive commit failures"), or clears back to
  /// `.healthy` if a subsequent sync catches us up.
  case epochBehind

  /// No local MLS group state exists for this conversation. The server still
  /// has it — we need to fetch a Welcome or External Commit.
  ///
  /// Transient. Detected at runtime (local HashMap lookup miss or DB
  /// `groupExists == false`). Resolved via inline recovery (spec §8.3).
  case groupMissing

  /// Flagged for deferred External Commit rejoin. This is the persistent
  /// "recovery is needed" marker.
  ///
  /// **Persisted** via `MLSConversationModel.needsRejoin == true`. Runs under
  /// `S1` rate-limits: global 30s minimum between any two External Commits
  /// client-wide, per-conversation backoff `[30s, 2m, 10m]`, max 3 attempts.
  /// Exits to `.healthy` on success, or `.unrecoverableLocal` after max
  /// attempts (spec §8.2).
  case needsRejoin

  /// An External Commit / Welcome fetch is currently in flight for this
  /// conversation.
  ///
  /// Transient. Tracked in-memory to prevent overlapping attempts; paired
  /// with `MLSRecoveryManager.beginRejoinAttempt` at the call site. Exits to
  /// `.healthy` on success, `.needsRejoin` on retryable failure, or
  /// `.unrecoverableLocal` on terminal failure.
  case recovering

  /// Maximum attempts exhausted. Locally unrecoverable; the client has
  /// reported the failure to the server and is waiting for a quorum-mediated
  /// reset (spec §8.6). This state is the input to the `S2` quorum reset
  /// pathway.
  ///
  /// **Persisted** via `MLSConversationModel.isUnrecoverable == true`. Does
  /// NOT clear on restart — the spec says "Max attempts exhausted" is a
  /// sticky local state that only server intervention (or a successful reset
  /// event) can lift.
  case unrecoverableLocal

  /// The server has issued a group reset for this conversation. The local
  /// group state must be wiped and the client must rejoin at epoch 0 with a
  /// fresh `groupId`.
  ///
  /// **Persisted** via `MLSConversationModel.needsReset == true`. Handled in
  /// Phase 1 of `runDeferredEpochRecovery` (spec §8.5), before any
  /// `.needsRejoin` conversations, because a reset supersedes any pending
  /// rejoin on the same conversation.
  case resetPending

  // MARK: - Classification

  /// Whether this state survives app restart via a GRDB column.
  ///
  /// Matches the "Persisted?" column in the §8.1 state table:
  /// - `.needsRejoin`, `.unrecoverableLocal`, `.resetPending`: persisted
  /// - everything else: transient (derived at runtime)
  public var isPersisted: Bool {
    switch self {
    case .needsRejoin, .unrecoverableLocal, .resetPending:
      return true
    case .healthy, .epochBehind, .groupMissing, .recovering:
      return false
    }
  }

  /// Whether the conversation is currently delivering messages successfully.
  /// Any other state means "something is off," even if it's benign.
  public var isHealthy: Bool {
    self == .healthy
  }

  /// Whether a recovery operation should be allowed to start from this state.
  /// Used by the deferred-recovery hook to filter candidate conversations.
  public var allowsRecoveryAttempt: Bool {
    switch self {
    case .needsRejoin, .resetPending, .groupMissing, .epochBehind:
      return true
    case .healthy, .recovering, .unrecoverableLocal:
      return false
    }
  }
}

// MARK: - Transition Validation

@available(iOS 18.0, macOS 13.0, *)
public extension ConversationRecoveryState {
  /// Whether the transition `self -> next` is legal per spec §8.2.
  ///
  /// This is the machine-checkable form of the §8.2 diagram. Use it to gate
  /// state writes in `MLSRecoveryManager` and to unit-test the transition
  /// graph in `MLSRecoveryStateMachineTests`.
  ///
  /// Transitions allowed (from §8.2):
  ///
  /// ```
  /// HEALTHY      → EPOCH_BEHIND | NEEDS_REJOIN | GROUP_MISSING | RESET_PENDING
  /// EPOCH_BEHIND → HEALTHY | NEEDS_REJOIN
  /// GROUP_MISSING→ RECOVERING | NEEDS_REJOIN | HEALTHY
  /// NEEDS_REJOIN → RECOVERING | UNRECOVERABLE_LOCAL | HEALTHY | RESET_PENDING
  /// RECOVERING   → HEALTHY | NEEDS_REJOIN | UNRECOVERABLE_LOCAL
  /// UNRECOVERABLE_LOCAL → RESET_PENDING (only via server event)
  /// RESET_PENDING→ HEALTHY | NEEDS_REJOIN
  /// ```
  ///
  /// Self-transitions (`.needsRejoin → .needsRejoin` for attempt increment)
  /// are also legal — the implementation may re-write the same state to
  /// update internal counters without violating the machine.
  func canTransition(to next: ConversationRecoveryState) -> Bool {
    if self == next { return true }  // self-transitions always legal

    switch self {
    case .healthy:
      return next == .epochBehind
        || next == .needsRejoin
        || next == .groupMissing
        || next == .resetPending

    case .epochBehind:
      return next == .healthy || next == .needsRejoin

    case .groupMissing:
      return next == .recovering || next == .needsRejoin || next == .healthy

    case .needsRejoin:
      return next == .recovering
        || next == .unrecoverableLocal
        || next == .healthy
        || next == .resetPending

    case .recovering:
      return next == .healthy || next == .needsRejoin || next == .unrecoverableLocal

    case .unrecoverableLocal:
      // UNRECOVERABLE_LOCAL is a sticky terminal state. The only way out is a
      // server-mediated reset event (spec §8.2 / §8.6). Local code MUST NOT
      // transition out of unrecoverable on its own.
      return next == .resetPending

    case .resetPending:
      return next == .healthy || next == .needsRejoin
    }
  }
}
