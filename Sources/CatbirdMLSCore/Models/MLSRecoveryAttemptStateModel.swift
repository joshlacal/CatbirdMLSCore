//
//  MLSRecoveryAttemptStateModel.swift
//  CatbirdMLSCore
//
//  WS-6.4 (2026-06-09 MLS stack improvement plan) — persisted recovery
//  counters. Resolves N21 (dead `hydrateFromDatabase`) and batches N20
//  (bootstrap-success cleanup).
//
//  E7 coordinated semantics — this is the Swift twin of the Rust
//  orchestrator's persisted `RecoveryTracker` (WS-5.4). Both twins persist
//  the same logical schema:
//
//    per-conversation: { failed_rejoin_count,
//                        last_attempt_at   (unix epoch milliseconds),
//                        quarantined_until (unix epoch milliseconds, nullable) }
//    global:           { last_global_rejoin_attempt_at (unix epoch ms) }
//
//  TTL: entries whose `last_attempt_at` is older than 24h are IGNORED at
//  hydration (`MLSRecoveryManager.persistedStateTTL`). This guarantees a
//  persisted counter can never permanently wedge a conversation — at worst
//  it suppresses automated rejoin for the remainder of the 24h window.
//
//  Write-through happens on every counter state change inside
//  `MLSRecoveryManager`; persistence failures are logged loudly (.error),
//  never silently dropped.
//

import Foundation
import GRDB

/// Per-conversation persisted rejoin-attempt state.
///
/// One row per (conversationID, currentUserDID). Replaced wholesale on each
/// state change (write-through), deleted on successful rejoin
/// (`clearRejoinTracking`).
public struct MLSRecoveryAttemptStateModel: Codable, FetchableRecord, PersistableRecord, Sendable {
  public static let databaseTableName = "MLSRecoveryAttemptStateModel"

  /// Conflict policy: re-recording the same (convoId, currentUserDID) replaces.
  public static let persistenceConflictPolicy = PersistenceConflictPolicy(
    insert: .replace,
    update: .replace
  )

  /// Conversation identifier.
  public let conversationID: String

  /// Per-user DB partition key.
  public let currentUserDID: String

  /// Cumulative failed External-Commit rejoin attempts within the TTL window.
  /// Mirrors Rust `RecoveryTracker.failed_rejoins[convo].count`.
  public var failedRejoinCount: Int

  /// Unix epoch **milliseconds** of the most recent attempt outcome.
  /// Mirrors Rust `last_attempt_at`. The 24h TTL is evaluated against this.
  public var lastAttemptAtMs: Int64

  /// Optional explicit quarantine horizon, unix epoch **milliseconds**.
  /// Set when the rejoin-attempt ceiling trips or a conversation is marked
  /// server-corrupted. Hydration honors any remaining quarantine but never
  /// extends it. Mirrors Rust `quarantined_until`.
  public var quarantinedUntilMs: Int64?

  public enum Columns {
    public static let conversationID = Column(CodingKeys.conversationID)
    public static let currentUserDID = Column(CodingKeys.currentUserDID)
    public static let failedRejoinCount = Column(CodingKeys.failedRejoinCount)
    public static let lastAttemptAtMs = Column(CodingKeys.lastAttemptAtMs)
    public static let quarantinedUntilMs = Column(CodingKeys.quarantinedUntilMs)
  }

  public init(
    conversationID: String,
    currentUserDID: String,
    failedRejoinCount: Int,
    lastAttemptAtMs: Int64,
    quarantinedUntilMs: Int64? = nil
  ) {
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.failedRejoinCount = failedRejoinCount
    self.lastAttemptAtMs = lastAttemptAtMs
    self.quarantinedUntilMs = quarantinedUntilMs
  }
}

/// Global (per-user) persisted recovery state. Single row per user DID.
///
/// Carries the cross-conversation rejoin floor timestamp so a restart cannot
/// reset the 30s global External-Commit spacing (spec §8.4 / §10).
public struct MLSRecoveryGlobalStateModel: Codable, FetchableRecord, PersistableRecord, Sendable {
  public static let databaseTableName = "MLSRecoveryGlobalStateModel"

  public static let persistenceConflictPolicy = PersistenceConflictPolicy(
    insert: .replace,
    update: .replace
  )

  /// Per-user DB partition key (primary key).
  public let currentUserDID: String

  /// Unix epoch **milliseconds** of the most recent global rejoin attempt
  /// outcome. Mirrors Rust `RecoveryTracker.last_global_rejoin_at`. May sit
  /// in the near future when projected from a Rust-gate suppression
  /// (`recordRejoinOutcome`).
  public var lastGlobalRejoinAttemptAtMs: Int64

  public enum Columns {
    public static let currentUserDID = Column(CodingKeys.currentUserDID)
    public static let lastGlobalRejoinAttemptAtMs = Column(CodingKeys.lastGlobalRejoinAttemptAtMs)
  }

  public init(currentUserDID: String, lastGlobalRejoinAttemptAtMs: Int64) {
    self.currentUserDID = currentUserDID
    self.lastGlobalRejoinAttemptAtMs = lastGlobalRejoinAttemptAtMs
  }
}
