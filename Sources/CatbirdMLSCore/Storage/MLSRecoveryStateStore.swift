//
//  MLSRecoveryStateStore.swift
//  CatbirdMLSCore
//
//  WS-6.4 (2026-06-09 plan) — GRDB-backed persistence for
//  `MLSRecoveryManager`'s rejoin-attempt counters and global cooldown.
//  Swift twin of the Rust orchestrator's persisted `RecoveryTracker`
//  (invariant E7: coordinated semantics across both implementations).
//
//  `MLSRecoveryManager` intentionally does not own GRDB access — this store
//  is constructed by the platform layer (the single startup caller in
//  `MLSConversationManager.initialize()`) and injected via
//  `MLSRecoveryManager.setPersistence(_:)`.
//

import Foundation
import GRDB
import OSLog

/// Snapshot of all persisted recovery state for one user, returned at
/// hydration time. Timestamps are unix epoch **milliseconds** (E7 schema).
public struct MLSRecoveryPersistenceSnapshot: Sendable {
  public struct ConversationEntry: Sendable {
    public let conversationID: String
    public let failedRejoinCount: Int
    public let lastAttemptAtMs: Int64
    public let quarantinedUntilMs: Int64?

    public init(
      conversationID: String,
      failedRejoinCount: Int,
      lastAttemptAtMs: Int64,
      quarantinedUntilMs: Int64?
    ) {
      self.conversationID = conversationID
      self.failedRejoinCount = failedRejoinCount
      self.lastAttemptAtMs = lastAttemptAtMs
      self.quarantinedUntilMs = quarantinedUntilMs
    }
  }

  public let conversations: [ConversationEntry]
  public let lastGlobalRejoinAttemptAtMs: Int64?

  public init(conversations: [ConversationEntry], lastGlobalRejoinAttemptAtMs: Int64?) {
    self.conversations = conversations
    self.lastGlobalRejoinAttemptAtMs = lastGlobalRejoinAttemptAtMs
  }
}

/// Persistence seam for `MLSRecoveryManager` recovery counters.
///
/// Write-through contract (E7, coordinated with the Rust twin):
/// - called on EVERY counter state change;
/// - implementations throw on failure — the manager logs loudly and keeps
///   the in-memory state authoritative for the current process;
/// - successful rejoin clears the conversation's persisted entry.
public protocol MLSRecoveryStatePersisting: Sendable {
  /// Insert-or-replace the persisted entry for a conversation.
  func upsertConversationState(
    conversationID: String,
    failedRejoinCount: Int,
    lastAttemptAtMs: Int64,
    quarantinedUntilMs: Int64?
  ) async throws

  /// Delete the persisted entry for a conversation (rejoin success path).
  func clearConversationState(conversationID: String) async throws

  /// Insert-or-replace the global rejoin-floor timestamp.
  func setGlobalLastRejoinAttempt(atMs: Int64) async throws

  /// Load everything for hydration. TTL filtering is the manager's job —
  /// the store returns rows verbatim.
  func loadSnapshot() async throws -> MLSRecoveryPersistenceSnapshot
}

/// GRDB implementation bound to one user's encrypted database.
///
/// Tables are created by `MLSGRDBManager` migration `v32_recovery_attempt_state`.
public struct MLSRecoveryStateStore: MLSRecoveryStatePersisting {
  private let database: any DatabaseWriter
  /// Normalized (trimmed + lowercased) DID — the key actually written to the
  /// v32 tables. N43: both v32-table writers (this store and
  /// `MLSOrchestratorStorageAdapter`) MUST key by
  /// `MLSStorageHelpers.normalizeDID`, otherwise a mixed-case/whitespace DID
  /// partitions rows under different `currentUserDID` keys.
  private let currentUserDID: String
  /// The DID exactly as the caller passed it, kept solely for the one-time
  /// legacy re-key pass in `loadSnapshot()` (rows written before N43 may be
  /// keyed by the raw DID).
  private let rawUserDID: String

  public init(database: any DatabaseWriter, currentUserDID: String) {
    self.database = database
    self.rawUserDID = currentUserDID
    self.currentUserDID = MLSStorageHelpers.normalizeDID(currentUserDID)
  }

  public func upsertConversationState(
    conversationID: String,
    failedRejoinCount: Int,
    lastAttemptAtMs: Int64,
    quarantinedUntilMs: Int64?
  ) async throws {
    let model = MLSRecoveryAttemptStateModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      failedRejoinCount: failedRejoinCount,
      lastAttemptAtMs: lastAttemptAtMs,
      quarantinedUntilMs: quarantinedUntilMs
    )
    try await database.write { db in
      try model.insert(db)
    }
  }

  public func clearConversationState(conversationID: String) async throws {
    let userDID = currentUserDID
    try await database.write { db in
      try db.execute(
        sql: """
            DELETE FROM MLSRecoveryAttemptStateModel
            WHERE conversationID = ? AND currentUserDID = ?;
          """,
        arguments: [conversationID, userDID]
      )
    }
  }

  public func setGlobalLastRejoinAttempt(atMs: Int64) async throws {
    let model = MLSRecoveryGlobalStateModel(
      currentUserDID: currentUserDID,
      lastGlobalRejoinAttemptAtMs: atMs
    )
    try await database.write { db in
      try model.insert(db)
    }
  }

  public func loadSnapshot() async throws -> MLSRecoveryPersistenceSnapshot {
    let userDID = currentUserDID
    try await rekeyLegacyRawDIDRowsIfNeeded()
    return try await database.read { db in
      let rows = try MLSRecoveryAttemptStateModel
        .filter(MLSRecoveryAttemptStateModel.Columns.currentUserDID == userDID)
        .fetchAll(db)
      let global = try MLSRecoveryGlobalStateModel
        .filter(MLSRecoveryGlobalStateModel.Columns.currentUserDID == userDID)
        .fetchOne(db)
      return MLSRecoveryPersistenceSnapshot(
        conversations: rows.map {
          MLSRecoveryPersistenceSnapshot.ConversationEntry(
            conversationID: $0.conversationID,
            failedRejoinCount: $0.failedRejoinCount,
            lastAttemptAtMs: $0.lastAttemptAtMs,
            quarantinedUntilMs: $0.quarantinedUntilMs
          )
        },
        lastGlobalRejoinAttemptAtMs: global?.lastGlobalRejoinAttemptAtMs
      )
    }
  }

  /// One-time hydration migration (N43): rows written before this store
  /// normalized its key may be stored under the raw (mixed-case/untrimmed)
  /// DID. Re-key them to the normalized DID so they hydrate and so future
  /// upserts/deletes hit the same rows.
  ///
  /// Conflict policy: where a normalized-key row already exists for the same
  /// primary key, it was written post-fix (therefore newer) — keep it and
  /// drop the stale raw-key row.
  ///
  /// No-op (a single string comparison) whenever the caller already passed a
  /// normalized DID, which is the steady state after the construction site
  /// fix; `loadSnapshot()` runs once per startup, so legacy rows are healed
  /// on the first launch with a non-normalized DID.
  private func rekeyLegacyRawDIDRowsIfNeeded() async throws {
    let rawDID = rawUserDID
    let normalizedDID = currentUserDID
    guard rawDID != normalizedDID else { return }

    try await database.write { db in
      try db.execute(
        sql: """
          DELETE FROM MLSRecoveryAttemptStateModel
          WHERE currentUserDID = :raw
            AND EXISTS (
              SELECT 1 FROM MLSRecoveryAttemptStateModel AS n
              WHERE n.currentUserDID = :normalized
                AND n.conversationID = MLSRecoveryAttemptStateModel.conversationID
            );
          """,
        arguments: ["raw": rawDID, "normalized": normalizedDID]
      )
      try db.execute(
        sql: """
          UPDATE MLSRecoveryAttemptStateModel SET currentUserDID = :normalized
          WHERE currentUserDID = :raw;
          """,
        arguments: ["raw": rawDID, "normalized": normalizedDID]
      )
      try db.execute(
        sql: """
          DELETE FROM MLSRecoveryGlobalStateModel
          WHERE currentUserDID = :raw
            AND EXISTS (
              SELECT 1 FROM MLSRecoveryGlobalStateModel AS n
              WHERE n.currentUserDID = :normalized
            );
          """,
        arguments: ["raw": rawDID, "normalized": normalizedDID]
      )
      try db.execute(
        sql: """
          UPDATE MLSRecoveryGlobalStateModel SET currentUserDID = :normalized
          WHERE currentUserDID = :raw;
          """,
        arguments: ["raw": rawDID, "normalized": normalizedDID]
      )
    }
  }
}
