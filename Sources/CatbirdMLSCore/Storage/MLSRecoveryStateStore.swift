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
  private let currentUserDID: String

  public init(database: any DatabaseWriter, currentUserDID: String) {
    self.database = database
    self.currentUserDID = currentUserDID
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
}
