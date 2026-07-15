//
//  MLSOrchestratorStorageAdapter.swift
//  CatbirdMLSCore
//
//  Bridges the Rust orchestrator's OrchestratorStorageCallback protocol to
//  the existing Swift GRDB storage layer. Called synchronously from Rust on
//  a background thread -- all GRDB operations use synchronous DatabasePool
//  read/write to avoid blocking on the Swift async runtime.
//

import CatbirdMLS
import Foundation
import GRDB
import OSLog

// MARK: - Synchronous Database Access

/// Provides synchronous GRDB access for use in UniFFI callbacks.
///
/// `MLSGRDBManager` is an actor with async read/write methods, but the Rust
/// orchestrator calls these callbacks synchronously on a background thread.
/// We need a direct `DatabasePool` reference to call `pool.read { }` and
/// `pool.write { }` synchronously (GRDB supports this natively).
///
/// The pool is obtained once during adapter construction (when we still have
/// an async context) and held for the adapter's lifetime.
public final class MLSOrchestratorStorageAdapter: OrchestratorStorageCallback, @unchecked Sendable {

  // MARK: - Properties

  private let dbPool: DatabasePool
  private let userDID: String
  private let mlsContext: MlsContext
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "OrchestratorStorageAdapter")

  // MARK: - ISO 8601 Formatter

  /// Thread-safe static formatter for ISO 8601 date strings used by the Rust FFI layer.
  private static let iso8601Formatter: ISO8601DateFormatter = {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return formatter
  }()

  /// Fallback formatter without fractional seconds.
  private static let iso8601FallbackFormatter: ISO8601DateFormatter = {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime]
    return formatter
  }()

  // MARK: - Initialization

  /// Create the adapter with a pre-obtained DatabasePool.
  ///
  /// - Parameters:
  ///   - dbPool: A GRDB DatabasePool for the current user's MLS database.
  ///   - userDID: The current user's DID (normalized).
  ///   - mlsContext: The active Rust context used for field-level payload encryption.
  public init(dbPool: DatabasePool, userDID: String, mlsContext: MlsContext) throws {
    self.dbPool = dbPool
    self.userDID = MLSStorageHelpers.normalizeDID(userDID)
    self.mlsContext = mlsContext

    // Create orchestrator-specific tables up front so read-only paths
    // never need to issue DDL on a WAL snapshot connection.
    do {
      try requireValidDID(self.userDID, field: "bound user DID")
      try dbPool.write { db in
        try db.execute(sql: """
          CREATE TABLE IF NOT EXISTS mls_orchestrator_sync_cursors (
            user_did TEXT PRIMARY KEY NOT NULL,
            conversations_cursor TEXT,
            messages_cursor TEXT,
            updated_at DATETIME NOT NULL
          )
          """)
        try db.execute(sql: """
          CREATE TABLE IF NOT EXISTS mls_orchestrator_group_state (
            group_id TEXT PRIMARY KEY NOT NULL,
            conversation_id TEXT NOT NULL,
            epoch INTEGER NOT NULL DEFAULT 0,
            members_json BLOB,
            updated_at DATETIME NOT NULL
          )
          """)
        try db.execute(sql: """
          CREATE TABLE IF NOT EXISTS mls_orchestrator_pending_local_deletes (
            conversation_id TEXT NOT NULL,
            user_did TEXT NOT NULL,
            group_id_hex TEXT,
            created_at DATETIME NOT NULL,
            PRIMARY KEY (conversation_id, user_did)
          )
          """)
        try db.execute(sql: """
          CREATE TABLE IF NOT EXISTS mls_orchestrator_security_state (
            conversation_id TEXT NOT NULL,
            user_did TEXT NOT NULL,
            quarantine_reason TEXT,
            quarantined_since_ms INTEGER,
            reset_notified_at_ms INTEGER,
            applied_reset_generation INTEGER CHECK (applied_reset_generation >= 0),
            updated_at DATETIME NOT NULL,
            PRIMARY KEY (conversation_id, user_did)
          )
          """)
        let securityColumns = Set(
          try db.columns(in: "mls_orchestrator_security_state").map(\.name)
        )
        if !securityColumns.contains("applied_reset_generation") {
          try db.execute(sql: """
            ALTER TABLE mls_orchestrator_security_state
            ADD COLUMN applied_reset_generation INTEGER
              CHECK (applied_reset_generation >= 0)
            """)
        }
        try db.execute(sql: """
          CREATE TABLE IF NOT EXISTS mls_orchestrator_pending_messages (
            message_id TEXT NOT NULL,
            user_did TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            created_at DATETIME NOT NULL,
            PRIMARY KEY (message_id, user_did)
          )
          """)
        try db.execute(sql: """
          CREATE TABLE IF NOT EXISTS mls_orchestrator_sequencer_receipts (
            conversation_id TEXT NOT NULL,
            user_did TEXT NOT NULL,
            epoch INTEGER NOT NULL CHECK (epoch >= 0),
            sequencer_term INTEGER NOT NULL CHECK (sequencer_term >= 0),
            commit_hash BLOB NOT NULL CHECK (length(commit_hash) = 32),
            sequencer_did TEXT NOT NULL,
            issued_at INTEGER NOT NULL CHECK (issued_at >= 0),
            signature BLOB NOT NULL CHECK (length(signature) = 64),
            PRIMARY KEY (conversation_id, user_did, epoch)
          )
          """)
        let requiredTables = [
          "mls_orchestrator_security_state",
          "mls_orchestrator_pending_messages",
          "mls_orchestrator_sequencer_receipts",
          "mls_orchestrator_pending_local_deletes",
        ]
        for table in requiredTables {
          guard try db.tableExists(table) else {
            throw OrchestratorBridgeError.Storage(message: "required security table missing: \(table)")
          }
        }
        let requiredExistingSchema: [(String, Set<String>)] = [
          (
            "mls_orchestrator_security_state",
            [
              "conversation_id", "user_did", "quarantine_reason",
              "quarantined_since_ms", "reset_notified_at_ms",
              "applied_reset_generation", "updated_at",
            ]
          ),
          (
            "mls_orchestrator_pending_messages",
            ["message_id", "user_did", "conversation_id", "created_at"]
          ),
          (
            "mls_orchestrator_sequencer_receipts",
            [
              "conversation_id", "user_did", "epoch", "sequencer_term",
              "commit_hash", "sequencer_did", "issued_at", "signature",
            ]
          ),
          (
            "mls_orchestrator_pending_local_deletes",
            ["conversation_id", "user_did", "group_id_hex", "created_at"]
          ),
          (
            MLSConversationModel.databaseTableName,
            [
              "conversationID", "currentUserDID", "isActive", "needsRejoin",
              "needsReset", "isUnrecoverable", "pendingNewGroupId",
              "pendingResetGeneration", "updatedAt",
            ]
          ),
          (
            MLSRecoveryAttemptStateModel.databaseTableName,
            [
              "conversationID", "currentUserDID", "failedRejoinCount",
              "lastAttemptAtMs", "quarantinedUntilMs",
            ]
          ),
          (
            MLSRecoveryGlobalStateModel.databaseTableName,
            ["currentUserDID", "lastGlobalRejoinAttemptAtMs"]
          ),
        ]
        for (table, requiredColumns) in requiredExistingSchema {
          guard try db.tableExists(table) else {
            throw OrchestratorBridgeError.Storage(message: "required security table missing: \(table)")
          }
          let availableColumns = Set(try db.columns(in: table).map(\.name))
          let missingColumns = requiredColumns.subtracting(availableColumns).sorted()
          guard missingColumns.isEmpty else {
            throw OrchestratorBridgeError.Storage(
              message: "required security columns missing from \(table): \(missingColumns.joined(separator: ","))"
            )
          }
        }
        let requiredPrimaryKeys: [(String, [String])] = [
          ("mls_orchestrator_security_state", ["conversation_id", "user_did"]),
          ("mls_orchestrator_pending_messages", ["message_id", "user_did"]),
          (
            "mls_orchestrator_sequencer_receipts",
            ["conversation_id", "user_did", "epoch"]
          ),
          ("mls_orchestrator_pending_local_deletes", ["conversation_id", "user_did"]),
        ]
        for (table, expectedColumns) in requiredPrimaryKeys {
          let actualColumns = try db.primaryKey(table).columns
          guard actualColumns == expectedColumns else {
            throw OrchestratorBridgeError.Storage(
              message: "required security primary key mismatch for \(table)"
            )
          }
        }
      }
    } catch {
      throw OrchestratorBridgeError.Storage(
        message: "failed to initialize required security storage: \(error.localizedDescription)"
      )
    }

    logger.debug("OrchestratorStorageAdapter initialized for \(userDID.prefix(20), privacy: .private)")
  }

  // MARK: - Date Helpers

  private func parseDate(_ isoString: String?) -> Date? {
    guard let str = isoString, !str.isEmpty else { return nil }
    return Self.iso8601Formatter.date(from: str)
      ?? Self.iso8601FallbackFormatter.date(from: str)
  }

  private func formatDate(_ date: Date?) -> String? {
    guard let date = date else { return nil }
    return Self.iso8601Formatter.string(from: date)
  }

  // MARK: - Security Boundary Validation

  private func storageFailure(_ message: String) -> OrchestratorBridgeError {
    .Storage(message: message)
  }

  private func requireBoundUser(_ suppliedDID: String) throws -> String {
    let normalizedDID = MLSStorageHelpers.normalizeDID(suppliedDID)
    guard normalizedDID == userDID else {
      throw storageFailure("storage callback principal does not match the bound user")
    }
    return normalizedDID
  }

  private func requireNonEmptyIdentifier(_ value: String, field: String) throws {
    guard !value.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
      throw storageFailure("\(field) must not be empty")
    }
  }

  private func requireCanonicalGroupID(_ value: String, field: String) throws {
    try requireNonEmptyIdentifier(value, field: field)
    guard let decoded = Data(hexEncoded: value),
          !decoded.isEmpty,
          decoded.hexEncodedString() == value
    else {
      throw storageFailure("\(field) must be non-empty canonical hexadecimal")
    }
  }

  private func requireValidDID(_ value: String, field: String) throws {
    let components = value.split(separator: ":", maxSplits: 2, omittingEmptySubsequences: false)
    guard components.count == 3,
          components[0] == "did",
          !components[1].isEmpty,
          !components[2].isEmpty
    else {
      throw storageFailure("\(field) is not a valid DID")
    }
  }

  private func requireOwnedConversation(_ conversationId: String, in db: Database) throws {
    try requireNonEmptyIdentifier(conversationId, field: "conversation id")
    let exists = try Bool.fetchOne(
      db,
      sql: """
        SELECT EXISTS(
          SELECT 1 FROM MLSConversationModel
          WHERE conversationID = ? AND currentUserDID = ?
        )
        """,
      arguments: [conversationId, userDID]
    ) ?? false
    guard exists else {
      throw storageFailure("conversation is not owned by the bound user")
    }
  }

  private func clearResetNotification(conversationId: String, in db: Database) throws {
    try db.execute(
      sql: """
        UPDATE mls_orchestrator_security_state
        SET reset_notified_at_ms = NULL, updated_at = ?
        WHERE conversation_id = ? AND user_did = ?
        """,
      arguments: [Date(), conversationId, userDID]
    )
    try db.execute(
      sql: """
        DELETE FROM mls_orchestrator_security_state
        WHERE conversation_id = ? AND user_did = ?
          AND quarantine_reason IS NULL
          AND quarantined_since_ms IS NULL
          AND reset_notified_at_ms IS NULL
          AND applied_reset_generation IS NULL
        """,
      arguments: [conversationId, userDID]
    )
  }

  private func recordAppliedResetGeneration(
    conversationId: String,
    generation: Int64,
    in db: Database
  ) throws {
    try db.execute(
      sql: """
        INSERT INTO mls_orchestrator_security_state
          (conversation_id, user_did, applied_reset_generation, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(conversation_id, user_did) DO UPDATE SET
          applied_reset_generation = CASE
            WHEN applied_reset_generation IS NULL
              OR applied_reset_generation < excluded.applied_reset_generation
            THEN excluded.applied_reset_generation
            ELSE applied_reset_generation
          END,
          updated_at = excluded.updated_at
        """,
      arguments: [conversationId, userDID, generation, Date()]
    )
  }

  private func validatedReceiptTerm(_ receipt: FfiSequencerReceipt) throws -> Int64 {
    try requireNonEmptyIdentifier(receipt.convoId, field: "receipt conversation id")
    guard receipt.epoch >= 0 else {
      throw storageFailure("sequencer receipt epoch must not be negative")
    }
    guard let term = Int64(exactly: receipt.sequencerTerm) else {
      throw storageFailure("sequencer receipt term exceeds durable storage range")
    }
    guard receipt.commitHash.count == 32 else {
      throw storageFailure("sequencer receipt commit hash must be 32 bytes")
    }
    try requireValidDID(receipt.sequencerDid, field: "sequencer DID")
    guard receipt.issuedAt >= 0 else {
      throw storageFailure("sequencer receipt issued-at must not be negative")
    }
    guard receipt.signature.count == 64 else {
      throw storageFailure("sequencer receipt signature must be 64 bytes")
    }
    return term
  }

  private func decodeReceipt(_ row: Row) throws -> FfiSequencerReceipt {
    let conversationId: String = try row.decode(forColumn: "conversation_id")
    let epoch: Int64 = try row.decode(forColumn: "epoch")
    let term: Int64 = try row.decode(forColumn: "sequencer_term")
    let commitHash: Data = try row.decode(forColumn: "commit_hash")
    let sequencerDID: String = try row.decode(forColumn: "sequencer_did")
    let issuedAt: Int64 = try row.decode(forColumn: "issued_at")
    let signature: Data = try row.decode(forColumn: "signature")
    guard let epoch32 = Int32(exactly: epoch), epoch32 >= 0, term >= 0 else {
      throw storageFailure("stored sequencer receipt has an invalid epoch or term")
    }
    let receipt = FfiSequencerReceipt(
      convoId: conversationId,
      epoch: epoch32,
      sequencerTerm: UInt64(term),
      commitHash: commitHash,
      sequencerDid: sequencerDID,
      issuedAt: issuedAt,
      signature: signature
    )
    _ = try validatedReceiptTerm(receipt)
    return receipt
  }

  // MARK: - Conversation Operations

  public func ensureConversationExists(
    userDid: String,
    conversationId: String,
    groupId: String
  ) throws {
    let normalizedDID = try requireBoundUser(userDid)
    try requireNonEmptyIdentifier(conversationId, field: "conversation id")

    try dbPool.write { db in
      // Check if conversation already exists
      let count = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationId)
        .filter(MLSConversationModel.Columns.currentUserDID == normalizedDID)
        .fetchCount(db)

      guard count == 0 else { return }

      // Convert groupId hex string to Data
      guard let groupIDData = Data(hexEncoded: groupId) else {
        throw MLSStorageError.invalidGroupID(groupId)
      }

      let conversation = MLSConversationModel(
        conversationID: conversationId,
        currentUserDID: normalizedDID,
        groupID: groupIDData,
        epoch: 0,
        joinMethod: .unknown,
        joinEpoch: 0,
        title: nil,
        avatarURL: nil,
        createdAt: Date(),
        updatedAt: Date(),
        lastMessageAt: nil,
        lastMembershipChangeAt: nil,
        unacknowledgedMemberChanges: 0,
        isActive: true,
        needsRejoin: false,
        rejoinRequestedAt: nil,
        lastRecoveryAttempt: nil,
        consecutiveFailures: 0,
        isPlaceholder: false,
        requestState: .none
      )
      try conversation.insert(db)
    }
  }

  public func updateJoinInfo(
    conversationId: String,
    userDid: String,
    joinMethod: String,
    joinEpoch: UInt64
  ) throws {
    let normalizedDID = try requireBoundUser(userDid)
    let method: MLSJoinMethod = {
      switch joinMethod.lowercased() {
      case "welcome": return .welcome
      case "external_commit", "externalcommit": return .externalCommit
      case "creator": return .creator
      default: return .unknown
      }
    }()

    try dbPool.write { db in
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationId)
        .filter(MLSConversationModel.Columns.currentUserDID == normalizedDID)
        .fetchOne(db)
      {
        let updated = conversation.withJoinInfo(method: method, epoch: Int64(joinEpoch))
        try updated.update(db)
      }
    }
  }

  public func getConversation(
    userDid: String,
    conversationId: String
  ) throws -> FfiConversationView? {
    let normalizedDID = try requireBoundUser(userDid)

    return try dbPool.read { db in
      guard let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationId)
        .filter(MLSConversationModel.Columns.currentUserDID == normalizedDID)
        .fetchOne(db)
      else {
        return nil
      }

      // Fetch active members for this conversation
      let members = try MLSMemberModel
        .filter(MLSMemberModel.Columns.conversationID == conversationId)
        .filter(MLSMemberModel.Columns.currentUserDID == normalizedDID)
        .filter(MLSMemberModel.Columns.isActive == true)
        .fetchAll(db)

      return self.conversationToFfi(conversation, members: members)
    }
  }

  public func listConversations(userDid: String) throws -> [FfiConversationView] {
    let normalizedDID = try requireBoundUser(userDid)

    return try dbPool.read { db in
      let conversations = try MLSConversationModel
        .filter(MLSConversationModel.Columns.isActive == true)
        .filter(MLSConversationModel.Columns.currentUserDID == normalizedDID)
        .order(MLSConversationModel.Columns.lastMessageAt.desc)
        .fetchAll(db)

      guard !conversations.isEmpty else { return [] }

      // Batch fetch all active members
      let conversationIDs = conversations.map { $0.conversationID }
      let allMembers = try MLSMemberModel
        .filter(conversationIDs.contains(MLSMemberModel.Columns.conversationID))
        .filter(MLSMemberModel.Columns.currentUserDID == normalizedDID)
        .filter(MLSMemberModel.Columns.isActive == true)
        .fetchAll(db)

      let membersByConvoID = Dictionary(grouping: allMembers) { $0.conversationID }

      return conversations.map { convo in
        let members = membersByConvoID[convo.conversationID] ?? []
        return self.conversationToFfi(convo, members: members)
      }
    }
  }

  public func deleteConversations(userDid: String, ids: [String]) throws {
    let normalizedDID = try requireBoundUser(userDid)

    try dbPool.write { db in
      // Mark conversations as inactive rather than hard-deleting,
      // matching the existing soft-delete pattern in the codebase.
      // CASCADE on MLSMessageModel will handle cleanup if we hard-delete,
      // but soft-delete preserves message history.
      for conversationId in ids {
        if let conversation = try MLSConversationModel
          .filter(MLSConversationModel.Columns.conversationID == conversationId)
          .filter(MLSConversationModel.Columns.currentUserDID == normalizedDID)
          .fetchOne(db)
        {
          let updated = conversation.withActiveStatus(false)
          try updated.update(db)
        }
      }
    }
  }

  public func setConversationState(conversationId: String, state: String) throws {
    // Sealed Rust emits `active`, `needs_rejoin`, and `failed` for the durable
    // scalar states handled here. Payload-bearing reset/quarantine states use
    // their dedicated callbacks. Legacy platform `left`/`inactive` remains
    // supported; every other tag fails closed.
    try dbPool.write { db in
      try requireOwnedConversation(conversationId, in: db)
      guard let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationId)
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .fetchOne(db)
      else {
        throw storageFailure("conversation disappeared during state transition")
      }
      switch state.lowercased() {
      case "left", "inactive":
        try conversation.withActiveStatus(false).update(db)
      case "active":
        guard !conversation.needsReset else {
          throw storageFailure("pending reset requires exact completion")
        }
        try db.execute(
          sql: """
            UPDATE MLSConversationModel
            SET isActive = 1,
                needsRejoin = 0,
                isUnrecoverable = 0,
                updatedAt = ?
            WHERE conversationID = ? AND currentUserDID = ?
            """,
          arguments: [Date(), conversationId, userDID]
        )
      case "needs_rejoin":
        try conversation
          .withRejoinState(needsRejoin: true, rejoinRequestedAt: Date())
          .update(db)
      case "failed":
        guard !conversation.needsReset else {
          throw storageFailure("pending reset requires exact completion")
        }
        try db.execute(
          sql: """
            UPDATE MLSConversationModel
            SET isActive = 0,
                needsRejoin = 0,
                isUnrecoverable = 1,
                updatedAt = ?
            WHERE conversationID = ? AND currentUserDID = ?
            """,
          arguments: [Date(), conversationId, userDID]
        )
      default:
        throw storageFailure("unknown conversation state")
      }
    }
  }

  public func getConversationState(conversationId: String) throws -> FfiConversationState? {
    try dbPool.read { db in
      guard let row = try Row.fetchOne(
        db,
        sql: """
          SELECT isActive, needsRejoin, needsReset, isUnrecoverable, pendingNewGroupId,
                 pendingResetGeneration
          FROM MLSConversationModel
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [conversationId, userDID]
      ) else { return nil }

      let security = try Row.fetchOne(
        db,
        sql: """
          SELECT quarantine_reason, quarantined_since_ms, reset_notified_at_ms
          FROM mls_orchestrator_security_state
          WHERE conversation_id = ? AND user_did = ?
          """,
        arguments: [conversationId, userDID]
      )
      let reason: String? = try security?.decode(forColumn: "quarantine_reason")
      let since: Int64? = try security?.decode(forColumn: "quarantined_since_ms")
      let notifiedAt: Int64? = try security?.decode(forColumn: "reset_notified_at_ms")
      let newGroupId: String? = try row.decode(forColumn: "pendingNewGroupId")
      let generation64: Int64? = try row.decode(forColumn: "pendingResetGeneration")
      let generation: Int32?
      if let generation64 {
        guard let exact = Int32(exactly: generation64), exact >= 0 else {
          throw storageFailure("stored reset generation is invalid")
        }
        generation = exact
      } else {
        generation = nil
      }
      if let newGroupId, Data(hexEncoded: newGroupId) == nil {
        throw storageFailure("stored reset group id is malformed")
      }
      if let notifiedAt, notifiedAt < 0 {
        throw storageFailure("stored reset notification time is invalid")
      }
      if let reason {
        guard !reason.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
              let since,
              since >= 0
        else {
          throw storageFailure("stored quarantine state is malformed")
        }
      } else if since != nil {
        throw storageFailure("stored quarantine timestamp has no reason")
      }
      let needsReset: Bool = try row.decode(forColumn: "needsReset")
      let isUnrecoverable: Bool = try row.decode(forColumn: "isUnrecoverable")
      let needsRejoin: Bool = try row.decode(forColumn: "needsRejoin")
      let isActive: Bool = try row.decode(forColumn: "isActive")
      let state = reason != nil ? "quarantined" : needsReset ? "reset_pending" : isUnrecoverable ? "failed" : needsRejoin ? "needs_rejoin" : isActive ? "active" : "left"
      return FfiConversationState(
        state: state,
        newGroupId: newGroupId,
        resetGeneration: generation,
        notifiedAtMs: notifiedAt,
        quarantineReason: reason,
        quarantinedSinceMs: since
      )
    }
  }

  // MARK: - Reset Pending Operations (§8.5 Phase 1)

  /// Persist the `RESET_PENDING` payload so recovery survives app restart.
  /// Called by the Rust orchestrator on every `GroupResetEvent` before it
  /// mutates local MLS state. Writes go to the `pendingNewGroupId` +
  /// `pendingResetGeneration` columns on `MLSConversationModel` (schema v28).
  ///
  /// A stale-generation guard rejects older writes. An exact replay verifies
  /// its payload and backfills security metadata for pre-capability rows.
  public func markResetPending(
    conversationId: String,
    newGroupIdHex: String,
    resetGeneration: Int32,
    notifiedAtMs: Int64
  ) throws {
    try requireNonEmptyIdentifier(newGroupIdHex, field: "pending reset group id")
    guard Data(hexEncoded: newGroupIdHex) != nil else {
      throw storageFailure("pending reset group id must be valid hexadecimal")
    }
    guard resetGeneration >= 0, notifiedAtMs >= 0 else {
      throw storageFailure("pending reset generation and notification time must not be negative")
    }
    let incomingGen = Int64(resetGeneration)
    try dbPool.write { db in
      try requireOwnedConversation(conversationId, in: db)
      let stored = try Row.fetchOne(
        db,
        sql: """
          SELECT c.pendingResetGeneration, c.pendingNewGroupId,
                 s.applied_reset_generation
          FROM MLSConversationModel AS c
          LEFT JOIN mls_orchestrator_security_state AS s
            ON s.conversation_id = c.conversationID
           AND s.user_did = c.currentUserDID
          WHERE c.conversationID = ? AND c.currentUserDID = ?
          """,
        arguments: [conversationId, userDID]
      )
      let appliedGeneration: Int64? = try stored?.decode(
        forColumn: "applied_reset_generation"
      )
      if let appliedGeneration {
        guard appliedGeneration >= 0 else {
          throw storageFailure("stored applied reset generation is invalid")
        }
        if incomingGen <= appliedGeneration {
          throw storageFailure("pending reset generation was already applied")
        }
      }
      let storedGeneration: Int64? = try stored?.decode(forColumn: "pendingResetGeneration")
      if let storedGeneration {
        guard storedGeneration <= incomingGen else {
          throw storageFailure("pending reset generation must advance monotonically")
        }
        if storedGeneration == incomingGen {
          let storedGroupId: String? = try stored?.decode(forColumn: "pendingNewGroupId")
          guard storedGroupId == newGroupIdHex else {
            throw storageFailure("conflicting pending reset target for the same generation")
          }
          let storedNotifiedAt = try Int64.fetchOne(
            db,
            sql: """
              SELECT reset_notified_at_ms
              FROM mls_orchestrator_security_state
              WHERE conversation_id = ? AND user_did = ?
              """,
            arguments: [conversationId, userDID]
          )
          if let storedNotifiedAt, storedNotifiedAt != notifiedAtMs {
            throw storageFailure("conflicting reset notification for the same generation")
          }
        }
      }
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET needsReset = 1,
              needsRejoin = 1,
              isUnrecoverable = 0,
              pendingNewGroupId = ?,
              pendingResetGeneration = ?,
              updatedAt = ?
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [newGroupIdHex, incomingGen, Date(), conversationId, userDID]
      )
      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_security_state
            (conversation_id, user_did, reset_notified_at_ms, updated_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(conversation_id, user_did) DO UPDATE SET
            reset_notified_at_ms = excluded.reset_notified_at_ms,
            updated_at = excluded.updated_at
          """,
        arguments: [conversationId, userDID, notifiedAtMs, Date()]
      )
    }
  }

  /// Atomically replace the losing reset candidate with the server-authoritative
  /// winner without completing the reset or changing any generation fence.
  ///
  /// This CAS is intentionally narrower than `markResetPending`: same-generation
  /// replacement is allowed only when the caller supplies the exact durable old
  /// target, and only for a complete ResetPending tuple with a notification.
  public func adoptResetPendingTarget(
    conversationId: String,
    expectedGeneration: Int32,
    expectedOldTarget: String,
    authoritativeNewTarget: String
  ) throws -> Bool {
    guard expectedGeneration >= 0 else {
      throw storageFailure("expected reset generation must not be negative")
    }
    try requireCanonicalGroupID(expectedOldTarget, field: "expected old reset target")
    try requireCanonicalGroupID(authoritativeNewTarget, field: "authoritative reset target")

    return try dbPool.write { db in
      try requireOwnedConversation(conversationId, in: db)
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET pendingNewGroupId = ?
          WHERE conversationID = ? AND currentUserDID = ?
            AND needsReset = 1
            AND needsRejoin = 1
            AND pendingResetGeneration = ?
            AND pendingNewGroupId = ?
            AND EXISTS (
              SELECT 1
              FROM mls_orchestrator_security_state AS security
              WHERE security.conversation_id = MLSConversationModel.conversationID
                AND security.user_did = MLSConversationModel.currentUserDID
                AND security.reset_notified_at_ms IS NOT NULL
                AND security.reset_notified_at_ms >= 0
            )
          """,
        arguments: [
          authoritativeNewTarget,
          conversationId,
          userDID,
          Int64(expectedGeneration),
          expectedOldTarget,
        ]
      )
      return db.changesCount == 1
    }
  }

  /// Complete exactly one durable reset tuple. This is a single CAS over the
  /// generation and target, so a stale worker cannot project Active or replace
  /// the durable conversation-to-group mapping after a newer reset arrives.
  public func completeResetPending(
    conversationId: String,
    expectedGeneration: Int32,
    expectedNewGroupIdHex: String,
    landedEpoch: UInt64
  ) throws -> Bool {
    guard expectedGeneration >= 0 else {
      throw storageFailure("expected reset generation must not be negative")
    }
    guard let durableEpoch = Int64(exactly: landedEpoch) else {
      throw storageFailure("landed epoch exceeds durable storage range")
    }
    try requireNonEmptyIdentifier(conversationId, field: "conversation id")
    try requireNonEmptyIdentifier(expectedNewGroupIdHex, field: "expected reset group id")
    guard let expectedGroupID = Data(hexEncoded: expectedNewGroupIdHex) else {
      throw storageFailure("expected reset group id must be valid hexadecimal")
    }
    return try dbPool.write { db in
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET groupID = ?,
              epoch = ?,
              joinEpoch = ?,
              isActive = 1,
              needsReset = 0,
              needsRejoin = 0,
              rejoinRequestedAt = NULL,
              isUnrecoverable = 0,
              pendingNewGroupId = NULL,
              pendingResetGeneration = NULL,
              updatedAt = ?
          WHERE conversationID = ? AND currentUserDID = ?
            AND needsReset = 1
            AND pendingResetGeneration = ?
            AND pendingNewGroupId = ?
          """,
        arguments: [
          expectedGroupID, durableEpoch, durableEpoch, Date(), conversationId, userDID,
          Int64(expectedGeneration), expectedNewGroupIdHex,
        ]
      )
      guard db.changesCount == 1 else { return false }
      try recordAppliedResetGeneration(
        conversationId: conversationId,
        generation: Int64(expectedGeneration),
        in: db
      )
      try clearResetNotification(conversationId: conversationId, in: db)
      return true
    }
  }

  /// Clear exactly one durable reset generation for local deletion. Unlike
  /// completion, this does not project Active or change the durable group map.
  public func clearResetPendingForDelete(
    conversationId: String,
    expectedGeneration: Int32
  ) throws -> Bool {
    guard expectedGeneration >= 0 else {
      throw storageFailure("expected reset generation must not be negative")
    }
    return try dbPool.write { db in
      try requireOwnedConversation(conversationId, in: db)
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET needsReset = 0,
              needsRejoin = 0,
              pendingNewGroupId = NULL,
              pendingResetGeneration = NULL,
              updatedAt = ?
          WHERE conversationID = ? AND currentUserDID = ?
            AND needsReset = 1
            AND pendingResetGeneration = ?
          """,
        arguments: [Date(), conversationId, userDID, Int64(expectedGeneration)]
      )
      guard db.changesCount == 1 else { return false }
      try recordAppliedResetGeneration(
        conversationId: conversationId,
        generation: Int64(expectedGeneration),
        in: db
      )
      try clearResetNotification(conversationId: conversationId, in: db)
      return true
    }
  }

  public func markQuarantined(conversationId: String, reasonTag: String, sinceMs: Int64) throws {
    guard !reasonTag.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
      throw OrchestratorBridgeError.Storage(message: "quarantine reason must not be empty")
    }
    guard sinceMs >= 0 else {
      throw storageFailure("quarantine timestamp must not be negative")
    }
    try dbPool.write { db in
      try requireOwnedConversation(conversationId, in: db)
      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_security_state
            (conversation_id, user_did, quarantine_reason, quarantined_since_ms, updated_at)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(conversation_id, user_did) DO UPDATE SET
            quarantine_reason = excluded.quarantine_reason,
            quarantined_since_ms = excluded.quarantined_since_ms,
            updated_at = excluded.updated_at
          """,
        arguments: [conversationId, userDID, reasonTag, sinceMs, Date()]
      )
    }
  }

  public func clearQuarantine(conversationId: String) throws {
    try dbPool.write { db in
      try requireOwnedConversation(conversationId, in: db)
      try db.execute(
        sql: """
          UPDATE mls_orchestrator_security_state
          SET quarantine_reason = NULL,
              quarantined_since_ms = NULL,
              updated_at = ?
          WHERE conversation_id = ? AND user_did = ?
          """,
        arguments: [Date(), conversationId, userDID]
      )
      try db.execute(
        sql: """
          DELETE FROM mls_orchestrator_security_state
          WHERE conversation_id = ? AND user_did = ?
            AND quarantine_reason IS NULL
            AND quarantined_since_ms IS NULL
            AND reset_notified_at_ms IS NULL
            AND applied_reset_generation IS NULL
          """,
        arguments: [conversationId, userDID]
      )
    }
  }

  public func markNeedsRejoin(conversationId: String) throws {
    try dbPool.write { db in
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationId)
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .fetchOne(db)
      {
        let updated = conversation.withRejoinState(needsRejoin: true, rejoinRequestedAt: Date())
        try updated.update(db)
      }
    }
  }

  public func needsRejoin(conversationId: String) throws -> Bool {
    try dbPool.read { db in
      let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationId)
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .fetchOne(db)
      return conversation?.needsRejoin ?? false
    }
  }

  public func clearRejoinFlag(conversationId: String) throws {
    try dbPool.write { db in
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationId)
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .fetchOne(db)
      {
        let updated = conversation.withRejoinState(needsRejoin: false, rejoinRequestedAt: nil)
        try updated.update(db)
      }
    }
  }

  // MARK: - Message Operations

  public func storeMessage(message: FfiMessage) throws {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDID)
    let timestamp = parseDate(message.timestamp) ?? Date()

    let payload: MLSMessagePayload?
    if let payloadJson = message.payloadJson {
      guard let payloadData = payloadJson.data(using: .utf8) else {
        throw OrchestratorBridgeError.Storage(message: "Rust message payload JSON was not valid UTF-8")
      }
      payload = try MLSMessagePayload.decodeFromJSON(payloadData)
    } else if !message.text.isEmpty {
      payload = MLSMessagePayload.text(message.text, embed: nil)
    } else {
      payload = nil
    }

    guard let payload else { return }

    let isDelivered: Bool
    switch message.deliveryStatus {
    case .deliveredToAll:
      isDelivered = true
    case .partial(let ackedCount, let totalCount):
      isDelivered = totalCount > 0 && ackedCount >= totalCount
    case .pending, .localOnly:
      isDelivered = false
    case nil:
      isDelivered = !message.isOwn
    }

    try dbPool.write { db in
      try MLSStorageHelpers.savePayloadSync(
        context: mlsContext,
        in: db,
        messageID: message.id,
        conversationID: message.conversationId,
        currentUserDID: normalizedDID,
        payload: payload,
        senderID: message.senderDid,
        epoch: Int64(message.epoch),
        sequenceNumber: Int64(message.sequenceNumber),
        timestamp: timestamp
      )

      try db.execute(
        sql: """
          UPDATE MLSMessageModel
          SET isDelivered = ?,
              isSent = ?,
              processingState = ?
          WHERE messageID = ? AND currentUserDID = ?;
          """,
        arguments: [
          isDelivered,
          message.isOwn,
          MLSMessageProcessingState.cached,
          message.id,
          normalizedDID,
        ]
      )
    }
  }

  public func getMessages(
    conversationId: String,
    limit: UInt32,
    beforeSequence: UInt64?
  ) throws -> [FfiMessage] {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDID)

    let models: [MLSMessageModel] = try dbPool.read { db in
      var request = MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationId)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedDID)

      if let before = beforeSequence {
        request = request.filter(MLSMessageModel.Columns.sequenceNumber < Int64(before))
      }

      return try request
        .order(
          MLSMessageModel.Columns.sequenceNumber.desc,
          MLSMessageModel.Columns.timestamp.desc,
          MLSMessageModel.Columns.messageID.desc
        )
        .limit(Int(limit))
        .fetchAll(db)
    }

    // Return in chronological order (oldest first), matching the MLSStorage convention
    return models.reversed().map { messageToFfi($0) }
  }

  public func messageExists(messageId: String) throws -> Bool {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDID)

    return try dbPool.read { db in
      let count = try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageId)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedDID)
        .fetchCount(db)
      return count > 0
    }
  }

  public func storePendingMessage(conversationId: String, messageId: String) throws {
    try requireNonEmptyIdentifier(messageId, field: "pending message id")
    try dbPool.write { db in
      try requireOwnedConversation(conversationId, in: db)
      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_pending_messages
            (message_id, user_did, conversation_id, created_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(message_id, user_did) DO UPDATE SET
            conversation_id = excluded.conversation_id
          """,
        arguments: [messageId, userDID, conversationId, Date()]
      )
    }
  }

  public func removePendingMessage(messageId: String) throws -> Bool {
    try requireNonEmptyIdentifier(messageId, field: "pending message id")
    return try dbPool.write { db in
      try db.execute(
        sql: """
          DELETE FROM mls_orchestrator_pending_messages
          WHERE message_id = ? AND user_did = ?
          """,
        arguments: [messageId, userDID]
      )
      return db.changesCount > 0
    }
  }

  public func storeSequencerReceipt(receipt: FfiSequencerReceipt) throws {
    let sequencerTerm = try validatedReceiptTerm(receipt)
    try dbPool.write { db in
      try requireOwnedConversation(receipt.convoId, in: db)
      if let existing = try Row.fetchOne(
        db,
        sql: """
          SELECT conversation_id, epoch, sequencer_term, commit_hash,
                 sequencer_did, issued_at, signature
          FROM mls_orchestrator_sequencer_receipts
          WHERE conversation_id = ? AND user_did = ? AND epoch = ?
          """,
        arguments: [receipt.convoId, userDID, receipt.epoch]
      ) {
        guard try decodeReceipt(existing) == receipt else {
          throw OrchestratorBridgeError.Storage(
            message: "sequencer receipt equivocation for \(receipt.convoId) epoch \(receipt.epoch)"
          )
        }
        return
      }
      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_sequencer_receipts
            (conversation_id, user_did, epoch, sequencer_term, commit_hash,
             sequencer_did, issued_at, signature)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          """,
        arguments: [
          receipt.convoId, userDID, receipt.epoch, sequencerTerm,
          receipt.commitHash, receipt.sequencerDid, receipt.issuedAt, receipt.signature,
        ]
      )
    }
  }

  public func getSequencerReceipts(conversationId: String, sinceEpoch: Int32?) throws -> [FfiSequencerReceipt] {
    if let sinceEpoch, sinceEpoch < 0 {
      throw storageFailure("receipt epoch filter must not be negative")
    }
    return try dbPool.read { db in
      try requireOwnedConversation(conversationId, in: db)
      let rows = try Row.fetchAll(
        db,
        sql: """
          SELECT conversation_id, epoch, sequencer_term, commit_hash,
                 sequencer_did, issued_at, signature
          FROM mls_orchestrator_sequencer_receipts
          WHERE conversation_id = ? AND user_did = ?
            AND (? IS NULL OR epoch >= ?)
          ORDER BY epoch ASC, issued_at ASC
          """,
        arguments: [conversationId, userDID, sinceEpoch, sinceEpoch]
      )
      return try rows.map(decodeReceipt)
    }
  }

  public func clearSequencerReceipts(conversationId: String) throws {
    try dbPool.write { db in
      try requireOwnedConversation(conversationId, in: db)
      try db.execute(
        sql: """
          DELETE FROM mls_orchestrator_sequencer_receipts
          WHERE conversation_id = ? AND user_did = ?
          """,
        arguments: [conversationId, userDID]
      )
    }
  }

  // MARK: - Sync Cursor Operations

  /// Sync cursors are stored in a lightweight table. Since no existing GRDB model
  /// covers this, we use raw SQL with a dedicated table created at migration time.
  /// If the table does not exist yet (pre-migration), we create it on first access.

  public func getSyncCursor(userDid: String) throws -> FfiSyncCursor {
    let normalizedDID = try requireBoundUser(userDid)

    return try dbPool.read { db in
      // Table is created at init; if somehow missing, return empty cursor.
      guard try db.tableExists("mls_orchestrator_sync_cursors") else {
        return FfiSyncCursor(conversationsCursor: nil, messagesCursor: nil)
      }

      let row = try Row.fetchOne(
        db,
        sql: """
          SELECT conversations_cursor, messages_cursor
          FROM mls_orchestrator_sync_cursors
          WHERE user_did = ?
          """,
        arguments: [normalizedDID]
      )

      return FfiSyncCursor(
        conversationsCursor: row?["conversations_cursor"] as? String,
        messagesCursor: row?["messages_cursor"] as? String
      )
    }
  }

  public func setSyncCursor(userDid: String, cursor: FfiSyncCursor) throws {
    let normalizedDID = try requireBoundUser(userDid)

    try dbPool.write { db in
      ensureSyncCursorTableExists(db)

      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_sync_cursors (user_did, conversations_cursor, messages_cursor, updated_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(user_did) DO UPDATE SET
            conversations_cursor = excluded.conversations_cursor,
            messages_cursor = excluded.messages_cursor,
            updated_at = excluded.updated_at
          """,
        arguments: [
          normalizedDID,
          cursor.conversationsCursor,
          cursor.messagesCursor,
          Date(),
        ]
      )
    }
  }

  // MARK: - Group State Operations

  /// Group state is stored in a lightweight table. Since the existing Swift
  /// `MLSGroupState` is an in-memory struct (not GRDB-backed), we maintain
  /// a dedicated table for the Rust orchestrator's persistent group state.

  public func setGroupState(state: FfiGroupState) throws {
    try dbPool.write { db in
      ensureGroupStateTableExists(db)

      let membersJSON = try JSONEncoder().encode(state.members)

      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_group_state (group_id, conversation_id, epoch, members_json, updated_at)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(group_id) DO UPDATE SET
            conversation_id = excluded.conversation_id,
            epoch = excluded.epoch,
            members_json = excluded.members_json,
            updated_at = excluded.updated_at
          """,
        arguments: [
          state.groupId,
          state.conversationId,
          Int64(state.epoch),
          membersJSON,
          Date(),
        ]
      )
    }
  }

  public func getGroupState(groupId: String) throws -> FfiGroupState? {
    return try dbPool.read { db in
      // Table is created at init; if somehow missing, return nil.
      guard try db.tableExists("mls_orchestrator_group_state") else {
        return nil
      }

      guard let row = try Row.fetchOne(
        db,
        sql: """
          SELECT group_id, conversation_id, epoch, members_json
          FROM mls_orchestrator_group_state
          WHERE group_id = ?
          """,
        arguments: [groupId]
      ) else {
        return nil
      }

      let membersData: Data = row["members_json"] ?? Data()
      let members = (try? JSONDecoder().decode([String].self, from: membersData)) ?? []

      return FfiGroupState(
        groupId: row["group_id"],
        conversationId: row["conversation_id"],
        epoch: UInt64(row["epoch"] as Int64? ?? 0),
        members: members
      )
    }
  }

  public func deleteGroupState(groupId: String) throws {
    try dbPool.write { db in
      ensureGroupStateTableExists(db)

      try db.execute(
        sql: "DELETE FROM mls_orchestrator_group_state WHERE group_id = ?",
        arguments: [groupId]
      )
    }
  }

  // MARK: - RecoveryTracker Persistence (WS-5.4 / E7)

  /// These route to the SAME v32 tables the Swift recovery twin uses
  /// (`MLSRecoveryAttemptStateModel` + `MLSRecoveryGlobalStateModel`, created
  /// by `MLSGRDBManager` migration `v32_recovery_attempt_state` and written
  /// by `MLSRecoveryManager` via `MLSRecoveryStateStore`). One persisted
  /// schema, two writers — E7 coordinated semantics; the Rust orchestrator
  /// and the Swift recovery manager must never diverge on what survived a
  /// restart.

  /// Read the persisted RecoveryTracker state for startup hydration.
  public func getRecoveryState() throws -> FfiPersistedRecoveryState {
    let normalizedDID = userDID
    return try dbPool.read { db in
      // Defensive: v32 runs before this adapter is ever constructed, but a
      // read-only snapshot must never throw "no such table" on a fresh DB.
      guard try db.tableExists(MLSRecoveryAttemptStateModel.databaseTableName) else {
        return FfiPersistedRecoveryState(entries: [], lastGlobalRejoinAttemptAtMs: nil)
      }
      let rows = try MLSRecoveryAttemptStateModel
        .filter(MLSRecoveryAttemptStateModel.Columns.currentUserDID == normalizedDID)
        .fetchAll(db)
      let global = try MLSRecoveryGlobalStateModel
        .filter(MLSRecoveryGlobalStateModel.Columns.currentUserDID == normalizedDID)
        .fetchOne(db)
      let entries = try rows.map { row -> FfiPersistedRecoveryBackoff in
        guard let failedRejoinCount = UInt32(exactly: row.failedRejoinCount) else {
          throw storageFailure("stored recovery failure count is invalid")
        }
        guard row.lastAttemptAtMs >= 0,
              row.quarantinedUntilMs.map({ $0 >= 0 }) ?? true
        else {
          throw storageFailure("stored recovery timestamp is invalid")
        }
        return FfiPersistedRecoveryBackoff(
          conversationId: row.conversationID,
          failedRejoinCount: failedRejoinCount,
          lastAttemptAtMs: row.lastAttemptAtMs,
          quarantinedUntilMs: row.quarantinedUntilMs
        )
      }
      if let globalTimestamp = global?.lastGlobalRejoinAttemptAtMs, globalTimestamp < 0 {
        throw storageFailure("stored global recovery timestamp is invalid")
      }
      return FfiPersistedRecoveryState(
        entries: entries,
        lastGlobalRejoinAttemptAtMs: global?.lastGlobalRejoinAttemptAtMs
      )
    }
  }

  /// Write-through one conversation's rejoin-backoff snapshot.
  public func setRecoveryBackoff(entry: FfiPersistedRecoveryBackoff) throws {
    guard entry.lastAttemptAtMs >= 0,
          entry.quarantinedUntilMs.map({ $0 >= 0 }) ?? true
    else {
      throw storageFailure("recovery timestamp must not be negative")
    }
    let model = MLSRecoveryAttemptStateModel(
      conversationID: entry.conversationId,
      currentUserDID: userDID,
      failedRejoinCount: Int(entry.failedRejoinCount),
      lastAttemptAtMs: entry.lastAttemptAtMs,
      quarantinedUntilMs: entry.quarantinedUntilMs
    )
    try dbPool.write { db in
      // Conflict policy on the model is .replace — insert acts as upsert,
      // matching MLSRecoveryStateStore.upsertConversationState.
      try model.insert(db)
    }
  }

  /// Remove a conversation's persisted backoff entry.
  public func clearRecoveryBackoff(conversationId: String) throws {
    let normalizedDID = userDID
    try dbPool.write { db in
      try db.execute(
        sql: """
          DELETE FROM MLSRecoveryAttemptStateModel
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [conversationId, normalizedDID]
      )
    }
  }

  /// Persist the global last-rejoin-attempt timestamp (epoch ms).
  public func setLastGlobalRejoinAttemptAt(atMs: Int64) throws {
    guard atMs >= 0 else {
      throw storageFailure("global recovery timestamp must not be negative")
    }
    let model = MLSRecoveryGlobalStateModel(
      currentUserDID: userDID,
      lastGlobalRejoinAttemptAtMs: atMs
    )
    try dbPool.write { db in
      try model.insert(db)
    }
  }

  // MARK: - Pending Local Deletes (WS-5.3 crash-safe force_delete_local)

  /// Pending local-delete intents live in an adapter-owned lightweight table
  /// (same pattern as sync cursors / group state above): the intent is
  /// orchestrator bookkeeping, not user-visible model state, so it does not
  /// belong on `MLSConversationModel`.

  /// Record the intent to locally delete a conversation BEFORE the MLS-layer
  /// and storage deletes run. Idempotent — re-marking updates the group id
  /// but keeps the original intent timestamp.
  public func markPendingLocalDelete(conversationId: String, groupIdHex: String?) throws {
    if let groupIdHex {
      try requireNonEmptyIdentifier(groupIdHex, field: "pending deletion group id")
      guard Data(hexEncoded: groupIdHex) != nil else {
        throw storageFailure("pending deletion group id must be valid hexadecimal")
      }
    }
    try dbPool.write { db in
      try requireOwnedConversation(conversationId, in: db)
      ensurePendingLocalDeleteTableExists(db)
      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_pending_local_deletes (conversation_id, user_did, group_id_hex, created_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(conversation_id, user_did) DO UPDATE SET
            group_id_hex = excluded.group_id_hex
          """,
        arguments: [conversationId, userDID, groupIdHex, Date()]
      )
    }
  }

  /// Clear a pending local-delete intent after all delete steps succeeded.
  public func clearPendingLocalDelete(conversationId: String) throws {
    try dbPool.write { db in
      ensurePendingLocalDeleteTableExists(db)
      try db.execute(
        sql: """
          DELETE FROM mls_orchestrator_pending_local_deletes
          WHERE conversation_id = ? AND user_did = ?
          """,
        arguments: [conversationId, userDID]
      )
    }
  }

  /// List local deletes that were started but never completed (crash between
  /// intent and completion). Consumed by the startup reconcile sweep.
  public func listPendingLocalDeletes() throws -> [FfiPendingLocalDelete] {
    let normalizedDID = userDID
    return try dbPool.read { db in
      guard try db.tableExists("mls_orchestrator_pending_local_deletes") else {
        return []
      }
      let rows = try Row.fetchAll(
        db,
        sql: """
          SELECT conversation_id, group_id_hex
          FROM mls_orchestrator_pending_local_deletes
          WHERE user_did = ?
          ORDER BY created_at ASC
          """,
        arguments: [normalizedDID]
      )
      return rows.map { row in
        FfiPendingLocalDelete(
          conversationId: row["conversation_id"],
          groupIdHex: row["group_id_hex"]
        )
      }
    }
  }

  // MARK: - Table Creation Helpers

  /// Ensure the sync cursor table exists. Uses `CREATE TABLE IF NOT EXISTS`
  /// so it is safe to call from both read and write contexts (the read path
  /// will silently succeed if the table already exists; if not, the first
  /// write will create it).
  private func ensureSyncCursorTableExists(_ db: Database) {
    do {
      try db.execute(sql: """
        CREATE TABLE IF NOT EXISTS mls_orchestrator_sync_cursors (
          user_did TEXT PRIMARY KEY NOT NULL,
          conversations_cursor TEXT,
          messages_cursor TEXT,
          updated_at DATETIME NOT NULL
        )
        """)
    } catch {
      logger.error("Failed to ensure sync cursor table: \(error)")
    }
  }

  /// Ensure the group state table exists.
  private func ensureGroupStateTableExists(_ db: Database) {
    do {
      try db.execute(sql: """
        CREATE TABLE IF NOT EXISTS mls_orchestrator_group_state (
          group_id TEXT PRIMARY KEY NOT NULL,
          conversation_id TEXT NOT NULL,
          epoch INTEGER NOT NULL DEFAULT 0,
          members_json BLOB,
          updated_at DATETIME NOT NULL
        )
        """)
    } catch {
      logger.error("Failed to ensure group state table: \(error)")
    }
  }

  /// Ensure the pending local-delete intent table exists (WS-5.3).
  private func ensurePendingLocalDeleteTableExists(_ db: Database) {
    do {
      try db.execute(sql: """
        CREATE TABLE IF NOT EXISTS mls_orchestrator_pending_local_deletes (
          conversation_id TEXT NOT NULL,
          user_did TEXT NOT NULL,
          group_id_hex TEXT,
          created_at DATETIME NOT NULL,
          PRIMARY KEY (conversation_id, user_did)
        )
        """)
    } catch {
      logger.error("Failed to ensure pending local-delete table: \(error)")
    }
  }

  // MARK: - Model Conversion Helpers

  /// Convert an MLSConversationModel + members to the FFI representation.
  private func conversationToFfi(
    _ model: MLSConversationModel,
    members: [MLSMemberModel]
  ) -> FfiConversationView {
    let ffiMembers = members.map { member in
      FfiMemberView(
        did: member.did,
        role: member.role.rawValue
      )
    }

    return FfiConversationView(
      groupId: model.groupID.hexEncodedString(),
      conversationId: model.conversationID,
      epoch: UInt64(model.epoch),
      members: ffiMembers,
      name: model.title,
      description: nil,
      avatarUrl: model.avatarURL,
      createdAt: formatDate(model.createdAt),
      updatedAt: formatDate(model.updatedAt)
    )
  }

  /// Convert an MLSMessageModel to the FFI representation.
  private func messageToFfi(_ model: MLSMessageModel) -> FfiMessage {
    let deliveryStatus: FfiDeliveryStatus? = {
      if model.isSent && model.isDelivered {
        return .deliveredToAll
      } else if model.isSent {
        return .pending
      } else {
        return .localOnly
      }
    }()

    return FfiMessage(
      id: model.messageID,
      conversationId: model.conversationID,
      senderDid: model.senderID,
      text: model.plaintext ?? "",
      timestamp: formatDate(model.timestamp) ?? "",
      epoch: UInt64(model.epoch),
      sequenceNumber: UInt64(model.sequenceNumber),
      isOwn: model.senderID == userDID,
      deliveryStatus: deliveryStatus,
      payloadJson: model.payloadJSON.flatMap { String(data: $0, encoding: .utf8) }
    )
  }
}
