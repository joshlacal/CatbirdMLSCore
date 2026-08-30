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
  public init(dbPool: DatabasePool, userDID: String, mlsContext: MlsContext) {
    self.dbPool = dbPool
    self.userDID = MLSStorageHelpers.normalizeDID(userDID)
    self.mlsContext = mlsContext

    // Create orchestrator-specific tables up front so read-only paths
    // never need to issue DDL on a WAL snapshot connection.
    do {
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
          CREATE TABLE IF NOT EXISTS mls_orchestrator_quarantine (
            conversation_id TEXT NOT NULL,
            user_did TEXT NOT NULL,
            reason_tag TEXT NOT NULL,
            since_ms INTEGER NOT NULL,
            PRIMARY KEY (conversation_id, user_did)
          )
          """)
        try db.execute(sql: """
          CREATE TABLE IF NOT EXISTS mls_orchestrator_pending_messages (
            message_id TEXT PRIMARY KEY NOT NULL,
            conversation_id TEXT NOT NULL,
            user_did TEXT NOT NULL,
            created_at DATETIME NOT NULL
          )
          """)
        try db.execute(sql: """
          CREATE TABLE IF NOT EXISTS mls_orchestrator_sequencer_receipts (
            conversation_id TEXT NOT NULL,
            user_did TEXT NOT NULL,
            epoch INTEGER NOT NULL,
            sequencer_term INTEGER NOT NULL DEFAULT 0,
            commit_hash BLOB NOT NULL,
            sequencer_did TEXT NOT NULL,
            issued_at INTEGER NOT NULL,
            signature BLOB NOT NULL,
            PRIMARY KEY (conversation_id, user_did, epoch)
          )
          """)
      }
    } catch {
      // Non-fatal: tables may already exist or will be created on first write.
      Logger(subsystem: "blue.catbird.mls", category: "OrchestratorStorageAdapter")
        .warning("Failed to pre-create orchestrator tables: \(error)")
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

  /// Read-only callback identity resolution. A missing exact raw group id may
  /// remain on its normalized raw id for legacy insertion paths; a missing
  /// stable id fails closed so an auxiliary child cannot be stranded beside a
  /// canonical parent that the projection callback has not adopted yet.
  private func resolvedConversationID(
    in db: Database,
    requestedID: String,
    groupID: String? = nil
  ) throws -> String {
    if let resolved = try MLSStorageHelpers.resolveCanonicalConversationIDSync(
      in: db,
      userDID: userDID,
      conversationID: requestedID,
      groupID: groupID
    ) {
      return resolved
    }

    if groupID == nil,
       requestedID == requestedID.lowercased(),
       let rawData = Data(hexEncoded: requestedID)
    {
      return rawData.hexEncodedString()
    }
    throw MLSStorageError.invalidConversationID(requestedID)
  }

  /// Pending-delete rows are crash-recovery intent keys, not conversation
  /// foreign keys. Preserve the orchestrator's raw-to-stable handoff exactly.
  private func validatedPendingLocalDeleteID(_ requestedID: String) throws -> String {
    if MLSStorageHelpers.isCanonicalUUIDv4(requestedID) {
      return requestedID
    }
    guard let rawID = Data(hexEncoded: requestedID),
          !rawID.isEmpty,
          rawID.hexEncodedString() == requestedID
    else {
      throw MLSStorageError.invalidConversationID(requestedID)
    }
    return requestedID
  }

  // MARK: - Conversation Operations

  public func ensureConversationExists(
    userDid: String,
    conversationId: String,
    groupId: String
  ) throws {
    try dbPool.write { db in
      guard Data(hexEncoded: groupId) != nil else {
        throw MLSStorageError.invalidGroupID(groupId)
      }
      _ = try MLSStorageHelpers.ensureConversationExistsSync(
        in: db,
        userDID: userDid,
        conversationID: conversationId,
        groupID: groupId,
        isPlaceholder: false
      )
    }
  }

  public func updateJoinInfo(
    conversationId: String,
    userDid: String,
    joinMethod: String,
    joinEpoch: UInt64
  ) throws {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)
    let method: MLSJoinMethod = {
      switch joinMethod.lowercased() {
      case "welcome": return .welcome
      case "external_commit", "externalcommit": return .externalCommit
      case "creator": return .creator
      default: return .unknown
      }
    }()

    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == effectiveID)
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
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)

    return try dbPool.read { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      guard let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == effectiveID)
        .filter(MLSConversationModel.Columns.currentUserDID == normalizedDID)
        .fetchOne(db)
      else {
        return nil
      }

      // Fetch active members for this conversation
      let members = try MLSMemberModel
        .filter(MLSMemberModel.Columns.conversationID == effectiveID)
        .filter(MLSMemberModel.Columns.currentUserDID == normalizedDID)
        .filter(MLSMemberModel.Columns.isActive == true)
        .fetchAll(db)

      return self.conversationToFfi(conversation, members: members)
    }
  }

  public func listConversations(userDid: String) throws -> [FfiConversationView] {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)

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
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)

    try dbPool.write { db in
      // Mark conversations as inactive rather than hard-deleting,
      // matching the existing soft-delete pattern in the codebase.
      // CASCADE on MLSMessageModel will handle cleanup if we hard-delete,
      // but soft-delete preserves message history.
      for conversationId in ids {
        let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
        if let conversation = try MLSConversationModel
          .filter(MLSConversationModel.Columns.conversationID == effectiveID)
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
    // The Rust orchestrator uses a string "state" field (e.g. "active", "left", "error").
    // Map this to the existing MLSConversationModel fields.
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == effectiveID)
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .fetchOne(db)
      {
        let updated: MLSConversationModel
        switch state.lowercased() {
        case "left", "inactive":
          updated = conversation.withActiveStatus(false)
        case "active":
          updated = conversation.withActiveStatus(true)
        case "needs_rejoin":
          updated = conversation.withRejoinState(needsRejoin: true, rejoinRequestedAt: Date())
        default:
          // Unknown state -- log warning and store as-is via active status
          logger.warning(
            "Unknown conversation state from orchestrator: \(state) for \(conversationId)"
          )
          updated = conversation
        }
        try updated.update(db)
      }
    }
  }

  public func getConversationState(conversationId: String) throws -> FfiConversationState? {
    try dbPool.read { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      guard let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == effectiveID)
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .fetchOne(db)
      else {
        return nil
      }

      if let quarantine = try Row.fetchOne(
        db,
        sql: """
          SELECT reason_tag, since_ms
          FROM mls_orchestrator_quarantine
          WHERE conversation_id = ? AND user_did = ?
          """,
        arguments: [effectiveID, userDID]
      ) {
        return FfiConversationState(
          state: "quarantined",
          newGroupId: nil,
          resetGeneration: nil,
          notifiedAtMs: nil,
          quarantineReason: quarantine["reason_tag"],
          quarantinedSinceMs: quarantine["since_ms"]
        )
      }

      if conversation.needsReset, let newGroupId = conversation.pendingNewGroupId,
         let generation = conversation.pendingResetGeneration
      {
        return FfiConversationState(
          state: "reset_pending",
          newGroupId: newGroupId,
          resetGeneration: Int32(clamping: generation),
          notifiedAtMs: Int64(conversation.updatedAt.timeIntervalSince1970 * 1_000),
          quarantineReason: nil,
          quarantinedSinceMs: nil
        )
      }

      let state: String
      if conversation.needsRejoin {
        state = "needs_rejoin"
      } else if conversation.isUnrecoverable {
        state = "failed"
      } else if conversation.isActive {
        state = "active"
      } else {
        // The Rust state projection has no `left` state. Inactive rows are
        // not eligible for recovery, so expose the durable failure state.
        state = "failed"
      }
      return FfiConversationState(
        state: state,
        newGroupId: nil,
        resetGeneration: nil,
        notifiedAtMs: nil,
        quarantineReason: nil,
        quarantinedSinceMs: nil
      )
    }
  }

  public func markQuarantined(conversationId: String, reasonTag: String, sinceMs: Int64) throws {
    guard ["peer_bad_commit", "multi_peer_bad_commits", "repeated_framing_failures"].contains(reasonTag) else {
      throw OrchestratorBridgeError.Storage(message: "Unknown quarantine reason: \(reasonTag)")
    }
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_quarantine (conversation_id, user_did, reason_tag, since_ms)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(conversation_id, user_did) DO UPDATE SET
            reason_tag = excluded.reason_tag,
            since_ms = excluded.since_ms
          """,
        arguments: [effectiveID, userDID, reasonTag, sinceMs]
      )
    }
  }

  public func clearQuarantine(conversationId: String) throws {
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      try db.execute(
        sql: "DELETE FROM mls_orchestrator_quarantine WHERE conversation_id = ? AND user_did = ?",
        arguments: [effectiveID, userDID]
      )
    }
  }

  // MARK: - Reset Pending Operations (§8.5 Phase 1)

  /// Persist the `RESET_PENDING` payload so recovery survives app restart.
  /// Called by the Rust orchestrator on every `GroupResetEvent` before it
  /// mutates local MLS state. Writes go to the `pendingNewGroupId` +
  /// `pendingResetGeneration` columns on `MLSConversationModel` (schema v28).
  ///
  /// A stale-generation guard rejects writes where the stored generation is
  /// ≥ the incoming one. This defends against duplicate SSE/poll deliveries.
  public func markResetPending(
    conversationId: String,
    newGroupIdHex: String,
    resetGeneration: Int32,
    notifiedAtMs: Int64
  ) throws {
    let incomingGen = Int64(resetGeneration)
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      let stored = try Int64.fetchOne(
        db,
        sql: """
          SELECT pendingResetGeneration FROM MLSConversationModel
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [effectiveID, userDID]
      )
      if let stored, stored >= incomingGen {
        return
      }
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET needsReset = 1,
              needsRejoin = 0,
              isUnrecoverable = 0,
              pendingNewGroupId = ?,
              pendingResetGeneration = ?,
              updatedAt = ?
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [newGroupIdHex, incomingGen, Date(), effectiveID, userDID]
      )
    }
  }

  /// Clear the staged pending-reset target after the conversation has adopted
  /// the new group. Leaves `needsReset` and the reset generation high-water
  /// untouched — callers handle the lifecycle state separately. The current
  /// schema has one generation column, so it is retained as the provisional
  /// high-water until the sealed split-column contract is available.
  public func clearResetPending(conversationId: String) throws {
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET pendingNewGroupId = NULL,
              updatedAt = ?
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [Date(), effectiveID, userDID]
      )
    }
  }

  public func adoptResetPendingTarget(
    conversationId: String,
    expectedGeneration: Int32,
    expectedOldTarget: String,
    authoritativeNewTarget: String
  ) throws -> Bool {
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET pendingNewGroupId = ?,
              updatedAt = ?
          WHERE conversationID = ?
            AND currentUserDID = ?
            AND pendingResetGeneration = ?
            AND (pendingNewGroupId = ? OR pendingNewGroupId IS NULL)
          """,
        arguments: [
          authoritativeNewTarget, Date(), effectiveID, userDID, Int64(expectedGeneration), expectedOldTarget,
        ]
      )
      return db.changesCount > 0
    }
  }

  public func completeResetPending(
    conversationId: String,
    expectedGeneration: Int32,
    expectedNewGroupIdHex: String,
    landedEpoch: UInt64
  ) throws -> Bool {
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      guard let groupData = Data(hexEncoded: expectedNewGroupIdHex) else {
        return false
      }
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET groupID = ?,
              epoch = ?,
              needsReset = 0,
              needsRejoin = 0,
              isUnrecoverable = 0,
              pendingNewGroupId = NULL,
              pendingResetGeneration = COALESCE(pendingResetGeneration, ?),
              updatedAt = ?
          WHERE conversationID = ?
            AND currentUserDID = ?
            AND (pendingResetGeneration = ? OR pendingResetGeneration IS NULL)
            AND (LOWER(pendingNewGroupId) = LOWER(?) OR pendingNewGroupId IS NULL)
          """,
        arguments: [
          groupData, Int64(landedEpoch), Int64(expectedGeneration), Date(), effectiveID, userDID,
          Int64(expectedGeneration), expectedNewGroupIdHex,
        ]
      )
      return db.changesCount > 0
    }
  }

  public func clearResetPendingForDelete(
    conversationId: String,
    expectedGeneration: Int32
  ) throws -> Bool {
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET pendingNewGroupId = NULL,
              pendingResetGeneration = COALESCE(pendingResetGeneration, ?),
              updatedAt = ?
          WHERE conversationID = ?
            AND currentUserDID = ?
            AND (pendingResetGeneration = ? OR pendingResetGeneration IS NULL)
          """,
        arguments: [
          Int64(expectedGeneration), Date(), effectiveID, userDID, Int64(expectedGeneration),
        ]
      )
      return db.changesCount > 0
    }
  }

  public func requestWelcomeReissue(
    convoId: String,
    recipientDeviceDid: String,
    reason: String
  ) throws {
    logger.info("Welcome reissue requested for convo \(convoId), recipient \(recipientDeviceDid), reason: \(reason)")
  }

  public func markNeedsRejoin(conversationId: String) throws {
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == effectiveID)
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
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == effectiveID)
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .fetchOne(db)
      return conversation?.needsRejoin ?? false
    }
  }

  public func clearRejoinFlag(conversationId: String) throws {
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == effectiveID)
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
      let effectiveID = try resolvedConversationID(
        in: db,
        requestedID: message.conversationId
      )
      try MLSStorageHelpers.savePayloadSync(
        context: mlsContext,
        in: db,
        messageID: message.id,
        conversationID: effectiveID,
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
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      var request = MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == effectiveID)
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
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_pending_messages (message_id, conversation_id, user_did, created_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(message_id) DO UPDATE SET
            conversation_id = excluded.conversation_id,
            user_did = excluded.user_did
          """,
        arguments: [messageId, effectiveID, userDID, Date()]
      )
    }
  }

  public func removePendingMessage(messageId: String) throws -> Bool {
    try dbPool.write { db in
      try db.execute(
        sql: "DELETE FROM mls_orchestrator_pending_messages WHERE message_id = ? AND user_did = ?",
        arguments: [messageId, userDID]
      )
      return db.changesCount > 0
    }
  }

  public func storeSequencerReceipt(receipt: FfiSequencerReceipt) throws {
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: receipt.convoId)
      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_sequencer_receipts
            (conversation_id, user_did, epoch, sequencer_term, commit_hash, sequencer_did, issued_at, signature)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(conversation_id, user_did, epoch) DO UPDATE SET
            sequencer_term = excluded.sequencer_term,
            commit_hash = excluded.commit_hash,
            sequencer_did = excluded.sequencer_did,
            issued_at = excluded.issued_at,
            signature = excluded.signature
          """,
        arguments: [
          effectiveID,
          userDID,
          receipt.epoch,
          Int64(receipt.sequencerTerm),
          receipt.commitHash,
          receipt.sequencerDid,
          receipt.issuedAt,
          receipt.signature,
        ]
      )
    }
  }

  public func getSequencerReceipts(
    conversationId: String,
    sinceEpoch: Int32?
  ) throws -> [FfiSequencerReceipt] {
    try dbPool.read { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      var sql = """
        SELECT conversation_id, epoch, sequencer_term, commit_hash, sequencer_did, issued_at, signature
        FROM mls_orchestrator_sequencer_receipts
        WHERE conversation_id = ? AND user_did = ?
        """
      var arguments: StatementArguments = [effectiveID, userDID]
      if let sinceEpoch {
        sql += " AND epoch >= ?"
        arguments += [sinceEpoch]
      }
      sql += " ORDER BY epoch ASC"
      return try Row.fetchAll(db, sql: sql, arguments: arguments).map { row in
        let term: Int64 = (row["sequencer_term"] as Int64?) ?? 0
        return FfiSequencerReceipt(
          convoId: row["conversation_id"],
          epoch: row["epoch"],
          sequencerTerm: UInt64(term),
          commitHash: row["commit_hash"],
          sequencerDid: row["sequencer_did"],
          issuedAt: row["issued_at"],
          signature: row["signature"]
        )
      }
    }
  }

  public func clearSequencerReceipts(conversationId: String) throws {
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      try db.execute(
        sql: "DELETE FROM mls_orchestrator_sequencer_receipts WHERE conversation_id = ? AND user_did = ?",
        arguments: [effectiveID, userDID]
      )
    }
  }

  // MARK: - Sync Cursor Operations

  /// Sync cursors are stored in a lightweight table. Since no existing GRDB model
  /// covers this, we use raw SQL with a dedicated table created at migration time.
  /// If the table does not exist yet (pre-migration), we create it on first access.

  public func getSyncCursor(userDid: String) throws -> FfiSyncCursor {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)

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
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)

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
      let effectiveID = try resolvedConversationID(
        in: db,
        requestedID: state.conversationId,
        groupID: state.groupId
      )

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
          effectiveID,
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

      let effectiveID = try resolvedConversationID(
        in: db,
        requestedID: row["conversation_id"],
        groupID: groupId
      )
      return FfiGroupState(
        groupId: row["group_id"],
        conversationId: effectiveID,
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
      return FfiPersistedRecoveryState(
        entries: try rows.map { row in
          FfiPersistedRecoveryBackoff(
            // Ambiguous or invalid identity must propagate to the Rust
            // callback rather than silently exposing the raw alias.  The
            // resolver is read-only; adoption belongs to the transactional
            // projection path.
            conversationId: try self.resolvedConversationID(in: db, requestedID: row.conversationID),
            failedRejoinCount: UInt32(clamping: row.failedRejoinCount),
            lastAttemptAtMs: row.lastAttemptAtMs,
            quarantinedUntilMs: row.quarantinedUntilMs
          )
        },
        lastGlobalRejoinAttemptAtMs: global?.lastGlobalRejoinAttemptAtMs
      )
    }
  }

  /// Write-through one conversation's rejoin-backoff snapshot.
  public func setRecoveryBackoff(entry: FfiPersistedRecoveryBackoff) throws {
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: entry.conversationId)
      let model = MLSRecoveryAttemptStateModel(
        conversationID: effectiveID,
        currentUserDID: userDID,
        failedRejoinCount: Int(entry.failedRejoinCount),
        lastAttemptAtMs: entry.lastAttemptAtMs,
        quarantinedUntilMs: entry.quarantinedUntilMs
      )
      // Conflict policy on the model is .replace — insert acts as upsert,
      // matching MLSRecoveryStateStore.upsertConversationState.
      try model.insert(db)
    }
  }

  /// Remove a conversation's persisted backoff entry.
  public func clearRecoveryBackoff(conversationId: String) throws {
    let normalizedDID = userDID
    try dbPool.write { db in
      let effectiveID = try resolvedConversationID(in: db, requestedID: conversationId)
      try db.execute(
        sql: """
          DELETE FROM MLSRecoveryAttemptStateModel
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [effectiveID, normalizedDID]
      )
    }
  }

  /// Persist the global last-rejoin-attempt timestamp (epoch ms).
  public func setLastGlobalRejoinAttemptAt(atMs: Int64) throws {
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
    try dbPool.write { db in
      ensurePendingLocalDeleteTableExists(db)
      let effectiveID = try validatedPendingLocalDeleteID(conversationId)
      try db.execute(
        sql: """
          INSERT INTO mls_orchestrator_pending_local_deletes (conversation_id, user_did, group_id_hex, created_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(conversation_id, user_did) DO UPDATE SET
            group_id_hex = excluded.group_id_hex
          """,
        arguments: [effectiveID, userDID, groupIdHex, Date()]
      )
    }
  }

  /// Clear a pending local-delete intent after all delete steps succeeded.
  public func clearPendingLocalDelete(conversationId: String) throws {
    try dbPool.write { db in
      ensurePendingLocalDeleteTableExists(db)
      let effectiveID = try validatedPendingLocalDeleteID(conversationId)
      try db.execute(
        sql: """
          DELETE FROM mls_orchestrator_pending_local_deletes
          WHERE conversation_id = ? AND user_did = ?
          """,
        arguments: [effectiveID, userDID]
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
      return try rows.map { row in
        let requestedID: String = row["conversation_id"]
        let groupID: String? = row["group_id_hex"]
        return FfiPendingLocalDelete(
          conversationId: try self.validatedPendingLocalDeleteID(requestedID),
          groupIdHex: groupID
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
