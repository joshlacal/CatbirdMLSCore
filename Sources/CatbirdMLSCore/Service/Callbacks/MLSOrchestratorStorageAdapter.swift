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
  public init(dbPool: DatabasePool, userDID: String) {
    self.dbPool = dbPool
    self.userDID = MLSStorageHelpers.normalizeDID(userDID)

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

  // MARK: - Conversation Operations

  public func ensureConversationExists(
    userDid: String,
    conversationId: String,
    groupId: String
  ) throws {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)

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
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)

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
    // The Rust orchestrator uses a string "state" field (e.g. "active", "left", "error").
    // Map this to the existing MLSConversationModel fields.
    try dbPool.write { db in
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationId)
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

  public func markNeedsRejoin(conversationId: String) throws {
    try dbPool.write { db in
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationId)
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
        .fetchOne(db)
      return conversation?.needsRejoin ?? false
    }
  }

  public func clearRejoinFlag(conversationId: String) throws {
    try dbPool.write { db in
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationId)
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

    // Prefer the full payload JSON from the Rust bridge when present.
    let payloadData: Data?
    if let payloadJson = message.payloadJson {
      payloadData = payloadJson.data(using: .utf8)
    } else if !message.text.isEmpty {
      let payload = MLSMessagePayload.text(message.text, embed: nil)
      payloadData = try? payload.encodeToJSON()
    } else {
      payloadData = nil
    }

    let model = MLSMessageModel(
      messageID: message.id,
      currentUserDID: normalizedDID,
      conversationID: message.conversationId,
      senderID: message.senderDid,
      payloadJSON: payloadData,
      wireFormat: nil,
      contentType: "application/json",
      timestamp: timestamp,
      epoch: Int64(message.epoch),
      sequenceNumber: Int64(message.sequenceNumber),
      authenticatedData: nil,
      signature: nil,
      isDelivered: true,
      isRead: false,
      isSent: message.isOwn,
      sendAttempts: 0,
      error: nil,
      processingState: "delivered",
      gapBefore: false,
      payloadExpired: false,
      processingError: nil,
      processingAttempts: 0,
      validationFailureReason: nil
    )

    try dbPool.write { db in
      // Use save (insert or update) to handle idempotent re-stores
      try model.save(db)
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
          MLSMessageModel.Columns.epoch.desc,
          MLSMessageModel.Columns.sequenceNumber.desc
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
