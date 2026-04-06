//
//  MLSStorageHelpers.swift
//  Catbird
//
//  Critical MLS storage operations for GRDB
//  Provides payload caching, transactions, and complex queries
//

import Foundation
import GRDB
import OSLog

/// Helper functions for critical MLS storage operations
/// Works with GRDB directly for write operations
public struct MLSStorageHelpers {

  // MARK: - Properties

  private static let logger = Logger(subsystem: "Catbird", category: "MLSStorage")

  // MARK: - DID Normalization

  /// Normalize a DID for consistent database storage and lookup
  ///
  /// CRITICAL: DIDs must be normalized to prevent lookup mismatches.
  /// Without normalization, messages saved with "did:plc:ABC" won't be
  /// found when looking up with "did:plc:abc".
  ///
  /// - Parameter did: The DID to normalize
  /// - Returns: Normalized DID (trimmed whitespace, lowercased)
  public static func normalizeDID(_ did: String) -> String {
    return did.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
  }

  private static func normalizeOptionalMessageID(_ messageID: String?) -> String? {
    guard let messageID else { return nil }
    let trimmed = messageID.trimmingCharacters(in: .whitespacesAndNewlines)
    return trimmed.isEmpty ? nil : trimmed
  }

  private static func shouldAdvanceFrontier(
    existingEpoch: Int64,
    existingSequenceNumber: Int64,
    newEpoch: Int64,
    newSequenceNumber: Int64
  ) -> Bool {
    if newEpoch > existingEpoch { return true }
    if newEpoch < existingEpoch { return false }
    return newSequenceNumber > existingSequenceNumber
  }

  private static func shouldAdvanceRemoteCursor(
    existingEpoch: Int64?,
    existingSequenceNumber: Int64?,
    newEpoch: Int64?,
    newSequenceNumber: Int64?
  ) -> Bool {
    guard let newEpoch, let newSequenceNumber else { return false }
    guard let existingEpoch, let existingSequenceNumber else { return true }
    return shouldAdvanceFrontier(
      existingEpoch: existingEpoch,
      existingSequenceNumber: existingSequenceNumber,
      newEpoch: newEpoch,
      newSequenceNumber: newSequenceNumber
    )
  }

  // MARK: - Critical Operations

  /// Save payload immediately after MLS decryption
  /// CRITICAL: MLS ratchet burns secrets after first decrypt - must cache (upsert) immediately.
  ///
  /// **FOREIGN KEY FIX**: This method now ensures the conversation exists before inserting
  /// a message. For new conversations (e.g., Welcome message processing), a placeholder
  /// conversation is created if needed. The main app will heal this placeholder with full
  /// metadata on the next sync.
  ///
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - messageID: Unique message identifier
  ///   - payload: Decrypted message payload
  ///   - conversationID: Conversation identifier for the message
  ///   - currentUserDID: DID of the current user (scopes update/insert)
  ///   - senderID: Sender DID extracted from MLS credentials
  ///   - epoch: MLS epoch number
  ///   - sequenceNumber: Message sequence in epoch
  public static func savePayload(
    in database: MLSDatabase,
    messageID: String,
    conversationID: String,
    currentUserDID: String,
    payload: MLSMessagePayload,
    senderID: String,
    epoch: Int64,
    sequenceNumber: Int64,
    timestamp: Date = Date()
  ) async throws {
    // Normalize DIDs for consistent storage
    let normalizedUserDID = normalizeDID(currentUserDID)
    let normalizedSenderID = normalizeDID(senderID)

    // Encode payload
    let payloadData = try payload.encodeToJSON()

    try await database.write { db in
      // Update message with payload using GRDB QueryInterface
      try db.execute(
        sql: """
          UPDATE MLSMessageModel
          SET payloadJSON = ?,
              senderID = ?,
              epoch = ?,
              sequenceNumber = ?,
              payloadExpired = 0
          WHERE messageID = ? AND currentUserDID = ?;
          """,
        arguments: [
          payloadData,
          normalizedSenderID,
          epoch,
          sequenceNumber,
          messageID,
          normalizedUserDID,
        ])

      // If no rows were updated, insert a new cached record so future decrypts skip MLS
      if db.changesCount == 0 {
        // 🚨 FOREIGN KEY FIX: Ensure conversation exists before inserting message
        // This prevents "FOREIGN KEY constraint failed" errors when processing
        // messages from new conversations (e.g., Welcome message arrives first)
        try ensureConversationExistsInTransaction(
          db: db,
          conversationID: conversationID,
          userDID: normalizedUserDID,
          senderDID: normalizedSenderID
        )

        let message = MLSMessageModel(
          messageID: messageID,
          currentUserDID: normalizedUserDID,
          conversationID: conversationID,
          senderID: normalizedSenderID,
          payloadJSON: payloadData,
          wireFormat: nil,
          contentType: "application/json",
          timestamp: timestamp,
          epoch: epoch,
          sequenceNumber: sequenceNumber,
          authenticatedData: nil,
          signature: nil,
          isDelivered: true,
          isRead: false,
          isSent: false,
          sendAttempts: 0,
          error: nil,
          processingState: "cached",
          gapBefore: false,
          payloadExpired: false,
          processingError: nil,
          processingAttempts: 0,
          validationFailureReason: nil
        )

        try message.insert(db)
      }

      logger.info("💾 Cached payload for message: \(messageID)")
    }
  }

  /// Synchronous version of savePayload for use within a write(for:) closure.
  ///
  /// This method takes a raw GRDB Database connection instead of a DatabasePool,
  /// allowing it to be used inside the `MLSGRDBManager.write(for:)` closure which
  /// provides smart routing (DatabasePool for active users, DatabaseQueue for inactive users).
  ///
  /// - Parameters:
  ///   - db: Active GRDB database connection (within a write transaction)
  ///   - messageID: Unique message identifier
  ///   - conversationID: Conversation identifier for the message
  ///   - currentUserDID: DID of the current user (scopes update/insert)
  ///   - payload: Decrypted message payload
  ///   - senderID: Sender DID extracted from MLS credentials
  ///   - epoch: MLS epoch number
  ///   - sequenceNumber: Message sequence in epoch
  ///   - timestamp: Message timestamp
  public static func savePayloadSync(
    in db: Database,
    messageID: String,
    conversationID: String,
    currentUserDID: String,
    payload: MLSMessagePayload,
    senderID: String,
    epoch: Int64,
    sequenceNumber: Int64,
    timestamp: Date = Date()
  ) throws {
    // Normalize DIDs for consistent storage
    let normalizedUserDID = normalizeDID(currentUserDID)
    let normalizedSenderID = normalizeDID(senderID)

    // Encode payload
    let payloadData = try payload.encodeToJSON()

    // Update message with payload using GRDB QueryInterface
    try db.execute(
      sql: """
        UPDATE MLSMessageModel
        SET payloadJSON = ?,
            senderID = ?,
            epoch = ?,
            sequenceNumber = ?,
            payloadExpired = 0
        WHERE messageID = ? AND currentUserDID = ?;
        """,
      arguments: [
        payloadData,
        normalizedSenderID,
        epoch,
        sequenceNumber,
        messageID,
        normalizedUserDID,
      ])

    // If no rows were updated, insert a new cached record so future decrypts skip MLS
    if db.changesCount == 0 {
      // 🚨 FOREIGN KEY FIX: Ensure conversation exists before inserting message
      try ensureConversationExistsInTransaction(
        db: db,
        conversationID: conversationID,
        userDID: normalizedUserDID,
        senderDID: normalizedSenderID
      )

      let message = MLSMessageModel(
        messageID: messageID,
        currentUserDID: normalizedUserDID,
        conversationID: conversationID,
        senderID: normalizedSenderID,
        payloadJSON: payloadData,
        wireFormat: nil,
        contentType: "application/json",
        timestamp: timestamp,
        epoch: epoch,
        sequenceNumber: sequenceNumber,
        authenticatedData: nil,
        signature: nil,
        isDelivered: true,
        isRead: false,
        isSent: false,
        sendAttempts: 0,
        error: nil,
        processingState: "cached",
        gapBefore: false,
        payloadExpired: false,
        processingError: nil,
        processingAttempts: 0,
        validationFailureReason: nil
      )

      try message.insert(db)
    }

    logger.info("💾 Cached payload for message (sync): \(messageID)")
  }

  // MARK: - Reaction Persistence (Sync)

  /// Save a reaction to local storage within an existing write transaction.
  /// Mirrors `MLSStorage.saveReaction` but operates synchronously on the provided `Database`.
  public static func saveReactionSync(
    in db: Database,
    reaction: MLSReactionModel
  ) throws {
    let normalized = MLSReactionModel(
      reactionID: reaction.reactionID,
      messageID: reaction.messageID,
      conversationID: reaction.conversationID,
      currentUserDID: normalizeDID(reaction.currentUserDID),
      actorDID: normalizeDID(reaction.actorDID),
      emoji: reaction.emoji,
      action: reaction.action,
      timestamp: reaction.timestamp
    )

    do {
      _ = try MLSReactionModel
        .filter(MLSReactionModel.Columns.messageID == normalized.messageID)
        .filter(MLSReactionModel.Columns.actorDID == normalized.actorDID)
        .filter(MLSReactionModel.Columns.emoji == normalized.emoji)
        .filter(MLSReactionModel.Columns.currentUserDID == normalized.currentUserDID)
        .deleteAll(db)

      try normalized.insert(db)
    } catch let error as DatabaseError {
      // Foreign key constraint violation (parent message missing) → save as orphan.
      if error.resultCode == .SQLITE_CONSTRAINT && error.message?.contains("FOREIGN KEY") == true {
        let orphan = MLSOrphanedReactionModel(
          reactionID: normalized.reactionID,
          messageID: normalized.messageID,
          conversationID: normalized.conversationID,
          currentUserDID: normalized.currentUserDID,
          actorDID: normalized.actorDID,
          emoji: normalized.emoji,
          action: normalized.action,
          timestamp: normalized.timestamp
        )

        _ = try MLSOrphanedReactionModel
          .filter(MLSOrphanedReactionModel.Columns.messageID == orphan.messageID)
          .filter(MLSOrphanedReactionModel.Columns.actorDID == orphan.actorDID)
          .filter(MLSOrphanedReactionModel.Columns.emoji == orphan.emoji)
          .filter(MLSOrphanedReactionModel.Columns.currentUserDID == orphan.currentUserDID)
          .deleteAll(db)

        try orphan.insert(db)
        logger.info("[ORPHAN] Saved orphaned reaction (sync): \(orphan.emoji) for missing message \(orphan.messageID)")
      } else {
        throw error
      }
    }
  }

  /// Delete a reaction (and any matching orphan) within an existing write transaction.
  public static func deleteReactionSync(
    in db: Database,
    messageID: String,
    actorDID: String,
    emoji: String,
    currentUserDID: String
  ) throws {
    let normalizedUserDID = normalizeDID(currentUserDID)
    let normalizedActorDID = normalizeDID(actorDID)

    _ = try MLSReactionModel
      .filter(MLSReactionModel.Columns.messageID == messageID)
      .filter(MLSReactionModel.Columns.actorDID == normalizedActorDID)
      .filter(MLSReactionModel.Columns.emoji == emoji)
      .filter(MLSReactionModel.Columns.currentUserDID == normalizedUserDID)
      .deleteAll(db)

    _ = try MLSOrphanedReactionModel
      .filter(MLSOrphanedReactionModel.Columns.messageID == messageID)
      .filter(MLSOrphanedReactionModel.Columns.actorDID == normalizedActorDID)
      .filter(MLSOrphanedReactionModel.Columns.emoji == emoji)
      .filter(MLSOrphanedReactionModel.Columns.currentUserDID == normalizedUserDID)
      .deleteAll(db)
  }

  // MARK: - Foreign Key Fix Helpers
  
  /// Public API for ensuring a conversation exists (for use in write(for:) closures)
  ///
  /// This method creates a minimal "placeholder" conversation record if one doesn't exist,
  /// satisfying foreign key constraints for message inserts.
  ///
  /// - Parameters:
  ///   - db: Active GRDB database connection (within a write transaction)
  ///   - userDID: User DID
  ///   - conversationID: Conversation identifier (hex-encoded group ID)
  ///   - groupID: MLS group ID (hex-encoded)
  public static func ensureConversationExistsSync(
    in db: Database,
    userDID: String,
    conversationID: String,
    groupID: String
  ) throws {
    let normalizedUserDID = normalizeDID(userDID)
    
    // Check if conversation already exists
    let existingCount =
      try MLSConversationModel
      .filter(MLSConversationModel.Columns.conversationID == conversationID)
      .filter(MLSConversationModel.Columns.currentUserDID == normalizedUserDID)
      .fetchCount(db)

    if existingCount > 0 {
      // Conversation exists, nothing to do
      return
    }

    // Create placeholder conversation
    guard let groupIDData = Data(hexEncoded: groupID) else {
      logger.warning("⚠️ [FK-FIX] groupID not valid hex: \(groupID.prefix(16))...")
      return
    }

    let placeholder = MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: normalizedUserDID,
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
      isPlaceholder: true
    )

    try placeholder.insert(db)
    logger.info("🔧 [FK-FIX] Created placeholder conversation (sync): \(conversationID.prefix(16))...")
  }

  /// Ensure a conversation exists within an active database transaction
  ///
  /// **CRITICAL FOR FORWARD SECRECY**: When a notification arrives for a new conversation,
  /// the MLS layer processes the Welcome message and decrypts the first message. If we can't
  /// save that decrypted payload because the conversation doesn't exist yet, the payload
  /// is lost forever due to MLS forward secrecy (keys are burned after first use).
  ///
  /// This method creates a minimal "placeholder" conversation record that satisfies the
  /// foreign key constraint, allowing the message to be saved. The main app will later
  /// heal this placeholder with full metadata during the next conversation list sync.
  ///
  /// - Parameters:
  ///   - db: Active GRDB database connection (within a write transaction)
  ///   - conversationID: Conversation identifier (hex-encoded group ID)
  ///   - userDID: Normalized user DID
  ///   - senderDID: Normalized sender DID (used for placeholder title)
  private static func ensureConversationExistsInTransaction(
    db: Database,
    conversationID: String,
    userDID: String,
    senderDID: String
  ) throws {
    // Check if conversation already exists
    let existingCount =
      try MLSConversationModel
      .filter(MLSConversationModel.Columns.conversationID == conversationID)
      .filter(MLSConversationModel.Columns.currentUserDID == userDID)
      .fetchCount(db)

    if existingCount > 0 {
      // Conversation exists, nothing to do
      return
    }

    // Create placeholder conversation
    // Use conversationID as groupID (they should be the same hex string)
    guard let groupIDData = Data(hexEncoded: conversationID) else {
      // If conversationID isn't valid hex, use it as UTF-8 bytes
      logger.warning(
        "⚠️ [FK-FIX] conversationID not valid hex, using UTF-8 bytes: \(conversationID.prefix(16))..."
      )
      let groupIDData = Data(conversationID.utf8)

      let placeholderTitle: String
      if senderDID.hasPrefix("did:plc:") {
        placeholderTitle = "Chat with \(senderDID.suffix(8))..."
      } else {
        placeholderTitle = senderDID.isEmpty ? "New Conversation" : senderDID
      }

      let placeholder = MLSConversationModel(
        conversationID: conversationID,
        currentUserDID: userDID,
        groupID: groupIDData,
        epoch: 0,
        joinMethod: .unknown,
        joinEpoch: 0,
        title: placeholderTitle,
        avatarURL: nil,
        createdAt: Date(),
        updatedAt: Date(),
        lastMessageAt: Date(),
        lastMembershipChangeAt: nil,
        unacknowledgedMemberChanges: 0,
        isActive: true,
        needsRejoin: false,
        rejoinRequestedAt: nil,
        lastRecoveryAttempt: nil,
        consecutiveFailures: 0,
        isPlaceholder: true
      )
      try placeholder.insert(db)

      logger.warning(
        "🆕 [FK-FIX] Created PLACEHOLDER conversation: \(conversationID.prefix(16))... (UTF-8 groupID)"
      )
      return
    }

    // Build placeholder title from sender DID
    let placeholderTitle: String
    if senderDID.hasPrefix("did:plc:") {
      placeholderTitle = "Chat with \(senderDID.suffix(8))..."
    } else {
      placeholderTitle = senderDID.isEmpty ? "New Conversation" : senderDID
    }

    let placeholder = MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: userDID,
      groupID: groupIDData,
      epoch: 0,
      joinMethod: .unknown,
      joinEpoch: 0,
      title: placeholderTitle,
      avatarURL: nil,
      createdAt: Date(),
      updatedAt: Date(),
      lastMessageAt: Date(),  // There's a message coming, so set this
      lastMembershipChangeAt: nil,
      unacknowledgedMemberChanges: 0,
      isActive: true,
      needsRejoin: false,
      rejoinRequestedAt: nil,
      lastRecoveryAttempt: nil,
      consecutiveFailures: 0,
      isPlaceholder: true  // Mark as placeholder for later healing
    )
    try placeholder.insert(db)

    logger.warning(
      "🆕 [FK-FIX] Created PLACEHOLDER conversation: \(conversationID.prefix(16))... - will be healed on next sync"
    )
  }

  /// Mark payload as expired (forward secrecy enforcement)
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - olderThan: Expire messages older than this date
  ///   - conversationID: Optional conversation filter
  public static func markPayloadExpired(
    in database: MLSDatabase,
    olderThan date: Date,
    conversationID: String? = nil
  ) async throws {
    try await database.write { db in
      if let convID = conversationID {
        // Expire for specific conversation
        try db.execute(
          sql: """
            UPDATE MLSMessageModel
            SET payloadJSON = NULL,
                payloadExpired = 1
            WHERE conversationID = ? AND timestamp < ?;
            """, arguments: [convID, date])
      } else {
        // Expire all messages older than date
        try db.execute(
          sql: """
            UPDATE MLSMessageModel
            SET payloadJSON = NULL,
                payloadExpired = 1
            WHERE timestamp < ?;
            """, arguments: [date])
      }

      logger.info("🔒 Expired payload for messages older than: \(date)")
    }
  }

  // MARK: - Batch Operations

  /// Insert multiple messages atomically
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - messages: Array of messages to insert
  public static func batchInsertMessages(
    in database: MLSDatabase,
    messages: [MLSMessageModel]
  ) async throws {
    try await database.write { db in
      for message in messages {
        try message.insert(db)
      }
      logger.info("💾 Batch inserted \(messages.count) messages")
    }
  }

  /// Insert multiple members atomically
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - members: Array of members to insert
  public static func batchInsertMembers(
    in database: MLSDatabase,
    members: [MLSMemberModel]
  ) async throws {
    try await database.write { db in
      for member in members {
        try member.insert(db)
      }
      logger.info("💾 Batch inserted \(members.count) members")
    }
  }

  // MARK: - Complex Queries

  /// Fetch messages with payload available
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user DID
  ///   - limit: Maximum number of messages
  /// - Returns: Array of messages with payload
  public static func fetchDecryptedMessages(
    from database: MLSDatabase,
    conversationID: String,
    currentUserDID: String,
    limit: Int = 50
  ) async throws -> [MLSMessageModel] {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.payloadExpired == false)
        .order(MLSMessageModel.Columns.timestamp.desc)
        .limit(limit)
        .fetchAll(db)
    }
  }

  // MARK: - Read Frontier

  /// Upsert/advance the local read frontier for a conversation.
  ///
  /// Monotonicity is enforced by `(epoch, sequenceNumber)` ordering.
  ///
  /// - Returns: `true` when the frontier was inserted/advanced, `false` when ignored.
  @discardableResult
  public static func upsertReadFrontier(
    in database: MLSDatabase,
    conversationID: String,
    currentUserDID: String,
    epoch: Int64,
    sequenceNumber: Int64,
    messageID: String? = nil
  ) async throws -> Bool {
    try await database.write { db in
      try upsertReadFrontierSync(
        in: db,
        conversationID: conversationID,
        currentUserDID: currentUserDID,
        epoch: epoch,
        sequenceNumber: sequenceNumber,
        messageID: messageID
      )
    }
  }

  /// Synchronous version of read frontier upsert for use inside write(for:) closures.
  ///
  /// - Returns: `true` when the frontier was inserted/advanced, `false` when ignored.
  @discardableResult
  public static func upsertReadFrontierSync(
    in db: Database,
    conversationID: String,
    currentUserDID: String,
    epoch: Int64,
    sequenceNumber: Int64,
    messageID: String? = nil
  ) throws -> Bool {
    let normalizedUserDID = normalizeDID(currentUserDID)
    let normalizedMessageID = normalizeOptionalMessageID(messageID)
    let now = Date()

    if let existing =
      try MLSReadFrontierModel
      .filter(MLSReadFrontierModel.Columns.conversationID == conversationID)
      .filter(MLSReadFrontierModel.Columns.currentUserDID == normalizedUserDID)
      .fetchOne(db)
    {
      let advanced = shouldAdvanceFrontier(
        existingEpoch: existing.epoch,
        existingSequenceNumber: existing.sequenceNumber,
        newEpoch: epoch,
        newSequenceNumber: sequenceNumber
      )

      let canBackfillMessageID =
        !advanced
        && existing.epoch == epoch
        && existing.sequenceNumber == sequenceNumber
        && existing.messageID == nil
        && normalizedMessageID != nil

      guard advanced || canBackfillMessageID else {
        return false
      }

      var updated = existing
      if advanced {
        updated.epoch = epoch
        updated.sequenceNumber = sequenceNumber
        updated.messageID = normalizedMessageID
      } else if canBackfillMessageID {
        updated.messageID = normalizedMessageID
      }
      updated.updatedAt = now
      try updated.update(db)
      return true
    }

    let frontier = MLSReadFrontierModel(
      conversationID: conversationID,
      currentUserDID: normalizedUserDID,
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      messageID: normalizedMessageID,
      updatedAt: now
    )
    try frontier.insert(db)
    return true
  }

  /// Fetch read frontier for a conversation.
  public static func fetchReadFrontier(
    from database: MLSDatabase,
    conversationID: String,
    currentUserDID: String
  ) async throws -> MLSReadFrontierModel? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      try MLSReadFrontierModel
        .filter(MLSReadFrontierModel.Columns.conversationID == conversationID)
        .filter(MLSReadFrontierModel.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)
    }
  }

  /// Synchronous read frontier fetch for use inside read/write closures.
  public static func fetchReadFrontierSync(
    from db: Database,
    conversationID: String,
    currentUserDID: String
  ) throws -> MLSReadFrontierModel? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try MLSReadFrontierModel
      .filter(MLSReadFrontierModel.Columns.conversationID == conversationID)
      .filter(MLSReadFrontierModel.Columns.currentUserDID == normalizedUserDID)
      .fetchOne(db)
  }

  // MARK: - Remote Read Cursor

  /// Persist the latest known read cursor for a remote participant.
  ///
  /// Cursor monotonicity is enforced when `(epoch, sequenceNumber)` metadata is available.
  /// Message identifiers may be stored independently so ordering metadata can be backfilled later.
  @discardableResult
  public static func upsertRemoteReadCursor(
    in database: MLSDatabase,
    conversationID: String,
    currentUserDID: String,
    readerDID: String,
    epoch: Int64? = nil,
    sequenceNumber: Int64? = nil,
    messageID: String? = nil
  ) async throws -> Bool {
    try await database.write { db in
      try upsertRemoteReadCursorSync(
        in: db,
        conversationID: conversationID,
        currentUserDID: currentUserDID,
        readerDID: readerDID,
        epoch: epoch,
        sequenceNumber: sequenceNumber,
        messageID: messageID
      )
    }
  }

  /// Synchronous version of remote read cursor upsert for use inside write(for:) closures.
  ///
  /// - Returns: `true` when the cursor was inserted/advanced/backfilled, `false` when ignored.
  @discardableResult
  public static func upsertRemoteReadCursorSync(
    in db: Database,
    conversationID: String,
    currentUserDID: String,
    readerDID: String,
    epoch: Int64? = nil,
    sequenceNumber: Int64? = nil,
    messageID: String? = nil
  ) throws -> Bool {
    let normalizedUserDID = normalizeDID(currentUserDID)
    let normalizedReaderDID = normalizeDID(readerDID)
    let normalizedMessageID = normalizeOptionalMessageID(messageID)
    let now = Date()

    if var existing =
      try MLSRemoteReadCursorModel
      .filter(MLSRemoteReadCursorModel.Columns.conversationID == conversationID)
      .filter(MLSRemoteReadCursorModel.Columns.currentUserDID == normalizedUserDID)
      .filter(MLSRemoteReadCursorModel.Columns.readerDID == normalizedReaderDID)
      .fetchOne(db)
    {
      let shouldAdvance = shouldAdvanceRemoteCursor(
        existingEpoch: existing.epoch,
        existingSequenceNumber: existing.sequenceNumber,
        newEpoch: epoch,
        newSequenceNumber: sequenceNumber
      )
      let canBackfillCoordinates =
        existing.epoch == nil
        && existing.sequenceNumber == nil
        && epoch != nil
        && sequenceNumber != nil
      let canBackfillMessageID = existing.messageID == nil && normalizedMessageID != nil

      guard shouldAdvance || canBackfillCoordinates || canBackfillMessageID else {
        return false
      }

      if shouldAdvance || canBackfillCoordinates {
        existing.epoch = epoch
        existing.sequenceNumber = sequenceNumber
        existing.messageID = normalizedMessageID ?? existing.messageID
      } else if canBackfillMessageID {
        existing.messageID = normalizedMessageID
      }

      existing.updatedAt = now
      try existing.update(db)
      return true
    }

    let cursor = MLSRemoteReadCursorModel(
      conversationID: conversationID,
      currentUserDID: normalizedUserDID,
      readerDID: normalizedReaderDID,
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      messageID: normalizedMessageID,
      updatedAt: now
    )
    try cursor.insert(db)
    return true
  }

  /// Fetch all persisted remote read cursors for a conversation.
  public static func fetchRemoteReadCursors(
    from database: MLSDatabase,
    conversationID: String,
    currentUserDID: String
  ) async throws -> [MLSRemoteReadCursorModel] {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      try MLSRemoteReadCursorModel
        .filter(MLSRemoteReadCursorModel.Columns.conversationID == conversationID)
        .filter(MLSRemoteReadCursorModel.Columns.currentUserDID == normalizedUserDID)
        .order(
          MLSRemoteReadCursorModel.Columns.epoch.desc,
          MLSRemoteReadCursorModel.Columns.sequenceNumber.desc,
          MLSRemoteReadCursorModel.Columns.updatedAt.desc
        )
        .fetchAll(db)
    }
  }

  /// Synchronous remote read cursor fetch for use inside read/write closures.
  public static func fetchRemoteReadCursorsSync(
    from db: Database,
    conversationID: String,
    currentUserDID: String
  ) throws -> [MLSRemoteReadCursorModel] {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try MLSRemoteReadCursorModel
      .filter(MLSRemoteReadCursorModel.Columns.conversationID == conversationID)
      .filter(MLSRemoteReadCursorModel.Columns.currentUserDID == normalizedUserDID)
      .order(
        MLSRemoteReadCursorModel.Columns.epoch.desc,
        MLSRemoteReadCursorModel.Columns.sequenceNumber.desc,
        MLSRemoteReadCursorModel.Columns.updatedAt.desc
      )
      .fetchAll(db)
  }

  /// Apply the stored read frontier to message read state.
  ///
  /// This marks late/backfilled messages as read if their `(epoch, sequenceNumber)`
  /// is at or before the local frontier.
  ///
  /// - Returns: Number of rows marked as read.
  @discardableResult
  public static func applyReadFrontierToMessages(
    in database: MLSDatabase,
    conversationID: String,
    currentUserDID: String
  ) async throws -> Int {
    try await database.write { db in
      try applyReadFrontierToMessagesSync(
        in: db,
        conversationID: conversationID,
        currentUserDID: currentUserDID
      )
    }
  }

  /// Synchronous version of read frontier application for use inside write(for:) closures.
  ///
  /// - Returns: Number of rows marked as read.
  @discardableResult
  public static func applyReadFrontierToMessagesSync(
    in db: Database,
    conversationID: String,
    currentUserDID: String
  ) throws -> Int {
    guard
      let frontier = try fetchReadFrontierSync(
        from: db,
        conversationID: conversationID,
        currentUserDID: currentUserDID
      )
    else {
      return 0
    }

    try db.execute(
      sql: """
        UPDATE MLSMessageModel
        SET isRead = 1
        WHERE conversationID = ?
          AND currentUserDID = ?
          AND isRead = 0
          AND processingError IS NULL
          AND payloadExpired = 0
          AND (
            epoch < ?
            OR (epoch = ? AND sequenceNumber <= ?)
          )
        """,
      arguments: [
        frontier.conversationID,
        frontier.currentUserDID,
        frontier.epoch,
        frontier.epoch,
        frontier.sequenceNumber,
      ]
    )
    return db.changesCount
  }

  /// Get unread message count for conversation
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user DID
  /// - Returns: Number of unread messages
  public static func getUnreadCount(
    from database: MLSDatabase,
    conversationID: String,
    currentUserDID: String
  ) async throws -> Int {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.isRead == false)
        .filter(MLSMessageModel.Columns.processingError == nil)
        .filter(MLSMessageModel.Columns.payloadExpired == false)
        .fetchCount(db)
    }
  }

  /// Mark all messages in a conversation as read
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user DID
  /// - Returns: Number of messages marked as read
  @discardableResult
  public static func markAllMessagesAsRead(
    in database: MLSDatabase,
    conversationID: String,
    currentUserDID: String
  ) async throws -> Int {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.write { db in
      try db.execute(
        sql: """
          UPDATE MLSMessageModel
          SET isRead = 1
          WHERE conversationID = ? AND currentUserDID = ? AND isRead = 0
            AND processingError IS NULL AND payloadExpired = 0
          """,
        arguments: [conversationID, normalizedUserDID]
      )
      return db.changesCount
    }
  }
  
  /// Synchronous version of markAllMessagesAsRead for use inside write(for:) closures.
  /// - Parameters:
  ///   - db: Active GRDB database connection (within a write transaction)
  ///   - conversationID: Conversation to mark as read
  ///   - currentUserDID: Current user DID
  /// - Returns: Number of messages marked as read
  public static func markAllMessagesAsReadSync(
    in db: Database,
    conversationID: String,
    currentUserDID: String
  ) throws -> Int {
    let normalizedUserDID = normalizeDID(currentUserDID)
    try db.execute(
      sql: """
        UPDATE MLSMessageModel
        SET isRead = 1
        WHERE conversationID = ? AND currentUserDID = ? AND isRead = 0
          AND processingError IS NULL AND payloadExpired = 0
        """,
      arguments: [conversationID, normalizedUserDID]
    )
    return db.changesCount
  }

  /// Fetch active conversations sorted by last message
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - currentUserDID: Current user DID
  /// - Returns: Array of active conversations
  public static func fetchActiveConversations(
    from database: MLSDatabase,
    currentUserDID: String
  ) async throws -> [MLSConversationModel] {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSConversationModel.Columns.isActive == true)
        .order(MLSConversationModel.Columns.lastMessageAt.desc)
        .fetchAll(db)
    }
  }

  /// Get unread message counts for all conversations in a single batch query.
  /// More efficient than calling getUnreadCount for each conversation.
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - currentUserDID: Current user DID
  /// - Returns: Dictionary mapping conversationID to unread count
  public static func getUnreadCountsForAllConversations(
    from database: MLSDatabase,
    currentUserDID: String
  ) async throws -> [String: Int] {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      // Use raw SQL for efficient GROUP BY query
      let rows = try Row.fetchAll(
        db,
        sql: """
          SELECT conversationID, COUNT(*) as unreadCount
          FROM MLSMessageModel
          WHERE currentUserDID = ? AND isRead = 0 AND senderID != ?
            AND processingError IS NULL AND payloadExpired = 0
          GROUP BY conversationID
          """, arguments: [normalizedUserDID, normalizedUserDID])

      var result: [String: Int] = [:]
      for row in rows {
        let conversationID: String = row["conversationID"]
        let count: Int = row["unreadCount"]
        result[conversationID] = count
      }
      return result
    }
  }
  
  /// Synchronous version for use inside read(for:) closures.
  /// - Parameters:
  ///   - db: Active GRDB database connection
  ///   - currentUserDID: Current user DID
  /// - Returns: Dictionary mapping conversationID to unread count
  public static func getUnreadCountsForAllConversationsSync(
    from db: Database,
    currentUserDID: String
  ) throws -> [String: Int] {
    let normalizedUserDID = normalizeDID(currentUserDID)
    // Use raw SQL for efficient GROUP BY query
    let rows = try Row.fetchAll(
      db,
      sql: """
        SELECT conversationID, COUNT(*) as unreadCount
        FROM MLSMessageModel
        WHERE currentUserDID = ? AND isRead = 0 AND senderID != ?
          AND processingError IS NULL AND payloadExpired = 0
        GROUP BY conversationID
        """, arguments: [normalizedUserDID, normalizedUserDID])

    var result: [String: Int] = [:]
    for row in rows {
      let conversationID: String = row["conversationID"]
      let count: Int = row["unreadCount"]
      result[conversationID] = count
    }
    return result
  }

  // MARK: - Transactions

  /// Execute multiple operations atomically
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - block: Transaction block
  public static func transaction<T: Sendable>(
    in database: MLSDatabase,
    _ block: @Sendable @escaping (Database) throws -> T
  ) async throws -> T {
    try await database.write { db in
      try block(db)
    }
  }

  // MARK: - Profile Enrichment for Notifications

  /// Update member profile info in the database
  ///
  /// This is called by the main app's profile enricher to persist fetched profile data
  /// to the MLS member table. The NSE can then query this data to show rich notifications
  /// with sender names instead of "New Message".
  ///
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - did: Member's DID
  ///   - handle: Member's handle (e.g., "alice.bsky.social")
  ///   - displayName: Member's display name (e.g., "Alice")
  ///   - currentUserDID: Current user's DID (for scoping updates)
  /// - Returns: Number of rows updated
  @discardableResult
  public static func updateMemberProfile(
    in database: MLSDatabase,
    did: String,
    handle: String?,
    displayName: String?,
    currentUserDID: String
  ) async throws -> Int {
    let normalizedDID = normalizeDID(did)
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.write { db in
      try db.execute(
        sql: """
          UPDATE MLSMemberModel
          SET handle = ?,
              displayName = ?,
              updatedAt = ?
          WHERE did = ? AND currentUserDID = ?
          """,
        arguments: [
          handle,
          displayName,
          Date(),
          normalizedDID,
          normalizedUserDID,
        ])

      let count = db.changesCount
      if count > 0 {
        logger.debug(
          "👤 Updated profile for member: \(normalizedDID.prefix(24))... (\(count) rows)")
      }
      return count
    }
  }

  /// Batch update member profiles efficiently
  ///
  /// Updates multiple member profiles in a single transaction.
  /// Called by the profile enricher after fetching a batch of profiles.
  ///
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - profiles: Array of (did, handle, displayName) tuples
  ///   - currentUserDID: Current user's DID
  /// - Returns: Total number of rows updated
  @discardableResult
  public static func batchUpdateMemberProfiles(
    in database: MLSDatabase,
    profiles: [(did: String, handle: String?, displayName: String?, avatarURL: String?)],
    currentUserDID: String
  ) async throws -> Int {
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.write { db in
      var totalUpdated = 0

      for profile in profiles {
        let normalizedDID = normalizeDID(profile.did)

        try db.execute(
          sql: """
            UPDATE MLSMemberModel
            SET handle = ?,
                displayName = ?,
                avatarURL = ?,
                updatedAt = ?
            WHERE did = ? AND currentUserDID = ?
            """,
          arguments: [
            profile.handle,
            profile.displayName,
            profile.avatarURL,
            Date(),
            normalizedDID,
            normalizedUserDID,
          ])

        totalUpdated += db.changesCount
      }

      if totalUpdated > 0 {
        logger.info("👤 Batch updated \(totalUpdated) member profile(s)")
      }
      return totalUpdated
    }
  }
  
  /// Synchronous version for use inside write(for:) closures.
  /// - Parameters:
  ///   - db: Active GRDB database connection
  ///   - profiles: Array of profiles to update
  ///   - currentUserDID: Current user DID
  /// - Returns: Number of profiles updated
  public static func batchUpdateMemberProfilesSync(
    in db: Database,
    profiles: [(did: String, handle: String?, displayName: String?, avatarURL: String?)],
    currentUserDID: String
  ) throws -> Int {
    let normalizedUserDID = normalizeDID(currentUserDID)
    var totalUpdated = 0

    for profile in profiles {
      let normalizedDID = normalizeDID(profile.did)

      try db.execute(
        sql: """
          UPDATE MLSMemberModel
          SET handle = ?,
              displayName = ?,
              avatarURL = ?,
              updatedAt = ?
          WHERE did = ? AND currentUserDID = ?
          """,
        arguments: [
          profile.handle,
          profile.displayName,
          profile.avatarURL,
          Date(),
          normalizedDID,
          normalizedUserDID,
        ])

      totalUpdated += db.changesCount
    }

    if totalUpdated > 0 {
      logger.info("👤 Batch updated \(totalUpdated) member profile(s) (sync)")
    }
    return totalUpdated
  }
}
