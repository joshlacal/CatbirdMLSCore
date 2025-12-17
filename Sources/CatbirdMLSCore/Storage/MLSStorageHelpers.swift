//
//  MLSStorageHelpers.swift
//  Catbird
//
//  Critical MLS storage operations for GRDB
//  Provides plaintext caching, transactions, and complex queries
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

  // MARK: - Critical Operations

  /// Save plaintext immediately after MLS decryption
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
  ///   - plaintext: Decrypted message text
  ///   - conversationID: Conversation identifier for the message
  ///   - currentUserDID: DID of the current user (scopes update/insert)
  ///   - senderID: Sender DID extracted from MLS credentials
  ///   - embedDataJSON: Optional rich embed data (as JSON Data)
  ///   - epoch: MLS epoch number
  ///   - sequenceNumber: Message sequence in epoch
  public static func savePlaintext(
    in database: MLSDatabase,
    messageID: String,
    conversationID: String,
    currentUserDID: String,
    plaintext: String,
    senderID: String,
    embedDataJSON: Data? = nil,
    epoch: Int64,
    sequenceNumber: Int64,
    timestamp: Date = Date()
  ) async throws {
    // Normalize DIDs for consistent storage
    let normalizedUserDID = normalizeDID(currentUserDID)
    let normalizedSenderID = normalizeDID(senderID)
    
    try await database.write { db in
      // Update message with plaintext using GRDB QueryInterface
      if let embedDataJSON = embedDataJSON {
        try db.execute(sql: """
          UPDATE MLSMessageModel
          SET plaintext = ?,
              embedDataJSON = ?,
              senderID = ?,
              epoch = ?,
              sequenceNumber = ?,
              plaintextExpired = 0
          WHERE messageID = ? AND currentUserDID = ?;
        """, arguments: [
          plaintext,
          embedDataJSON,
          normalizedSenderID,
          epoch,
          sequenceNumber,
          messageID,
          normalizedUserDID,
        ])
      } else {
        try db.execute(sql: """
          UPDATE MLSMessageModel
          SET plaintext = ?,
              senderID = ?,
              epoch = ?,
              sequenceNumber = ?,
              plaintextExpired = 0
          WHERE messageID = ? AND currentUserDID = ?;
        """, arguments: [
          plaintext,
          normalizedSenderID,
          epoch,
          sequenceNumber,
          messageID,
          normalizedUserDID,
        ])
      }

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
          plaintext: plaintext,
          embedDataJSON: embedDataJSON,
          wireFormat: nil,
          contentType: "text/plain",
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
          plaintextExpired: false,
          processingError: nil,
          processingAttempts: 0,
          validationFailureReason: nil
        )

        try message.insert(db)
      }

      logger.info("💾 Cached plaintext for message: \(messageID)")
    }
  }
  
  // MARK: - Foreign Key Fix Helpers
  
  /// Ensure a conversation exists within an active database transaction
  ///
  /// **CRITICAL FOR FORWARD SECRECY**: When a notification arrives for a new conversation,
  /// the MLS layer processes the Welcome message and decrypts the first message. If we can't
  /// save that decrypted plaintext because the conversation doesn't exist yet, the plaintext
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
    let existingCount = try MLSConversationModel
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
      logger.warning("⚠️ [FK-FIX] conversationID not valid hex, using UTF-8 bytes: \(conversationID.prefix(16))...")
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
      
      logger.warning("🆕 [FK-FIX] Created PLACEHOLDER conversation: \(conversationID.prefix(16))... (UTF-8 groupID)")
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
    
    logger.warning("🆕 [FK-FIX] Created PLACEHOLDER conversation: \(conversationID.prefix(16))... - will be healed on next sync")
  }

  /// Mark plaintext as expired (forward secrecy enforcement)
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - olderThan: Expire messages older than this date
  ///   - conversationID: Optional conversation filter
  public static func markPlaintextExpired(
    in database: MLSDatabase,
    olderThan date: Date,
    conversationID: String? = nil
  ) async throws {
    try await database.write { db in
      if let convID = conversationID {
        // Expire for specific conversation
        try db.execute(sql: """
          UPDATE MLSMessageModel
          SET plaintext = NULL,
              embedDataJSON = NULL,
              plaintextExpired = 1
          WHERE conversationID = ? AND timestamp < ?;
        """, arguments: [convID, date])
      } else {
        // Expire all messages older than date
        try db.execute(sql: """
          UPDATE MLSMessageModel
          SET plaintext = NULL,
              embedDataJSON = NULL,
              plaintextExpired = 1
          WHERE timestamp < ?;
        """, arguments: [date])
      }

      logger.info("🔒 Expired plaintext for messages older than: \(date)")
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

  /// Fetch messages with plaintext available
  /// - Parameters:
  ///   - database: GRDB DatabaseQueue
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user DID
  ///   - limit: Maximum number of messages
  /// - Returns: Array of messages with plaintext
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
        .filter(MLSMessageModel.Columns.plaintextExpired == false)
        .order(MLSMessageModel.Columns.timestamp.desc)
        .limit(limit)
        .fetchAll(db)
    }
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
          """,
        arguments: [conversationID, normalizedUserDID]
      )
      return db.changesCount
    }
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
      let rows = try Row.fetchAll(db, sql: """
        SELECT conversationID, COUNT(*) as unreadCount
        FROM MLSMessageModel
        WHERE currentUserDID = ? AND isRead = 0 AND senderID != ?
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
      try db.execute(sql: """
        UPDATE MLSMemberModel
        SET handle = ?,
            displayName = ?,
            updatedAt = ?
        WHERE did = ? AND currentUserDID = ?
      """, arguments: [
        handle,
        displayName,
        Date(),
        normalizedDID,
        normalizedUserDID
      ])
      
      let count = db.changesCount
      if count > 0 {
        logger.debug("👤 Updated profile for member: \(normalizedDID.prefix(24))... (\(count) rows)")
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
    profiles: [(did: String, handle: String?, displayName: String?)],
    currentUserDID: String
  ) async throws -> Int {
    let normalizedUserDID = normalizeDID(currentUserDID)
    
    return try await database.write { db in
      var totalUpdated = 0
      
      for profile in profiles {
        let normalizedDID = normalizeDID(profile.did)
        
        try db.execute(sql: """
          UPDATE MLSMemberModel
          SET handle = ?,
              displayName = ?,
              updatedAt = ?
          WHERE did = ? AND currentUserDID = ?
        """, arguments: [
          profile.handle,
          profile.displayName,
          Date(),
          normalizedDID,
          normalizedUserDID
        ])
        
        totalUpdated += db.changesCount
      }
      
      if totalUpdated > 0 {
        logger.info("👤 Batch updated \(totalUpdated) member profile(s)")
      }
      return totalUpdated
    }
  }
}

// MARK: - MLSEmbedData Codable Helpers

extension MLSEmbedData {
  /// Encode to Data for storage
  func encoded() -> Data? {
    try? JSONEncoder().encode(self)
  }

  /// Decode from Data
  public static func decoded(from data: Data?) -> MLSEmbedData? {
    guard let data = data else { return nil }
    return try? JSONDecoder().decode(MLSEmbedData.self, from: data)
  }
}
