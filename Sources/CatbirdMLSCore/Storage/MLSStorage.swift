//
//  MLSStorage.swift
//  Catbird
//
//  MLS SQLCipher storage layer providing CRUD operations for encrypted messages
//

import Foundation
import OSLog
import GRDB

/// MLS Storage Manager providing encrypted database operations using SQLCipher
///
/// Note: No @MainActor - database operations should run on background threads.
/// Methods are async and use GRDB's built-in concurrency handling.
public final class MLSStorage: @unchecked Sendable {

  // MARK: - Properties

  public static let shared = MLSStorage()

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSStorage")

  // MARK: - Initialization

  private init() {
    logger.info("MLSStorage initialized with SQLCipher backend")
  }

  // MARK: - Database Access

  /// Get the encrypted database for the current user
  // MARK: - Conversation Operations

  /// Ensure a conversation exists in database, creating it if necessary (idempotent)
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - groupID: MLS group ID (hex-encoded string)
  ///   - database: DatabaseQueue to use for operations
  /// - Throws: MLSStorageError if creation fails
  @discardableResult
  public   func ensureConversationExists(userDID: String, 
    conversationID: String,
    groupID: String,
    database: DatabaseQueue
  ) async throws -> String {

    // Check if conversation already exists
    let exists = try await database.read { db in
      let count = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .fetchCount(db)
      return count > 0
    }

    if exists {
      logger.debug("Conversation already exists: \(conversationID)")
      return conversationID
    }

    // Create new conversation
    try await database.write { db in
      // Convert groupID string to Data
      guard let groupIDData = Data(hexEncoded: groupID) else {
        throw MLSStorageError.invalidGroupID(groupID)
      }

      let conversation = MLSConversationModel(
        conversationID: conversationID,
        currentUserDID: userDID,
        groupID: groupIDData,
        createdAt: Date(),
        updatedAt: Date()
      )
      try conversation.insert(db)
    }

    logger.info("✅ Created conversation: \(conversationID)")
    return conversationID
  }

  /// Fetch a persisted conversation for the current user if it exists
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current authenticated user's DID
  ///   - database: Database queue to read from
  /// - Returns: Stored `MLSConversationModel` or `nil` when missing
  public   func fetchConversation(
    conversationID: String,
    currentUserDID: String,
    database: DatabaseQueue
  ) async throws -> MLSConversationModel? {
    try await database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)
    }
  }

  // MARK: - Message Plaintext Caching

  /// Save plaintext for a message after decryption
  ///
  /// **CRITICAL**: MLS ratchet burns secrets after first decryption - must cache immediately!
  ///
  /// **SECURITY MODEL**:
  /// - Plaintext stored in SQLCipher database with AES-256-CBC encryption
  /// - Per-user encryption keys stored in iOS Keychain
  /// - Database excluded from iCloud/iTunes backup
  /// - iOS Data Protection (FileProtectionType.complete) for at-rest security
  ///
  /// - Parameters:
  ///   - messageID: Unique message identifier
  ///   - conversationID: Conversation this message belongs to
  ///   - plaintext: Decrypted message text
  ///   - senderID: DID of message sender
  ///   - currentUserDID: DID of current user
  ///   - embed: Optional embed data (GIF, Bluesky post, etc.)
  ///   - epoch: MLS epoch number
  ///   - sequenceNumber: MLS sequence number within epoch
  ///   - timestamp: Message timestamp
  ///   - database: DatabaseQueue to use for operations
  /// - Throws: MLSStorageError if save fails
  public   func savePlaintextForMessage(
    messageID: String,
    conversationID: String,
    plaintext: String,
    senderID: String,
    currentUserDID: String,
    embed: MLSEmbedData? = nil,
    epoch: Int64,
    sequenceNumber: Int64,
    timestamp: Date,
    database: DatabaseQueue,
    processingError: String? = nil,
    validationFailureReason: String? = nil
  ) async throws {
    logger.info("💾 Caching plaintext: \(messageID) (epoch: \(epoch), seq: \(sequenceNumber), hasEmbed: \(embed != nil), hasError: \(processingError != nil))")

    // Encode embed data if provided
    let embedDataEncoded: Data?
    if let embed = embed {
      let encoder = JSONEncoder()
      encoder.dateEncodingStrategy = .iso8601
      embedDataEncoded = try encoder.encode(embed)
      logger.debug("Encoded embed data (\(embedDataEncoded?.count ?? 0) bytes)")
    } else {
      embedDataEncoded = nil
    }

    try await database.write { db in
      // Check if message exists
      let count = try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == currentUserDID)
        .fetchCount(db)
      let exists = count > 0

      if exists {
        // Update existing message
        try db.execute(sql: """
          UPDATE MLSMessageModel
          SET plaintext = ?,
              embedData = ?,
              senderID = ?,
              epoch = ?,
              sequenceNumber = ?,
              timestamp = ?,
              plaintextExpired = 0,
              processingError = ?,
              processingAttempts = processingAttempts + 1,
              validationFailureReason = ?
          WHERE messageID = ? AND currentUserDID = ?;
        """, arguments: [
          plaintext,
          embedDataEncoded,
          senderID,
          epoch,
          sequenceNumber,
          timestamp,
          processingError,
          validationFailureReason,
          messageID,
          currentUserDID
        ])

        logger.debug("Updated existing message with plaintext cache")
      } else {
        // Create new message
        let message = MLSMessageModel(
          messageID: messageID,
          currentUserDID: currentUserDID,
          conversationID: conversationID,
          senderID: senderID,
          plaintext: plaintext,
          embedDataJSON: embedDataEncoded,
          wireFormat: Data(),
          contentType: "text/plain",
          timestamp: timestamp,
          epoch: epoch,
          sequenceNumber: sequenceNumber,
          authenticatedData: nil,
          signature: nil,
          isDelivered: true,
          isRead: false,
          isSent: true,
          sendAttempts: 0,
          error: nil,
          processingState: "cached",
          gapBefore: false,
          plaintextExpired: false,
          processingError: processingError,
          processingAttempts: processingError != nil ? 1 : 0,
          validationFailureReason: validationFailureReason
        )
        try message.insert(db)

        logger.debug("Created new message with plaintext cache")
      }
    }

    logger.info("✅ Plaintext cached: \(messageID)")
  }

  /// Fetch cached plaintext for a message
  ///
  /// Returns cached plaintext if available, or nil if message hasn't been decrypted yet.
  /// This prevents re-decryption attempts that would fail with SecretReuseError.
  ///
  /// - Parameters:
  ///   - messageID: The message ID to fetch
  ///   - currentUserDID: The DID of the current user
  ///   - database: DatabaseQueue to use for operations
  /// - Returns: Cached plaintext if available, nil otherwise
  /// - Throws: MLSStorageError if fetch fails
  public   func fetchPlaintextForMessage(
    _ messageID: String,
    currentUserDID: String,
    database: DatabaseQueue
  ) async throws -> String? {
    logger.debug("Fetching plaintext: \(messageID)")

    let plaintext = try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)?.plaintext
    }

    if let plaintext = plaintext {
      logger.debug("✅ Found cached plaintext: \(messageID)")
      return plaintext
    } else {
      logger.warning("⚠️ No cached plaintext found: \(messageID)")
      return nil
    }
  }

  /// Fetch cached embed data for a message
  ///
  /// Returns cached embed if available, or nil if no embed was cached.
  ///
  /// - Parameters:
  ///   - messageID: The message ID to fetch
  ///   - currentUserDID: The DID of the current user
  ///   - database: DatabaseQueue to use for operations
  /// - Returns: Cached embed data if available, nil otherwise
  /// - Throws: MLSStorageError if fetch fails
  public   func fetchEmbedForMessage(
    _ messageID: String,
    currentUserDID: String,
    database: DatabaseQueue
  ) async throws -> MLSEmbedData? {
    logger.debug("Fetching embed: \(messageID)")

    let embed = try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)?.parsedEmbed
    }

    if embed != nil {
      logger.debug("✅ Found cached embed: \(messageID)")
    }

    return embed
  }

  /// Fetch cached sender DID for a message
  ///
  /// Returns the sender's DID extracted from MLS credentials during decryption.
  ///
  /// - Parameters:
  ///   - messageID: The message ID to fetch
  ///   - currentUserDID: The DID of the current user
  ///   - database: DatabaseQueue to use for operations
  /// - Returns: Sender DID if available, nil otherwise
  /// - Throws: MLSStorageError if fetch fails
  public   func fetchSenderForMessage(
    _ messageID: String,
    currentUserDID: String,
    database: DatabaseQueue
  ) async throws -> String? {
    logger.debug("Fetching sender: \(messageID)")

    let senderID = try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)?.senderID
    }

    if let senderID = senderID {
      logger.debug("✅ Found cached sender: \(messageID) -> \(senderID)")
      return senderID
    } else {
      logger.warning("⚠️ No cached sender found: \(messageID)")
      return nil
    }
  }

  /// Fetch the most recent cached messages for a conversation.
  ///
  /// Returns up to `limit` messages sorted for display (oldest → newest) while prioritizing
  /// the newest decrypted records when trimming large conversations.
  /// Useful for cache-first display before fetching from server.
  ///
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: The DID of the current user
  ///   - database: DatabaseQueue to use for operations
  ///   - limit: Maximum number of messages to return (default: 50)
  /// - Returns: Array of cached messages sorted from oldest to newest
  /// - Throws: MLSStorageError if fetch fails
  public   func fetchMessagesForConversation(
    _ conversationID: String,
    currentUserDID: String,
    database: DatabaseQueue,
    limit: Int = 50
  ) async throws -> [MLSMessageModel] {
    logger.info("🔍 [DB] Fetching cached messages - conversationID: \(conversationID), currentUserDID: \(currentUserDID), limit: \(limit)")

    let messages = try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == currentUserDID)
        .order(MLSMessageModel.Columns.epoch.desc, MLSMessageModel.Columns.sequenceNumber.desc)
        .limit(limit)
        .fetchAll(db)
    }

    logger.info("📦 [DB] Query returned \(messages.count) messages")
    if !messages.isEmpty {
      for msg in messages {
        logger.debug("  - Message \(msg.messageID): epoch=\(msg.epoch), seq=\(msg.sequenceNumber), hasPlaintext=\(msg.plaintext != nil), sender=\(msg.senderID)")
      }
    }

    let orderedMessages = Array(messages.reversed())
    logger.info("✅ [DB] Returning \(orderedMessages.count) cached messages (oldest→newest)")
    return orderedMessages
  }

  /// Fetch messages older than a specific epoch/sequence for pagination
  public   func fetchMessagesBeforeSequence(
    conversationId: String,
    currentUserDID: String,
    beforeEpoch: Int64,
    beforeSeq: Int64,
    database: DatabaseQueue,
    limit: Int = 50
  ) async throws -> [MLSMessageModel] {
    logger.info("🔍 [DB] Fetching older messages - conversationID: \(conversationId), currentUserDID: \(currentUserDID), beforeEpoch: \(beforeEpoch), beforeSeq: \(beforeSeq), limit: \(limit)")

    let messages = try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationId)
        .filter(MLSMessageModel.Columns.currentUserDID == currentUserDID)
        .filter(
          MLSMessageModel.Columns.epoch < beforeEpoch ||
          (MLSMessageModel.Columns.epoch == beforeEpoch && MLSMessageModel.Columns.sequenceNumber < beforeSeq)
        )
        .order(MLSMessageModel.Columns.epoch.desc, MLSMessageModel.Columns.sequenceNumber.desc)
        .limit(limit)
        .fetchAll(db)
    }

    logger.info("📦 [DB] Query returned \(messages.count) older messages")
    if !messages.isEmpty {
      for msg in messages {
        logger.debug("  - Older message \(msg.messageID): epoch=\(msg.epoch), seq=\(msg.sequenceNumber), hasPlaintext=\(msg.plaintext != nil)")
      }
    }

    let orderedMessages = Array(messages.reversed())
    logger.info("✅ [DB] Returning \(orderedMessages.count) older messages (oldest→newest)")
    return orderedMessages
  }

  // MARK: - Epoch Key Management

  /// Store epoch secret with actual key material
  /// - Parameters:
  ///   - conversationID: Hex-encoded conversation/group ID
  ///   - epoch: MLS epoch number
  ///   - secretData: Epoch secret key material
  ///   - database: DatabaseQueue to use for operations
  /// - Throws: MLSStorageError if save fails
  public   func saveEpochSecret(userDID: String, 
    conversationID: String,
    epoch: UInt64,
    secretData: Data,
    database: DatabaseQueue
  ) async throws {

    do {
      try await database.write { db in
        let epochKey = MLSEpochKeyModel(
          epochKeyID: "\(conversationID)-\(epoch)",
          conversationID: conversationID,
          currentUserDID: userDID,
          epoch: Int64(epoch),
          keyMaterial: secretData,  // Actual epoch secret
          createdAt: Date(),
          expiresAt: nil,
          isActive: true
        )
        // ⭐ CRITICAL FIX: Use save() instead of insert() to handle duplicate epoch exports
        // This allows epoch 0 to be exported both at group creation AND before merge_pending_commit
        // without hitting UNIQUE constraint violations
        try epochKey.save(db)
      }

      logger.info("✅ Saved epoch secret: \(conversationID) epoch \(epoch), \(secretData.count) bytes")
    } catch let error as DatabaseError {
      // ⭐ FIXED: Foreign key violations should NEVER occur now that we create
      // the SQLCipher conversation record BEFORE creating the MLS group
      if error.resultCode == .SQLITE_CONSTRAINT && error.message?.contains("FOREIGN KEY") == true {
        logger.error("❌ [EPOCH-STORAGE] CRITICAL: Foreign key violation storing epoch secret - conversation \(conversationID.prefix(16))... not found. This indicates a bug in conversation creation order!")
        throw MLSStorageError.foreignKeyViolation("Conversation \(conversationID) must exist before storing epoch secrets")
      }
      // Re-throw all database errors
      throw error
    }
  }

  /// Retrieve epoch secret key material
  /// - Parameters:
  ///   - conversationID: Hex-encoded conversation/group ID
  ///   - epoch: MLS epoch number
  ///   - database: DatabaseQueue to use for operations
  /// - Returns: Epoch secret data if found, nil otherwise
  public   func getEpochSecret(userDID: String, 
    conversationID: String,
    epoch: UInt64,
    database: DatabaseQueue
  ) async throws -> Data? {

    let secret = try await database.read { db in
      try MLSEpochKeyModel
        .filter(MLSEpochKeyModel.Columns.conversationID == conversationID)
        .filter(MLSEpochKeyModel.Columns.currentUserDID == userDID)
        .filter(MLSEpochKeyModel.Columns.epoch == Int64(epoch))
        .filter(MLSEpochKeyModel.Columns.isActive == true)
        .fetchOne(db)?
        .keyMaterial
    }

    if let secret = secret {
      logger.debug("Retrieved epoch secret: \(conversationID) epoch \(epoch), \(secret.count) bytes")
    } else {
      logger.debug("No epoch secret found: \(conversationID) epoch \(epoch)")
    }

    return secret
  }

  /// Delete epoch secret
  /// - Parameters:
  ///   - conversationID: Hex-encoded conversation/group ID
  ///   - epoch: MLS epoch number
  ///   - database: DatabaseQueue to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public   func deleteEpochSecret(userDID: String, 
    conversationID: String,
    epoch: UInt64,
    database: DatabaseQueue
  ) async throws {

    try await database.write { db in
      let now = Date()
      try db.execute(sql: """
        UPDATE MLSEpochKeyModel
        SET deletedAt = ?, isActive = ?
        WHERE conversationID = ? AND currentUserDID = ? AND epoch = ?;
      """, arguments: [now, false, conversationID, userDID, Int64(epoch)])
    }

    logger.info("Deleted epoch secret: \(conversationID) epoch \(epoch)")
  }

  /// Record an epoch key for forward secrecy tracking (deprecated - use saveEpochSecret)
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - epoch: MLS epoch number
  ///   - userDID: User's decentralized identifier
  ///   - database: DatabaseQueue to use for operations
  /// - Throws: MLSStorageError if save fails
  @available(*, deprecated, message: "Use saveEpochSecret instead")
  public   func recordEpochKey(
    conversationID: String,
    epoch: Int64,
    userDID: String,
    database: DatabaseQueue
  ) async throws {

    try await database.write { db in
      let epochKey = MLSEpochKeyModel(
        epochKeyID: "\(conversationID)-\(epoch)",
        conversationID: conversationID,
        currentUserDID: userDID,
        epoch: epoch,
        keyMaterial: Data(), // Placeholder - actual key material should be provided
        createdAt: Date(),
        expiresAt: nil,
        isActive: true
      )
      try epochKey.insert(db)
    }

    logger.info("Recorded epoch key: \(conversationID) epoch \(epoch)")
  }

  /// Delete old epoch keys, keeping only the most recent ones
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - userDID: User's decentralized identifier
  ///   - keepLast: Number of recent keys to keep
  ///   - database: DatabaseQueue to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public   func deleteOldEpochKeys(
    conversationID: String,
    userDID: String,
    keepLast: Int,
    database: DatabaseQueue
  ) async throws {

    try await database.write { db in
      // Get all epoch keys for this conversation
      let allKeys = try MLSEpochKeyModel
        .filter(MLSEpochKeyModel.Columns.conversationID == conversationID)
        .filter(MLSEpochKeyModel.Columns.currentUserDID == userDID)
        .filter(MLSEpochKeyModel.Columns.isActive == true)
        .order(MLSEpochKeyModel.Columns.epoch.desc)
        .fetchAll(db)

      guard allKeys.count > keepLast else {
        logger.debug("No old epoch keys to delete")
        return
      }

      // Mark old keys for deletion
      let keysToDelete = allKeys.dropFirst(keepLast)
      let now = Date()

      for key in keysToDelete {
        try db.execute(sql: """
          UPDATE MLSEpochKeyModel
          SET deletedAt = ?
          WHERE conversationID = ? AND currentUserDID = ? AND epoch = ?;
        """, arguments: [now, conversationID, userDID, key.epoch])
      }

      logger.info("Marked \(keysToDelete.count) epoch keys for deletion")
    }
  }

  /// Clean up old message keys older than specified date
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - date: Delete messages older than this date
  ///   - database: DatabaseQueue to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public   func cleanupMessageKeys(
    userDID: String,
    olderThan date: Date,
    database: DatabaseQueue
  ) async throws {

    let deletedCount = try await database.write { db -> Int in
      try db.execute(sql: """
        DELETE FROM MLSMessageModel
        WHERE currentUserDID = ? AND timestamp < ?;
      """, arguments: [userDID, date])

      return db.changesCount
    }

    logger.info("Cleaned up \(deletedCount) message keys older than \(date)")
  }

  /// Delete epoch keys that have been marked for deletion
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - database: DatabaseQueue to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public   func deleteMarkedEpochKeys(
    userDID: String,
    database: DatabaseQueue
  ) async throws {

    let deletedCount = try await database.write { db -> Int in
      try db.execute(sql: """
        DELETE FROM MLSEpochKeyModel
        WHERE currentUserDID = ? AND deletedAt IS NOT NULL;
      """, arguments: [userDID])

      return db.changesCount
    }

    logger.info("Deleted \(deletedCount) marked epoch keys")
  }

  /// Delete expired key packages
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - database: DatabaseQueue to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public   func deleteExpiredKeyPackages(
    userDID: String,
    database: DatabaseQueue
  ) async throws {

    let now = Date()
    let deletedCount = try await database.write { db -> Int in
      try db.execute(sql: """
        DELETE FROM MLSKeyPackageModel
        WHERE currentUserDID = ? AND expiresAt IS NOT NULL AND expiresAt < ?;
      """, arguments: [userDID, now])

      return db.changesCount
    }

    logger.info("Deleted \(deletedCount) expired key packages")
  }

  // MARK: - Member Queries

  /// Get count of active members in a conversation
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user DID
  ///   - database: DatabaseQueue to use for operations
  /// - Returns: Number of active members
  /// - Throws: MLSStorageError if query fails
  public   func getMemberCount(
    conversationID: String,
    currentUserDID: String,
    database: DatabaseQueue
  ) async throws -> Int {
    return try await database.read { db in
      try MLSMemberModel
        .filter(MLSMemberModel.Columns.conversationID == conversationID)
        .filter(MLSMemberModel.Columns.currentUserDID == currentUserDID)
        .filter(MLSMemberModel.Columns.isActive == true)
        .fetchCount(db)
    }
  }

  /// Fetch active members for a conversation
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user DID
  ///   - database: DatabaseQueue to use for operations
  /// - Returns: Array of active members
  /// - Throws: MLSStorageError if query fails
  public func fetchMembers(
    conversationID: String,
    currentUserDID: String,
    database: DatabaseQueue
  ) async throws -> [MLSMemberModel] {
    return try await database.read { db in
      try MLSMemberModel
        .filter(MLSMemberModel.Columns.conversationID == conversationID)
        .filter(MLSMemberModel.Columns.currentUserDID == currentUserDID)
        .filter(MLSMemberModel.Columns.isActive == true)
        .order(MLSMemberModel.Columns.addedAt)
        .fetchAll(db)
    }
  }

  /// Upsert members for a conversation (replaces existing members)
  /// - Parameters:
  ///   - members: Array of member models to persist
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user DID
  ///   - database: DatabaseQueue to use for operations
  /// - Throws: MLSStorageError if operation fails
  public func upsertMembers(
    _ members: [MLSMemberModel],
    conversationID: String,
    currentUserDID: String,
    database: DatabaseQueue
  ) async throws {
    try await database.write { db in
      // Mark all existing members as inactive first
      try db.execute(
        sql: """
          UPDATE MLSMemberModel
          SET isActive = 0, removedAt = ?, updatedAt = ?
          WHERE conversationID = ? AND currentUserDID = ? AND isActive = 1
          """,
        arguments: [Date(), Date(), conversationID, currentUserDID]
      )

      // Insert or update the provided members
      for member in members {
        try member.save(db)
      }
    }

    logger.info("✅ Upserted \(members.count) members for conversation: \(conversationID)")
  }

  // MARK: - Reports (Feature Removed - Admin roster and reports not implemented)

  // These methods reference MLSReportModel and MLSAdminRosterModel which don't exist
}

// MARK: - Errors

enum MLSStorageError: LocalizedError {
  case noAuthentication
  case conversationNotFound(String)
  case memberNotFound(String)
  case messageNotFound(String)
  case keyPackageNotFound(String)
  case invalidGroupID(String)
  case saveFailed(Error)
  case foreignKeyViolation(String)

  var errorDescription: String? {
    switch self {
    case .noAuthentication:
      return "No authenticated user"
    case .conversationNotFound(let id):
      return "Conversation not found: \(id)"
    case .memberNotFound(let id):
      return "Member not found: \(id)"
    case .messageNotFound(let id):
      return "Message not found: \(id)"
    case .keyPackageNotFound(let id):
      return "Key package not found: \(id)"
    case .invalidGroupID(let id):
      return "Invalid group ID format: \(id)"
    case .saveFailed(let error):
      return "Failed to save: \(error.localizedDescription)"
    case .foreignKeyViolation(let message):
      return "Foreign key constraint violation: \(message)"
    }
  }
}
