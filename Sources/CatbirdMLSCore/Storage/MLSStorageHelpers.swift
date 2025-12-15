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
