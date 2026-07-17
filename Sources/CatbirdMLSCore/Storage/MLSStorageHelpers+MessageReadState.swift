//
//  MLSStorageHelpers+MessageReadState.swift
//  CatbirdMLSCore
//
//  Per-message read/unread state. Mirrors the queue/sync conventions of
//  `markAllMessagesAsRead` / `applyReadFrontierToMessagesSync` in
//  MLSStorageHelpers.swift — async entry points normalize the DID and hand
//  off to a `Sync` sibling usable inside an existing `write(for:)` closure.
//

import CatbirdMLS
import Foundation
import GRDB

extension MLSStorageHelpers {

  // MARK: - Mark Read (cascades to earlier sequence numbers)

  /// Mark a single message as read, along with every earlier-sequenced
  /// message in the same conversation ("messages-domain semantics" — reading
  /// a message implies everything before it has been seen too).
  ///
  /// - Parameters:
  ///   - database: GRDB database writer
  ///   - messageID: The message the caller actually read
  ///   - conversationID: Conversation the message belongs to
  ///   - currentUserDID: Current user DID
  /// - Returns: Number of rows flipped from unread to read
  @discardableResult
  public static func markMessageAsRead(
    in database: MLSDatabase,
    messageID: String,
    conversationID: String,
    currentUserDID: String
  ) async throws -> Int {
    try await database.write { db in
      try markMessageAsReadSync(
        in: db,
        messageID: messageID,
        conversationID: conversationID,
        currentUserDID: currentUserDID
      )
    }
  }

  /// Synchronous version of `markMessageAsRead` for use inside an existing
  /// `write(for:)` closure.
  @discardableResult
  public static func markMessageAsReadSync(
    in db: Database,
    messageID: String,
    conversationID: String,
    currentUserDID: String
  ) throws -> Int {
    let normalizedUserDID = normalizeDID(currentUserDID)

    // Anchor on the target message's own sequence number so the cascade is
    // scoped to "everything at or before what was actually read" rather than
    // blindly marking the whole conversation.
    guard
      let targetSequenceNumber = try Int64.fetchOne(
        db,
        sql: """
          SELECT sequenceNumber FROM MLSMessageModel
          WHERE messageID = ? AND conversationID = ? AND currentUserDID = ?
          """,
        arguments: [messageID, conversationID, normalizedUserDID]
      )
    else {
      return 0
    }

    try db.execute(
      sql: """
        UPDATE MLSMessageModel
        SET isRead = 1
        WHERE conversationID = ? AND currentUserDID = ? AND isRead = 0
          AND processingError IS NULL AND payloadExpired = 0
          AND sequenceNumber <= ?
        """,
      arguments: [conversationID, normalizedUserDID, targetSequenceNumber]
    )
    return db.changesCount
  }

  // MARK: - Mark Unread (single row only)

  /// Mark a single message as unread. Unlike `markMessageAsRead`, this does
  /// NOT cascade — only the targeted row is affected.
  ///
  /// - Parameters:
  ///   - database: GRDB database writer
  ///   - messageID: The message to mark unread
  ///   - conversationID: Conversation the message belongs to
  ///   - currentUserDID: Current user DID
  /// - Returns: 1 if the row was flipped from read to unread, 0 otherwise
  @discardableResult
  public static func markMessageAsUnread(
    in database: MLSDatabase,
    messageID: String,
    conversationID: String,
    currentUserDID: String
  ) async throws -> Int {
    try await database.write { db in
      try markMessageAsUnreadSync(
        in: db,
        messageID: messageID,
        conversationID: conversationID,
        currentUserDID: currentUserDID
      )
    }
  }

  /// Synchronous version of `markMessageAsUnread` for use inside an existing
  /// `write(for:)` closure.
  @discardableResult
  public static func markMessageAsUnreadSync(
    in db: Database,
    messageID: String,
    conversationID: String,
    currentUserDID: String
  ) throws -> Int {
    let normalizedUserDID = normalizeDID(currentUserDID)
    try db.execute(
      sql: """
        UPDATE MLSMessageModel
        SET isRead = 0
        WHERE messageID = ? AND conversationID = ? AND currentUserDID = ? AND isRead = 1
        """,
      arguments: [messageID, conversationID, normalizedUserDID]
    )
    return db.changesCount
  }

  // MARK: - Newest Message Lookup

  /// Whether `messageID` carries the highest `sequenceNumber` of any message
  /// in the conversation (i.e. it is the newest message). Used to decide
  /// whether marking a message read should also advance the server read
  /// cursor.
  ///
  /// - Returns: `false` if `messageID` cannot be found.
  public static func isNewestMessage(
    in database: MLSDatabase,
    messageID: String,
    conversationID: String,
    currentUserDID: String
  ) async throws -> Bool {
    try await database.read { db in
      try isNewestMessageSync(
        in: db,
        messageID: messageID,
        conversationID: conversationID,
        currentUserDID: currentUserDID
      )
    }
  }

  /// Synchronous version of `isNewestMessage` for use inside an existing
  /// `read`/`write(for:)` closure.
  public static func isNewestMessageSync(
    in db: Database,
    messageID: String,
    conversationID: String,
    currentUserDID: String
  ) throws -> Bool {
    let normalizedUserDID = normalizeDID(currentUserDID)

    guard
      let targetSequenceNumber = try Int64.fetchOne(
        db,
        sql: """
          SELECT sequenceNumber FROM MLSMessageModel
          WHERE messageID = ? AND conversationID = ? AND currentUserDID = ?
          """,
        arguments: [messageID, conversationID, normalizedUserDID]
      )
    else {
      return false
    }

    let maxSequenceNumber =
      try Int64.fetchOne(
        db,
        sql: """
          SELECT MAX(sequenceNumber) FROM MLSMessageModel
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [conversationID, normalizedUserDID]
      ) ?? Int64.min

    return targetSequenceNumber >= maxSequenceNumber
  }
}
