//
//  MLSMessageOrderingModel.swift
//  CatbirdMLSCore
//
//  Database-backed message ordering to ensure messages are processed in sequence order.
//  This coordinates between NSE and main app to prevent out-of-order processing.
//
//  Created: 2024-12-26
//

import Foundation
import GRDB

// MARK: - Conversation Sequence State

/// Tracks the last processed sequence number per conversation
/// Used to ensure messages are processed in order across NSE and main app
public struct MLSConversationSequenceState: Codable, FetchableRecord, PersistableRecord, Sendable {
  public static let databaseTableName = "mls_conversation_sequence_state"
  
  /// Conversation ID (primary key with currentUserDID)
  public let conversationID: String
  
  /// Current user's DID (for multi-user isolation)
  public let currentUserDID: String
  
  /// Last successfully processed sequence number (-1 if none processed yet)
  public var lastProcessedSeq: Int64
  
  /// Timestamp of last update
  public var updatedAt: Date
  
  public init(
    conversationID: String,
    currentUserDID: String,
    lastProcessedSeq: Int64 = -1,
    updatedAt: Date = Date()
  ) {
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.lastProcessedSeq = lastProcessedSeq
    self.updatedAt = updatedAt
  }
  
  // MARK: - Column Definitions
  
  public enum Columns: String, ColumnExpression {
    case conversationID
    case currentUserDID
    case lastProcessedSeq
    case updatedAt
  }
}

// MARK: - Pending Message Buffer

/// Stores messages that arrived out of order and are waiting for predecessors
/// Both NSE and main app can add to this buffer, and either can process when ready
public struct MLSPendingMessageModel: Codable, FetchableRecord, PersistableRecord, Sendable {
  public static let databaseTableName = "mls_pending_messages"
  
  /// Unique message ID (primary key with currentUserDID)
  public let messageID: String
  
  /// Current user's DID (for multi-user isolation)
  public let currentUserDID: String
  
  /// Conversation ID this message belongs to
  public let conversationID: String
  
  /// Sequence number of this message
  public let sequenceNumber: Int64
  
  /// Epoch of this message
  public let epoch: Int64
  
  /// Full MessageView JSON for later processing
  public let messageViewJSON: Data
  
  /// When this message was received/buffered
  public let receivedAt: Date
  
  /// Number of times we've attempted to process this message
  public var processAttempts: Int
  
  /// Source of this message (for debugging)
  public let source: String  // "sse", "nse", "sync", "catchup"
  
  public init(
    messageID: String,
    currentUserDID: String,
    conversationID: String,
    sequenceNumber: Int64,
    epoch: Int64,
    messageViewJSON: Data,
    receivedAt: Date = Date(),
    processAttempts: Int = 0,
    source: String = "unknown"
  ) {
    self.messageID = messageID
    self.currentUserDID = currentUserDID
    self.conversationID = conversationID
    self.sequenceNumber = sequenceNumber
    self.epoch = epoch
    self.messageViewJSON = messageViewJSON
    self.receivedAt = receivedAt
    self.processAttempts = processAttempts
    self.source = source
  }
  
  // MARK: - Column Definitions
  
  public enum Columns: String, ColumnExpression {
    case messageID
    case currentUserDID
    case conversationID
    case sequenceNumber
    case epoch
    case messageViewJSON
    case receivedAt
    case processAttempts
    case source
  }
}

// MARK: - Database Schema Creation

extension MLSConversationSequenceState {
  /// Create the database table
  public static func createTable(in db: Database) throws {
    try db.create(table: databaseTableName, ifNotExists: true) { t in
      t.column(Columns.conversationID.rawValue, .text).notNull()
      t.column(Columns.currentUserDID.rawValue, .text).notNull()
      t.column(Columns.lastProcessedSeq.rawValue, .integer).notNull().defaults(to: -1)
      t.column(Columns.updatedAt.rawValue, .datetime).notNull()
      
      t.primaryKey([Columns.conversationID.rawValue, Columns.currentUserDID.rawValue])
    }
    
    // Index for quick lookup by user
    try db.create(
      index: "idx_\(databaseTableName)_user",
      on: databaseTableName,
      columns: [Columns.currentUserDID.rawValue],
      ifNotExists: true
    )
  }
}

extension MLSPendingMessageModel {
  /// Create the database table
  public static func createTable(in db: Database) throws {
    try db.create(table: databaseTableName, ifNotExists: true) { t in
      t.column(Columns.messageID.rawValue, .text).notNull()
      t.column(Columns.currentUserDID.rawValue, .text).notNull()
      t.column(Columns.conversationID.rawValue, .text).notNull()
      t.column(Columns.sequenceNumber.rawValue, .integer).notNull()
      t.column(Columns.epoch.rawValue, .integer).notNull()
      t.column(Columns.messageViewJSON.rawValue, .blob).notNull()
      t.column(Columns.receivedAt.rawValue, .datetime).notNull()
      t.column(Columns.processAttempts.rawValue, .integer).notNull().defaults(to: 0)
      t.column(Columns.source.rawValue, .text).notNull()
      
      t.primaryKey([Columns.messageID.rawValue, Columns.currentUserDID.rawValue])
    }
    
    // Index for finding pending messages by conversation and sequence
    try db.create(
      index: "idx_\(databaseTableName)_convo_seq",
      on: databaseTableName,
      columns: [
        Columns.currentUserDID.rawValue,
        Columns.conversationID.rawValue,
        Columns.sequenceNumber.rawValue
      ],
      ifNotExists: true
    )
    
    // Index for cleanup of old pending messages
    try db.create(
      index: "idx_\(databaseTableName)_received",
      on: databaseTableName,
      columns: [Columns.receivedAt.rawValue],
      ifNotExists: true
    )
  }
}
