//
//  MLSReadFrontierModel.swift
//  CatbirdMLSCore
//
//  Tracks local per-conversation read frontier for backfilled message reconciliation.
//

import Foundation
import GRDB

/// Local read frontier for a conversation.
///
/// The frontier is monotonic by stable conversation sequenceNumber. `messageID` is optional
/// metadata for traceability when the frontier came from a specific message.
public struct MLSReadFrontierModel: Codable, FetchableRecord, PersistableRecord, Sendable {
  public static let databaseTableName = "mls_conversation_read_frontier"

  public let conversationID: String
  public let currentUserDID: String
  public var epoch: Int64
  public var sequenceNumber: Int64
  public var messageID: String?
  public var updatedAt: Date

  public init(
    conversationID: String,
    currentUserDID: String,
    epoch: Int64,
    sequenceNumber: Int64,
    messageID: String? = nil,
    updatedAt: Date = Date()
  ) {
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.epoch = epoch
    self.sequenceNumber = sequenceNumber
    self.messageID = messageID
    self.updatedAt = updatedAt
  }

  public enum Columns: String, ColumnExpression {
    case conversationID
    case currentUserDID
    case epoch
    case sequenceNumber
    case messageID
    case updatedAt
  }
}

extension MLSReadFrontierModel {
  public static func createTable(in db: Database) throws {
    try db.create(table: databaseTableName, ifNotExists: true) { t in
      t.column(Columns.conversationID.rawValue, .text).notNull()
      t.column(Columns.currentUserDID.rawValue, .text).notNull()
      t.column(Columns.epoch.rawValue, .integer).notNull()
      t.column(Columns.sequenceNumber.rawValue, .integer).notNull()
      t.column(Columns.messageID.rawValue, .text)
      t.column(Columns.updatedAt.rawValue, .datetime).notNull()
      t.primaryKey([Columns.conversationID.rawValue, Columns.currentUserDID.rawValue])
    }

    try db.create(
      index: "idx_\(databaseTableName)_user",
      on: databaseTableName,
      columns: [Columns.currentUserDID.rawValue],
      ifNotExists: true
    )

    try db.create(
      index: "idx_\(databaseTableName)_cursor",
      on: databaseTableName,
      columns: [
        Columns.currentUserDID.rawValue,
        Columns.conversationID.rawValue,
        Columns.epoch.rawValue,
        Columns.sequenceNumber.rawValue,
      ],
      ifNotExists: true
    )
  }
}
