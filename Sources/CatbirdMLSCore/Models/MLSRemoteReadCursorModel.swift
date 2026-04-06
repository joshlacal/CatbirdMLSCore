//
//  MLSRemoteReadCursorModel.swift
//  CatbirdMLSCore
//
//  Persists remote participants' read cursors so read indicators survive reloads.
//

import Foundation
import GRDB

/// Tracks where a remote participant has read up to in a conversation.
///
/// Rows are scoped by the current user's database identity and keyed by
/// `(conversationID, currentUserDID, readerDID)`. Cursor coordinates are optional
/// so we can temporarily persist a `messageID` even before its ordering metadata is
/// available locally.
public struct MLSRemoteReadCursorModel: Codable, FetchableRecord, PersistableRecord, Sendable {
  public static let databaseTableName = "mls_remote_read_cursor"

  public let conversationID: String
  public let currentUserDID: String
  public let readerDID: String
  public var epoch: Int64?
  public var sequenceNumber: Int64?
  public var messageID: String?
  public var updatedAt: Date

  public init(
    conversationID: String,
    currentUserDID: String,
    readerDID: String,
    epoch: Int64? = nil,
    sequenceNumber: Int64? = nil,
    messageID: String? = nil,
    updatedAt: Date = Date()
  ) {
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.readerDID = readerDID
    self.epoch = epoch
    self.sequenceNumber = sequenceNumber
    self.messageID = messageID
    self.updatedAt = updatedAt
  }

  public enum Columns: String, ColumnExpression {
    case conversationID
    case currentUserDID
    case readerDID
    case epoch
    case sequenceNumber
    case messageID
    case updatedAt
  }
}

extension MLSRemoteReadCursorModel {
  public static func createTable(in db: Database) throws {
    try db.create(table: databaseTableName, ifNotExists: true) { t in
      t.column(Columns.conversationID.rawValue, .text).notNull()
      t.column(Columns.currentUserDID.rawValue, .text).notNull()
      t.column(Columns.readerDID.rawValue, .text).notNull()
      t.column(Columns.epoch.rawValue, .integer)
      t.column(Columns.sequenceNumber.rawValue, .integer)
      t.column(Columns.messageID.rawValue, .text)
      t.column(Columns.updatedAt.rawValue, .datetime).notNull()
      t.primaryKey([
        Columns.conversationID.rawValue,
        Columns.currentUserDID.rawValue,
        Columns.readerDID.rawValue,
      ])
    }

    try db.create(
      index: "idx_\(databaseTableName)_conversation",
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
