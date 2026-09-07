//
//  MLSConversationDeletionMarkerModel.swift
//  CatbirdMLSCore
//
//  Tracks per-user local conversation deletion markers for "Delete for me".
//

import Foundation
import GRDB

/// Local deletion marker for a conversation scoped to the current user's DID.
///
/// When a user deletes a conversation locally ("Delete for me"), local message history is
/// purged and a marker is recorded so sync, inventory, or app restarts do not resurrect
/// the conversation or its purged history. Server membership, OpenMLS group state, and
/// epoch cryptographic keys are preserved so that cryptographic synchronization continues
/// safely and new incoming messages can still be decrypted.
public struct MLSConversationDeletionMarkerModel: Codable, FetchableRecord, PersistableRecord, Sendable, Hashable {
  public static let databaseTableName = "MLSConversationDeletionMarkerModel"

  public let conversationID: String
  public let currentUserDID: String
  public let deletedAt: Date
  public let clearedThroughSequenceNumber: Int64?
  public var isHiddenFromList: Bool

  public init(
    conversationID: String,
    currentUserDID: String,
    deletedAt: Date = Date(),
    clearedThroughSequenceNumber: Int64? = nil,
    isHiddenFromList: Bool = true
  ) {
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.deletedAt = deletedAt
    self.clearedThroughSequenceNumber = clearedThroughSequenceNumber
    self.isHiddenFromList = isHiddenFromList
  }

  public enum Columns: String, ColumnExpression {
    case conversationID
    case currentUserDID
    case deletedAt
    case clearedThroughSequenceNumber
    case isHiddenFromList
  }

  public static func createTable(in db: Database) throws {
    try db.create(table: databaseTableName, ifNotExists: true) { t in
      t.column(Columns.conversationID.rawValue, .text).notNull()
      t.column(Columns.currentUserDID.rawValue, .text).notNull()
      t.column(Columns.deletedAt.rawValue, .datetime).notNull()
      t.column(Columns.clearedThroughSequenceNumber.rawValue, .integer)
      t.column(Columns.isHiddenFromList.rawValue, .boolean).notNull().defaults(to: true)
      t.primaryKey([Columns.conversationID.rawValue, Columns.currentUserDID.rawValue])
    }

    if try db.tableExists(databaseTableName) {
      let columns = try db.columns(in: databaseTableName).map(\.name)
      if !columns.contains(Columns.isHiddenFromList.rawValue) {
        try db.alter(table: databaseTableName) { t in
          t.add(column: Columns.isHiddenFromList.rawValue, .boolean).notNull().defaults(to: true)
        }
      }
    }

    try db.create(
      index: "idx_\(databaseTableName)_user",
      on: databaseTableName,
      columns: [Columns.currentUserDID.rawValue, Columns.conversationID.rawValue],
      ifNotExists: true
    )
  }
}
