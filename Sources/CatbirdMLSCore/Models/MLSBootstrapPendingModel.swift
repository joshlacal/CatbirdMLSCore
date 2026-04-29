//
//  MLSBootstrapPendingModel.swift
//  CatbirdMLSCore
//
//  CLIENT E — atomic bootstrap persistence.
//
//  Records every in-flight first-responder bootstrap attempt. We insert a
//  row immediately after `MLSClient.createGroupWithId` returns (durable
//  OpenMLS state on disk) and delete it after `confirmCommit` has applied
//  the staged add-members commit. If the process is killed or an
//  account-switch fires between those two points, the row survives and
//  the boot path can locate the orphaned half-staged group, delete it
//  from OpenMLS storage, and unblock the conversation for retry.
//

import Foundation
import GRDB

public struct MLSBootstrapPendingModel: Codable, FetchableRecord, PersistableRecord, Sendable {
  public static let databaseTableName = "MLSBootstrapPendingModel"

  /// Conflict policy: re-marking the same (convoId, currentUserDID) replaces.
  public static let persistenceConflictPolicy = PersistenceConflictPolicy(
    insert: .replace,
    update: .replace
  )

  /// Conversation ID under bootstrap (stable across resets).
  public let conversationID: String

  /// Hex-encoded MLS group ID this attempt created locally.
  public let groupIdHex: String

  /// Per-user DB partition key.
  public let currentUserDID: String

  /// When the marker was inserted (for stale-marker triage).
  public let createdAt: Date

  public enum Columns {
    public static let conversationID = Column(CodingKeys.conversationID)
    public static let groupIdHex = Column(CodingKeys.groupIdHex)
    public static let currentUserDID = Column(CodingKeys.currentUserDID)
    public static let createdAt = Column(CodingKeys.createdAt)
  }

  public init(
    conversationID: String,
    groupIdHex: String,
    currentUserDID: String,
    createdAt: Date = Date()
  ) {
    self.conversationID = conversationID
    self.groupIdHex = groupIdHex
    self.currentUserDID = currentUserDID
    self.createdAt = createdAt
  }
}

extension MLSBootstrapPendingModel {
  /// Record a bootstrap-pending marker. Idempotent: replaces any existing
  /// marker for the same (conversationID, currentUserDID).
  public static func record(
    conversationID: String,
    groupIdHex: String,
    currentUserDID: String,
    database: any DatabaseWriter
  ) async throws {
    let model = MLSBootstrapPendingModel(
      conversationID: conversationID,
      groupIdHex: groupIdHex,
      currentUserDID: currentUserDID
    )
    try await database.write { db in
      try model.insert(db)
    }
  }

  /// Clear the bootstrap-pending marker for a (conversationID, currentUserDID)
  /// pair. Safe to call repeatedly — no-op if no marker exists.
  public static func clear(
    conversationID: String,
    currentUserDID: String,
    database: any DatabaseWriter
  ) async {
    try? await database.write { db in
      try db.execute(
        sql: "DELETE FROM MLSBootstrapPendingModel WHERE conversationID = ? AND currentUserDID = ?",
        arguments: [conversationID, currentUserDID]
      )
    }
  }

  /// Fetch all pending-bootstrap markers for a user (boot-time scan).
  public static func fetchAll(
    currentUserDID: String,
    database: any DatabaseReader
  ) async throws -> [MLSBootstrapPendingModel] {
    try await database.read { db in
      try MLSBootstrapPendingModel
        .filter(Columns.currentUserDID == currentUserDID)
        .fetchAll(db)
    }
  }
}
