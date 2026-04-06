//
//  MLSDeliveryAckModel.swift
//  CatbirdMLSCore
//
//  GRDB record storing per-member delivery acks for MLS messages.
//  One row per (messageId, senderDID, currentUserDID) — the composite primary key
//  makes upserts idempotent: duplicate acks from retries are silent no-ops.
//

import Foundation
import GRDB

public struct MLSDeliveryAckModel: Codable, FetchableRecord, PersistableRecord, Sendable {
  public static let databaseTableName = "MLSDeliveryAck"

  /// Conflict policy: duplicate (messageId, senderDID, currentUserDID) silently replaces.
  public static let persistenceConflictPolicy = PersistenceConflictPolicy(
    insert: .replace,
    update: .replace
  )

  /// Server-assigned messageId that was acked.
  public let messageId: String

  /// Conversation this ack belongs to.
  public let conversationId: String

  /// DID of the member who sent this ack (extracted from MLS credential).
  public let senderDID: String

  /// When the ack message was decrypted locally.
  public let ackedAt: Date

  /// Per-user DB partition key — matches the DB file owner.
  public let currentUserDID: String

  public enum Columns {
    public static let messageId = Column(CodingKeys.messageId)
    public static let conversationId = Column(CodingKeys.conversationId)
    public static let senderDID = Column(CodingKeys.senderDID)
    public static let ackedAt = Column(CodingKeys.ackedAt)
    public static let currentUserDID = Column(CodingKeys.currentUserDID)
  }

  public init(
    messageId: String,
    conversationId: String,
    senderDID: String,
    ackedAt: Date,
    currentUserDID: String
  ) {
    self.messageId = messageId
    self.conversationId = conversationId
    self.senderDID = senderDID
    self.ackedAt = ackedAt
    self.currentUserDID = currentUserDID
  }
}
