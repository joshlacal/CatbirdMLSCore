//
//  MLSOrphanedMutationModel.swift
//  CatbirdMLSCore
//
//  Parking-lot table for message edits/deletes (spec §5.7.2 / §5.8.2) that
//  arrive before their target message is persisted locally. Mirrors
//  `MLSOrphanedReactionModel`'s design (no foreign key on the target ID —
//  that is the whole point), keyed by `targetMessageID` instead of the
//  reaction's `messageID`.
//

import Foundation
import GRDB

/// Discriminator for the kind of pending mutation parked in this table.
public enum MLSMutationKind: String, Sendable, Codable, Equatable {
  case edit
  case delete
}

/// Model for an edit/delete whose target message does not yet exist locally.
/// These are stored temporarily until the target message arrives, at which
/// point they are adopted (applied) in the same way orphaned reactions are.
public struct MLSOrphanedMutationModel: Codable, Sendable, Hashable, Identifiable {
  /// Unique mutation identifier (UUID)
  public let mutationID: String

  /// ID of the message this mutation targets (which doesn't exist yet in MLSMessageModel)
  public let targetMessageID: String

  /// Conversation this mutation belongs to
  public let conversationID: String

  /// DID of the current authenticated user
  public let currentUserDID: String

  /// DID of the user who applied this mutation (the editor/deleter). Authorization
  /// (spec §5.7.2 step 2 / §5.8.2 step 2) is re-checked against the target's actual
  /// sender at adoption time — this is only the claimed applier pending that check.
  public let applierDID: String

  /// "edit" or "delete" — see `MLSMutationKind`.
  public let mutationType: String

  /// New text for an edit mutation. Nil for delete mutations.
  public let newText: String?

  /// The `seq` of the edit/delete message itself (NOT the target's seq). Used for
  /// last-writer-wins among multiple pending edits for the same target.
  public let mutationSeq: Int64

  /// When this orphan was recorded.
  public let createdAt: Date

  public var id: String { mutationID }

  // MARK: - Initialization

  public init(
    mutationID: String = UUID().uuidString,
    targetMessageID: String,
    conversationID: String,
    currentUserDID: String,
    applierDID: String,
    mutationType: String,
    newText: String? = nil,
    mutationSeq: Int64,
    createdAt: Date = Date()
  ) {
    self.mutationID = mutationID
    self.targetMessageID = targetMessageID
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.applierDID = applierDID
    self.mutationType = mutationType
    self.newText = newText
    self.mutationSeq = mutationSeq
    self.createdAt = createdAt
  }
}

// MARK: - GRDB Conformance

extension MLSOrphanedMutationModel: FetchableRecord, PersistableRecord {
  public static let databaseTableName = "MLSOrphanedMutationModel"

  public enum Columns {
    public static let mutationID = Column("mutationID")
    public static let targetMessageID = Column("targetMessageID")
    public static let conversationID = Column("conversationID")
    public static let currentUserDID = Column("currentUserDID")
    public static let applierDID = Column("applierDID")
    public static let mutationType = Column("mutationType")
    public static let newText = Column("newText")
    public static let mutationSeq = Column("mutationSeq")
    public static let createdAt = Column("createdAt")
  }
}
