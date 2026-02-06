//
//  MLSOrphanedReactionModel.swift
//  CatbirdMLSCore
//
//  Created for Catbird.
//

import Foundation
import GRDB

/// Model for reactions where the parent message does not yet exist locally.
/// These are stored temporarily until the parent message arrives.
public struct MLSOrphanedReactionModel: Codable, Sendable, Hashable, Identifiable {
  /// Unique reaction identifier (UUID)
  public let reactionID: String
  
  /// ID of the message this reaction is on (which doesn't exist yet in MLSMessageModel)
  public let messageID: String
  
  /// Conversation this reaction belongs to
  public let conversationID: String
  
  /// DID of the current authenticated user
  public let currentUserDID: String
  
  /// DID of the user who added this reaction
  public let actorDID: String
  
  /// The reaction emoji
  public let emoji: String
  
  /// Action type: "add" or "remove"
  public let action: String
  
  /// When the reaction was recorded
  public let timestamp: Date
  
  public var id: String { reactionID }
  
  // MARK: - Initialization
  
  public init(
    reactionID: String = UUID().uuidString,
    messageID: String,
    conversationID: String,
    currentUserDID: String,
    actorDID: String,
    emoji: String,
    action: String = "add",
    timestamp: Date = Date()
  ) {
    self.reactionID = reactionID
    self.messageID = messageID
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.actorDID = actorDID
    self.emoji = emoji
    self.action = action
    self.timestamp = timestamp
  }
}

// MARK: - GRDB Conformance

extension MLSOrphanedReactionModel: FetchableRecord, PersistableRecord {
  public static let databaseTableName = "MLSOrphanedReactionModel"
  
  public enum Columns {
    public static let reactionID = Column("reactionID")
    public static let messageID = Column("messageID")
    public static let conversationID = Column("conversationID")
    public static let currentUserDID = Column("currentUserDID")
    public static let actorDID = Column("actorDID")
    public static let emoji = Column("emoji")
    public static let action = Column("action")
    public static let timestamp = Column("timestamp")
  }
  
  /// Conversion to standard reaction model
  public func toRegularModel() -> MLSReactionModel {
    MLSReactionModel(
      reactionID: reactionID,
      messageID: messageID,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      actorDID: actorDID,
      emoji: emoji,
      action: action,
      timestamp: timestamp
    )
  }
}
