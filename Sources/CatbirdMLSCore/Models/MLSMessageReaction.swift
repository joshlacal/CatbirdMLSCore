//
//  MLSMessageReaction.swift
//  CatbirdMLSCore
//
//  Simple reaction data transfer object for UI/API layer
//

import Foundation

/// Represents a reaction on an MLS message
public struct MLSMessageReaction: Codable, Sendable, Hashable {
  /// ID of the message this reaction is on
  public let messageId: String
  
  /// The reaction emoji
  public let reaction: String
  
  /// DID of the user who reacted
  public let senderDID: String
  
  /// When the reaction was added
  public let reactedAt: Date
  
  public init(
    messageId: String,
    reaction: String,
    senderDID: String,
    reactedAt: Date
  ) {
    self.messageId = messageId
    self.reaction = reaction
    self.senderDID = senderDID
    self.reactedAt = reactedAt
  }
}
