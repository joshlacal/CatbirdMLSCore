//
//  MLSAdminRosterModel.swift
//  CatbirdMLSCore
//
//  Local cache of the encrypted admin roster for an MLS conversation
//

import Foundation
import GRDB

/// Stores the latest encrypted admin roster for a conversation
public struct MLSAdminRosterModel: Codable, Sendable, Hashable, Identifiable {
  public let conversationID: String
  public let version: Int
  public let rosterHash: String
  public let encryptedRoster: Data
  public let updatedAt: Date

  public var id: String { conversationID }

  public init(
    conversationID: String,
    version: Int,
    rosterHash: String,
    encryptedRoster: Data,
    updatedAt: Date = Date()
  ) {
    self.conversationID = conversationID
    self.version = version
    self.rosterHash = rosterHash
    self.encryptedRoster = encryptedRoster
    self.updatedAt = updatedAt
  }
}

// MARK: - GRDB

extension MLSAdminRosterModel: FetchableRecord, PersistableRecord {
  public static let databaseTableName = "MLSAdminRosterModel"

  public enum Columns {
    public static let conversationID = Column("convo_id")
    public static let version = Column("version")
    public static let rosterHash = Column("roster_hash")
    public static let encryptedRoster = Column("encrypted_roster")
    public static let updatedAt = Column("updated_at")
  }

  enum CodingKeys: String, CodingKey {
    case conversationID = "convo_id"
    case version
    case rosterHash = "roster_hash"
    case encryptedRoster = "encrypted_roster"
    case updatedAt = "updated_at"
  }
}
