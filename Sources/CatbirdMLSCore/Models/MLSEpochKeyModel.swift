//
//  MLSEpochKeyModel.swift
//  Catbird
//
//  MLS epoch key tracking model
//

import Foundation
import GRDB

/// MLS epoch key tracking model (matches v1_initial_schema migration)
public struct MLSEpochKeyModel: Codable, Sendable, Hashable, Identifiable {
  public   let epochKeyID: String
  public   let conversationID: String
  public   let currentUserDID: String
  public   let epoch: Int64
  public   let keyMaterial: Data
  public   let createdAt: Date
  public   let expiresAt: Date?
  public   let isActive: Bool
  public   let deletedAt: Date?

  public var id: String { epochKeyID }

  // MARK: - Initialization

  public init(
    epochKeyID: String,
    conversationID: String,
    currentUserDID: String,
    epoch: Int64,
    keyMaterial: Data,
    createdAt: Date = Date(),
    expiresAt: Date? = nil,
    isActive: Bool = true,
    deletedAt: Date? = nil
  ) {
    self.epochKeyID = epochKeyID
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.epoch = epoch
    self.keyMaterial = keyMaterial
    self.createdAt = createdAt
    self.expiresAt = expiresAt
    self.isActive = isActive
    self.deletedAt = deletedAt
  }

  // MARK: - Update Methods

  /// Create copy marked as inactive
  func markAsInactive() -> MLSEpochKeyModel {
    MLSEpochKeyModel(
      epochKeyID: epochKeyID,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      epoch: epoch,
      keyMaterial: keyMaterial,
      createdAt: createdAt,
      expiresAt: expiresAt,
      isActive: false,
      deletedAt: deletedAt
    )
  }

  // MARK: - Computed Properties

  /// Check if epoch key is expired
  public   var isExpired: Bool {
    guard let expiry = expiresAt else { return false }
    return Date() > expiry
  }

  /// Age of epoch key in seconds
  public   var ageInSeconds: TimeInterval {
    Date().timeIntervalSince(createdAt)
  }

  /// Age of epoch key in days
  public   var ageInDays: Int {
    Int(ageInSeconds / 86400)
  }
}

// MARK: - GRDB Conformance
extension MLSEpochKeyModel: FetchableRecord, PersistableRecord {
  public static let databaseTableName = "MLSEpochKeyModel"

  public enum Columns {
    static let epochKeyID = Column("epochKeyID")
    static let conversationID = Column("conversationID")
    static let currentUserDID = Column("currentUserDID")
    static let epoch = Column("epoch")
    static let keyMaterial = Column("keyMaterial")
    static let createdAt = Column("createdAt")
    static let expiresAt = Column("expiresAt")
    static let isActive = Column("isActive")
    static let deletedAt = Column("deletedAt")
  }
}
