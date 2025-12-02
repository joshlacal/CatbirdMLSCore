//
//  MLSMemberModel.swift
//  Catbird
//
//  MLS group member data model
//

import Foundation
import GRDB

/// MLS group member model
public struct MLSMemberModel: Codable, Sendable, Hashable, Identifiable {
  public   let memberID: String
  public   let conversationID: String
  public   let currentUserDID: String
  public   let did: String
  public   let handle: String?
  public   let displayName: String?
  public   let leafIndex: Int
  public   let credentialData: Data?
  public   let signaturePublicKey: Data?
  public   let addedAt: Date
  public   let updatedAt: Date
  public   let removedAt: Date?
  public   let removedBy: String?
  public   let removalReason: String?
  public   let isActive: Bool
  public   let role: Role
  public   let capabilitiesData: Data?

  public var id: String { memberID }

  // Computed property for capabilities array
  public   var capabilities: [String]? {
    get {
      guard let data = capabilitiesData else { return nil }
      return try? JSONDecoder().decode([String].self, from: data)
    }
  }

  // Helper to create capabilitiesData from array
  static func encodeCapabilities(_ capabilities: [String]?) -> Data? {
    guard let capabilities = capabilities else { return nil }
    return try? JSONEncoder().encode(capabilities)
  }

  // MARK: - Role Enum

  /// Member role
  public enum Role: String, Codable, Sendable {
    case member
    case admin
    case moderator
  }
}

// MARK: - GRDB Conformance
extension MLSMemberModel: FetchableRecord, PersistableRecord {
  public static let databaseTableName = "MLSMemberModel"

  public enum Columns {
    public static let memberID = Column("memberID")
    public static let conversationID = Column("conversationID")
    public static let currentUserDID = Column("currentUserDID")
    public static let did = Column("did")
    public static let handle = Column("handle")
    public static let displayName = Column("displayName")
    public static let leafIndex = Column("leafIndex")
    public static let credentialData = Column("credentialData")
    public static let signaturePublicKey = Column("signaturePublicKey")
    public static let addedAt = Column("addedAt")
    public static let updatedAt = Column("updatedAt")
    public static let removedAt = Column("removedAt")
    public static let removedBy = Column("removedBy")
    public static let removalReason = Column("removalReason")
    public static let isActive = Column("isActive")
    public static let role = Column("role")
    public static let capabilitiesData = Column("capabilities")
  }

  enum CodingKeys: String, CodingKey {
    case memberID
    case conversationID
    case currentUserDID
    case did
    case handle
    case displayName
    case leafIndex
    case credentialData
    case signaturePublicKey
    case addedAt
    case updatedAt
    case removedAt
    case removedBy
    case removalReason
    case isActive
    case role
    case capabilitiesData = "capabilities"
  }
}

// MARK: - MLSMemberModel

extension MLSMemberModel {
  // MARK: - Initialization

  public init(
    memberID: String,
    conversationID: String,
    currentUserDID: String,
    did: String,
    handle: String? = nil,
    displayName: String? = nil,
    leafIndex: Int,
    credentialData: Data? = nil,
    signaturePublicKey: Data? = nil,
    addedAt: Date = Date(),
    updatedAt: Date = Date(),
    removedAt: Date? = nil,
    removedBy: String? = nil,
    removalReason: String? = nil,
    isActive: Bool = true,
    role: Role = .member,
    capabilities: [String]? = nil
  ) {
    self.memberID = memberID
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.did = did
    self.handle = handle
    self.displayName = displayName
    self.leafIndex = leafIndex
    self.credentialData = credentialData
    self.signaturePublicKey = signaturePublicKey
    self.addedAt = addedAt
    self.updatedAt = updatedAt
    self.removedAt = removedAt
    self.removedBy = removedBy
    self.removalReason = removalReason
    self.isActive = isActive
    self.role = role
    self.capabilitiesData = Self.encodeCapabilities(capabilities)
  }

  // MARK: - Update Methods

  /// Create copy marked as removed
  func withRemoved(at date: Date = Date(), by: String? = nil, reason: String? = nil) -> MLSMemberModel {
    MLSMemberModel(
      memberID: memberID,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      did: did,
      handle: handle,
      displayName: displayName,
      leafIndex: leafIndex,
      credentialData: credentialData,
      signaturePublicKey: signaturePublicKey,
      addedAt: addedAt,
      updatedAt: date,
      removedAt: date,
      removedBy: by,
      removalReason: reason,
      isActive: false,
      role: role,
      capabilities: capabilities
    )
  }

  /// Create copy with updated profile info
  func withProfileInfo(handle: String?, displayName: String?) -> MLSMemberModel {
    MLSMemberModel(
      memberID: memberID,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      did: did,
      handle: handle,
      displayName: displayName,
      leafIndex: leafIndex,
      credentialData: credentialData,
      signaturePublicKey: signaturePublicKey,
      addedAt: addedAt,
      updatedAt: Date(),
      removedAt: removedAt,
      removedBy: removedBy,
      removalReason: removalReason,
      isActive: isActive,
      role: role,
      capabilities: capabilities
    )
  }
}
