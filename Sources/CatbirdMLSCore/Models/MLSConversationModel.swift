//
//  MLSConversationModel.swift
//  Catbird
//
//  MLS conversation data model
//

import Foundation
import GRDB

public enum MLSJoinMethod: String, Codable, Sendable {
  case welcome
  case externalCommit
  case creator
  case unknown
}

/// MLS group conversation model
public struct MLSConversationModel: Codable, Sendable, Hashable, Identifiable {
  public let conversationID: String
  public let currentUserDID: String
  public let groupID: Data
  public let epoch: Int64
  public let joinMethod: MLSJoinMethod
  public let joinEpoch: Int64
  public let title: String?
  public let avatarURL: String?
  public let createdAt: Date
  public let updatedAt: Date
  public let lastMessageAt: Date?
  public let lastMembershipChangeAt: Date?
  public let unacknowledgedMemberChanges: Int
  public let isActive: Bool
  public let needsRejoin: Bool
  public let rejoinRequestedAt: Date?
  public let lastRecoveryAttempt: Date?  // When we last attempted automatic recovery
  public let consecutiveFailures: Int  // Count of consecutive decryption failures
  public let isPlaceholder: Bool  // True if created by NSE as a placeholder (needs metadata sync)

  public var id: String { conversationID }

  // MARK: - Initialization

  public init(
    conversationID: String,
    currentUserDID: String,
    groupID: Data,
    epoch: Int64 = 0,
    joinMethod: MLSJoinMethod = .unknown,
    joinEpoch: Int64 = 0,
    title: String? = nil,
    avatarURL: String? = nil,
    createdAt: Date = Date(),
    updatedAt: Date = Date(),
    lastMessageAt: Date? = nil,
    lastMembershipChangeAt: Date? = nil,
    unacknowledgedMemberChanges: Int = 0,
    isActive: Bool = true,
    needsRejoin: Bool = false,
    rejoinRequestedAt: Date? = nil,
    lastRecoveryAttempt: Date? = nil,
    consecutiveFailures: Int = 0,
    isPlaceholder: Bool = false
  ) {
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.groupID = groupID
    self.epoch = epoch
    self.joinMethod = joinMethod
    self.joinEpoch = joinEpoch
    self.title = title
    self.avatarURL = avatarURL
    self.createdAt = createdAt
    self.updatedAt = updatedAt
    self.lastMessageAt = lastMessageAt
    self.lastMembershipChangeAt = lastMembershipChangeAt
    self.unacknowledgedMemberChanges = unacknowledgedMemberChanges
    self.isActive = isActive
    self.needsRejoin = needsRejoin
    self.rejoinRequestedAt = rejoinRequestedAt
    self.lastRecoveryAttempt = lastRecoveryAttempt
    self.consecutiveFailures = consecutiveFailures
    self.isPlaceholder = isPlaceholder
  }

  // MARK: - Update Methods

  /// Create updated copy with new epoch
  func withEpoch(_ newEpoch: Int64) -> MLSConversationModel {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      groupID: groupID,
      epoch: newEpoch,
      joinMethod: joinMethod,
      joinEpoch: joinEpoch,
      title: title,
      avatarURL: avatarURL,
      createdAt: createdAt,
      updatedAt: Date(),
      lastMessageAt: lastMessageAt,
      lastMembershipChangeAt: lastMembershipChangeAt,
      unacknowledgedMemberChanges: unacknowledgedMemberChanges,
      isActive: isActive,
      needsRejoin: needsRejoin,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: consecutiveFailures,
      isPlaceholder: isPlaceholder
    )
  }

  func withJoinInfo(method: MLSJoinMethod, epoch: Int64) -> MLSConversationModel {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      groupID: groupID,
      epoch: self.epoch,
      joinMethod: method,
      joinEpoch: epoch,
      title: title,
      avatarURL: avatarURL,
      createdAt: createdAt,
      updatedAt: Date(),
      lastMessageAt: lastMessageAt,
      lastMembershipChangeAt: lastMembershipChangeAt,
      unacknowledgedMemberChanges: unacknowledgedMemberChanges,
      isActive: isActive,
      needsRejoin: needsRejoin,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: consecutiveFailures,
      isPlaceholder: isPlaceholder
    )
  }

  /// Create updated copy with new last message timestamp
  func withLastMessageAt(_ timestamp: Date) -> MLSConversationModel {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      groupID: groupID,
      epoch: epoch,
      joinMethod: joinMethod,
      joinEpoch: joinEpoch,
      title: title,
      avatarURL: avatarURL,
      createdAt: createdAt,
      updatedAt: Date(),
      lastMessageAt: timestamp,
      lastMembershipChangeAt: lastMembershipChangeAt,
      unacknowledgedMemberChanges: unacknowledgedMemberChanges,
      isActive: isActive,
      needsRejoin: needsRejoin,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: consecutiveFailures,
      isPlaceholder: isPlaceholder
    )
  }

  /// Create updated copy with active status
  func withActiveStatus(_ active: Bool) -> MLSConversationModel {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      groupID: groupID,
      epoch: epoch,
      joinMethod: joinMethod,
      joinEpoch: joinEpoch,
      title: title,
      avatarURL: avatarURL,
      createdAt: createdAt,
      updatedAt: Date(),
      lastMessageAt: lastMessageAt,
      lastMembershipChangeAt: lastMembershipChangeAt,
      unacknowledgedMemberChanges: unacknowledgedMemberChanges,
      isActive: active,
      needsRejoin: needsRejoin,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: consecutiveFailures,
      isPlaceholder: isPlaceholder
    )
  }

  /// Create updated copy with new title and avatar
  /// Note: Setting metadata clears the isPlaceholder flag (placeholder healed)
  func withMetadata(title: String?, avatarURL: String?) -> MLSConversationModel {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      groupID: groupID,
      epoch: epoch,
      joinMethod: joinMethod,
      joinEpoch: joinEpoch,
      title: title,
      avatarURL: avatarURL,
      createdAt: createdAt,
      updatedAt: Date(),
      lastMessageAt: lastMessageAt,
      lastMembershipChangeAt: lastMembershipChangeAt,
      unacknowledgedMemberChanges: unacknowledgedMemberChanges,
      isActive: isActive,
      needsRejoin: needsRejoin,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: consecutiveFailures,
      isPlaceholder: false  // Clear placeholder flag when metadata is set
    )
  }

  /// Create updated copy with rejoin state
  func withRejoinState(needsRejoin: Bool, rejoinRequestedAt: Date?) -> MLSConversationModel {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      groupID: groupID,
      epoch: epoch,
      joinMethod: joinMethod,
      joinEpoch: joinEpoch,
      title: title,
      avatarURL: avatarURL,
      createdAt: createdAt,
      updatedAt: Date(),
      lastMessageAt: lastMessageAt,
      lastMembershipChangeAt: lastMembershipChangeAt,
      unacknowledgedMemberChanges: unacknowledgedMemberChanges,
      isActive: isActive,
      needsRejoin: needsRejoin,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: consecutiveFailures,
      isPlaceholder: isPlaceholder
    )
  }

  /// Create updated copy with recovery state
  func withRecoveryState(lastRecoveryAttempt: Date?, consecutiveFailures: Int)
    -> MLSConversationModel
  {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      groupID: groupID,
      epoch: epoch,
      joinMethod: joinMethod,
      joinEpoch: joinEpoch,
      title: title,
      avatarURL: avatarURL,
      createdAt: createdAt,
      updatedAt: Date(),
      lastMessageAt: lastMessageAt,
      lastMembershipChangeAt: lastMembershipChangeAt,
      unacknowledgedMemberChanges: unacknowledgedMemberChanges,
      isActive: isActive,
      needsRejoin: needsRejoin,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: consecutiveFailures,
      isPlaceholder: isPlaceholder
    )
  }

  /// Reset consecutive failures (after successful message processing)
  func withResetFailures() -> MLSConversationModel {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      groupID: groupID,
      epoch: epoch,
      joinMethod: joinMethod,
      joinEpoch: joinEpoch,
      title: title,
      avatarURL: avatarURL,
      createdAt: createdAt,
      updatedAt: updatedAt,
      lastMessageAt: lastMessageAt,
      lastMembershipChangeAt: lastMembershipChangeAt,
      unacknowledgedMemberChanges: unacknowledgedMemberChanges,
      isActive: isActive,
      needsRejoin: needsRejoin,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: 0,
      isPlaceholder: isPlaceholder
    )
  }

  /// Create updated copy with membership change tracking
  func withMembershipChange(timestamp: Date, unacknowledged: Int = 1) -> MLSConversationModel {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      groupID: groupID,
      epoch: epoch,
      joinMethod: joinMethod,
      joinEpoch: joinEpoch,
      title: title,
      avatarURL: avatarURL,
      createdAt: createdAt,
      updatedAt: Date(),
      lastMessageAt: lastMessageAt,
      lastMembershipChangeAt: timestamp,
      unacknowledgedMemberChanges: unacknowledgedMemberChanges + unacknowledged,
      isActive: isActive,
      needsRejoin: needsRejoin,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: consecutiveFailures,
      isPlaceholder: isPlaceholder
    )
  }

  /// Create updated copy with membership change badge cleared
  func clearMembershipChangeBadge() -> MLSConversationModel {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      groupID: groupID,
      epoch: epoch,
      joinMethod: joinMethod,
      joinEpoch: joinEpoch,
      title: title,
      avatarURL: avatarURL,
      createdAt: createdAt,
      updatedAt: Date(),
      lastMessageAt: lastMessageAt,
      lastMembershipChangeAt: lastMembershipChangeAt,
      unacknowledgedMemberChanges: 0,
      isActive: isActive,
      needsRejoin: needsRejoin,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: consecutiveFailures,
      isPlaceholder: isPlaceholder
    )
  }
}

// MARK: - GRDB Conformance
extension MLSConversationModel: FetchableRecord, PersistableRecord {
  public static let databaseTableName = "MLSConversationModel"

  public enum Columns {
    public static let conversationID = Column("conversationID")
    public static let currentUserDID = Column("currentUserDID")
    public static let groupID = Column("groupID")
    public static let epoch = Column("epoch")
    public static let joinMethod = Column("joinMethod")
    public static let joinEpoch = Column("joinEpoch")
    public static let title = Column("title")
    public static let avatarURL = Column("avatarURL")
    public static let createdAt = Column("createdAt")
    public static let updatedAt = Column("updatedAt")
    public static let lastMessageAt = Column("lastMessageAt")
    public static let lastMembershipChangeAt = Column("lastMembershipChangeAt")
    public static let unacknowledgedMemberChanges = Column("unacknowledgedMemberChanges")
    public static let isActive = Column("isActive")
    public static let needsRejoin = Column("needsRejoin")
    public static let rejoinRequestedAt = Column("rejoinRequestedAt")
    public static let lastRecoveryAttempt = Column("lastRecoveryAttempt")
    public static let consecutiveFailures = Column("consecutiveFailures")
    public static let isPlaceholder = Column("isPlaceholder")
  }

  enum CodingKeys: String, CodingKey {
    case conversationID
    case currentUserDID
    case groupID
    case epoch
    case joinMethod
    case joinEpoch
    case title
    case avatarURL
    case createdAt
    case updatedAt
    case lastMessageAt
    case lastMembershipChangeAt
    case unacknowledgedMemberChanges
    case isActive
    case needsRejoin
    case rejoinRequestedAt
    case lastRecoveryAttempt
    case consecutiveFailures
    case isPlaceholder
  }
}
