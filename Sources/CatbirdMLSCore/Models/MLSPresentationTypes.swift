//
//  MLSPresentationTypes.swift
//  CatbirdMLSCore
//
//  Pure value models and presentation helpers bridging UniFFI CatbirdMLS rows
//  to Apple platform views without any Swift-side SQLite/GRDB dependency.
//

import CatbirdMLS
import Foundation

// MARK: - Unshadowed UniFFI Row Aliases

public typealias MLSConversationRow = CatbirdMLS.ConversationRow
public typealias MLSMessageRow = CatbirdMLS.MessageRow
public typealias MLSRequestRow = CatbirdMLS.RequestRow
public typealias MLSGroupMetadataRow = CatbirdMLS.GroupMetadataRow
public typealias MLSMemberRole = CatbirdMLS.MemberRole

// MARK: - Credential Binding Pure Helpers

public enum MLSCredentialBinding {
  /// Extract the DID root from a credential identity (`did:plc:alice#device-1` -> `did:plc:alice`).
  public static func credentialRootDID(_ identity: String) -> String {
    let trimmed = identity.trimmingCharacters(in: .whitespacesAndNewlines)
    return trimmed.split(separator: "#", maxSplits: 1, omittingEmptySubsequences: false)
      .first
      .map(String.init) ?? ""
  }
}

// MARK: - Conversation Recovery & Leave Presentation

public enum ConversationRecoveryState: String, Codable, Equatable, Sendable, CaseIterable {
  case healthy
  case epochBehind
  case groupMissing
  case needsRejoin
  case recovering
  case unrecoverableLocal
  case resetPending
  case deviceRemoved
  case closed

  public var isPersisted: Bool {
    switch self {
    case .needsRejoin, .unrecoverableLocal, .resetPending, .deviceRemoved, .closed:
      return true
    case .healthy, .epochBehind, .groupMissing, .recovering:
      return false
    }
  }

  public var isHealthy: Bool {
    self == .healthy
  }

  public var allowsRecoveryAttempt: Bool {
    switch self {
    case .needsRejoin, .resetPending, .groupMissing, .epochBehind:
      return true
    case .healthy, .recovering, .unrecoverableLocal, .deviceRemoved, .closed:
      return false
    }
  }

  public func canTransition(to next: ConversationRecoveryState) -> Bool {
    if self == next { return true }
    if next == .deviceRemoved || next == .closed { return self != .closed }
    switch self {
    case .deviceRemoved:
      return next == .healthy
    case .closed:
      return false
    case .healthy:
      return next == .epochBehind || next == .needsRejoin || next == .groupMissing || next == .resetPending
    case .epochBehind:
      return next == .healthy || next == .needsRejoin
    case .groupMissing:
      return next == .recovering || next == .needsRejoin || next == .healthy
    case .needsRejoin:
      return next == .recovering || next == .unrecoverableLocal || next == .healthy || next == .resetPending
    case .recovering:
      return next == .healthy || next == .needsRejoin || next == .unrecoverableLocal
    case .unrecoverableLocal:
      return next == .resetPending
    case .resetPending:
      return next == .healthy || next == .needsRejoin
    }
  }
}

public enum MLSConversationLeavePresentation: Equatable, Sendable {
  case none
  case pending
  case checking
  case left
}

public enum MLSSystemMessagePresentation {
  public static func isSystem(_ payload: MLSMessagePayload) -> Bool {
    payload.version == 1 && payload.messageType == .system
  }

  public static func text(_ payload: MLSMessagePayload, verifiedAccountLeave: Bool = false) -> String {
    guard isSystem(payload) else { return payload.text ?? "" }
    switch payload.text {
    case "history_boundary.new_member":
      return "You joined this conversation"
    case "history_boundary.device_rejoined":
      return "Messages before this point aren't available on this device"
    case "membership.left":
      return verifiedAccountLeave ? "You left this conversation" : "Conversation membership changed"
    case "conversation.closed":
      return "Conversation status changed"
    case "message.undecryptable":
      return "This message couldn't be decrypted on this device"
    default:
      return payload.text ?? ""
    }
  }

  public static func isVerifiedAccountLeave(
    payload: MLSMessagePayload,
    messageID: String,
    senderDID: String,
    currentUserDID: String,
    currentDeviceID: String,
    terminalState: String?
  ) -> Bool {
    guard isSystem(payload),
          payload.text == "membership.left",
          senderDID == currentUserDID,
          ["device_removed", "closed"].contains(terminalState ?? ""),
          canonicalUUID(currentDeviceID)
    else {
      return false
    }
    let components = messageID.split(separator: ":", omittingEmptySubsequences: false)
    guard components.count == 3,
          components[0] == "membership-left",
          canonicalUUID(String(components[1])),
          components[2] == currentDeviceID
    else {
      return false
    }
    return true
  }

  public static func canonicalUUID(_ value: String) -> Bool {
    guard let uuid = UUID(uuidString: value), uuid.uuidString.lowercased() == value else {
      return false
    }
    let bytes = uuid.uuid
    return bytes.6 >> 4 == 4 && bytes.8 & 0xc0 == 0x80
  }
}

// MARK: - Pure Value Models (Bridged from CatbirdMlsClient Rows)

public enum MLSJoinMethod: String, Codable, Sendable, Hashable {
  case welcome
  case externalCommit
  case creator
  case unknown
}

public enum MLSRequestState: String, Codable, Sendable, Hashable {
  case none
  case pendingInbound
  case pendingOutbound
}

public struct MLSConversationModel: Codable, Sendable, Hashable, Identifiable {
  public let conversationID: String
  public let currentUserDID: String
  public let groupID: Data
  public let epoch: Int64
  public let generation: Int64
  public let sequencerDID: String
  public let spaceURI: String?
  public let joinMethod: MLSJoinMethod
  public let joinEpoch: Int64
  public let title: String?
  public let description: String?
  public let avatarURL: String?
  public let avatarImageData: Data?
  public let createdAt: Date
  public let updatedAt: Date
  public let lastMessageAt: Date?
  public let lastMembershipChangeAt: Date?
  public let unacknowledgedMemberChanges: Int
  public let unreadCount: Int
  public let isActive: Bool
  public let needsRejoin: Bool
  public let needsReset: Bool
  public let isUnrecoverable: Bool
  public let rejoinRequestedAt: Date?
  public let lastRecoveryAttempt: Date?
  public let consecutiveFailures: Int
  public let isPlaceholder: Bool
  public let requestState: MLSRequestState
  public let mutedUntil: Date?
  public let pendingNewGroupId: String?
  public let pendingResetGeneration: Int64?

  public var id: String { conversationID }
  public var conversationId: String { conversationID }
  public var isPendingRequest: Bool { requestState == .pendingInbound }
  public var isMuted: Bool { mutedUntil.map { $0 > Date() } ?? false }

  public var persistedRecoveryState: ConversationRecoveryState {
    if needsReset { return .resetPending }
    if isUnrecoverable { return .unrecoverableLocal }
    if needsRejoin { return .needsRejoin }
    return .healthy
  }

  public init(
    conversationID: String,
    currentUserDID: String,
    groupID: Data,
    epoch: Int64 = 0,
    generation: Int64 = 0,
    sequencerDID: String = "",
    spaceURI: String? = nil,
    joinMethod: MLSJoinMethod = .unknown,
    joinEpoch: Int64 = 0,
    title: String? = nil,
    description: String? = nil,
    avatarURL: String? = nil,
    avatarImageData: Data? = nil,
    createdAt: Date = Date(),
    updatedAt: Date = Date(),
    lastMessageAt: Date? = nil,
    lastMembershipChangeAt: Date? = nil,
    unacknowledgedMemberChanges: Int = 0,
    unreadCount: Int = 0,
    isActive: Bool = true,
    needsRejoin: Bool = false,
    needsReset: Bool = false,
    isUnrecoverable: Bool = false,
    rejoinRequestedAt: Date? = nil,
    lastRecoveryAttempt: Date? = nil,
    consecutiveFailures: Int = 0,
    isPlaceholder: Bool = false,
    requestState: MLSRequestState = .none,
    mutedUntil: Date? = nil,
    pendingNewGroupId: String? = nil,
    pendingResetGeneration: Int64? = nil
  ) {
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.groupID = groupID
    self.epoch = epoch
    self.generation = generation
    self.sequencerDID = sequencerDID
    self.spaceURI = spaceURI
    self.joinMethod = joinMethod
    self.joinEpoch = joinEpoch
    self.title = title
    self.description = description
    self.avatarURL = avatarURL
    self.avatarImageData = avatarImageData
    self.createdAt = createdAt
    self.updatedAt = updatedAt
    self.lastMessageAt = lastMessageAt
    self.lastMembershipChangeAt = lastMembershipChangeAt
    self.unacknowledgedMemberChanges = unacknowledgedMemberChanges
    self.unreadCount = unreadCount
    self.isActive = isActive
    self.needsRejoin = needsRejoin
    self.needsReset = needsReset
    self.isUnrecoverable = isUnrecoverable
    self.rejoinRequestedAt = rejoinRequestedAt
    self.lastRecoveryAttempt = lastRecoveryAttempt
    self.consecutiveFailures = consecutiveFailures
    self.isPlaceholder = isPlaceholder
    self.requestState = requestState
    self.mutedUntil = mutedUntil
    self.pendingNewGroupId = pendingNewGroupId
    self.pendingResetGeneration = pendingResetGeneration
  }

  public init(row: MLSConversationRow, currentUserDID: String, metadata: MLSGroupMetadataRow? = nil) {
    let timestamp = Date(timeIntervalSince1970: Double(row.updatedAtMs) / 1_000.0)
    self.init(
      conversationID: row.conversationId,
      currentUserDID: currentUserDID,
      groupID: row.groupId,
      epoch: row.epoch,
      generation: row.generation,
      sequencerDID: row.sequencerDid,
      spaceURI: row.spaceUri,
      joinMethod: .welcome,
      joinEpoch: 0,
      title: metadata?.title ?? row.title,
      description: metadata?.description,
      createdAt: timestamp,
      updatedAt: timestamp,
      lastMessageAt: timestamp,
      unreadCount: max(0, Int(row.unreadCount))
    )
  }
}

public struct MLSMemberModel: Codable, Sendable, Hashable, Identifiable {
  public enum Role: String, Codable, Sendable, Hashable {
    case member
    case admin
    case moderator
  }

  public let memberID: String
  public let conversationID: String
  public let currentUserDID: String
  public let did: String
  public let handle: String?
  public let displayName: String?
  public let leafIndex: Int
  public let credentialData: Data?
  public let signaturePublicKey: Data?
  public let addedAt: Date
  public let updatedAt: Date
  public let removedAt: Date?
  public let removedBy: String?
  public let removalReason: String?
  public let isActive: Bool
  public let role: Role
  public let capabilitiesData: Data?
  public let avatarURL: String?

  public var id: String { memberID }

  public init(
    memberID: String,
    conversationID: String,
    currentUserDID: String,
    did: String,
    handle: String? = nil,
    displayName: String? = nil,
    leafIndex: Int = 0,
    credentialData: Data? = nil,
    signaturePublicKey: Data? = nil,
    addedAt: Date = Date(),
    updatedAt: Date = Date(),
    removedAt: Date? = nil,
    removedBy: String? = nil,
    removalReason: String? = nil,
    isActive: Bool = true,
    role: Role = .member,
    capabilitiesData: Data? = nil,
    avatarURL: String? = nil
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
    self.capabilitiesData = capabilitiesData
    self.avatarURL = avatarURL
  }
}

public struct MLSMembershipEventModel: Codable, Sendable, Hashable, Identifiable {
  public enum EventType: String, Codable, Sendable, Hashable {
    case joined
    case left
    case removed
    case kicked
    case roleChanged
    case deviceAdded
    case deviceRemoved
  }

  public let id: String
  public let conversationID: String
  public let currentUserDID: String
  public let memberDID: String
  public let eventType: EventType
  public let timestamp: Date
  public let actorDID: String?
  public let epoch: Int64
  public let metadata: Data?

  public init(
    id: String = UUID().uuidString,
    conversationID: String,
    currentUserDID: String,
    memberDID: String,
    eventType: EventType,
    timestamp: Date = Date(),
    actorDID: String? = nil,
    epoch: Int64,
    metadata: Data? = nil
  ) {
    self.id = id
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.memberDID = memberDID
    self.eventType = eventType
    self.timestamp = timestamp
    self.actorDID = actorDID
    self.epoch = epoch
    self.metadata = metadata
  }
}

public struct MLSMessageModel: Codable, Sendable, Hashable, Identifiable {
  public let messageID: String
  public let conversationID: String
  public let currentUserDID: String
  public let senderID: String
  public let senderDeviceID: String
  public let plaintextJSON: Data
  public let epoch: Int64
  public let sequenceNumber: Int64
  public let timestamp: Date
  public let isOwn: Bool

  public var id: String { messageID }

  public init(
    messageID: String,
    conversationID: String,
    currentUserDID: String,
    senderID: String,
    senderDeviceID: String = "",
    plaintextJSON: Data,
    epoch: Int64,
    sequenceNumber: Int64,
    timestamp: Date,
    isOwn: Bool
  ) {
    self.messageID = messageID
    self.conversationID = conversationID
    self.currentUserDID = currentUserDID
    self.senderID = senderID
    self.senderDeviceID = senderDeviceID
    self.plaintextJSON = plaintextJSON
    self.epoch = epoch
    self.sequenceNumber = sequenceNumber
    self.timestamp = timestamp
    self.isOwn = isOwn
  }

  public init(row: MLSMessageRow, currentUserDID: String) {
    self.init(
      messageID: row.messageId,
      conversationID: row.conversationId,
      currentUserDID: currentUserDID,
      senderID: row.senderDid,
      senderDeviceID: row.senderDeviceId,
      plaintextJSON: Data(row.plaintextJson.utf8),
      epoch: row.epoch,
      sequenceNumber: row.seq,
      timestamp: Date(timeIntervalSince1970: Double(row.createdAtMs) / 1_000.0),
      isOwn: row.isOwn
    )
  }

  public func parsedPayload() -> MLSMessagePayload? {
    try? MLSMessagePayload.decodeFromJSON(plaintextJSON)
  }
}

// MARK: - Diagnostics Ring Buffer (Zero Content / Keys / Tokens)

public enum MLSDiagnosticEvent: String, Codable, Sendable, CaseIterable {
  case sendFailed
  case sendRecovered
  case streamPaused
  case streamResumed
  case rejoinWaiting
  case conversationLoadFailed
  case decryptRefused
}

public struct MLSDiagnosticRecord: Sendable, Codable, Equatable {
  public let event: MLSDiagnosticEvent
  public let code: String
  public let conversationIDPrefix: String?
  public let epoch: UInt64?
  public let generation: UInt64?
  public let stateVersion: UInt64?
  public let retryAfter: TimeInterval?
  public let occurredAt: Date
  public let attempt: Int?
  public let detail: [String: String]

  public init(
    event: MLSDiagnosticEvent,
    code: String,
    conversationIDPrefix: String? = nil,
    epoch: UInt64? = nil,
    generation: UInt64? = nil,
    stateVersion: UInt64? = nil,
    retryAfter: TimeInterval? = nil,
    occurredAt: Date = Date(),
    attempt: Int? = nil,
    detail: [String: String] = [:]
  ) {
    self.event = event
    self.code = code
    self.conversationIDPrefix = conversationIDPrefix
    self.epoch = epoch
    self.generation = generation
    self.stateVersion = stateVersion
    self.retryAfter = retryAfter
    self.occurredAt = occurredAt
    self.attempt = attempt
    self.detail = detail
  }
}

public enum MLSDiagnostics {
  public typealias Reporter = @Sendable (MLSDiagnosticRecord) -> Void

  private static let lock = NSLock()
  private static nonisolated(unsafe) var _reporter: Reporter?
  private static nonisolated(unsafe) var _records: [MLSDiagnosticRecord] = []
  private static let maxRecords = 200

  public static var reporter: Reporter? {
    get {
      lock.lock()
      defer { lock.unlock() }
      return _reporter
    }
    set {
      lock.lock()
      defer { lock.unlock() }
      _reporter = newValue
    }
  }

  public static func record(_ record: MLSDiagnosticRecord) {
    let activeReporter: Reporter?
    lock.lock()
    _records.append(record)
    if _records.count > maxRecords {
      _records.removeFirst(_records.count - maxRecords)
    }
    activeReporter = _reporter
    lock.unlock()
    activeReporter?(record)
  }

  public static func record(
    _ event: MLSDiagnosticEvent,
    code: String,
    conversation: String? = nil,
    epoch: UInt64? = nil,
    generation: UInt64? = nil,
    stateVersion: UInt64? = nil,
    retryAfter: TimeInterval? = nil,
    attempt: Int? = nil,
    detail: [String: String] = [:]
  ) {
    let prefix = conversation.map { String($0.prefix(16)) }
    record(
      MLSDiagnosticRecord(
        event: event,
        code: String(code.prefix(80)),
        conversationIDPrefix: prefix,
        epoch: epoch,
        generation: generation,
        stateVersion: stateVersion,
        retryAfter: retryAfter,
        occurredAt: Date(),
        attempt: attempt,
        detail: detail
      )
    )
  }

  public static func recent(limit: Int = 100) -> [MLSDiagnosticRecord] {
    lock.lock()
    defer { lock.unlock() }
    return Array(_records.suffix(max(0, limit)).reversed())
  }

  public static func clear() {
    lock.lock()
    _records.removeAll()
    lock.unlock()
  }

  public static func exportText() -> String {
    let items = recent(limit: maxRecords)
    guard !items.isEmpty else { return "No MLS diagnostic events recorded." }
    let formatter = ISO8601DateFormatter()
    return items.map { item in
      let ts = formatter.string(from: item.occurredAt)
      let convo = item.conversationIDPrefix ?? "-"
      return "[\(ts)] \(item.event.rawValue) code=\(item.code) convo=\(convo)"
    }.joined(separator: "\n")
  }
}

// MARK: - Epoch Key Retention Policy

public struct MLSEpochKeyRetentionPolicy: Sendable, Codable, Equatable {
  public let retentionPeriod: TimeInterval
  public let cleanupInterval: TimeInterval
  public let deleteCiphertextOnExpiry: Bool

  public static let conservative = MLSEpochKeyRetentionPolicy(
    retentionPeriod: 90 * 24 * 60 * 60,
    cleanupInterval: 24 * 60 * 60,
    deleteCiphertextOnExpiry: false
  )
  public static let balanced = MLSEpochKeyRetentionPolicy(
    retentionPeriod: 30 * 24 * 60 * 60,
    cleanupInterval: 24 * 60 * 60,
    deleteCiphertextOnExpiry: false
  )
  public static let aggressive = MLSEpochKeyRetentionPolicy(
    retentionPeriod: 7 * 24 * 60 * 60,
    cleanupInterval: 24 * 60 * 60,
    deleteCiphertextOnExpiry: false
  )
  public static let paranoid = MLSEpochKeyRetentionPolicy(
    retentionPeriod: 24 * 60 * 60,
    cleanupInterval: 6 * 60 * 60,
    deleteCiphertextOnExpiry: true
  )
  public static let noRetention = MLSEpochKeyRetentionPolicy(
    retentionPeriod: .infinity,
    cleanupInterval: .infinity,
    deleteCiphertextOnExpiry: false
  )

  public init(
    retentionPeriod: TimeInterval,
    cleanupInterval: TimeInterval = 24 * 60 * 60,
    deleteCiphertextOnExpiry: Bool = false
  ) {
    self.retentionPeriod = retentionPeriod
    self.cleanupInterval = cleanupInterval
    self.deleteCiphertextOnExpiry = deleteCiphertextOnExpiry
  }
}
