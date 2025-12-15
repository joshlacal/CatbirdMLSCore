import Foundation

// MARK: - Message Type Enum

/// Discriminator for MLS message payload types
public enum MLSMessageType: String, Codable, Sendable, Equatable {
  case text
  case reaction
  case readReceipt
  case typing
  case adminRoster
  case adminAction
}

// MARK: - Reaction Payload

/// Encrypted reaction to a message (add or remove emoji)
public struct MLSReactionPayload: Codable, Sendable, Equatable {
  public let messageId: String
  public let emoji: String
  public let action: ReactionAction

  public enum ReactionAction: String, Codable, Sendable {
    case add
    case remove
  }

  public init(messageId: String, emoji: String, action: ReactionAction) {
    self.messageId = messageId
    self.emoji = emoji
    self.action = action
  }
}

// MARK: - Read Receipt Payload

/// Encrypted read receipt for a specific message
public struct MLSReadReceiptPayload: Codable, Sendable, Equatable {
  public let messageId: String

  public init(messageId: String) {
    self.messageId = messageId
  }
}

// MARK: - Typing Payload

/// Encrypted typing indicator (ephemeral, not stored)
/// Sent as MLS PrivateMessage without epoch advancement
public struct MLSTypingPayload: Codable, Sendable, Equatable {
  public let isTyping: Bool
  public let ts: Int64?

  public init(isTyping: Bool, ts: Int64? = nil) {
    self.isTyping = isTyping
    self.ts = ts ?? Int64(Date().timeIntervalSince1970 * 1000)
  }
}

// MARK: - Admin Roster Payload

/// Encrypted admin roster distributed via MLS application messages
public struct MLSAdminRosterPayload: Codable, Sendable, Equatable {
  public let version: Int
  public let admins: [String]
  public let hash: String?

  public init(version: Int, admins: [String], hash: String? = nil) {
    self.version = version
    self.admins = admins
    self.hash = hash
  }
}

// MARK: - Admin Action Payload

/// Admin action notification (E2EE)
public struct MLSAdminActionPayload: Codable, Sendable, Equatable {
  public let action: AdminAction
  public let targetDid: String
  public let timestamp: Date
  public let reason: String?

  public enum AdminAction: String, Codable, Sendable {
    case promote
    case demote
    case remove
  }

  public init(
    action: AdminAction, targetDid: String, timestamp: Date = Date(), reason: String? = nil
  ) {
    self.action = action
    self.targetDid = targetDid
    self.timestamp = timestamp
    self.reason = reason
  }
}

// MARK: - Message Payload

/// Encrypted message payload structure for MLS messages
/// This structure is encoded to JSON and then encrypted
public struct MLSMessagePayload: Codable, Sendable {
  /// Protocol version for future compatibility
  public let version: Int

  /// Message type discriminator (required)
  public let messageType: MLSMessageType

  /// Message text content (for messageType: text)
  public let text: String?

  /// Optional embed data (record, link, or GIF)
  public let embed: MLSEmbedData?

  /// Reaction payload (for messageType: reaction)
  public let reaction: MLSReactionPayload?

  /// Read receipt payload (for messageType: readReceipt)
  public let readReceipt: MLSReadReceiptPayload?

  /// Typing indicator payload (for messageType: typing)
  public let typing: MLSTypingPayload?

  /// Admin roster payload (for messageType: adminRoster)
  public let adminRoster: MLSAdminRosterPayload?

  /// Admin action payload (for messageType: adminAction)
  public let adminAction: MLSAdminActionPayload?

  // MARK: - Memberwise Init

  public init(
    version: Int = 1,
    messageType: MLSMessageType,
    text: String? = nil,
    embed: MLSEmbedData? = nil,
    reaction: MLSReactionPayload? = nil,
    readReceipt: MLSReadReceiptPayload? = nil,
    typing: MLSTypingPayload? = nil,
    adminRoster: MLSAdminRosterPayload? = nil,
    adminAction: MLSAdminActionPayload? = nil
  ) {
    self.version = version
    self.messageType = messageType
    self.text = text
    self.embed = embed
    self.reaction = reaction
    self.readReceipt = readReceipt
    self.typing = typing
    self.adminRoster = adminRoster
    self.adminAction = adminAction
  }

  // MARK: - Factory Methods

  /// Create a text message payload
  public static func text(_ text: String, embed: MLSEmbedData? = nil) -> MLSMessagePayload {
    MLSMessagePayload(
      messageType: .text,
      text: text,
      embed: embed
    )
  }

  /// Create a reaction payload
  public static func reaction(
    messageId: String,
    emoji: String,
    action: MLSReactionPayload.ReactionAction
  ) -> MLSMessagePayload {
    MLSMessagePayload(
      messageType: .reaction,
      reaction: MLSReactionPayload(messageId: messageId, emoji: emoji, action: action)
    )
  }

  /// Create a read receipt payload
  public static func readReceipt(messageId: String) -> MLSMessagePayload {
    MLSMessagePayload(
      messageType: .readReceipt,
      readReceipt: MLSReadReceiptPayload(messageId: messageId)
    )
  }

  /// Create a typing indicator payload
  public static func typing(isTyping: Bool) -> MLSMessagePayload {
    MLSMessagePayload(
      messageType: .typing,
      typing: MLSTypingPayload(isTyping: isTyping)
    )
  }

  /// Create an admin roster payload
  public static func adminRoster(version: Int, admins: [String], hash: String? = nil)
    -> MLSMessagePayload
  {
    MLSMessagePayload(
      messageType: .adminRoster,
      adminRoster: MLSAdminRosterPayload(version: version, admins: admins, hash: hash)
    )
  }

  /// Create an admin action payload
  public static func adminAction(
    action: MLSAdminActionPayload.AdminAction,
    targetDid: String,
    reason: String? = nil
  ) -> MLSMessagePayload {
    MLSMessagePayload(
      messageType: .adminAction,
      adminAction: MLSAdminActionPayload(action: action, targetDid: targetDid, reason: reason)
    )
  }
}

// MARK: - Payload Encoding/Decoding

extension MLSMessagePayload {
  /// Encode payload to JSON data for encryption
  public func encodeToJSON() throws -> Data {
    let encoder = JSONEncoder()
    encoder.dateEncodingStrategy = .iso8601
    return try encoder.encode(self)
  }

  /// Decode payload from JSON data after decryption
  public static func decodeFromJSON(_ data: Data) throws -> MLSMessagePayload {
    let decoder = JSONDecoder()
    decoder.dateDecodingStrategy = .iso8601
    return try decoder.decode(MLSMessagePayload.self, from: data)
  }
}
