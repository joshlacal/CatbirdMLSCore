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

// MARK: - Embed Types

/// Rich embed data for MLS messages
public enum MLSEmbedData: Codable, Sendable, Hashable {
  case link(MLSLinkEmbed)
  case gif(MLSGIFEmbed)
  case post(MLSPostEmbed)

  enum CodingKeys: String, CodingKey {
    case type
    case data
  }

  enum EmbedType: String, Codable {
    case link
    case gif
    case post
  }

  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let type = try container.decode(EmbedType.self, forKey: .type)

    switch type {
    case .link:
      let link = try container.decode(MLSLinkEmbed.self, forKey: .data)
      self = .link(link)
    case .gif:
      let gif = try container.decode(MLSGIFEmbed.self, forKey: .data)
      self = .gif(gif)
    case .post:
      let post = try container.decode(MLSPostEmbed.self, forKey: .data)
      self = .post(post)
    }
  }

  public func encode(to encoder: Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)

    switch self {
    case .link(let link):
      try container.encode(EmbedType.link, forKey: .type)
      try container.encode(link, forKey: .data)
    case .gif(let gif):
      try container.encode(EmbedType.gif, forKey: .type)
      try container.encode(gif, forKey: .data)
    case .post(let post):
      try container.encode(EmbedType.post, forKey: .type)
      try container.encode(post, forKey: .data)
    }
  }

  public var cid: String? {
    switch self {
    case .post(let post):
      return post.cid
    case .link, .gif:
      return nil
    }
  }

  /// Decode from JSON data
  public static func fromJSONData(_ data: Data) throws -> MLSEmbedData {
    let decoder = JSONDecoder()
    decoder.dateDecodingStrategy = .iso8601
    return try decoder.decode(MLSEmbedData.self, from: data)
  }

  /// Encode to JSON data
  public func toJSONData() throws -> Data {
    let encoder = JSONEncoder()
    encoder.dateEncodingStrategy = .iso8601
    return try encoder.encode(self)
  }
}

/// Link/URL preview embed
public struct MLSLinkEmbed: Codable, Sendable, Hashable {
  public let url: String
  public let title: String?
  public let description: String?
  public let thumbnailURL: String?
  public let domain: String?

  public init(
    url: String,
    title: String? = nil,
    description: String? = nil,
    thumbnailURL: String? = nil,
    domain: String? = nil
  ) {
    self.url = url
    self.title = title
    self.description = description
    self.thumbnailURL = thumbnailURL
    self.domain = domain
  }

  enum CodingKeys: String, CodingKey {
    case url
    case title
    case description
    case thumbnailURL = "thumbnail_url"
    case domain
  }
}

/// Tenor GIF embed
public struct MLSGIFEmbed: Codable, Sendable, Hashable {
  public let tenorURL: String
  public let mp4URL: String
  public let title: String?
  public let thumbnailURL: String?
  public let width: Int?
  public let height: Int?

  public init(
    tenorURL: String,
    mp4URL: String,
    title: String? = nil,
    thumbnailURL: String? = nil,
    width: Int? = nil,
    height: Int? = nil
  ) {
    self.tenorURL = tenorURL
    self.mp4URL = mp4URL
    self.title = title
    self.thumbnailURL = thumbnailURL
    self.width = width
    self.height = height
  }

  enum CodingKeys: String, CodingKey {
    case tenorURL = "tenor_url"
    case mp4URL = "mp4_url"
    case title
    case thumbnailURL = "thumbnail_url"
    case width
    case height
  }
}

/// Bluesky post embed for sharing posts in MLS messages
public struct MLSPostEmbed: Codable, Sendable, Hashable {
  public let uri: String
  public let cid: String?
  public let authorDid: String
  public let authorHandle: String?
  public let authorDisplayName: String?
  public let authorAvatar: URL?
  public let text: String?
  public let createdAt: Date?
  public let likeCount: Int?
  public let replyCount: Int?
  public let repostCount: Int?
  public let images: [MLSPostImage]?

  /// Whether this embed has full post data or just a minimal reference
  public var needsFetch: Bool {
    text == nil || authorHandle == nil
  }

  public init(
    uri: String,
    cid: String? = nil,
    authorDid: String,
    authorHandle: String? = nil,
    authorDisplayName: String? = nil,
    authorAvatar: URL? = nil,
    text: String? = nil,
    createdAt: Date? = nil,
    likeCount: Int? = nil,
    replyCount: Int? = nil,
    repostCount: Int? = nil,
    images: [MLSPostImage]? = nil
  ) {
    self.uri = uri
    self.cid = cid
    self.authorDid = authorDid
    self.authorHandle = authorHandle
    self.authorDisplayName = authorDisplayName
    self.authorAvatar = authorAvatar
    self.text = text
    self.createdAt = createdAt
    self.likeCount = likeCount
    self.replyCount = replyCount
    self.repostCount = repostCount
    self.images = images
  }

  enum CodingKeys: String, CodingKey {
    case uri
    case cid
    case authorDid = "author_did"
    case authorHandle = "author_handle"
    case authorDisplayName = "author_display_name"
    case authorAvatar = "author_avatar"
    case text
    case createdAt = "created_at"
    case likeCount = "like_count"
    case replyCount = "reply_count"
    case repostCount = "repost_count"
    case images
  }
}

/// Image attachment in a post embed
public struct MLSPostImage: Codable, Sendable, Hashable {
  public let thumb: URL
  public let fullsize: URL?
  public let alt: String?

  public init(thumb: URL, fullsize: URL? = nil, alt: String? = nil) {
    self.thumb = thumb
    self.fullsize = fullsize
    self.alt = alt
  }
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
