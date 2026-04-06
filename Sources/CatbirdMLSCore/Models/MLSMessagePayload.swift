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
  case system
  case deliveryAck       // proof of successful decryption
  case recoveryRequest   // request re-delivery of a missed message
}

// MARK: - Embed Types

/// Rich embed data for MLS messages
public enum MLSEmbedData: Codable, Sendable, Hashable {
  case link(MLSLinkEmbed)
  case gif(MLSGIFEmbed)
  case post(MLSPostEmbed)
  case image(MLSImageEmbed)
  case audio(MLSAudioEmbed)
  case unknown(type: String)

  enum CodingKeys: String, CodingKey {
    case type
    case data
  }

  enum EmbedType: String, Codable {
    case link
    case gif
    case post
    case image
    case audio
  }

  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let typeString = try container.decode(String.self, forKey: .type)

    guard let type = EmbedType(rawValue: typeString) else {
      self = .unknown(type: typeString)
      return
    }

    switch type {
    case .link:
      let data = try container.decode(MLSLinkEmbed.self, forKey: .data)
      self = .link(data)
    case .gif:
      let data = try container.decode(MLSGIFEmbed.self, forKey: .data)
      self = .gif(data)
    case .post:
      let data = try container.decode(MLSPostEmbed.self, forKey: .data)
      self = .post(data)
    case .image:
      let data = try container.decode(MLSImageEmbed.self, forKey: .data)
      self = .image(data)
    case .audio:
      let data = try container.decode(MLSAudioEmbed.self, forKey: .data)
      self = .audio(data)
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
    case .image(let data):
      try container.encode(EmbedType.image, forKey: .type)
      try container.encode(data, forKey: .data)
    case .audio(let data):
      try container.encode(EmbedType.audio, forKey: .type)
      try container.encode(data, forKey: .data)
    case .unknown(let type):
      try container.encode(type, forKey: .type)
      // Encode empty object for data to maintain structure
      try container.encode([String: String](), forKey: .data)
    }
  }

  public var cid: String? {
    switch self {
    case .post(let post):
      return post.cid
    case .link, .gif, .image, .audio, .unknown:
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

// MARK: - Image Embed

/// A first-class image attachment in MLS DMs.
/// The image bytes are encrypted separately (AES-256-GCM) and stored as a blob.
/// This embed carries the decryption key and metadata inside the E2EE MLS message.
public struct MLSImageEmbed: Codable, Sendable, Hashable {
  public let blobId: String
  public let key: Data
  public let iv: Data
  public let sha256: String
  public let contentType: String
  public let size: Int
  public let width: Int
  public let height: Int
  public let altText: String?
  public let blurhash: String?

  public init(
    blobId: String,
    key: Data,
    iv: Data,
    sha256: String,
    contentType: String,
    size: Int,
    width: Int,
    height: Int,
    altText: String? = nil,
    blurhash: String? = nil
  ) {
    self.blobId = blobId
    self.key = key
    self.iv = iv
    self.sha256 = sha256
    self.contentType = contentType
    self.size = size
    self.width = width
    self.height = height
    self.altText = altText
    self.blurhash = blurhash
  }

  enum CodingKeys: String, CodingKey {
    case blobId = "blob_id"
    case key, iv, sha256
    case contentType = "content_type"
    case size, width, height
    case altText = "alt_text"
    case blurhash
  }
}

// MARK: - Audio Embed

/// Encrypted voice message attachment
public struct MLSAudioEmbed: Codable, Sendable, Hashable {
  public let blobId: String
  public let key: Data
  public let iv: Data
  public let sha256: String
  public let contentType: String
  public let size: UInt64
  public let durationMs: UInt64
  public let waveform: [Float]
  public let transcript: String?

  public init(
    blobId: String,
    key: Data,
    iv: Data,
    sha256: String,
    contentType: String,
    size: UInt64,
    durationMs: UInt64,
    waveform: [Float],
    transcript: String? = nil
  ) {
    self.blobId = blobId
    self.key = key
    self.iv = iv
    self.sha256 = sha256
    self.contentType = contentType
    self.size = size
    self.durationMs = durationMs
    self.waveform = waveform
    self.transcript = transcript
  }

  enum CodingKeys: String, CodingKey {
    case blobId = "blob_id"
    case key, iv, sha256
    case contentType = "content_type"
    case size
    case durationMs = "duration_ms"
    case waveform
    case transcript
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

// MARK: - Delivery Ack Payload

/// Emitted immediately after a device successfully decrypts a message.
/// Sender DID is proven via MLS credential — not included in payload.
public struct MLSDeliveryAckPayload: Codable, Sendable, Equatable {
  public let messageId: String

  public init(messageId: String) {
    self.messageId = messageId
  }
}

/// Emitted by a recipient device that failed to decrypt or missed a message.
/// Distinct from MLS group recovery (epoch/tree divergence).
public struct MLSMessageRecoveryRequestPayload: Codable, Sendable, Equatable {
  public let messageId: String
  public let epoch: Int64
  public let sequenceNumber: Int64

  public init(messageId: String, epoch: Int64, sequenceNumber: Int64) {
    self.messageId = messageId
    self.epoch = epoch
    self.sequenceNumber = sequenceNumber
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

  /// Delivery ack payload (for messageType: deliveryAck)
  public let deliveryAck: MLSDeliveryAckPayload?

  /// Message recovery request payload (for messageType: recoveryRequest)
  public let recoveryRequest: MLSMessageRecoveryRequestPayload?

  /// Set on a re-sent message to identify which original message this recovers.
  /// Nil on normal messages.
  public let recoveredMessageId: String?

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
    adminAction: MLSAdminActionPayload? = nil,
    deliveryAck: MLSDeliveryAckPayload? = nil,
    recoveryRequest: MLSMessageRecoveryRequestPayload? = nil,
    recoveredMessageId: String? = nil
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
    self.deliveryAck = deliveryAck
    self.recoveryRequest = recoveryRequest
    self.recoveredMessageId = recoveredMessageId
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

  /// Create a delivery ack payload (sent after successful decryption)
  public static func deliveryAck(messageId: String) -> MLSMessagePayload {
    MLSMessagePayload(
      messageType: .deliveryAck,
      deliveryAck: MLSDeliveryAckPayload(messageId: messageId)
    )
  }

  /// Create a message recovery request (sent when decryption fails after retries)
  public static func recoveryRequest(
    messageId: String,
    epoch: Int64,
    sequenceNumber: Int64
  ) -> MLSMessagePayload {
    MLSMessagePayload(
      messageType: .recoveryRequest,
      recoveryRequest: MLSMessageRecoveryRequestPayload(
        messageId: messageId,
        epoch: epoch,
        sequenceNumber: sequenceNumber
      )
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
