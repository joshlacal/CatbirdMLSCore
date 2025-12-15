//
//  MLSEmbedData.swift
//  Catbird
//
//  MLS message embed data types
//

import Foundation

/// Rich embed data for MLS messages
public enum MLSEmbedData: Codable, Sendable, Hashable {
  case link(MLSLinkEmbed)
  case gif(MLSGIFEmbed)
  case post(MLSPostEmbed)

  // MARK: - Codable

  enum CodingKeys: String, CodingKey {
    case type
    case data
  }

  enum EmbedType: String, Codable {
    case record  // Legacy - migrates to post
    case link
    case gif
    case post
  }

  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let type = try container.decode(EmbedType.self, forKey: .type)

    switch type {
    case .record:
      // Migrate legacy record embeds to post embeds
      let legacy = try container.decode(LegacyRecordEmbed.self, forKey: .data)
      self = .post(legacy.toPostEmbed())
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
}

// MARK: - Legacy Record Embed (for migration)

/// Legacy record embed type - automatically migrates to MLSPostEmbed on decode
private struct LegacyRecordEmbed: Codable {
  let uri: String
  let cid: String?
  let authorDID: String
  let previewText: String?
  let createdAt: Date?

  enum CodingKeys: String, CodingKey {
    case uri
    case cid
    case authorDID = "author_did"
    case previewText = "preview_text"
    case createdAt = "created_at"
  }

  func toPostEmbed() -> MLSPostEmbed {
    MLSPostEmbed(
      uri: uri,
      cid: cid,
      authorDid: authorDID,
      text: previewText,
      createdAt: createdAt
    )
  }
}

// MARK: - Link Embed

/// Link/URL preview embed
public struct MLSLinkEmbed: Codable, Sendable, Hashable {
  public let url: String
  public let title: String?
  public let description: String?
  public let thumbnailURL: String?
  public let domain: String?

  public init(url: String, title: String? = nil, description: String? = nil, thumbnailURL: String? = nil, domain: String? = nil) {
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

// MARK: - GIF Embed

/// Tenor GIF embed
public struct MLSGIFEmbed: Codable, Sendable, Hashable {
  public let tenorURL: String
  public let mp4URL: String
  public let title: String?
  public let thumbnailURL: String?
  public let width: Int?
  public let height: Int?

  public init(tenorURL: String, mp4URL: String, title: String? = nil, thumbnailURL: String? = nil, width: Int? = nil, height: Int? = nil) {
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

// MARK: - Post Embed

/// Bluesky post embed for sharing posts in MLS messages
/// Supports both full post data (from share action) and minimal references (that need API fetch)
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
    // If we don't have text or author handle, we need to fetch full data
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

// MARK: - JSON Serialization Helpers

extension MLSEmbedData {
  /// Serialize to JSON data for storage
  public func toJSONData() throws -> Data {
    let encoder = JSONEncoder()
    encoder.dateEncodingStrategy = .iso8601
    return try encoder.encode(self)
  }

  /// Deserialize from JSON data
  public static func fromJSONData(_ data: Data) throws -> MLSEmbedData {
    let decoder = JSONDecoder()
    decoder.dateDecodingStrategy = .iso8601
    return try decoder.decode(MLSEmbedData.self, from: data)
  }
}
