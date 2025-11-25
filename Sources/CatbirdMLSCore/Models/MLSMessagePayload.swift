import Foundation



// MARK: - Message Payload

/// Encrypted message payload structure for MLS messages
/// This structure is encoded to JSON and then encrypted
public struct MLSMessagePayload: Codable {
  /// Protocol version for future compatibility
  public let version: Int

  /// Message text content
  public let text: String

  /// Optional embed data (record, link, or GIF)
  public let embed: MLSEmbedData?

  public init(text: String, embed: MLSEmbedData? = nil, version: Int = 1) {
    self.version = version
    self.text = text
    self.embed = embed
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


