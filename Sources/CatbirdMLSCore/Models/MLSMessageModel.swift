//
//  MLSMessageModel.swift
//  Catbird
//
//  MLS message data model with plaintext caching
//

import Foundation
import GRDB

/// MLS message model (critical for plaintext caching)
public struct MLSMessageModel: Codable, Sendable, Hashable, Identifiable {
  public   let messageID: String
  public   let currentUserDID: String
  public   let conversationID: String
  public   let senderID: String
  public   let plaintext: String?
  public   let embedDataJSON: Data?  // Stores MLSEmbedData as JSON
  public   let wireFormat: Data?
  public   let contentType: String
  public   let timestamp: Date
  public   let epoch: Int64
  public   let sequenceNumber: Int64
  public   let authenticatedData: Data?
  public   let signature: Data?
  public   let isDelivered: Bool
  public   let isRead: Bool
  public   let isSent: Bool
  public   let sendAttempts: Int
  public   let error: String?
  public   let processingState: String
  public   let gapBefore: Bool
  public   let plaintextExpired: Bool
  public   let processingError: String?  // Specific error from decryption/processing
  public   let processingAttempts: Int  // Number of times processing was attempted
  public   let validationFailureReason: String?  // Why pre-processing validation failed

  public var id: String { messageID }

  // MARK: - Computed Properties

  /// Parse embedData from JSON
  public   var parsedEmbed: MLSEmbedData? {
    guard let json = embedDataJSON else { return nil }
    return try? MLSEmbedData.fromJSONData(json)
  }

  // MARK: - Initialization

  public init(
    messageID: String,
    currentUserDID: String,
    conversationID: String,
    senderID: String,
    plaintext: String? = nil,
    embedDataJSON: Data? = nil,
    wireFormat: Data? = nil,
    contentType: String = "text",
    timestamp: Date = Date(),
    epoch: Int64,
    sequenceNumber: Int64,
    authenticatedData: Data? = nil,
    signature: Data? = nil,
    isDelivered: Bool = false,
    isRead: Bool = false,
    isSent: Bool = false,
    sendAttempts: Int = 0,
    error: String? = nil,
    processingState: String = "delivered",
    gapBefore: Bool = false,
    plaintextExpired: Bool = false,
    processingError: String? = nil,
    processingAttempts: Int = 0,
    validationFailureReason: String? = nil
  ) {
    self.messageID = messageID
    self.currentUserDID = currentUserDID
    self.conversationID = conversationID
    self.senderID = senderID
    self.plaintext = plaintext
    self.embedDataJSON = embedDataJSON
    self.wireFormat = wireFormat
    self.contentType = contentType
    self.timestamp = timestamp
    self.epoch = epoch
    self.sequenceNumber = sequenceNumber
    self.authenticatedData = authenticatedData
    self.signature = signature
    self.isDelivered = isDelivered
    self.isRead = isRead
    self.isSent = isSent
    self.sendAttempts = sendAttempts
    self.error = error
    self.processingState = processingState
    self.gapBefore = gapBefore
    self.plaintextExpired = plaintextExpired
    self.processingError = processingError
    self.processingAttempts = processingAttempts
    self.validationFailureReason = validationFailureReason
  }

  // MARK: - Update Methods

  /// Create copy with plaintext cached
  func withPlaintext(_ text: String, embedData: MLSEmbedData? = nil) -> MLSMessageModel {
    let embedJSON = embedData.flatMap { try? $0.toJSONData() }
    return MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      plaintext: text,
      embedDataJSON: embedJSON,
      wireFormat: wireFormat,
      contentType: contentType,
      timestamp: timestamp,
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      authenticatedData: authenticatedData,
      signature: signature,
      isDelivered: isDelivered,
      isRead: isRead,
      isSent: isSent,
      sendAttempts: sendAttempts,
      error: error,
      processingState: processingState,
      gapBefore: gapBefore,
      plaintextExpired: plaintextExpired,
      processingError: processingError,
      processingAttempts: processingAttempts,
      validationFailureReason: validationFailureReason
    )
  }

  /// Create copy marked as sent
  func withSentStatus() -> MLSMessageModel {
    MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      plaintext: plaintext,
      embedDataJSON: embedDataJSON,
      wireFormat: wireFormat,
      contentType: contentType,
      timestamp: timestamp,
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      authenticatedData: authenticatedData,
      signature: signature,
      isDelivered: true,
      isRead: isRead,
      isSent: true,
      sendAttempts: sendAttempts,
      error: nil,
      processingState: processingState,
      gapBefore: gapBefore,
      plaintextExpired: plaintextExpired,
      processingError: processingError,
      processingAttempts: processingAttempts,
      validationFailureReason: validationFailureReason
    )
  }

  /// Create copy marked as read
  func withReadStatus() -> MLSMessageModel {
    MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      plaintext: plaintext,
      embedDataJSON: embedDataJSON,
      wireFormat: wireFormat,
      contentType: contentType,
      timestamp: timestamp,
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      authenticatedData: authenticatedData,
      signature: signature,
      isDelivered: isDelivered,
      isRead: true,
      isSent: isSent,
      sendAttempts: sendAttempts,
      error: error,
      processingState: processingState,
      gapBefore: gapBefore,
      plaintextExpired: plaintextExpired,
      processingError: processingError,
      processingAttempts: processingAttempts,
      validationFailureReason: validationFailureReason
    )
  }

  /// Create copy with send error
  func withError(_ errorMessage: String) -> MLSMessageModel {
    MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      plaintext: plaintext,
      embedDataJSON: embedDataJSON,
      wireFormat: wireFormat,
      contentType: contentType,
      timestamp: timestamp,
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      authenticatedData: authenticatedData,
      signature: signature,
      isDelivered: false,
      isRead: isRead,
      isSent: false,
      sendAttempts: sendAttempts + 1,
      error: errorMessage,
      processingState: "failed",
      gapBefore: gapBefore,
      plaintextExpired: plaintextExpired,
      processingError: processingError,
      processingAttempts: processingAttempts,
      validationFailureReason: validationFailureReason
    )
  }

  /// Create copy marked as expired (forward secrecy)
  func withExpiredPlaintext() -> MLSMessageModel {
    MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      plaintext: nil, // Clear plaintext
      embedDataJSON: nil, // Clear embed
      wireFormat: wireFormat,
      contentType: contentType,
      timestamp: timestamp,
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      authenticatedData: authenticatedData,
      signature: signature,
      isDelivered: isDelivered,
      isRead: isRead,
      isSent: isSent,
      sendAttempts: sendAttempts,
      error: error,
      processingState: processingState,
      gapBefore: gapBefore,
      plaintextExpired: true,
      processingError: processingError,
      processingAttempts: processingAttempts,
      validationFailureReason: validationFailureReason
    )
  }

  /// Create copy with processing error
  func withProcessingError(_ errorMessage: String, validationReason: String? = nil) -> MLSMessageModel {
    MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      plaintext: plaintext,
      embedDataJSON: embedDataJSON,
      wireFormat: wireFormat,
      contentType: contentType,
      timestamp: timestamp,
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      authenticatedData: authenticatedData,
      signature: signature,
      isDelivered: isDelivered,
      isRead: isRead,
      isSent: isSent,
      sendAttempts: sendAttempts,
      error: error,
      processingState: processingState,
      gapBefore: gapBefore,
      plaintextExpired: plaintextExpired,
      processingError: errorMessage,
      processingAttempts: processingAttempts + 1,
      validationFailureReason: validationReason
    )
  }
}

// MARK: - GRDB Conformance
extension MLSMessageModel: FetchableRecord, PersistableRecord {
  public static let databaseTableName = "MLSMessageModel"

  public enum Columns {
    public static let messageID = Column("messageID")
    public static let currentUserDID = Column("currentUserDID")
    public static let conversationID = Column("conversationID")
    public static let senderID = Column("senderID")
    public static let plaintext = Column("plaintext")
    public static let embedDataJSON = Column("embedData")
    public static let wireFormat = Column("wireFormat")
    public static let contentType = Column("contentType")
    public static let timestamp = Column("timestamp")
    public static let epoch = Column("epoch")
    public static let sequenceNumber = Column("sequenceNumber")
    public static let authenticatedData = Column("authenticatedData")
    public static let signature = Column("signature")
    public static let isDelivered = Column("isDelivered")
    public static let isRead = Column("isRead")
    public static let isSent = Column("isSent")
    public static let sendAttempts = Column("sendAttempts")
    public static let error = Column("error")
    public static let processingState = Column("processingState")
    public static let gapBefore = Column("gapBefore")
    public static let plaintextExpired = Column("plaintextExpired")
    public static let processingError = Column("processingError")
    public static let processingAttempts = Column("processingAttempts")
    public static let validationFailureReason = Column("validationFailureReason")
  }

  enum CodingKeys: String, CodingKey {
    case messageID
    case currentUserDID
    case conversationID
    case senderID
    case plaintext
    case embedDataJSON = "embedData"
    case wireFormat
    case contentType
    case timestamp
    case epoch
    case sequenceNumber
    case authenticatedData
    case signature
    case isDelivered
    case isRead
    case isSent
    case sendAttempts
    case error
    case processingState
    case gapBefore
    case plaintextExpired
    case processingError
    case processingAttempts
    case validationFailureReason
  }
}

// MARK: - Helpers

extension MLSMessageModel {
  /// Check if message has plaintext available
  public   var hasPlaintext: Bool {
    plaintext != nil && !plaintextExpired
  }

  /// Check if message can be retried
  public   var canRetry: Bool {
    !isSent && sendAttempts < 3
  }

  /// Display text (handles expired messages)
  public   var displayText: String {
    if plaintextExpired {
      return "🔒 Message expired (forward secrecy)"
    } else if let text = plaintext {
      return text
    } else {
      return "[Encrypted]"
    }
  }
}
