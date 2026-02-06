//
//  MLSMessageModel.swift
//  Catbird
//
//  MLS message data model with payload caching
//

import Foundation
import GRDB

/// MLS message model with full payload storage
public struct MLSMessageModel: Codable, Sendable, Hashable, Identifiable {
  public let messageID: String
  public let currentUserDID: String
  public let conversationID: String
  public let senderID: String
  public let payloadJSON: Data?  // Full MLSMessagePayload as JSON
  public let wireFormat: Data?
  public let contentType: String
  public let timestamp: Date
  public let epoch: Int64
  public let sequenceNumber: Int64
  public let authenticatedData: Data?
  public let signature: Data?
  public let isDelivered: Bool
  public let isRead: Bool
  public let isSent: Bool
  public let sendAttempts: Int
  public let error: String?
  public let processingState: String
  public let gapBefore: Bool
  public let payloadExpired: Bool
  public let processingError: String?
  public let processingAttempts: Int
  public let validationFailureReason: String?

  public var id: String { messageID }

  // MARK: - Computed Properties

  /// Parse full payload from JSON
  public var parsedPayload: MLSMessagePayload? {
    guard let json = payloadJSON else { return nil }
    return try? MLSMessagePayload.decodeFromJSON(json)
  }

  /// Get plaintext from payload (convenience)
  public var plaintext: String? {
    parsedPayload?.text
  }

  /// Get embed from payload (convenience)
  public var parsedEmbed: MLSEmbedData? {
    parsedPayload?.embed
  }

  /// Message type from payload
  public var messageType: MLSMessageType? {
    parsedPayload?.messageType
  }

  // MARK: - Initialization

  public init(
    messageID: String,
    currentUserDID: String,
    conversationID: String,
    senderID: String,
    payloadJSON: Data? = nil,
    wireFormat: Data? = nil,
    contentType: String = "application/json",
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
    payloadExpired: Bool = false,
    processingError: String? = nil,
    processingAttempts: Int = 0,
    validationFailureReason: String? = nil
  ) {
    self.messageID = messageID
    self.currentUserDID = currentUserDID
    self.conversationID = conversationID
    self.senderID = senderID
    self.payloadJSON = payloadJSON
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
    self.payloadExpired = payloadExpired
    self.processingError = processingError
    self.processingAttempts = processingAttempts
    self.validationFailureReason = validationFailureReason
  }

  // MARK: - Update Methods

  /// Create copy with payload cached
  public func withPayload(_ payload: MLSMessagePayload) -> MLSMessageModel {
    let payloadData = try? payload.encodeToJSON()
    return MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      payloadJSON: payloadData,
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
      payloadExpired: false,
      processingError: processingError,
      processingAttempts: processingAttempts,
      validationFailureReason: validationFailureReason
    )
  }

  /// Create copy marked as sent
  public func withSentStatus() -> MLSMessageModel {
    MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      payloadJSON: payloadJSON,
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
      payloadExpired: payloadExpired,
      processingError: processingError,
      processingAttempts: processingAttempts,
      validationFailureReason: validationFailureReason
    )
  }

  /// Create copy marked as read
  public func withReadStatus() -> MLSMessageModel {
    MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      payloadJSON: payloadJSON,
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
      payloadExpired: payloadExpired,
      processingError: processingError,
      processingAttempts: processingAttempts,
      validationFailureReason: validationFailureReason
    )
  }

  /// Create copy with send error
  public func withError(_ errorMessage: String) -> MLSMessageModel {
    MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      payloadJSON: payloadJSON,
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
      payloadExpired: payloadExpired,
      processingError: processingError,
      processingAttempts: processingAttempts,
      validationFailureReason: validationFailureReason
    )
  }

  /// Create copy marked as expired (forward secrecy)
  public func withExpiredPayload() -> MLSMessageModel {
    MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      payloadJSON: nil,  // Clear payload
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
      payloadExpired: true,
      processingError: processingError,
      processingAttempts: processingAttempts,
      validationFailureReason: validationFailureReason
    )
  }

  /// Create copy with processing error
  public func withProcessingError(_ errorMessage: String, validationReason: String? = nil)
    -> MLSMessageModel
  {
    MLSMessageModel(
      messageID: messageID,
      currentUserDID: currentUserDID,
      conversationID: conversationID,
      senderID: senderID,
      payloadJSON: payloadJSON,
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
      payloadExpired: payloadExpired,
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
    public static let payloadJSON = Column("payloadJSON")
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
    public static let payloadExpired = Column("payloadExpired")
    public static let processingError = Column("processingError")
    public static let processingAttempts = Column("processingAttempts")
    public static let validationFailureReason = Column("validationFailureReason")
  }

  /// Custom init to handle both old (plaintext/embedData) and new (payloadJSON) column names
  public init(row: Row) throws {
    messageID = row["messageID"] ?? ""
    currentUserDID = row["currentUserDID"] ?? ""
    conversationID = row["conversationID"] ?? ""
    senderID = row["senderID"] ?? ""
    
    // Handle payloadJSON with fallback from old plaintext + embedData columns
    if let payload: Data = row["payloadJSON"] {
      payloadJSON = payload
    } else if let plaintext: String = row["plaintext"] {
      // Migrate from old format: create payloadJSON from plaintext + embedData
      let embedData: Data? = row["embedData"]
      if let embedData = embedData,
         let embed = try? MLSEmbedData.fromJSONData(embedData) {
        let payload = MLSMessagePayload.text(plaintext, embed: embed)
        payloadJSON = try? payload.encodeToJSON()
      } else {
        let payload = MLSMessagePayload.text(plaintext, embed: nil)
        payloadJSON = try? payload.encodeToJSON()
      }
    } else {
      payloadJSON = nil
    }
    
    wireFormat = row["wireFormat"]
    contentType = row["contentType"] ?? "application/json"
    timestamp = row["timestamp"] ?? Date()
    epoch = row["epoch"] ?? 0
    sequenceNumber = row["sequenceNumber"] ?? 0
    authenticatedData = row["authenticatedData"]
    signature = row["signature"]
    isDelivered = row["isDelivered"] ?? false
    isRead = row["isRead"] ?? false
    isSent = row["isSent"] ?? false
    sendAttempts = row["sendAttempts"] ?? 0
    error = row["error"]
    processingState = row["processingState"] ?? "unknown"
    gapBefore = row["gapBefore"] ?? false
    
    // Handle payloadExpired with fallback from old plaintextExpired column
    if let expired: Bool = row["payloadExpired"] {
      payloadExpired = expired
    } else if let expired: Bool = row["plaintextExpired"] {
      payloadExpired = expired
    } else {
      payloadExpired = false
    }
    
    processingError = row["processingError"]
    processingAttempts = row["processingAttempts"] ?? 0
    validationFailureReason = row["validationFailureReason"]
  }

  /// Encode for persistence (only uses new column names)
  public func encode(to container: inout PersistenceContainer) throws {
    container["messageID"] = messageID
    container["currentUserDID"] = currentUserDID
    container["conversationID"] = conversationID
    container["senderID"] = senderID
    container["payloadJSON"] = payloadJSON
    container["wireFormat"] = wireFormat
    container["contentType"] = contentType
    container["timestamp"] = timestamp
    container["epoch"] = epoch
    container["sequenceNumber"] = sequenceNumber
    container["authenticatedData"] = authenticatedData
    container["signature"] = signature
    container["isDelivered"] = isDelivered
    container["isRead"] = isRead
    container["isSent"] = isSent
    container["sendAttempts"] = sendAttempts
    container["error"] = error
    container["processingState"] = processingState
    container["gapBefore"] = gapBefore
    container["payloadExpired"] = payloadExpired
    container["processingError"] = processingError
    container["processingAttempts"] = processingAttempts
    container["validationFailureReason"] = validationFailureReason
  }
}

// MARK: - Helpers

extension MLSMessageModel {
  /// Check if message has payload available
  public var hasPayload: Bool {
    payloadJSON != nil && !payloadExpired
  }

  /// Check if message can be retried
  public var canRetry: Bool {
    !isSent && sendAttempts < 3
  }

  /// Display text (handles expired messages)
  public var displayText: String {
    if payloadExpired {
      return "🔒 Message expired (forward secrecy)"
    } else if let text = plaintext {
      return text
    } else {
      return "[Encrypted]"
    }
  }
}
