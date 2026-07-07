//
//  MLSMessageModel.swift
//  Catbird
//
//  MLS message data model with payload caching
//

import Foundation
import GRDB

public enum MLSMessageProcessingState {
  public static let cached = "cached"
  public static let pendingSelfSend = "pendingSelfSend"
}

/// MLS message model with full payload storage
public struct MLSMessageModel: Codable, Sendable, Hashable, Identifiable {
  public let messageID: String
  public let currentUserDID: String
  public let conversationID: String
  public let senderID: String
  /// Legacy column. After the v31 field-encryption migration, new rows write
  /// encrypted data into `payloadEncrypted` instead. Preserved only for any
  /// pre-migration rows that survive in dev/test data.
  public let payloadJSON: Data?
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
  /// Field-level encrypted payload (replaces `payloadJSON` post-migration).
  public let payloadEncrypted: Data?
  /// HMAC over the row's stable fields, computed with the row entry key.
  public let entryHMAC: Data?
  /// Version of the content root key used to derive this row's per-entry keys.
  public let payloadKeyVersion: Int?
  /// Tombstone flag (0 = live, 1 = tombstoned). Stored as Int for SQLite compatibility.
  public let isTombstone: Int
  /// Tombstone timestamp (unix epoch milliseconds). Nil when not tombstoned.
  public let deletedAt: Int64?
  /// Edit flag (0 = never edited, 1 = edited at least once). Stored as Int for SQLite compatibility.
  public let isEdited: Int
  /// Timestamp of the most recently applied edit. Nil when never edited.
  public let editedAt: Date?
  /// Local-only bookkeeping: the `seq` of the last-applied edit message, used to
  /// enforce last-writer-wins (spec §5.7.2 step 3) across process restarts. Not
  /// part of the wire protocol.
  public let appliedEditSeq: Int64?

  public var id: String { messageID }

  // MARK: - Computed Properties

  /// Parse full payload from JSON.
  ///
  /// **Legacy column only.** Returns `nil` for rows written after the v31
  /// field-encryption migration — those rows only have `payloadEncrypted`
  /// populated and require an `MlsContext` to decrypt. Use
  /// `decryptedPayload(context:)` at every call site that consumes message
  /// content; this property is retained for the migration-window read
  /// fallback and for code paths that already have a decoded payload in
  /// hand.
  public var parsedPayload: MLSMessagePayload? {
    guard let json = payloadJSON else { return nil }
    return try? MLSMessagePayload.decodeFromJSON(json)
  }

  /// Decrypt the row's payload using the per-DID `MlsContext`'s content
  /// root key, falling back to the legacy `payloadJSON` column for rows
  /// written before the v31 field-encryption migration. Pure in-memory
  /// work — no DB roundtrip — so safe to call inside tight loops over
  /// fetched models.
  public func decryptedPayload(context: MlsContext) -> MLSMessagePayload? {
    if let encrypted = payloadEncrypted {
      guard
        let plain = try? MLSFieldEncryption.decrypt(
          context: context,
          conversationID: conversationID,
          wire: encrypted
        )
      else { return nil }
      return try? MLSMessagePayload.decodeFromJSON(plain)
    }
    return parsedPayload
  }

  /// Convenience: plaintext text from the legacy payload column only.
  /// **For new field-encrypted rows, prefer
  /// `decryptedPayload(context:)?.text`** — this property silently returns
  /// `nil` when only `payloadEncrypted` is populated.
  public var plaintext: String? {
    parsedPayload?.text
  }

  /// Convenience: embed from the legacy payload column only. Same caveat
  /// as `plaintext`.
  public var parsedEmbed: MLSEmbedData? {
    parsedPayload?.embed
  }

  /// Convenience: message type from the legacy payload column only. Same
  /// caveat as `plaintext`.
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
    validationFailureReason: String? = nil,
    payloadEncrypted: Data? = nil,
    entryHMAC: Data? = nil,
    payloadKeyVersion: Int? = nil,
    isTombstone: Int = 0,
    deletedAt: Int64? = nil,
    isEdited: Int = 0,
    editedAt: Date? = nil,
    appliedEditSeq: Int64? = nil
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
    self.payloadEncrypted = payloadEncrypted
    self.entryHMAC = entryHMAC
    self.payloadKeyVersion = payloadKeyVersion
    self.isTombstone = isTombstone
    self.deletedAt = deletedAt
    self.isEdited = isEdited
    self.editedAt = editedAt
    self.appliedEditSeq = appliedEditSeq
  }

  // MARK: - Codable

  /// Custom `init(from:)` so pre-v31 JSON snapshots (which lack the
  /// field-encryption columns) decode cleanly. Swift's synthesized
  /// `init(from:)` does not honor stored-property defaults, so
  /// `isTombstone` is decoded with `decodeIfPresent` and defaults to 0.
  /// The other newly-added properties are already `Optional<T>` and decode
  /// as nil when absent.
  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    self.messageID = try container.decode(String.self, forKey: .messageID)
    self.currentUserDID = try container.decode(String.self, forKey: .currentUserDID)
    self.conversationID = try container.decode(String.self, forKey: .conversationID)
    self.senderID = try container.decode(String.self, forKey: .senderID)
    self.payloadJSON = try container.decodeIfPresent(Data.self, forKey: .payloadJSON)
    self.wireFormat = try container.decodeIfPresent(Data.self, forKey: .wireFormat)
    self.contentType = try container.decode(String.self, forKey: .contentType)
    self.timestamp = try container.decode(Date.self, forKey: .timestamp)
    self.epoch = try container.decode(Int64.self, forKey: .epoch)
    self.sequenceNumber = try container.decode(Int64.self, forKey: .sequenceNumber)
    self.authenticatedData = try container.decodeIfPresent(Data.self, forKey: .authenticatedData)
    self.signature = try container.decodeIfPresent(Data.self, forKey: .signature)
    self.isDelivered = try container.decode(Bool.self, forKey: .isDelivered)
    self.isRead = try container.decode(Bool.self, forKey: .isRead)
    self.isSent = try container.decode(Bool.self, forKey: .isSent)
    self.sendAttempts = try container.decode(Int.self, forKey: .sendAttempts)
    self.error = try container.decodeIfPresent(String.self, forKey: .error)
    self.processingState = try container.decode(String.self, forKey: .processingState)
    self.gapBefore = try container.decode(Bool.self, forKey: .gapBefore)
    self.payloadExpired = try container.decode(Bool.self, forKey: .payloadExpired)
    self.processingError = try container.decodeIfPresent(String.self, forKey: .processingError)
    self.processingAttempts = try container.decode(Int.self, forKey: .processingAttempts)
    self.validationFailureReason = try container.decodeIfPresent(
      String.self, forKey: .validationFailureReason)
    self.payloadEncrypted = try container.decodeIfPresent(Data.self, forKey: .payloadEncrypted)
    self.entryHMAC = try container.decodeIfPresent(Data.self, forKey: .entryHMAC)
    self.payloadKeyVersion = try container.decodeIfPresent(Int.self, forKey: .payloadKeyVersion)
    self.isTombstone = try container.decodeIfPresent(Int.self, forKey: .isTombstone) ?? 0
    self.deletedAt = try container.decodeIfPresent(Int64.self, forKey: .deletedAt)
    self.isEdited = try container.decodeIfPresent(Int.self, forKey: .isEdited) ?? 0
    self.editedAt = try container.decodeIfPresent(Date.self, forKey: .editedAt)
    self.appliedEditSeq = try container.decodeIfPresent(Int64.self, forKey: .appliedEditSeq)
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
      validationFailureReason: validationFailureReason,
      payloadEncrypted: payloadEncrypted,
      entryHMAC: entryHMAC,
      payloadKeyVersion: payloadKeyVersion,
      isTombstone: isTombstone,
      deletedAt: deletedAt,
      isEdited: isEdited,
      editedAt: editedAt,
      appliedEditSeq: appliedEditSeq
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
      validationFailureReason: validationFailureReason,
      payloadEncrypted: payloadEncrypted,
      entryHMAC: entryHMAC,
      payloadKeyVersion: payloadKeyVersion,
      isTombstone: isTombstone,
      deletedAt: deletedAt,
      isEdited: isEdited,
      editedAt: editedAt,
      appliedEditSeq: appliedEditSeq
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
      validationFailureReason: validationFailureReason,
      payloadEncrypted: payloadEncrypted,
      entryHMAC: entryHMAC,
      payloadKeyVersion: payloadKeyVersion,
      isTombstone: isTombstone,
      deletedAt: deletedAt,
      isEdited: isEdited,
      editedAt: editedAt,
      appliedEditSeq: appliedEditSeq
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
      validationFailureReason: validationFailureReason,
      payloadEncrypted: payloadEncrypted,
      entryHMAC: entryHMAC,
      payloadKeyVersion: payloadKeyVersion,
      isTombstone: isTombstone,
      deletedAt: deletedAt,
      isEdited: isEdited,
      editedAt: editedAt,
      appliedEditSeq: appliedEditSeq
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
      validationFailureReason: validationFailureReason,
      payloadEncrypted: nil,  // Clear encrypted payload for forward secrecy
      entryHMAC: entryHMAC,
      payloadKeyVersion: payloadKeyVersion,
      isTombstone: isTombstone,
      deletedAt: deletedAt,
      isEdited: isEdited,
      editedAt: editedAt,
      appliedEditSeq: appliedEditSeq
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
      validationFailureReason: validationReason,
      payloadEncrypted: payloadEncrypted,
      entryHMAC: entryHMAC,
      payloadKeyVersion: payloadKeyVersion,
      isTombstone: isTombstone,
      deletedAt: deletedAt,
      isEdited: isEdited,
      editedAt: editedAt,
      appliedEditSeq: appliedEditSeq
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
    public static let payloadEncrypted = Column("payloadEncrypted")
    public static let entryHMAC = Column("entryHMAC")
    public static let payloadKeyVersion = Column("payloadKeyVersion")
    public static let isTombstone = Column("isTombstone")
    public static let deletedAt = Column("deletedAt")
    public static let isEdited = Column("isEdited")
    public static let editedAt = Column("editedAt")
    public static let appliedEditSeq = Column("appliedEditSeq")
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

    // Field-level encryption columns (nullable in pre-migration rows)
    payloadEncrypted = row["payloadEncrypted"]
    entryHMAC = row["entryHMAC"]
    payloadKeyVersion = row["payloadKeyVersion"]
    isTombstone = row["isTombstone"] ?? 0
    deletedAt = row["deletedAt"]
    isEdited = row["isEdited"] ?? 0
    editedAt = row["editedAt"]
    appliedEditSeq = row["appliedEditSeq"]
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
    container["payloadEncrypted"] = payloadEncrypted
    container["entryHMAC"] = entryHMAC
    container["payloadKeyVersion"] = payloadKeyVersion
    container["isTombstone"] = isTombstone
    container["deletedAt"] = deletedAt
    container["isEdited"] = isEdited
    container["editedAt"] = editedAt
    container["appliedEditSeq"] = appliedEditSeq
  }
}

// MARK: - Helpers

extension MLSMessageModel {
  /// Check if message has payload available
  public var hasPayload: Bool {
    payloadJSON != nil && !payloadExpired
  }

  /// True only after the row contains a successfully decoded plaintext payload.
  public var hasProcessedPayloadForOrdering: Bool {
    processingState != MLSMessageProcessingState.pendingSelfSend
      && processingError == nil
      && !payloadExpired
      && parsedPayload != nil
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
