//
//  MLSStorage.swift
//  Catbird
//
//  MLS SQLCipher storage layer providing CRUD operations for encrypted messages.
//  Designed for Swift 6 concurrency with proper actor isolation.
//

import Foundation
import GRDB
import OSLog

/// Type alias for database writer - supports both DatabaseQueue and DatabasePool
public typealias MLSDatabase = any DatabaseWriter

/// MLS Storage Manager providing encrypted database operations using SQLCipher.
///
/// Thread-safe through GRDB's built-in concurrency handling.
/// All methods accept a DatabaseWriter (DatabaseQueue or DatabasePool) for operations.
/// Uses `@unchecked Sendable` because GRDB handles its own thread-safety internally.
public final class MLSStorage: @unchecked Sendable {

  // MARK: - Properties

  public static let shared = MLSStorage()

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSStorage")

  // MARK: - Initialization

  private init() {
    logger.info("MLSStorage initialized with SQLCipher backend (DatabasePool)")
  }

  // MARK: - DID Normalization

  /// Normalize a DID for consistent database storage and lookup
  /// Uses the shared implementation from MLSStorageHelpers
  private func normalizeDID(_ did: String) -> String {
    return MLSStorageHelpers.normalizeDID(did)
  }

  /// Only inbound text payloads should contribute to unread badges.
  private func shouldPersistAsUnread(
    payload: MLSMessagePayload,
    senderID: String,
    currentUserDID: String,
    processingError: String? = nil
  ) -> Bool {
    guard payload.messageType == .text else { return false }
    guard processingError == nil else { return false }
    return normalizeDID(senderID) != normalizeDID(currentUserDID)
  }

  // MARK: - Database Ownership Validation

  /// Verify that a database operation is being performed on the correct user's database
  /// This prevents cross-account access during account switching race conditions
  /// - Parameters:
  ///   - currentUserDID: The user DID that should own this database
  ///   - database: The database pool being accessed
  ///   - manager: The GRDB manager (optional, for validation)
  /// - Throws: MLSStorageError.accountMismatch if DIDs don't match
  private func verifyDatabaseOwnership(
    currentUserDID: String,
    database: MLSDatabase,
    manager: MLSGRDBManager? = nil
  ) async throws {
    // Note: Since MLSDatabase doesn't expose the owner DID, and this storage layer
    // doesn't hold a reference to the manager, we rely on the caller (MLSCoreContext)
    // to ensure they're passing the correct database for the current user.
    // The manager-level validation in getDatabasePool() is advisory logging only.
    // This method exists for future enhancement if needed.
  }

  // MARK: - Conversation Operations

  /// Ensure a conversation exists in database, creating it if necessary (idempotent)
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - groupID: MLS group ID (hex-encoded string)
  ///   - database: DatabaseWriter (DatabaseQueue or DatabasePool) to use for operations
  /// - Throws: MLSStorageError if creation fails
  @discardableResult
  public func ensureConversationExists(
    userDID: String,
    conversationID: String,
    groupID: String,
    database: MLSDatabase
  ) async throws -> String {
    let normalizedUserDID = normalizeDID(userDID)

    // Check if conversation already exists (Standard Check)
    let exists = try await database.read { db in
      let count =
        try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == normalizedUserDID)
        .fetchCount(db)
      return count > 0
    }

    if exists {
      logger.debug("Conversation already exists: \(conversationID)")
      return conversationID
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // DUPLICATE HEALING (2024-12-21)
    // ═══════════════════════════════════════════════════════════════════════════
    // Check if a PLACEHOLDER exists with the same Group ID but different Conversation ID.
    // This happens when NSE creates a conversation via Hex ID (from push)
    // but the App syncs it via UUID (from backend).
    //
    // If found, we must MIGRATE the messages to the new ID and DELETE the placeholder.
    // ═══════════════════════════════════════════════════════════════════════════

    guard let groupIDData = Data(hexEncoded: groupID) else {
      throw MLSStorageError.invalidGroupID(groupID)
    }

    try await database.write { db in
      // 1. Search for duplicate placeholder by Group ID
      let placeholder =
        try MLSConversationModel
        .filter(MLSConversationModel.Columns.groupID == groupIDData)
        .filter(MLSConversationModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSConversationModel.Columns.conversationID != conversationID)  // Not the one we're creating
        .fetchOne(db)

      if let placeholder = placeholder {
        let oldID = placeholder.conversationID
        logger.warning(
          "🩹 [HEALING] Found duplicate placeholder: \(oldID) -> New: \(conversationID)")

        // 2. Migrate Messages
        try db.execute(
          sql: """
              UPDATE MLSMessageModel
              SET conversationID = ?
              WHERE conversationID = ? AND currentUserDID = ?
            """, arguments: [conversationID, oldID, normalizedUserDID])

        // 3. Migrate Epoch Keys
        try db.execute(
          sql: """
              UPDATE MLSEpochKeyModel
              SET conversationID = ?
              WHERE conversationID = ? AND currentUserDID = ?
            """, arguments: [conversationID, oldID, normalizedUserDID])

        // 4. Delete Placeholder
        try placeholder.delete(db)
        logger.info("✅ [HEALING] Migrated data and deleted placeholder \(oldID)")
      }

      // 5. Create new conversation
      let conversation = MLSConversationModel(
        conversationID: conversationID,
        currentUserDID: normalizedUserDID,
        groupID: groupIDData,
        createdAt: Date(),
        updatedAt: Date()
      )
      try conversation.insert(db)
    }

    logger.info("✅ Created conversation: \(conversationID)")
    return conversationID
  }

  /// Ensure a conversation exists, creating a placeholder if necessary
  ///
  /// **CRITICAL FOR NSE**: This method is designed for the Notification Service Extension.
  /// It performs a "Smart Lookup":
  /// 1. Checks if `conversationID` exists -> Returns it.
  /// 2. Checks if ANY conversation exists with `groupID` -> Returns that ID (prevents duplicates).
  /// 3. Creates placeholder with `conversationID` if neither exists -> Returns `conversationID`.
  ///
  /// - Parameters:
  ///   - userDID: User's DID
  ///   - conversationID: Requested conversation identifier (usually Hex ID from push)
  ///   - groupID: MLS group ID (hex-encoded string)
  ///   - senderDID: Optional sender DID (used as placeholder title if available)
  ///   - database: DatabaseWriter to use for operations
  /// - Returns: The **Effective Conversation ID** to use for storing messages.
  @discardableResult
  public func ensureConversationExistsOrPlaceholder(
    userDID: String,
    conversationID: String,
    groupID: String,
    senderDID: String? = nil,
    database: MLSDatabase
  ) async throws -> String {
    let normalizedUserDID = normalizeDID(userDID)
    
    guard let groupIDData = Data(hexEncoded: groupID) else {
      throw MLSStorageError.invalidGroupID(groupID)
    }

    // 1. Direct Check: Does the requested ID exist?
    let existingDirect = try await database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)
    }

    if let existing = existingDirect {
      logger.debug("Conversation exists (Direct Match): \(conversationID)")
      return existing.conversationID
    }
    
    // 2. Smart Lookup: Does ANY conversation exist for this Group ID?
    // This prevents creating a Hex-ID placeholder if the UUID-ID real conversation exists.
    let existingByGroup = try await database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.groupID == groupIDData)
        .filter(MLSConversationModel.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)
    }

    if let existing = existingByGroup {
      logger.info(
        "ℹ️ [NSE-Dedup] Found existing conversation \(existing.conversationID) for group \(groupID)")
      logger.info("   Redirecting message from requested ID \(conversationID) to existing ID")
      return existing.conversationID
    }

    // 3. Create placeholder conversation (if absolutely no record exists)
    // The main app will heal this with real metadata during listConvos sync
    try await database.write { db in
      // Use sender DID as a placeholder title, or "New Conversation" if unknown
      let placeholderTitle: String?
      if let senderDID = senderDID {
        if senderDID.hasPrefix("did:plc:") {
          placeholderTitle = "Chat with \(senderDID.suffix(8))..."
        } else {
          placeholderTitle = senderDID
        }
      } else {
        placeholderTitle = "New Conversation"
      }

      let placeholder = MLSConversationModel(
        conversationID: conversationID,
        currentUserDID: normalizedUserDID,
        groupID: groupIDData,
        epoch: 0,
        joinMethod: .unknown,
        joinEpoch: 0,
        title: placeholderTitle,
        avatarURL: nil,
        createdAt: Date(),
        updatedAt: Date(),
        lastMessageAt: Date(),
        lastMembershipChangeAt: nil,
        unacknowledgedMemberChanges: 0,
        isActive: true,
        needsRejoin: false,
        rejoinRequestedAt: nil,
        lastRecoveryAttempt: nil,
        consecutiveFailures: 0,
        isPlaceholder: true
      )
      try placeholder.insert(db)
    }

    logger.warning("🆕 [NSE] Created PLACEHOLDER conversation: \(conversationID)")
    return conversationID
  }

  /// Fetch a persisted conversation for the current user if it exists
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current authenticated user's DID
  ///   - database: Database queue to read from
  /// - Returns: Stored `MLSConversationModel` or `nil` when missing
  public func fetchConversation(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> MLSConversationModel? {
    try await database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)
    }
  }

  // MARK: - Message Payload Caching

  /// Save payload for a message after decryption
  ///
  /// **CRITICAL**: MLS ratchet burns secrets after first decryption - must cache immediately!
  ///
  /// **SECURITY MODEL**:
  /// - Payload stored in SQLCipher database with AES-256-CBC encryption
  /// - Per-user encryption keys stored in iOS Keychain
  /// - Database excluded from iCloud/iTunes backup
  /// - iOS Data Protection (FileProtectionType.complete) for at-rest security
  ///
  /// - Parameters:
  ///   - messageID: Unique message identifier
  ///   - conversationID: Conversation this message belongs to
  ///   - payload: Decrypted message payload (contains text, embed, reaction, etc.)
  ///   - senderID: DID of message sender
  ///   - currentUserDID: DID of current user
  ///   - epoch: MLS epoch number
  ///   - sequenceNumber: Stable conversation sequence number
  ///   - timestamp: Message timestamp
  ///   - database: MLSDatabase to use for operations
  ///   - processingError: Optional error from processing
  ///   - validationFailureReason: Optional validation failure reason
  /// - Throws: MLSStorageError if save fails
  @discardableResult
  public func savePayloadForMessage(
    messageID: String,
    conversationID: String,
    payload: MLSMessagePayload,
    senderID: String,
    currentUserDID: String,
    epoch: Int64,
    sequenceNumber: Int64,
    timestamp: Date,
    database: MLSDatabase,
    processingError: String? = nil,
    validationFailureReason: String? = nil,
    processingState: String = MLSMessageProcessingState.cached,
    advanceSequenceState: Bool = false
  ) async throws -> [AdoptedReaction] {
    logger.info(
      "💾 Caching payload: \(messageID) (epoch: \(epoch), seq: \(sequenceNumber), type: \(String(describing: payload.messageType)), hasEmbed: \(payload.embed != nil), hasError: \(processingError != nil))"
    )

    // Encode full payload
    let payloadData = try payload.encodeToJSON()
    logger.debug("Encoded payload (\(payloadData.count) bytes)")

    let normalizedUserDID = normalizeDID(currentUserDID)
    let shouldBeUnread = shouldPersistAsUnread(
      payload: payload,
      senderID: senderID,
      currentUserDID: normalizedUserDID,
      processingError: processingError
    )
    let adopted = try await database.write { db -> [AdoptedReaction] in
      // Check if message exists and fetch its current state
      var existingMessage =
        try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)

      if existingMessage == nil, normalizedUserDID != currentUserDID {
        existingMessage =
          try MLSMessageModel
          .filter(MLSMessageModel.Columns.messageID == messageID)
          .filter(MLSMessageModel.Columns.currentUserDID == currentUserDID)
          .fetchOne(db)
        if existingMessage != nil {
          logger.warning(
            "⚠️ [DID-NORMALIZE] Found legacy row with non-normalized DID for \(messageID.prefix(16)); normalizing on write")
        }
      }

      let exists = existingMessage != nil

      if exists {
        // ═══════════════════════════════════════════════════════════════════════════
        // CRITICAL FIX: Don't overwrite valid data with errors (NSE/Foreground Race)
        // ═══════════════════════════════════════════════════════════════════════════
        // Scenario: NSE decrypts message successfully, saves to DB.
        // Foreground app tries to decrypt same message, gets SecretReuseError.
        // Foreground app calls savePayloadForMessage with processingError="Decryption Failed".
        //
        // OLD BEHAVIOR: Overwrites NSE's valid decrypted payload with error placeholder.
        // NEW BEHAVIOR: If existing message has NO error AND new save HAS error, SKIP UPDATE.
        // ═══════════════════════════════════════════════════════════════════════════
        let existingHasValidPayload =
          (existingMessage?.processingError == nil)
          && (existingMessage?.payloadJSON != nil)
          && !(existingMessage?.payloadJSON?.isEmpty ?? true)
        let newHasError = (processingError != nil)

        if existingHasValidPayload && newHasError {
          logger.warning(
            "⚠️ [SAVE-SKIP] Message \(messageID) already has valid payload - NOT overwriting with error state"
          )
          logger.warning(
            "   Existing payload size: \(existingMessage?.payloadJSON?.count ?? 0) bytes, error: \(existingMessage?.processingError ?? "none")"
          )
          logger.warning("   Skipped error: \(processingError ?? "unknown")")
          // Still need to adopt orphans even if we skip the update
          // Fall through to orphan adoption logic
        } else {
          // Update existing message (normal path)
          try db.execute(
            sql: """
              UPDATE MLSMessageModel
              SET payloadJSON = ?,
                  senderID = ?,
                  epoch = ?,
                  sequenceNumber = ?,
                  timestamp = ?,
                  isRead = CASE WHEN ? = 1 THEN 1 ELSE isRead END,
                  payloadExpired = 0,
                  processingState = ?,
                  processingError = ?,
                  processingAttempts = processingAttempts + 1,
                  validationFailureReason = ?
              WHERE messageID = ? AND currentUserDID IN (?, ?);
            """,
            arguments: [
              payloadData,
              senderID,
              epoch,
              sequenceNumber,
              timestamp,
              shouldBeUnread ? 0 : 1,
              processingState,
              processingError,
              validationFailureReason,
              messageID,
              normalizedUserDID,
              currentUserDID,
            ])

          logger.debug("Updated existing message with payload cache")
        }
      } else {
        // Create new message
        let message = MLSMessageModel(
          messageID: messageID,
          currentUserDID: normalizedUserDID,
          conversationID: conversationID,
          senderID: senderID,
          payloadJSON: payloadData,
          wireFormat: Data(),
          contentType: "application/json",
          timestamp: timestamp,
          epoch: epoch,
          sequenceNumber: sequenceNumber,
          authenticatedData: nil,
          signature: nil,
          isDelivered: true,
          isRead: !shouldBeUnread,
          isSent: true,
          sendAttempts: 0,
          error: nil,
          processingState: processingState,
          gapBefore: false,
          payloadExpired: false,
          processingError: processingError,
          processingAttempts: processingError != nil ? 1 : 0,
          validationFailureReason: validationFailureReason
        )
        try message.insert(db)

        logger.debug("Created new message with payload cache")
      }

      _ = try MLSStorageHelpers.applyReadFrontierToMessagesSync(
        in: db,
        conversationID: conversationID,
        currentUserDID: normalizedUserDID
      )

      // Adopt any orphaned reactions for this message (within same transaction for atomicity)
      let orphans =
        try MLSOrphanedReactionModel
        .filter(MLSOrphanedReactionModel.Columns.messageID == messageID)
        .filter(MLSOrphanedReactionModel.Columns.currentUserDID == normalizedUserDID)
        .fetchAll(db)

      guard !orphans.isEmpty else { return [] }

      var adoptedOrphans: [MLSOrphanedReactionModel] = []
      for orphan in orphans {
        let regular = orphan.toRegularModel()
        do {
          try regular.save(db, onConflict: .ignore)
          try orphan.delete(db)
          adoptedOrphans.append(orphan)
        } catch let error as DatabaseError {
          // Handle unexpected FK error (message deleted during transaction - shouldn't happen but be safe)
          let isForeignKeyError =
            error.extendedResultCode.rawValue == 787
            || (error.resultCode == .SQLITE_CONSTRAINT
              && error.message?.uppercased().contains("FOREIGN KEY") == true)

          if isForeignKeyError {
            logger.warning(
              "[ORPHAN] Unexpected FK error adopting \(orphan.emoji) - keeping as orphan")
          } else {
            throw error
          }
        }
      }

      if advanceSequenceState {
        if let existingState =
          try MLSConversationSequenceState
          .filter(MLSConversationSequenceState.Columns.conversationID == conversationID)
          .filter(MLSConversationSequenceState.Columns.currentUserDID == normalizedUserDID)
          .fetchOne(db)
        {
          if sequenceNumber > existingState.lastProcessedSeq {
            var updated = existingState
            updated.lastProcessedSeq = sequenceNumber
            updated.updatedAt = Date()
            try updated.update(db)
          }
        } else {
          let state = MLSConversationSequenceState(
            conversationID: conversationID,
            currentUserDID: normalizedUserDID,
            lastProcessedSeq: sequenceNumber,
            updatedAt: Date()
          )
          try state.insert(db)
        }
      }

      return adoptedOrphans.map { orphan in
        AdoptedReaction(
          messageID: orphan.messageID,
          conversationID: orphan.conversationID,
          actorDID: orphan.actorDID,
          emoji: orphan.emoji,
          action: orphan.action
        )
      }
    }

    logger.info("✅ Payload cached: \(messageID)")

    if !adopted.isEmpty {
      logger.info("[ORPHAN-ADOPT] Adopted \(adopted.count) reaction(s) for message \(messageID)")
    }

    // CRITICAL FIX: Notify UI of new message immediately for realtime updates
    // This decouples the storage layer from the UI while ensuring updates flow
    NotificationCenter.default.post(
      name: Notification.Name("MLSMessageSaved"),
      object: nil,
      userInfo: [
        "messageID": messageID,
        "conversationID": conversationID,
      ]
    )

    return adopted
  }

  /// Update message metadata (epoch, sequence number, timestamp) for an existing message
  /// - Parameters:
  ///   - messageID: Unique message identifier
  ///   - currentUserDID: DID of current user
  ///   - epoch: Actual MLS epoch number from server
  ///   - sequenceNumber: Actual MLS sequence number from server
  ///   - timestamp: Actual message timestamp from server
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if update fails
  public func updateMessageMetadata(
    messageID: String,
    currentUserDID: String,
    epoch: Int64,
    sequenceNumber: Int64,
    timestamp: Date,
    database: MLSDatabase,
    newMessageID: String? = nil
  ) async throws {
    logger.info(
      "🆙 Updating metadata for \(messageID.prefix(16)): epoch=\(epoch), seq=\(sequenceNumber), newID=\(newMessageID ?? "nil")"
    )

    try await database.write { db in
      if let newID = newMessageID, newID != messageID {
        // ID change required - use Insert-Move-Delete to preserve FKs and Payload
        // We cannot use PRAGMA foreign_keys = OFF inside a transaction consistently, so we strictly move data.

        // 1. Create temp table snapshot of old record
        try db.execute(
          sql:
            "CREATE TEMPORARY TABLE temp_msg_update AS SELECT * FROM MLSMessageModel WHERE messageID = ?",
          arguments: [messageID])

        // 2. Update metadata in temp table (changing the ID to the new Server ID)
        try db.execute(
          sql:
            "UPDATE temp_msg_update SET messageID = ?, epoch = ?, sequenceNumber = ?, timestamp = ?, processingState = ? WHERE messageID = ?",
          arguments: [
            newID,
            epoch,
            sequenceNumber,
            timestamp,
            MLSMessageProcessingState.cached,
            messageID,
          ]
        )

        // 3. Insert new record (replacing if exists to ensure we keep the local payload if checking against placeholder)
        // Note: OR REPLACE might cascade delete reactions on the existing record if it existed, but we prioritize the local payload.
        try db.execute(sql: "INSERT OR REPLACE INTO MLSMessageModel SELECT * FROM temp_msg_update")

        // 4. Move reactions from old ID to new ID
        // Note: This must happen BEFORE deleting the old record (which would cascade delete them)
        try db.execute(
          sql: "UPDATE MLSMessageReactionModel SET messageID = ? WHERE messageID = ?",
          arguments: [newID, messageID])

        // 5. Delete old record (now safe as reactions are moved)
        try db.execute(
          sql: "DELETE FROM MLSMessageModel WHERE messageID = ?", arguments: [messageID])

        // 6. Cleanup
        try db.execute(sql: "DROP TABLE temp_msg_update")
      } else {
        // Standard metadata update
        try db.execute(
          sql: """
                UPDATE MLSMessageModel
                SET epoch = ?,
                    sequenceNumber = ?,
                    timestamp = ?,
                    processingState = ?
              WHERE messageID = ? AND currentUserDID IN (?, ?);
            """,
          arguments: [
            epoch,
            sequenceNumber,
            timestamp,
            MLSMessageProcessingState.cached,
            messageID,
            normalizeDID(currentUserDID),
            currentUserDID,
          ]
        )
      }
    }

    // Attempt to adopt any orphans waiting for the new ID
    if let newID = newMessageID, newID != messageID {
      try? await self.adoptOrphansForMessage(
        newID,
        currentUserDID: currentUserDID,
        database: database
      )
    }
  }

  /// Fetch cached payload for a message
  ///
  /// Returns cached payload if available, or nil if message hasn't been decrypted yet.
  /// This prevents re-decryption attempts that would fail with SecretReuseError.
  ///
  /// - Parameters:
  ///   - messageID: The message ID to fetch
  ///   - currentUserDID: The DID of the current user
  ///   - database: MLSDatabase to use for operations
  /// - Returns: Cached payload if available, nil otherwise
  /// - Throws: MLSStorageError if fetch fails
  public func fetchPayloadForMessage(
    _ messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> MLSMessagePayload? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    logger.debug("Fetching payload: \(messageID)")

    let payload = try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)?.parsedPayload
    }

    if payload != nil {
      logger.debug("✅ Found cached payload: \(messageID)")
    } else {
      logger.warning("⚠️ No cached payload found: \(messageID)")
    }

    return payload
  }

  /// Fetch cached plaintext for a message (convenience wrapper)
  ///
  /// Returns cached plaintext if available, or nil if message hasn't been decrypted yet.
  ///
  /// - Parameters:
  ///   - messageID: The message ID to fetch
  ///   - currentUserDID: The DID of the current user
  ///   - database: MLSDatabase to use for operations
  /// - Returns: Cached plaintext if available, nil otherwise
  /// - Throws: MLSStorageError if fetch fails
  public func fetchPlaintextForMessage(
    _ messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> String? {
    try await fetchPayloadForMessage(messageID, currentUserDID: currentUserDID, database: database)?
      .text
  }

  /// Fetch cached embed data for a message (convenience wrapper)
  ///
  /// Returns cached embed if available, or nil if no embed was cached.
  ///
  /// - Parameters:
  ///   - messageID: The message ID to fetch
  ///   - currentUserDID: The DID of the current user
  ///   - database: MLSDatabase to use for operations
  /// - Returns: Cached embed data if available, nil otherwise
  /// - Throws: MLSStorageError if fetch fails
  public func fetchEmbedForMessage(
    _ messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> MLSEmbedData? {
    try await fetchPayloadForMessage(messageID, currentUserDID: currentUserDID, database: database)?
      .embed
  }

  /// Fetch cached sender DID for a message
  ///
  /// Returns the sender's DID extracted from MLS credentials during decryption.
  ///
  /// - Parameters:
  ///   - messageID: The message ID to fetch
  ///   - currentUserDID: The DID of the current user
  ///   - database: MLSDatabase to use for operations
  /// - Returns: Sender DID if available, nil otherwise
  /// - Throws: MLSStorageError if fetch fails
  public func fetchSenderForMessage(
    _ messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> String? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    logger.debug("Fetching sender: \(messageID)")

    let senderID = try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)?.senderID
    }

    if let senderID = senderID {
      logger.debug("✅ Found cached sender: \(messageID) -> \(senderID)")
      return senderID
    } else {
      logger.warning("⚠️ No cached sender found: \(messageID)")
      return nil
    }
  }

  /// Fetch the oldest unconfirmed self-sent message for a conversation.
  ///
  /// Pre-cached self-sent messages are stored under their local temp ID with
  /// `processingState == pendingSelfSend` until the HTTP send response promotes
  /// them to the server-assigned ID. The optimistic sequence number is only for
  /// UI ordering and must not be used as the pending marker.
  ///
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - senderDID: The DID of the sender (current user)
  ///   - currentUserDID: The DID of the current user (for DB partitioning)
  ///   - database: MLSDatabase to use for operations
  /// - Returns: The oldest unconfirmed self-sent message, or nil
  /// - Throws: MLSStorageError if fetch fails
  public func fetchOldestUnconfirmedSelfSentMessage(
    conversationID: String,
    senderDID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> MLSMessageModel? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.senderID == senderDID)
        .filter(MLSMessageModel.Columns.processingState == MLSMessageProcessingState.pendingSelfSend)
        .filter(MLSMessageModel.Columns.processingError == nil)
        .filter(MLSMessageModel.Columns.payloadJSON != nil)
        .order(MLSMessageModel.Columns.timestamp.asc)
        .fetchOne(db)
    }
  }

  /// Fetch the most recent cached messages for a conversation.
  ///
  /// Returns up to `limit` messages sorted for display (oldest → newest) while prioritizing
  /// the newest decrypted records when trimming large conversations.
  /// Useful for cache-first display before fetching from server.
  ///
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: The DID of the current user
  ///   - database: MLSDatabase to use for operations
  ///   - limit: Maximum number of messages to return (default: 50)
  /// - Returns: Array of cached messages sorted from oldest to newest
  /// - Throws: MLSStorageError if fetch fails
  public func fetchMessagesForConversation(
    _ conversationID: String,
    currentUserDID: String,
    database: MLSDatabase,
    limit: Int = 50
  ) async throws -> [MLSMessageModel] {
    let normalizedUserDID = normalizeDID(currentUserDID)
    logger.info(
      "🔍 [DB] Fetching cached messages - conversationID: \(conversationID), currentUserDID: \(normalizedUserDID), limit: \(limit)"
    )

    let messages = try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .order(
          MLSMessageModel.Columns.sequenceNumber.desc,
          MLSMessageModel.Columns.timestamp.desc,
          MLSMessageModel.Columns.messageID.desc
        )
        .limit(limit)
        .fetchAll(db)
    }

    logger.info("📦 [DB] Query returned \(messages.count) messages")
    if !messages.isEmpty {
      for msg in messages {
        logger.debug(
          "  - Message \(msg.messageID): epoch=\(msg.epoch), seq=\(msg.sequenceNumber), hasPlaintext=\(msg.plaintext != nil), sender=\(msg.senderID)"
        )
      }
    }

    let orderedMessages = Array(messages.reversed())
    logger.info("✅ [DB] Returning \(orderedMessages.count) cached messages (oldest→newest)")
    return orderedMessages
  }

  /// Fetch messages older than a specific conversation sequence for pagination.
  /// `beforeEpoch` is retained for API compatibility, but display ordering is sequence-first
  /// because MLS epoch resets when recovery rotates the underlying group.
  public func fetchMessagesBeforeSequence(
    conversationId: String,
    currentUserDID: String,
    beforeEpoch: Int64,
    beforeSeq: Int64,
    database: MLSDatabase,
    limit: Int = 50
  ) async throws -> [MLSMessageModel] {
    let normalizedUserDID = normalizeDID(currentUserDID)
    logger.info(
      "🔍 [DB] Fetching older messages - conversationID: \(conversationId), currentUserDID: \(normalizedUserDID), beforeEpoch: \(beforeEpoch), beforeSeq: \(beforeSeq), limit: \(limit)"
    )

    let messages = try await database.read { db in
      var request = MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationId)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)

      if beforeSeq > 0 && beforeSeq < Int64.max {
        request = request.filter(MLSMessageModel.Columns.sequenceNumber < beforeSeq)
      }

      return try request
        .order(
          MLSMessageModel.Columns.sequenceNumber.desc,
          MLSMessageModel.Columns.timestamp.desc,
          MLSMessageModel.Columns.messageID.desc
        )
        .limit(limit)
        .fetchAll(db)
    }

    logger.info("📦 [DB] Query returned \(messages.count) older messages")
    if !messages.isEmpty {
      for msg in messages {
        logger.debug(
          "  - Older message \(msg.messageID): epoch=\(msg.epoch), seq=\(msg.sequenceNumber), hasPlaintext=\(msg.plaintext != nil)"
        )
      }
    }

    let orderedMessages = Array(messages.reversed())
    logger.info("✅ [DB] Returning \(orderedMessages.count) older messages (oldest→newest)")
    return orderedMessages
  }

  // MARK: - Epoch Key Management

  /// Store epoch secret with actual key material
  /// - Parameters:
  ///   - conversationID: Hex-encoded conversation/group ID
  ///   - epoch: MLS epoch number
  ///   - secretData: Epoch secret key material
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if save fails
  public func saveEpochSecret(
    userDID: String,
    conversationID: String,
    epoch: UInt64,
    secretData: Data,
    database: MLSDatabase
  ) async throws {
    let normalizedUserDID = normalizeDID(userDID)

    do {
      try await database.write { db in
        let epochKey = MLSEpochKeyModel(
          epochKeyID: "\(conversationID)-\(epoch)",
          conversationID: conversationID,
          currentUserDID: normalizedUserDID,
          epoch: Int64(epoch),
          keyMaterial: secretData,  // Actual epoch secret
          createdAt: Date(),
          expiresAt: nil,
          isActive: true
        )
        // ⭐ CRITICAL FIX: Use save() instead of insert() to handle duplicate epoch exports
        // This allows epoch 0 to be exported both at group creation AND before merge_pending_commit
        // without hitting UNIQUE constraint violations
        try epochKey.save(db)
      }

      logger.info(
        "✅ Saved epoch secret: \(conversationID) epoch \(epoch), \(secretData.count) bytes")
    } catch let error as DatabaseError {
      // ⭐ FIXED: Foreign key violations should NEVER occur now that we create
      // the SQLCipher conversation record BEFORE creating the MLS group
      if error.resultCode == .SQLITE_CONSTRAINT && error.message?.contains("FOREIGN KEY") == true {
        logger.error(
          "❌ [EPOCH-STORAGE] CRITICAL: Foreign key violation storing epoch secret - conversation \(conversationID.prefix(16))... not found. This indicates a bug in conversation creation order!"
        )
        throw MLSStorageError.foreignKeyViolation(
          "Conversation \(conversationID) must exist before storing epoch secrets")
      }
      // Re-throw all database errors
      throw error
    }
  }

  /// Retrieve epoch secret key material
  /// - Parameters:
  ///   - conversationID: Hex-encoded conversation/group ID
  ///   - epoch: MLS epoch number
  ///   - database: MLSDatabase to use for operations
  /// - Returns: Epoch secret data if found, nil otherwise
  public func getEpochSecret(
    userDID: String,
    conversationID: String,
    epoch: UInt64,
    database: MLSDatabase
  ) async throws -> Data? {
    let normalizedUserDID = normalizeDID(userDID)

    let secret = try await database.read { db in
      try MLSEpochKeyModel
        .filter(MLSEpochKeyModel.Columns.conversationID == conversationID)
        .filter(MLSEpochKeyModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSEpochKeyModel.Columns.epoch == Int64(epoch))
        .filter(MLSEpochKeyModel.Columns.isActive == true)
        .fetchOne(db)?
        .keyMaterial
    }

    if let secret = secret {
      logger.debug(
        "Retrieved epoch secret: \(conversationID) epoch \(epoch), \(secret.count) bytes")
    } else {
      logger.debug("No epoch secret found: \(conversationID) epoch \(epoch)")
    }

    return secret
  }

  /// Delete epoch secret
  /// - Parameters:
  ///   - conversationID: Hex-encoded conversation/group ID
  ///   - epoch: MLS epoch number
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public func deleteEpochSecret(
    userDID: String,
    conversationID: String,
    epoch: UInt64,
    database: MLSDatabase
  ) async throws {
    let normalizedUserDID = normalizeDID(userDID)

    try await database.write { db in
      let now = Date()
      try db.execute(
        sql: """
            UPDATE MLSEpochKeyModel
            SET deletedAt = ?, isActive = ?
            WHERE conversationID = ? AND currentUserDID = ? AND epoch = ?;
          """, arguments: [now, false, conversationID, normalizedUserDID, Int64(epoch)])
    }

    logger.info("Deleted epoch secret: \(conversationID) epoch \(epoch)")
  }

  // MARK: - Sync versions for EpochSecretStorageBridge (used via read/write closures)

  /// Synchronous version of saveEpochSecret for use within db closures
  public func saveEpochSecretSync(
    userDID: String,
    conversationID: String,
    epoch: UInt64,
    secretData: Data,
    db: Database
  ) throws {
    let normalizedUserDID = normalizeDID(userDID)

    let epochKey = MLSEpochKeyModel(
      epochKeyID: "\(conversationID)-\(epoch)",
      conversationID: conversationID,
      currentUserDID: normalizedUserDID,
      epoch: Int64(epoch),
      keyMaterial: secretData,
      createdAt: Date(),
      expiresAt: nil,
      isActive: true
    )
    try epochKey.save(db)
  }

  /// Synchronous version of getEpochSecret for use within db closures
  public func getEpochSecretSync(
    userDID: String,
    conversationID: String,
    epoch: UInt64,
    db: Database
  ) throws -> Data? {
    let normalizedUserDID = normalizeDID(userDID)

    return try MLSEpochKeyModel
      .filter(MLSEpochKeyModel.Columns.conversationID == conversationID)
      .filter(MLSEpochKeyModel.Columns.currentUserDID == normalizedUserDID)
      .filter(MLSEpochKeyModel.Columns.epoch == Int64(epoch))
      .filter(MLSEpochKeyModel.Columns.isActive == true)
      .fetchOne(db)?
      .keyMaterial
  }

  /// Synchronous version of deleteEpochSecret for use within db closures
  public func deleteEpochSecretSync(
    userDID: String,
    conversationID: String,
    epoch: UInt64,
    db: Database
  ) throws {
    let normalizedUserDID = normalizeDID(userDID)

    let now = Date()
    try db.execute(
      sql: """
          UPDATE MLSEpochKeyModel
          SET deletedAt = ?, isActive = ?
          WHERE conversationID = ? AND currentUserDID = ? AND epoch = ?;
        """, arguments: [now, false, conversationID, normalizedUserDID, Int64(epoch)])
  }

  /// Synchronous version of deleteEpochsBefore for use within db closures
  public func deleteEpochsBeforeSync(
    userDID: String,
    conversationID: String,
    cutoffEpoch: UInt64,
    db: Database
  ) throws -> Int {
    let normalizedUserDID = normalizeDID(userDID)
    let now = Date()

    try db.execute(
      sql: """
          UPDATE MLSEpochKeyModel
          SET deletedAt = ?, isActive = ?
          WHERE conversationID = ? AND currentUserDID = ? AND epoch < ? AND isActive = 1;
        """, arguments: [now, false, conversationID, normalizedUserDID, Int64(cutoffEpoch)])

    return db.changesCount
  }

  /// Synchronous version of ensureConversationExists for use within db closures
  public func ensureConversationExistsSync(
    userDID: String,
    conversationID: String,
    groupID: String,
    db: Database
  ) throws {
    let normalizedUserDID = normalizeDID(userDID)

    // Check if conversation already exists
    let existing =
      try MLSConversationModel
      .filter(MLSConversationModel.Columns.conversationID == conversationID)
      .filter(MLSConversationModel.Columns.currentUserDID == normalizedUserDID)
      .fetchOne(db)

    if existing == nil {
      // Convert groupID string to Data
      let groupIDData = Data(groupID.utf8)

      // Create minimal placeholder conversation
      let conversation = MLSConversationModel(
        conversationID: conversationID,
        currentUserDID: normalizedUserDID,
        groupID: groupIDData,
        epoch: 0,
        joinMethod: .unknown,
        joinEpoch: 0,
        title: nil,
        avatarURL: nil,
        createdAt: Date(),
        updatedAt: Date(),
        lastMessageAt: nil,
        lastMembershipChangeAt: nil,
        unacknowledgedMemberChanges: 0,
        isActive: true,
        needsRejoin: false,
        rejoinRequestedAt: nil,
        lastRecoveryAttempt: nil,
        consecutiveFailures: 0,
        isPlaceholder: true
      )
      try conversation.insert(db)
      logger.debug(
        "[EPOCH-STORAGE] Created placeholder conversation: \(conversationID.prefix(16))...")
    }
  }

  /// Record an epoch key for forward secrecy tracking (deprecated - use saveEpochSecret)
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - epoch: MLS epoch number
  ///   - userDID: User's decentralized identifier
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if save fails
  @available(*, deprecated, message: "Use saveEpochSecret instead")
  public func recordEpochKey(
    conversationID: String,
    epoch: Int64,
    userDID: String,
    database: MLSDatabase
  ) async throws {
    let normalizedUserDID = normalizeDID(userDID)

    try await database.write { db in
      let epochKeyID = "\(conversationID)-\(epoch)"
      let existingKey =
        try MLSEpochKeyModel
        .filter(MLSEpochKeyModel.Columns.epochKeyID == epochKeyID)
        .fetchOne(db)

      guard existingKey == nil else {
        logger.debug("Epoch key already recorded: \(conversationID) epoch \(epoch)")
        return
      }

      let epochKey = MLSEpochKeyModel(
        epochKeyID: epochKeyID,
        conversationID: conversationID,
        currentUserDID: normalizedUserDID,
        epoch: epoch,
        keyMaterial: Data(),  // Placeholder - actual key material should be provided
        createdAt: Date(),
        expiresAt: nil,
        isActive: true
      )
      try epochKey.insert(db)
    }

    logger.info("Recorded epoch key: \(conversationID) epoch \(epoch)")
  }

  /// Delete old epoch keys, keeping only the most recent ones
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - userDID: User's decentralized identifier
  ///   - keepLast: Number of recent keys to keep
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public func deleteOldEpochKeys(
    conversationID: String,
    userDID: String,
    keepLast: Int,
    database: MLSDatabase
  ) async throws {

    try await database.write { db in
      // Get all epoch keys for this conversation
      let allKeys =
        try MLSEpochKeyModel
        .filter(MLSEpochKeyModel.Columns.conversationID == conversationID)
        .filter(MLSEpochKeyModel.Columns.currentUserDID == userDID)
        .filter(MLSEpochKeyModel.Columns.isActive == true)
        .order(MLSEpochKeyModel.Columns.epoch.desc)
        .fetchAll(db)

      guard allKeys.count > keepLast else {
        logger.debug("No old epoch keys to delete")
        return
      }

      // Mark old keys for deletion
      let keysToDelete = allKeys.dropFirst(keepLast)
      let now = Date()

      for key in keysToDelete {
        try db.execute(
          sql: """
              UPDATE MLSEpochKeyModel
              SET deletedAt = ?
              WHERE conversationID = ? AND currentUserDID = ? AND epoch = ?;
            """, arguments: [now, conversationID, userDID, key.epoch])
      }

      logger.info("Marked \(keysToDelete.count) epoch keys for deletion")
    }
  }

  /// Clean up old message keys older than specified date
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - date: Delete messages older than this date
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public func cleanupMessageKeys(
    userDID: String,
    olderThan date: Date,
    database: MLSDatabase
  ) async throws {

    let deletedCount = try await database.write { db -> Int in
      try db.execute(
        sql: """
            DELETE FROM MLSMessageModel
            WHERE currentUserDID = ? AND timestamp < ?;
          """, arguments: [userDID, date])

      return db.changesCount
    }

    logger.info("Cleaned up \(deletedCount) message keys older than \(date)")
  }

  /// Delete epoch keys that have been marked for deletion
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public func deleteMarkedEpochKeys(
    userDID: String,
    database: MLSDatabase
  ) async throws {

    let deletedCount = try await database.write { db -> Int in
      try db.execute(
        sql: """
            DELETE FROM MLSEpochKeyModel
            WHERE currentUserDID = ? AND deletedAt IS NOT NULL;
          """, arguments: [userDID])

      return db.changesCount
    }

    logger.info("Deleted \(deletedCount) marked epoch keys")
  }

  /// Delete expired key packages
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public func deleteExpiredKeyPackages(
    userDID: String,
    database: MLSDatabase
  ) async throws {

    let now = Date()
    let deletedCount = try await database.write { db -> Int in
      try db.execute(
        sql: """
            DELETE FROM MLSKeyPackageModel
            WHERE currentUserDID = ? AND expiresAt IS NOT NULL AND expiresAt < ?;
          """, arguments: [userDID, now])

      return db.changesCount
    }

    logger.info("Deleted \(deletedCount) expired key packages")
  }

  // MARK: - Member Queries

  /// Get count of active members in a conversation
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user DID
  ///   - database: MLSDatabase to use for operations
  /// - Returns: Number of active members
  /// - Throws: MLSStorageError if query fails
  public func getMemberCount(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> Int {
    return try await database.read { db in
      try MLSMemberModel
        .filter(MLSMemberModel.Columns.conversationID == conversationID)
        .filter(MLSMemberModel.Columns.currentUserDID == currentUserDID)
        .filter(MLSMemberModel.Columns.isActive == true)
        .fetchCount(db)
    }
  }

  /// Fetch active members for a conversation
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user DID
  ///   - database: MLSDatabase to use for operations
  /// - Returns: Array of active members
  /// - Throws: MLSStorageError if query fails
  public func fetchMembers(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [MLSMemberModel] {
    return try await database.read { db in
      try MLSMemberModel
        .filter(MLSMemberModel.Columns.conversationID == conversationID)
        .filter(MLSMemberModel.Columns.currentUserDID == currentUserDID)
        .filter(MLSMemberModel.Columns.isActive == true)
        .order(MLSMemberModel.Columns.addedAt)
        .fetchAll(db)
    }
  }

  /// Upsert members for a conversation (replaces existing members)
  /// - Parameters:
  ///   - members: Array of member models to persist
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user DID
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if operation fails
  public func upsertMembers(
    _ members: [MLSMemberModel],
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      // Mark all existing members as inactive first
      try db.execute(
        sql: """
          UPDATE MLSMemberModel
          SET isActive = 0, removedAt = ?, updatedAt = ?
          WHERE conversationID = ? AND currentUserDID = ? AND isActive = 1
          """,
        arguments: [Date(), Date(), conversationID, currentUserDID]
      )

      // Insert or update the provided members
      for member in members {
        try member.save(db)
      }
    }

    logger.info("✅ Upserted \(members.count) members for conversation: \(conversationID)")
  }

  // MARK: - Batch Queries (Optimized for UI)

  /// Fetch all active conversations with their members in a single batch query.
  /// Eliminates N+1 queries when loading conversation list with participants.
  ///
  /// - Parameters:
  ///   - currentUserDID: Current user's DID
  ///   - database: Database writer
  /// - Returns: Tuple of conversations and members grouped by conversation ID
  public func fetchConversationsWithMembers(
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> (
    conversations: [MLSConversationModel], membersByConvoID: [String: [MLSMemberModel]]
  ) {
    return try await database.read { db in
      // Fetch all active conversations
      let conversations =
        try MLSConversationModel
        .filter(MLSConversationModel.Columns.isActive == true)
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .order(MLSConversationModel.Columns.lastMessageAt.desc)
        .fetchAll(db)

      guard !conversations.isEmpty else {
        return ([], [:])
      }

      // Batch fetch all members for these conversations in ONE query
      let conversationIDs = conversations.map { $0.conversationID }
      let allMembers =
        try MLSMemberModel
        .filter(conversationIDs.contains(MLSMemberModel.Columns.conversationID))
        .filter(MLSMemberModel.Columns.currentUserDID == currentUserDID)
        .filter(MLSMemberModel.Columns.isActive == true)
        .order(MLSMemberModel.Columns.addedAt)
        .fetchAll(db)

      // Group members by conversation
      let membersByConvoID = Dictionary(grouping: allMembers) { $0.conversationID }

      return (conversations, membersByConvoID)
    }
  }
  
  /// Fetch all conversations with their members using smart routing.
  /// Uses DatabasePool for active users, DatabaseQueue for inactive users.
  ///
  /// - Parameters:
  ///   - currentUserDID: Current user's DID
  /// - Returns: Tuple of conversations and members grouped by conversation ID
  public func fetchConversationsWithMembersUsingSmartRouting(
    currentUserDID: String
  ) async throws -> (
    conversations: [MLSConversationModel], membersByConvoID: [String: [MLSMemberModel]]
  ) {
    return try await MLSGRDBManager.shared.read(for: currentUserDID) { db in
      // Fetch all active conversations
      let conversations =
        try MLSConversationModel
        .filter(MLSConversationModel.Columns.isActive == true)
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .order(MLSConversationModel.Columns.lastMessageAt.desc)
        .fetchAll(db)

      guard !conversations.isEmpty else {
        return ([], [:])
      }

      // Batch fetch all members for these conversations in ONE query
      let conversationIDs = conversations.map { $0.conversationID }
      let allMembers =
        try MLSMemberModel
        .filter(conversationIDs.contains(MLSMemberModel.Columns.conversationID))
        .filter(MLSMemberModel.Columns.currentUserDID == currentUserDID)
        .filter(MLSMemberModel.Columns.isActive == true)
        .order(MLSMemberModel.Columns.addedAt)
        .fetchAll(db)

      // Group members by conversation
      let membersByConvoID = Dictionary(grouping: allMembers) { $0.conversationID }

      return (conversations, membersByConvoID)
    }
  }

  /// Fetch the last message cursor for a conversation.
  /// Used for pagination when fetching newer messages from server.
  ///
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - currentUserDID: Current user's DID
  ///   - database: Database writer
  /// - Returns: Tuple of (epoch, sequenceNumber, messageID) or nil if no messages cached.
  ///   `sequenceNumber` is the stable conversation timeline cursor; `epoch` is retained for crypto diagnostics.
  public func fetchLastMessageCursor(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> (epoch: Int64, seq: Int64, messageID: String)? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .order(
          MLSMessageModel.Columns.sequenceNumber.desc,
          MLSMessageModel.Columns.timestamp.desc,
          MLSMessageModel.Columns.messageID.desc
        )
        .limit(1)
        .fetchOne(db)
        .map { ($0.epoch, $0.sequenceNumber, $0.messageID) }
    }
  }

  /// Fetch the last successfully decrypted/displayable message cursor for a conversation.
  ///
  /// This excludes payloads that failed processing, expired payloads, and non-displayable
  /// control payloads so read receipts only advance on messages the user can actually read.
  public func fetchLastDecryptedMessageCursor(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> (epoch: Int64, seq: Int64, messageID: String)? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      guard let row = try Row.fetchOne(
        db,
        sql: """
          SELECT epoch, sequenceNumber, messageID
          FROM MLSMessageModel
          WHERE conversationID = ?
            AND currentUserDID = ?
            AND processingError IS NULL
            AND payloadExpired = 0
            AND json_valid(CAST(payloadJSON AS TEXT)) = 1
            AND COALESCE(
              json_extract(CAST(payloadJSON AS TEXT), '$.messageType'),
              json_extract(CAST(payloadJSON AS TEXT), '$.type')
            ) IN ('text', 'system')
          ORDER BY sequenceNumber DESC, timestamp DESC, messageID DESC
          LIMIT 1
          """,
        arguments: [conversationID, normalizedUserDID]
      ) else {
        return nil
      }

      let epoch: Int64 = row["epoch"]
      let sequenceNumber: Int64 = row["sequenceNumber"]
      let messageID: String = row["messageID"]
      return (epoch, sequenceNumber, messageID)
    }
  }

  /// Fetch the last successfully stored current-user message cursor for a conversation.
  ///
  /// Used to persist fallback remote read receipts when the server event omits a `messageId`.
  public func fetchLastCurrentUserMessageCursor(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> (epoch: Int64, seq: Int64, messageID: String)? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    let senderCandidates = Array(Set([normalizedUserDID, currentUserDID]))

    return try await database.read { db in
      var request = MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.processingError == nil)
        .filter(MLSMessageModel.Columns.payloadExpired == false)

      if senderCandidates.count == 1, let senderCandidate = senderCandidates.first {
        request = request.filter(MLSMessageModel.Columns.senderID == senderCandidate)
      } else {
        request = request.filter(senderCandidates.contains(MLSMessageModel.Columns.senderID))
      }

      return try request
        .order(
          MLSMessageModel.Columns.sequenceNumber.desc,
          MLSMessageModel.Columns.timestamp.desc,
          MLSMessageModel.Columns.messageID.desc
        )
        .limit(1)
        .fetchOne(db)
        .map { ($0.epoch, $0.sequenceNumber, $0.messageID) }
    }
  }

  /// Fetch a single message by ID.
  ///
  /// - Parameters:
  ///   - messageID: Message identifier
  ///   - currentUserDID: Current user's DID
  ///   - database: Database writer
  /// - Returns: Message model or nil if not found
  public func fetchMessage(
    messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> MLSMessageModel? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      if let normalizedMatch = try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)
      {
        return normalizedMatch
      }

      guard normalizedUserDID != currentUserDID else { return nil }

      let legacyMatch = try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)

      if legacyMatch != nil {
        logger.warning(
          "⚠️ [DID-NORMALIZE] Legacy message row matched via raw DID for \(messageID.prefix(16))")
      }
      return legacyMatch
    }
  }

  /// Update the sequence number (and epoch) for an existing message.
  /// Used to correct messages saved by the NSE with wrong seq values.
  public func updateMessageSequenceNumber(
    messageID: String,
    currentUserDID: String,
    sequenceNumber: Int64,
    epoch: Int64,
    database: MLSDatabase
  ) async throws {
    let normalizedUserDID = normalizeDID(currentUserDID)
    try await database.write { db in
      try db.execute(
        sql: """
          UPDATE MLSMessageModel
          SET sequenceNumber = ?, epoch = ?
          WHERE messageID = ? AND currentUserDID = ?;
          """,
        arguments: [sequenceNumber, epoch, messageID, normalizedUserDID])
    }
  }

  // MARK: - Read Frontier

  /// Upsert/advance local read frontier for a conversation.
  ///
  /// Monotonicity is enforced by stable conversation `sequenceNumber`.
  ///
  /// - Returns: `true` when frontier is inserted/advanced, `false` when ignored.
  @discardableResult
  public func upsertReadFrontier(
    conversationID: String,
    currentUserDID: String,
    epoch: Int64,
    sequenceNumber: Int64,
    messageID: String? = nil,
    database: MLSDatabase
  ) async throws -> Bool {
    try await MLSStorageHelpers.upsertReadFrontier(
      in: database,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      messageID: messageID
    )
  }

  /// Fetch local read frontier for a conversation.
  public func fetchReadFrontier(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> MLSReadFrontierModel? {
    try await MLSStorageHelpers.fetchReadFrontier(
      from: database,
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )
  }

  /// Apply local read frontier to mark late/backfilled messages as read.
  ///
  /// - Returns: Number of rows marked as read.
  @discardableResult
  public func applyReadFrontierToMessages(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> Int {
    try await MLSStorageHelpers.applyReadFrontierToMessages(
      in: database,
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )
  }

  // MARK: - Remote Read Cursor

  /// Upsert/advance a persisted remote read cursor for another participant.
  @discardableResult
  public func upsertRemoteReadCursor(
    conversationID: String,
    currentUserDID: String,
    readerDID: String,
    epoch: Int64? = nil,
    sequenceNumber: Int64? = nil,
    messageID: String? = nil,
    database: MLSDatabase
  ) async throws -> Bool {
    try await MLSStorageHelpers.upsertRemoteReadCursor(
      in: database,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      readerDID: readerDID,
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      messageID: messageID
    )
  }

  /// Fetch persisted remote read cursors for a conversation.
  public func fetchRemoteReadCursors(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [MLSRemoteReadCursorModel] {
    try await MLSStorageHelpers.fetchRemoteReadCursors(
      from: database,
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )
  }

  // MARK: - Admin Roster

  /// Persist the latest encrypted admin roster for a conversation
  public func saveAdminRoster(_ roster: MLSAdminRosterModel, database: MLSDatabase) async throws {
    try await database.write { db in
      try roster.save(db)
    }

    logger.info("✅ Saved admin roster v\(roster.version) for convo \(roster.conversationID)")
  }

  /// Fetch the cached admin roster for a conversation
  public func fetchAdminRoster(
    conversationID: String,
    database: MLSDatabase
  ) async throws -> MLSAdminRosterModel? {
    try await database.read { db in
      try MLSAdminRosterModel
        .filter(MLSAdminRosterModel.Columns.conversationID == conversationID)
        .fetchOne(db)
    }
  }

  // MARK: - Membership History

  /// Record a membership event in the audit log
  public func recordMembershipEvent(
    _ event: MLSMembershipEventModel,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      try event.save(db)
    }
    logger.info(
      "✅ Recorded membership event: \(event.eventType.rawValue) for \(event.memberDID.prefix(20))")
  }

  /// Fetch membership history for a conversation
  public func fetchMembershipHistory(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase,
    limit: Int = 100
  ) async throws -> [MLSMembershipEventModel] {
    return try await database.read { db in
      try MLSMembershipEventModel
        .filter(MLSMembershipEventModel.Columns.conversationID == conversationID)
        .filter(MLSMembershipEventModel.Columns.currentUserDID == currentUserDID)
        .order(MLSMembershipEventModel.Columns.timestamp.desc)
        .limit(limit)
        .fetchAll(db)
    }
  }

  /// Fetch members including removed ones (for history view)
  public func fetchAllMembers(
    conversationID: String,
    currentUserDID: String,
    includeRemoved: Bool = false,
    database: MLSDatabase
  ) async throws -> [MLSMemberModel] {
    return try await database.read { db in
      var query =
        MLSMemberModel
        .filter(MLSMemberModel.Columns.conversationID == conversationID)
        .filter(MLSMemberModel.Columns.currentUserDID == currentUserDID)

      if !includeRemoved {
        query = query.filter(MLSMemberModel.Columns.isActive == true)
      }

      return
        try query
        .order(MLSMemberModel.Columns.addedAt.desc)
        .fetchAll(db)
    }
  }

  /// Update conversation's epoch in GRDB after epoch-advancing operations (send, commit, sync)
  public func updateConversationEpoch(
    conversationID: String,
    currentUserDID: String,
    epoch: Int64,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      if var conversation =
        try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)
      {
        let updated = conversation.withEpoch(epoch)
        try updated.update(db)
      }
    }
  }

  /// Update conversation's lastMessageAt timestamp (e.g. after sending a message)
  public func updateConversationLastMessageAt(
    conversationID: String,
    currentUserDID: String,
    timestamp: Date,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      if let conversation =
        try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)
      {
        let updated = conversation.withLastMessageAt(timestamp)
        try updated.update(db)
      }
    }
  }

  /// Update conversation's last membership change timestamp
  public func updateConversationJoinInfo(
    conversationID: String,
    currentUserDID: String,
    joinMethod: MLSJoinMethod,
    joinEpoch: Int64,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      if var conversation =
        try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)
      {
        let updated = conversation.withJoinInfo(method: joinMethod, epoch: joinEpoch)
        try updated.update(db)
      }
    }
  }

  public func updateConversationMembershipTimestamp(
    conversationID: String,
    currentUserDID: String,
    timestamp: Date = Date(),
    incrementBadge: Bool = true,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      if var conversation =
        try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)
      {

        let updated = conversation.withMembershipChange(
          timestamp: timestamp,
          unacknowledged: incrementBadge ? 1 : 0
        )
        try updated.update(db)
      }
    }
  }

  /// Clear membership change badge for a conversation
  public func clearMembershipChangeBadge(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      if var conversation =
        try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)
      {

        let updated = conversation.clearMembershipChangeBadge()
        try updated.update(db)
      }
    }
  }

  // MARK: - Chat Request State Methods

  /// Update the request state of a conversation (local-only, not synced to server)
  public func updateConversationRequestState(
    conversationID: String,
    currentUserDID: String,
    state: MLSRequestState,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      if let conversation = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .fetchOne(db)
      {
        let updated = conversation.withRequestState(state)
        try updated.update(db)
      }
    }
  }

  // MARK: - Timed Silence Methods

  /// Set or clear the muted-until date for a conversation
  public func setMutedUntil(
    conversationID: String,
    currentUserDID: String,
    mutedUntil: Date?,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET mutedUntil = ?, updatedAt = ?
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [mutedUntil, Date(), conversationID, currentUserDID]
      )
    }
  }

  /// Fetch all pending inbound chat request conversations
  public func fetchPendingRequestConversations(
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [MLSConversationModel] {
    try await database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .filter(MLSConversationModel.Columns.requestState == MLSRequestState.pendingInbound.rawValue)
        .filter(MLSConversationModel.Columns.isActive == true)
        .order(MLSConversationModel.Columns.createdAt.desc)
        .fetchAll(db)
    }
  }

  /// Fetch conversations that are NOT pending requests (normal inbox)
  public func fetchAcceptedConversations(
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [MLSConversationModel] {
    try await database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.currentUserDID == currentUserDID)
        .filter(MLSConversationModel.Columns.requestState != MLSRequestState.pendingInbound.rawValue)
        .filter(MLSConversationModel.Columns.isActive == true)
        .order(MLSConversationModel.Columns.lastMessageAt.desc)
        .fetchAll(db)
    }
  }

  /// Accept a conversation request (local-only operation)
  public func acceptConversationRequest(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws {
    try await updateConversationRequestState(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      state: .none,
      database: database
    )
  }

  /// Fetch recent membership changes (last 24 hours) across all conversations
  public func fetchRecentMembershipChanges(
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [MLSMembershipEventModel] {
    let cutoff = Date().addingTimeInterval(-86400)  // 24 hours ago

    return try await database.read { db in
      try MLSMembershipEventModel
        .filter(MLSMembershipEventModel.Columns.currentUserDID == currentUserDID)
        .filter(MLSMembershipEventModel.Columns.timestamp > cutoff)
        .order(MLSMembershipEventModel.Columns.timestamp.desc)
        .fetchAll(db)
    }
  }

  // MARK: - Reaction Persistence

  /// Save a reaction to local storage
  /// Reactions arrive via SSE and must be persisted locally since the server doesn't store them
  /// - Parameters:
  ///   - reaction: The reaction model to save
  ///   - database: Database to write to
  public func saveReaction(
    _ reaction: MLSReactionModel,
    database: MLSDatabase
  ) async throws {
    logger.debug(
      "💾 Saving reaction: \(reaction.emoji) on \(reaction.messageID) by \(reaction.actorDID)")
      
    let normalized = MLSReactionModel(
      reactionID: reaction.reactionID,
      messageID: reaction.messageID,
      conversationID: reaction.conversationID,
      currentUserDID: normalizeDID(reaction.currentUserDID),
      actorDID: normalizeDID(reaction.actorDID),
      emoji: reaction.emoji,
      action: reaction.action,
      timestamp: reaction.timestamp
    )

    do {
      try await database.write { db in
        // De-dupe by (messageID, actorDID, emoji, currentUserDID) to avoid replay duplicates.
        _ =
          try MLSReactionModel
          .filter(MLSReactionModel.Columns.messageID == normalized.messageID)
          .filter(MLSReactionModel.Columns.actorDID == normalized.actorDID)
          .filter(MLSReactionModel.Columns.emoji == normalized.emoji)
          .filter(MLSReactionModel.Columns.currentUserDID == normalized.currentUserDID)
          .deleteAll(db)

        try normalized.insert(db)
      }
      logger.debug("✅ Reaction saved: \(reaction.reactionID)")
    } catch let error as DatabaseError {
      // Check for foreign key constraint violation (messageID not found)
      // SQLITE_CONSTRAINT_FOREIGNKEY = 787
      let isForeignKeyError =
        error.extendedResultCode.rawValue == 787
        || (error.resultCode == .SQLITE_CONSTRAINT
          && error.message?.uppercased().contains("FOREIGN KEY") == true)

      if isForeignKeyError {
        logger.warning(
          "[ORPHAN] Parent message \(normalized.messageID) not found (FK error) - saving as ORPHANED reaction: \(normalized.emoji) by \(normalized.actorDID.prefix(20))"
        )

        // Log diagnostic info about the error for debugging
        logger.debug(
          "[ORPHAN] FK Error details -> Code: \(error.resultCode), Extended: \(error.extendedResultCode), Message: \(error.message ?? "nil")"
        )

        let orphan = MLSOrphanedReactionModel(
          reactionID: normalized.reactionID,
          messageID: normalized.messageID,
          conversationID: normalized.conversationID,
          currentUserDID: normalized.currentUserDID,
          actorDID: normalized.actorDID,
          emoji: normalized.emoji,
          action: normalized.action,
          timestamp: normalized.timestamp
        )

        try await saveOrphanedReaction(orphan, database: database)

        // Log for diagnostic purposes - caller may want to trigger parent fetch
        logger.info(
          "[ORPHAN] Created orphaned reaction \(normalized.emoji) (id: \(normalized.reactionID)) for missing message \(normalized.messageID) in \(normalized.conversationID.prefix(20))"
        )
      } else {
        // If it's a constraint error but not foreign key, log it specifically
        if error.resultCode == .SQLITE_CONSTRAINT {
          logger.error("❌ SQL Constraint Error in saveReaction: \(error)")
        }
        throw error
      }
    }
  }

  /// Delete a reaction from local storage
  /// Called when a user removes their reaction via SSE event
  /// - Parameters:
  ///   - messageID: The message the reaction was on
  ///   - actorDID: The DID of the user who removed the reaction
  ///   - emoji: The emoji that was removed
  ///   - currentUserDID: Current authenticated user's DID
  ///   - database: Database to write to
  public func deleteReaction(
    messageID: String,
    actorDID: String,
    emoji: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws {
    logger.debug("🗑️ Deleting reaction: \(emoji) on \(messageID) by \(actorDID)")
    
    let normalizedUserDID = normalizeDID(currentUserDID)
    let normalizedActorDID = normalizeDID(actorDID)

    // 1. Delete from active reactions
    let deletedCount = try await database.write { db -> Int in
      try MLSReactionModel
        .filter(MLSReactionModel.Columns.messageID == messageID)
        .filter(MLSReactionModel.Columns.actorDID == normalizedActorDID)
        .filter(MLSReactionModel.Columns.emoji == emoji)
        .filter(MLSReactionModel.Columns.currentUserDID == normalizedUserDID)
        .deleteAll(db)
    }

    // 2. Delete from orphaned reactions (Zombie Prevention)
    // If the "Add" reaction is still orphaned (waiting for parent message),
    // we must delete it there too, otherwise it will be adopted and resurrected
    // when the parent message finally arrives.
    let deletedOrphans = try await database.write { db -> Int in
      try MLSOrphanedReactionModel
        .filter(MLSOrphanedReactionModel.Columns.messageID == messageID)
        .filter(MLSOrphanedReactionModel.Columns.actorDID == normalizedActorDID)
        .filter(MLSOrphanedReactionModel.Columns.emoji == emoji)
        .filter(MLSOrphanedReactionModel.Columns.currentUserDID == normalizedUserDID)
        .deleteAll(db)
    }

    logger.debug("✅ Deleted \(deletedCount) reaction(s) and \(deletedOrphans) orphan(s)")
  }

  /// Fetch all reactions for a conversation
  /// Used when opening a conversation to restore cached reactions
  /// - Parameters:
  ///   - conversationID: The conversation to fetch reactions for
  ///   - currentUserDID: Current authenticated user's DID
  ///   - database: Database to read from
  /// - Returns: Dictionary mapping messageID to array of reactions
  public func fetchReactionsForConversation(
    _ conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [String: [MLSReactionModel]] {
    logger.debug("🔍 Fetching reactions for conversation: \(conversationID)")
    
    let normalizedUserDID = normalizeDID(currentUserDID)

    // Retry logic for transient SQLite errors (OOM, busy, locked)
    var lastError: Error?
    for attempt in 1...3 {
      do {
        let reactions = try await database.read { db in
          try MLSReactionModel
            .filter(MLSReactionModel.Columns.conversationID == conversationID)
            .filter(MLSReactionModel.Columns.currentUserDID == normalizedUserDID)
            .filter(MLSReactionModel.Columns.action == "add")
            .order(MLSReactionModel.Columns.timestamp.asc)
            .fetchAll(db)
        }

        // Group by messageID
        let grouped = Dictionary(grouping: reactions) { $0.messageID }

        logger.debug("✅ Found \(reactions.count) reactions across \(grouped.count) messages")
        return grouped
      } catch {
        lastError = error
        let desc = error.localizedDescription
        // Check for retryable SQLite errors
        if (desc.contains("out of memory") || desc.contains("busy") || desc.contains("locked")
          || desc.contains("error 7") || desc.contains("error 5") || desc.contains("error 6"))
          && attempt < 3
        {
          logger.warning("⚠️ [REACTION-FETCH] Transient error (attempt \(attempt)): \(desc)")
          try? await Task.sleep(nanoseconds: UInt64(50 * attempt) * 1_000_000)  // 50ms, 100ms backoff
          continue
        }
        throw error
      }
    }
    throw lastError ?? MLSStorageError.noAuthentication
  }

  /// Fetch reactions for specific messages
  /// Used for efficiently loading reactions for visible messages
  /// - Parameters:
  ///   - messageIDs: Array of message IDs to fetch reactions for
  ///   - currentUserDID: Current authenticated user's DID
  ///   - database: Database to read from
  /// - Returns: Dictionary mapping messageID to array of reactions
  public func fetchReactionsForMessages(
    _ messageIDs: [String],
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [String: [MLSReactionModel]] {
    guard !messageIDs.isEmpty else { return [:] }

    logger.debug("🔍 Fetching reactions for \(messageIDs.count) messages")
    
    let normalizedUserDID = normalizeDID(currentUserDID)

    let reactions = try await database.read { db in
      try MLSReactionModel
        .filter(messageIDs.contains(MLSReactionModel.Columns.messageID))
        .filter(MLSReactionModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSReactionModel.Columns.action == "add")
        .order(MLSReactionModel.Columns.timestamp.asc)
        .fetchAll(db)
    }

    // Group by messageID
    let grouped = Dictionary(grouping: reactions) { $0.messageID }

    logger.debug("✅ Found \(reactions.count) reactions for requested messages")
    return grouped
  }

  /// Delete all reactions for a conversation
  /// Used when leaving a conversation or for cleanup
  /// - Parameters:
  ///   - conversationID: The conversation to delete reactions for
  ///   - currentUserDID: Current authenticated user's DID
  ///   - database: Database to write to
  public func deleteReactionsForConversation(
    _ conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws {
    logger.debug("🗑️ Deleting all reactions for conversation: \(conversationID)")
    
    let normalizedUserDID = normalizeDID(currentUserDID)

    let deletedCount = try await database.write { db -> Int in
      try MLSReactionModel
        .filter(MLSReactionModel.Columns.conversationID == conversationID)
        .filter(MLSReactionModel.Columns.currentUserDID == normalizedUserDID)
        .deleteAll(db)
    }

    logger.debug("✅ Deleted \(deletedCount) reactions")
  }

  // MARK: - Orphaned Reaction Management

  /// Save an orphaned reaction (parent message missing)
  private func saveOrphanedReaction(
    _ orphan: MLSOrphanedReactionModel,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      // De-dupe by (messageID, actorDID, emoji, currentUserDID) to avoid replay duplicates.
      _ =
        try MLSOrphanedReactionModel
        .filter(MLSOrphanedReactionModel.Columns.messageID == orphan.messageID)
        .filter(MLSOrphanedReactionModel.Columns.actorDID == orphan.actorDID)
        .filter(MLSOrphanedReactionModel.Columns.emoji == orphan.emoji)
        .filter(MLSOrphanedReactionModel.Columns.currentUserDID == orphan.currentUserDID)
        .deleteAll(db)

      try orphan.insert(db)
    }
    logger.info(
      "[ORPHAN] Saved orphaned reaction: \(orphan.emoji) (id: \(orphan.reactionID)) for missing message \(orphan.messageID)"
    )
  }

  /// A reaction that was adopted from the orphan table
  public struct AdoptedReaction {
    public let messageID: String
    public let conversationID: String
    public let actorDID: String
    public let emoji: String
    public let action: String
  }

  /// Adopt any orphaned reactions for a newly arrived message (public version)
  /// - Parameters:
  ///   - messageID: The message ID that just arrived
  ///   - currentUserDID: Current user's DID
  ///   - database: Database to use
  /// - Returns: Array of adopted reactions (for UI notification)
  @discardableResult
  public func adoptOrphansForMessage(
    _ messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [AdoptedReaction] {
    let normalizedUserDID = normalizeDID(currentUserDID)

    // 1. Find orphans and verify parent message exists in the same transaction
    let (orphans, messageExists) = try await database.read {
      db -> ([MLSOrphanedReactionModel], Bool) in
      let orphanList =
        try MLSOrphanedReactionModel
        .filter(MLSOrphanedReactionModel.Columns.messageID == messageID)
        .filter(MLSOrphanedReactionModel.Columns.currentUserDID == normalizedUserDID)
        .fetchAll(db)

      // Check if parent message exists
      let exists =
        try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .fetchCount(db) > 0

      return (orphanList, exists)
    }

    guard !orphans.isEmpty else { return [] }

    // If parent message doesn't exist, don't attempt adoption - keep as orphan
    guard messageExists else {
      logger.debug(
        "[ORPHAN] Skipping adoption for \(orphans.count) reaction(s) - parent message \(messageID.prefix(16)) not yet available"
      )
      return []
    }

    logger.info(
      "[ORPHAN] Found \(orphans.count) orphaned reactions for message \(messageID) - adopting now")

    // 2. Move to regular table with FK error handling
    var adoptedOrphans: [MLSOrphanedReactionModel] = []
    try await database.write { db in
      for orphan in orphans {
        // Convert to regular model
        let regular = orphan.toRegularModel()

        do {
          // De-dupe by (messageID, actorDID, emoji, currentUserDID) to avoid replay duplicates.
          // This mirrors saveReaction logic to ensure consistency
          _ =
            try MLSReactionModel
            .filter(MLSReactionModel.Columns.messageID == regular.messageID)
            .filter(MLSReactionModel.Columns.actorDID == regular.actorDID)
            .filter(MLSReactionModel.Columns.emoji == regular.emoji)
            .filter(MLSReactionModel.Columns.currentUserDID == regular.currentUserDID)
            .deleteAll(db)

          // Save regular (which should now work as message exists)
          try regular.insert(db)
          // Delete orphan only if save succeeded
          try orphan.delete(db)
          adoptedOrphans.append(orphan)
        } catch let error as DatabaseError {
          // Handle race condition: message may have been deleted between check and insert
          let isForeignKeyError =
            error.extendedResultCode.rawValue == 787
            || (error.resultCode == .SQLITE_CONSTRAINT
              && error.message?.uppercased().contains("FOREIGN KEY") == true)

          if isForeignKeyError {
            // Parent message disappeared (race condition) - keep as orphan for next attempt
            logger.debug(
              "[ORPHAN] FK error during adoption for \(orphan.emoji) on \(messageID.prefix(16)) - keeping as orphan"
            )
          } else {
            throw error
          }
        }
      }
    }

    // 3. Return adopted reactions for UI notification
    let adopted = adoptedOrphans.map { orphan in
      AdoptedReaction(
        messageID: orphan.messageID,
        conversationID: orphan.conversationID,
        actorDID: orphan.actorDID,
        emoji: orphan.emoji,
        action: orphan.action
      )
    }

    if !adopted.isEmpty {
      logger.info("[ORPHAN-ADOPT] Adopted \(adopted.count) reaction(s) for message \(messageID)")
    }
    return adopted
  }

  /// Adopt any orphaned reactions for a newly arrived message (internal version)
  private func adoptOrphanedReactions(
    for messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws {
    _ = try await adoptOrphansForMessage(
      messageID, currentUserDID: currentUserDID, database: database)
  }

  /// Fetch all orphaned reactions for a conversation (for background adoption task)
  /// - Parameters:
  ///   - conversationID: The conversation to check
  ///   - currentUserDID: Current user's DID
  ///   - limit: Maximum number to return
  ///   - database: Database to use
  /// - Returns: Array of (messageID, orphanCount) tuples grouped by parent message
  public func fetchOrphanedReactionStats(
    for conversationID: String,
    currentUserDID: String,
    limit: Int = 50,
    database: MLSDatabase
  ) async throws -> [(messageID: String, count: Int)] {
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.read { db in
      let rows = try Row.fetchAll(
        db,
        sql: """
          SELECT messageID, COUNT(*) as count
          FROM MLSOrphanedReactionModel
          WHERE conversationID = ? AND currentUserDID = ?
          GROUP BY messageID
          ORDER BY count DESC
          LIMIT ?
          """, arguments: [conversationID, normalizedUserDID, limit])

      return rows.map { row in
        (messageID: row["messageID"] as String, count: row["count"] as Int)
      }
    }
  }

  /// Fetch all unique parent message IDs that have orphaned reactions
  /// - Parameters:
  ///   - currentUserDID: Current user's DID
  ///   - limit: Maximum number to return
  ///   - database: Database to use
  /// - Returns: Array of (messageID, conversationID) tuples for missing parent messages
  public func fetchMissingParentMessageIDs(
    currentUserDID: String,
    limit: Int = 100,
    database: MLSDatabase
  ) async throws -> [(messageID: String, conversationID: String)] {
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.read { db in
      let rows = try Row.fetchAll(
        db,
        sql: """
          SELECT DISTINCT messageID, conversationID
          FROM MLSOrphanedReactionModel
          WHERE currentUserDID = ?
          LIMIT ?
          """, arguments: [normalizedUserDID, limit])

      return rows.map { row in
        (messageID: row["messageID"] as String, conversationID: row["conversationID"] as String)
      }
    }
  }

  // MARK: - Message Ordering (Cross-Process Coordination)

  /// Get the last processed sequence number for a conversation
  /// Used by both NSE and main app to coordinate message ordering
  public func getLastProcessedSeq(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> Int64 {
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.read { db in
      if let state =
        try MLSConversationSequenceState
        .filter(MLSConversationSequenceState.Columns.conversationID == conversationID)
        .filter(MLSConversationSequenceState.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)
      {
        return state.lastProcessedSeq
      }
      return -1  // No messages processed yet
    }
  }

  /// Get the maximum cached message sequence number for a conversation
  /// Used to initialize lastProcessedSeq when no sequence state exists yet
  /// (e.g., messages were loaded during initial sync before seq-order tracking was set up)
  public func getMaxCachedMessageSeq(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> Int64? {
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.read { db in
      let maxSeq = try Int64.fetchOne(
        db,
        sql: """
          SELECT MAX(sequenceNumber)
          FROM \(MLSMessageModel.databaseTableName)
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [conversationID, normalizedUserDID]
      )
      return maxSeq
    }
  }

  /// Update the last processed sequence number for a conversation
  /// Called after successfully processing a message
  public func updateLastProcessedSeq(
    conversationID: String,
    currentUserDID: String,
    sequenceNumber: Int64,
    database: MLSDatabase
  ) async throws {
    let normalizedUserDID = normalizeDID(currentUserDID)

    try await database.write { db in
      // Only update if the new seq is higher (prevents race conditions)
      if let existing =
        try MLSConversationSequenceState
        .filter(MLSConversationSequenceState.Columns.conversationID == conversationID)
        .filter(MLSConversationSequenceState.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)
      {

        if sequenceNumber > existing.lastProcessedSeq {
          var updated = existing
          updated.lastProcessedSeq = sequenceNumber
          updated.updatedAt = Date()
          try updated.update(db)
          // logger.debug("[SEQ] Updated lastProcessedSeq for \(conversationID.prefix(16)): \(existing.lastProcessedSeq) -> \(sequenceNumber)")
        }
      } else {
        // Create new state
        let state = MLSConversationSequenceState(
          conversationID: conversationID,
          currentUserDID: normalizedUserDID,
          lastProcessedSeq: sequenceNumber,
          updatedAt: Date()
        )
        try state.insert(db)
        // logger.debug("[SEQ] Created sequence state for \(conversationID.prefix(16)): seq=\(sequenceNumber)")
      }
    }
  }

  /// Buffer a message that arrived out of order
  /// Both NSE and main app can add messages to this buffer
  public func bufferPendingMessage(
    _ pending: MLSPendingMessageModel,
    database: MLSDatabase
  ) async throws {
    try await database.write { db in
      try pending.save(db, onConflict: .replace)
    }
    logger.info(
      "[SEQ-BUFFER] Buffered message \(pending.messageID.prefix(16)) seq=\(pending.sequenceNumber) from \(pending.source)"
    )
  }

  /// Get all pending messages for a conversation that can now be processed
  /// Returns messages in sequence order that are <= the given target sequence
  public func getPendingMessagesUpTo(
    conversationID: String,
    currentUserDID: String,
    targetSeq: Int64,
    database: MLSDatabase
  ) async throws -> [MLSPendingMessageModel] {
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.read { db in
      try MLSPendingMessageModel
        .filter(MLSPendingMessageModel.Columns.conversationID == conversationID)
        .filter(MLSPendingMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSPendingMessageModel.Columns.sequenceNumber <= targetSeq)
        .order(MLSPendingMessageModel.Columns.sequenceNumber)
        .fetchAll(db)
    }
  }

  /// Get the next pending message in sequence for a conversation
  /// Returns the message with the lowest sequence number > lastProcessedSeq
  public func getNextPendingMessage(
    conversationID: String,
    currentUserDID: String,
    afterSeq: Int64,
    database: MLSDatabase
  ) async throws -> MLSPendingMessageModel? {
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.read { db in
      try MLSPendingMessageModel
        .filter(MLSPendingMessageModel.Columns.conversationID == conversationID)
        .filter(MLSPendingMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSPendingMessageModel.Columns.sequenceNumber > afterSeq)
        .order(MLSPendingMessageModel.Columns.sequenceNumber)
        .limit(1)
        .fetchOne(db)
    }
  }

  /// Remove a pending message after it's been successfully processed
  public func removePendingMessage(
    messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws {
    let normalizedUserDID = normalizeDID(currentUserDID)

    try await database.write { db in
      try MLSPendingMessageModel
        .filter(MLSPendingMessageModel.Columns.messageID == messageID)
        .filter(MLSPendingMessageModel.Columns.currentUserDID == normalizedUserDID)
        .deleteAll(db)
    }
  }

  /// Check if a message is already pending (to avoid duplicate buffering)
  public func isMessagePending(
    messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> Bool {
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.read { db in
      try MLSPendingMessageModel
        .filter(MLSPendingMessageModel.Columns.messageID == messageID)
        .filter(MLSPendingMessageModel.Columns.currentUserDID == normalizedUserDID)
        .fetchCount(db) > 0
    }
  }

  /// Get all pending messages for a conversation (for flush/cleanup)
  public func getAllPendingMessages(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [MLSPendingMessageModel] {
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.read { db in
      try MLSPendingMessageModel
        .filter(MLSPendingMessageModel.Columns.conversationID == conversationID)
        .filter(MLSPendingMessageModel.Columns.currentUserDID == normalizedUserDID)
        .order(MLSPendingMessageModel.Columns.sequenceNumber)
        .fetchAll(db)
    }
  }

  /// Clean up old pending messages (older than timeout)
  /// Should be called periodically to prevent unbounded growth
  public func cleanupOldPendingMessages(
    olderThan timeout: TimeInterval,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> Int {
    let normalizedUserDID = normalizeDID(currentUserDID)
    let cutoff = Date().addingTimeInterval(-timeout)

    return try await database.write { db in
      let count =
        try MLSPendingMessageModel
        .filter(MLSPendingMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSPendingMessageModel.Columns.receivedAt < cutoff)
        .deleteAll(db)

      if count > 0 {
        // Logging moved to caller
      }
      return count
    }
  }

  /// Increment process attempts for a pending message (for retry tracking)
  public func incrementPendingMessageAttempts(
    messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws {
    let normalizedUserDID = normalizeDID(currentUserDID)

    try await database.write { db in
      if var pending =
        try MLSPendingMessageModel
        .filter(MLSPendingMessageModel.Columns.messageID == messageID)
        .filter(MLSPendingMessageModel.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)
      {
        pending.processAttempts += 1
        try pending.update(db)
      }
    }
  }
}

// MARK: - Errors

enum MLSStorageError: LocalizedError {
  case noAuthentication
  case conversationNotFound(String)
  case memberNotFound(String)
  case messageNotFound(String)
  case keyPackageNotFound(String)
  case invalidGroupID(String)
  case saveFailed(Error)
  case foreignKeyViolation(String)
  case accountMismatch(requested: String, active: String)
  case databaseClosed

  var errorDescription: String? {
    switch self {
    case .noAuthentication:
      return "No authenticated user"
    case .conversationNotFound(let id):
      return "Conversation not found: \(id)"
    case .memberNotFound(let id):
      return "Member not found: \(id)"
    case .messageNotFound(let id):
      return "Message not found: \(id)"
    case .keyPackageNotFound(let id):
      return "Key package not found: \(id)"
    case .invalidGroupID(let id):
      return "Invalid group ID format: \(id)"
    case .saveFailed(let error):
      return "Failed to save: \(error.localizedDescription)"
    case .foreignKeyViolation(let message):
      return "Foreign key constraint violation: \(message)"
    case .accountMismatch(let requested, let active):
      return
        "Database account mismatch: requested \(requested.prefix(20))..., but active is \(active.prefix(20))..."
    case .databaseClosed:
      return "Database has been closed"
    }
  }
}
