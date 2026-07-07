//
//  MLSStorage.swift
//  Catbird
//
//  MLS SQLCipher storage layer providing CRUD operations for encrypted messages.
//  Designed for Swift 6 concurrency with proper actor isolation.
//

import CatbirdMLS
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

  /// Walk the per-conversation HMAC chain to its tail and return the highest
  /// sequence-number row's `entryHMAC` (sync helper for use inside a write
  /// closure). Returns `nil` when there is no prior entry; the Rust HMAC
  /// implementation will seed with 32 zero bytes in that case.
  ///
  /// Tombstones are intentionally NOT filtered: chain walking covers the full
  /// transcript including soft-deleted rows so the verifier can prove
  /// continuity.
  fileprivate static func fetchLastEntryHMACSync(
    conversationID: String,
    currentUserDID: String,
    legacyUserDID: String?,
    db: Database
  ) throws -> Data? {
    if let row = try MLSMessageModel
      .filter(MLSMessageModel.Columns.conversationID == conversationID)
      .filter(MLSMessageModel.Columns.currentUserDID == currentUserDID)
      .order(
        MLSMessageModel.Columns.sequenceNumber.desc,
        MLSMessageModel.Columns.timestamp.desc,
        MLSMessageModel.Columns.messageID.desc
      )
      .fetchOne(db)
    {
      return row.entryHMAC
    }
    if let legacy = legacyUserDID, legacy != currentUserDID,
       let row = try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == legacy)
        .order(
          MLSMessageModel.Columns.sequenceNumber.desc,
          MLSMessageModel.Columns.timestamp.desc,
          MLSMessageModel.Columns.messageID.desc
        )
        .fetchOne(db)
    {
      return row.entryHMAC
    }
    return nil
  }

  /// Async wrapper over `fetchLastEntryHMACSync` for tests / future callers.
  public func fetchLastEntryHMAC(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> Data? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      try Self.fetchLastEntryHMACSync(
        conversationID: conversationID,
        currentUserDID: normalizedUserDID,
        legacyUserDID: currentUserDID,
        db: db
      )
    }
  }

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
    context: MlsContext,
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

    // Encode full payload to JSON, then encrypt at the field level so the
    // ciphertext is what lands in the DB (defense in depth above SQLCipher).
    let payloadData = try payload.encodeToJSON()
    logger.debug("Encoded payload (\(payloadData.count) bytes)")

    let encryptedWire = try MLSFieldEncryption.encrypt(
      context: context,
      conversationID: conversationID,
      plaintext: payloadData
    )
    logger.debug("Encrypted payload (\(encryptedWire.count) bytes wire)")

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

      // Compute the entry HMAC over (prev_hmac || messageID || encryptedWire).
      // For the first message in a conversation prev_hmac is nil (Rust seeds
      // with 32 zero bytes). For subsequent messages we walk the highest
      // sequence-number row. This is a tail-insert chain; UPDATE-path rows
      // re-seal with the same prev_hmac (best-effort under the current
      // verifier contract).
      let prevHMAC = try Self.fetchLastEntryHMACSync(
        conversationID: conversationID,
        currentUserDID: normalizedUserDID,
        legacyUserDID: currentUserDID,
        db: db
      )
      let entryHMAC = try MLSFieldEncryption.computeHMAC(
        context: context,
        conversationID: conversationID,
        previousHMAC: prevHMAC,
        messageID: messageID,
        payloadWire: encryptedWire
      )

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
          && (
            (existingMessage?.payloadEncrypted != nil
              && !(existingMessage?.payloadEncrypted?.isEmpty ?? true))
            || (existingMessage?.payloadJSON != nil
              && !(existingMessage?.payloadJSON?.isEmpty ?? true))
          )
        let newHasError = (processingError != nil)

        if existingHasValidPayload && newHasError {
          let existingSize = (existingMessage?.payloadEncrypted?.count
            ?? existingMessage?.payloadJSON?.count ?? 0)
          logger.warning(
            "⚠️ [SAVE-SKIP] Message \(messageID) already has valid payload - NOT overwriting with error state"
          )
          logger.warning(
            "   Existing payload size: \(existingSize) bytes, error: \(existingMessage?.processingError ?? "none")"
          )
          logger.warning("   Skipped error: \(processingError ?? "unknown")")
          // Still need to adopt orphans even if we skip the update
          // Fall through to orphan adoption logic
        } else {
          // Update existing message (normal path). Clear legacy payloadJSON
          // and write the new encrypted columns; entryHMAC is recomputed
          // against the latest tail (best-effort for mid-chain updates).
          try db.execute(
            sql: """
              UPDATE MLSMessageModel
              SET payloadJSON = NULL,
                  payloadEncrypted = ?,
                  entryHMAC = ?,
                  payloadKeyVersion = 1,
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
              encryptedWire,
              entryHMAC,
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

          logger.debug("Updated existing message with encrypted payload + HMAC")
        }
      } else {
        // Create new message — write only the encrypted columns; legacy
        // payloadJSON stays nil for all new rows.
        let message = MLSMessageModel(
          messageID: messageID,
          currentUserDID: normalizedUserDID,
          conversationID: conversationID,
          senderID: senderID,
          payloadJSON: nil,
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
          validationFailureReason: validationFailureReason,
          payloadEncrypted: encryptedWire,
          entryHMAC: entryHMAC,
          payloadKeyVersion: 1
        )
        try message.insert(db)

        logger.debug("Created new message with encrypted payload + HMAC")
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
    context: MlsContext,
    _ messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> MLSMessagePayload? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    logger.debug("Fetching payload: \(messageID)")

    // Hide tombstones from user-facing reads; chain walkers use their own
    // unfiltered queries.
    let row = try await database.read { db -> MLSMessageModel? in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.isTombstone == 0)
        .fetchOne(db)
    }

    guard let model = row else {
      logger.warning("⚠️ No cached payload found: \(messageID)")
      return nil
    }

    if let encrypted = model.payloadEncrypted, !encrypted.isEmpty {
      do {
        let plain = try MLSFieldEncryption.decrypt(
          context: context,
          conversationID: model.conversationID,
          wire: encrypted
        )
        let payload = try MLSMessagePayload.decodeFromJSON(plain)
        logger.debug("✅ Decrypted cached payload: \(messageID)")
        return payload
      } catch {
        logger.error(
          "❌ Failed to decrypt cached payload for \(messageID): \(error.localizedDescription)")
        throw error
      }
    }

    if let legacy = model.payloadJSON, !legacy.isEmpty {
      // Pre-migration row — fall back to plaintext JSON.
      let payload = try? MLSMessagePayload.decodeFromJSON(legacy)
      if payload != nil {
        logger.debug("✅ Found legacy plaintext payload: \(messageID)")
      } else {
        logger.warning("⚠️ Legacy payloadJSON failed to decode for \(messageID)")
      }
      return payload
    }

    logger.warning("⚠️ Row exists but no payload present: \(messageID)")
    return nil
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
    context: MlsContext,
    _ messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> String? {
    try await fetchPayloadForMessage(
      context: context,
      messageID,
      currentUserDID: currentUserDID,
      database: database
    )?.text
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
    context: MlsContext,
    _ messageID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> MLSEmbedData? {
    try await fetchPayloadForMessage(
      context: context,
      messageID,
      currentUserDID: currentUserDID,
      database: database
    )?.embed
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
      // Hide tombstones from user-facing reads. Accept either the new
      // encrypted payload column or the legacy plaintext column so this query
      // works across the migration window.
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.senderID == senderDID)
        .filter(MLSMessageModel.Columns.processingState == MLSMessageProcessingState.pendingSelfSend)
        .filter(MLSMessageModel.Columns.processingError == nil)
        .filter(
          MLSMessageModel.Columns.payloadEncrypted != nil
            || MLSMessageModel.Columns.payloadJSON != nil
        )
        .filter(MLSMessageModel.Columns.isTombstone == 0)
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
      // Hide tombstones from user-facing reads
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.isTombstone == 0)
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
      // Hide tombstones from user-facing reads
      var request = MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationId)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.isTombstone == 0)

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
      // `groupID` is a HEX string (the MLS group id). Store the RAW group bytes,
      // not the hex string's ASCII bytes. `Data(groupID.utf8)` was wrong: it
      // stored e.g. the 32 ASCII bytes of "4c71…", so `asConvoView()`'s later
      // `groupID.hexEncodedString()` produced a DOUBLE-hex-encoded id ("3463…" =
      // hex of ASCII "4c71…"), which then failed every FFI group lookup
      // (get_epoch → GroupNotFound) on the DB-projection fallback path.
      let groupIDData = Data(hexEncoded: groupID) ?? Data(groupID.utf8)

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

  /// Default retention window for parked (orphaned) edit/delete mutations and
  /// reactions whose target message never arrives. Without a reap, these
  /// parking-lot tables grow unbounded (e.g. a target permanently lost to a
  /// gap-fill failure, or a mutation for a message from a since-left
  /// conversation). Mirrors `messageKeyCleanupThreshold`'s retention style.
  public static let orphanedMutationRetentionDays = 30

  /// Reap orphaned edit/delete mutations (`MLSOrphanedMutationModel`) whose
  /// target message never arrived within the retention window. Call from the
  /// same background-cleanup pass as `cleanupMessageKeys` /
  /// `deleteExpiredKeyPackages`.
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - date: Delete orphaned mutations recorded before this date
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public func reapExpiredOrphanedMutations(
    userDID: String,
    olderThan date: Date,
    database: MLSDatabase
  ) async throws {
    let normalizedUserDID = normalizeDID(userDID)
    let deletedCount = try await database.write { db -> Int in
      try db.execute(
        sql: """
            DELETE FROM MLSOrphanedMutationModel
            WHERE currentUserDID = ? AND createdAt < ?;
          """, arguments: [normalizedUserDID, date])

      return db.changesCount
    }

    logger.info("Reaped \(deletedCount) expired orphaned mutation(s) older than \(date)")
  }

  /// Reap orphaned reactions (`MLSOrphanedReactionModel`) whose target message
  /// never arrived within the retention window. Pre-existing gap: this table
  /// was never swept by background cleanup prior to this fix.
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - date: Delete orphaned reactions recorded before this date
  ///   - database: MLSDatabase to use for operations
  /// - Throws: MLSStorageError if deletion fails
  public func reapExpiredOrphanedReactions(
    userDID: String,
    olderThan date: Date,
    database: MLSDatabase
  ) async throws {
    let normalizedUserDID = normalizeDID(userDID)
    let deletedCount = try await database.write { db -> Int in
      try db.execute(
        sql: """
            DELETE FROM MLSOrphanedReactionModel
            WHERE currentUserDID = ? AND timestamp < ?;
          """, arguments: [normalizedUserDID, date])

      return db.changesCount
    }

    logger.info("Reaped \(deletedCount) expired orphaned reaction(s) older than \(date)")
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
  /// This excludes payloads that failed processing, expired payloads, and tombstones
  /// so read receipts only advance on messages the user can actually read.
  ///
  /// NOTE (field encryption migration): The pre-encryption implementation used
  /// SQL `json_extract` on `payloadJSON` to keep only `text`/`system` rows. SQL
  /// cannot peek inside `payloadEncrypted`, so the messageType filter has been
  /// dropped at the SQL layer. The cursor may now occasionally advance over a
  /// non-displayable control row; the cross-platform chain verifier (Task 27)
  /// will own message-type filtering by decrypting in-process. Until then,
  /// callers that need a strict displayable cursor should fall back to the
  /// in-memory model-level filter.
  public func fetchLastDecryptedMessageCursor(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> (epoch: Int64, seq: Int64, messageID: String)? {
    let normalizedUserDID = normalizeDID(currentUserDID)
    return try await database.read { db in
      // Hide tombstones from user-facing reads. Match rows that have either
      // the new encrypted payload or a legacy plaintext payload.
      guard let row = try Row.fetchOne(
        db,
        sql: """
          SELECT epoch, sequenceNumber, messageID
          FROM MLSMessageModel
          WHERE conversationID = ?
            AND currentUserDID = ?
            AND processingError IS NULL
            AND payloadExpired = 0
            AND isTombstone = 0
            AND (
              (payloadEncrypted IS NOT NULL AND LENGTH(payloadEncrypted) > 0)
              OR (payloadJSON IS NOT NULL AND LENGTH(payloadJSON) > 0)
            )
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
      // Hide tombstones from user-facing reads
      var request = MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.processingError == nil)
        .filter(MLSMessageModel.Columns.payloadExpired == false)
        .filter(MLSMessageModel.Columns.isTombstone == 0)

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
      // Hide tombstones from user-facing reads
      if let normalizedMatch = try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.isTombstone == 0)
        .fetchOne(db)
      {
        return normalizedMatch
      }

      guard normalizedUserDID != currentUserDID else { return nil }

      let legacyMatch = try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageID)
        .filter(MLSMessageModel.Columns.currentUserDID == currentUserDID)
        .filter(MLSMessageModel.Columns.isTombstone == 0)
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

  // MARK: - Message Edit / Unsend Application (spec §5.7 / §5.8)

  /// A mutation (edit or delete) that was adopted from the orphan table, or applied
  /// live at receive time. Returned so callers can notify UI observers.
  public struct AdoptedMutation {
    public let kind: MLSMutationKind
    public let conversationID: String
    public let targetMessageID: String
  }

  /// Result of attempting to apply an edit/delete mutation against a target
  /// message. Distinguishes "target row exists" from "the mutation actually
  /// changed persisted state" — callers must only notify UI observers when
  /// `mutated` is `true`. A target that exists but silently rejects the
  /// mutation (authorization mismatch, stale `editSeq`, already-tombstoned,
  /// non-text target, idempotent replay) reports `found: true, mutated: false`.
  public struct MLSMutationApplyResult: Sendable, Equatable {
    /// `true` if the target message row exists locally — the mutation was
    /// evaluated (applied or silently dropped). `false` only when the target
    /// doesn't exist yet, in which case the caller should park an orphan.
    public let found: Bool
    /// `true` only if the mutation was actually applied (the row was
    /// rewritten). `false` when the target exists but the mutation was
    /// silently dropped.
    public let mutated: Bool

    public init(found: Bool, mutated: Bool) {
      self.found = found
      self.mutated = mutated
    }

    /// Target row doesn't exist locally yet — caller should park an orphan.
    public static let notFound = MLSMutationApplyResult(found: false, mutated: false)
    /// Target exists and the mutation was applied — notify observers.
    public static let applied = MLSMutationApplyResult(found: true, mutated: true)
    /// Target exists but the mutation was silently dropped — do NOT notify.
    public static let dropped = MLSMutationApplyResult(found: true, mutated: false)
  }

  /// Apply an in-place edit (spec §5.7.2) to an already-persisted target message.
  ///
  /// Authorization: `editorUserDID` must equal the target's original sender DID,
  /// both DID-root-normalized (device-suffix stripped via
  /// `MLSCredentialBinding.credentialRootDID`) and case/whitespace-normalized —
  /// mirroring the sender-identity comparison used for reactions/roster elsewhere
  /// in this file. A mismatch is a silent no-op (no error), per spec.
  ///
  /// Last-writer-wins: an edit whose `editSeq` is <= the target's persisted
  /// `appliedEditSeq` is silently dropped.
  ///
  /// A tombstoned target can never be resurrected by a later-arriving edit — the
  /// edit is silently dropped.
  ///
  /// Idempotent: safe to call twice with the same edit (the second call is a no-op
  /// once `appliedEditSeq` has advanced past `editSeq`).
  ///
  /// - Returns: `.notFound` if the target does not exist yet locally (caller
  ///   should park the edit as an orphan — see `applyOrPersistOrphanedEdit`).
  ///   `.applied` if the edit was actually applied. `.dropped` if the target
  ///   exists but the edit was silently rejected (per the rules above) — the
  ///   caller must NOT notify observers in that case.
  @discardableResult
  public func applyEdit(
    conversationID: String,
    targetMessageID: String,
    newText: String,
    editorUserDID: String,
    editSeq: Int64,
    currentUserDID: String,
    context: MlsContext,
    database: MLSDatabase
  ) async throws -> MLSMutationApplyResult {
    let normalizedUserDID = normalizeDID(currentUserDID)
    let normalizedEditorDID = normalizeDID(MLSCredentialBinding.credentialRootDID(editorUserDID))

    return try await database.write { db -> MLSMutationApplyResult in
      guard
        let existing = try MLSMessageModel
          .filter(MLSMessageModel.Columns.messageID == targetMessageID)
          .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
          .fetchOne(db)
      else {
        return .notFound
      }

      // A tombstoned message has no text left to edit — delete always wins.
      if existing.isTombstone == 1 {
        self.logger.info(
          "[EDIT] Dropping edit for tombstoned message \(targetMessageID.prefix(16))...")
        return .dropped
      }

      let normalizedSenderDID = self.normalizeDID(
        MLSCredentialBinding.credentialRootDID(existing.senderID))
      guard normalizedSenderDID == normalizedEditorDID else {
        self.logger.warning(
          "[EDIT] Rejected — editor \(normalizedEditorDID.prefix(16))... != sender \(normalizedSenderDID.prefix(16))... for \(targetMessageID.prefix(16))..."
        )
        return .dropped
      }

      if let appliedSeq = existing.appliedEditSeq, editSeq <= appliedSeq {
        self.logger.info(
          "[EDIT] Dropping stale edit seq=\(editSeq) <= applied=\(appliedSeq) for \(targetMessageID.prefix(16))..."
        )
        return .dropped
      }

      let existingPayload = existing.decryptedPayload(context: context)
      // v1 is text-only: refuse to "edit" a non-text control message.
      guard existingPayload == nil || existingPayload?.messageType == .text else {
        self.logger.warning(
          "[EDIT] Target \(targetMessageID.prefix(16))... is not a text message — dropping edit")
        return .dropped
      }

      // Replace `text` only — embed is preserved unchanged (spec §5.7: "MUST NOT
      // alter the target message's existing embed").
      let updatedPayload = MLSMessagePayload.text(newText, embed: existingPayload?.embed)
      let payloadData = try updatedPayload.encodeToJSON()
      let encryptedWire = try MLSFieldEncryption.encrypt(
        context: context,
        conversationID: conversationID,
        plaintext: payloadData
      )
      // Best-effort HMAC re-seal against the current tail, mirroring
      // `savePayloadForMessage`'s UPDATE path for mid-chain rewrites.
      let prevHMAC = try Self.fetchLastEntryHMACSync(
        conversationID: conversationID,
        currentUserDID: normalizedUserDID,
        legacyUserDID: nil,
        db: db
      )
      let entryHMAC = try MLSFieldEncryption.computeHMAC(
        context: context,
        conversationID: conversationID,
        previousHMAC: prevHMAC,
        messageID: targetMessageID,
        payloadWire: encryptedWire
      )

      try db.execute(
        sql: """
          UPDATE MLSMessageModel
          SET payloadJSON = NULL,
              payloadEncrypted = ?,
              entryHMAC = ?,
              isEdited = 1,
              editedAt = ?,
              appliedEditSeq = ?
          WHERE messageID = ? AND currentUserDID = ?;
          """,
        arguments: [
          encryptedWire, entryHMAC, Date(), editSeq, targetMessageID, normalizedUserDID,
        ])

      self.logger.info("[EDIT] Applied edit seq=\(editSeq) to \(targetMessageID.prefix(16))...")
      return .applied
    }
  }

  /// Apply a tombstone/unsend (spec §5.8.2) to an already-persisted target message.
  ///
  /// Authorization: identical rule to `applyEdit` — `senderUserDID` must equal the
  /// target's original sender DID (DID-root-normalized). Mismatch is a silent no-op.
  ///
  /// Idempotent: a second delete for an already-tombstoned message is a no-op that
  /// still reports the target as existing (but does NOT report as mutated).
  ///
  /// - Returns: `.notFound` if the target does not exist yet locally (caller
  ///   should park as an orphan). `.applied` if the tombstone was actually
  ///   applied. `.dropped` if the target exists but the delete was silently
  ///   rejected or was already applied — the caller must NOT notify observers
  ///   in that case.
  @discardableResult
  public func applyTombstone(
    conversationID: String,
    targetMessageID: String,
    senderUserDID: String,
    currentUserDID: String,
    context: MlsContext,
    database: MLSDatabase
  ) async throws -> MLSMutationApplyResult {
    let normalizedUserDID = normalizeDID(currentUserDID)
    let normalizedApplierDID = normalizeDID(MLSCredentialBinding.credentialRootDID(senderUserDID))

    return try await database.write { db -> MLSMutationApplyResult in
      guard
        let existing = try MLSMessageModel
          .filter(MLSMessageModel.Columns.messageID == targetMessageID)
          .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
          .fetchOne(db)
      else {
        return .notFound
      }

      if existing.isTombstone == 1 {
        // Idempotent no-op.
        return .dropped
      }

      let normalizedSenderDID = self.normalizeDID(
        MLSCredentialBinding.credentialRootDID(existing.senderID))
      guard normalizedSenderDID == normalizedApplierDID else {
        self.logger.warning(
          "[DELETE] Rejected — applier \(normalizedApplierDID.prefix(16))... != sender \(normalizedSenderDID.prefix(16))... for \(targetMessageID.prefix(16))..."
        )
        return .dropped
      }

      // Blank the local plaintext/embed through the same encrypted path everything
      // else uses (spec §5.8.2 step 3).
      let blanked = MLSMessagePayload.text("")
      let payloadData = try blanked.encodeToJSON()
      let encryptedWire = try MLSFieldEncryption.encrypt(
        context: context,
        conversationID: conversationID,
        plaintext: payloadData
      )
      let prevHMAC = try Self.fetchLastEntryHMACSync(
        conversationID: conversationID,
        currentUserDID: normalizedUserDID,
        legacyUserDID: nil,
        db: db
      )
      let entryHMAC = try MLSFieldEncryption.computeHMAC(
        context: context,
        conversationID: conversationID,
        previousHMAC: prevHMAC,
        messageID: targetMessageID,
        payloadWire: encryptedWire
      )

      try db.execute(
        sql: """
          UPDATE MLSMessageModel
          SET payloadJSON = NULL,
              payloadEncrypted = ?,
              entryHMAC = ?,
              isTombstone = 1,
              deletedAt = ?
          WHERE messageID = ? AND currentUserDID = ?;
          """,
        arguments: [
          encryptedWire, entryHMAC, Int64(Date().timeIntervalSince1970 * 1000), targetMessageID,
          normalizedUserDID,
        ])

      // Remove all reactions attached to the target, including still-pending
      // orphaned reactions keyed to it (spec §5.8.2 step 3).
      _ =
        try MLSReactionModel
        .filter(MLSReactionModel.Columns.messageID == targetMessageID)
        .filter(MLSReactionModel.Columns.currentUserDID == normalizedUserDID)
        .deleteAll(db)
      _ =
        try MLSOrphanedReactionModel
        .filter(MLSOrphanedReactionModel.Columns.messageID == targetMessageID)
        .filter(MLSOrphanedReactionModel.Columns.currentUserDID == normalizedUserDID)
        .deleteAll(db)

      // A delete wins over any still-pending orphaned edit for the same target
      // (spec §5.8.2 "Interaction with edit") — it can never be shown again.
      _ =
        try MLSOrphanedMutationModel
        .filter(MLSOrphanedMutationModel.Columns.targetMessageID == targetMessageID)
        .filter(MLSOrphanedMutationModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSOrphanedMutationModel.Columns.mutationType == MLSMutationKind.edit.rawValue)
        .deleteAll(db)

      self.logger.info("[DELETE] Applied tombstone to \(targetMessageID.prefix(16))...")
      return .applied
    }
  }

  /// Park an edit/delete whose target does not exist locally yet (spec §5.7.2 /
  /// §5.8.2 step 1). Mirrors `saveOrphanedReaction`'s dedupe-then-insert shape.
  private func saveOrphanedMutation(
    targetMessageID: String,
    conversationID: String,
    currentUserDID: String,
    applierDID: String,
    kind: MLSMutationKind,
    newText: String?,
    mutationSeq: Int64,
    database: MLSDatabase
  ) async throws {
    let normalizedUserDID = normalizeDID(currentUserDID)
    let normalizedApplierDID = normalizeDID(MLSCredentialBinding.credentialRootDID(applierDID))

    try await database.write { db in
      let hasPendingDelete =
        try MLSOrphanedMutationModel
        .filter(MLSOrphanedMutationModel.Columns.targetMessageID == targetMessageID)
        .filter(MLSOrphanedMutationModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSOrphanedMutationModel.Columns.mutationType == MLSMutationKind.delete.rawValue)
        .fetchCount(db) > 0

      if kind == .edit {
        // A pending delete always wins — a later-arriving edit orphan can never
        // resurrect a message that will be tombstoned once its target adopts.
        guard !hasPendingDelete else {
          self.logger.info(
            "[ORPHAN] Dropping orphaned edit — pending delete already wins for \(targetMessageID.prefix(16))..."
          )
          return
        }

        // Keep only the latest (highest mutationSeq) pending edit per target+applier
        // so last-writer-wins holds even while still orphaned.
        let existingEdits =
          try MLSOrphanedMutationModel
          .filter(MLSOrphanedMutationModel.Columns.targetMessageID == targetMessageID)
          .filter(MLSOrphanedMutationModel.Columns.currentUserDID == normalizedUserDID)
          .filter(MLSOrphanedMutationModel.Columns.applierDID == normalizedApplierDID)
          .filter(MLSOrphanedMutationModel.Columns.mutationType == MLSMutationKind.edit.rawValue)
          .fetchAll(db)

        if let latest = existingEdits.max(by: { $0.mutationSeq < $1.mutationSeq }),
          latest.mutationSeq >= mutationSeq
        {
          self.logger.info(
            "[ORPHAN] Dropping stale orphaned edit seq=\(mutationSeq) <= pending=\(latest.mutationSeq) for \(targetMessageID.prefix(16))..."
          )
          return
        }
        for stale in existingEdits {
          try stale.delete(db)
        }
      } else {
        // Delete wins: discard any pending edit orphans for this target, and
        // dedupe against an already-pending delete (idempotent).
        _ =
          try MLSOrphanedMutationModel
          .filter(MLSOrphanedMutationModel.Columns.targetMessageID == targetMessageID)
          .filter(MLSOrphanedMutationModel.Columns.currentUserDID == normalizedUserDID)
          .filter(MLSOrphanedMutationModel.Columns.mutationType == MLSMutationKind.edit.rawValue)
          .deleteAll(db)
        _ =
          try MLSOrphanedMutationModel
          .filter(MLSOrphanedMutationModel.Columns.targetMessageID == targetMessageID)
          .filter(MLSOrphanedMutationModel.Columns.currentUserDID == normalizedUserDID)
          .filter(
            MLSOrphanedMutationModel.Columns.mutationType == MLSMutationKind.delete.rawValue
          )
          .deleteAll(db)
      }

      let orphan = MLSOrphanedMutationModel(
        targetMessageID: targetMessageID,
        conversationID: conversationID,
        currentUserDID: normalizedUserDID,
        applierDID: normalizedApplierDID,
        mutationType: kind.rawValue,
        newText: kind == .edit ? newText : nil,
        mutationSeq: mutationSeq
      )
      try orphan.insert(db)
      self.logger.info(
        "[ORPHAN] Saved orphaned \(kind.rawValue) for missing message \(targetMessageID.prefix(16))..."
      )
    }
  }

  /// Attempt to apply an edit now; if the target doesn't exist locally yet, park it
  /// as an orphan (spec §5.7.2 steps 1/5). Mirrors the FK-error-driven orphan
  /// fallback `saveReaction` uses for reactions, but check-then-park rather than
  /// insert-then-catch since edits mutate an existing row instead of inserting one.
  @discardableResult
  public func applyOrPersistOrphanedEdit(
    conversationID: String,
    targetMessageID: String,
    newText: String,
    editorUserDID: String,
    editSeq: Int64,
    currentUserDID: String,
    context: MlsContext,
    database: MLSDatabase
  ) async throws -> MLSMutationApplyResult {
    let result = try await applyEdit(
      conversationID: conversationID,
      targetMessageID: targetMessageID,
      newText: newText,
      editorUserDID: editorUserDID,
      editSeq: editSeq,
      currentUserDID: currentUserDID,
      context: context,
      database: database
    )
    if !result.found {
      try await saveOrphanedMutation(
        targetMessageID: targetMessageID,
        conversationID: conversationID,
        currentUserDID: currentUserDID,
        applierDID: editorUserDID,
        kind: .edit,
        newText: newText,
        mutationSeq: editSeq,
        database: database
      )
    }
    return result
  }

  /// Attempt to apply a delete now; if the target doesn't exist locally yet, park
  /// it as an orphan (spec §5.8.2 step 1).
  @discardableResult
  public func applyOrPersistOrphanedDelete(
    conversationID: String,
    targetMessageID: String,
    senderUserDID: String,
    deleteSeq: Int64,
    currentUserDID: String,
    context: MlsContext,
    database: MLSDatabase
  ) async throws -> MLSMutationApplyResult {
    let result = try await applyTombstone(
      conversationID: conversationID,
      targetMessageID: targetMessageID,
      senderUserDID: senderUserDID,
      currentUserDID: currentUserDID,
      context: context,
      database: database
    )
    if !result.found {
      try await saveOrphanedMutation(
        targetMessageID: targetMessageID,
        conversationID: conversationID,
        currentUserDID: currentUserDID,
        applierDID: senderUserDID,
        kind: .delete,
        newText: nil,
        mutationSeq: deleteSeq,
        database: database
      )
    }
    return result
  }

  /// Adopt any orphaned edit/delete mutations for a newly arrived message (spec
  /// §5.7.2 / §5.8.2 step 5). Call whenever a message row is first persisted
  /// locally — mirrors `adoptOrphansForMessage` (reactions).
  ///
  /// A pending delete always wins over pending edits, regardless of seq ordering
  /// (spec §5.8.2 "Interaction with edit"). Otherwise pending edits are applied in
  /// ascending `mutationSeq` order so last-writer-wins holds across the batch.
  @discardableResult
  public func adoptOrphanedMutationsForMessage(
    _ messageID: String,
    conversationID: String,
    currentUserDID: String,
    context: MlsContext,
    database: MLSDatabase
  ) async throws -> [AdoptedMutation] {
    let normalizedUserDID = normalizeDID(currentUserDID)

    let orphans = try await database.read { db in
      try MLSOrphanedMutationModel
        .filter(MLSOrphanedMutationModel.Columns.targetMessageID == messageID)
        .filter(MLSOrphanedMutationModel.Columns.currentUserDID == normalizedUserDID)
        .fetchAll(db)
    }
    guard !orphans.isEmpty else { return [] }

    var results: [AdoptedMutation] = []

    if let deleteOrphan = orphans.first(where: {
      $0.mutationType == MLSMutationKind.delete.rawValue
    }) {
      let applied = try await applyTombstone(
        conversationID: conversationID,
        targetMessageID: messageID,
        senderUserDID: deleteOrphan.applierDID,
        currentUserDID: currentUserDID,
        context: context,
        database: database
      )
      if applied.mutated {
        results.append(
          AdoptedMutation(kind: .delete, conversationID: conversationID, targetMessageID: messageID)
        )
      }
      try await database.write { db in
        _ =
          try MLSOrphanedMutationModel
          .filter(MLSOrphanedMutationModel.Columns.targetMessageID == messageID)
          .filter(MLSOrphanedMutationModel.Columns.currentUserDID == normalizedUserDID)
          .deleteAll(db)
      }
      if !results.isEmpty {
        logger.info("[ORPHAN-ADOPT] Adopted delete for message \(messageID.prefix(16))...")
      }
      return results
    }

    let edits = orphans
      .filter { $0.mutationType == MLSMutationKind.edit.rawValue }
      .sorted { $0.mutationSeq < $1.mutationSeq }

    for edit in edits {
      let applied = try await applyEdit(
        conversationID: conversationID,
        targetMessageID: messageID,
        newText: edit.newText ?? "",
        editorUserDID: edit.applierDID,
        editSeq: edit.mutationSeq,
        currentUserDID: currentUserDID,
        context: context,
        database: database
      )
      if applied.mutated {
        results.append(
          AdoptedMutation(kind: .edit, conversationID: conversationID, targetMessageID: messageID)
        )
      }
    }

    try await database.write { db in
      _ =
        try MLSOrphanedMutationModel
        .filter(MLSOrphanedMutationModel.Columns.targetMessageID == messageID)
        .filter(MLSOrphanedMutationModel.Columns.currentUserDID == normalizedUserDID)
        .deleteAll(db)
    }

    if !results.isEmpty {
      logger.info(
        "[ORPHAN-ADOPT] Adopted \(results.count) edit(s) for message \(messageID.prefix(16))...")
    }
    return results
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

  /// Atomic drain: fetch all pending messages for the conversation/user pair
  /// AND delete them from the pending table within a single write transaction.
  ///
  /// This is Phase D-Swift's idempotency fix for the catch-up buffer (Task
  /// D-S.2). The previous flow (`flushBufferedMessages` -> caller iterates ->
  /// caller calls `processServerMessage` which eventually calls
  /// `recordMessageProcessed` which calls `removePendingMessage`) leaves rows
  /// in the pending table when intermediate steps fail or return early, so
  /// the next sync pass re-fetches the same messages and re-buffers them,
  /// creating a tight loop.
  ///
  /// `drainPendingMessages` returns the same list `getAllPendingMessages`
  /// would, but is guaranteed to leave the pending table empty for that
  /// (conversation, user) pair on return. Caller failures during processing
  /// of returned messages now produce typed errors rather than re-population
  /// of the buffer.
  public func drainPendingMessages(
    conversationID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async throws -> [MLSPendingMessageModel] {
    let normalizedUserDID = normalizeDID(currentUserDID)

    return try await database.write { db in
      let pending = try MLSPendingMessageModel
        .filter(MLSPendingMessageModel.Columns.conversationID == conversationID)
        .filter(MLSPendingMessageModel.Columns.currentUserDID == normalizedUserDID)
        .order(MLSPendingMessageModel.Columns.sequenceNumber)
        .fetchAll(db)

      if !pending.isEmpty {
        _ = try MLSPendingMessageModel
          .filter(MLSPendingMessageModel.Columns.conversationID == conversationID)
          .filter(MLSPendingMessageModel.Columns.currentUserDID == normalizedUserDID)
          .deleteAll(db)
      }

      return pending
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
