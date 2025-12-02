//
//  MLSCoreContext.swift
//  CatbirdMLSCore
//
//  Core MLS context manager with decryption and storage capabilities
//

import Foundation
import GRDB
@_exported import MLSFFI
import OSLog

/// Manages MLS contexts and handles message decryption with database storage
/// Thread-safe singleton providing per-user context isolation
public actor MLSCoreContext {

  // MARK: - Singleton

  public static let shared = MLSCoreContext()

  // MARK: - Properties

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSCoreContext")

  /// Per-user MLS context cache
  private var contexts: [String: MlsContext] = [:]

  private struct DecryptionOutcome {
    let plaintext: String
    let embed: MLSEmbedData?
  }

  /// Deduplicate concurrent decrypt attempts for the same messageID
  private var inFlightDecryptions: [String: Task<DecryptionOutcome, Error>] = [:]

  /// Storage manager for database operations
  private let storage = MLSStorage.shared

  /// Keychain access group for App Group sharing
  private var keychainAccessGroup: String? {
    #if targetEnvironment(simulator)
    return nil
    #else
    return "group.blue.catbird.shared"
    #endif
  }

  /// Storage directory for MLS database files
  private var storageDirectory: URL {
    // Use the shared App Group container so the main app and notification extension
    // operate on the same MLS state and avoid divergent ratchets.
    if let shared = FileManager.default.containerURL(forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared") {
      return shared.appendingPathComponent("CatbirdMLS", isDirectory: true)
    }

    // Simulator / fallback
    let fileManager = FileManager.default
    guard let appSupport = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask).first else {
      fatalError("Could not access Application Support directory")
    }
    return appSupport.appendingPathComponent("CatbirdMLS", isDirectory: true)
  }

  // MARK: - Initialization

  private init() {
    logger.info("MLSCoreContext initialized")
    Task {
      await configureKeychainAccess()
    }
  }

  // MARK: - Configuration

  /// Configure keychain access for App Group sharing
  private func configureKeychainAccess() async {
    if let accessGroup = keychainAccessGroup {
      MLSKeychainManager.shared.accessGroup = accessGroup
      logger.info("Configured keychain access group: \(accessGroup)")
    } else {
      logger.info("Running in simulator - using default keychain access")
    }
  }

  // MARK: - Context Management

  /// Get or create MLS context for a user
  /// - Parameter userDid: User's decentralized identifier
  /// - Returns: MLS context for the user
  /// - Throws: MLSError if context creation fails
  public func getContext(for userDid: String) async throws -> MlsContext {
    if let existingContext = contexts[userDid] {
      return existingContext
    }

    logger.info("Creating new MLS context for user: \(userDid, privacy: .private)")

    // Create storage path for this user
    let storagePath = try createStoragePath(for: userDid)

    // Get or create encryption key
    let encryptionKey = try await getEncryptionKey(for: userDid)

    // Create keychain access bridge
    let keychainBridge = MLSKeychainAccessBridge()

    let newContext = try MlsContext(
      storagePath: storagePath,
      encryptionKey: encryptionKey,
      keychain: keychainBridge
    )

    contexts[userDid] = newContext
    logger.info("Created MLS context for user: \(userDid, privacy: .private)")

    return newContext
  }

  // MARK: - Helper Methods

  private func createStoragePath(for userDid: String) throws -> String {
    // Create user-specific directory
    let userHash = userDid.data(using: .utf8)!.sha256Hex
    let directory = storageDirectory.appendingPathComponent(userHash)

    // Create directory if needed
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)

    // Return database path
    return directory.appendingPathComponent("mls-state.db").path
  }

  private func getEncryptionKey(for userDid: String) async throws -> String {
    // Use SQLCipher encryption manager to get/create key
    let keyData = try await MLSSQLCipherEncryption.shared.getOrCreateKey(for: userDid)
    return keyData.base64EncodedString()
  }

  /// Remove context for a user (e.g., on logout)
  /// - Parameter userDid: User's decentralized identifier
  public func removeContext(for userDid: String) {
    contexts.removeValue(forKey: userDid)
    logger.info("Removed MLS context for user: \(userDid, privacy: .private)")
  }

  /// Clear all contexts
  public func clearAllContexts() {
    contexts.removeAll()
    logger.info("Cleared all MLS contexts")
  }

  // MARK: - Decryption Coordination

  private func decryptOnce(
    userDid: String,
    groupId: Data,
    ciphertext: Data,
    conversationID: String,
    messageID: String,
    epoch: Int64?,
    sequenceNumber: Int64?,
    senderID: String?
  ) async throws -> DecryptionOutcome {
    if let inFlight = inFlightDecryptions[messageID] {
      logger.debug("[DECRYPT] Awaiting in-flight decryption for message: \(messageID)")
      return try await inFlight.value
    }

    let task = Task { [weak self] in
      guard let self else { throw CancellationError() }
      return try await self.performDecryption(
        userDid: userDid,
        groupId: groupId,
        ciphertext: ciphertext,
        conversationID: conversationID,
        messageID: messageID,
        epoch: epoch,
        sequenceNumber: sequenceNumber,
        senderID: senderID
      )
    }

    inFlightDecryptions[messageID] = task

    do {
      let outcome = try await task.value
      inFlightDecryptions.removeValue(forKey: messageID)
      return outcome
    } catch {
      inFlightDecryptions.removeValue(forKey: messageID)
      throw error
    }
  }

  private func performDecryption(
    userDid: String,
    groupId: Data,
    ciphertext: Data,
    conversationID: String,
    messageID: String,
    epoch: Int64?,
    sequenceNumber: Int64?,
    senderID: String?
  ) async throws -> DecryptionOutcome {
    // Wrap database operations in Task to prevent priority inversion
    // when called from high-priority contexts like @MainActor
    return try await Task(priority: .userInitiated) {
      let database = try await MLSGRDBManager.shared.getDatabasePool(for: userDid)

      // Check cache first
      if let cachedPlaintext = try await storage.fetchPlaintextForMessage(messageID,
        currentUserDID: userDid,
        database: database
      ) {
        let cachedEmbed = try await storage.fetchEmbedForMessage(messageID,
          currentUserDID: userDid,
          database: database
        )
        logger.debug("[DECRYPT] Cache hit for \(messageID) - skipping MLS ratchet decrypt")
        return DecryptionOutcome(plaintext: cachedPlaintext, embed: cachedEmbed)
      }

      // Get context (must be done in actor context)
      let context = try await getContext(for: userDid)

      logger.debug("[DECRYPT] Performing MLS decrypt for message: \(messageID)")

      let result = try context.decryptMessage(groupId: groupId, ciphertext: ciphertext)
      let payloadData = result.plaintext

      let payload = try? MLSMessagePayload.decodeFromJSON(payloadData)
      let plaintext = payload?.text ?? (String(data: payloadData, encoding: .utf8) ?? "")
      let embedData = payload?.embed

      let actualEpoch = epoch ?? Int64(result.epoch)
      let actualSeq = sequenceNumber ?? Int64(result.sequenceNumber)
      let actualSender = senderID ?? "unknown"

      logger.info(
        "[DECRYPT] Decrypted: epoch=\(actualEpoch), seq=\(actualSeq), sender=\(actualSender == "unknown" ? "unknown" : actualSender.prefix(20)), hasEmbed=\(embedData != nil), \(plaintext.count) chars"
      )

      try await storage.ensureConversationExists(userDID: userDid,
        conversationID: conversationID,
        groupID: groupId.hexEncodedString(),
        database: database
      )

      let embedJSON = try embedData?.toJSONData()

      try await MLSStorageHelpers.savePlaintext(
        in: database,
        messageID: messageID,
        conversationID: conversationID,
        currentUserDID: userDid,
        plaintext: plaintext,
        senderID: actualSender,
        embedDataJSON: embedJSON,
        epoch: actualEpoch,
        sequenceNumber: actualSeq
      )

      logger.info("[DECRYPT] Stored plaintext for message: \(messageID)")

      return DecryptionOutcome(plaintext: plaintext, embed: embedData)
    }.value
  }

  // MARK: - Main Decryption Method

  /// Decrypt MLS message and store plaintext in database
  ///
  /// This is the primary decryption method that:
  /// 1. Decrypts the ciphertext using the MLS context
  /// 2. Extracts epoch and sequence number from DecryptResult
  /// 3. Stores the plaintext in the database with proper metadata
  ///
  /// - Parameters:
  ///   - userDid: User's decentralized identifier
  ///   - groupId: MLS group identifier
  ///   - ciphertext: Encrypted message data
  ///   - conversationID: Conversation identifier for database storage
  ///   - messageID: Unique message identifier for database storage
  ///   - epoch: Optional epoch number from server (when nil, extracted from decryption)
  ///   - sequenceNumber: Optional sequence number from server (when nil, extracted from decryption)
  ///   - senderID: Optional sender DID from server (when nil, stored as "unknown" until main app processes)
  /// - Returns: Decrypted plaintext string
  /// - Throws: MLSError or storage errors if decryption or storage fails
  /// - Note: Providing server metadata ensures accuracy. Without it, sender will be "unknown" until UPDATE.
  public func decryptAndStore(
    userDid: String,
    groupId: Data,
    ciphertext: Data,
    conversationID: String,
    messageID: String,
    epoch: Int64? = nil,
    sequenceNumber: Int64? = nil,
    senderID: String? = nil
  ) async throws -> String {
    // Capture logger before async work
    let logger = self.logger

    logger.debug(
      "[DECRYPT] Starting decryption: message=\(messageID), hasMetadata=\(epoch != nil && sequenceNumber != nil && senderID != nil)"
    )

    do {
      // decryptOnce calls performDecryption which already uses Task.detached
      let outcome = try await decryptOnce(
        userDid: userDid,
        groupId: groupId,
        ciphertext: ciphertext,
        conversationID: conversationID,
        messageID: messageID,
        epoch: epoch,
        sequenceNumber: sequenceNumber,
        senderID: senderID
      )

      return outcome.plaintext

    } catch {
      logger.error("[DECRYPT] Failed to decrypt message: \(error.localizedDescription)")
      throw error
    }
  }

  // MARK: - Advanced Decryption with Embeds

  /// Decrypt MLS message with embed data and store in database
  ///
  /// Enhanced version that handles rich embed data (quotes, links, GIFs)
  ///
  /// - Parameters:
  ///   - userDid: User's decentralized identifier
  ///   - groupId: MLS group identifier
  ///   - ciphertext: Encrypted message data
  ///   - conversationID: Conversation identifier for database storage
  ///   - messageID: Unique message identifier for database storage
  /// - Returns: Tuple of (plaintext, embedData)
  /// - Throws: MLSError or storage errors if decryption or storage fails
  public func decryptAndStoreWithEmbeds(
    userDid: String,
    groupId: Data,
    ciphertext: Data,
    conversationID: String,
    messageID: String,
    epoch: Int64? = nil,
    sequenceNumber: Int64? = nil,
    senderID: String? = nil
  ) async throws -> (plaintext: String, embed: MLSEmbedData?) {
    // Capture logger before async work
    let logger = self.logger

    logger.debug(
      "[DECRYPT+EMBED] Starting decryption: message=\(messageID), hasMetadata=\(epoch != nil && sequenceNumber != nil && senderID != nil)"
    )

    do {
      // decryptOnce calls performDecryption which already uses Task.detached
      let outcome = try await decryptOnce(
        userDid: userDid,
        groupId: groupId,
        ciphertext: ciphertext,
        conversationID: conversationID,
        messageID: messageID,
        epoch: epoch,
        sequenceNumber: sequenceNumber,
        senderID: senderID
      )

      return (outcome.plaintext, outcome.embed)

    } catch {
      logger.error("[DECRYPT+EMBED] Failed to decrypt message: \(error.localizedDescription)")
      throw error
    }
  }

  // MARK: - Batch Decryption

  /// Decrypt multiple messages efficiently
  ///
  /// - Parameters:
  ///   - userDid: User's decentralized identifier
  ///   - groupId: MLS group identifier
  ///   - messages: Array of (ciphertext, conversationID, messageID) tuples
  /// - Returns: Array of decrypted plaintext strings
  /// - Throws: MLSError or storage errors
  public func decryptBatch(
    userDid: String,
    groupId: Data,
    messages: [(ciphertext: Data, conversationID: String, messageID: String)]
  ) async throws -> [String] {
    // Capture logger before async work
    let logger = self.logger

    logger.info("[DECRYPT-BATCH] Decrypting \(messages.count) messages")

    var plaintexts: [String] = []

    // Each decryptAndStore call uses Task.detached internally via performDecryption
    // This prevents priority inversion for the entire batch
    for (ciphertext, conversationID, messageID) in messages {
      do {
        let plaintext = try await decryptAndStore(
          userDid: userDid,
          groupId: groupId,
          ciphertext: ciphertext,
          conversationID: conversationID,
          messageID: messageID
        )
        plaintexts.append(plaintext)
      } catch {
        logger.error(
          "[DECRYPT-BATCH] Failed to decrypt message \(messageID): \(error.localizedDescription)")
        throw error
      }
    }

    logger.info("[DECRYPT-BATCH] Successfully decrypted \(plaintexts.count) messages")

    return plaintexts
  }

  // MARK: - Context Information

  /// Get current epoch for a group
  /// - Parameters:
  ///   - userDid: User's decentralized identifier
  ///   - groupId: MLS group identifier
  /// - Returns: Current epoch number
  /// - Throws: MLSError if context not found
    public func getCurrentEpoch(userDid: String, groupId: Data) async throws -> UInt64 {
        let context = try await getContext(for: userDid)
    return try context.getEpoch(groupId: groupId)
  }

  /// Get member count for a group
  /// - Parameters:
  ///   - userDid: User's decentralized identifier
  ///   - groupId: MLS group identifier
  /// - Returns: Number of members in the group
  /// - Throws: MLSError if context not found
    public func getMemberCount(userDid: String, groupId: Data) async throws -> Int {
        let context = try await getContext(for: userDid)
    return try Int(context.getGroupMemberCount(groupId: groupId))
  }

  // MARK: - Private Helpers

  /// Extract sender DID from MLS credential data
  /// - Parameter credential: MLS credential containing sender identity
  /// - Returns: Sender DID string
  /// - Throws: MLSError if credential cannot be decoded or DID format is invalid
  private func extractSenderDID(from credential: CredentialData) throws -> String {
    guard let didString = String(data: credential.identity, encoding: .utf8) else {
      logger.error("❌ Failed to decode credential identity as UTF-8")
      throw MLSError.invalidCredential("Cannot decode sender credential")
    }

    guard didString.starts(with: "did:") else {
      logger.error("❌ Invalid DID format in credential: \(didString)")
      throw MLSError.invalidCredential("Invalid DID format: \(didString)")
    }

    return didString
  }
}
