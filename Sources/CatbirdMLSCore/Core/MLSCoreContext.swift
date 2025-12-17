//
//  MLSCoreContext.swift
//  CatbirdMLSCore
//
//  Core MLS context manager with decryption and storage capabilities
//
//  CRITICAL FIX (2024-12-15): Ephemeral Database Access for Notifications
//
//  Problem: When a push notification arrives for User B while User A is active,
//  the notification handler tried to switch the active database from A→B.
//  This triggers a WAL checkpoint on User A's database, which fails with
//  "database locked" if User A has active read/write operations.
//
//  The checkpoint failure caused:
//  1. Epoch advancement to fail saving to disk
//  2. Next message encrypted for new epoch can't be decrypted
//  3. Permanent "ratchet desync" requiring group rejoin
//
//  Solution: New `decryptForNotification()` method uses `getEphemeralDatabasePool()`
//  which opens User B's database WITHOUT checkpointing User A's database.
//  This allows concurrent access to multiple user databases.
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

  /// Per-user state version at time of context creation/reload.
  /// Used to detect when NSE has advanced the ratchet and we need to reload.
  private var contextVersions: [String: Int] = [:]

  private struct DecryptionOutcome {
    let plaintext: String
    let embed: MLSEmbedData?
    let senderDID: String?  // Extracted from MLS credential
  }
  
  /// Result of notification decryption with sender info for rich notifications
  public struct NotificationDecryptResult: Sendable {
    public let plaintext: String
    public let senderDID: String?
    
    public init(plaintext: String, senderDID: String?) {
      self.plaintext = plaintext
      self.senderDID = senderDID
    }
  }

  /// Deduplicate concurrent decrypt attempts for the same messageID
  private var inFlightDecryptions: [String: Task<DecryptionOutcome, Error>] = [:]

  /// Storage manager for database operations
  private let storage = MLSStorage.shared

  /// Keychain access group for shared Keychain access between app + extensions.
  /// Resolve the fully-qualified (TeamID-prefixed) group from entitlements to avoid -34018.
  private var keychainAccessGroup: String? {
    #if targetEnvironment(simulator)
      return nil
    #else
      return MLSKeychainManager.resolvedAccessGroup(suffix: "blue.catbird.shared")
    #endif
  }

  /// Storage directory for MLS database files
  /// CRITICAL: Must match the path used by MLSClient in the main app
  private var storageDirectory: URL {
    // Use the shared App Group container so the main app and notification extension
    // operate on the same MLS state and avoid divergent ratchets.
    if let shared = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared")
    {
      return shared.appendingPathComponent("mls-state", isDirectory: true)
    }

    // Simulator / fallback
    let fileManager = FileManager.default
    guard
      let appSupport = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask)
        .first
    else {
      fatalError("Could not access Application Support directory")
    }
    return appSupport.appendingPathComponent("mls-state", isDirectory: true)
  }

  // MARK: - Initialization

  private init() {
    logger.info("MLSCoreContext initialized")
    Task {
      await configureKeychainAccess()
    }
  }

  // MARK: - Configuration

  /// Track whether keychain has been configured
  private var keychainConfigured = false

  /// Ensure keychain access group is configured (idempotent)
  private func ensureKeychainConfigured() async {
    guard !keychainConfigured else { return }
    await configureKeychainAccess()
    keychainConfigured = true
  }

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

  /// Get or create MLS context for a user.
  ///
  /// This method includes **monotonic version checking** to detect when NSE has
  /// advanced the MLS ratchet on disk. If the disk version is newer than our
  /// cached context version, the context is automatically reloaded.
  ///
  /// - Parameter userDid: User's decentralized identifier
  /// - Returns: MLS context for the user
  /// - Throws: MLSError if context creation fails
  public func getContext(for userDid: String) async throws -> MlsContext {
    // ═══════════════════════════════════════════════════════════════════════════
    // MONOTONIC VERSION CHECK: Detect NSE ratchet advancement
    // ═══════════════════════════════════════════════════════════════════════════
    // Before returning a cached context, check if NSE has advanced the state.
    // If diskVersion > memoryVersion, our cached context is stale and must be reloaded.
    // This is faster than waiting for Darwin notifications.
    // ═══════════════════════════════════════════════════════════════════════════
    if let existingContext = contexts[userDid] {
      let memoryVersion = contextVersions[userDid] ?? 0
      let diskVersion = MLSStateVersionManager.shared.getDiskVersion(for: userDid)

      if diskVersion > memoryVersion {
        logger.warning("🔄 [CONTEXT] Stale context detected for \(userDid.prefix(20))...: disk=\(diskVersion), memory=\(memoryVersion)")
        logger.info("   NSE advanced the ratchet - reloading context from disk")

        // Close the stale context
        try? existingContext.flushAndPrepareClose()
        contexts.removeValue(forKey: userDid)
        contextVersions.removeValue(forKey: userDid)

        // Fall through to create fresh context below
      } else {
        return existingContext
      }
    }

    // Ensure keychain is configured before creating context
    // This fixes a race condition where context creation could happen
    // before the async configureKeychainAccess() completes
    await ensureKeychainConfigured()

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

    // Track the current disk version at context creation time
    let currentDiskVersion = MLSStateVersionManager.shared.getDiskVersion(for: userDid)
    contexts[userDid] = newContext
    contextVersions[userDid] = currentDiskVersion

    // Sync the version manager's cache
    MLSStateVersionManager.shared.syncLastKnownVersion(for: userDid)

    logger.info("Created MLS context for user: \(userDid, privacy: .private) at version \(currentDiskVersion)")

    return newContext
  }

  /// Force reload MLS context from disk.
  ///
  /// Call this when you know the disk state has changed (e.g., after Darwin notification).
  /// This bypasses the version check and always creates a fresh context.
  ///
  /// - Parameter userDid: User's decentralized identifier
  /// - Throws: MLSError if context creation fails
  public func reloadContext(for userDid: String) async throws {
    logger.info("🔄 [CONTEXT] Force reloading context for \(userDid.prefix(20))...")

    // Close existing context if any
    if let existingContext = contexts.removeValue(forKey: userDid) {
      try? existingContext.flushAndPrepareClose()
    }
    contextVersions.removeValue(forKey: userDid)

    // Create fresh context (this will update version tracking)
    _ = try await getContext(for: userDid)
  }

  // MARK: - Helper Methods

  private func createStoragePath(for userDid: String) throws -> String {
    // CRITICAL: Use the exact same hashing scheme as MLSClient in the main app
    // This ensures the NSE can find and decrypt from the same database

    // Create directory if needed
    try FileManager.default.createDirectory(at: storageDirectory, withIntermediateDirectories: true)

    // Hash the DID using base64 (matching MLSClient.createContext)
    let didHash =
      userDid.data(using: .utf8)?.base64EncodedString()
      .replacingOccurrences(of: "/", with: "_")
      .replacingOccurrences(of: "+", with: "-")
      .replacingOccurrences(of: "=", with: "")
      .prefix(64) ?? "default"

    // Return database path (matching MLSClient: {didHash}.db)
    return storageDirectory.appendingPathComponent("\(didHash).db").path
  }

  private func getEncryptionKey(for userDid: String) async throws -> String {
    // CRITICAL: Use the exact same keychain manager and encoding as MLSClient
    // MLSClient uses MLSKeychainManager.getOrCreateEncryptionKey() and hexEncodedString()
    // Using a different source or encoding will fail to decrypt the database
    let keyData = try MLSKeychainManager.shared.getOrCreateEncryptionKey(forUserDID: userDid)
    return keyData.hexEncodedString()
  }

  /// Remove context for a user (e.g., on logout)
  /// - Parameter userDid: User's decentralized identifier
  public func removeContext(for userDid: String) {
    if let context = contexts.removeValue(forKey: userDid) {
      try? context.flushAndPrepareClose()
    }
    contextVersions.removeValue(forKey: userDid)
    logger.info("Removed MLS context for user: \(userDid, privacy: .private)")
  }

  /// Clear all contexts
  public func clearAllContexts() {
    for (_, context) in contexts {
      try? context.flushAndPrepareClose()
    }
    contexts.removeAll()
    contextVersions.removeAll()
    logger.info("Cleared all MLS contexts")
  }

  // MARK: - Context Validation for Account Switching

  /// Ensure context is available for a specific user, clearing stale contexts for other users
  ///
  /// CRITICAL FIX: This method handles account switching correctly by:
  /// 1. Removing cached contexts for OTHER users (prevents cross-account decryption)
  /// 2. Creating a fresh context for the requested user if needed
  ///
  /// This is essential for the Notification Service Extension which may have stale
  /// contexts cached from a previous app session with a different user.
  ///
  /// - Parameter userDid: User's decentralized identifier
  /// - Throws: MLSError if context creation fails
  /// - Note: Safe to call from NSE - does not depend on AppState
  public func ensureContext(for userDid: String) async throws {
    // Normalize the user DID for consistent matching
    let normalizedUserDid = userDid.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()

    // Check if we have any contexts for OTHER users (stale from account switching)
    let staleContextUsers = contexts.keys.filter { key in
      key.lowercased() != normalizedUserDid
    }

    // Remove stale contexts to prevent cross-account decryption issues
    if !staleContextUsers.isEmpty {
      for staleUser in staleContextUsers {
        if let context = contexts.removeValue(forKey: staleUser) {
          try? context.flushAndPrepareClose()
        }
        contextVersions.removeValue(forKey: staleUser)
        logger.warning(
          "🔄 [ensureContext] Removed stale context for user: \(staleUser.prefix(20), privacy: .private)..."
        )
      }
      logger.info(
        "🔄 [ensureContext] Cleared \(staleContextUsers.count) stale context(s) for account switch")
    }

    // Check if we already have a context for the correct user
    // Note: contexts dictionary keys may not be normalized, so check both forms
    let existingContext =
      contexts[userDid]
      ?? contexts.first(where: {
        $0.key.lowercased() == normalizedUserDid
      })?.value

    if existingContext != nil {
      // Even if context exists, check version staleness
      // (getContext handles this, but we call it to ensure version check)
      _ = try await getContext(for: userDid)
      logger.debug(
        "✅ [ensureContext] Context validated for user: \(userDid.prefix(20), privacy: .private)..."
      )
      return
    }

    // Create fresh context for the requested user
    logger.info(
      "📝 [ensureContext] Creating fresh context for user: \(userDid.prefix(20), privacy: .private)..."
    )
    _ = try await getContext(for: userDid)
    logger.info(
      "✅ [ensureContext] Context created for user: \(userDid.prefix(20), privacy: .private)...")
  }

  /// Check if a context exists for a specific user without creating one
  /// - Parameter userDid: User's decentralized identifier
  /// - Returns: true if a valid context exists for this user
  public func hasContext(for userDid: String) -> Bool {
    let normalizedUserDid = userDid.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return contexts.keys.contains { $0.lowercased() == normalizedUserDid }
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
    senderID: String?,
    useEphemeralAccess: Bool = false
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
        senderID: senderID,
        useEphemeralAccess: useEphemeralAccess
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
    senderID: String?,
    useEphemeralAccess: Bool = false
  ) async throws -> DecryptionOutcome {
    // Wrap database operations in Task to prevent priority inversion
    // when called from high-priority contexts like @MainActor
    return try await Task(priority: .userInitiated) {
      // CRITICAL FIX: Use ephemeral database access for non-active users
      // This prevents "database locked" errors during notification decryption
      // when the notification is for a different user than the active UI user
      let database: DatabasePool
      if useEphemeralAccess {
        logger.debug("[DECRYPT] Using EPHEMERAL database access for user: \(userDid.prefix(20))")
        database = try await MLSGRDBManager.shared.getEphemeralDatabasePool(for: userDid)
      } else {
        database = try await MLSGRDBManager.shared.getDatabasePool(for: userDid)
      }

      // ═══════════════════════════════════════════════════════════════════════════
      // CRITICAL: Second Idempotency Check (Defense in Depth)
      // ═══════════════════════════════════════════════════════════════════════════
      // This is the second cache check - we already checked before acquiring the lock,
      // but NSE might have completed while we were waiting for the lock.
      // This is our last chance to avoid the SecretReuseError.
      // ═══════════════════════════════════════════════════════════════════════════
      if let cachedPlaintext = try await storage.fetchPlaintextForMessage(
        messageID,
        currentUserDID: userDid,
        database: database
      ) {
        let cachedEmbed = try await storage.fetchEmbedForMessage(
          messageID,
          currentUserDID: userDid,
          database: database
        )
        let cachedSender = try await storage.fetchSenderForMessage(
          messageID,
          currentUserDID: userDid,
          database: database
        )
        logger.info("✅ [DECRYPT] IDEMPOTENCY HIT (post-lock): Message \(messageID.prefix(16))... already decrypted")
        logger.info("   NSE completed while we waited for lock - using cached result")
        return DecryptionOutcome(plaintext: cachedPlaintext, embed: cachedEmbed, senderDID: cachedSender)
      }

      // Get context (must be done in actor context)
      let context = try await getContext(for: userDid)

      // CRITICAL FIX: Strip padding envelope before MLS deserialization
      // Messages may be padded to bucket sizes (512, 1024, etc.) for traffic analysis resistance.
      // Format: [4-byte BE length][actual MLS ciphertext][zero padding...]
      let actualCiphertext = MLSPaddingUtility.stripPaddingIfPresent(ciphertext)

      if actualCiphertext.count != ciphertext.count {
        logger.info(
          "[DECRYPT] Stripped padding: \(ciphertext.count) -> \(actualCiphertext.count) bytes")
      }

      logger.debug("[DECRYPT] Performing MLS decrypt for message: \(messageID)")

      let result: DecryptResult
      do {
        result = try context.decryptMessage(groupId: groupId, ciphertext: actualCiphertext)
      } catch {
        let desc = error.localizedDescription
        if desc.contains("SecretReuseError") || desc.contains("SecretTreeError") {
          // Duplicate processing: attempt to use cache instead of treating as state desync.
          if let cachedPlaintext = try? await storage.fetchPlaintextForMessage(
            messageID,
            currentUserDID: userDid,
            database: database
          ) {
            let cachedEmbed = try? await storage.fetchEmbedForMessage(
              messageID,
              currentUserDID: userDid,
              database: database
            )
            let cachedSender = try? await storage.fetchSenderForMessage(
              messageID,
              currentUserDID: userDid,
              database: database
            )
            logger.info("✅ [DECRYPT] SecretReuseError → cache hit; skipping MLS decrypt")
            return DecryptionOutcome(plaintext: cachedPlaintext, embed: cachedEmbed, senderDID: cachedSender)
          }

          logger.warning("⚠️ [DECRYPT] SecretReuseError but no cached plaintext; skipping")
          throw MLSError.secretReuseSkipped(messageID: messageID)
        }
        throw error
      }

      let payloadData = result.plaintext

      let payload = try? MLSMessagePayload.decodeFromJSON(payloadData)
      let plaintext = payload?.text ?? (String(data: payloadData, encoding: .utf8) ?? "")
      let embedData = payload?.embed

      let actualEpoch = epoch ?? Int64(result.epoch)
      let actualSeq = sequenceNumber ?? Int64(result.sequenceNumber)
      
      // Extract sender DID from MLS credential (cryptographically authenticated)
      // This is more reliable than the passed-in senderID which may be "unknown"
      let actualSender: String
      if let senderDID = String(data: result.senderCredential.identity, encoding: .utf8),
         senderDID.starts(with: "did:") {
        actualSender = senderDID
        logger.debug("[DECRYPT] Sender extracted from MLS credential: \(senderDID.prefix(24))...")
      } else if let fallbackSender = senderID, fallbackSender != "unknown" {
        actualSender = fallbackSender
        logger.debug("[DECRYPT] Using fallback sender from parameter: \(fallbackSender.prefix(24))...")
      } else {
        actualSender = "unknown"
        logger.debug("[DECRYPT] Unable to extract sender from credential, using 'unknown'")
      }

      logger.info(
        "[DECRYPT] Decrypted: epoch=\(actualEpoch), seq=\(actualSeq), sender=\(actualSender == "unknown" ? "unknown" : actualSender.prefix(24)), hasEmbed=\(embedData != nil), \(plaintext.count) chars"
      )

      // ═══════════════════════════════════════════════════════════════════════════════
      // 🔒 CRITICAL FIX: NSE Foreign Key Race Condition
      // ═══════════════════════════════════════════════════════════════════════════════
      //
      // PROBLEM: When NSE decrypts a message for a new conversation (just joined via
      // Welcome), the conversation record may not exist in the SQLCipher database yet.
      // The message INSERT fails with "FOREIGN KEY constraint failed", and the decrypted
      // plaintext is LOST. When the main app opens and advances the epoch via Commit,
      // the old-epoch message can no longer be decrypted (Forward Secrecy).
      //
      // SOLUTION: Use ensureConversationExistsOrPlaceholder which creates a minimal
      // placeholder conversation record if one doesn't exist. This satisfies the FK
      // constraint, allowing the message to be saved. The main app heals the placeholder
      // with full metadata during the next listConvos sync.
      //
      // ═══════════════════════════════════════════════════════════════════════════════
      _ = try await storage.ensureConversationExistsOrPlaceholder(
        userDID: userDid,
        conversationID: conversationID,
        groupID: groupId.hexEncodedString(),
        senderDID: actualSender != "unknown" ? actualSender : nil,
        database: database
      )

      let embedJSON = try embedData?.toJSONData()

      // Attempt to save plaintext with FK-error recovery
      do {
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
      } catch let error as DatabaseError where error.resultCode == .SQLITE_CONSTRAINT {
        // FK constraint failed despite ensureConversationExistsOrPlaceholder
        // This can happen in race conditions - retry once with forced placeholder
        logger.warning("⚠️ [FK-RECOVERY] FK constraint failed, forcing placeholder creation...")

        _ = try await storage.ensureConversationExistsOrPlaceholder(
          userDID: userDid,
          conversationID: conversationID,
          groupID: groupId.hexEncodedString(),
          senderDID: actualSender != "unknown" ? actualSender : nil,
          database: database
        )

        // Retry the save
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

        logger.info("✅ [FK-RECOVERY] Plaintext saved after placeholder creation")
      }

      logger.info("[DECRYPT] Stored plaintext for message: \(messageID)")

      // ═══════════════════════════════════════════════════════════════════════════════
      // MONOTONIC VERSION INCREMENT: Signal state change to other processes
      // ═══════════════════════════════════════════════════════════════════════════════
      // After successful decryption, the MLS ratchet has advanced. Increment the
      // state version so the main app (or NSE) knows to reload its context.
      // ═══════════════════════════════════════════════════════════════════════════════
      let newVersion = MLSStateVersionManager.shared.incrementVersion(for: userDid)
      contextVersions[userDid] = newVersion
      logger.debug("[DECRYPT] State version incremented to \(newVersion)")

      return DecryptionOutcome(plaintext: plaintext, embed: embedData, senderDID: actualSender != "unknown" ? actualSender : nil)
    }.value
  }

  // MARK: - Main Decryption Method

  /// Decrypt MLS message and store plaintext in database
  ///
  /// This is the primary decryption method that:
  /// 1. Checks if message was already decrypted (idempotency - prevents double-decrypt race)
  /// 2. Decrypts the ciphertext using the MLS context
  /// 3. Extracts epoch and sequence number from DecryptResult
  /// 4. Stores the plaintext in the database with proper metadata
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
      return try await withMLSUserPermit(for: userDid) { [self] in
    // Capture logger before async work
    let logger = self.logger

    logger.debug(
      "[DECRYPT] Starting decryption: message=\(messageID), hasMetadata=\(epoch != nil && sequenceNumber != nil && senderID != nil)"
    )

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX (2024-12): Idempotency Check BEFORE File Coordination
    // ═══════════════════════════════════════════════════════════════════════════
    //
    // Problem: "Double Processing" Race Condition
    // - NSE and Main App both receive the same message (APNS vs sync)
    // - Both race to decrypt it
    // - NSE wins, decrypts, deletes the one-time key (Forward Secrecy)
    // - Main App tries to decrypt with stale state → SecretReuseError
    //
    // Solution: Check if message is already decrypted BEFORE acquiring the lock
    // This prevents wasting time waiting for the lock only to find the work done.
    //
    // Note: We check again AFTER acquiring the lock (defense in depth) because
    // NSE might complete between our check and lock acquisition.
    //
    // ═══════════════════════════════════════════════════════════════════════════
    do {
      let database = try await MLSGRDBManager.shared.getDatabasePool(for: userDid)
      if let cachedPlaintext = try await storage.fetchPlaintextForMessage(
        messageID,
        currentUserDID: userDid,
        database: database
      ) {
        logger.info("✅ [DECRYPT] IDEMPOTENCY HIT (pre-lock): Message \(messageID.prefix(16))... already decrypted")
        logger.info("   Skipping MLS decryption to prevent SecretReuseError")
        return cachedPlaintext
      }
    } catch let error as MLSSQLCipherError {
      // Fail-closed: if storage is unavailable we must not advance the ratchet.
      throw error
    } catch {
      // Non-fatal: if we can't check the cache, proceed with normal flow.
      // The lock-protected cache check may still catch it.
      logger.debug("[DECRYPT] Pre-lock cache check failed (continuing): \(error.localizedDescription)")
    }

    do {
      // Phase 2 (single-writer): Acquire advisory lock so NSE/app cannot both advance ratchet.
      let lockAcquired = MLSAdvisoryLockCoordinator.shared.acquireExclusiveLock(for: userDid, timeout: 5.0)
      if !lockAcquired {
        logger.warning("🔒 [DECRYPT] Advisory lock busy for \(userDid.prefix(20))... - cancelling decryption")
        throw CancellationError()
      }
      defer { MLSAdvisoryLockCoordinator.shared.releaseExclusiveLock(for: userDid) }

      // CRITICAL FIX (2024-12): Use file coordination to prevent NSE conflicts
      // The NSE may try to write to the database while the main app is decrypting.
      // Without coordination, this causes "HMAC check failed" corruption.
      let outcome = try await MLSDatabaseCoordinator.shared.performWrite(for: userDid, timeout: 15.0) {
        try await self.decryptOnce(
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

      return outcome.plaintext

    } catch {
      logger.error("[DECRYPT] Failed to decrypt message: \(error.localizedDescription)")
      throw error
    }
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
    return try await withMLSUserPermit(for: userDid) {
    // Capture logger before async work
    let logger = self.logger

    logger.debug(
      "[DECRYPT+EMBED] Starting decryption: message=\(messageID), hasMetadata=\(epoch != nil && sequenceNumber != nil && senderID != nil)"
    )

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX (2024-12): Idempotency Check BEFORE File Coordination
    // See decryptAndStore() for full explanation of the Double Processing race.
    // ═══════════════════════════════════════════════════════════════════════════
    do {
      let database = try await MLSGRDBManager.shared.getDatabasePool(for: userDid)
      if let cachedPlaintext = try await storage.fetchPlaintextForMessage(
        messageID,
        currentUserDID: userDid,
        database: database
      ) {
        let cachedEmbed = try await storage.fetchEmbedForMessage(
          messageID,
          currentUserDID: userDid,
          database: database
        )
        logger.info("✅ [DECRYPT+EMBED] IDEMPOTENCY HIT (pre-lock): Message \(messageID.prefix(16))... already decrypted")
        logger.info("   Skipping MLS decryption to prevent SecretReuseError")
        return (cachedPlaintext, cachedEmbed)
      }
    } catch {
      // Non-fatal: if we can't check the cache, proceed with normal flow
      logger.debug("[DECRYPT+EMBED] Pre-lock cache check failed (continuing): \(error.localizedDescription)")
    }

    do {
      // Phase 2 (single-writer): Acquire advisory lock so NSE/app cannot both advance ratchet.
      let lockAcquired = MLSAdvisoryLockCoordinator.shared.acquireExclusiveLock(for: userDid, timeout: 5.0)
      if !lockAcquired {
        logger.warning("🔒 [DECRYPT+EMBED] Advisory lock busy for \(userDid.prefix(20))... - cancelling decryption")
        throw CancellationError()
      }
      defer { MLSAdvisoryLockCoordinator.shared.releaseExclusiveLock(for: userDid) }

      // CRITICAL FIX (2024-12): Use file coordination to prevent NSE conflicts
      let outcome = try await MLSDatabaseCoordinator.shared.performWrite(for: userDid, timeout: 15.0) {
        try await self.decryptOnce(
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

      return (outcome.plaintext, outcome.embed)

    } catch {
      logger.error("[DECRYPT+EMBED] Failed to decrypt message: \(error.localizedDescription)")
      throw error
    }
    }
  }

  // MARK: - Ephemeral Decryption (for Notifications)

  /// Decrypt MLS message for a non-active user WITHOUT triggering database switching
  ///
  /// CRITICAL: Use this method when decrypting notifications for users other than
  /// the currently active user. This prevents "database locked" errors caused by
  /// the notification handler trying to checkpoint the active user's database.
  ///
  /// When to use:
  /// - Push notification arrives for User B while User A is active in the UI
  /// - Notification Service Extension needs to decrypt for any user
  /// - Background refresh for non-active accounts
  ///
  /// This method uses `getEphemeralDatabasePool` which:
  /// - Does NOT checkpoint the active user's database
  /// - Does NOT change the activeUserDID tracking
  /// - Allows concurrent database access for multiple users
  ///
  /// - Parameters:
  ///   - userDid: User's decentralized identifier (the notification recipient)
  ///   - groupId: MLS group identifier
  ///   - ciphertext: Encrypted message data
  ///   - conversationID: Conversation identifier for database storage
  ///   - messageID: Unique message identifier for database storage
  /// - Returns: NotificationDecryptResult containing plaintext and sender DID
  /// - Throws: MLSError or storage errors if decryption or storage fails
  public func decryptForNotification(
    userDid: String,
    groupId: Data,
    ciphertext: Data,
    conversationID: String,
    messageID: String
  ) async throws -> NotificationDecryptResult {
    return try await withMLSUserPermit(for: userDid) {
    let logger = self.logger

    logger.info("[DECRYPT-NOTIF] Starting ephemeral decryption for notification")
    logger.debug("   userDid: \(userDid.prefix(20))...")
    logger.debug("   messageID: \(messageID.prefix(16))...")

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX (2024-12): Idempotency Check BEFORE File Coordination
    // 
    // The NSE might be invoked multiple times for the same message (APNS retry,
    // or the main app might have already decrypted it during a sync).
    // Check if the message is already decrypted before acquiring the lock.
    // ═══════════════════════════════════════════════════════════════════════════
    do {
      let database = try await MLSGRDBManager.shared.getEphemeralDatabasePool(for: userDid)
      if let cachedPlaintext = try await storage.fetchPlaintextForMessage(
        messageID,
        currentUserDID: userDid,
        database: database
      ) {
        // Also fetch the cached sender for rich notification display
        let cachedSender = try await storage.fetchSenderForMessage(
          messageID,
          currentUserDID: userDid,
          database: database
        )
        logger.info("✅ [DECRYPT-NOTIF] IDEMPOTENCY HIT: Message \(messageID.prefix(16))... already decrypted")
        logger.info("   Returning cached plaintext, skipping MLS decryption")
        return NotificationDecryptResult(plaintext: cachedPlaintext, senderDID: cachedSender)
      }
    } catch {
      // Non-fatal: if we can't check the cache, proceed with normal flow
      logger.debug("[DECRYPT-NOTIF] Pre-lock cache check failed (continuing): \(error.localizedDescription)")
    }

    // CRITICAL FIX (2024-12): Use file coordination for NSE database access
    // The NSE and main app run as separate processes. Without coordination,
    // concurrent writes to the encrypted database can cause:
    // - "HMAC check failed" (page corruption from simultaneous writes)
    // - "SQLite error 7: out of memory" (file descriptor exhaustion)
    // - Ratchet state desync (NSE advances epoch but main app doesn't know)
    //
    // By wrapping the entire decrypt+store operation in file coordination,
    // we ensure exclusive access during the critical section.
    do {
      let lockAcquired = MLSAdvisoryLockCoordinator.shared.acquireExclusiveLock(for: userDid, timeout: 5.0)
      if !lockAcquired {
        logger.warning("🔒 [DECRYPT-NOTIF] Advisory lock busy for \(userDid.prefix(20))... - cancelling decryption")
        throw CancellationError()
      }
      defer { MLSAdvisoryLockCoordinator.shared.releaseExclusiveLock(for: userDid) }

      let outcome = try await MLSDatabaseCoordinator.shared.performWrite(for: userDid, timeout: 15.0) {
        try await self.decryptOnce(
          userDid: userDid,
          groupId: groupId,
          ciphertext: ciphertext,
          conversationID: conversationID,
          messageID: messageID,
          epoch: nil,
          sequenceNumber: nil,
          senderID: nil,
          useEphemeralAccess: true  // CRITICAL: Use ephemeral access
        )
      }

      logger.info("✅ [DECRYPT-NOTIF] Ephemeral decryption SUCCESS (with file coordination)")
      logger.debug("   Sender DID: \(outcome.senderDID?.prefix(24) ?? "unknown")...")
      return NotificationDecryptResult(plaintext: outcome.plaintext, senderDID: outcome.senderDID)

    } catch {
      logger.error("❌ [DECRYPT-NOTIF] Ephemeral decryption FAILED: \(error.localizedDescription)")
      throw error
    }
    }
  }

  /// Check if using ephemeral access is recommended for a user
  ///
  /// Returns true if the user is NOT the currently active database user,
  /// meaning ephemeral access should be used to prevent lock contention.
  ///
  /// - Parameter userDid: User's decentralized identifier
  /// - Returns: true if ephemeral access should be used
  public func shouldUseEphemeralAccess(for userDid: String) async -> Bool {
    let isActive = await MLSGRDBManager.shared.isActiveUser(userDid)
    if !isActive {
      logger.debug(
        "[MLSCoreContext] User \(userDid.prefix(20)) is NOT active - ephemeral access recommended")
    }
    return !isActive
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
    return try await withMLSUserPermit(for: userDid) {
    // Capture logger before async work
    let logger = self.logger

    logger.info("[DECRYPT-BATCH] Decrypting \(messages.count) messages")

    // Phase 2 (single-writer): Acquire advisory lock so NSE/app cannot both advance ratchet.
    let lockAcquired = MLSAdvisoryLockCoordinator.shared.acquireExclusiveLock(for: userDid, timeout: 5.0)
    if !lockAcquired {
      logger.warning("🔒 [DECRYPT-BATCH] Advisory lock busy for \(userDid.prefix(20))... - cancelling batch")
      throw CancellationError()
    }
    defer { MLSAdvisoryLockCoordinator.shared.releaseExclusiveLock(for: userDid) }

    // CRITICAL FIX (2024-12): Use single coordination block for entire batch
    // This is more efficient than acquiring/releasing the lock for each message
    return try await MLSDatabaseCoordinator.shared.performWrite(for: userDid, timeout: 30.0) {
      var plaintexts: [String] = []
      
      for (ciphertext, conversationID, messageID) in messages {
        do {
          // Call decryptOnce directly to avoid double-coordination
          let outcome = try await self.decryptOnce(
            userDid: userDid,
            groupId: groupId,
            ciphertext: ciphertext,
            conversationID: conversationID,
            messageID: messageID,
            epoch: nil,
            sequenceNumber: nil,
            senderID: nil
          )
          plaintexts.append(outcome.plaintext)
        } catch {
          logger.error(
            "[DECRYPT-BATCH] Failed to decrypt message \(messageID): \(error.localizedDescription)")
          throw error
        }
      }
      
      logger.info("[DECRYPT-BATCH] Successfully decrypted \(plaintexts.count) messages")
      return plaintexts
    }
    }
  }

  // MARK: - Context Information

  /// Check if a message is already cached (decrypted)
  /// - Parameters:
  ///   - messageID: Unique message identifier
  ///   - userDid: User's decentralized identifier
  /// - Returns: Cached plaintext if available, nil if not cached
  /// - Note: This is useful for NSE to check if a message was already decrypted by the main app
  public func getCachedPlaintext(messageID: String, userDid: String) async -> String? {
    do {
      let database = try await MLSGRDBManager.shared.getDatabasePool(for: userDid)
      return try await storage.fetchPlaintextForMessage(
        messageID, currentUserDID: userDid, database: database)
    } catch {
      logger.debug(
        "[CACHE] Failed to check cache for message \(messageID): \(error.localizedDescription)")
      return nil
    }
  }

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

  // MARK: - State Version Queries

  /// Get the current in-memory state version for a user.
  ///
  /// - Parameter userDid: User's decentralized identifier
  /// - Returns: Memory version, or nil if no context exists
  public func getMemoryVersion(for userDid: String) -> Int? {
    return contextVersions[userDid]
  }

  /// Check if the cached context is stale compared to disk.
  ///
  /// This is a fast check (no disk I/O, just UserDefaults read).
  /// Use this before operations that depend on fresh MLS state.
  ///
  /// - Parameter userDid: User's decentralized identifier
  /// - Returns: true if context needs to be reloaded
  public func isContextStale(for userDid: String) -> Bool {
    guard let memoryVersion = contextVersions[userDid] else {
      // No context = not stale (will be created fresh)
      return false
    }
    return MLSStateVersionManager.shared.isContextStale(for: userDid, memoryVersion: memoryVersion)
  }

  /// Check if the MLS lock is currently held by another process.
  ///
  /// Use this to show a loading indicator when NSE is processing.
  ///
  /// - Returns: true if lock is available, false if another process holds it
  public func isLockAvailable() -> Bool {
    return MLSStateVersionManager.shared.isLockAvailable()
  }
}
