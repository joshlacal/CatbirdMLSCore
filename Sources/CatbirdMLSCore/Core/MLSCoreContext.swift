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
//  CRITICAL FIX (2024-12-21): Per-Group Decryption Serialization
//
//  Problem: When multiple messages for the SAME group arrive simultaneously
//  (e.g., Message Seq 2 and Reaction Seq 3), they race to modify the MLS ratchet.
//  The NSE and Main App (or different threads) each try to decrypt concurrently,
//  causing OpenMLS to throw SecretReuseError because ratchet keys are single-use.
//
//  Root cause: The `inFlightDecryptions` deduplication only prevents duplicate
//  processing of the SAME messageID. It did NOT prevent concurrent processing
//  of DIFFERENT messages for the same group.
//
//  Solution: Added `groupDecryptionQueues` and `withSerialGroupAccess()` to
//  serialize all decryption operations per (userDID, groupID) pair. Each group's
//  ratchet is independent, but within a group, messages MUST be processed one
//  at a time in strict order.
//

import Foundation
import GRDB
@_exported import CatbirdMLS
import OSLog
import Synchronization

/// Manages MLS contexts and handles message decryption with database storage
/// Thread-safe singleton providing per-user context isolation
public actor MLSCoreContext {

  // MARK: - Singleton

  public static let shared = MLSCoreContext()

  // MARK: - Emergency Suspension Close (0xdead10cc Prevention)

  /// Thread-safe state for emergency suspension close, protected by Mutex.
  /// This is necessary because iOS can suspend us at any point after scenePhase changes
  /// and we MUST release SQLite file handles synchronously or face 0xdead10cc termination.
  private struct EmergencyState: @unchecked Sendable {
    var contexts: [String: MlsContext] = [:]
    var cacheInvalidated = false
    var suspensionInProgress = false
  }

  private static let emergencyState = Mutex(EmergencyState())
  private nonisolated static let staticLogger = Logger(subsystem: "blue.catbird.mls", category: "MLSCoreContext")

  /// Check if suspension is in progress (thread-safe read)
  public nonisolated static var isSuspensionInProgress: Bool {
    emergencyState.withLock { $0.suspensionInProgress }
  }

  /// Set suspension flag - call this BEFORE emergency close
  public nonisolated static func markSuspensionInProgress() {
    emergencyState.withLock { $0.suspensionInProgress = true }
    staticLogger.warning("🚨 [0xdead10cc-FIX] Suspension flag SET - blocking all MLS operations")
  }

  /// Clear suspension flag - call this when returning to foreground
  public nonisolated static func clearSuspensionFlag() {
    emergencyState.withLock { $0.suspensionInProgress = false }
    staticLogger.debug("✅ [0xdead10cc-FIX] Suspension flag CLEARED - MLS operations allowed")
  }

  /// Emergency synchronous close of all Rust MLS contexts for 0xdead10cc prevention.
  /// Call this SYNCHRONOUSLY when transitioning to inactive/background.
  /// This is safe to call from any thread and does not require actor isolation.
  public nonisolated static func emergencyCloseAllContexts() {
    let process = Bundle.main.bundlePath.hasSuffix(".appex") ? "nse" : "app"
    // Extract contexts and set flags atomically, then close outside the lock
    // to avoid holding the lock during potentially slow FFI calls.
    let contextsToClose: [String: MlsContext] = emergencyState.withLock { state in
      state.suspensionInProgress = true
      let snapshot = state.contexts
      state.contexts.removeAll()
      state.cacheInvalidated = true
      return snapshot
    }

    staticLogger.warning("🚨 [0xdead10cc-FIX] Suspension flag SET, emergency closing \(contextsToClose.count) Rust MLS contexts")
    MLSSuspensionFlightRecorder.shared.record(
      .flushStarted,
      details: "MLSCoreContext emergencyCloseAllContexts: closing \(contextsToClose.count)",
      process: process
    )

    for (userDID, context) in contextsToClose {
      do {
        try context.flushAndPrepareClose()
        staticLogger.debug("✅ [0xdead10cc-FIX] Rust context closed for \(userDID.prefix(20), privacy: .private)...")
      } catch {
        staticLogger.warning("⚠️ [0xdead10cc-FIX] Rust context close failed for \(userDID.prefix(20), privacy: .private)...: \(error)")
        MLSSuspensionFlightRecorder.shared.record(
          .flushFailed,
          details: "MLSCoreContext close failed for \(userDID.prefix(20)): \(error.localizedDescription)",
          process: process
        )
      }
    }

    staticLogger.debug("✅ [0xdead10cc-FIX] All Rust MLS contexts emergency closed")
    MLSSuspensionFlightRecorder.shared.record(
      .flushCompleted,
      details: "MLSCoreContext emergencyCloseAllContexts: closed \(contextsToClose.count)",
      process: process
    )
  }

  /// Register a context for emergency close.
  /// Called internally when a context is created or retrieved.
  private nonisolated static func registerForEmergencyClose(_ context: MlsContext, for userDID: String) {
    emergencyState.withLock { $0.contexts[userDID] = context }
  }

  /// Unregister a context from emergency close.
  /// Called internally when a context is explicitly closed.
  private nonisolated static func unregisterFromEmergencyClose(for userDID: String) {
    emergencyState.withLock { $0.contexts.removeValue(forKey: userDID) }
  }

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
    let messageType: MLSMessageType?
    let reaction: MLSReactionPayload?
    let epoch: UInt64?  // For epoch checkpoint recording
  }
  
  /// Result of notification decryption with sender info for rich notifications
  public struct NotificationDecryptResult: Sendable {
    public let plaintext: String
    public let senderDID: String?
    public let messageType: MLSMessageType?
    public let reaction: MLSReactionPayload?
    public let epoch: UInt64?  // For epoch checkpoint recording

    public init(
      plaintext: String,
      senderDID: String?,
      messageType: MLSMessageType? = nil,
      reaction: MLSReactionPayload? = nil,
      epoch: UInt64? = nil
    ) {
      self.plaintext = plaintext
      self.senderDID = senderDID
      self.messageType = messageType
      self.reaction = reaction
      self.epoch = epoch
    }
  }

  /// Deduplicate concurrent decrypt attempts for the same messageID
  private var inFlightDecryptions: [String: Task<DecryptionOutcome, Error>] = [:]

  // ═══════════════════════════════════════════════════════════════════════════
  // CRITICAL FIX (2024-12): Per-Group Decryption Serialization
  // ═══════════════════════════════════════════════════════════════════════════
  //
  // Problem: MLS uses a "secret tree" ratchet where each message's decryption
  // key is derived sequentially. If two messages for the same group are
  // decrypted concurrently (even with different messageIDs), the ratchet state
  // can become corrupted, causing SecretReuseError.
  //
  // Example failure scenario:
  // 1. User joins group, receives Message (Seq 2) and Reaction (Seq 3)
  // 2. NSE thread grabs Reaction (Seq 3), Main App thread grabs Message (Seq 2)
  // 3. Both try to advance the ratchet simultaneously
  // 4. OpenMLS throws SecretReuseError because keys are use-once
  //
  // Solution: Serialize all decryption operations per (userDID, groupID) pair.
  // Each group's ratchet is independent, but within a group, messages MUST
  // be processed one at a time.
  //
  // ═══════════════════════════════════════════════════════════════════════════

  /// Tracks the last decryption task for each group to ensure serial execution.
  /// Key: "\(userDID):\(groupIdHex)" → Task that completes when decryption is done
  /// This creates a chain where each new decryption awaits the previous one.
  private var groupDecryptionQueues: [String: Task<Void, Never>] = [:]

  /// Storage manager for database operations
  private let storage = MLSStorage.shared

  /// Database manager owned by this context (per-instance ownership)
  /// CRITICAL: Each MLSCoreContext instance owns its database pool lifecycle
  private let databaseManager = MLSGRDBManager()

  /// Cached keychain access group (probing is expensive)
  private var _cachedKeychainAccessGroup: String??

  /// Keychain access group for shared Keychain access between app + extensions.
  /// Resolve the fully-qualified (TeamID-prefixed) group from entitlements to avoid -34018.
  private var keychainAccessGroup: String? {
    #if targetEnvironment(simulator)
      return nil
    #else
      // Use cached value if available
      if let cached = _cachedKeychainAccessGroup {
        return cached
      }
      let resolved = MLSKeychainManager.resolvedAccessGroup(suffix: "blue.catbird.shared")
      _cachedKeychainAccessGroup = .some(resolved)
      return resolved
    #endif
  }

  /// Storage directory for MLS database files
  /// CRITICAL: Must match the path used by MLSClient in the main app
  private var storageDirectory: URL {
    // Use custom storage directory if configured (stress test mode)
    if let customDir = configuration.storageDirectory {
      return customDir
    }
    
    // Use shared storage base so main app + extensions align when available.
    let baseDirectory = MLSStoragePaths.baseContainerURL()
    return baseDirectory.appendingPathComponent("mls-state", isDirectory: true)
  }

  // MARK: - Configuration

  /// Configuration for stress testing and non-singleton usage
  public struct Configuration: Sendable {
    /// Custom storage directory (nil = use default App Group container)
    public let storageDirectory: URL?
    
    /// Custom keychain access group (nil = use default)
    public let keychainAccessGroup: String?
    
    /// Disable Darwin notifications for isolated testing
    public let disableDarwinNotifications: Bool
    
    /// Custom logger subsystem
    public let loggerSubsystem: String
    
    /// Create default production configuration
    public static let production = Configuration(
      storageDirectory: nil,
      keychainAccessGroup: nil,
      disableDarwinNotifications: false,
      loggerSubsystem: "blue.catbird.mls"
    )
    
    /// Create isolated configuration for stress testing
    /// - Parameter isolationId: Unique identifier for this test instance
    /// - Returns: Configuration with isolated storage path
    public static func stressTest(isolationId: String) -> Configuration {
      let tempDir = FileManager.default.temporaryDirectory
        .appendingPathComponent("mls-stress-test")
        .appendingPathComponent(isolationId)
      return Configuration(
        storageDirectory: tempDir,
        keychainAccessGroup: nil,
        disableDarwinNotifications: true,
        loggerSubsystem: "blue.catbird.mls.stress"
      )
    }
    
    public init(
      storageDirectory: URL?,
      keychainAccessGroup: String?,
      disableDarwinNotifications: Bool,
      loggerSubsystem: String
    ) {
      self.storageDirectory = storageDirectory
      self.keychainAccessGroup = keychainAccessGroup
      self.disableDarwinNotifications = disableDarwinNotifications
      self.loggerSubsystem = loggerSubsystem
    }
  }
  
  /// Configuration for this instance
  private let configuration: Configuration
  
  /// Whether this is a stress test instance (non-singleton)
  public var isStressTestInstance: Bool {
    configuration.disableDarwinNotifications
  }

  // MARK: - Initialization

  /// Create instance with custom configuration for stress testing
  /// - Parameter configuration: Custom configuration for this instance
  public init(configuration: Configuration) {
    self.configuration = configuration
    
    // Use custom logger subsystem if provided
    logger.info("MLSCoreContext initialized (custom configuration)")
    
    if let customDir = configuration.storageDirectory {
      logger.info("📁 [MLSCoreContext] Using custom storage: \(customDir.path)")
      // Create directory if needed
      try? FileManager.default.createDirectory(at: customDir, withIntermediateDirectories: true)
    }
    
    if configuration.disableDarwinNotifications {
      logger.info("🔕 [MLSCoreContext] Darwin notifications DISABLED (stress test mode)")
    }
    
    Task {
      await configureKeychainAccess()
    }
  }

  private init() {
    self.configuration = .production
    logger.info("MLSCoreContext initialized")
    
    // Log storage and keychain configuration immediately
    #if targetEnvironment(simulator)
      logger.warning("⚠️ [MLSCoreContext] Running on SIMULATOR - keychain sharing disabled")
    #else
      if let accessGroup = keychainAccessGroup {
        logger.info("✅ [MLSCoreContext] Keychain access group: \(accessGroup)")
      } else {
        logger.error("⚠️ [MLSCoreContext] Failed to resolve keychain access group!")
      }
    #endif

    let baseDirectory = MLSStoragePaths.baseContainerURL()
    logger.info("✅ [MLSCoreContext] Storage base: \(baseDirectory.path)")

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
      logger.info(
        "✅ [MLSCoreContext] Configured MLSKeychainManager.shared.accessGroup: \(accessGroup)")
    } else {
      #if targetEnvironment(simulator)
        logger.info("ℹ️ [MLSCoreContext] Simulator mode - using default keychain access")
      #else
        logger.error(
          "⚠️ [MLSCoreContext] No keychain access group - extension sharing will NOT work!")
      #endif
    }
    keychainConfigured = true
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
    // CRITICAL: Check suspension flag FIRST - abort immediately if app is going to background
    // This prevents the race condition where we start creating a context right before suspension
    if Self.isSuspensionInProgress {
      logger.warning("🚫 [0xdead10cc-FIX] getContext BLOCKED - suspension in progress")
      throw MLSError.contextCreationBlocked(reason: "App is transitioning to background - MLS operations suspended")
    }

    // CRITICAL: Clear stale cache after emergency suspension close
    // Emergency close happens synchronously from a nonisolated context and can't clear the actor's cache directly.
    // So we check the flag here and clear the cache if needed.
    let needsCacheClear = Self.emergencyState.withLock { state in
      if state.cacheInvalidated {
        state.cacheInvalidated = false
        return true
      }
      return false
    }
    if needsCacheClear {
      logger.debug("🔄 [0xdead10cc-FIX] Clearing stale Rust context cache after emergency close")
      contexts.removeAll()
      contextVersions.removeAll()
    }

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
        Self.unregisterFromEmergencyClose(for: userDid)

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
    
    // Set up external join authorizer
    // This allows the application to validate external join requests
    let authorizer = MLSExternalJoinAuthorizerBridge()
    try newContext.setExternalJoinAuthorizer(authorizer: authorizer)

    // Track the current disk version at context creation time
    let currentDiskVersion = MLSStateVersionManager.shared.getDiskVersion(for: userDid)
    contexts[userDid] = newContext
    contextVersions[userDid] = currentDiskVersion

    // Register for emergency close (0xdead10cc prevention)
    Self.registerForEmergencyClose(newContext, for: userDid)

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
      Self.unregisterFromEmergencyClose(for: userDid)
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
      Self.unregisterFromEmergencyClose(for: userDid)
    }
    contextVersions.removeValue(forKey: userDid)
    logger.info("Removed MLS context for user: \(userDid, privacy: .private)")
  }

  /// Clear all contexts
  public func clearAllContexts() {
    for (userDid, context) in contexts {
      try? context.flushAndPrepareClose()
      Self.unregisterFromEmergencyClose(for: userDid)
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
          Self.unregisterFromEmergencyClose(for: staleUser)
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

  // MARK: - Cross-Process Decryption Coordination

  /// Result of checking for in-flight or cached decryption
  public enum DecryptionCoordinationResult: Sendable {
    /// Message was already decrypted (cached plaintext found)
    case alreadyDecrypted(plaintext: String, senderDID: String?)
    /// Message is currently being decrypted by NSE or another thread - awaited and got result
    case awaitedInFlight(plaintext: String, senderDID: String?)
    /// No in-flight decryption and no cache - caller should proceed with decryption
    case shouldProceed
    /// Cache lookup failed with error
    case error(Error)
  }

  /// Check if a message is already decrypted or being processed, and coordinate accordingly.
  ///
  /// **CRITICAL**: Call this BEFORE attempting MLS decryption to prevent SecretReuseError.
  ///
  /// This method provides cross-process coordination between the Notification Service Extension
  /// and the main app. It checks:
  /// 1. If the message is already decrypted (cached in database)
  /// 2. If the message is currently being decrypted by NSE (in-flight)
  ///
  /// If either is true, returns the result without the caller needing to decrypt.
  /// If neither, returns `.shouldProceed` and the caller can safely decrypt.
  ///
  /// - Parameters:
  ///   - messageID: Unique message identifier
  ///   - userDID: User's decentralized identifier
  /// - Returns: Coordination result indicating how the caller should proceed
  public func checkOrAwaitDecryption(
    messageID: String,
    userDID: String
  ) async -> DecryptionCoordinationResult {
    // Check 1: Is there an in-flight decryption for this message?
    if let inFlightTask = inFlightDecryptions[messageID] {
      logger.info(
        "🔗 [COORD] Message \(messageID.prefix(16))... has in-flight decryption - awaiting")
      do {
        let outcome = try await inFlightTask.value
        logger.info("✅ [COORD] Awaited in-flight decryption for \(messageID.prefix(16))...")
        return .awaitedInFlight(plaintext: outcome.plaintext, senderDID: outcome.senderDID)
      } catch {
        logger.warning(
          "⚠️ [COORD] In-flight decryption failed for \(messageID.prefix(16))...: \(error.localizedDescription)"
        )
        // Let caller proceed - they may have different context/state
        return .shouldProceed
      }
    }

    // Check 2: Is the message already cached in the database?
    // Use safe read() which auto-routes to lightweight access for inactive users
    do {
      let normalizedDID = MLSStorageHelpers.normalizeDID(userDID)
      let cachedMessage = try await databaseManager.read(for: userDID) { db in
        try MLSMessageModel
          .filter(MLSMessageModel.Columns.messageID == messageID)
          .filter(MLSMessageModel.Columns.currentUserDID == normalizedDID)
          .fetchOne(db)
      }
      if let cachedMessage,
         let payload = cachedMessage.parsedPayload,
         let cachedPlaintext = cachedPlaintext(from: payload) {
        logger.info("✅ [COORD] Message \(messageID.prefix(16))... already cached - returning")
        return .alreadyDecrypted(plaintext: cachedPlaintext, senderDID: cachedMessage.senderID)
      }
    } catch {
      logger.warning(
        "⚠️ [COORD] Cache check failed for \(messageID.prefix(16))...: \(error.localizedDescription)"
      )
      // Non-fatal: proceed with decryption
    }

    // No in-flight, no cache - caller should proceed with their own decryption
    logger.debug(
      "📋 [COORD] Message \(messageID.prefix(16))... not in-flight or cached - caller should proceed"
    )
    return .shouldProceed
  }

  // MARK: - Per-Group Decryption Serialization

  /// Executes an operation with serial access to a group's MLS state.
  ///
  /// This ensures that only one decryption operation runs at a time for any
  /// given (userDID, groupID) pair. This prevents SecretReuseError caused by
  /// concurrent ratchet modifications.
  ///
  /// The implementation uses a task-chaining pattern: each new operation awaits
  /// the completion of the previous operation for the same group before proceeding.
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - groupId: MLS group identifier
  ///   - messageID: Message ID for logging
  ///   - operation: The async operation to execute with serialized access
  /// - Returns: The result of the operation
  /// - Throws: Rethrows any error from the operation
  private func withSerialGroupAccess<T: Sendable>(
    userDID: String,
    groupId: Data,
    messageID: String,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    let groupIdHex = groupId.prefix(8).hexEncodedString()
    let key = "\(userDID):\(groupId.hexEncodedString())"

    // Capture the previous task for this group (if any)
    let previousTask = groupDecryptionQueues[key]

    // Create a signal for when our operation completes
    // We use an AsyncStream as a simple completion signal
    let (completionStream, completionContinuation) = AsyncStream.makeStream(of: Void.self)

    // Create a task that waits for our completion signal
    // This becomes the "gate" for the next operation on this group
    let myCompletionTask = Task<Void, Never> {
      for await _ in completionStream { break }
    }
    groupDecryptionQueues[key] = myCompletionTask

    // If there was a previous operation, wait for it to complete
    if let previousTask {
      let waitStart = ContinuousClock.now
      logger.info(
        "🔗 [GROUP-SERIAL] Message \(messageID.prefix(16))... queued behind previous decryption for group \(groupIdHex)..."
      )
      await previousTask.value
      let waited = waitStart.duration(to: ContinuousClock.now)
      logger.info(
        "🔗 [GROUP-SERIAL] Message \(messageID.prefix(16))... waited \(waited) for group \(groupIdHex)..."
      )
    }

    // Execute our operation, ensuring we always signal completion
    do {
      let result = try await operation()
      completionContinuation.yield()
      completionContinuation.finish()
      logger.debug(
        "🔗 [GROUP-SERIAL] Message \(messageID.prefix(16))... completed for group \(groupIdHex)...")
      return result
    } catch {
      completionContinuation.yield()
      completionContinuation.finish()
      logger.debug(
        "🔗 [GROUP-SERIAL] Message \(messageID.prefix(16))... failed for group \(groupIdHex)...")
      throw error
    }
  }

  /// Periodically clean up completed tasks from the serialization queue.
  /// Call this during idle time to prevent memory growth.
  private func cleanupGroupDecryptionQueues() {
    // Remove tasks that have already completed
    // This is safe because completed tasks just return immediately on await
    var keysToRemove: [String] = []
    for (key, task) in groupDecryptionQueues {
      // Check if task is done by creating a timeout race
      // If the task completes immediately, it's done
      if Task.isCancelled { break }
      keysToRemove.append(key)
    }
    // For simplicity, we just clear old entries periodically
    // The queue will rebuild naturally as new operations come in
    if groupDecryptionQueues.count > 100 {
      logger.debug(
        "🧹 [GROUP-SERIAL] Clearing \(self.groupDecryptionQueues.count) stale queue entries")
      groupDecryptionQueues.removeAll()
    }
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
    // Per-messageID deduplication (same message from NSE + App)
    if let inFlight = inFlightDecryptions[messageID] {
      logger.debug("[DECRYPT] Awaiting in-flight decryption for message: \(messageID)")
      return try await inFlight.value
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX: Per-Group Serialization
    // ═══════════════════════════════════════════════════════════════════════════
    // Wrap decryption in per-group serial access to prevent SecretReuseError.
    // This ensures that even if multiple messages for the same group arrive
    // simultaneously (e.g., Seq 2 and Seq 3), they are processed one at a time.
    // ═══════════════════════════════════════════════════════════════════════════

    let task = Task { [weak self] in
      guard let self else { throw CancellationError() }

      // Apply per-group serialization INSIDE the task
      // This ensures the ratchet is only touched by one operation at a time
      return try await self.withSerialGroupAccess(
        userDID: userDid,
        groupId: groupId,
        messageID: messageID
      ) {
        try await self.performDecryption(
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
      // CRITICAL FIX: Auto-detect inactive users and force ephemeral access
      // This prevents crashes when getDatabasePool() is called for inactive users.
      // Even if caller forgets to pass useEphemeralAccess=true, we detect it here.
      let isActiveUser = await databaseManager.isActiveUser(userDid)
      let shouldUseEphemeral = useEphemeralAccess || !isActiveUser

      let database: DatabasePool
      if shouldUseEphemeral {
        if !isActiveUser {
          logger.debug(
            "[DECRYPT] Auto-detected INACTIVE user - using ephemeral access: \(userDid.prefix(20))")
        }
        database = try await databaseManager.getEphemeralDatabasePool(for: userDid)
      } else {
        database = try await databaseManager.getDatabasePool(for: userDid)
      }

      // ═══════════════════════════════════════════════════════════════════════════
      // CRITICAL: Second Idempotency Check (Defense in Depth)
      // ═══════════════════════════════════════════════════════════════════════════
      // This is the second cache check - we already checked before acquiring the lock,
      // but NSE might have completed while we were waiting for the lock.
      // This is our last chance to avoid the SecretReuseError.
      // ENHANCED (2025-01): Fetch full payload including reaction data
      // ═══════════════════════════════════════════════════════════════════════════
      if let cachedPayload = try await storage.fetchPayloadForMessage(
        messageID,
        currentUserDID: userDid,
        database: database
      ) {
        let cachedSender = try await storage.fetchSenderForMessage(
          messageID,
          currentUserDID: userDid,
          database: database
        )
        logger.info("✅ [DECRYPT] IDEMPOTENCY HIT (post-lock): Message \(messageID.prefix(16))... already decrypted")
        logger.info("   NSE completed while we waited for lock - using cached result")
        return DecryptionOutcome(
          plaintext: cachedPayload.text ?? "",
          embed: cachedPayload.embed,
          senderDID: cachedSender,
          messageType: cachedPayload.messageType,
          reaction: cachedPayload.reaction,
          epoch: nil  // Cached - don't have epoch info
        )
      }

      // Get context (must be done in actor context)
      let context = try await getContext(for: userDid)

      // Padding is stripped by catbird-mls decrypt_message internally.
      let actualCiphertext = ciphertext

      logger.debug("[DECRYPT] Performing MLS decrypt for message: \(messageID)")

      // ═══════════════════════════════════════════════════════════════════════════════
      // CRITICAL FIX (2025-01): Cross-Process Account Switch Detection
      // ═══════════════════════════════════════════════════════════════════════════════
      // Capture the account switch epoch BEFORE the MLS decrypt operation.
      // After decryption + storage, we check if the epoch changed - if so, an account
      // switch happened during our operation and we must abort to prevent returning
      // data that was decrypted with potentially corrupted state.
      // ═══════════════════════════════════════════════════════════════════════════════
      let accountSwitchEpochBefore = MLSAppActivityState.getAccountSwitchEpoch()

      // ═══════════════════════════════════════════════════════════════════════════════
      // CRITICAL FIX (2025-01): Skip FFI decrypt for self-sent messages
      // ═══════════════════════════════════════════════════════════════════════════════
      // MLS protocol returns CannotDecryptOwnMessage when trying to decrypt our own
      // sent messages. We MUST have cached the plaintext at send time (see
      // MLSConversationManager+Messaging.swift). If the cache is empty, we have data loss.
      // ═══════════════════════════════════════════════════════════════════════════════
      if let senderID = senderID {
        let normalizedSender = MLSStorageHelpers.normalizeDID(senderID)
        let normalizedUser = MLSStorageHelpers.normalizeDID(userDid)
        if normalizedSender == normalizedUser {
          logger.info(
            "🔓 [DECRYPT] Self-sent message detected (sender == user) - checking cache only")
          if let cachedPayload = try await storage.fetchPayloadForMessage(
            messageID,
            currentUserDID: userDid,
            database: database
          ) {
            logger.info("✅ [DECRYPT] Self-sent message found in cache")
            let cachedSender = try await storage.fetchSenderForMessage(
              messageID,
              currentUserDID: userDid,
              database: database
            )
            return DecryptionOutcome(
              plaintext: cachedPayload.text ?? "",
              embed: cachedPayload.embed,
              senderDID: cachedSender ?? userDid,
              messageType: cachedPayload.messageType,
              reaction: cachedPayload.reaction,
              epoch: nil
            )
          }
          // Self-sent message NOT in cache - return placeholder to avoid corrupting state
          // CRITICAL FIX: Do NOT throw ratchetStateDesync - that marks the conversation as corrupted
          // and triggers rejoin flows. Instead, return a placeholder that the UI can display gracefully.
          logger.error(
            "❌ [DECRYPT] Self-sent message NOT in cache - plaintext was not persisted at send time!"
          )
          logger.error(
            "   MessageID: \(messageID), ConvoID: \(conversationID), Epoch: \(epoch ?? -1)"
          )

          // Return placeholder instead of throwing error
          return DecryptionOutcome(
            plaintext: "⚠️ Message unavailable (sent from this account)",
            embed: nil,
            senderDID: userDid,
            messageType: .text,
            reaction: nil,
            epoch: nil
          )
        }
      }

      let result: DecryptResult
      do {
        result = try context.decryptMessage(groupId: groupId, ciphertext: actualCiphertext)
      } catch {
        let desc = String(describing: error)

        // ═══════════════════════════════════════════════════════════════════════════
        // CRITICAL FIX (2025-01): Handle Self-Message Decryption Failure
        // ═══════════════════════════════════════════════════════════════════════════
        // OpenMLS throws "ValidationError(CannotDecryptOwnMessage)" (or similar)
        // when trying to decrypt a message we sent ourselves because the ratchet
        // state was already advanced on send. This is NOT a desync.
        // ═══════════════════════════════════════════════════════════════════════════
        if desc.contains("CannotDecryptOwnMessage") {
          logger.info("✅ [DECRYPT] Caught CannotDecryptOwnMessage - treating as self-message")

          if let cachedPayload = try? await storage.fetchPayloadForMessage(
            messageID,
            currentUserDID: userDid,
            database: database
          ) {
            let cachedSender = try? await storage.fetchSenderForMessage(
              messageID,
              currentUserDID: userDid,
              database: database
            )
            return DecryptionOutcome(
              plaintext: cachedPayload.text ?? "",
              embed: cachedPayload.embed,
              senderDID: cachedSender ?? userDid,
              messageType: cachedPayload.messageType,
              reaction: cachedPayload.reaction,
              epoch: nil
            )
          } else {
            logger.warning("⚠️ [DECRYPT] Self-message not found in cache - returning placeholder")
            return DecryptionOutcome(
              plaintext: "⚠️ Message unavailable (sent from this account)",
              embed: nil,
              senderDID: userDid,
              messageType: .text,
              reaction: nil,
              epoch: nil
            )
          }
        }

        // SecretReuseError handling (existing logic)
        if desc.contains("SecretReuseError") || desc.contains("SecretTreeError") {
          // Duplicate processing: attempt to use cache with RETRY/BACKOFF
          // The other process might still be writing to the DB.
          for i in 0..<3 {
            if i > 0 {
              let delay = UInt64(50 * (1 << (i - 1)))  // 50ms, 100ms
              try? await Task.sleep(nanoseconds: delay * 1_000_000)
            }

            if let cachedPayload = try? await storage.fetchPayloadForMessage(
              messageID,
              currentUserDID: userDid,
              database: database
            ) {
              let cachedSender = try? await storage.fetchSenderForMessage(
                messageID,
                currentUserDID: userDid,
                database: database
              )
              logger.info("✅ [DECRYPT] SecretReuseError recovery (attempt \(i+1)) → cache hit")
              return DecryptionOutcome(
                plaintext: cachedPayload.text ?? "",
                embed: cachedPayload.embed,
                senderDID: cachedSender,
                messageType: cachedPayload.messageType,
                reaction: cachedPayload.reaction,
                epoch: nil  // Recovery from cache - no epoch
              )
            }
          }

          logger.warning("⚠️ [DECRYPT] SecretReuseError: Cache miss after retries; skipping")
          throw MLSError.secretReuseSkipped(messageID: messageID)
        }
        throw error
      }

      let payloadData = result.plaintext

      let payload = try? MLSMessagePayload.decodeFromJSON(payloadData)
      var detectedMessageType: MLSMessageType? = payload?.messageType
      var reactionPayload: MLSReactionPayload?

      let actualEpoch = epoch ?? Int64(result.epoch)
      let actualSeq = sequenceNumber ?? Int64(result.sequenceNumber)
      
      // LOGGING UPDATE (2024-12-24): Coordination Generation + Epoch Trio Visibility
      let coordinationGen = MLSCoordinationStore.shared.getState().coordinationGeneration
      logger.info(
        "🔓 [De-Cipher] [Gen: \(coordinationGen)] [Epoch: \(actualEpoch)] Success: Seq=\(actualSeq) | Group=\(groupId.prefix(8).hexEncodedString())..."
      )

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

      // For control messages (reactions/readReceipts/typing/etc) we cache the full payload
      // so they are filtered from chat history, and persist side effects as needed.
      let plaintext: String
      var embedData: MLSEmbedData?

      if let payload {
        detectedMessageType = payload.messageType
        embedData = payload.embed

        switch payload.messageType {
        case .text:
          plaintext = payload.text ?? (String(data: payloadData, encoding: .utf8) ?? "")

        case .reaction:
          reactionPayload = payload.reaction
          if let reaction = payload.reaction {
            let verb = (reaction.action == .add) ? "Reacted" : "Removed reaction"
            plaintext = "\(verb) \(reaction.emoji)"

            // ═══════════════════════════════════════════════════════════════════════════════
            // CRITICAL FIX (2024-12-26): ALWAYS persist reactions, even in ephemeral mode
            // ═══════════════════════════════════════════════════════════════════════════════
            // Problem: When NSE decrypts a reaction in ephemeral mode, it advances the ratchet
            // but previously skipped saving the reaction. When the main app tries to decrypt,
            // it gets SecretReuseError and can't find the reaction in the cache → lost data.
            //
            // Solution: Always persist the reaction payload. Ephemeral mode only affects
            // database connection management (no checkpointing), not data persistence.
            // ═══════════════════════════════════════════════════════════════════════════════
            if actualSender != "unknown" {
              let reactionID =
                "reaction:\(conversationID):\(reaction.messageId):\(actualSender):\(reaction.emoji)"
              switch reaction.action {
              case .add:
                let model = MLSReactionModel(
                  reactionID: reactionID,
                  messageID: reaction.messageId,
                  conversationID: conversationID,
                  currentUserDID: userDid,
                  actorDID: actualSender,
                  emoji: reaction.emoji,
                  action: "add",
                  timestamp: Date()
                )
                do {
                  try await storage.saveReaction(model, database: database)
                  logger.info(
                    "[DECRYPT] Persisted reaction: \(reaction.emoji) on \(reaction.messageId.prefix(16))... (ephemeral=\(useEphemeralAccess))"
                  )
                } catch {
                  logger.error(
                    "❌ [DECRYPT] FAILED to save reaction: \(reaction.emoji) on \(reaction.messageId.prefix(16))... error: \(error.localizedDescription)"
                  )
                }
              case .remove:
                do {
                  try await storage.deleteReaction(
                    messageID: reaction.messageId,
                    actorDID: actualSender,
                    emoji: reaction.emoji,
                    currentUserDID: userDid,
                    database: database
                  )
                  logger.info(
                    "[DECRYPT] Removed reaction: \(reaction.emoji) on \(reaction.messageId.prefix(16))... (ephemeral=\(useEphemeralAccess))"
                  )
                } catch {
                  logger.error(
                    "❌ [DECRYPT] FAILED to delete reaction: \(reaction.emoji) on \(reaction.messageId.prefix(16))... error: \(error.localizedDescription)"
                  )
                }
              }
            }
          } else {
            plaintext = "Reaction"
          }

        case .readReceipt:
          plaintext = "Read receipt"
          embedData = nil

        case .typing:
          plaintext = "Typing"
          embedData = nil

        case .adminRoster:
          plaintext = "Roster update"
          embedData = nil

        case .adminAction:
          plaintext = "Admin update"
          embedData = nil
        }
      } else {
        plaintext = String(data: payloadData, encoding: .utf8) ?? ""
        embedData = nil
      }

      logger.info(
        "[DECRYPT] [Gen: \(coordinationGen)] Decrypted: epoch=\(actualEpoch), seq=\(actualSeq), sender=\(actualSender == "unknown" ? "unknown" : actualSender.prefix(24)), hasEmbed=\(embedData != nil), \(plaintext.count) chars"
      )

      // ═══════════════════════════════════════════════════════════════════════════════
      // CRITICAL FIX (2024-12-26): ALWAYS persist payloads, even in ephemeral mode
      // ═══════════════════════════════════════════════════════════════════════════════
      // Problem: When NSE decrypts a message in ephemeral mode, it advances the ratchet
      // but previously skipped saving the payload. When the main app tries to decrypt,
      // it gets SecretReuseError and can't find the payload in the cache → lost data.
      //
      // Solution: Always persist the payload. Ephemeral mode only affects database
      // connection management (no checkpointing of other user's DB), not data persistence.
      // The version increment is still skipped in ephemeral mode since the main app
      // will handle that when it processes the message via its normal sync path.
      // ═══════════════════════════════════════════════════════════════════════════════
      
      // Ensure conversation exists for FK constraint
      let effectiveConversationID = try await storage.ensureConversationExistsOrPlaceholder(
        userDID: userDid,
        conversationID: conversationID,
        groupID: groupId.hexEncodedString(),
        senderDID: actualSender != "unknown" ? actualSender : nil,
        database: database
      )

      // Build payload to store (use original payload if available, else create text payload)
      let payloadToStore = payload ?? MLSMessagePayload.text(plaintext, embed: embedData)

      // Attempt to save payload with FK-error recovery
      do {
        try await MLSStorageHelpers.savePayload(
          in: database,
          messageID: messageID,
          conversationID: effectiveConversationID,
          currentUserDID: userDid,
          payload: payloadToStore,
          senderID: actualSender,
          epoch: actualEpoch,
          sequenceNumber: actualSeq
        )
        logger.info(
          "[DECRYPT] Stored payload for message: \(messageID) (ephemeral=\(useEphemeralAccess))")
      } catch let error as DatabaseError where error.resultCode == .SQLITE_CONSTRAINT {
        // FK constraint failed despite ensureConversationExistsOrPlaceholder
        // This can happen in race conditions - retry once with forced placeholder
        logger.warning("⚠️ [FK-RECOVERY] FK constraint failed, forcing placeholder creation...")

        let retryConversationID = try await storage.ensureConversationExistsOrPlaceholder(
          userDID: userDid,
          conversationID: conversationID,
          groupID: groupId.hexEncodedString(),
          senderDID: actualSender != "unknown" ? actualSender : nil,
          database: database
        )

        // Retry the save
        try await MLSStorageHelpers.savePayload(
          in: database,
          messageID: messageID,
          conversationID: retryConversationID,
          currentUserDID: userDid,
          payload: payloadToStore,
          senderID: actualSender,
          epoch: actualEpoch,
          sequenceNumber: actualSeq
        )
        logger.info("✅ [FK-RECOVERY] Payload saved after placeholder creation")
      }

      // ═══════════════════════════════════════════════════════════════════════════════
      // MONOTONIC VERSION INCREMENT: Only in non-ephemeral mode
      // ═══════════════════════════════════════════════════════════════════════════════
      // Version increment is still skipped in ephemeral mode since the main app
      // handles version management through its normal sync path.
      // ═══════════════════════════════════════════════════════════════════════════════
      if !useEphemeralAccess {
        let newVersion = MLSStateVersionManager.shared.incrementVersion(for: userDid)
        contextVersions[userDid] = newVersion
        logger.debug("[DECRYPT] State version incremented to \(newVersion)")
      } else {
        logger.debug("🛡️ [EPHEMERAL] Skipping version increment (main app will handle)")
      }

      // ═══════════════════════════════════════════════════════════════════════════════
      // CRITICAL CHECK: Detect if account switch happened during our operation
      // ═══════════════════════════════════════════════════════════════════════════════
      // If the account switch epoch changed during decryption + storage, the state may
      // be corrupted. We abort here to prevent returning potentially invalid data.
      // The message data is already saved, so it can be retrieved from cache later.
      // ═══════════════════════════════════════════════════════════════════════════════
      let accountSwitchEpochAfter = MLSAppActivityState.getAccountSwitchEpoch()
      if accountSwitchEpochBefore != accountSwitchEpochAfter {
        logger.error(
          "🚨 [DECRYPT] Account switch detected during operation! epoch: \(accountSwitchEpochBefore) → \(accountSwitchEpochAfter)"
        )
        logger.error(
          "   Message \(messageID) was saved but returning error to prevent state corruption")
        throw MLSError.accountSwitchInterrupted(
          epochBefore: accountSwitchEpochBefore,
          epochAfter: accountSwitchEpochAfter
        )
      }

      return DecryptionOutcome(
        plaintext: plaintext,
        embed: embedData,
        senderDID: actualSender != "unknown" ? actualSender : nil,
        messageType: detectedMessageType,
        reaction: reactionPayload,
        epoch: result.epoch  // Fresh decryption - record actual epoch
      )
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
    // Capture logger before async work
    let logger = self.logger
    let coordinationGen = MLSCoordinationStore.shared.getState().coordinationGeneration

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
      // Use safe read() which auto-routes to lightweight access for inactive users
      let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)
      let cachedMessage = try await databaseManager.read(for: userDid) { db in
        try MLSMessageModel
          .filter(MLSMessageModel.Columns.messageID == messageID)
          .filter(MLSMessageModel.Columns.currentUserDID == normalizedDID)
          .fetchOne(db)
      }
      if let cachedMessage,
         let payload = cachedMessage.parsedPayload,
         let plaintext = cachedPlaintext(from: payload) {
        logger.info("✅ [DECRYPT] IDEMPOTENCY HIT (pre-lock): Message \(messageID.prefix(16))... already decrypted")
        logger.info("   Skipping MLS decryption to prevent SecretReuseError")
        return plaintext
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
      // ═══════════════════════════════════════════════════════════════════════════
      // ADVISORY LOCKS REMOVED (2026-02): Signal-style 0xdead10cc prevention
      // ═══════════════════════════════════════════════════════════════════════════
      // Advisory locks (flock/fcntl) cause 0xdead10cc crashes when held during
      // iOS app suspension. We now rely on:
      // 1. SQLite WAL-mode locking (iOS exempts this from 0xdead10cc)
      // 2. Budget-based TRUNCATE checkpoints to keep WAL small
      // 3. Darwin notifications for cross-process coordination signals
      // 4. Idempotency checks to detect already-decrypted messages
      // ═══════════════════════════════════════════════════════════════════════════
      let groupIdHex = groupId.hexEncodedString()

      // ═══════════════════════════════════════════════════════════════════════════
      // Context reload to catch NSE changes (replaces post-lock invalidation)
      // ═══════════════════════════════════════════════════════════════════════════
      // NSE and Main App are separate processes with separate in-memory contexts.
      // If NSE processed messages, our cached context may be stale. Always reload
      // from disk to ensure we have the latest ratchet state.
      // ═══════════════════════════════════════════════════════════════════════════
      if contexts[userDid] != nil {
        contexts.removeValue(forKey: userDid)
        Self.unregisterFromEmergencyClose(for: userDid)
        logger.info("🔄 [DECRYPT] Invalidated cached context for fresh disk read")
      }

      // SECOND IDEMPOTENCY CHECK: Before attempting decryption
      // Another process may have completed decryption while we waited
      // Use safe read() which auto-routes to lightweight access for inactive users
      do {
        let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)
        let cachedMessage = try await databaseManager.read(for: userDid) { db in
          try MLSMessageModel
            .filter(MLSMessageModel.Columns.messageID == messageID)
            .filter(MLSMessageModel.Columns.currentUserDID == normalizedDID)
            .fetchOne(db)
        }
        if let cachedMessage,
           let payload = cachedMessage.parsedPayload,
           let plaintext = cachedPlaintext(from: payload) {
          logger.info(
            "✅ [DECRYPT] IDEMPOTENCY HIT (post-group-lock): Message \(messageID.prefix(16))... already decrypted"
          )
          return plaintext
        }
      } catch {
        logger.debug(
          "[DECRYPT] Post-lock cache check failed (continuing): \(error.localizedDescription)")
      }

      // ═══════════════════════════════════════════════════════════════════════════
      // OPERATION TICKET: Acquire ticket before database access
      // ═══════════════════════════════════════════════════════════════════════════
      // The withMLSDatabaseOperation wrapper:
      // 1. Atomically rejects operations if shutdown is in progress
      // 2. Tracks the operation for drain waiting during shutdown
      // 3. Uses withMLSExclusiveAccess internally for coordination
      // ═══════════════════════════════════════════════════════════════════════════
      let outcome = try await withMLSDatabaseOperation(
        for: userDid,
        purpose: .decrypt
      ) { [self] in
        // Run uninterruptibly so we can't advance the ratchet without persisting cache.
        try await Task.detached {
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
        }.value
      }

      return outcome.plaintext

    } catch let error as MLSGateError {
      logger.warning(
        "🛑 [DECRYPT] Shutdown in progress for \(userDid.prefix(20))... - \(error.localizedDescription)"
      )
      throw CancellationError()
    } catch is MLSExclusiveAccessError {
      logger.warning("🔒 [DECRYPT] Exclusive access busy for \(userDid.prefix(20))... - cancelling decryption")
      throw CancellationError()
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

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX (2024-12): Idempotency Check BEFORE File Coordination
    // See decryptAndStore() for full explanation of the Double Processing race.
    // Use safe read() which auto-routes to lightweight access for inactive users
    // ═══════════════════════════════════════════════════════════════════════════
    do {
      let normalizedDID = MLSStorageHelpers.normalizeDID(userDid)
      let cachedMessage = try await databaseManager.read(for: userDid) { db in
        try MLSMessageModel
          .filter(MLSMessageModel.Columns.messageID == messageID)
          .filter(MLSMessageModel.Columns.currentUserDID == normalizedDID)
          .fetchOne(db)
      }
      if let cachedMessage,
         let payload = cachedMessage.parsedPayload,
         let plaintext = cachedPlaintext(from: payload) {
        logger.info("✅ [DECRYPT+EMBED] IDEMPOTENCY HIT (pre-lock): Message \(messageID.prefix(16))... already decrypted")
        logger.info("   Skipping MLS decryption to prevent SecretReuseError")
        return (plaintext, payload.embed)
      }
    } catch {
      // Non-fatal: if we can't check the cache, proceed with normal flow
      logger.debug("[DECRYPT+EMBED] Pre-lock cache check failed (continuing): \(error.localizedDescription)")
    }

    do {
      let outcome = try await withMLSDatabaseOperation(
        for: userDid,
        purpose: .decrypt
      ) { [self] in
        // Run uninterruptibly so we can't advance the ratchet without persisting cache.
        try await Task.detached {
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
        }.value
      }

      return (outcome.plaintext, outcome.embed)

    } catch let error as MLSGateError {
      logger.warning(
        "🛑 [DECRYPT+EMBED] Shutdown in progress for \(userDid.prefix(20))... - \(error.localizedDescription)"
      )
      throw CancellationError()
    } catch is MLSExclusiveAccessError {
      logger.warning("🔒 [DECRYPT+EMBED] Exclusive access busy for \(userDid.prefix(20))... - cancelling decryption")
      throw CancellationError()
    } catch {
      logger.error("[DECRYPT+EMBED] Failed to decrypt message: \(error.localizedDescription)")
      throw error
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
  ///   - sequenceNumber: Optional sequence number from push payload for ordering tracking
  /// - Returns: NotificationDecryptResult containing plaintext, sender DID, and control metadata
  /// - Throws: MLSError or storage errors if decryption or storage fails
  public func decryptForNotification(
    userDid: String,
    groupId: Data,
    ciphertext: Data,
    conversationID: String,
    messageID: String,
    sequenceNumber: Int64? = nil,
    useEphemeralAccess: Bool = true
  ) async throws -> NotificationDecryptResult {
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
    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX (2024-12): Idempotency Check & Smart ID Resolution
    // ═══════════════════════════════════════════════════════════════════════════

    // We start with the requested ID (Hex from NSE), but will try to resolve
    // the canonical UUID if it exists via "Smart Lookup".
    var resolvedConversationID = conversationID

    do {
      // CRITICAL: For notifications, ALWAYS use ephemeral access since notifications can
      // arrive for any user (active or inactive). getDatabasePool() will crash for inactive users.
      let database = try await databaseManager.getEphemeralDatabasePool(for: userDid)

      // Resolve the real Conversation ID before proceeding.
      // If this throws, we fail back to catch block but keep original ID.
      resolvedConversationID = try await storage.ensureConversationExistsOrPlaceholder(
        userDID: userDid,
        conversationID: conversationID,
        groupID: groupId.hexEncodedString(),
        senderDID: nil,  // Unknown at this stage
        database: database
      )

      if let cachedPayload = try await storage.fetchPayloadForMessage(
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
        logger.info(
          "✅ [DECRYPT-NOTIF] IDEMPOTENCY HIT: Message \(messageID.prefix(16))... already decrypted conversationID=\(resolvedConversationID)")
        logger.info("   Returning cached plaintext, skipping MLS decryption")

        let displayText = notificationDisplayText(from: cachedPayload)
        return NotificationDecryptResult(
          plaintext: displayText,
          senderDID: cachedSender,
          messageType: cachedPayload.messageType,
          reaction: cachedPayload.reaction,
          epoch: nil  // Cached result - no epoch available
        )
      }
    } catch {
      // Non-fatal: if we can't check the cache, proceed with normal flow
      logger.debug(
        "[DECRYPT-NOTIF] Pre-lock cache/ID check failed (continuing with \(resolvedConversationID)): \(error.localizedDescription)"
      )
    }

    // Cross-process ordering gate: if the main app is currently processing a Welcome
    // for this conversation, wait briefly before attempting decryption.
    let welcomeReady = await MLSWelcomeGate.shared.waitForWelcomeIfPending(
      for: conversationID,
      userDID: userDid,
      timeout: .seconds(3)
    )
    if !welcomeReady {
      logger.info(
        "[DECRYPT-NOTIF] Welcome gate timeout - skipping decryption for convo=\(conversationID.prefix(16))..."
      )
      throw CancellationError()
    }

    // If the group isn't initialized locally yet, fail fast and let the main app handle it.
    if !(await groupExists(userDid: userDid, groupId: groupId)) {
      logger.info(
        "[DECRYPT-NOTIF] Group not initialized - skipping decryption for convo=\(conversationID.prefix(16))..."
      )
      throw CancellationError()
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ADVISORY LOCKS REMOVED (2026-02): Signal-style 0xdead10cc prevention
    // ═══════════════════════════════════════════════════════════════════════════
    // Advisory locks (flock/fcntl) cause 0xdead10cc crashes when held during
    // iOS app suspension. We now rely on:
    // 1. SQLite WAL-mode locking (iOS exempts this from 0xdead10cc)
    // 2. Budget-based TRUNCATE checkpoints to keep WAL small
    // 3. Darwin notifications for cross-process coordination signals
    // 4. Idempotency checks to detect already-decrypted messages
    // ═══════════════════════════════════════════════════════════════════════════
    let groupIdHex = groupId.hexEncodedString()

    // ═══════════════════════════════════════════════════════════════════════════
    // Context reload to catch main app changes (replaces post-lock invalidation)
    // ═══════════════════════════════════════════════════════════════════════════
    // NSE and Main App are separate processes with separate in-memory contexts.
    // The main app may have processed messages. Always reload from disk to ensure
    // we have the latest ratchet state.
    // ═══════════════════════════════════════════════════════════════════════════
    if contexts[userDid] != nil {
      contexts.removeValue(forKey: userDid)
      Self.unregisterFromEmergencyClose(for: userDid)
      logger.info("🔄 [DECRYPT-NOTIF] Invalidated cached context for fresh disk read")
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Post-lock cache check: Another process may have completed while we waited
    // ═══════════════════════════════════════════════════════════════════════════
    do {
      let database = try await databaseManager.getEphemeralDatabasePool(for: userDid)
      if let cachedPayload = try await storage.fetchPayloadForMessage(
        messageID,
        currentUserDID: userDid,
        database: database
      ) {
        let cachedSender = try await storage.fetchSenderForMessage(
          messageID,
          currentUserDID: userDid,
          database: database
        )
        logger.info(
          "✅ [DECRYPT-NOTIF] IDEMPOTENCY HIT (post-group-lock): Message \(messageID.prefix(16))... already decrypted"
        )
        let displayText = notificationDisplayText(from: cachedPayload)
        return NotificationDecryptResult(
          plaintext: displayText,
          senderDID: cachedSender,
          messageType: cachedPayload.messageType,
          reaction: cachedPayload.reaction,
          epoch: nil
        )
      }
    } catch {
      logger.debug(
        "[DECRYPT-NOTIF] Post-lock cache check failed (continuing): \(error.localizedDescription)"
      )
    }

    do {
      // ═══════════════════════════════════════════════════════════════════════════
      // OPERATION TICKET: Acquire ticket before database access (NSE path)
      // ═══════════════════════════════════════════════════════════════════════════
      // For NSE, this allows the operation to be tracked and rejected atomically
      // if the main app is in the middle of an account switch.
      // ═══════════════════════════════════════════════════════════════════════════
      let outcome = try await withMLSDatabaseOperation(
        for: userDid,
        purpose: .decrypt
      ) { [self, resolvedConversationID] in
        // Run uninterruptibly so we can't advance the ratchet without persisting cache.
        try await Task.detached {
          try await self.decryptOnce(
            userDid: userDid,
            groupId: groupId,
            ciphertext: ciphertext,
            conversationID: resolvedConversationID,  // Use the resolved ID
            messageID: messageID,
            epoch: nil,
            sequenceNumber: nil,
            senderID: nil,
            useEphemeralAccess: useEphemeralAccess
          )
        }.value
      }

      logger.info("✅ [DECRYPT-NOTIF] Ephemeral decryption SUCCESS")
      logger.debug("   Sender DID: \(outcome.senderDID?.prefix(24) ?? "unknown")...")

      // ═══════════════════════════════════════════════════════════════════════════
      // MESSAGE ORDERING: Update sequence tracking after successful decryption
      // ═══════════════════════════════════════════════════════════════════════════
      // If sequence number was provided in the push payload, update the tracking.
      // This ensures the main app knows this message was processed by the NSE.
      // CRITICAL: Always use ephemeral for notifications since they can be for any user.
      // ═══════════════════════════════════════════════════════════════════════════
      if let seq = sequenceNumber {
        do {
          let database = try await databaseManager.getEphemeralDatabasePool(for: userDid)

          try await storage.updateLastProcessedSeq(
            conversationID: resolvedConversationID,
            currentUserDID: userDid,
            sequenceNumber: seq,
            database: database
          )
          logger.info(
            "[DECRYPT-NOTIF] Updated sequence tracking: seq=\(seq) for convo=\(resolvedConversationID.prefix(16))"
          )
        } catch {
          logger.warning(
            "⚠️ [DECRYPT-NOTIF] Failed to update sequence tracking: \(error.localizedDescription)")
          // Non-fatal: the message was still decrypted successfully
        }
      } else {
        logger.debug(
          "[DECRYPT-NOTIF] No sequence number provided - skipping sequence tracking update")
      }

      return NotificationDecryptResult(
        plaintext: outcome.plaintext,
        senderDID: outcome.senderDID,
        messageType: outcome.messageType,
        reaction: outcome.reaction,
        epoch: outcome.epoch  // Pass epoch for checkpoint recording
      )

    } catch let error as MLSGateError {
      logger.warning(
        "🛑 [DECRYPT-NOTIF] Shutdown in progress for \(userDid.prefix(20))... - \(error.localizedDescription)"
      )
      throw CancellationError()
    } catch is MLSExclusiveAccessError {
      logger.warning("🔒 [DECRYPT-NOTIF] Exclusive access busy for \(userDid.prefix(20))... - cancelling decryption")
      throw CancellationError()
    } catch {
      logger.error("❌ [DECRYPT-NOTIF] Ephemeral decryption FAILED: \(error.localizedDescription)")
      throw error
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
    let isActive = await databaseManager.isActiveUser(userDid)
    if !isActive {
      logger.debug(
        "[MLSCoreContext] User \(userDid.prefix(20)) is NOT active - ephemeral access recommended")
    }
    return !isActive
  }

  private func inferMessageType(from cachedPlaintext: String) -> MLSMessageType? {
    // Parse JSON payload to get message type
    if cachedPlaintext.hasPrefix("{"),
      let data = cachedPlaintext.data(using: .utf8),
      let payload = try? MLSMessagePayload.decodeFromJSON(data)
    {
      return payload.messageType
    }
    return nil
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

    do {
      return try await withMLSDatabaseOperation(
        for: userDid,
        purpose: .decryptBatch
      ) { [self] in
        var plaintexts: [String] = []

        for (ciphertext, conversationID, messageID) in messages {
          do {
            // Call decryptOnce directly to avoid double-coordination
            let outcome = try await decryptOnce(
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
    } catch let error as MLSGateError {
      logger.warning(
        "🛑 [DECRYPT-BATCH] Shutdown in progress for \(userDid.prefix(20))... - \(error.localizedDescription)"
      )
      throw CancellationError()
    } catch is MLSExclusiveAccessError {
      logger.warning("🔒 [DECRYPT-BATCH] Exclusive access busy for \(userDid.prefix(20))... - cancelling batch")
      throw CancellationError()
    }
  }

  // MARK: - Context Information

  /// Check if a message is already cached (decrypted)
  /// - Parameters:
  ///   - messageID: Unique message identifier
  ///   - userDid: User's decentralized identifier
  /// - Returns: Cached plaintext if available, nil if not cached
  /// - Note: This is useful for NSE to check if a message was already decrypted by the main app
  public func getCachedPlaintext(
    messageID: String,
    userDid: String,
    useEphemeralAccess: Bool = false
  ) async -> String? {
    do {
      // Always use safe read access - it auto-routes to lightweight Queue for inactive users
      let normalizedUserDID = MLSStorageHelpers.normalizeDID(userDid)
      let cachedMessage = try await databaseManager.read(for: userDid) { db in
        try MLSMessageModel
          .filter(MLSMessageModel.Columns.messageID == messageID)
          .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
          .fetchOne(db)
      }
      if let cachedMessage,
         let payload = cachedMessage.parsedPayload,
         let cachedPlaintext = cachedPlaintext(from: payload) {
        return cachedPlaintext
      }
    } catch {
      logger.debug(
        "[CACHE] Failed to check cache for message \(messageID): \(error.localizedDescription)")
    }
    return nil
  }
  
  /// Check if a notification-safe message body is already cached.
  /// - Parameters:
  ///   - messageID: Unique message identifier
  ///   - userDid: User's decentralized identifier
  ///   - useEphemeralAccess: Use ephemeral DB access to avoid active-user checkpoints
  /// - Returns: Display-ready plaintext if available, nil if not cached
  public func getCachedNotificationPlaintext(
    messageID: String,
    userDid: String,
    useEphemeralAccess: Bool = false
  ) async -> String? {
    do {
      let normalizedUserDID = MLSStorageHelpers.normalizeDID(userDid)
      if let payload = try await databaseManager.read(for: userDid) { db in
        try MLSMessageModel
          .filter(MLSMessageModel.Columns.messageID == messageID)
          .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
          .fetchOne(db)?.parsedPayload
      } {
        return notificationDisplayText(from: payload)
      }
    } catch {
      logger.debug(
        "[CACHE] Failed to check notification cache for message \(messageID): \(error.localizedDescription)"
      )
    }
    return nil
  }

  private func notificationDisplayText(from payload: MLSMessagePayload) -> String {
    switch payload.messageType {
    case .text:
      return payload.text ?? "New Message"
    case .reaction:
      if let reaction = payload.reaction {
        let verb = (reaction.action == .add) ? "Reacted with" : "Removed reaction"
        return "\(verb) \(reaction.emoji)"
      }
      return "Reaction update"
    case .readReceipt:
      return "Read receipt"
    case .typing:
      return "Typing..."
    case .adminRoster, .adminAction:
      return "Group update"
    }
  }

  private func cachedPlaintext(from payload: MLSMessagePayload) -> String? {
    switch payload.messageType {
    case .text:
      return payload.text
    case .reaction:
      if let reaction = payload.reaction {
        let verb = (reaction.action == .add) ? "Reacted" : "Removed reaction"
        return "\(verb) \(reaction.emoji)"
      }
      return "Reaction"
    case .readReceipt:
      return "Read receipt"
    case .typing:
      return "Typing"
    case .adminRoster:
      return "Roster update"
    case .adminAction:
      return "Admin update"
    }
  }

  private func notificationDisplayText(from cachedPlaintext: String) -> String {
    // 1. Try decoding as JSON (New Format)
    if cachedPlaintext.starts(with: "{"),
      let data = cachedPlaintext.data(using: .utf8),
      let payload = try? MLSMessagePayload.decodeFromJSON(data)
    {
      return notificationDisplayText(from: payload)
    }

    // For non-JSON plaintext, return as-is
    return cachedPlaintext
  }

  /// Returns true if this messageID was already persisted as a locally-originated message.
  ///
  /// This is used to ignore "own-message echo" deliveries (e.g., push fanout of a message
  /// this device just sent). Attempting to MLS-decrypt such echoes can trigger OpenMLS
  /// `SecretReuseError` because the sender generation was already consumed during encryption.
  public func isMessageSentByThisDevice(
    messageID: String,
    userDid: String,
    useEphemeralAccess: Bool = true
  ) async -> Bool {
    do {
      // CRITICAL: Auto-detect inactive users and force ephemeral access
      let isActiveUser = await databaseManager.isActiveUser(userDid)
      let shouldUseEphemeral = useEphemeralAccess || !isActiveUser

      let database: DatabasePool
      if shouldUseEphemeral {
        database = try await databaseManager.getEphemeralDatabasePool(for: userDid)
      } else {
        database = try await databaseManager.getDatabasePool(for: userDid)
      }

      if let message = try await storage.fetchMessage(
        messageID: messageID,
        currentUserDID: userDid,
        database: database
      ) {
        return message.isSent
      }
    } catch {
      logger.debug(
        "[CACHE] Failed to check locally-sent state for message \(messageID): \(error.localizedDescription)"
      )
    }
    return false
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

  /// Discard a pending external join after server rejection.
  ///
  /// CRITICAL: Call this when the delivery service rejects an external commit.
  /// This cleans up the local group state, signature keys, and manifest entries
  /// to prevent orphaned cryptographic material.
  ///
  /// - Parameters:
  ///   - userDid: User's decentralized identifier
  ///   - groupId: Group identifier from the rejected external commit
  /// - Throws: MLSError if cleanup fails
  public func discardPendingExternalJoin(userDid: String, groupId: Data) async throws {
    let context = try await getContext(for: userDid)
    try context.discardPendingExternalJoin(groupId: groupId)
    logger.info("✅ [MLSCoreContext] Discarded pending external join for group \(groupId.hexEncodedString())")
  }

  /// Check if a group exists in local MLS storage.
  public func groupExists(userDid: String, groupId: Data) async -> Bool {
    (try? await getContext(for: userDid).groupExists(groupId: groupId)) ?? false
  }

  /// Get detailed information about pending proposals for a group
  ///
  /// Use this to inspect proposals before committing them.
  ///
  /// - Parameters:
  ///   - userDid: User's decentralized identifier
  ///   - groupId: MLS group identifier
  /// - Returns: List of pending proposal details
  /// - Throws: MLSError if context not found or query fails
  public func getPendingProposalDetails(userDid: String, groupId: Data) async throws -> [PendingProposalDetail] {
    let context = try await getContext(for: userDid)
    return try context.getPendingProposalDetails(groupId: groupId)
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
