import Combine
import CatbirdMLS
import Foundation
import OSLog
import Petrel
import Synchronization

/// Modern MLS wrapper using UniFFI bindings
/// This replaces the legacy C FFI approach with type-safe Swift APIs
public actor MLSClient {
  /// Shared singleton instance - MLS context must persist across app lifetime
  /// to maintain group state in memory and keychain persistence
  public static let shared = MLSClient()

  // MARK: - Emergency Suspension Close (0xdead10cc Prevention)

  /// MLSClient delegates UniFFI `MlsContext` ownership to `MLSCoreContext`.
  /// Keep only suspension flags here so existing lifecycle callers still block sender-side FFI work.
  private struct EmergencyState: @unchecked Sendable {
    var cacheInvalidated = false
    var suspensionInProgress = false
  }

  private static let emergencyState = Mutex(EmergencyState())

  public nonisolated static var isSuspensionInProgress: Bool {
    emergencyState.withLock { $0.suspensionInProgress }
  }

  /// Set suspension flag (idempotent). Call synchronously when scenePhase becomes inactive/background.
  public nonisolated static func markSuspensionInProgress(reason: String = "unknown") {
    emergencyState.withLock { $0.suspensionInProgress = true }
    MLSCoreContext.markSuspensionInProgress()
    MLSSuspensionFlightRecorder.shared.record(
      .suspensionPrepare,
      details: "MLSClient markSuspensionInProgress: \(reason)",
      process: "app"
    )
  }

  /// Fire sqlite3_interrupt() on all shared MLS contexts WITHOUT closing them.
  /// Safe to call from any thread — InterruptHandle is Send+Sync and doesn't require the Rust Mutex.
  /// Call this SYNCHRONOUSLY in handleScenePhaseChange before the async emergency close Task,
  /// so in-flight sqlite3_step calls abort immediately even if iOS suspends before the Task runs.
  public nonisolated static func interruptAllContexts() {
    MLSCoreContext.interruptAllContexts()
  }

  /// Clear suspension flag (idempotent). Call when the app returns to foreground, or when BGTasks
  /// need to run MLS work while the app is backgrounded.
  public nonisolated static func clearSuspensionFlag(reason: String = "unknown") {
    emergencyState.withLock { $0.suspensionInProgress = false }
    MLSCoreContext.clearSuspensionFlag()
    MLSSuspensionFlightRecorder.shared.record(
      .resumeFromSuspension,
      details: "MLSClient clearSuspensionFlag: \(reason)",
      process: "app"
    )
  }

  /// Emergency synchronous close of all Rust MLS contexts.
  /// Safe to call from any thread and without actor isolation.
  public nonisolated static func emergencyCloseAllContexts(reason: String = "unknown") {
    emergencyState.withLock { state in
      state.suspensionInProgress = true
      state.cacheInvalidated = true
    }

    MLSSuspensionFlightRecorder.shared.record(
      .flushStarted,
      details: "MLSClient emergencyCloseAllContexts(\(reason)): delegating to MLSCoreContext",
      process: "app"
    )

    MLSCoreContext.emergencyCloseAllContexts()

    MLSSuspensionFlightRecorder.shared.record(
      .flushCompleted,
      details: "MLSClient emergencyCloseAllContexts: delegated close complete",
      process: "app"
    )
  }

  /// Per-user generation token.
  /// Bump this before account switches / storage resets so in-flight tasks fail fast.
  private var generations: [String: UInt64] = [:]

  /// Per-user API clients for server operations
  private var apiClients: [String: MLSAPIClient] = [:]

  /// Per-user device managers for multi-device support
  private var deviceManagers: [String: MLSDeviceManager] = [:]

  /// Per-user recovery managers for silent auto-recovery from desync
  private var recoveryManagers: [String: MLSRecoveryManager] = [:]

  /// Deduplicates concurrent External Commit joins per (user, conversation).
  private var inFlightExternalCommits: [String: Task<Data, any Error>] = [:]

  /// Optional app-provided coordinator for storage maintenance flows.
  private var storageMaintenanceCoordinator: MLSStorageMaintenanceCoordinating?

  private let logger = Logger(
    subsystem: Bundle.main.bundleIdentifier ?? "blue.catbird", category: "MLSClient")
  private var cancellables = Set<AnyCancellable>()

  // MARK: - Initialization

  private init() {
    logger.info("🔐 MLSClient initialized with per-user context isolation")

    // Configure keychain access group for shared access between app and extensions
    // This allows NotificationServiceExtension to access MLS encryption keys
    #if os(iOS)
      #if targetEnvironment(simulator)
        // Simulator bug: Keychain access groups don't work reliably
        // Use nil to fall back to default keychain (no sharing, but prevents -34018 error)
        MLSKeychainManager.shared.accessGroup = nil
        logger.warning("⚠️ Running on simulator - keychain access group disabled (sharing won't work)")
      #else
        // Device: shared access between app and extensions (must match Keychain Sharing entitlement).
        let accessGroup = MLSKeychainManager.resolvedAccessGroup(suffix: "blue.catbird.shared")
        MLSKeychainManager.shared.accessGroup = accessGroup
        logger.debug("🔑 Configured keychain access group: \(accessGroup ?? "nil")")
      #endif
    #endif

    // Lifecycle observers are handled by AppState/AuthManager, not here
    // See setupLifecycleObservers() documentation for rationale
    logger.debug("📍 [MLSClient.init] Complete")
  }

  /// Configure the MLS API client (Phase 3/4)
  /// Must be called before using Welcome validation or bundle monitoring
  public func configure(for userDID: String, apiClient: MLSAPIClient, atProtoClient: ATProtoClient) {
    let normalizedDID = normalizeUserDID(userDID)
    self.apiClients[normalizedDID] = apiClient

    // Create managers for this specific user context
    self.deviceManagers[normalizedDID] = MLSDeviceManager(
      apiClient: atProtoClient, mlsAPIClient: apiClient, mlsClient: self)

    self.recoveryManagers[normalizedDID] = MLSRecoveryManager(
      mlsClient: self, mlsAPIClient: apiClient)

    logger.info(
      "✅ MLSClient configured for user \(normalizedDID.prefix(20))... with API client, device manager, and recovery manager"
    )
  }

  /// Invalidate cached API client for a user (called after E2E re-login)
  /// This ensures subsequent MLS operations use the fresh client with new tokens
  public func invalidateCachedClient(for userDID: String) async {
    let normalizedDID = normalizeUserDID(userDID)
    apiClients.removeValue(forKey: normalizedDID)
    deviceManagers.removeValue(forKey: normalizedDID)
    recoveryManagers.removeValue(forKey: normalizedDID)
    await MLSCoreContext.shared.removeContext(for: normalizedDID)
    logger.info("[E2E] Invalidated cached MLS clients and context for \(normalizedDID.prefix(20))...")
  }

  /// Provide an app-level storage maintenance coordinator (optional).
  public func setStorageMaintenanceCoordinator(_ coordinator: MLSStorageMaintenanceCoordinating?) {
    storageMaintenanceCoordinator = coordinator
  }

  /// Get the recovery manager for error handling
  public func recovery(for userDID: String) -> MLSRecoveryManager? {
    let normalizedDID = normalizeUserDID(userDID)
    return recoveryManagers[normalizedDID]
  }

  /// Ensure device is registered and get MLS DID
  /// Must be called before creating key packages
  public func ensureDeviceRegistered(userDid: String) async throws -> String {
    let normalizedDID = normalizeUserDID(userDid)
    guard let deviceManager = deviceManagers[normalizedDID] else {
      logger.error(
        "❌ Device manager not configured for user \(normalizedDID) - call configure() first")
      throw MLSError.configurationError
    }
    return try await deviceManager.ensureDeviceRegistered(userDid: userDid)
  }

  /// Get device info for key package uploads for a specific user
  /// - Parameter userDID: The user's DID
  /// - Returns: Device info tuple or nil if not registered
  public func getDeviceInfo(for userDID: String) async -> (
    deviceId: String, mlsDid: String, deviceUUID: String?
  )? {
    let normalizedDID = normalizeUserDID(userDID)
    return await deviceManagers[normalizedDID]?.getDeviceInfo(for: userDID)
  }

  /// Get the MLS client identity for a user on this device.
  /// Returns `did#deviceUUID` format for proper multi-device MLS support.
  ///
  /// - Parameter userDID: The user's DID
  /// - Returns: Client identity or nil if device not registered
  public func getClientIdentity(for userDID: String) async -> String? {
    let normalizedDID = normalizeUserDID(userDID)
    return await deviceManagers[normalizedDID]?.getClientIdentity(for: userDID)
  }

  /// Force re-registration of device with fresh key packages
  /// Used by recovery manager for silent recovery from desync
  public func reregisterDevice(for userDid: String) async throws -> String {
    let normalizedDID = normalizeUserDID(userDid)
    guard let deviceManager = deviceManagers[normalizedDID] else {
      logger.error(
        "❌ Device manager not configured for user \(normalizedDID) - call configure() first")
      throw MLSError.configurationError
    }
    return try await deviceManager.reregisterDevice(userDid: userDid)
  }

  /// Execute FFI operation on background thread to prevent MainActor blocking
  private func runFFI<T: Sendable>(_ operation: @Sendable @escaping () throws -> T) async throws
    -> T
  {
    // Pre-dispatch check: fail fast before queuing work that may execute during suspension.
    // This prevents the race where suspension is signaled after the dispatch is queued
    // but before the block starts executing on the background thread.
    guard !Self.isSuspensionInProgress else {
      throw MLSError.contextCreationBlocked(
        reason: "App is transitioning to background - MLS operations suspended (pre-dispatch)")
    }
    return try await withCheckedThrowingContinuation { continuation in
      DispatchQueue.global(qos: .userInitiated).async {
        // Double-check suspension flag INSIDE the dispatch block, right before executing.
        // This catches the race where suspension is signaled after the pre-dispatch check
        // but before the actual SQLCipher operation starts on this background thread.
        guard !Self.isSuspensionInProgress else {
          continuation.resume(
            throwing: MLSError.contextCreationBlocked(
              reason: "App is transitioning to background - MLS operations suspended"))
          return
        }
        do {
          let result = try operation()
          continuation.resume(returning: result)
        } catch {
          continuation.resume(throwing: error)
        }
      }
    }
  }

  /// Execute FFI operation with automatic recovery from poisoned context
  /// If the context is poisoned (previous operation panicked), clears it and retries once
  private func runFFIWithRecoveryLocked<T: Sendable>(
    for userDID: String,
    operation: @Sendable @escaping (MlsContext) throws -> T
  ) async throws -> T {
    var context = try await getContext(for: userDID)

    for attempt in 1...2 {
      do {
        return try await runFFI {
          try operation(context)
        }
      } catch let error as MlsError {
        if isPoisonedContextError(error) && attempt == 1 {
          logger.warning(
            "⚠️ [MLSClient] Context poisoned for user \(userDID.prefix(20))..., clearing and retrying (attempt \(attempt))"
          )
          await clearPoisonedContext(for: userDID)
          context = try await getContext(for: userDID)
          continue
        }
        throw error
      }
    }

    throw MLSError.operationFailed
  }

  /// Execute FFI operation with automatic recovery from poisoned context,
  /// serialized per-user and coordinated cross-process to avoid ratchet/db desync.
  private func runFFIWithRecovery<T: Sendable>(
    for userDID: String,
    operation: @Sendable @escaping (MlsContext) throws -> T
  ) async throws -> T {
    let normalizedDID = normalizeUserDID(userDID)
    let generation = currentGeneration(for: normalizedDID)

    return try await withMLSUserPermit(for: normalizedDID) {
      try assertGeneration(generation, for: normalizedDID)

      // No advisory lock needed - SQLite WAL handles concurrent access
      // Cross-process coordination uses `MLSStateChangeNotifier` / `MLSNotificationCoordinator`

      try assertGeneration(generation, for: normalizedDID)

      // Direct FFI call
      let result = try await self.runFFIWithRecoveryLocked(for: normalizedDID, operation: operation)

      try assertGeneration(generation, for: normalizedDID)
      return result
    }
  }

  /// Normalize user DID to ensure consistent context lookup
  /// Prevents multiple contexts for the same user due to whitespace/encoding differences
  private func normalizeUserDID(_ userDID: String) -> String {
    return userDID.trimmingCharacters(in: .whitespacesAndNewlines)
  }

  private func currentGeneration(for normalizedDID: String) -> UInt64 {
    generations[normalizedDID] ?? 0
  }

  public func bumpGeneration(for userDID: String) {
    let normalizedDID = normalizeUserDID(userDID)
    let next = (generations[normalizedDID] ?? 0) &+ 1
    generations[normalizedDID] = next
    logger.debug("🔁 [MLSClient] Bumped generation for \(normalizedDID.prefix(20))... → \(next)")
  }

  private func assertGeneration(_ captured: UInt64, for normalizedDID: String) throws {
    if currentGeneration(for: normalizedDID) != captured {
      throw CancellationError()
    }
  }

  /// Check if an MlsError indicates a poisoned/unrecoverable context
  /// This happens when a previous FFI operation panicked while holding the Mutex lock
  private func isPoisonedContextError(_ error: MlsError) -> Bool {
    if case .ContextNotInitialized = error {
      return true
    }
    return false
  }

  /// Clear a poisoned context from the cache to allow recovery on next attempt
  /// Call this when FFI operations fail with ContextNotInitialized
  private func clearPoisonedContext(for userDID: String) async {
    let normalizedDID = normalizeUserDID(userDID)
    await MLSCoreContext.shared.removeContext(for: normalizedDID)
    logger.warning(
      "🔄 [MLSClient] Cleared poisoned shared context for user: \(normalizedDID.prefix(20))... (will recreate on next operation)"
    )
  }

  /// Get or create a context for a specific user.
  private func getContext(for userDID: String) async throws -> MlsContext {
    let normalizedDID = normalizeUserDID(userDID)

    // Block creation/use while app is suspending to avoid 0xdead10cc termination.
    if Self.isSuspensionInProgress {
      logger.warning("🚫 [0xdead10cc-FIX] MLSClient.getContext BLOCKED - suspension in progress")
      throw MLSError.contextCreationBlocked(
        reason: "App is transitioning to background - MLS operations suspended"
      )
    }

    // If contexts were closed out-of-band (nonisolated emergency close), clear the shared cache.
    let needsCacheClear = Self.emergencyState.withLock { state in
      if state.cacheInvalidated {
        state.cacheInvalidated = false
        return true
      }
      return false
    }
    if needsCacheClear {
      await MLSCoreContext.shared.clearAllContexts()
    }

    guard apiClients[normalizedDID] != nil else {
      logger.error("❌ MLS API client not configured for user \(normalizedDID.prefix(20))...")
      throw MLSError.configurationError
    }

    return try await MLSCoreContext.shared.getContext(for: normalizedDID)
  }

  /// Reload MLS context from storage for non-destructive recovery
  /// This clears the in-memory context and recreates it, forcing a reload from SQLite
  /// Returns the number of bundles found after reload
  private func reloadContextFromStorage(for userDID: String) async throws -> UInt64 {
    let normalizedDID = normalizeUserDID(userDID)
    logger.info(
      "🔄 [Recovery] Attempting non-destructive context reload for user: \(normalizedDID.prefix(20))..."
    )

    try await MLSCoreContext.shared.reloadContext(for: normalizedDID)
    let newContext = try await getContext(for: normalizedDID)
    logger.debug("   ✅ Created fresh context from SQLite storage")

    // Check if bundles were recovered
    let bundleCount = UInt64(try newContext.getKeyPackageBundleCount())
    logger.info("   📊 Bundle count after reload: \(bundleCount)")

    if bundleCount > 0 {
      logger.info("✅ [Recovery] Non-destructive recovery successful! Found \(bundleCount) bundles")
    } else {
      logger.warning(
        "⚠️ [Recovery] Non-destructive recovery found 0 bundles - may need full re-registration")
    }

    return bundleCount
  }

  // MARK: - Device Status

  /// Check if a device is currently registered for the given user
  /// - Parameter userDID: The user's DID
  /// - Returns: True if device info exists locally
  public nonisolated func isDeviceRegistered(for userDID: String) -> Bool {
    // This is a convenient non-async check against shared state if possible,
    // but MLSDeviceManager encapsulates this. We'll need to check via device manager access.
    // Since we can't easily access actor state synchronously/non-isolated,
    // we'll rely on the caller to await the actor call if needed, OR
    // we can check our own map if we trust it's up to date (it is).
    // Note: This access is racy if we don't await, but `deviceManagers` is an actor property.
    // For a `nonisolated` implementation, we'd need a separate thread-safe store.
    // Instead, we'll make this async.
    return false // Placeholder - see async version
  }

  /// Check if a device is currently registered for the given user (async)
  public func isDeviceRegisteredAsync(for userDID: String) -> Bool {
    let normalizedDID = normalizeUserDID(userDID)
    // We check if we have a device manager and if it has device info
    guard let manager = deviceManagers[normalizedDID] else { return false }
    // We can't synchronously peek into the manager actor, so we return true if manager exists
    // and rely on the manager's methods for details.
    // Actually, MLSClient *initializes* the manager, but the manager manages the persistence.
    // Let's rely on checking `deviceInfoByUser` in UserDefaults which is the source of truth,
    // but that's inside MLSDeviceManager.
    //
    // Simplified approach: If we have a device manager confgured, we assume the POTENTIAL for registration.
    // But for the specific "0 bundle" check, we need to know if we *think* we are registered.
    //
    // Let's add a helper to MLSDeviceManager first, but for now we'll route via MLSClient.
    return deviceManagers[normalizedDID] != nil
  }

  // MARK: - Key Package Management

  /// Create a batch of key packages in a single transaction
  /// This prevents lock contention and race conditions during registration
  public func batchCreateKeyPackages(for userDID: String, identity: String, count: Int) async throws -> [Data] {
    logger.info("🔐 [MLSClient] Creating batch of \(count) key packages for \(identity.prefix(20))...")
    
    // Validate count
    guard count > 0 else { return [] }
    let safeCount = min(count, 100) // Cap at 100 to prevent timeouts
    
    let identityBytes = Data(identity.utf8)
    
    // Execute all creations under ONE lock acquisition
    return try await runFFIWithRecovery(for: userDID) { ctx in
      var packages: [Data] = []
      for i in 0..<safeCount {
        // Create package
        let package = try ctx.createKeyPackage(identityBytes: identityBytes)
        packages.append(package.keyPackageData)
      }
      
      // Force a database sync after the batch (checkpoint WAL)
      // This ensures all packages are durably persisted
      try ctx.syncDatabase()
      
      return packages
    }
  }

  // MARK: - Group Management

  /// Create a new MLS group using client identity (did#deviceUUID)
  /// Each device is a unique MLS leaf node for proper multi-device support.
  /// - Parameters:
  ///   - userDID: The user's DID
  ///   - configuration: Group configuration (security parameters + optional metadata)
  /// - Returns: The group ID as raw bytes
  public func createGroup(for userDID: String, configuration: MLSGroupConfiguration = .default)
    async throws -> Data
  {
    logger.info("📍 [MLSClient.createGroup] START - user: \(userDID.prefix(20), privacy: .private)")

    // Get client identity (did#deviceUUID) for this device
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.createGroup] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }

    logger.debug(
      "[MLSClient.createGroup] Client identity: '\(clientIdentity)' (length: \(clientIdentity.count))"
    )

    // Log bundle count BEFORE group creation
    let context = try await getContext(for: userDID)
    if let bundleCount = try? context.getKeyPackageBundleCount() {
      logger.debug("[MLSClient.createGroup] Bundle count BEFORE group creation: \(bundleCount)")
      if bundleCount == 0 {
        logger.error(
          "🚨 [MLSClient.createGroup] CRITICAL: Context has 0 bundles before group creation!")
        logger.error(
          "   This indicates bundles were lost between key package creation and group creation")
      }
    }

    // Use client identity (did#deviceUUID) as MLS credential identity
    // This allows each device to be a unique leaf node in the MLS tree
    let identityBytes = Data(clientIdentity.utf8)

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.createGroup(identityBytes: identityBytes, config: configuration)
      }
      logger.info(
        "✅ [MLSClient.createGroup] Group created - ID: \(result.groupId.hexEncodedString().prefix(16))"
      )

      // 🔒 CRITICAL FIX: Force database sync after group creation
      // This ensures the new group state is durably persisted before any subsequent operations.
      // Without this, if context gets recreated (via clearPoisonedContext) before WAL checkpoint,
      // the group would be lost and groupExists() would return false, triggering a spurious
      // External Commit rejoin for the creator - which breaks the invitee's epoch.
      do {
        try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.syncDatabase()
        }
        logger.info("✅ [MLSClient.createGroup] Database synced after group creation")
      } catch {
        logger.error("⚠️ [MLSClient.createGroup] Database sync failed: \(error.localizedDescription)")
        // Continue anyway - the group was created, sync failure is not fatal but may cause issues
      }

      return result.groupId
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.createGroup] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Create a new MLS group with a predetermined `groupId`.
  ///
  /// First-responder bootstrap path (spec §8.5): the auto-reset transaction
  /// stages a `pendingNewGroupId` and emits `GroupResetEvent`. Recipients
  /// race to bootstrap that empty group. To make the race converge, every
  /// candidate must build their local MLS state at the SAME `groupId`
  /// (the staged `pendingNewGroupId`) so the winner's Welcome can deserialize
  /// for every recipient. Use `createGroup` (not this overload) for normal
  /// admin-initiated creation, where the `groupId` should be FFI-generated.
  ///
  /// - Parameters:
  ///   - userDID: The user's DID
  ///   - groupId: Predetermined raw MLS group identifier bytes (NOT hex)
  ///   - configuration: Group configuration (security parameters + optional metadata)
  /// - Returns: The full GroupCreationResult (groupId echoes input)
  public func createGroupWithId(
    for userDID: String,
    groupId: Data,
    configuration: MLSGroupConfiguration = .default
  ) async throws -> GroupCreationResult {
    logger.info(
      "📍 [MLSClient.createGroupWithId] START - user: \(userDID.prefix(20), privacy: .private), groupId: \(groupId.hexEncodedString().prefix(16))"
    )

    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.createGroupWithId] Device not registered")
      throw MLSError.configurationError
    }

    let identityBytes = Data(clientIdentity.utf8)

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.createGroupWithId(identityBytes: identityBytes, groupId: groupId, config: configuration)
      }
      logger.info(
        "✅ [MLSClient.createGroupWithId] Group created - ID: \(result.groupId.hexEncodedString().prefix(16))"
      )

      do {
        try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.syncDatabase()
        }
        logger.info("✅ [MLSClient.createGroupWithId] Database synced after group creation")
      } catch {
        logger.error(
          "⚠️ [MLSClient.createGroupWithId] Database sync failed: \(error.localizedDescription)")
      }

      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.createGroupWithId] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Create a new MLS group and return the full GroupCreationResult including metadata v2 artifacts.
  /// This is the v2 variant that exposes encrypted_metadata_blob, metadata_reference_json,
  /// and metadata_blob_locator fields for the metadata v2 flow.
  ///
  /// - Parameters:
  ///   - userDID: The user's DID
  ///   - configuration: Group configuration (security parameters + optional metadata)
  /// - Returns: The full GroupCreationResult from the Rust FFI
  public func createGroupV2(for userDID: String, configuration: MLSGroupConfiguration = .default)
    async throws -> GroupCreationResult
  {
    logger.info("📍 [MLSClient.createGroupV2] START - user: \(userDID.prefix(20), privacy: .private)")

    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.createGroupV2] Device not registered")
      throw MLSError.configurationError
    }

    let identityBytes = Data(clientIdentity.utf8)

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.createGroup(identityBytes: identityBytes, config: configuration)
      }
      logger.info(
        "✅ [MLSClient.createGroupV2] Group created - ID: \(result.groupId.hexEncodedString().prefix(16))"
      )

      // Log metadata v2 artifacts if present
      if let blob = result.encryptedMetadataBlob {
        logger.info(
          "📋 [MLSClient.createGroupV2] Metadata v2 blob present: \(blob.count) bytes, locator: \(result.metadataBlobLocator ?? "nil")"
        )
      }

      // Force database sync
      do {
        try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.syncDatabase()
        }
        logger.info("✅ [MLSClient.createGroupV2] Database synced after group creation")
      } catch {
        logger.error("⚠️ [MLSClient.createGroupV2] Database sync failed: \(error.localizedDescription)")
      }

      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.createGroupV2] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  // MARK: - Group Metadata

  /// Get decrypted metadata from MLS group context extension
  /// - Parameters:
  ///   - userDID: The user's DID
  ///   - groupId: Raw group ID bytes
  /// - Returns: Decoded group metadata, or nil if no metadata is set
  public func getGroupMetadata(for userDID: String, groupId: Data) async throws -> GroupMetadataPayload? {
    let metadataBytes = try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.getGroupMetadata(groupId: groupId)
    }

    guard !metadataBytes.isEmpty else {
      return nil
    }

    do {
      return try JSONDecoder().decode(GroupMetadataPayload.self, from: metadataBytes)
    } catch {
      logger.error("❌ [MLSClient.getGroupMetadata] Failed to decode metadata JSON: \(error.localizedDescription)")
      return nil
    }
  }

  /// Update encrypted metadata in MLS group context extension
  /// - Parameters:
  ///   - userDID: The user's DID
  ///   - groupId: Raw group ID bytes
  ///   - name: New group name (nil to leave unchanged)
  ///   - description: New group description (nil to leave unchanged)
  /// - Returns: Commit data to send to server
  public func updateGroupMetadata(
    for userDID: String,
    groupId: Data,
    name: String?,
    description: String?
  ) async throws -> Data {
    let payload = GroupMetadataPayload(v: 1, name: name, description: description)
    let metadataJson = try JSONEncoder().encode(payload)

    let commitData = try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.updateGroupMetadata(groupId: groupId, metadataJson: metadataJson)
    }

    return commitData
  }

  /// Join an existing group using a welcome message (low-level with explicit identity)
  /// Use the convenience method without identity parameter for automatic bare DID usage
  public func joinGroup(
    for userDID: String, welcome: Data, identity: String,
    configuration: MLSGroupConfiguration = .default
  ) async throws -> Data {
    logger.info(
      "📍 [MLSClient.joinGroup] START - user: \(userDID.prefix(20), privacy: .private), identity: \(identity.prefix(30), privacy: .private), welcome size: \(welcome.count) bytes"
    )
    logger.debug("[MLSClient.joinGroup] Full userDID: '\(userDID, privacy: .private)' (length: \(userDID.count))")
    logger.debug("[MLSClient.joinGroup] Full identity: '\(identity, privacy: .private)' (length: \(identity.count))")

    // Phase 3 validation now occurs on the sender before the Welcome is uploaded.
    // Recipients proceed directly to processing since the server has already approved the Welcome.
    let identityBytes = Data(identity.utf8)

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.processWelcome(
          welcomeBytes: welcome, identityBytes: identityBytes, config: configuration)
      }
      logger.info(
        "✅ [MLSClient.joinGroup] Joined group - ID: \(result.groupId.hexEncodedString().prefix(16))"
      )

      // 🔒 FIX #2: Force database sync after Welcome processing
      // This ensures the new group state is durably persisted before any messages are sent/received
      // Without this, app restart could cause SecretReuseError from incomplete WAL checkpoint
      do {
        try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.syncDatabase()
        }
        logger.info("✅ [MLSClient.joinGroup] Database synced after Welcome processing")
      } catch {
        logger.error("⚠️ [MLSClient.joinGroup] Database sync failed: \(error.localizedDescription)")
        // Continue anyway - the group was joined, sync failure is not fatal
      }

      return result.groupId
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.joinGroup] FAILED: \(error.localizedDescription)")

      // 🔍 DIAGNOSTIC: If NoMatchingKeyPackage, log local hashes for comparison
      let errorStr = String(describing: error)
      if errorStr.contains("NoMatchingKeyPackage") || errorStr.contains("no matching key package") {
        logger.error(
          "🔍 [MLSClient.joinGroup] NoMatchingKeyPackage - Listing local manifest hashes...")

        do {
          let context = try await getContext(for: userDID)
          let localHashes = try context.debugListKeyPackageHashes()
          logger.error("🔍 Local manifest contains \(localHashes.count) key package hashes:")
          for (i, hash) in localHashes.prefix(10).enumerated() {
            logger.error("   [\(i)] \(hash)")
          }
          if localHashes.count > 10 {
            logger.error("   ... and \(localHashes.count - 10) more")
          }
          logger.error("🔍 Compare with the hash used in the Welcome (logged on creator side)")
        } catch {
          logger.error("🔍 Failed to list local hashes: \(error)")
        }
        
        // ⭐ CRITICAL FIX: Re-throw the original MlsError.NoMatchingKeyPackage
        // This allows initializeGroupFromWelcome to catch it and trigger External Commit fallback
        // Previously we wrapped this as MLSError.operationFailed, which lost the error type
        // and prevented the External Commit recovery path from executing
        throw error
      }

      throw MLSError.operationFailed
    }
  }

  /// Join an existing group using a welcome message with client identity (did#deviceUUID)
  /// Each device is a unique MLS leaf node for proper multi-device support.
  public func joinGroup(
    for userDID: String, welcome: Data, configuration: MLSGroupConfiguration = .default
  ) async throws -> Data {
    // Get client identity (did#deviceUUID) for this device
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.joinGroup] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }
    return try await joinGroup(
      for: userDID, welcome: welcome, identity: clientIdentity, configuration: configuration)
  }

  /// Join a group via External Commit using GroupInfo
  /// This allows joining without a Welcome message from an existing member
  /// Includes retry logic for transient deserialization errors (EndOfStream, truncated data)
  public func joinByExternalCommit(for userDID: String, convoId: String) async throws -> Data {
    let normalizedDID = normalizeUserDID(userDID)
    let dedupeKey = "\(normalizedDID)::\(convoId)"

    if let inFlight = inFlightExternalCommits[dedupeKey] {
      logger.info(
        "⏳ [MLSClient.joinByExternalCommit] Reusing in-flight External Commit for convoId: \(convoId, privacy: .private)"
      )
      return try await inFlight.value
    }

    let task = Task<Data, any Error> {
      try await self.joinByExternalCommitUncoalesced(
        for: userDID,
        convoId: convoId,
        normalizedDID: normalizedDID
      )
    }

    inFlightExternalCommits[dedupeKey] = task
    defer {
      if inFlightExternalCommits[dedupeKey] == task {
        inFlightExternalCommits[dedupeKey] = nil
      }
    }

    return try await task.value
  }

  private func joinByExternalCommitUncoalesced(
    for userDID: String,
    convoId: String,
    normalizedDID: String
  ) async throws -> Data {
    logger.info("📍 [MLSClient.joinByExternalCommit] START - user: \(userDID, privacy: .private), convoId: \(convoId, privacy: .private)")

    guard let apiClient = self.apiClients[normalizedDID] else {
      throw MLSError.configurationError
    }

    // Prevent stale account contexts from issuing external commits after account switch.
    guard await apiClient.isAuthenticatedAs(normalizedDID) else {
      logger.warning(
        "⏸️ [MLSClient.joinByExternalCommit] Skipping - \(normalizedDID.prefix(20))... is not the active authenticated account"
      )
      throw MLSError.operationFailed
    }

    // Get client identity (did#deviceUUID) for this device
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.joinByExternalCommit] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }

    // Defensive invariant: credential identity must match the user DID context.
    guard clientIdentity == normalizedDID else {
      logger.error(
        "❌ [MLSClient.joinByExternalCommit] Identity mismatch - userDID=\(normalizedDID), clientIdentity=\(clientIdentity)"
      )
      throw MLSError.invalidCredential(
        "External Commit identity mismatch for \(normalizedDID)")
    }

    let maxRetries = 3
    var lastError: Error?
    // Tracks the group_id from a successful `createExternalCommit` whose
    // server ACK is still pending. OpenMLS's external-commit builder writes
    // the new group to storage and overwrites any prior state immediately, so
    // every failure exit after `createExternalCommit` must call
    // `discardPendingExternalJoin` to restore "not joined" state. Cleared to
    // nil on server success (line ~return) and on retry after a successful
    // discard.
    var pendingGroupId: Data? = nil

    // Local helper: swallow discard errors (cleanup is best-effort — don't
    // mask the original failure reason with cleanup noise).
    func discardPendingIfNeeded() async {
      guard let gid = pendingGroupId else { return }
      do {
        try await self.discardPendingExternalJoin(for: userDID, groupId: gid)
      } catch {
        logger.warning(
          "⚠️ [MLSClient.joinByExternalCommit] discardPendingExternalJoin failed (non-fatal): \(error.localizedDescription)"
        )
      }
      pendingGroupId = nil
    }

    for attempt in 1...maxRetries {
      logger.info("🔄 [MLSClient.joinByExternalCommit] Attempt \(attempt)/\(maxRetries)")

      do {
        // 1. Fetch FRESH GroupInfo with metadata for each attempt
        let (groupInfo, epoch, expiresAt) = try await apiClient.getGroupInfo(convoId: convoId)

        // 2. Validate GroupInfo freshness
        if let expiresAt = expiresAt {
          if expiresAt < Date() {
            logger.error(
              "❌ [MLSClient.joinByExternalCommit] GroupInfo EXPIRED - expires: \(expiresAt), now: \(Date())"
            )
            logger.error("   GroupInfo epoch: \(epoch), size: \(groupInfo.count) bytes")
            logger.error("   External Commit cannot proceed with stale GroupInfo")

            // CRITICAL FIX: Request GroupInfo refresh from active members and wait before retrying
            // This allows recovery when GroupInfo TTL has expired
            logger.info(
              "🔄 [MLSClient.joinByExternalCommit] Requesting GroupInfo refresh from active members..."
            )
            var refreshRequested = false
            do {
              let (requested, activeMembers) = try await apiClient.groupInfoRefresh(
                convoId: convoId)
              refreshRequested = requested
              if requested {
                logger.info(
                  "✅ [MLSClient.joinByExternalCommit] GroupInfo refresh requested - \(activeMembers ?? 0) active members notified"
                )
              } else {
                logger.warning(
                  "⚠️ [MLSClient.joinByExternalCommit] No active members to refresh GroupInfo")
              }
            } catch {
              logger.warning(
                "⚠️ [MLSClient.joinByExternalCommit] Failed to request GroupInfo refresh: \(error.localizedDescription)"
              )
            }

            // If we have more retries and refresh was requested, wait and retry
            if attempt < maxRetries && refreshRequested {
              // Wait for active members to publish fresh GroupInfo
              // Use exponential backoff with jitter: base (2s/4s) + random jitter
              let baseWaitSeconds = 2 * attempt
              let jitterMs = UInt64.random(in: 0...1000)  // Up to 1s jitter
              logger.info(
                "🔄 [MLSClient.joinByExternalCommit] Waiting ~\(baseWaitSeconds)s for fresh GroupInfo before retry..."
              )
              try await Task.sleep(for: .seconds(baseWaitSeconds))
              try await Task.sleep(for: .milliseconds(jitterMs))
              continue  // Retry with fresh GroupInfo
            }

            // No more retries or refresh not possible - throw error
            throw MLSError.staleGroupInfo(
              convoId: convoId,
              message:
                "GroupInfo expired at \(expiresAt) (epoch \(epoch)) - refresh requested from active members"
            )
          } else {
            let remaining = expiresAt.timeIntervalSince(Date())
            logger.info(
              "✅ [MLSClient.joinByExternalCommit] GroupInfo valid - expires in \(Int(remaining))s")
          }
        } else {
          logger.warning(
            "⚠️ [MLSClient.joinByExternalCommit] No expiry on GroupInfo - proceeding cautiously")
        }

        // 3. Validate GroupInfo size (minimum 100 bytes for valid MLS GroupInfo)
        if groupInfo.count < 100 {
          logger.error(
            "❌ [MLSClient.joinByExternalCommit] GroupInfo suspiciously small: \(groupInfo.count) bytes"
          )
          logger.error("   Expected minimum ~100 bytes for valid MLS GroupInfo structure")
          logger.error("   First 32 bytes (hex): \(groupInfo.prefix(32).hexEncodedString())")
          throw MLSError.invalidGroupInfo(
            convoId: convoId,
            message: "GroupInfo too small: \(groupInfo.count) bytes (minimum 100 expected)"
          )
        }

        // 4. Check for base64 encoding issues (GroupInfo should be binary, not ASCII-only)
        let isAsciiOnly = groupInfo.allSatisfy { byte in
          (byte >= 0x20 && byte <= 0x7E) || byte == 0x0A || byte == 0x0D  // printable ASCII + newlines
        }
        if isAsciiOnly && groupInfo.count > 50 {
          logger.error(
            "❌ [MLSClient.joinByExternalCommit] GroupInfo appears to be text/base64 encoded!")
          logger.error("   Raw bytes appear to be ASCII text, not binary MLS data")
          logger.error("   This suggests base64 decoding was skipped somewhere")
          logger.error(
            "   First 100 chars: \(String(data: groupInfo.prefix(100), encoding: .utf8) ?? "n/a")")
          throw MLSError.invalidGroupInfo(
            convoId: convoId,
            message: "GroupInfo appears base64-encoded - decoding may have been skipped"
          )
        }

        logger.info(
          "📊 [MLSClient.joinByExternalCommit] GroupInfo validated: \(groupInfo.count) bytes, epoch \(epoch)"
        )

        // 5. Create External Commit using client identity (did#deviceUUID)
        let identityBytes = Data(clientIdentity.utf8)
        let result = try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.createExternalCommit(
            groupInfoBytes: groupInfo, identityBytes: identityBytes)
        }
        // `createExternalCommit` has already written the new group to storage
        // and overwritten any prior state under `result.groupId`. From here
        // until server ACK, that local state is unconfirmed — every non-return
        // exit must discard it.
        pendingGroupId = Data(result.groupId)

        // 6. Send Commit to Server — capture the returned server epoch
        // Get confirmation tag from the new local group state after external commit
        let tagData = try? await getConfirmationTag(for: userDID, groupId: Data(result.groupId))
        let tagB64 = tagData?.base64EncodedString()

        var serverEpochAfterCommit: UInt64? = nil
        do {
          let (_, serverNewEpoch) = try await apiClient.processExternalCommit(
            convoId: convoId,
            externalCommit: result.commitData,
            groupInfo: groupInfo,  // Send pre-commit GroupInfo so server can sync epoch
            confirmationTag: tagB64
          )
          serverEpochAfterCommit = UInt64(serverNewEpoch)
        } catch let apiError as MLSAPIError {
          if case .httpError(let statusCode, _) = apiError {
            if statusCode == 409 {
              // 409 = epoch conflict — another commit landed between our GroupInfo fetch
              // and our External Commit send. Discard pending state and retry with fresh GroupInfo.
              logger.warning(
                "⚠️ [MLSClient.joinByExternalCommit] 409 epoch conflict on attempt \(attempt) — retrying with fresh GroupInfo"
              )
              await discardPendingIfNeeded()
              if attempt < maxRetries {
                let waitSeconds = 1 * attempt
                let jitterMs = UInt64.random(in: 0...500)
                try await Task.sleep(for: .seconds(waitSeconds))
                try await Task.sleep(for: .milliseconds(jitterMs))
                continue
              }
            } else if statusCode == 403 {
              logger.warning(
                "⚠️ [MLSClient.joinByExternalCommit] External Commit rejected (HTTP 403) - requesting GroupInfo refresh"
              )
              await discardPendingIfNeeded()
              do {
                let (requested, activeMembers) = try await apiClient.groupInfoRefresh(convoId: convoId)
                if requested {
                  logger.info(
                    "✅ [MLSClient.joinByExternalCommit] GroupInfo refresh requested - \(activeMembers ?? 0) active members notified"
                  )
                } else {
                  logger.warning(
                    "⚠️ [MLSClient.joinByExternalCommit] No active members to refresh GroupInfo")
                }
              } catch {
                logger.warning(
                  "⚠️ [MLSClient.joinByExternalCommit] GroupInfo refresh request failed: \(error.localizedDescription)"
                )
              }

              if attempt < maxRetries {
                let waitSeconds = 2 * attempt
                logger.info(
                  "🔄 [MLSClient.joinByExternalCommit] Waiting ~\(waitSeconds)s before retry after 403..."
                )
                try await Task.sleep(for: .seconds(waitSeconds))
                continue
              }
            }
          }
          // Any other HTTP status (429 rate limit, 500 server error, etc.) or
          // non-HTTP api error — discard before propagating so we don't leave
          // stale post-commit state in `self.groups`.
          await discardPendingIfNeeded()
          throw apiError
        }

        logger.info(
          "✅ [MLSClient.joinByExternalCommit] Success - Joined group \(convoId) on attempt \(attempt)"
        )
        // Server accepted the commit — the local post-commit state is now
        // authoritative; clear the pending-discard guard so we don't undo it.
        pendingGroupId = nil

        // Compare local epoch vs server epoch to detect race conditions
        let groupIdData = Data(result.groupId)
        var localEpoch: UInt64 = 0
        do {
          localEpoch = try await getEpoch(for: userDID, groupId: groupIdData)
          if localEpoch > 1 {
            logger.warning("⚠️ [EPOCH WARNING] External Commit joined at epoch \(localEpoch)")
          }
          if let serverEpoch = serverEpochAfterCommit, serverEpoch != localEpoch {
            logger.warning(
              "⚠️ [EPOCH GAP] Local epoch \(localEpoch) != server epoch \(serverEpoch) after external commit for \(convoId.prefix(16))"
            )
            logger.warning(
              "   A concurrent commit occurred between GroupInfo fetch and external commit send"
            )
          }
        } catch {
          logger.debug("Could not fetch epoch for comparison: \(error.localizedDescription)")
        }

        // PHASE 3.1: Publish fresh GroupInfo after successful External Commit
        // Pass the server epoch so publishGroupInfo can skip if we're behind
        do {
          try await publishGroupInfo(
            for: userDID, convoId: convoId, groupId: groupIdData,
            knownServerEpoch: serverEpochAfterCommit
          )
          logger.info("✅ [MLSClient.joinByExternalCommit] GroupInfo published after External Commit")
        } catch {
          // Non-fatal: GroupInfo upload failure shouldn't block the join
          // Another member may publish it, or we can retry later
          logger.warning("⚠️ [MLSClient.joinByExternalCommit] Failed to publish GroupInfo: \(error.localizedDescription)")
        }

        return result.groupId

      } catch let error as MlsError {
        lastError = error
        let errorMessage = error.localizedDescription.lowercased()

        // Check if this is a retriable deserialization error
        let isDeserializationError =
          errorMessage.contains("endofstream") || errorMessage.contains("deseriali")
          || errorMessage.contains("truncat") || errorMessage.contains("invalid groupinfo")
          || errorMessage.contains("malformed")

        if isDeserializationError && attempt < maxRetries {
          // Exponential backoff with jitter: base * 2^(attempt-1) + random jitter
          // Jitter helps prevent thundering herd when multiple devices retry simultaneously
          let baseDelayMs = UInt64(100 * (1 << (attempt - 1)))
          let jitterMs = UInt64.random(in: 0...(baseDelayMs / 2))
          let totalDelayMs = baseDelayMs + jitterMs
          logger.warning(
            "⚠️ [MLSClient.joinByExternalCommit] Deserialization error on attempt \(attempt): \(error.localizedDescription)"
          )
          logger.info("   🔄 Retrying in \(totalDelayMs)ms with fresh GroupInfo...")
          // Discard any half-joined state before retrying so the next
          // attempt starts from a clean slate. (Usually `pendingGroupId`
          // is nil here — the retriable errors above typically fire inside
          // `ctx.createExternalCommit` itself, before we set it — but be
          // defensive in case a later-stage error path routes here.)
          await discardPendingIfNeeded()
          try await Task.sleep(for: .milliseconds(totalDelayMs))
          continue
        }

        // Non-retriable error or exhausted retries
        logger.error(
          "❌ [MLSClient.joinByExternalCommit] FAILED after \(attempt) attempt(s): \(error.localizedDescription)"
        )

        // 🔄 RECOVERY: Check if this error warrants device-level recovery
        // CRITICAL FIX: Mark as remote data error since GroupInfo comes from server
        // This prevents destructive local database wipe when server data is corrupted
        if let recoveryMgr = self.recoveryManagers[normalizedDID] {
          let errorMessage = error.localizedDescription.lowercased()
          let isServerDataCorruption =
            errorMessage.contains("invalidvectorlength") || errorMessage.contains("endofstream")
            || errorMessage.contains("malformed") || errorMessage.contains("truncat")

          if isServerDataCorruption {
            // Mark conversation as having corrupted server data - don't attempt recovery
            await recoveryMgr.markConversationServerCorrupted(
              convoId: convoId,
              errorMessage: "GroupInfo deserialization failed: \(error.localizedDescription)"
            )
            logger.error(
              "🚫 [MLSClient.joinByExternalCommit] Server data corrupted - NOT triggering local recovery"
            )
            logger.error("   GroupInfo for conversation \(convoId.prefix(16)) is malformed")
            logger.error("   Server team must investigate and republish valid GroupInfo")
          } else {
            // Only attempt recovery for LOCAL errors (e.g., key package issues)
            let recovered = await recoveryMgr.attemptRecoveryIfNeeded(
              for: error,
              userDid: userDID,
              convoIds: [convoId],
              isRemoteDataError: true  // GroupInfo is remote data
            )
            if recovered {
              logger.info(
                "🔄 [MLSClient.joinByExternalCommit] Recovery initiated - caller should retry")
            }
          }
        }

        // Terminal failure — discard any half-joined state so the next
        // caller sees "not joined" and can rejoin cleanly.
        await discardPendingIfNeeded()
        throw MLSError.operationFailed

      } catch {
        // Non-MlsError - don't retry
        lastError = error
        logger.error(
          "❌ [MLSClient.joinByExternalCommit] Non-MLS error: \(error.localizedDescription)")
        await discardPendingIfNeeded()
        throw error
      }
    }

    // Should never reach here, but handle gracefully
    logger.error("❌ [MLSClient.joinByExternalCommit] Exhausted all \(maxRetries) retries")
    await discardPendingIfNeeded()
    if let error = lastError {
      throw error
    }
    throw MLSError.operationFailed
  }

  /// Manually export epoch secret for a group
  /// Call this after creating the conversation record to ensure epoch secrets persist correctly
  public func exportEpochSecret(for userDID: String, groupId: Data) async throws {
    logger.info(
      "📍 [MLSClient.exportEpochSecret] Exporting epoch secret for group: \(groupId.hexEncodedString().prefix(16))"
    )
    try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.exportEpochSecret(groupId: groupId)
    }
    logger.info("✅ [MLSClient.exportEpochSecret] Successfully exported epoch secret")
  }

  /// Minimum valid GroupInfo size in bytes
  private static let minGroupInfoSize = 100

  /// Publish GroupInfo to the server to allow external joins
  /// Should be called after any operation that advances the epoch (add, remove, update, commit)
  /// CRITICAL: This function now throws errors - callers must handle failures
  /// - Throws: MLSError if export fails, validation fails, or upload fails
  public func publishGroupInfo(for userDID: String, convoId: String, groupId: Data, knownServerEpoch: UInt64? = nil) async throws {
    logger.info("📤 [MLSClient.publishGroupInfo] Starting for \(convoId)")

    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      logger.error(
        "❌ [MLSClient.publishGroupInfo] No API client configured for user \(normalizedDID)")
      throw MLSError.configurationError
    }

    // 1. Export GroupInfo from FFI
    // CRITICAL FIX: Use clientIdentity (did#deviceUUID) NOT bare userDID
    // The signer is registered with clientIdentity during key package creation,
    // so we must use the same identity format here to find it.
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error(
        "❌ [MLSClient.publishGroupInfo] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }
    let identityBytes = Data(clientIdentity.utf8)
    let groupInfoBytes = try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.exportGroupInfo(groupId: groupId, signerIdentityBytes: identityBytes)
    }

    // 2. Validate exported GroupInfo meets minimum size
    guard groupInfoBytes.count >= Self.minGroupInfoSize else {
      logger.error(
        "❌ [MLSClient.publishGroupInfo] Exported GroupInfo too small: \(groupInfoBytes.count) bytes"
      )
      throw MLSError.operationFailed
    }

    // 🔒 FIX #3: Validate GroupInfo format before upload using FFI
    // This catches serialization corruption BEFORE it reaches the server
    let isValid = try await runFFIWithRecovery(for: userDID) { ctx in
      ctx.validateGroupInfoFormat(groupInfoBytes: groupInfoBytes)
    }
    guard isValid else {
      logger.error(
        "❌ [MLSClient.publishGroupInfo] GroupInfo validation FAILED - NOT uploading corrupt data")
      logger.error("   Size: \(groupInfoBytes.count) bytes")
      logger.error("   First 32 bytes: \(groupInfoBytes.prefix(32).hexEncodedString())")
      throw MLSError.invalidGroupInfo(
        convoId: convoId,
        message: "Export produced invalid GroupInfo - validation failed before upload")
    }
    logger.info("✅ [MLSClient.publishGroupInfo] GroupInfo validated: \(groupInfoBytes.count) bytes")

    // 3. Get current epoch
    let epoch = try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.getEpoch(groupId: groupId)
    }

    // 3a. Guard against uploading stale GroupInfo (epoch behind server)
    if let serverEpoch = knownServerEpoch, epoch < serverEpoch {
      logger.warning("⚠️ [MLSClient.publishGroupInfo] Skipping upload - local epoch \(epoch) < server epoch \(serverEpoch) for \(convoId.prefix(16))")
      return
    }

    // 4. Upload to server (MLSAPIClient now has retry logic + verification)
    try await apiClient.updateGroupInfo(
      convoId: convoId, groupInfo: groupInfoBytes, epoch: Int(epoch))

    logger.info(
      "✅ [MLSClient.publishGroupInfo] Success - Published epoch \(epoch), size: \(groupInfoBytes.count) bytes"
    )
  }

  // MARK: - Member Management

  /// Add members to an existing group
  public func addMembers(for userDID: String, groupId: Data, keyPackages: [Data]) async throws
    -> AddMembersResult
  {
    logger.info(
      "📍 [MLSClient.addMembers] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16)), keyPackages: \(keyPackages.count)"
    )
    guard !keyPackages.isEmpty else {
      logger.error("❌ [MLSClient.addMembers] No key packages provided")
      throw MLSError.operationFailed
    }
    let keyPackageData = keyPackages.map { KeyPackageData(data: $0) }
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.addMembers(groupId: groupId, keyPackages: keyPackageData)
      }
      logger.info(
        "✅ [MLSClient.addMembers] Success - commit: \(result.commitData.count) bytes, welcome: \(result.welcomeData.count) bytes"
      )
      return result
    } catch let error as MlsError {
      // Extract the error message for specific error detection
      let errorMessage: String
      switch error {
      case .InvalidInput(let msg): errorMessage = msg
      case .OpenMlsError(let msg): errorMessage = msg
      default: errorMessage = error.localizedDescription
      }

      logger.error("❌ [MLSClient.addMembers] FAILED: \(errorMessage)")

      // Check for "member already in group" error to enable proper recovery
      // This allows the caller to unreserve key packages and show appropriate UI
      if errorMessage.lowercased().contains("member already in group")
        || errorMessage.lowercased().contains("already in group")
      {
        logger.warning(
          "⚠️ [MLSClient.addMembers] Member already exists - UI may be out of sync with MLS state")
        throw MLSError.memberAlreadyInGroup(member: "unknown")
      }

      throw MLSError.operationFailed
    }
  }

  /// Create a self-update commit to force epoch advancement
  /// This is used to prevent ratchet desynchronization when changing senders
  /// Returns commit data to be sent to server (no welcome for self-updates)
  ///
  /// - Parameters:
  ///   - userDID: User's DID
  ///   - groupId: Group identifier
  /// - Returns: AddMembersResult with commit data (welcomeData will be empty)
  /// - Throws: MLSError if the operation fails
  ///
  /// - Note: After sending commit to server, caller MUST call mergePendingCommit()
  public func selfUpdate(for userDID: String, groupId: Data) async throws -> AddMembersResult {
    logger.info(
      "📍 [MLSClient.selfUpdate] START - user: \(userDID.prefix(20)), groupId: \(groupId.hexEncodedString().prefix(16))"
    )
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.selfUpdate(groupId: groupId)
      }
      logger.info("✅ [MLSClient.selfUpdate] Success - commit: \(result.commitData.count) bytes")
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.selfUpdate] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Remove members from the group (cryptographically secure)
  /// This creates an MLS commit that advances the epoch and revokes decryption keys
  /// - Parameters:
  ///   - userDID: The DID of the current user
  ///   - groupId: The MLS group identifier
  ///   - memberIdentities: Array of member credential data (DID bytes) to remove
  /// - Returns: Commit data to send to server
  public func removeMembers(for userDID: String, groupId: Data, memberIdentities: [Data]) async throws
    -> Data
  {
    logger.info(
      "📍 [MLSClient.removeMembers] Removing \(memberIdentities.count) members from group \(groupId.hexEncodedString().prefix(16))"
    )

    do {
      let commitData = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.removeMembers(groupId: groupId, memberIdentities: memberIdentities)
      }

      logger.info(
        "✅ [MLSClient.removeMembers] Success - commit: \(commitData.count) bytes")
      return commitData
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.removeMembers] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Propose adding a member (does not commit)
  /// Use commit_pending_proposals() to commit accumulated proposals
  /// - Parameters:
  ///   - userDID: The DID of the current user
  ///   - groupId: The MLS group identifier
  ///   - keyPackageData: Serialized key package of member to add
  /// - Returns: ProposeResult with proposal message and reference
  public func proposeAddMember(for userDID: String, groupId: Data, keyPackageData: Data) async throws
    -> ProposeResult
  {
    logger.info(
      "📍 [MLSClient.proposeAddMember] Creating add proposal for group \(groupId.hexEncodedString().prefix(16))"
    )

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.proposeAddMember(groupId: groupId, keyPackageData: keyPackageData)
      }

      logger.info(
        "✅ [MLSClient.proposeAddMember] Success - message: \(result.proposalMessage.count) bytes"
      )
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.proposeAddMember] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Propose removing a member (does not commit)
  /// Use commit_pending_proposals() to commit accumulated proposals
  /// - Parameters:
  ///   - userDID: The DID of the current user
  ///   - groupId: The MLS group identifier
  ///   - memberIdentity: DID bytes of member to remove
  /// - Returns: ProposeResult with proposal message and reference
  public func proposeRemoveMember(for userDID: String, groupId: Data, memberIdentity: Data) async throws
    -> ProposeResult
  {
    logger.info(
      "📍 [MLSClient.proposeRemoveMember] Creating remove proposal for member"
    )

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.proposeRemoveMember(groupId: groupId, memberIdentity: memberIdentity)
      }

      logger.info(
        "✅ [MLSClient.proposeRemoveMember] Success - message: \(result.proposalMessage.count) bytes"
      )
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.proposeRemoveMember] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Propose self-update (does not commit)
  /// Use commit_pending_proposals() to commit accumulated proposals
  /// - Parameters:
  ///   - userDID: The DID of the current user
  ///   - groupId: The MLS group identifier
  /// - Returns: ProposeResult with proposal message and reference
  public func proposeSelfUpdate(for userDID: String, groupId: Data) async throws -> ProposeResult {
    logger.info(
      "📍 [MLSClient.proposeSelfUpdate] Creating self-update proposal for group \(groupId.hexEncodedString().prefix(16))"
    )

    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.proposeSelfUpdate(groupId: groupId)
      }

      logger.info(
        "✅ [MLSClient.proposeSelfUpdate] Success - message: \(result.proposalMessage.count) bytes"
      )
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.proposeSelfUpdate] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Delete a group from MLS storage
  public func deleteGroup(for userDID: String, groupId: Data) async throws {
    logger.info(
      "📍 [MLSClient.deleteGroup] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16))"
    )
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.deleteGroup(groupId: groupId)
      }
      logger.info("✅ [MLSClient.deleteGroup] Successfully deleted group")
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.deleteGroup] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  // MARK: - Message Encryption/Decryption

  /// Encrypt a message for the group
  public func encryptMessage(for userDID: String, groupId: Data, plaintext: Data) async throws
    -> EncryptResult
  {
    logger.info(
      "📍 [MLSClient.encryptMessage] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16)), plaintext: \(plaintext.count) bytes"
    )
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.encryptMessage(groupId: groupId, plaintext: plaintext)
      }
      logger.info(
        "✅ [MLSClient.encryptMessage] Success - ciphertext: \(result.ciphertext.count) bytes")
      return result
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.encryptMessage] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Decrypt a message from the group
  /// Returns the raw DecryptResult from FFI including plaintext, epoch, sequence, and sender credential
  /// Note: For most use cases, prefer MLSCoreContext.shared.decryptAndStore which also persists to database
  public func decryptMessage(
    for userDID: String, groupId: Data, ciphertext: Data, conversationID: String, messageID: String
  ) async throws -> DecryptResult {
    logger.info(
      "📍 [MLSClient.decryptMessage] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16)), messageID: \(messageID)"
    )

    // ═══════════════════════════════════════════════════════════════════════════
    // EPOCH FENCE PROTOCOL: Ensure in-memory state is synced with disk before decrypt
    // ═══════════════════════════════════════════════════════════════════════════
    // This is the CRITICAL fix for NSE<>App epoch desync. The NSE may have advanced
    // the MLS ratchet on disk while the app was backgrounded. We MUST check and
    // reload before attempting decryption to prevent forward secrecy violations.
    // ═══════════════════════════════════════════════════════════════════════════
    let contextWasReloaded = try await ensureContextSyncedBeforeDecrypt(
      for: userDID,
      groupId: groupId,
      conversationID: conversationID
    )
    if contextWasReloaded {
      logger.info("🔄 [EPOCH FENCE] Context reloaded before decrypt - proceeding with fresh state")
    }

    do {
      // Call FFI directly to get full DecryptResult with sender credential
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.decryptMessage(groupId: groupId, ciphertext: ciphertext)
      }
      
      let gen = MLSCoordinationStore.shared.currentGeneration
      logger.info(
        "✅ Decrypted message [Gen: \(gen)] - epoch: \(result.epoch), seq: \(result.sequenceNumber), plaintext: \(result.plaintext.count) bytes")
      
      // Extract sender DID for logging
      if let senderDID = String(data: result.senderCredential.identity, encoding: .utf8) {
        logger.debug("   Sender: \(senderDID.prefix(24))...")
      }
      
      // ═══════════════════════════════════════════════════════════════════════════
      // Update epoch checkpoint after successful decryption (for next staleness check)
      // ═══════════════════════════════════════════════════════════════════════════
      await MLSEpochCheckpoint.shared.recordEpoch(
        groupId: groupId,
        epoch: result.epoch,
        isNSE: false  // This is the main app
      )

      return result

    } catch let error as MlsError {
      // Extract message from error case
      let errorMessage: String
      switch error {
      case .DecryptionFailed(let msg): errorMessage = msg
      case .OpenMlsError(let msg): errorMessage = msg
      case .InvalidInput(let msg): errorMessage = msg
      default: errorMessage = error.localizedDescription
      }

      let errorMessageLower = errorMessage.lowercased()

      // Detect ratchet state desynchronization errors
      // These can occur when SSE connection fails and client state becomes stale
      if case .DecryptionFailed = error {
        // ANY DecryptionFailed during message processing could indicate state desync
        // OpenMLS errors like RatchetTypeError, InvalidSignature, SecretReuse are all wrapped as DecryptionFailed
        logger.error("🔴 RATCHET STATE DESYNC DETECTED: DecryptionFailed - likely stale MLS state")
        logger.error("   Original error: \(errorMessage)")
        logger.error("   This indicates the client's MLS state is out of sync with the group")
        logger.error(
          "   Recovery requires re-joining the group or processing a fresh Welcome message")
        throw MLSError.ratchetStateDesync(
          message: "DecryptionFailed - MLS state out of sync: \(errorMessage)")
      }

      // Also check message content for specific error keywords
      if errorMessageLower.contains("ratchet") || errorMessageLower.contains("invalidsignature")
        || errorMessageLower.contains("secretreuse") || errorMessageLower.contains("epoch")
      {
        logger.error("🔴 RATCHET STATE DESYNC DETECTED: \(errorMessage)")
        logger.error("   This indicates the client's MLS state is out of sync with the group")
        logger.error(
          "   Recovery requires re-joining the group or processing a fresh Welcome message")
        throw MLSError.ratchetStateDesync(message: errorMessage)
      }

      logger.error("❌ Decryption failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    } catch {
      logger.error("❌ Decryption failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }
  
  // MARK: - Epoch Fence Protocol

  /// Ensure in-memory MLS context is synced with disk before decryption
  /// This is the CRITICAL fix for NSE<>App epoch desync
  /// - Returns: true if context was reloaded (was stale), false if already synced
  private func ensureContextSyncedBeforeDecrypt(
    for userDID: String,
    groupId: Data,
    conversationID: String
  ) async throws -> Bool {
    let normalizedDID = normalizeUserDID(userDID)

    // Get current in-memory epoch for this group
    let inMemoryEpoch: UInt64
    do {
      inMemoryEpoch = try await getEpoch(for: normalizedDID, groupId: groupId)
    } catch {
      // Group doesn't exist in memory - will be initialized during decrypt
      logger.debug(
        "[EPOCH FENCE] No in-memory epoch for group \(conversationID.prefix(16))... - skipping fence"
      )
      return false
    }

    // Check against epoch checkpoint
    guard
      let checkResult = await MLSEpochCheckpoint.shared.checkStaleness(
        groupId: groupId,
        inMemoryEpoch: inMemoryEpoch
      )
    else {
      // No checkpoint exists - first time seeing this group, nothing to check
      logger.debug(
        "[EPOCH FENCE] No checkpoint for group \(conversationID.prefix(16))... - first time")
      return false
    }

    if !checkResult.wasStale {
      // Context is up to date
      logger.debug(
        "[EPOCH FENCE] Context synced for group \(conversationID.prefix(16))... (epoch: \(inMemoryEpoch))"
      )
      return false
    }

    // ⚠️ STALE CONTEXT DETECTED - Must reload before proceeding
    logger.warning(
      "🔄 [EPOCH FENCE] Stale context detected for group \(conversationID.prefix(16))...")
    logger.warning(
      "   In-memory epoch: \(checkResult.memoryEpoch), Disk epoch: \(checkResult.diskEpoch)")
    if let modifiedBy = checkResult.modifiedBy {
      logger.warning("   Last modified by: \(modifiedBy.rawValue)")
    }

    // Reload context from storage
    do {
      let bundleCount = try await reloadContextFromStorage(for: normalizedDID)
      logger.info("✅ [EPOCH FENCE] Context reloaded - bundle count: \(bundleCount)")

      // Verify epoch is now correct
      let newEpoch = try await getEpoch(for: normalizedDID, groupId: groupId)
      if newEpoch >= checkResult.diskEpoch {
        logger.info("✅ [EPOCH FENCE] Epoch now synced: \(newEpoch)")
      } else {
        logger.warning(
          "⚠️ [EPOCH FENCE] Epoch still behind after reload: memory=\(newEpoch), disk=\(checkResult.diskEpoch)"
        )
      }

      return true
    } catch {
      logger.error("❌ [EPOCH FENCE] Failed to reload context: \(error.localizedDescription)")
      // Don't throw - let the decrypt attempt proceed, it may still work or provide better error
      return false
    }
  }

  // MARK: - Key Package Management

  /// Create a key package for this user (low-level with explicit identity)
  /// Use the convenience method without identity parameter for automatic bare DID usage
  public func createKeyPackage(for userDID: String, identity: String) async throws -> Data {
    // NOTE: very noisy in CLI runs
    // logger.info(
    //   "📍 [MLSClient.createKeyPackage] START - user: \(userDID.prefix(20)), identity: \(identity.prefix(30))"
    // )
    // logger.debug("[MLSClient.createKeyPackage] Full userDID: '\(userDID)' (length: \(userDID.count))")
    // logger.debug("[MLSClient.createKeyPackage] Full identity: '\(identity)' (length: \(identity.count))")

    // RECOVERY CHECK: Check if we have a saved identity key in Keychain but not in Rust context
    // This happens on reinstall. If found, import it before creating key package.
    let identityKeyKey = "mls_identity_key_\(userDID)"
    if let savedKeyData = try? MLSKeychainManager.shared.retrieve(forKey: identityKeyKey) {
      let keyData = savedKeyData
      // logger.info("♻️ Found saved identity key in Keychain. Importing to restore identity...")
      do {
        try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.importIdentityKey(identity: identity, keyData: keyData)
        }
        // logger.info("✅ Identity key restored successfully")
      } catch let error as MlsError {
        logger.error("❌ Failed to restore identity key: \(error.localizedDescription)")
        // Continue - will generate new key, but this is suboptimal
      } catch {
        logger.error("❌ Failed to restore identity key: \(error.localizedDescription)")
      }
    }

    let identityBytes = Data(identity.utf8)
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.createKeyPackage(identityBytes: identityBytes)
      }

      // BACKUP: Export and save the identity key to Keychain for future recovery
      if let identityKeyData = try? await runFFIWithRecovery(for: userDID, operation: { ctx in
        try ctx.exportIdentityKey(identity: identity)
      }) {
        try? MLSKeychainManager.shared.store(identityKeyData, forKey: identityKeyKey)
        // logger.debug("💾 Identity key backed up to Keychain for recovery")
      }

      // Log bundle count after creation
      if let bundleCount = try? await runFFIWithRecovery(for: userDID, operation: { ctx in
        try ctx.getKeyPackageBundleCount()
      }) {
        // logger.debug("[MLSClient.createKeyPackage] Bundle count after creation: \(bundleCount)")
      }

      // logger.info("✅ [MLSClient.createKeyPackage] Success - \(result.keyPackageData.count) bytes")
      return result.keyPackageData
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.createKeyPackage] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Create a key package for this user using client identity (did#deviceUUID)
  /// Each device is a unique MLS leaf node for proper multi-device support.
  public func createKeyPackage(for userDID: String) async throws -> Data {
    // Get client identity (did#deviceUUID) for this device
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.createKeyPackage] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }
    return try await createKeyPackage(for: userDID, identity: clientIdentity)
  }

  /// Compute the hash reference for a key package
  public func computeKeyPackageHash(for userDID: String, keyPackageData: Data) async throws -> Data {
    logger.debug(
      "📍 [MLSClient.computeKeyPackageHash] Computing hash for \(keyPackageData.count) bytes")
    do {
      let hashBytes = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.computeKeyPackageHash(keyPackageBytes: keyPackageData)
      }
      logger.debug("✅ [MLSClient.computeKeyPackageHash] Hash:  \(hashBytes.hexEncodedString())")
      return hashBytes
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.computeKeyPackageHash] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Extract the MLS signature public key and algorithm from a serialized key package.
  /// This is used by declaration-chain verification to authorize device keys.
  public func extractKeyPackageSignatureKey(
    keyPackageData: Data
  ) throws -> (publicKey: Data, algorithm: String) {
    let publicKey = try mlsExtractKeyPackageSignaturePublicKey(keyPackageBytes: keyPackageData)
    let algorithm = try mlsExtractKeyPackageSignatureAlgorithm(keyPackageBytes: keyPackageData)
    return (publicKey, algorithm)
  }

  /// Sign declaration proof bytes using the persistent MLS signer for the current identity.
  /// The signature key matches the leaf-node key used in key packages and group operations.
  public func signDeclarationProof(
    for userDID: String,
    payload: Data
  ) async throws -> Data {
    let normalizedDID = normalizeUserDID(userDID)
    let identity = await getClientIdentity(for: normalizedDID) ?? normalizedDID

    do {
      return try await runFFIWithRecovery(for: normalizedDID) { ctx in
        try ctx.signWithIdentityKey(identity: identity, payload: payload)
      }
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.signDeclarationProof] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Get all local key package hashes for a user
  /// Used to verify that local storage matches server inventory
  public func getLocalKeyPackageHashes(for userDID: String) async throws -> [String] {
    logger.debug(
      "📍 [MLSClient.getLocalKeyPackageHashes] Getting local hashes for \(userDID.prefix(20))...")
    do {
      let hashes = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.debugListKeyPackageHashes()
      }
      logger.debug("✅ [MLSClient.getLocalKeyPackageHashes] Found \(hashes.count) local hashes")
      return hashes
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.getLocalKeyPackageHashes] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Update key package for an existing group
  public func updateKeyPackage(for userDID: String, groupId: Data) async throws -> Data {
    logger.error("Update key package not yet implemented in UniFFI API")
    throw MLSError.operationFailed
  }

  // MARK: - Group State

  /// Get the current epoch for a group
  public func getEpoch(for userDID: String, groupId: Data) async throws -> UInt64 {
    do {
      return try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.getEpoch(groupId: groupId)
      }
    } catch let error as MlsError {
      let shouldReload: Bool
      switch error {
      case .GroupNotFound:
        logger.warning(
          "⚠️ [MLSClient.getEpoch] Group not found - attempting context reload before retry")
        shouldReload = true
      case .ContextNotInitialized:
        logger.warning(
          "⚠️ [MLSClient.getEpoch] Context not initialized - attempting context reload before retry"
        )
        shouldReload = true
      case .ContextClosed:
        logger.warning("⚠️ [MLSClient.getEpoch] Context closed - attempting context reload before retry")
        shouldReload = true
      default:
        shouldReload = false
      }

      if shouldReload {
        do {
          _ = try await reloadContextFromStorage(for: userDID)
          return try await runFFIWithRecovery(for: userDID) { ctx in
            try ctx.getEpoch(groupId: groupId)
          }
        } catch {
          logger.error("Get epoch retry failed: \(error.localizedDescription)")
        }
      }

      logger.error("Get epoch failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Get the RFC 9420 §8.7 epoch authenticator for a group, hex-encoded.
  ///
  /// Used by the server's A7 reset-vote pyramid
  /// (`mls-ds/server/src/handlers/mls_chat/report_recovery_failure.rs`) to tie
  /// a recovery-failure vote to a specific cryptographic view of the group;
  /// without it, the server records the report but short-circuits with
  /// `reason: "missing_authenticator"` and the vote does **not** count toward
  /// quorum.
  ///
  /// Returns nil (never throws) when the group isn't present locally (e.g.
  /// already deleted before this call, context closed, or group-not-found).
  /// Callers who need a real vote must invoke this **before** tearing down
  /// local group state (`deleteGroup`) and stash the result in a local.
  public func epochAuthenticatorHex(for userDID: String, groupId: Data) async -> String? {
    do {
      let authenticator = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.epochAuthenticator(groupId: groupId)
      }
      return authenticator.hexEncodedString()
    } catch let error as MlsError {
      switch error {
      case .GroupNotFound, .ContextNotInitialized, .ContextClosed:
        logger.debug(
          "[MLSClient.epochAuthenticatorHex] Group not available: \(error.localizedDescription)")
        return nil
      default:
        logger.warning(
          "⚠️ [MLSClient.epochAuthenticatorHex] Failed: \(error.localizedDescription)")
        return nil
      }
    } catch {
      logger.warning(
        "⚠️ [MLSClient.epochAuthenticatorHex] Unexpected error: \(error.localizedDescription)")
      return nil
    }
  }

  /// Get the confirmation tag for the current epoch of a group.
  /// The confirmation tag is a cryptographic value unique to each MLS tree state,
  /// used by the server to detect tree divergence between clients.
  /// Returns nil if the group is not found or context is unavailable.
  public func getConfirmationTag(for userDID: String, groupId: Data) async throws -> Data? {
    do {
      return try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.getConfirmationTag(groupId: groupId)
      }
    } catch let error as MlsError {
      switch error {
      case .GroupNotFound, .ContextNotInitialized, .ContextClosed:
        logger.debug(
          "[MLSClient.getConfirmationTag] Group not available: \(error.localizedDescription)")
        return nil
      default:
        logger.warning(
          "⚠️ [MLSClient.getConfirmationTag] Failed: \(error.localizedDescription)")
        return nil
      }
    }
  }

  /// Get the current metadata bootstrap info for an already-joined group.
  /// Returns nil only when the group is unavailable or still in legacy state without
  /// a committed MetadataReference; groups using the cleaned-up protocol should return
  /// a metadata key plus the current reference from MLS state.
  public func getCurrentMetadata(for userDID: String, groupId: Data) async throws
    -> CurrentMetadataInfo?
  {
    do {
      return try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.getCurrentMetadata(groupId: groupId)
      }
    } catch let error as MlsError {
      logger.error("Get current metadata failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Get debug information about group members
  public func debugGroupMembers(for userDID: String, groupId: Data) async throws -> GroupDebugInfo {
    do {
      return try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.debugGroupMembers(groupId: groupId)
      }
    } catch let error as MlsError {
      logger.error("Debug group members failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Export a secret from the group's key schedule for debugging/comparison
  /// This can be used to verify that two clients at the same epoch have the same cryptographic state
  public func exportSecret(
    for userDID: String, groupId: Data, label: String, context contextData: Data, keyLength: UInt64
  ) async throws -> Data {
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.exportSecret(
          groupId: groupId, label: label, context: contextData, keyLength: keyLength)
      }
      return result.secret
    } catch let error as MlsError {
      logger.error("Export secret failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Check if a group exists in local storage
  public func groupExists(for userDID: String, groupId: Data) async -> Bool {
    (try? await getContext(for: userDID).groupExists(groupId: groupId)) ?? false
  }

  /// Get group info for external parties
  public func getGroupInfo(for userDID: String, groupId: Data) async throws -> Data {
    logger.error("Get group info not yet implemented in UniFFI API")
    throw MLSError.operationFailed
  }

  /// Export raw GroupInfo bytes from the local MLS context (no server upload).
  ///
  /// Mirrors the export step inside `publishGroupInfo` but returns the bytes
  /// instead of POSTing them. Used by the first-responder bootstrap path
  /// (spec §8.5) to attach freshly-built GroupInfo to the
  /// `bootstrapResetGroup` request body so the server can populate the
  /// post-reset row in a single round-trip.
  ///
  /// Validates the export against the FFI's GroupInfo formatter before
  /// returning so callers don't have to repeat the check.
  public func exportLocalGroupInfo(for userDID: String, groupId: Data) async throws -> Data {
    logger.info(
      "📤 [MLSClient.exportLocalGroupInfo] START - groupId: \(groupId.hexEncodedString().prefix(16))"
    )

    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error(
        "❌ [MLSClient.exportLocalGroupInfo] Device not registered - cannot determine client identity"
      )
      throw MLSError.configurationError
    }
    let identityBytes = Data(clientIdentity.utf8)
    let groupInfoBytes = try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.exportGroupInfo(groupId: groupId, signerIdentityBytes: identityBytes)
    }

    guard groupInfoBytes.count >= Self.minGroupInfoSize else {
      logger.error(
        "❌ [MLSClient.exportLocalGroupInfo] Exported GroupInfo too small: \(groupInfoBytes.count) bytes"
      )
      throw MLSError.operationFailed
    }

    let isValid = try await runFFIWithRecovery(for: userDID) { ctx in
      ctx.validateGroupInfoFormat(groupInfoBytes: groupInfoBytes)
    }
    guard isValid else {
      logger.error("❌ [MLSClient.exportLocalGroupInfo] GroupInfo format validation failed")
      throw MLSError.operationFailed
    }

    logger.info(
      "✅ [MLSClient.exportLocalGroupInfo] Exported \(groupInfoBytes.count) bytes")
    return groupInfoBytes
  }

  /// Process a commit message
  public func processCommit(for userDID: String, groupId: Data, commitData: Data) async throws
    -> ProcessCommitResult
  {
    logger.info(
      "📍 [MLSClient.processCommit] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16)), commit: \(commitData.count) bytes"
    )
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.processCommit(groupId: groupId, commitData: commitData)
      }
      logger.info(
        "✅ [MLSClient.processCommit] Success - newEpoch: \(result.newEpoch), updateProposals: \(result.updateProposals.count)"
      )
      return result
    } catch let error as MlsError {
      let detail: String
      switch error {
      case .CommitProcessingFailed(let message),
           .OpenMlsError(let message),
           .SerializationError(let message),
           .InvalidCommit(let message):
        detail = message
      default:
        detail = error.localizedDescription
      }
      logger.error("❌ [MLSClient.processCommit] FAILED: \(detail)")
      throw MLSError.commitProcessingFailed(message: detail)
    }
  }

  /// Create a commit for pending proposals
  public func createCommit(for userDID: String, groupId: Data) async throws -> Data {
    logger.error("Create commit not yet implemented in UniFFI API")
    throw MLSError.operationFailed
  }

  /// Clear pending commit for a group
  public func clearPendingCommit(for userDID: String, groupId: Data) async throws {
    logger.info(
      "📍 [MLSClient.clearPendingCommit] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16))"
    )
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.clearPendingCommit(groupId: groupId)
      }
      logger.info("✅ [MLSClient.clearPendingCommit] Success")
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.clearPendingCommit] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Discard a rejected external join.
  ///
  /// The OpenMLS `external_commit_builder().finalize()` API writes the new
  /// group to storage and overwrites any prior state for `groupId` — there is
  /// no "pending" stage to clear via `clearPendingCommit`. When the server
  /// rejects our external commit (409/403/429/etc.), we must call this to
  /// remove the half-joined group from `self.groups`, OpenMLS storage, and
  /// the manifest. Without this, the client carries a post-commit epoch that
  /// the server never accepted — every subsequent message send fails with
  /// TreeStateDiverged until next recovery.
  public func discardPendingExternalJoin(for userDID: String, groupId: Data) async throws {
    logger.info(
      "📍 [MLSClient.discardPendingExternalJoin] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16))"
    )
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.discardPendingExternalJoin(groupId: groupId)
      }
      logger.info("✅ [MLSClient.discardPendingExternalJoin] Success")
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.discardPendingExternalJoin] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Merge a pending commit after validation
  public func mergePendingCommit(for userDID: String, groupId: Data, convoId: String? = nil) async throws
    -> UInt64
  {
    logger.info(
      "📍 [MLSClient.mergePendingCommit] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16))"
    )
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.mergePendingCommit(groupId: groupId)
      }
      logger.info("✅ [MLSClient.mergePendingCommit] Success - newEpoch: \(result.newEpoch)")

      // If convoId is provided, publish the new GroupInfo
      // CRITICAL: Now awaited instead of fire-and-forget
      if let convoId = convoId {
        try await self.publishGroupInfo(for: userDID, convoId: convoId, groupId: groupId)
      }

      return result.newEpoch
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.mergePendingCommit] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Merge a pending commit and return metadata key material for the new epoch.
  /// This variant returns CommitMetadataInfo so the sender can
  /// re-encrypt and upload metadata blobs after merge.
  ///
  /// - Note: `mergePendingCommit` in the FFI returns
  ///   `MergePendingCommitResult` (with `commitMetadata: Option<CommitMetadataInfo>`).
  public func mergePendingCommitV2(for userDID: String, groupId: Data, convoId: String? = nil)
    async throws -> (newEpoch: UInt64, metadataKey: Data?, metadataEpoch: UInt64?)
  {
    logger.info(
      "📍 [MLSClient.mergePendingCommitV2] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16))"
    )
    do {
      let result = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.mergePendingCommit(groupId: groupId)
      }
      logger.info("✅ [MLSClient.mergePendingCommitV2] Success - newEpoch: \(result.newEpoch)")

      // If convoId is provided, publish the new GroupInfo
      if let convoId = convoId {
        try await self.publishGroupInfo(for: userDID, convoId: convoId, groupId: groupId)
      }

      // Extract metadata key material from the MergePendingCommitResult
      if let metadataInfo = result.commitMetadata {
        logger.info(
          "📋 [MLSClient.mergePendingCommitV2] Metadata key available for epoch \(metadataInfo.epoch)"
        )
        return (result.newEpoch, metadataInfo.metadataKey, metadataInfo.epoch)
      }

      return (result.newEpoch, nil, nil)
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.mergePendingCommitV2] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Merge a staged commit after validation
  public func mergeStagedCommit(for userDID: String, groupId: Data) async throws -> UInt64 {
    do {
      let newEpoch = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.mergeStagedCommit(groupId: groupId)
      }
      logger.info("Staged commit merged, new epoch: \(newEpoch)")
      return newEpoch
    } catch let error as MlsError {
      logger.error("Merge staged commit failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  // MARK: - Task #46 — Explicit receive-path stage/merge/discard

  /// Confirm and merge an incoming staged commit keyed by (groupId, targetEpoch).
  ///
  /// After `processMessage` returns `.stagedCommit(newEpoch, _)` the commit is
  /// staged but the local epoch has NOT advanced. Callers must invoke this to
  /// actually advance the epoch. Failure here means OpenMLS dropped the staged
  /// commit — the caller should surface the error and let the sync loop refetch.
  public func mergeIncomingCommit(
    for userDID: String, groupId: Data, targetEpoch: UInt64
  ) async throws -> UInt64 {
    do {
      let mergedEpoch = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.mergeIncomingCommit(groupId: groupId, targetEpoch: targetEpoch)
      }
      logger.info(
        "[RECV] Merged staged commit for group \(groupId.hexEncodedString().prefix(16)) → epoch \(mergedEpoch)"
      )
      return mergedEpoch
    } catch let error as MlsError {
      logger.error(
        "[RECV] mergeIncomingCommit failed for group \(groupId.hexEncodedString().prefix(16)) epoch=\(targetEpoch): \(error.localizedDescription)"
      )
      throw MLSError.operationFailed
    }
  }

  /// Drop a previously staged incoming commit without advancing the epoch.
  ///
  /// Best-effort cleanup: never throws. Use after a merge failure or when a
  /// policy decision (fork detected, rejoin planned) means we should not apply
  /// the staged commit. Idempotent on the Rust side.
  public func discardIncomingCommit(
    for userDID: String, groupId: Data, targetEpoch: UInt64
  ) async {
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.discardIncomingCommit(groupId: groupId, targetEpoch: targetEpoch)
      }
      logger.debug(
        "[RECV] Discarded staged commit for group \(groupId.hexEncodedString().prefix(16)) epoch=\(targetEpoch)"
      )
    } catch {
      // Best-effort: log and swallow. Rust-side is idempotent for missing entries.
      logger.warning(
        "[RECV] discardIncomingCommit failed (swallowed) for group \(groupId.hexEncodedString().prefix(16)) epoch=\(targetEpoch): \(error.localizedDescription)"
      )
    }
  }

  // MARK: - Task #44/#62 — Explicit sender-path stage/confirm/discard
  //
  // Three-phase API for sender-side commit operations (addMembers,
  // removeMembers, swapMembers, updateMetadata). Mirrors the receive-path
  // pattern above: stage locally, ship bytes to the DS, then confirm (on
  // success) or discard (on failure). Callers own the post-merge bookkeeping
  // (publishing GroupInfo, updating groupStates, metadata re-wrap, etc.) —
  // the Rust MlsContext path intentionally does NOT perform those side
  // effects (see catbird-mls task #62 commit body).
  //
  // These wrappers are ADDITIVE — the legacy `addMembers` / `removeMembers`
  // / `mergePendingCommit` / `clearPendingCommit` wrappers above still exist
  // and still work for any direct callers at lower layers.

  /// Stage a commit for a sender-side group operation.
  ///
  /// Returns an `FfiCommitPlan` containing the commit bytes (to POST to DS),
  /// optional welcome bytes (for `addMembers`/`swapMembers`), a fresh
  /// `groupInfo` export, and a `FfiStagedCommitHandle` the caller holds onto
  /// for the subsequent `confirmCommit` or `discardPending`.
  ///
  /// - Parameters:
  ///   - userDID: User DID (normalized internally).
  ///   - conversationId: Hex-encoded conversation/group id (NOT raw bytes —
  ///     the Rust `stage_commit` signature takes a `String`).
  ///   - kind: Which kind of commit to build. See `FfiCommitKind`.
  /// - Returns: The plan, including the handle to pass to
  ///   `confirmCommit`/`discardPending`.
  /// - Throws: `MLSError.operationFailed` (wrapping the underlying `MlsError`).
  ///
  /// - Note: Only one pending sender commit may exist per group at a time
  ///   (OpenMLS constraint). Staging twice without confirm/discard between
  ///   returns `MlsError.InvalidInput`. The caller should typically call
  ///   `clearPendingCommit` first to flush any stale state from a prior
  ///   failed run.
  public func stageCommit(
    for userDID: String,
    conversationId: String,
    kind: FfiCommitKind
  ) async throws -> FfiCommitPlan {
    logger.info(
      "📍 [MLSClient.stageCommit] START - user: \(userDID.prefix(20)), convo: \(conversationId.prefix(16)), kind: \(String(describing: kind).prefix(32))"
    )
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [MLSClient.stageCommit] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }
    let signerIdentityBytes = Data(clientIdentity.utf8)
    do {
      let plan = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.stageCommit(
          conversationId: conversationId,
          kind: kind,
          signerIdentityBytes: signerIdentityBytes
        )
      }
      logger.info(
        "✅ [MLSClient.stageCommit] Staged - nonce: \(plan.handle.nonce), source→target: \(plan.sourceEpoch)→\(plan.targetEpoch), commit: \(plan.commitBytes.count) bytes, welcome: \(plan.welcomeBytes?.count ?? 0) bytes"
      )
      return plan
    } catch let error as MlsError {
      logger.error("❌ [MLSClient.stageCommit] FAILED: \(error.localizedDescription)")
      // Preserve the "member already in group" signal that addMembersImpl
      // downstream uses for key-package unreservation / user-facing error.
      let message: String
      switch error {
      case .InvalidInput(let m): message = m
      case .OpenMlsError(let m): message = m
      default: message = error.localizedDescription
      }
      if case .addMembers = kind,
         message.lowercased().contains("already in group") ||
         message.lowercased().contains("member already in group")
      {
        throw MLSError.memberAlreadyInGroup(member: "unknown")
      }
      throw MLSError.operationFailed
    }
  }

  /// Confirm a previously staged commit after the DS has accepted it.
  ///
  /// - Parameters:
  ///   - userDID: User DID.
  ///   - handle: The handle returned from `stageCommit`.
  ///   - serverEpoch: The epoch the DS reported after accepting the commit.
  ///     For API paths that don't echo an epoch (`removeMembers`,
  ///     `commitGroupChange`), pass the sentinel returned from
  ///     `mlsSkipServerEpochFence()`.
  /// - Returns: `FfiConfirmedCommit` with the post-merge epoch and optional
  ///   metadata key material (equivalent to what `mergePendingCommitV2`
  ///   returned).
  /// - Throws: `MLSError.operationFailed`. If the server-epoch fence
  ///   trips (i.e. server accepted a different epoch than staged), the Rust
  ///   side emits `MlsError.EpochMismatch` and leaves the staged commit in
  ///   place; the caller should catch, call `discardPending`, and resync.
  public func confirmCommit(
    for userDID: String,
    handle: FfiStagedCommitHandle,
    serverEpoch: UInt64
  ) async throws -> FfiConfirmedCommit {
    logger.info(
      "📍 [MLSClient.confirmCommit] START - user: \(userDID.prefix(20)), group: \(handle.groupId.prefix(16)), nonce: \(handle.nonce), serverEpoch: \(serverEpoch)"
    )
    do {
      let confirmed = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.confirmCommit(handle: handle, serverEpoch: serverEpoch)
      }
      logger.info(
        "✅ [MLSClient.confirmCommit] Confirmed - newEpoch: \(confirmed.newEpoch), metadataKey: \(confirmed.metadataKey != nil ? "yes" : "no")"
      )
      return confirmed
    } catch let error as MlsError {
      // TODO(#63): MlsError.EpochMismatch is currently flat (message-only).
      // When task #63 re-adds structured local/remote fields, callers that
      // need to log the gap can pattern-match here without parsing strings.
      logger.error("❌ [MLSClient.confirmCommit] FAILED: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Discard a previously staged sender-side commit without merging it.
  ///
  /// Best-effort cleanup: never throws. Use when the DS rejected the commit,
  /// an HTTP error occurred, or the higher layer decided not to merge.
  /// Idempotent on the Rust side.
  public func discardPending(
    for userDID: String,
    handle: FfiStagedCommitHandle
  ) async {
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.discardPending(handle: handle)
      }
      logger.debug(
        "✅ [MLSClient.discardPending] Dropped staged commit for group \(handle.groupId.prefix(16)) nonce \(handle.nonce)"
      )
    } catch {
      // Best-effort: log and swallow. Rust-side is idempotent for missing
      // entries — callers in error-paths should not have their original
      // failure masked by cleanup noise.
      logger.warning(
        "⚠️ [MLSClient.discardPending] discardPending failed (swallowed) for group \(handle.groupId.prefix(16)) nonce \(handle.nonce): \(error.localizedDescription)"
      )
    }
  }

  // MARK: - Proposal Inspection and Management

  /// Process a message and return detailed information about its content
  public func processMessage(for userDID: String, groupId: Data, messageData: Data) async throws
    -> ProcessedContent
  {
    // Padding is stripped by catbird-mls process_message internally.
    let actualMessageData = messageData
    
    logger.info(
      "📍 [MLSClient.processMessage] START - user: \(userDID), groupId: \(groupId.hexEncodedString().prefix(16)), message: \(actualMessageData.count) bytes"
    )
    // ═══════════════════════════════════════════════════════════════════════════
    // EPOCH FENCE PROTOCOL: Ensure in-memory state is synced with disk before process
    // ═══════════════════════════════════════════════════════════════════════════
    // This is the CRITICAL fix for NSE<>App epoch desync. The NSE may have advanced
    // the MLS ratchet on disk while the app was backgrounded. We MUST check and
    // reload before attempting message processing to prevent forward secrecy violations.
    // ═══════════════════════════════════════════════════════════════════════════
    let contextWasReloaded = try await ensureContextSyncedBeforeDecrypt(
      for: userDID,
      groupId: groupId,
      conversationID: "push-message"  // We don't have convoID here, but logging inside handles checking
    )
    if contextWasReloaded {
      logger.info(
        "🔄 [EPOCH FENCE] Context reloaded before processMessage - proceeding with fresh state")
    }

    do {
      let content = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.processMessage(groupId: groupId, messageData: actualMessageData)
      }
      
      // ═══════════════════════════════════════════════════════════════════════════
      // Update epoch checkpoint after successful processing (for next staleness check)
      // ═══════════════════════════════════════════════════════════════════════════
      // Note: We need to get the new epoch to update the checkpoint.
      // process_message returns ProcessedContent which wraps the ApplicationMessage
      // It doesn't explicitly return the new epoch, but if successful, we are at the epoch
      // of the message. We'll fetch the current epoch to be safe.

      // Perform async epoch fetch to update checkpoint without blocking return
      Task {
        do {
          let newEpoch = try await self.getEpoch(for: userDID, groupId: groupId)
          await MLSEpochCheckpoint.shared.recordEpoch(
            groupId: groupId,
            epoch: newEpoch,
            isNSE: false
          )
        } catch {
          self.logger.warning(
            "⚠️ [MLSClient.processMessage] Failed to update epoch checkpoint: \(error.localizedDescription)"
          )
        }
      }

      logger.info(
        "✅ [MLSClient.processMessage] Success - content type: \(String(describing: content))")
      return content
    } catch let error as MlsError {
      // Extract message from error case
      // NOTE: UniFFI generates two different Swift cases for OpenMLS errors:
      //   - .OpenMlsError - for Rust MLSError::OpenMLSError (unit variant, generic message)
      //   - .OpenMls - for Rust MLSError::OpenMLS(String) (has detailed error message)
      let errorMessage: String
      switch error {
      case .DecryptionFailed(let msg): errorMessage = msg
      case .OpenMlsError(let msg): errorMessage = msg
      case .OpenMls(let msg): errorMessage = msg  // Detailed OpenMLS errors with context
      case .InvalidInput(let msg): errorMessage = msg
      default: errorMessage = error.localizedDescription
      }

      let errorMessageLower = errorMessage.lowercased()

      // Check for "old epoch" error which is safe to ignore for new joiners
      // This happens when the server sends the Commit message that added us, but we joined via Welcome (already at new epoch)
      // Also check for OpenMLS epoch-related errors like "EpochMismatch", "WrongEpoch", "ValidationError"
      if errorMessage.contains("Cannot decrypt message from epoch") 
         || errorMessageLower.contains("epochmismatch")
         || errorMessageLower.contains("wrong epoch")
         || errorMessageLower.contains("wrongepoch")
         || (errorMessageLower.contains("validation") && errorMessageLower.contains("epoch")) {
        logger.warning("⚠️ Ignoring message from old epoch: \(errorMessage)")
        throw MLSError.ignoredOldEpochMessage
      }

      // ═══════════════════════════════════════════════════════════════════════════
      // CRITICAL FIX (2024-12): Handle SecretReuseError as a skip, NOT a desync
      // ═══════════════════════════════════════════════════════════════════════════
      //
      // Problem: SecretReuseError occurs when the same message is decrypted twice.
      // This commonly happens when:
      // 1. NSE decrypts a message (advances ratchet, deletes key)
      // 2. Main app tries to decrypt the same message (key is gone)
      //
      // Old behavior: Treated as ratchetStateDesync → triggers group rejoin
      // New behavior: Treat as secretReuseSkipped → caller should check DB cache
      //
      // This is NOT a true desync - the message WAS decrypted successfully (by NSE).
      // The plaintext should be in the database cache.
      //
      // ═══════════════════════════════════════════════════════════════════════════
      if errorMessageLower.contains("secretreuse") || errorMessageLower.contains("secret_reuse")
         || errorMessage.contains("SecretReuseError") || errorMessage.contains("SecretTreeError(SecretReuseError)")
      {
        logger.info("ℹ️ [MLSClient.processMessage] SecretReuseError - message already decrypted (likely by NSE)")
        logger.info("   This is expected when NSE and main app race to decrypt the same message")
        logger.info("   Caller should retrieve plaintext from database cache")
        // Note: We don't have messageID here, but caller will handle appropriately
        throw MLSError.secretReuseSkipped(messageID: "unknown")
      }

      // ═══════════════════════════════════════════════════════════════════════════
      // CRITICAL FIX (2026-01): Handle CannotDecryptOwnMessage BEFORE ratchetStateDesync
      // ═══════════════════════════════════════════════════════════════════════════
      // This error occurs when we try to decrypt a message we sent ourselves.
      // MLS encrypts messages for recipients only - senders can't decrypt their own messages.
      // This is NOT a desync - it's a permanent failure that retrying won't fix.
      // The caller should use the pre-cached payload from when the message was sent.
      // ═══════════════════════════════════════════════════════════════════════════
      if errorMessage.contains("CannotDecryptOwnMessage") || errorMessageLower.contains("cannotdecryptownmessage") {
        logger.warning("⚠️ [MLSClient.processMessage] CannotDecryptOwnMessage - this is a self-sent message")
        logger.info("   MLS messages are encrypted for recipients only, senders cannot decrypt their own messages")
        logger.info("   Caller should use pre-cached payload from send operation")
        throw MLSError.cannotDecryptOwnMessage
      }

      // Detect ratchet state desynchronization errors
      // These can occur when SSE connection fails and client state becomes stale
      if case .DecryptionFailed = error {
        // DecryptionFailed OTHER than SecretReuseError indicates true state desync
        // OpenMLS errors like RatchetTypeError, InvalidSignature are wrapped as DecryptionFailed
        logger.error(
          "🔴 RATCHET STATE DESYNC DETECTED in processMessage: DecryptionFailed - likely stale MLS state"
        )
        logger.error("   Original error: \(errorMessage)")
        logger.error("   This indicates the client's MLS state is out of sync with the group")
        logger.error(
          "   Recovery requires re-joining the group or processing a fresh Welcome message")
        throw MLSError.ratchetStateDesync(
          message: "DecryptionFailed - MLS state out of sync: \(errorMessage)")
      }
      
      // Check OpenMlsError case (generic, unit variant from Rust)
      if case .OpenMlsError = error {
        logger.error(
          "🔴 OpenMLS error in processMessage: \(errorMessage)"
        )
        logger.error("   This may indicate protocol issues or state desync")
        // Check if it's an epoch-related or decryption error
        if errorMessageLower.contains("decrypt") || errorMessageLower.contains("epoch") 
           || errorMessageLower.contains("ratchet") || errorMessageLower.contains("signature") {
          throw MLSError.ratchetStateDesync(
            message: "OpenMlsError - MLS state issue: \(errorMessage)")
        }
        // For other OpenMLS errors, throw with the message
        throw MLSError.invalidContent("OpenMLS error: \(errorMessage)")
      }
      
      // Check OpenMls case (detailed errors with context from Rust MLSError::OpenMLS(String))
      // This is the variant used by process_message failures with detailed error info
      if case .OpenMls = error {
        logger.error(
          "🔴 OpenMLS detailed error in processMessage: \(errorMessage)"
        )
        logger.error("   This contains detailed error context from the Rust FFI")
        // Check if it's an epoch-related or decryption error
        if errorMessageLower.contains("decrypt") || errorMessageLower.contains("epoch") 
           || errorMessageLower.contains("ratchet") || errorMessageLower.contains("signature")
           || errorMessageLower.contains("secrettreerror") || errorMessageLower.contains("secret_tree") {
          throw MLSError.ratchetStateDesync(
            message: "OpenMLS - MLS state issue: \(errorMessage)")
        }
        // For other OpenMLS errors, throw with the detailed message
        throw MLSError.invalidContent("OpenMLS error: \(errorMessage)")
      }

      // Also check message content for specific error keywords (excluding SecretReuse which is handled above)
      if errorMessageLower.contains("ratchet") || errorMessageLower.contains("invalidsignature")
        || errorMessageLower.contains("epoch")
      {
        logger.error("🔴 RATCHET STATE DESYNC DETECTED in processMessage: \(errorMessage)")
        logger.error("   This indicates the client's MLS state is out of sync with the group")
        logger.error(
          "   Recovery requires re-joining the group or processing a fresh Welcome message")
        throw MLSError.ratchetStateDesync(message: errorMessage)
      }

      logger.error("❌ [MLSClient.processMessage] FAILED: \(error.localizedDescription)")
      logger.error("   Error type: \(type(of: error)), error case: \(error)")
      logger.error("   Error message extracted: \(errorMessage)")
      throw MLSError.operationFailed
    }
  }

  /// Store a validated proposal in the proposal queue
  public func storeProposal(for userDID: String, groupId: Data, proposalRef: ProposalRef) async throws {
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.storeProposal(groupId: groupId, proposalRef: proposalRef)
      }
      logger.info("Proposal stored successfully")
    } catch let error as MlsError {
      logger.error("Store proposal failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// List all pending proposals for a group
  public func listPendingProposals(for userDID: String, groupId: Data) async throws -> [ProposalRef] {
    do {
      let proposals = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.listPendingProposals(groupId: groupId)
      }
      logger.info("Found \(proposals.count) pending proposals")
      return proposals
    } catch let error as MlsError {
      logger.error("List proposals failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Remove a proposal from the proposal queue
  public func removeProposal(for userDID: String, groupId: Data, proposalRef: ProposalRef) async throws {
    do {
      try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.removeProposal(groupId: groupId, proposalRef: proposalRef)
      }
      logger.info("Proposal removed successfully")
    } catch let error as MlsError {
      logger.error("Remove proposal failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  /// Commit all pending proposals that have been validated
  public func commitPendingProposals(for userDID: String, groupId: Data) async throws -> Data {
    do {
      let commitData = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.commitPendingProposals(groupId: groupId)
      }
      logger.info("Pending proposals committed successfully")
      return commitData
    } catch let error as MlsError {
      logger.error("Commit proposals failed: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }

  // MARK: - Persistence

  /// Phase 4: Monitor and automatically replenish key package bundles
  /// Proactively checks server inventory and uploads bundles when running low
  /// - Parameter userDID: User DID to monitor bundles for
  /// - Returns: Tuple of (available bundles on server, bundles uploaded)
  public func monitorAndReplenishBundles(for userDID: String) async throws -> (
    available: Int, uploaded: Int
  ) {
    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      logger.error(
        "❌ [Phase 4] API client not configured for user \(normalizedDID) - cannot monitor bundles")
      throw MLSError.operationFailed
    }

    // Prevent stale account contexts from uploading key packages under the wrong session.
    guard await apiClient.isAuthenticatedAs(normalizedDID) else {
      logger.warning(
        "⏸️ [Phase 4] Skipping bundle replenish - \(normalizedDID.prefix(20))... is not the active authenticated account"
      )
      throw MLSError.operationFailed
    }

    logger.info(
      "🔍 [Phase 4] Starting proactive bundle monitoring for user: \(userDID.prefix(20))...")

    // CRITICAL: Check local bundles FIRST before querying server
    // This catches the desync case where local=0 but server>0
    var localBundleCount: UInt64 = 0
    do {
      localBundleCount = try await ensureLocalBundlesAvailable(for: userDID)
      logger.info("📍 [Phase 4] Local bundle count: \(localBundleCount)")
    } catch {
      logger.warning("⚠️ [Phase 4] Failed to check local bundles: \(error.localizedDescription)")
    }

    // Query user-level server status for diagnostics.
    let status = try await apiClient.getKeyPackageStatus()

    logger.info("📊 [Phase 4] Server bundle status (user aggregate):")
    logger.debug("   - Published: \(status.stats.published)")
    logger.debug("   - Available: \(status.stats.available)")
    logger.debug("   - Expired: \(status.stats.expired)")

    // Prefer device-specific availability (critical for multi-device).
    var availableForThisDevice = status.stats.available
    do {
      let sync = try await syncKeyPackageHashes(for: userDID)
      availableForThisDevice = sync.remainingAvailable
      logger.info("📱 [Phase 4] Device-specific available bundles: \(availableForThisDevice)")
    } catch {
      logger.warning(
        "⚠️ [Phase 4] Device-specific sync failed; using aggregate availability: \(error.localizedDescription)"
      )
    }

    // Detect and handle local=0, device-server>0 desync
    if localBundleCount == 0 && availableForThisDevice > 0 {
      logger.warning(
        "⚠️ [Phase 4] DESYNC DETECTED: Local=0, DeviceServer=\(availableForThisDevice)"
      )
      logger.info("   🔄 Attempting non-destructive context reload...")

      do {
        let recoveredCount = try await reloadContextFromStorage(for: userDID)
        if recoveredCount > 0 {
          logger.info("   ✅ Recovered \(recoveredCount) bundles from storage reload!")
          localBundleCount = recoveredCount
        } else {
          logger.warning("   ⚠️ Storage reload found 0 bundles - will need reconciliation")
        }
      } catch {
        logger.error("   ❌ Context reload failed: \(error.localizedDescription)")
      }
    }

    // Detect recovery mode condition: both local and server have 0 bundles for this device
    // This indicates a fresh install or corrupted state that needs rate limit bypass
    let useRecoveryMode = localBundleCount == 0 && availableForThisDevice == 0
    if useRecoveryMode {
      logger.warning("🔑 [Phase 4] Recovery mode detected: local=0, server=0 - will bypass rate limits")
    }

    // Configuration for bundle replenishment
    let minimumAvailableBundles = 10
    // In recovery mode, limit to 50 bundles (server's MAX_RECOVERY_BATCH)
    let targetBundleCount = useRecoveryMode ? 25 : 25
    let batchUploadSize = 5

    // Check if replenishment is needed
    if availableForThisDevice >= minimumAvailableBundles {
      logger.info(
        "✅ [Phase 4] Sufficient bundles available for this device (\(availableForThisDevice)) - no action needed")
      return (available: availableForThisDevice, uploaded: 0)
    }

    // Calculate how many bundles to upload
    let neededCount = targetBundleCount - availableForThisDevice
    logger.warning(
      "⚠️ [Phase 4] Low bundle count for this device! Available: \(availableForThisDevice), minimum: \(minimumAvailableBundles)"
    )
    logger.info(
      "🔧 [Phase 4] Replenishing \(neededCount) bundles to reach target of \(targetBundleCount)")

    // Get client identity (did#deviceUUID) for this device
    guard let clientIdentity = await getClientIdentity(for: userDID) else {
      logger.error("❌ [Phase 4] Device not registered - cannot determine client identity")
      throw MLSError.configurationError
    }

    guard clientIdentity == normalizedDID else {
      logger.error(
        "❌ [Phase 4] Identity mismatch before key package upload - userDID=\(normalizedDID), clientIdentity=\(clientIdentity)"
      )
      throw MLSError.invalidCredential(
        "Key package identity mismatch for \(normalizedDID)")
    }

    // Get device ID for server-side deduplication
    let deviceInfo = await getDeviceInfo(for: userDID)
    let deviceId = deviceInfo?.deviceId

    // Create and upload bundles in batches
    var uploadedCount = 0

    for batchIndex in stride(from: 0, to: neededCount, by: batchUploadSize) {
      let batchCount = min(batchUploadSize, neededCount - batchIndex)
      logger.debug(
        "📦 [Phase 4] Creating batch \(batchIndex/batchUploadSize + 1) - \(batchCount) bundles")

      var batchPackages: [MLSKeyPackageUploadData] = []

      for i in 0..<batchCount {
        do {
          let keyPackageBytes = try await createKeyPackage(for: userDID, identity: clientIdentity)
          let idempotencyKey = UUID().uuidString.lowercased()

          // B14 normalization: store raw TLS bytes; ATProto Bytes wrapping
          // happens at the wire boundary in publishKeyPackagesBatchDirect.
          batchPackages.append(
            MLSKeyPackageUploadData(
              keyPackage: keyPackageBytes,
              cipherSuite: "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
              expires: Date().addingTimeInterval(90 * 24 * 60 * 60),  // 90 days
              idempotencyKey: idempotencyKey,
              deviceId: deviceId,
              credentialDid: clientIdentity
            ))

          logger.debug("   ✅ Created bundle \(batchIndex + i + 1)/\(neededCount)")
        } catch {
          logger.error(
            "   ❌ Failed to create bundle \(batchIndex + i + 1): \(error.localizedDescription)")
          throw error
        }
      }

      // Upload batch to server (use recovery mode to bypass rate limits if device has 0 key packages)
      do {
        let result = try await apiClient.publishKeyPackagesBatch(batchPackages, recoveryMode: useRecoveryMode, deviceId: deviceId)
        logger.debug(
          "   📤 Batch upload complete - succeeded: \(result.succeeded), failed: \(result.failed)")

        if let errors = result.errors, !errors.isEmpty {
          logger.warning("   ⚠️ Some uploads failed:")
          for error in errors {
            logger.debug("      - Index \(error.index): \(error.error)")
          }
        }

        uploadedCount += result.succeeded
      } catch {
        logger.error("   ❌ Batch upload failed: \(error.localizedDescription)")
        throw error
      }

      // Small delay between batches to avoid overwhelming server
      if batchIndex + batchUploadSize < neededCount {
        try await Task.sleep(for: .milliseconds(100))
      }
    }

    logger.info("✅ [Phase 4] Replenishment complete - uploaded \(uploadedCount) bundles")
    logger.info("📊 [Phase 4] New device bundle estimate: \(availableForThisDevice + uploadedCount)")

    return (available: availableForThisDevice + uploadedCount, uploaded: uploadedCount)
  }

  /// Phase 4: Diagnostic logging for bundle lifecycle
  /// Logs comprehensive bundle state for debugging
  public func logBundleDiagnostics(for userDID: String) async throws {
    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      logger.error(
        "❌ [Phase 4] API client not configured for user \(normalizedDID, privacy: .private) - cannot run diagnostics")
      throw MLSError.operationFailed
    }

    logger.info("🔬 [Phase 4] Bundle Diagnostics for user: \(userDID.prefix(20), privacy: .private)")

    // Local bundle count (Phase 2 FFI query)
    let localCount: UInt64
    do {
      localCount = try await runFFIWithRecovery(for: userDID) { ctx in
        try ctx.getKeyPackageBundleCount()
      }
      logger.info("   📍 Local bundles in cache: \(localCount)")
    } catch {
      logger.warning("   ⚠️ Failed to query local bundles: \(error.localizedDescription)")
      throw error
    }

    // Server bundle status (Phase 3 endpoint)
    do {
      let status = try await apiClient.getKeyPackageStatus(limit: 5)
      logger.info("   📍 Server bundle status:")
      logger.info("      - Published: \(status.stats.published)")
      logger.info("      - Available: \(status.stats.available)")
      logger.info("      - Expired: \(status.stats.expired)")

      // Warning thresholds
      let minimumAvailableBundles = 10
      if status.stats.available < minimumAvailableBundles {
        logger.warning(
          "   ⚠️ ALERT: Available bundles (\(status.stats.available)) below minimum threshold (\(minimumAvailableBundles))"
        )
        logger.warning("      ACTION REQUIRED: Call monitorAndReplenishBundles() to replenish")
      }

      if status.stats.available == 0 {
        logger.error("   🚨 CRITICAL: No bundles available! Cannot process Welcome messages!")
      }
    } catch {
      logger.error("   ❌ Failed to query server status: \(error.localizedDescription)")
      throw error
    }

    logger.info("✅ [Phase 4] Diagnostics complete")
  }

  /// Verify that local key packages exist in SQLite storage
  /// With automatic SQLite persistence, bundles should exist after initial creation
  /// Returns the number of local bundles available
  public func ensureLocalBundlesAvailable(for userDID: String) async throws -> UInt64 {
    let bundleCount = try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.getKeyPackageBundleCount()
    }

    if bundleCount == 0 {
      logger.warning("⚠️ No local bundles found in SQLite storage for user: \(userDID.prefix(20), privacy: .private)")
      logger.warning("   This may indicate first use or post-logout state")
      logger.warning(
        "   Consider calling monitorAndReplenishBundles() to generate and upload bundles")
    } else {
      logger.debug("✅ Found \(bundleCount) local bundles in SQLite storage")
    }

    return bundleCount
  }

  /// Get the current key package bundle count for a user
  /// Used by recovery manager to check for desync
  public func getKeyPackageBundleCount(for userDID: String) async throws -> UInt64 {
    try await runFFIWithRecovery(for: userDID) { ctx in
      try ctx.getKeyPackageBundleCount()
    }
  }

  /// Setup lifecycle observers for automatic storage persistence
  ///
  /// Note: MLSClient is a singleton that manages multiple per-user MLS contexts.
  /// Lifecycle observers (app backgrounding, termination) should be handled by
  /// AppState or AuthManager which knows the currently active user, then call
  /// flushStorage(for:) on the appropriate user's context.
  ///
  /// This approach is intentional to maintain clean separation between the
  /// crypto layer (MLSClient) and app state management.
  private func setupLifecycleObservers() {
    // Intentionally empty - see note above
  }

  /// Force flush all pending database writes to disk for a specific user
  ///
  /// This executes a SQLite WAL checkpoint to ensure all pending writes are
  /// durably persisted to the main database file. The Rust FFI layer now
  /// auto-flushes after each key package creation, but this method can be
  /// called explicitly after batch operations for extra safety.
  ///
  /// - Parameter userDID: The user's DID
  /// - Throws: MLSError if flush fails
  public func flushStorage(for userDID: String) async throws {
    let normalizedDID = normalizeUserDID(userDID)

    try await withMLSUserPermit(for: normalizedDID) {
      // No advisory lock needed - SQLite WAL handles concurrent access
      // Cross-process coordination uses `MLSStateChangeNotifier` / `MLSNotificationCoordinator`

      try await self.flushStorageLocked(normalizedDID: normalizedDID)
    }
  }

  private func flushStorageLocked(normalizedDID: String) async throws {
    logger.info("💾 Flushing MLS storage for user: \(normalizedDID.prefix(20))")

    do {
      try await runFFIWithRecoveryLocked(for: normalizedDID) { ctx in
        try ctx.flushStorage()
      }
      logger.info("✅ MLS storage flushed successfully")
    } catch let error as MlsError {
      logger.error("❌ Failed to flush MLS storage: \(error.localizedDescription)")
      throw MLSError.operationFailed
    }
  }
  
  /// Close and release an MLS context for a specific user
  ///
  /// CRITICAL: Call this during account switching to prevent SQLite connection exhaustion.
  /// This method:
  /// 1. Flushes all pending writes to disk (WAL checkpoint)
  /// 2. Removes the context from the in-memory cache
  /// 3. Removes associated API clients and managers
  ///
  /// The underlying Rust FFI context will be deallocated when all Arc references are dropped.
  /// SQLite connections are closed when the rusqlite::Connection is dropped.
  ///
  /// - Parameter userDID: The user's DID to close context for
  /// - Returns: True if a context was closed, false if no context existed for this user
  @discardableResult
  public func closeContext(for userDID: String) async -> Bool {
    let normalizedDID = normalizeUserDID(userDID)
    bumpGeneration(for: normalizedDID)

    do {
      return try await withMLSUserPermit(for: normalizedDID) {
        // No advisory lock needed - SQLite WAL handles concurrent access
        // Cross-process coordination uses `MLSStateChangeNotifier` / `MLSNotificationCoordinator`

        return await self.closeContextLocked(normalizedDID: normalizedDID)
      }
    } catch {
      logger.error("🚨 [MLSClient] Failed to acquire permit for closeContext: \(error.localizedDescription)")
      return false
    }
  }

  private func closeContextLocked(normalizedDID: String) async -> Bool {
    logger.info("🛑 [MLSClient] Closing context for user: \(normalizedDID.prefix(20))...")

    let hadContext = await MLSCoreContext.shared.removeContext(for: normalizedDID)

    apiClients.removeValue(forKey: normalizedDID)
    deviceManagers.removeValue(forKey: normalizedDID)
    recoveryManagers.removeValue(forKey: normalizedDID)

    if hadContext {
      logger.info("   ✅ Context closed and removed from cache")
    } else {
      logger.debug("   ℹ️ No context existed for this user")
    }

    return hadContext
  }
  
  /// Close all contexts except for the specified user
  ///
  /// CRITICAL: Call this during account switching to prevent SQLite connection exhaustion.
  /// This closes all contexts for other users, preventing "out of memory" errors from
  /// accumulated SQLite connections.
  ///
  /// - Parameter keepUserDID: The user DID to keep open (the active user after switch)
  /// - Returns: Number of contexts that were closed
  @discardableResult
  public func closeAllContextsExcept(keepUserDID: String) async -> Int {
    let normalizedKeepDID = normalizeUserDID(keepUserDID)
    logger.info("🧹 [MLSClient] Closing all contexts except: \(normalizedKeepDID.prefix(20))...")

    let usersToForget = Set(apiClients.keys)
      .union(deviceManagers.keys)
      .union(recoveryManagers.keys)
      .filter { $0 != normalizedKeepDID }

    for userDID in usersToForget {
      apiClients.removeValue(forKey: userDID)
      deviceManagers.removeValue(forKey: userDID)
      recoveryManagers.removeValue(forKey: userDID)
    }

    let closedCount = await MLSCoreContext.shared.removeAllContextsExcept(keepUserDid: normalizedKeepDID)
    logger.info("   ✅ Closed \(closedCount) context(s), kept context for \(normalizedKeepDID.prefix(20))")
    return closedCount
  }

  /// Clear all MLS storage for a specific user.
  ///
  /// IMPORTANT: This is a manual, user-initiated operation. It quarantines files (does not delete).
  public func clearStorage(for userDID: String) async throws {
    let normalizedDID = normalizeUserDID(userDID)
    logger.info("🧰 [Diagnostics] Resetting MLS storage for user: \(normalizedDID)")

    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 1: Signal globally that this user is under maintenance
    // ═══════════════════════════════════════════════════════════════════════════
    // This tells ALL processes (main app, NSE) to stop touching this user's data.
    // We increment generation to invalidate any in-flight tasks.
    // ═══════════════════════════════════════════════════════════════════════════
    MLSAppActivityState.setShuttingDown(true, userDID: normalizedDID)
    MLSCoordinationStore.shared.incrementGeneration(for: normalizedDID)
    bumpGeneration(for: normalizedDID)

    // Ensure we clear the shutdown flag when done
    defer {
      MLSAppActivityState.setShuttingDown(false, userDID: normalizedDID)
    }
    
    if let coordinator = storageMaintenanceCoordinator {
      await coordinator.beginStorageMaintenance(for: normalizedDID)
      defer {
        Task { await coordinator.endStorageMaintenance(for: normalizedDID) }
      }
      await coordinator.prepareMLSStorageReset(for: normalizedDID)
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 2: Wait for in-flight operations to notice and cancel
    // ═══════════════════════════════════════════════════════════════════════════
    // Give any active operations a moment to check their generation and bail out.
    // This is a safety buffer, not a hard synchronization point.
    // ═══════════════════════════════════════════════════════════════════════════
    try? await Task.sleep(nanoseconds: 300_000_000)  // 300ms

    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 3: Close the database gate and wait for drain
    // ═══════════════════════════════════════════════════════════════════════════
    // This is the authoritative point where we block new connections and wait
    // for existing ones to finish. If drain times out, we force close.
    // ═══════════════════════════════════════════════════════════════════════════
    do {
      try await MLSDatabaseGate.shared.closeGateAndDrain(for: normalizedDID, timeout: .seconds(5))
      logger.info("✅ [Diagnostics] Database gate closed and drained")
    } catch {
      logger.warning(
        "⚠️ [Diagnostics] Gate drain timed out - force closing: \(error.localizedDescription)")
      await MLSDatabaseGate.shared.forceCloseGate(for: normalizedDID)
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 4: Perform the actual reset
    // ═══════════════════════════════════════════════════════════════════════════

    // Drop in-memory Rust context so it will reload from disk on next operation.
    await MLSCoreContext.shared.removeContext(for: normalizedDID)

    // Quarantine + reset the Swift SQLCipher database.
    try await MLSGRDBManager.shared.quarantineAndResetDatabase(for: normalizedDID)

    // Quarantine the Rust SQLite file (mls-state) so it can be recreated fresh.
    let appSupport = MLSStoragePaths.baseContainerURL()
    let mlsStateDir = appSupport.appendingPathComponent("mls-state", isDirectory: true)

    let didHash = normalizedDID.data(using: .utf8)?.base64EncodedString()
      .replacingOccurrences(of: "/", with: "_")
      .replacingOccurrences(of: "+", with: "-")
      .replacingOccurrences(of: "=", with: "")
      .prefix(64) ?? "default"

    let storageFileURL = mlsStateDir.appendingPathComponent("\(didHash).db")
    let wal = storageFileURL.appendingPathExtension("wal")
    let shm = storageFileURL.appendingPathExtension("shm")

    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withDashSeparatorInDate, .withColonSeparatorInTime]
    let timestamp = formatter.string(from: Date())

    let quarantineDir = mlsStateDir
      .appendingPathComponent("Quarantine", isDirectory: true)
      .appendingPathComponent("\(timestamp)_\(didHash.prefix(16))", isDirectory: true)

    try? FileManager.default.createDirectory(at: quarantineDir, withIntermediateDirectories: true)

    for url in [storageFileURL, wal, shm] {
      guard FileManager.default.fileExists(atPath: url.path) else { continue }
      let dest = quarantineDir.appendingPathComponent(url.lastPathComponent)
      do {
        try FileManager.default.moveItem(at: url, to: dest)
        logger.info("📦 [Diagnostics] Quarantined Rust storage file: \(url.lastPathComponent)")
      } catch {
        logger.warning("⚠️ [Diagnostics] Failed to quarantine \(url.lastPathComponent): \(error.localizedDescription)")
      }
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 5: Reopen the gate for fresh operations
    // ═══════════════════════════════════════════════════════════════════════════
    await MLSDatabaseGate.shared.openGate(for: normalizedDID)

    logger.info("✅ [Diagnostics] MLS storage reset complete for \(normalizedDID)")
  }

  /// Delete specific consumed key package bundles from storage
  ///
  /// Removes bundles that the server has marked as consumed but remain in local storage.
  /// This prevents the "local 101 vs server 47" desync issue without requiring full re-registration.
  ///
  /// - Parameters:
  ///   - userDID: User DID
  ///   - hashRefs: Array of hash references to delete (from server's consumedPackages)
  /// - Returns: Number of bundles successfully deleted
  /// - Throws: MLSError if deletion fails
  public func deleteKeyPackageBundles(for userDID: String, hashRefs: [Data]) async throws -> UInt64
  {
    let normalizedDID = normalizeUserDID(userDID)

    guard !hashRefs.isEmpty else {
      logger.debug("No key package bundles to delete")
      return 0
    }

    logger.info(
      "🗑️ Deleting \(hashRefs.count) consumed key package bundles for \(normalizedDID.prefix(20))..."
    )

    // Call Rust FFI method to delete from both in-memory and persistent storage
    // hashRefs is already [Data], which UniFFI will convert to Vec<Vec<u8>>
    let deletedCount = try await runFFIWithRecovery(for: normalizedDID) { ctx in
      try ctx.deleteKeyPackageBundles(hashRefs: hashRefs)
    }

    logger.info("✅ Deleted \(deletedCount) bundles from storage")

    return deletedCount
  }

  // MARK: - Server Reconciliation (Phase 2)

  /// Reconcile local key package bundles with server inventory
  /// Detects storage corruption and server-client desync
  /// Should be called during app launch after storage load
  /// - Parameter userDID: User DID to reconcile bundles for
  /// - Returns: Tuple of (server available count, local bundle count, desync detected)
  public func reconcileKeyPackagesWithServer(for userDID: String) async throws -> (
    serverAvailable: Int, localBundles: Int, desyncDetected: Bool
  ) {
    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      logger.error(
        "❌ [Reconciliation] API client not configured for user \(normalizedDID) - cannot reconcile")
      throw MLSError.configurationError
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // MULTI-ACCOUNT FIX: Skip reconciliation if this account is not active
    // ═══════════════════════════════════════════════════════════════════════════
    // In multi-account scenarios, the ATProtoClient's getDid() returns whichever
    // account is currently active - NOT necessarily the account we're reconciling for.
    // Instead of throwing an error, gracefully skip reconciliation for inactive accounts.
    // ═══════════════════════════════════════════════════════════════════════════
    let isActiveAccount = await apiClient.isAuthenticatedAs(userDID)
    if !isActiveAccount {
      // This is normal in multi-account scenarios - just skip silently
      logger.info("⏸️ [Reconciliation] Skipping - this account (\(userDID.prefix(20), privacy: .private)...) is not the active account")
      return (serverAvailable: 0, localBundles: 0, desyncDetected: false)
    }

    logger.info(
      "🔍 [Reconciliation] Starting key package reconciliation for user: \(userDID.prefix(20))...")

    // Query local bundle count
    var localCount: Int
    do {
      localCount = Int(
        try await runFFIWithRecovery(for: userDID) { ctx in
          try ctx.getKeyPackageBundleCount()
        })
      logger.info("📍 [Reconciliation] Local bundles in cache: \(localCount)")
    } catch {
      logger.error(
        "❌ [Reconciliation] Failed to query local bundle count: \(error.localizedDescription)")
      throw error
    }

    // SQLite storage is automatic - no need to manually load/hydrate

    // Query server bundle inventory
    var serverStats: BlueCatbirdMlsChatPublishKeyPackages.Output
    do {
      let oldStats = try await apiClient.getKeyPackageStats()
      serverStats = oldStats
      logger.info("📍 [Reconciliation] Server bundle status:")
      logger.info("   - Available: \(serverStats.stats.available)")
      logger.info("   - Published: \(serverStats.stats.published)")
    } catch {
      logger.error(
        "❌ [Reconciliation] Failed to query server inventory: \(error.localizedDescription)")
      throw error
    }

    var desyncDetected = localCount != serverStats.stats.available

    if desyncDetected {
      logger.error("🚨 [Reconciliation] KEY PACKAGE DESYNC DETECTED!")
      logger.error("   Local storage: \(localCount) bundles")
      logger.error("   Server inventory: \(serverStats.stats.available) bundles")
      logger.error("   Difference: \(abs(localCount - serverStats.stats.available)) bundles")

      if localCount == 0 && serverStats.stats.available > 0 {
        logger.error(
          "   ❌ STORAGE CORRUPTION SUSPECTED: Local storage empty but server has \(serverStats.stats.available) bundles"
        )

        // Double-check server inventory before any recovery action
        do {
          serverStats = try await apiClient.getKeyPackageStats()
          logger.info(
            "   📍 [Reconciliation] Confirmation check - server available: \(serverStats.stats.available)"
          )
        } catch {
          logger.warning("   ⚠️ Confirmation check failed: \(error.localizedDescription)")
        }

        if serverStats.stats.available == 0 {
          logger.info("   ✅ Server inventory drained during confirmation - skipping recovery")
          desyncDetected = localCount != serverStats.stats.available
        } else {
          // PHASE 1: Try non-destructive recovery first by reloading context from SQLite
          logger.info("   🔄 [Phase 1] Attempting non-destructive context reload...")
          do {
            let recoveredCount = try await reloadContextFromStorage(for: userDID)
            if recoveredCount > 0 {
              logger.info(
                "   ✅ [Phase 1] Non-destructive recovery successful! Recovered \(recoveredCount) bundles"
              )
              localCount = Int(recoveredCount)
              desyncDetected = localCount != serverStats.stats.available
              // Skip destructive recovery
            } else {
              logger.warning(
                "   ⚠️ [Phase 1] Non-destructive recovery found 0 bundles - proceeding to Phase 2")

              // PHASE 2: Fall back to destructive recovery if non-destructive failed
              let normalizedDID = normalizeUserDID(userDID)
              if let deviceManager = self.deviceManagers[normalizedDID] {
                do {
                  logger.warning("   ⚠️ ⚠️ ⚠️ [Phase 2] INITIATING DESTRUCTIVE RECOVERY ⚠️ ⚠️ ⚠️")
                  logger.warning("   This will delete server bundles and clear local storage")
                  try await deviceManager.recoverFromKeyPackageDesync(userDid: userDID)
                  localCount = 0
                } catch {
                  logger.error("   ❌ Destructive recovery FAILED: \(error.localizedDescription)")
                  logger.error(
                    "      ACTION REQUIRED: Manually call deviceManager.recoverFromKeyPackageDesync(userDid:)"
                  )
                }
              } else {
                logger.error(
                  "   ❌ Cannot auto-recover: deviceManager not configured for user \(normalizedDID)"
                )
                logger.error(
                  "      ACTION: Call deviceManager.recoverFromKeyPackageDesync(userDid:)")
              }
            }
          } catch {
            logger.error("   ❌ Non-destructive recovery failed: \(error.localizedDescription)")

            // PHASE 2: Fall back to destructive recovery on error
            let normalizedDID = normalizeUserDID(userDID)
            if let deviceManager = self.deviceManagers[normalizedDID] {
              do {
                logger.warning("   ⚠️ ⚠️ ⚠️ [Phase 2] INITIATING DESTRUCTIVE RECOVERY ⚠️ ⚠️ ⚠️")
                logger.warning("   This will delete server bundles and clear local storage")
                try await deviceManager.recoverFromKeyPackageDesync(userDid: userDID)
                localCount = 0
              } catch {
                logger.error("   ❌ Destructive recovery FAILED: \(error.localizedDescription)")
                logger.error(
                  "      ACTION REQUIRED: Manually call deviceManager.recoverFromKeyPackageDesync(userDid:)"
                )
              }
            } else {
              logger.error(
                "   ❌ Cannot auto-recover: deviceManager not configured for user \(normalizedDID)")
              logger.error("      ACTION: Call deviceManager.recoverFromKeyPackageDesync(userDid:)")
            }
          }
        }
      } else if localCount > 0 && serverStats.stats.available == 0 {
        logger.error("   ⚠️ SERVER DESYNC: Local has \(localCount) bundles but server has 0")
        logger.error("   📋 Root Cause: Bundles created locally but never uploaded to server")
        logger.error("   🔧 Recovery Required: Upload local bundles to server")
        logger.error(
          "      ACTION: Automatically calling monitorAndReplenishBundles() to upload bundles")

        do {
          logger.info("📤 Auto-repairing: Uploading \(localCount) local bundles to server...")
          let uploadResult = try await monitorAndReplenishBundles(for: userDID)
          logger.info(
            "✅ Auto-repair successful! Uploaded bundles - available: \(uploadResult.available), uploaded: \(uploadResult.uploaded)"
          )
          serverStats = try await apiClient.getKeyPackageStats()
          desyncDetected = localCount != serverStats.stats.available
        } catch {
          logger.error("❌ Auto-repair failed: \(error.localizedDescription)")
          logger.error("   Manual intervention required: Restart app or call reregisterDevice()")
        }
      } else if localCount > serverStats.stats.available {
        let difference = localCount - serverStats.stats.available
        logger.error("   ⚠️ BUNDLE MISMATCH: Local has \(difference) extra bundles")
        logger.error("   📋 Possible Causes:")
        logger.error("      - Server consumed bundles but local cache not updated")
        logger.error("   🔧 Attempting surgical cleanup of consumed bundles...")

        // Fetch consumed packages from server
        do {
          let status = try await apiClient.getKeyPackageStatus(limit: 100)
          logger.info("   📊 Server Status Details:")
          logger.info("      - Published: \(status.stats.published)")
          logger.info("      - Available: \(status.stats.available)")
          logger.info("      - Expired: \(status.stats.expired)")
          logger.info(
            "      - Local vs Available difference: \(difference)"
          )

          // Note: consumedPackages detail is no longer available in the new API
          // Log the desync for manual investigation
          logger.warning("   🔧 Local has extra bundles - consider running syncKeyPackageHashes to reconcile")
          logger.warning("      Manual intervention may be required if issues persist")
        } catch {
          logger.warning("   ⚠️ Could not fetch status info: \(error.localizedDescription)")
          logger.warning(
            "      Desync remains unresolved - monitor for NoMatchingKeyPackage errors")
        }
      } else {
        logger.error(
          "   ⚠️ LOCAL STORAGE DESYNC: Server has \(serverStats.stats.available - localCount) extra bundles"
        )
        logger.error("   📋 Possible Causes:")
        logger.error("      - Deserialization bug dropped bundles from local storage")
        logger.error("      - Storage corrupted after bundles were uploaded")
        
        // CRITICAL FIX: Automatically sync hashes to remove orphaned server packages
        // Orphaned packages cause NoMatchingKeyPackage when others try to add us
        logger.info("   🔄 [AUTO-RECOVERY] Syncing key package hashes to remove orphaned server packages...")
        
        do {
          let syncResult = try await syncKeyPackageHashes(for: userDID)
          
          if syncResult.orphanedCount > 0 {
            logger.info("   ✅ [AUTO-RECOVERY] Deleted \(syncResult.deletedCount) orphaned packages from server")
            logger.info("      - Orphaned packages (on server, not local): \(syncResult.orphanedCount)")
            logger.info("      - Remaining available on server: \(syncResult.remainingAvailable)")
            
            // Check if we need to replenish after orphan cleanup
            if syncResult.remainingAvailable < 20 {
              logger.info("   📦 [AUTO-RECOVERY] Replenishing key packages after orphan cleanup...")
              do {
                let replenishResult = try await monitorAndReplenishBundles(for: userDID)
                logger.info("   ✅ [AUTO-RECOVERY] Replenished to \(replenishResult.available) packages")
                desyncDetected = false  // Recovery successful
              } catch {
                logger.error("   ⚠️ Replenishment failed: \(error.localizedDescription)")
              }
            } else {
              desyncDetected = false  // Orphans cleaned up, remaining are sufficient
            }
          } else {
            logger.warning("   ⚠️ No orphaned packages found - desync cause may be different")
            logger.warning("      Consider calling deviceManager.reregisterDevice(userDid:)")
          }
        } catch {
          logger.error("   ❌ Hash sync failed: \(error.localizedDescription)")
          logger.error("   🔧 Recovery Required: Re-register to regenerate local bundles")
          logger.error("      ACTION: Call deviceManager.reregisterDevice(userDid:)")
        }
      }

      // Log diagnostics for visibility
      logger.warning("   📊 Diagnostic Info:")
      logger.warning("      - User DID: \(userDID.prefix(30), privacy: .private)...")
      logger.warning("      - Local bundle count: \(localCount)")
      logger.warning("      - Server available: \(serverStats.stats.available)")
      logger.warning("      - Server expired: \(serverStats.stats.expired)")

      do {
        let status = try await apiClient.getKeyPackageStatus(limit: 3)
        logger.debug("   📜 Key package stats: published=\(status.stats.published), available=\(status.stats.available), expired=\(status.stats.expired)")
      } catch {
        logger.warning("   ⚠️ Could not query status: \(error.localizedDescription)")
      }
    } else {
      logger.info("✅ [Reconciliation] Key packages in sync:")
      logger.info("   - Local bundles: \(localCount)")
      logger.info("   - Server available: \(serverStats.stats.available)")
      logger.info("   - No desync detected")
    }

    return (
      serverAvailable: serverStats.stats.available, localBundles: localCount,
      desyncDetected: desyncDetected
    )
  }

  // MARK: - Key Package Hash Synchronization (NoMatchingKeyPackage Prevention)

  /// Synchronize key packages at the hash level to prevent NoMatchingKeyPackage errors
  ///
  /// This method solves the root cause of the NoMatchingKeyPackage bug:
  /// - When a device loses its local key packages (app reinstall, storage corruption, etc.)
  ///   the server still has those key packages and will serve them to other users
  /// - When someone tries to add this user to a group, they get an old key package
  /// - The user's device receives a Welcome encrypted to a public key it no longer has
  /// - Result: NoMatchingKeyPackage error and corrupted group state
  ///
  /// This method:
  /// 1. Gets the current device ID (REQUIRED - fails if not registered)
  /// 2. Gets all local key package hashes from the device
  /// 3. Sends them to the server via syncKeyPackages endpoint
  /// 4. Server compares against its available (unconsumed) key packages FOR THIS DEVICE ONLY
  /// 5. Server deletes any "orphaned" packages (on server but not in local storage)
  /// 6. Returns the count of deleted orphaned packages
  ///
  /// MULTI-DEVICE SUPPORT:
  /// The device ID is REQUIRED to ensure only THIS device's key packages are synced.
  /// This prevents Device A from accidentally deleting Device B's packages.
  /// Device ID comes from registerDevice and is persisted in UserDefaults.
  ///
  /// Should be called:
  /// - On app launch after device registration
  /// - After account switch
  /// - When recovering from any storage corruption
  ///
  /// - Parameter userDID: User DID to sync key packages for
  /// - Returns: Tuple of (orphanedCount, deletedCount, remainingAvailable)
  /// - Throws: MLSError.configurationError if device is not registered
  public func syncKeyPackageHashes(for userDID: String) async throws -> (
    orphanedCount: Int, deletedCount: Int, remainingAvailable: Int
  ) {
    let normalizedDID = normalizeUserDID(userDID)
    guard let apiClient = self.apiClients[normalizedDID] else {
      logger.error("❌ [SyncKeyPackages] API client not configured for user \(normalizedDID, privacy: .private)")
      throw MLSError.configurationError
    }

    logger.info("🔄 [SyncKeyPackages] START - user: \(userDID.prefix(20), privacy: .private)...")

    // Step 0: Get device ID (REQUIRED for multi-device support)
    guard let deviceInfo = await getDeviceInfo(for: userDID) else {
      logger.error("❌ [SyncKeyPackages] Device not registered - cannot sync without device ID")
      logger.error("   Call ensureDeviceRegistered() first to register this device")
      throw MLSError.configurationError
    }
    let deviceId = deviceInfo.deviceId
    logger.info("📱 [SyncKeyPackages] Device ID: \(deviceId)")

    // Step 1: Get all local key package hashes
    let localHashes: [String]
    do {
      localHashes = try await getLocalKeyPackageHashes(for: userDID)
      logger.info("📍 [SyncKeyPackages] Found \(localHashes.count) local key packages")
      if localHashes.isEmpty {
        logger.warning(
          "⚠️ [SyncKeyPackages] No local key packages found - all server packages are orphaned!")
      }
    } catch {
      logger.error("❌ [SyncKeyPackages] Failed to get local hashes: \(error.localizedDescription)")
      throw error
    }

    // Step 2: Call server to sync and delete orphaned packages (device ID is required)
    let result:
      (
        serverHashes: [String], orphanedCount: Int, deletedCount: Int, orphanedHashes: [String],
        remainingAvailable: Int
      )
    do {
      result = try await apiClient.syncKeyPackages(localHashes: localHashes, deviceId: deviceId)
      logger.info("📊 [SyncKeyPackages] Server response:")
      logger.info("   - Device: \(deviceId)")
      logger.info("   - Orphaned packages detected: \(result.orphanedCount)")
      logger.info("   - Orphaned packages deleted: \(result.deletedCount)")
      logger.info("   - Remaining available on server: \(result.remainingAvailable)")
    } catch {
      logger.error("❌ [SyncKeyPackages] Server sync failed: \(error.localizedDescription)")
      throw error
    }

    // Step 3: Log results and warnings
    if result.orphanedCount > 0 {
      logger.warning(
        "🗑️ [SyncKeyPackages] Deleted \(result.deletedCount) ORPHANED key packages from server")
      logger.warning(
        "   These packages were on the server but the device no longer has the private keys")
      logger.warning("   Root cause: App reinstall, storage corruption, or cache clear")

      if result.orphanedCount > 5 {
        logger.warning("   Orphaned hashes (first 5):")
        for (i, hash) in result.orphanedHashes.prefix(5).enumerated() {
          logger.warning("      [\(i)] \(hash.prefix(16))...")
        }
        logger.warning("   ... and \(result.orphanedCount - 5) more")
      } else if !result.orphanedHashes.isEmpty {
        logger.warning("   Orphaned hashes:")
        for (i, hash) in result.orphanedHashes.enumerated() {
          logger.warning("      [\(i)] \(hash.prefix(16))...")
        }
      }
    } else {
      logger.info("✅ [SyncKeyPackages] No orphaned key packages found - all synced!")
    }

    // Step 3.5: 🧹 [CLIENT F] Evict stale LOCAL bundles whose hashes the
    // server has no record of for this device. Without this, a desync
    // where local has thousands of zombie bundles (e.g. 6500 local vs
    // 30 server) re-uploads stale-hash arrays on every reconciliation
    // cycle. The server's `syncKeyPackages` response includes the
    // canonical `serverHashes` for THIS device — anything in
    // `localHashes` that is not in `serverHashes` is local-only and
    // should be deleted from local FFI storage.
    do {
      let serverHashSet = Set(result.serverHashes.map { $0.lowercased() })
      let staleLocalHashes = localHashes.filter {
        !serverHashSet.contains($0.lowercased())
      }

      // Cap evictions per cycle so we don't stall the FFI on a runaway
      // desync (e.g. 6500 stale entries) while still making forward
      // progress on subsequent runs.
      //
      // 🧹 [CLIENT K] Adaptive cap: when initial drift exceeds 1000 the
      // user is in a high-drift state (typical: thousands of zombie
      // bundles after FFI restore from backup or storage corruption).
      // Raise the cap to 2000 so they drain in ~3 cycles instead of
      // ~10. Each cycle ships 184-438 KB of zombie hashes; faster drain
      // measurably reduces wasted bandwidth. Conservative 500 cap stays
      // for normal/small drift to avoid stalling the FFI for routine
      // reconciliations.
      let highDrift = staleLocalHashes.count > 1000
      let evictionCap = highDrift ? 2000 : 500
      if highDrift {
        logger.warning(
          "🧹 [CLIENT K] High-drift KP eviction (\(staleLocalHashes.count) stale) — raising cap to \(evictionCap) for faster drain"
        )
      }
      let toEvict = Array(staleLocalHashes.prefix(evictionCap))

      if !toEvict.isEmpty {
        // Convert hex hashes back to Data hashRefs for the FFI.
        let hashRefs: [Data] = toEvict.compactMap { Data(hexEncoded: $0) }
        if hashRefs.count != toEvict.count {
          logger.warning(
            "🧹 [CLIENT F] \(toEvict.count - hashRefs.count) hash strings failed hex decode and were skipped"
          )
        }
        if !hashRefs.isEmpty {
          do {
            let evicted = try await deleteKeyPackageBundles(for: userDID, hashRefs: hashRefs)
            logger.warning(
              "🧹 [CLIENT F] Evicted \(evicted) stale local key packages (totalStale=\(staleLocalHashes.count), capped at \(evictionCap))"
            )
            if staleLocalHashes.count > evictionCap {
              logger.warning(
                "🧹 [CLIENT F] \(staleLocalHashes.count - evictionCap) stale local packages deferred to next sync cycle"
              )
            }
          } catch {
            logger.error(
              "🧹 [CLIENT F] deleteKeyPackageBundles failed: \(error.localizedDescription) — leaving stale bundles in place"
            )
          }
        }
      } else {
        logger.debug("🧹 [CLIENT F] No stale local key packages to evict")
      }
    }

    // Step 4: Check if replenishment is needed
    if result.remainingAvailable < 20 {
      logger.warning(
        "⚠️ [SyncKeyPackages] Low key package inventory: \(result.remainingAvailable) remaining")
      logger.warning("   Consider calling monitorAndReplenishBundles() to upload more")
    }

    logger.info("✅ [SyncKeyPackages] COMPLETE")

    return (
      orphanedCount: result.orphanedCount,
      deletedCount: result.deletedCount,
      remainingAvailable: result.remainingAvailable
    )
  }
}

/// Adapter to expose Keychain access to Rust FFI
/// This allows the Rust layer to store sensitive keys in the system Keychain
/// while keeping bulk data in SQLite.
public class MLSKeychainAdapter: KeychainAccess {
  public func read(key: String) throws -> Data? {
    return try MLSKeychainManager.shared.retrieve(forKey: key)
  }

  public func write(key: String, value: Data) throws {
    try MLSKeychainManager.shared.store(value, forKey: key)
  }

  public func delete(key: String) throws {
    try MLSKeychainManager.shared.delete(forKey: key)
  }
}
