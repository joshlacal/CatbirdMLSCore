import Foundation
import GRDB
import OSLog
import Petrel
import Synchronization

extension MLSConversationManager {

  // MARK: - App Suspension (0xdead10cc Prevention)

  /// Flag indicating MLS operations are suspended for app backgrounding
  /// This is different from `isShuttingDown` - suspension is temporary and reversible
  /// Protected by Mutex for thread-safe access from any isolation context
  private static let _isSuspending = Mutex<Bool>(false)

  /// Check if MLS operations are suspended for app backgrounding
  public var isSuspending: Bool {
    get { Self._isSuspending.withLock { $0 } }
    set { Self._isSuspending.withLock { $0 = newValue } }
  }

  /// Suspend all MLS operations when app enters background
  /// This prevents 0xdead10cc crashes by ensuring no database operations
  /// continue running when iOS suspends the app
  ///
  /// Call this BEFORE GRDBSuspensionCoordinator.setLifecycleSuspended() to ensure
  /// MLS tasks are cancelled before GRDB starts rejecting operations
  ///
  /// - Note: This is NOT the same as shutdown() - we don't close databases or clear state
  ///         We simply cancel in-flight tasks to prevent them from holding locks
  @MainActor
  public func suspendMLSOperations() {
    logger.info("⏸️ [SUSPEND] Suspending MLS operations for app background")
    MLSSuspensionFlightRecorder.shared.record(
      .suspensionPrepare,
      details: "MLSConversationManager.suspendMLSOperations",
      process: "app"
    )

    // Set flag to reject new operations
    isSuspending = true
    isSyncPaused = true
    MLSCoreContext.markSuspensionInProgress()
    MLSClient.markSuspensionInProgress(reason: "MLSConversationManager.suspendMLSOperations")
    // Reset circuit breaker during lifecycle suspension so transient suspended errors
    // cannot strand foreground sync for the full backoff window.
    consecutiveSyncFailures = 0
    syncPausedAt = nil

    // Cancel all background tasks that might hold database connections
    // These tasks perform database operations and would cause 0xdead10cc if running during suspension

    if let task = missingConversationsTask {
      task.cancel()
      logger.debug("   Cancelled missingConversationsTask")
    }
    missingConversationsTask = nil

    if let task = keyPackageRefreshTask {
      task.cancel()
      logger.debug("   Cancelled keyPackageRefreshTask")
    }
    keyPackageRefreshTask = nil

    if let task = groupInfoRefreshTask {
      task.cancel()
      logger.debug("   Cancelled groupInfoRefreshTask")
    }
    groupInfoRefreshTask = nil

    if let task = periodicSyncTask {
      task.cancel()
      logger.debug("   Cancelled periodicSyncTask")
    }
    periodicSyncTask = nil

    if let task = orphanAdoptionTask {
      task.cancel()
      logger.debug("   Cancelled orphanAdoptionTask")
    }
    orphanAdoptionTask = nil

    if let task = cleanupTask {
      task.cancel()
      logger.debug("   Cancelled cleanupTask (background cleanup)")
    }
    cleanupTask = nil

    // Also cancel tracked tasks to invalidate any in-flight operations
    cancelAllTrackedTasks()

    logger.info("✅ [SUSPEND] MLS operations suspended - safe for app suspension")
  }

  /// Resume MLS operations when app returns to foreground
  /// This restarts background tasks that were cancelled during suspension
  @MainActor
  public func resumeMLSOperations() {
    guard isSuspending else {
      logger.debug("🔄 [RESUME] MLS not suspended - nothing to resume")
      return
    }

    logger.info("▶️ [RESUME] Resuming MLS operations after app foreground")
    MLSSuspensionFlightRecorder.shared.record(
      .resumeFromSuspension,
      details: "MLSConversationManager.resumeMLSOperations",
      process: "app"
    )

    // Clear suspension flags
    isSuspending = false
    isSyncPaused = false
    MLSCoreContext.clearSuspensionFlag()
    MLSClient.clearSuspensionFlag(reason: "MLSConversationManager.resumeMLSOperations")

    // Restart background tasks (only if we're initialized)
    guard isInitialized else {
      logger.debug("   Skipping task restart - not initialized")
      return
    }

    // Restart periodic tasks
    if configuration.enableAutomaticCleanup && cleanupTask == nil {
      startBackgroundCleanup()
    }

    if periodicSyncTask == nil {
      startPeriodicSync()
    }

    if orphanAdoptionTask == nil {
      startOrphanAdoptionTask()
    }

    if groupInfoRefreshTask == nil {
      startGroupInfoRefreshTask()
    }

    if let activeDid = userDid, !configuration.skipDeviceRecordPublishing {
      Task { [weak self] in
        guard let self else { return }
        do {
          try await self.deviceRecordService.ensureDeviceRecordPublished(userDid: activeDid)
        } catch {
          self.logger.error("Failed to publish device record on resume: \(error.localizedDescription)")
        }
      }
    }

    // Note: missingConversationsTask is typically only run during initialization
    // If needed, it will be triggered by syncWithServer or explicit rejoin requests

    logger.info("✅ [RESUME] MLS operations resumed")
  }

  internal func throwIfShuttingDown(_ operation: String) throws {
    if isShuttingDown {
      logger.warning("⏸️ [MLSConversationManager] \(operation) aborted - storage reset in progress")
      throw MLSConversationError.operationFailed("MLS storage reset in progress")
    }

    // Stop-The-World: Verify coordination generation hasn't moved
    // This catches "zombie" tasks from a previous user context after an account switch
    do {
      try MLSCoordinationStore.shared.validateGeneration(currentCoordinationGeneration)
    } catch {
      logger.error("🛑 [COORD] Generation mismatch in \(operation) - aborting stale task")
      throw error
    }
  }

  /// Prepare the conversation manager for a storage reset operation
  /// This is similar to shutdown() but specifically for storage maintenance
  @MainActor
  public func prepareForStorageReset() async {
    guard !isShuttingDown else {
      logger.debug("MLSConversationManager already preparing for storage reset")
      return
    }

    logger.info("⚠️ [MLSConversationManager] Preparing for SQLCipher storage reset")

    // CRITICAL: Capture userDid before clearing state
    let resetUserDid = userDid
    userDid = nil  // Fail-fast any new operations

    isShuttingDown = true

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX: Force-release any stuck permits BEFORE cancelling tasks
    // ═══════════════════════════════════════════════════════════════════════════
    // Background tasks may hold permits and never release them if cancelled mid-operation.
    // This would cause closeContext() to hang forever waiting to acquire a permit.
    // By force-releasing all permits first, we ensure subsequent operations won't deadlock.
    // ═══════════════════════════════════════════════════════════════════════════
    if let resetUserDid = resetUserDid {
      logger.info("🔓 [MLSConversationManager] Force-releasing all permits for shutdown")
      await MLSUserOperationCoordinator.shared.forceReleaseAll(for: resetUserDid)
    }

    // Cancel all background tasks
    cleanupTask?.cancel()
    cleanupTask = nil

    periodicSyncTask?.cancel()
    periodicSyncTask = nil

    orphanAdoptionTask?.cancel()
    orphanAdoptionTask = nil

    groupInfoRefreshTask?.cancel()
    groupInfoRefreshTask = nil

    // CRITICAL: Cancel the missing conversations task to prevent hang during reset
    missingConversationsTask?.cancel()
    missingConversationsTask = nil

    keyPackageRefreshTask?.cancel()
    keyPackageRefreshTask = nil

    deduplicationCleanupTimer?.invalidate()
    deduplicationCleanupTimer = nil

    // Shutdown device sync manager
    if let deviceSyncManager = deviceSyncManager {
      await deviceSyncManager.shutdown()
      self.deviceSyncManager = nil
    }

    // Clear in-memory state
    conversations.removeAll()
    groupStates.removeAll()
    recentlySentMessages.removeAll()
    pendingMessages.removeAll()
    ownCommits.removeAll()
    conversationStates.removeAll()
    await keyPackageManager.clearAllExhaustedKeyPackages()
    observers.withLock { $0.removeAll() }
    isInitialized = false
    isSyncing = false

    // ═══════════════════════════════════════════════════════════════════════════
    // Use MLSShutdownCoordinator for proper shutdown sequence
    // ═══════════════════════════════════════════════════════════════════════════
    if let resetUserDid = resetUserDid {
      // Close the app-layer MLSClient context first
      let closedAppContext = await MLSClient.shared.closeContext(for: resetUserDid)
      if closedAppContext {
        logger.info("✅ [MLSConversationManager] Closed MLSClient context (app layer) for reset")
      }

      // Use the centralized shutdown coordinator for core infrastructure
      // This handles: FFI context (core) → WAL checkpoint → DB close → 200ms sleep
      let result = await MLSShutdownCoordinator.shared.shutdown(
        for: resetUserDid, databaseManager: databaseManager, timeout: 5.0)

      switch result {
      case .success(let durationMs):
        logger.info(
          "✅ [MLSConversationManager] Core shutdown for reset complete in \(durationMs)ms")
      case .successWithWarnings(let durationMs, let warnings):
        logger.warning(
          "⚠️ [MLSConversationManager] Core shutdown for reset in \(durationMs)ms with \(warnings.count) warning(s)"
        )
      case .timedOut(let durationMs, let phase):
        logger.critical(
          "🚨 [MLSConversationManager] Core shutdown for reset timed out at \(phase.rawValue) after \(durationMs)ms"
        )
      case .failed(let error):
        logger.critical(
          "🚨 [MLSConversationManager] Core shutdown for reset failed: \(error.localizedDescription)"
        )
      }
    } else {
      logger.warning(
        "⚠️ [MLSConversationManager] No user DID for storage reset - skipping core shutdown")
    }

    logger.info("✅ [MLSConversationManager] Ready for storage reset")
  }

  /// Shutdown the conversation manager for account switching
  ///
  /// CRITICAL: Call this method BEFORE switching to a different user account.
  /// This ensures:
  /// 1. All background tasks are cancelled
  /// 2. The database connection is properly released
  /// 3. No stale operations from the previous user can corrupt the new user's data
  ///
  /// After calling shutdown(), you must create a NEW MLSConversationManager instance
  /// for the new user - do NOT reuse the existing instance.
  ///
  /// Note: This method has a 5-second timeout to prevent hanging during account switch.
  @MainActor
  @discardableResult
  public func shutdown() async -> Bool {
    guard !isShuttingDown else {
      logger.debug("MLSConversationManager already shutting down")
      return false
    }

    logger.info(
      """
      🛑 [SHUTDOWN-START] Starting graceful shutdown
      User: \(self.userDid?.prefix(20) ?? "unknown", privacy: .private)...
      Generation: \(self.currentCoordinationGeneration, privacy: .public)
      Initialized: \(self.isInitialized, privacy: .public)
      Syncing: \(self.isSyncing, privacy: .public)
      Thread: \(Thread.current.description, privacy: .public)
      Active tasks: \(self.activeTasks.count, privacy: .public)
      """)

    // CRITICAL FIX: Capture userDid and immediately clear the property
    // This causes any racing operations to fail fast with nil check instead of
    // proceeding with stale user context. Previously, stale sync operations could
    // read userDid after isShuttingDown was set but before cleanup completed.
    let shutdownUserDid = userDid
    userDid = nil  // Immediately invalidate to fail-fast any new operations

    isShuttingDown = true
    isSyncPaused = true  // CRITICAL: Reject any new sync attempts immediately
    var shutdownWasSafe = true

    logger.info(
      """
      📝 [SHUTDOWN-STEP-1] User context invalidated
      Captured DID: \(shutdownUserDid?.prefix(20) ?? "none", privacy: .private)...
      Shutdown flag set: true
      Sync paused: true
      """)

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX: Cross-process wait for active database operations
    // ═══════════════════════════════════════════════════════════════════════════
    // ViewModels, background tasks, and NSE may still be executing database operations.
    // Draining is now handled by MLSShutdownCoordinator below.
    // ═══════════════════════════════════════════════════════════════════════════
    // 2. Wait a moment for writes to finish (optional but recommended for SQLCipher flush)
    try? await Task.sleep(nanoseconds: 200_000_000)

    // Cancel all background tasks immediately and wait for them to finish
    // This is the "Stop-the-World" phase for app-level concurrency

    logger.info("📝 [SHUTDOWN-STEP-3] Cancelling background tasks...")
    // 1. Cancel all tracked tasks (this invalidates their generation)
    cancelAllTrackedTasks()
    logger.info("   Tracked tasks cancelled")

    // 2. Collect all local tasks
    var localTasks: [Task<Void, Never>] = []

    if let task = cleanupTask { localTasks.append(task) }
    if let task = periodicSyncTask { localTasks.append(task) }
    if let task = orphanAdoptionTask { localTasks.append(task) }
    if let task = groupInfoRefreshTask { localTasks.append(task) }
    // CRITICAL FIX: Include missingConversationsTask which runs External Commit operations
    // Previously this task was fire-and-forget, causing 40+ second hangs during account switch
    if let task = missingConversationsTask { localTasks.append(task) }
    if let task = keyPackageRefreshTask { localTasks.append(task) }

    // 3. Trigger cancellation
    cleanupTask?.cancel()
    cleanupTask = nil

    periodicSyncTask?.cancel()
    periodicSyncTask = nil

    orphanAdoptionTask?.cancel()
    orphanAdoptionTask = nil

    groupInfoRefreshTask?.cancel()
    groupInfoRefreshTask = nil

    // CRITICAL: Cancel the missing conversations task - this is the main culprit for hangs
    // External Commit operations inside detectAndRejoinMissingConversations() are long-running
    missingConversationsTask?.cancel()
    missingConversationsTask = nil

    keyPackageRefreshTask?.cancel()
    keyPackageRefreshTask = nil

    // 4. Timers don't support async wait, just invalidate
    deduplicationCleanupTimer?.invalidate()
    deduplicationCleanupTimer = nil

    logger.info("   Local tasks to wait for: \(localTasks.count, privacy: .public)")
    // 5. BLOCKING wait for task completion with timeout
    // CRITICAL FIX: Tasks must complete before shutdown proceeds to prevent zombie tasks
    // that would wake up after account switch and process data for the wrong user
    if !localTasks.isEmpty {
      logger.info(
        "⏳ [SHUTDOWN-STEP-3] BLOCKING wait for \(localTasks.count, privacy: .public) background tasks..."
      )

      // Use a proper blocking wait with timeout
      let tasksCompleted = await withTaskGroup(of: Bool.self) { group in
        // Task that waits for all background tasks to complete
        group.addTask {
          await withTaskGroup(of: Void.self) { innerGroup in
            for task in localTasks {
              innerGroup.addTask { await task.value }
            }
            await innerGroup.waitForAll()
          }
          return true
        }

        // Timeout task - 2 seconds max wait
        group.addTask {
          try? await Task.sleep(nanoseconds: 2 * 1_000_000_000)
          return false
        }

        // Return first result (either tasks completed or timeout)
        let first = await group.next() ?? false
        group.cancelAll()
        return first
      }

      if tasksCompleted {
        logger.info("✅ [SHUTDOWN] All background tasks completed cleanly")
      } else {
        logger.warning("⚠️ [SHUTDOWN] Background task timeout - forcing ahead")
        // Tasks are already cancelled, we just couldn't wait for their completion
      }
    }

    // Shutdown device sync manager with timeout
    if let deviceSyncManager = deviceSyncManager {
      await deviceSyncManager.shutdown()
      self.deviceSyncManager = nil
    }

    // Clear in-memory state to prevent stale data usage
    // This also helps garbage collection
    conversations.removeAll()
    groupStates.removeAll()
    recentlySentMessages.removeAll()
    pendingMessages.removeAll()
    ownCommits.removeAll()
    conversationStates.removeAll()
    await keyPackageManager.clearAllExhaustedKeyPackages()
    observers.withLock { $0.removeAll() }

    // Clear consumption tracking
    keyPackageMonitor = nil
    consumptionTracker = nil

    // Mark as not initialized so any lingering calls will fail fast
    isInitialized = false
    isSyncing = false

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL: Use MLSShutdownCoordinator for proper shutdown sequence
    // ═══════════════════════════════════════════════════════════════════════════
    // The shutdown sequence MUST be:
    // 1. Close FFI context (release Rust SQLite handles)
    // 2. Checkpoint WAL (flush pending writes)
    // 3. Close Swift DB (close GRDB pool)
    // 4. Sleep 200ms (let OS reclaim mlocked memory)
    //
    // MLSShutdownCoordinator enforces this sequence. We also close the app-layer
    // MLSClient context separately since it has its own cache.
    // ═══════════════════════════════════════════════════════════════════════════

    if let shutdownUserDid = shutdownUserDid {
      // Step 1: Close the app-layer MLSClient context (separate from core package)
      let closedAppContext = await MLSClient.shared.closeContext(for: shutdownUserDid)
      if closedAppContext {
        logger.info("✅ [MLSConversationManager.shutdown] Closed MLSClient context (app layer)")
      }

      // Step 2: Use the centralized shutdown coordinator for core infrastructure
      // This handles: FFI context (core) → WAL checkpoint → DB close → 200ms sleep
      let result = await MLSShutdownCoordinator.shared.shutdown(
        for: shutdownUserDid, databaseManager: databaseManager)

      switch result {
      case .success(let durationMs):
        logger.info("✅ [MLSConversationManager.shutdown] Core shutdown complete in \(durationMs)ms")
      case .successWithWarnings(let durationMs, let warnings):
        logger.warning(
          "⚠️ [MLSConversationManager.shutdown] Core shutdown in \(durationMs)ms with warnings:")
        for warning in warnings {
          logger.warning("   - \(warning)")
        }
      case .timedOut(let durationMs, let phase):
        shutdownWasSafe = false
        logger.critical(
          "🚨 [MLSConversationManager.shutdown] Core shutdown timed out at \(phase.rawValue) after \(durationMs)ms"
        )
      case .failed(let error):
        shutdownWasSafe = false
        logger.critical(
          "🚨 [MLSConversationManager.shutdown] Core shutdown failed: \(error.localizedDescription)")
      }
    }

    if shutdownWasSafe {
      logger.info("✅ [MLSConversationManager.shutdown] Shutdown complete - safe to switch accounts")
    } else {
      logger.critical("🚨 [MLSConversationManager.shutdown] Shutdown complete but was NOT safe")
    }

    return shutdownWasSafe
  }

  /// Reload MLS group state from disk to catch up with NSE changes
  @MainActor
  public func reloadStateFromDisk() async {
    guard let userDid = userDid else {
      logger.warning("🔄 [MLS Reload] No user DID - skipping state reload")
      return
    }

    // Mark reload as in progress to block concurrent MLS operations
    isStateReloadInProgress = true
    lastForegroundTime = Date()

    logger.info("🔄 [MLS Reload] Reloading MLS state from disk for user: \(userDid.prefix(20))...")
    logger.info("   Reason: NSE may have advanced the ratchet while app was in background")

    // Track how many groups we're invalidating
    let groupCount = groupStates.count
    let conversationCount = conversationStates.count

    // Step 1: Clear in-memory group states
    groupStates.removeAll()

    // Step 2: Clear conversation initialization states
    conversationStates.removeAll()

    // Step 3: Clear pending message tracking
    pendingMessagesLock.withLock {
      pendingMessages.removeAll()
    }

    // Step 4: Clear own commits tracking
    ownCommitsLock.withLock {
      ownCommits.removeAll()
    }

    // Step 5: Clear recently sent messages deduplication
    recentlySentMessages.removeAll()

    logger.info(
      "✅ [MLS Reload] Cleared \(groupCount) group states, \(conversationCount) conversation states")

    // Step 6: Reload epoch checkpoint cache from disk (may have been updated by NSE)
    await CatbirdMLSCore.MLSEpochCheckpoint.shared.reloadCacheFromDisk()

    // Step 7: Reload MLS context from disk
    do {
      try await MLSCoreContext.shared.reloadContext(for: userDid)
      logger.info("✅ [MLS Reload] MLSCoreContext reloaded from disk")
    } catch {
      logger.warning(
        "⚠️ [MLS Reload] Failed to reload MLSCoreContext: \(error.localizedDescription)")
    }

    // Step 7: Mark reload as complete and notify waiters
    isStateReloadInProgress = false
    let waiters = stateReloadWaiters
    stateReloadWaiters.removeAll()
    for waiter in waiters {
      waiter.resume()
    }
    logger.debug("🔄 [MLS Reload] Notified \(waiters.count) waiting operation(s)")

    // Step 8: Optionally trigger a sync
    Task(priority: .userInitiated) { [weak self] in
      guard let self = self else { return }
      do {
        try await self.syncWithServer(fullSync: false)
        self.logger.info("✅ [MLS Reload] Post-reload sync completed")
      } catch {
        self.logger.warning("⚠️ [MLS Reload] Post-reload sync failed: \(error.localizedDescription)")
      }
    }
  }

  public func ensureStateReloaded() async throws {
    let needsToWait = await MainActor.run { [self] in
      return isStateReloadInProgress
    }

    if needsToWait {
      logger.info("⏳ [MLS Reload] Operation waiting for state reload to complete...")

      await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
        Task { @MainActor in
          stateReloadWaiters.append(continuation)
        }
      }

      logger.info("✅ [MLS Reload] State reload completed - operation may proceed")
      return
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL FIX: Check if NSE processed a message while app was backgrounded
    // ═══════════════════════════════════════════════════════════════════════════
    // If NSE decrypted a message, the MLS ratchet was advanced but the foreground
    // app may have stale in-memory state. Force a reload to prevent SecretReuseError.
    // ═══════════════════════════════════════════════════════════════════════════
    if let userDid = await MainActor.run(body: { [self] in self.userDid }) {
      if MLSAppActivityState.hasNSEProcessed(for: userDid) {
        logger.warning(
          "⚠️ [MLS Reload] NSE processed message while backgrounded - forcing state reload")

        // Clear the flag BEFORE reloading to prevent duplicate reloads
        MLSAppActivityState.clearNSEProcessedFlag()

        await reloadStateFromDisk()
        logger.info("✅ [MLS Reload] State reloaded after NSE processing")
        return
      }
    }

    let timeSinceForeground = await MainActor.run { [self] () -> TimeInterval? in
      guard let lastForeground = lastForegroundTime else { return nil }
      return Date().timeIntervalSince(lastForeground)
    }

    if let elapsed = timeSinceForeground, elapsed < foregroundSyncGracePeriod {
      logger.debug(
        "🔄 [MLS Reload] Within grace period (\(String(format: "%.1f", elapsed))s) - state should be fresh"
      )
    }
  }

  /// Initialize the MLS crypto context
  public func initialize() async throws {
    guard !isInitialized else {
      logger.debug("MLS context already initialized")
      return
    }

    if let userDid = userDid {
      await MLSClient.shared.configure(
        for: userDid, apiClient: apiClient, atProtoClient: atProtoClient)

      // CRITICAL FIX: Ensure device is registered with MLS server before proceeding
      // This prevents "Missing key packages" errors if device registration was skipped/removed
      do {
        try await MLSClient.shared.ensureDeviceRegistered(userDid: userDid)
        logger.info("✅ Device registered with MLS server")
      } catch {
        logger.error("❌ Failed to register device with MLS server: \(error.localizedDescription)")
        // Continue initialization but warn - functionality may be limited
      }

      if !configuration.skipDeviceRecordPublishing {
        do {
          try await deviceRecordService.ensureDeviceRecordPublished(userDid: userDid)
        } catch {
          logger.error(
            "❌ [MLS Init] Failed to publish device record: \(error.localizedDescription)"
          )
        }
      } else {
        logger.info("Skipping device record publish (skipDeviceRecordPublishing=true)")
      }

      logger.info("Loading persisted MLS storage for user: \(userDid)")
      do {
        logger.info("✅ MLS storage loaded successfully")

        do {
          let localBundleCount = try await MLSClient.shared.getKeyPackageBundleCount(for: userDid)
          logger.info("📊 [MLS Init] Local bundle count: \(localBundleCount)")

          if localBundleCount == 0 {
            logger.warning("⚠️ [MLS Init] No local bundles found - will need replenishment")
            Task {
              do {
                let result = try await MLSClient.shared.reconcileKeyPackagesWithServer(for: userDid)
                logger.info(
                  "📊 [MLS Init] Reconciliation complete - server: \(result.serverAvailable), local: \(result.localBundles), desync: \(result.desyncDetected)"
                )
              } catch {
                logger.error("❌ [MLS Init] Reconciliation failed: \(error.localizedDescription)")
              }
            }
          }

          Task {
            do {
              let syncResult = try await MLSClient.shared.syncKeyPackageHashes(for: userDid)
              if syncResult.orphanedCount > 0 {
                logger.warning(
                  "🔄 [MLS Init] Synced key packages - deleted \(syncResult.deletedCount) ORPHANED packages"
                )
                logger.info("   Remaining valid packages: \(syncResult.remainingAvailable)")
              } else {
                logger.info("✅ [MLS Init] Key package hashes in sync - no orphans found")
              }

              // CRITICAL FIX: If server has 0 key packages for THIS device, we must upload immediately
              // This can happen after app reinstall or device re-registration when old packages
              // belong to a different device_id. Without this fix, invites fail with NoMatchingKeyPackage.
              if syncResult.remainingAvailable == 0 {
                logger.warning("🚨 [MLS Init] Server has 0 key packages for this device - uploading batch now")
                do {
                    try await self.keyPackageManager.uploadKeyPackageBatchSmart(for: userDid, count: 25)
                  logger.info("✅ [MLS Init] Emergency key package upload complete")
                } catch {
                  logger.error("❌ [MLS Init] Emergency key package upload failed: \(error.localizedDescription)")
                }
              }
            } catch {
              logger.error(
                "❌ [MLS Init] Key package hash sync failed: \(error.localizedDescription)")
            }
          }
        } catch {
          logger.warning(
            "⚠️ [MLS Init] Failed to check local bundle count: \(error.localizedDescription)")
        }
      } catch {
        logger.warning(
          "⚠️ Failed to load MLS storage (will start fresh): \(error.localizedDescription)")
      }
    } else {
      logger.warning("No user DID provided - MLS storage will not be persisted")
    }

    logger.info("MLS context initialized successfully")
    isInitialized = true

    if let userDid = userDid {
      consumptionTracker = MLSConsumptionTracker(userDID: userDid, dbManager: databaseManager)
      keyPackageMonitor = MLSKeyPackageMonitor(
        userDID: userDid,
        consumptionTracker: consumptionTracker,
        dbManager: databaseManager
      )
      logger.info("✅ Initialized smart key package monitoring")

      if let deviceSyncManager = deviceSyncManager {
        let deviceInfo = await mlsClient.getDeviceInfo(for: userDid)
        let deviceUUID = deviceInfo?.deviceUUID

        await deviceSyncManager.configure(
          userDid: userDid,
          deviceUUID: deviceUUID,
          addDeviceHandler: { [weak self] convoId, deviceCredentialDid, keyPackageData in
            guard let self = self else { throw MLSConversationError.contextNotInitialized }
            return try await self.addDeviceWithKeyPackage(
              convoId: convoId,
              deviceCredentialDid: deviceCredentialDid,
              keyPackageData: keyPackageData
            )
          }
        )
        await deviceSyncManager.startPolling(interval: 60)
        logger.info(
          "✅ Configured device sync manager for multi-device support (deviceUUID: \(deviceUUID ?? "not registered"))"
        )
      }
    }

    keyPackageRefreshTask = Task(priority: .utility) { [weak self] in
      guard let self else { return }
      do {
        try Task.checkCancellation()
        try await self.smartRefreshKeyPackages()
        try Task.checkCancellation()
        await self.keyPackageManager.setLastRefresh(Date())
      } catch is CancellationError {
        self.logger.warning("⚠️ Initial key package upload cancelled - will retry on next trigger")
      } catch {
        self.logger.error("Failed to upload initial key packages: \(error.localizedDescription)")
      }
    }

    await validateGroupStates()

    // Run detectAndRejoinMissingConversations in background to avoid blocking startup
    // CRITICAL FIX: Store task reference so it can be properly cancelled during shutdown
    // Previously this was a fire-and-forget Task.detached which caused 40+ second hangs
    // during account switching as External Commit operations continued running
    missingConversationsTask = Task(priority: .utility) { [weak self] in
      guard let self else { return }
      do {
        // Check for cancellation before starting potentially long operation
        try Task.checkCancellation()
        try await self.detectAndRejoinMissingConversations()
      } catch is CancellationError {
        self.logger.info("📭 Missing conversation detection cancelled (expected during shutdown)")
      } catch {
        self.logger.error(
          "Failed to auto-rejoin missing conversations: \(error.localizedDescription)")
      }
    }
    if configuration.enableAutomaticCleanup {
      startBackgroundCleanup()
    }

    startPeriodicSync()
    startOrphanAdoptionTask()
    startGroupInfoRefreshTask()
  }

  internal func validateGroupStates() async {
    logger.info("🔍 [STARTUP] Validating MLS group state for all conversations...")

    guard let userDid = userDid else {
      logger.warning("[STARTUP] No user DID - skipping group state validation")
      return
    }

    do {
      let conversations = try await database.read { db in
        try MLSConversationModel
          .filter(MLSConversationModel.Columns.currentUserDID == userDid)
          .fetchAll(db)
      }

      logger.info("📋 [STARTUP] Found \(conversations.count) conversations to validate")

      var corruptedCount = 0
      var validatedCount = 0

      for conversation in conversations {
        // CRITICAL FIX: Check for shutdown/cancellation before each iteration
        // to prevent "false corruption" detection during account switching
        if isShuttingDown || Task.isCancelled {
          logger.warning("⚠️ [STARTUP] Validation interrupted by shutdown - state is likely FINE")
          return
        }

        let groupIdData = conversation.groupID
        let convoIdPrefix = String(conversation.conversationID.prefix(8))

        do {
          let epoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
          logger.debug("✅ Group \(convoIdPrefix)... validated - epoch: \(epoch)")
          validatedCount += 1
        } catch is CancellationError {
          // CRITICAL FIX: Do NOT treat CancellationError as corruption!
          // This happens during shutdown/account switch - the group is fine
          logger.warning(
            "⚠️ [STARTUP] Validation cancelled for \(convoIdPrefix)... - state preserved (not corruption)"
          )
          return  // Stop entire loop, do not delete anything
        } catch {
          let errorDesc = error.localizedDescription.lowercased()

          // CRITICAL FIX: Lock/busy errors are NOT corruption
          if errorDesc.contains("lock") || errorDesc.contains("busy")
            || errorDesc.contains("shutdown")
          {
            logger.warning(
              "⚠️ [STARTUP] Lock/busy during validation for \(convoIdPrefix)... - skipping (not corruption)"
            )
            continue  // Skip this group, try the rest
          }

          // Only treat genuine MLS errors as corruption
          logger.error(
            "❌ [STARTUP] Corrupted group state detected for conversation \(convoIdPrefix)...")
          logger.error("   Error: \(error.localizedDescription)")

          do {
            try await mlsClient.deleteGroup(for: userDid, groupId: groupIdData)
            logger.info("🗑️ Deleted corrupted local group state for \(convoIdPrefix)...")
          } catch {
            logger.error("   Failed to delete corrupted group: \(error.localizedDescription)")
          }

          do {
            try await markConversationNeedsRejoin(conversation.conversationID)
            logger.info("⚠️ Marked conversation \(convoIdPrefix)... for rejoin")
            corruptedCount += 1
          } catch {
            logger.error("   Failed to mark conversation for rejoin: \(error.localizedDescription)")
          }
        }
      }

      if corruptedCount > 0 {
        logger.warning(
          "⚠️ [STARTUP] Found \(corruptedCount) conversation(s) with corrupted MLS state - marked for rejoin"
        )
      } else {
        logger.info("✅ [STARTUP] All \(validatedCount) conversation(s) have valid MLS group state")
      }
    } catch {
      logger.error("❌ [STARTUP] Failed to validate group states: \(error.localizedDescription)")
    }
  }

  public func detectAndRejoinMissingConversations() async throws {
    logger.info("🔍 Detecting missing conversations for auto-rejoin")
    try throwIfShuttingDown("detectAndRejoinMissingConversations")

    guard isInitialized else {
      logger.warning("MLS not initialized - skipping missing conversation detection")
      return
    }

    guard let userDid = userDid else {
      logger.warning("No user DID - skipping missing conversation detection")
      return
    }

    guard await ensureActiveAccount(for: userDid, operation: "detectAndRejoinMissingConversations")
    else {
      return
    }

    do {
      let corruptedConvos = try await database.read { db in
        try MLSConversationModel
          .filter(MLSConversationModel.Columns.currentUserDID == userDid)
          .filter(MLSConversationModel.Columns.needsRejoin == true)
          .fetchAll(db)
      }

      if !corruptedConvos.isEmpty {
        logger.info(
          "🔄 Found \(corruptedConvos.count) locally corrupted conversation(s) needing rejoin")

        for convo in corruptedConvos {
          // CRITICAL: Check for shutdown/cancellation between each rejoin attempt
          if isShuttingDown || Task.isCancelled {
            logger.warning("⚠️ [REJOIN] Interrupted by shutdown - stopping corrupted convos loop")
            return
          }

          guard
            beginRejoinAttempt(
              conversationID: convo.conversationID,
              source: "corrupted-local"
            )
          else {
            continue
          }

          let _ = await attemptRejoinWithWelcomeFallback(
            convoId: convo.conversationID,
            displayName: convo.conversationID,
            reason: "corrupted local state"
          )
          endRejoinAttempt(conversationID: convo.conversationID)
        }
      }

      // Check for cancellation before making network call
      if isShuttingDown || Task.isCancelled {
        logger.warning("⚠️ [REJOIN] Aborting before getExpectedConversations - shutdown in progress")
        return
      }

      guard await ensureActiveAccount(for: userDid, operation: "detectAndRejoinMissingConversations")
      else {
        return
      }

      let deviceInfo = await mlsClient.getDeviceInfo(for: userDid)

      let response = try await apiClient.getExpectedConversations(deviceId: deviceInfo?.mlsDid)
      let expectedConvos = response.conversations

      logger.info("📋 Found \(expectedConvos.count) expected conversations")

      // All convos returned with filter "expected" are ones we should be in
      let missingConvos = expectedConvos

      guard !missingConvos.isEmpty else {
        logger.info("✅ No missing conversations detected")
        return
      }

      logger.info("🔄 Detected \(missingConvos.count) missing conversations - initiating rejoin")

      var successCount = 0
      var failureCount = 0
      var skippedCount = 0

      for convo in missingConvos {
        // CRITICAL FIX: Check for shutdown/cancellation between each rejoin
        // to prevent flooding locks during account switch
        if isShuttingDown || Task.isCancelled {
          logger.warning("⚠️ [REJOIN] Interrupted by shutdown - stopping rejoin loop")
          break
        }

        guard await ensureActiveAccount(for: userDid, operation: "detectAndRejoinMissingConversations")
        else {
          logger.info("⏸️ [REJOIN] Stopping missing-conversation loop for inactive account")
          break
        }

        guard let groupIdData = Data(hexEncoded: convo.groupId) else {
          logger.warning("⚠️ Invalid groupId format for \(convo.groupId) - skipping")
          failureCount += 1
          continue
        }

        let groupExists = await mlsClient.groupExists(for: userDid, groupId: groupIdData)

        if groupExists {
          do {
            let epoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
            logger.info(
              "✅ Group \(convo.groupId.prefix(8))... already exists locally (epoch: \(epoch)) - server tracking stale, skipping rejoin"
            )
            skippedCount += 1
            await clearConversationRejoinFlag(convo.groupId)
            continue
          } catch is CancellationError {
            logger.warning("⚠️ [REJOIN] Cancelled during epoch check - stopping")
            break
          } catch {
            logger.warning(
              "⚠️ Group \(convo.groupId.prefix(8))... exists but cannot get epoch: \(error.localizedDescription)"
            )
          }
        }

        guard
          beginRejoinAttempt(
            conversationID: convo.groupId,
            source: "missing-convo"
          )
        else {
          skippedCount += 1
          continue
        }

        let joined = await attemptRejoinWithWelcomeFallback(
          convoId: convo.groupId,
          displayName: convo.metadata?.name,
          reason: "server reported missing"
        )
        endRejoinAttempt(conversationID: convo.groupId)

        if joined {
          successCount += 1
        } else {
          failureCount += 1
        }

        // CRITICAL FIX: Add delay between rejoin attempts to let DB locks recover
        // This prevents overwhelming the lock system with 33 concurrent operations
        try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms
      }

      logger.info(
        "🎉 Rejoin detection complete: \(successCount) successful, \(failureCount) failed"
      )

      try await syncWithServer(fullSync: false)

    } catch {
      logger.error("❌ Failed to detect missing conversations: \(error.localizedDescription)")
      throw error
    }
  }

  private enum WelcomeRejoinResult {
    case joined
    case fallbackToExternalCommit(reason: String)
    case doNotFallback(reason: String)
  }

  @discardableResult
  internal func attemptRejoinWithWelcomeFallback(
    convoId: String, displayName: String?, reason: String
  ) async -> Bool {
    let label = displayName ?? convoId
    logger.info("📞 Requesting recovery for \(label) (\(reason))")

    guard let userDid = userDid else {
      logger.error("❌ Cannot rejoin \(label): missing user DID")
      return false
    }

    guard await ensureActiveAccount(for: userDid, operation: "attemptRejoinWithWelcomeFallback")
    else {
      return false
    }

    // ALWAYS try Welcome first, even for creators without local state.
    // External Commit should only be used when Welcome is truly unavailable (404/410).
    // This preserves the epoch and avoids unnecessary epoch advancement on transient errors.
    let welcomeResult = await attemptWelcomeRejoin(convoId: convoId, label: label)
    switch welcomeResult {
    case .joined:
      return true
    case .doNotFallback(let reason):
      logger.info("⏸️ [attemptRejoin] Skipping External Commit for \(label): \(reason)")
      return false
    case .fallbackToExternalCommit(let fallbackReason):
      // CRITICAL: Check for shutdown/cancellation BEFORE starting expensive External Commit
      // This prevents the main cause of 40+ second hangs during account switch
      if isShuttingDown || Task.isCancelled {
        logger.warning("⚠️ [attemptRejoin] Aborted before External Commit - shutdown in progress")
        return false
      }

      logger.info(
        "🔄 [attemptRejoin] \(fallbackReason) for \(label), attempting External Commit...")

      do {
        _ = try await attemptExternalCommitFallback(
          convoId: convoId,
          userDid: userDid,
          reason: "\(fallbackReason) (rejoin)"
        )
        logger.info("✅ Successfully rejoined \(label) via External Commit")
        await clearConversationRejoinFlag(convoId)
        return true
      } catch is CancellationError {
        logger.info("📭 [attemptRejoin] External Commit cancelled for \(label) (expected during shutdown)")
        return false
      } catch {
        logger.error("❌ Failed to rejoin \(label) via External Commit: \(error.localizedDescription)")
        return false
      }
    }
  }

  /// Attempt to join using a Welcome message if available.
  /// Returns explicit classification so callers only escalate to External Commit when appropriate.
  private func attemptWelcomeRejoin(convoId: String, label: String) async -> WelcomeRejoinResult {
    guard let convo = await fetchConversationForRejoin(convoId: convoId) else {
      logger.warning("⚠️ No conversation view available for \(label) when attempting Welcome join")
      return .doNotFallback(reason: "conversation unavailable")
    }

    do {
      // Use existing Welcome initialization logic with retry for 401 (auth transitions)
      try await initializeGroupFromWelcomeWithRetry(convo: convo)
      logger.info("✅ Successfully rejoined \(label) via Welcome message")
      await clearConversationRejoinFlag(convoId)
      return .joined
    } catch {
      return classifyWelcomeRejoinFailure(error, label: label)
    }
  }

  private func classifyWelcomeRejoinFailure(_ error: Error, label: String) -> WelcomeRejoinResult {
    if error is CancellationError {
      logger.info("📭 Welcome rejoin cancelled for \(label)")
      return .doNotFallback(reason: "welcome join cancelled")
    }

    if let apiError = error as? MLSAPIError {
      if case .invalidResponse(let message) = apiError,
        message == "No welcome message in response"
      {
        logger.info(
          "📭 No Welcome available for \(label) (missing welcome in 200 response) - will try External Commit"
        )
        return .fallbackToExternalCommit(reason: "Welcome unavailable (missing welcome in 200 response)")
      }

      if case .httpError(let code, _) = apiError {
        switch code {
        case 404:
          logger.info("📭 No Welcome available for \(label) (404) - will try External Commit")
          return .fallbackToExternalCommit(reason: "Welcome unavailable (HTTP 404)")
        case 410:
          logger.info("📭 Welcome expired for \(label) (410) - will try External Commit")
          return .fallbackToExternalCommit(reason: "Welcome expired (HTTP 410)")
        case 401, 408, 425, 429, 500...599:
          logger.info(
            "🔄 [attemptRejoin] Transient Welcome error for \(label): HTTP \(code) - skipping External Commit"
          )
          return .doNotFallback(reason: "transient Welcome error (HTTP \(code))")
        default:
          logger.warning(
            "⚠️ Welcome fetch failed for \(label): HTTP \(code) - not eligible for External Commit fallback"
          )
          return .doNotFallback(reason: "Welcome fetch failed (HTTP \(code))")
        }
      }
    }

    if let networkError = error as? NetworkError {
      switch networkError {
      case .responseError(let statusCode), .serverError(let statusCode, _):
        switch statusCode {
        case 404:
          logger.info("📭 No Welcome available for \(label) (NetworkError 404) - will try External Commit")
          return .fallbackToExternalCommit(reason: "Welcome unavailable (NetworkError 404)")
        case 410:
          logger.info("📭 Welcome expired for \(label) (NetworkError 410) - will try External Commit")
          return .fallbackToExternalCommit(reason: "Welcome expired (NetworkError 410)")
        case 401, 408, 425, 429, 500...599:
          logger.info(
            "🔄 [attemptRejoin] Transient network Welcome error for \(label): HTTP \(statusCode) - skipping External Commit"
          )
          return .doNotFallback(reason: "transient network error (HTTP \(statusCode))")
        default:
          logger.warning(
            "⚠️ Welcome fetch failed for \(label): NetworkError HTTP \(statusCode) - not eligible for External Commit fallback"
          )
          return .doNotFallback(reason: "network Welcome error (HTTP \(statusCode))")
        }
      default:
        logger.info(
          "🔄 [attemptRejoin] Network Welcome error for \(label): \(networkError.localizedDescription) - skipping External Commit"
        )
        return .doNotFallback(reason: "transient network Welcome failure")
      }
    }

    logger.warning(
      "⚠️ Welcome rejoin failed for \(label): \(error.localizedDescription) - not escalating to External Commit"
    )
    return .doNotFallback(reason: "Welcome processing failed")
  }

  /// Initialize group from Welcome with retry for transient auth errors (401)
  /// This handles the race condition during account switch where auth may not be fully established
  private func initializeGroupFromWelcomeWithRetry(
    convo: BlueCatbirdMlsChatDefs.ConvoView,
    maxAttempts: Int = 3,
    baseDelayMs: UInt64 = 500
  ) async throws {
    var lastError: Error?

    for attempt in 1...maxAttempts {
      do {
        try await initializeGroupFromWelcome(convo: convo)
        return  // Success
      } catch let error as MLSAPIError {
        if case .httpError(let code, _) = error {
          // 404/410 are terminal - don't retry (Welcome doesn't exist or is expired)
          if code == 404 || code == 410 {
            throw error
          }
          // 401 during auth transition - retry with exponential backoff
          if code == 401 && attempt < maxAttempts {
            let delay = baseDelayMs * UInt64(1 << (attempt - 1))
            logger.info(
              "🔄 Welcome fetch got 401, retrying in \(delay)ms (attempt \(attempt)/\(maxAttempts))"
            )
            try? await Task.sleep(nanoseconds: delay * 1_000_000)
            continue
          }
        }
        lastError = error
      } catch {
        lastError = error
      }
    }

    throw lastError ?? MLSConversationError.welcomeFetchFailed
  }

  internal func fetchConversationForRejoin(convoId: String) async -> BlueCatbirdMlsChatDefs.ConvoView? {
    // 1. Try in-memory first (fastest)
    if let convo = conversations[convoId] {
      return convo
    }

    // 2. Try local database
    do {
      if let localConvo = try await database.read({ db in
        try MLSConversationModel
          .filter(MLSConversationModel.Columns.conversationID == convoId)
          .fetchOne(db)?
          .asConvoView()
      }) {
        return localConvo
      }
    } catch {
      logger.warning("⚠️ Database lookup failed for \(convoId): \(error.localizedDescription)")
    }

    // 3. CRITICAL: Fallback to server fetch for newly added conversations
    // This bridges the gap between 'ExpectedConversation' (lite) and 'ConvoView' (full)
    // Without this, Welcome processing fails for new joins and falls back to External Commit
    do {
      logger.info(
        "📡 [fetchConversationForRejoin] Fetching from server for \(convoId.prefix(16))...")
      let conversations = try await apiClient.getConversations(limit: 100)

      if let match = conversations.convos.first(where: { $0.groupId == convoId }) {
        // Cache it so we don't fetch again immediately
        self.conversations[convoId] = match
        logger.info("✅ [fetchConversationForRejoin] Found conversation on server, cached locally")
        return match
      } else {
        logger.warning(
          "⚠️ [fetchConversationForRejoin] Conversation \(convoId.prefix(16))... not found in server list"
        )
      }
    } catch {
      logger.error(
        "❌ [fetchConversationForRejoin] Server fetch failed: \(error.localizedDescription)")
    }

    return nil
  }
}
