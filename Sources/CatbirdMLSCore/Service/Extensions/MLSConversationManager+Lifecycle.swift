import Foundation
import GRDB
import OSLog
import Petrel
import PetrelCatbird
import Synchronization

public enum MLSResumeResult: Sendable, Equatable {
  case resumed
  case failedStillSuspended
}

/// Opaque, one-shot authority for a foreground account-switch transaction to release the exact
/// suspension owned by this manager after shutdown proves every MLS boundary is closed.
public struct MLSAccountSwitchSuspensionAuthorization: @unchecked Sendable {
  fileprivate let managerIdentity: ObjectIdentifier
  fileprivate let normalizedDID: String
  fileprivate let capability: MLSClient.SuspensionAbandonmentCapability
}

private let suspendedResumeStateRefreshOverride = Mutex<
  (@Sendable (String, MLSClient.SuspendedResumeCapability) async throws -> Void)?
>(nil)
private let suspendedResumeFinalReleaseOverride = Mutex<(@Sendable () async -> Void)?>(nil)
private let suspendedResumeOtherBindingInvalidationOverride = Mutex<
  (@Sendable (String) async throws -> Void)?
>(nil)
private let shutdownAfterSuspensionCapabilityTestOverride = Mutex<(@Sendable () -> Void)?>(nil)

extension MLSConversationManager {

  internal nonisolated static func setShutdownAfterSuspensionCapabilityTestOverride(
    _ operation: (@Sendable () -> Void)?
  ) {
    shutdownAfterSuspensionCapabilityTestOverride.withLock { $0 = operation }
  }

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

  internal nonisolated static func resetSuspensionStateForTesting() {
    _isSuspending.withLock { $0 = false }
  }

  /// Synchronous scene-phase boundary. The currently active manager owns the exact suspension
  /// transition before the lifecycle coordinator performs any await or starts background cleanup.
  @MainActor
  public func markSuspensionInProgressForLifecycleTransition(
    reason: String = "scene lifecycle transition"
  ) {
    isSuspending = true
    isSyncPaused = true
    guard let userDid else {
      MLSClient.markSuspensionInProgress(
        reason: reason,
        noUserOwnerToken: suspensionAbandonmentOwnerToken
      )
      return
    }
    MLSClient.markSuspensionInProgress(
      reason: reason,
      abandonmentOwnerDID: userDid,
      abandonmentOwnerToken: suspensionAbandonmentOwnerToken
    )
  }

  @MainActor
  public func authorizeSuspensionAbandonmentForAccountSwitch()
    -> MLSAccountSwitchSuspensionAuthorization?
  {
    guard !isShuttingDown, let userDid,
      let capability = MLSClient.ownedSuspensionAbandonmentCapability(
        for: userDid,
        ownerToken: suspensionAbandonmentOwnerToken
      )
    else {
      return nil
    }
    return MLSAccountSwitchSuspensionAuthorization(
      managerIdentity: ObjectIdentifier(self),
      normalizedDID: userDid.trimmingCharacters(in: .whitespacesAndNewlines),
      capability: capability
    )
  }

  /// Suspend all MLS operations when app enters background
  /// This prevents 0xdead10cc crashes by ensuring no database operations
  /// continue running when iOS suspends the app
  ///
  /// Call this BEFORE GRDBSuspensionCoordinator.setLifecycleSuspended() to ensure
  /// MLS tasks are cancelled before GRDB starts rejecting operations
  ///
  /// - Returns: `true` when a `.rustFull` runtime is available for the post-drain
  ///   preparation step. A true result does not mean the runtime is prepared yet.
  @MainActor
  @discardableResult
  public func suspendMLSOperations() -> Bool {
    logger.info("⏸️ [SUSPEND] Suspending MLS operations for app background")
    MLSSuspensionFlightRecorder.shared.record(
      .suspensionPrepare,
      details: "MLSConversationManager.suspendMLSOperations",
      process: "app"
    )

    // Set flag to reject new operations
    markSuspensionInProgressForLifecycleTransition(
      reason: "MLSConversationManager.suspendMLSOperations"
    )
    deviceAuthBindingRequiresResumeRebind =
      userDid.map {
        MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: $0)
      } ?? false
    rustSuspensionPreparationGeneration &+= 1
    rustPreparedSuspensionGeneration = nil
    let rustPreparationAvailable = protocolAuthorityMode == .rustFull && orchestratorRuntime != nil
    if protocolAuthorityMode == .rustFull, !rustPreparationAvailable {
      logger.warning("⚠️ [MLS-FULL-RUST] Missing runtime during suspend; legacy fallback required")
    } else if protocolAuthorityMode != .rustFull && protocolAuthorityMode != .swiftLegacy {
      rustShadowSuspensionTeardownTask?.cancel()
      let teardownGeneration = rustSuspensionPreparationGeneration
      rustShadowSuspensionTeardownTask = Task { [weak self] in
        guard await MLSClient.drainInFlightFFIOperations(timeout: 5), !Task.isCancelled,
          let self, self.isSuspending,
          self.rustSuspensionPreparationGeneration == teardownGeneration
        else {
          return
        }
        do {
          try MLSClient.withSuspensionPreparationLease {
            guard !Task.isCancelled, self.isSuspending,
              self.rustSuspensionPreparationGeneration == teardownGeneration
            else {
              throw CancellationError()
            }
            // This shutdown is a teardown operation under the exclusive post-drain lease. The
            // lease blocks ordinary, resume, and initialization admission until reset completes.
            self.resetOrchestratorRuntime(reason: "rustShadow suspension after tracked drain")
          }
        } catch {
          self.logger.error(
            "❌ [MLS-SHADOW] Could not acquire post-drain runtime teardown lease"
          )
        }
      }
    }
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

    if let task = deferredEpochRecoveryTask {
      task.cancel()
      logger.debug("   Cancelled deferredEpochRecoveryTask")
    }
    deferredEpochRecoveryTask = nil

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
    return rustPreparationAvailable
  }

  /// Waits for every admitted synchronous bridge call, then prepares the Rust runtime.
  /// Production callers must await this before normal context/runtime close. On timeout,
  /// the method interrupts stragglers and leaves every suspension gate closed.
  @MainActor
  @discardableResult
  public func prepareRustRuntimeForSuspensionAfterDrain(
    timeout: TimeInterval = 5.0
  ) async -> Bool {
    guard protocolAuthorityMode == .rustFull, isSuspending else { return false }
    let generation = rustSuspensionPreparationGeneration
    if rustPreparedSuspensionGeneration == generation { return true }

    let drained = await MLSClient.drainInFlightFFIOperations(timeout: timeout)
    guard drained else {
      MLSClient.interruptAllContexts()
      MLSCoreContext.interruptAllContexts()
      logger.error("❌ [MLS-FULL-RUST] Timed out draining Rust work before suspend prepare")
      return false
    }
    guard isSuspending,
      generation == rustSuspensionPreparationGeneration,
      let runtime = orchestratorRuntime
    else {
      return false
    }

    do {
      try MLSClient.withSuspensionPreparationLease {
        _ = try runtime.prepareForSuspend(
          reason: "MLSConversationManager.suspendMLSOperations"
        )
      }
      rustRuntimeRequiresForegroundRestore = false
      rustStartupReconcileCompleted = false
      rustPreparedSuspensionGeneration = generation
      return true
    } catch {
      MLSClient.interruptAllContexts()
      MLSCoreContext.interruptAllContexts()
      logger.error(
        "❌ [MLS-FULL-RUST] prepareForSuspend failed: \(error.localizedDescription, privacy: .public)"
      )
      return false
    }
  }

  internal func waitForRustShadowSuspensionTeardownForTesting() async {
    await rustShadowSuspensionTeardownTask?.value
  }

  /// Marks the rustFull runtime as unusable after an app-level force close
  /// destroyed the underlying Rust MLS context during suspension.
  @MainActor
  public func markRustRuntimeClosedForSuspend(reason: String) {
    guard protocolAuthorityMode == .rustFull else { return }
    rustRuntimeRequiresForegroundRestore = true
    rustStartupReconcileCompleted = false
    invalidateOrchestratorRuntime(reason: reason)
  }

  /// Resume MLS operations when app returns to foreground
  /// This restarts background tasks that were cancelled during suspension
  @MainActor
  @discardableResult
  public func resumeMLSOperations() async -> MLSResumeResult {
    await resumeMLSOperations(trackedFFIDrainTimeout: 5.0)
  }

  @MainActor
  @discardableResult
  internal func resumeMLSOperations(
    trackedFFIDrainTimeout: TimeInterval
  ) async -> MLSResumeResult {
    guard !isShuttingDown else {
      logger.debug("🔄 [RESUME] Shutdown owns the suspension; resume remains denied")
      return .failedStillSuspended
    }
    guard isSuspending else {
      logger.debug("🔄 [RESUME] MLS not suspended - nothing to resume")
      return .resumed
    }
    rustShadowSuspensionTeardownTask?.cancel()
    rustShadowSuspensionTeardownTask = nil

    func restoreClosedResumeGates(_ reason: String) {
      if MLSClient.isSuspensionInProgress {
        // Preserve another exact resume capability; only repair a split Core gate.
        MLSClient.restoreCoreSuspensionGateIfClientSuspended()
      } else {
        MLSClient.markSuspensionInProgress(reason: reason)
      }
    }

    let resumeGateWasSplit =
      MLSClient.isSuspensionInProgress != MLSCoreContext.isSuspensionInProgress
    restoreClosedResumeGates("resume pre-drain admission closure")
    let resumeSuspensionPreparationGeneration = rustSuspensionPreparationGeneration
    guard let resumeDrainSnapshot = MLSClient.captureSuspendedResumeDrainSnapshot() else {
      restoreClosedResumeGates("resume could not capture exact pre-drain suspension")
      logger.error("❌ [RESUME] Failed to capture exact pre-drain suspension authority")
      return .failedStillSuspended
    }
    let drained = await MLSClient.drainInFlightFFIOperations(
      timeout: trackedFFIDrainTimeout
    )
    guard drained, !Task.isCancelled else {
      MLSClient.interruptAllContexts()
      MLSCoreContext.interruptAllContexts()
      restoreClosedResumeGates("resume tracked FFI drain failed")
      logger.error("❌ [RESUME] Timed out or cancelled while draining tracked FFI work")
      return .failedStillSuspended
    }
    guard !resumeGateWasSplit,
      resumeSuspensionPreparationGeneration == rustSuspensionPreparationGeneration
    else {
      restoreClosedResumeGates("resume pre-drain authority was superseded or split")
      logger.error("❌ [RESUME] Pre-drain suspension authority changed while waiting")
      return .failedStillSuspended
    }

    guard let userDid else {
      guard !isShuttingDown,
        let capability = MLSClient.ownedNoUserSuspendedResumeCapability(
          ownerToken: suspensionAbandonmentOwnerToken,
          matching: resumeDrainSnapshot
        ),
        await MLSClient.finishNoUserSuspendedResumeCapability(capability)
      else {
        if !isShuttingDown {
          restoreClosedResumeGates("resume failed without exact no-user suspension authority")
        }
        logger.error("❌ [MLS] Failed to release exact no-user suspension authority")
        return .failedStillSuspended
      }
      deviceAuthBindingRequiresResumeRebind = false
      isSuspending = false
      isSyncPaused = false
      logger.info("✅ [RESUME] Released exact context-free no-user suspension")
      return .resumed
    }
    var suspendedResumeCapability = MLSClient.beginSuspendedResumeCapability(
      for: userDid,
      ownerToken: suspensionAbandonmentOwnerToken,
      matching: resumeDrainSnapshot
    )
    if suspendedResumeCapability == nil,
      !deviceAuthBindingRequiresResumeRebind,
      !MLSClient.isSuspensionInProgress,
      !MLSCoreContext.isSuspensionInProgress
    {
      suspendedResumeCapability =
        MLSClient.beginSuspendedResumeCapabilityAfterLegacyGateClear(for: userDid)
    }
    guard let suspendedResumeCapability else {
      restoreClosedResumeGates("resume capability acquisition failed")
      deviceAuthBindingRequiresResumeRebind =
        deviceAuthBindingRequiresResumeRebind
        || MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: userDid)
      logger.error("❌ [MLS] Failed to acquire the suspended resume capability")
      return .failedStillSuspended
    }
    guard
      let snapshotRequiresDeviceAuthRebind =
        MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(
          for: userDid,
          capability: suspendedResumeCapability
        )
    else {
      MLSClient.cancelSuspendedResumeCapability(suspendedResumeCapability)
      restoreClosedResumeGates("resume capability was revoked before snapshot validation")
      deviceAuthBindingRequiresResumeRebind =
        deviceAuthBindingRequiresResumeRebind
        || MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: userDid)
      logger.error("❌ [MLS] Resume capability was revoked before snapshot validation")
      return .failedStillSuspended
    }
    let resumeRequiresDeviceAuthRebind =
      deviceAuthBindingRequiresResumeRebind || snapshotRequiresDeviceAuthRebind
    let resumeAttemptID = UUID()

    func failStillSuspended(_ message: String) async -> MLSResumeResult {
      MLSClient.cancelSuspendedResumeCapability(suspendedResumeCapability)
      MLSClient.markSuspensionInProgress(reason: "resume failure: \(message)")
      if suspendedResumeRuntimeAttemptID == resumeAttemptID,
        let ownedContextIdentity = suspendedResumeRuntimeContextIdentity
      {
        if orchestratorRuntime?.mlsContextIdentity == ownedContextIdentity {
          resetOrchestratorRuntime(reason: "suspended resume failed")
        }
        _ = await MLSCoreContext.shared.removeContext(
          for: userDid,
          matching: ownedContextIdentity
        )
        if suspendedResumeRuntimeAttemptID == resumeAttemptID {
          suspendedResumeRuntimeAttemptID = nil
          suspendedResumeRuntimeContextIdentity = nil
        }
      }
      logger.error("❌ [RESUME] \(message, privacy: .public)")
      return .failedStillSuspended
    }

    logger.info("▶️ [RESUME] Resuming MLS operations after app foreground")
    MLSSuspensionFlightRecorder.shared.record(
      .resumeFromSuspension,
      details: "MLSConversationManager.resumeMLSOperations",
      process: "app"
    )

    var rebuildCoreOwnedRustRuntimeAfterReload = false
    if protocolAuthorityMode == .rustFull {
      let resumeReason = "MLSConversationManager.resumeMLSOperations"
      if rustRuntimeRequiresForegroundRestore || orchestratorRuntime == nil {
        if orchestratorRuntimeResumeFactory == nil {
          rebuildCoreOwnedRustRuntimeAfterReload = true
        } else {
          logger.info(
            "🔄 [MLS-FULL-RUST] Restoring test runtime before foreground resume"
          )
          guard
            await restoreOrchestratorRuntimeAfterSuspendClose(
              reason: resumeReason,
              suspendedResumeCapability: suspendedResumeCapability
            ) != nil
          else {
            logger.error(
              "❌ [MLS-FULL-RUST] Failed to rebuild runtime after suspension close; keeping MLS work suspended"
            )
            return await failStillSuspended("runtime restore failed")
          }
          rustRuntimeRequiresForegroundRestore = false
        }
      } else if let runtime = orchestratorRuntime {
        let runtimeOwnsCoreContext = runtime.mlsContextIdentity != nil
        if runtimeOwnsCoreContext {
          // Rust resume only re-opens the existing engine lifecycle. It does not refresh
          // MLSContext from disk, so a Core-owning runtime must remain quiesced until it
          // is discarded and rebuilt on the authoritative suspended-resume reload below.
          rebuildCoreOwnedRustRuntimeAfterReload = runtimeOwnsCoreContext
        } else {
          do {
            try await withTrackedRustRuntime(
              runtime,
              operation: "rustFullResumeFromSuspend",
              suspendedResumeCapability: suspendedResumeCapability
            ) { runtime in
              try runtime.resumeFromSuspend(reason: resumeReason)
            }
          } catch {
            logger.error(
              "❌ [MLS-FULL-RUST] resumeFromSuspend failed: \(error.localizedDescription, privacy: .public)"
            )
            rustRuntimeRequiresForegroundRestore = true
            resetOrchestratorRuntime(reason: "resumeFromSuspend failed")
            guard
              await restoreOrchestratorRuntimeAfterSuspendClose(
                reason: resumeReason,
                suspendedResumeCapability: suspendedResumeCapability
              ) != nil
            else {
              logger.error(
                "❌ [MLS-FULL-RUST] Failed to rebuild runtime after resume failure; keeping MLS work suspended"
              )
              return await failStillSuspended("runtime rebuild after resume failure failed")
            }
            rustRuntimeRequiresForegroundRestore = false
          }
        }
      }
    }

    guard
      MLSClient.isCurrentSuspendedResumeCapability(
        suspendedResumeCapability,
        for: userDid
      )
    else {
      return await failStillSuspended("resume capability was revoked during runtime restore")
    }
    do {
      try Task.checkCancellation()
    } catch {
      return await failStillSuspended("resume task was cancelled during runtime restore")
    }

    do {
      try await refreshAuthoritativeStateForSuspendedResume(
        userDid: userDid,
        capability: suspendedResumeCapability,
        rebuildCoreOwnedRustRuntime: rebuildCoreOwnedRustRuntimeAfterReload,
        resumeAttemptID: resumeAttemptID
      )
    } catch {
      return await failStillSuspended(
        "authoritative state refresh failed: \(error.localizedDescription)"
      )
    }

    // Keep every ordinary MLS admission gate closed until device authentication has been
    // re-established against the exact resumed runtime and transport boundary.
    if resumeRequiresDeviceAuthRebind {
      do {
        if let deviceAuthResumeRebindOverride {
          try await deviceAuthResumeRebindOverride()
          guard MLSClient.recordCompletedDeviceAuthRebindForTesting(
            for: userDid,
            capability: suspendedResumeCapability
          ) else {
            throw CancellationError()
          }
        } else if protocolAuthorityMode == .rustFull {
          _ = try await bindRustDeviceAuthentication(
            userDid: userDid,
            force: true,
            suspendedResumePermit: suspendedResumeCapability
          )
        } else {
          _ = try await MLSClient.shared.bindLegacyDeviceAuthentication(
            for: userDid,
            suspendedResumePermit: suspendedResumeCapability
          )
        }
      } catch {
        return await failStillSuspended(
          "device authentication rebind failed: \(error.localizedDescription)"
        )
      }
    }

    do {
      try await MLSClient.shared.invalidateOtherDeviceAuthBindingsForSuspendedResume(
        activeUserDID: userDid,
        capability: suspendedResumeCapability,
        afterInvalidation: suspendedResumeOtherBindingInvalidationOverride.withLock { $0 }
      )
    } catch {
      return await failStillSuspended(
        "inactive account device authentication invalidation failed: \(error.localizedDescription)"
      )
    }

    do {
      try Task.checkCancellation()
    } catch {
      return await failStillSuspended("resume task was cancelled before gate release")
    }

    // Stage the manager and Core gates while MLSClient still rejects every ordinary operation.
    // Finishing the opaque capability is the final release/linearization point; no manager state
    // is written after it, so a newer suspension cannot be overwritten by this resume attempt.
    deviceAuthBindingRequiresResumeRebind = false
    isSuspending = false
    isSyncPaused = false
    if let finalReleaseOverride = suspendedResumeFinalReleaseOverride.withLock({ $0 }) {
      await finalReleaseOverride()
    }
    guard await MLSClient.shared.finishSuspendedResumeCapability(suspendedResumeCapability) else {
      deviceAuthBindingRequiresResumeRebind =
        resumeRequiresDeviceAuthRebind
        || MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: userDid)
      isSuspending = true
      isSyncPaused = true
      return await failStillSuspended("resume capability was revoked before final gate release")
    }
    guard !MLSClient.isSuspensionInProgress, !MLSCoreContext.isSuspensionInProgress else {
      deviceAuthBindingRequiresResumeRebind =
        resumeRequiresDeviceAuthRebind
        || MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: userDid)
      isSuspending = true
      isSyncPaused = true
      return await failStillSuspended("resume final release postcondition failed")
    }
    if suspendedResumeRuntimeAttemptID == resumeAttemptID {
      suspendedResumeRuntimeAttemptID = nil
      suspendedResumeRuntimeContextIdentity = nil
    }
    schedulePostReloadSyncIfNeeded()

    // Restart background tasks (only if we're initialized)
    guard isInitialized else {
      logger.debug("   Skipping task restart - not initialized")
      return .resumed
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

    if userDid != nil, !configuration.skipDeviceRecordPublishing {
      Task { [weak self] in
        guard let self else { return }
        do {
          try await self.ensureDeviceRecordPublished()
        } catch {
          self.logger.error(
            "Failed to publish device record on resume: \(error.localizedDescription)")
        }
      }
    }

    // Note: missingConversationsTask is typically only run during initialization
    // If needed, it will be triggered by syncWithServer or explicit rejoin requests

    logger.info("✅ [RESUME] MLS operations resumed")
    return .resumed
  }

  @MainActor
  private func refreshAuthoritativeStateForSuspendedResume(
    userDid: String,
    capability: MLSClient.SuspendedResumeCapability,
    rebuildCoreOwnedRustRuntime: Bool,
    resumeAttemptID: UUID
  ) async throws {
    guard MLSClient.isCurrentSuspendedResumeCapability(capability, for: userDid) else {
      throw CancellationError()
    }
    try Task.checkCancellation()
    if let operation = suspendedResumeStateRefreshOverride.withLock({ $0 }) {
      try await operation(userDid, capability)
      try Task.checkCancellation()
      guard MLSClient.isCurrentSuspendedResumeCapability(capability, for: userDid) else {
        throw CancellationError()
      }
      return
    }

    isStateReloadInProgress = true
    defer {
      isStateReloadInProgress = false
      let waiters = stateReloadWaiters
      stateReloadWaiters.removeAll()
      waiters.forEach { $0.resume() }
    }

    groupStates.removeAll()
    conversationStates.removeAll()
    pendingMessagesLock.withLock { pendingMessages.removeAll() }
    ownCommitsLock.withLock { ownCommits.removeAll() }
    recentlySentMessages.removeAll()
    await CatbirdMLSCore.MLSEpochCheckpoint.shared.reloadCacheFromDisk()

    try Task.checkCancellation()
    guard MLSClient.isCurrentSuspendedResumeCapability(capability, for: userDid) else {
      throw CancellationError()
    }
    if rebuildCoreOwnedRustRuntime {
      resetOrchestratorRuntime(reason: "authoritative suspended-resume Core reload")
    }
    let reloadedContextIdentity = try await MLSCoreContext.shared.reloadContextForSuspendedResume(
      for: userDid,
      capability: capability
    )
    if rebuildCoreOwnedRustRuntime {
      guard let reloadedContextIdentity else {
        throw MLSConversationError.operationFailed(
          "Authoritative suspended-resume Core reload did not install a context"
        )
      }
      suspendedResumeRuntimeAttemptID = resumeAttemptID
      suspendedResumeRuntimeContextIdentity = reloadedContextIdentity
    }
    try Task.checkCancellation()
    guard MLSClient.isCurrentSuspendedResumeCapability(capability, for: userDid) else {
      throw CancellationError()
    }

    if rebuildCoreOwnedRustRuntime {
      guard
        let rebuiltRuntime = await restoreOrchestratorRuntimeAfterSuspendClose(
          reason: "MLSConversationManager.resumeMLSOperations.authoritativeCoreReload",
          suspendedResumeCapability: capability
        )
      else {
        throw MLSConversationError.operationFailed(
          "Failed to rebuild rustFull runtime on the authoritative reloaded Core context"
        )
      }
      guard rebuiltRuntime.mlsContextIdentity == reloadedContextIdentity else {
        rebuiltRuntime.shutdown()
        if orchestratorRuntime === rebuiltRuntime {
          orchestratorRuntime = nil
          rustStartupReconcileCompleted = false
        }
        throw MLSConversationError.operationFailed(
          "Rebuilt rustFull runtime was not bound to the authoritative reloaded Core context"
        )
      }
      rustRuntimeRequiresForegroundRestore = false
    }
    try Task.checkCancellation()
    guard MLSClient.isCurrentSuspendedResumeCapability(capability, for: userDid) else {
      throw CancellationError()
    }

    if protocolAuthorityMode == .rustFull {
      _ = try await withRustAuthoritativeRuntime(
        operation: "resumeAuthoritativeStateRefresh",
        suspendedResumeCapability: capability
      ) { runtime in
        try runtime.startupReconcile()
      }
      rustStartupReconcileCompleted = true
    }
    try Task.checkCancellation()
    lastForegroundTime = Date()
    postReloadSyncPending = true
  }

  internal static func setSuspendedResumeStateRefreshOverride(
    _ operation: (@Sendable (String, MLSClient.SuspendedResumeCapability) async throws -> Void)?
  ) {
    suspendedResumeStateRefreshOverride.withLock { $0 = operation }
  }

  internal static func setSuspendedResumeFinalReleaseOverride(
    _ operation: (@Sendable () async -> Void)?
  ) {
    suspendedResumeFinalReleaseOverride.withLock { $0 = operation }
  }

  internal static func setSuspendedResumeOtherBindingInvalidationOverride(
    _ operation: (@Sendable (String) async throws -> Void)?
  ) {
    suspendedResumeOtherBindingInvalidationOverride.withLock { $0 = operation }
  }

  @MainActor
  private func schedulePostReloadSyncIfNeeded() {
    guard postReloadSyncPending else { return }
    guard !isSuspending, !isSyncPaused, !MLSClient.isSuspensionInProgress else {
      logger.info("⏸️ [MLS Reload] Deferring post-reload sync while MLS remains suspended")
      return
    }

    postReloadSyncPending = false
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

  internal func throwIfShuttingDown(_ operation: String) throws {
    if isShuttingDown {
      logger.warning("⏸️ [MLSConversationManager] \(operation) aborted - storage reset in progress")
      throw MLSConversationError.operationFailed("MLS storage reset in progress")
    }

    // WS-6.2 suspension handshake: also abort when the app is transitioning
    // to background. Previously this only checked shutdown, so a pipeline
    // step that re-validated mid-send (e.g. sendMessage's preCache check)
    // would proceed into GRDB writes while the emergency close / GRDB
    // suspension was racing it (SQLite error 21). Consult the FFI-level flag
    // (`MLSClient.isSuspensionInProgress`) rather than the manager's
    // `isSuspending`: BGTask runners (`MLSBackgroundRefreshManager`) clear
    // the FFI flag before doing background MLS work, so this stays
    // BGTask-safe while still failing fast during a real suspension.
    if MLSClient.isSuspensionInProgress {
      logger.warning(
        "⏸️ [MLSConversationManager] \(operation) aborted - app suspension in progress")
      throw MLSConversationError.operationFailed(
        "MLS operations suspended (app transitioning to background)")
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

    if let resetUserDid {
      await MLSClient.shared.invalidateDeviceAuthBinding(for: resetUserDid)
    }

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

    deferredEpochRecoveryTask?.cancel()
    deferredEpochRecoveryTask = nil

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
  public func shutdown(
    accountSwitchSuspensionAuthorization authorization:
      MLSAccountSwitchSuspensionAuthorization? = nil
  ) async -> Bool {
    await shutdown(
      accountSwitchSuspensionAuthorization: authorization,
      trackedFFIDrainTimeout: 5.0
    )
  }

  @MainActor
  @discardableResult
  internal func shutdown(
    accountSwitchSuspensionAuthorization authorization:
      MLSAccountSwitchSuspensionAuthorization?,
    trackedFFIDrainTimeout: TimeInterval = 5.0
  ) async -> Bool {
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
    let normalizedShutdownDID = shutdownUserDid?.trimmingCharacters(in: .whitespacesAndNewlines)
    let explicitAuthorizationMatches =
      authorization?.managerIdentity == ObjectIdentifier(self)
      && authorization?.normalizedDID == normalizedShutdownDID
      && authorization.map {
        MLSClient.isCurrentSuspensionAbandonmentCapability(
          $0.capability,
          for: $0.normalizedDID,
          ownerToken: suspensionAbandonmentOwnerToken
        )
      } == true
    let suspensionAlreadyInProgress = MLSClient.isSuspensionInProgress
    isShuttingDown = true
    isSyncPaused = true
    isSuspending = true
    MLSClient.markShutdownInProgress(
      reason: "MLSConversationManager.shutdown",
      abandonmentOwnerDID: shutdownUserDid,
      abandonmentOwnerToken: suspensionAbandonmentOwnerToken
    )
    // markShutdown records and completes a fresh Core-close handoff. Refresh an otherwise valid
    // pre-shutdown authorization onto that exact close serial; the stale wrapped capability must
    // never be carried across the new handoff.
    let refreshedAuthorizedSuspensionCapability: MLSClient.SuspensionAbandonmentCapability? =
      if explicitAuthorizationMatches,
        let normalizedShutdownDID,
        !normalizedShutdownDID.isEmpty
      {
        MLSClient.ownedSuspensionAbandonmentCapability(
          for: normalizedShutdownDID,
          ownerToken: suspensionAbandonmentOwnerToken
        )
      } else {
        nil
      }
    // A foreground account switch can enter shutdown directly, without first asking the
    // lifecycle coordinator for an authorization. In that case this manager owns the exact
    // suspension tuple that markShutdownInProgress just created and may carry that authority
    // through safe teardown. Never acquire authority implicitly for a pre-existing transition:
    // background suspension still requires its explicit authorization, and a racing/superseding
    // owner will fail this exact owner-token lookup.
    let implicitShutdownSuspensionCapability: MLSClient.SuspensionAbandonmentCapability? =
      if !suspensionAlreadyInProgress,
        let normalizedShutdownDID,
        !normalizedShutdownDID.isEmpty
      {
        MLSClient.ownedSuspensionAbandonmentCapability(
          for: normalizedShutdownDID,
          ownerToken: suspensionAbandonmentOwnerToken
        )
      } else {
        nil
      }
    let shutdownSuspensionCapability =
      refreshedAuthorizedSuspensionCapability ?? implicitShutdownSuspensionCapability
    let noUserShutdownSuspensionCapability: MLSClient.NoUserSuspendedResumeCapability? =
      if normalizedShutdownDID == nil || normalizedShutdownDID?.isEmpty == true {
        MLSClient.ownedNoUserShutdownSuspensionCapability(
          ownerToken: suspensionAbandonmentOwnerToken
        )
      } else {
        nil
      }
    shutdownAfterSuspensionCapabilityTestOverride.withLock { $0 }?()
    userDid = nil  // Immediately invalidate to fail-fast any new operations

    if let shutdownUserDid {
      await MLSClient.shared.invalidateDeviceAuthBinding(for: shutdownUserDid)
    }

    rustStartupReconcileCompleted = false
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
    if let task = deferredEpochRecoveryTask { localTasks.append(task) }
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

    deferredEpochRecoveryTask?.cancel()
    deferredEpochRecoveryTask = nil

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

    let drained = await MLSClient.drainInFlightFFIOperations(
      timeout: trackedFFIDrainTimeout
    )
    guard drained else {
      MLSClient.interruptAllContexts()
      MLSCoreContext.interruptAllContexts()
      logger.critical(
        "🚨 [MLSConversationManager.shutdown] Timed out draining tracked Rust/FFI work"
      )
      return false
    }
    guard
      let shutdownLease = MLSClient.beginShutdownQuiescenceLease(
        abandonmentCapability: shutdownSuspensionCapability,
        noUserCapability: noUserShutdownSuspensionCapability,
        excludingUserDID: shutdownUserDid
      )
    else {
      MLSClient.interruptAllContexts()
      MLSCoreContext.interruptAllContexts()
      logger.critical(
        "🚨 [MLSConversationManager.shutdown] Could not acquire post-drain shutdown lease"
      )
      return false
    }
    defer { MLSClient.cancelShutdownQuiescenceLease(shutdownLease) }
    if let runtime = orchestratorRuntime {
      do {
        _ = try runtime.prepareForSuspend(reason: "MLSConversationManager.shutdown")
      } catch {
        MLSClient.interruptAllContexts()
        MLSCoreContext.interruptAllContexts()
        logger.critical(
          "🚨 [MLSConversationManager.shutdown] Rust quiescence failed: \(error.localizedDescription, privacy: .public)"
        )
        return false
      }
      resetOrchestratorRuntime(reason: "manager shutdown after tracked drain")
    }

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
      let releasedSuspension: Bool
      if shutdownSuspensionCapability != nil {
        releasedSuspension = await MLSClient.abandonSuspensionAfterSafeShutdown(shutdownLease)
      } else if noUserShutdownSuspensionCapability != nil {
        releasedSuspension = await MLSClient.abandonNoUserSuspensionAfterSafeShutdown(
          shutdownLease
        )
      } else {
        releasedSuspension = false
      }
      if releasedSuspension {
        isSuspending = false
        isSyncPaused = false
        deviceAuthBindingRequiresResumeRebind = false
      } else {
        shutdownWasSafe = false
        logger.critical(
          "🚨 [MLSConversationManager.shutdown] Safe teardown could not consume exact suspension abandonment authority"
        )
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

    // Step 8: Optionally trigger a sync, but never while lifecycle suspension is still active.
    postReloadSyncPending = true
    schedulePostReloadSyncIfNeeded()
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

      if let recoveryManager = await MLSClient.shared.recovery(for: userDid) {
        let recoveryStore = MLSRecoveryStateStore(database: database, currentUserDID: userDid)
        await recoveryManager.setPersistence(recoveryStore)
        await recoveryManager.hydrateFromDatabase()

        await recoveryManager.setDeferredRejoinHandler {
          [weak self] handlerUserDid, conversationIds in
          guard let self, self.userDid == handlerUserDid else { return }
          await self.persistDeferredRejoinRequests(
            conversationIds,
            reason: "silent recovery after device re-registration"
          )
        }
      }

      if protocolAuthorityMode == .rustFull {
        await prepareRustFullStartupDeviceAndKeyPackages(
          userDid: userDid,
          operation: "earlyStartupReconcile"
        )
      } else {
        await prepareSwiftLegacyStartupDeviceAndKeyPackages(userDid: userDid)
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

      if protocolAuthorityMode == .rustFull {
        logger.info(
          "⏭️ [MLS-FULL-RUST] Skipping Swift device sync manager; Rust owns device membership changes"
        )
      } else if let deviceSyncManager = deviceSyncManager {
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

    if protocolAuthorityMode == .rustFull {
      logger.info(
        "⏭️ [MLS-FULL-RUST] Skipping Swift initial key package refresh; Rust owns replenishment")
    } else {
      keyPackageRefreshTask = Task(priority: .utility) { [weak self] in
        guard let self else { return }
        do {
          // Wait for app to settle before heavy FFI work.
          // If user backgrounds immediately after launch, suspendMLSOperations() cancels
          // this task and the sleep throws CancellationError — preventing the 0xdead10cc
          // crash seen when create_key_package races against suspension (3-second crash).
          try await Task.sleep(nanoseconds: 5_000_000_000)
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
    }

    await validateGroupStates()

    if protocolAuthorityMode == .rustFull {
      logger.info(
        "⏭️ [MLS-FULL-RUST] Skipping Swift missing-conversation sweep after Rust startup reconcile")
    } else {
      // Run detectAndRejoinMissingConversations in background to avoid blocking startup
      // CRITICAL FIX: Store task reference so it can be properly cancelled during shutdown
      // Previously this was a fire-and-forget Task.detached which caused 40+ second hangs
      // during account switching as External Commit operations continued running
      missingConversationsTask = Task(priority: .utility) { [weak self] in
        guard let self else { return }
        do {
          // Wait for app to settle before heavy FFI work (External Commits).
          try await Task.sleep(nanoseconds: 5_000_000_000)
          try Task.checkCancellation()
          try await self.detectAndRejoinMissingConversations()
        } catch is CancellationError {
          self.logger.info("📭 Missing conversation detection cancelled (expected during shutdown)")
        } catch {
          self.logger.error(
            "Failed to auto-rejoin missing conversations: \(error.localizedDescription)")
        }
      }
    }
    if configuration.enableAutomaticCleanup {
      startBackgroundCleanup()
    }

    startPeriodicSync()
    startOrphanAdoptionTask()
    startGroupInfoRefreshTask()
  }

  @discardableResult
  internal func prepareRustFullStartupDeviceAndKeyPackages(
    userDid: String,
    operation: String
  ) async -> Bool {
    guard protocolAuthorityMode == .rustFull else { return false }

    let reconciled = await runRustStartupReconcileIfNeeded(operation: operation)

    do {
      let mlsDid = try await withRustAuthoritativeRuntime(
        operation: "rustFullEnsureDeviceRegistered"
      ) { runtime in
        try runtime.ensureDeviceRegistered()
      }
      logger.info(
        "✅ [MLS-FULL-RUST] Device registered via Rust for \(userDid.prefix(20), privacy: .private) mlsDid=\(mlsDid.prefix(20), privacy: .private)"
      )
      _ = try await bindRustDeviceAuthentication(userDid: userDid, force: true)
    } catch {
      logger.error(
        "❌ [MLS-FULL-RUST] Failed to register device via Rust: \(error.localizedDescription, privacy: .public)"
      )
      return false
    }

    // Key-package replenishment tops the server pool back up to the target (50)
    // by publishing fresh KPs — and the Rust orchestrator's publish callback is
    // single-KP, so a full refill is ~50 SEQUENTIAL HTTP POSTs. Awaiting that
    // here blocked `initialize()` → `isInitialized` → manager-ready, which is
    // why messages "take forever" to appear after an account switch (the app's
    // 30s init timeout was firing on it). Replenishment is NOT needed to open,
    // read, or send in existing groups — it only refills the pool OTHERS draw
    // from to add us to FUTURE groups — so detach it onto the tracked,
    // shutdown-cancellable `keyPackageRefreshTask` and let init return now. The
    // 5s settle sleep mirrors the legacy path: it keeps `create_key_package`
    // FFI writes clear of an immediate background/suspend (0xdead10cc).
    keyPackageRefreshTask?.cancel()
    keyPackageRefreshTask = Task(priority: .utility) { [weak self] in
      guard let self else { return }
      do {
        try await Task.sleep(nanoseconds: 5_000_000_000)
        try Task.checkCancellation()
        try await self.withRustAuthoritativeRuntime(
          operation: "rustFullReplenishKeyPackagesIfNeeded"
        ) { runtime in
          try runtime.replenishKeyPackagesIfNeeded()
        }
        self.logger.info(
          "✅ [MLS-FULL-RUST] Key package replenishment checked via Rust (background)")
      } catch is CancellationError {
        self.logger.info(
          "⚠️ [MLS-FULL-RUST] Background key package replenishment cancelled (expected during shutdown)"
        )
      } catch {
        self.logger.error(
          "❌ [MLS-FULL-RUST] Rust key package replenishment failed: \(error.localizedDescription, privacy: .public)"
        )
      }
    }

    if configuration.skipDeviceRecordPublishing {
      logger.info("Skipping device record publish (skipDeviceRecordPublishing=true)")
    } else {
      logger.info(
        "⏭️ [MLS-FULL-RUST] Skipping Swift device record publish; Rust owns MLS device readiness")
    }

    logger.info("Loading persisted MLS storage for user: \(userDid)")
    logger.info("✅ MLS storage loaded successfully")
    return reconciled
  }

  @discardableResult
  internal func bindRustDeviceAuthentication(
    userDid: String,
    force: Bool = false,
    suspendedResumePermit: MLSClient.SuspendedResumeCapability? = nil
  ) async throws -> MLSDeviceAuthBindingStatus {
    if force {
      await MLSClient.shared.invalidateDeviceAuthBindingForReplacement(
        for: userDid,
        preservingSuspendedResumeCapability: suspendedResumePermit
      )
    }
    let deviceIDProviderOverride = deviceAuthDeviceIDProviderOverride
    let (runtime, runtimeDeviceID) = try await withRustAuthoritativeRuntime(
      operation: "rustFullDeviceAuthCurrentDevice",
      suspendedResumeCapability: suspendedResumePermit
    ) { runtime in
      (runtime, try deviceIDProviderOverride == nil ? runtime.currentDeviceInfo()?.deviceId : nil)
    }
    let deviceID =
      if let deviceIDProviderOverride {
        try await deviceIDProviderOverride()
      } else {
        runtimeDeviceID
      }
    guard let deviceID, !deviceID.isEmpty else {
      throw MLSDeviceAuthBindingError.malformedResponse
    }

    return try await MLSClient.shared.bindDeviceAuthentication(
      for: userDid,
      deviceID: deviceID,
      apiOverride: deviceAuthBindingAPIOverride,
      suspendedResumePermit: suspendedResumePermit
    ) { challenge in
      // Keep device resolution and proof signing on one runtime generation.
      // Runtime shutdown is the fail-closed boundary: the Rust bridge refuses
      // device-auth signing after shutdown, while binding invalidation prevents
      // an in-flight result from restoring local authority after replacement.
      try await self.withTrackedRustRuntime(
        runtime,
        operation: "rustFullDeviceAuthSignChallenge",
        suspendedResumeCapability: suspendedResumePermit
      ) { runtime in
        try runtime.signDeviceAuthChallenge(challenge)
      }
    }
  }

  /// Explicit hook for restored sessions, device re-registration, signer
  /// rotation, and transport rebinds. Existing volatile status is discarded
  /// before a fresh server challenge is requested.
  @discardableResult
  public func rebindCurrentDeviceAuthentication() async throws -> MLSDeviceAuthBindingStatus {
    guard let userDid else {
      throw MLSConversationError.noAuthentication
    }
    await MLSClient.shared.invalidateDeviceAuthBindingForReplacement(for: userDid)

    if protocolAuthorityMode == .rustFull {
      _ = try await withRustAuthoritativeRuntime(
        operation: "rustFullRebindEnsureDeviceRegistered"
      ) { runtime in
        try runtime.ensureDeviceRegistered()
      }
      return try await bindRustDeviceAuthentication(userDid: userDid, force: true)
    }

    _ = try await MLSClient.shared.ensureDeviceRegistered(userDid: userDid)
    guard let status = await MLSClient.shared.deviceAuthBindingStatus(for: userDid) else {
      throw MLSDeviceAuthBindingError.bindingMismatch
    }
    return status
  }

  private func prepareSwiftLegacyStartupDeviceAndKeyPackages(userDid: String) async {
    // CRITICAL FIX: Ensure device is registered with MLS server before proceeding
    // This prevents "Missing key packages" errors if device registration was skipped/removed
    do {
      _ = try await MLSClient.shared.ensureDeviceRegistered(userDid: userDid)
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
    logger.info("✅ MLS storage loaded successfully")

    do {
      let localBundleCount = try await MLSClient.shared.getKeyPackageBundleCount(for: userDid)
      logger.info("📊 [MLS Init] Local bundle count: \(localBundleCount)")

      keyPackageRefreshTask = Task { [weak self] in
        guard let self = self else { return }

        if localBundleCount == 0 {
          self.logger.warning("⚠️ [MLS Init] No local bundles found - will need replenishment")
          do {
            let result = try await MLSClient.shared.reconcileKeyPackagesWithServer(for: userDid)
            self.logger.info(
              "📊 [MLS Init] Reconciliation complete - server: \(result.serverAvailable), local: \(result.localBundles), desync: \(result.desyncDetected)"
            )
          } catch {
            self.logger.error("❌ [MLS Init] Reconciliation failed: \(error.localizedDescription)")
          }
        }

        do {
          let syncResult = try await MLSClient.shared.syncKeyPackageHashes(for: userDid)
          if syncResult.orphanedCount > 0 {
            self.logger.warning(
              "🔄 [MLS Init] Synced key packages - deleted \(syncResult.deletedCount) ORPHANED packages"
            )
            self.logger.info("   Remaining valid packages: \(syncResult.remainingAvailable)")
          } else {
            self.logger.info("✅ [MLS Init] Key package hashes in sync - no orphans found")
          }

          // CRITICAL FIX: If server has 0 key packages for THIS device, we must upload immediately
          // This can happen after app reinstall or device re-registration when old packages
          // belong to a different device_id. Without this fix, invites fail with NoMatchingKeyPackage.
          if syncResult.remainingAvailable == 0 {
            self.logger.warning(
              "🚨 [MLS Init] Server has 0 key packages for this device - uploading batch now")
            do {
              try await self.keyPackageManager.uploadKeyPackageBatchSmart(for: userDid, count: 25)
              self.logger.info("✅ [MLS Init] Emergency key package upload complete")
            } catch {
              self.logger.error(
                "❌ [MLS Init] Emergency key package upload failed: \(error.localizedDescription)")
            }
          }
        } catch {
          self.logger.error(
            "❌ [MLS Init] Key package hash sync failed: \(error.localizedDescription)")
        }
      }
    } catch {
      logger.warning(
        "⚠️ [MLS Init] Failed to check local bundle count: \(error.localizedDescription)")
    }
  }

  @discardableResult
  internal func runRustStartupReconcileIfNeeded(operation: String) async -> Bool {
    guard protocolAuthorityMode == .rustFull else { return false }

    guard !rustStartupReconcileCompleted else {
      logger.info(
        "⏭️ [MLS-FULL-RUST] Startup reconcile already completed; skipping \(operation, privacy: .public)"
      )
      return true
    }

    do {
      let report = try await withRustAuthoritativeRuntime(operation: operation) { runtime in
        try runtime.startupReconcile()
      }
      rustStartupReconcileCompleted = true
      logger.info(
        "✅ [MLS-FULL-RUST] Startup reconcile completed scanned=\(report.scanned, privacy: .public) healthy=\(report.healthy, privacy: .public) needsRejoin=\(report.needsRejoin, privacy: .public) resetPending=\(report.resetPending, privacy: .public) unrecoverableLocal=\(report.unrecoverableLocal, privacy: .public)"
      )
      return true
    } catch {
      logger.error(
        "❌ [MLS-FULL-RUST] Startup reconcile failed: \(error.localizedDescription, privacy: .public)"
      )
      return false
    }
  }

  internal func validateGroupStates() async {
    if protocolAuthorityMode == .rustFull {
      await runRustStartupReconcileIfNeeded(operation: "startupReconcile")
      return
    }

    #if MLS_SWIFT_LEGACY_PROTOCOL
      await validateGroupStatesLegacy()
    #else
      do {
        try assertSwiftProtocolMutationAllowed("validateGroupStates legacy protocol implementation")
        await validateGroupStatesLegacy()
      } catch {
        logger.info(
          "⏭️ [MLS-FULL-RUST] Skipping validateGroupStates in \(self.protocolAuthorityMode.rawValue, privacy: .public): \(error.localizedDescription, privacy: .public)"
        )
      }
    #endif
  }

  private func validateGroupStatesLegacy() async {
    logger.info("🔍 [STARTUP] Validating MLS group state for all conversations...")

    guard let userDid = userDid else {
      logger.warning("[STARTUP] No user DID - skipping group state validation")
      return
    }

    do {
      try assertSwiftProtocolMutationAllowed("startup validation")
    } catch {
      logger.info(
        "⏭️ [MLS-FULL-RUST] Skipping validateGroupStates in \(self.protocolAuthorityMode.rawValue, privacy: .public): \(error.localizedDescription, privacy: .public)"
      )
      return
    }

    do {
      let conversations = try await database.read { db in
        try MLSConversationModel
          .filter(MLSConversationModel.Columns.currentUserDID == userDid)
          .fetchAll(db)
      }

      logger.info("📋 [STARTUP] Found \(conversations.count) conversations to validate")

      var corruptedConversations: [MLSConversationModel] = []
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
            "❌ [STARTUP] Suspect corrupted group state detected for conversation \(convoIdPrefix)..."
          )
          logger.error("   Error: \(error.localizedDescription)")
          corruptedConversations.append(conversation)
        }
      }

      switch MLSStartupValidationPolicy.decision(
        totalConversations: conversations.count,
        validatedCount: validatedCount,
        corruptedCount: corruptedConversations.count
      ) {
      case .noCorruption:
        logger.info("✅ [STARTUP] All \(validatedCount) conversation(s) have valid MLS group state")

      case .deferDestructiveRecovery:
        let suppressedUntil = Date().addingTimeInterval(
          MLSStartupValidationPolicy.automaticRecoverySuppressionInterval
        )
        automaticMissingConversationRecoverySuppressedUntil = suppressedUntil
        logger.error(
          "🛑 [STARTUP] Detected systemic MLS state loss: \(corruptedConversations.count)/\(conversations.count) conversations failed validation with only \(validatedCount) success(es). Preserving local records and suppressing automatic missing-conversation recovery until \(suppressedUntil)."
        )
        return

      case .markCorruptedGroupsForRejoin:
        var markedCount = 0
        for conversation in corruptedConversations {
          if isShuttingDown || Task.isCancelled {
            logger.warning("⚠️ [STARTUP] Corruption marking interrupted by shutdown")
            return
          }

          let groupIdData = conversation.groupID
          let convoIdPrefix = String(conversation.conversationID.prefix(8))
          do {
            try await mlsClient.deleteGroup(for: userDid, groupId: groupIdData)
            logger.info("🗑️ Deleted corrupted local group state for \(convoIdPrefix)...")
          } catch {
            logger.error("   Failed to delete corrupted group: \(error.localizedDescription)")
          }

          do {
            try await markConversationNeedsRejoin(conversation.conversationID)
            logger.info("⚠️ Marked conversation \(convoIdPrefix)... for rejoin")
            markedCount += 1
          } catch {
            logger.error("   Failed to mark conversation for rejoin: \(error.localizedDescription)")
          }
        }

        logger.warning(
          "⚠️ [STARTUP] Found \(markedCount) conversation(s) with corrupted MLS state - marked for rejoin"
        )
      }
    } catch {
      logger.error("❌ [STARTUP] Failed to validate group states: \(error.localizedDescription)")
    }
  }

  public func detectAndRejoinMissingConversations() async throws {
    if protocolAuthorityMode == .rustFull {
      let report = try await withRustAuthoritativeRuntime(
        operation: "detectAndRejoinMissingConversations"
      ) { runtime in
        try runtime.startupReconcile()
      }
      logger.info(
        "✅ [MLS-FULL-RUST] Missing-conversation Rust reconcile completed scanned=\(report.scanned, privacy: .public) healthy=\(report.healthy, privacy: .public) needsRejoin=\(report.needsRejoin, privacy: .public) resetPending=\(report.resetPending, privacy: .public) unrecoverableLocal=\(report.unrecoverableLocal, privacy: .public)"
      )
      return
    }

    #if MLS_SWIFT_LEGACY_PROTOCOL
      try await detectAndRejoinMissingConversationsLegacy()
    #else
      try assertSwiftProtocolMutationAllowed(
        "detectAndRejoinMissingConversations legacy protocol implementation"
      )
      try await detectAndRejoinMissingConversationsLegacy()
    #endif
  }

  private func detectAndRejoinMissingConversationsLegacy() async throws {
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

    if let suppressedUntil = automaticMissingConversationRecoverySuppressedUntil {
      if Date() < suppressedUntil {
        logger.error(
          "🛑 [REJOIN] Automatic missing-conversation recovery suppressed until \(suppressedUntil) after systemic startup validation failure"
        )
        return
      }
      automaticMissingConversationRecoverySuppressedUntil = nil
    }

    try assertSwiftProtocolMutationAllowed("detectAndRejoinMissingConversations")

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

        let mode: MLSRecoveryManager.RecoveryMode =
          corruptedConvos.count >= MLSRecoveryManager.batchRecoveryThreshold
          ? .batchRecovery : .normal

        if mode == .batchRecovery {
          logger.info(
            "⚡ [REJOIN] Entering BATCH recovery mode for \(corruptedConvos.count) corrupted conversations"
          )
          let chunks = corruptedConvos.chunked(into: MLSRecoveryManager.maxConcurrentRejoins)
          for chunk in chunks {
            if isShuttingDown || Task.isCancelled { break }

            await withTaskGroup(of: Void.self) { group in
              for convo in chunk {
                group.addTask { [weak self] in
                  guard let self = self, let userDid = self.userDid else { return }

                  if let recoveryManager = await self.mlsClient.recovery(for: userDid) {
                    let shouldSkip = await recoveryManager.shouldSkipRejoin(
                      convoId: convo.conversationID, mode: .batchRecovery)
                    if shouldSkip {
                      self.logger.info(
                        "⏭️ [REJOIN] Skipping \(convo.conversationID.prefix(16))... - MLSRecoveryManager backoff active"
                      )
                      return
                    }
                  }

                  guard
                    self.beginRejoinAttempt(
                      conversationID: convo.conversationID, source: "deferred-epoch-recovery")
                  else {
                    return
                  }

                  let groupIdData = convo.groupID
                  let preDeleteAuthHex: String? =
                    await self.mlsClient.groupExists(for: userDid, groupId: groupIdData)
                    ? await self.mlsClient.epochAuthenticatorHex(for: userDid, groupId: groupIdData)
                    : nil

                  if await self.mlsClient.groupExists(for: userDid, groupId: groupIdData) {
                    self.logger.info(
                      "🗑️ [REJOIN] Deleting stale local group state for \(convo.conversationID.prefix(16))..."
                    )
                    do {
                      try await self.mlsClient.deleteGroup(for: userDid, groupId: groupIdData)
                    } catch {
                      self.logger.warning(
                        "⚠️ [REJOIN] Failed to delete stale group: \(error.localizedDescription)")
                    }
                    self.removeCachedGroupState(
                      conversationID: convo.conversationID, groupID: groupIdData)
                  }

                  let rejoinResult = await self.attemptRejoinWithWelcomeFallback(
                    convoId: convo.conversationID,
                    displayName: convo.conversationID,
                    reason: "deferred epoch recovery (sync catch-up failed, batch)",
                    preDeleteAuthHex: preDeleteAuthHex
                  )
                  self.endRejoinAttempt(conversationID: convo.conversationID)

                  if let recoveryManager = await self.mlsClient.recovery(for: userDid) {
                    if rejoinResult.didJoin {
                      await recoveryManager.clearRejoinTracking(convoId: convo.conversationID)
                    } else if rejoinResult.shouldRecordFailure {
                      await recoveryManager.recordFailedRejoin(
                        convoId: convo.conversationID,
                        epochAuthenticatorHex: preDeleteAuthHex,
                        failureType: "deferred_epoch_recovery_failed"
                      )
                    }
                  }
                }
              }
            }

            if chunks.last != chunk {
              logger.info(
                "⚡ [REJOIN] Pausing \(MLSRecoveryManager.batchPauseSec)s between batch chunks...")
              try? await Task.sleep(for: .seconds(MLSRecoveryManager.batchPauseSec))
            }
          }
        } else {
          for convo in corruptedConvos {
            // CRITICAL: Check for shutdown/cancellation between each rejoin attempt
            if isShuttingDown || Task.isCancelled {
              logger.warning("⚠️ [REJOIN] Interrupted by shutdown - stopping corrupted convos loop")
              return
            }

            // ⭐ FIX: Gate deferred rejoins through MLSRecoveryManager backoff (30s → 2m → 10m → 1h)
            // instead of the weaker 60s beginRejoinAttempt cooldown. This prevents sync-triggered
            // epoch inflation from repeatedly External-Committing every minute.
            if let recoveryManager = await mlsClient.recovery(for: userDid) {
              let shouldSkip = await recoveryManager.shouldSkipRejoin(
                convoId: convo.conversationID, mode: .normal)
              if shouldSkip {
                logger.info(
                  "⏭️ [REJOIN] Skipping \(convo.conversationID.prefix(16))... - MLSRecoveryManager backoff active"
                )
                continue
              }
            }

            guard
              beginRejoinAttempt(
                conversationID: convo.conversationID,
                source: "deferred-epoch-recovery"
              )
            else {
              continue
            }

            // ⭐ FIX P1b: Delete stale local group state BEFORE attempting rejoin.
            // Without this, `attemptExternalCommitFallback` sees `groupExists == true`
            // and returns immediately without repairing the desynchronized ratchet state.
            let groupIdData = convo.groupID
            // Capture authenticator BEFORE deletion so the A7 reset-vote pyramid
            // receives a real vote on failure (post-delete, FFI returns
            // GroupNotFound and server short-circuits as missing_authenticator).
            let preDeleteAuthHex: String? =
              await mlsClient.groupExists(for: userDid, groupId: groupIdData)
              ? await mlsClient.epochAuthenticatorHex(for: userDid, groupId: groupIdData)
              : nil
            if await mlsClient.groupExists(for: userDid, groupId: groupIdData) {
              logger.info(
                "🗑️ [REJOIN] Deleting stale local group state for \(convo.conversationID.prefix(16))..."
              )
              do {
                try await mlsClient.deleteGroup(for: userDid, groupId: groupIdData)
              } catch {
                logger.warning(
                  "⚠️ [REJOIN] Failed to delete stale group: \(error.localizedDescription)")
              }
              removeCachedGroupState(conversationID: convo.conversationID, groupID: groupIdData)
            }

            let rejoinResult = await attemptRejoinWithWelcomeFallback(
              convoId: convo.conversationID,
              displayName: convo.conversationID,
              reason: "deferred epoch recovery (sync catch-up failed)",
              preDeleteAuthHex: preDeleteAuthHex
            )
            endRejoinAttempt(conversationID: convo.conversationID)

            // Record success/failure in MLSRecoveryManager for backoff tracking
            if let recoveryManager = await mlsClient.recovery(for: userDid) {
              if rejoinResult.didJoin {
                await recoveryManager.clearRejoinTracking(convoId: convo.conversationID)
              } else if rejoinResult.shouldRecordFailure {
                await recoveryManager.recordFailedRejoin(
                  convoId: convo.conversationID,
                  epochAuthenticatorHex: preDeleteAuthHex,
                  failureType: "deferred_epoch_recovery_failed"
                )
              }
            }
          }
        }
      }

      // Check for cancellation before making network call
      if isShuttingDown || Task.isCancelled {
        logger.warning("⚠️ [REJOIN] Aborting before getExpectedConversations - shutdown in progress")
        return
      }

      guard
        await ensureActiveAccount(for: userDid, operation: "detectAndRejoinMissingConversations")
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

      var convosNeedingRejoin = missingConvos
      convosNeedingRejoin.removeAll()
      for convo in missingConvos {
        guard let groupIdData = Data(hexEncoded: convo.groupId) else { continue }
        let exists = await mlsClient.groupExists(for: userDid, groupId: groupIdData)
        if exists {
          if (try? await mlsClient.getEpoch(for: userDid, groupId: groupIdData)) != nil {
            await clearConversationRejoinFlag(convo.conversationId)
            skippedCount += 1
            continue
          }
        }
        convosNeedingRejoin.append(convo)
      }

      let mode: MLSRecoveryManager.RecoveryMode =
        convosNeedingRejoin.count >= MLSRecoveryManager.batchRecoveryThreshold
        ? .batchRecovery : .normal

      if mode == .batchRecovery {
        logger.info(
          "⚡ [REJOIN] Entering BATCH recovery mode for \(convosNeedingRejoin.count) missing conversations"
        )
        let chunks = convosNeedingRejoin.chunked(into: MLSRecoveryManager.maxConcurrentRejoins)
        for chunk in chunks {
          if isShuttingDown || Task.isCancelled { break }

          await withTaskGroup(of: Void.self) { group in
            for convo in chunk {
              group.addTask { [weak self] in
                guard let self = self, let userDid = self.userDid else { return }

                guard let groupIdData = Data(hexEncoded: convo.groupId) else {
                  self.logger.warning(
                    "⚠️ Invalid groupId format for \(convo.conversationId) - skipping")
                  return
                }

                let groupExists = await self.mlsClient.groupExists(
                  for: userDid, groupId: groupIdData)

                if let recoveryManager = await self.mlsClient.recovery(for: userDid) {
                  let shouldSkip = await recoveryManager.shouldSkipRejoin(
                    convoId: convo.conversationId, mode: .batchRecovery)
                  if shouldSkip {
                    self.logger.warning(
                      "⏭️ [REJOIN] Skipping \(convo.conversationId.prefix(16))... - recovery tracking says skip"
                    )
                    return
                  }
                }

                guard
                  self.beginRejoinAttempt(
                    conversationID: convo.conversationId, source: "missing-convo")
                else {
                  return
                }

                let preDeleteAuthHex: String? =
                  groupExists
                  ? await self.mlsClient.epochAuthenticatorHex(for: userDid, groupId: groupIdData)
                  : nil

                let rejoinResult = await self.attemptRejoinWithWelcomeFallback(
                  convoId: convo.conversationId,
                  displayName: nil,
                  reason: "server reported missing (batch)",
                  preDeleteAuthHex: preDeleteAuthHex
                )
                self.endRejoinAttempt(conversationID: convo.conversationId)

                if rejoinResult.didJoin {
                  if let recoveryManager = await self.mlsClient.recovery(for: userDid) {
                    await recoveryManager.clearRejoinTracking(convoId: convo.conversationId)
                  }
                } else if rejoinResult.shouldRecordFailure {
                  if let recoveryManager = await self.mlsClient.recovery(for: userDid) {
                    await recoveryManager.recordFailedRejoin(
                      convoId: convo.conversationId,
                      epochAuthenticatorHex: preDeleteAuthHex,
                      failureType: "missing_convo_rejoin_failed"
                    )
                  }
                }
              }
            }
          }

          if chunks.last != chunk {
            logger.info(
              "⚡ [REJOIN] Pausing \(MLSRecoveryManager.batchPauseSec)s between batch chunks...")
            try? await Task.sleep(for: .seconds(MLSRecoveryManager.batchPauseSec))
          }
        }
      } else {
        for convo in convosNeedingRejoin {
          if isShuttingDown || Task.isCancelled {
            logger.warning("⚠️ [REJOIN] Interrupted by shutdown - stopping rejoin loop")
            break
          }

          guard
            await ensureActiveAccount(
              for: userDid, operation: "detectAndRejoinMissingConversations")
          else {
            logger.info("⏸️ [REJOIN] Stopping missing-conversation loop for inactive account")
            break
          }

          guard let groupIdData = Data(hexEncoded: convo.groupId) else {
            logger.warning("⚠️ Invalid groupId format for \(convo.conversationId) - skipping")
            failureCount += 1
            continue
          }

          let groupExists = await mlsClient.groupExists(for: userDid, groupId: groupIdData)

          guard
            beginRejoinAttempt(
              conversationID: convo.conversationId,
              source: "missing-convo"
            )
          else {
            skippedCount += 1
            continue
          }

          let preDeleteAuthHex: String? =
            groupExists
            ? await mlsClient.epochAuthenticatorHex(for: userDid, groupId: groupIdData)
            : nil

          let rejoinResult = await attemptRejoinWithWelcomeFallback(
            convoId: convo.conversationId,
            displayName: nil,
            reason: "server reported missing",
            preDeleteAuthHex: preDeleteAuthHex
          )
          endRejoinAttempt(conversationID: convo.conversationId)

          if rejoinResult.didJoin {
            successCount += 1
            if let recoveryManager = await mlsClient.recovery(for: userDid) {
              await recoveryManager.clearRejoinTracking(convoId: convo.conversationId)
            }
          } else if rejoinResult.shouldRecordFailure {
            failureCount += 1
            if let recoveryManager = await mlsClient.recovery(for: userDid) {
              await recoveryManager.recordFailedRejoin(
                convoId: convo.conversationId,
                epochAuthenticatorHex: preDeleteAuthHex,
                failureType: "missing_convo_rejoin_failed"
              )
            }
          } else {
            skippedCount += 1
          }

          try? await Task.sleep(nanoseconds: 100_000_000)
        }
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
    convoId: String,
    displayName: String?,
    reason: String,
    preDeleteAuthHex: String? = nil
  ) async -> RejoinAttemptResult {
    let label = displayName ?? convoId
    logger.info("📞 Requesting recovery for \(label) (\(reason))")

    guard let userDid = userDid else {
      logger.error("❌ Cannot rejoin \(label): missing user DID")
      return .skippedNoAttempt
    }

    guard await ensureActiveAccount(for: userDid, operation: "attemptRejoinWithWelcomeFallback")
    else {
      return .skippedNoAttempt
    }

    do {
      if (try await joinOrRejoinWithRustAuthorityIfNeeded(
        conversationId: convoId,
        operation: "attemptRejoinWithWelcomeFallback"
      )) != nil {
        await clearConversationRejoinFlag(convoId)
        return .joined
      }
    } catch is CancellationError {
      logger.info(
        "📭 [attemptRejoin] Rust joinOrRejoin cancelled for \(label) (expected during shutdown)")
      return .skippedNoAttempt
    } catch {
      logger.error(
        "❌ [attemptRejoin] Rust joinOrRejoin failed for \(label): \(error.localizedDescription)")
      return .failed
    }

    // ALWAYS try Welcome first, even for creators without local state.
    // External Commit should only be used when Welcome is truly unavailable (404/410).
    // This preserves the epoch and avoids unnecessary epoch advancement on transient errors.
    let welcomeResult = await attemptWelcomeRejoin(convoId: convoId, label: label)
    switch welcomeResult {
    case .joined:
      return .joined
    case .doNotFallback(let reason):
      logger.info("⏸️ [attemptRejoin] Skipping External Commit for \(label): \(reason)")
      return .failed
    case .fallbackToExternalCommit(let fallbackReason):
      // CRITICAL: Check for shutdown/cancellation BEFORE starting expensive External Commit
      // This prevents the main cause of 40+ second hangs during account switch
      if isShuttingDown || Task.isCancelled {
        logger.warning("⚠️ [attemptRejoin] Aborted before External Commit - shutdown in progress")
        return .skippedNoAttempt
      }

      logger.info(
        "🔄 [attemptRejoin] \(fallbackReason) for \(label), attempting External Commit...")

      if let recoveryManager = await mlsClient.recovery(for: userDid),
        let remaining = await recoveryManager.successCooldownRemaining(convoId: convoId)
      {
        logger.info(
          "⏭️ [attemptRejoin] Skipping External Commit for \(label): successful-rejoin cooldown active (\(Int(remaining))s remaining)"
        )
        return .skippedNoAttempt
      }

      // Phase 2 Stage 3 (spec §8.6 / ADR-008 D1): if the Welcome failure was
      // specifically the "200 with no welcome blob" sentinel, this attempt is
      // a candidate for the operationally-unrecoverable trifecta. The third
      // condition (FFI GroupNotFound) is implicit at this point — callers
      // (`runDeferredEpochRecovery`, `detectAndRejoinMissingConversations`)
      // delete stale local state before invoking us, and
      // `attemptExternalCommitFallback` early-returns on `groupExists == true`
      // (Messaging.swift:5388-5395). Reaching the EC catch implies the
      // local FFI state is absent.
      let isTrifectaCandidate = Self.isTrifectaWelcomeReason(fallbackReason)

      do {
        _ = try await attemptExternalCommitFallback(
          convoId: convoId,
          userDid: userDid,
          reason: "\(fallbackReason) (rejoin)"
        )
        logger.info("✅ Successfully rejoined \(label) via External Commit")
        await clearConversationRejoinFlag(convoId)
        return .joined
      } catch is CancellationError {
        logger.info(
          "📭 [attemptRejoin] External Commit cancelled for \(label) (expected during shutdown)")
        return .skippedNoAttempt
      } catch let error as RejoinSkippedNoAttemptError {
        logger.info("⏸️ [attemptRejoin] \(error.localizedDescription) for \(label)")
        return .skippedNoAttempt
      } catch {
        logger.error(
          "❌ Failed to rejoin \(label) via External Commit: \(error.localizedDescription)")

        // B11: First-responder bootstrap fallback for the needsRejoin path.
        // Mirrors `runDeferredEpochRecoveryRecipient`'s bootstrap fallback
        // (Sync.swift:966-988) but covers convos that never carried
        // `needsReset = true` to the deferred-recovery loop — typically
        // ones that previously succeeded a recipient rejoin and got
        // re-flagged as `needsRejoin` later when startup validation noticed
        // the FFI lacks the group. Without this, every such convo enters a
        // permanent "EC → 404 GroupInfo → backoff" loop because no member
        // ever races to populate the empty post-reset row.
        //
        // Idempotency: server's `bootstrapResetGroup` only writes when
        // `group_info IS NULL`; concurrent racers get 409 AlreadyBootstrapped
        // and fall through to the winner's GroupInfo on the next loop.
        let groupInfoMissing = await isGroupInfoMissing(convoId: convoId)
        if groupInfoMissing {
          guard
            let localConvo = await loadLocalConvoForBootstrap(
              convoId: convoId, userDid: userDid
            )
          else {
            logger.warning(
              "⚠️ [attemptRejoin] GroupInfo absent for \(label) but local convo not loadable — skipping bootstrap fallback"
            )
            if isTrifectaCandidate, Self.isTrifectaExternalCommitError(error) {
              await recordTrifectaFailure(
                convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
            }
            return .failed
          }

          logger.warning(
            "🚀 [attemptRejoin] GroupInfo absent for \(label) — racing for first-responder bootstrap (post-reset path)"
          )
          await attemptFirstResponderBootstrap(
            convoId: convoId,
            userDid: userDid,
            pendingNewGroupIdHex: localConvo.groupID.hexEncodedString(),
            observedGeneration: localConvo.pendingResetGeneration,
            preDeleteAuthHex: preDeleteAuthHex
          )

          let groupIdData = localConvo.groupID
          if await mlsClient.groupExists(for: userDid, groupId: groupIdData) {
            logger.info(
              "✅ [attemptRejoin] First-responder bootstrap succeeded for \(label)")
            await clearConversationRejoinFlag(convoId)
            return .joined
          }
          logger.info(
            "⏳ [attemptRejoin] Bootstrap dispatched for \(label) but group not yet present locally — next sync will retry"
          )
          return .failed
        }

        if isTrifectaCandidate, Self.isTrifectaExternalCommitError(error) {
          await recordTrifectaFailure(
            convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
        }
        return .failed
      }
    }
  }

  /// B11: Load the local `MLSConversationModel` for `convoId` so the
  /// rejoin path can extract the staged `groupID` to feed
  /// `attemptFirstResponderBootstrap`. Doesn't trust the in-memory
  /// `conversations` dictionary alone — falls through to the persistent
  /// store so a stale cache (post-restart, post-account-switch) can't
  /// cause us to bootstrap with the wrong groupId.
  private func loadLocalConvoForBootstrap(
    convoId: String, userDid: String
  ) async -> MLSConversationModel? {
    do {
      return try await storage.fetchConversation(
        conversationID: convoId,
        currentUserDID: userDid,
        database: database
      )
    } catch {
      logger.warning(
        "⚠️ [loadLocalConvoForBootstrap] DB lookup failed for \(convoId.prefix(16)): \(error.localizedDescription)"
      )
      return nil
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
      return await classifyWelcomeRejoinFailure(error, label: label, convo: convo)
    }
  }

  private func classifyWelcomeRejoinFailure(
    _ error: Error,
    label: String,
    convo: BlueCatbirdMlsChatDefs.ConvoView
  ) async -> WelcomeRejoinResult {
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
        return .fallbackToExternalCommit(
          reason: "Welcome unavailable (missing welcome in 200 response)")
      }

      if case .httpError(let code, _) = apiError {
        switch code {
        case 404:
          return await classifyMissingWelcomeRecovery(
            for: convo,
            label: label,
            failure: .welcomeUnavailable,
            fallbackReason: "Welcome unavailable (HTTP 404)"
          )
        case 410:
          return await classifyMissingWelcomeRecovery(
            for: convo,
            label: label,
            failure: .welcomeExpired,
            fallbackReason: "Welcome expired (HTTP 410)"
          )
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
          return await classifyMissingWelcomeRecovery(
            for: convo,
            label: label,
            failure: .welcomeUnavailable,
            fallbackReason: "Welcome unavailable (NetworkError 404)"
          )
        case 410:
          return await classifyMissingWelcomeRecovery(
            for: convo,
            label: label,
            failure: .welcomeExpired,
            fallbackReason: "Welcome expired (NetworkError 410)"
          )
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

  private func classifyMissingWelcomeRecovery(
    for convo: BlueCatbirdMlsChatDefs.ConvoView,
    label: String,
    failure: WelcomeRecoveryFailure,
    fallbackReason: String
  ) async -> WelcomeRejoinResult {
    switch await decideWelcomeRecovery(for: convo, failure: failure) {
    case .requestReissue(let reason, let nextAttempt):
      do {
        try await requestWelcomeReissueAndWait(
          convo: convo,
          reason: reason,
          nextAttempt: nextAttempt
        )
      } catch {
        logger.info(
          "📨 [attemptRejoin] Welcome reissue path selected for \(label): \(error.localizedDescription)"
        )
      }
      return .doNotFallback(reason: "Welcome reissue requested")
    case .externalCommitWithHistoryGap:
      logger.info("📭 \(fallbackReason) for \(label) - will try External Commit")
      return .fallbackToExternalCommit(reason: fallbackReason)
    case .surrender(let reason, _):
      await markWelcomeRecoverySurrendered(convoId: convo.conversationId, reason: reason)
      return .doNotFallback(reason: reason)
    case .accept:
      return .doNotFallback(reason: "Welcome recovery accepted unexpectedly")
    }
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

  internal func fetchConversationForRejoin(convoId: String) async -> BlueCatbirdMlsChatDefs
    .ConvoView?
  {
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

      if let match = conversations.convos.first(where: {
        MLSConversationIdentity.matches(
          requestedId: convoId,
          conversationId: $0.conversationId,
          groupId: $0.groupId
        )
      }) {
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
