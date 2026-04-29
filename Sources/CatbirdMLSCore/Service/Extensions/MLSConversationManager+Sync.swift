
import Foundation
import GRDB
import OSLog
import Petrel
import Synchronization

public extension MLSConversationManager {

  // MARK: - Server Synchronization
  
  /// Wait for any in-progress sync to complete, then trigger a fresh sync
  /// This is useful for E2E testing where we need to ensure fresh data
  /// - Parameter maxWait: Maximum time to wait for ongoing sync (default 30 seconds)
  public func waitAndSyncWithServer(maxWait: TimeInterval = 30) async throws {
    if isSyncPaused || isSuspending || MLSClient.isSuspensionInProgress {
      logger.info("⏸️ [waitAndSync] Skipping sync while MLS is paused/suspending")
      return
    }

    let startTime = Date()
    var waitCount = 0
    
    // Wait for sync lock to be available
    while true {
      let elapsed = Date().timeIntervalSince(startTime)
      if elapsed >= maxWait {
        logger.warning("⚠️ [waitAndSync] Timed out waiting for sync lock after \(Int(elapsed))s")
        break
      }
      
      let isSyncing = syncState.withLock { $0 }
      if !isSyncing {
        // Lock is free, proceed with sync
        logger.info("🔓 [waitAndSync] Sync lock released after \(Int(elapsed))s, triggering fresh sync")
        break
      }
      
      waitCount += 1
      if waitCount % 5 == 1 {
        logger.info("⏳ [waitAndSync] Waiting for sync lock (\(Int(elapsed))s elapsed)...")
      }
      try await Task.sleep(nanoseconds: 500_000_000) // 0.5 second intervals
    }
    
    // Now trigger a fresh sync
    try await syncWithServer(fullSync: false)
  }

  /// Sync conversations with server
  /// - Parameter fullSync: Whether to perform full sync or incremental
  public func syncWithServer(fullSync: Bool = false) async throws {
    // CRITICAL: Capture session generation at start to detect account switches
    let myGeneration = sessionGeneration

    try throwIfShuttingDown("syncWithServer")

    // Reconnect database pool if it was closed during recovery
    try await refreshDatabaseIfNeeded()

    // 🪝 [CLIENT E] One-time-per-session boot scan for half-staged
    // bootstrap groups. Must run BEFORE the first sync iteration so any
    // half-staged group is cleaned up before the new sync's recovery
    // logic re-encounters it and double-acts.
    if !didRunBootstrapPendingScan {
      didRunBootstrapPendingScan = true
      await cleanUpHalfStagedBootstrapGroups()
    }

    // Do not start sync while lifecycle has MLS paused/suspending.
    if isSyncPaused || isSuspending || MLSClient.isSuspensionInProgress {
      logger.info("⏸️ [SYNC] Skipping sync while MLS is paused/suspending")
      return
    }

    // Validate session generation
    try validateSessionGeneration(capturedGeneration: myGeneration)

    // CIRCUIT BREAKER: Check if sync is paused due to repeated failures
    if let pausedAt = syncPausedAt {
      let elapsed = Date().timeIntervalSince(pausedAt)
      if elapsed < syncPauseDuration {
        let remaining = Int(syncPauseDuration - elapsed)
        logger.warning(
          "⛔ Sync paused due to \(self.consecutiveSyncFailures) consecutive failures (\(remaining)s remaining)"
        )
        return
      } else {
        // Reset circuit breaker after pause period
        logger.info("✅ Sync pause period expired, resuming normal operation")
        syncPausedAt = nil
        consecutiveSyncFailures = 0
      }
    }

    // CRITICAL FIX: Use Mutex to atomically check and set sync state
    // This prevents race conditions where multiple syncs start simultaneously
    let didAcquire = syncState.withLock { syncing -> Bool in
      if syncing {
        return false
      }
      syncing = true
      return true
    }
    guard didAcquire else {
      // Normal condition - sync is serialized. Use debug level to reduce log noise.
      logger.debug("⏸️ Sync skipped - another sync already in progress")
      return
    }

    defer {
      syncState.withLock { $0 = false }
    }

    // CRITICAL FIX: Validate that we're syncing for the correct user
    // This prevents account switch race conditions where the API client
    // has already switched to a different user but sync is still running
    guard let userDid = userDid else {
      logger.error("Cannot sync: no user DID")
      return
    }

    // Validate generation again after acquiring lock
    try validateSessionGeneration(capturedGeneration: myGeneration)

    // ═══════════════════════════════════════════════════════════════════════════
    // MULTI-ACCOUNT FIX: Skip sync if this manager's account is not the active one
    // ═══════════════════════════════════════════════════════════════════════════
    // In multi-account scenarios, cached AppStates may have MLSConversationManagers
    // that run background tasks. The ATProtoClient is shared and getDid() returns
    // whichever account is currently active - NOT this manager's bound account.
    //
    // Previous behavior: validateAuthentication() would throw an error, causing
    // "Account mismatch" errors even though the manager was working correctly
    // for its own account.
    //
    // New behavior: Check if this manager's userDid matches the ATProtoClient's
    // current session. If not, gracefully skip the sync (this manager's account
    // is not active right now) rather than throwing an error.
    // ═══════════════════════════════════════════════════════════════════════════
    let isActiveAccount = await apiClient.isAuthenticatedAs(userDid)
    if !isActiveAccount {
      // This is normal in multi-account scenarios - just skip silently
      logger.info("⏸️ [SYNC] Skipping sync - this account (\(userDid.prefix(20))...) is not the active account")
      return
    }

    logger.info("Starting server sync (full: \(fullSync))")

    do {
      // Fetch conversations from server
      var allConvos: [BlueCatbirdMlsChatDefs.ConvoView] = []
      var cursor: String?
      var pageCount = 0

      repeat {
        pageCount += 1
        // CRITICAL FIX: Check shutdown state during pagination loop
        // This prevents continuing to fetch while account is switching
        try throwIfShuttingDown("syncWithServer pagination")
        
        let result = try await apiClient.getConversations(limit: 100, cursor: cursor)
        allConvos.append(contentsOf: result.convos)
        cursor = result.cursor
      } while cursor != nil

      // ⭐ FIX: Filter out conversations where user is no longer a member
      // Server may return stale conversations after user has left
      // Also track stale conversations to clean up local state
      let normalizedUserDid = userDid.lowercased()
      var staleConvoIds: [String] = []

      allConvos = allConvos.filter { convo in
        let isUserMember = convo.members.contains {
          $0.did.description.lowercased() == normalizedUserDid
        }
        if !isUserMember {
          logger.info(
            "⏭️ [SYNC] Filtering out conversation \(convo.conversationId.prefix(16))... - user is not a member"
          )
          staleConvoIds.append(convo.conversationId)
        }
        return isUserMember
      }

      // Clean up stale conversations from local state
      if !staleConvoIds.isEmpty {
        logger.info("🧹 [SYNC] Cleaning up \(staleConvoIds.count) stale conversation(s) after leave")
        for convoId in staleConvoIds {
          conversations.removeValue(forKey: convoId)
          groupStates.removeValue(forKey: convoId)
        }
        // Delete from database (await to ensure plaintext is securely deleted)
        try await deleteConversationsFromDatabase(staleConvoIds)
      }

      // Update local state and initialize MLS groups
      for convo in allConvos {
        // CRITICAL FIX: Check shutdown state for each conversation
        // This prevents continuing to process while account is switching
        if isShuttingDown {
          logger.warning("⚠️ [SYNC] Shutdown detected during conversation processing - aborting")
          break
        }
        
        let existingConvo = conversations[convo.conversationId]
        conversations[convo.conversationId] = convo

        // Check if we need to initialize the MLS group
        let needsGroupInit = groupStates[convo.groupId] == nil

        // Update group state metadata
        if groupStates[convo.groupId] == nil {
          // ⭐ CRITICAL FIX: Verify epoch from FFI instead of trusting server
          // Note: userDid is guaranteed non-nil from auth check at start of function

          guard let groupIdData = Data(hexEncoded: convo.groupId) else {
            logger.error("Invalid group ID hex: \(convo.groupId)")
            continue  // Skip this conversation
          }

          let serverEpoch = UInt64(convo.epoch)
          var ffiEpoch = serverEpoch  // Default to server if FFI query fails

          // Try to get FFI epoch, but don't fail sync if group not yet initialized
          do {
            ffiEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)

            if serverEpoch != ffiEpoch {
              logger.warning("⚠️ EPOCH MISMATCH in syncWithServer (new group):")
              logger.warning("   Server: \(serverEpoch), FFI: \(ffiEpoch)")

              // ⭐ Skip epoch catch-up for conversations already flagged needsRejoin.
              // Deferred recovery will handle these outside the sync hot-path.
              if await conversationNeedsRejoin(convo.conversationId) {
                logger.info("⏭️ [SYNC] Skipping epoch catch-up for \(convo.conversationId.prefix(16))... - already flagged needsRejoin")
              } else
              // Attempt to catch up by processing missed commits
              if serverEpoch > ffiEpoch {
                let result = await fetchAndProcessMissingCommits(
                  conversationID: convo.conversationId,
                  groupId: convo.groupId,
                  localEpoch: ffiEpoch,
                  targetEpoch: Int(serverEpoch)
                )
                switch result {
                case .advanced, .noGap:
                  ffiEpoch = (try? await mlsClient.getEpoch(for: userDid, groupId: groupIdData)) ?? ffiEpoch
                  logger.info("✅ Epoch catch-up successful (new group): now at \(ffiEpoch)")
                case .needsDeferredRejoin(let failedEpoch, let reason):
                  // ⭐ FIX: Do NOT call handleRatchetDesync/forceRejoin from the sync
                  // hot-path. Flag the conversation for deferred recovery instead.
                  // This breaks the epoch inflation feedback loop between devices.
                  logger.warning("⚠️ Epoch catch-up failed (new group) - deferring rejoin for \(convo.conversationId.prefix(16))...")
                  logger.warning("   Local epoch \(failedEpoch) cannot process commits to reach server epoch \(serverEpoch)")
                  logger.warning("   Reason: \(reason)")
                  try? await markConversationNeedsRejoin(convo.conversationId)
                case .serverDataGap:
                  logger.warning("⚠️ Server data gap (new group) for \(convo.conversationId.prefix(16))... - epoch \(ffiEpoch) unbridgeable to \(serverEpoch)")
                  let auth = await mlsClient.epochAuthenticatorHex(
                    for: userDid, groupId: groupIdData)
                  if let recoveryManager = await mlsClient.recovery(for: userDid) {
                    await recoveryManager.recordServerDataGap(
                      convoId: convo.conversationId, epochAuthenticatorHex: auth)
                  }
                case .transientFetchError(let reason):
                  logger.warning("⚠️ Transient fetch error (new group) for \(convo.conversationId.prefix(16))...: \(reason) - will retry next sync")
                }
              }
            }
          } catch {
            // Group may not exist in FFI yet (e.g., before processing Welcome)
            logger.debug("Could not get FFI epoch for \(convo.conversationId.prefix(16)): \(error)")
            logger.debug("Using server epoch \(serverEpoch) as fallback")
          }

          groupStates[convo.groupId] = MLSGroupState(
            groupId: convo.groupId,
            convoId: convo.conversationId,
            epoch: ffiEpoch,  // Use FFI epoch if available, else server epoch
            members: Set(convo.members.map { $0.did.description }),
            knownServerEpoch: serverEpoch
          )
        } else if var state = groupStates[convo.groupId] {
          if state.epoch != convo.epoch {
            // ⭐ CRITICAL FIX: Verify epoch from FFI instead of trusting server
            // Note: userDid is guaranteed non-nil from auth check at start of function

            guard let groupIdData = Data(hexEncoded: convo.groupId) else {
              logger.error("Invalid group ID hex: \(convo.groupId)")
              continue  // Skip this conversation
            }

            let serverEpoch = UInt64(convo.epoch)
            var ffiEpoch = serverEpoch  // Default to server if FFI query fails

            // Try to get FFI epoch
            do {
              ffiEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)

              if serverEpoch != ffiEpoch {
                logger.warning("⚠️ EPOCH MISMATCH in syncWithServer (update):")
                logger.warning("   Server: \(serverEpoch), FFI: \(ffiEpoch)")

                // ⭐ Skip epoch catch-up for conversations already flagged needsRejoin.
                // Deferred recovery will handle these outside the sync hot-path.
                if await conversationNeedsRejoin(convo.conversationId) {
                  logger.info("⏭️ [SYNC] Skipping epoch catch-up for \(convo.conversationId.prefix(16))... - already flagged needsRejoin")
                } else
                // Attempt to catch up by processing missed commits
                if serverEpoch > ffiEpoch {
                  let result = await fetchAndProcessMissingCommits(
                    conversationID: convo.conversationId,
                    groupId: convo.groupId,
                    localEpoch: ffiEpoch,
                    targetEpoch: Int(serverEpoch)
                  )
                  switch result {
                  case .advanced, .noGap:
                    ffiEpoch = (try? await mlsClient.getEpoch(for: userDid, groupId: groupIdData)) ?? ffiEpoch
                    logger.info("✅ Epoch catch-up successful (update): now at \(ffiEpoch)")
                  case .needsDeferredRejoin(let failedEpoch, let reason):
                    // ⭐ FIX: Do NOT call handleRatchetDesync/forceRejoin from the sync
                    // hot-path. Flag the conversation for deferred recovery instead.
                    logger.warning("⚠️ Epoch catch-up failed (update) - deferring rejoin for \(convo.conversationId.prefix(16))...")
                    logger.warning("   Local epoch \(failedEpoch) cannot process commits to reach server epoch \(serverEpoch)")
                    logger.warning("   Reason: \(reason)")
                    try? await markConversationNeedsRejoin(convo.conversationId)
                  case .serverDataGap:
                    logger.warning("⚠️ Server data gap (update) for \(convo.conversationId.prefix(16))... - epoch \(ffiEpoch) unbridgeable to \(serverEpoch)")
                    let auth = await mlsClient.epochAuthenticatorHex(
                      for: userDid, groupId: groupIdData)
                    if let recoveryManager = await mlsClient.recovery(for: userDid) {
                      await recoveryManager.recordServerDataGap(
                        convoId: convo.conversationId, epochAuthenticatorHex: auth)
                    }
                  case .transientFetchError(let reason):
                    logger.warning("⚠️ Transient fetch error (update) for \(convo.conversationId.prefix(16))...: \(reason) - will retry next sync")
                  }
                }
              }
            } catch {
              logger.debug("Could not get FFI epoch for \(convo.conversationId.prefix(16)): \(error)")
              logger.debug("Using server epoch \(serverEpoch) as fallback")
            }

            state.epoch = ffiEpoch  // Use FFI epoch if available, else server epoch
            state.knownServerEpoch = UInt64(convo.epoch)
            state.members = Set(convo.members.map { $0.did.description })
            groupStates[convo.groupId] = state

            // Persist epoch to GRDB so DataSource reads are consistent
            do {
              try await storage.updateConversationEpoch(
                conversationID: convo.conversationId,
                currentUserDID: userDid,
                epoch: Int64(ffiEpoch),
                database: database
              )
            } catch {
              logger.warning("Failed to persist epoch during sync for \(convo.conversationId.prefix(16)): \(error.localizedDescription)")
            }

            // Notify epoch update
            notifyObservers(.epochUpdated(convo.conversationId, Int(ffiEpoch)))
          }
        }

        // Check confirmation tag divergence (only if server provides one and epochs match)
        if let serverTag = convo.confirmationTag,
           let groupIdData = Data(hexEncoded: convo.groupId),
           !(await conversationNeedsRejoin(convo.conversationId)) {
          let localTagData = try? await mlsClient.getConfirmationTag(for: userDid, groupId: groupIdData)
          let localTagB64 = localTagData?.base64EncodedString()
          if let localTagB64 = localTagB64, localTagB64 != serverTag.data.base64EncodedString() {
            logger.warning("⚠️ [SYNC] Tree divergence detected for \(convo.conversationId.prefix(16))... - local tag != server tag")
            try? await markConversationNeedsRejoin(convo.conversationId)
          }
        }

        // Initialize MLS group if needed
        if needsGroupInit {
          // Check if group exists locally via FFI
          guard let groupIdData = Data(hexEncoded: convo.groupId) else {
            logger.error("Invalid group ID format for \(convo.conversationId)")
            continue
          }

          // Note: userDid is guaranteed non-nil from auth check at start of function

          // Run blocking FFI call on background thread to avoid priority inversion
          // The Rust RwLock can cause priority inversion if called from main/UI thread
          let groupExists = await Task(priority: .background) {
            await mlsClient.groupExists(for: userDid, groupId: groupIdData)
          }.value

          if !groupExists {
            // ═══════════════════════════════════════════════════════════════════
            // FIX: Welcome First, External Commit Fallback
            // ═══════════════════════════════════════════════════════════════════
            // When user is missing local group state, ALWAYS try Welcome first.
            //
            // The previous logic checked if user was "already a member" and used
            // External Commit. This was WRONG because:
            // - When User A invites User B, User B is IMMEDIATELY in the member list
            // - So "already member" check was true for NEW invitations
            // - Result: New invitees got External Commit instead of Welcome
            // - This advanced the epoch, making all messages undecryptable
            //
            // Correct flow:
            // 1. Try Welcome (works for new invitations)
            // 2. On 404/410 (Welcome unavailable/expired), fall back to External Commit
            //    (this handles true device-sync scenarios where Welcome was already consumed)
            //
            // Note: initializeGroupFromWelcome() already handles 410 internally by
            // falling back to External Commit. We add 404 handling here for clarity.
            // ═══════════════════════════════════════════════════════════════════

            logger.info("🔄 [SYNC] Member missing local group for \(convo.conversationId.prefix(16))... - trying Welcome first")

            do {
              try await initializeGroupFromWelcome(convo: convo)
              logger.info("✅ [SYNC] Successfully joined via Welcome message")
            } catch let mlsApiError as MLSAPIError {
              // Check if Welcome is unavailable (device-sync scenario)
              if case .httpError(let code, _) = mlsApiError, code == 404 {
                // 404: No Welcome found - this is a true device-sync scenario
                // (Welcome was already consumed by another device, or creator)
                logger.info("📭 [SYNC] No Welcome available (HTTP 404) - this is device-sync, using External Commit")

                do {
                  let _ = try await mlsClient.joinByExternalCommit(
                    for: userDid, convoId: convo.conversationId)
                  logger.info("✅ [SYNC] Device successfully joined via External Commit")
                } catch {
                  logger.error(
                    "❌ [SYNC] External Commit failed for device-sync: \(error.localizedDescription)"
                  )
                  logger.error(
                    "   Conversation \(convo.conversationId.prefix(16))... will be unavailable")

                  // Non-fatal: conversation will retry on next sync
                  continue  // Skip this conversation
                }
              } else {
                // Other API error - log and skip
                logger.error(
                  "❌ CRITICAL: Failed to initialize MLS group for \(convo.conversationId): MLSAPIError - \(mlsApiError.localizedDescription)"
                )
                if case .invalidResponse(let message) = mlsApiError {
                  logger.error("  → Invalid response details: \(message)")
                }
                logger.error("❌ This conversation cannot be used - cryptographic join failed")
                logger.error("❌ Skipping conversation to prevent zombie group state")

                // ⭐ ZOMBIE CONVERSATION PREVENTION
                // Do NOT add this conversation to allConvos - it will be excluded from:
                // 1. In-memory conversations dictionary
                // 2. Database persistence (line 2456: persistConversationsToDatabase)
                // 3. UI display
                // This prevents a "zombie" conversation that appears functional but cannot decrypt/send messages
                continue
              }
            } catch {
              logger.error(
                "❌ CRITICAL: Failed to initialize MLS group for \(convo.conversationId): \(type(of: error)) - \(error.localizedDescription)"
              )
              logger.error("❌ This conversation cannot be used - cryptographic join failed")
              logger.error("❌ Skipping conversation to prevent zombie group state")

              // 🔄 RECOVERY: Check if this error warrants device-level recovery
              if let recoveryManager = await mlsClient.recovery(for: userDid) {
                let recovered = await recoveryManager.attemptRecoveryIfNeeded(
                  for: error,
                  userDid: userDid,
                  convoIds: [convo.conversationId]
                )
                if recovered {
                  logger.info(
                    "🔄 Silent recovery initiated for conversation \(convo.conversationId.prefix(16))")
                }
              }

              // ⭐ ZOMBIE CONVERSATION PREVENTION
              // Do NOT add this conversation to allConvos - it will be excluded from:
              // 1. In-memory conversations dictionary
              // 2. Database persistence (line 2456: persistConversationsToDatabase)
              // 3. UI display
              // This prevents a "zombie" conversation that appears functional but cannot decrypt/send messages
              continue
            }
          } else {
            logger.debug("Group already exists locally for conversation: \(convo.conversationId)")
          }
        }

        if needsGroupInit || fullSync {
          await catchUpMessagesIfNeeded(for: convo, force: needsGroupInit)
        }

        // Notify if new conversation
        if existingConvo == nil {
          notifyObservers(.conversationCreated(convo))
        }

        // ═══════════════════════════════════════════════════════════════════
        // YIELD: Give other operations a chance to acquire the permit
        // ═══════════════════════════════════════════════════════════════════
        // Without this yield, the sync loop can monopolize permit acquisition.
        // Each iteration does multiple FFI calls that acquire/release permits,
        // but re-acquire immediately. This starves user-initiated operations
        // like createGroup that are waiting for the permit.
        //
        // Task.yield() tells the Swift concurrency runtime to check if other
        // tasks are waiting, giving them a chance to run.
        // ═══════════════════════════════════════════════════════════════════
        await Task.yield()
      }

      // Persist conversations to local database
      try await persistConversationsToDatabase(allConvos)

      // Persist conversation members to local database
      try await persistMembersToDatabase(allConvos)

      // Reconcile database: delete conversations that exist locally but not on server.
      //
      // B7 (CRITICAL): build the set from `conversationId`, NOT `groupId`. The
      // two are equal at convo creation but DIVERGE after any server-side reset
      // (Phase 2 sweep, admin reset, quorum reset). Using `groupId` causes the
      // reconciler downstream — which compares against local convos by their
      // stable `conversationID` — to classify every reset convo as "removed from
      // server", verify FFI no longer has the (new) group, mark it zombie, and
      // force-delete it from the encrypted DB. User-visible effect: reset convos
      // vanish from the conversation list entirely. Spec ref:
      // `catbird-mls/CLAUDE.md` — "GroupId changes on reset; ConversationId
      // does not. Never confuse them."
      let serverConvoIDs = Set(allConvos.map { $0.conversationId })
      try await reconcileDatabase(with: serverConvoIDs)

      // Notify sync complete
      notifyObservers(.syncCompleted(allConvos.count))

      // CRITICAL FIX: Trigger orphan adoption immediately after sync
      // This ensures any reactions that arrived before their parent messages are processed now
      logger.info("🧹 [SYNC] Triggering immediate orphan adoption after sync")
      await adoptPendingOrphans(userDID: userDid)

      logger.info("Successfully synced \(allConvos.count) conversations")

      // Reset circuit breaker on success
      consecutiveSyncFailures = 0

      // ⭐ FIX: Schedule deferred recovery for any conversations flagged needsRejoin
      // during this sync pass. Without this, needsRejoin set mid-session would only
      // be consumed at next app launch (detectAndRejoinMissingConversations is one-shot).
      // Single-flight: skip if a previous recovery pass is still running.
      if deferredEpochRecoveryTask == nil {
        deferredEpochRecoveryTask = Task(priority: .utility) { [weak self] in
          guard let self else { return }
          defer { self.deferredEpochRecoveryTask = nil }
          do {
            try await self.runDeferredEpochRecovery()
          } catch {
            self.logger.warning("⚠️ [SYNC] Deferred epoch recovery failed: \(error.localizedDescription)")
          }
        }
      }

    } catch {
      if isSuspensionRelatedSyncError(error) {
        logger.info(
          "⏸️ [SYNC] Ignoring suspension-related sync failure without tripping circuit breaker: \(error.localizedDescription)"
        )
        return
      }

      // Increment circuit breaker counter
      consecutiveSyncFailures += 1
      logger.error(
        "Sync failed (\(self.consecutiveSyncFailures)/\(self.maxConsecutiveSyncFailures)): \(error.localizedDescription)"
      )

      // Check if we should trip the circuit breaker
      if consecutiveSyncFailures >= maxConsecutiveSyncFailures {
        syncPausedAt = Date()
        logger.error(
          "🚨 Circuit breaker tripped after \(self.consecutiveSyncFailures) consecutive sync failures"
        )
        logger.error(
          "   Sync will be paused for \(Int(self.syncPauseDuration))s to prevent resource exhaustion"
        )
        logger.error("   Error pattern: \(error.localizedDescription)")
      }

      notifyObservers(.syncFailed(error))
      throw MLSConversationError.syncFailed(error)
    }
  }

  // MARK: - Deferred Epoch Recovery

  /// Recover conversations flagged `needsRejoin` during a sync pass.
  /// Runs outside the sync hot-path with MLSRecoveryManager backoff to
  /// prevent the epoch inflation feedback loop.
  ///
  /// Key difference from `detectAndRejoinMissingConversations`: this method
  /// **deletes stale local group state** before attempting rejoin so that
  /// `attemptExternalCommitFallback` doesn't short-circuit on `groupExists`.
  internal func runDeferredEpochRecovery() async throws {
    guard !isShuttingDown, !Task.isCancelled else { return }
    guard let userDid = userDid else { return }
    guard await ensureActiveAccount(for: userDid, operation: "runDeferredEpochRecovery") else {
      return
    }

    // Phase 1: Handle needsReset conversations (group reset — fresh MLS group at epoch 0)
    // This MUST run before needsRejoin to prevent External Commits on diverged groups.
    let resetConvos = try await database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.currentUserDID == userDid)
        .filter(MLSConversationModel.Columns.needsReset == true)
        .fetchAll(db)
    }

    for convo in resetConvos {
      if isShuttingDown || Task.isCancelled { return }

      // Gate through MLSRecoveryManager backoff
      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        let shouldSkip = await recoveryManager.shouldSkipRejoin(convoId: convo.conversationID)
        if shouldSkip {
          logger.info(
            "⏭️ [EPOCH-RESET] Skipping \(convo.conversationID.prefix(16))... — backoff active")
          continue
        }
      }

      // Branch: recipient path (SSE GroupResetEvent staged newGroupId) vs
      // admin-initiator path (this device is the reset originator). Recipients
      // MUST NOT call the admin resetGroup endpoint or createGroup — that's
      // what caused the "non-admin orphans a fresh local group" bug in the
      // pre-§8.5 implementation (MLS_CLIENT_PROTOCOL.md §8.5 Phase 1).
      if let pendingNewGroupIdHex = convo.pendingNewGroupId {
        await runDeferredEpochRecoveryRecipient(
          convo: convo,
          userDid: userDid,
          pendingNewGroupIdHex: pendingNewGroupIdHex
        )
        continue
      }

      logger.warning(
        "🔄 [EPOCH-RESET] Starting automatic group reset for \(convo.conversationID.prefix(16))...")

      do {
        // 1. Create a fresh MLS group
        let newGroupIdData = try await mlsClient.createGroup(for: userDid)
        let newGroupIdHex = newGroupIdData.hexEncodedString()

        // 2. Get GroupInfo for the new group (may not be implemented yet)
        let groupInfoData = try? await mlsClient.getGroupInfo(for: userDid, groupId: newGroupIdData)
        let groupInfoBase64 = groupInfoData?.base64EncodedString()

        // 3. Call server resetGroup endpoint
        let result = try await apiClient.resetGroup(
          convoId: convo.conversationID,
          newGroupId: newGroupIdHex,
          cipherSuite: "MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519",
          groupInfo: groupInfoBase64,
          reason: "Automatic reset: epoch divergence exceeded threshold"
        )

        // 4. Update local state
        try await database.write { db in
          try db.execute(
            sql: """
                UPDATE MLSConversationModel
                SET needsReset = 0,
                    needsRejoin = 0,
                    epoch = 0,
                    pendingNewGroupId = NULL,
                    pendingResetGeneration = NULL,
                    updatedAt = ?
                WHERE conversationID = ? AND currentUserDID = ?;
            """, arguments: [Date(), convo.conversationID, userDid])
        }

        // 5. Clear old group state
        groupStates.removeValue(forKey: convo.conversationID)

        logger.warning(
          "✅ [EPOCH-RESET] Successfully reset \(convo.conversationID.prefix(16)) — newGroupId=\(newGroupIdHex.prefix(16)), generation=\(result.resetGeneration)"
        )

        // Clear recovery tracking
        if let recoveryManager = await mlsClient.recovery(for: userDid) {
          await recoveryManager.clearRejoinTracking(convoId: convo.conversationID)
        }

      } catch {
        let errorDesc = error.localizedDescription.lowercased()

        if errorDesc.contains("notadmin") {
          logger.error(
            "❌ [EPOCH-RESET] Not admin for \(convo.conversationID.prefix(16)) — cannot reset, clearing flag"
          )
          await clearConversationResetFlag(convo.conversationID)
        } else if errorDesc.contains("resetinprogress") {
          logger.info(
            "⏭️ [EPOCH-RESET] Reset already in progress for \(convo.conversationID.prefix(16))")
        } else {
          logger.error(
            "❌ [EPOCH-RESET] Failed for \(convo.conversationID.prefix(16)): \(error.localizedDescription)"
          )
          // Record failure for backoff. Fetch the old-group authenticator so
          // the server's A7 pyramid gets a real vote — the old group state
          // is still present locally on this admin-initiator failure path
          // (nothing has been deleted yet).
          if let recoveryManager = await mlsClient.recovery(for: userDid) {
            let authHex = await mlsClient.epochAuthenticatorHex(
              for: userDid, groupId: convo.groupID)
            await recoveryManager.recordFailedRejoin(
              convoId: convo.conversationID, epochAuthenticatorHex: authHex)
          }
        }
      }
    }

    // Phase 2: Handle needsRejoin conversations (external commit rejoin)
    let flaggedConvos = try await database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.currentUserDID == userDid)
        .filter(MLSConversationModel.Columns.needsRejoin == true)
        .filter(MLSConversationModel.Columns.needsReset == false)
        .fetchAll(db)
    }

    guard !flaggedConvos.isEmpty else { return }

    logger.info(
      "🔄 [DEFERRED-RECOVERY] Found \(flaggedConvos.count) conversation(s) needing deferred epoch recovery"
    )

    for convo in flaggedConvos {
      if isShuttingDown || Task.isCancelled { return }

      // Gate through MLSRecoveryManager backoff (30s → 2m → 10m → 1h)
      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        let shouldSkip = await recoveryManager.shouldSkipRejoin(convoId: convo.conversationID)
        if shouldSkip {
          logger.info(
            "⏭️ [DEFERRED-RECOVERY] Skipping \(convo.conversationID.prefix(16))... - backoff active"
          )
          continue
        }
      }

      guard beginRejoinAttempt(
        conversationID: convo.conversationID,
        source: "deferred-epoch-recovery"
      ) else {
        continue
      }

      // P1: Try commit catch-up BEFORE deleting local state and doing external commit.
      // If we have local MLS state at a non-zero epoch, we might be able to advance
      // by processing pending commits — avoiding an external commit entirely.
      let groupIdData = convo.groupID
      var caughtUp = false
      if await mlsClient.groupExists(for: userDid, groupId: groupIdData) {
        let localEpoch = (try? await mlsClient.getEpoch(for: userDid, groupId: groupIdData)) ?? 0
        if localEpoch > 0 {
          logger.info(
            "🔄 [DEFERRED-RECOVERY] Local state at epoch \(localEpoch) for \(convo.conversationID.prefix(16)) — trying commit catch-up before external commit"
          )
          let serverEpoch = convo.epoch
          if serverEpoch > Int64(localEpoch) {
            let groupIdHex = convo.conversationID  // conversationID is the hex group ID
            let result = await fetchAndProcessMissingCommits(
              conversationID: groupIdHex,
              groupId: groupIdHex,
              localEpoch: localEpoch,
              targetEpoch: Int(serverEpoch)
            )
            switch result {
            case .advanced, .noGap:
              logger.info(
                "✅ [DEFERRED-RECOVERY] Commit catch-up succeeded for \(convo.conversationID.prefix(16)) — no external commit needed"
              )
              await clearConversationRejoinFlag(convo.conversationID)
              endRejoinAttempt(conversationID: convo.conversationID)
              caughtUp = true
            default:
              logger.info(
                "⚠️ [DEFERRED-RECOVERY] Commit catch-up did not advance epoch — falling back to external commit"
              )
            }
          }
        }
      }

      if caughtUp { continue }

      // Capture epoch authenticator BEFORE deleting local state so the A7
      // reset-vote pyramid gets a real (non-`missing_authenticator`) vote if
      // the subsequent rejoin fails. Once `deleteGroup` runs, the FFI can no
      // longer produce one for this group.
      let preDeleteAuthHex: String? =
        await mlsClient.groupExists(for: userDid, groupId: groupIdData)
        ? await mlsClient.epochAuthenticatorHex(for: userDid, groupId: groupIdData)
        : nil

      // Delete stale local group state BEFORE attempting rejoin.
      // Without this, `attemptExternalCommitFallback` sees `groupExists == true`
      // and returns immediately without repairing the desynchronized ratchet state.
      if await mlsClient.groupExists(for: userDid, groupId: groupIdData) {
        logger.info(
          "🗑️ [DEFERRED-RECOVERY] Deleting stale local group state for \(convo.conversationID.prefix(16))..."
        )
        do {
          try await mlsClient.deleteGroup(for: userDid, groupId: groupIdData)
        } catch {
          logger.warning(
            "⚠️ [DEFERRED-RECOVERY] Failed to delete stale group: \(error.localizedDescription)"
          )
        }
        groupStates.removeValue(forKey: convo.conversationID)
      }

      let succeeded = await attemptRejoinWithWelcomeFallback(
        convoId: convo.conversationID,
        displayName: convo.conversationID,
        reason: "deferred epoch recovery (sync catch-up failed)",
        preDeleteAuthHex: preDeleteAuthHex
      )
      endRejoinAttempt(conversationID: convo.conversationID)

      // Record success/failure in MLSRecoveryManager for backoff tracking
      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        if succeeded {
          await recoveryManager.clearRejoinTracking(convoId: convo.conversationID)
        } else {
          await recoveryManager.recordFailedRejoin(
            convoId: convo.conversationID,
            epochAuthenticatorHex: preDeleteAuthHex
          )
        }
      }
    }
  }

  /// §8.5 Phase 1 recipient path: this device received a `GroupResetEvent`
  /// from the server naming `pendingNewGroupIdHex` as the replacement group.
  /// We fetch GroupInfo for the new group, run an External Commit to join it,
  /// and swap the local `MLSConversationModel.groupID` over to the new bytes.
  ///
  /// Safety:
  /// - Guarded by `beginRejoinAttempt` to prevent concurrent retries and
  ///   interleaving with Phase 2's External Commits.
  /// - Verifies the joined group's hex matches `pendingNewGroupIdHex` before
  ///   clearing the RESET_PENDING flag; on mismatch we leave the flag set so
  ///   we retry on the next sync tick.
  /// - Re-reads `pendingResetGeneration` after the External Commit lands; if a
  ///   newer `GroupResetEvent` advanced the generation mid-flight, we clear
  ///   only the old pointer (via `clearPendingReset`) and leave `needsReset`
  ///   set so the newer generation gets processed on the next pass.
  private func runDeferredEpochRecoveryRecipient(
    convo: MLSConversationModel,
    userDid: String,
    pendingNewGroupIdHex: String
  ) async {
    let observedGeneration = convo.pendingResetGeneration
    let convoId = convo.conversationID

    guard beginRejoinAttempt(
      conversationID: convoId,
      source: "group-reset-recipient"
    ) else {
      return
    }
    defer { endRejoinAttempt(conversationID: convoId) }

    logger.warning(
      "🔄 [EPOCH-RESET] Starting recipient rejoin for \(convoId.prefix(16)) — pendingNewGroupId=\(pendingNewGroupIdHex.prefix(16)), gen=\(observedGeneration.map(String.init) ?? "nil")"
    )

    // Capture the old-group authenticator BEFORE deleting so we can feed the
    // A7 reset-vote pyramid a real vote on failure paths below. After
    // `deleteGroup`, the FFI can no longer compute one for this groupID.
    let preDeleteAuthHex: String? =
      await mlsClient.groupExists(for: userDid, groupId: convo.groupID)
      ? await mlsClient.epochAuthenticatorHex(for: userDid, groupId: convo.groupID)
      : nil

    // Drop stale local MLS state for the old groupID before external-commiting
    // into the new one. The old group is being abandoned by server fiat; no
    // further commits will ever land against it.
    if await mlsClient.groupExists(for: userDid, groupId: convo.groupID) {
      do {
        try await mlsClient.deleteGroup(for: userDid, groupId: convo.groupID)
      } catch {
        logger.warning(
          "⚠️ [EPOCH-RESET] Failed to delete stale local group for \(convoId.prefix(16)): \(error.localizedDescription)"
        )
      }
      groupStates.removeValue(forKey: convoId)
    }

    do {
      // Uses convoId to fetch GroupInfo for the *new* group the server has
      // pinned this convo to. Returns the groupId bytes that actually landed.
      let newGroupIdData = try await mlsClient.joinByExternalCommit(
        for: userDid, convoId: convoId
      )
      let landedHex = newGroupIdData.hexEncodedString()

      guard landedHex.lowercased() == pendingNewGroupIdHex.lowercased() else {
        logger.error(
          "❌ [EPOCH-RESET] Recipient rejoin landed on \(landedHex.prefix(16)) but staged was \(pendingNewGroupIdHex.prefix(16)) — leaving RESET_PENDING set for retry"
        )
        if let recoveryManager = await mlsClient.recovery(for: userDid) {
          await recoveryManager.recordFailedRejoin(
            convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
        }
        return
      }

      let landedEpoch: Int64
      do {
        landedEpoch = Int64(try await mlsClient.getEpoch(for: userDid, groupId: newGroupIdData))
      } catch {
        logger.warning(
          "⚠️ [EPOCH-RESET] Could not read epoch for \(convoId.prefix(16)) after rejoin: \(error.localizedDescription)"
        )
        landedEpoch = 0
      }

      // Concurrency guard: if a newer GroupResetEvent advanced the generation
      // between the DB read at top-of-loop and now, we must NOT fully clear
      // RESET_PENDING — the newer generation points at a different group and
      // still needs to be processed.
      let now = Date()
      try await database.write { db in
        let currentGeneration = try MLSConversationResetSQL.loadPendingResetGeneration(
          db: db, conversationID: convoId, currentUserDID: userDid
        )
        let generationAdvanced: Bool = {
          switch (currentGeneration, observedGeneration) {
          case let (.some(current), .some(observed)):
            return current > observed
          case (.some, .none):
            return true
          default:
            return false
          }
        }()

        if generationAdvanced {
          // Newer event arrived; clear only the stale pointer and leave
          // needsReset=1 so the next sync tick handles the fresh generation.
          try MLSConversationResetSQL.clearPendingReset(
            db: db, conversationID: convoId, currentUserDID: userDid, now: now
          )
        } else {
          try MLSConversationResetSQL.applyRecipientResetSuccess(
            db: db,
            conversationID: convoId,
            currentUserDID: userDid,
            newGroupID: newGroupIdData,
            newEpoch: landedEpoch,
            now: now
          )
        }
      }

      groupStates.removeValue(forKey: convoId)

      logger.warning(
        "✅ [EPOCH-RESET] Recipient rejoin succeeded for \(convoId.prefix(16)) → newGroupId=\(landedHex.prefix(16)), epoch=\(landedEpoch)"
      )

      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        await recoveryManager.clearRejoinTracking(convoId: convoId)
      }

    } catch {
      logger.error(
        "❌ [EPOCH-RESET] Recipient rejoin failed for \(convoId.prefix(16)): \(error.localizedDescription)"
      )

      // First-responder bootstrap (spec §8.5):
      // After auto-reset, the server clears `group_info` to NULL. External
      // Commit needs GroupInfo to land — so EC will keep failing until
      // SOMEONE bootstraps the empty group. If a fresh probe confirms
      // GroupInfo is absent, race to be the bootstrap winner. The server
      // returns 409 `AlreadyBootstrapped` to losers, who then fall back to
      // the winner's Welcome on the next deferred-recovery loop.
      let groupInfoMissing = await isGroupInfoMissing(convoId: convoId)
      if groupInfoMissing {
        await attemptFirstResponderBootstrap(
          convoId: convoId,
          userDid: userDid,
          pendingNewGroupIdHex: pendingNewGroupIdHex,
          observedGeneration: observedGeneration,
          preDeleteAuthHex: preDeleteAuthHex
        )
        return
      }

      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        await recoveryManager.recordFailedRejoin(
          convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
      }
    }
  }

  /// Probe whether GroupInfo is absent on the server for `convoId`.
  ///
  /// Used after an External Commit failure to decide whether to attempt
  /// first-responder bootstrap (GroupInfo absent → empty post-reset group →
  /// bootstrap) vs. record the EC failure for the next backoff cycle
  /// (GroupInfo present → transient DS error). Returns `true` only when the
  /// server explicitly signals "no GroupInfo for this convo"; transient
  /// network failures return `false` so we don't bootstrap on top of a
  /// healthy group whose GroupInfo we just couldn't fetch.
  ///
  /// **Visibility note (B9):** promoted from `private` to `internal` so
  /// `ensureGroupInitialized` (the synchronous open-conversation path) can
  /// share the same probe as the async deferred-recovery loop.
  internal func isGroupInfoMissing(convoId: String) async -> Bool {
    do {
      // B12: single-shot probe (maxRetries=1). The default 3-attempt
      // exponential-backoff retry adds ~7s of latency hammering the server
      // with the same 404 each call site has already gotten. We just need
      // a single yes/no answer to decide whether to bootstrap.
      _ = try await apiClient.getGroupInfo(convoId: convoId, maxRetries: 1)
      return false
    } catch let error as MLSAPIError {
      switch error {
      case .invalidResponse:
        // `MLSAPIClient.getGroupInfo` throws `.invalidResponse(message: "No GroupInfo available")`
        // when the server returns 200 with null/empty payload, which is the
        // post-auto-reset signal.
        return true
      case .httpError(let statusCode, _) where statusCode == 404:
        return true
      default:
        return false
      }
    } catch let networkError as NetworkError {
      // B12: `MLSAPIClient.getGroupInfo` rethrows `MLSAPIError` directly but
      // catches every other error (incl. `Petrel.NetworkError.responseError`)
      // and retries until exhaustion, then throws the underlying NetworkError
      // — NOT wrapped as `MLSAPIError.httpError`. Without this catch arm,
      // server 404s on GroupInfo fall through to the generic catch below
      // returning `false`, which means `attemptFirstResponderBootstrap` never
      // fires from B9 (creator branch) or B11 (rejoin path) and reset convos
      // stay stuck forever.
      switch networkError {
      case .responseError(let code):
        return code == 404
      case .serverError(let code, _):
        return code == 404
      default:
        return false
      }
    } catch {
      return false
    }
  }

  /// First-responder bootstrap branch (spec §8.5).
  ///
  /// Build a local MLS group at the staged `pendingNewGroupId` and POST
  /// `bootstrapResetGroup` to populate the empty post-reset row in place.
  /// On 200 the row gains GroupInfo and a Welcome message; on 409
  /// `AlreadyBootstrapped` the race is lost and the local pre-bootstrap
  /// state is discarded so the next loop joins via the winner's Welcome.
  ///
  /// **Visibility note (B9):** promoted from `private` to `internal` so the
  /// synchronous open-conversation path (`ensureGroupInitialized` in
  /// MLSConversationManager+Messaging.swift) can also fire the bootstrap
  /// when the user opens a stuck-and-reset convo. Previously only the
  /// async deferred-recovery loop could trigger it, but it was blocked by
  /// the per-convo rejoin lock that `ensureGroupInitialized` had already
  /// claimed — so reset convos never got bootstrapped until the user
  /// quit and relaunched, and even then only via the deferred path.
  /// CLIENT E: scan the bootstrap-pending table on first sync per session
  /// and tear down any half-staged OpenMLS groups whose `confirmCommit`
  /// never landed (process kill, account switch mid-flight, crash). The
  /// surviving local group sits at epoch 0 with a phantom group_id —
  /// `getEpoch` returns GroupNotFound forever and `sendMessage` 409s.
  /// Cleanup deletes the local group and the marker; downstream sync
  /// will re-discover the convo (Welcome from sibling winner, or fresh
  /// bootstrap retry if we still need to be the first responder).
  internal func cleanUpHalfStagedBootstrapGroups() async {
    guard let userDid = userDid else { return }

    let markers: [MLSBootstrapPendingModel]
    do {
      markers = try await MLSBootstrapPendingModel.fetchAll(
        currentUserDID: userDid, database: database)
    } catch {
      logger.warning(
        "🪝 [CLIENT E] Failed to fetch bootstrap-pending markers: \(error.localizedDescription)"
      )
      return
    }

    guard !markers.isEmpty else {
      logger.debug("🪝 [CLIENT E] No half-staged bootstrap markers to clean up")
      return
    }

    logger.warning(
      "🪝 [CLIENT E] Cleaning up \(markers.count) half-staged bootstrap group(s) for \(userDid.prefix(20))"
    )

    for marker in markers {
      guard let groupIdData = Data(hexEncoded: marker.groupIdHex) else {
        logger.warning(
          "🪝 [CLIENT E] Marker for \(marker.conversationID.prefix(16)) has invalid hex groupId — clearing without delete"
        )
        await MLSBootstrapPendingModel.clear(
          conversationID: marker.conversationID,
          currentUserDID: userDid,
          database: database
        )
        continue
      }

      // Delete the OpenMLS group state. Idempotent — if confirmCommit
      // had landed and the marker simply leaked, deleteGroup will surface
      // an error which we tolerate (the convo's manifest entry already
      // points at a valid group).
      let exists = await mlsClient.groupExists(for: userDid, groupId: groupIdData)
      if exists {
        try? await mlsClient.deleteGroup(for: userDid, groupId: groupIdData)
        logger.warning(
          "🪝 [CLIENT E] Deleted half-staged group \(marker.groupIdHex.prefix(16)) for convo \(marker.conversationID.prefix(16))"
        )
      } else {
        logger.info(
          "🪝 [CLIENT E] Half-staged group \(marker.groupIdHex.prefix(16)) for convo \(marker.conversationID.prefix(16)) already absent — clearing marker only"
        )
      }

      groupStates.removeValue(forKey: marker.conversationID)

      // Always clear the marker so it doesn't fire again next boot.
      await MLSBootstrapPendingModel.clear(
        conversationID: marker.conversationID,
        currentUserDID: userDid,
        database: database
      )
    }
  }

  internal func attemptFirstResponderBootstrap(
    convoId: String,
    userDid: String,
    pendingNewGroupIdHex: String,
    observedGeneration: Int64?,
    preDeleteAuthHex: String?
  ) async {
    // B15 (Half 1): per-convo dedup. Two trigger sources (deferred-epoch
    // recovery loop + missing-convo reconciler) can both fire for the same
    // convo. The losing race-mate would otherwise stage a parallel
    // create+commit and, on its inevitable failure, delete the SAME group
    // the winner just bootstrapped. Skip if a sibling is already in flight.
    let claimed: Bool = bootstrappingConvos.withLock { set in
      if set.contains(convoId) { return false }
      set.insert(convoId)
      return true
    }
    guard claimed else {
      logger.info(
        "⏳ [BOOTSTRAP] Skipping - another attempt in flight for \(convoId.prefix(16))"
      )
      return
    }
    defer {
      bootstrappingConvos.withLock { _ = $0.remove(convoId) }
    }

    logger.warning(
      "🚀 [BOOTSTRAP] First-responder bootstrap for \(convoId.prefix(16)) → \(pendingNewGroupIdHex.prefix(16))"
    )

    guard let pendingGroupIdData = Data(hexEncoded: pendingNewGroupIdHex) else {
      logger.error("❌ [BOOTSTRAP] Invalid pendingNewGroupId hex for \(convoId.prefix(16))")
      return
    }

    // Phase 0 Q3: groupResetEvent carries no roster, fetch fresh from server.
    let convoView: BlueCatbirdMlsChatDefs.ConvoView
    do {
      guard let fetched = try await apiClient.getConversation(convoId: convoId) else {
        logger.error(
          "❌ [BOOTSTRAP] getConversation returned nil for \(convoId.prefix(16)) — leaving RESET_PENDING set"
        )
        if let recoveryManager = await mlsClient.recovery(for: userDid) {
          await recoveryManager.recordFailedRejoin(
            convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
        }
        return
      }
      convoView = fetched
    } catch {
      logger.error(
        "❌ [BOOTSTRAP] getConversation failed for \(convoId.prefix(16)): \(error.localizedDescription)"
      )
      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        await recoveryManager.recordFailedRejoin(
          convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
      }
      return
    }

    let selfDidLower = userDid.lowercased()
    let memberDids: [DID] = convoView.members.map { $0.did }
    let otherMemberDids: [DID] = memberDids.filter {
      $0.description.lowercased() != selfDidLower
    }
    let bootstrapCipherSuite = convoView.cipherSuite

    // Build the local MLS group at the predetermined groupId so all
    // bootstrap candidates converge on the same MLS GroupId.
    //
    // B15 (Half 2): track whether THIS attempt actually created the group.
    // If a sibling External Commit / bootstrap already populated local state
    // for `pendingGroupIdData`, we must NOT delete the group on a later
    // staging failure — that group now belongs to the sibling and the
    // server has bound state to it. `wasGroupCreatedByUs` gates every
    // `deleteGroup` cleanup below; `discardPending(handle:)` remains safe
    // (local-only) and is called regardless.
    var wasGroupCreatedByUs = false

    // 🪝 [CLIENT E] Always clear the bootstrap-pending marker on function
    // exit. Set after createGroupWithId; cleared here regardless of which
    // path (200 success, 409 winner, 409 loser, irrecoverable, rollback)
    // we exit through. The boot scan ignores rows that survive crashes —
    // those are the genuine half-staged groups we need to clean.
    let bootstrapPendingDB = self.database
    defer {
      Task { [bootstrapPendingDB] in
        await MLSBootstrapPendingModel.clear(
          conversationID: convoId,
          currentUserDID: userDid,
          database: bootstrapPendingDB
        )
      }
    }

    if await mlsClient.groupExists(for: userDid, groupId: pendingGroupIdData) {
      logger.info(
        "ℹ️ [BOOTSTRAP] Group \(pendingNewGroupIdHex.prefix(16)) already exists locally for \(convoId.prefix(16)) — sibling won the create race; reusing without delete-on-failure"
      )
    } else {
      do {
        let groupConfig = configuration.toFFI(groupName: nil, groupDescription: nil)
        _ = try await mlsClient.createGroupWithId(
          for: userDid, groupId: pendingGroupIdData, configuration: groupConfig)
        wasGroupCreatedByUs = true
        // 🪝 [CLIENT E] Atomic bootstrap-pending marker. Recorded
        // immediately after the FFI's durable createGroupWithId returns
        // and cleared after confirmCommit (or on every rollback path).
        // Boot-time scan will clean up half-staged groups whose
        // confirmCommit never landed (process kill, account switch,
        // crash between createGroupWithId and confirmCommit).
        try? await MLSBootstrapPendingModel.record(
          conversationID: convoId,
          groupIdHex: pendingNewGroupIdHex,
          currentUserDID: userDid,
          database: database
        )
      } catch {
        // `MLSClient.createGroupWithId` swallows the underlying `MlsError`
        // variant and re-throws as bare `MLSError.operationFailed`, so we
        // can't discriminate "already exists" from a genuine create
        // failure off the catch alone. Re-probe: if the group is now
        // present, a sibling raced ahead between our existence check and
        // our create call — treat as if we found it on entry.
        if await mlsClient.groupExists(for: userDid, groupId: pendingGroupIdData) {
          logger.warning(
            "⚠️ [BOOTSTRAP] createGroupWithId failed for \(convoId.prefix(16)) but group now exists — sibling won the race; continuing without delete-on-failure"
          )
        } else {
          logger.error(
            "❌ [BOOTSTRAP] createGroupWithId failed for \(convoId.prefix(16)): \(error.localizedDescription)"
          )
          if let recoveryManager = await mlsClient.recovery(for: userDid) {
            await recoveryManager.recordFailedRejoin(
              convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
          }
          return
        }
      }
    }

    // Stage the add-members commit + Welcome in one shot.
    var welcomeData: Data?
    var hashEntries: [BlueCatbirdMlsChatBootstrapResetGroup.KeyPackageHashEntry] = []
    var stagedHandle: FfiStagedCommitHandle?
    var bootstrapTargetEpoch: UInt64 = 0
    if !otherMemberDids.isEmpty {
      do {
        let (keyPackages, missing) = try await apiClient.getKeyPackages(
          dids: otherMemberDids, forceRefresh: false)
        if let missing, !missing.isEmpty {
          logger.error(
            "❌ [BOOTSTRAP] Missing key packages for \(missing.count) member(s) in \(convoId.prefix(16))"
          )
          // B15 (Half 2): only delete if WE created this group. If a
          // sibling External Commit / bootstrap got here first, the group
          // is theirs — destroying it would undo their successful join.
          if wasGroupCreatedByUs {
            try? await mlsClient.deleteGroup(for: userDid, groupId: pendingGroupIdData)
          } else {
            logger.info(
              "🛡️ [BOOTSTRAP] Skipping deleteGroup for \(convoId.prefix(16)) — group not owned by this attempt"
            )
          }
          if let recoveryManager = await mlsClient.recovery(for: userDid) {
            await recoveryManager.recordFailedRejoin(
              convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
          }
          return
        }

        let selectedPackages = try await selectKeyPackages(
          for: otherMemberDids, from: keyPackages, userDid: userDid)
        hashEntries = selectedPackages.map {
          BlueCatbirdMlsChatBootstrapResetGroup.KeyPackageHashEntry(
            did: $0.did, hash: $0.hash)
        }
        let keyPackageData = selectedPackages.map { $0.data }
        let memberStrings = otherMemberDids.map { $0.description }
        let plan = try await mlsClient.stageCommit(
          for: userDid,
          conversationId: pendingNewGroupIdHex,
          kind: .addMembers(memberDids: memberStrings, keyPackages: keyPackageData)
        )
        welcomeData = plan.welcomeBytes
        stagedHandle = plan.handle
        bootstrapTargetEpoch = plan.targetEpoch
      } catch {
        logger.error(
          "❌ [BOOTSTRAP] Member staging failed for \(convoId.prefix(16)): \(error.localizedDescription)"
        )
        // B15 (Half 2): only delete if WE created this group.
        if wasGroupCreatedByUs {
          try? await mlsClient.deleteGroup(for: userDid, groupId: pendingGroupIdData)
        } else {
          logger.info(
            "🛡️ [BOOTSTRAP] Skipping deleteGroup for \(convoId.prefix(16)) after staging failure — group not owned by this attempt"
          )
        }
        if let recoveryManager = await mlsClient.recovery(for: userDid) {
          await recoveryManager.recordFailedRejoin(
            convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
        }
        return
      }
    }

    // Export the GroupInfo to attach to the bootstrap request.
    let groupInfoBytes: Data
    do {
      groupInfoBytes = try await mlsClient.exportLocalGroupInfo(
        for: userDid, groupId: pendingGroupIdData)
    } catch {
      logger.error(
        "❌ [BOOTSTRAP] exportLocalGroupInfo failed for \(convoId.prefix(16)): \(error.localizedDescription)"
      )
      if let handle = stagedHandle {
        await mlsClient.discardPending(for: userDid, handle: handle)
      }
      // B15 (Half 2): only delete if WE created this group.
      if wasGroupCreatedByUs {
        try? await mlsClient.deleteGroup(for: userDid, groupId: pendingGroupIdData)
      } else {
        logger.info(
          "🛡️ [BOOTSTRAP] Skipping deleteGroup for \(convoId.prefix(16)) after export failure — group not owned by this attempt"
        )
      }
      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        await recoveryManager.recordFailedRejoin(
          convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
      }
      return
    }

    // POST bootstrapResetGroup. On 200 the server populates the post-reset
    // row in place; on 409 the race is lost.
    do {
      let bootstrapped = try await apiClient.bootstrapResetGroup(
        originalConvoId: convoId,
        newGroupId: pendingNewGroupIdHex,
        cipherSuite: bootstrapCipherSuite,
        groupInfo: groupInfoBytes,
        members: memberDids,
        welcomeMessage: welcomeData,
        keyPackageHashes: hashEntries.isEmpty ? nil : hashEntries,
        currentEpoch: bootstrapTargetEpoch > 0 ? Int(bootstrapTargetEpoch) : nil
      )

      // Confirm any staged add-members commit so local epoch matches server.
      if let handle = stagedHandle {
        let serverEpoch = UInt64(bootstrapped.epoch)
        do {
          _ = try await mlsClient.confirmCommit(
            for: userDid, handle: handle, serverEpoch: serverEpoch)
        } catch {
          logger.error(
            "❌ [BOOTSTRAP] confirmCommit failed after server 200 for \(convoId.prefix(16)): \(error.localizedDescription)"
          )
          await mlsClient.discardPending(for: userDid, handle: handle)
          // Server already accepted; surface to recovery tracker but do NOT
          // delete the local group — server state is now bound to it.
          if let recoveryManager = await mlsClient.recovery(for: userDid) {
            await recoveryManager.recordFailedRejoin(
              convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
          }
          return
        }
      }

      // Read FFI-actual epoch (post-confirm) for the persistence write.
      let landedEpoch: Int64
      do {
        landedEpoch = Int64(
          try await mlsClient.getEpoch(for: userDid, groupId: pendingGroupIdData))
      } catch {
        landedEpoch = Int64(bootstrapped.epoch)
      }

      // 🛡️ [CLIENT D] Publish post-confirm groupInfo. The bootstrap RPC's
      // groupInfo arg captured the PRE-confirm group at epoch 0; after
      // confirmCommit the local group advances to epoch 1+, and no other
      // path uploads the post-commit groupInfo. Without this publish,
      // server.active_crypto_session.group_info stays NULL forever, which
      // means subsequent External Commit attempts on this convo all 404
      // (the production state from team-lead's 4b2cdbaa diagnostic).
      // Asymmetric failure handling: this is the 200 path so the server
      // already committed our bootstrap — log warning but DO NOT roll
      // back. The publish is best-effort polish on top of the win.
      do {
        try await mlsClient.publishGroupInfo(
          for: userDid, convoId: convoId, groupId: pendingGroupIdData,
          knownServerEpoch: UInt64(landedEpoch)
        )
        logger.info(
          "🛡️ [CLIENT D] Published post-confirm groupInfo at epoch=\(landedEpoch) for \(convoId.prefix(16)) (200 winner)"
        )
      } catch {
        logger.warning(
          "🛡️ [CLIENT D] Post-confirm publishGroupInfo failed for \(convoId.prefix(16)) on 200 path: \(error.localizedDescription) — keeping local win, server-side groupInfo will need backfill"
        )
      }

      let now = Date()
      do {
        try await database.write { db in
          let currentGeneration = try MLSConversationResetSQL.loadPendingResetGeneration(
            db: db, conversationID: convoId, currentUserDID: userDid)
          let generationAdvanced: Bool = {
            switch (currentGeneration, observedGeneration) {
            case let (.some(current), .some(observed)):
              return current > observed
            case (.some, .none):
              return true
            default:
              return false
            }
          }()

          if generationAdvanced {
            try MLSConversationResetSQL.clearPendingReset(
              db: db, conversationID: convoId, currentUserDID: userDid, now: now)
          } else {
            try MLSConversationResetSQL.applyRecipientResetSuccess(
              db: db,
              conversationID: convoId,
              currentUserDID: userDid,
              newGroupID: pendingGroupIdData,
              newEpoch: landedEpoch,
              now: now
            )
          }
        }

        groupStates.removeValue(forKey: convoId)
        conversations[convoId] = bootstrapped

        logger.warning(
          "✅ [BOOTSTRAP] Won race for \(convoId.prefix(16)) → newGroupId=\(pendingNewGroupIdHex.prefix(16)), epoch=\(landedEpoch)"
        )
        if let recoveryManager = await mlsClient.recovery(for: userDid) {
          await recoveryManager.clearRejoinTracking(convoId: convoId)
        }
      } catch {
        logger.error(
          "❌ [BOOTSTRAP] DB write failed after server 200 for \(convoId.prefix(16)): \(error.localizedDescription)"
        )
      }

    } catch let error as MLSAPIError {
      switch error {
      case .alreadyBootstrapped:
        // 409 has TWO causes that look identical from the wire:
        //   (a) sibling beat us to POST → we lost the race, server holds
        //       sibling's group_info, a Welcome was inserted for us.
        //   (b) OUR earlier POST committed the transaction but the response
        //       was lost (network 5xx, dropped TLS, etc.) → we retried,
        //       server says "already done", but the bootstrap that won was
        //       OURS — there is no Welcome for us, and the local stagedHandle
        //       is the one that produced the server's group_info.
        // Disambiguate via getWelcome: if a Welcome targeted at us exists,
        // (a) is true; otherwise (b) is true and we should commit our staged
        // state instead of wiping it (B16).
        logger.warning(
          "🥈 [BOOTSTRAP] 409 for \(convoId.prefix(16)) — disambiguating winner-vs-loser via Welcome lookup"
        )
        var welcomeForUs: Bool = false
        do {
          _ = try await apiClient.getWelcome(convoId: convoId)
          welcomeForUs = true
        } catch let mlsErr as MLSAPIError {
          if case .invalidResponse = mlsErr {
            welcomeForUs = false
          } else {
            // Treat unknown errors as "loser" (the safer default — we won't
            // accidentally double-commit, just retry next loop).
            logger.warning(
              "⚠️ [BOOTSTRAP] getWelcome errored during 409 disambiguation for \(convoId.prefix(16)): \(mlsErr.errorDescription ?? "?") — defaulting to loser path"
            )
            welcomeForUs = true
          }
        } catch {
          logger.warning(
            "⚠️ [BOOTSTRAP] getWelcome errored during 409 disambiguation for \(convoId.prefix(16)): \(error.localizedDescription) — defaulting to loser path"
          )
          welcomeForUs = true
        }

        if welcomeForUs {
          // Race lost. The winner's Welcome will be consumed by the
          // deferred-recovery loop. Discard our staging so it doesn't conflict.
          logger.warning(
            "🥈 [BOOTSTRAP] Race lost for \(convoId.prefix(16)) (Welcome present) — clearing local pre-bootstrap state, awaiting Welcome"
          )
          if let handle = stagedHandle {
            await mlsClient.discardPending(for: userDid, handle: handle)
          }
          if wasGroupCreatedByUs {
            try? await mlsClient.deleteGroup(for: userDid, groupId: pendingGroupIdData)
          } else {
            logger.info(
              "🛡️ [BOOTSTRAP] Skipping deleteGroup on 409 for \(convoId.prefix(16)) — group not owned by this attempt"
            )
          }
        } else {
          // (b) — our prior POST committed; this 409 is our own retry
          // hitting our own success. Confirm the staged commit and persist
          // exactly as the 200 path would. Without this branch we destroy
          // the local group state that the server is now bound to and the
          // user gets stuck with `GroupNotFound` on every send.
          //
          // 🛡️ [CLIENT D] Verify-after-confirm: absence of welcome alone
          // does NOT prove we won. A sibling could have won and the server
          // simply never had a welcome for us. We must verify that the
          // server-bound group_id matches our pendingNewGroupIdHex BEFORE
          // ratcheting, and verify the post-confirm epoch matches the
          // server's groupInfo AFTER ratcheting.
          logger.warning(
            "🏆 [BOOTSTRAP] 409 with no Welcome for us → tentatively WE WON (lost-response recovery) for \(convoId.prefix(16)); verifying"
          )
          let serverConvo: BlueCatbirdMlsChatDefs.ConvoView?
          do {
            serverConvo = try await apiClient.getConversation(convoId: convoId)
          } catch {
            serverConvo = nil
          }

          // Pre-confirm groupId verification: cheapest discriminator. If
          // the server's bound group_id is NOT our pendingNewGroupIdHex,
          // the sibling won and we must take the loser path (regardless
          // of welcome presence — server B may not have inserted one yet).
          if let serverConvo {
            let serverGroupIdHex = serverConvo.groupId
            if !serverGroupIdHex.isEmpty
              && serverGroupIdHex.lowercased() != pendingNewGroupIdHex.lowercased()
            {
              logger.warning(
                "🛡️ [CLIENT D] BOOTSTRAP race lost (verified): serverGroupId=\(serverGroupIdHex.prefix(16)) ≠ pendingNewGroupId=\(pendingNewGroupIdHex.prefix(16)) — rolling back to loser path"
              )
              if let handle = stagedHandle {
                await mlsClient.discardPending(for: userDid, handle: handle)
              }
              if wasGroupCreatedByUs {
                try? await mlsClient.deleteGroup(
                  for: userDid, groupId: pendingGroupIdData)
              }
              if let recoveryManager = await mlsClient.recovery(for: userDid) {
                await recoveryManager.clearRejoinTracking(convoId: convoId)
              }
              return
            }
          }

          if let handle = stagedHandle {
            let serverEpoch = UInt64(serverConvo?.epoch ?? 1)
            do {
              _ = try await mlsClient.confirmCommit(
                for: userDid, handle: handle, serverEpoch: serverEpoch)
            } catch {
              // CRITICAL: confirmCommit lost (likely a concurrent context
              // reload wiped the in-memory staged-commit map between stage and
              // confirm). The local group at `pendingGroupIdData` still exists
              // BUT IS AT THE WRONG EPOCH — createGroupWithId puts it at
              // epoch 0; the staged addMembers commit (which would have
              // advanced to epoch 1) was never applied. The server, however,
              // is at epoch 1 because the bootstrap committed there.
              //
              // We CANNOT just persist as success — the local group's epoch-0
              // ratchet keys are incompatible with anything that comes through
              // the server (which sees epoch 1). Sends would encrypt at
              // epoch 0 and other members (joined via Welcome at epoch 1)
              // could not decrypt them. This was the original symptom: both
              // accounts could send but neither received.
              //
              // The bootstrap addMembers commit is delivered ONLY via the
              // Welcome — it is NOT stored on the wire as type=commit — so
              // we cannot recover by fetching the commit and processing it.
              // The only recovery is to delete the half-baked local group and
              // rejoin via External Commit (which advances server epoch and
              // triggers a commit fanout that other members will process).
              logger.error(
                "❌ [BOOTSTRAP] confirmCommit failed during 409-winner recovery for \(convoId.prefix(16)): \(error.localizedDescription) — local group is stuck at pre-confirm epoch; deleting and queuing External Commit rejoin"
              )
              try? await mlsClient.deleteGroup(for: userDid, groupId: pendingGroupIdData)
              if let recoveryManager = await mlsClient.recovery(for: userDid) {
                // Don't increment failure counter — this is an in-band
                // recovery, not a true failure. Just clear so next sync sees
                // it as eligible for External Commit join.
                await recoveryManager.clearRejoinTracking(convoId: convoId)
              }
              return
            }
          }
          let landedEpoch: Int64
          do {
            landedEpoch = Int64(
              try await mlsClient.getEpoch(for: userDid, groupId: pendingGroupIdData))
          } catch {
            landedEpoch = Int64(serverConvo?.epoch ?? 1)
          }

          // 🛡️ [CLIENT D] Publish-then-verify backstop. The server's
          // active crypto_session for a freshly-bootstrapped convo
          // typically has group_info=NULL — confirmCommit advanced our
          // local ratchet but no one has published the post-commit
          // groupInfo yet. The actual proof of winning is whether the
          // server accepts OUR publish (the alternative — sibling won
          // and is the rightful publisher — would either reject our
          // publish via 410 groupReset, or our re-fetch would surface
          // a different groupInfo than what we just sent).
          //
          // 1. Publish our post-confirm groupInfo.
          // 2. Re-fetch (with one retry to absorb stale read).
          // 3. Verify epoch matches AND non-NULL groupInfo present.
          //
          // 410 Gone (server contract: locked with SERVER B PR #11) is
          // the unambiguous "you lost / mid-reset" signal. Any other
          // failure or mismatch is treated as ambiguous → roll back.
          var verifiedWin = false
          var detectedGroupReset = false
          do {
            try await mlsClient.publishGroupInfo(
              for: userDid, convoId: convoId, groupId: pendingGroupIdData,
              knownServerEpoch: UInt64(landedEpoch)
            )
            logger.info(
              "🛡️ [CLIENT D] Published groupInfo at epoch=\(landedEpoch) for \(convoId.prefix(16)); verifying server accepted"
            )

            // Re-fetch with one retry to absorb stale-read race.
            var attempt = 0
            while attempt < 2 {
              do {
                let (_, serverInfoEpoch, _) = try await apiClient.getGroupInfo(
                  convoId: convoId, maxRetries: 1)
                if Int64(serverInfoEpoch) == landedEpoch {
                  verifiedWin = true
                  logger.info(
                    "🛡️ [CLIENT D] Post-publish verify OK for \(convoId.prefix(16)): server epoch=\(serverInfoEpoch) matches landedEpoch=\(landedEpoch)"
                  )
                  break
                } else {
                  logger.warning(
                    "🛡️ [CLIENT D] Post-publish epoch mismatch for \(convoId.prefix(16)): server=\(serverInfoEpoch) vs local=\(landedEpoch) — sibling won"
                  )
                  break
                }
              } catch let mlsErr as MLSAPIError {
                // 410 from getGroupInfo surfaces as MLSAPIError.httpError.
                // 410 is the SERVER B "groupReset" signal — convo is
                // mid-reset or in a superseded crypto session.
                if case .httpError(let code, _) = mlsErr, code == 410 {
                  detectedGroupReset = true
                  logger.warning(
                    "🛡️ [CLIENT D] Post-publish saw 410 groupReset for \(convoId.prefix(16)) — server has new active session; rolling back to recipient-rejoin"
                  )
                  break
                }
                if attempt == 0 {
                  attempt += 1
                  try? await Task.sleep(nanoseconds: 750_000_000)
                  continue
                }
                logger.warning(
                  "🛡️ [CLIENT D] Post-publish getGroupInfo failed for \(convoId.prefix(16)): \(mlsErr.errorDescription ?? "?") — treating as ambiguous"
                )
                break
              }
            }
          } catch {
            // publishGroupInfo failure is itself evidence we did not win
            // — server rejected our update (likely because the active
            // crypto session is bound to a sibling's groupId).
            let lowered = error.localizedDescription.lowercased()
            if lowered.contains("410") || lowered.contains("groupreset") {
              detectedGroupReset = true
              logger.warning(
                "🛡️ [CLIENT D] publishGroupInfo got 410/groupReset for \(convoId.prefix(16)) — sibling owns the active session"
              )
            } else {
              logger.warning(
                "🛡️ [CLIENT D] publishGroupInfo failed for \(convoId.prefix(16)) (\(error.localizedDescription)) — treating as ambiguous"
              )
            }
          }

          if !verifiedWin {
            // Rollback. The local ratchet has advanced past confirmCommit,
            // but we cannot prove the server is bound to it. Tear down
            // the local group; recovery's downstream path depends on
            // signal type:
            //   - 410 groupReset: server has a new active crypto session;
            //     stage recipient rejoin against that session (same path
            //     as CLIENT G uses for WrongGroupId).
            //   - everything else: ambiguous, sit on RESET_PENDING and
            //     let next sync iteration retry.
            try? await mlsClient.deleteGroup(for: userDid, groupId: pendingGroupIdData)
            if let recoveryManager = await mlsClient.recovery(for: userDid) {
              await recoveryManager.clearRejoinTracking(convoId: convoId)
            }

            if detectedGroupReset {
              // Look up server's current authoritative groupId so the
              // recipient path can verify the landed group matches.
              do {
                if let serverConvo = try await apiClient.getConversation(convoId: convoId) {
                  let serverGroupIdHex = serverConvo.groupId
                  let serverGeneration = serverConvo.resetGeneration.map { Int64($0) }
                  if !serverGroupIdHex.isEmpty {
                    try? await markConversationNeedsReset(
                      convoId,
                      pendingNewGroupId: serverGroupIdHex,
                      pendingResetGeneration: serverGeneration
                    )
                    logger.warning(
                      "🛡️ [CLIENT D] Staged recipient rejoin for \(convoId.prefix(16)) → newGroupId=\(serverGroupIdHex.prefix(16)) gen=\(serverGeneration.map(String.init) ?? "nil")"
                    )
                  }
                }
              } catch {
                logger.warning(
                  "🛡️ [CLIENT D] Could not stage recipient rejoin after groupReset for \(convoId.prefix(16)) — getConversation failed: \(error.localizedDescription)"
                )
              }
            }
            return
          }

          let now = Date()
          do {
            try await database.write { db in
              let currentGeneration = try MLSConversationResetSQL.loadPendingResetGeneration(
                db: db, conversationID: convoId, currentUserDID: userDid)
              let generationAdvanced: Bool = {
                switch (currentGeneration, observedGeneration) {
                case let (.some(current), .some(observed)):
                  return current > observed
                case (.some, .none):
                  return true
                default:
                  return false
                }
              }()
              if generationAdvanced {
                try MLSConversationResetSQL.clearPendingReset(
                  db: db, conversationID: convoId, currentUserDID: userDid, now: now)
              } else {
                try MLSConversationResetSQL.applyRecipientResetSuccess(
                  db: db,
                  conversationID: convoId,
                  currentUserDID: userDid,
                  newGroupID: pendingGroupIdData,
                  newEpoch: landedEpoch,
                  now: now
                )
              }
            }
            groupStates.removeValue(forKey: convoId)
            if let serverConvo {
              conversations[convoId] = serverConvo
            }
            logger.warning(
              "✅ [BOOTSTRAP] 409-winner recovery complete for \(convoId.prefix(16)) → epoch=\(landedEpoch) (verified)"
            )
            if let recoveryManager = await mlsClient.recovery(for: userDid) {
              await recoveryManager.clearRejoinTracking(convoId: convoId)
            }
          } catch {
            logger.error(
              "❌ [BOOTSTRAP] DB write failed during 409-winner recovery for \(convoId.prefix(16)): \(error.localizedDescription)"
            )
          }
        }

      case .bootstrapTargetNotFound, .notMember:
        // Irrecoverable from this end — either the post-reset row no longer
        // matches (subsequent reset overwrote it) or we're not in the
        // member roster. Drop the staging pointer so we don't loop.
        logger.error(
          "❌ [BOOTSTRAP] Irrecoverable for \(convoId.prefix(16)): \(error.errorDescription ?? "?") — clearing RESET_PENDING staging"
        )
        if let handle = stagedHandle {
          await mlsClient.discardPending(for: userDid, handle: handle)
        }
        // B15 (Half 2): only delete if WE created this group.
        if wasGroupCreatedByUs {
          try? await mlsClient.deleteGroup(for: userDid, groupId: pendingGroupIdData)
        } else {
          logger.info(
            "🛡️ [BOOTSTRAP] Skipping deleteGroup on irrecoverable for \(convoId.prefix(16)) — group not owned by this attempt"
          )
        }
        try? await database.write { db in
          try MLSConversationResetSQL.clearPendingReset(
            db: db, conversationID: convoId, currentUserDID: userDid, now: Date())
        }

      default:
        logger.error(
          "❌ [BOOTSTRAP] bootstrapResetGroup failed for \(convoId.prefix(16)): \(error.errorDescription ?? "?")"
        )
        if let handle = stagedHandle {
          await mlsClient.discardPending(for: userDid, handle: handle)
        }
        // B15 (Half 2): only delete if WE created this group.
        if wasGroupCreatedByUs {
          try? await mlsClient.deleteGroup(for: userDid, groupId: pendingGroupIdData)
        } else {
          logger.info(
            "🛡️ [BOOTSTRAP] Skipping deleteGroup on API error for \(convoId.prefix(16)) — group not owned by this attempt"
          )
        }
        if let recoveryManager = await mlsClient.recovery(for: userDid) {
          await recoveryManager.recordFailedRejoin(
            convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
        }
      }
    } catch {
      logger.error(
        "❌ [BOOTSTRAP] Unexpected error for \(convoId.prefix(16)): \(error.localizedDescription)"
      )
      if let handle = stagedHandle {
        await mlsClient.discardPending(for: userDid, handle: handle)
      }
      // B15 (Half 2): only delete if WE created this group.
      if wasGroupCreatedByUs {
        try? await mlsClient.deleteGroup(for: userDid, groupId: pendingGroupIdData)
      } else {
        logger.info(
          "🛡️ [BOOTSTRAP] Skipping deleteGroup on unexpected error for \(convoId.prefix(16)) — group not owned by this attempt"
        )
      }
      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        await recoveryManager.recordFailedRejoin(
          convoId: convoId, epochAuthenticatorHex: preDeleteAuthHex)
      }
    }
  }

  private func isSuspensionRelatedSyncError(_ error: Error) -> Bool {
    if isSyncPaused || isSuspending || MLSClient.isSuspensionInProgress {
      return true
    }

    switch error {
    case MLSConversationError.storageUnavailable(reason: _):
      return true
    default:
      break
    }

    let message = error.localizedDescription.lowercased()
    if message.contains("suspend")
      || message.contains("storage unavailable")
      || message.contains("account switch in progress")
      || message.contains("transitioning to background")
      || message.contains("gate is closing")
    {
      return true
    }

    return false
  }

  internal func persistConversationsToDatabase(_ convos: [BlueCatbirdMlsChatDefs.ConvoView]) async throws {
    guard let userDid = userDid else {
      logger.error("Cannot persist conversations - no user DID")
      return
    }

    // Pre-check which conversations are new and need trust checking
    // We need to do trust checks outside the database write transaction (async)
    var trustCheckResults: [String: MLSRequestState] = [:]
    
    for convo in convos {
      // Check if conversation already exists
      let existingConvo = try await storage.fetchConversation(
        conversationID: convo.conversationId,
        currentUserDID: userDid,
        database: database
      )

      if existingConvo != nil {
        // Preserve existing request state
        trustCheckResults[convo.conversationId] = existingConvo!.requestState
      } else {
        // New conversation - determine initial request state
        let creatorDid = convo.creator.description
        let isCreator = creatorDid.lowercased() == userDid.lowercased()

        if isCreator {
          // User created this conversation - not a request
          trustCheckResults[convo.conversationId] = .none
        } else {
          // Someone else created - check if we trust them
          let isTrusted = await trustChecker.isTrusted(did: creatorDid)
          if isTrusted {
            trustCheckResults[convo.conversationId] = .none
          } else {
            logger.info("📬 New inbound chat request from \(creatorDid.prefix(20))...")
            trustCheckResults[convo.conversationId] = .pendingInbound
          }
        }
      }
    }

    // Pre-fetch MLS metadata for all conversations (encrypted in group context extensions).
    // This runs outside the database write block since FFI calls are async.
    var mlsMetadataByGroupId: [String: GroupMetadataPayload] = [:]
    for convo in convos {
      guard let groupIdData = Data(hexEncoded: convo.groupId) else { continue }
      do {
        if let metadata = try await mlsClient.getGroupMetadata(for: userDid, groupId: groupIdData) {
          mlsMetadataByGroupId[convo.conversationId] = metadata
        }
      } catch {
        // Non-fatal: group may not exist locally yet (e.g., pending welcome)
        logger.debug("⚠️ Could not read MLS metadata for \(convo.conversationId.prefix(16))...: \(error.localizedDescription)")
      }
    }

    try await database.write { db in
      for convo in convos {
        guard let groupIdData = Data(hexEncoded: convo.groupId) else {
          self.logger.error("Invalid group ID format for conversation \(convo.conversationId)")
          continue
        }

        // Prefer MLS-encrypted metadata over server plaintext metadata
        let mlsMeta = mlsMetadataByGroupId[convo.conversationId]
        let title = mlsMeta?.name ?? convo.metadata?.name
        let requestState = trustCheckResults[convo.conversationId] ?? .none

        // Merge with existing row to preserve local-only state (lastMessageAt,
        // mutedUntil, joinMethod, consecutiveFailures, etc.) that would be
        // clobbered by a blind INSERT OR REPLACE.
        let existing = try MLSConversationModel
          .filter(MLSConversationModel.Columns.conversationID == convo.conversationId)
          .filter(MLSConversationModel.Columns.currentUserDID == userDid)
          .fetchOne(db)

        // For lastMessageAt, prefer the most recent value between server and local
        let serverLastMessage = convo.lastMessageAt?.date
        let mergedLastMessage: Date? = {
          switch (existing?.lastMessageAt, serverLastMessage) {
          case let (local?, server?): return max(local, server)
          case let (local?, nil): return local
          case let (nil, server?): return server
          case (nil, nil): return nil
          }
        }()

        let model = MLSConversationModel(
          conversationID: convo.conversationId,
          currentUserDID: userDid,
          groupID: groupIdData,
          epoch: Int64(convo.epoch),
          joinMethod: existing?.joinMethod ?? .unknown,
          joinEpoch: existing?.joinEpoch ?? 0,
          title: title,
          avatarURL: existing?.avatarURL,
          avatarImageData: existing?.avatarImageData,
          createdAt: convo.createdAt.date,
          updatedAt: Date(),
          lastMessageAt: mergedLastMessage,
          lastMembershipChangeAt: existing?.lastMembershipChangeAt,
          unacknowledgedMemberChanges: existing?.unacknowledgedMemberChanges ?? 0,
          isActive: true,
          needsRejoin: existing?.needsRejoin ?? false,
          rejoinRequestedAt: existing?.rejoinRequestedAt,
          lastRecoveryAttempt: existing?.lastRecoveryAttempt,
          consecutiveFailures: existing?.consecutiveFailures ?? 0,
          isPlaceholder: existing?.isPlaceholder ?? false,
          requestState: existing?.requestState ?? requestState,
          mutedUntil: existing?.mutedUntil
        )

        try model.save(db)
      }
    }

    let pendingCount = trustCheckResults.values.filter { $0 == .pendingInbound }.count
    if pendingCount > 0 {
      logger.info("💾 Persisted \(convos.count) conversations (\(pendingCount) as chat requests)")
    } else {
      logger.info("💾 Persisted \(convos.count) conversations to encrypted database")
    }
  }

  internal func persistMembersToDatabase(_ convos: [BlueCatbirdMlsChatDefs.ConvoView]) async throws {
    guard let userDid = userDid else {
      logger.error("Cannot persist members - no user DID")
      return
    }

    let normalizedUserDid = MLSStorageHelpers.normalizeDID(userDid)
    try await database.write { db in
      for convo in convos {
        try db.execute(
          sql: """
            UPDATE MLSMemberModel
            SET isActive = 0, removedAt = ?, updatedAt = ?
            WHERE conversationID = ? AND currentUserDID = ? AND isActive = 1
            """,
          arguments: [Date(), Date(), convo.conversationId, normalizedUserDid]
        )

        for (index, apiMember) in convo.members.enumerated() {
          let memberID = "\(convo.conversationId)_\(apiMember.did.description)"
          let normalizedDid = MLSStorageHelpers.normalizeDID(apiMember.did.description)
          let normalizedUserDid = MLSStorageHelpers.normalizeDID(userDid)
          let now = Date()
          let role = apiMember.isAdmin ? "admin" : "member"

          // UPSERT: insert new members, but preserve existing profile fields
          // (handle, displayName, avatarURL) that the profile enricher populated.
          try db.execute(
            sql: """
              INSERT INTO MLSMemberModel
                (memberID, conversationID, currentUserDID, did, handle, displayName, avatarURL,
                 leafIndex, addedAt, updatedAt, isActive, role)
              VALUES (?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, 1, ?)
              ON CONFLICT(memberID) DO UPDATE SET
                leafIndex = excluded.leafIndex,
                updatedAt = excluded.updatedAt,
                isActive = 1,
                removedAt = NULL,
                role = excluded.role
              """,
            arguments: [
              memberID, convo.conversationId, normalizedUserDid, normalizedDid,
              index, now, now, role,
            ])
        }
      }
    }

    logger.info("💾 Persisted members for \(convos.count) conversations to encrypted database (batched)")
  }

  internal func reconcileDatabase(with serverConvoIDs: Set<String>) async throws {
    guard let userDid = userDid else {
      logger.error("Cannot reconcile database - no user DID")
      return
    }

    let localConvos: [MLSConversationModel]
    do {
      localConvos = try await database.read { db in
        try MLSConversationModel
          .filter(MLSConversationModel.Columns.currentUserDID == userDid)
          .fetchAll(db)
      }
    } catch {
      // TODO: Restore database recovery logic once MLSGRDBManager is located/restored
      /*
      if MLSGRDBManager.shared.isRecoverableCodecError(error) {
        logger.warning("⚠️ Recoverable database error in reconcileDatabase, attempting recovery...")
        do {
          let freshDatabase = try await MLSGRDBManager.shared.reconnectDatabase(for: userDid)
          localConvos = try await freshDatabase.read { db in
            try MLSConversationModel
              .filter(MLSConversationModel.Columns.currentUserDID == userDid)
              .fetchAll(db)
          }
          logger.info("✅ Database recovered in reconcileDatabase")
        } catch {
          logger.error("❌ Database recovery failed in reconcileDatabase: \(error.localizedDescription)")
          throw error
        }
      } else {
        throw error
      }
      */
      throw error
    }

    let localConvoIDs = localConvos.map { $0.conversationID }
    let removedConvoIDs = Set(localConvoIDs).subtracting(serverConvoIDs)

    let zombieThreshold: TimeInterval = 300
    let now = Date()
    var zombiesDetected: [String] = []

    for convo in localConvos {
      let convoId = convo.conversationID
      let age = now.timeIntervalSince(convo.createdAt)

      if serverConvoIDs.contains(convoId) { continue }

      if age > zombieThreshold {
        let groupExists = await mlsClient.groupExists(for: userDid, groupId: convo.groupID)
        if !groupExists {
          logger.warning("🧟 [RECONCILE] Detected zombie conversation: \(convoId.prefix(16))...")
          zombiesDetected.append(convoId)
        }
      }
    }

    if !zombiesDetected.isEmpty {
      logger.info("🧹 [RECONCILE] Cleaning up \(zombiesDetected.count) zombie conversation(s)")
      try await deleteConversationsFromDatabase(zombiesDetected)
    }

    let remainingRemoved = removedConvoIDs.subtracting(zombiesDetected)

    guard !remainingRemoved.isEmpty else { return }

    if serverConvoIDs.isEmpty && !localConvoIDs.isEmpty && zombiesDetected.isEmpty {
      logger.warning("⚠️ [RECONCILE] Server returned 0 conversations but we have \(localConvoIDs.count) locally")
      return
    }

    // ⭐ RACE CONDITION FIX: Filter out groups that were recently created.
    // A sync response fetched before creation completes will not include the new conversation.
    // By keeping a 120s grace window after creation, we prevent stale sync responses from
    // triggering force-deletion of conversations that actually exist on the server.
    let recentCreationWindow: TimeInterval = 120
    let recentlyCreated: Set<String> = groupsBeingCreated.withLock { entries in
      // Prune entries older than the window
      entries = entries.filter { now.timeIntervalSince($0.value) < recentCreationWindow }
      return Set(entries.keys)
    }
    let safeToDelete = remainingRemoved.subtracting(recentlyCreated)

    if safeToDelete.count != remainingRemoved.count {
      let skipped = remainingRemoved.subtracting(safeToDelete)
      logger.info("⏳ [RECONCILE] Skipping \(skipped.count) recently-created conversation(s): \(skipped.map { $0.prefix(16) })")
    }

    guard !safeToDelete.isEmpty else { return }

    logger.info("🗑️ [RECONCILE] Removing \(safeToDelete.count) conversation(s) not on server")

    for convoId in safeToDelete {
      let groupIdHex: String
      if let groupIdData = localConvos.first(where: { $0.conversationID == convoId })?.groupID {
        groupIdHex = groupIdData.hexEncodedString()
      } else {
        groupIdHex = convoId
      }

      await forceDeleteConversationLocally(convoId: convoId, groupId: groupIdHex)
    }
  }

  internal func deleteConversationsFromDatabase(_ convoIds: [String]) async throws {
    guard let userDID = userDid else { return }

    try await database.write { db in
      for convoId in convoIds {
        try db.execute(
          sql: "DELETE FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?;",
          arguments: [convoId, userDID])
        try db.execute(
          sql: "DELETE FROM MLSMessageModel WHERE conversationID = ? AND currentUserDID = ?;",
          arguments: [convoId, userDID])
        try db.execute(
          sql: "DELETE FROM MLSMemberModel WHERE conversationID = ? AND currentUserDID = ?;",
          arguments: [convoId, userDID])
        try db.execute(
          sql: "DELETE FROM MLSEpochKeyModel WHERE conversationID = ? AND currentUserDID = ?;",
          arguments: [convoId, userDID])
      }
    }

    for convoId in convoIds {
      let groupId = conversations[convoId]?.groupId
      if let groupIdHex = groupId, let groupIdData = Data(hexEncoded: groupIdHex) {
        do {
          try await mlsClient.deleteGroup(for: userDID, groupId: groupIdData)
        } catch {}
      }
      conversations.removeValue(forKey: convoId)
      if let groupId = groupId {
        let groupStillInUse = conversations.values.contains(where: { $0.groupId == groupId })
        if !groupStillInUse {
          groupStates.removeValue(forKey: groupId)
        }
      }
    }
  }

}
