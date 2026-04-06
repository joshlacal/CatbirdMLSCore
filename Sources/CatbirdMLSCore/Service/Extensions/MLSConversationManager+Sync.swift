
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
            "⏭️ [SYNC] Filtering out conversation \(convo.groupId.prefix(16))... - user is not a member"
          )
          staleConvoIds.append(convo.groupId)
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
        
        let existingConvo = conversations[convo.groupId]
        conversations[convo.groupId] = convo

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
              if await conversationNeedsRejoin(convo.groupId) {
                logger.info("⏭️ [SYNC] Skipping epoch catch-up for \(convo.groupId.prefix(16))... - already flagged needsRejoin")
              } else
              // Attempt to catch up by processing missed commits
              if serverEpoch > ffiEpoch {
                let result = await fetchAndProcessMissingCommits(
                  conversationID: convo.groupId,
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
                  logger.warning("⚠️ Epoch catch-up failed (new group) - deferring rejoin for \(convo.groupId.prefix(16))...")
                  logger.warning("   Local epoch \(failedEpoch) cannot process commits to reach server epoch \(serverEpoch)")
                  logger.warning("   Reason: \(reason)")
                  try? await markConversationNeedsRejoin(convo.groupId)
                case .serverDataGap:
                  logger.warning("⚠️ Server data gap (new group) for \(convo.groupId.prefix(16))... - will retry next sync")
                case .transientFetchError(let reason):
                  logger.warning("⚠️ Transient fetch error (new group) for \(convo.groupId.prefix(16))...: \(reason) - will retry next sync")
                }
              }
            }
          } catch {
            // Group may not exist in FFI yet (e.g., before processing Welcome)
            logger.debug("Could not get FFI epoch for \(convo.groupId.prefix(16)): \(error)")
            logger.debug("Using server epoch \(serverEpoch) as fallback")
          }

          groupStates[convo.groupId] = MLSGroupState(
            groupId: convo.groupId,
            convoId: convo.groupId,
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
                if await conversationNeedsRejoin(convo.groupId) {
                  logger.info("⏭️ [SYNC] Skipping epoch catch-up for \(convo.groupId.prefix(16))... - already flagged needsRejoin")
                } else
                // Attempt to catch up by processing missed commits
                if serverEpoch > ffiEpoch {
                  let result = await fetchAndProcessMissingCommits(
                    conversationID: convo.groupId,
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
                    logger.warning("⚠️ Epoch catch-up failed (update) - deferring rejoin for \(convo.groupId.prefix(16))...")
                    logger.warning("   Local epoch \(failedEpoch) cannot process commits to reach server epoch \(serverEpoch)")
                    logger.warning("   Reason: \(reason)")
                    try? await markConversationNeedsRejoin(convo.groupId)
                  case .serverDataGap:
                    logger.warning("⚠️ Server data gap (update) for \(convo.groupId.prefix(16))... - will retry next sync")
                  case .transientFetchError(let reason):
                    logger.warning("⚠️ Transient fetch error (update) for \(convo.groupId.prefix(16))...: \(reason) - will retry next sync")
                  }
                }
              }
            } catch {
              logger.debug("Could not get FFI epoch for \(convo.groupId.prefix(16)): \(error)")
              logger.debug("Using server epoch \(serverEpoch) as fallback")
            }

            state.epoch = ffiEpoch  // Use FFI epoch if available, else server epoch
            state.knownServerEpoch = UInt64(convo.epoch)
            state.members = Set(convo.members.map { $0.did.description })
            groupStates[convo.groupId] = state

            // Persist epoch to GRDB so DataSource reads are consistent
            do {
              try await storage.updateConversationEpoch(
                conversationID: convo.groupId,
                currentUserDID: userDid,
                epoch: Int64(ffiEpoch),
                database: database
              )
            } catch {
              logger.warning("Failed to persist epoch during sync for \(convo.groupId.prefix(16)): \(error.localizedDescription)")
            }

            // Notify epoch update
            notifyObservers(.epochUpdated(convo.groupId, Int(ffiEpoch)))
          }
        }

        // Check confirmation tag divergence (only if server provides one and epochs match)
        if let serverTag = convo.confirmationTag,
           let groupIdData = Data(hexEncoded: convo.groupId),
           !(await conversationNeedsRejoin(convo.groupId)) {
          let localTagData = try? await mlsClient.getConfirmationTag(for: userDid, groupId: groupIdData)
          let localTagB64 = localTagData?.base64EncodedString()
          if let localTagB64 = localTagB64, localTagB64 != serverTag {
            logger.warning("⚠️ [SYNC] Tree divergence detected for \(convo.groupId.prefix(16))... - local tag != server tag")
            try? await markConversationNeedsRejoin(convo.groupId)
          }
        }

        // Initialize MLS group if needed
        if needsGroupInit {
          // Check if group exists locally via FFI
          guard let groupIdData = Data(hexEncoded: convo.groupId) else {
            logger.error("Invalid group ID format for \(convo.groupId)")
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

            logger.info("🔄 [SYNC] Member missing local group for \(convo.groupId.prefix(16))... - trying Welcome first")

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
                    for: userDid, convoId: convo.groupId)
                  logger.info("✅ [SYNC] Device successfully joined via External Commit")
                } catch {
                  logger.error(
                    "❌ [SYNC] External Commit failed for device-sync: \(error.localizedDescription)"
                  )
                  logger.error(
                    "   Conversation \(convo.groupId.prefix(16))... will be unavailable")

                  // Non-fatal: conversation will retry on next sync
                  continue  // Skip this conversation
                }
              } else {
                // Other API error - log and skip
                logger.error(
                  "❌ CRITICAL: Failed to initialize MLS group for \(convo.groupId): MLSAPIError - \(mlsApiError.localizedDescription)"
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
                "❌ CRITICAL: Failed to initialize MLS group for \(convo.groupId): \(type(of: error)) - \(error.localizedDescription)"
              )
              logger.error("❌ This conversation cannot be used - cryptographic join failed")
              logger.error("❌ Skipping conversation to prevent zombie group state")

              // 🔄 RECOVERY: Check if this error warrants device-level recovery
              if let recoveryManager = await mlsClient.recovery(for: userDid) {
                let recovered = await recoveryManager.attemptRecoveryIfNeeded(
                  for: error,
                  userDid: userDid,
                  convoIds: [convo.groupId]
                )
                if recovered {
                  logger.info(
                    "🔄 Silent recovery initiated for conversation \(convo.groupId.prefix(16))")
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
            logger.debug("Group already exists locally for conversation: \(convo.groupId)")
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

      // Reconcile database: delete conversations that exist locally but not on server
      let serverConvoIDs = Set(allConvos.map { $0.groupId })
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
                SET needsReset = 0, needsRejoin = 0, epoch = 0, updatedAt = ?
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
          // Record failure for backoff
          if let recoveryManager = await mlsClient.recovery(for: userDid) {
            await recoveryManager.recordFailedRejoin(convoId: convo.conversationID)
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
        reason: "deferred epoch recovery (sync catch-up failed)"
      )
      endRejoinAttempt(conversationID: convo.conversationID)

      // Record success/failure in MLSRecoveryManager for backoff tracking
      if let recoveryManager = await mlsClient.recovery(for: userDid) {
        if succeeded {
          await recoveryManager.clearRejoinTracking(convoId: convo.conversationID)
        } else {
          await recoveryManager.recordFailedRejoin(convoId: convo.conversationID)
        }
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
        conversationID: convo.groupId,
        currentUserDID: userDid,
        database: database
      )
      
      if existingConvo != nil {
        // Preserve existing request state
        trustCheckResults[convo.groupId] = existingConvo!.requestState
      } else {
        // New conversation - determine initial request state
        let creatorDid = convo.creator.description
        let isCreator = creatorDid.lowercased() == userDid.lowercased()
        
        if isCreator {
          // User created this conversation - not a request
          trustCheckResults[convo.groupId] = .none
        } else {
          // Someone else created - check if we trust them
          let isTrusted = await trustChecker.isTrusted(did: creatorDid)
          if isTrusted {
            trustCheckResults[convo.groupId] = .none
          } else {
            logger.info("📬 New inbound chat request from \(creatorDid.prefix(20))...")
            trustCheckResults[convo.groupId] = .pendingInbound
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
          mlsMetadataByGroupId[convo.groupId] = metadata
        }
      } catch {
        // Non-fatal: group may not exist locally yet (e.g., pending welcome)
        logger.debug("⚠️ Could not read MLS metadata for \(convo.groupId.prefix(16))...: \(error.localizedDescription)")
      }
    }

    try await database.write { db in
      for convo in convos {
        guard let groupIdData = Data(hexEncoded: convo.groupId) else {
          self.logger.error("Invalid group ID format for conversation \(convo.groupId)")
          continue
        }

        // Prefer MLS-encrypted metadata over server plaintext metadata
        let mlsMeta = mlsMetadataByGroupId[convo.groupId]
        let title = mlsMeta?.name ?? convo.metadata?.name
        let requestState = trustCheckResults[convo.groupId] ?? .none

        // Merge with existing row to preserve local-only state (lastMessageAt,
        // mutedUntil, joinMethod, consecutiveFailures, etc.) that would be
        // clobbered by a blind INSERT OR REPLACE.
        let existing = try MLSConversationModel
          .filter(MLSConversationModel.Columns.conversationID == convo.groupId)
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
          conversationID: convo.groupId,
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
          arguments: [Date(), Date(), convo.groupId, normalizedUserDid]
        )

        for (index, apiMember) in convo.members.enumerated() {
          let memberID = "\(convo.groupId)_\(apiMember.did.description)"
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
              memberID, convo.groupId, normalizedUserDid, normalizedDid,
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
