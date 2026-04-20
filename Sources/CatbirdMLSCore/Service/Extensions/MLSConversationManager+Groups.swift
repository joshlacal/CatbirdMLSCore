import CryptoKit
import Foundation
import GRDB
import OSLog
import Petrel
import Synchronization

extension MLSConversationManager {
  // MARK: - Direct Conversation Lookup

  /// Find an existing active 1:1 conversation with the given DID.
  /// - Parameter did: The target user's DID string
  /// - Returns: The conversation ID if an active 1:1 conversation exists, nil otherwise
  public func findDirectConversation(with did: DID) async throws -> String? {
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    let targetDid = did.description

    return try await database.read { db in
      // Find conversations where:
      // 1. The conversation belongs to the current user and is active
      // 2. The conversation has exactly 2 active members
      // 3. One of the members is the target DID
      let conversationID = try String.fetchOne(db, sql: """
        SELECT c.conversationID
        FROM MLSConversationModel c
        WHERE c.currentUserDID = ?
          AND c.isActive = 1
          AND (
            SELECT COUNT(*) FROM MLSMemberModel m
            WHERE m.conversationID = c.conversationID
              AND m.currentUserDID = c.currentUserDID
              AND m.isActive = 1
          ) = 2
          AND EXISTS (
            SELECT 1 FROM MLSMemberModel m
            WHERE m.conversationID = c.conversationID
              AND m.currentUserDID = c.currentUserDID
              AND m.isActive = 1
              AND LOWER(m.did) = LOWER(?)
          )
        ORDER BY c.updatedAt DESC
        LIMIT 1
        """,
        arguments: [userDid, targetDid]
      )

      return conversationID
    }
  }

  // MARK: - Group Initialization

  /// Create a new MLS group/conversation
  /// - Parameters:
  ///   - initialMembers: DIDs of initial members to add (optional)
  ///   - name: Conversation name
  ///   - description: Conversation description (optional)
  ///   - avatarUrl: Avatar URL (optional)
  /// - Returns: Created conversation view
  public func createGroup(
    initialMembers: [DID]? = nil,
    name: String,
    description: String? = nil,
    avatarUrl: String? = nil
  ) async throws -> BlueCatbirdMlsChatDefs.ConvoView {
    logger.info(
      "🔵 [MLSConversationManager.createGroup] START - name: '\(name)', initialMembers: \(initialMembers?.count ?? 0)"
    )
    try throwIfShuttingDown("createGroup")

    guard isInitialized else {
      logger.error("❌ [MLSConversationManager.createGroup] Context not initialized")
      throw MLSConversationError.contextNotInitialized
    }

    guard let userDid = userDid else {
      logger.error("❌ [MLSConversationManager.createGroup] No authentication")
      throw MLSConversationError.noAuthentication
    }

    // ⭐ FIX #1: Filter out the creator's DID from initialMembers
    // In MLS, you only fetch key packages for OTHER members you're adding.
    // The creator is implicitly added during group creation.
    let filteredMembers: [DID]?
    if let members = initialMembers {
      let selfDid = userDid.lowercased()
      let filtered = members.filter { $0.description.lowercased() != selfDid }
      if filtered.count != members.count {
        logger.warning(
          "⚠️ [createGroup] Filtered out self-DID from initialMembers (was \(members.count), now \(filtered.count))"
        )
      }
      filteredMembers = filtered.isEmpty ? nil : filtered
    } else {
      filteredMembers = nil
    }

    // Create temporary tracking ID for initialization state
    let tempId = UUID().uuidString
    conversationStates[tempId] = .initializing

    defer {
      conversationStates.removeValue(forKey: tempId)
    }

    logger.debug("📍 [MLSConversationManager.createGroup] Creating local group for user: \(userDid)")

    // Ensure we have local key package bundles before touching the FFI context.
    // Without at least one bundle, OpenMLS generated credentials are not persisted and
    // later signature verification fails (see InvalidSignature in logs).
    do {
      let bundleCount = try await mlsClient.ensureLocalBundlesAvailable(for: userDid)
      if bundleCount == 0 {
        logger.warning(
          "⚠️ [MLSConversationManager.createGroup] No local bundles available - attempting replenishment"
        )

        // Try to replenish asynchronously but wait for result
          try await mlsClient.monitorAndReplenishBundles(for: userDid)

        // Check again
        let countAfter = try await mlsClient.ensureLocalBundlesAvailable(for: userDid)
        if countAfter == 0 {
          logger.error(
            "❌ [MLSConversationManager.createGroup] Still no bundles after replenishment - cannot create group"
          )
          throw MLSConversationError.operationFailed(
            "No key packages available even after replenishment attempt."
          )
        }
      }
    } catch {
      logger.error(
        "❌ [MLSConversationManager.createGroup] Failed to verify local key packages: \(error.localizedDescription)"
      )
      throw MLSConversationError.operationFailed(
        "Unable to verify local key packages: \(error.localizedDescription)")
    }

    // ⭐ CRITICAL FIX: Create MLS group locally FIRST to get the groupID
    // Uses mlsDid (device-specific DID) automatically
    // Pass name/description via GroupConfig so they are encrypted in MLS group context extensions
    let groupConfig = configuration.toFFI(
      groupName: name.isEmpty ? nil : name,
      groupDescription: description
    )

    // Use createGroupV2 to get full result including metadata artifacts
    let mlsCreationResult = try await mlsClient.createGroupV2(
      for: userDid, configuration: groupConfig)
    let groupId = mlsCreationResult.groupId
    let groupIdHex = groupId.hexEncodedString()
    logger.info(
      "🔵 [MLSConversationManager.createGroup] Local group created: \(groupIdHex.prefix(16))...")

    // ⭐ RACE CONDITION FIX: Track this group as "being created" with a timestamp.
    // The timestamp persists after creation completes so that reconcileDatabase() won't
    // delete a newly-created conversation when its sync response was fetched before creation.
    // Entries are pruned after 120s in reconcileDatabase().
    groupsBeingCreated.withLock { $0[groupIdHex] = Date() }

    // 🔬 CRITICAL DIAGNOSTIC: Log creator's initial state (before adding members)
    await logGroupStateDiagnostics(
      userDid: userDid, groupId: groupId, context: "After Group Creation (Creator, Epoch 0)")

    // ⭐ FIXED: Use groupIdHex as conversationID (not random UUID) so Rust FFI epoch storage succeeds
    // The Rust FFI passes groupIdHex as the conversationId when storing epoch secrets,
    // so our database must use the same identifier as the primary key for foreign key constraints to work
    do {
      try await storage.ensureConversationExists(
        userDID: userDid,
        conversationID: groupIdHex,  // ← Use groupIdHex, not tempId
        groupID: groupIdHex,
        database: database
      )

      // Track how we joined so UI can explain missing history after External Commit.
      try await storage.updateConversationJoinInfo(
        conversationID: groupIdHex,
        currentUserDID: userDid,
        joinMethod: .creator,
        joinEpoch: 0,
        database: database
      )

      // Write the group title to the local DB immediately so the creator sees it.
      // The encrypted metadata blob is uploaded to the server later, but the
      // local conversation record needs the plaintext title right away.
      if !name.isEmpty {
        try await database.write { db in
          try db.execute(
            sql: """
              UPDATE MLSConversationModel
              SET title = ?, updatedAt = ?
              WHERE conversationID = ? AND currentUserDID = ?
              """,
            arguments: [name, Date(), groupIdHex, userDid]
          )
        }
        logger.info("✅ Wrote group title '\(name)' to local conversation record")
      }

      logger.info("✅ Created SQLCipher conversation record with ID: \(groupIdHex.prefix(16))...")
    } catch {
      logger.error("❌ Failed to create SQLCipher conversation: \(error.localizedDescription)")
      throw MLSConversationError.operationFailed(
        "Failed to create local conversation record: \(error.localizedDescription)")
    }

    // CRITICAL FIX: Manually export epoch secret AFTER conversation record exists
    // The createGroup() call above attempts to export the epoch 0 secret, but it fails
    // because the conversation record didn't exist yet. Now that the record exists,
    // we can successfully export the epoch secret to satisfy foreign key constraints.
    do {
      try await mlsClient.exportEpochSecret(for: userDid, groupId: groupId)
      logger.info("✅ Exported epoch 0 secret after conversation record creation")
    } catch {
      logger.error("❌ Failed to export epoch secret: \(error.localizedDescription)")
      logger.warning("⚠️ This may cause decryption failures for epoch 0 messages")
      // Non-fatal: Continue with group creation even if epoch secret export fails
    }

    var welcomeDataArray: [Data] = []
    var commitData: Data?

    // Metadata is now encrypted inside MLS group context extensions (via GroupConfig).
    // Do NOT send plaintext metadata to the server.
    let metadataInput: BlueCatbirdMlsChatCreateConvo.MetadataInput? = nil

    // Create conversation on server (handles key package retries internally)
    let creationResult: ServerConversationCreationResult
    do {
      creationResult = try await createConversationOnServer(
        userDid: userDid,
        groupId: groupId,
        groupIdHex: groupIdHex,
        initialMembers: filteredMembers,  // ⭐ Use filtered members (self-DID removed)
        metadata: metadataInput
      )
    } catch {
      logger.error(
        "❌ [MLSConversationManager.createGroup] Server creation failed: \(error.localizedDescription)"
      )

      // SAFETY: Create safe copy of error description before storing in state
      let safeErrorDesc = String(describing: error.localizedDescription)
      conversationStates[tempId] = .failed(safeErrorDesc)

      // ⭐ FIX #2: ROLLBACK - Delete the prematurely created SQLCipher conversation record
      // This prevents "zombie" conversations that exist locally but not on the server
      logger.info(
        "🗑️ [MLSConversationManager.createGroup] Rolling back local conversation record: \(groupIdHex.prefix(16))..."
      )
      do {
        try await database.write { db in
          try db.execute(
            sql: """
                  DELETE FROM MLSConversationModel
                  WHERE conversationID = ? AND currentUserDID = ?;
              """, arguments: [groupIdHex, userDid])
        }
        logger.info("✅ [MLSConversationManager.createGroup] Rolled back local conversation record")
      } catch {
        logger.error(
          "❌ [MLSConversationManager.createGroup] Failed to rollback conversation record: \(error.localizedDescription)"
        )
      }

      // Also delete the local MLS group state to prevent orphaned cryptographic material
      do {
        try await mlsClient.deleteGroup(for: userDid, groupId: groupId)
        logger.info("✅ [MLSConversationManager.createGroup] Deleted local MLS group state")
      } catch {
        logger.warning(
          "⚠️ [MLSConversationManager.createGroup] Failed to delete local MLS group: \(error.localizedDescription)"
        )
      }

      if let members = filteredMembers, !members.isEmpty {
        logger.debug("📍 [MLSConversationManager.createGroup] Cleaning up pending commit...")
        do {
          try await mlsClient.clearPendingCommit(for: userDid, groupId: groupId)
          logger.info("✅ [MLSConversationManager.createGroup] Cleared pending commit")
        } catch {
          logger.error(
            "❌ [MLSConversationManager.createGroup] Failed to clear pending commit: \(error.localizedDescription)"
          )
        }
      }

      if let mlsError = error as? MLSConversationError {
        throw mlsError
      }

      throw MLSConversationError.serverError(error)
    }

    if let welcomeData = creationResult.welcomeData {
      welcomeDataArray = [welcomeData]
    }
    commitData = creationResult.commitData
    let convo = creationResult.convo

    // GREENFIELD: Server uses groupId as canonical ID (no migration, no fallbacks)
    logger.info("✅ Conversation created: \(groupIdHex.prefix(16))...")

    // Store conversation state using groupId as canonical ID
    conversations[groupIdHex] = convo

    // ⭐ CRITICAL FIX: Verify epoch from FFI instead of trusting server's response
    let serverEpoch = UInt64(convo.epoch)
    let ffiEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupId)

    if serverEpoch != ffiEpoch {
      logger.warning("⚠️ EPOCH MISMATCH at group creation:")
      logger.warning("   Server reported: \(serverEpoch)")
      logger.warning("   FFI actual: \(ffiEpoch)")
      logger.warning("   Using FFI epoch to prevent state desynchronization")
    }

    groupStates[groupIdHex] = MLSGroupState(
      groupId: groupIdHex,
      convoId: groupIdHex,
      epoch: ffiEpoch,  // Use FFI epoch, not server epoch
      members: Set(convo.members.map { $0.did.description })
    )

    // Persist MLS state to SQLCipher immediately after group creation
    do {
      logger.info("✅ Persisted MLS state after group creation")
    } catch {
      logger.error("⚠️ Failed to persist MLS state: \(error.localizedDescription)")
    }

    // CRITICAL FIX: If members were added, sync with server BEFORE allowing messages
    if let members = filteredMembers, !members.isEmpty, let commitData = commitData {
      // Check if createConvo already processed the members (serverEpoch >= 1)
      // If so, we should NOT call addMembers again - just confirm our staged
      // commit locally.
      if serverEpoch >= 1 {
        logger.info("✅ Server already processed members during createConvo (epoch: \(serverEpoch))")
        logger.info("   Confirming staged commit locally to sync epochs...")

        // Task #44/#62: confirm via the handle returned from
        // prepareInitialMembers. Fall back to legacy mergePendingCommitV2 only
        // if the handle is missing (e.g. an intermediate refactor path) —
        // this keeps the code resilient during rollout.
        let mergedEpoch: UInt64
        let metadataKey: Data?
        let metadataEpoch: UInt64?
        if let handle = creationResult.stagedCommitHandle {
          let confirmed: FfiConfirmedCommit
          do {
            confirmed = try await mlsClient.confirmCommit(
              for: userDid, handle: handle, serverEpoch: serverEpoch)
          } catch {
            await mlsClient.discardPending(for: userDid, handle: handle)
            throw error
          }
          mergedEpoch = confirmed.newEpoch
          metadataKey = confirmed.metadataKey
          metadataEpoch = confirmed.newEpoch
        } else {
          logger.warning(
            "⚠️ [createGroup] Missing staged commit handle; falling back to mergePendingCommitV2"
          )
          let (e, k, me) = try await mlsClient.mergePendingCommitV2(
            for: userDid, groupId: groupId)
          mergedEpoch = e
          metadataKey = k
          metadataEpoch = me
        }
        logger.info("✅ [createGroup] Commit merged - local epoch now: \(mergedEpoch)")

        // Verify merged epoch matches server's epoch
        if mergedEpoch != serverEpoch {
          logger.error(
            "❌ CRITICAL: Merged epoch (\(mergedEpoch)) doesn't match server epoch (\(serverEpoch))"
          )
          logger.error("   This indicates a protocol violation - secret trees are now desynced")
          throw MLSConversationError.epochMismatch
        }

        groupStates[groupIdHex]?.epoch = mergedEpoch
        logger.debug("📊 Updated local group state: epoch=\(mergedEpoch)")

        // METADATA: Re-wrap metadata for the new epoch after adding members
        if let key = metadataKey, let epoch = metadataEpoch {
          await reWrapMetadataAfterMerge(
            groupIdHex: groupIdHex,
            metadataKey: key,
            epoch: epoch
          )
        }

        // Mark conversation as active
        conversationStates[groupIdHex] = .active
        return convo
      }
      
      // Fallback: Server didn't process members during createConvo, so we need to call addMembers
      logger.info("🔄 Syncing \(members.count) members with server to prevent epoch mismatch...")

      // Get current epoch before server call
      let currentEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupId)
      logger.debug("📍 Current local epoch before sync: \(currentEpoch)")

      // PHASE 3 FIX: Protect server send + commit merge from cancellation
      // This sequence MUST complete atomically to prevent epoch desync:
      // 1. Server processes commit and advances epoch
      // 2. We merge the pending commit locally to match server
      // If cancelled between these steps, client and server epochs diverge
      do {
        try await withTaskCancellationHandler {
          // Track this commit as our own to prevent re-processing via SSE
          trackOwnCommit(commitData)
          logger.debug("📝 Tracked own addMembers commit to prevent SSE re-processing")

          // ⚠️ NOTE: createConvo already atomically adds initialMembers.
          // This addMembers call is a confirmation/sync step and may return AlreadyMember
          // if the server processed members during createConvo. We handle this gracefully.
          let addResult: (success: Bool, newEpoch: Int)
          do {
            logger.debug("📤 Calling addMembers API for group \(groupIdHex) with \(members.count) members")
            addResult = try await apiClient.addMembers(
              convoId: groupIdHex,
              didList: members,
              commit: commitData,
              welcomeMessage: welcomeDataArray.first
            )
            logger.debug("📥 addMembers API returned: success=\(addResult.success), newEpoch=\(addResult.newEpoch)")
          } catch let apiError as MLSAPIError {
            // Handle AlreadyMember gracefully - this means createConvo already added them
            if case .memberAlreadyExists = apiError {
              logger.info(
                "ℹ️ [createGroup] Members already exist (added during createConvo) - treating as success"
              )
              // Members were already added, so we don't need to merge the pending commit.
              // Task #44/#62: discard via handle if we have one; fall back to
              // clearPendingCommit defensively.
              if let handle = creationResult.stagedCommitHandle {
                await mlsClient.discardPending(for: userDid, handle: handle)
                logger.debug("🧹 Discarded staged commit after AlreadyMember response")
              }
              do {
                try await mlsClient.clearPendingCommit(for: userDid, groupId: groupId)
                logger.debug("🧹 Cleared pending commit after AlreadyMember response")
              } catch {
                logger.warning("⚠️ Failed to clear pending commit: \(error.localizedDescription)")
              }
              // Keep local epoch unchanged and continue to mark conversation as active
              groupStates[groupIdHex]?.epoch = currentEpoch
              return  // Exit the closure successfully
            }
            // Re-throw other API errors
            logger.error("❌ addMembers failed with MLSAPIError: \(apiError)")
            throw apiError
          } catch {
            logger.error("❌ addMembers failed with unexpected error: \(error)")
            throw error
          }

          guard addResult.success else {
            logger.error("❌ Server member sync failed - success=false")
            conversationStates[groupIdHex] = .failed("Member sync failed")
            throw MLSConversationError.memberSyncFailed
          }

          logger.debug("📍 Server returned epoch: \(addResult.newEpoch)")
          logger.info("🔄 [createGroup] Confirming staged commit after successful addMembers...")

          // Always confirm after a successful addMembers. The server may return
          // newEpoch=0 when it nil-defaults the epoch field (idempotency hit),
          // but that does NOT mean the commit was a no-op locally — the
          // OpenMLS group still has a pending staged commit that must be
          // merged. Skipping confirm breaks all subsequent message encryption.
          // Task #44/#62: confirm via handle; when server returns newEpoch=0
          // (idempotency hit) we pass the skip-fence sentinel so we don't
          // trip the server-epoch check against the staged target.
          let serverEpochForConfirm: UInt64 =
            addResult.newEpoch > 0
            ? UInt64(addResult.newEpoch)
            : mlsSkipServerEpochFence()
          let mergedEpoch: UInt64
          let metadataKey2: Data?
          let metadataEpoch2: UInt64?
          if let handle = creationResult.stagedCommitHandle {
            let confirmed = try await mlsClient.confirmCommit(
              for: userDid, handle: handle, serverEpoch: serverEpochForConfirm)
            mergedEpoch = confirmed.newEpoch
            metadataKey2 = confirmed.metadataKey
            metadataEpoch2 = confirmed.newEpoch
          } else {
            logger.warning(
              "⚠️ [createGroup] Missing staged commit handle; falling back to mergePendingCommitV2"
            )
            let (e, k, me) = try await mlsClient.mergePendingCommitV2(
              for: userDid, groupId: groupId)
            mergedEpoch = e
            metadataKey2 = k
            metadataEpoch2 = me
          }
          logger.info("✅ [createGroup] Commit merged - local epoch now: \(mergedEpoch)")
          groupStates[groupIdHex]?.epoch = mergedEpoch
          logger.debug("📊 Updated local group state: epoch=\(mergedEpoch)")

          // METADATA: Re-wrap metadata for the new epoch after addMembers
          if let key = metadataKey2, let epoch = metadataEpoch2 {
            await reWrapMetadataAfterMerge(
              groupIdHex: groupIdHex,
              metadataKey: key,
              epoch: epoch
            )
          }

          await logGroupStateDiagnostics(
            userDid: userDid, groupId: groupId, context: "After Merge Commit (Creator)")
        } onCancel: {
          logger.warning(
            "⚠️ [createGroup] Commit operation was cancelled - allowing completion to prevent epoch desync"
          )
        }
      } catch {
        logger.error("❌ Server member sync failed: \(error.localizedDescription)")
        // Task #44/#62: if we staged a commit but the sync/confirm threw,
        // discard via handle so we don't carry stale pending sender state.
        if let handle = creationResult.stagedCommitHandle {
          await mlsClient.discardPending(for: userDid, handle: handle)
        }
        // SAFETY: Create safe copy of error description before storing in state
        let safeErrorDesc = String(describing: error.localizedDescription)
        conversationStates[groupIdHex] = .failed(safeErrorDesc)
        throw MLSConversationError.memberSyncFailed
      }
    }

    // Mark conversation as active AFTER server sync completes
    conversationStates[groupIdHex] = .active
    logger.info("✅ Conversation '\(groupIdHex)' marked as ACTIVE - ready for messaging")

    // Publish GroupInfo to enable external joins (welcome backup)
    // CRITICAL: If this fails, new group cannot accept external joins
    try await publishLatestGroupInfo(
      userDid: userDid,
      convoId: groupIdHex,
      groupId: groupId,
      context: "after createGroup"
    )

    // METADATA: Upload encrypted metadata blob if present in creation result.
    // The Rust FFI encrypts metadata during group creation and returns the blob
    // along with a locator. We upload the blob to the server so other members
    // can fetch and decrypt it after processing the Welcome/commit.
    if let encryptedBlob = mlsCreationResult.encryptedMetadataBlob,
      let locator = mlsCreationResult.metadataBlobLocator
    {
      await uploadMetadataBlobAfterCreation(
        groupIdHex: groupIdHex,
        encryptedBlob: encryptedBlob,
        blobLocator: locator,
        metadataReferenceJSON: mlsCreationResult.metadataReferenceJson
      )
    }

    // Notify observers AFTER state is active
    notifyObservers(.conversationCreated(convo))

    // Track key package consumption if members were added
    if let members = filteredMembers, !members.isEmpty {
      Task {
        do {
          try await keyPackageMonitor?.trackConsumption(
            count: members.count,
            operation: .createConversation,
            context: "Created group '\(name)' with \(members.count) initial members"
          )
          logger.info("📊 Tracked consumption: \(members.count) packages for group creation")

          // Proactive refresh check after consumption
          try await smartRefreshKeyPackages()
        } catch {
          logger.warning("⚠️ Failed to track consumption or refresh: \(error.localizedDescription)")
        }
      }
    }

    logger.info(
      "✅ [MLSConversationManager.createGroup] COMPLETE - convoId: \(groupIdHex), epoch: \(convo.epoch)"
    )
    return convo
  }

  /// Join an existing group using a Welcome message
  /// - Parameter welcomeMessage: Base64-encoded Welcome message
  /// - Returns: Joined conversation view
  public func joinGroup(welcomeMessage: String) async throws -> BlueCatbirdMlsChatDefs.ConvoView {
    logger.info("Joining group from Welcome message")
    try throwIfShuttingDown("joinGroup")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard isInitialized else {
      throw MLSConversationError.contextNotInitialized
    }

    // Decode and process Welcome message
    guard let welcomeData = Data(base64Encoded: welcomeMessage) else {
      throw MLSConversationError.invalidWelcomeMessage
    }

    // Uses mlsDid (device-specific DID) automatically
    let groupId = try await processWelcome(welcomeData: welcomeData)
    logger.debug("Processed Welcome message, group ID: \(groupId)")

    // Fetch conversation details from server
    let conversations = try await apiClient.getConversations(limit: 100)
    guard let convo = conversations.convos.first(where: { $0.groupId == groupId }) else {
      throw MLSConversationError.conversationNotFound
    }

    // Store conversation state
    self.conversations[convo.conversationId] = convo

    // ⭐ CRITICAL FIX: Verify epoch from FFI instead of trusting server's response
    guard let groupIdData = Data(hexEncoded: groupId) else {
      throw MLSConversationError.invalidGroupId
    }

    let serverEpoch = UInt64(convo.epoch)
    let ffiEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)

    if serverEpoch != ffiEpoch {
      logger.warning("⚠️ EPOCH MISMATCH when joining group:")
      logger.warning("   Server reported: \(serverEpoch)")
      logger.warning("   FFI actual: \(ffiEpoch)")
      logger.warning("   Using FFI epoch to prevent state desynchronization")
    }

    groupStates[groupId] = MLSGroupState(
      groupId: groupId,
      convoId: convo.conversationId,
      epoch: ffiEpoch,  // Use FFI epoch, not server epoch
      members: Set(convo.members.map { $0.did.description })
    )

    // Insert history boundary marker so the UI shows "You joined this conversation"
    await insertHistoryBoundaryMarker(
      conversationId: convo.conversationId,
      senderDID: userDid,
      epoch: ffiEpoch,
      contentKey: "history_boundary.new_member"
    )

    // Notify observers
    notifyObservers(.conversationJoined(convo))

    logger.info("Successfully joined conversation: \(convo.conversationId)")
    return convo
  }

  // MARK: - History Boundary Markers

  /// Insert a history boundary system message into the local database.
  /// These markers appear as inline pills in the chat UI to indicate where the user's
  /// message history begins (e.g., after joining or rejoining a conversation).
  ///
  /// - Parameters:
  ///   - conversationId: The group/conversation ID (hex-encoded)
  ///   - senderDID: The DID of the user who triggered the boundary
  ///   - epoch: The MLS epoch at which the boundary occurred
  ///   - contentKey: The system message content key (e.g., "history_boundary.new_member")
  internal func insertHistoryBoundaryMarker(
    conversationId: String,
    senderDID: String,
    epoch: UInt64,
    contentKey: String
  ) async {
    let markerID = "hb-\(conversationId)-\(epoch)"
    let normalizedDID = MLSStorageHelpers.normalizeDID(senderDID)

    let payload = MLSMessagePayload(
      messageType: .system,
      text: contentKey
    )

    guard let payloadData = try? payload.encodeToJSON() else {
      logger.warning("⚠️ Failed to encode history boundary payload")
      return
    }

    let model = MLSMessageModel(
      messageID: markerID,
      currentUserDID: normalizedDID,
      conversationID: conversationId,
      senderID: normalizedDID,
      payloadJSON: payloadData,
      wireFormat: nil,
      contentType: "application/json",
      timestamp: Date(),
      epoch: Int64(epoch),
      sequenceNumber: 0,
      authenticatedData: nil,
      signature: nil,
      isDelivered: true,
      isRead: false,
      isSent: true,
      sendAttempts: 0,
      error: nil,
      processingState: "delivered",
      gapBefore: false,
      payloadExpired: false,
      processingError: nil,
      processingAttempts: 0,
      validationFailureReason: nil
    )

    do {
      try await database.write { db in
        try model.save(db)
      }
      logger.info("📌 Inserted history boundary marker: \(markerID)")
    } catch {
      logger.warning("⚠️ Failed to store history boundary marker: \(error.localizedDescription)")
    }
  }

  /// Remove current user from conversation
  /// - Parameter convoId: Conversation identifier
  public func leaveConversation(convoId: String) async throws {
    logger.info("Leaving conversation: \(convoId)")

    guard let userDid = userDid else {
      throw MLSConversationError.contextNotInitialized
    }

    // Try to get conversation from memory first, or look up from database
    let convo: BlueCatbirdMlsChatDefs.ConvoView
    if let memoryConvo = conversations[convoId] {
      convo = memoryConvo
    } else {
      // Conversation not in memory - check database for zombie/orphan conversations
      let dbConvo = try await database.read { db in
        try MLSConversationModel
          .filter(MLSConversationModel.Columns.conversationID == convoId)
          .filter(MLSConversationModel.Columns.currentUserDID == userDid)
          .fetchOne(db)
      }

      if let dbConvo = dbConvo {
        logger.warning(
          "⚠️ [leaveConversation] Conversation \(convoId.prefix(16))... found in database but not in memory - treating as orphan"
        )
        // This is a zombie/orphan conversation - skip server call and force delete locally
        await forceDeleteConversationLocally(
          convoId: convoId, groupId: dbConvo.groupID.hexEncodedString())
        notifyObservers(.conversationLeft(convoId))
        logger.info(
          "✅ [leaveConversation] Cleaned up orphan conversation: \(convoId.prefix(16))...")
        return
      } else {
        throw MLSConversationError.conversationNotFound
      }
    }

    do {
      _ = try await apiClient.leaveConversation(convoId: convoId)
      logger.info("✅ Left conversation on server: \(convoId)")

      // CRITICAL: Force delete local state after successful server leave
      // This bypasses the conservative reconciliation logic that would otherwise
      // preserve the conversation if the MLS group still exists locally.
      await forceDeleteConversationLocally(convoId: convoId, groupId: convo.groupId)

      // Notify observers
      notifyObservers(.conversationLeft(convoId))

      logger.info("✅ Successfully left and cleaned up conversation: \(convoId)")

    } catch let networkError as NetworkError {
      // If server returns 403 (forbidden) or 404 (not found), the user is already removed
      // from the conversation on the server - clean up local state
      switch networkError {
      case .serverError(let code, _) where code == 403 || code == 404:
        logger.warning(
          "⚠️ [leaveConversation] Server returned \(code) - user already removed, cleaning up locally"
        )
        await forceDeleteConversationLocally(convoId: convoId, groupId: convo.groupId)
        notifyObservers(.conversationLeft(convoId))
        logger.info(
          "✅ [leaveConversation] Cleaned up stale conversation after server \(code): \(convoId.prefix(16))..."
        )
        return
      default:
        logger.error("Failed to leave conversation: \(networkError.localizedDescription)")
        throw MLSConversationError.serverError(networkError)
      }
    } catch {
      logger.error("Failed to leave conversation: \(error.localizedDescription)")
      throw MLSConversationError.serverError(error)
    }
  }

  /// Force delete a conversation from local storage, bypassing reconciliation safeguards.
  /// Use this when:
  /// 1. User explicitly left/deleted the conversation (server confirmed)
  /// 2. User was removed/kicked from the conversation (detected via sync or SSE)
  /// 3. Admin deleted the conversation on the server
  ///
  /// This method:
  /// - Deletes MLS group from OpenMLS storage (even if group exists and is valid)
  /// - Deletes all local database records (conversation, messages, members, epoch keys)
  /// - Removes from in-memory state
  ///
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - groupId: MLS group identifier (hex string)
  internal func forceDeleteConversationLocally(convoId: String, groupId: String) async {
    logger.info(
      "🗑️ [FORCE DELETE] Deleting conversation \(convoId.prefix(16))... from local storage")

    guard let userDid = userDid else {
      logger.error("❌ [FORCE DELETE] No user DID available")
      return
    }

    // Delete MLS group from local OpenMLS storage
    if let groupIdData = Data(hexEncoded: groupId) {
      do {
        try await mlsClient.deleteGroup(for: userDid, groupId: groupIdData)
        logger.info(
          "✅ [FORCE DELETE] Deleted MLS group from local storage: \(groupId.prefix(16))...")
      } catch {
        logger.warning(
          "⚠️ [FORCE DELETE] Failed to delete MLS group \(groupId.prefix(16))...: \(error.localizedDescription)"
        )
        // Continue anyway - we still want to clean up database and memory
      }
    }

    // Delete from database (conversation, messages, members, epoch keys)
    do {
      try await deleteConversationsFromDatabase([convoId])
    } catch {
      logger.error("❌ [FORCE DELETE] Failed to delete from database: \(error.localizedDescription)")
    }

    // Remove from in-memory state
    conversations.removeValue(forKey: convoId)
    groupStates.removeValue(forKey: groupId)

    logger.info("✅ [FORCE DELETE] Completed for conversation: \(convoId.prefix(16))...")
  }

  /// Public method to force delete a broken/stale conversation from local storage.
  /// Use this when:
  /// 1. A conversation is stuck in an invalid state
  /// 2. The server confirmed the conversation no longer exists
  /// 3. The user wants to manually clean up a ghost conversation
  ///
  /// This bypasses all reconciliation safeguards and removes the conversation immediately.
  ///
  /// - Parameter convoId: Conversation identifier to delete
  /// - Note: This does NOT call the server - use leaveConversation() if you want to leave properly
  public func forceDeleteConversation(convoId: String) async {
    logger.warning("⚠️ [FORCE DELETE PUBLIC] Force deleting conversation: \(convoId)")

    let groupId = conversations[convoId]?.groupId ?? convoId
    await forceDeleteConversationLocally(convoId: convoId, groupId: groupId)

    // Notify observers
    notifyObservers(.conversationLeft(convoId))
  }

  /// Publish current GroupInfo to the server
  /// CRITICAL: This function now throws errors - failures will propagate to callers
  /// - Throws: Error if GroupInfo export or upload fails
  internal func publishLatestGroupInfo(
    userDid: String, convoId: String, groupId: Data, context: String
  ) async throws {
    logger.info("📤 [publishLatestGroupInfo] Starting \(context) for convo: \(convoId)")
    try await mlsClient.publishGroupInfo(for: userDid, convoId: convoId, groupId: groupId)
    logger.info("✅ [publishLatestGroupInfo] Success \(context) for convo: \(convoId)")
  }

  // MARK: - Chat Request Management

  /// Accept a pending chat request, moving the conversation to the main inbox.
  /// This is a local-only operation - no server call needed since the E2EE
  /// conversation already exists.
  ///
  /// - Parameter convoId: The conversation ID to accept
  /// - Throws: MLSConversationError if the conversation doesn't exist
  public func acceptConversationRequest(convoId: String) async throws {
    guard let userDid = userDid else {
      throw MLSConversationError.contextNotInitialized
    }
    
    logger.info("✅ Accepting chat request: \(convoId.prefix(16))...")
    
    try await storage.acceptConversationRequest(
      conversationID: convoId,
      currentUserDID: userDid,
      database: database
    )
    
    notifyObservers(.conversationRequestAccepted(convoId))
    logger.info("✅ Chat request accepted, moved to inbox: \(convoId.prefix(16))...")
  }

  /// Decline a pending chat request by leaving the MLS group and deleting local data.
  /// This notifies the server that we've left, effectively declining the request.
  ///
  /// - Parameter convoId: The conversation ID to decline
  /// - Throws: MLSConversationError if the operation fails
  public func declineConversationRequest(convoId: String) async throws {
    logger.info("❌ Declining chat request: \(convoId.prefix(16))...")
    
    // Declining uses the same flow as leaving - we leave the MLS group on the server
    // and delete all local data. The sender will see we've left.
    try await leaveConversation(convoId: convoId)
    
    logger.info("❌ Chat request declined and removed: \(convoId.prefix(16))...")
  }

  /// Fetch all pending inbound chat request conversations
  /// - Returns: Array of conversations that are pending acceptance
  public func fetchPendingRequestConversations() async throws -> [MLSConversationModel] {
    guard let userDid = userDid else {
      throw MLSConversationError.contextNotInitialized
    }
    
    return try await storage.fetchPendingRequestConversations(
      currentUserDID: userDid,
      database: database
    )
  }

  /// Fetch members for a conversation from local storage
  /// - Parameter convoId: The conversation ID
  /// - Returns: Array of member models
  public func fetchConversationMembers(convoId: String) async throws -> [MLSMemberModel] {
    guard let userDid = userDid else {
      throw MLSConversationError.contextNotInitialized
    }

    return try await storage.fetchMembers(
      conversationID: convoId,
      currentUserDID: userDid,
      database: database
    )
  }

  // MARK: - Group Metadata (Encrypted)

  /// Read decrypted group metadata from the MLS group context extension.
  /// Metadata is encrypted with a per-group MEK and never stored in plaintext on the server.
  ///
  /// - Parameter conversationId: Conversation/group ID (hex string)
  /// - Returns: Decoded metadata payload, or nil if no metadata is set
  public func getGroupMetadata(conversationId: String) async throws -> GroupMetadataPayload? {
    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard isInitialized else {
      throw MLSConversationError.contextNotInitialized
    }

    guard let groupIdData = Data(hexEncoded: conversationId) else {
      throw MLSConversationError.invalidGroupId
    }

    return try await mlsClient.getGroupMetadata(for: userDid, groupId: groupIdData)
  }

  /// Update encrypted group metadata in the MLS group context extension.
  /// This creates an MLS commit that re-encrypts the metadata and sends it to the server.
  /// Other group members will receive the updated metadata through the commit.
  ///
  /// - Parameters:
  ///   - conversationId: Conversation/group ID (hex string)
  ///   - name: New group name (nil to leave unchanged)
  ///   - description: New group description (nil to leave unchanged)
  public func updateGroupMetadata(
    conversationId: String,
    name: String?,
    description: String?
  ) async throws {
    logger.info(
      "🔵 [updateGroupMetadata] START - convo: \(conversationId.prefix(16))..., name: \(name ?? "nil")"
    )

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard isInitialized else {
      throw MLSConversationError.contextNotInitialized
    }

    guard let groupIdData = Data(hexEncoded: conversationId) else {
      throw MLSConversationError.invalidGroupId
    }

    // Three-phase sender lifecycle (task #44/#62): stage → POST → confirm.
    // commitGroupChange does NOT echo newEpoch, so we pass the skip-fence
    // sentinel on confirm.

    // 1. Build the GroupMetadataPayload JSON blob (matches what the legacy
    //    `ctx.updateGroupMetadata(groupId:metadataJson:)` was producing
    //    internally via the MLSClient wrapper).
    let payload = GroupMetadataPayload(v: 1, name: name, description: description)
    let metadataJson: Data
    do {
      metadataJson = try JSONEncoder().encode(payload)
    } catch {
      logger.error("❌ [updateGroupMetadata] Failed to encode metadata JSON: \(error.localizedDescription)")
      throw MLSConversationError.operationFailed("Failed to encode metadata JSON")
    }

    // 2. Stage the updateMetadata commit
    let plan = try await mlsClient.stageCommit(
      for: userDid,
      conversationId: conversationId,
      kind: .updateMetadata(groupInfoExtension: metadataJson)
    )

    // 3. Send commit to server via commitGroupChange
    let input = BlueCatbirdMlsChatCommitGroupChange.Input(
      convoId: conversationId,
      action: "updateMetadata",
      commit: Bytes(data: plan.commitBytes)
    )

    let responseCode: Int
    let output: BlueCatbirdMlsChatCommitGroupChange.Output?
    do {
      (responseCode, output) = try await apiClient.client.blue.catbird.mlschat.commitGroupChange(
        input: input
      )
    } catch {
      await mlsClient.discardPending(for: userDid, handle: plan.handle)
      throw error
    }

    guard responseCode == 200, output != nil else {
      logger.error("❌ [updateGroupMetadata] Server rejected commit: HTTP \(responseCode)")
      await mlsClient.discardPending(for: userDid, handle: plan.handle)
      throw MLSConversationError.operationFailed(
        "Server rejected metadata update commit (HTTP \(responseCode))"
      )
    }

    // 4. Confirm commit locally. commitGroupChange does not echo newEpoch,
    //    so fence with the skip sentinel and trust the staged target epoch.
    //    If confirm throws (e.g. EpochMismatch — #63 flat error), the Rust
    //    side leaves the staged commit in place; explicitly discard so we
    //    don't carry stale pending state.
    let confirmed: FfiConfirmedCommit
    do {
      confirmed = try await mlsClient.confirmCommit(
        for: userDid,
        handle: plan.handle,
        serverEpoch: mlsSkipServerEpochFence()
      )
    } catch {
      await mlsClient.discardPending(for: userDid, handle: plan.handle)
      throw error
    }
    let mergedEpoch = confirmed.newEpoch
    logger.info("✅ [updateGroupMetadata] Commit merged - epoch now: \(mergedEpoch)")

    // 5. Update local group state
    groupStates[conversationId]?.epoch = mergedEpoch

    // 6. METADATA: Re-wrap metadata for the new epoch.
    // After a metadata update commit, the metadata must be re-encrypted
    // with the new epoch's key and uploaded to the server.
    if let key = confirmed.metadataKey {
      await reWrapMetadataAfterMerge(
        groupIdHex: conversationId,
        metadataKey: key,
        epoch: mergedEpoch
      )
    }

    // 7. Publish updated GroupInfo for external joins
    try await publishLatestGroupInfo(
      userDid: userDid,
      convoId: conversationId,
      groupId: groupIdData,
      context: "after updateGroupMetadata"
    )

    logger.info("✅ [updateGroupMetadata] COMPLETE - convo: \(conversationId.prefix(16))...")
  }

}
