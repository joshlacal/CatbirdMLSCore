import CryptoKit
import Foundation
import GRDB
import OSLog
import Petrel
import Synchronization

public extension MLSConversationManager {
  // MARK: - Member Management

  /// Add members to an existing conversation
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - memberDids: DIDs of members to add
  public func addMembers(convoId: String, memberDids: [String]) async throws {
    logger.info(
      "🔵 [MLSConversationManager.addMembers] START - convoId: \(convoId), members: \(memberDids.count)"
    )
    try throwIfShuttingDown("addMembers")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      logger.error("❌ [MLSConversationManager.addMembers] Conversation not found")
      throw MLSConversationError.conversationNotFound
    }

    guard let groupState = groupStates[convo.groupId] else {
      logger.error("❌ [MLSConversationManager.addMembers] Group state not found")
      throw MLSConversationError.groupStateNotFound
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      logger.error("❌ [MLSConversationManager.addMembers] Invalid groupId")
      throw MLSConversationError.invalidGroupId
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // 🔍 PRE-FLIGHT CHECK: Verify members aren't already in MLS group
    // ═══════════════════════════════════════════════════════════════════════════════
    do {
      let debugInfo = try await mlsClient.debugGroupMembers(for: userDid, groupId: groupIdData)
      let currentMemberDids = debugInfo.members.map {
        String(data: $0.credentialIdentity, encoding: .utf8)?.lowercased() ?? ""
      }

      // Check if any of the members we're trying to add are already in the group
      var alreadyInGroup: [String] = []
      for memberDid in memberDids {
        let normalizedDid = memberDid.lowercased()
        if currentMemberDids.contains(where: {
          $0.contains(normalizedDid) || normalizedDid.contains($0)
        }) {
          alreadyInGroup.append(memberDid)
        }
      }

      if !alreadyInGroup.isEmpty {
        logger.warning(
          "⚠️ [MLSConversationManager.addMembers] PRE-FLIGHT: \(alreadyInGroup.count) member(s) already in MLS group"
        )
        // Update groupStates to reflect actual MLS membership
        var updatedState = groupStates[convo.groupId] ?? groupState
        updatedState.members = Set(currentMemberDids)
        groupStates[convo.groupId] = updatedState
        logger.info(
          "🔄 Synced groupStates.members with MLS FFI state (\(currentMemberDids.count) members)")

        // If ALL members are already in group, throw helpful error
        if alreadyInGroup.count == memberDids.count {
          throw MLSConversationError.operationFailed(
            "All selected members are already in this conversation")
        }

        // Note: if partial membership overlap is common, we should filter existing members and add only new ones.
        throw MLSConversationError.operationFailed(
          "Some members are already in this conversation: \(alreadyInGroup.joined(separator: ", "))"
        )
      }
    } catch let error as MLSConversationError {
      throw error  // Re-throw our own errors
    } catch {
      logger.warning(
        "⚠️ [MLSConversationManager.addMembers] PRE-FLIGHT check failed, proceeding anyway: \(error.localizedDescription)"
      )
    }

    // Convert String DIDs to DID type
    let dids = try memberDids.map { try DID(didString: $0) }

    let maxAttempts = 2
    var keyPackagesWithHashes: [KeyPackageWithHash] = []
    var lastError: Error?

    for attempt in 1...maxAttempts {
      let forceRefresh = attempt > 1
      if forceRefresh {
        for did in dids {
          await keyPackageManager.clearExhaustedKeyPackages(for: did.description)
        }
        logger.info(
          "🔄 [MLSConversationManager.addMembers] Retrying key package fetch with force refresh (attempt \(attempt))"
        )
      }

      let keyPackagesResult = try await apiClient.getKeyPackages(
        dids: dids,
        forceRefresh: forceRefresh
      )

      if let missing = keyPackagesResult.missing, !missing.isEmpty {
        logger.warning("⚠️ [MLSConversationManager.addMembers] Missing key packages: \(missing)")
        if let reason = await describeMissingKeyPackages(
          for: missing,
          reason: "addMembers",
          convoId: convoId
        ) {
          throw MLSConversationError.operationFailed(reason)
        }
        let missingError = MLSConversationError.missingKeyPackages(missing.map { $0.description })
        lastError = missingError
        if attempt < maxAttempts {
          continue
        }
        throw missingError
      }

      let keyPackages = keyPackagesResult.keyPackages
      if keyPackages.isEmpty {
        logger.warning("⚠️ [MLSConversationManager.addMembers] No key packages returned")
        if let reason = await describeMissingKeyPackages(
          for: dids,
          reason: "addMembers",
          convoId: convoId
        ) {
          throw MLSConversationError.operationFailed(reason)
        }
        let emptyError = MLSConversationError.missingKeyPackages(dids.map { $0.description })
        lastError = emptyError
        if attempt < maxAttempts {
          continue
        }
        throw emptyError
      }

      do {
        keyPackagesWithHashes = try await selectKeyPackages(
          for: dids, from: keyPackages, userDid: userDid)
        break
      } catch let error as MLSConversationError {
        if case .missingKeyPackages = error, attempt < maxAttempts {
          lastError = error
          continue
        }
        throw error
      }
    }

    guard !keyPackagesWithHashes.isEmpty else {
      if let reason = await describeMissingKeyPackages(
        for: dids,
        reason: "addMembers",
        convoId: convoId
      ) {
        throw MLSConversationError.operationFailed(reason)
      }
      throw lastError ?? MLSConversationError.missingKeyPackages(dids.map { $0.description })
    }

    // Extract just the data for MLSClient
    let keyPackagesArray = keyPackagesWithHashes.map { $0.data }

    // Use GroupOperationCoordinator to serialize operations on this group
    try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
      try await addMembersImpl(
        convoId: convoId,
        memberDids: memberDids,
        dids: dids,
        userDid: userDid,
        groupIdData: groupIdData,
        groupState: groupState,
        convo: convo,
        keyPackagesArray: keyPackagesArray,
        keyPackagesWithHashes: keyPackagesWithHashes
      )
    }
  }

  /// Internal implementation of addMembers (called within exclusive lock)
  internal func addMembersImpl(
    convoId: String,
    memberDids: [String],
    dids: [DID],
    userDid: String,
    groupIdData: Data,
    groupState: MLSGroupState,
    convo: BlueCatbirdMlsChatDefs.ConvoView,
    keyPackagesArray: [Data],
    keyPackagesWithHashes: [KeyPackageWithHash]
  ) async throws {
    // Three-phase sender lifecycle (task #44/#62): stageCommit → POST →
    // confirmCommit(serverEpoch) on success, discardPending on failure.
    // Handle is declared up here so the outer `catch` can discard if the
    // stage succeeded but a later step threw before we confirmed.
    var stagedHandleForCleanup: FfiStagedCommitHandle?
    do {
      // 0. Clear any stale pending commit from a previous failed operation
      do {
        try await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)
      } catch {
        // Ignore errors
      }

      // 1. Stage commit locally (creates pending outgoing commit — NOT merged)
      let plan = try await mlsClient.stageCommit(
        for: userDid,
        conversationId: convo.groupId,
        kind: .addMembers(memberDids: memberDids, keyPackages: keyPackagesArray)
      )
      stagedHandleForCleanup = plan.handle
      let welcomeData = plan.welcomeBytes ?? Data()

      // 2. Send commit and welcome to server
      // Build key package hash entries for server lifecycle tracking
      let keyPackageHashEntries: [BlueCatbirdMlsChatCommitGroupChange.KeyPackageHashEntry] =
        keyPackagesWithHashes.map { kp in
          BlueCatbirdMlsChatCommitGroupChange.KeyPackageHashEntry(did: kp.did, hash: kp.hash)
        }

      // PHASE 3 FIX: Protect server send + commit merge + state update from cancellation
      let (newEpoch, confirmed) = try await withTaskCancellationHandler {
        // Track this commit as our own to prevent re-processing via SSE
        trackOwnCommit(plan.commitBytes)

        let addMembersResult: (success: Bool, newEpoch: Int)
        do {
          addMembersResult = try await apiClient.addMembers(
            convoId: convoId,
            didList: dids,
            commit: plan.commitBytes,
            welcomeMessage: welcomeData,
            keyPackageHashes: keyPackageHashEntries
          )
        } catch let apiError as MLSAPIError {
          let normalizedError = normalizeKeyPackageError(apiError)
          switch normalizedError {
          case .keyPackageNotFound(let detail):
            await recordKeyPackageFailure(detail: detail)
            do {
              _ = try await apiClient.requestKeyPackageReplenish(
                dids: dids,
                reason: "addMembers",
                convoId: convoId
              )
            } catch {
              logger.warning(
                "⚠️ [MLSConversationManager.addMembers] Failed to request peer key package replenish signal: \(error.localizedDescription)"
              )
            }
            throw MLSConversationError.missingKeyPackages(memberDids)
          case .conversationNotFound:
            throw MLSConversationError.conversationNotFound
          case .notConversationMember:
            throw MLSConversationError.groupNotInitialized
          case .memberAlreadyExists:
            throw MLSConversationError.operationFailed(
              "One or more members are already part of this conversation")
          case .memberBlocked, .mutualBlockDetected:
            throw MLSConversationError.operationFailed(
              "Cannot add members due to Bluesky block relationships")
          case .tooManyMembers:
            throw MLSConversationError.operationFailed(
              "Adding these members would exceed the maximum allowed")
          default:
            throw MLSConversationError.serverError(normalizedError)
          }
        }

        guard addMembersResult.success else {
          throw MLSConversationError.operationFailed("Server rejected member addition")
        }
        let newEpoch = addMembersResult.newEpoch

        // ✅ RATCHET DESYNC FIX: Confirm commit ONLY after server confirmation.
        // Server echoes newEpoch for addMembers — use it to fence against a
        // stale confirm against a different target epoch.
        // Idempotency-hit: server may nil-default newEpoch to 0 when the
        // commit was already applied in a prior attempt (see createGroup
        // fallback at Groups.swift). In that case we cannot fence against
        // the echoed epoch; pass the skip sentinel and trust the staged
        // target_epoch.
        let serverEpochForConfirm: UInt64 =
          newEpoch > 0 ? UInt64(newEpoch) : mlsSkipServerEpochFence()
        let confirmed = try await mlsClient.confirmCommit(
          for: userDid, handle: plan.handle, serverEpoch: serverEpochForConfirm)
        stagedHandleForCleanup = nil  // confirmed — no cleanup needed
        return (newEpoch, confirmed)
      } onCancel: {
        logger.warning(
          "⚠️ [addMembers] Commit operation was cancelled - allowing completion to prevent epoch desync"
        )
      }
      _ = confirmed

      // 3. Update local state
      var updatedState = groupStates[convo.groupId] ?? groupState
      updatedState.epoch = UInt64(newEpoch)
      updatedState.members.formUnion(memberDids)
      groupStates[convo.groupId] = updatedState

      // Publish updated GroupInfo after membership change
      try await publishLatestGroupInfo(
        userDid: userDid,
        convoId: convoId,
        groupId: groupIdData,
        context: "after addMembers"
      )

      // Record membership events in database
      do {
        for did in dids {
          let event = MLSMembershipEventModel(
            conversationID: convoId,
            currentUserDID: userDid,
            memberDID: did.description,
            eventType: .joined,
            epoch: Int64(newEpoch)
          )
          try await storage.recordMembershipEvent(event, database: database)
        }
        try await storage.updateConversationMembershipTimestamp(
          conversationID: convoId, currentUserDID: userDid, database: database)

        for did in dids {
          notifyObservers(.membershipChanged(convoId: convoId, did: did, action: .joined))
        }
      } catch {
        logger.error("Failed to record membership events: \(error.localizedDescription)")
      }

      // Also notify with legacy events
      notifyObservers(.membersAdded(convoId, dids))
      notifyObservers(.epochUpdated(convoId, Int(newEpoch)))

      // Track key package consumption
      Task {
        do {
          try await keyPackageMonitor?.trackConsumption(
            count: memberDids.count,
            operation: .addMembers,
            context: "Added \(memberDids.count) members to conversation \(convoId)"
          )
          try await smartRefreshKeyPackages()
        } catch {
          logger.warning("⚠️ Failed to track consumption or refresh: \(error.localizedDescription)")
        }
      }

      logger.info(
        "✅ [MLSConversationManager.addMembers] COMPLETE - convoId: \(convoId), epoch: \(newEpoch), members: \(updatedState.members.count)"
      )

    } catch {
      logger.error(
        "❌ [MLSConversationManager.addMembers] Error, cleaning up: \(error.localizedDescription)")

      // Task #44/#62: discard the staged commit via its handle if we have
      // one; fall back to clearPendingCommit (OpenMLS-level) defensively to
      // cover pre-stage failures and any stale state.
      if let handle = stagedHandleForCleanup {
        await mlsClient.discardPending(for: userDid, handle: handle)
      }
      do {
        try await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)
      } catch {
        logger.error(
          "❌ [MLSConversationManager.addMembers] Failed to clear pending commit: \(error.localizedDescription)"
        )
      }

      // Unreserve key packages on errors where they weren't actually consumed
      var shouldUnreserve = false

      if case .memberAlreadyInGroup = error as? MLSError {
        shouldUnreserve = true
      } else if case .serverError(let innerError) = error as? MLSConversationError,
        case .httpError(let statusCode, _) = innerError as? MLSAPIError,
        (500...599).contains(statusCode)
      {
        shouldUnreserve = true
      } else if let apiError = error as? MLSAPIError,
        case .httpError(let statusCode, _) = apiError,
        (500...599).contains(statusCode)
      {
        shouldUnreserve = true
      } else if case .operationFailed = error as? MLSError {
        shouldUnreserve = true
      }

      if shouldUnreserve {
        await keyPackageManager.unreserveKeyPackages(keyPackagesWithHashes)
      }

      if case .memberAlreadyInGroup = error as? MLSError {
        throw MLSConversationError.operationFailed(
          "Member is already in this conversation - please refresh the member list")
      }

      throw MLSConversationError.serverError(error)
    }
  }

  /// Remove a member from conversation (admin-only)
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - memberDid: DID of member to remove
  ///   - reason: Optional reason for removal
  public func removeMember(from convoId: String, memberDid: String, reason: String? = nil) async throws {
    logger.info(
      "🔵 [MLSConversationManager.removeMember] START - convoId: \(convoId), memberDid: \(memberDid)"
    )

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      logger.error("❌ [MLSConversationManager.removeMember] Conversation not found")
      throw MLSConversationError.conversationNotFound
    }

    guard let groupIdData = Data(hexEncoded: convo.groupId) else {
      throw MLSConversationError.operationFailed("Failed to decode groupId hex string")
    }

    do {
      try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
        do {
          try await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)
        } catch {
          // Ignore errors
        }

        // Three-phase sender lifecycle (task #44/#62): stage → POST → confirm.
        // The removeMember HTTP endpoint returns only an optional epochHint
        // (no authoritative newEpoch echo), so we fence with the skip sentinel
        // and rely on the staged target_epoch.
        let plan = try await mlsClient.stageCommit(
          for: userDid,
          conversationId: convo.groupId,
          kind: .removeMembers(memberDids: [memberDid])
        )

        let idempotencyKey = UUID().uuidString.lowercased()
        let targetDid = try DID(didString: memberDid)
        let commitBase64 = plan.commitBytes.base64EncodedString()

        let (ok, epochHint): (Bool, Int?)
        do {
          (ok, epochHint) = try await apiClient.removeMember(
            convoId: convoId,
            targetDid: targetDid,
            reason: reason,
            commit: commitBase64,
            idempotencyKey: idempotencyKey
          )
        } catch {
          await mlsClient.discardPending(for: userDid, handle: plan.handle)
          throw error
        }

        guard ok else {
          await mlsClient.discardPending(for: userDid, handle: plan.handle)
          throw MLSConversationError.operationFailed("Server rejected member removal")
        }

        logger.info(
          "🔵 [MLSConversationManager.removeMember] Server authorized removal - epochHint: \(epochHint.map { String($0) } ?? "nil")"
        )

        // Server accepted the removal — confirm the staged commit locally.
        // The removeMember endpoint does not echo a newEpoch, so we pass the
        // skip-fence sentinel and trust the staged target_epoch.
        // If confirm fails, the server-side removal still succeeded; local
        // state will catch up on next sync.
        do {
          _ = try await mlsClient.confirmCommit(
            for: userDid,
            handle: plan.handle,
            serverEpoch: mlsSkipServerEpochFence()
          )
          // Publish fresh GroupInfo so external-join peers see the new epoch.
          try? await mlsClient.publishGroupInfo(
            for: userDid, convoId: convoId, groupId: groupIdData)
        } catch {
          logger.warning(
            "⚠️ [MLSConversationManager.removeMember] Local confirm failed (server removal succeeded, will sync): \(error.localizedDescription)"
          )
          await mlsClient.discardPending(for: userDid, handle: plan.handle)
        }

        let newEpoch = (try? await mlsClient.getEpoch(for: userDid, groupId: groupIdData)) ?? UInt64(epochHint ?? 0)

        // Record membership event
        do {
          let event = MLSMembershipEventModel(
            conversationID: convoId,
            currentUserDID: userDid,
            memberDID: memberDid,
            eventType: .left,
            epoch: Int64(newEpoch)
          )
          try await storage.recordMembershipEvent(event, database: database)
          try await storage.updateConversationMembershipTimestamp(
            conversationID: convoId, currentUserDID: userDid, database: database)
          notifyObservers(.membershipChanged(convoId: convoId, did: targetDid, action: .removed))
        } catch {
          logger.error(
            "Failed to record membership event for removal: \(error.localizedDescription)")
        }

        do {
          try await syncGroupState(for: convoId)
        } catch {
          logger.warning(
            "⚠️ [MLSConversationManager.removeMember] Post-removal sync failed (non-fatal): \(error.localizedDescription)"
          )
        }
      }
    } catch {
      logger.error("❌ [MLSConversationManager.removeMember] Failed: \(error.localizedDescription)")
      throw MLSConversationError.serverError(error)
    }
  }

  /// Promote a member to admin
  public func promoteAdmin(convoId: String, memberDid: String) async throws {
    logger.info(
      "🔵 [MLSConversationManager.promoteAdmin] START - convoId: \(convoId), member: \(memberDid)")
    try throwIfShuttingDown("promoteAdmin")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      throw MLSConversationError.conversationNotFound
    }

    let targetDid = try DID(didString: memberDid)

    try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
      let success = try await apiClient.promoteAdmin(convoId: convoId, targetDid: targetDid)

      guard success else {
        throw MLSConversationError.operationFailed("Server failed to promote admin")
      }

      logger.info("✅ [MLSConversationManager.promoteAdmin] Success")

      try await syncGroupState(for: convoId)

      try await refreshConversationSnapshotAfterAdminRoleChange(
        convoId: convoId,
        groupId: convo.groupId
      )
    }
  }

  /// Demote an admin to member
  public func demoteAdmin(convoId: String, memberDid: String) async throws {
    logger.info(
      "🔵 [MLSConversationManager.demoteAdmin] START - convoId: \(convoId), member: \(memberDid)")
    try throwIfShuttingDown("demoteAdmin")

    guard let userDid = userDid else {
      throw MLSConversationError.noAuthentication
    }

    guard let convo = conversations[convoId] else {
      throw MLSConversationError.conversationNotFound
    }

    let targetDid = try DID(didString: memberDid)

    try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
      let success = try await apiClient.demoteAdmin(convoId: convoId, targetDid: targetDid)

      guard success else {
        throw MLSConversationError.operationFailed("Server failed to demote admin")
      }

      logger.info("✅ [MLSConversationManager.demoteAdmin] Success")

      try await syncGroupState(for: convoId)

      try await refreshConversationSnapshotAfterAdminRoleChange(
        convoId: convoId,
        groupId: convo.groupId
      )
    }
  }

  private func refreshConversationSnapshotAfterAdminRoleChange(
    convoId: String,
    groupId: String
  ) async throws {
    let (convos, _) = try await apiClient.getConversations(limit: 100)

    guard let updatedConvo = convos.first(where: { $0.groupId == groupId }) else {
      logger.warning(
        "⚠️ [MLSConversationManager.refreshConversationSnapshotAfterAdminRoleChange] Conversation refresh missing for \(convoId)"
      )
      return
    }

    try await persistMembersToDatabase([updatedConvo])
    conversations[convoId] = updatedConvo
    notifyObservers(.conversationJoined(updatedConvo))
  }

}
