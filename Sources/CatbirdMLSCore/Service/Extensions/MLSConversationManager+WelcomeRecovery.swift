import CatbirdMLS
import Foundation
import GRDB
import Petrel
import PetrelCatbird

extension MLSConversationManager {
    static func isWelcomeReissueRequestForCurrentDevice(
        recipientDeviceDid: String,
        currentDeviceDid: String
    ) -> Bool {
        let recipient = recipientDeviceDid.trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
        let current = currentDeviceDid.trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
        return !recipient.isEmpty && recipient == current
    }

    /// True when a Welcome-reissue response failed because THIS device genuinely
    /// cannot fulfill it: it does not hold the conversation's group state, so it
    /// can never seal a Welcome (only a current member with the group secrets
    /// can). Such failures must NOT trigger endless re-attempts on this device —
    /// another admin (or this device, once a cursor replay delivers the group)
    /// fulfills it. Transient/networky failures are deliberately excluded so they
    /// still retry.
    static func isWelcomeReissueUnfulfillableHere(_ error: Error) -> Bool {
        switch error {
        case MLSConversationError.conversationNotFound,
             MLSConversationError.groupStateNotFound:
            return true
        default:
            break
        }
        if let mlsError = error as? MlsError {
            switch mlsError {
            case .GroupNotFound:
                return true
            default:
                break
            }
        }
        // Wrapped/rethrown variants surface only through the message.
        let message = error.localizedDescription.lowercased()
        return message.contains("group not found")
            || message.contains("not present in local mls group state")
    }

    public func handleWelcomeReissueRequested(
        event: BlueCatbirdMlsChatSubscribeEvents.WelcomeReissueRequestedEvent
    ) async {
        if protocolAuthorityMode == .rustFull {
            logger.info(
                "🔒 [WELCOME-REISSUE] Skipping Swift Welcome reissue response in rustFull mode for \(event.convoId.prefix(16))"
            )
            return
        }

        let shouldRespond = welcomeReissueResponseState.withLock { handled in
            if handled.contains(event.requestId) {
                return false
            }
            handled.insert(event.requestId)
            return true
        }

        guard shouldRespond else {
            logger.info(
                "📨 [WELCOME-REISSUE] Already handling request \(event.requestId.prefix(16)) for \(event.convoId.prefix(16))"
            )
            return
        }

        do {
            try await respondToWelcomeReissueRequest(
                convoId: event.convoId,
                recipientDeviceDid: event.recipientDeviceDid,
                requestId: event.requestId
            )
        } catch {
            if Self.isWelcomeReissueUnfulfillableHere(error) {
                // W3: this device genuinely cannot fulfill — it does not hold the
                // conversation's group state, so it can never seal a Welcome.
                // Keep the request marked handled so we stop re-attempting on
                // every event (capture Scenario 3b: endless no-op retries on a
                // GroupNotFound admin). Cursor-replay or another admin device
                // that holds the group fulfills it instead.
                logger.warning(
                    "🚫 [WELCOME-REISSUE] This device cannot fulfill reissue \(event.requestId.prefix(16)) for \(event.convoId.prefix(16)) (no local group state) — leaving for another admin: \(error.localizedDescription)"
                )
            } else {
                // Transient failure (network, epoch race, etc.) — allow a retry
                // on the next event by clearing the handled marker.
                welcomeReissueResponseState.withLock { handled in
                    _ = handled.remove(event.requestId)
                }
                logger.warning(
                    "⚠️ [WELCOME-REISSUE] Auto-response failed (will retry) for \(event.convoId.prefix(16)) request \(event.requestId.prefix(16)): \(error.localizedDescription)"
                )
            }
        }
    }

    func respondToWelcomeReissueRequest(
        convoId: String,
        recipientDeviceDid: String,
        requestId: String
    ) async throws {
        if protocolAuthorityMode == .rustFull {
            logger.info(
                "🔒 [WELCOME-REISSUE] Skipping Swift Welcome reissue responder mutation in rustFull mode for \(convoId.prefix(16))"
            )
            return
        }

        try throwIfShuttingDown("respondToWelcomeReissueRequest")

        guard let userDid else {
            throw MLSConversationError.noAuthentication
        }
        guard await ensureActiveAccount(for: userDid, operation: "respondToWelcomeReissueRequest")
        else {
            throw MLSConversationError.noAuthentication
        }

        let recipientUserDid = Self.userDid(fromDeviceQualifiedDid: recipientDeviceDid)
        let currentDeviceDid = await mlsClient.getDeviceInfo(for: userDid)?.mlsDid
        if let currentDeviceDid,
           Self.isWelcomeReissueRequestForCurrentDevice(
               recipientDeviceDid: recipientDeviceDid,
               currentDeviceDid: currentDeviceDid
           )
        {
            logger.info("📨 [WELCOME-REISSUE] Ignoring own reissue request for \(convoId.prefix(16))")
            return
        }
        if currentDeviceDid == nil && recipientUserDid == userDid {
            logger.warning(
                "⚠️ [WELCOME-REISSUE] Ignoring same-user reissue request for \(convoId.prefix(16)) because current device DID is unavailable"
            )
            return
        }

        // W3: the admin may be a legitimate member who simply hasn't loaded this
        // conversation into memory yet (fresh launch / not-yet-synced convo).
        // Hydrate from local DB / server before giving up, so a transient
        // "not synced" state doesn't strand the requester in a reissue loop
        // (capture Scenario 3a: the responder threw "Conversation not found").
        if conversations[convoId] == nil {
            logger.info(
                "📨 [WELCOME-REISSUE] Conversation \(convoId.prefix(16)) not in memory — hydrating before responding"
            )
            _ = await fetchConversationForRejoin(convoId: convoId)
        }

        guard let convo = conversations[convoId] else {
            throw MLSConversationError.conversationNotFound
        }
        guard let groupState = groupStates[convo.groupId] else {
            throw MLSConversationError.groupStateNotFound
        }
        guard let groupIdData = Data(hexEncoded: convo.groupId) else {
            throw MLSConversationError.invalidGroupId
        }

        let recipientDid = try DID(didString: recipientUserDid)
        await keyPackageManager.clearExhaustedKeyPackages(for: recipientUserDid)
        let keyPackagesResult = try await apiClient.getKeyPackages(
            dids: [recipientDid],
            forceRefresh: true
        )
        if let missing = keyPackagesResult.missing, !missing.isEmpty {
            throw MLSConversationError.missingKeyPackages(missing.map { $0.description })
        }
        let selectedPackages = try await selectKeyPackages(
            for: [recipientDid],
            from: keyPackagesResult.keyPackages,
            userDid: userDid
        )
        guard !selectedPackages.isEmpty else {
            throw MLSConversationError.missingKeyPackages([recipientUserDid])
        }

        let debugInfo = try await mlsClient.debugGroupMembers(for: userDid, groupId: groupIdData)
        let removeDids = Self.removeIdentities(
            forRecipientDeviceDid: recipientDeviceDid,
            from: debugInfo.members
        )
        guard !removeDids.isEmpty else {
            throw MLSConversationError.operationFailed(
                "Recipient is not present in local MLS group state"
            )
        }

        let keyPackageData = selectedPackages.map(\.data)
        let keyPackageHashes: [BlueCatbirdMlsChatCommitGroupChange.KeyPackageHashEntry] =
            selectedPackages.map { package in
                BlueCatbirdMlsChatCommitGroupChange.KeyPackageHashEntry(
                    did: package.did,
                    hash: package.hash
                )
            }

        try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
            var stagedHandleForCleanup: FfiStagedCommitHandle?
            do {
                try? await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)

                let plan = try await mlsClient.stageCommit(
                    for: userDid,
                    conversationId: convo.groupId,
                    kind: .swapMembers(
                        removeDids: removeDids,
                        addDids: [recipientUserDid],
                        addKeyPackages: keyPackageData
                    )
                )
                stagedHandleForCleanup = plan.handle
                guard let welcomeData = plan.welcomeBytes, !welcomeData.isEmpty else {
                    throw MLSConversationError.invalidWelcomeMessage
                }

                trackOwnCommit(plan.commitBytes)
                let postCommitGroupInfo = await mlsClient.exportPostCommitGroupInfo(
                    for: userDid,
                    groupId: groupIdData
                )

                let addMembersResult = try await apiClient.addMembers(
                    convoId: convoId,
                    didList: [recipientDid],
                    commit: plan.commitBytes,
                    welcomeMessage: welcomeData,
                    groupInfo: postCommitGroupInfo,
                    keyPackageHashes: keyPackageHashes,
                    idempotencyKey: requestId
                )

                guard addMembersResult.success else {
                    throw MLSConversationError.operationFailed("Server rejected Welcome reissue response")
                }

                let serverEpochForConfirm: UInt64 =
                    addMembersResult.newEpoch > 0
                        ? UInt64(addMembersResult.newEpoch)
                        : mlsSkipServerEpochFence()
                let confirmed = try await mlsClient.confirmCommit(
                    for: userDid,
                    handle: plan.handle,
                    serverEpoch: serverEpochForConfirm
                )
                stagedHandleForCleanup = nil

                var updatedState = groupStates[convo.groupId] ?? groupState
                updatedState.epoch = confirmed.newEpoch
                updatedState.members.insert(recipientUserDid)
                groupStates[convo.groupId] = updatedState

                do {
                    try await publishLatestGroupInfo(
                        userDid: userDid,
                        convoId: convoId,
                        groupId: groupIdData,
                        context: "after Welcome reissue response"
                    )
                } catch {
                    logger.warning(
                        "⚠️ [WELCOME-REISSUE] Post-response GroupInfo publish failed after server accepted commit: \(error.localizedDescription)"
                    )
                }

                logger.info(
                    "✅ [WELCOME-REISSUE] Auto-responded to request \(requestId.prefix(16)) for \(recipientUserDid.prefix(20)) in \(convoId.prefix(16))"
                )
            } catch {
                if let stagedHandleForCleanup {
                    await mlsClient.discardPending(for: userDid, handle: stagedHandleForCleanup)
                }
                try? await mlsClient.clearPendingCommit(for: userDid, groupId: groupIdData)
                await keyPackageManager.unreserveKeyPackages(selectedPackages)
                throw error
            }
        }
    }

    func decideWelcomeRecovery(
        for convo: BlueCatbirdMlsChatDefs.ConvoView,
        failure: WelcomeRecoveryFailure,
        externalCommitAlreadyFailed: Bool = false
    ) async -> WelcomeRecoveryDecision {
        let priorAttempts = await storedWelcomeReissueAttempts(convoId: convo.conversationId)
        let hasAdmin = convo.members.contains { $0.isAdmin }
        let currentUserDid = userDid.map { Self.userDid(fromDeviceQualifiedDid: $0.lowercased()) }
        let serverListsCurrentUserAsMember = currentUserDid.map { normalizedUserDid in
            convo.members.contains { member in
                let memberUserDid = Self.userDid(
                    fromDeviceQualifiedDid: member.userDid.didString().lowercased()
                )
                let memberDid = Self.userDid(
                    fromDeviceQualifiedDid: member.did.description.lowercased()
                )
                return memberUserDid == normalizedUserDid || memberDid == normalizedUserDid
            }
        } ?? false
        let lastSeenEpoch = UInt64(max(convo.epoch, 0))
        logger.info(
            "🧭 [WELCOME-RECOVERY] Decision context for \(convo.conversationId.prefix(16)): failure=\(failure.reason), priorAttempts=\(priorAttempts), serverListsCurrentUserAsMember=\(serverListsCurrentUserAsMember), hasAdmin=\(hasAdmin), memberCount=\(convo.members.count), epoch=\(lastSeenEpoch)"
        )

        return WelcomeRecoveryPolicy.decide(
            welcomeFailure: failure,
            priorReissueAttempts: priorAttempts,
            hasCurrentAdmin: hasAdmin,
            serverListsCurrentUserAsMember: serverListsCurrentUserAsMember,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: externalCommitAlreadyFailed,
            lastSeenEpoch: lastSeenEpoch
        )
    }

    func requestWelcomeReissueAndWait(
        convo: BlueCatbirdMlsChatDefs.ConvoView,
        reason: String,
        nextAttempt: Int
    ) async throws {
        guard let userDid else {
            throw MLSConversationError.noAuthentication
        }

        let recipientDeviceDid: String
        if let deviceInfo = await mlsClient.getDeviceInfo(for: userDid) {
            recipientDeviceDid = deviceInfo.mlsDid
        } else {
            recipientDeviceDid = try await mlsClient.ensureDeviceRegistered(userDid: userDid)
        }

        try await prepareKeyPackagesForWelcomeReissue(userDid: userDid, reason: reason)

        let response = try await apiClient.requestWelcomeReissue(
            convoId: convo.conversationId,
            recipientDeviceDid: recipientDeviceDid,
            reason: reason
        )
        await recordWelcomeReissueAttempt(convoId: convo.conversationId, attempt: nextAttempt)
        logger.info(
            "📨 [WELCOME-RECOVERY] Requested Welcome reissue for \(convo.conversationId.prefix(16)) attempt \(nextAttempt): requested=\(response.requested), recipientDeviceDid=\(recipientDeviceDid.prefix(32))"
        )

        throw MLSConversationError.operationFailed(
            "Welcome reissue requested; waiting for inviter to retry"
        )
    }

    private func prepareKeyPackagesForWelcomeReissue(userDid: String, reason: String) async throws {
        do {
            let sync = try await mlsClient.syncKeyPackageHashes(for: userDid)
            logger.info(
                "🔑 [WELCOME-RECOVERY] Synced key packages before reissue (\(reason)): remainingAvailable=\(sync.remainingAvailable), orphaned=\(sync.orphanedCount), deleted=\(sync.deletedCount)"
            )
        } catch {
            logger.warning(
                "⚠️ [WELCOME-RECOVERY] Key package hash sync before reissue failed (\(reason)): \(error.localizedDescription)"
            )
        }

        do {
            let replenish = try await mlsClient.monitorAndReplenishBundles(for: userDid)
            logger.info(
                "🔑 [WELCOME-RECOVERY] Prepared key packages before reissue (\(reason)): available=\(replenish.available), uploaded=\(replenish.uploaded)"
            )
        } catch {
            logger.warning(
                "⚠️ [WELCOME-RECOVERY] Failed to prepare key packages before reissue (\(reason)): \(error.localizedDescription)"
            )
            throw error
        }
    }

    func markWelcomeRecoverySurrendered(convoId: String, reason: String) async {
        guard let userDid else { return }

        do {
            let now = Date()
            try await database.write { db in
                try db.execute(
                    sql: """
                    UPDATE MLSConversationModel
                    SET isUnrecoverable = 1,
                        needsRejoin = 0,
                        lastRecoveryAttempt = ?,
                        updatedAt = ?
                    WHERE conversationID = ?
                      AND currentUserDID = ?
                    """,
                    arguments: [now, now, convoId, userDid]
                )
            }
            logger.warning(
                "🛑 [WELCOME-RECOVERY] Surrendered \(convoId.prefix(16)): \(reason)"
            )
        } catch {
            logger.warning(
                "⚠️ [WELCOME-RECOVERY] Failed to persist surrender for \(convoId.prefix(16)): \(error.localizedDescription)"
            )
        }
    }

    private func storedWelcomeReissueAttempts(convoId: String) async -> Int {
        guard let userDid else { return 0 }

        do {
            return try await database.read { db in
                try MLSConversationModel
                    .filter(MLSConversationModel.Columns.conversationID == convoId)
                    .filter(MLSConversationModel.Columns.currentUserDID == userDid)
                    .fetchOne(db)?
                    .consecutiveFailures ?? 0
            }
        } catch {
            logger.warning(
                "⚠️ [WELCOME-RECOVERY] Failed to read reissue attempts for \(convoId.prefix(16)): \(error.localizedDescription)"
            )
            return 0
        }
    }

    private static func userDid(fromDeviceQualifiedDid did: String) -> String {
        did.split(separator: "#", maxSplits: 1).first.map(String.init) ?? did
    }

    static func removeIdentities(
        forRecipientDeviceDid recipientDeviceDid: String,
        from members: [GroupMemberDebugInfo]
    ) -> [String] {
        let requestedLower = recipientDeviceDid.lowercased()
        let recipientUserDid = userDid(fromDeviceQualifiedDid: requestedLower)
        let recipientLower = recipientUserDid.lowercased()
        let isDeviceQualified = requestedLower.contains("#")
        var seen = Set<String>()
        var identities: [String] = []

        for member in members {
            guard let identity = String(data: member.credentialIdentity, encoding: .utf8) else {
                continue
            }
            let normalized = identity.lowercased()
            let identityUser = userDid(fromDeviceQualifiedDid: normalized)
            guard identityUser == recipientLower else { continue }
            if seen.insert(identity).inserted {
                identities.append(identity)
            }
        }

        if isDeviceQualified {
            let exactDeviceMatches = identities.filter { $0.lowercased() == requestedLower }
            if !exactDeviceMatches.isEmpty {
                return exactDeviceMatches
            }

            return identities.filter { identity in
                identity.lowercased() == recipientLower
            }
        }

        return identities
    }

    private func recordWelcomeReissueAttempt(convoId: String, attempt: Int) async {
        guard let userDid else { return }

        do {
            try await database.write { db in
                if var conversation = try MLSConversationModel
                    .filter(MLSConversationModel.Columns.conversationID == convoId)
                    .filter(MLSConversationModel.Columns.currentUserDID == userDid)
                    .fetchOne(db)
                {
                    conversation = conversation.withRecoveryState(
                        lastRecoveryAttempt: Date(),
                        consecutiveFailures: attempt
                    )
                    try conversation.update(db)
                }
            }
        } catch {
            logger.warning(
                "⚠️ [WELCOME-RECOVERY] Failed to persist reissue attempt for \(convoId.prefix(16)): \(error.localizedDescription)"
            )
        }
    }
}
