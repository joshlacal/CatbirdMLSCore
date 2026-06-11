import Foundation
import GRDB
import Petrel
import PetrelCatbird

extension MLSConversationManager {
  internal func decideWelcomeRecovery(
    for convo: BlueCatbirdMlsChatDefs.ConvoView,
    failure: WelcomeRecoveryFailure,
    externalCommitAlreadyFailed: Bool = false
  ) async -> WelcomeRecoveryDecision {
    let priorAttempts = await storedWelcomeReissueAttempts(convoId: convo.conversationId)
    let hasAdmin = convo.members.contains { $0.isAdmin }
    let lastSeenEpoch = UInt64(max(convo.epoch, 0))

    return WelcomeRecoveryPolicy.decide(
      welcomeFailure: failure,
      priorReissueAttempts: priorAttempts,
      hasCurrentAdmin: hasAdmin,
      groupInfoUnrecoverable: false,
      externalCommitAlreadyFailed: externalCommitAlreadyFailed,
      lastSeenEpoch: lastSeenEpoch
    )
  }

  internal func requestWelcomeReissueAndWait(
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

    let response = try await apiClient.requestWelcomeReissue(
      convoId: convo.conversationId,
      recipientDeviceDid: recipientDeviceDid,
      reason: reason
    )
    await recordWelcomeReissueAttempt(convoId: convo.conversationId, attempt: nextAttempt)
    logger.info(
      "📨 [WELCOME-RECOVERY] Requested Welcome reissue for \(convo.conversationId.prefix(16)) attempt \(nextAttempt): requested=\(response.requested), recipientDeviceDid=\(recipientDeviceDid.prefix(32))"
    )

    Task.detached(priority: .utility) { [mlsClient, logger, userDid] in
      do {
        _ = try await mlsClient.syncKeyPackageHashes(for: userDid)
      } catch {
        logger.warning(
          "⚠️ [WELCOME-RECOVERY] Failed to sync key package hashes after \(reason): \(error.localizedDescription)"
        )
      }

      do {
        _ = try await mlsClient.monitorAndReplenishBundles(for: userDid)
      } catch {
        logger.warning(
          "⚠️ [WELCOME-RECOVERY] Failed to replenish key packages after \(reason): \(error.localizedDescription)"
        )
      }
    }

    throw MLSConversationError.operationFailed(
      "Welcome reissue requested; waiting for inviter to retry"
    )
  }

  internal func markWelcomeRecoverySurrendered(convoId: String, reason: String) async {
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
