//
//  MLSConversationManager+DeliveryAcks.swift
//  CatbirdMLSCore
//
//  Delivery ack send/receive and message recovery request logic.
//

import Foundation
import GRDB
import OSLog
import Petrel
import PetrelCatbird

/// Conversations with more than this many members skip delivery acks
/// to avoid SSE amplification storms.
private let defaultDeliveryAckGroupSizeCap = 20

extension MLSConversationManager {

  // MARK: - Fetch

  /// Batch-fetches delivery acks for a set of messageIds in one DB query.
  public func fetchDeliveryAcks(
    messageIds: [String],
    conversationId: String,
    currentUserDID: String
  ) async throws -> [MLSDeliveryAckModel] {
    guard !messageIds.isEmpty else { return [] }
    return try await database.read { db in
      try MLSDeliveryAckModel
        .filter(
          sql: "messageId IN (\(messageIds.map { _ in "?" }.joined(separator: ","))) AND conversationId = ? AND currentUserDID = ?",
          arguments: StatementArguments(messageIds + [conversationId, currentUserDID])
        )
        .fetchAll(db)
    }
  }

  /// Returns the active member count for a conversation (includes sender).
  public func memberCount(for conversationId: String) async -> Int? {
    guard let userDid = userDid else { return nil }
    return try? await database.read { db in
      try MLSMemberModel
        .filter(
          MLSMemberModel.Columns.conversationID == conversationId &&
          MLSMemberModel.Columns.currentUserDID == userDid &&
          MLSMemberModel.Columns.isActive == true
        )
        .fetchCount(db)
    }
  }

  // MARK: - Send

  /// Enqueues an encrypted delivery ack for a message that was successfully decrypted.
  ///
  /// Called immediately after `decryptMessageWithSender` succeeds for a non-self message.
  /// Skips silently if this device has already acked this message (idempotent).
  ///
  /// - Parameters:
  ///   - messageId: The server-assigned messageId that was just decrypted.
  ///   - conversationId: The conversation the message belongs to.
  func enqueueDeliveryAck(messageId: String, conversationId: String) {
    if protocolAuthorityMode == .rustFull {
      logger.info(
        "[MLS-FULL-RUST] Skipping Swift deliveryAck send path; Rust authority owns encrypted control messages for \(conversationId.prefix(16), privacy: .private)"
      )
      return
    }

    guard let userDid = userDid else { return }

    Task { [weak self] in
      guard let self else { return }

      // Skip acks for large groups to prevent SSE amplification.
      if let count = await self.memberCount(for: conversationId), count > defaultDeliveryAckGroupSizeCap {
        return
      }

      // Deduplication: skip if we already acked this message from this device.
      let alreadyAcked: Bool = (try? await self.database.read { db in
        try MLSDeliveryAckModel
          .filter(
            MLSDeliveryAckModel.Columns.messageId == messageId &&
            MLSDeliveryAckModel.Columns.senderDID == userDid &&
            MLSDeliveryAckModel.Columns.currentUserDID == userDid
          )
          .fetchOne(db)
      }) != nil
      guard !alreadyAcked else { return }

      // In-memory dedup gate: prevent concurrent calls for the same messageId from each
      // racing past the DB check (TOCTOU). Insert before sending; removed on SSE echo.
      guard self.pendingDeliveryAcks.insert(messageId).inserted else { return }

      try? await self.sendDeliveryAck(messageId: messageId, conversationId: conversationId, userDid: userDid)
    }
  }

  /// Sends an encrypted `deliveryAck` MLS application message.
  /// Follows the same pre-cache-before-send pattern as `sendEncryptedReaction`.
  private func sendDeliveryAck(messageId: String, conversationId: String, userDid: String) async throws {
    try assertSwiftProtocolMutationAllowed("sendDeliveryAck")
    try throwIfShuttingDown("sendDeliveryAck")

    guard let convo = conversations[conversationId] else { return }
    let groupIdData = convo.coordinates.groupId.data
    let groupIdHex = groupIdData.hexEncodedString()

    try await sendQueueCoordinator.enqueueSend(conversationID: conversationId) { [self] in
      try throwIfShuttingDown("sendDeliveryAck-queued")

      let payload = MLSMessagePayload.deliveryAck(messageId: messageId)
      let payloadData = try payload.encodeToJSON()

      let result = try await groupOperationCoordinator.withExclusiveLock(groupId: groupIdHex) { [self] in
        let localEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
        let tagData = try? await mlsClient.getConfirmationTag(for: userDid, groupId: groupIdData)
        let tagB64 = tagData?.base64EncodedString()
        let ciphertext = try await encryptMessageImpl(groupId: groupIdHex, plaintext: payloadData)
        let paddedSize = ciphertext.count
        let localMsgId = UUID().uuidString

        let optimisticSeq: Int
        if let cursor = try? await storage.fetchLastMessageCursor(
          conversationID: conversationId,
          currentUserDID: userDid,
          database: database
        ) {
          optimisticSeq = Int(cursor.seq) + 1
        } else {
          optimisticSeq = 1
        }

        // Pre-cache before network send so the own SSE echo resolves durably.
        try throwIfShuttingDown("sendDeliveryAck-preCache")
        try await cacheControlMessageEnvelope(
          messageID: localMsgId,
          conversationID: conversationId,
          epoch: Int64(localEpoch),
          seq: Int64(optimisticSeq),
          timestamp: Date(),
          payload: payload,
          senderDID: userDid,
          currentUserDID: userDid,
          processingState: MLSMessageProcessingState.pendingSelfSend
        )

        let sendResult = try await apiClient.sendMessage(
          convoId: conversationId,
          msgId: localMsgId,
          ciphertext: ciphertext,
          epoch: Int(localEpoch),
          paddedSize: paddedSize,
          senderDid: try DID(didString: userDid),
          confirmationTag: tagB64
        )
        return (localMsgId, sendResult)
      }

      let (localMsgId, sendResult) = result
      do {
        try await storage.updateMessageMetadata(
          messageID: localMsgId,
          currentUserDID: userDid,
          epoch: sendResult.epoch,
          sequenceNumber: sendResult.sequenceNumber,
          timestamp: sendResult.receivedAt.date,
          database: database,
          newMessageID: sendResult.messageId
        )
      } catch {
        logger.error(
          "❌ [SEND] PERSISTENCE FAILURE (updateMessageMetadata, delivery-ack-send) for \(conversationId.prefix(16)) msgId=\(localMsgId.prefix(16)): \(error.localizedDescription) — server-confirmed seq/epoch/timestamp NOT persisted locally; pending self-send row not reconciled (E7 write-through violated)"
        )
      }
    }
  }

  // MARK: - Receive

  /// Stores a received delivery ack and notifies the UI.
  /// Called from the `.deliveryAck` case in the message processing switch.
  func handleReceivedDeliveryAck(
    payload: MLSDeliveryAckPayload,
    senderDID: String,
    conversationId: String
  ) async {
    guard let userDid = userDid else { return }

    let ack = MLSDeliveryAckModel(
      messageId: payload.messageId,
      conversationId: conversationId,
      senderDID: senderDID,
      ackedAt: Date(),
      currentUserDID: userDid
    )

    try? await database.write { db in try ack.save(db) }

    // Clear in-memory pending gate now that the ack is durably stored.
    pendingDeliveryAcks.remove(payload.messageId)

    await MainActor.run {
      NotificationCenter.default.post(
        name: Notification.Name("MLSDeliveryAckReceived"),
        object: nil,
        userInfo: [
          "messageId": payload.messageId,
          "conversationId": conversationId,
          "senderDID": senderDID,
        ]
      )
    }
  }

  // MARK: - Recovery Request

  /// Sends an encrypted recovery request for a message this device failed to decrypt.
  /// Called by the message ordering coordinator after its buffer timeout exhausts.
  func sendRecoveryRequest(
    messageId: String,
    epoch: Int64,
    sequenceNumber: Int64,
    conversationId: String
  ) async {
    if protocolAuthorityMode == .rustFull {
      logger.info(
        "[MLS-FULL-RUST] Skipping Swift recoveryRequest send path; Rust authority owns encrypted control messages for \(conversationId.prefix(16), privacy: .private)"
      )
      return
    }

    do {
      try assertSwiftProtocolMutationAllowed("sendRecoveryRequest")
    } catch {
      logger.error(
        "[MLS-FULL-RUST] Blocking Swift recoveryRequest send path: \(error.localizedDescription, privacy: .public)"
      )
      return
    }

    guard let userDid = userDid, let convo = conversations[conversationId] else { return }
    let groupIdData = convo.coordinates.groupId.data
    let groupIdHex = groupIdData.hexEncodedString()

    do {
      try await sendQueueCoordinator.enqueueSend(conversationID: conversationId) { [self] in
        try throwIfShuttingDown("sendRecoveryRequest-queued")

        let payload = MLSMessagePayload.recoveryRequest(
          messageId: messageId,
          epoch: epoch,
          sequenceNumber: sequenceNumber
        )
        let payloadData = try payload.encodeToJSON()

        let result = try await groupOperationCoordinator.withExclusiveLock(groupId: groupIdHex) { [self] in
          let localEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
          let tagData = try? await mlsClient.getConfirmationTag(for: userDid, groupId: groupIdData)
          let tagB64 = tagData?.base64EncodedString()
          let ciphertext = try await encryptMessageImpl(groupId: groupIdHex, plaintext: payloadData)
          let paddedSize = ciphertext.count
          let localMsgId = UUID().uuidString

          let optimisticSeq: Int
          if let cursor = try? await storage.fetchLastMessageCursor(
            conversationID: conversationId,
            currentUserDID: userDid,
            database: database
          ) {
            optimisticSeq = Int(cursor.seq) + 1
          } else {
            optimisticSeq = 1
          }

          try throwIfShuttingDown("sendRecoveryRequest-preCache")
          try await cacheControlMessageEnvelope(
            messageID: localMsgId,
            conversationID: conversationId,
            epoch: Int64(localEpoch),
            seq: Int64(optimisticSeq),
            timestamp: Date(),
            payload: payload,
            senderDID: userDid,
            currentUserDID: userDid,
            processingState: MLSMessageProcessingState.pendingSelfSend
          )

          let sendResult = try await apiClient.sendMessage(
            convoId: conversationId,
            msgId: localMsgId,
            ciphertext: ciphertext,
            epoch: Int(localEpoch),
            paddedSize: paddedSize,
            senderDid: try DID(didString: userDid),
            confirmationTag: tagB64
          )
          return (localMsgId, sendResult)
        }

        let (localMsgId, sendResult) = result
        do {
          try await storage.updateMessageMetadata(
            messageID: localMsgId,
            currentUserDID: userDid,
            epoch: sendResult.epoch,
            sequenceNumber: sendResult.sequenceNumber,
            timestamp: sendResult.receivedAt.date,
            database: database,
            newMessageID: sendResult.messageId
          )
        } catch {
          logger.error(
            "❌ [SEND] PERSISTENCE FAILURE (updateMessageMetadata, recovery-request-send) for \(conversationId.prefix(16)) msgId=\(localMsgId.prefix(16)): \(error.localizedDescription) — server-confirmed seq/epoch/timestamp NOT persisted locally; pending self-send row not reconciled (E7 write-through violated)"
          )
        }
      }
    } catch {
      logger.error("❌ [SEND] Recovery request failed for \(conversationId.prefix(16)): \(error.localizedDescription)")
    }
  }

  /// Handles an incoming recovery request from another group member.
  /// If this device holds the requested plaintext, it re-sends the original content
  /// after a random jitter window (500–2000ms) to avoid duplicate responses.
  func handleRecoveryRequest(
    payload: MLSMessageRecoveryRequestPayload,
    requesterDID: String,
    conversationId: String
  ) async {
    if protocolAuthorityMode == .rustFull {
      logger.info(
        "[MLS-FULL-RUST] Skipping Swift recoveryRequest response path; Rust authority owns encrypted control messages for \(conversationId.prefix(16), privacy: .private)"
      )
      return
    }

    do {
      try assertSwiftProtocolMutationAllowed("handleRecoveryRequest")
    } catch {
      logger.error(
        "[MLS-FULL-RUST] Blocking Swift recoveryRequest response path: \(error.localizedDescription, privacy: .public)"
      )
      return
    }

    guard let userDid = userDid else { return }
    let mlsContext = try? await MLSCoreContext.shared.getContext(for: userDid)
    let currentVerifiedEpoch = conversations[conversationId].map { Int64($0.coordinates.epoch) }

    guard let candidate = await Self.validateAndFetchRecoveryCandidate(
      messageId: payload.messageId,
      conversationId: conversationId,
      requesterDID: requesterDID,
      currentUserDID: userDid,
      mlsContext: mlsContext,
      currentVerifiedEpoch: currentVerifiedEpoch,
      database: database
    ) else { return }
    let originalPayload = candidate.payload

    // Jitter: 500–2000ms random delay before responding.
    // First responder wins; others observe the recovered message arrive on SSE and skip.
    let jitterNs = UInt64.random(in: 500_000_000...2_000_000_000) // nanoseconds
    try? await Task.sleep(nanoseconds: jitterNs)

    // Check if another device already responded (recovered message would reference our messageId).
    let alreadyRecovered = await Self.hasExistingRecoveryResponse(
      forMessageId: payload.messageId,
      conversationId: conversationId,
      currentUserDID: userDid,
      mlsContext: mlsContext,
      database: database
    )
    guard !alreadyRecovered else { return }

    guard let convo = conversations[conversationId] else { return }
    let groupIdData = convo.coordinates.groupId.data
    let groupIdHex = groupIdData.hexEncodedString()

    do {
      try await sendQueueCoordinator.enqueueSend(conversationID: conversationId) { [self] in
        let recoveryPayload = MLSMessagePayload(
          messageType: originalPayload.messageType,
          text: originalPayload.text,
          embed: originalPayload.embed,
          recoveredMessageId: payload.messageId
        )
        let payloadData = try recoveryPayload.encodeToJSON()

        let result = try await groupOperationCoordinator.withExclusiveLock(groupId: groupIdHex) { [self] in
          let localEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
          let tagData = try? await mlsClient.getConfirmationTag(for: userDid, groupId: groupIdData)
          let tagB64 = tagData?.base64EncodedString()
          let ciphertext = try await encryptMessageImpl(groupId: groupIdHex, plaintext: payloadData)
          let paddedSize = ciphertext.count
          let localMsgId = UUID().uuidString

          let optimisticSeq: Int
          if let cursor = try? await storage.fetchLastMessageCursor(
            conversationID: conversationId,
            currentUserDID: userDid,
            database: database
          ) {
            optimisticSeq = Int(cursor.seq) + 1
          } else {
            optimisticSeq = 1
          }

          try throwIfShuttingDown("handleRecoveryRequest-preCache")
          try await cacheControlMessageEnvelope(
            messageID: localMsgId,
            conversationID: conversationId,
            epoch: Int64(localEpoch),
            seq: Int64(optimisticSeq),
            timestamp: Date(),
            payload: recoveryPayload,
            senderDID: userDid,
            currentUserDID: userDid,
            processingState: MLSMessageProcessingState.pendingSelfSend
          )

          let sendResult = try await apiClient.sendMessage(
            convoId: conversationId,
            msgId: localMsgId,
            ciphertext: ciphertext,
            epoch: Int(localEpoch),
            paddedSize: paddedSize,
            senderDid: try DID(didString: userDid),
            confirmationTag: tagB64
          )
          return (localMsgId, sendResult)
        }

        let (localMsgId, sendResult) = result
        do {
          try await storage.updateMessageMetadata(
            messageID: localMsgId,
            currentUserDID: userDid,
            epoch: sendResult.epoch,
            sequenceNumber: sendResult.sequenceNumber,
            timestamp: sendResult.receivedAt.date,
            database: database,
            newMessageID: sendResult.messageId
          )
        } catch {
          logger.error(
            "❌ [SEND] PERSISTENCE FAILURE (updateMessageMetadata, recovery-resend) for \(conversationId.prefix(16)) msgId=\(localMsgId.prefix(16)): \(error.localizedDescription) — server-confirmed seq/epoch/timestamp NOT persisted locally; pending self-send row not reconciled (E7 write-through violated)"
          )
        }
      }
    } catch {
      logger.error("❌ [SEND] Recovery resend failed for \(conversationId.prefix(16)): \(error.localizedDescription)")
    }
  }

  internal static func isValidRecoveryMessageId(_ messageId: String) -> Bool {
    guard !messageId.isEmpty, messageId.count <= 128 else {
      return false
    }
    for scalar in messageId.unicodeScalars {
      // Printable ASCII without whitespace/control characters (0x21...0x7E)
      guard scalar.value >= 0x21, scalar.value <= 0x7E else {
        return false
      }
      // Reject SQL LIKE wildcards and quote/escape characters
      switch scalar {
      case "%", "_", "\"", "\\", "'":
        return false
      default:
        continue
      }
    }
    return true
  }

  internal static func validateAndFetchRecoveryCandidate(
    messageId: String,
    conversationId: String,
    requesterDID: String,
    currentUserDID: String,
    mlsContext: MlsContext?,
    currentVerifiedEpoch: Int64?,
    database: MLSDatabase
  ) async -> (storedMessage: MLSMessageModel, payload: MLSMessagePayload)? {
    guard isValidRecoveryMessageId(messageId), !conversationId.isEmpty else {
      return nil
    }

    let normalizedUserDID = MLSStorageHelpers.normalizeDID(
      MLSCredentialBinding.credentialRootDID(currentUserDID)
    )
    let normalizedRequesterDID = MLSStorageHelpers.normalizeDID(
      MLSCredentialBinding.credentialRootDID(requesterDID)
    )
    guard !normalizedUserDID.isEmpty,
          !normalizedRequesterDID.isEmpty,
          normalizedRequesterDID != normalizedUserDID
    else {
      return nil
    }

    guard let storedMessage = try? await database.read({ db -> MLSMessageModel? in
      guard let storedMessage = try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == messageId)
        .filter(MLSMessageModel.Columns.conversationID == conversationId)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.isTombstone == 0)
        .fetchOne(db),
        storedMessage.conversationID == conversationId
      else {
        return nil
      }

      let storedSenderDID = MLSStorageHelpers.normalizeDID(
        MLSCredentialBinding.credentialRootDID(storedMessage.senderID)
      )
      guard storedSenderDID == normalizedUserDID else {
        return nil
      }

      // Verify requester is currently an active verified member of conversationId.
      let activeMembers = try MLSMemberModel
        .filter(MLSMemberModel.Columns.conversationID == conversationId)
        .filter(MLSMemberModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMemberModel.Columns.isActive == true)
        .fetchAll(db)
      let isCurrentMember = activeMembers.contains { member in
        MLSStorageHelpers.normalizeDID(MLSCredentialBinding.credentialRootDID(member.did))
          == normalizedRequesterDID
      }
      guard isCurrentMember else {
        return nil
      }

      // Verify requester was a member at storedMessage.epoch (prevent pre-join exfiltration).
      let messageEpoch = storedMessage.epoch
      if let snapshot = try MLSRosterSnapshotModel
        .filter(Column("conversationID") == conversationId)
        .filter(Column("epoch") == messageEpoch)
        .fetchOne(db) {
        let wasInSnapshot = snapshot.memberDIDs.contains { did in
          MLSStorageHelpers.normalizeDID(MLSCredentialBinding.credentialRootDID(did))
            == normalizedRequesterDID
        }
        guard wasInSnapshot else {
          return nil
        }
      } else {
        let allEvents = try MLSMembershipEventModel
          .filter(MLSMembershipEventModel.Columns.conversationID == conversationId)
          .filter(MLSMembershipEventModel.Columns.currentUserDID == normalizedUserDID)
          .order(MLSMembershipEventModel.Columns.epoch.asc, MLSMembershipEventModel.Columns.timestamp.asc)
          .fetchAll(db)
          .filter { event in
            MLSStorageHelpers.normalizeDID(MLSCredentialBinding.credentialRootDID(event.memberDID))
              == normalizedRequesterDID
          }

        if !allEvents.isEmpty {
          var activeAtMessageEpoch = false
          var sawJoinOrLeaveBeforeOrAtEpoch = false
          for event in allEvents where event.epoch <= messageEpoch {
            switch event.eventType {
            case .joined:
              activeAtMessageEpoch = true
              sawJoinOrLeaveBeforeOrAtEpoch = true
            case .left, .removed, .kicked:
              activeAtMessageEpoch = false
              sawJoinOrLeaveBeforeOrAtEpoch = true
            case .roleChanged, .deviceAdded, .deviceRemoved:
              break
            }
          }
          // If the earliest recorded join is strictly after messageEpoch, or the member was not active
          // at messageEpoch, refuse recovery.
          if let latestJoinAfter = allEvents.last(where: { $0.eventType == .joined && $0.epoch > messageEpoch }) {
            _ = latestJoinAfter
            if !activeAtMessageEpoch {
              return nil
            }
          }
          guard sawJoinOrLeaveBeforeOrAtEpoch && activeAtMessageEpoch else {
            return nil
          }
        } else {
          // No roster snapshot or membership audit history proves membership at messageEpoch:
          // refuse recovery for epochs prior to the current group state's verified epoch.
          let dbEpoch = try Int64.fetchOne(
            db,
            sql: "SELECT epoch FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
            arguments: [conversationId, normalizedUserDID]
          )
          guard let verifiedFloor = currentVerifiedEpoch ?? dbEpoch,
                messageEpoch >= verifiedFloor
          else {
            return nil
          }
        }
      }

      return storedMessage
    })
    else {
      return nil
    }

    let originalPayload: MLSMessagePayload
    if let encrypted = storedMessage.payloadEncrypted, !encrypted.isEmpty {
      guard let mlsContext,
            let plain = try? MLSFieldEncryption.decrypt(
              context: mlsContext,
              conversationID: storedMessage.cryptoConversationID ?? storedMessage.conversationID,
              wire: encrypted
            ),
            let payload = try? MLSMessagePayload.decodeFromJSON(plain)
      else { return nil }
      originalPayload = payload
    } else if let legacyJSON = storedMessage.payloadJSON,
              let payload = try? MLSMessagePayload.decodeFromJSON(legacyJSON) {
      originalPayload = payload
    } else {
      return nil
    }

    // Only allow recovery of plain text messages (never control messages or chained recoveries).
    guard originalPayload.messageType == .text,
          originalPayload.recoveredMessageId == nil
    else {
      return nil
    }

    return (storedMessage, originalPayload)
  }

  internal static func hasExistingRecoveryResponse(
    forMessageId messageId: String,
    conversationId: String,
    currentUserDID: String,
    mlsContext: MlsContext?,
    database: MLSDatabase
  ) async -> Bool {
    guard isValidRecoveryMessageId(messageId), !conversationId.isEmpty else {
      return true
    }
    let normalizedUserDID = MLSStorageHelpers.normalizeDID(currentUserDID)
    guard let candidates = try? await database.read({ db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationId)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .filter(MLSMessageModel.Columns.isTombstone == 0)
        .fetchAll(db)
    }) else {
      return false
    }

    for candidate in candidates {
      if let mlsContext, let decoded = candidate.decryptedPayload(context: mlsContext) {
        if decoded.recoveredMessageId == messageId {
          return true
        }
      } else if let decoded = candidate.parsedPayload {
        if decoded.recoveredMessageId == messageId {
          return true
        }
      }
    }
    return false
  }

  internal static func isValidIncomingRecoveryResponse(
    recoveredMessageId: String,
    conversationId: String,
    senderDID: String,
    currentUserDID: String,
    database: MLSDatabase
  ) async -> Bool {
    guard isValidRecoveryMessageId(recoveredMessageId), !conversationId.isEmpty else {
      return false
    }
    let normalizedUserDID = MLSStorageHelpers.normalizeDID(currentUserDID)
    let normalizedIncomingSender = MLSStorageHelpers.normalizeDID(
      MLSCredentialBinding.credentialRootDID(senderDID)
    )
    guard !normalizedIncomingSender.isEmpty else {
      return false
    }

    guard let targetMessage = try? await database.read({ db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == recoveredMessageId)
        .filter(MLSMessageModel.Columns.currentUserDID == normalizedUserDID)
        .fetchOne(db)
    }) else {
      return false
    }

    guard targetMessage.conversationID == conversationId else {
      return false
    }

    let normalizedTargetSender = MLSStorageHelpers.normalizeDID(
      MLSCredentialBinding.credentialRootDID(targetMessage.senderID)
    )
    return !normalizedTargetSender.isEmpty && normalizedTargetSender == normalizedIncomingSender
  }
}
