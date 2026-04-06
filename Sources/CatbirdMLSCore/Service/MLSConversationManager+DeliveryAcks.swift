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
    guard let userDid = userDid else { return }

    Task { [weak self] in
      guard let self else { return }

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
    try throwIfShuttingDown("sendDeliveryAck")

    guard let convo = conversations[conversationId] else { return }
    guard let groupIdData = Data(hexEncoded: convo.groupId) else { return }

    try await sendQueueCoordinator.enqueueSend(conversationID: conversationId) { [self] in
      try throwIfShuttingDown("sendDeliveryAck-queued")

      let payload = MLSMessagePayload.deliveryAck(messageId: messageId)
      let payloadData = try payload.encodeToJSON()

      let result = try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
        let localEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
        let tagData = try? await mlsClient.getConfirmationTag(for: userDid, groupId: groupIdData)
        let tagB64 = tagData?.base64EncodedString()
        let ciphertext = try await encryptMessageImpl(groupId: convo.groupId, plaintext: payloadData)
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

        // Pre-cache BEFORE network send to avoid CannotDecryptOwnMessage race.
        try throwIfShuttingDown("sendDeliveryAck-preCache")
        try await cacheControlMessageEnvelope(
          message: BlueCatbirdMlsChatDefs.MessageView(
            id: localMsgId,
            convoId: conversationId,
            ciphertext: Bytes(data: ciphertext),
            epoch: Int(localEpoch),
            seq: optimisticSeq,
            createdAt: ATProtocolDate(date: Date()),
            messageType: "deliveryAck"
          ),
          payload: payload,
          senderDID: userDid,
          currentUserDID: userDid
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
      try? await storage.updateMessageMetadata(
        messageID: localMsgId,
        currentUserDID: userDid,
        epoch: sendResult.epoch,
        sequenceNumber: sendResult.sequenceNumber,
        timestamp: sendResult.receivedAt.date,
        database: database,
        newMessageID: sendResult.messageId
      )
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
    guard let userDid = userDid, let convo = conversations[conversationId] else { return }
    guard let groupIdData = Data(hexEncoded: convo.groupId) else { return }

    _ = try? await sendQueueCoordinator.enqueueSend(conversationID: conversationId) { [self] in
      try throwIfShuttingDown("sendRecoveryRequest-queued")

      let payload = MLSMessagePayload.recoveryRequest(
        messageId: messageId,
        epoch: epoch,
        sequenceNumber: sequenceNumber
      )
      let payloadData = try payload.encodeToJSON()

      let result = try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
        let localEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
        let tagData = try? await mlsClient.getConfirmationTag(for: userDid, groupId: groupIdData)
        let tagB64 = tagData?.base64EncodedString()
        let ciphertext = try await encryptMessageImpl(groupId: convo.groupId, plaintext: payloadData)
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
          message: BlueCatbirdMlsChatDefs.MessageView(
            id: localMsgId,
            convoId: conversationId,
            ciphertext: Bytes(data: ciphertext),
            epoch: Int(localEpoch),
            seq: optimisticSeq,
            createdAt: ATProtocolDate(date: Date()),
            messageType: "recoveryRequest"
          ),
          payload: payload,
          senderDID: userDid,
          currentUserDID: userDid
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
      try? await storage.updateMessageMetadata(
        messageID: localMsgId,
        currentUserDID: userDid,
        epoch: sendResult.epoch,
        sequenceNumber: sendResult.sequenceNumber,
        timestamp: sendResult.receivedAt.date,
        database: database,
        newMessageID: sendResult.messageId
      )
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
    guard let userDid = userDid else { return }
    guard requesterDID != userDid else { return }

    // Check if we hold the plaintext for the requested message.
    guard let storedMessage = try? await database.read({ db in
      try MLSMessageModel
        .filter(
          MLSMessageModel.Columns.messageID == payload.messageId &&
          MLSMessageModel.Columns.currentUserDID == MLSStorageHelpers.normalizeDID(userDid)
        )
        .fetchOne(db)
    }),
    let payloadJSONData = storedMessage.payloadJSON,
    let originalPayload = try? JSONDecoder().decode(MLSMessagePayload.self, from: payloadJSONData)
    else { return }

    // Jitter: 500–2000ms random delay before responding.
    // First responder wins; others observe the recovered message arrive on SSE and skip.
    let jitterNs = UInt64.random(in: 500_000_000...2_000_000_000) // nanoseconds
    try? await Task.sleep(nanoseconds: jitterNs)

    // Check if another device already responded (recovered message would reference our messageId).
    let alreadyRecovered = (try? await database.read({ db in
      try MLSMessageModel
        .filter(
          sql: "payloadJSON LIKE ?",
          arguments: ["%\"recoveredMessageId\":\"\(payload.messageId)\"%"]
        )
        .filter(MLSMessageModel.Columns.conversationID == conversationId)
        .fetchOne(db)
    })) != nil
    guard !alreadyRecovered else { return }

    guard let convo = conversations[conversationId],
          let groupIdData = Data(hexEncoded: convo.groupId)
    else { return }

    _ = try? await sendQueueCoordinator.enqueueSend(conversationID: conversationId) { [self] in
      let recoveryPayload = MLSMessagePayload(
        messageType: originalPayload.messageType,
        text: originalPayload.text,
        embed: originalPayload.embed,
        recoveredMessageId: payload.messageId
      )
      let payloadData = try recoveryPayload.encodeToJSON()

      let result = try await groupOperationCoordinator.withExclusiveLock(groupId: convo.groupId) { [self] in
        let localEpoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)
        let tagData = try? await mlsClient.getConfirmationTag(for: userDid, groupId: groupIdData)
        let tagB64 = tagData?.base64EncodedString()
        let ciphertext = try await encryptMessageImpl(groupId: convo.groupId, plaintext: payloadData)
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
          message: BlueCatbirdMlsChatDefs.MessageView(
            id: localMsgId,
            convoId: conversationId,
            ciphertext: Bytes(data: ciphertext),
            epoch: Int(localEpoch),
            seq: optimisticSeq,
            createdAt: ATProtocolDate(date: Date()),
            messageType: originalPayload.messageType.rawValue
          ),
          payload: recoveryPayload,
          senderDID: userDid,
          currentUserDID: userDid
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
      try? await storage.updateMessageMetadata(
        messageID: localMsgId,
        currentUserDID: userDid,
        epoch: sendResult.epoch,
        sequenceNumber: sendResult.sequenceNumber,
        timestamp: sendResult.receivedAt.date,
        database: database,
        newMessageID: sendResult.messageId
      )
    }
  }
}
