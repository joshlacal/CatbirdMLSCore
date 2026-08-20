//
//  MLSOrchestratorAPIAdapter.swift
//  CatbirdMLSCore
//
//  Bridges the Rust orchestrator's synchronous UniFFI API callback surface to
//  Catbird's async Petrel MLS API client.
//

import CatbirdMLS
import CryptoKit
import Foundation
import OSLog
import Petrel
import PetrelCatbird

public final class MLSOrchestratorAPIAdapter: OrchestratorApiCallback, @unchecked Sendable {
  private let apiClient: MLSAPIClient
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "OrchestratorAPIAdapter")

  private static let iso8601Formatter: ISO8601DateFormatter = {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return formatter
  }()

  public init(apiClient: MLSAPIClient) {
    self.apiClient = apiClient
  }

  public func isAuthenticatedAs(did: String) -> Bool {
    (try? blocking { await self.apiClient.isAuthenticatedAs(did) }) ?? false
  }

  public func currentDid() -> String? {
    try? blocking { await self.apiClient.authenticatedUserDID() }
  }

  public func getConversations(limit: UInt32, cursor: String?) throws -> FfiConversationListPage {
    let result = try blocking {
      try await self.apiClient.getCanonicalConversationStates(limit: Int(limit), cursor: cursor)
    }
    return FfiConversationListPage(
      conversations: result.states.map(Self.conversationView),
      cursor: result.cursor
    )
  }

  public func createConversation(
    groupId: String,
    initialMembers: [String]?,
    metadataName _: String?,
    metadataDescription _: String?,
    commitData: Data?,
    welcomeData: Data?
  ) throws -> FfiCreateConversationResult {
    let members = try initialMembers?.map { try DID(didString: $0) }
    let convo = try blocking {
      try await self.apiClient.createConversation(
        groupId: groupId,
        cipherSuite: "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
        initialMembers: members,
        welcomeMessage: welcomeData,
        groupInfo: commitData
      )
    }
    return FfiCreateConversationResult(
      conversation: Self.conversationView(convo),
      commitData: commitData,
      welcomeData: welcomeData
    )
  }

  public func leaveConversation(convoId: String) throws {
    _ = try blocking { try await self.apiClient.leaveConversation(convoId: convoId) }
  }

  public func addMembers(
    convoId: String,
    memberDids: [String],
    commitData: Data,
    welcomeData: Data?
  ) throws -> FfiAddMembersResult {
    let dids = try memberDids.map { try DID(didString: $0) }
    let result = try blocking {
      try await self.apiClient.addMembers(
        convoId: convoId,
        didList: dids,
        commit: commitData,
        welcomeMessage: welcomeData
      )
    }
    return FfiAddMembersResult(
      success: result.success,
      newEpoch: UInt64(clamping: result.newEpoch),
      receipt: nil
    )
  }

  public func addMembersWithIdempotency(
    convoId: String,
    memberDids: [String],
    commitData: Data,
    welcomeData: Data?,
    idempotencyKey: String
  ) throws -> FfiAddMembersResult {
    let dids = try memberDids.map { try DID(didString: $0) }
    let result = try blocking {
      try await self.apiClient.addMembers(
        convoId: convoId,
        didList: dids,
        commit: commitData,
        welcomeMessage: welcomeData,
        idempotencyKey: idempotencyKey
      )
    }
    return FfiAddMembersResult(
      success: result.success,
      newEpoch: UInt64(clamping: result.newEpoch),
      receipt: nil
    )
  }

  public func removeMembers(convoId: String, memberDids: [String], commitData: Data) throws {
    for memberDid in memberDids {
      let did = try DID(didString: memberDid)
      _ = try blocking {
        try await self.apiClient.removeMember(
          convoId: convoId,
          targetDid: did,
          commit: commitData.base64EncodedString()
        )
      }
    }
  }

  public func sendMessage(
    convoId: String, ciphertext: Data, epoch: UInt64, msgId: String
  ) throws -> FfiSendMessageResponse {
    guard let didString = currentDid() else {
      throw OrchestratorBridgeError.NotAuthenticated
    }
    let did = try DID(didString: didString)
    let result = try blocking {
      try await self.apiClient.sendMessage(
        convoId: convoId,
        msgId: msgId,
        ciphertext: ciphertext,
        epoch: Int(clamping: epoch),
        paddedSize: ciphertext.count,
        senderDid: did
      )
    }
    return FfiSendMessageResponse(
      messageId: result.messageId,
      seq: UInt64(clamping: result.sequenceNumber),
      epoch: UInt64(clamping: result.epoch)
    )
  }

  public func getMessages(
    convoId: String,
    cursor: String?,
    limit: UInt32,
    messageType: String?,
    fromEpoch: UInt32?,
    toEpoch: UInt32?
  ) throws -> FfiMessagesPage {
    let result = try blocking {
      let page = try await self.apiClient.getCanonicalMessagePage(
        conversationId: convoId,
        afterSeq: cursor.flatMap(Int.init) ?? 0,
        limit: Int(limit),
        messageType: messageType
      )
      let messages = page.messages.filter { (message: BlueCatbirdChatDefs.ApplicationEntry) -> Bool in
        return true
      }
      return (messages: messages, lastSeq: page.lastSeq)
    }
    return FfiMessagesPage(
      envelopes: result.messages.map(Self.incomingEnvelope),
      cursor: result.lastSeq.map(String.init)
    )
  }

  public func publishKeyPackage(
    keyPackage: Data,
    cipherSuite: String,
    expiresAt: String,
    deviceId: String?
  ) throws {
    let expires = Self.iso8601Formatter.date(from: expiresAt).map(ATProtocolDate.init(date:))
    try blocking {
      // Pure relay: Rust owns the device identity and hands us the deviceId to
      // scope this publish. The server rejects an unscoped publish (403) for a
      // fresh device, so forward whatever Rust resolved — no Swift identity logic.
      try await self.apiClient.publishKeyPackage(
        keyPackage: keyPackage,
        cipherSuite: cipherSuite,
        expiresAt: expires,
        deviceId: deviceId
      )
    }
  }

  public func publishKeyPackages(
    keyPackages: [Data],
    cipherSuite: String,
    expiresAt: String,
    deviceId: String?
  ) throws {
    let expires = Self.iso8601Formatter.date(from: expiresAt).map(ATProtocolDate.init(date:))
    try blocking {
      // Batched relay: the Rust orchestrator generates the whole replenishment
      // batch and hands it over in one call so we POST it as a single request
      // (server cap 100). Same device-scoping rules as the single publish.
      try await self.apiClient.publishKeyPackages(
        keyPackages: keyPackages,
        cipherSuite: cipherSuite,
        expiresAt: expires,
        deviceId: deviceId
      )
    }
  }

  public func getKeyPackages(actorDeviceId _: String, dids: [String]) throws -> [FfiKeyPackageRef] {
    let didObjects = try dids.map { try DID(didString: $0) }
    let result = try blocking {
      try await self.apiClient.getKeyPackages(dids: didObjects, forceRefresh: true)
    }
    return result.keyPackages.map(Self.keyPackageRef)
  }

  public func getKeyPackageStats() throws -> FfiKeyPackageStats {
    let output = try blocking { try await self.apiClient.getKeyPackageStats() }
    return FfiKeyPackageStats(
      available: UInt32(clamping: output.available),
      total: UInt32(clamping: output.total)
    )
  }

  public func syncKeyPackages(localHashes: [String], deviceId: String) throws -> FfiKeyPackageSyncResult {
    let result = try blocking {
      try await self.apiClient.syncKeyPackages(localHashes: localHashes, deviceId: deviceId)
    }
    return FfiKeyPackageSyncResult(
      orphanedCount: UInt32(clamping: result.orphanedCount),
      deletedCount: UInt32(clamping: result.deletedCount)
    )
  }

  public func registerDevice(
    deviceUuid: String,
    deviceName: String,
    mlsDid: String,
    signatureKey: Data,
    keyPackages: [Data],
    preparedRequestBody: Data
  ) throws -> FfiDeviceInfo {
    let output = try blocking {
      let items = keyPackages.map { packageData -> BlueCatbirdChatDefs.KeyPackageArtifact in
        let packageBytes = Bytes(data: packageData)
        let sha256Bytes = Bytes(data: Data(SHA256.hash(data: packageData)))
        return BlueCatbirdChatDefs.KeyPackageArtifact(
          framing: "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
          contentType: "application/mls-keypackage",
          bytes: packageBytes,
          sha256: sha256Bytes,
          keyPackageRef: sha256Bytes
        )
      }
      let capability = BlueCatbirdChatDefs.DeviceCapability(
        protocolVersion: .value_1,
        mlsVersion: "1.0",
        cipherSuite: .value_MLS_u5f_256_u5f_XWING_u5f_CHACHA20POLY1305_u5f_SHA256_u5f_Ed25519,
        credentialType: "basic",
        addByValue: "supported",
        updatePath: "supported",
        removeByValue: "supported",
        ratchetTreeGroupInfo: "supported",
        externalPubGroupInfo: "supported",
        applicationFrameProfile: "supported",
        controlProfile: "supported",
        attachmentProfile: "supported",
        metadataProfile: "supported",
        typingProfile: "supported"
      )
      let input = BlueCatbirdChatEnrollDevice.Input(
        signedRequest: BlueCatbirdChatDefs.SignedDeviceEnrollment(
          body: .blueCatbirdChatDefsDeviceEnrollmentBody(
            BlueCatbirdChatDefs.DeviceEnrollmentBody(
              signatureDomain: "blue.catbird.chat",
              actorDid: (try? DID(didString: mlsDid)) ?? (try! DID(didString: "did:plc:placeholder")),
              deviceId: deviceUuid,
              deviceName: deviceName,
              keyId: "k0",
              signaturePublicKey: Bytes(data: signatureKey),
              dpopJkt: "",
              expectedAuthGeneration: 0,
              capability: capability,
              keyPackages: items,
              idempotencyKey: UUID().uuidString,
              signedAt: BlueCatbirdChatDefs.CanonicalDatetime(date: Date())
            )
          ),
          signature: Bytes(data: Data())
        )
      )
      let (responseCode, output) = try await self.apiClient.client.blue.catbird.chat
        .enrollDevice(input: input)
      guard responseCode == 200, let output else {
        throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to register device")
      }
      return output
    }
    return FfiDeviceInfo(
      deviceId: output.device.deviceId,
      mlsDid: mlsDid,
      deviceUuid: deviceUuid,
      createdAt: Self.iso8601Formatter.string(from: output.device.createdAt.date)
    )
  }

  public func listDevices(actorDeviceId _: String) throws -> [FfiDeviceInfo] {
    let output = try blocking {
      let input = BlueCatbirdChatGetOwnDevices.Parameters()
      let (responseCode, output) = try await self.apiClient.client.blue.catbird.chat
        .getOwnDevices(input: input)
      guard responseCode == 200, let output else {
        throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to list devices")
      }
      return output
    }
    return output.items.map { item in
      FfiDeviceInfo(
        deviceId: item.device.deviceId,
        mlsDid: "",
        deviceUuid: item.device.deviceId,
        createdAt: Self.iso8601Formatter.string(from: item.device.createdAt.date)
      )
    }
  }

  public func removeDevice(deviceId: String) throws {
    guard let did = currentDid() else {
      throw OrchestratorBridgeError.NotAuthenticated
    }
    try blocking {
      let input = BlueCatbirdChatRevokeDevice.Input(
        signedRequest: BlueCatbirdChatDefs.SignedDeviceRevocation(
          body: .blueCatbirdChatDefsDeviceRevocationBody(
            BlueCatbirdChatDefs.DeviceRevocationBody(
              signatureDomain: "blue.catbird.chat",
              actorDid: (try? DID(didString: did)) ?? (try! DID(didString: "did:plc:placeholder")),
              actorDeviceId: deviceId,
              keyId: "k0",
              authGeneration: 1,
              targetDeviceId: deviceId,
              targetAuthGeneration: 1,
              idempotencyKey: UUID().uuidString,
              signedAt: BlueCatbirdChatDefs.CanonicalDatetime(date: Date())
            )
          ),
          signature: Bytes(data: Data())
        )
      )
      let (responseCode, _) = try await self.apiClient.client.blue.catbird.chat
        .revokeDevice(input: input)
      guard responseCode == 200 else {
        throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to remove device")
      }
    }
  }

  public func publishGroupInfo(convoId: String, groupInfo: Data) throws {
    try blocking {
      try await self.apiClient.updateGroupInfo(
        convoId: convoId,
        groupInfo: groupInfo,
        epoch: 0,
        verifyUpload: false
      )
    }
  }

  public func getGroupInfo(convoId: String) throws -> Data {
    try blocking {
      let result = try await self.apiClient.getGroupInfo(convoId: convoId)
      return result.groupInfo
    }
  }

  public func getWelcome(convoId: String) throws -> Data {
    try blocking { try await self.apiClient.getWelcome(convoId: convoId) }
  }

  public func requestWelcomeReissue(
    convoId: String,
    recipientDeviceDid: String,
    reason: String
  ) throws {
    _ = try blocking {
      try await self.apiClient.requestWelcomeReissue(
        convoId: convoId,
        recipientDeviceDid: recipientDeviceDid,
        reason: reason
      )
    }
  }

  public func processExternalCommit(
    convoId: String,
    commitData: Data,
    groupInfo: Data?,
    confirmationTag: String?
  ) throws -> FfiProcessExternalCommitResult {
    let result = try blocking {
      try await self.apiClient.processExternalCommit(
        convoId: convoId,
        externalCommit: commitData,
        groupInfo: groupInfo,
        confirmationTag: confirmationTag
      )
    }
    return FfiProcessExternalCommitResult(
      epoch: UInt64(clamping: result.newEpoch),
      rejoinedAt: Self.iso8601Formatter.string(from: Date()),
      receipt: nil
    )
  }

  public func reportRecoveryFailure(
    convoId: String,
    failureType: String,
    epochAuthenticator: String?,
    failureMode: String?
  ) throws {
    _ = try blocking {
      try await self.apiClient.reportRecoveryFailure(
        convoId: convoId,
        failureType: failureType,
        failureMode: failureMode,
        epochAuthenticator: epochAuthenticator
      )
    }
  }

  public func putGroupMetadataBlob(
    convoId: String,
    groupIdHex: String,
    blobLocator: String,
    ciphertext: Data,
    kind: String,
    metadataVersion: UInt64,
    resetGeneration: Int32?
  ) throws {
    _ = try blocking {
      try await self.apiClient.putGroupMetadataBlob(
        blobLocator: blobLocator,
        groupId: groupIdHex,
        conversationId: convoId,
        resetGeneration: resetGeneration.map { Int($0) },
        metadataVersion: metadataVersion,
        kind: kind,
        encryptedBlob: ciphertext
      )
    }
  }

  public func getGroupMetadataBlob(
    convoId: String,
    groupIdHex: String,
    blobLocator: String
  ) throws -> Data {
    try blocking {
      try await self.apiClient.getGroupMetadataBlob(
        blobLocator: blobLocator,
        groupId: groupIdHex,
        conversationId: convoId,
        kind: "metadata"
      )
    }
  }

  public func commitGroupChange(
    convoId: String,
    commitData: Data,
    action: String,
    confirmationTag: String?
  ) throws {
    _ = try blocking {
      try await self.apiClient.commitGroupChange(
        convoId: convoId,
        action: action,
        commit: commitData,
        confirmationTag: confirmationTag
      )
    }
  }

  private func blocking<T>(_ operation: @escaping () async throws -> T) throws -> T {
    let semaphore = DispatchSemaphore(value: 0)
    let lock = NSLock()
    var result: Result<T, Error>?

    Task.detached {
      let operationResult: Result<T, Error>
      do {
        operationResult = .success(try await operation())
      } catch {
        operationResult = .failure(Self.bridgeError(from: error))
      }
      lock.lock()
      result = operationResult
      lock.unlock()
      semaphore.signal()
    }

    semaphore.wait()
    lock.lock()
    let finalResult = result
    lock.unlock()

    switch finalResult {
    case .success(let value):
      return value
    case .failure(let error):
      throw error
    case .none:
      throw OrchestratorBridgeError.Api(message: "API callback completed without a result")
    }
  }

  private static func bridgeError(from error: Error) -> Error {
    if let bridgeError = error as? OrchestratorBridgeError {
      return bridgeError
    }
    if let apiError = error as? MLSAPIError {
      switch apiError {
      case .noAuthentication:
        return OrchestratorBridgeError.NotAuthenticated
      case .conversationNotFound(let detail):
        return OrchestratorBridgeError.ServerError(status: 404, body: detail ?? apiError.localizedDescription)
      case .rateLimited:
        return OrchestratorBridgeError.ServerError(status: 429, body: apiError.localizedDescription)
      case .httpError(let statusCode, let message):
        return OrchestratorBridgeError.ServerError(status: UInt16(clamping: statusCode), body: message)
      default:
        return OrchestratorBridgeError.Api(message: apiError.localizedDescription)
      }
    }
    return OrchestratorBridgeError.Api(message: error.localizedDescription)
  }

  private static func conversationView(_ convo: BlueCatbirdChatDefs.ConversationState) -> FfiConversationView {
    FfiConversationView(
      groupId: convo.groupId,
      conversationId: convo.conversationId,
      epoch: UInt64(clamping: convo.epoch),
      members: convo.participants.map(memberView),
      name: nil,
      description: nil,
      avatarUrl: nil,
      createdAt: iso8601Formatter.string(from: Date()),
      updatedAt: nil
    )
  }

  private static func memberView(_ member: BlueCatbirdChatDefs.ParticipantView) -> FfiMemberView {
    FfiMemberView(
      did: member.userDid.description,
      role: member.role == .value_admin ? "admin" : "member"
    )
  }

  private static func keyPackageRef(_ ref: KeyPackageWithHash) -> FfiKeyPackageRef {
    FfiKeyPackageRef(
      did: ref.did.description,
      keyPackageData: ref.data,
      hash: ref.hash,
      cipherSuite: ""
    )
  }

  private static func incomingEnvelope(_ message: BlueCatbirdChatDefs.ApplicationEntry) -> FfiIncomingEnvelope {
    let ciphertextData: Data
    let senderDid: String
    switch message.signedRequest.body {
    case .blueCatbirdChatDefsApplicationSendBody(let body):
      ciphertextData = body.applicationMessage.bytes.data
      senderDid = body.actorDid.description
    case .unexpected:
      ciphertextData = Data()
      senderDid = ""
    }
    return FfiIncomingEnvelope(
      conversationId: message.conversationId,
      senderDid: senderDid,
      ciphertext: ciphertextData,
      timestamp: iso8601Formatter.string(from: message.receivedAt.date),
      serverMessageId: message.entryId
    )
  }
}
