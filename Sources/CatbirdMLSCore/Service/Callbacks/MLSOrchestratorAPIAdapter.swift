//
//  MLSOrchestratorAPIAdapter.swift
//  CatbirdMLSCore
//
//  Bridges the Rust orchestrator's synchronous UniFFI API callback surface to
//  Catbird's async Petrel MLS API client.
//

import CatbirdMLS
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
      try await self.apiClient.getConversations(limit: Int(limit), cursor: cursor)
    }
    return FfiConversationListPage(
      conversations: result.convos.map(Self.conversationView),
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
    return FfiAddMembersResult(success: result.success, newEpoch: UInt64(clamping: result.newEpoch))
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

  public func sendMessage(convoId: String, ciphertext: Data, epoch: UInt64) throws {
    guard let didString = currentDid() else {
      throw OrchestratorBridgeError.NotAuthenticated
    }
    let did = try DID(didString: didString)
    _ = try blocking {
      try await self.apiClient.sendMessage(
        convoId: convoId,
        msgId: UUID().uuidString.lowercased(),
        ciphertext: ciphertext,
        epoch: Int(clamping: epoch),
        paddedSize: ciphertext.count,
        senderDid: did
      )
    }
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
      let input = BlueCatbirdMlsChatGetMessages.Parameters(
        convoId: convoId,
        limit: Int(limit),
        sinceSeq: cursor.flatMap(Int.init),
        type: messageType,
        fromEpoch: fromEpoch.map(Int.init),
        toEpoch: toEpoch.map(Int.init)
      )
      let (responseCode, output) = try await self.apiClient.client.blue.catbird.mlsChat
        .getMessages(input: input)
      guard responseCode == 200, let output else {
        throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to fetch messages")
      }
      return output
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

  public func getKeyPackages(dids: [String]) throws -> [FfiKeyPackageRef] {
    let didObjects = try dids.map { try DID(didString: $0) }
    let result = try blocking {
      try await self.apiClient.getKeyPackages(dids: didObjects, forceRefresh: true)
    }
    return result.keyPackages.map(Self.keyPackageRef)
  }

  public func getKeyPackageStats() throws -> FfiKeyPackageStats {
    let output = try blocking { try await self.apiClient.getKeyPackageStats() }
    return FfiKeyPackageStats(
      available: UInt32(clamping: output.stats.available),
      total: UInt32(clamping: output.stats.published)
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
    keyPackages: [Data]
  ) throws -> FfiDeviceInfo {
    let output = try blocking {
      let items = keyPackages.map {
        BlueCatbirdMlsChatRegisterDevice.KeyPackageItem(
          keyPackage: Bytes(data: $0),
          cipherSuite: "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
          expires: ATProtocolDate(date: Date().addingTimeInterval(90 * 24 * 60 * 60))
        )
      }
      let input = BlueCatbirdMlsChatRegisterDevice.Input(
        deviceName: deviceName,
        deviceUUID: deviceUuid,
        keyPackages: items,
        signaturePublicKey: Bytes(data: signatureKey)
      )
      let (responseCode, output) = try await self.apiClient.client.blue.catbird.mlsChat
        .registerDevice(input: input)
      guard responseCode == 200, let output else {
        throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to register device")
      }
      return output
    }
    return FfiDeviceInfo(
      deviceId: output.deviceId,
      mlsDid: output.mlsDid.isEmpty ? mlsDid : output.mlsDid,
      deviceUuid: deviceUuid,
      createdAt: Self.iso8601Formatter.string(from: Date())
    )
  }

  public func listDevices() throws -> [FfiDeviceInfo] {
    let output = try blocking {
      let input = BlueCatbirdMlsChatListDevices.Parameters()
      let (responseCode, output) = try await self.apiClient.client.blue.catbird.mlsChat
        .listDevices(input: input)
      guard responseCode == 200, let output else {
        throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to list devices")
      }
      return output
    }
    return output.devices.map { device in
      FfiDeviceInfo(
        deviceId: device.deviceId,
        mlsDid: device.credentialDid,
        deviceUuid: device.deviceUUID ?? "",
        createdAt: Self.iso8601Formatter.string(from: device.registeredAt.date)
      )
    }
  }

  public func removeDevice(deviceId: String) throws {
    try blocking {
      let input = BlueCatbirdMlsChatRemoveDevice.Input(deviceId: deviceId)
      let (responseCode, _) = try await self.apiClient.client.blue.catbird.mlsChat
        .removeDevice(input: input)
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
      rejoinedAt: Self.iso8601Formatter.string(from: Date())
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
        failureMode: failureMode,
        failureType: failureType,
        epochAuthenticator: epochAuthenticator
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

  private static func conversationView(_ convo: BlueCatbirdMlsChatDefs.ConvoView) -> FfiConversationView {
    FfiConversationView(
      groupId: convo.groupId,
      conversationId: convo.conversationId,
      epoch: UInt64(clamping: convo.epoch),
      members: convo.members.map(memberView),
      name: nil,
      description: nil,
      avatarUrl: nil,
      createdAt: iso8601Formatter.string(from: convo.createdAt.date),
      updatedAt: convo.lastMessageAt.map { iso8601Formatter.string(from: $0.date) }
    )
  }

  private static func memberView(_ member: BlueCatbirdMlsChatDefs.MemberView) -> FfiMemberView {
    FfiMemberView(
      did: member.userDid.description,
      role: member.isAdmin ? "admin" : "member"
    )
  }

  private static func keyPackageRef(_ ref: BlueCatbirdMlsChatDefs.KeyPackageRef) -> FfiKeyPackageRef {
    FfiKeyPackageRef(
      did: ref.did.description,
      keyPackageData: ref.keyPackage.data,
      hash: ref.keyPackageHash,
      cipherSuite: ref.cipherSuite
    )
  }

  private static func incomingEnvelope(_ message: BlueCatbirdMlsChatDefs.MessageView) -> FfiIncomingEnvelope {
    FfiIncomingEnvelope(
      conversationId: message.convoId,
      senderDid: "",
      ciphertext: message.ciphertext.data,
      timestamp: iso8601Formatter.string(from: message.createdAt.date),
      serverMessageId: message.id
    )
  }
}
