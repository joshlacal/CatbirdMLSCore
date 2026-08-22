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

  public func submitPreparedRequest(
    method: String,
    nsid: String,
    body: Data?,
    query: Data?
  ) throws -> FfiGatewayResponse {
    let result = try blocking {
      let (data, httpResponse) = try await self.apiClient.submitPreparedRequest(
        method: method,
        nsid: nsid,
        body: body,
        query: query
      )
      return FfiGatewayResponse(
        status: UInt16(httpResponse.statusCode),
        contentType: httpResponse.value(forHTTPHeaderField: "Content-Type"),
        body: data
      )
    }
    return result
  }

  public func getDeliveryStatus(convoId: String, messageIds: [String]) throws -> [FfiDeliveryStatusPair] {
    return []
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
      return (messages: page.messages, lastSeq: page.lastSeq)
    }
    return FfiMessagesPage(
      envelopes: result.messages.map(Self.incomingEnvelope),
      cursor: result.lastSeq.map(String.init)
    )
  }

  public func getKeyPackages(actorDeviceId: String, dids: [String]) throws -> [FfiKeyPackageRef] {
    let didObjects = try dids.map { try DID(didString: $0) }
    let result = try blocking {
      try await self.apiClient.getKeyPackages(actorDeviceId: actorDeviceId, dids: didObjects, forceRefresh: true)
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

  public func listDevices(actorDeviceId: String) throws -> [FfiDeviceInfo] {
    let output = try blocking {
      let input = BlueCatbirdChatGetOwnDevices.Parameters(actorDeviceId: actorDeviceId)
      let (responseCode, output) = try await self.apiClient.client.blue.catbird.chat
        .getOwnDevices(input: input)
      guard responseCode == 200, let output else {
        // Carry the lexicon error code, not a generic string: the orchestrator's
        // readiness probe distinguishes `DeviceNotRegistered` (enroll this device)
        // from `DeviceRevoked` / `AccountSessionExpired` (never mint a replacement),
        // and both arrive as HTTP 401.
        let code = await self.apiClient.deviceProbeErrorCode(actorDeviceId: actorDeviceId)
        throw MLSAPIError.httpError(
          statusCode: responseCode,
          message: code.map { "Failed to list devices: \($0)" } ?? "Failed to list devices"
        )
      }
      return output
    }
    // Every field below is carried by the response and is load-bearing: the
    // orchestrator's `server_matches_custody` check requires status == "active"
    // plus a keyId/signaturePublicKey that agree with local custody. Leaving them
    // nil made that check unsatisfiable, so the already-registered fast path could
    // never be taken and every startup re-enrolled — which then collides with the
    // globally unique device_keys.key_id, because durable signer reuse derives the
    // same keyId, and surfaces as an unmapped HTTP 500.
    return output.items.map { item in
      FfiDeviceInfo(
        deviceId: item.device.deviceId,
        mlsDid: "",
        deviceUuid: item.device.deviceId,
        createdAt: Self.iso8601Formatter.string(from: item.device.createdAt.date),
        keyId: item.device.keyId,
        signaturePublicKey: item.device.signaturePublicKey.data,
        authGeneration: Int64(item.device.authGeneration),
        status: item.device.status.rawValue,
        availablePackageCount: UInt32(clamping: item.device.availablePackageCount),
        reservedPackageCount: UInt32(clamping: item.device.reservedPackageCount)
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
