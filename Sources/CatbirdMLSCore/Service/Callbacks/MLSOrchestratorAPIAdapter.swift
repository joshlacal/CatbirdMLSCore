//
//  MLSOrchestratorAPIAdapter.swift
//  CatbirdMLSCore
//
//  Bridges the OrchestratorApiCallback protocol (called synchronously from Rust)
//  to the existing async MLSAPIClient Swift service.
//

import CatbirdMLS
import Foundation
import OSLog
import Petrel

// MARK: - Synchronous Bridge Helper

/// Dedicated dispatch queue for bridging async Swift calls back to synchronous Rust callbacks.
/// Dispatching the Task onto a non-cooperative thread prevents cooperative pool starvation
/// when the calling Rust thread blocks with a semaphore.
private let runBlockingQueue = DispatchQueue(label: "blue.catbird.mls.runBlocking")

/// Executes an async operation synchronously by blocking the current thread.
/// Used because UniFFI callback methods are invoked from Rust on a background thread
/// and must return synchronously.
///
/// The Task is dispatched onto `runBlockingQueue` (a non-cooperative thread) to avoid
/// deadlocking the Swift cooperative thread pool when the semaphore blocks.
///
/// IMPORTANT: Must never be called from the main thread or from an actor-isolated context.
private func runBlocking<T>(_ operation: @escaping () async throws -> T) throws -> T {
  let semaphore = DispatchSemaphore(value: 0)
  nonisolated(unsafe) var result: Result<T, Error>!
  runBlockingQueue.async {
    Task {
      do {
        result = .success(try await operation())
      } catch {
        result = .failure(error)
      }
      semaphore.signal()
    }
  }
  semaphore.wait()
  return try result.get()
}

// MARK: - MLSOrchestratorAPIAdapter

/// Adapts the existing `MLSAPIClient` (async, Petrel-based) to the synchronous
/// `OrchestratorApiCallback` protocol expected by the Rust orchestrator via UniFFI.
///
/// Each method blocks the calling Rust thread using a semaphore while the underlying
/// async API call executes on a Task. The adapter converts between Petrel/ATProto types
/// and the FFI record types defined in the CatbirdMLS bindings.
public final class MLSOrchestratorAPIAdapter: OrchestratorApiCallback, @unchecked Sendable {

  private let apiClient: MLSAPIClient
  private let logger = Logger(subsystem: "blue.catbird", category: "OrchestratorAPIAdapter")

  // MARK: - Initialization

  /// Create an adapter that bridges to the given MLSAPIClient.
  /// - Parameter apiClient: The existing async API client to delegate to.
  public init(apiClient: MLSAPIClient) {
    self.apiClient = apiClient
  }

  // MARK: - Authentication

  public func isAuthenticatedAs(did: String) -> Bool {
    do {
      return try runBlocking {
        await self.apiClient.isAuthenticatedAs(did)
      }
    } catch {
      logger.error("isAuthenticatedAs failed: \(error.localizedDescription)")
      return false
    }
  }

  public func currentDid() -> String? {
    do {
      return try runBlocking {
        await self.apiClient.authenticatedUserDID()
      }
    } catch {
      logger.error("currentDid failed: \(error.localizedDescription)")
      return nil
    }
  }

  // MARK: - Conversations

  public func getConversations(limit: UInt32, cursor: String?) throws -> FfiConversationListPage {
    try runBlocking {
      let result = try await self.apiClient.getConversations(limit: Int(limit), cursor: cursor)
      return FfiConversationListPage(
        conversations: result.convos.map { self.convertConvoView($0) },
        cursor: result.cursor
      )
    }
  }

  public func createConversation(
    groupId: String,
    initialMembers: [String]?,
    metadataName: String?,
    metadataDescription: String?,
    commitData: Data?,
    welcomeData: Data?
  ) throws -> FfiCreateConversationResult {
    try runBlocking {
      let memberDIDs = try initialMembers?.map { try DID(didString: $0) }

      // Metadata is now encrypted inside MLS group context extensions.
      // Do NOT send plaintext metadata to the server.
      let convo = try await self.apiClient.createConversation(
        groupId: groupId,
        cipherSuite: "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
        initialMembers: memberDIDs,
        welcomeMessage: welcomeData,
        metadata: nil
      )

      return FfiCreateConversationResult(
        conversation: self.convertConvoView(convo),
        commitData: commitData,
        welcomeData: welcomeData
      )
    }
  }

  public func leaveConversation(convoId: String) throws {
    try runBlocking {
      _ = try await self.apiClient.leaveConversation(convoId: convoId)
    }
  }

  public func addMembers(
    convoId: String,
    memberDids: [String],
    commitData: Data,
    welcomeData: Data?
  ) throws -> FfiAddMembersResult {
    try runBlocking {
      let dids = try memberDids.map { try DID(didString: $0) }
      let result = try await self.apiClient.addMembers(
        convoId: convoId,
        didList: dids,
        commit: commitData,
        welcomeMessage: welcomeData
      )
      return FfiAddMembersResult(
        success: result.success,
        newEpoch: UInt64(result.newEpoch)
      )
    }
  }

  public func removeMembers(convoId: String, memberDids: [String], commitData: Data) throws {
    // TODO: MLSAPIClient does not currently expose a direct removeMembers method.
    // The server uses commitGroupChange with action "removeMembers".
    // For now, throw an error indicating this needs wiring.
    throw OrchestratorBridgeError.Api(
      message: "removeMembers not yet wired to MLSAPIClient - needs commitGroupChange integration"
    )
  }

  // MARK: - Messages

  public func sendMessage(convoId: String, ciphertext: Data, epoch: UInt64) throws {
    try runBlocking {
      let msgId = UUID().uuidString.lowercased()
      // Use a standard padding bucket size based on ciphertext length
      let paddedSize = Self.paddingBucket(for: ciphertext.count)

      // We need a sender DID for the API call. Get it from the authenticated session.
      guard let senderDidString = await self.apiClient.authenticatedUserDID() else {
        throw OrchestratorBridgeError.NotAuthenticated
      }
      let senderDid = try DID(didString: senderDidString)

      _ = try await self.apiClient.sendMessage(
        convoId: convoId,
        msgId: msgId,
        ciphertext: ciphertext,
        epoch: Int(epoch),
        paddedSize: paddedSize,
        senderDid: senderDid
      )
    }
  }

  public func getMessages(convoId: String, cursor: String?, limit: UInt32) throws -> FfiMessagesPage {
    try runBlocking {
      // The existing API uses sinceSeq (Int?) rather than a string cursor.
      // Convert the cursor string to an Int if provided.
      let sinceSeq = cursor.flatMap { Int($0) }

      let result = try await self.apiClient.getMessages(
        convoId: convoId,
        limit: Int(limit),
        sinceSeq: sinceSeq
      )

      // MessageView fields: id, convoId, ciphertext (Bytes), epoch, seq, createdAt, messageType
      // The Rust orchestrator decrypts ciphertext to extract sender/content; we pass raw envelopes.
      let envelopes: [FfiIncomingEnvelope] = result.messages.map { msg in
        return FfiIncomingEnvelope(
          conversationId: convoId,
          senderDid: "",  // Not available in MessageView; extracted after MLS decryption
          ciphertext: msg.ciphertext.data,
          timestamp: msg.createdAt.iso8601String,
          serverMessageId: msg.id
        )
      }

      // Return the lastSeq as the cursor for next page
      let nextCursor = result.lastSeq.map { String($0) }

      return FfiMessagesPage(
        envelopes: envelopes,
        cursor: nextCursor
      )
    }
  }

  // MARK: - Key Packages

  public func publishKeyPackage(keyPackage: Data, cipherSuite: String, expiresAt: String) throws {
    try runBlocking {
      let expirationDate = ISO8601DateFormatter().date(from: expiresAt)
      let atprotoDate = expirationDate.map { ATProtocolDate(date: $0) }

      try await self.apiClient.publishKeyPackage(
        keyPackage: keyPackage,
        cipherSuite: cipherSuite,
        expiresAt: atprotoDate
      )
    }
  }

  public func getKeyPackages(dids: [String]) throws -> [FfiKeyPackageRef] {
    try runBlocking {
      let didValues = try dids.map { try DID(didString: $0) }
      let result = try await self.apiClient.getKeyPackages(dids: didValues)

      return result.keyPackages.compactMap { kpRef -> FfiKeyPackageRef? in
        // KeyPackageRef stores keyPackage as a base64 string
        guard let keyPackageData = Data(base64Encoded: kpRef.keyPackage) else {
          self.logger.warning("Failed to decode key package base64 for DID: \(kpRef.did)")
          return nil
        }
        return FfiKeyPackageRef(
          did: kpRef.did.description,
          keyPackageData: keyPackageData,
          hash: kpRef.keyPackageHash,
          cipherSuite: kpRef.cipherSuite
        )
      }
    }
  }

  public func getKeyPackageStats() throws -> FfiKeyPackageStats {
    try runBlocking {
      let output = try await self.apiClient.getKeyPackageStats()
      return FfiKeyPackageStats(
        available: UInt32(output.stats.available),
        total: UInt32(output.stats.published)
      )
    }
  }

  public func syncKeyPackages(localHashes: [String], deviceId: String) throws -> FfiKeyPackageSyncResult {
    try runBlocking {
      let result = try await self.apiClient.syncKeyPackages(
        localHashes: localHashes,
        deviceId: deviceId
      )
      return FfiKeyPackageSyncResult(
        orphanedCount: UInt32(result.orphanedCount),
        deletedCount: UInt32(result.deletedCount)
      )
    }
  }

  // MARK: - Devices

  public func registerDevice(
    deviceUuid: String,
    deviceName: String,
    mlsDid: String,
    signatureKey: Data,
    keyPackages: [Data]
  ) throws -> FfiDeviceInfo {
    try runBlocking {
      let expirationDate = Date().addingTimeInterval(90 * 24 * 60 * 60)
      let keyPackageItems = keyPackages.map { package in
        BlueCatbirdMlsChatRegisterDevice.KeyPackageItem(
          keyPackage: package.base64EncodedString(),
          cipherSuite: "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
          expires: ATProtocolDate(date: expirationDate)
        )
      }

      let input = BlueCatbirdMlsChatRegisterDevice.Input(
        deviceName: deviceName,
        deviceUUID: deviceUuid,
        keyPackages: keyPackageItems,
        signaturePublicKey: Bytes(data: signatureKey)
      )

      let (responseCode, output) = try await self.apiClient.client.blue.catbird.mlschat.registerDevice(
        input: input
      )

      guard responseCode == 200, let output else {
        throw OrchestratorBridgeError.Api(message: "registerDevice failed with HTTP \(responseCode)")
      }

      do {
        _ = try await self.apiClient.optIn(deviceId: output.deviceId)
      } catch {
        self.logger.warning("registerDevice opt-in failed: \(error.localizedDescription)")
      }

      return FfiDeviceInfo(
        deviceId: output.deviceId,
        mlsDid: output.mlsDid,
        deviceUuid: deviceUuid,
        createdAt: nil
      )
    }
  }

  public func listDevices() throws -> [FfiDeviceInfo] {
    // TODO: MLSAPIClient does not expose a listDevices method directly.
    // Device management goes through MLSDeviceManager and the raw ATProtoClient.
    throw OrchestratorBridgeError.Api(
      message: "listDevices not yet wired - device management uses MLSDeviceManager on iOS"
    )
  }

  public func removeDevice(deviceId: String) throws {
    // TODO: Device removal goes through MLSDeviceManager.deleteDevice().
    throw OrchestratorBridgeError.Api(
      message: "removeDevice not yet wired - device management uses MLSDeviceManager on iOS"
    )
  }

  // MARK: - Group Info

  public func publishGroupInfo(convoId: String, groupInfo: Data) throws {
    try runBlocking {
      try await self.apiClient.updateGroupInfo(
        convoId: convoId,
        groupInfo: groupInfo,
        epoch: 0,  // Epoch is determined server-side during upload
        verifyUpload: false
      )
    }
  }

  public func getGroupInfo(convoId: String) throws -> Data {
    try runBlocking {
      let result = try await self.apiClient.getGroupInfo(convoId: convoId)
      return result.groupInfo
    }
  }

  // MARK: - Private Helpers

  /// Convert a Petrel ConvoView to an FFI ConversationView.
  private func convertConvoView(_ convo: BlueCatbirdMlsChatDefs.ConvoView) -> FfiConversationView {
    let members: [FfiMemberView] = convo.members.map { member in
      let role: String
      if member.isAdmin {
        role = "admin"
      } else if member.isModerator == true {
        role = "moderator"
      } else {
        role = "member"
      }
      return FfiMemberView(
        did: member.did.description,
        role: role
      )
    }

    return FfiConversationView(
      groupId: convo.groupId,
      epoch: UInt64(convo.epoch),
      members: members,
      name: convo.metadata?.name,
      description: convo.metadata?.description,
      avatarUrl: nil,  // ConvoMetadata does not include avatar
      createdAt: convo.createdAt.iso8601String,
      updatedAt: convo.lastMessageAt?.iso8601String  // Best proxy for "updated at"
    )
  }

  /// Calculate the MLS message padding bucket for a given ciphertext size.
  /// Buckets: 512, 1024, 2048, 4096, 8192, then multiples of 8192.
  private static func paddingBucket(for size: Int) -> Int {
    let buckets = [512, 1024, 2048, 4096, 8192]
    for bucket in buckets {
      if size <= bucket {
        return bucket
      }
    }
    // Round up to next multiple of 8192
    return ((size + 8191) / 8192) * 8192
  }
}
