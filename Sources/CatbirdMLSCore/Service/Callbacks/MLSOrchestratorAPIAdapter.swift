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

  internal func authorizedDeviceKeys(userDid: String) throws -> [Data] {
    do {
      return try blocking {
        try await MLSPublicPDSReader.fetchAuthorizedDeviceSignatureKeys(
          did: userDid,
          resolvePDS: { did in try await MLSPublicPDSReader.resolveCurrentPDS(did: did) })
      }
    } catch {
      throw OrchestratorBridgeError.Credential(
        message: "device_authorization_unavailable: Repository authorization could not be verified. \(error.localizedDescription)")
    }
  }

  internal func directRequestReceiptAuthorityDocument(expectedAuthorityDid: String) throws -> Data {
    do {
      return try blocking {
        try await MLSPublicPDSReader.fetchDirectRequestReceiptAuthorityDocument(
          authorityDid: expectedAuthorityDid)
      }
    } catch {
      throw OrchestratorBridgeError.Credential(
        message: "receipt_authority_unavailable: The direct-request receipt authority could not be verified. \(error.localizedDescription)"
      )
    }
  }

  internal func chatDeclarationAuthority(userDid: String) throws -> String? {
    do {
      return try blocking {
        try await MLSPublicPDSReader.fetchDeclaration(
          did: userDid,
          resolvePDS: { did in try await MLSPublicPDSReader.resolveCurrentPDS(did: did) }
        )?.deliveryService.didString()
      }
    } catch {
      throw OrchestratorBridgeError.Credential(
        message: "chat_declaration_unavailable: The account home could not be verified. \(error.localizedDescription)"
      )
    }
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
      conversations: try result.states.map(Self.conversationView),
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
      let page = try await self.apiClient.getCanonicalEntriesRetainingJSON(
        conversationId: convoId,
        afterSeq: cursor.flatMap(Int.init) ?? 0,
        limit: Int(limit)
      )
      return (
        entries: zip(page.output.entries, page.entriesJSON).map { $0 },
        lastSeq: MLSAPIClient.canonicalContinuationAfterSeq(
          hasMore: page.output.hasMore,
          nextAfterSeq: page.output.nextAfterSeq
        )
      )
    }
    return FfiMessagesPage(
      envelopes: try result.entries.compactMap {
        try Self.incomingEnvelope($0.0, servedJSON: $0.1, messageType: messageType)
      },
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

  // MARK: - Labyrinth history backup (catbird-mls feature `labyrinth`)
  //
  // UniFFI callback methods cannot be cfg-gated, so they are in every build.
  // Labyrinth XRPCs go straight to the delivery service with a service-auth
  // token (MLSAPIClient+Labyrinth.swift): a PDS proxy would drop the signed
  // write headers. mailboxKeys records live in the account's own repo.

  public func submitLabyrinthRequest(
    method: String,
    nsid: String,
    path: String,
    body: Data?,
    headers: [FfiLabyrinthHeader]
  ) throws -> FfiGatewayResponse {
    let pairs = headers.map { (name: $0.name, value: $0.value) }
    return try blocking {
      let (data, response) = try await self.apiClient.submitLabyrinthRequest(
        method: method, nsid: nsid, path: path, body: body, headers: pairs)
      return FfiGatewayResponse(
        status: UInt16(clamping: response.statusCode),
        contentType: response.value(forHTTPHeaderField: "Content-Type"),
        body: data
      )
    }
  }

  public func labyrinthPdsEnabled() -> Bool {
    true
  }

  public func putLabyrinthMailboxKeys(rkey: String, record: Data) throws {
    try blocking {
      try await self.apiClient.putLabyrinthMailboxKeys(rkey: rkey, record: record)
    }
  }

  public func getLabyrinthMailboxKeys(did: String, rkey: String) throws -> Data? {
    try blocking {
      try await self.apiClient.getLabyrinthMailboxKeys(did: did, rkey: rkey)
    }
  }

  public func deleteLabyrinthMailboxKeys() throws {
    try blocking {
      try await self.apiClient.deleteLabyrinthMailboxKeys()
    }
  }

  public func labyrinthSpaceEnabled() -> Bool {
    // This advertises the adapter capability. Rust probes the host and
    // persists placement; an outage never creates a second mailbox.
    MLSAPIClient.labyrinthSpaceTransportAvailable
  }

  public func submitLabyrinthSpaceRequest(
    method: String,
    nsid: String,
    path: String,
    body: Data?
  ) throws -> FfiGatewayResponse {
    try blocking {
      let (data, response) = try await self.apiClient.submitLabyrinthSpaceRequest(
        method: method, nsid: nsid, path: path, body: body)
      return FfiGatewayResponse(
        status: UInt16(clamping: response.statusCode),
        contentType: response.value(forHTTPHeaderField: "Content-Type"),
        body: data
      )
    }
  }

  private func blocking<T>(_ operation: @escaping () async throws -> T) throws -> T {
    // ponytail: sync FFI callback bridging async networking needs a free cooperative thread.
    // Ceiling: Bound by RustAuthorityAdmissionGate (<= 4 concurrent calls) and preview coalescing,
    // ensuring the Swift cooperative thread pool cannot starve.
    // Upgrade path: UniFFI async foreign callbacks once core FFI re-freezes.
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

  private static func conversationView(_ convo: BlueCatbirdChatDefs.ConversationState) throws -> FfiConversationView {
    FfiConversationView(
      canonicalStateJson: String(decoding: try JSONEncoder().encode(convo), as: UTF8.self),
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

  /// Project a canonical entry into the FFI envelope shape. `messageType`
  /// mirrors the orchestrator's filter: `"app"` keeps only application
  /// entries, `"commit"` keeps every commit-bearing entry, and `nil` keeps
  /// both. Other entry kinds project to nil; malformed supported entries throw so
  /// the caller cannot advance its cursor past an unprocessed message.
  /// `servedJSON` is the entry exactly as the service returned it; request
  /// control rows are forwarded from it, never from a DTO re-encode.
  static func incomingEnvelope(
    _ entry: BlueCatbirdChatDefs.ConversationEntry,
    servedJSON: Data,
    messageType: String? = nil
  ) throws -> FfiIncomingEnvelope? {
    let wantsApplication = messageType == nil || messageType == "app"
    let wantsCommit = messageType == nil || messageType == "commit"

    switch entry {
    case let .blueCatbirdChatDefsApplicationEntry(message) where wantsApplication:
      return try applicationEnvelope(message)

    case let .blueCatbirdChatDefsCommitEntry(message) where wantsCommit:
      guard case let .blueCatbirdChatDefsCommitTransitionBody(body) = message.signedRequest.body,
            body.prior.conversationId == message.conversationId,
            body.next.conversationId == message.conversationId else {
        throw malformedCanonicalEnvelope()
      }
      return try canonicalEnvelope(
        conversationId: message.conversationId,
        senderDid: body.actorDid,
        ciphertext: body.commit.bytes.data,
        receivedAt: message.receivedAt,
        entryId: message.entryId,
        sequence: message.seq,
        epoch: body.next.epoch
      )

    case let .blueCatbirdChatDefsLeafRecoveryFulfillmentEntry(message) where wantsCommit:
      guard case let .blueCatbirdChatDefsLeafRecoveryFulfillmentBody(body) = message.signedRequest.body,
            body.prior.conversationId == message.conversationId,
            body.next.conversationId == message.conversationId else {
        throw malformedCanonicalEnvelope()
      }
      return try canonicalEnvelope(
        conversationId: message.conversationId,
        senderDid: body.actorDid,
        ciphertext: body.commit.bytes.data,
        receivedAt: message.receivedAt,
        entryId: message.entryId,
        sequence: message.seq,
        epoch: body.next.epoch
      )

    case let .blueCatbirdChatDefsLeaveCommitFulfillmentEntry(message) where wantsCommit:
      guard case let .blueCatbirdChatDefsLeaveCommitFulfillmentBody(body) = message.signedRequest.body,
            body.prior.conversationId == message.conversationId,
            body.next.conversationId == message.conversationId else {
        throw malformedCanonicalEnvelope()
      }
      return try canonicalEnvelope(
        conversationId: message.conversationId,
        senderDid: body.actorDid,
        ciphertext: body.commit.bytes.data,
        receivedAt: message.receivedAt,
        entryId: message.entryId,
        sequence: message.seq,
        epoch: body.next.epoch
      )

    case let .blueCatbirdChatDefsDirectRequestEntry(message):
      guard case let .blueCatbirdChatDefsDirectRequestPublicationBody(body) =
        message.signedRequest.body,
        body.conversationId == message.conversationId,
        body.next.conversationId == message.conversationId
      else {
        throw malformedCanonicalEnvelope()
      }
      return try canonicalControlEnvelope(
        servedJSON: servedJSON,
        conversationId: message.conversationId,
        senderDid: body.actorDid,
        receivedAt: message.receivedAt,
        entryId: message.entryId,
        sequence: message.seq,
        epoch: body.next.epoch
      )

    case let .blueCatbirdChatDefsDirectRequestAcceptanceEntry(message):
      guard case let .blueCatbirdChatDefsDirectRequestAcceptanceBody(body) =
        message.signedRequest.body,
        body.prior.conversationId == message.conversationId,
        body.next.conversationId == message.conversationId
      else {
        throw malformedCanonicalEnvelope()
      }
      return try canonicalControlEnvelope(
        servedJSON: servedJSON,
        conversationId: message.conversationId,
        senderDid: body.actorDid,
        receivedAt: message.receivedAt,
        entryId: message.entryId,
        sequence: message.seq,
        epoch: body.next.epoch
      )

    case let .blueCatbirdChatDefsConversationCloseEntry(message):
      guard case let .blueCatbirdChatDefsConversationCloseBody(body) =
        message.signedRequest.body,
        body.prior.conversationId == message.conversationId,
        body.retired.conversationId == message.conversationId
      else {
        throw malformedCanonicalEnvelope()
      }
      return try canonicalControlEnvelope(
        servedJSON: servedJSON,
        conversationId: message.conversationId,
        senderDid: body.actorDid,
        receivedAt: message.receivedAt,
        entryId: message.entryId,
        sequence: message.seq,
        epoch: body.prior.epoch
      )

    default:
      return nil
    }
  }

  /// Canonical receipt metadata is retained exactly at first authenticated ingress.
  /// It is never used to rewrite an older deduplicated row without wire proof.
  static func canonicalEnvelope(
    conversationId: BlueCatbirdChatDefs.OperationId,
    senderDid: BlueCatbirdChatDefs.BareDid,
    ciphertext: Data,
    receivedAt: BlueCatbirdChatDefs.CanonicalDatetime,
    entryId: BlueCatbirdChatDefs.OperationId,
    sequence: Int,
    epoch: Int,
    canonicalEntryJson: String? = nil
  ) throws -> FfiIncomingEnvelope {
    let timestamp = receivedAt.iso8601String
    guard MLSSystemMessagePresentation.canonicalUUID(conversationId),
          MLSSystemMessagePresentation.canonicalUUID(entryId),
          (1...9_007_199_254_740_991).contains(sequence),
          (0...9_007_199_254_740_991).contains(epoch),
          isCanonicalTimestamp(timestamp, date: receivedAt.date) else {
      throw malformedCanonicalEnvelope()
    }
    return FfiIncomingEnvelope(
      canonicalEntryJson: canonicalEntryJson,
      conversationId: conversationId,
      senderDid: senderDid.description,
      ciphertext: ciphertext,
      timestamp: timestamp,
      serverMessageId: entryId,
      serverSequence: UInt64(sequence),
      serverEpoch: UInt64(epoch)
    )
  }

  /// Retains the complete signed control row for Rust verification and
  /// dispatch. Returning nil here would let the page cursor advance past an
  /// unprocessed request transition.
  private static func canonicalControlEnvelope(
    servedJSON: Data,
    conversationId: BlueCatbirdChatDefs.OperationId,
    senderDid: BlueCatbirdChatDefs.BareDid,
    receivedAt: BlueCatbirdChatDefs.CanonicalDatetime,
    entryId: BlueCatbirdChatDefs.OperationId,
    sequence: Int,
    epoch: Int
  ) throws -> FfiIncomingEnvelope {
    guard let json = String(data: servedJSON, encoding: .utf8) else {
      throw malformedCanonicalEnvelope()
    }
    return try canonicalEnvelope(
      conversationId: conversationId,
      senderDid: senderDid,
      ciphertext: Data(),
      receivedAt: receivedAt,
      entryId: entryId,
      sequence: sequence,
      epoch: epoch,
      canonicalEntryJson: json
    )
  }

  static func applicationEnvelope(_ message: BlueCatbirdChatDefs.ApplicationEntry) throws -> FfiIncomingEnvelope {
    guard let body = message.parsedBody,
          body.prior.conversationId == message.conversationId else { throw malformedCanonicalEnvelope() }
    return try canonicalEnvelope(
      conversationId: message.conversationId, senderDid: body.actorDid,
      ciphertext: body.applicationMessage.bytes.data, receivedAt: message.receivedAt,
      entryId: message.entryId, sequence: message.seq, epoch: body.prior.epoch)
  }

  private static func malformedCanonicalEnvelope() -> MLSConversationError {
    .operationFailed("A secure message could not be verified. Please try syncing again.")
  }

  private static func isCanonicalTimestamp(_ value: String, date: Date) -> Bool {
    let bytes = Array(value.utf8)
    guard bytes.count == 24, date.timeIntervalSince1970.isFinite else { return false }
    let separators: [Int: UInt8] = [4: 45, 7: 45, 10: 84, 13: 58, 16: 58, 19: 46, 23: 90]
    for (index, byte) in bytes.enumerated() {
      if let separator = separators[index] {
        guard byte == separator else { return false }
      } else if !(48...57).contains(byte) { return false }
    }
    // Date may round a fractional millisecond when formatted. Compare only the
    // calendar fields; preserve the exact original three fractional digits above.
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = TimeZone(secondsFromGMT: 0)!
    let fields = calendar.dateComponents([.year, .month, .day, .hour, .minute, .second], from: date)
    let wholeSeconds = String(format: "%04d-%02d-%02dT%02d:%02d:%02d",
      fields.year ?? -1, fields.month ?? -1, fields.day ?? -1,
      fields.hour ?? -1, fields.minute ?? -1, fields.second ?? -1)
    return value.hasPrefix(wholeSeconds)
  }
}
