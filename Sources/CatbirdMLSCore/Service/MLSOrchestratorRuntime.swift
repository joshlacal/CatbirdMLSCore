import CatbirdMLS
import Foundation
import GRDB
import OSLog
import Petrel
import PetrelCatbird

/// Owns the Rust orchestrator bridge and its Swift callback adapters.
///
/// The runtime does not change Catbird iOS behavior by itself. Callers opt into
/// `rustShadow` to mirror decisions for telemetry, then `rustAuthoritative` to
/// delegate protocol decisions after parity is proven. Swift still owns
/// SQLCipher/GRDB lifecycle, Keychain setup, App Group paths, NSE coordination,
/// suspension orchestration, App Group shutdown ordering, and 0xdead10cc
/// handling. In `.rustFull`, the host-driven suspend path currently asks Rust
/// to prepare by internally shutting down the engine while preserving enough
/// lifecycle state for resume.
public final class MLSOrchestratorRuntime: @unchecked Sendable {
  public let userDID: String
  public let mode: MLSProtocolAuthorityMode

  public let bridge: OrchestratorBridge

  private let storageAdapter: OrchestratorStorageCallback?
  private let apiClient: OrchestratorApiCallback?
  private let credentialAdapter: OrchestratorCredentialCallback?
  private let eventCallback: OrchestratorEventCallback?
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "OrchestratorRuntime")

  public init(
    userDID: String,
    mode: MLSProtocolAuthorityMode = MLSProtocolAuthorityMode.defaultMode,
    mlsContext: MlsContext,
    databasePool: DatabasePool,
    apiClient: OrchestratorApiCallback,
    keychainManager: MLSKeychainManager = .shared,
    authorizedDeviceKeyResolver: (@Sendable (String) -> [Data]?)? = nil,
    signingPublicKeyResolver: MLSOrchestratorCredentialAdapter.SigningPublicKeyResolver? = nil,
    signingBindingResolver: MLSOrchestratorCredentialAdapter.SigningBindingResolver? = nil,
    config: FfiOrchestratorConfig? = nil,
    eventCallback: OrchestratorEventCallback? = nil
  ) {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDID)
    let storageAdapter = MLSOrchestratorStorageAdapter(
      dbPool: databasePool,
      userDID: normalizedDID,
      mlsContext: mlsContext
    )
    let credentialAdapter = MLSOrchestratorCredentialAdapter(
      keychainManager: keychainManager,
      authorizedDeviceKeyResolver: authorizedDeviceKeyResolver,
      transcriptSigner: { identity, transcript in
        try mlsContext.signWithIdentityKey(identity: identity, payload: transcript)
      },
      signingPublicKeyResolver: signingPublicKeyResolver,
      signingBindingResolver: signingBindingResolver
    )

    self.userDID = normalizedDID
    self.mode = mode
    self.storageAdapter = storageAdapter
    self.apiClient = apiClient
    self.credentialAdapter = credentialAdapter
    self.eventCallback = eventCallback
    let capabilities = SecurityStorageCapabilities(
      version: 3,
      resetState: true,
      quarantine: true,
      pendingMessageProtection: true,
      sequencerReceipts: true,
      recoveryBackoff: true,
      pendingDeletion: true,
      authorizedDeviceResolution: true
    )
    do {
      bridge = try OrchestratorBridge(
        mlsContext: mlsContext,
        storage: storageAdapter,
        apiClient: apiClient,
        credentials: credentialAdapter,
        capabilities: capabilities,
        config: config ?? .default
      )
    } catch {
      // Capabilities are compile-time paired with this adapter surface. A
      // rejected bridge is therefore a programming/configuration failure and
      // must not leave a runtime that appears initialized but cannot enforce
      // the Rust security contract.
      fatalError("Failed to initialize MLS orchestrator bridge: \(error)")
    }
    bridge.setEventCallback(callback: eventCallback)
    bridge.setStoreControlMessages(enabled: mode.usesRustForDecisions)

    logger.info(
      "MLSOrchestratorRuntime initialized mode=\(mode.rawValue, privacy: .public) user=\(normalizedDID.prefix(20), privacy: .private)"
    )
  }

  internal init(
    userDID: String,
    mode: MLSProtocolAuthorityMode = MLSProtocolAuthorityMode.defaultMode,
    bridge: OrchestratorBridge,
    eventCallback: OrchestratorEventCallback? = nil
  ) {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDID)
    self.userDID = normalizedDID
    self.mode = mode
    self.bridge = bridge
    self.storageAdapter = nil
    self.apiClient = nil
    self.credentialAdapter = nil
    self.eventCallback = eventCallback
  }

  public func initialize() throws {
    let mutationLease = try MLSStorageCoordinator.shared.acquireMutationMutexSync(for: .rustState, userDID: userDID)
    defer { mutationLease.release() }
    try bridge.initialize(userDid: userDID)
    logger.info(
      "MlsEngine initialized mode=\(self.mode.rawValue, privacy: .public) user=\(self.userDID.prefix(20), privacy: .private)"
    )
  }

  public func syncWithServer(fullSync: Bool) throws {
    try bridge.syncWithServer(fullSync: fullSync)
  }

  public func listConversations() throws -> [BlueCatbirdChatDefs.ConversationState] {
    try bridge.listConversations(userDid: userDID).map { conversation in
      try decodeConversationSnapshot(conversation, fallbackUserDID: userDID)
    }
  }

  public func startupReconcile() throws -> FfiStartupReconcileReport {
    try bridge.startupReconcile()
  }

  public func runDeferredRecovery(reason: String) throws -> FfiDeferredRecoveryReport {
    try bridge.runDeferredRecovery(reason: reason)
  }

  public func prepareForSuspend(
    reason: String,
    deadlineMs: UInt64 = 1_500
  ) throws -> FfiSuspendResult {
    try bridge.prepareForSuspend(reason: reason, deadlineMs: deadlineMs)
  }

  /// Prepare one canonical signed mutation through the Rust-owned signer.
  /// The returned request intentionally contains no Authorization or DPoP
  /// values; the selected host transport attaches its own current session
  /// credentials after this exact binding snapshot is signed.
  public func prepareCleanChatSignedRequest(
    binding: CleanChatSigningContextFfi,
    operation: CleanChatOperationFfi,
    bodyJson: Data
  ) throws -> CleanChatPreparedRequestFfi {
    try bridge.prepareCleanChatSignedRequest(
      binding: binding,
      operation: operation,
      bodyJson: bodyJson
    )
  }

  public func reattachAfterSuspend(reason: String) throws {
    try bridge.reattachAfterSuspend(userDid: userDID, reason: reason)
  }

  public func resumeFromSuspend(reason: String) throws {
    try bridge.resumeFromSuspend(reason: reason)
  }

  public func interruptStorage(reason: String) throws {
    try bridge.interruptStorage(reason: reason)
  }

  public func emergencyClose(reason: String) throws {
    try bridge.emergencyClose(reason: reason)
  }

  public func storageLifecycleStatus() -> StorageLifecycleStatus {
    bridge.storageLifecycleStatus()
  }

  @discardableResult
  public func sendMessage(conversationId: String, text: String) throws -> FfiMessage {
    try bridge.sendMessage(conversationId: conversationId, text: text)
  }

  @discardableResult
  public func sendPayload(conversationId: String, payload: MLSMessagePayload) throws -> FfiMessage {
    let payloadJson = try encodePayloadJson(payload)
    return try bridge.sendPayloadJson(conversationId: conversationId, payloadJson: payloadJson)
  }

  @discardableResult
  public func sendPayloadResult(conversationId: String, payload: MLSMessagePayload) throws -> FfiSendResult {
    let payloadJson = try encodePayloadJson(payload)
    return try bridge.sendPayloadResultJson(conversationId: conversationId, payloadJson: payloadJson)
  }

  @discardableResult
  public func sendReaction(
    conversationId: String,
    messageId: String,
    emoji: String,
    action: MLSReactionPayload.ReactionAction
  ) throws -> FfiMessage {
    try bridge.sendReaction(
      conversationId: conversationId,
      messageId: messageId,
      emoji: emoji,
      action: action.rawValue
    )
  }

  @discardableResult
  public func processIncoming(envelope: FfiIncomingEnvelope) throws -> FfiMessage? {
    try bridge.processIncoming(envelope: envelope)
  }

  @discardableResult
  public func processIncomingMessage(
    envelope: FfiIncomingEnvelope,
    serverEpoch: UInt64?
  ) throws -> FfiMessageProcessingResult {
    try bridge.processIncomingMessage(envelope: envelope, serverEpoch: serverEpoch)
  }

  public func processServerEvent(eventJson: String) throws -> [FfiEngineEvent] {
    try bridge.processServerEvent(eventJson: eventJson)
  }

  public func recordGroupReset(
    conversationId: String,
    newGroupIdHex: String,
    resetGeneration: Int32
  ) throws {
    try bridge.recordGroupReset(
      convoId: conversationId,
      newGroupIdHex: newGroupIdHex,
      resetGeneration: resetGeneration
    )
  }

  @discardableResult
  public func recordGroupResetOutcome(
    conversationId: String,
    newGroupIdHex: String,
    resetGeneration: Int32
  ) throws -> MLSResetRecordOutcome {
    let outcome = try bridge.recordGroupResetOutcome(
      convoId: conversationId,
      newGroupIdHex: newGroupIdHex,
      resetGeneration: resetGeneration
    )
    return MLSResetRecordOutcome(ffiOutcome: outcome)
  }

  public func recordResetRequested(
    conversationId: String,
    cryptoSessionId: String,
    resetGeneration: Int32,
    trigger: String,
    requestEventId: String,
    expectedNewMlsGroupIdHex: String?
  ) throws {
    try bridge.recordResetRequested(
      convoId: conversationId,
      cryptoSessionId: cryptoSessionId,
      resetGeneration: resetGeneration,
      trigger: trigger,
      requestEventId: requestEventId,
      expectedNewMlsGroupIdHex: expectedNewMlsGroupIdHex
    )
  }

  @discardableResult
  public func recordResetRequestedOutcome(
    conversationId: String,
    cryptoSessionId: String,
    resetGeneration: Int32,
    trigger: String,
    requestEventId: String,
    expectedNewMlsGroupIdHex: String?
  ) throws -> MLSResetRecordOutcome {
    let outcome = try bridge.recordResetRequestedOutcome(
      convoId: conversationId,
      cryptoSessionId: cryptoSessionId,
      resetGeneration: resetGeneration,
      trigger: trigger,
      requestEventId: requestEventId,
      expectedNewMlsGroupIdHex: expectedNewMlsGroupIdHex
    )
    return MLSResetRecordOutcome(ffiOutcome: outcome)
  }

  public func performSilentRecovery(conversationIds: [String]) throws {
    try bridge.performSilentRecovery(conversationIds: conversationIds)
  }

  public func joinOrRejoin(conversationId: String) throws -> MLSJoinOrRejoinResult {
    let result = try bridge.joinOrRejoin(convoId: conversationId)
    return MLSJoinOrRejoinResult(ffiResult: result)
  }

  public func ensureConversationReady(conversationId: String) throws -> MLSConversationReadyResult {
    let result = try bridge.ensureConversationReady(convoId: conversationId)
    return MLSConversationReadyResult(ffiResult: result)
  }

  public func debugWipeLocalGroupForRecovery(
    conversationId: String
  ) throws -> MLSDebugWipeLocalGroupResult {
    let result = try bridge.debugWipeLocalGroupForRecovery(convoId: conversationId)
    return MLSDebugWipeLocalGroupResult(ffiResult: result)
  }

  @discardableResult
  public func ensureDeviceRegistered() throws -> String {
    let mutationLease = try MLSStorageCoordinator.shared.acquireMutationMutexSync(for: .rustState, userDID: userDID)
    defer { mutationLease.release() }
    return try bridge.ensureDeviceRegistered()
  }
  public func replenishKeyPackagesIfNeeded() throws {
    try bridge.replenishKeyPackagesIfNeeded()
  }

  public func listDevices() throws -> [MLSRegisteredDeviceInfo] {
    try bridge.listDevices().map(MLSRegisteredDeviceInfo.init(ffiDeviceInfo:))
  }

  public func removeDevice(deviceId: String) throws {
    try bridge.removeDevice(deviceId: deviceId)
  }

  public func currentDeviceInfo() throws -> MLSRegisteredDeviceInfo? {
    // The credential store holds the SERVER-MINTED device id (the delivery
    // service mints its own id and stores `device_uuid` as NULL), and
    // `listDevices` identifies devices by `deviceId`. Match on `deviceId`, not
    // `deviceUuid` — the latter is always empty server-side, so matching it
    // returned nil and stranded push-token registration / device lookups in
    // rustFull.
    guard let registeredDeviceId = try credentialAdapter?.getDeviceUuid(userDid: userDID) else {
      return nil
    }
    return try bridge.listDevices()
      .first { device in device.deviceId == registeredDeviceId }
      .map(MLSRegisteredDeviceInfo.init(ffiDeviceInfo:))
  }

  public func conversationRecoveryState(conversationId: String) throws -> ConversationRecoveryState {
    let ffiState = try bridge.getConversationRecoveryState(conversationId: conversationId)
    return ConversationRecoveryState(ffiRecoveryState: ffiState)
  }

  public func createConversation(
    name: String,
    initialMemberDids: [String],
    description: String?
  ) throws -> MLSCreateConversationResult {
    let ffiResult = try bridge.createConversation(
      name: name,
      initialMembers: initialMemberDids.isEmpty ? nil : initialMemberDids,
      description: description
    )
    return try MLSCreateConversationResult(ffiResult: ffiResult, userDID: userDID)
  }

  public func addMembers(
    conversationId: String,
    memberDids: [String]
  ) throws -> MLSGroupMutationResult {
    let ffiResult = try bridge.addMembersResult(
      conversationId: conversationId,
      memberDids: memberDids
    )
    return try MLSGroupMutationResult(ffiResult: ffiResult, userDID: userDID)
  }

  public func removeMembers(
    conversationId: String,
    memberDids: [String]
  ) throws -> MLSGroupMutationResult {
    let ffiResult = try bridge.removeMembersResult(
      conversationId: conversationId,
      memberDids: memberDids
    )
    return try MLSGroupMutationResult(ffiResult: ffiResult, userDID: userDID)
  }

  /// Update a group's encrypted metadata (title / description / avatar) via the
  /// Rust orchestrator (rustFull authority). Supply the full desired state:
  /// the commit replaces the metadata blob, so pass current avatar bytes +
  /// locator when renaming to avoid dropping the avatar.
  public func updateGroupMetadataEncrypted(
    conversationId: String,
    title: String?,
    description: String?,
    avatarBlobLocator: String?,
    avatarContentType: String?,
    avatarBytes: Data?
  ) throws {
    try bridge.updateGroupMetadataEncrypted(
      conversationId: conversationId,
      title: title,
      description: description,
      avatarBlobLocator: avatarBlobLocator,
      avatarContentType: avatarContentType,
      avatarBytes: avatarBytes
    )
  }

  public func respondToWelcomeReissue(
    conversationId: String,
    recipientDeviceDid: String,
    requestId: String
  ) throws {
    try bridge.respondToWelcomeReissue(
      convoId: conversationId,
      recipientDeviceDid: recipientDeviceDid,
      requestId: requestId
    )
  }

  public func leaveConversation(
    conversationId: String
  ) throws -> MLSLeaveConversationResult {
    MLSLeaveConversationResult(
      ffiResult: try bridge.leaveConversation(conversationId: conversationId)
    )
  }

  public func acceptConversation(conversationId: String) throws {
    try bridge.acceptConversation(conversationId: conversationId)
  }

  public func fulfillLeafRecovery(conversationId: String) throws {
    try bridge.fulfillLeafRecovery(conversationId: conversationId)
  }

  public func reportUnrecoverableLocal(conversationId: String, reason: String) throws {
    try bridge.reportUnrecoverableLocal(convoId: conversationId, reason: reason)
  }

  public func recordShadowDecisionMismatch(
    operation: String,
    conversationId: String?,
    swiftDecision: String,
    rustDecision: String
  ) {
    guard mode == .rustShadow else { return }
    logger.warning(
      "Rust shadow decision mismatch operation=\(operation, privacy: .public) conversation=\(conversationId ?? "none", privacy: .private) swift=\(swiftDecision, privacy: .public) rust=\(rustDecision, privacy: .public)"
    )
  }

  public func shutdown() {
    bridge.shutdown()
  }

  private func encodePayloadJson(_ payload: MLSMessagePayload) throws -> String {
    let payloadData = try payload.encodeToJSON()
    guard let payloadJson = String(data: payloadData, encoding: .utf8) else {
      throw MLSConversationError.operationFailed("MLS payload JSON was not valid UTF-8")
    }
    return payloadJson
  }
}

public struct MLSCreateConversationResult: Sendable {
  public let conversation: BlueCatbirdChatDefs.ConversationState
  public let metadata: MLSConversationSnapshotMetadata

  init(ffiResult result: FfiCreateConversationResult, userDID: String) throws {
    self.conversation = try decodeConversationSnapshot(result.conversation, fallbackUserDID: userDID)
    self.metadata = MLSConversationSnapshotMetadata(ffiConversation: result.conversation)
  }

  public init(
    conversation: BlueCatbirdChatDefs.ConversationState,
    metadata: MLSConversationSnapshotMetadata = MLSConversationSnapshotMetadata()
  ) {
    self.conversation = conversation
    self.metadata = metadata
  }
}

public struct MLSGroupMutationResult: Sendable {
  public let conversation: BlueCatbirdChatDefs.ConversationState
  public let metadata: MLSConversationSnapshotMetadata

  init(ffiResult result: FfiGroupMutationResult, userDID: String) throws {
    self.conversation = try decodeConversationSnapshot(result.conversation, fallbackUserDID: userDID)
    self.metadata = MLSConversationSnapshotMetadata(ffiConversation: result.conversation)
  }

  public init(
    conversation: BlueCatbirdChatDefs.ConversationState,
    metadata: MLSConversationSnapshotMetadata = MLSConversationSnapshotMetadata()
  ) {
    self.conversation = conversation
    self.metadata = metadata
  }
}

public struct MLSLeaveConversationResult: Equatable, Sendable {
  public let conversationId: String
  public let groupId: String?

  init(ffiResult result: FfiLeaveResult) {
    self.conversationId = result.conversationId
    self.groupId = result.groupId
  }

  public init(conversationId: String, groupId: String?) {
    self.conversationId = conversationId
    self.groupId = groupId
  }
}

public struct MLSConversationSnapshotMetadata: Equatable, Sendable {
  public let title: String?
  public let description: String?
  public let avatarUrl: String?

  init(ffiConversation: FfiConversationView) {
    self.init(
      title: ffiConversation.name,
      description: ffiConversation.description,
      avatarUrl: ffiConversation.avatarUrl
    )
  }

  public init(
    title: String? = nil,
    description: String? = nil,
    avatarUrl: String? = nil
  ) {
    self.title = Self.nonEmpty(title)
    self.description = Self.nonEmpty(description)
    self.avatarUrl = Self.nonEmpty(avatarUrl)
  }

  func fillingMissingValues(from fallback: MLSConversationSnapshotMetadata) -> MLSConversationSnapshotMetadata {
    MLSConversationSnapshotMetadata(
      title: title ?? fallback.title,
      description: description ?? fallback.description,
      avatarUrl: avatarUrl ?? fallback.avatarUrl
    )
  }

  static func nonEmpty(_ value: String?) -> String? {
    guard let value, !value.isEmpty else { return nil }
    return value
  }
}

public struct MLSJoinOrRejoinResult: Equatable, Sendable {
  public let epoch: UInt64
  public let recoveryState: ConversationRecoveryState

  init(ffiResult result: FfiJoinOrRejoinResult) {
    self.epoch = result.epoch
    self.recoveryState = ConversationRecoveryState(ffiRecoveryState: result.recoveryState)
  }

  public init(epoch: UInt64, recoveryState: ConversationRecoveryState) {
    self.epoch = epoch
    self.recoveryState = recoveryState
  }
}

public struct MLSRegisteredDeviceInfo: Equatable, Sendable {
  public let deviceId: String
  public let mlsDid: String
  public let deviceUUID: String?
  public let createdAt: String?

  public init(deviceId: String, mlsDid: String, deviceUUID: String?, createdAt: String?) {
    self.deviceId = deviceId
    self.mlsDid = mlsDid
    self.deviceUUID = deviceUUID
    self.createdAt = createdAt
  }

  init(ffiDeviceInfo deviceInfo: FfiDeviceInfo) {
    self.init(
      deviceId: deviceInfo.deviceId,
      mlsDid: deviceInfo.mlsDid,
      deviceUUID: deviceInfo.deviceUuid.isEmpty ? nil : deviceInfo.deviceUuid,
      createdAt: deviceInfo.createdAt
    )
  }
}

public struct MLSConversationReadyResult: Equatable, Sendable {
  public let recoveryState: ConversationRecoveryState
  public let epoch: UInt64?
  public let sendAllowed: Bool

  init(ffiResult result: FfiConversationReadyResult) {
    self.recoveryState = ConversationRecoveryState(ffiRecoveryState: result.recoveryState)
    self.epoch = result.epoch
    self.sendAllowed = result.sendAllowed
  }

  public init(
    recoveryState: ConversationRecoveryState,
    epoch: UInt64?,
    sendAllowed: Bool
  ) {
    self.recoveryState = recoveryState
    self.epoch = epoch
    self.sendAllowed = sendAllowed
  }
}

public struct MLSDebugWipeLocalGroupResult: Equatable, Sendable {
  public let conversationId: String
  public let groupId: String?
  public let deletedLocalGroup: Bool

  init(ffiResult result: FfiDebugWipeLocalGroupResult) {
    self.conversationId = result.conversationId
    self.groupId = result.groupId
    self.deletedLocalGroup = result.deletedLocalGroup
  }

  public init(
    conversationId: String,
    groupId: String?,
    deletedLocalGroup: Bool
  ) {
    self.conversationId = conversationId
    self.groupId = groupId
    self.deletedLocalGroup = deletedLocalGroup
  }
}

private func decodeConversationSnapshot(
  _ ffiConversation: FfiConversationView,
  fallbackUserDID: String
) throws -> BlueCatbirdChatDefs.ConversationState {
  let creatorDIDString =
    ffiConversation.members.first(where: { $0.role.lowercased() == "admin" })?.did ?? fallbackUserDID
  let creatorDID = (try? DID(didString: creatorDIDString)) ?? (try! DID(didString: "did:plc:unknown"))
  let participants: [BlueCatbirdChatDefs.ParticipantView] = ffiConversation.members.compactMap { member in
    guard let did = try? DID(didString: member.did) else { return nil }
    return BlueCatbirdChatDefs.ParticipantView(
      userDid: did,
      role: member.role.lowercased() == "admin" ? .value_admin : .value_member,
      status: .value_active,
      invitationProvenance: nil,
      leafCount: 1
    )
  }

  let coordinates = BlueCatbirdChatDefs.ConversationCoordinates(
    conversationId: ffiConversation.conversationId,
    generation: 1,
    stateVersion: 1,
    groupId: Bytes(data: Data(hexEncoded: ffiConversation.groupId) ?? Data()),
    epoch: Int(ffiConversation.epoch),
    groupContextHash: Bytes(data: Data()),
    confirmationTag: Bytes(data: Data()),
    lifecycle: .value_active
  )

  let metadataSnapshot = BlueCatbirdChatDefs.MetadataSnapshot(
    coordinate: BlueCatbirdChatDefs.MetadataCryptoContext(
      conversationId: Bytes(data: Data(ffiConversation.conversationId.utf8)),
      generation: 1,
      groupId: Bytes(data: Data(hexEncoded: ffiConversation.groupId) ?? Data()),
      epoch: Int(ffiConversation.epoch),
      groupContextHash: Bytes(data: Data()),
      confirmationTag: Bytes(data: Data())
    ),
    originTransitionId: ffiConversation.conversationId,
    metadataVersion: 1,
    nonce: Bytes(data: Data()),
    ciphertext: Bytes(data: Data()),
    ciphertextSha256: Bytes(data: Data()),
    ciphertextSize: 0,
    avatarBinding: nil,
    authorProof: BlueCatbirdChatDefs.MetadataAuthorProof(
      authorDid: creatorDID,
      authorDeviceId: "primary",
      authorKeyId: "key-1",
      signaturePublicKey: Bytes(data: Data()),
      authGenerationAtOrigin: 1,
      originTransitionId: ffiConversation.conversationId,
      originSeq: 1,
      roleAtOrigin: "admin",
      deviceStatusAtOrigin: "active"
    )
  )

  return BlueCatbirdChatDefs.ConversationState(
    conversationKind: .value_group,
    coordinates: coordinates,
    cipherSuite: .value_MLS_u5f_256_u5f_XWING_u5f_CHACHA20POLY1305_u5f_SHA256_u5f_Ed25519,
    participants: participants,
    leaves: [],
    metadataSnapshot: metadataSnapshot,
    snapshotSeq: 1,
    sequencerDid: nil,
    sequencerTerm: nil
  )
}

private let ffiConversationDateFormatter: ISO8601DateFormatter = {
  let formatter = ISO8601DateFormatter()
  formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
  return formatter
}()

private func parseFFIISO8601Date(_ value: String?) -> Date? {
  guard let value else { return nil }
  return ffiConversationDateFormatter.date(from: value)
    ?? ISO8601DateFormatter().date(from: value)
}

extension MLSOrchestratorRuntime {
  internal static func messageProcessingOutcome(from message: FfiMessage?) throws -> MessageProcessingOutcome {
    guard let message else { return .nonApplication }

    let payload: MLSMessagePayload
    if let payloadJson = message.payloadJson {
      guard let payloadData = payloadJson.data(using: .utf8) else {
        throw MLSConversationError.operationFailed("Rust message payload JSON was not valid UTF-8")
      }
      payload = try MLSMessagePayload.decodeFromJSON(payloadData)
    } else {
      payload = .text(message.text, embed: nil)
    }

    return .application(payload: payload, sender: message.senderDid)
  }
}

extension ConversationRecoveryState {
  init(ffiRecoveryState state: FfiConversationRecoveryState) {
    switch state {
    case .healthy:
      self = .healthy
    case .epochBehind:
      self = .epochBehind
    case .groupMissing:
      self = .groupMissing
    case .needsRejoin:
      self = .needsRejoin
    case .recovering:
      self = .recovering
    case .unrecoverableLocal:
      self = .unrecoverableLocal
    case .resetPending:
      self = .resetPending
    }
  }
}

public enum MLSResetRecordOutcome: String, Codable, Equatable, Sendable, CaseIterable {
  case recorded
  case staleOrDuplicate
  case selfEchoNoOp

  init(ffiOutcome outcome: FfiResetRecordOutcome) {
    switch outcome {
    case .recorded:
      self = .recorded
    case .staleOrDuplicate:
      self = .staleOrDuplicate
    case .selfEchoNoOp:
      self = .selfEchoNoOp
    }
  }

  public var didRecordResetState: Bool {
    self == .recorded
  }
}
