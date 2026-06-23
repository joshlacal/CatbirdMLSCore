import CatbirdMLS
import Foundation
import GRDB
import OSLog

/// Owns the Rust orchestrator bridge and its Swift callback adapters.
///
/// The runtime does not change Catbird iOS behavior by itself. Callers opt into
/// `rustShadow` to mirror decisions for telemetry, then `rustAuthoritative` to
/// delegate protocol decisions after parity is proven. Swift still owns
/// SQLCipher/GRDB lifecycle, Keychain setup, App Group paths, NSE coordination,
/// suspension, shutdown, and 0xdead10cc handling.
public final class MLSOrchestratorRuntime: @unchecked Sendable {
  public let userDID: String
  public let mode: MLSProtocolAuthorityMode

  public let bridge: OrchestratorBridge

  private let storageAdapter: OrchestratorStorageCallback
  private let apiClient: OrchestratorApiCallback
  private let credentialAdapter: OrchestratorCredentialCallback
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
      authorizedDeviceKeyResolver: authorizedDeviceKeyResolver
    )

    self.userDID = normalizedDID
    self.mode = mode
    self.storageAdapter = storageAdapter
    self.apiClient = apiClient
    self.credentialAdapter = credentialAdapter
    self.eventCallback = eventCallback
    bridge = OrchestratorBridge(
      mlsContext: mlsContext,
      storage: storageAdapter,
      apiClient: apiClient,
      credentials: credentialAdapter,
      config: config ?? .default
    )
    bridge.setEventCallback(callback: eventCallback)
    bridge.setStoreControlMessages(enabled: mode.usesRustForDecisions)

    logger.info(
      "MLSOrchestratorRuntime initialized mode=\(mode.rawValue, privacy: .public) user=\(normalizedDID.prefix(20), privacy: .private)"
    )
  }

  public func initialize() throws {
    try bridge.initialize(userDid: userDID)
  }

  public func syncWithServer(fullSync: Bool) throws {
    try bridge.syncWithServer(fullSync: fullSync)
  }

  @discardableResult
  public func sendMessage(conversationId: String, text: String) throws -> FfiMessage {
    try bridge.sendMessage(conversationId: conversationId, text: text)
  }

  @discardableResult
  public func sendPayload(conversationId: String, payload: MLSMessagePayload) throws -> FfiMessage {
    let payloadData = try payload.encodeToJSON()
    guard let payloadJson = String(data: payloadData, encoding: .utf8) else {
      throw MLSConversationError.operationFailed("MLS payload JSON was not valid UTF-8")
    }
    return try bridge.sendPayloadJson(conversationId: conversationId, payloadJson: payloadJson)
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

  public func performSilentRecovery(conversationIds: [String]) throws {
    try bridge.performSilentRecovery(conversationIds: conversationIds)
  }

  @discardableResult
  public func ensureDeviceRegistered() throws -> String {
    try bridge.ensureDeviceRegistered()
  }

  public func conversationRecoveryState(conversationId: String) throws -> ConversationRecoveryState {
    let ffiState = try bridge.getConversationRecoveryState(conversationId: conversationId)
    return ConversationRecoveryState(ffiRecoveryState: ffiState)
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
