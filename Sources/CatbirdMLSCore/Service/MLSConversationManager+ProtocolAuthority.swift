import CatbirdMLS
import Foundation
import GRDB

public struct MLSConversationDiagnosticsProjection: Equatable, Sendable {
  public let recoveryState: ConversationRecoveryState
  public let epoch: UInt64?
  public let sendAllowed: Bool?

  public init(
    recoveryState: ConversationRecoveryState,
    epoch: UInt64?,
    sendAllowed: Bool?
  ) {
    self.recoveryState = recoveryState
    self.epoch = epoch
    self.sendAllowed = sendAllowed
  }
}

public extension MLSConversationManager {
  var isRustProtocolAuthorityEnabled: Bool {
    protocolAuthorityMode != .swiftLegacy
  }

  @discardableResult
  func joinOrRejoinConversation(conversationId: String) async throws -> MLSJoinOrRejoinResult? {
    if protocolAuthorityMode.usesRustForDecisions {
      return try await joinOrRejoinWithRustAuthorityIfNeeded(
        conversationId: conversationId,
        operation: "joinOrRejoinConversation"
      )
    }

    guard let userDid else {
      throw MLSConversationError.noAuthentication
    }

    _ = try await mlsClient.joinByExternalCommit(for: userDid, convoId: conversationId)
    return nil
  }

  func registeredDeviceInfoForPushTokenRegistration() async throws -> MLSRegisteredDeviceInfo? {
    if protocolAuthorityMode == .rustFull {
      return try await withRustAuthoritativeRuntime(
        operation: "registeredDeviceInfoForPushTokenRegistration"
      ) { runtime in
        try runtime.ensureDeviceRegistered()
        return try runtime.currentDeviceInfo()
      }
    }

    guard let userDid else {
      throw MLSConversationError.noAuthentication
    }

    _ = try await mlsClient.ensureDeviceRegistered(userDid: userDid)
    guard let deviceInfo = await mlsClient.getDeviceInfo(for: userDid) else {
      return nil
    }
    return MLSRegisteredDeviceInfo(
      deviceId: deviceInfo.deviceId,
      mlsDid: deviceInfo.mlsDid,
      deviceUUID: deviceInfo.deviceUUID,
      createdAt: nil
    )
  }

  func conversationDiagnosticsProjection(
    conversationId: String,
    ensureReady: Bool = false
  ) async throws -> MLSConversationDiagnosticsProjection {
    let fallback = await cachedOrStoredConversationProjection(conversationId: conversationId)

    if protocolAuthorityMode == .rustFull {
      return try await withRustAuthoritativeRuntime(
        operation: ensureReady
          ? "conversationDiagnosticsProjection.ensureReady"
          : "conversationDiagnosticsProjection.readOnly"
      ) { runtime in
        if ensureReady {
          let ready = try runtime.ensureConversationReady(conversationId: conversationId)
          return MLSConversationDiagnosticsProjection(
            recoveryState: ready.recoveryState,
            epoch: ready.epoch ?? fallback?.epoch,
            sendAllowed: ready.sendAllowed
          )
        }

        let recoveryState = try runtime.conversationRecoveryState(conversationId: conversationId)
        let rustEpoch = try? runtime.listConversations()
          .first { $0.conversationId == conversationId }
          .map { UInt64($0.epoch) }

        return MLSConversationDiagnosticsProjection(
          recoveryState: recoveryState,
          epoch: rustEpoch ?? fallback?.epoch,
          sendAllowed: nil
        )
      }
    }

    return MLSConversationDiagnosticsProjection(
      recoveryState: await swiftRecoveryState(conversationId: conversationId),
      epoch: await legacyLocalEpochForDiagnostics(conversationId: conversationId, fallback: fallback),
      sendAllowed: nil
    )
  }
}

extension MLSConversationManager {
  internal func buildOrchestratorRuntime() async -> MLSOrchestratorRuntime? {
    guard protocolAuthorityMode != .swiftLegacy else { return nil }
    guard let userDid else { return nil }

    guard let databasePool = database as? DatabasePool else {
      logger.warning(
        "⚠️ [MLS-AUTHORITY] Rust orchestrator disabled for \(userDid.prefix(20), privacy: .private): database writer is not DatabasePool"
      )
      return nil
    }

    do {
      let context = try await MLSCoreContext.shared.getContext(for: userDid)
      let apiAdapter = MLSOrchestratorAPIAdapter(apiClient: apiClient)
      return MLSOrchestratorRuntime(
        userDID: userDid,
        mode: protocolAuthorityMode,
        mlsContext: context,
        databasePool: databasePool,
        apiClient: apiAdapter
      )
    } catch {
      logger.error(
        "❌ [MLS-AUTHORITY] Failed to build Rust orchestrator runtime: \(error.localizedDescription, privacy: .public)"
      )
      return nil
    }
  }

  internal func assertSwiftProtocolMutationAllowed(_ operation: StaticString) throws {
    guard !protocolAuthorityMode.requiresRustOnlyProtocolMutations else {
      logger.fault(
        "[MLS-FULL-RUST] Swift protocol mutation blocked: \(String(describing: operation), privacy: .public)"
      )
      throw MLSConversationError.operationFailed("Swift MLS protocol mutation blocked in rustFull mode")
    }
  }

  internal func ensureOrchestratorRuntime() async -> MLSOrchestratorRuntime? {
    guard protocolAuthorityMode != .swiftLegacy else { return nil }
    if let orchestratorRuntime { return orchestratorRuntime }
    guard let runtime = await buildOrchestratorRuntime() else { return nil }

    do {
      try runtime.initialize()
      orchestratorRuntime = runtime
      return runtime
    } catch {
      logger.error(
        "❌ [MLS-AUTHORITY] Failed to initialize Rust orchestrator runtime: \(error.localizedDescription, privacy: .public)"
      )
      return nil
    }
  }

  internal func resetOrchestratorRuntime(reason: String) {
    guard let runtime = orchestratorRuntime else { return }
    logger.info("🔄 [MLS-AUTHORITY] Resetting Rust orchestrator runtime: \(reason, privacy: .public)")
    runtime.shutdown()
    orchestratorRuntime = nil
    rustStartupReconcileCompleted = false
  }

  internal func invalidateOrchestratorRuntime(reason: String) {
    guard orchestratorRuntime != nil else { return }
    logger.info("🔄 [MLS-AUTHORITY] Invalidating Rust orchestrator runtime: \(reason, privacy: .public)")
    orchestratorRuntime = nil
    rustStartupReconcileCompleted = false
  }

  internal func restoreOrchestratorRuntimeAfterSuspendClose(
    reason: String
  ) async -> MLSOrchestratorRuntime? {
    let runtime: MLSOrchestratorRuntime?
    if let runtimeFactory = orchestratorRuntimeResumeFactory {
      runtime = await runtimeFactory()
    } else {
      runtime = await buildOrchestratorRuntime()
    }
    guard let runtime else { return nil }

    do {
      try runtime.reattachAfterSuspend(reason: reason)
      orchestratorRuntime = runtime
      rustStartupReconcileCompleted = false
      return runtime
    } catch {
      logger.error(
        "❌ [MLS-AUTHORITY] Failed to reattach Rust orchestrator runtime after suspend close: \(error.localizedDescription, privacy: .public)"
      )
      return nil
    }
  }

  internal func withRustAuthoritativeRuntime<T>(
    operation: String,
    body: (MLSOrchestratorRuntime) throws -> T
  ) async throws -> T {
    guard protocolAuthorityMode.usesRustForDecisions else {
      throw MLSConversationError.operationFailed("Rust authority requested while mode is \(protocolAuthorityMode.rawValue)")
    }
    guard let runtime = await ensureOrchestratorRuntime() else {
      throw MLSConversationError.operationFailed("Rust orchestrator runtime unavailable for \(operation)")
    }
    return try body(runtime)
  }

  internal func joinOrRejoinWithRustAuthorityIfNeeded(
    conversationId: String,
    operation: String
  ) async throws -> MLSJoinOrRejoinResult? {
    guard protocolAuthorityMode.usesRustForDecisions else {
      await mirrorRustRecoveryState(operation: operation, conversationId: conversationId)
      return nil
    }

    let result = try await withRustAuthoritativeRuntime(operation: operation) { runtime in
      try runtime.joinOrRejoin(conversationId: conversationId)
    }
    logger.info(
      "✅ [MLS-AUTHORITY] \(operation, privacy: .public) Rust joinOrRejoin completed for \(conversationId.prefix(16), privacy: .private) epoch=\(result.epoch, privacy: .public) state=\(result.recoveryState.rawValue, privacy: .public)"
    )
    return result
  }

  internal func mirrorRustRecoveryState(
    operation: String,
    conversationId: String,
    swiftDecision: ConversationRecoveryState? = nil
  ) async {
    guard protocolAuthorityMode == .rustShadow else { return }
    guard let runtime = await ensureOrchestratorRuntime() else { return }

    let swiftState = await swiftRecoveryState(conversationId: conversationId, override: swiftDecision)
    do {
      let rustState = try runtime.conversationRecoveryState(conversationId: conversationId)
      if rustState != swiftState {
        runtime.recordShadowDecisionMismatch(
          operation: operation,
          conversationId: conversationId,
          swiftDecision: swiftState.rawValue,
          rustDecision: rustState.rawValue
        )
      }
    } catch {
      runtime.recordShadowDecisionMismatch(
        operation: operation,
        conversationId: conversationId,
        swiftDecision: swiftState.rawValue,
        rustDecision: "error:\(String(describing: error))"
      )
    }
  }

  internal func mirrorRustRecoveryStates(
    operation: String,
    conversationIds: [String]
  ) async {
    guard protocolAuthorityMode == .rustShadow else { return }
    for conversationId in conversationIds {
      await mirrorRustRecoveryState(operation: operation, conversationId: conversationId)
    }
  }

  internal func swiftRecoveryState(
    conversationId: String,
    override: ConversationRecoveryState? = nil
  ) async -> ConversationRecoveryState {
    if let override { return override }
    if await conversationNeedsReset(conversationId) { return .resetPending }
    if await conversationNeedsRejoin(conversationId) { return .needsRejoin }

    guard let convo = conversations[conversationId],
          let groupId = Data(hexEncoded: convo.groupId),
          let userDid
    else {
      return .groupMissing
    }

    let exists = await mlsClient.groupExists(for: userDid, groupId: groupId)
    return exists ? .healthy : .groupMissing
  }

  private func cachedOrStoredConversationProjection(
    conversationId: String
  ) async -> (groupID: Data?, epoch: UInt64)? {
    if let convo = conversations[conversationId] {
      return (Data(hexEncoded: convo.groupId), UInt64(convo.epoch))
    }

    guard let userDid,
          let model = try? await storage.fetchConversation(
            conversationID: conversationId,
            currentUserDID: userDid,
            database: database
          )
    else {
      return nil
    }

    return (model.groupID, UInt64(model.epoch))
  }

  private func legacyLocalEpochForDiagnostics(
    conversationId: String,
    fallback: (groupID: Data?, epoch: UInt64)?
  ) async -> UInt64? {
    guard let userDid,
          let groupID = fallback?.groupID
    else {
      return fallback?.epoch
    }

    return (try? await mlsClient.getEpoch(for: userDid, groupId: groupID)) ?? fallback?.epoch
  }
}
