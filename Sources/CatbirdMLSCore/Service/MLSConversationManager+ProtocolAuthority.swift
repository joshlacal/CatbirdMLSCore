import CatbirdMLS
import Foundation
import GRDB

public extension MLSConversationManager {
  var isRustProtocolAuthorityEnabled: Bool {
    protocolAuthorityMode != .swiftLegacy
  }
}

extension MLSConversationManager {
  internal func ensureOrchestratorRuntime() async -> MLSOrchestratorRuntime? {
    guard protocolAuthorityMode != .swiftLegacy else { return nil }
    if let orchestratorRuntime { return orchestratorRuntime }
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
      let runtime = MLSOrchestratorRuntime(
        userDID: userDid,
        mode: protocolAuthorityMode,
        mlsContext: context,
        databasePool: databasePool,
        apiClient: apiAdapter
      )
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
}
