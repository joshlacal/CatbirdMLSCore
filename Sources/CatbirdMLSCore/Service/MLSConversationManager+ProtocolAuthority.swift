import CatbirdMLS
import Foundation
import GRDB
import Synchronization

private let trackedRustRuntimePostBodyTestOverride = Mutex<(@Sendable () -> Void)?>(nil)

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
      let deviceInfo = try await withRustAuthoritativeRuntime(
        operation: "registeredDeviceInfoForPushTokenRegistration"
      ) { runtime in
        try runtime.ensureDeviceRegistered()
        return try runtime.currentDeviceInfo()
      }
      if deviceInfo != nil, let userDid {
        // Push-token lookups are benign readiness checks. Reuse or coalesce an
        // enrollment already started by device registration; only explicit
        // rebind/reset flows may invalidate the binding generation.
        _ = try await bindRustDeviceAuthentication(userDid: userDid)
      }
      return deviceInfo
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

  func debugWipeLocalGroupForRecovery(
    conversationId: String
  ) async throws -> MLSDebugWipeLocalGroupResult? {
    guard protocolAuthorityMode == .rustFull else {
      return nil
    }

    let result = try await withRustAuthoritativeRuntime(
      operation: "debugWipeLocalGroupForRecovery"
    ) { runtime in
      try runtime.debugWipeLocalGroupForRecovery(conversationId: conversationId)
    }
    logger.info(
      "✅ [MLS-AUTHORITY] debugWipeLocalGroupForRecovery completed for \(conversationId.prefix(16), privacy: .private) groupId=\(result.groupId ?? "nil", privacy: .private) deletedLocalGroup=\(result.deletedLocalGroup, privacy: .public)"
    )
    return result
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
  internal nonisolated static func setTrackedRustRuntimePostBodyTestOverride(
    _ operation: (@Sendable () -> Void)?
  ) {
    trackedRustRuntimePostBodyTestOverride.withLock { $0 = operation }
  }

  internal func buildOrchestratorRuntime(
    suspendedResumeCapability: MLSClient.SuspendedResumeCapability? = nil
  ) async -> MLSOrchestratorRuntime? {
    guard protocolAuthorityMode != .swiftLegacy else { return nil }
    guard let userDid else { return nil }

    guard let databasePool = database as? DatabasePool else {
      logger.warning(
        "⚠️ [MLS-AUTHORITY] Rust orchestrator disabled for \(userDid.prefix(20), privacy: .private): database writer is not DatabasePool"
      )
      return nil
    }

    var acquiredContextIdentity: ObjectIdentifier?
    do {
      try Task.checkCancellation()
      let context: MlsContext
      if let suspendedResumeCapability {
        guard
          MLSClient.isCurrentSuspendedResumeCapability(
            suspendedResumeCapability,
            for: userDid
          )
        else {
          return nil
        }
        context = try await MLSCoreContext.shared.contextForSuspendedResume(
          for: userDid,
          capability: suspendedResumeCapability
        )
        acquiredContextIdentity = ObjectIdentifier(context)
        try Task.checkCancellation()
        guard
          MLSClient.isCurrentSuspendedResumeCapability(
            suspendedResumeCapability,
            for: userDid
          )
        else {
          if let acquiredContextIdentity {
            _ = await MLSCoreContext.shared.removeContext(
              for: userDid,
              matching: acquiredContextIdentity
            )
          }
          return nil
        }
      } else {
        context = try await MLSCoreContext.shared.getContext(for: userDid)
      }
      let apiAdapter = MLSOrchestratorAPIAdapter(apiClient: apiClient)
      return try MLSOrchestratorRuntime(
        userDID: userDid,
        mode: protocolAuthorityMode,
        mlsContext: context,
        databasePool: databasePool,
        apiClient: apiAdapter
      )
    } catch {
      if suspendedResumeCapability != nil, let acquiredContextIdentity {
        _ = await MLSCoreContext.shared.removeContext(
          for: userDid,
          matching: acquiredContextIdentity
        )
      }
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
    var reservationToken: UUID?
    while reservationToken == nil {
      switch orchestratorRuntimeStorage.beginInitialization() {
      case .existing(let runtime):
        return runtime
      case .reserved(let token):
        reservationToken = token
      case .wait:
        guard !Task.isCancelled, rustRuntimeAdmissionIsAllowed(nil) else { return nil }
        try? await Task.sleep(nanoseconds: 5_000_000)
        continue
      }
    }
    guard let reservationToken else { return nil }
    let runtime =
      if let orchestratorRuntimeFactory {
        await orchestratorRuntimeFactory()
      } else {
        await buildOrchestratorRuntime()
      }
    guard let runtime else {
      orchestratorRuntimeStorage.cancelInitialization(token: reservationToken)
      return nil
    }

    do {
      try await withTrackedRustRuntime(
        runtime,
        operation: "initializeOrchestratorRuntime",
        runtimeInstallationReservationToken: reservationToken
      ) { runtime in
        try runtime.initialize()
      }
      rustStartupReconcileCompleted = false
      return runtime
    } catch {
      logger.error(
        "❌ [MLS-AUTHORITY] Failed to initialize Rust orchestrator runtime: \(error.localizedDescription, privacy: .public)"
      )
      return nil
    }
  }

  internal func resetOrchestratorRuntime(reason: String) {
    guard let runtime = orchestratorRuntimeStorage.take() else { return }
    logger.info("🔄 [MLS-AUTHORITY] Resetting Rust orchestrator runtime: \(reason, privacy: .public)")
    runtime.shutdown()
    rustStartupReconcileCompleted = false
  }

  internal func invalidateOrchestratorRuntime(reason: String) {
    guard orchestratorRuntimeStorage.take() != nil else { return }
    logger.info("🔄 [MLS-AUTHORITY] Invalidating Rust orchestrator runtime: \(reason, privacy: .public)")
    rustStartupReconcileCompleted = false
  }

  internal func restoreOrchestratorRuntimeAfterSuspendClose(
    reason: String,
    suspendedResumeCapability: MLSClient.SuspendedResumeCapability
  ) async -> MLSOrchestratorRuntime? {
    guard let userDid,
      !Task.isCancelled,
      MLSClient.isCurrentSuspendedResumeCapability(suspendedResumeCapability, for: userDid)
    else {
      return nil
    }
    guard case .reserved(let reservationToken) =
      orchestratorRuntimeStorage.beginInitialization()
    else {
      return nil
    }
    let runtime: MLSOrchestratorRuntime?
    if let runtimeFactory = orchestratorRuntimeResumeFactory {
      runtime = await runtimeFactory()
    } else {
      runtime = await buildOrchestratorRuntime(
        suspendedResumeCapability: suspendedResumeCapability
      )
    }
    guard let runtime else {
      orchestratorRuntimeStorage.cancelInitialization(token: reservationToken)
      return nil
    }
    guard
      !Task.isCancelled,
      runtime.userDID == MLSStorageHelpers.normalizeDID(userDid),
      MLSClient.isCurrentSuspendedResumeCapability(suspendedResumeCapability, for: userDid)
    else {
      orchestratorRuntimeStorage.cancelInitialization(token: reservationToken)
      if MLSClient.isCurrentSuspendedResumeCapability(suspendedResumeCapability, for: userDid) {
        _ = try? MLSClient.withTrackedFFIAdmission(
          suspendedResumeCapability: suspendedResumeCapability,
          for: userDid
        ) {
          runtime.shutdown()
        }
      }
      // This candidate was never installed. Without exact resume admission it must be discarded,
      // not synchronously torn down underneath a newer suspension's quiescence boundary.
      if let runtimeContextIdentity = runtime.mlsContextIdentity {
        _ = await MLSCoreContext.shared.removeContext(
          for: userDid,
          matching: runtimeContextIdentity
        )
      }
      return nil
    }

    do {
      await orchestratorRuntimeBeforeReattachTestOverride?()
      try Task.checkCancellation()
      try await withTrackedRustRuntime(
        runtime,
        operation: "reattachOrchestratorRuntimeAfterSuspend",
        suspendedResumeCapability: suspendedResumeCapability,
        runtimeInstallationReservationToken: reservationToken
      ) { runtime in
        try runtime.reattachAfterSuspend(reason: reason)
      }
      rustStartupReconcileCompleted = false
      return runtime
    } catch {
      // Cancellation can arrive after the reservation was created but before tracked atomic
      // installation begins. Release only this initializer's token before any async cleanup so
      // waiters can reserve immediately and a newer reservation remains generation-safe.
      orchestratorRuntimeStorage.cancelInitialization(token: reservationToken)
      // A failed or superseded candidate is intentionally never installed. Teardown bridge calls
      // require their own exact admission; the authoritative context owner performs final close.
      if let runtimeContextIdentity = runtime.mlsContextIdentity {
        _ = await MLSCoreContext.shared.removeContext(
          for: userDid,
          matching: runtimeContextIdentity
        )
      }
      logger.error(
        "❌ [MLS-AUTHORITY] Failed to reattach Rust orchestrator runtime after suspend close: \(error.localizedDescription, privacy: .public)"
      )
      return nil
    }
  }

  /// Dedicated executor for the synchronous Rust orchestrator bridge.
  ///
  /// The UniFFI bridge body blocks its calling thread for the entire crypto +
  /// network round-trip (`block_in_place` on the Rust side) and re-enters Swift
  /// synchronously through `MLSOrchestratorAPIAdapter.blocking` — a
  /// `DispatchSemaphore.wait()`. Running that on the Swift cooperative thread
  /// pool (or the main thread) starves the bounded pool: every blocked worker
  /// also spawns a `Task.detached` that needs a *free* cooperative worker to make
  /// progress, so under concurrency the pool deadlocks and the app hard-freezes.
  /// GCD grows this concurrent pool on demand, so blocking its threads is safe.
  private static let rustAuthorityExecutionQueue = DispatchQueue(
    label: "blue.catbird.mls.rust-authority",
    qos: .userInitiated,
    attributes: .concurrent
  )

  internal func withRustAuthoritativeRuntime<T>(
    operation: String,
    suspendedResumeCapability: MLSClient.SuspendedResumeCapability? = nil,
    body: @escaping (MLSOrchestratorRuntime) throws -> T
  ) async throws -> T {
    guard protocolAuthorityMode.usesRustForDecisions else {
      throw MLSConversationError.operationFailed("Rust authority requested while mode is \(protocolAuthorityMode.rawValue)")
    }
    guard rustRuntimeAdmissionIsAllowed(suspendedResumeCapability) else {
      throw MLSConversationError.operationFailed(
        "Rust authority blocked by lifecycle suspension for \(operation)"
      )
    }
    guard let runtime = await ensureOrchestratorRuntime() else {
      throw MLSConversationError.operationFailed("Rust orchestrator runtime unavailable for \(operation)")
    }

    return try await withTrackedRustRuntime(
      runtime,
      operation: operation,
      suspendedResumeCapability: suspendedResumeCapability,
      body: body
    )
  }

  /// Executes a synchronous call against an already-resolved runtime while preserving
  /// that exact runtime generation and participating in the suspension drain.
  internal func withTrackedRustRuntime<T>(
    _ runtime: MLSOrchestratorRuntime,
    operation: String,
    suspendedResumeCapability: MLSClient.SuspendedResumeCapability? = nil,
    runtimeInstallationReservationToken: UUID? = nil,
    body: @escaping (MLSOrchestratorRuntime) throws -> T
  ) async throws -> T {
    let runtimeStorage = orchestratorRuntimeStorage
    guard protocolAuthorityMode != .swiftLegacy else {
      if let runtimeInstallationReservationToken {
        runtimeStorage.cancelInitialization(token: runtimeInstallationReservationToken)
      }
      throw MLSConversationError.operationFailed(
        "Rust authority requested while mode is \(protocolAuthorityMode.rawValue)"
      )
    }
    guard rustRuntimeAdmissionIsAllowed(suspendedResumeCapability) else {
      if let runtimeInstallationReservationToken {
        runtimeStorage.cancelInitialization(token: runtimeInstallationReservationToken)
      }
      throw MLSConversationError.operationFailed(
        "Rust authority blocked by lifecycle suspension for \(operation)"
      )
    }

    // Execute the synchronous bridge body OFF the caller's executor (main thread or
    // Swift cooperative pool) on the dedicated concurrent GCD queue, freeing the
    // bounded cooperative pool while the FFI `block_in_place` + nested
    // `DispatchSemaphore.wait()` runs (otherwise the pool starves and the app
    // hard-freezes — the original rustFull deadlock).
    //
    // `body` is `@escaping` here ON PURPOSE. The earlier `withoutActuallyEscaping`
    // wrapper trapped at runtime (`swift_isEscapingClosureAtFileLocation`): a
    // `CheckedContinuation` may resume INLINE on the resuming thread, so the awaiting
    // task can return out of the `withoutActuallyEscaping` scope while the dispatched
    // block still references the closure (refcount > 1) → fatal escape assertion. With
    // a genuinely escaping body, ARC owns its lifetime and there is no escape check to
    // race. The body still does not outlive this call — we await its completion before
    // returning.
    let result = try await withCheckedThrowingContinuation {
      (continuation: CheckedContinuation<T, Error>) in
      Self.rustAuthorityExecutionQueue.async {
        let bridgeResult = Result {
          if let runtimeInstallationReservationToken {
            try MLSClient.withTrackedFFIAdmissionAndAtomicRuntimeInstall(
              suspendedResumeCapability: suspendedResumeCapability,
              for: runtime.userDID,
              operation: { try body(runtime) },
              runtime: runtime,
              runtimeStorage: runtimeStorage,
              reservationToken: runtimeInstallationReservationToken
            )
          } else {
            try MLSClient.withTrackedFFIAdmission(
              suspendedResumeCapability: suspendedResumeCapability,
              for: runtime.userDID
            ) {
              try body(runtime)
            }
          }
        }
        trackedRustRuntimePostBodyTestOverride.withLock { $0 }?()
        continuation.resume(with: bridgeResult)
      }
    }
    // Admission and drain ownership are decided before and during the synchronous bridge call.
    // Once `body` returns, a non-idempotent Rust mutation may already be committed. A suspension
    // that begins before this awaiting task resumes must close future admissions, but must not
    // rewrite the completed operation into a retryable failure or discard its exact result.
    return result
  }

  private func rustRuntimeAdmissionIsAllowed(
    _ suspendedResumeCapability: MLSClient.SuspendedResumeCapability?
  ) -> Bool {
    if let suspendedResumeCapability {
      guard let userDid else { return false }
      return MLSClient.isCurrentSuspendedResumeCapability(
        suspendedResumeCapability,
        for: userDid
      )
    }
    return !isSuspending
      && !MLSClient.isSuspensionInProgress
      && !MLSCoreContext.isSuspensionInProgress
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

    let swiftState = await swiftRecoveryState(
      conversationId: conversationId,
      override: swiftDecision
    )
    guard let runtime = await ensureOrchestratorRuntime() else { return }
    do {
      try await withTrackedRustRuntime(
        runtime,
        operation: "mirrorRustRecoveryState.\(operation)"
      ) { runtime in
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
    } catch {
      logger.error(
        "❌ [MLS-SHADOW] Unable to admit Rust recovery mirror for \(operation, privacy: .public): \(error.localizedDescription, privacy: .public)"
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
