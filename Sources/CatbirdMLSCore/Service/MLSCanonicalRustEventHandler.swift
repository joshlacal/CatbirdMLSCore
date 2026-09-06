import Foundation
import PetrelCatbird

/// The platform forwards generated hints; Rust alone reads authoritative state
/// and decides whether any membership or recovery work is permitted.
internal enum MLSCanonicalRustEventHandler {
  static func make(
    prepareInventory: @escaping () async throws -> Void,
    processEvent: @escaping (String) async throws -> Void
  ) -> MLSWebSocketManager.EventHandler {
    func forward<T: Encodable>(_ value: T) async throws {
      let json = String(decoding: try JSONEncoder().encode(value), as: UTF8.self)
      try await processEvent(json)
    }
    func changed(_ id: BlueCatbirdChatDefs.OperationId) async throws {
      try await forward(BlueCatbirdChatDefs.ConversationChangedEvent(conversationId: id))
    }
    var handler = MLSWebSocketManager.EventHandler(
      onCanonicalInventoryReconciliationStarted: prepareInventory,
      onCanonicalConversationInventoryState: { try await changed($0.coordinates.conversationId) },
      onCanonicalConversationRemovalTombstone: { value in
        try await forward(BlueCatbirdChatDefs.AccessEndedEvent(
          conversationId: value.conversationId, membershipIntervalId: value.membershipIntervalId,
          userDid: value.userDid, deviceId: value.deviceId, terminalSeq: value.terminalSeq))
      },
      onCanonicalConversationCloseTombstone: { value in
        try await forward(BlueCatbirdChatDefs.ConversationClosedEvent(
          conversationId: value.conversationId, conversationKind: value.conversationKind,
          terminalSeq: value.terminalSeq))
      },
      onCanonicalPendingWelcome: { value in
        try await forward(BlueCatbirdChatDefs.WelcomeAvailableEvent(
          welcomeId: value.welcomeId, conversationId: value.conversationId))
      },
      onCanonicalLeafRecovery: { item in
        switch item {
        case .blueCatbirdChatDefsLeafRecoveryView(let value): try await changed(value.conversationId)
        case .blueCatbirdChatDefsRecoveryWorkPendingView(let value): try await changed(value.conversationId)
        case .blueCatbirdChatDefsRecoveryWorkCompletedByTransitionView(let value): try await changed(value.conversationId)
        case .blueCatbirdChatDefsRecoveryWorkSupersededByTransitionView(let value): try await changed(value.conversationId)
        case .blueCatbirdChatDefsRecoveryWorkSupersededByRevocationView(let value): try await changed(value.conversationId)
        case .unexpected:
          throw MLSConversationError.operationFailed("This version of Catbird cannot process this recovery update.")
        }
      },
      onCanonicalDurableEventActions: .init(
        onConversationChanged: { try await forward($0) },
        onConversationClosed: { try await forward($0) },
        // Entries fetched by the stream are only hints here. Rust does its own
        // authenticated catch-up and applies every entry before acknowledging.
        onMessageAvailable: { event, _, _ in try await forward(event) },
        onWelcomeAvailable: { try await forward($0) },
        onWelcomeDisposition: { try await forward($0) },
        onResetRequested: { try await forward($0) },
        onLeafRecovery: { try await forward($0) },
        onLeaveRequest: { try await forward($0) },
        onAccessEnded: { try await forward($0) },
        onWatermark: { try await forward($0) },
        // Typing has no durable state; the chat composer uses encrypted typing.
        onTyping: { _ in }
      )
    )
    // Retained terminal requests can greatly outnumber conversations. Their
    // individual IDs are not native effects: each arm above schedules the
    // same authenticated current-state read. Reconcile it once per CID in
    // this aggregate's recovery phase, still awaiting durable projection.
    handler.onCanonicalRecoveryConversationState = changed
    return handler
  }
}

extension MLSConversationManager {
  public func makeCanonicalWebSocketHandler() -> MLSWebSocketManager.EventHandler {
    MLSCanonicalRustEventHandler.make(
      prepareInventory: { [weak self] in
        guard let self else { throw CancellationError() }
        guard await self.runRustStartupReconcileIfNeeded(operation: "canonicalInventoryStartup") else {
          throw MLSConversationError.operationFailed("Secure chat is still reconnecting. Please try again.")
        }
      },
      processEvent: { [weak self] json in
        guard let self else { throw CancellationError() }
        try await self.processCanonicalServerEvent(json)
      }
    )
  }

  internal func processCanonicalServerEvent(_ json: String) async throws {
    let generation = sessionGeneration
    try throwIfShuttingDown("canonicalServerEvent")
    guard let userDid else { throw MLSConversationError.noAuthentication }
    try await MLSCanonicalEventTransaction.run(userDid: userDid, read: { [self] in
      try Task.checkCancellation()
      try validateSessionGeneration(capturedGeneration: generation)
      try throwIfShuttingDown("canonicalServerEvent")
      return try await withRustAuthoritativeRuntime(operation: "canonicalServerEvent") { runtime in
        let events = try runtime.processServerEvent(eventJson: json)
        return (events, try runtime.listConversationSnapshots())
      }
    }, apply: { [self] events, snapshots in
      try Task.checkCancellation()
      try validateSessionGeneration(capturedGeneration: generation)
      try throwIfShuttingDown("canonicalServerEvent projection")
      let affectedIDs = Set(events.map(\.conversationId)).filter { !$0.isEmpty }
      try await persistRustConversationSnapshots(snapshots, reason: "canonicalServerEvent")
      // Observed caches and notifications are published together on the UI
      // executor, while the per-user transaction still excludes other events.
      try await MainActor.run {
        try Task.checkCancellation()
        try validateSessionGeneration(capturedGeneration: generation)
        try throwIfShuttingDown("canonicalServerEvent publish")
        for id in affectedIDs { notifyObservers(.messagesUpdated(convoId: id, count: 0)) }
        if !events.isEmpty { notifyObservers(.syncCompleted(snapshots.count)) }
      }
    })
  }
}

/// Keep the authoritative read and every awaited projection write under one
/// permit. Releasing after the read lets overlapping streams publish backwards.
internal enum MLSCanonicalEventTransaction {
  static func run<Projection>(
    userDid: String,
    read: () async throws -> Projection,
    apply: (Projection) async throws -> Void
  ) async throws {
    try await withMLSUserPermit(for: userDid) {
      let projection = try await read()
      try await apply(projection)
    }
  }
}
