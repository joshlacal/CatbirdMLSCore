import Foundation
import OSLog
import Petrel
import PetrelCatbird

/// Manages WebSocket subscriptions for MLS conversations
/// Provides real-time message delivery using WebSocket with DAG-CBOR encoding
/// Actor isolation keeps long-running stream work off the main thread while
/// preserving thread-safe access to subscription state.
public actor MLSWebSocketManager {
  private let logger = Logger(subsystem: "blue.catbird", category: "MLSWebSocket")

  // MARK: - Properties

  private let apiClient: MLSAPIClient
  private var activeSubscriptions: [String: Task<Void, Never>] = [:]
  private var subscriptionRuns: [String: MLSCanonicalSubscriptionRun] = [:]
  private var eventHandlers: [String: EventHandler] = [:]
  public typealias HandlerProvider = @Sendable () async -> EventHandler?
  private var eventHandlerProviders: [String: HandlerProvider] = [:]
  private var connectionState: [String: ConnectionState] = [:]
  private var lastCursor: [String: String] = [:]

  /// Flags to signal graceful shutdown (not cancellation)
  private var shouldStop: [String: Bool] = [:]

  /// Optional persistent cursor storage (survives app restart)
  private var cursorStore: MLSEventCursorStore?

  /// Retains an unwritten terminal record after a subscription run exits.
  /// Reconnect consumes it only after the exact scoped record is durably
  /// written to the current protected store.
  private let canonicalSubscriptionFailureLifecycle =
    MLSCanonicalSubscriptionFailureLifecycle()

  /// Optional stable device identity supplied by the host. When omitted,
  /// the manager derives one from the existing protected device identity.
  /// A missing identity deliberately disables durable failure persistence
  /// rather than creating an insecure process-global latch.
  private var canonicalSubscriptionDeviceIdentifier: String?

  // MARK: - Types

  public enum ConnectionState {
    case disconnected
    case connecting
    case connected
    case reconnecting
    case error(Error)
  }

  public enum WebSocketError: Error, LocalizedError {
    case ticketExpired
    case invalidFrame
    case errorFrame(String)
    case decodingFailed(String)
    case connectionClosed
    case invalidURL

    public var errorDescription: String? {
      switch self {
      case .ticketExpired:
        return "WebSocket ticket expired"
      case .invalidFrame:
        return "Invalid WebSocket frame format"
      case .errorFrame(let message):
        return "Server error: \(message)"
      case .decodingFailed(let details):
        return "Failed to decode frame: \(details)"
      case .connectionClosed:
        return "WebSocket connection closed"
      case .invalidURL:
        return "Invalid WebSocket URL"
      }
    }
  }

  public struct EventHandler {
    /// Canonical inventory actions. Every item returned by the aggregate
    /// must reach one of these concrete reconciliation closures before the
    /// snapshot cursor is installed.
    public var onCanonicalInventoryReconciliationStarted: (() async throws -> Void)?
    internal var onCanonicalInventoryBatch: ((MLSCanonicalInventorySnapshot) async throws -> Void)?
    public var onCanonicalConversationInventoryState:
      ((BlueCatbirdChatDefs.ConversationState) async throws -> Void)?
    {
      didSet { onCanonicalInventoryBatch = nil }
    }
    public var onCanonicalConversationRemovalTombstone:
      ((BlueCatbirdChatDefs.ConversationRemovalTombstone) async throws -> Void)?
    {
      didSet { onCanonicalInventoryBatch = nil }
    }
    public var onCanonicalConversationCloseTombstone:
      ((BlueCatbirdChatDefs.ConversationCloseTombstone) async throws -> Void)?
    {
      didSet { onCanonicalInventoryBatch = nil }
    }
    public var onCanonicalPendingWelcome: ((BlueCatbirdChatDefs.WelcomeView) async throws -> Void)?
    {
      didSet { onCanonicalInventoryBatch = nil }
    }
    public var onCanonicalLeafRecovery:
      ((BlueCatbirdChatDefs.LeafRecoveryInboxItem) async throws -> Void)?
    {
      didSet {
        onCanonicalRecoveryConversationState = nil
        onCanonicalInventoryBatch = nil
      }
    }
    /// Installed only by Core's current-state consumer. Replacing the
    /// public item callback restores ordinary per-item dispatch.
    internal var onCanonicalRecoveryConversationState:
      MLSCanonicalInventoryActionSet.RecoveryConversationStateHandler?
    /// Required typed actions for every canonical durable arm. The action
    /// table is optional as a whole so a missing consumer fails closed at
    /// dispatch time; individual missing arms also throw rather than
    /// becoming successful no-ops.
    public var onCanonicalDurableEventActions:
      MLSCanonicalTransportAdapter.MLSCanonicalDurableEventActions?
    public var onMembershipChanged: ((String, DID, MembershipAction) async -> Void)?
    public var onKickedFromConversation: ((String, DID, String?) async -> Void)?
    public var onConversationNeedsRecovery: ((String, RecoveryReason) async -> Void)?
    public var onError: ((Error) async -> Void)?
    public var onReconnected: (() async -> Void)?

    public init() {}

    public init(
      onCanonicalInventoryReconciliationStarted: (() async throws -> Void)? = nil,
      onCanonicalConversationInventoryState: (
        (BlueCatbirdChatDefs.ConversationState) async throws -> Void
      )? = nil,
      onCanonicalConversationRemovalTombstone: (
        (BlueCatbirdChatDefs.ConversationRemovalTombstone) async throws -> Void
      )? = nil,
      onCanonicalConversationCloseTombstone: (
        (BlueCatbirdChatDefs.ConversationCloseTombstone) async throws -> Void
      )? = nil,
      onCanonicalPendingWelcome: ((BlueCatbirdChatDefs.WelcomeView) async throws -> Void)? = nil,
      onCanonicalLeafRecovery: ((BlueCatbirdChatDefs.LeafRecoveryInboxItem) async throws -> Void)? =
        nil,
      onCanonicalDurableEventActions: MLSCanonicalTransportAdapter
        .MLSCanonicalDurableEventActions? = nil,
      onMembershipChanged: ((String, DID, MembershipAction) async -> Void)? = nil,
      onKickedFromConversation: ((String, DID, String?) async -> Void)? = nil,
      onConversationNeedsRecovery: ((String, RecoveryReason) async -> Void)? = nil,
      onError: ((Error) async -> Void)? = nil,
      onReconnected: (() async -> Void)? = nil
    ) {
      self.onCanonicalInventoryReconciliationStarted = onCanonicalInventoryReconciliationStarted
      self.onCanonicalConversationInventoryState = onCanonicalConversationInventoryState
      self.onCanonicalConversationRemovalTombstone = onCanonicalConversationRemovalTombstone
      self.onCanonicalConversationCloseTombstone = onCanonicalConversationCloseTombstone
      self.onCanonicalPendingWelcome = onCanonicalPendingWelcome
      self.onCanonicalLeafRecovery = onCanonicalLeafRecovery
      self.onCanonicalDurableEventActions = onCanonicalDurableEventActions
      self.onMembershipChanged = onMembershipChanged
      self.onKickedFromConversation = onKickedFromConversation
      self.onConversationNeedsRecovery = onConversationNeedsRecovery
      self.onError = onError
      self.onReconnected = onReconnected
    }
  }

  // MARK: - Initialization

  public init(apiClient: MLSAPIClient) {
    self.apiClient = apiClient
  }

  // MARK: - Configuration

  /// Configure persistent cursor storage for surviving app restarts
  public func configureCursorStore(_ store: MLSEventCursorStore) {
    cursorStore = store
    logger.info("CursorStore configured for persistent cursor storage")
  }

  /// Override the device component used to scope durable canonical
  /// subscription failures. The value must be stable for this device and
  /// must not be shared across accounts or environments.
  public func configureCanonicalSubscriptionDeviceIdentifier(_ identifier: String?) {
    let normalized = identifier?.trimmingCharacters(in: .whitespacesAndNewlines)
    canonicalSubscriptionDeviceIdentifier = normalized.flatMap { $0.isEmpty ? nil : $0 }
  }

  // MARK: - Public Methods

  /// Subscribe to real-time events for a conversation via WebSocket
  /// - Parameters:
  ///   - convoId: Conversation ID to subscribe to. If nil, subscribes to ALL conversations (global stream).
  ///   - cursor: Optional cursor to resume from (for reconnection)
  ///   - handler: Event handler for different event types
  public func subscribe(
    to convoId: String?,
    cursor: String? = nil,
    handler: EventHandler,
    handlerProvider: HandlerProvider? = nil
  ) {
    let key = convoId ?? "__global__"
    let logPrefix = convoId != nil ? "convoId: \(convoId!)" : "GLOBAL"

    logger.info("🔌 WS: subscribe() called for \(logPrefix), cursor: \(cursor ?? "nil")")

    // Stop existing subscription if any
    stop(key)

    let run = MLSCanonicalSubscriptionRun()
    subscriptionRuns[key] = run

    // Store handler and reset stop flag
    eventHandlers[key] = handler
    if let handlerProvider {
      eventHandlerProviders[key] = handlerProvider
    }
    logger.info("🔌 WS: Handler registered for \(key)")

    // Update state
    connectionState[key] = .connecting
    logger.info("🔌 WS: State set to .connecting for \(key)")

    // Determine effective cursor: provided > in-memory > persistent store
    let effectiveCursor = cursor ?? lastCursor[key]

    // Start subscription task as DETACHED to survive view lifecycle changes
    let task = Task.detached(priority: .utility) { [weak self] in
      guard let self = self else { return }
      // Try to load from persistent store if no cursor available
      var cursorToUse = effectiveCursor
      if cursorToUse == nil, let store = await self.cursorStore {
        cursorToUse = await self.loadPersistentCursor(for: key, store: store)
      }
      await self.runSubscription(convoId: convoId, key: key, cursor: cursorToUse, run: run)
    }

    activeSubscriptions[key] = task
  }

  /// Load cursor from persistent storage
  private func loadPersistentCursor(for convoId: String, store: MLSEventCursorStore) async
    -> String?
  {
    do {
      let cursor = try await MainActor.run {
        try store.getCursor(for: convoId)
      }
      if let cursor = cursor {
        logger.info("📍 Loaded persistent cursor for \(convoId): \(cursor.prefix(20))...")
      }
      return cursor
    } catch {
      logger.warning(
        "⚠️ Failed to load persistent cursor for \(convoId): \(error.localizedDescription)"
      )
      return nil
    }
  }

  /// Stop subscription for a specific conversation
  public func stop(_ convoId: String) {
    logger.info("Stopping WebSocket subscription for: \(convoId)")

    // Set the graceful shutdown flag FIRST so the loop can exit cleanly
    shouldStop[convoId] = true

    subscriptionRuns.removeValue(forKey: convoId)?.invalidate()
    activeSubscriptions[convoId]?.cancel()
    activeSubscriptions.removeValue(forKey: convoId)
    eventHandlers.removeValue(forKey: convoId)
    eventHandlerProviders.removeValue(forKey: convoId)
  }

  /// Stop all active subscriptions
  public func stopAll() {
    logger.info("Stopping all WebSocket subscriptions")

    for convoId in activeSubscriptions.keys {
      stop(convoId)
    }
  }

  /// Stop all subscriptions and wait for them to complete
  public func stopAllAndWait(timeout: TimeInterval = 2.0) async {
    logger.info("🛑 Stopping all WebSocket subscriptions and waiting for completion...")

    let tasksToWait = Array(activeSubscriptions.values)
    let convoIds = Array(activeSubscriptions.keys)

    // Set all stop flags first
    for convoId in convoIds {
      shouldStop[convoId] = true
    }

    // Cancel all tasks
    for convoId in convoIds {
      stop(convoId)
    }

    // Wait for all tasks with timeout
    if !tasksToWait.isEmpty {
      logger.info("   Waiting for \(tasksToWait.count) WebSocket task(s) to complete...")

      await withTaskGroup(of: Void.self) { group in
        group.addTask {
          for task in tasksToWait {
            _ = await task.result
          }
        }

        group.addTask {
          try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
        }

        _ = await group.next()
        group.cancelAll()
      }

      logger.info("✅ All WebSocket tasks stopped")
    }
  }

  /// Reconnect to a conversation (using last cursor)
  public func reconnect(_ convoId: String) {
    guard let handler = eventHandlers[convoId] else {
      logger.warning("No handler found for reconnection: \(convoId)")
      return
    }

    logger.info("Reconnecting WebSocket to conversation: \(convoId)")

    let cursor = lastCursor[convoId]
    subscribe(to: convoId, cursor: cursor, handler: handler)
  }

  // MARK: - Private Methods

  /// Shared startup gate used by public subscribe/reconnect. A pending
  /// terminal write is retried before the run can load inventory or mint a
  /// ticket. This internal seam lets Core tests exercise the manager-owned
  /// lifecycle without opening a real network stream.
  internal func prepareCanonicalSubscriptionForReconnect(
    _ coordinator: MLSCanonicalSubscriptionFailureCoordinator
  ) async throws -> MLSCanonicalSubscriptionFailureCoordinator {
    var restored =
      try await canonicalSubscriptionFailureLifecycle
      .restorePendingIfNeeded(coordinator)
    try await restored.load()
    return restored
  }

  /// Retain the coordinator when a run exits after a terminal write failure.
  /// The value remains scoped inside this manager until public reconnect can
  /// retry it against the current protected store.
  internal func retainCanonicalSubscriptionFailure(
    _ coordinator: MLSCanonicalSubscriptionFailureCoordinator
  ) async {
    await canonicalSubscriptionFailureLifecycle.remember(coordinator)
  }

  private func runSubscription(
    convoId: String?, key: String, cursor: String?, run: MLSCanonicalSubscriptionRun
  ) async {
    guard run.isValid else { return }
    logger.info("🔌 WS: runSubscription() started for \(key), cursor: \(cursor ?? "nil")")
    var reconnectAttempts = 0
    var latestSavedCursor = cursor
    var subscriptionFence: MLSCanonicalSubscriptionFence?
    var replayRequirement = MLSCanonicalSubscriptionReplayRequirement()
    let initialHandler = eventHandlers[key]
    var currentHandler = initialHandler
    var subscriptionInitialGeneration = MLSCoordinationStore.shared.currentGeneration
    var failureCoordinator: MLSCanonicalSubscriptionFailureCoordinator
    do {
      let scope = try await canonicalSubscriptionScope(for: key)
      let coordinator = try Self.makeCanonicalSubscriptionFailureCoordinator(
        scope: scope,
        handler: initialHandler,
        store: cursorStore
      )
      failureCoordinator = try await prepareCanonicalSubscriptionForReconnect(coordinator)
      try run.check()
    } catch {
      guard run.isValid else { return }
      logger.error("🔌 WS: Failed to load canonical subscription failure state for \(key): \(error)")
      connectionState[key] = .error(error)
      await initialHandler?.onError?(error)
      guard run.isValid else { return }
      connectionState[key] = .disconnected
      return
    }
    // Spec §7: Exponential backoff (1s, 2s, 4s, 8s, max 30s), no give-up limit
    let maxReconnectDelay: TimeInterval = 30.0

    while run.isValid, !Task.isCancelled, shouldStop[key] != true {
      latestSavedCursor = lastCursor[key] ?? latestSavedCursor

      do {
        logger.info("🔌 WS: Attempting connection for: \(key), attempt: \(reconnectAttempts + 1)")

        connectionState[key] = .connecting

        guard let handler = currentHandler ?? eventHandlers[key] else {
          throw MLSCanonicalInventoryActionMissingError.conversationState
        }
        let apiClient = self.apiClient
        // The canonical stream is ticketed by one completed inventory
        // snapshot. Keep this fence across event-triggered reconnects;
        // fetching a newer aggregate here would authorize past an
        // unhandled event.
        let previousFence = subscriptionFence
        let fence = try await MLSCanonicalSubscriptionCoordinator.prepare(
          fence: &subscriptionFence,
          initialCursor: latestSavedCursor,
          terminalFailure: failureCoordinator.terminalFailure,
          hasUnhandledEvent: replayRequirement.isRequired,
          fetchInventory: {
            try await apiClient.getCanonicalInventoryAggregateSnapshot(limit: 100)
          },
          reconcile: { snapshot in
            try run.check()
            try await Self.reconcileCanonicalInventory(snapshot, with: handler)
            try run.check()
          },
          installCompletion: { snapshot in
            try? run.whileValid { apiClient.recordCompletedCanonicalInventory(snapshot) }
          },
          persistFence: { cursor in
            try await self.saveCursor(cursor, for: key, run: run)
          }
        )
        try run.check()
        let resumeCursor = fence.snapshotEventCursor
        let ticket: BlueCatbirdChatGetSubscriptionTicket.Output
        do {
          ticket = try await apiClient.getCanonicalSubscriptionTicket(
            inventorySessionId: fence.inventorySessionId,
            eventCursor: resumeCursor
          )
        } catch {
          // Reuse the outer reconnect/backoff lifecycle. A rejected
          // capability requires full reconciliation on the next run;
          // an outstanding event or terminal latch forbids that move.
          try MLSCanonicalSubscriptionCoordinator.invalidateRejectedTicketFence(
            error,
            fence: &subscriptionFence,
            terminalFailure: failureCoordinator.terminalFailure,
            hasUnhandledEvent: replayRequirement.isRequired
          )
          throw error
        }
        let stream = try await apiClient.subscribeCanonicalEvents(
          ticket: ticket.ticket,
          cursor: resumeCursor
        )
        // The ticket always starts at the retained snapshot fence. On
        // reconnect, replay already-committed events from this fence
        // until the local unadvanced cursor is reached; never install
        // a newer inventory fence merely because an event failed.
        var replayGate = Self.canonicalReplayGate(
          previousFence: previousFence,
          currentFence: fence,
          savedCursor: latestSavedCursor
        )
        try run.check()
        connectionState[key] = .connected
        logger.info("🔌 WS: Connected for \(key) - entering event loop")

        // Trigger reconnected callback if this was a reconnection
        if reconnectAttempts > 0 {
          logger.info("✅ Reconnected successfully for: \(key) after \(reconnectAttempts) attempts")
          MLSDiagnostics.record(
            .streamResumed,
            code: "StreamResumed",
            conversation: key == "__global__" ? nil : key,
            attempt: reconnectAttempts,
            detail: ["attempts": String(reconnectAttempts)]
          )
          if let handler = eventHandlers[key], let reconnectedHandler = handler.onReconnected {
            await reconnectedHandler()
          }
        }
        try run.check()
        // Reset attempts on successful connection
        reconnectAttempts = 0

        // 4. Process messages
        var reconnectRequested = false
        var failurePersistenceUnavailable = false
        let loopOutcome = try await MLSCanonicalTransportAdapter.consumeCanonicalStream(
          stream,
          shouldStop: {
            let stopped = await self.shouldStop[key]
            return !run.isValid || stopped == true
          },
          handle: { message in
            guard run.isValid else { return .reconnect(CancellationError()) }
            switch replayGate.decide(message) {
            case .skip:
              // This envelope was already committed on the
              // retained fence. Do not dispatch it again or
              // write its cursor backward into the store.
              return .handled
            case .reconnect(let error):
              replayRequirement.recordFailure(message)
              await handler.onError?(error)
              return .reconnect(error)
            case .handle(let expectedPreviousCursor):
              let result = await self.handleCanonicalEvent(
                message,
                for: key,
                expectedPreviousCursor: expectedPreviousCursor,
                run: run
              )
              if case .handled = result {
                replayRequirement.recordHandled(message)
                latestSavedCursor = await self.lastCursor[key] ?? latestSavedCursor
              } else {
                replayRequirement.recordFailure(message)
              }
              return result
            }
          }
        )
        try run.check()
        switch loopOutcome {
        case .ended:
          break
        case .stopped:
          break
        case .reconnect(let error, _):
          do {
            _ = try await failureCoordinator.record(error)
            try run.check()
          } catch {
            guard run.isValid else { return }
            logger.error(
              "🔌 WS: Failed to persist canonical subscription failure for \(key): \(error)")
            await canonicalSubscriptionFailureLifecycle.remember(failureCoordinator)
            guard run.isValid else { return }
            await handler.onError?(error)
            failurePersistenceUnavailable = true
          }
          if !failurePersistenceUnavailable {
            reconnectRequested = true
          }
        }

        if failurePersistenceUnavailable {
          break
        }

        if shouldStop[key] == true {
          logger.info("🔌 WS: Exiting loop due to graceful shutdown for: \(key)")
          break
        }

        if reconnectRequested {
          reconnectAttempts += 1
          connectionState[key] = .reconnecting
          logger.info(
            "🔌 WS: Reconnecting immediately after canonical event failure for \(key) (attempt \(reconnectAttempts))"
          )
          continue
        }

        // Stream ended without error — reconnect with backoff
        if !Task.isCancelled, shouldStop[key] != true {
          reconnectAttempts += 1
          connectionState[key] = .reconnecting
          let delay = min(pow(2.0, Double(reconnectAttempts - 1)), maxReconnectDelay)
          logger.info(
            "🔌 WS: Stream ended for \(key), reconnecting in \(String(format: "%.0f", delay))s (attempt \(reconnectAttempts))"
          )
          try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
        }

      } catch {
        guard run.isValid else { return }
        if shouldStop[key] == true || Task.isCancelled {
          logger.info("🔌 WS: Exiting due to shutdown/cancellation for: \(key)")
          break
        }

        do {
          _ = try await failureCoordinator.record(error)
          try run.check()
        } catch {
          guard run.isValid else { return }
          logger.error(
            "🔌 WS: Failed to persist canonical subscription failure for \(key): \(error)")
          await canonicalSubscriptionFailureLifecycle.remember(failureCoordinator)
          guard run.isValid else { return }
          if let handler = eventHandlers[key] {
            await handler.onError?(error)
          }
          break
        }
        let isClosedContext = Self.isClosedContextError(error)
        let isGenMismatch = Self.isGenerationMismatchError(error)
        let currentCoordGen = MLSCoordinationStore.shared.currentGeneration
        let isStaleGeneration = isGenMismatch || (subscriptionInitialGeneration != currentCoordGen)

        if isClosedContext || isStaleGeneration {
          let diagCode = isClosedContext ? "ContextClosed" : "GenerationMismatch"
          logger.warning("🔌 WS: Detected \(diagCode) for \(key) — attempting to rebind to active account")
          MLSDiagnostics.record(
            .streamPaused,
            code: diagCode,
            conversation: key == "__global__" ? nil : key,
            attempt: reconnectAttempts + 1,
            detail: ["reason": diagCode, "attempt": String(reconnectAttempts + 1)]
          )

          // If the account switched away from this stream's client, stop gracefully instead of spinning
          if let activeDID = MLSCoordinationStore.shared.getState().activeUserDID,
             let clientDID = await apiClient.authenticatedUserDID(),
             !activeDID.isEmpty,
             clientDID.lowercased() != activeDID.lowercased() {
            logger.info("🔌 WS: Account switched away (\(clientDID) != \(activeDID)) — stopping stream for \(key)")
            shouldStop[key] = true
            break
          }

          // Try to rebind via handlerProvider or current eventHandlers
          if let provider = eventHandlerProviders[key], let freshHandler = await provider() {
            logger.info("🔌 WS: Rebound stream for \(key) to fresh active account handler")
            eventHandlers[key] = freshHandler
            currentHandler = freshHandler
            subscriptionFence = nil
            subscriptionInitialGeneration = currentCoordGen
            reconnectAttempts = 0
            MLSDiagnostics.record(
              .streamResumed,
              code: diagCode,
              conversation: key == "__global__" ? nil : key,
              attempt: 0,
              detail: ["reason": "rebound_active_handler"]
            )
            continue
          } else if let registeredHandler = eventHandlers[key] {
            logger.info("🔌 WS: Adopting registered handler for \(key) after context reset")
            currentHandler = registeredHandler
            subscriptionFence = nil
            subscriptionInitialGeneration = currentCoordGen
            reconnectAttempts = 0
            MLSDiagnostics.record(
              .streamResumed,
              code: diagCode,
              conversation: key == "__global__" ? nil : key,
              attempt: 0,
              detail: ["reason": "adopted_registered_handler"]
            )
            continue
          }
        }

        logger.error("🔌 WS: Connection error for \(key): \(error)")

        connectionState[key] = .error(error)

        // Notify error handler
        if let handler = eventHandlers[key], let errorHandler = handler.onError {
          await errorHandler(error)
        }

        if !Task.isCancelled, shouldStop[key] != true {
          reconnectAttempts += 1
          connectionState[key] = .reconnecting
          // Respect server Retry-After even when it exceeds normal backoff.
          let delay = MLSStreamReconnectPolicy.delay(attempt: reconnectAttempts, error: error)
          logger.info(
            "🔌 WS: Reconnecting in \(String(format: "%.0f", delay))s (attempt \(reconnectAttempts)) for: \(key)"
          )
          do {
            try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
          } catch {
            break
          }
        }
      }
    }

    guard run.isValid else { return }
    if shouldStop[key] == true {
      logger.info("🔌 WS: Subscription stopped gracefully for: \(key)")
    }
    connectionState[key] = .disconnected
  }

  /// Dispatch one canonical event through the shared availability handler.
  private func handleCanonicalEvent(
    _ message: BlueCatbirdChatSubscribeEvents.Message,
    for key: String,
    expectedPreviousCursor: String,
    run: MLSCanonicalSubscriptionRun
  ) async -> MLSCanonicalTransportAdapter.MLSCanonicalStreamHandlingResult {
    guard run.isValid, let handler = eventHandlers[key] else {
      logger.warning("🔌 WS: No handler found for canonical stream key \(key)")
      let error = MLSCanonicalInventoryActionMissingError.conversationState
      return .reconnect(error)
    }

    let apiClient = self.apiClient
    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      message,
      subscriptionKey: key,
      expectedPreviousCursor: expectedPreviousCursor,
      loadEntries: { conversationId, afterSeq in
        try await apiClient.getCanonicalEntries(
          conversationId: conversationId,
          afterSeq: afterSeq,
          limit: 100
        ).entries
      },
      onDurableEvent: { event in
        try run.check()
        try await Self.canonicalDurableEventActions(for: handler).dispatch(event)
        try run.check()
      },
      saveCursor: { cursor in
        try await self.saveCursor(cursor, for: key, run: run)
      }
    )
    guard run.isValid else { return .reconnect(CancellationError()) }
    if case .reconnect(let error) = result {
      await handler.onError?(error)
    }
    return result
  }

  internal static func reconcileCanonicalInventory(
    _ snapshot: MLSCanonicalInventorySnapshot,
    with handler: EventHandler
  ) async throws {
    try await handler.onCanonicalInventoryReconciliationStarted?()
    if let batch = handler.onCanonicalInventoryBatch {
      try await batch(snapshot)
      return
    }
    try await MLSCanonicalInventoryReconciler.reconcile(
      snapshot,
      actions: Self.canonicalInventoryActions(for: handler)
    )
  }

  internal static func canonicalInventoryActions(
    for handler: EventHandler
  ) -> MLSCanonicalInventoryActionSet {
    MLSCanonicalInventoryActionSet(
      onConversationState: handler.onCanonicalConversationInventoryState,
      onConversationRemoval: handler.onCanonicalConversationRemovalTombstone,
      onConversationClose: handler.onCanonicalConversationCloseTombstone,
      onPendingWelcome: handler.onCanonicalPendingWelcome,
      onLeafRecovery: handler.onCanonicalLeafRecovery,
      onRecoveryConversationState: handler.onCanonicalRecoveryConversationState
    )
  }

  /// Build the typed action table used by the stream loop. This is internal
  /// so Core tests and future Core consumers can prove that every generated
  /// arm has a reachable action; it deliberately does not fabricate legacy
  /// DTOs for canonical events whose shapes do not match them.
  internal static func canonicalDurableEventActions(
    for handler: EventHandler
  ) -> MLSCanonicalTransportAdapter.MLSCanonicalDurableEventActions {
    return handler.onCanonicalDurableEventActions
      ?? MLSCanonicalTransportAdapter.MLSCanonicalDurableEventActions()
  }

  /// Build the replay gate from the fence that was active before this
  /// attempt. A newly fetched and persisted snapshot is a new audience: a
  /// stale input cursor from another/older stream must not become a replay
  /// target. Only an unchanged same-fence reconnect may skip its committed
  /// prefix using the local cursor.
  internal static func canonicalReplayGate(
    previousFence: MLSCanonicalSubscriptionFence?,
    currentFence: MLSCanonicalSubscriptionFence,
    savedCursor: String?
  ) -> MLSCanonicalTransportAdapter.MLSCanonicalReplayGate {
    let replayCursor =
      previousFence == currentFence
      ? savedCursor
      : currentFence.snapshotEventCursor
    return MLSCanonicalTransportAdapter.MLSCanonicalReplayGate(
      snapshotCursor: currentFence.snapshotEventCursor,
      savedCursor: replayCursor
    )
  }

  /// Construct the lifecycle coordinator used by every subscription run.
  /// Keeping this factory on the manager makes manager recreation and app
  /// restart use the same scoped durable-state path as reconnect.
  internal static func makeCanonicalSubscriptionFailureCoordinator(
    scope: MLSCanonicalSubscriptionScope?,
    handler: EventHandler?,
    store: MLSEventCursorStore?
  ) throws -> MLSCanonicalSubscriptionFailureCoordinator {
    guard let actions = handler?.onCanonicalDurableEventActions else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.incompleteActionTable
    }
    return try MLSCanonicalSubscriptionFailureCoordinator(
      scope: scope,
      capability: actions.capabilityIdentity,
      store: store
    )
  }

  private func canonicalSubscriptionScope(for key: String) async throws
    -> MLSCanonicalSubscriptionScope
  {
    guard let account = await apiClient.authenticatedUserDID() else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.missingScope
    }
    let normalizedAccount = account.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !normalizedAccount.isEmpty else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.missingScope
    }
    let environment = apiClient.mlsServiceDID.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !environment.isEmpty else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.missingScope
    }

    let device: String?
    if let configured = canonicalSubscriptionDeviceIdentifier {
      device = configured
    } else {
      #if os(iOS) || os(macOS)
        if #available(iOS 18.0, macOS 13.0, *) {
          device = MLSDeviceManager.currentDeviceScopeIdentifier()
        } else {
          device = nil
        }
      #else
        device = nil
      #endif
    }
    guard let device, !device.isEmpty else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.missingScope
    }
    let normalizedKey = key.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !normalizedKey.isEmpty else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.missingScope
    }

    return MLSCanonicalSubscriptionScope(
      accountIdentifier: normalizedAccount,
      environmentIdentifier: environment,
      deviceIdentifier: device,
      subscriptionIdentifier: normalizedKey
    )
  }

  /// Save cursor to both in-memory cache and persistent storage
  private func saveCursor(_ cursor: String, for convoId: String, run: MLSCanonicalSubscriptionRun)
    async throws
  {
    try run.check()
    try await MLSCanonicalTransportAdapter.persistCanonicalCursor(
      cursor,
      for: convoId,
      store: cursorStore,
      run: run
    )
    try run.check()
    lastCursor[convoId] = cursor
  }
  internal static func isClosedContextError(_ error: Error) -> Bool {
    let message: String
    if let bridge = error as? OrchestratorBridgeError {
      switch bridge {
      case .Mls(let msg), .Storage(let msg), .Api(let msg):
        message = msg
      default:
        message = error.localizedDescription
      }
    } else {
      message = error.localizedDescription
    }
    let lower = message.lowercased()
    return lower.contains("context closed")
      || lower.contains("connections have been released")
      || lower.contains("database connections have been released")
      || lower.contains("database closed")
      || lower.contains("database is closed")
      || lower.contains("storage is closed")
  }
  internal static func isGenerationMismatchError(_ error: Error) -> Bool {
    if error is MLSCoordinationAwareTask.GenerationStaleError { return true }
    let lower = error.localizedDescription.lowercased()
    return lower.contains("generation mismatch")
      || lower.contains("coordination generation mismatch")
      || lower.contains("generationstale")
  }
}
