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
    private var eventHandlers: [String: EventHandler] = [:]

    private var connectionState: [String: ConnectionState] = [:]
    private var lastCursor: [String: String] = [:]

    /// Flags to signal graceful shutdown (not cancellation)
    private var shouldStop: [String: Bool] = [:]

    /// Optional persistent cursor storage (survives app restart)
    private var cursorStore: MLSEventCursorStore?

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
            case let .errorFrame(message):
                return "Server error: \(message)"
            case let .decodingFailed(details):
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
        public var onCanonicalConversationInventoryState:
            ((BlueCatbirdChatDefs.ConversationState) async throws -> Void)?
        public var onCanonicalConversationRemovalTombstone:
            ((BlueCatbirdChatDefs.ConversationRemovalTombstone) async throws -> Void)?
        public var onCanonicalConversationCloseTombstone:
            ((BlueCatbirdChatDefs.ConversationCloseTombstone) async throws -> Void)?
        public var onCanonicalPendingWelcome:
            ((BlueCatbirdChatDefs.WelcomeView) async throws -> Void)?
        public var onCanonicalLeafRecovery:
            ((BlueCatbirdChatDefs.LeafRecoveryInboxItem) async throws -> Void)?
        /// Required typed actions for every canonical durable arm. The action
        /// table is optional as a whole so a missing consumer fails closed at
        /// dispatch time; individual missing arms also throw rather than
        /// becoming successful no-ops.
        public var onCanonicalDurableEventActions:
            MLSCanonicalTransportAdapter.MLSCanonicalDurableEventActions?
        public var onMessage: ((BlueCatbirdMlsChatSubscribeEvents.MessageEvent) async -> Void)?
        public var onReaction: ((BlueCatbirdMlsChatSubscribeEvents.ReactionEvent) async -> Void)?
        public var onTyping: ((BlueCatbirdMlsChatSubscribeEvents.TypingEvent) async -> Void)?
        public var onInfo: ((BlueCatbirdMlsChatSubscribeEvents.InfoEvent) async -> Void)?
        public var onNewDevice: ((BlueCatbirdMlsChatSubscribeEvents.NewDeviceEvent) async -> Void)?
        public var onGroupInfoRefreshRequested:
            ((BlueCatbirdMlsChatSubscribeEvents.GroupInfoRefreshRequestedEvent) async -> Void)?
        public var onReadditionRequested:
            ((BlueCatbirdMlsChatSubscribeEvents.ReadditionRequestedEvent) async -> Void)?
        public var onWelcomeReissueRequested:
            ((BlueCatbirdMlsChatSubscribeEvents.WelcomeReissueRequestedEvent) async -> Void)?
        public var onMembershipChanged: ((String, DID, MembershipAction) async -> Void)?
        public var onKickedFromConversation: ((String, DID, String?) async -> Void)?
        public var onConversationNeedsRecovery: ((String, RecoveryReason) async -> Void)?
        public var onTreeChanged: ((BlueCatbirdMlsChatSubscribeEvents.TreeChanged) async -> Void)?
        public var onGroupReset: ((BlueCatbirdMlsChatSubscribeEvents.GroupResetEvent) async -> Void)?
        /// Phase 2.5 indirect-trigger reset request from the DS — server has NOT
        /// minted a new group id and is asking subscribed clients to elect a
        /// first responder via `bootstrapResetGroup` / `commitGroupChange`.
        /// Mirrors `onGroupReset` shape; see
        /// `docs/plans/phase-2-5-indirect-funneling.md` §3.
        public var onResetRequested:
            ((BlueCatbirdMlsChatSubscribeEvents.ResetRequestedEvent) async -> Void)?
        public var onError: ((Error) async -> Void)?
        public var onReconnected: (() async -> Void)?

        public init() {}

        public init(
            onCanonicalConversationInventoryState: ((BlueCatbirdChatDefs.ConversationState) async throws -> Void)? = nil,
            onCanonicalConversationRemovalTombstone: ((BlueCatbirdChatDefs.ConversationRemovalTombstone) async throws -> Void)? = nil,
            onCanonicalConversationCloseTombstone: ((BlueCatbirdChatDefs.ConversationCloseTombstone) async throws -> Void)? = nil,
            onCanonicalPendingWelcome: ((BlueCatbirdChatDefs.WelcomeView) async throws -> Void)? = nil,
            onCanonicalLeafRecovery: ((BlueCatbirdChatDefs.LeafRecoveryInboxItem) async throws -> Void)? = nil,
            onCanonicalDurableEventActions: MLSCanonicalTransportAdapter.MLSCanonicalDurableEventActions? = nil,
            onMessage: ((BlueCatbirdMlsChatSubscribeEvents.MessageEvent) async -> Void)? = nil,
            onReaction: ((BlueCatbirdMlsChatSubscribeEvents.ReactionEvent) async -> Void)? = nil,
            onTyping: ((BlueCatbirdMlsChatSubscribeEvents.TypingEvent) async -> Void)? = nil,
            onInfo: ((BlueCatbirdMlsChatSubscribeEvents.InfoEvent) async -> Void)? = nil,
            onNewDevice: ((BlueCatbirdMlsChatSubscribeEvents.NewDeviceEvent) async -> Void)? = nil,
            onGroupInfoRefreshRequested: (
                (BlueCatbirdMlsChatSubscribeEvents.GroupInfoRefreshRequestedEvent) async -> Void
            )? = nil,
            onReadditionRequested: (
                (BlueCatbirdMlsChatSubscribeEvents.ReadditionRequestedEvent) async -> Void
            )? = nil,
            onWelcomeReissueRequested: (
                (BlueCatbirdMlsChatSubscribeEvents.WelcomeReissueRequestedEvent) async -> Void
            )? = nil,
            onMembershipChanged: ((String, DID, MembershipAction) async -> Void)? = nil,
            onKickedFromConversation: ((String, DID, String?) async -> Void)? = nil,
            onConversationNeedsRecovery: ((String, RecoveryReason) async -> Void)? = nil,
            onTreeChanged: ((BlueCatbirdMlsChatSubscribeEvents.TreeChanged) async -> Void)? = nil,
            onGroupReset: ((BlueCatbirdMlsChatSubscribeEvents.GroupResetEvent) async -> Void)? = nil,
            onResetRequested: (
                (BlueCatbirdMlsChatSubscribeEvents.ResetRequestedEvent) async -> Void
            )? = nil,
            onError: ((Error) async -> Void)? = nil,
            onReconnected: (() async -> Void)? = nil
        ) {
            self.onCanonicalConversationInventoryState = onCanonicalConversationInventoryState
            self.onCanonicalConversationRemovalTombstone = onCanonicalConversationRemovalTombstone
            self.onCanonicalConversationCloseTombstone = onCanonicalConversationCloseTombstone
            self.onCanonicalPendingWelcome = onCanonicalPendingWelcome
            self.onCanonicalLeafRecovery = onCanonicalLeafRecovery
            self.onCanonicalDurableEventActions = onCanonicalDurableEventActions
            self.onMessage = onMessage
            self.onReaction = onReaction
            self.onTyping = onTyping
            self.onInfo = onInfo
            self.onNewDevice = onNewDevice
            self.onGroupInfoRefreshRequested = onGroupInfoRefreshRequested
            self.onReadditionRequested = onReadditionRequested
            self.onWelcomeReissueRequested = onWelcomeReissueRequested
            self.onMembershipChanged = onMembershipChanged
            self.onKickedFromConversation = onKickedFromConversation
            self.onConversationNeedsRecovery = onConversationNeedsRecovery
            self.onTreeChanged = onTreeChanged
            self.onGroupReset = onGroupReset
            self.onResetRequested = onResetRequested
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

    // MARK: - Public Methods

    /// Subscribe to real-time events for a conversation via WebSocket
    /// - Parameters:
    ///   - convoId: Conversation ID to subscribe to. If nil, subscribes to ALL conversations (global stream).
    ///   - cursor: Optional cursor to resume from (for reconnection)
    ///   - handler: Event handler for different event types
    public func subscribe(
        to convoId: String?,
        cursor: String? = nil,
        handler: EventHandler
    ) {
        let key = convoId ?? "__global__"
        let logPrefix = convoId != nil ? "convoId: \(convoId!)" : "GLOBAL"

        logger.info("🔌 WS: subscribe() called for \(logPrefix), cursor: \(cursor ?? "nil")")

        // Stop existing subscription if any
        stop(key)

        // Store handler and reset stop flag
        eventHandlers[key] = handler
        shouldStop[key] = false
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
            await self.runSubscription(convoId: convoId, key: key, cursor: cursorToUse)
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

        activeSubscriptions[convoId]?.cancel()
        activeSubscriptions.removeValue(forKey: convoId)
        eventHandlers.removeValue(forKey: convoId)
        connectionState[convoId] = .disconnected
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

    private func runSubscription(convoId: String?, key: String, cursor: String?) async {
        logger.info("🔌 WS: runSubscription() started for \(key), cursor: \(cursor ?? "nil")")
        var reconnectAttempts = 0
        var latestSavedCursor = cursor
        var subscriptionFence: MLSCanonicalSubscriptionFence?
        // This latch belongs to the whole subscription attempt, not one
        // transport connection. Transport reconnects retain it; a caller
        // installing a replacement action table starts a new subscribe call.
        var subscriptionFailureLatch = MLSCanonicalSubscriptionFailureLatch()
        // Spec §7: Exponential backoff (1s, 2s, 4s, 8s, max 30s), no give-up limit
        let maxReconnectDelay: TimeInterval = 30.0

        while !Task.isCancelled, shouldStop[key] != true {
            latestSavedCursor = lastCursor[key] ?? latestSavedCursor

            do {
                logger.info("🔌 WS: Attempting connection for: \(key), attempt: \(reconnectAttempts + 1)")

                connectionState[key] = .connecting

                guard let handler = eventHandlers[key] else {
                    throw MLSCanonicalInventoryActionMissingError.conversationState
                }
                let apiClient = self.apiClient
                // The canonical stream is ticketed by one completed inventory
                // snapshot. Keep this fence across event-triggered reconnects;
                // fetching a newer aggregate here would authorize past an
                // unhandled event.
                let fence = try await MLSCanonicalSubscriptionCoordinator.prepare(
                    fence: &subscriptionFence,
                    initialCursor: latestSavedCursor,
                    terminalFailure: subscriptionFailureLatch.terminalFailure,
                    fetchInventory: {
                        try await apiClient.getCanonicalInventoryAggregateSnapshot(limit: 100)
                    },
                    reconcile: { snapshot in
                        try await self.reconcileCanonicalInventory(snapshot, with: handler)
                    },
                    installCompletion: { snapshot in
                        apiClient.recordCompletedCanonicalInventory(snapshot)
                    },
                    persistFence: { cursor in
                        try await self.saveCursor(cursor, for: key)
                    }
                )
                let resumeCursor = fence.snapshotEventCursor
                let ticket = try await apiClient.getCanonicalSubscriptionTicket(
                    inventorySessionId: fence.inventorySessionId,
                    eventCursor: resumeCursor
                )
                let stream = try await apiClient.subscribeCanonicalEvents(
                    ticket: ticket.ticket,
                    cursor: resumeCursor
                )
                // The ticket always starts at the retained snapshot fence. On
                // reconnect, replay already-committed events from this fence
                // until the local unadvanced cursor is reached; never install
                // a newer inventory fence merely because an event failed.
                var replayGate = MLSCanonicalTransportAdapter.MLSCanonicalReplayGate(
                    snapshotCursor: resumeCursor,
                    savedCursor: latestSavedCursor
                )
                connectionState[key] = .connected
                logger.info("🔌 WS: Connected for \(key) - entering event loop")

                // Trigger reconnected callback if this was a reconnection
                if reconnectAttempts > 0 {
                    logger.info("✅ Reconnected successfully for: \(key) after \(reconnectAttempts) attempts")
                    if let handler = eventHandlers[key], let reconnectedHandler = handler.onReconnected {
                        await reconnectedHandler()
                    }
                }

                // Reset attempts on successful connection
                reconnectAttempts = 0

                // 4. Process messages
                var reconnectRequested = false
                let loopOutcome = try await MLSCanonicalTransportAdapter.consumeCanonicalStream(
                    stream,
                    shouldStop: { await self.shouldStop[key] == true },
                    handle: { message in
                        switch replayGate.decide(message) {
                        case .skip:
                            // This envelope was already committed on the
                            // retained fence. Do not dispatch it again or
                            // write its cursor backward into the store.
                            return .handled
                        case let .reconnect(error):
                            _ = subscriptionFailureLatch.record(error)
                            await handler.onError?(error)
                            return .reconnect(error)
                        case let .handle(expectedPreviousCursor):
                            let result = await self.handleCanonicalEvent(
                                message,
                                for: key,
                                expectedPreviousCursor: expectedPreviousCursor
                            )
                            if case .handled = result {
                                latestSavedCursor = await self.lastCursor[key] ?? latestSavedCursor
                            } else if case let .reconnect(error) = result {
                                _ = subscriptionFailureLatch.record(error)
                            }
                            return result
                        }
                    }
                )
                switch loopOutcome {
                case .ended:
                    break
                case .stopped:
                    break
                case .reconnect:
                    reconnectRequested = true
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
                    logger.info("🔌 WS: Stream ended for \(key), reconnecting in \(String(format: "%.0f", delay))s (attempt \(reconnectAttempts))")
                    try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                }

            } catch {
                if shouldStop[key] == true || Task.isCancelled {
                    logger.info("🔌 WS: Exiting due to shutdown/cancellation for: \(key)")
                    break
                }

                _ = subscriptionFailureLatch.record(error)

                logger.error("🔌 WS: Connection error for \(key): \(error)")

                connectionState[key] = .error(error)

                // Notify error handler
                if let handler = eventHandlers[key], let errorHandler = handler.onError {
                    await errorHandler(error)
                }

                if !Task.isCancelled, shouldStop[key] != true {
                    reconnectAttempts += 1
                    connectionState[key] = .reconnecting
                    // Spec §7: Exponential backoff 1s, 2s, 4s, 8s, ... capped at 30s
                    let delay = min(pow(2.0, Double(reconnectAttempts - 1)), maxReconnectDelay)
                    logger.info(
                        "🔌 WS: Reconnecting in \(String(format: "%.0f", delay))s (attempt \(reconnectAttempts)) for: \(key)"
                    )
                    try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                }
            }
        }

        if shouldStop[key] == true {
            logger.info("🔌 WS: Subscription stopped gracefully for: \(key)")
        }
        connectionState[key] = .disconnected
    }

    /// Dispatch one canonical event through the shared availability handler.
    private func handleCanonicalEvent(
        _ message: BlueCatbirdChatSubscribeEvents.Message,
        for key: String,
        expectedPreviousCursor: String
    ) async -> MLSCanonicalTransportAdapter.MLSCanonicalStreamHandlingResult {
        guard let handler = eventHandlers[key] else {
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
                try await Self.canonicalDurableEventActions(for: handler).dispatch(event)
            },
            saveCursor: { cursor in
                try await self.saveCursor(cursor, for: key)
            }
        )
        if case let .reconnect(error) = result {
            await handler.onError?(error)
        }
        return result
    }

    private func reconcileCanonicalInventory(
        _ snapshot: MLSCanonicalInventorySnapshot,
        with handler: EventHandler
    ) async throws {
        try await MLSCanonicalInventoryReconciler.reconcile(
            snapshot,
            actions: MLSCanonicalInventoryActionSet(
                onConversationState: handler.onCanonicalConversationInventoryState,
                onConversationRemoval: handler.onCanonicalConversationRemovalTombstone,
                onConversationClose: handler.onCanonicalConversationCloseTombstone,
                onPendingWelcome: handler.onCanonicalPendingWelcome,
                onLeafRecovery: handler.onCanonicalLeafRecovery
            )
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



    /// Save cursor to both in-memory cache and persistent storage
    private func saveCursor(_ cursor: String, for convoId: String) async throws {
        try await MLSCanonicalTransportAdapter.persistCanonicalCursor(
            cursor,
            for: convoId,
            store: cursorStore
        )
        lastCursor[convoId] = cursor
    }
}
