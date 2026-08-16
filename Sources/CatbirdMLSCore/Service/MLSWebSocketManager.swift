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
        // Spec §7: Exponential backoff (1s, 2s, 4s, 8s, max 30s), no give-up limit
        let maxReconnectDelay: TimeInterval = 30.0

        while !Task.isCancelled, shouldStop[key] != true {
            latestSavedCursor = lastCursor[key] ?? latestSavedCursor

            do {
                logger.info("🔌 WS: Attempting connection for: \(key), attempt: \(reconnectAttempts + 1)")

                connectionState[key] = .connecting

                // The canonical stream is ticketed by one completed inventory
                // snapshot. Never reuse the legacy ticket endpoint or proxy the
                // WebSocket upgrade through a second DPoP flow.
                let inventory = try await apiClient.getCanonicalInventorySnapshot(limit: 100)
                let sessionCursor = inventory.snapshotEventCursor
                let resumeCursor = MLSCanonicalTransportAdapter.reconciledResumeCursor(
                    savedCursor: latestSavedCursor,
                    inventorySnapshotCursor: sessionCursor
                )
                if let latestSavedCursor, latestSavedCursor != resumeCursor {
                    logger.info(
                        "🔌 WS: Reconciled persisted cursor \(latestSavedCursor.prefix(20)) to inventory snapshot fence \(resumeCursor.prefix(20))"
                    )
                } else {
                    logger.info(
                        "🔌 WS: Using inventory snapshot fence \(resumeCursor.prefix(20))"
                    )
                }
                let ticket = try await apiClient.getCanonicalSubscriptionTicket(
                    inventorySessionId: inventory.inventorySessionId,
                    eventCursor: resumeCursor
                )
                let stream = try await apiClient.subscribeCanonicalEvents(
                    ticket: ticket.ticket,
                    cursor: resumeCursor
                )
                if latestSavedCursor != resumeCursor {
                    saveCursor(resumeCursor, for: key)
                }
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
                var eventCount = 0

                for try await message in stream {
                    if Task.isCancelled || shouldStop[key] == true {
                        break
                    }
                    eventCount += 1
                    await handleCanonicalEvent(message, for: key)
                }


                if shouldStop[key] == true {
                    logger.info("🔌 WS: Exiting loop due to graceful shutdown for: \(key)")
                    break
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
        for key: String
    ) async {
        guard let handler = eventHandlers[key] else {
            logger.warning("🔌 WS: No handler found for canonical stream key \(key)")
            return
        }

        let apiClient = self.apiClient
        await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
            message,
            subscriptionKey: key,
            loadEntries: { conversationId, afterSeq in
                try await apiClient.getCanonicalEntries(
                    conversationId: conversationId,
                    afterSeq: afterSeq,
                    limit: 100
                ).entries
            },
            onMessage: { event in
                await handler.onMessage?(event)
            },
            onError: { error in
                await handler.onError?(error)
            },
            saveCursor: { cursor in
                self.saveCursor(cursor, for: key)
            }
        )
    }



    /// Save cursor to both in-memory cache and persistent storage
    private func saveCursor(_ cursor: String, for convoId: String) {
        lastCursor[convoId] = cursor

        if let store = cursorStore {
            Task {
                do {
                    try await MainActor.run {
                        try store.updateCursor(for: convoId, cursor: cursor)
                    }
                } catch {
                    logger.warning("⚠️ Failed to persist cursor for \(convoId): \(error.localizedDescription)")
                }
            }
        }
    }
}
