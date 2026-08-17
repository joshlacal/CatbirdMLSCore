import Combine
import Foundation
import OSLog
import Petrel
import PetrelCatbird

/// Manages SSE (Server-Sent Events) subscriptions for MLS conversations
/// Provides real-time message delivery and reactions
/// Actor isolation keeps long-running stream work off the main thread while
/// preserving thread-safe access to subscription state.
public actor MLSEventStreamManager {
    private let logger = Logger(subsystem: "blue.catbird", category: "MLSEventStream")

    // MARK: - Properties

    private let apiClient: MLSAPIClient
    private var activeSubscriptions: [String: Task<Void, Never>] = [:]
    private var eventHandlers: [String: EventHandler] = [:]

    private var connectionState: [String: ConnectionState] = [:]
    private var lastCursor: [String: String] = [:]

    /// Flags to signal graceful shutdown (not cancellation)
    /// This allows the SSE loop to exit cleanly without CancellationError
    private var shouldStop: [String: Bool] = [:]

    /// Optional persistent cursor storage (survives app restart)
    private var cursorStore: MLSEventCursorStore?

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
        public var onGroupInfoRefreshRequested: ((BlueCatbirdMlsChatSubscribeEvents.GroupInfoRefreshRequestedEvent) async -> Void)?
        public var onReadditionRequested:
            ((BlueCatbirdMlsChatSubscribeEvents.ReadditionRequestedEvent) async -> Void)?
        public var onWelcomeReissueRequested:
            ((BlueCatbirdMlsChatSubscribeEvents.WelcomeReissueRequestedEvent) async -> Void)?
        public var onMembershipChanged: ((String, DID, MembershipAction) async -> Void)?
        public var onKickedFromConversation: ((String, DID, String?) async -> Void)?
        public var onConversationNeedsRecovery: ((String, RecoveryReason) async -> Void)?
        public var onTreeChanged: ((BlueCatbirdMlsChatSubscribeEvents.TreeChanged) async -> Void)?
        public var onGroupReset: ((BlueCatbirdMlsChatSubscribeEvents.GroupResetEvent) async -> Void)?
        /// Phase 2.5 indirect-trigger reset request from the DS — see
        /// `MLSWebSocketManager.EventHandler.onResetRequested` for shape.
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
            onGroupInfoRefreshRequested: ((BlueCatbirdMlsChatSubscribeEvents.GroupInfoRefreshRequestedEvent) async -> Void)? = nil,
            onReadditionRequested: ((BlueCatbirdMlsChatSubscribeEvents.ReadditionRequestedEvent) async -> Void)? = nil,
            onWelcomeReissueRequested: ((BlueCatbirdMlsChatSubscribeEvents.WelcomeReissueRequestedEvent) async -> Void)? = nil,
            onMembershipChanged: ((String, DID, MembershipAction) async -> Void)? = nil,
            onKickedFromConversation: ((String, DID, String?) async -> Void)? = nil,
            onConversationNeedsRecovery: ((String, RecoveryReason) async -> Void)? = nil,
            onTreeChanged: ((BlueCatbirdMlsChatSubscribeEvents.TreeChanged) async -> Void)? = nil,
            onGroupReset: ((BlueCatbirdMlsChatSubscribeEvents.GroupResetEvent) async -> Void)? = nil,
            onResetRequested: ((BlueCatbirdMlsChatSubscribeEvents.ResetRequestedEvent) async -> Void)? = nil,
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
    /// - Parameter store: The CursorStore instance to use for persistence
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

    /// Subscribe to real-time events for a conversation
    /// - Parameters:
    ///   - convoId: Conversation ID to subscribe to
    ///   - cursor: Optional cursor to resume from (for reconnection)
    ///   - handler: Event handler for different event types
    public func subscribe(
        to convoId: String,
        cursor: String? = nil,
        handler: EventHandler
    ) {
        print("[SSE] subscribe() called for convoId: \(convoId.prefix(12))...")
        logger.info("📡 SSE: subscribe() called for convoId: \(convoId), cursor: \(cursor ?? "nil")")

        // Stop existing subscription if any
        stop(convoId)

        // Store handler and reset stop flag
        eventHandlers[convoId] = handler
        shouldStop[convoId] = false
        logger.info("📡 SSE: Handler registered for convoId: \(convoId)")

        // Update state
        connectionState[convoId] = .connecting
        logger.info("📡 SSE: State set to .connecting for convoId: \(convoId)")

        // Determine effective cursor: provided > in-memory > persistent store
        let effectiveCursor = cursor ?? lastCursor[convoId]

        // Start subscription task as DETACHED to survive view lifecycle changes
        // The task checks shouldStop[convoId] flag for graceful shutdown
        // This prevents CancellationError from propagating to the SSE stream
        let task = Task.detached(priority: .utility) { [weak self] in
            guard let self = self else { return }
            // Try to load from persistent store if no cursor available
            var cursorToUse = effectiveCursor
            if cursorToUse == nil, let store = await self.cursorStore {
                cursorToUse = await self.loadPersistentCursor(for: convoId, store: store)
            }
            await self.runSubscription(convoId: convoId, cursor: cursorToUse)
        }

        activeSubscriptions[convoId] = task
    }

    /// Load cursor from persistent storage
    private func loadPersistentCursor(for convoId: String, store: MLSEventCursorStore) async -> String? {
        do {
            let cursor = try await MainActor.run {
                try store.getCursor(for: convoId)
            }
            if let cursor = cursor {
                logger.info("📍 Loaded persistent cursor for \(convoId): \(cursor.prefix(20))...")
            }
            return cursor
        } catch {
            logger.warning("⚠️ Failed to load persistent cursor for \(convoId): \(error.localizedDescription)")
            return nil
        }
    }

    /// Stop subscription for a specific conversation
    /// - Parameter convoId: Conversation ID
    public func stop(_ convoId: String) {
        logger.info("Stopping subscription for: \(convoId)")

        // Set the graceful shutdown flag FIRST so the loop can exit cleanly
        shouldStop[convoId] = true

        activeSubscriptions[convoId]?.cancel()
        activeSubscriptions.removeValue(forKey: convoId)
        eventHandlers.removeValue(forKey: convoId)
        connectionState[convoId] = .disconnected
    }

    /// Stop all active subscriptions (synchronous - for quick cancellation)
    public func stopAll() {
        logger.info("Stopping all subscriptions")

        for convoId in activeSubscriptions.keys {
            stop(convoId)
        }
    }

    /// Stop all subscriptions and wait for them to complete
    /// CRITICAL: Call this during account switching to ensure all SSE tasks have
    /// finished writing to the database before closing it
    /// - Parameter timeout: Maximum time to wait for tasks to complete (default 2 seconds)
    public func stopAllAndWait(timeout: TimeInterval = 2.0) async {
        logger.info("🛑 Stopping all subscriptions and waiting for completion...")

        // Capture tasks before stopping (stop() removes them from the dictionary)
        let tasksToWait = Array(activeSubscriptions.values)
        let convoIds = Array(activeSubscriptions.keys)

        // Set all stop flags first to signal graceful shutdown
        for convoId in convoIds {
            shouldStop[convoId] = true
        }

        // Cancel all tasks
        for convoId in convoIds {
            stop(convoId)
        }

        // Wait for all tasks to complete with timeout
        if !tasksToWait.isEmpty {
            logger.info("   Waiting for \(tasksToWait.count) SSE task(s) to complete...")

            await withTaskGroup(of: Void.self) { group in
                // Add task to wait for all SSE tasks
                group.addTask {
                    for task in tasksToWait {
                        // Wait for each task to complete (they're already cancelled)
                        _ = await task.result
                    }
                }

                // Add timeout task
                group.addTask {
                    try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                }

                // Wait for whichever finishes first
                _ = await group.next()
                group.cancelAll()
            }

            logger.info("✅ All SSE tasks stopped")
        } else {
            logger.info("✅ No active SSE tasks to wait for")
        }
    }

    /// Reconnect to a conversation (using last cursor)
    /// - Parameter convoId: Conversation ID
    public func reconnect(_ convoId: String) {
        guard let handler = eventHandlers[convoId] else {
            logger.warning("No handler found for reconnection: \(convoId)")
            return
        }

        logger.info("Reconnecting to conversation: \(convoId)")

        let cursor = lastCursor[convoId]
        subscribe(to: convoId, cursor: cursor, handler: handler)
    }

    // MARK: - Private Methods

    private func runSubscription(convoId: String, cursor: String?) async {
        print("[SSE] runSubscription() started for convoId: \(convoId.prefix(12))...")
        logger.info("📡 SSE: runSubscription() started for convoId: \(convoId), cursor: \(cursor ?? "nil")")
        var reconnectAttempts = 0
        var latestSavedCursor = cursor
        var subscriptionFence: MLSCanonicalSubscriptionFence?
        let initialHandler = eventHandlers[convoId]
        var failureCoordinator = Self.makeCanonicalSubscriptionFailureCoordinator(
            scope: await canonicalSubscriptionScope(for: convoId),
            handler: initialHandler,
            store: cursorStore
        )
        do {
            try await failureCoordinator.load()
        } catch {
            logger.error("📡 SSE: Failed to load canonical subscription failure state for \(convoId): \(error)")
            connectionState[convoId] = .error(error)
            await initialHandler?.onError?(error)
            connectionState[convoId] = .disconnected
            return
        }
        let maxReconnectAttempts = 5
        let reconnectDelay: TimeInterval = 2.0

        // Check both Task.isCancelled and shouldStop flag for graceful shutdown
        while !Task.isCancelled, shouldStop[convoId] != true, reconnectAttempts < maxReconnectAttempts {
            latestSavedCursor = lastCursor[convoId] ?? latestSavedCursor
            let connectionStartTime = Date()

            do {
                // Connect to SSE event stream
                print("[SSE] Attempting connection for: \(convoId.prefix(12))..., attempt: \(reconnectAttempts + 1)")
                logger.info("📡 SSE: Attempting connection for: \(convoId), attempt: \(reconnectAttempts + 1)")

                connectionState[convoId] = .connecting

                guard let handler = eventHandlers[convoId] else {
                    throw MLSCanonicalInventoryActionMissingError.conversationState
                }
                let apiClient = self.apiClient
                // Canonical subscriptions require one completed inventory
                // session and an exact snapshot cursor in both ticket and
                // upgrade. Keep this fence across event-triggered reconnects;
                // a newer aggregate would skip an unhandled event.
                let fence = try await MLSCanonicalSubscriptionCoordinator.prepare(
                    fence: &subscriptionFence,
                    initialCursor: latestSavedCursor,
                    terminalFailure: failureCoordinator.terminalFailure,
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
                        try await self.saveCursor(cursor, for: convoId)
                    }
                )
                let resumeCursor = fence.snapshotEventCursor
                let ticket = try await apiClient.getCanonicalSubscriptionTicket(
                    inventorySessionId: fence.inventorySessionId,
                    eventCursor: resumeCursor
                )
                let eventStream = try await apiClient.subscribeCanonicalEvents(
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

                connectionState[convoId] = .connected
                print("[SSE] Connected to: \(convoId.prefix(12))... - entering event loop")
                logger.info("📡 SSE: State set to .connected for convoId: \(convoId) - entering event loop")

                // If this is a successful reconnection (not initial connection), trigger catchup
                if reconnectAttempts > 0 {
                    logger.info("✅ Reconnected successfully for: \(convoId) after \(reconnectAttempts) attempts - triggering catchup")
                    if let handler = eventHandlers[convoId], let reconnectedHandler = handler.onReconnected {
                        await reconnectedHandler()
                    }
                }

                // Process events from stream
                print("[SSE] Starting event loop for: \(convoId.prefix(12))..., waiting for events...")
                logger.info("📡 SSE: Starting event loop for convoId: \(convoId)")
                var reconnectRequested = false
                let loopOutcome = try await MLSCanonicalTransportAdapter.consumeCanonicalStream(
                    eventStream,
                    shouldStop: { await self.shouldStop[convoId] == true },
                    handle: { output in
                        switch replayGate.decide(output) {
                        case .skip:
                            // Already committed on this fence; avoid a
                            // duplicate action and never regress the cursor.
                            return .handled
                        case let .reconnect(error):
                            await handler.onError?(error)
                            return .reconnect(error)
                        case let .handle(expectedPreviousCursor):
                            let result = await self.handleCanonicalEvent(
                                output,
                                for: convoId,
                                expectedPreviousCursor: expectedPreviousCursor
                            )
                            if case .handled = result {
                                latestSavedCursor = await self.lastCursor[convoId] ?? latestSavedCursor
                            }
                            return result
                        }
                    }
                )
                let eventCount: Int
                switch loopOutcome {
                case let .ended(count), let .stopped(count):
                    eventCount = count
                case let .reconnect(error, count):
                    eventCount = count
                    do {
                        _ = try await failureCoordinator.record(error)
                    } catch {
                        logger.error("📡 SSE: Failed to persist canonical subscription failure for \(convoId): \(error)")
                        await handler.onError?(error)
                    }
                    reconnectRequested = true
                }

                // Check if we're stopping gracefully
                if shouldStop[convoId] == true {
                    logger.info("📡 SSE: Exiting loop due to graceful shutdown for: \(convoId)")
                    break
                }

                if reconnectRequested {
                    reconnectAttempts += 1
                    connectionState[convoId] = .reconnecting
                    logger.info(
                        "📡 SSE: Reconnecting immediately after canonical event failure for \(convoId) (attempt \(reconnectAttempts))"
                    )
                    continue
                }

                print("[SSE] Stream ended for: \(convoId.prefix(12))..., received \(eventCount) events, duration: \(Date().timeIntervalSince(connectionStartTime))s")
                // Check if connection was stable for a while (reset retries if > 5 seconds)
                let duration = Date().timeIntervalSince(connectionStartTime)
                if duration > 5.0 {
                    reconnectAttempts = 0
                }

                // If we reach here, connection was closed
                if eventCount == 0 {
                    // Stream closed immediately without any events - treat as error and retry
                    logger.warning("📡 SSE: Stream closed with 0 events for: \(convoId) - will retry")
                    reconnectAttempts += 1
                } else {
                    logger.info("📡 SSE: Stream ended after \(eventCount) events for: \(convoId) - reconnecting")
                    // If connection was short but had events, treat as unstable
                    if duration < 5.0 {
                        reconnectAttempts += 1
                    }
                }

                if reconnectAttempts < maxReconnectAttempts, reconnectAttempts > 0, shouldStop[convoId] != true {
                    connectionState[convoId] = .reconnecting
                    try? await Task.sleep(nanoseconds: UInt64(reconnectDelay * Double(reconnectAttempts) * 1_000_000_000))
                }

            } catch {
                // Check if this is a cancellation error during graceful shutdown
                if shouldStop[convoId] == true || Task.isCancelled {
                    logger.info("📡 SSE: Exiting due to shutdown/cancellation for: \(convoId)")
                    break
                }

                do {
                    _ = try await failureCoordinator.record(error)
                } catch {
                    logger.error("📡 SSE: Failed to persist canonical subscription failure for \(convoId): \(error)")
                    if let handler = eventHandlers[convoId] {
                        await handler.onError?(error)
                    }
                }

                print("[SSE] Connection error for \(convoId.prefix(12))...: \(error.localizedDescription)")
                logger.error("📡 SSE: Connection error for \(convoId): \(error.localizedDescription) - \(String(describing: error))")

                connectionState[convoId] = .error(error)

                // Notify error handler
                if let handler = eventHandlers[convoId], let errorHandler = handler.onError {
                    await errorHandler(error)
                }

                // Check duration for reset
                if Date().timeIntervalSince(connectionStartTime) > 5.0 {
                    reconnectAttempts = 0
                }

                // Attempt reconnect only if not shutting down
                if !Task.isCancelled, shouldStop[convoId] != true {
                    reconnectAttempts += 1

                    if reconnectAttempts < maxReconnectAttempts {
                        logger.info("Attempting reconnect \(reconnectAttempts)/\(maxReconnectAttempts) for: \(convoId)")
                        connectionState[convoId] = .reconnecting

                        try? await Task.sleep(nanoseconds: UInt64(reconnectDelay * Double(reconnectAttempts) * 1_000_000_000))
                    }
                }
            }
        }

        if reconnectAttempts >= maxReconnectAttempts {
            logger.error("Max reconnect attempts reached for: \(convoId)")
            connectionState[convoId] = .disconnected
        } else if shouldStop[convoId] == true {
            logger.info("📡 SSE: Subscription stopped gracefully for: \(convoId)")
            connectionState[convoId] = .disconnected
        }
    }

    /// Dispatch one canonical event through the shared availability handler.
    private func handleCanonicalEvent(
        _ message: BlueCatbirdChatSubscribeEvents.Message,
        for convoId: String,
        expectedPreviousCursor: String
    ) async -> MLSCanonicalTransportAdapter.MLSCanonicalStreamHandlingResult {
        guard let handler = eventHandlers[convoId] else {
            logger.warning("📡 SSE: No handler found for canonical stream \(convoId)")
            return .reconnect(MLSCanonicalInventoryActionMissingError.conversationState)
        }

        let apiClient = self.apiClient
        let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
            message,
            subscriptionKey: convoId,
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
                try await self.saveCursor(cursor, for: convoId)
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

    internal static func canonicalSupportRevision(for handler: EventHandler?) -> String {
        handler?.onCanonicalDurableEventActions?.supportRevision
            ?? MLSCanonicalTransportAdapter.MLSCanonicalDurableEventActions.missingTableSupportRevision
    }

    /// Construct the lifecycle coordinator used by every subscription run.
    /// Keeping this factory on the manager makes manager recreation and app
    /// restart use the same scoped durable-state path as reconnect.
    internal static func makeCanonicalSubscriptionFailureCoordinator(
        scope: MLSCanonicalSubscriptionScope?,
        handler: EventHandler?,
        store: MLSEventCursorStore?
    ) -> MLSCanonicalSubscriptionFailureCoordinator {
        MLSCanonicalSubscriptionFailureCoordinator(
            scope: scope,
            supportRevision: canonicalSupportRevision(for: handler),
            store: store
        )
    }

    private func canonicalSubscriptionScope(for convoId: String) async -> MLSCanonicalSubscriptionScope? {
        guard let account = await apiClient.authenticatedUserDID() else {
            logger.warning("📡 SSE: No authenticated account; canonical failure state will remain in-memory")
            return nil
        }
        let normalizedAccount = account.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalizedAccount.isEmpty else { return nil }

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
            logger.warning("📡 SSE: No stable device identity; canonical failure state will remain in-memory")
            return nil
        }

        return MLSCanonicalSubscriptionScope(
            accountIdentifier: normalizedAccount,
            environmentIdentifier: apiClient.mlsServiceDID,
            deviceIdentifier: device,
            subscriptionIdentifier: convoId
        )
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

// NOTE: SSE event stream implementation is now in MLSAPIClient.swift
