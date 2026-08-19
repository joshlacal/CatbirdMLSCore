import Foundation
import Petrel
import PetrelCatbird
import XCTest
@testable import CatbirdMLSCore

final class MLSInventoryAndDurableEventTests: XCTestCase {
  private let expiry = ATProtocolDate(date: Date(timeIntervalSinceNow: 3_600))

  func testConversationOnlyInventoryCannotMintSubscriptionTicket() throws {
    let completion = MLSInventorySessionCompletion(
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-1",
      completedDomains: [.conversations]
    )

    XCTAssertThrowsError(
      try MLSInventorySessionCompletion.requireTicketReady(
        inventorySessionId: "session-1",
        eventCursor: "cursor-1",
        completion: completion
      )
    ) { error in
      XCTAssertEqual(error as? MLSInventorySessionError, .sessionIncomplete)
    }
  }

  func testAllThreeInventoryDomainsShareOneSessionAndSnapshotCursor() async throws {
    var conversationCursors: [String?] = []
    var welcomeCursors: [String?] = []
    var recoveryCursors: [String?] = []

    let snapshot = try await MLSInventorySessionAssembler.assemble(
      fetchConversations: { cursor in
        conversationCursors.append(cursor)
        return self.conversationPage(cursor: cursor)
      },
      fetchPendingWelcomes: { session, cursor in
        XCTAssertEqual(session, "session-1")
        welcomeCursors.append(cursor)
        return self.welcomePage(cursor: cursor)
      },
      fetchLeafRecoveryInbox: { session, cursor in
        XCTAssertEqual(session, "session-1")
        recoveryCursors.append(cursor)
        return self.recoveryPage(cursor: cursor)
      }
    )

    XCTAssertEqual(snapshot.inventorySessionId, "session-1")
    XCTAssertEqual(snapshot.snapshotEventCursor, "event-1")
    XCTAssertTrue(snapshot.conversationItems.isEmpty)
    XCTAssertTrue(snapshot.pendingWelcomeItems.isEmpty)
    XCTAssertTrue(snapshot.leafRecoveryItems.isEmpty)
    XCTAssertEqual(conversationCursors.map { $0 ?? "nil" }, ["nil", "conversation-2"])
    XCTAssertEqual(welcomeCursors.map { $0 ?? "nil" }, ["nil", "welcome-2"])
    XCTAssertEqual(recoveryCursors.map { $0 ?? "nil" }, ["nil", "recovery-2"])
  }

  func testManagerOuterReconnectRetainsFailedFenceAndDoesNotFetchNewInventory() async throws {
    let snapshot = MLSCanonicalInventorySnapshot(
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-0",
      snapshotExpiresAt: expiry.date,
      conversationItems: [],
      pendingWelcomeItems: [],
      leafRecoveryItems: []
    )
    var inventoryFetches = 0
    var completionInstalls = 0
    var persistedCursors: [String] = []
    var fence: MLSCanonicalSubscriptionFence?

    let firstFence = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &fence,
      initialCursor: nil,
      fetchInventory: {
        inventoryFetches += 1
        return snapshot
      },
      reconcile: { _ in },
      installCompletion: { _ in completionInstalls += 1 },
      persistFence: { persistedCursors.append($0) }
    )

    let failedEvent = BlueCatbirdChatSubscribeEvents.Message.unexpected(.object([:]))
    let failure = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      failedEvent,
      subscriptionKey: "conversation-1",
      expectedPreviousCursor: firstFence.snapshotEventCursor,
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in XCTFail("unknown event must not dispatch") },
      saveCursor: { _ in XCTFail("failed event must not advance the fence") }
    )
    guard case .reconnect = failure else {
      return XCTFail("failed event must request an outer reconnect")
    }

    // This is the manager's next outer-loop attempt: the same coordinator
    // state must issue the same ticket fence without fetching a newer snapshot.
    let replayFence = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &fence,
      initialCursor: "cursor-after-a-different-event",
      fetchInventory: {
        inventoryFetches += 1
        XCTFail("event failure must not authorize a fresh inventory")
        return snapshot
      },
      reconcile: { _ in XCTFail("event failure must not re-reconcile a new snapshot") },
      installCompletion: { _ in completionInstalls += 1 },
      persistFence: { persistedCursors.append($0) }
    )

    XCTAssertEqual(replayFence, firstFence)
    XCTAssertEqual(fence, firstFence)
    XCTAssertEqual(inventoryFetches, 1)
    XCTAssertEqual(completionInstalls, 1)
    XCTAssertEqual(persistedCursors, ["cursor-0"])
  }

  func testManagersStartReplayAtFreshSnapshotFenceAndRetryFailedPostSnapshotEvent() async throws {
    let freshSnapshot = MLSCanonicalInventorySnapshot(
      inventorySessionId: "session-fresh",
      snapshotEventCursor: "cursor-snapshot",
      snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600),
      conversationItems: [],
      pendingWelcomeItems: [],
      leafRecoveryItems: []
    )
    var freshFence: MLSCanonicalSubscriptionFence?
    var persistedSnapshotCursors: [String] = []
    let prepared = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &freshFence,
      initialCursor: "foreign-cursor",
      fetchInventory: { freshSnapshot },
      reconcile: { _ in },
      installCompletion: { _ in },
      persistFence: { persistedSnapshotCursors.append($0) }
    )
    XCTAssertEqual(prepared.snapshotEventCursor, "cursor-snapshot")
    XCTAssertEqual(persistedSnapshotCursors, ["cursor-snapshot"])

    let postSnapshotEvent = BlueCatbirdChatSubscribeEvents.Message.blueCatbirdChatDefsEventEnvelope(
      .init(
        previousCursor: "cursor-snapshot",
        cursor: "cursor-post-snapshot",
        payload: .blueCatbirdChatDefsConversationChangedEvent(
          .init(conversationId: "conversation-1")
        ),
        createdAt: expiry
      )
    )

    // A fresh aggregate fence must discard a stale/foreign input cursor. Both
    // managers use the same decision contract, but each factory is exercised
    // independently so neither can regress to the stale cursor behavior.
    let gateFactories: [
      (MLSCanonicalSubscriptionFence?, MLSCanonicalSubscriptionFence, String?)
        -> MLSCanonicalReplayGate
    ] = [
      MLSWebSocketManager.canonicalReplayGate,
      MLSEventStreamManager.canonicalReplayGate,
    ]
    for makeGate in gateFactories {
      var gate = makeGate(nil, prepared, "foreign-cursor")
      guard case let .handle(expectedPreviousCursor) = gate.decide(postSnapshotEvent) else {
        return XCTFail("a post-snapshot event must not be skipped by a stale cursor")
      }
      XCTAssertEqual(expectedPreviousCursor, "cursor-snapshot")

      var savedCursors: [String] = []
      var dispatched = 0
      let success = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
        postSnapshotEvent,
        subscriptionKey: "conversation-1",
        expectedPreviousCursor: expectedPreviousCursor,
        loadEntries: { _, _ in [] },
        onDurableEvent: { _ in dispatched += 1 },
        saveCursor: { savedCursors.append($0) }
      )
      XCTAssertEqual(success, .handled)
      XCTAssertEqual(dispatched, 1)
      XCTAssertEqual(savedCursors, ["cursor-post-snapshot"])

      // A failed action must reconnect without advancing. Recreating the
      // gate on the retained same fence must handle the exact event again,
      // rather than skipping it because the earlier cursor was advertised.
      var retryGate = makeGate(prepared, prepared, "cursor-snapshot")
      guard case let .handle(retryPreviousCursor) = retryGate.decide(postSnapshotEvent) else {
        return XCTFail("a failed post-snapshot event must remain replayable")
      }
      var failedSavedCursors: [String] = []
      let failed = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
        postSnapshotEvent,
        subscriptionKey: "conversation-1",
        expectedPreviousCursor: retryPreviousCursor,
        loadEntries: { _, _ in [] },
        onDurableEvent: { _ in throw DurableEventTestError.handlerFailed },
        saveCursor: { failedSavedCursors.append($0) }
      )
      guard case let .reconnect(error) = failed else {
        return XCTFail("a failed post-snapshot action must request reconnect")
      }
      XCTAssertEqual(error as? DurableEventTestError, .handlerFailed)
      XCTAssertTrue(failedSavedCursors.isEmpty)

      var finalGate = makeGate(prepared, prepared, "cursor-snapshot")
      guard case let .handle(finalPreviousCursor) = finalGate.decide(postSnapshotEvent) else {
        return XCTFail("the failed post-snapshot event must replay on reconnect")
      }
      XCTAssertEqual(finalPreviousCursor, "cursor-snapshot")
    }

    // Snapshot-cursor persistence is a prerequisite for gate/stream setup.
    // A protected-store failure leaves no installed fence and therefore no
    // replay gate or stream attempt can be authorized.
    var failedFence: MLSCanonicalSubscriptionFence?
    do {
      _ = try await MLSCanonicalSubscriptionCoordinator.prepare(
        fence: &failedFence,
        initialCursor: "foreign-cursor",
        fetchInventory: { freshSnapshot },
        reconcile: { _ in },
        installCompletion: { _ in },
        persistFence: { _ in throw DurableEventTestError.persistenceFailed }
      )
      XCTFail("snapshot persistence failure must stop before gate/stream setup")
    } catch let error as DurableEventTestError {
      XCTAssertEqual(error, .persistenceFailed)
    }
    XCTAssertNil(failedFence)
  }

  func testStableFenceReplaySkipsCommittedPrefixWithoutRegressingCursor() async {
    let committed = BlueCatbirdChatSubscribeEvents.Message.blueCatbirdChatDefsEventEnvelope(
      .init(
        previousCursor: "cursor-0",
        cursor: "cursor-1",
        payload: .blueCatbirdChatDefsConversationChangedEvent(
          .init(conversationId: "conversation-1")
        ),
        createdAt: expiry
      )
    )
    let next = BlueCatbirdChatSubscribeEvents.Message.blueCatbirdChatDefsEventEnvelope(
      .init(
        previousCursor: "cursor-1",
        cursor: "cursor-2",
        payload: .blueCatbirdChatDefsConversationChangedEvent(
          .init(conversationId: "conversation-1")
        ),
        createdAt: expiry
      )
    )
    var gate = MLSCanonicalReplayGate(
      snapshotCursor: "cursor-0",
      savedCursor: "cursor-1"
    )

    guard case .skip = gate.decide(committed) else {
      return XCTFail("the already-persisted prefix must be skipped")
    }
    XCTAssertEqual(gate.scanCursor, "cursor-1")
    XCTAssertNil(gate.targetCursor)

    guard case let .handle(expectedPreviousCursor) = gate.decide(next) else {
      return XCTFail("the first uncommitted event must be handled")
    }
    XCTAssertEqual(expectedPreviousCursor, "cursor-1")

    var dispatched = 0
    var savedCursors: [String] = []
    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      next,
      subscriptionKey: "conversation-1",
      expectedPreviousCursor: expectedPreviousCursor,
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in dispatched += 1 },
      saveCursor: { savedCursors.append($0) }
    )
    guard case .handled = result else {
      return XCTFail("the first uncommitted event must commit after handling")
    }
    XCTAssertEqual(dispatched, 1)
    XCTAssertEqual(savedCursors, ["cursor-2"])

    let unknown = BlueCatbirdChatSubscribeEvents.Message.blueCatbirdChatDefsEventEnvelope(
      .init(
        previousCursor: "cursor-0",
        cursor: "cursor-1",
        payload: .unexpected(.object([:])),
        createdAt: expiry
      )
    )
    var failedReplayGate = MLSCanonicalReplayGate(
      snapshotCursor: "cursor-0",
      savedCursor: "cursor-2"
    )
    guard case let .handle(unknownExpectedCursor) = failedReplayGate.decide(unknown) else {
      return XCTFail("an unknown envelope cannot be skipped during replay")
    }
    var unknownSavedCursors: [String] = []
    let unknownResult = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      unknown,
      subscriptionKey: "conversation-1",
      expectedPreviousCursor: unknownExpectedCursor,
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in XCTFail("unknown payload must not dispatch") },
      saveCursor: { unknownSavedCursors.append($0) }
    )
    guard case .reconnect = unknownResult else {
      return XCTFail("unknown replay payload must request reconnect")
    }
    XCTAssertTrue(unknownSavedCursors.isEmpty)
  }

  func testExpiredFenceForcesExplicitInventoryRefresh() async throws {
    let refreshedSnapshot = MLSCanonicalInventorySnapshot(
      inventorySessionId: "session-2",
      snapshotEventCursor: "cursor-2",
      snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600),
      conversationItems: [],
      pendingWelcomeItems: [],
      leafRecoveryItems: []
    )
    var fence: MLSCanonicalSubscriptionFence? = MLSCanonicalSubscriptionFence(
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-1",
      snapshotExpiresAt: Date(timeIntervalSince1970: 1)
    )
    var fetched = 0

    let refreshed = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &fence,
      initialCursor: "cursor-1",
      fetchInventory: {
        fetched += 1
        return refreshedSnapshot
      },
      reconcile: { _ in },
      installCompletion: { _ in },
      persistFence: { _ in }
    )

    XCTAssertEqual(fetched, 1)
    XCTAssertEqual(refreshed.inventorySessionId, "session-2")
    XCTAssertEqual(refreshed.snapshotEventCursor, "cursor-2")
    XCTAssertEqual(fence, refreshed)
  }

  func testUnsupportedEventLatchBlocksExpiredFenceUntilExplicitRecovery() async throws {
    let failedEvent = BlueCatbirdChatSubscribeEvents.Message.unexpected(.object([:]))
    let failureResult = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      failedEvent,
      subscriptionKey: "conversation-1",
      expectedPreviousCursor: "cursor-0",
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in XCTFail("unknown event must not dispatch") },
      saveCursor: { _ in XCTFail("unknown event must not advance the cursor") }
    )
    guard case let .reconnect(failure) = failureResult else {
      return XCTFail("unknown event must produce a reconnect failure")
    }

    var latch = MLSCanonicalSubscriptionFailureLatch()
    XCTAssertTrue(latch.record(failure))
    XCTAssertEqual(
      latch.terminalFailure,
      .unsupportedDurableEvent(typeIdentifier: "blue.catbird.chat.defs#eventEnvelope")
    )

    var fence: MLSCanonicalSubscriptionFence? = MLSCanonicalSubscriptionFence(
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-0",
      snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600)
    )
    var inventoryFetches = 0
    var persistedCursors: [String] = []
    let refreshedSnapshot = MLSCanonicalInventorySnapshot(
      inventorySessionId: "session-2",
      snapshotEventCursor: "cursor-2",
      snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600),
      conversationItems: [],
      pendingWelcomeItems: [],
      leafRecoveryItems: []
    )

    let replayFence = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &fence,
      initialCursor: "cursor-1",
      terminalFailure: latch.terminalFailure,
      fetchInventory: {
        inventoryFetches += 1
        XCTFail("same-fence reconnect must not fetch inventory")
        return refreshedSnapshot
      },
      reconcile: { _ in XCTFail("same-fence reconnect must not reconcile") },
      installCompletion: { _ in XCTFail("same-fence reconnect must not install completion") },
      persistFence: { persistedCursors.append($0) }
    )
    XCTAssertEqual(replayFence.inventorySessionId, "session-1")
    XCTAssertEqual(replayFence.snapshotEventCursor, "cursor-0")
    fence = MLSCanonicalSubscriptionFence(
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-0",
      snapshotExpiresAt: Date(timeIntervalSince1970: 1)
    )

    do {
      _ = try await MLSCanonicalSubscriptionCoordinator.prepare(
        fence: &fence,
        initialCursor: "cursor-0",
        terminalFailure: latch.terminalFailure,
        fetchInventory: {
          inventoryFetches += 1
          return refreshedSnapshot
        },
        reconcile: { _ in XCTFail("blocked failure must not reconcile") },
        installCompletion: { _ in XCTFail("blocked failure must not install completion") },
        persistFence: { persistedCursors.append($0) }
      )
      XCTFail("terminal durable failure must block refresh")
    } catch let error as MLSCanonicalSubscriptionCoordinatorError {
      XCTAssertEqual(
        error,
        .blocked(.unsupportedDurableEvent(typeIdentifier: "blue.catbird.chat.defs#eventEnvelope"))
      )
    }
    XCTAssertEqual(inventoryFetches, 0)
    XCTAssertTrue(persistedCursors.isEmpty)
    XCTAssertEqual(fence?.inventorySessionId, "session-1")

    latch.clear(after: .supportedClientRecovery)
    XCTAssertNil(latch.terminalFailure)
    let refreshed = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &fence,
      initialCursor: "cursor-0",
      terminalFailure: latch.terminalFailure,
      fetchInventory: {
        inventoryFetches += 1
        return refreshedSnapshot
      },
      reconcile: { _ in },
      installCompletion: { _ in },
      persistFence: { persistedCursors.append($0) }
    )
    XCTAssertEqual(refreshed.inventorySessionId, "session-2")
    XCTAssertEqual(inventoryFetches, 1)
    XCTAssertEqual(persistedCursors, ["cursor-2"])
  }

  func testMissingActionLatchAlsoSurvivesFenceExpiryUntilActionTableReplacement() async throws {
    let envelope = BlueCatbirdChatDefs.EventEnvelope(
      previousCursor: "cursor-0",
      cursor: "cursor-1",
      payload: .blueCatbirdChatDefsConversationChangedEvent(
        .init(conversationId: "conversation-1")
      ),
      createdAt: expiry
    )
    let actions = MLSCanonicalDurableEventActions()
    let failureResult = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      .blueCatbirdChatDefsEventEnvelope(envelope),
      subscriptionKey: "conversation-1",
      expectedPreviousCursor: "cursor-0",
      loadEntries: { _, _ in [] },
      onDurableEvent: { event in try await actions.dispatch(event) },
      saveCursor: { _ in XCTFail("missing action must not advance the cursor") }
    )
    guard case let .reconnect(failure) = failureResult else {
      return XCTFail("missing action must produce a reconnect failure")
    }

    var latch = MLSCanonicalSubscriptionFailureLatch()
    XCTAssertTrue(latch.record(failure))
    XCTAssertEqual(
      latch.terminalFailure,
      .missingDurableAction(action: "conversationChanged")
    )

    var fence: MLSCanonicalSubscriptionFence? = MLSCanonicalSubscriptionFence(
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-0",
      snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600)
    )
    var inventoryFetches = 0
    var persistedCursors: [String] = []
    let replayFence = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &fence,
      initialCursor: "cursor-1",
      terminalFailure: latch.terminalFailure,
      fetchInventory: {
        inventoryFetches += 1
        XCTFail("same-fence reconnect must not fetch inventory")
        return MLSCanonicalInventorySnapshot(
          inventorySessionId: "unexpected",
          snapshotEventCursor: "unexpected",
          snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600),
          conversationItems: [],
          pendingWelcomeItems: [],
          leafRecoveryItems: []
        )
      },
      reconcile: { _ in XCTFail("same-fence reconnect must not reconcile") },
      installCompletion: { _ in XCTFail("same-fence reconnect must not install completion") },
      persistFence: { persistedCursors.append($0) }
    )
    XCTAssertEqual(replayFence.inventorySessionId, "session-1")
    XCTAssertEqual(replayFence.snapshotEventCursor, "cursor-0")
    fence = MLSCanonicalSubscriptionFence(
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-0",
      snapshotExpiresAt: Date(timeIntervalSince1970: 1)
    )
    do {
      _ = try await MLSCanonicalSubscriptionCoordinator.prepare(
        fence: &fence,
        initialCursor: "cursor-0",
        terminalFailure: latch.terminalFailure,
        fetchInventory: {
          inventoryFetches += 1
          XCTFail("missing action latch must block expired-fence fetch")
          return MLSCanonicalInventorySnapshot(
            inventorySessionId: "unexpected",
            snapshotEventCursor: "unexpected",
            snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600),
            conversationItems: [],
            pendingWelcomeItems: [],
            leafRecoveryItems: []
          )
        },
        reconcile: { _ in XCTFail("blocked failure must not reconcile") },
        installCompletion: { _ in XCTFail("blocked failure must not install completion") },
        persistFence: { persistedCursors.append($0) }
      )
      XCTFail("missing action must remain terminal across expiry")
    } catch let error as MLSCanonicalSubscriptionCoordinatorError {
      XCTAssertEqual(
        error,
        .blocked(.missingDurableAction(action: "conversationChanged"))
      )
    }
    XCTAssertEqual(inventoryFetches, 0)
    XCTAssertTrue(persistedCursors.isEmpty)

    // Replacing the action table is the supported client transition that
    // clears the terminal latch and permits a new coherent inventory fence.
    latch.clear(after: .actionTableReplaced)
    XCTAssertNil(latch.terminalFailure)
    let refreshed = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &fence,
      initialCursor: "cursor-0",
      terminalFailure: latch.terminalFailure,
      fetchInventory: {
        inventoryFetches += 1
        return MLSCanonicalInventorySnapshot(
          inventorySessionId: "session-2",
          snapshotEventCursor: "cursor-2",
          snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600),
          conversationItems: [],
          pendingWelcomeItems: [],
          leafRecoveryItems: []
        )
      },
      reconcile: { _ in },
      installCompletion: { _ in },
      persistFence: { persistedCursors.append($0) }
    )
    XCTAssertEqual(refreshed.inventorySessionId, "session-2")
    XCTAssertEqual(inventoryFetches, 1)
    XCTAssertEqual(persistedCursors, ["cursor-2"])
  }

  func testTransientCursorAndProjectionFailuresDoNotLatchTerminalRecovery() {
    var latch = MLSCanonicalSubscriptionFailureLatch()

    XCTAssertFalse(
      latch.record(
        MLSCanonicalCursorError.cursorDidNotAdvance("cursor-1")
      )
    )
    XCTAssertFalse(
      latch.record(
        MLSCanonicalMessageAvailabilityError(
          conversationId: "conversation-1",
          sequence: 7
        )
      )
    )
    XCTAssertNil(latch.terminalFailure)
  }

  func testManagerLifecyclePersistsTerminalLatchAcrossReconnectRecreationAndRestart() async throws {
    let scope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:alice",
      environmentIdentifier: "did:web:mls.example#atproto_mls",
      deviceIdentifier: "device-1",
      subscriptionIdentifier: "conversation-1"
    )
    let store = ScopedRecordingCursorStore()
    let failure = MLSUnsupportedDurableEventError(typeIdentifier: "unknown")
    let actions = makeCompleteCanonicalDurableActions()
    let webSocketHandler = MLSWebSocketManager.EventHandler(
      onCanonicalDurableEventActions: actions
    )
    let sseHandler = MLSEventStreamManager.EventHandler(
      onCanonicalDurableEventActions: actions
    )

    var initialManager = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: scope,
      handler: webSocketHandler,
      store: store
    )
    let initialRecorded = try await initialManager.record(failure)
    XCTAssertTrue(initialRecorded)

    // Public reconnect/resubscribe in the same capability revision must load
    // the durable block rather than resetting it with a new task.
    var reconnectManager = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: scope,
      handler: webSocketHandler,
      store: store
    )
    try await reconnectManager.load()
    XCTAssertEqual(
      reconnectManager.terminalFailure,
      .unsupportedDurableEvent(typeIdentifier: "unknown")
    )

    // A recreated manager in the same process sees the same block.
    var recreatedManager = try MLSEventStreamManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: scope,
      handler: sseHandler,
      store: store
    )
    try await recreatedManager.load()
    XCTAssertEqual(recreatedManager.terminalFailure, reconnectManager.terminalFailure)

    // A same-version app restart using the same protected store also sees it.
    var restartedManager = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: scope,
      handler: webSocketHandler,
      store: store
    )
    try await restartedManager.load()
    XCTAssertEqual(restartedManager.terminalFailure, recreatedManager.terminalFailure)

    var expiredFence: MLSCanonicalSubscriptionFence? = MLSCanonicalSubscriptionFence(
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-0",
      snapshotExpiresAt: Date(timeIntervalSince1970: 1)
    )
    do {
      _ = try await MLSCanonicalSubscriptionCoordinator.prepare(
        fence: &expiredFence,
        initialCursor: "cursor-0",
        terminalFailure: restartedManager.terminalFailure,
        fetchInventory: {
          XCTFail("a persisted terminal block must prevent post-expiry inventory fetch")
          return MLSCanonicalInventorySnapshot(
            inventorySessionId: "unexpected",
            snapshotEventCursor: "unexpected",
            snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600),
            conversationItems: [],
            pendingWelcomeItems: [],
            leafRecoveryItems: []
          )
        },
        reconcile: { _ in XCTFail("a persisted terminal block must prevent reconciliation") },
        installCompletion: { _ in XCTFail("a persisted terminal block must prevent completion") },
        persistFence: { _ in XCTFail("a persisted terminal block must prevent cursor writes") }
      )
      XCTFail("post-expiry persisted terminal failure must remain blocking")
    } catch let error as MLSCanonicalSubscriptionCoordinatorError {
      XCTAssertEqual(
        error,
        .blocked(.unsupportedDurableEvent(typeIdentifier: "unknown"))
      )
    }
  }

  @MainActor
  func testManagerLifecycleRevisionChangeClearsOnlyThatScopedLatch() async throws {
    let aliceScope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:alice",
      environmentIdentifier: "did:web:mls.example#atproto_mls",
      deviceIdentifier: "device-1",
      subscriptionIdentifier: "conversation-1"
    )
    let store = ScopedRecordingCursorStore()
    let oldCapability = MLSCanonicalSubscriptionCapability(
      generatedUnionRevision: .eventEnvelopeV1,
      actionTableRevision: .v1
    )
    try await MLSCanonicalSubscriptionFailurePersistence.save(
      failure: .unsupportedDurableEvent(typeIdentifier: "unknown"),
      scope: aliceScope,
      capability: oldCapability,
      store: store
    )

    // A caller cannot replace the Core-owned capability with an arbitrary
    // label. An incomplete table remains rejected even with an old record.
    do {
      _ = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
        scope: aliceScope,
        handler: MLSWebSocketManager.EventHandler(),
        store: store
      )
      XCTFail("an incomplete action table must not authorize recovery")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .incompleteActionTable)
    }

    let currentHandler = MLSWebSocketManager.EventHandler(
      onCanonicalDurableEventActions: makeCompleteCanonicalDurableActions()
    )
    var newRevision = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: aliceScope,
      handler: currentHandler,
      store: store
    )
    try await newRevision.load()
    XCTAssertNil(newRevision.terminalFailure)
    try await newRevision.clear(after: .actionTableReplaced)
    XCTAssertNil(newRevision.terminalFailure)

    // The same capability on another account must not inherit Alice's block.
    let bobScope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:bob",
      environmentIdentifier: aliceScope.environmentIdentifier,
      deviceIdentifier: aliceScope.deviceIdentifier,
      subscriptionIdentifier: aliceScope.subscriptionIdentifier
    )
    let sseHandler = MLSEventStreamManager.EventHandler(
      onCanonicalDurableEventActions: makeCompleteCanonicalDurableActions()
    )
    var bob = try MLSEventStreamManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: bobScope,
      handler: sseHandler,
      store: store
    )
    try await bob.load()
    XCTAssertNil(bob.terminalFailure)

    // Device, environment, and subscription changes are also isolated.
    for changedScope in [
      MLSCanonicalSubscriptionScope(
        accountIdentifier: aliceScope.accountIdentifier,
        environmentIdentifier: "did:web:staging.example#atproto_mls",
        deviceIdentifier: aliceScope.deviceIdentifier,
        subscriptionIdentifier: aliceScope.subscriptionIdentifier
      ),
      MLSCanonicalSubscriptionScope(
        accountIdentifier: aliceScope.accountIdentifier,
        environmentIdentifier: aliceScope.environmentIdentifier,
        deviceIdentifier: "device-2",
        subscriptionIdentifier: aliceScope.subscriptionIdentifier
      ),
      MLSCanonicalSubscriptionScope(
        accountIdentifier: aliceScope.accountIdentifier,
        environmentIdentifier: aliceScope.environmentIdentifier,
        deviceIdentifier: aliceScope.deviceIdentifier,
        subscriptionIdentifier: "conversation-2"
      ),
    ] {
      var isolated = try MLSEventStreamManager.makeCanonicalSubscriptionFailureCoordinator(
        scope: changedScope,
        handler: sseHandler,
        store: store
      )
      try await isolated.load()
      XCTAssertNil(isolated.terminalFailure)
    }
  }

  @MainActor
  func testManagerFailurePersistenceErrorDoesNotClaimDurableLatch() async throws {
    let scope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:alice",
      environmentIdentifier: "did:web:mls.example#atproto_mls",
      deviceIdentifier: "device-1",
      subscriptionIdentifier: "conversation-1"
    )
    let store = RecordingCursorStore(failure: .persistenceFailed)
    let handler = MLSWebSocketManager.EventHandler(
      onCanonicalDurableEventActions: makeCompleteCanonicalDurableActions()
    )
    var coordinator = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: scope,
      handler: handler,
      store: store
    )

    do {
      _ = try await coordinator.record(
        MLSUnsupportedDurableEventError(typeIdentifier: "unknown")
      )
      XCTFail("a protected-store failure must be observable")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .persistenceUnavailable)
    }
    XCTAssertEqual(
      coordinator.terminalFailure,
      .unsupportedDurableEvent(typeIdentifier: "unknown")
    )
    XCTAssertTrue(coordinator.persistenceUnavailable)
    XCTAssertTrue(store.savedCursors.isEmpty)

    // Once the protected store becomes writable, an explicit retry persists
    // the retained terminal latch; it is not silently discarded.
    store.failure = nil
    let retried = try await coordinator.record(
      MLSUnsupportedDurableEventError(typeIdentifier: "unknown")
    )
    XCTAssertTrue(retried)
    XCTAssertFalse(coordinator.persistenceUnavailable)
    XCTAssertFalse(store.savedCursors.isEmpty)
  }

  @MainActor
  func testManagerReconnectRetriesPendingTerminalWriteBeforeInventoryForWebSocketAndSSE() async throws {
    let aliceScope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:alice",
      environmentIdentifier: "did:web:mls.example#atproto_mls",
      deviceIdentifier: "device-1",
      subscriptionIdentifier: "conversation-1"
    )
    let actions = makeCompleteCanonicalDurableActions()
    let webSocketHandler = MLSWebSocketManager.EventHandler(
      onCanonicalDurableEventActions: actions
    )
    let sseHandler = MLSEventStreamManager.EventHandler(
      onCanonicalDurableEventActions: actions
    )
    let store = RecordingCursorStore(failure: .persistenceFailed)
    let webSocketManager = await Self.makeWebSocketManager(
      serviceDID: aliceScope.environmentIdentifier
    )
    await webSocketManager.configureCursorStore(store)

    // A terminal event is classified in the run, but the protected write
    // fails. The manager must retain the exact poison after that run exits.
    var failedRun = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: aliceScope,
      handler: webSocketHandler,
      store: store
    )
    do {
      _ = try await failedRun.record(
        MLSUnsupportedDurableEventError(typeIdentifier: "unknown")
      )
      XCTFail("the first terminal write must fail with the protected store")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .persistenceUnavailable)
    }
    await webSocketManager.retainCanonicalSubscriptionFailure(failedRun)
    XCTAssertTrue(store.savedCursors.isEmpty)

    // Public reconnect uses a newly-created coordinator and the same scoped
    // store. While storage remains unavailable, retrying must fail closed and
    // preserve the pending record for the next reconnect rather than allowing
    // aggregate/ticket work to begin.
    _ = try MLSEventStreamManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: aliceScope,
      handler: sseHandler,
      store: store
    )
    let failedReconnect = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: aliceScope,
      handler: webSocketHandler,
      store: store
    )
    do {
      _ = try await webSocketManager.prepareCanonicalSubscriptionForReconnect(failedReconnect)
      XCTFail("reconnect must remain hard stopped while the pending write fails")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .persistenceUnavailable)
    }
    XCTAssertTrue(store.savedCursors.isEmpty)

    // A second reconnect proves the failed pending state was retained after
    // the first run exited; it must not silently fall through to a fresh
    // inventory attempt.
    let continuedFailure = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: aliceScope,
      handler: webSocketHandler,
      store: store
    )
    do {
      _ = try await webSocketManager.prepareCanonicalSubscriptionForReconnect(continuedFailure)
      XCTFail("continued storage failure must remain hard stopped")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .persistenceUnavailable)
    }
    XCTAssertTrue(store.savedCursors.isEmpty)

    // Recovery writes the exact pending terminal record before any aggregate
    // fetch. Public reconnect enters this same manager startup gate.
    store.failure = nil
    let recoveredCandidate = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: aliceScope,
      handler: webSocketHandler,
      store: store
    )
    let recovered = try await webSocketManager.prepareCanonicalSubscriptionForReconnect(
      recoveredCandidate
    )
    XCTAssertEqual(
      recovered.terminalFailure,
      .unsupportedDurableEvent(typeIdentifier: "unknown")
    )
    XCTAssertFalse(store.savedCursors.isEmpty)
    let pendingRecord = try XCTUnwrap(
      try JSONDecoder().decode(
        MLSCanonicalSubscriptionFailureRecord.self,
        from: Data(store.savedCursors[0].utf8)
      )
    )
    XCTAssertEqual(pendingRecord.scope, aliceScope)
    XCTAssertEqual(pendingRecord.capability, .current)
    XCTAssertEqual(
      pendingRecord.failure,
      .unsupportedDurableEvent(typeIdentifier: "unknown")
    )

    // Exercise the mirrored SSE manager lifecycle with its own scoped store:
    // its public reconnect startup gate must retain the same hard-stop order.
    let sseStore = RecordingCursorStore(failure: .persistenceFailed)
    let sseManager = await Self.makeEventStreamManager(
      serviceDID: aliceScope.environmentIdentifier
    )
    await sseManager.configureCursorStore(sseStore)
    var sseRun = try MLSEventStreamManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: aliceScope,
      handler: sseHandler,
      store: sseStore
    )
    do {
      _ = try await sseRun.record(
        MLSUnsupportedDurableEventError(typeIdentifier: "sse-unknown")
      )
      XCTFail("the SSE terminal write must fail with the protected store")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .persistenceUnavailable)
    }
    await sseManager.retainCanonicalSubscriptionFailure(sseRun)
    let sseRetry = try MLSEventStreamManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: aliceScope,
      handler: sseHandler,
      store: sseStore
    )
    do {
      _ = try await sseManager.prepareCanonicalSubscriptionForReconnect(sseRetry)
      XCTFail("SSE reconnect must remain hard stopped while storage fails")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .persistenceUnavailable)
    }
    sseStore.failure = nil
    let sseRecoveredCandidate = try MLSEventStreamManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: aliceScope,
      handler: sseHandler,
      store: sseStore
    )
    let sseRecovered = try await sseManager.prepareCanonicalSubscriptionForReconnect(
      sseRecoveredCandidate
    )
    XCTAssertEqual(
      sseRecovered.terminalFailure,
      .unsupportedDurableEvent(typeIdentifier: "sse-unknown")
    )
    XCTAssertEqual(sseStore.savedCursors.count, 1)

    do {
      try await Self.prepareWithTerminalFailure(recovered.terminalFailure)
      XCTFail("the retained terminal failure must remain blocking after persistence")
    } catch let error as MLSCanonicalSubscriptionCoordinatorError {
      XCTAssertEqual(
        error,
        .blocked(.unsupportedDurableEvent(typeIdentifier: "unknown"))
      )
    }
    // A different account has a different durable key and cannot consume or
    // write Alice's pending poison during an account switch.
    let bobScope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:bob",
      environmentIdentifier: aliceScope.environmentIdentifier,
      deviceIdentifier: aliceScope.deviceIdentifier,
      subscriptionIdentifier: aliceScope.subscriptionIdentifier
    )
    let bobCandidate = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: bobScope,
      handler: webSocketHandler,
      store: store
    )
    let bob = try await webSocketManager.prepareCanonicalSubscriptionForReconnect(
      bobCandidate
    )
    XCTAssertNil(bob.terminalFailure)
    XCTAssertEqual(store.savedCursors.count, 1)
  }

  @MainActor
  func testManagerFailureLoadStopsBeforeSubscription() async throws {
    let scope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:alice",
      environmentIdentifier: "did:web:mls.example#atproto_mls",
      deviceIdentifier: "device-1",
      subscriptionIdentifier: "conversation-1"
    )
    let store = RecordingCursorStore(readFailure: .persistenceFailed)
    let handler = MLSWebSocketManager.EventHandler(
      onCanonicalDurableEventActions: makeCompleteCanonicalDurableActions()
    )
    var coordinator = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: scope,
      handler: handler,
      store: store
    )

    do {
      try await coordinator.load()
      XCTFail("a protected-store read failure must stop subscription startup")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .persistenceUnavailable)
    }
    XCTAssertTrue(coordinator.persistenceUnavailable)
  }

  @MainActor
  func testManagerLifecycleRequiresExactDurableScopeStorageAndCompleteActions() async throws {
    let validScope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:alice",
      environmentIdentifier: "did:web:mls.example#atproto_mls",
      deviceIdentifier: "device-1",
      subscriptionIdentifier: "conversation-1"
    )
    let completeHandler = MLSWebSocketManager.EventHandler(
      onCanonicalDurableEventActions: makeCompleteCanonicalDurableActions()
    )
    let store = ScopedRecordingCursorStore()

    do {
      _ = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
        scope: nil,
        handler: completeHandler,
        store: store
      )
      XCTFail("missing scope must fail before subscription startup")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .missingScope)
    }

    do {
      _ = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
        scope: validScope,
        handler: completeHandler,
        store: nil
      )
      XCTFail("missing durable storage must fail before subscription startup")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .missingStorage)
    }

    do {
      _ = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
        scope: validScope,
        handler: MLSWebSocketManager.EventHandler(),
        store: store
      )
      XCTFail("an incomplete action table must fail before subscription startup")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .incompleteActionTable)
    }

    do {
      _ = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
        scope: MLSCanonicalSubscriptionScope(
          accountIdentifier: "",
          environmentIdentifier: validScope.environmentIdentifier,
          deviceIdentifier: validScope.deviceIdentifier,
          subscriptionIdentifier: validScope.subscriptionIdentifier
        ),
        handler: completeHandler,
        store: store
      )
      XCTFail("an empty account component must fail closed")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .missingScope)
    }
  }

  @MainActor
  func testIncompleteLabelChangeCannotClearTerminalRecord() async throws {
    let scope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:alice",
      environmentIdentifier: "did:web:mls.example#atproto_mls",
      deviceIdentifier: "device-1",
      subscriptionIdentifier: "conversation-1"
    )
    let store = ScopedRecordingCursorStore()
    let oldCapability = MLSCanonicalSubscriptionCapability(
      generatedUnionRevision: .eventEnvelopeV1,
      actionTableRevision: .v1
    )
    try await MLSCanonicalSubscriptionFailurePersistence.save(
      failure: .unsupportedDurableEvent(typeIdentifier: "unknown"),
      scope: scope,
      capability: oldCapability,
      store: store
    )

    // A caller cannot replace the Core-owned capability with an arbitrary
    // label. An incomplete table remains rejected and cannot clear the record.
    let incompleteV2Handler = MLSWebSocketManager.EventHandler(
      onCanonicalDurableEventActions: MLSCanonicalDurableEventActions()
    )
    do {
      _ = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
        scope: scope,
        handler: incompleteV2Handler,
        store: store
      )
      XCTFail("an incomplete table must not authorize capability recovery")
    } catch let error as MLSCanonicalSubscriptionFailureConfigurationError {
      XCTAssertEqual(error, .incompleteActionTable)
    }

    let completeV2Handler = MLSWebSocketManager.EventHandler(
      onCanonicalDurableEventActions: makeCompleteCanonicalDurableActions()
    )
    var recovered = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: scope,
      handler: completeV2Handler,
      store: store
    )
    try await recovered.load()
    XCTAssertNil(recovered.terminalFailure)
  }

  @MainActor
  func testPersistedScopeMismatchIsInvalidRecord() async throws {
    let storedScope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:alice",
      environmentIdentifier: "did:web:mls.example#atproto_mls",
      deviceIdentifier: "device-1",
      subscriptionIdentifier: "conversation-1"
    )
    let requestedScope = MLSCanonicalSubscriptionScope(
      accountIdentifier: "did:plc:bob",
      environmentIdentifier: storedScope.environmentIdentifier,
      deviceIdentifier: storedScope.deviceIdentifier,
      subscriptionIdentifier: storedScope.subscriptionIdentifier
    )
    let record = MLSCanonicalSubscriptionFailureRecord(
      scope: storedScope,
      capability: MLSCanonicalSubscriptionCapability.current,
      failure: .unsupportedDurableEvent(typeIdentifier: "unknown")
    )
    let encoded = String(
      data: try JSONEncoder().encode(record),
      encoding: .utf8
    )!
    let store = SingleValueCursorStore(value: encoded)
    let handler = MLSWebSocketManager.EventHandler(
      onCanonicalDurableEventActions: makeCompleteCanonicalDurableActions()
    )
    var coordinator = try MLSWebSocketManager.makeCanonicalSubscriptionFailureCoordinator(
      scope: requestedScope,
      handler: handler,
      store: store
    )

    do {
      try await coordinator.load()
      XCTFail("a record for another scope must not be treated as capability recovery")
    } catch let error as MLSCanonicalSubscriptionFailurePersistenceError {
      XCTAssertEqual(error, .invalidRecord)
    }
  }

  func testChangedInventoryContinuationStateFailsClosed() async {
    do {
      _ = try await MLSInventorySessionAssembler.assemble(
        fetchConversations: { cursor in
          if cursor == nil {
            return self.conversationPage(cursor: nil)
          }
          return self.conversationPage(
            cursor: cursor,
            session: "different-session"
          )
        },
        fetchPendingWelcomes: { _, _ in
          XCTFail("changed conversation continuation must stop before welcome paging")
          return self.welcomePage(cursor: nil)
        },
        fetchLeafRecoveryInbox: { _, _ in
          XCTFail("changed conversation continuation must stop before recovery paging")
          return self.recoveryPage(cursor: nil)
        }
      )
      XCTFail("changed continuation state must throw")
    } catch let error as MLSInventorySessionError {
      guard case .continuationChanged(.conversations) = error else {
        return XCTFail("unexpected inventory error: \(error)")
      }
    } catch {
      XCTFail("unexpected error: \(error)")
    }
  }

  func testMissingInventoryContinuationCursorFailsClosed() async {
    do {
      _ = try await MLSInventorySessionAssembler.assemble(
        fetchConversations: { cursor in
          self.conversationPage(cursor: cursor, omitContinuation: cursor == nil)
        },
        fetchPendingWelcomes: { _, _ in
          XCTFail("missing conversation continuation must stop before welcome paging")
          return self.welcomePage(cursor: nil)
        },
        fetchLeafRecoveryInbox: { _, _ in
          XCTFail("missing conversation continuation must stop before recovery paging")
          return self.recoveryPage(cursor: nil)
        }
      )
      XCTFail("missing continuation cursor must throw")
    } catch let error as MLSInventorySessionError {
      guard case .missingContinuation(.conversations) = error else {
        return XCTFail("unexpected inventory error: \(error)")
      }
    } catch {
      XCTFail("unexpected error: \(error)")
    }
  }

  func testRepeatedInventoryContinuationCursorFailsClosed() async {
    var calls = 0
    do {
      _ = try await MLSInventorySessionAssembler.assemble(
        fetchConversations: { cursor in
          calls += 1
          if calls > 2 {
            XCTFail("assembler must stop before a repeated cursor can spin")
          }
          return self.conversationPage(
            cursor: cursor,
            repeatedContinuation: true
          )
        },
        fetchPendingWelcomes: { _, _ in
          XCTFail("repeated conversation cursor must stop before welcome paging")
          return self.welcomePage(cursor: nil)
        },
        fetchLeafRecoveryInbox: { _, _ in
          XCTFail("repeated conversation cursor must stop before recovery paging")
          return self.recoveryPage(cursor: nil)
        }
      )
      XCTFail("repeated continuation cursor must throw")
    } catch let error as MLSInventorySessionError {
      guard case .repeatedContinuation(.conversations, "conversation-2") = error else {
        return XCTFail("unexpected inventory error: \(error)")
      }
    } catch {
      XCTFail("unexpected error: \(error)")
    }
    XCTAssertEqual(calls, 2)
  }

  func testInventoryReconcilerRoutesEveryFetchedItemBeforeInstallingFence() async throws {
    let did = try DID(didString: "did:plc:inventory-reconcile-test")
    let coordinate = makeCoordinate(conversationId: "conversation-1")
    let metadata = BlueCatbirdChatDefs.MetadataSnapshot(
      coordinate: BlueCatbirdChatDefs.MetadataCryptoContext(
        conversationId: Bytes(data: Data([0x01])),
        generation: 1,
        groupId: Bytes(data: Data([0x02])),
        epoch: 1,
        groupContextHash: Bytes(data: Data([0x03])),
        confirmationTag: Bytes(data: Data([0x04]))
      ),
      originTransitionId: "transition-1",
      metadataVersion: 1,
      nonce: Bytes(data: Data([0x05])),
      ciphertext: Bytes(data: Data([0x06])),
      ciphertextSha256: Bytes(data: Data([0x07])),
      ciphertextSize: 1,
      avatarBinding: nil,
      authorProof: BlueCatbirdChatDefs.MetadataAuthorProof(
        authorDid: did,
        authorDeviceId: "device-1",
        authorKeyId: "key-1",
        signaturePublicKey: Bytes(data: Data([0x08])),
        authGenerationAtOrigin: 1,
        originTransitionId: "transition-1",
        originSeq: 1,
        roleAtOrigin: "admin",
        deviceStatusAtOrigin: "active"
      )
    )
    let state = BlueCatbirdChatDefs.ConversationState(
      conversationKind: .value_group,
      coordinates: coordinate,
      cipherSuite: .value_MLS_u5f_256_u5f_XWING_u5f_CHACHA20POLY1305_u5f_SHA256_u5f_Ed25519,
      participants: [],
      leaves: [],
      metadataSnapshot: metadata,
      snapshotSeq: 3
    )
    let removal = BlueCatbirdChatDefs.ConversationRemovalTombstone(
      conversationId: "conversation-removed",
      membershipIntervalId: "interval-1",
      userDid: did,
      deviceId: "device-1",
      terminalSeq: 4,
      removedAt: expiry
    )
    let close = BlueCatbirdChatDefs.ConversationCloseTombstone(
      conversationId: "conversation-closed",
      conversationKind: .value_group,
      retired: coordinate,
      closedByDid: did,
      closedByDeviceId: "device-1",
      terminalSeq: 5,
      closedAt: expiry
    )
    let welcome = BlueCatbirdChatDefs.WelcomeView(
      welcomeId: "welcome-1",
      conversationId: "conversation-1",
      transitionSeq: 6,
      coordinates: coordinate,
      status: .value_pending,
      opaqueWelcome: Bytes(data: Data([0x09])),
      sha256: Bytes(data: Data([0x0A])),
      recipientDid: did,
      recipientDeviceId: "device-1",
      provenance: BlueCatbirdChatDefs.RecoveryWelcomeProvenance(
        recoveryRequestId: "recovery-1",
        keyPackageRef: Bytes(data: Data([0x0B]))
      ),
      expiresAt: expiry
    )
    let recoveryWork = BlueCatbirdChatDefs.RecoveryWorkPendingView(
      recoveryWorkId: "recovery-work-1",
      conversationId: "conversation-1",
      recipientDid: did,
      recipientDeviceId: "device-1",
      sourceKind: .value_welcomeExpired,
      sourceId: "welcome-1",
      sourceCoordinate: coordinate,
      status: "pending",
      createdAt: expiry
    )
    let snapshot = MLSCanonicalInventorySnapshot(
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-1",
      snapshotExpiresAt: expiry.date,
      conversationItems: [
        .init(BlueCatbirdChatDefs.ConversationInventoryState(state: state)),
        .init(removal),
        .init(close),
      ],
      pendingWelcomeItems: [welcome],
      leafRecoveryItems: [.init(recoveryWork)]
    )

    var actionsSeen: [String] = []
    try await MLSCanonicalInventoryReconciler.reconcile(
      snapshot,
      actions: MLSCanonicalInventoryActionSet(
        onConversationState: { _ in actionsSeen.append("conversation-state") },
        onConversationRemoval: { _ in actionsSeen.append("conversation-removal") },
        onConversationClose: { _ in actionsSeen.append("conversation-close") },
        onPendingWelcome: { _ in actionsSeen.append("pending-welcome") },
        onLeafRecovery: { _ in actionsSeen.append("leaf-recovery") }
      )
    )

    XCTAssertEqual(
      actionsSeen,
      ["conversation-state", "conversation-removal", "conversation-close", "pending-welcome", "leaf-recovery"]
    )
  }

  func testInventoryReconcilerRejectsUnknownLeafRecoveryItemWithoutInstallingFence() async {
    let snapshot = MLSCanonicalInventorySnapshot(
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-1",
      snapshotExpiresAt: expiry.date,
      conversationItems: [],
      pendingWelcomeItems: [],
      leafRecoveryItems: [.unexpected(.object([:]))]
    )

    do {
      try await MLSCanonicalInventoryReconciler.reconcile(
        snapshot,
        actions: MLSCanonicalInventoryActionSet(onLeafRecovery: { _ in
          XCTFail("unknown recovery item must not reach a concrete action")
        })
      )
      XCTFail("unknown recovery item must fail closed before the snapshot fence is installed")
    } catch let error as MLSCanonicalInventoryActionMissingError {
      XCTAssertEqual(error, .unsupportedLeafRecoveryItem)
    } catch {
      XCTFail("unexpected inventory reconciliation error: \(error)")
    }
  }

  func testEveryGeneratedDurablePayloadDispatchesThroughTypedPath() async {
    let did = try! DID(didString: "did:plc:durable-event-test")
    let payloads: [BlueCatbirdChatDefs.ProtocolEventPayload] = [
      .blueCatbirdChatDefsConversationChangedEvent(
        .init(conversationId: "conversation-1")
      ),
      .blueCatbirdChatDefsConversationClosedEvent(
        .init(conversationId: "conversation-1", conversationKind: .value_group, terminalSeq: 8)
      ),
      .blueCatbirdChatDefsMessageAvailableEvent(
        .init(conversationId: "conversation-1", seq: 4)
      ),
      .blueCatbirdChatDefsWelcomeAvailableEvent(
        .init(welcomeId: "welcome-1", conversationId: "conversation-1")
      ),
      .blueCatbirdChatDefsWelcomeDispositionEvent(
        .init(welcomeId: "welcome-1", status: .value_rejected)
      ),
      .blueCatbirdChatDefsResetRequestedEvent(
        .init(resetRequestId: "reset-1", conversationId: "conversation-1")
      ),
      .blueCatbirdChatDefsLeafRecoveryEvent(
        .init(recoveryRequestId: "recovery-1", conversationId: "conversation-1", status: .value_open)
      ),
      .blueCatbirdChatDefsLeaveRequestEvent(
        .init(leaveRequestId: "leave-1", conversationId: "conversation-1", status: .value_pending)
      ),
      .blueCatbirdChatDefsAccessEndedEvent(
        .init(
          conversationId: "conversation-1",
          membershipIntervalId: "interval-1",
          userDid: did,
          deviceId: "device-1",
          terminalSeq: 10
        )
      ),
      .blueCatbirdChatDefsWatermarkEvent(
        .init(issuedAt: expiry)
      ),
    ]

    var dispatched = 0
    let actions = MLSCanonicalDurableEventActions(
      onConversationChanged: { _ in dispatched += 1 },
      onConversationClosed: { _ in dispatched += 1 },
      onMessageAvailable: { _, _, _ in dispatched += 1 },
      onWelcomeAvailable: { _ in dispatched += 1 },
      onWelcomeDisposition: { _ in dispatched += 1 },
      onResetRequested: { _ in dispatched += 1 },
      onLeafRecovery: { _ in dispatched += 1 },
      onLeaveRequest: { _ in dispatched += 1 },
      onAccessEnded: { _ in dispatched += 1 },
      onWatermark: { _ in dispatched += 1 },
      onTyping: { _ in dispatched += 1 }
    )
    var savedCursors: [String] = []
    for (index, payload) in payloads.enumerated() {
      let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
        .blueCatbirdChatDefsEventEnvelope(
          .init(
            previousCursor: "cursor-\(index)",
            cursor: "cursor-\(index + 1)",
            payload: payload,
            createdAt: expiry
          )
        ),
        subscriptionKey: "conversation-1",
        loadEntries: { conversationId, _ in
          conversationId == "conversation-1" ? [self.makeApplicationEntry()] : []
        },
        onDurableEvent: { event in try await actions.dispatch(event) },
        saveCursor: { savedCursors.append($0) }
      )

      XCTAssertEqual(result, .handled)
    }

    XCTAssertEqual(dispatched, payloads.count)
    XCTAssertEqual(savedCursors, (1 ... payloads.count).map { "cursor-\($0)" })
  }

  func testManagerActionFactoriesReachEveryCanonicalDurableArm() async throws {
    let did = try DID(didString: "did:plc:manager-action-test")
    let entry = makeApplicationEntry()
    let typing = BlueCatbirdChatDefs.TypingEvent(
      typingId: "typing-1",
      conversationId: "conversation-1",
      actorDid: did,
      actorDeviceId: "device-1",
      isTyping: true,
      expiresAt: expiry
    )
    let events: [MLSCanonicalTransportAdapter.MLSCanonicalDurableEvent] = [
      .typing(typing),
      .messageAvailable(
        .init(conversationId: "conversation-1", seq: entry.sequenceNumber),
        cursor: "cursor-1",
        messages: [entry]
      ),
      .conversationChanged(.init(conversationId: "conversation-1")),
      .conversationClosed(
        .init(conversationId: "conversation-1", conversationKind: .value_group, terminalSeq: 8)
      ),
      .welcomeAvailable(.init(welcomeId: "welcome-1", conversationId: "conversation-1")),
      .welcomeDisposition(.init(welcomeId: "welcome-1", status: .value_rejected)),
      .resetRequested(.init(resetRequestId: "reset-1", conversationId: "conversation-1")),
      .leafRecovery(
        .init(recoveryRequestId: "recovery-1", conversationId: "conversation-1", status: .value_open)
      ),
      .leaveRequest(
        .init(leaveRequestId: "leave-1", conversationId: "conversation-1", status: .value_pending)
      ),
      .accessEnded(
        .init(
          conversationId: "conversation-1",
          membershipIntervalId: "interval-1",
          userDid: did,
          deviceId: "device-1",
          terminalSeq: 10
        )
      ),
      .watermark(.init(issuedAt: expiry)),
    ]

    var websocketDispatched = 0
    let websocketDurableActions = MLSCanonicalDurableEventActions(
      onConversationChanged: { _ in websocketDispatched += 1 },
      onConversationClosed: { _ in websocketDispatched += 1 },
      onMessageAvailable: { _, _, _ in websocketDispatched += 1 },
      onWelcomeAvailable: { _ in websocketDispatched += 1 },
      onWelcomeDisposition: { _ in websocketDispatched += 1 },
      onResetRequested: { _ in websocketDispatched += 1 },
      onLeafRecovery: { _ in websocketDispatched += 1 },
      onLeaveRequest: { _ in websocketDispatched += 1 },
      onAccessEnded: { _ in websocketDispatched += 1 },
      onWatermark: { _ in websocketDispatched += 1 },
      onTyping: { _ in websocketDispatched += 1 }
    )
    let websocketHandler = MLSWebSocketManager.EventHandler(
      onCanonicalDurableEventActions: websocketDurableActions
    )
    let websocketActions = MLSWebSocketManager.canonicalDurableEventActions(for: websocketHandler)
    for event in events {
      try await websocketActions.dispatch(event)
    }
    XCTAssertEqual(websocketDispatched, events.count)

    var sseDispatched = 0
    let sseDurableActions = MLSCanonicalDurableEventActions(
      onConversationChanged: { _ in sseDispatched += 1 },
      onConversationClosed: { _ in sseDispatched += 1 },
      onMessageAvailable: { _, _, _ in sseDispatched += 1 },
      onWelcomeAvailable: { _ in sseDispatched += 1 },
      onWelcomeDisposition: { _ in sseDispatched += 1 },
      onResetRequested: { _ in sseDispatched += 1 },
      onLeafRecovery: { _ in sseDispatched += 1 },
      onLeaveRequest: { _ in sseDispatched += 1 },
      onAccessEnded: { _ in sseDispatched += 1 },
      onWatermark: { _ in sseDispatched += 1 },
      onTyping: { _ in sseDispatched += 1 }
    )
    let sseHandler = MLSEventStreamManager.EventHandler(
      onCanonicalDurableEventActions: sseDurableActions
    )
    let sseActions = MLSEventStreamManager.canonicalDurableEventActions(for: sseHandler)
    for event in events {
      try await sseActions.dispatch(event)
    }
    XCTAssertEqual(sseDispatched, events.count)

    let missingActions = MLSWebSocketManager.canonicalDurableEventActions(
      for: MLSWebSocketManager.EventHandler()
    )
    do {
      try await missingActions.dispatch(.welcomeAvailable(.init(
        welcomeId: "welcome-1",
        conversationId: "conversation-1"
      )))
      XCTFail("a required canonical action must not be treated as a successful no-op")
    } catch let error as MLSCanonicalActionMissingError {
      XCTAssertEqual(error, .welcomeAvailable)
    }

    var legacyMessageFallbackCalled = false
    let missingMessageActions = MLSWebSocketManager.canonicalDurableEventActions(
      for: MLSWebSocketManager.EventHandler()
    )
    do {
      try await missingMessageActions.dispatch(.messageAvailable(
        .init(conversationId: "conversation-1", seq: entry.sequenceNumber),
        cursor: "cursor-1",
        messages: [entry]
      ))
      XCTFail("canonical message handling must require a throwing typed action")
    } catch let error as MLSCanonicalActionMissingError {
      XCTAssertEqual(error, .messageAvailable)
    }
    XCTAssertFalse(legacyMessageFallbackCalled)
  }

  func testDurableHandlerFailureDoesNotAdvanceCursor() async {
    let envelope = BlueCatbirdChatDefs.EventEnvelope(
      previousCursor: "cursor-0",
      cursor: "cursor-1",
      payload: .blueCatbirdChatDefsConversationChangedEvent(
        .init(conversationId: "conversation-1")
      ),
      createdAt: expiry
    )
    var savedCursors: [String] = []

    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      .blueCatbirdChatDefsEventEnvelope(envelope),
      subscriptionKey: "conversation-1",
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in throw DurableEventTestError.handlerFailed },
      saveCursor: { savedCursors.append($0) }
    )

    guard case .reconnect(let error) = result else {
      return XCTFail("handler failure must request a retry without advancing")
    }
    XCTAssertEqual(error as? DurableEventTestError, .handlerFailed)
    XCTAssertTrue(savedCursors.isEmpty)
  }

  func testCanonicalStreamLoopStopsFailedStreamAndReplaysFromUnadvancedCursor() async throws {
    let unknown = BlueCatbirdChatSubscribeEvents.Message.unexpected(.object([:]))
    let valid = BlueCatbirdChatSubscribeEvents.Message.blueCatbirdChatDefsEventEnvelope(
      .init(
        previousCursor: "cursor-0",
        cursor: "cursor-1",
        payload: .blueCatbirdChatDefsConversationChangedEvent(
          .init(conversationId: "conversation-1")
        ),
        createdAt: expiry
      )
    )
    let firstStream = AsyncThrowingStream<BlueCatbirdChatSubscribeEvents.Message, Error> {
      continuation in
      continuation.yield(unknown)
      continuation.yield(valid)
      continuation.finish()
    }
    var handled = 0
    var savedCursors: [String] = []
    let firstOutcome = try await MLSCanonicalTransportAdapter.consumeCanonicalStream(
      firstStream,
      shouldStop: { false },
      handle: { message in
        await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
          message,
          subscriptionKey: "conversation-1",
          expectedPreviousCursor: "cursor-0",
          loadEntries: { _, _ in [] },
          onDurableEvent: { _ in handled += 1 },
          saveCursor: { savedCursors.append($0) }
        )
      }
    )

    guard case let .reconnect(_, eventCount: firstEventCount) = firstOutcome else {
      return XCTFail("failed stream must terminate at the first reconnect request")
    }
    XCTAssertEqual(firstEventCount, 1)
    XCTAssertEqual(handled, 0)
    XCTAssertTrue(savedCursors.isEmpty)

    let replayStream = AsyncThrowingStream<BlueCatbirdChatSubscribeEvents.Message, Error> {
      continuation in
      continuation.yield(valid)
      continuation.finish()
    }
    let replayOutcome = try await MLSCanonicalTransportAdapter.consumeCanonicalStream(
      replayStream,
      shouldStop: { false },
      handle: { message in
        await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
          message,
          subscriptionKey: "conversation-1",
          expectedPreviousCursor: "cursor-0",
          loadEntries: { _, _ in [] },
          onDurableEvent: { _ in handled += 1 },
          saveCursor: { savedCursors.append($0) }
        )
      }
    )

    guard case let .ended(eventCount: replayEventCount) = replayOutcome else {
      return XCTFail("replayed stream must finish after the durable cursor advances")
    }
    XCTAssertEqual(replayEventCount, 1)
    XCTAssertEqual(handled, 1)
    XCTAssertEqual(savedCursors, ["cursor-1"])
  }

  func testDurableEnvelopePreviousCursorMustMatchFence() async {
    let envelope = BlueCatbirdChatDefs.EventEnvelope(
      previousCursor: "cursor-before-fence",
      cursor: "cursor-1",
      payload: .blueCatbirdChatDefsConversationChangedEvent(
        .init(conversationId: "conversation-1")
      ),
      createdAt: expiry
    )
    var savedCursors: [String] = []

    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      .blueCatbirdChatDefsEventEnvelope(envelope),
      subscriptionKey: "conversation-1",
      expectedPreviousCursor: "cursor-0",
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in XCTFail("fence mismatch must not dispatch") },
      saveCursor: { cursor in
        savedCursors.append(cursor)
      }
    )

    guard case .reconnect(let error) = result else {
      return XCTFail("previous cursor mismatch must reconnect")
    }
    XCTAssertTrue(error is MLSCanonicalCursorError)
    XCTAssertTrue(savedCursors.isEmpty)
  }

  func testDurableCursorPersistenceFailureRequestsReconnect() async {
    let envelope = BlueCatbirdChatDefs.EventEnvelope(
      previousCursor: "cursor-0",
      cursor: "cursor-1",
      payload: .blueCatbirdChatDefsConversationChangedEvent(
        .init(conversationId: "conversation-1")
      ),
      createdAt: expiry
    )

    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      .blueCatbirdChatDefsEventEnvelope(envelope),
      subscriptionKey: "conversation-1",
      expectedPreviousCursor: "cursor-0",
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in },
      saveCursor: { _ in
        throw DurableEventTestError.persistenceFailed
      }
    )

    guard case .reconnect(let error) = result else {
      return XCTFail("cursor persistence failure must reconnect")
    }
    XCTAssertEqual(error as? DurableEventTestError, .persistenceFailed)
  }

  @MainActor
  func testCursorStoreFailureIsPropagatedBeforeInMemoryCommit() async {
    let store = RecordingCursorStore(failure: .persistenceFailed)
    var inMemoryCursors: [String] = []

    do {
      try await MLSCanonicalTransportAdapter.persistCanonicalCursor(
        "cursor-1",
        for: "conversation-1",
        store: store
      )
      inMemoryCursors.append("cursor-1")
      XCTFail("cursor-store failure must propagate before the in-memory fence changes")
    } catch {
      XCTAssertEqual(error as? DurableEventTestError, .persistenceFailed)
    }
    XCTAssertTrue(inMemoryCursors.isEmpty)
    XCTAssertTrue(store.savedCursors.isEmpty)

    store.failure = nil
    try? await MLSCanonicalTransportAdapter.persistCanonicalCursor(
      "cursor-1",
      for: "conversation-1",
      store: store
    )
    XCTAssertEqual(store.savedCursors, ["cursor-1"])
  }

  func testMessageAvailableMustProjectAdvertisedSequenceBeforeAdvance() async {
    let envelope = BlueCatbirdChatDefs.EventEnvelope(
      previousCursor: "cursor-0",
      cursor: "cursor-1",
      payload: .blueCatbirdChatDefsMessageAvailableEvent(
        .init(conversationId: "conversation-1", seq: 4)
      ),
      createdAt: expiry
    )
    var savedCursors: [String] = []

    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      .blueCatbirdChatDefsEventEnvelope(envelope),
      subscriptionKey: "conversation-1",
      expectedPreviousCursor: "cursor-0",
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in XCTFail("unprojected messageAvailable must not dispatch") },
      saveCursor: { savedCursors.append($0) }
    )

    guard case .reconnect(let error) = result else {
      return XCTFail("missing advertised sequence must reconnect")
    }
    XCTAssertTrue(error is MLSCanonicalMessageAvailabilityError)
    XCTAssertTrue(savedCursors.isEmpty)
  }

  func testMissingRequiredDurableActionRequestsReconnect() async {
    let actions = MLSCanonicalDurableEventActions(
      onConversationChanged: nil,
      onConversationClosed: nil,
      onMessageAvailable: nil,
      onWelcomeAvailable: nil,
      onWelcomeDisposition: nil,
      onResetRequested: nil,
      onLeafRecovery: nil,
      onLeaveRequest: nil,
      onAccessEnded: nil,
      onWatermark: nil,
      onTyping: nil
    )
    let envelope = BlueCatbirdChatDefs.EventEnvelope(
      previousCursor: "cursor-0",
      cursor: "cursor-1",
      payload: .blueCatbirdChatDefsConversationChangedEvent(
        .init(conversationId: "conversation-1")
      ),
      createdAt: expiry
    )
    var savedCursors: [String] = []

    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      .blueCatbirdChatDefsEventEnvelope(envelope),
      subscriptionKey: "conversation-1",
      expectedPreviousCursor: "cursor-0",
      loadEntries: { _, _ in [] },
      onDurableEvent: { event in
        try await actions.dispatch(event)
      },
      saveCursor: { savedCursors.append($0) }
    )

    guard case .reconnect(let error) = result else {
      return XCTFail("missing required action must reconnect")
    }
    XCTAssertTrue(error is MLSCanonicalActionMissingError)
    XCTAssertTrue(savedCursors.isEmpty)
  }

  func testUnknownDurablePayloadDoesNotAdvanceCursorAndRequestsReconnect() async {
    let envelope = BlueCatbirdChatDefs.EventEnvelope(
      previousCursor: "cursor-0",
      cursor: "cursor-unknown",
      payload: .unexpected(.object([:])),
      createdAt: expiry
    )
    var savedCursors: [String] = []

    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      .blueCatbirdChatDefsEventEnvelope(envelope),
      subscriptionKey: "conversation-1",
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in XCTFail("unknown payload must never dispatch") },
      saveCursor: { savedCursors.append($0) }
    )

    guard case .reconnect(let error) = result else {
      return XCTFail("unknown durable payload must request reconnect")
    }
    XCTAssertTrue(error is MLSUnsupportedDurableEventError)
    XCTAssertTrue(savedCursors.isEmpty)
  }

  func testLegacyCompatibilityHandlerRejectsUnhandledDurableVariant() async {
    let envelope = BlueCatbirdChatDefs.EventEnvelope(
      previousCursor: "cursor-0",
      cursor: "cursor-1",
      payload: .blueCatbirdChatDefsConversationChangedEvent(
        .init(conversationId: "conversation-1")
      ),
      createdAt: expiry
    )
    var savedCursors: [String] = []
    var eventReceived = false

    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      .blueCatbirdChatDefsEventEnvelope(envelope),
      subscriptionKey: "conversation-1",
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in
        eventReceived = true
        throw MLSCanonicalActionMissingError.conversationChanged
      },
      saveCursor: { savedCursors.append($0) }
    )

    XCTAssertTrue(eventReceived)
    if case let .reconnect(error) = result {
      XCTAssertTrue(error is MLSCanonicalActionMissingError)
    } else {
      XCTFail("expected reconnect on unhandled error")
    }
    XCTAssertTrue(savedCursors.isEmpty)
  }

  func testKnownTypingVariantDoesNotAdvanceDurableCursor() async throws {
    let did = try DID(didString: "did:plc:typing-event-test")
    let typing = BlueCatbirdChatDefs.TypingEvent(
      typingId: "typing-1",
      conversationId: "conversation-1",
      actorDid: did,
      actorDeviceId: "device-1",
      isTyping: true,
      expiresAt: expiry
    )
    var dispatchedTyping = false
    var savedCursors: [String] = []

    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      .blueCatbirdChatDefsTypingEvent(typing),
      subscriptionKey: "conversation-1",
      loadEntries: { _, _ in [] },
      onDurableEvent: { event in
        guard case .typing = event else {
          return XCTFail("known typing variant must use the typed best-effort path")
        }
        dispatchedTyping = true
      },
      saveCursor: { savedCursors.append($0) }
    )

    XCTAssertEqual(result, .handled)
    XCTAssertTrue(dispatchedTyping)
    XCTAssertTrue(savedCursors.isEmpty)
  }

  private func makeCompleteCanonicalDurableActions() -> MLSCanonicalDurableEventActions {
    MLSCanonicalDurableEventActions(
      onConversationChanged: { _ in },
      onConversationClosed: { _ in },
      onMessageAvailable: { _, _, _ in },
      onWelcomeAvailable: { _ in },
      onWelcomeDisposition: { _ in },
      onResetRequested: { _ in },
      onLeafRecovery: { _ in },
      onLeaveRequest: { _ in },
      onAccessEnded: { _ in },
      onWatermark: { _ in },
      onTyping: { _ in }
    )
  }

  private static func prepareWithTerminalFailure(
    _ terminalFailure: MLSCanonicalSubscriptionTerminalFailure?
  ) async throws {
    var fence: MLSCanonicalSubscriptionFence?
    _ = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &fence,
      initialCursor: nil,
      terminalFailure: terminalFailure,
      fetchInventory: {
        MLSCanonicalInventorySnapshot(
          inventorySessionId: "unexpected",
          snapshotEventCursor: "unexpected",
          snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600),
          conversationItems: [],
          pendingWelcomeItems: [],
          leafRecoveryItems: []
        )
      },
      reconcile: { _ in },
      installCompletion: { _ in },
      persistFence: { _ in }
    )
  }

  nonisolated private static func makeWebSocketManager(
    serviceDID: String
  ) async -> MLSWebSocketManager {
    let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: atProtoClient,
      environment: .custom(serviceDID: serviceDID)
    )
    return MLSWebSocketManager(apiClient: apiClient)
  }

  nonisolated private static func makeEventStreamManager(
    serviceDID: String
  ) async -> MLSEventStreamManager {
    let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: atProtoClient,
      environment: .custom(serviceDID: serviceDID)
    )
    return MLSEventStreamManager(apiClient: apiClient)
  }

  private func conversationPage(
    cursor: String?,
    session: String = "session-1",
    omitContinuation: Bool = false,
    repeatedContinuation: Bool = false
  ) -> BlueCatbirdChatGetConversations.Output {
    BlueCatbirdChatGetConversations.Output(
      items: [],
      inventorySessionId: session,
      snapshotEventCursor: "event-1",
      nextPageCursor: omitContinuation
        ? nil
        : (repeatedContinuation ? "conversation-2" : (cursor == nil ? "conversation-2" : nil)),
      hasMore: repeatedContinuation || cursor == nil,
      snapshotExpiresAt: expiry
    )
  }

  private func welcomePage(
    cursor: String?,
    session: String = "session-1"
  ) -> BlueCatbirdChatGetPendingWelcomes.Output {
    BlueCatbirdChatGetPendingWelcomes.Output(
      items: [],
      inventorySessionId: session,
      snapshotEventCursor: "event-1",
      nextPageCursor: cursor == nil ? "welcome-2" : nil,
      hasMore: cursor == nil,
      snapshotExpiresAt: expiry
    )
  }

  private func recoveryPage(
    cursor: String?,
    session: String = "session-1"
  ) -> BlueCatbirdChatGetLeafRecoveryInbox.Output {
    BlueCatbirdChatGetLeafRecoveryInbox.Output(
      items: [],
      inventorySessionId: session,
      snapshotEventCursor: "event-1",
      nextPageCursor: cursor == nil ? "recovery-2" : nil,
      hasMore: cursor == nil,
      snapshotExpiresAt: expiry
    )
  }

  private func makeCoordinate(conversationId: String) -> BlueCatbirdChatDefs.ConversationCoordinates {
    BlueCatbirdChatDefs.ConversationCoordinates(
      conversationId: conversationId,
      generation: 1,
      stateVersion: 1,
      groupId: Bytes(data: Data([0x02])),
      epoch: 1,
      groupContextHash: Bytes(data: Data([0x03])),
      confirmationTag: Bytes(data: Data([0x04])),
      lifecycle: .value_active
    )
  }

  private func makeApplicationEntry() -> BlueCatbirdChatDefs.ConversationEntry {
    let did = try! DID(didString: "did:plc:durable-event-test")
    let prior = BlueCatbirdChatDefs.MlsAadPriorContext(
      conversationId: Bytes(data: Data([0x01])),
      generation: 1,
      stateVersion: 1,
      groupId: Bytes(data: Data([0x02])),
      epoch: 7,
      groupContextHash: Bytes(data: Data([0x03])),
      confirmationTag: Bytes(data: Data([0x04])),
      lifecycle: "active"
    )
    let coordinates = BlueCatbirdChatDefs.ConversationCoordinates(
      conversationId: "conversation-1",
      generation: 1,
      stateVersion: 1,
      groupId: Bytes(data: Data([0x02])),
      epoch: 7,
      groupContextHash: Bytes(data: Data([0x03])),
      confirmationTag: Bytes(data: Data([0x04])),
      lifecycle: .value_active
    )
    let body = BlueCatbirdChatDefs.ApplicationSendBody(
      signatureDomain: "blue.catbird.chat.application",
      messageId: "message-1",
      actorDid: did,
      actorDeviceId: "device-1",
      keyId: "key-1",
      authGeneration: 1,
      prior: coordinates,
      aad: BlueCatbirdChatDefs.ApplicationAad(
        protocolVersion: .value_1,
        conversationId: Bytes(data: Data([0x01])),
        generation: 1,
        messageId: Bytes(data: Data([0x06])),
        prior: prior
      ),
      applicationMessage: BlueCatbirdChatDefs.PrivateApplicationMessage(
        framing: "mls",
        contentType: "application/octet-stream",
        bytes: Bytes(data: Data([0xAA, 0xBB])),
        sha256: Bytes(data: Data([0x05]))
      ),
      blobBindings: [],
      signedAt: ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_000))
    )
    return .blueCatbirdChatDefsApplicationEntry(
      BlueCatbirdChatDefs.ApplicationEntry(
        entryId: "entry-1",
        conversationId: "conversation-1",
        seq: 4,
        signedRequest: BlueCatbirdChatDefs.SignedApplicationSend(
          body: .blueCatbirdChatDefsApplicationSendBody(body),
          signature: Bytes(data: Data([0x07]))
        ),
        receivedAt: ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_001))
      )
    )
  }
}

private enum DurableEventTestError: Error, Equatable {
  case handlerFailed
  case persistenceFailed
}

@MainActor
private final class ScopedRecordingCursorStore: MLSEventCursorStore {
  private var values: [String: String] = [:]

  func getCursor(for conversationId: String, eventType: String) throws -> String? {
    values[key(conversationId: conversationId, eventType: eventType)]
  }

  func updateCursor(for conversationId: String, cursor: String, eventType: String) throws {
    values[key(conversationId: conversationId, eventType: eventType)] = cursor
  }

  private func key(conversationId: String, eventType: String) -> String {
    "\(conversationId)|\(eventType)"
  }
}

@MainActor
private final class SingleValueCursorStore: MLSEventCursorStore {
  var value: String?

  init(value: String?) {
    self.value = value
  }

  func getCursor(for _: String, eventType _: String) throws -> String? {
    value
  }

  func updateCursor(for _: String, cursor: String, eventType _: String) throws {
    value = cursor
  }
}

@MainActor
private final class RecordingCursorStore: MLSEventCursorStore {
  var failure: DurableEventTestError?
  var readFailure: DurableEventTestError?
  var savedCursors: [String] = []
  private var values: [String: String] = [:]

  init(
    failure: DurableEventTestError? = nil,
    readFailure: DurableEventTestError? = nil
  ) {
    self.failure = failure
    self.readFailure = readFailure
  }

  func getCursor(for conversationId: String, eventType: String) throws -> String? {
    if let readFailure {
      throw readFailure
    }
    return values[key(conversationId: conversationId, eventType: eventType)]
  }

  func updateCursor(for conversationId: String, cursor: String, eventType: String) throws {
    if let failure {
      throw failure
    }
    values[key(conversationId: conversationId, eventType: eventType)] = cursor
    savedCursors.append(cursor)
  }

  private func key(conversationId: String, eventType: String) -> String {
    "\(conversationId)|\(eventType)"
  }
}
