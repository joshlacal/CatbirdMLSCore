import Petrel
import PetrelCatbird
import XCTest

@testable import CatbirdMLSCore

final class MLSCanonicalRustEventHandlerTests: XCTestCase {
  func testAggregateDispatchesOneNativeBatchAndPreservesRecoveryOrder() async throws {
    var received: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { received.append($0) })
    let items = try [
      recoveryItem(variant: 0, conversationID: conversationIDs[1], ordinal: 0),
      recoveryItem(variant: 1, conversationID: conversationIDs[0], ordinal: 1),
    ]
    try await MLSWebSocketManager.reconcileCanonicalInventory(
      snapshot(recoveries: items), with: handler)
    XCTAssertEqual(received.count, 1)
    let envelope = try XCTUnwrap(
      JSONSerialization.jsonObject(with: Data(received[0].utf8)) as? [String: Any])
    XCTAssertEqual(envelope["$type"] as? String, "blue.catbird.internal#inventoryHintBatch")
    let events = try XCTUnwrap(envelope["events"] as? [[String: Any]])
    XCTAssertEqual(
      events.compactMap { $0["conversationId"] as? String },
      [conversationIDs[1], conversationIDs[0]])
  }

  func testBatchFailureDoesNotInstallFence() async throws {
    struct Failed: Error {}
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { _ in throw Failed() })
    var fence: MLSCanonicalSubscriptionFence?
    do {
      _ = try await MLSCanonicalSubscriptionCoordinator.prepare(
        fence: &fence, initialCursor: nil, fetchInventory: { self.snapshot(recoveries: []) },
        reconcile: { try await MLSWebSocketManager.reconcileCanonicalInventory($0, with: handler) },
        installCompletion: { _ in XCTFail("Failed batch cannot install completion") },
        persistFence: { _ in XCTFail("Failed batch cannot advance cursor") })
      XCTFail("Expected batch failure")
    } catch is Failed {}
    XCTAssertNil(fence)
  }

  func testInventoryOverrideDisablesBatchOptimization() async throws {
    var received = 0
    var replaced = 0
    var handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { _ in received += 1 })
    handler.onCanonicalLeafRecovery = { _ in replaced += 1 }
    let item = try recoveryItem(variant: 0, conversationID: conversationIDs[0], ordinal: 0)
    try await MLSWebSocketManager.reconcileCanonicalInventory(
      snapshot(recoveries: [item]), with: handler)
    XCTAssertEqual(received, 0)
    XCTAssertEqual(replaced, 1)
  }

  func testMultiPhaseBatchPreservesDeterministicOrderingAndExactTerminalSelectors() async throws {
    var received: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { received.append($0) })
    let did = try DID(didString: "did:plc:batch-test")
    let now = ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_000))
    let state = try conversationState(conversationIDs[0])
    let removal = BlueCatbirdChatDefs.ConversationRemovalTombstone(
      conversationId: conversationIDs[1], membershipIntervalId: "interval-batch-1",
      userDid: did, deviceId: "device-batch-1", terminalSeq: 42, removedAt: now)
    let coordinates = BlueCatbirdChatDefs.ConversationCoordinates(
      conversationId: conversationIDs[2], generation: 1, stateVersion: 2,
      groupId: Bytes(data: Data([1])), epoch: 3,
      groupContextHash: Bytes(data: Data([2])), confirmationTag: Bytes(data: Data([3])),
      lifecycle: .value_active)
    let tombstone = BlueCatbirdChatDefs.ConversationCloseTombstone(
      conversationId: conversationIDs[2], conversationKind: .value_direct,
      retired: coordinates, closedByDid: did, closedByDeviceId: "device-batch-1", terminalSeq: 99,
      closedAt: now)
    let welcome = try welcomeView(conversationIDs[0])
    let recoveries = try [
      recoveryItem(variant: 0, conversationID: conversationIDs[2], ordinal: 10),
      recoveryItem(variant: 1, conversationID: conversationIDs[1], ordinal: 11),
      recoveryItem(variant: 2, conversationID: conversationIDs[2], ordinal: 12),
    ]
    let inventory = snapshot(
      conversations: [
        .blueCatbirdChatDefsConversationInventoryState(.init(state: state)),
        .blueCatbirdChatDefsConversationRemovalTombstone(removal),
        .blueCatbirdChatDefsConversationCloseTombstone(tombstone),
      ],
      welcomes: [welcome],
      recoveries: recoveries
    )
    try await MLSWebSocketManager.reconcileCanonicalInventory(inventory, with: handler)
    XCTAssertEqual(received.count, 1)
    let envelope = try XCTUnwrap(
      JSONSerialization.jsonObject(with: Data(received[0].utf8)) as? [String: Any])
    XCTAssertEqual(envelope["$type"] as? String, "blue.catbird.internal#inventoryHintBatch")
    let events = try XCTUnwrap(envelope["events"] as? [[String: Any]])
    XCTAssertEqual(events.count, 6)

    // Phase 1: Conversation items in snapshot order
    XCTAssertEqual(
      events[0]["$type"] as? String, BlueCatbirdChatDefs.ConversationChangedEvent.typeIdentifier)
    XCTAssertEqual(events[0]["conversationId"] as? String, conversationIDs[0])

    XCTAssertEqual(
      events[1]["$type"] as? String, BlueCatbirdChatDefs.AccessEndedEvent.typeIdentifier)
    XCTAssertEqual(events[1]["conversationId"] as? String, conversationIDs[1])
    XCTAssertEqual(events[1]["membershipIntervalId"] as? String, "interval-batch-1")
    XCTAssertEqual(events[1]["userDid"] as? String, did.didString())
    XCTAssertEqual(events[1]["deviceId"] as? String, "device-batch-1")
    XCTAssertEqual(events[1]["terminalSeq"] as? Int, 42)

    XCTAssertEqual(
      events[2]["$type"] as? String, BlueCatbirdChatDefs.ConversationClosedEvent.typeIdentifier)
    XCTAssertEqual(events[2]["conversationId"] as? String, conversationIDs[2])
    XCTAssertEqual(events[2]["conversationKind"] as? String, "direct")
    XCTAssertEqual(events[2]["terminalSeq"] as? Int, 99)

    // Phase 2: Pending welcomes in snapshot order
    XCTAssertEqual(
      events[3]["$type"] as? String, BlueCatbirdChatDefs.WelcomeAvailableEvent.typeIdentifier)
    XCTAssertEqual(events[3]["conversationId"] as? String, conversationIDs[0])
    XCTAssertEqual(events[3]["welcomeId"] as? String, conversationIDs[0])

    // Phase 3: Recovery items deduplicated in first-seen order
    XCTAssertEqual(
      events[4]["$type"] as? String, BlueCatbirdChatDefs.ConversationChangedEvent.typeIdentifier)
    XCTAssertEqual(events[4]["conversationId"] as? String, conversationIDs[2])

    XCTAssertEqual(
      events[5]["$type"] as? String, BlueCatbirdChatDefs.ConversationChangedEvent.typeIdentifier)
    XCTAssertEqual(events[5]["conversationId"] as? String, conversationIDs[1])
  }

  func testBatchRejectsExceedingTenThousandHints() async throws {
    var received: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { received.append($0) })
    let recoveries = try (0...10_000).map {
      try recoveryItem(
        variant: 0, conversationID: String(format: "550e8400-e29b-41d4-a716-%012d", $0), ordinal: $0
      )
    }
    let inventory = snapshot(recoveries: recoveries)
    do {
      try await MLSWebSocketManager.reconcileCanonicalInventory(inventory, with: handler)
      XCTFail("Expected exceeding 10000 hints to throw")
    } catch let error as MLSConversationError {
      guard case .operationFailed(let message) = error else {
        return XCTFail("Expected operationFailed error, got \(error)")
      }
      XCTAssertTrue(message.contains("10000 hints"))
    }
    XCTAssertTrue(received.isEmpty, "No batch may be dispatched when hint limit is exceeded")
  }

  func testBatchRejectsExceedingSixteenMegabytes() async throws {
    var received: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { received.append($0) })
    let did = try DID(didString: "did:plc:oversized-test")
    let now = ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_000))
    let oversizedInterval = String(repeating: "o", count: 17 * 1024 * 1024)
    let removal = BlueCatbirdChatDefs.ConversationRemovalTombstone(
      conversationId: conversationIDs[0], membershipIntervalId: oversizedInterval,
      userDid: did, deviceId: "device-oversized", terminalSeq: 1, removedAt: now)
    let inventory = snapshot(
      conversations: [.blueCatbirdChatDefsConversationRemovalTombstone(removal)],
      recoveries: []
    )
    do {
      try await MLSWebSocketManager.reconcileCanonicalInventory(inventory, with: handler)
      XCTFail("Expected exceeding 16MiB to throw")
    } catch let error as MLSConversationError {
      guard case .operationFailed(let message) = error else {
        return XCTFail("Expected operationFailed error, got \(error)")
      }
      XCTAssertTrue(message.contains("16MiB"))
    }
    XCTAssertTrue(received.isEmpty, "No batch may be dispatched when size limit is exceeded")
  }

  func testEveryPublicInventoryCallbackOverrideDisablesBatch() async throws {
    let did = try DID(didString: "did:plc:override-test")
    let now = ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_000))

    // 1. onCanonicalConversationInventoryState
    do {
      var receivedBatches = 0
      var receivedStates = 0
      var handler = MLSCanonicalRustEventHandler.make(
        prepareInventory: {}, processEvent: { _ in receivedBatches += 1 })
      handler.onCanonicalConversationInventoryState = { _ in receivedStates += 1 }
      let state = try conversationState(conversationIDs[0])
      let inv = snapshot(
        conversations: [.blueCatbirdChatDefsConversationInventoryState(.init(state: state))],
        recoveries: [])
      try await MLSWebSocketManager.reconcileCanonicalInventory(inv, with: handler)
      XCTAssertEqual(receivedBatches, 0)
      XCTAssertEqual(receivedStates, 1)
    }

    // 2. onCanonicalConversationRemovalTombstone
    do {
      var receivedBatches = 0
      var receivedRemovals = 0
      var handler = MLSCanonicalRustEventHandler.make(
        prepareInventory: {}, processEvent: { _ in receivedBatches += 1 })
      handler.onCanonicalConversationRemovalTombstone = { _ in receivedRemovals += 1 }
      let removal = BlueCatbirdChatDefs.ConversationRemovalTombstone(
        conversationId: conversationIDs[0], membershipIntervalId: "i1",
        userDid: did, deviceId: "d1", terminalSeq: 1, removedAt: now)
      let inv = snapshot(
        conversations: [.blueCatbirdChatDefsConversationRemovalTombstone(removal)], recoveries: [])
      try await MLSWebSocketManager.reconcileCanonicalInventory(inv, with: handler)
      XCTAssertEqual(receivedBatches, 0)
      XCTAssertEqual(receivedRemovals, 1)
    }

    // 3. onCanonicalConversationCloseTombstone
    do {
      var receivedBatches = 0
      var receivedCloses = 0
      var handler = MLSCanonicalRustEventHandler.make(
        prepareInventory: {}, processEvent: { _ in receivedBatches += 1 })
      handler.onCanonicalConversationCloseTombstone = { _ in receivedCloses += 1 }
      let coords = BlueCatbirdChatDefs.ConversationCoordinates(
        conversationId: conversationIDs[0], generation: 1, stateVersion: 1,
        groupId: Bytes(data: Data([1])), epoch: 1,
        groupContextHash: Bytes(data: Data([2])), confirmationTag: Bytes(data: Data([3])),
        lifecycle: .value_active)
      let tombstone = BlueCatbirdChatDefs.ConversationCloseTombstone(
        conversationId: conversationIDs[0], conversationKind: .value_direct,
        retired: coords, closedByDid: did, closedByDeviceId: "d1", terminalSeq: 1, closedAt: now)
      let inv = snapshot(
        conversations: [.blueCatbirdChatDefsConversationCloseTombstone(tombstone)], recoveries: [])
      try await MLSWebSocketManager.reconcileCanonicalInventory(inv, with: handler)
      XCTAssertEqual(receivedBatches, 0)
      XCTAssertEqual(receivedCloses, 1)
    }

    // 4. onCanonicalPendingWelcome
    do {
      var receivedBatches = 0
      var receivedWelcomes = 0
      var handler = MLSCanonicalRustEventHandler.make(
        prepareInventory: {}, processEvent: { _ in receivedBatches += 1 })
      handler.onCanonicalPendingWelcome = { _ in receivedWelcomes += 1 }
      let welcome = try welcomeView(conversationIDs[0])
      let inv = snapshot(welcomes: [welcome], recoveries: [])
      try await MLSWebSocketManager.reconcileCanonicalInventory(inv, with: handler)
      XCTAssertEqual(receivedBatches, 0)
      XCTAssertEqual(receivedWelcomes, 1)
    }

    // 5. onCanonicalLeafRecovery
    do {
      var receivedBatches = 0
      var receivedRecoveries = 0
      var handler = MLSCanonicalRustEventHandler.make(
        prepareInventory: {}, processEvent: { _ in receivedBatches += 1 })
      handler.onCanonicalLeafRecovery = { _ in receivedRecoveries += 1 }
      let item = try recoveryItem(variant: 0, conversationID: conversationIDs[0], ordinal: 0)
      let inv = snapshot(recoveries: [item])
      try await MLSWebSocketManager.reconcileCanonicalInventory(inv, with: handler)
      XCTAssertEqual(receivedBatches, 0)
      XCTAssertEqual(receivedRecoveries, 1)
    }
  }

  func testReconciliationStartedFailureBlocksBatchAndFence() async throws {
    struct StartupFailure: Error {}
    var processCalled = false
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: { throw StartupFailure() },
      processEvent: { _ in processCalled = true }
    )
    var fence: MLSCanonicalSubscriptionFence?
    var cursorPersisted = false
    var completionInstalled = false
    do {
      _ = try await MLSCanonicalSubscriptionCoordinator.prepare(
        fence: &fence, initialCursor: nil,
        fetchInventory: { self.snapshot(recoveries: []) },
        reconcile: { try await MLSWebSocketManager.reconcileCanonicalInventory($0, with: handler) },
        installCompletion: { _ in completionInstalled = true },
        persistFence: { _ in cursorPersisted = true }
      )
      XCTFail("Expected StartupFailure")
    } catch is StartupFailure {}
    XCTAssertFalse(processCalled)
    XCTAssertFalse(cursorPersisted)
    XCTAssertFalse(completionInstalled)
    XCTAssertNil(fence)
  }

  func testEventStreamBatchingAndOverrideParityWithWebSocket() async throws {
    var received: [String] = []
    var prepared = 0
    var handler = MLSCanonicalRustEventHandler.makeEventStream(
      prepareInventory: { prepared += 1 },
      processEvent: { received.append($0) }
    )
    let item = try recoveryItem(variant: 0, conversationID: conversationIDs[0], ordinal: 0)
    let inv = snapshot(recoveries: [item])

    try await MLSEventStreamManager.reconcileCanonicalInventory(inv, with: handler)
    XCTAssertEqual(prepared, 1)
    XCTAssertEqual(received.count, 1)
    let envelope = try XCTUnwrap(
      JSONSerialization.jsonObject(with: Data(received[0].utf8)) as? [String: Any])
    XCTAssertEqual(envelope["$type"] as? String, "blue.catbird.internal#inventoryHintBatch")

    // Test override on EventStream handler disables batching
    var receivedOverrides = 0
    handler.onCanonicalLeafRecovery = { _ in receivedOverrides += 1 }
    try await MLSEventStreamManager.reconcileCanonicalInventory(inv, with: handler)
    XCTAssertEqual(received.count, 1, "No new batch should be dispatched after override")
    XCTAssertEqual(receivedOverrides, 1)

    // Test failure in EventStream prepare blocks fence
    struct StreamFailure: Error {}
    let failingHandler = MLSCanonicalRustEventHandler.makeEventStream(
      prepareInventory: { throw StreamFailure() },
      processEvent: { _ in }
    )
    var fence: MLSCanonicalSubscriptionFence?
    do {
      _ = try await MLSCanonicalSubscriptionCoordinator.prepare(
        fence: &fence, initialCursor: nil,
        fetchInventory: { inv },
        reconcile: {
          try await MLSEventStreamManager.reconcileCanonicalInventory($0, with: failingHandler)
        },
        installCompletion: { _ in XCTFail("Completion must not be installed") },
        persistFence: { _ in XCTFail("Fence must not be persisted") }
      )
      XCTFail("Expected StreamFailure")
    } catch is StreamFailure {}
    XCTAssertNil(fence)
  }

  func testCancellationAbortsBatchBeforeProcessEvent() async throws {
    let item = try recoveryItem(variant: 0, conversationID: conversationIDs[0], ordinal: 0)
    let inv = snapshot(recoveries: [item])

    let task = Task { () -> (Bool, Bool) in
      var processCalled = false
      let handler = MLSCanonicalRustEventHandler.make(
        prepareInventory: {},
        processEvent: { _ in processCalled = true }
      )
      withUnsafeCurrentTask { $0?.cancel() }
      var caughtCancellation = false
      do {
        try await MLSWebSocketManager.reconcileCanonicalInventory(inv, with: handler)
      } catch is CancellationError {
        caughtCancellation = true
      } catch {}
      return (processCalled, caughtCancellation)
    }

    let (processCalled, caughtCancellation) = await task.value
    XCTAssertFalse(processCalled, "Cancellation must abort before processEvent is called")
    XCTAssertTrue(caughtCancellation, "Expected CancellationError from production path")
  }

  func testFactoryInstallsEveryRequiredActionAndForwardsCanonicalPayload() async throws {
    var received: [String] = []
    var prepared = 0
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: { prepared += 1 }, processEvent: { received.append($0) })
    let actions = try XCTUnwrap(handler.onCanonicalDurableEventActions)
    XCTAssertTrue(actions.hasCompleteRequiredActions)
    XCTAssertNotNil(handler.onCanonicalConversationInventoryState)
    XCTAssertNotNil(handler.onCanonicalConversationRemovalTombstone)
    XCTAssertNotNil(handler.onCanonicalConversationCloseTombstone)
    XCTAssertNotNil(handler.onCanonicalPendingWelcome)
    XCTAssertNotNil(handler.onCanonicalLeafRecovery)
    try await handler.onCanonicalInventoryReconciliationStarted?()
    XCTAssertEqual(prepared, 1)
    let json = Data(
      """
      {"$type":"blue.catbird.chat.defs#leaveRequestEvent","conversationId":"550e8400-e29b-41d4-a716-446655440000","leaveRequestId":"550e8400-e29b-41d4-a716-446655440001","status":"pending"}
      """.utf8)
    let event = try JSONDecoder().decode(BlueCatbirdChatDefs.LeaveRequestEvent.self, from: json)
    try await actions.onLeaveRequest?(event)
    let forwarded = try JSONDecoder().decode(
      BlueCatbirdChatDefs.LeaveRequestEvent.self, from: Data(XCTUnwrap(received.first).utf8))
    XCTAssertEqual(event, forwarded)
    XCTAssertTrue(received[0].contains("blue.catbird.chat.defs#leaveRequestEvent"))
  }

  func testInventoryTombstonesRetainExactTerminalSelectors() async throws {
    var received: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { received.append($0) })
    let did = try DID(didString: "did:plc:terminal-test")
    let now = ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_000))
    let removal = BlueCatbirdChatDefs.ConversationRemovalTombstone(
      conversationId: "removed-conversation", membershipIntervalId: "interval-exact",
      userDid: did, deviceId: "device-exact", terminalSeq: 74, removedAt: now)
    try await handler.onCanonicalConversationRemovalTombstone?(removal)
    let access = try JSONDecoder().decode(
      BlueCatbirdChatDefs.AccessEndedEvent.self, from: Data(received[0].utf8))
    XCTAssertEqual(access.conversationId, removal.conversationId)
    XCTAssertEqual(access.membershipIntervalId, removal.membershipIntervalId)
    XCTAssertEqual(access.userDid, removal.userDid)
    XCTAssertEqual(access.deviceId, removal.deviceId)
    XCTAssertEqual(access.terminalSeq, removal.terminalSeq)
    XCTAssertTrue(received[0].contains(BlueCatbirdChatDefs.AccessEndedEvent.typeIdentifier))

    let coordinates = BlueCatbirdChatDefs.ConversationCoordinates(
      conversationId: "closed-conversation", generation: 2, stateVersion: 8,
      groupId: Bytes(data: Data([1])), epoch: 7,
      groupContextHash: Bytes(data: Data([2])), confirmationTag: Bytes(data: Data([3])),
      lifecycle: .value_active)
    let tombstone = BlueCatbirdChatDefs.ConversationCloseTombstone(
      conversationId: "closed-conversation", conversationKind: .value_direct,
      retired: coordinates, closedByDid: did, closedByDeviceId: "device-exact", terminalSeq: 81,
      closedAt: now)
    try await handler.onCanonicalConversationCloseTombstone?(tombstone)
    let closed = try JSONDecoder().decode(
      BlueCatbirdChatDefs.ConversationClosedEvent.self, from: Data(received[1].utf8))
    XCTAssertEqual(closed.conversationId, tombstone.conversationId)
    XCTAssertEqual(closed.conversationKind, tombstone.conversationKind)
    XCTAssertEqual(closed.terminalSeq, tombstone.terminalSeq)
    XCTAssertTrue(received[1].contains(BlueCatbirdChatDefs.ConversationClosedEvent.typeIdentifier))
  }

  func testEngineFailurePropagatesBeforeDurableCursorCanAdvance() async throws {
    struct EngineFailure: Error {}
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { _ in throw EngineFailure() })
    let event = try JSONDecoder().decode(
      BlueCatbirdChatDefs.ConversationChangedEvent.self,
      from: Data(
        """
        {"conversationId":"550e8400-e29b-41d4-a716-446655440000"}
        """.utf8))
    do {
      try await handler.onCanonicalDurableEventActions?.onConversationChanged?(event)
      XCTFail("A failed authoritative read must fail the durable handler")
    } catch is EngineFailure {} catch { XCTFail("Unexpected error: \(error)") }
  }

  func testHundredsOfMixedRecoveryItemsReadEachConversationOnceBeforeInstallingFence() async throws
  {
    let order = [conversationIDs[1], conversationIDs[0], conversationIDs[2]]
    let items = try (0..<300).map {
      try recoveryItem(variant: $0 % 5, conversationID: order[$0 % order.count], ordinal: $0)
    }
    var received: [String] = []
    var steps: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {},
      processEvent: {
        received.append($0)
        steps.append("changed")
      })
    var fence: MLSCanonicalSubscriptionFence?
    let inventory = snapshot(recoveries: items)
    _ = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &fence, initialCursor: nil,
      fetchInventory: { inventory },
      reconcile: {
        try await MLSCanonicalInventoryReconciler.reconcile(
          $0, actions: MLSWebSocketManager.canonicalInventoryActions(for: handler))
      },
      installCompletion: { value in
        XCTAssertTrue(value.completion.isComplete)
        steps.append("install")
      },
      persistFence: { value in
        XCTAssertEqual(value, inventory.snapshotEventCursor)
        steps.append("persist")
      }
    )

    XCTAssertEqual(try changedConversationIDs(received), order)
    XCTAssertEqual(steps, ["changed", "changed", "changed", "persist", "install"])
    XCTAssertEqual(fence?.inventorySessionId, inventory.inventorySessionId)
    XCTAssertEqual(fence?.snapshotEventCursor, inventory.snapshotEventCursor)
  }

  func testRecoveryCoalescingDoesNotSkipEarlierConversationOrWelcomePhases() async throws {
    let id = conversationIDs[0]
    let state = try conversationState(id)
    let welcome = try welcomeView(id)
    let recoveries = try (0..<5).map {
      try recoveryItem(variant: $0, conversationID: id, ordinal: $0)
    }
    let inventory = snapshot(
      conversations: [.init(BlueCatbirdChatDefs.ConversationInventoryState(state: state))],
      welcomes: [welcome], recoveries: recoveries)
    var received: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { received.append($0) })

    try await MLSCanonicalInventoryReconciler.reconcile(
      inventory, actions: MLSWebSocketManager.canonicalInventoryActions(for: handler))

    let payloads = try received.map {
      let value = try JSONSerialization.jsonObject(with: Data($0.utf8))
      return try XCTUnwrap(value as? [String: Any])
    }
    XCTAssertEqual(
      payloads.compactMap { $0["$type"] as? String },
      [
        BlueCatbirdChatDefs.ConversationChangedEvent.typeIdentifier,
        BlueCatbirdChatDefs.WelcomeAvailableEvent.typeIdentifier,
        BlueCatbirdChatDefs.ConversationChangedEvent.typeIdentifier,
      ])
    XCTAssertEqual(payloads.compactMap { $0["conversationId"] as? String }, [id, id, id])
  }

  func testUnknownRecoveryTailFailsBeforeAnyRecoveryActionOrFenceAdvance() async throws {
    let items =
      try (0..<10).map {
        try recoveryItem(variant: $0 % 5, conversationID: conversationIDs[0], ordinal: $0)
      } + [.unexpected(.object([:]))]
    var received: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { received.append($0) })
    var fence: MLSCanonicalSubscriptionFence?
    do {
      let inventory = snapshot(
        conversations: [
          .init(
            BlueCatbirdChatDefs.ConversationInventoryState(
              state: try conversationState(conversationIDs[0])))
        ],
        welcomes: [try welcomeView(conversationIDs[0])], recoveries: items)
      try await prepare(inventory, handler: handler, fence: &fence)
      XCTFail("An unsupported tail must reject the entire recovery phase")
    } catch let error as MLSCanonicalInventoryActionMissingError {
      XCTAssertEqual(error, .unsupportedLeafRecoveryItem)
    }
    XCTAssertTrue(received.isEmpty)
    XCTAssertNil(fence)
  }

  func testMalformedRecoveryTailFailsBeforeAnyRecoveryActionOrFenceAdvance() async throws {
    let malformedIDs = [
      "not-a-conversation", conversationIDs[0].uppercased(), "{\(conversationIDs[0])}",
      "550e8400-e29b-11d4-a716-446655440000",
      "550e8400-e29b-41d4-7716-446655440000",
    ]
    for malformed in malformedIDs {
      for variant in 0..<5 {
        let items = try [
          recoveryItem(variant: 0, conversationID: conversationIDs[0], ordinal: 0),
          recoveryItem(variant: 1, conversationID: conversationIDs[0], ordinal: 1),
          recoveryItem(variant: variant, conversationID: malformed, ordinal: 2),
        ]
        var received: [String] = []
        let handler = MLSCanonicalRustEventHandler.make(
          prepareInventory: {}, processEvent: { received.append($0) })
        var fence: MLSCanonicalSubscriptionFence?
        do {
          try await prepare(snapshot(recoveries: items), handler: handler, fence: &fence)
          XCTFail("Malformed conversation ID must reject variant \(variant): \(malformed)")
        } catch let error as MLSInventorySessionError {
          XCTAssertEqual(error, .invalidRecoveryConversationID)
        }
        XCTAssertTrue(received.isEmpty, "Preflight must precede all recovery actions")
        XCTAssertNil(fence)
      }
    }
  }

  func testAnyRecoveryFailureBlocksLaterActionsAndFenceAdvance() async throws {
    enum RecoveryFailure: Error { case authoritativeRead }
    let items = try (0..<30).map {
      try recoveryItem(variant: $0 % 5, conversationID: conversationIDs[$0 % 3], ordinal: $0)
    }
    for failingAction in [1, 2, 3] {
      var received: [String] = []
      let handler = MLSCanonicalRustEventHandler.make(
        prepareInventory: {},
        processEvent: {
          received.append($0)
          if received.count == failingAction { throw RecoveryFailure.authoritativeRead }
        })
      var fence: MLSCanonicalSubscriptionFence?
      do {
        try await prepare(snapshot(recoveries: items), handler: handler, fence: &fence)
        XCTFail("The failed authoritative recovery read must escape")
      } catch RecoveryFailure.authoritativeRead {}
      XCTAssertEqual(
        try changedConversationIDs(received), Array(conversationIDs.prefix(failingAction)))
      XCTAssertNil(fence)
    }
  }

  func testGenericRecoveryHandlerRetainsEveryItemInWireOrder() async throws {
    let items = try (0..<20).map {
      try recoveryItem(variant: $0 % 5, conversationID: conversationIDs[$0 % 3], ordinal: $0)
    }
    var received: [BlueCatbirdChatDefs.LeafRecoveryInboxItem] = []
    let handler = MLSWebSocketManager.EventHandler(onCanonicalLeafRecovery: { received.append($0) })
    try await MLSCanonicalInventoryReconciler.reconcile(
      snapshot(recoveries: items),
      actions: MLSWebSocketManager.canonicalInventoryActions(for: handler))
    XCTAssertEqual(received, items)
  }

  func testReplacingFactoryRecoveryHandlerRestoresEveryItemInWireOrder() async throws {
    let items = try (0..<20).map {
      try recoveryItem(variant: $0 % 5, conversationID: conversationIDs[$0 % 3], ordinal: $0)
    }
    var received: [BlueCatbirdChatDefs.LeafRecoveryInboxItem] = []
    var handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {},
      processEvent: { _ in
        XCTFail("The replaced handler must not use the factory's optimized action")
      })
    handler.onCanonicalLeafRecovery = { received.append($0) }
    try await MLSCanonicalInventoryReconciler.reconcile(
      snapshot(recoveries: items),
      actions: MLSWebSocketManager.canonicalInventoryActions(for: handler))
    XCTAssertEqual(received, items)
  }

  func testEmptyRecoveryInventoryNeedsNoRecoveryHandler() async throws {
    try await MLSCanonicalInventoryReconciler.reconcile(
      snapshot(recoveries: []), actions: MLSWebSocketManager.canonicalInventoryActions(for: .init())
    )
  }

  func testRemovingFactoryRecoveryHandlerFailsClosedInsteadOfRetainingOptimization() async throws {
    var handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {},
      processEvent: { _ in
        XCTFail("A removed callback must not leave the optimized action active")
      })
    handler.onCanonicalLeafRecovery = nil
    var fence: MLSCanonicalSubscriptionFence?
    do {
      try await prepare(
        snapshot(recoveries: [
          recoveryItem(variant: 1, conversationID: conversationIDs[0], ordinal: 0)
        ]), handler: handler, fence: &fence)
      XCTFail("Missing recovery action must fail closed")
    } catch let error as MLSCanonicalInventoryActionMissingError {
      XCTAssertEqual(error, .leafRecovery)
    }
    XCTAssertNil(fence)
  }

  func testSSERecoveryOptInAndPublicCallbackReplacementUseProductionActionTable() async throws {
    let items = try (0..<20).map {
      try recoveryItem(variant: $0 % 5, conversationID: conversationIDs[$0 % 3], ordinal: $0)
    }
    let inventory = snapshot(recoveries: items)
    var received: [BlueCatbirdChatDefs.LeafRecoveryInboxItem] = []
    var changed: [String] = []
    var handler = MLSEventStreamManager.EventHandler(onCanonicalLeafRecovery: {
      received.append($0)
    })
    try await MLSCanonicalInventoryReconciler.reconcile(
      inventory, actions: MLSEventStreamManager.canonicalInventoryActions(for: handler))
    XCTAssertEqual(received, items)

    received.removeAll()
    handler.onCanonicalRecoveryConversationState = { changed.append($0) }
    try await MLSCanonicalInventoryReconciler.reconcile(
      inventory, actions: MLSEventStreamManager.canonicalInventoryActions(for: handler))
    XCTAssertEqual(changed, conversationIDs)
    XCTAssertTrue(received.isEmpty)

    handler.onCanonicalLeafRecovery = { received.append($0) }
    try await MLSCanonicalInventoryReconciler.reconcile(
      inventory, actions: MLSEventStreamManager.canonicalInventoryActions(for: handler))
    XCTAssertEqual(received, items)
    XCTAssertEqual(changed, conversationIDs, "Replacing the callback must retire the opt-in")

    handler.onCanonicalRecoveryConversationState = { _ in
      XCTFail("Removing the public callback must also retire the opt-in")
    }
    handler.onCanonicalLeafRecovery = nil
    do {
      try await MLSCanonicalInventoryReconciler.reconcile(
        inventory, actions: MLSEventStreamManager.canonicalInventoryActions(for: handler))
      XCTFail("SSE missing recovery action must fail closed")
    } catch let error as MLSCanonicalInventoryActionMissingError {
      XCTAssertEqual(error, .leafRecovery)
    }
  }

  private let conversationIDs = [
    "550e8400-e29b-41d4-a716-446655440000",
    "550e8400-e29b-41d4-a716-446655440001",
    "550e8400-e29b-41d4-a716-446655440002",
  ]

  private func snapshot(
    conversations: [BlueCatbirdChatDefs.ConversationInventoryItem] = [],
    welcomes: [BlueCatbirdChatDefs.WelcomeView] = [],
    recoveries: [BlueCatbirdChatDefs.LeafRecoveryInboxItem]
  ) -> MLSCanonicalInventorySnapshot {
    .init(
      inventorySessionId: "recovery-test-session", snapshotEventCursor: "recovery-test-cursor",
      snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600),
      conversationItems: conversations, pendingWelcomeItems: welcomes, leafRecoveryItems: recoveries
    )
  }

  /// Failure cases exercise the same coordinator barrier used before requesting a ticket.
  private func prepare(
    _ inventory: MLSCanonicalInventorySnapshot,
    handler: MLSWebSocketManager.EventHandler,
    fence: inout MLSCanonicalSubscriptionFence?
  ) async throws {
    _ = try await MLSCanonicalSubscriptionCoordinator.prepare(
      fence: &fence, initialCursor: nil, fetchInventory: { inventory },
      reconcile: {
        try await MLSCanonicalInventoryReconciler.reconcile(
          $0, actions: MLSWebSocketManager.canonicalInventoryActions(for: handler))
      },
      installCompletion: { _ in XCTFail("Failed recovery must not install completion") },
      persistFence: { _ in XCTFail("Failed recovery must not advance the cursor") })
  }

  private func changedConversationIDs(_ payloads: [String]) throws -> [String] {
    try payloads.map {
      let event = try JSONDecoder().decode(
        BlueCatbirdChatDefs.ConversationChangedEvent.self, from: Data($0.utf8))
      XCTAssertTrue($0.contains(BlueCatbirdChatDefs.ConversationChangedEvent.typeIdentifier))
      return event.conversationId
    }
  }

  private func coordinate(_ id: String) -> [String: Any] {
    let digest = ["$bytes": Data(repeating: 1, count: 32).base64EncodedString()]
    return [
      "conversationId": id, "generation": 0, "stateVersion": 1, "groupId": digest,
      "epoch": 1, "groupContextHash": digest, "confirmationTag": digest, "lifecycle": "active",
    ]
  }

  private func decode<T: Decodable>(_ value: [String: Any], as type: T.Type = T.self) throws -> T {
    try JSONDecoder().decode(type, from: JSONSerialization.data(withJSONObject: value))
  }

  private func recoveryItem(
    variant: Int, conversationID: String, ordinal: Int
  ) throws -> BlueCatbirdChatDefs.LeafRecoveryInboxItem {
    let id = String(format: "660e8400-e29b-41d4-a716-%012d", ordinal)
    let did = "did:plc:recovery-test"
    let device = "770e8400-e29b-41d4-a716-446655440000"
    let date = "2026-09-05T12:00:00.000Z"
    let bytes = ["$bytes": Data(repeating: 1, count: 32).base64EncodedString()]
    if variant == 0 {
      let reservation: [String: Any] = [
        "recoveryRequestId": id, "conversationId": conversationID,
        "boundCoordinate": coordinate(conversationID), "requesterDid": did,
        "requesterDeviceId": device, "requesterKeyId": "key-1", "requesterAuthGeneration": 1,
        "keyPackageRef": bytes, "cipherSuite": "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
        "purpose": "leafRecovery", "status": "released", "expiresAt": date,
        "keyPackage": [
          "framing": "mls-key-package", "contentType": "application/mls-key-package",
          "bytes": bytes, "sha256": bytes, "keyPackageRef": bytes,
        ],
      ]
      return try decode([
        "$type": BlueCatbirdChatDefs.LeafRecoveryView.typeIdentifier,
        "recoveryRequestId": id, "conversationId": conversationID, "requesterDid": did,
        "requesterDeviceId": device, "recoveryKind": "add",
        "boundCoordinate": coordinate(conversationID),
        "reservation": reservation, "status": "expired", "requestedAt": date, "expiresAt": date,
      ])
    }
    let types = [
      BlueCatbirdChatDefs.RecoveryWorkPendingView.typeIdentifier,
      BlueCatbirdChatDefs.RecoveryWorkCompletedByTransitionView.typeIdentifier,
      BlueCatbirdChatDefs.RecoveryWorkSupersededByTransitionView.typeIdentifier,
      BlueCatbirdChatDefs.RecoveryWorkSupersededByRevocationView.typeIdentifier,
    ]
    var value: [String: Any] = [
      "$type": types[variant - 1], "recoveryWorkId": id, "conversationId": conversationID,
      "recipientDid": did, "recipientDeviceId": device, "sourceKind": "welcomeExpired",
      "sourceId": id, "sourceCoordinate": coordinate(conversationID),
      "status": variant == 1 ? "pending" : variant == 2 ? "completed" : "superseded",
      "createdAt": date,
    ]
    if variant > 1 {
      value[variant == 4 ? "terminalRevocationId" : "terminalTransitionId"] = id
      value["terminalAt"] = date
    }
    return try decode(value)
  }

  private func conversationState(_ id: String) throws -> BlueCatbirdChatDefs.ConversationState {
    let bytes = ["$bytes": Data(repeating: 1, count: 32).base64EncodedString()]
    let metadata: [String: Any] = [
      "coordinate": [
        "conversationId": ["$bytes": Data(repeating: 1, count: 16).base64EncodedString()],
        "generation": 0, "groupId": bytes, "epoch": 1,
        "groupContextHash": bytes, "confirmationTag": bytes,
      ],
      "originTransitionId": id, "metadataVersion": 1, "nonce": bytes, "ciphertext": bytes,
      "ciphertextSha256": bytes, "ciphertextSize": 32,
      "authorProof": [
        "authorDid": "did:plc:recovery-test", "authorDeviceId": id,
        "authorKeyId": "key-1", "signaturePublicKey": bytes, "authGenerationAtOrigin": 1,
        "originTransitionId": id, "originSeq": 1, "roleAtOrigin": "admin",
        "deviceStatusAtOrigin": "active",
      ],
    ]
    return try decode([
      "conversationKind": "group", "coordinates": coordinate(id),
      "cipherSuite": "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
      "participants": [], "leaves": [], "metadataSnapshot": metadata, "snapshotSeq": 1,
    ])
  }

  private func welcomeView(_ id: String) throws -> BlueCatbirdChatDefs.WelcomeView {
    let bytes = ["$bytes": Data(repeating: 1, count: 32).base64EncodedString()]
    return try decode([
      "welcomeId": id, "conversationId": id, "transitionSeq": 1, "coordinates": coordinate(id),
      "status": "pending", "opaqueWelcome": bytes, "sha256": bytes,
      "recipientDid": "did:plc:recovery-test", "recipientDeviceId": id,
      "provenance": ["recoveryRequestId": id, "keyPackageRef": bytes],
      "expiresAt": "2026-09-05T12:00:00.000Z",
    ])
  }
}
