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
    let message = try XCTUnwrap(
      MLSCanonicalTransportAdapter.projectMessageView(from: makeApplicationEntry())
    )
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
        .init(conversationId: "conversation-1", seq: message.seq),
        cursor: "cursor-1",
        messages: [message]
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
      for: MLSWebSocketManager.EventHandler(
        onMessage: { _ in legacyMessageFallbackCalled = true }
      )
    )
    do {
      try await missingMessageActions.dispatch(.messageAvailable(
        .init(conversationId: "conversation-1", seq: message.seq),
        cursor: "cursor-1",
        messages: [message]
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
    var errors: [Error] = []

    await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      .blueCatbirdChatDefsEventEnvelope(envelope),
      subscriptionKey: "conversation-1",
      loadEntries: { _, _ in [] },
      onMessage: { _ in XCTFail("unhandled durable variant must not reach legacy message callback") },
      onError: { errors.append($0) },
      saveCursor: { savedCursors.append($0) }
    )

    XCTAssertEqual(errors.count, 1)
    XCTAssertTrue(errors[0] is MLSCanonicalActionMissingError)
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
private final class RecordingCursorStore: MLSEventCursorStore {
  var failure: DurableEventTestError?
  var savedCursors: [String] = []

  init(failure: DurableEventTestError? = nil) {
    self.failure = failure
  }

  func getCursor(for _: String, eventType _: String) throws -> String? {
    savedCursors.last
  }

  func updateCursor(for _: String, cursor: String, eventType _: String) throws {
    if let failure {
      throw failure
    }
    savedCursors.append(cursor)
  }
}
