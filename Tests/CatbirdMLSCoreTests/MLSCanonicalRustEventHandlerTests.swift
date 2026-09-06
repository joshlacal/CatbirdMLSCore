import XCTest
import Petrel
import PetrelCatbird
@testable import CatbirdMLSCore

final class MLSCanonicalRustEventHandlerTests: XCTestCase {
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
    let json = Data("""
      {"$type":"blue.catbird.chat.defs#leaveRequestEvent","conversationId":"550e8400-e29b-41d4-a716-446655440000","leaveRequestId":"550e8400-e29b-41d4-a716-446655440001","status":"pending"}
      """.utf8)
    let event = try JSONDecoder().decode(BlueCatbirdChatDefs.LeaveRequestEvent.self, from: json)
    try await actions.onLeaveRequest?(event)
    let forwarded = try JSONDecoder().decode(BlueCatbirdChatDefs.LeaveRequestEvent.self, from: Data(XCTUnwrap(received.first).utf8))
    XCTAssertEqual(event, forwarded)
    XCTAssertTrue(received[0].contains("blue.catbird.chat.defs#leaveRequestEvent"))
  }

  func testInventoryTombstonesRetainExactTerminalSelectors() async throws {
    var received: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(prepareInventory: {}, processEvent: { received.append($0) })
    let did = try DID(didString: "did:plc:terminal-test")
    let now = ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_000))
    let removal = BlueCatbirdChatDefs.ConversationRemovalTombstone(
      conversationId: "removed-conversation", membershipIntervalId: "interval-exact",
      userDid: did, deviceId: "device-exact", terminalSeq: 74, removedAt: now)
    try await handler.onCanonicalConversationRemovalTombstone?(removal)
    let access = try JSONDecoder().decode(BlueCatbirdChatDefs.AccessEndedEvent.self, from: Data(received[0].utf8))
    XCTAssertEqual(access.conversationId, removal.conversationId)
    XCTAssertEqual(access.membershipIntervalId, removal.membershipIntervalId)
    XCTAssertEqual(access.userDid, removal.userDid)
    XCTAssertEqual(access.deviceId, removal.deviceId)
    XCTAssertEqual(access.terminalSeq, removal.terminalSeq)
    XCTAssertTrue(received[0].contains(BlueCatbirdChatDefs.AccessEndedEvent.typeIdentifier))

    let coordinates = BlueCatbirdChatDefs.ConversationCoordinates(
      conversationId: "closed-conversation", generation: 2, stateVersion: 8,
      groupId: Bytes(data: Data([1])), epoch: 7,
      groupContextHash: Bytes(data: Data([2])), confirmationTag: Bytes(data: Data([3])), lifecycle: .value_active)
    let tombstone = BlueCatbirdChatDefs.ConversationCloseTombstone(
      conversationId: "closed-conversation", conversationKind: .value_direct,
      retired: coordinates, closedByDid: did, closedByDeviceId: "device-exact", terminalSeq: 81, closedAt: now)
    try await handler.onCanonicalConversationCloseTombstone?(tombstone)
    let closed = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationClosedEvent.self, from: Data(received[1].utf8))
    XCTAssertEqual(closed.conversationId, tombstone.conversationId)
    XCTAssertEqual(closed.conversationKind, tombstone.conversationKind)
    XCTAssertEqual(closed.terminalSeq, tombstone.terminalSeq)
    XCTAssertTrue(received[1].contains(BlueCatbirdChatDefs.ConversationClosedEvent.typeIdentifier))
  }

  func testEngineFailurePropagatesBeforeDurableCursorCanAdvance() async throws {
    struct EngineFailure: Error {}
    let handler = MLSCanonicalRustEventHandler.make(prepareInventory: {}, processEvent: { _ in throw EngineFailure() })
    let event = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationChangedEvent.self, from: Data("""
      {"conversationId":"550e8400-e29b-41d4-a716-446655440000"}
      """.utf8))
    do {
      try await handler.onCanonicalDurableEventActions?.onConversationChanged?(event)
      XCTFail("A failed authoritative read must fail the durable handler")
    } catch is EngineFailure {} catch { XCTFail("Unexpected error: \(error)") }
  }

  func testHundredsOfMixedRecoveryItemsReadEachConversationOnceBeforeInstallingFence() async throws {
    let order = [conversationIDs[1], conversationIDs[0], conversationIDs[2]]
    let items = try (0..<300).map {
      try recoveryItem(variant: $0 % 5, conversationID: order[$0 % order.count], ordinal: $0)
    }
    var received: [String] = []
    var steps: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(prepareInventory: {}, processEvent: {
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
    XCTAssertEqual(payloads.compactMap { $0["$type"] as? String }, [
      BlueCatbirdChatDefs.ConversationChangedEvent.typeIdentifier,
      BlueCatbirdChatDefs.WelcomeAvailableEvent.typeIdentifier,
      BlueCatbirdChatDefs.ConversationChangedEvent.typeIdentifier,
    ])
    XCTAssertEqual(payloads.compactMap { $0["conversationId"] as? String }, [id, id, id])
  }

  func testUnknownRecoveryTailFailsBeforeAnyRecoveryActionOrFenceAdvance() async throws {
    let items = try (0..<10).map {
      try recoveryItem(variant: $0 % 5, conversationID: conversationIDs[0], ordinal: $0)
    } + [.unexpected(.object([:]))]
    var received: [String] = []
    let handler = MLSCanonicalRustEventHandler.make(
      prepareInventory: {}, processEvent: { received.append($0) })
    var fence: MLSCanonicalSubscriptionFence?
    do {
      let inventory = snapshot(
        conversations: [.init(BlueCatbirdChatDefs.ConversationInventoryState(
          state: try conversationState(conversationIDs[0])))],
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
      let handler = MLSCanonicalRustEventHandler.make(prepareInventory: {}, processEvent: {
        received.append($0)
        if received.count == failingAction { throw RecoveryFailure.authoritativeRead }
      })
      var fence: MLSCanonicalSubscriptionFence?
      do {
        try await prepare(snapshot(recoveries: items), handler: handler, fence: &fence)
        XCTFail("The failed authoritative recovery read must escape")
      } catch RecoveryFailure.authoritativeRead {}
      XCTAssertEqual(try changedConversationIDs(received), Array(conversationIDs.prefix(failingAction)))
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
      snapshot(recoveries: items), actions: MLSWebSocketManager.canonicalInventoryActions(for: handler))
    XCTAssertEqual(received, items)
  }

  func testReplacingFactoryRecoveryHandlerRestoresEveryItemInWireOrder() async throws {
    let items = try (0..<20).map {
      try recoveryItem(variant: $0 % 5, conversationID: conversationIDs[$0 % 3], ordinal: $0)
    }
    var received: [BlueCatbirdChatDefs.LeafRecoveryInboxItem] = []
    var handler = MLSCanonicalRustEventHandler.make(prepareInventory: {}, processEvent: { _ in
      XCTFail("The replaced handler must not use the factory's optimized action")
    })
    handler.onCanonicalLeafRecovery = { received.append($0) }
    try await MLSCanonicalInventoryReconciler.reconcile(
      snapshot(recoveries: items), actions: MLSWebSocketManager.canonicalInventoryActions(for: handler))
    XCTAssertEqual(received, items)
  }

  func testEmptyRecoveryInventoryNeedsNoRecoveryHandler() async throws {
    try await MLSCanonicalInventoryReconciler.reconcile(
      snapshot(recoveries: []), actions: MLSWebSocketManager.canonicalInventoryActions(for: .init()))
  }

  func testRemovingFactoryRecoveryHandlerFailsClosedInsteadOfRetainingOptimization() async throws {
    var handler = MLSCanonicalRustEventHandler.make(prepareInventory: {}, processEvent: { _ in
      XCTFail("A removed callback must not leave the optimized action active")
    })
    handler.onCanonicalLeafRecovery = nil
    var fence: MLSCanonicalSubscriptionFence?
    do {
      try await prepare(snapshot(recoveries: [
        recoveryItem(variant: 1, conversationID: conversationIDs[0], ordinal: 0),
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
    var handler = MLSEventStreamManager.EventHandler(onCanonicalLeafRecovery: { received.append($0) })
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
    .init(inventorySessionId: "recovery-test-session", snapshotEventCursor: "recovery-test-cursor",
          snapshotExpiresAt: Date(timeIntervalSinceNow: 3_600),
          conversationItems: conversations, pendingWelcomeItems: welcomes, leafRecoveryItems: recoveries)
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
      let event = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationChangedEvent.self, from: Data($0.utf8))
      XCTAssertTrue($0.contains(BlueCatbirdChatDefs.ConversationChangedEvent.typeIdentifier))
      return event.conversationId
    }
  }

  private func coordinate(_ id: String) -> [String: Any] {
    let digest = ["$bytes": Data(repeating: 1, count: 32).base64EncodedString()]
    return ["conversationId": id, "generation": 0, "stateVersion": 1, "groupId": digest,
            "epoch": 1, "groupContextHash": digest, "confirmationTag": digest, "lifecycle": "active"]
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
        "keyPackage": ["framing": "mls-key-package", "contentType": "application/mls-key-package",
                       "bytes": bytes, "sha256": bytes, "keyPackageRef": bytes],
      ]
      return try decode([
        "$type": BlueCatbirdChatDefs.LeafRecoveryView.typeIdentifier,
        "recoveryRequestId": id, "conversationId": conversationID, "requesterDid": did,
        "requesterDeviceId": device, "recoveryKind": "add", "boundCoordinate": coordinate(conversationID),
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
      "status": variant == 1 ? "pending" : variant == 2 ? "completed" : "superseded", "createdAt": date,
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
      "coordinate": ["conversationId": ["$bytes": Data(repeating: 1, count: 16).base64EncodedString()],
                     "generation": 0, "groupId": bytes, "epoch": 1,
                     "groupContextHash": bytes, "confirmationTag": bytes],
      "originTransitionId": id, "metadataVersion": 1, "nonce": bytes, "ciphertext": bytes,
      "ciphertextSha256": bytes, "ciphertextSize": 32,
      "authorProof": ["authorDid": "did:plc:recovery-test", "authorDeviceId": id,
                      "authorKeyId": "key-1", "signaturePublicKey": bytes, "authGenerationAtOrigin": 1,
                      "originTransitionId": id, "originSeq": 1, "roleAtOrigin": "admin",
                      "deviceStatusAtOrigin": "active"],
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
