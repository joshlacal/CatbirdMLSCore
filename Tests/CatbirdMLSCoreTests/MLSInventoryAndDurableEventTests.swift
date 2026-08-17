import Foundation
import Petrel
import PetrelCatbird
import XCTest
@testable import CatbirdMLSCore

final class MLSInventoryAndDurableEventTests: XCTestCase {
  private let expiry = ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_000))

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
        loadEntries: { _, _ in [] },
        onDurableEvent: { _ in dispatched += 1 },
        saveCursor: { savedCursors.append($0) }
      )

      XCTAssertEqual(result, .handled)
    }

    XCTAssertEqual(dispatched, payloads.count)
    XCTAssertEqual(savedCursors, (1 ... payloads.count).map { "cursor-\($0)" })
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
    omitContinuation: Bool = false
  ) -> BlueCatbirdChatGetConversations.Output {
    BlueCatbirdChatGetConversations.Output(
      items: [],
      inventorySessionId: session,
      snapshotEventCursor: "event-1",
      nextPageCursor: omitContinuation ? nil : (cursor == nil ? "conversation-2" : nil),
      hasMore: cursor == nil,
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
}

private enum DurableEventTestError: Error, Equatable {
  case handlerFailed
}
