import XCTest
import Petrel
import PetrelCatbird
@testable import CatbirdMLSCore

final class MLSCanonicalConcurrencyTests: XCTestCase {
  func testReplacedRunCannotCommitCursorAfterItsOldHandlerResumes() async throws {
    let store = await MainActor.run { CursorStore() }
    let oldRun = MLSCanonicalSubscriptionRun()
    let suspended = Suspension()
    let entered = expectation(description: "Old handler entered")
    let envelope = BlueCatbirdChatDefs.EventEnvelope(
      previousCursor: "cursor-0", cursor: "cursor-old",
      payload: .blueCatbirdChatDefsConversationChangedEvent(.init(conversationId: "conversation-1")),
      createdAt: ATProtocolDate(date: Date()))
    let oldTask = Task {
      await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
        .blueCatbirdChatDefsEventEnvelope(envelope), subscriptionKey: "conversation-1",
        expectedPreviousCursor: "cursor-0", loadEntries: { _, _ in [] },
        onDurableEvent: { _ in entered.fulfill(); await suspended.wait() },
        saveCursor: { cursor in
          try await MLSCanonicalTransportAdapter.persistCanonicalCursor(cursor, for: "conversation-1", store: store, run: oldRun)
        })
    }
    await fulfillment(of: [entered], timeout: 1)
    // The managers invalidate the old token when replacing its subscription.
    oldRun.invalidate()
    let replacementRun = MLSCanonicalSubscriptionRun()
    try await MLSCanonicalTransportAdapter.persistCanonicalCursor("cursor-new-inventory", for: "conversation-1", store: store, run: replacementRun)
    await suspended.resume()
    let result = await oldTask.value
    guard case .reconnect(let error) = result else { return XCTFail("The old run must fail before cursor commit") }
    XCTAssertTrue(error is CancellationError)
    let saved = await MainActor.run { (store.cursor, store.writes) }
    XCTAssertEqual(saved.0, "cursor-new-inventory")
    XCTAssertEqual(saved.1, ["cursor-new-inventory"])
  }

  func testConcurrentEventKeepsPermitUntilProjectionAndNotificationComplete() async throws {
    let user = "did:plc:canonical-transaction-\(UUID().uuidString)"
    let state = ProjectionState()
    let suspended = Suspension()
    let firstApplying = expectation(description: "First projection suspended")
    let secondStarted = expectation(description: "Second callback started")
    let first = Task {
      try await MLSCanonicalEventTransaction.run(userDid: user, read: {
        await state.read(1)
        return 1
      }, apply: { value in
        firstApplying.fulfill()
        await suspended.wait()
        await state.publish(value)
      })
    }
    await fulfillment(of: [firstApplying], timeout: 1)
    let second = Task {
      secondStarted.fulfill()
      try await MLSCanonicalEventTransaction.run(userDid: user, read: {
        await state.read(2)
        return 2
      }, apply: { await state.publish($0) })
    }
    await fulfillment(of: [secondStarted], timeout: 1)
    // Give the second callback a scheduling turn while the first database
    // projection remains explicitly suspended, as it can during a GRDB write.
    try await Task.sleep(for: .milliseconds(30))
    let pendingReads = await state.reads
    XCTAssertEqual(pendingReads, [1], "A later event must not capture a snapshot before the prior projection completes")
    await suspended.resume()
    try await first.value
    try await second.value
    let published = await state.published
    XCTAssertEqual(published, [1, 2])
  }
}

private actor Suspension {
  private var released = false
  private var continuation: CheckedContinuation<Void, Never>?
  func wait() async {
    if released { return }
    await withCheckedContinuation { continuation = $0 }
  }
  func resume() {
    released = true
    continuation?.resume()
    continuation = nil
  }
}

private actor ProjectionState {
  var reads: [Int] = []
  var published: [Int] = []
  func read(_ value: Int) { reads.append(value) }
  func publish(_ value: Int) { published.append(value) }
}

@MainActor
private final class CursorStore: MLSEventCursorStore {
  var cursor: String?
  var writes: [String] = []
  func getCursor(for conversationId: String, eventType: String) throws -> String? { cursor }
  func updateCursor(for conversationId: String, cursor: String, eventType: String) throws {
    self.cursor = cursor
    writes.append(cursor)
  }
}
