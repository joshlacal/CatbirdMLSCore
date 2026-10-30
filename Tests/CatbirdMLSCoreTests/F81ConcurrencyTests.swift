import XCTest
@preconcurrency import CatbirdMLS
@testable import CatbirdMLSCore

final class F81ConcurrencyTests: XCTestCase {

  private static func makeDummyDirectRequestView(conversationId: String) -> DirectRequestView {
    DirectRequestView(
      introduction: nil,
      conversationId: conversationId,
      firstMessageId: nil,
      consent: .accepted,
      crypto: .ready,
      preview: nil,
      capabilities: RequestCapabilities(
        canPreview: true,
        canAccept: true,
        canClose: true,
        canSend: true,
        canEmitReceipts: true,
        canAdminister: true
      ),
      stateVersion: 1,
      generation: 1
    )
  }

  // MARK: - Admission Gate Tests

  func testRustAuthorityAdmissionGateBoundsConcurrency() async throws {
    let maxConcurrent = 3
    let gate = RustAuthorityAdmissionGate(maxConcurrent: maxConcurrent)
    let totalCallers = 20

    final class ConcurrencyTracker: @unchecked Sendable {
      private let lock = NSLock()
      private var current = 0
      var maxObserved = 0

      func enter() -> Int {
        lock.lock()
        defer { lock.unlock() }
        current += 1
        if current > maxObserved {
          maxObserved = current
        }
        return current
      }

      func exit() {
        lock.lock()
        defer { lock.unlock() }
        current -= 1
      }
    }

    let tracker = ConcurrencyTracker()

    try await withThrowingTaskGroup(of: Void.self) { group in
      for _ in 0..<totalCallers {
        group.addTask {
          try await gate.withPermit {
            let active = tracker.enter()
            XCTAssertLessThanOrEqual(active, maxConcurrent, "Active concurrent callers must not exceed maxConcurrent")
            try await Task.sleep(nanoseconds: 20_000_000) // 20ms to ensure overlap
            tracker.exit()
          }
        }
      }
      try await group.waitForAll()
    }

    XCTAssertEqual(tracker.maxObserved, maxConcurrent, "Should reach exactly the maximum allowed concurrent capacity")
  }

  func testRustAuthorityDefaultConstantIsFour() {
    XCTAssertEqual(
      RustAuthorityAdmissionGate.maxConcurrentRustAuthoritativeOperations,
      4,
      "Admission gate concurrency constant must be 4"
    )
  }

  // MARK: - Request Preview Coalescer Tests

  func testDirectRequestPreviewCoalescingDeduplicatesInFlightCalls() async throws {
    let coalescer = DirectRequestPreviewCoalescer()
    let conversationId = "convo-test-f81"
    let totalCallers = 15

    final class ExecutionCounter: @unchecked Sendable {
      private let lock = NSLock()
      private var count = 0

      func increment() -> Int {
        lock.lock()
        defer { lock.unlock() }
        count += 1
        return count
      }

      var value: Int {
        lock.lock()
        defer { lock.unlock() }
        return count
      }
    }

    let counter = ExecutionCounter()

    try await withThrowingTaskGroup(of: DirectRequestView.self) { group in
      for _ in 0..<totalCallers {
        group.addTask {
          try await coalescer.coalesce(conversationId: conversationId) {
            _ = counter.increment()
            try await Task.sleep(nanoseconds: 50_000_000) // 50ms in-flight window
            return Self.makeDummyDirectRequestView(conversationId: conversationId)
          }
        }
      }

      var views: [DirectRequestView] = []
      for try await view in group {
        views.append(view)
      }

      XCTAssertEqual(views.count, totalCallers)
      for view in views {
        XCTAssertEqual(view.conversationId, conversationId)
      }
    }

    XCTAssertEqual(
      counter.value,
      1,
      "Expected exactly 1 underlying execution for \(totalCallers) concurrent callers for the same conversation"
    )
  }

  func testDirectRequestPreviewDistinctConversationsExecuteIndependently() async throws {
    let coalescer = DirectRequestPreviewCoalescer()
    let convoA = "convo-A"
    let convoB = "convo-B"

    final class ExecutionRecorder: @unchecked Sendable {
      private let lock = NSLock()
      var recorded: [String] = []

      func record(_ id: String) {
        lock.lock()
        defer { lock.unlock() }
        recorded.append(id)
      }
    }

    let recorder = ExecutionRecorder()

    try await withThrowingTaskGroup(of: DirectRequestView.self) { group in
      group.addTask {
        try await coalescer.coalesce(conversationId: convoA) {
          recorder.record(convoA)
          try await Task.sleep(nanoseconds: 10_000_000)
          return Self.makeDummyDirectRequestView(conversationId: convoA)
        }
      }
      group.addTask {
        try await coalescer.coalesce(conversationId: convoB) {
          recorder.record(convoB)
          try await Task.sleep(nanoseconds: 10_000_000)
          return Self.makeDummyDirectRequestView(conversationId: convoB)
        }
      }

      for try await _ in group {}
    }

    XCTAssertEqual(recorder.recorded.count, 2, "Both distinct conversations should execute")
    XCTAssertTrue(recorder.recorded.contains(convoA))
    XCTAssertTrue(recorder.recorded.contains(convoB))
  }
}
