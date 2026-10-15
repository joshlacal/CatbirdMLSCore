import Foundation
import XCTest
@testable import CatbirdMLSCore

final class LabyrinthCallbackGateTests: XCTestCase {
  func testReturnsTheOperationResult() throws {
    let gate = LabyrinthCallbackGate()
    XCTAssertEqual(try gate.run { 42 }, 42)
  }

  func testPassesTheOperationErrorThrough() {
    let gate = LabyrinthCallbackGate()
    XCTAssertThrowsError(try gate.run { () async throws -> Int in throw URLError(.timedOut) }) {
      XCTAssertEqual(($0 as? URLError)?.code, .timedOut)
    }
  }

  func testGivesUpAtTheDeadline() {
    let gate = LabyrinthCallbackGate()
    let started = Date()
    XCTAssertThrowsError(try gate.run(deadline: 0.2) { () async throws -> Int in
      try await Task.sleep(nanoseconds: 10_000_000_000)
      return 1
    }) {
      XCTAssertEqual($0 as? LabyrinthCallbackError, .timedOut(0.2))
    }
    XCTAssertLessThan(Date().timeIntervalSince(started), 5)
  }

  func testSuspendFailsTheCallInFlightAtOnce() throws {
    let gate = LabyrinthCallbackGate()
    let entered = expectation(description: "operation started")
    let finished = expectation(description: "run returned")
    let outcome = ErrorBox()
    let started = Date()
    Thread.detachNewThread {
      do {
        _ = try gate.run(deadline: 30) { () async -> Int in
          entered.fulfill()
          // Ignores cancellation, like a fetch that does not honor it.
          await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            DispatchQueue.global().asyncAfter(deadline: .now() + 10) { continuation.resume() }
          }
          return 1
        }
      } catch {
        outcome.set(error)
      }
      finished.fulfill()
    }
    wait(for: [entered], timeout: 5)
    gate.suspend()
    wait(for: [finished], timeout: 5)
    XCTAssertEqual(outcome.get() as? LabyrinthCallbackError, .suspended)
    XCTAssertLessThan(Date().timeIntervalSince(started), 8)
  }

  func testRefusesNewCallsUntilResumed() throws {
    let gate = LabyrinthCallbackGate()
    gate.suspend()
    XCTAssertThrowsError(try gate.run { 1 }) {
      XCTAssertEqual($0 as? LabyrinthCallbackError, .suspended)
    }
    gate.resume()
    XCTAssertEqual(try gate.run { 2 }, 2)
  }

  func testResettingSuspensionResumesTheSharedGate() throws {
    defer { MLSConversationManager.resetSuspensionStateForTesting() }
    LabyrinthCallbackGate.shared.suspend()
    XCTAssertThrowsError(try LabyrinthCallbackGate.shared.run { 1 })
    MLSConversationManager.resetSuspensionStateForTesting()
    XCTAssertEqual(try LabyrinthCallbackGate.shared.run { 3 }, 3)
  }
}

private final class ErrorBox: @unchecked Sendable {
  private let lock = NSLock()
  private var error: Error?

  func set(_ error: Error) {
    lock.lock()
    self.error = error
    lock.unlock()
  }

  func get() -> Error? {
    lock.lock()
    defer { lock.unlock() }
    return error
  }
}
