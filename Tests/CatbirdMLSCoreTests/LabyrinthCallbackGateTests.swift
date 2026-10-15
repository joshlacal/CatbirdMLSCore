import CatbirdMLS
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

  func testTheSharedGateFollowsTheFFISuspensionFlag() throws {
    defer {
      MLSCoreContext.clearSuspensionFlag()
      MLSClient.clearSuspensionFlag(reason: "gate test cleanup")
      MLSConversationManager.resetSuspensionStateForTesting()
    }
    MLSClient.markSuspensionInProgress(reason: "gate test")
    XCTAssertThrowsError(try LabyrinthCallbackGate.shared.run { 1 }) {
      XCTAssertEqual($0 as? LabyrinthCallbackError, .suspended)
    }
    // A BGTask clears only the FFI flag before it runs MLS work.
    MLSClient.clearSuspensionFlag(reason: "gate test BGTask")
    XCTAssertEqual(try LabyrinthCallbackGate.shared.run { 4 }, 4)
  }

  func testTheSharedGateEndsWithTheFlagWhenMarkAndClearRace() {
    defer {
      MLSCoreContext.clearSuspensionFlag()
      MLSClient.clearSuspensionFlag(reason: "gate race test cleanup")
      MLSConversationManager.resetSuspensionStateForTesting()
    }
    // The flag write that mark, clear and the emergency close share, without
    // their flight-recorder and logging side effects.
    for round in 0..<20000 {
      DispatchQueue.concurrentPerform(iterations: 2) { index in
        MLSClient.writeSuspensionFlag(index == 0, cacheInvalidated: nil).forEach { $0() }
      }
      let admitted = (try? LabyrinthCallbackGate.shared.run { true }) ?? false
      XCTAssertEqual(admitted, !MLSClient.isSuspensionInProgress, "round \(round)")
    }
  }

  func testASuspensionHoldOutlastsResume() throws {
    let gate = LabyrinthCallbackGate()
    gate.beginSuspensionHold()
    gate.resume()
    XCTAssertThrowsError(try gate.run { 1 }) {
      XCTAssertEqual($0 as? LabyrinthCallbackError, .suspended)
    }
    gate.endSuspensionHold()
    XCTAssertEqual(try gate.run { 2 }, 2)
  }

  func testRuntimeCallsThatTakeRustsLabyrinthLockHoldTheGate() throws {
    defer { MLSConversationManager.resetSuspensionStateForTesting() }
    let bridge = GateProbeBridge()
    let runtime = MLSOrchestratorRuntime(userDID: "did:plc:gate", mode: .rustFull, bridge: bridge)
    _ = try runtime.prepareForSuspend(reason: "gate test")
    runtime.shutdown()
    try runtime.emergencyClose(reason: "gate test")
    XCTAssertEqual(
      bridge.admittedDuring,
      ["prepareForSuspend": false, "shutdown": false, "emergencyClose": false]
    )
    XCTAssertEqual(try LabyrinthCallbackGate.shared.run { 1 }, 1)
  }

  func testABackgroundTaskClearDuringSuspensionWaitsForTheHold() throws {
    defer {
      MLSCoreContext.clearSuspensionFlag()
      MLSClient.clearSuspensionFlag(reason: "gate hold test cleanup")
      MLSConversationManager.resetSuspensionStateForTesting()
    }
    LabyrinthCallbackGate.shared.beginSuspensionHold()
    MLSClient.markSuspensionInProgress(reason: "gate hold test")
    MLSClient.clearSuspensionFlag(reason: "gate hold test BGTask")
    XCTAssertThrowsError(try LabyrinthCallbackGate.shared.run { 1 })
    LabyrinthCallbackGate.shared.endSuspensionHold()
    XCTAssertEqual(try LabyrinthCallbackGate.shared.run { 2 }, 2)
  }
}

/// Records whether the shared gate admits a callback while each lifecycle
/// call that takes Rust's Labyrinth lock is running.
private final class GateProbeBridge: OrchestratorBridge {
  private(set) var admittedDuring: [String: Bool] = [:]

  init() {
    super.init(noPointer: .init())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    super.init(unsafeFromRawPointer: pointer)
  }

  private func probe(_ call: String) {
    admittedDuring[call] = (try? LabyrinthCallbackGate.shared.run { true }) ?? false
  }

  override func prepareForSuspend(reason: String, deadlineMs: UInt64) throws -> FfiSuspendResult {
    probe("prepareForSuspend")
    return FfiSuspendResult(acceptingNewWork: false, interruptedContexts: 0)
  }

  override func shutdown() {
    probe("shutdown")
  }

  override func emergencyClose(reason: String) throws {
    probe("emergencyClose")
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
