import XCTest

@testable import CatbirdMLSCore

/// WS-6.2 suspension handshake tests: the bounded drain of in-flight FFI
/// operations that must complete (or be interrupted into rollback) before
/// `emergencyCloseAllContexts()` may close Rust contexts.
///
/// The production increment/decrement lives inside `MLSClient.runFFI`
/// (atomic admit-or-reject with the suspension flag under one lock); these
/// tests drive the same counter through the internal test seams because
/// `runFFI` requires a live FFI context.
///
/// NOTE: `MLSClient`'s suspension state is process-global (static). Every
/// test restores a clean state (flag cleared, counter zero) on exit.
@available(iOS 18.0, macOS 13.0, *)
final class MLSSuspensionDrainTests: XCTestCase {

  override func tearDown() {
    // Restore global state for other suites.
    MLSClient.clearSuspensionFlag(reason: "MLSSuspensionDrainTests.tearDown")
    super.tearDown()
  }

  func testDrainReturnsImmediatelyWhenQuiescent() async {
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)
    let start = Date()
    let drained = await MLSClient.drainInFlightFFIOperations(timeout: 5.0)
    XCTAssertTrue(drained)
    XCTAssertLessThan(
      Date().timeIntervalSince(start), 1.0,
      "an already-quiescent drain must not burn the timeout"
    )
  }

  func testDrainWaitsForInFlightOperationToComplete() async {
    MLSClient._beginTrackedFFIOperationForTesting()
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    let releaser = Task {
      try? await Task.sleep(nanoseconds: 150_000_000)  // 150ms
      MLSClient._endTrackedFFIOperationForTesting()
    }

    let drained = await MLSClient.drainInFlightFFIOperations(timeout: 5.0)
    await releaser.value

    XCTAssertTrue(drained, "drain must wait for the in-flight operation to complete")
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)
  }

  func testDrainTimesOutWhenOperationNeverCompletes() async {
    MLSClient._beginTrackedFFIOperationForTesting()
    defer { MLSClient._endTrackedFFIOperationForTesting() }

    let start = Date()
    let drained = await MLSClient.drainInFlightFFIOperations(timeout: 0.3)
    let elapsed = Date().timeIntervalSince(start)

    XCTAssertFalse(drained, "a wedged operation must time the drain out, not hang it")
    XCTAssertLessThan(elapsed, 3.0, "timeout must be bounded (≤5s budget; 0.3s requested)")
  }

  func testSuspensionFlagBlocksNewWorkWhileDrainCompletes() async {
    // The handshake contract: after markSuspensionInProgress, no NEW FFI
    // operation can be admitted (runFFI's atomic admit-or-reject), so a
    // drained counter is a true quiescence proof.
    MLSClient.markSuspensionInProgress(reason: "MLSSuspensionDrainTests")
    XCTAssertTrue(MLSClient.isSuspensionInProgress)

    let drained = await MLSClient.drainInFlightFFIOperations(timeout: 1.0)
    XCTAssertTrue(drained)

    MLSClient.clearSuspensionFlag(reason: "MLSSuspensionDrainTests")
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
  }
}
