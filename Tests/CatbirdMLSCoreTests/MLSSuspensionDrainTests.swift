import Synchronization
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

  func testSuspendedResumeTrackedSigningBlocksNewDrainUntilRelease() async throws {
    let userDID = "did:plc:testuser"
    MLSClient.markSuspensionInProgress(reason: "tracked suspended-resume sign setup")
    let capability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: userDID)
    )
    let signStarted = DispatchSemaphore(value: 0)
    let releaseSign = DispatchSemaphore(value: 0)
    let drainFinished = Mutex(false)
    let closeReached = Mutex(false)

    let sign = Task {
      try await MLSClient._runTrackedSuspendedResumeFFIOperationForTesting(
        capability: capability,
        for: userDID
      ) {
        signStarted.signal()
        releaseSign.wait()
      }
    }
    XCTAssertEqual(signStarted.wait(timeout: .now() + 2), .success)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    let suspension = Task {
      let drained = await MLSClient.suspendAndDrain(
        reason: "new suspension during suspended-resume sign",
        timeout: 2
      )
      drainFinished.withLock { $0 = true }
      if drained {
        closeReached.withLock { $0 = true }
      }
      return drained
    }
    try await Task.sleep(nanoseconds: 100_000_000)

    XCTAssertFalse(drainFinished.withLock { $0 })
    XCTAssertFalse(closeReached.withLock { $0 })
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    releaseSign.signal()
    do {
      try await sign.value
      XCTFail("The superseded resume signing capability must be revoked")
    } catch is CancellationError {
      // Expected: the new suspension revokes the exact resume generation.
    }
    let drained = await suspension.value
    XCTAssertTrue(drained)
    XCTAssertTrue(drainFinished.withLock { $0 })
    XCTAssertTrue(closeReached.withLock { $0 })
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testStaleOrMismatchedResumeCapabilityCannotEnterTrackedFFI() throws {
    let userDID = "did:plc:testuser"
    MLSClient.markSuspensionInProgress(reason: "tracked capability admission setup")
    let capability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: userDID)
    )

    XCTAssertFalse(
      MLSClient._tryTrackedSuspendedResumeFFIAdmissionForTesting(
        capability: capability,
        for: "did:plc:attacker"
      )
    )
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)

    MLSClient.markSuspensionInProgress(reason: "supersede tracked capability")

    XCTAssertFalse(
      MLSClient._tryTrackedSuspendedResumeFFIAdmissionForTesting(
        capability: capability,
        for: userDID
      )
    )
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)
    XCTAssertFalse(MLSClient._tryTrackedFFIAdmissionForTesting())
  }
}
