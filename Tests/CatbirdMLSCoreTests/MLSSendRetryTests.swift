//
//  MLSSendRetryTests.swift
//  CatbirdMLSCoreTests
//

import XCTest
import CatbirdMLS
import GRDB
import Petrel
import PetrelCatbird

@testable import CatbirdMLSCore

private final class SafeBox<T>: @unchecked Sendable {
  private let lock = NSLock()
  private var _value: T

  init(_ value: T) {
    self._value = value
  }

  var value: T {
    get {
      lock.lock()
      defer { lock.unlock() }
      return _value
    }
    set {
      lock.lock()
      defer { lock.unlock() }
      _value = newValue
    }
  }

  func withLock<R>(_ block: (inout T) -> R) -> R {
    lock.lock()
    defer { lock.unlock() }
    return block(&_value)
  }
}

final class MLSSendRetryTests: XCTestCase {
  private let testConvoID = "550e8400-e29b-41d4-a716-446655440000"

  override func setUpWithError() throws {
    try super.setUpWithError()
    MLSDiagnostics.clear()
  }

  override func tearDownWithError() throws {
    MLSDiagnostics.clear()
    try super.tearDownWithError()
  }

  // MARK: - 1. Rate-Limited Send Retries After Server Retry-After and Then Succeeds

  func testRateLimitedSendRetriesAfterRetryAfterAndSucceeds() async throws {
    let callCount = SafeBox<Int>(0)
    let recordedDelays = SafeBox<[TimeInterval]>([])
    let reportedProgress = SafeBox<[MLSSendRetryProgress]>([])

    let result: String = try await MLSSendRetryCoordinator.performSendWithRetry(
      convoId: testConvoID,
      maxAttempts: 4,
      onRetryProgress: { progress in
        reportedProgress.withLock { $0.append(progress) }
      },
      sleep: { delay in
        recordedDelays.withLock { $0.append(delay) }
      }
    ) {
      let current = callCount.withLock { count -> Int in
        count += 1
        return count
      }
      if current == 1 {
        // Server returned 429 with retry-after hint of 3.5 seconds
        throw OrchestratorBridgeError.ServerError(
          status: 429,
          body: "{\"error\":\"RateLimited\",\"retryAfter\":3.5}"
        )
      }
      return "sent-message-id"
    }

    XCTAssertEqual(result, "sent-message-id")
    XCTAssertEqual(callCount.value, 2, "Should have succeeded on attempt 2")
    XCTAssertEqual(recordedDelays.value.count, 1)
    XCTAssertEqual(recordedDelays.value[0], 3.5, "Should have honored server's retryAfter hint exactly")
    XCTAssertEqual(reportedProgress.value.count, 1)
    XCTAssertEqual(reportedProgress.value[0].attempt, 1)
    XCTAssertEqual(reportedProgress.value[0].classification, .rateLimited(retryAfter: 3.5))

    // Verify diagnostic telemetry
    let records = MLSDiagnostics.recent()
    let sendFailed = records.first(where: { $0.event == .sendFailed })
    XCTAssertNotNil(sendFailed)
    XCTAssertEqual(sendFailed?.code, "RateLimited")
    XCTAssertEqual(sendFailed?.attempt, 1)
    XCTAssertEqual(sendFailed?.retryAfter, 3.5)

    let sendRecovered = records.first(where: { $0.event == .sendRecovered })
    XCTAssertNotNil(sendRecovered)
    XCTAssertEqual(sendRecovered?.attempt, 2)
  }

  // MARK: - 2. Stale-Coordinate Send Retries With Freshly Bound Coordinates and Succeeds Without Duplicating

  func testStaleCoordinatesSendRetriesWithFreshlyBoundCoordinatesAndSucceedsWithoutDuplicating() async throws {
    let callCount = SafeBox<Int>(0)
    let boundCoordinates = SafeBox<[(generation: Int64, stateVersion: Int64)]>([])
    let recordedDelays = SafeBox<[TimeInterval]>([])

    // Simulate authoritative point read that evolves after stale coordinate rejection
    let serverGeneration: Int64 = 1
    let serverStateVersion = SafeBox<Int64>(0)

    let result: String = try await MLSSendRetryCoordinator.performSendWithRetry(
      convoId: testConvoID,
      maxAttempts: 4,
      sleep: { delay in
        recordedDelays.withLock { $0.append(delay) }
      }
    ) {
      let currentCall = callCount.withLock { count -> Int in
        count += 1
        return count
      }
      // Bind coordinates from authoritative point read on each attempt
      let currentCoords = (generation: serverGeneration, stateVersion: serverStateVersion.value)
      boundCoordinates.withLock { $0.append(currentCoords) }

      if currentCall == 1 {
        // First send used stale coords (gen 1, sv 0) -> server rejects with StaleCoordinates
        serverStateVersion.value = 2 // Server state advanced
        throw OrchestratorBridgeError.ServerError(
          status: 400,
          body: "{\"error\":\"StaleCoordinates\",\"stateVersion\":2,\"generation\":1}"
        )
      }

      // Second send used fresh coordinates (gen 1, sv 2) -> server accepts
      return "confirmed-msg-id"
    }

    XCTAssertEqual(result, "confirmed-msg-id")
    XCTAssertEqual(callCount.value, 2)
    let coords = boundCoordinates.value
    XCTAssertEqual(coords.count, 2)
    // First attempt used stale coordinates
    XCTAssertEqual(coords[0].generation, 1)
    XCTAssertEqual(coords[0].stateVersion, 0)
    // Second attempt re-derived fresh coordinates
    XCTAssertEqual(coords[1].generation, 1)
    XCTAssertEqual(coords[1].stateVersion, 2)

    // Verify diagnostics recorded StaleCoordinates and then SendRecovered
    let records = MLSDiagnostics.recent()
    let failed = records.first(where: { $0.event == .sendFailed })
    XCTAssertNotNil(failed)
    XCTAssertEqual(failed?.code, "StaleCoordinates")

    let recovered = records.first(where: { $0.event == .sendRecovered })
    XCTAssertNotNil(recovered)
    XCTAssertEqual(recovered?.attempt, 2)
  }

  // MARK: - 3. Peer-Must-Act Failure Does NOT Spin and Surfaces a Waiting State

  func testPeerMustActFailureDoesNotSpinAndSurfacesWaitingState() async throws {
    let recipientNotReadyCallCount = SafeBox<Int>(0)

    do {
      _ = try await MLSSendRetryCoordinator.performSendWithRetry(
        convoId: testConvoID,
        maxAttempts: 4,
        sleep: { _ in XCTFail("Should not sleep for peer-must-act error") }
      ) {
        recipientNotReadyCallCount.withLock { $0 += 1 }
        throw OrchestratorBridgeError.ServerError(
          status: 400,
          body: "{\"error\":\"RecipientNotReady\",\"message\":\"did:plc:peer has no MLS leaf\"}"
        )
      }
      XCTFail("Expected RecipientNotReady error")
    } catch {
      let classification = MLSConversationLifecycleError.classifySendError(error)
      XCTAssertEqual(classification, .recipientNotReady(detail: "{\"error\":\"RecipientNotReady\",\"message\":\"did:plc:peer has no MLS leaf\"}"))
      XCTAssertTrue(classification.isWaitingForPeer)
      XCTAssertFalse(classification.isAutoRecoverable)
      XCTAssertEqual(classification.userVisibleReason, "Waiting for recipient to accept or activate their device")
    }

    XCTAssertEqual(recipientNotReadyCallCount.value, 1, "Peer-must-act failure MUST NOT spin (exactly 1 call)")

    // Verify rejoinWaiting recorded in diagnostics
    let records = MLSDiagnostics.recent()
    let waitingRecord = records.first(where: { $0.event == .rejoinWaiting })
    XCTAssertNotNil(waitingRecord, "Must record rejoinWaiting diagnostic for peer-must-act state")
    XCTAssertEqual(waitingRecord?.code, "RecipientNotReady")

    // Also test secret reuse / leaf recovery peer-must-act
    let secretReuseCallCount = SafeBox<Int>(0)
    do {
      _ = try await MLSSendRetryCoordinator.performSendWithRetry(
        convoId: testConvoID,
        maxAttempts: 4,
        sleep: { _ in XCTFail("Should not sleep for secret reuse") }
      ) {
        secretReuseCallCount.withLock { $0 += 1 }
        throw OrchestratorBridgeError.RecoveryFailed(
          message: "SecretReuse without durable envelope evidence for epoch 5"
        )
      }
      XCTFail("Expected RecoveryFailed error")
    } catch {
      let classification = MLSConversationLifecycleError.classifySendError(error)
      XCTAssertTrue(classification.isWaitingForPeer)
      XCTAssertFalse(classification.isAutoRecoverable)
      XCTAssertEqual(classification.diagnosticCode, "SecretReuse")
    }
    XCTAssertEqual(secretReuseCallCount.value, 1, "SecretReuse must NOT spin")
  }

  // MARK: - 4. Retries Are Bounded and Terminal State Carries a Reason

  func testRetriesAreBoundedAndTerminalStateCarriesReason() async throws {
    let callCount = SafeBox<Int>(0)
    let recordedDelays = SafeBox<[TimeInterval]>([])

    do {
      _ = try await MLSSendRetryCoordinator.performSendWithRetry(
        convoId: testConvoID,
        maxAttempts: 3,
        sleep: { delay in
          recordedDelays.withLock { $0.append(delay) }
        }
      ) {
        callCount.withLock { $0 += 1 }
        throw OrchestratorBridgeError.ServerError(
          status: 502,
          body: "Bad Gateway - upstream service timed out"
        )
      }
      XCTFail("Expected error after retries exhausted")
    } catch {
      let classification = MLSConversationLifecycleError.classifySendError(error)
      XCTAssertEqual(classification, .transientNetwork(status: 502, message: "Server error (502). Upstream service may be temporarily unavailable."))
      XCTAssertEqual(classification.diagnosticCode, "HTTP_502")
      XCTAssertEqual(classification.userVisibleReason, "Server temporarily unavailable (502)")
    }

    // Verify bounded execution: exactly 3 attempts made
    XCTAssertEqual(callCount.value, 3, "Execution must be bounded by maxAttempts (3)")
    XCTAssertEqual(recordedDelays.value.count, 2, "Should have slept before attempts 2 and 3")

    // Verify diagnostic history has 3 failed records, attempt 1, 2, 3
    let records = MLSDiagnostics.recent().filter { $0.event == .sendFailed }
    XCTAssertEqual(records.count, 3)
    XCTAssertEqual(records[0].attempt, 3, "Newest first")
    XCTAssertEqual(records[0].code, "HTTP_502")
  }
}
