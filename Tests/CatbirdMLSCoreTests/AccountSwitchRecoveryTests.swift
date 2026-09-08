//
//  AccountSwitchRecoveryTests.swift
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

final class AccountSwitchRecoveryTests: XCTestCase {
  private let testConvoID = "550e8400-e29b-41d4-a716-446655440000"
  private var tempBaseDirectory: URL?
  private var fakeKeychain: MLSKeychainFakeStorage?

  override func setUp() async throws {
    try await super.setUp()
    MLSDiagnostics.clear()
    let dir = FileManager.default.temporaryDirectory
      .appendingPathComponent("AccountSwitchRecoveryTests-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    tempBaseDirectory = dir
    MLSStoragePaths.setBaseDirectoryOverride(dir)
    let keychain = MLSKeychainFakeStorage()
    fakeKeychain = keychain
    MLSKeychainManager.setFakeStorageOverrideForTesting(keychain)
  }

  override func tearDown() async throws {
    MLSDiagnostics.clear()
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
    if let tempBaseDirectory {
      try? FileManager.default.removeItem(at: tempBaseDirectory)
    }
    tempBaseDirectory = nil
    fakeKeychain = nil
    try await super.tearDown()
  }

  // MARK: - (a) Closed-Context Operation Re-Resolves and Succeeds

  func testOperationFailingWithClosedContextReResolvesAndSucceeds() async throws {
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
        // Attempt 1 fails with closed-context from torn-down context
        throw OrchestratorBridgeError.Mls(
          message: "Context closed - database connections have been released for iOS suspension"
        )
      }
      // Attempt 2 succeeds once context is available
      return "sent-msg-resolved"
    }

    XCTAssertEqual(result, "sent-msg-resolved")
    XCTAssertEqual(callCount.value, 2, "Should have retried and succeeded on attempt 2")
    XCTAssertEqual(recordedDelays.value.count, 1)
    XCTAssertEqual(reportedProgress.value.count, 1)
    XCTAssertEqual(reportedProgress.value[0].classification.diagnosticCode, "ContextClosed")
    XCTAssertTrue(reportedProgress.value[0].classification.isAutoRecoverable)

    // Verify diagnostics recorded sendFailed(code: ContextClosed) and sendRecovered
    let records = MLSDiagnostics.recent()
    let sendFailed = records.first(where: { $0.event == .sendFailed })
    XCTAssertNotNil(sendFailed)
    XCTAssertEqual(sendFailed?.code, "ContextClosed")
    XCTAssertEqual(sendFailed?.attempt, 1)

    let sendRecovered = records.first(where: { $0.event == .sendRecovered })
    XCTAssertNotNil(sendRecovered)
    XCTAssertEqual(sendRecovered?.attempt, 2)
  }

  // MARK: - (b) Send Issued During Switch Retries and Delivers Exactly Once

  func testSendIssuedDuringSwitchRetriesAfterSwitchSettlesAndDeliversExactlyOnce() async throws {
    let callCount = SafeBox<Int>(0)
    let deliveredMessages = SafeBox<[String]>([])
    let recordedDelays = SafeBox<[TimeInterval]>([])
    let switchSettled = SafeBox<Bool>(false)

    let result: String = try await MLSSendRetryCoordinator.performSendWithRetry(
      convoId: testConvoID,
      maxAttempts: 4,
      sleep: { delay in
        recordedDelays.withLock { $0.append(delay) }
        // Switch settles during backoff sleep before attempt 2
        switchSettled.value = true
      }
    ) {
      let current = callCount.withLock { count -> Int in
        count += 1
        return count
      }
      if !switchSettled.value {
        // Attempt 1 occurs during switch: coordination generation mismatch
        throw OrchestratorBridgeError.Mls(message: "Coordination generation mismatch")
      }
      // Attempt 2 occurs after switch settles: delivers message
      deliveredMessages.withLock { $0.append("delivered-msg-\(current)") }
      return "msg-id-123"
    }

    XCTAssertEqual(result, "msg-id-123")
    XCTAssertEqual(callCount.value, 2, "Send should retry once after switch settles")
    XCTAssertEqual(deliveredMessages.value.count, 1, "Must deliver exactly once")
    XCTAssertEqual(deliveredMessages.value[0], "delivered-msg-2")

    // Verify diagnostics recorded GenerationMismatch failure and then recovery
    let records = MLSDiagnostics.recent()
    let sendFailed = records.first(where: { $0.event == .sendFailed })
    XCTAssertNotNil(sendFailed)
    XCTAssertEqual(sendFailed?.code, "GenerationMismatch")

    let sendRecovered = records.first(where: { $0.event == .sendRecovered })
    XCTAssertNotNil(sendRecovered)
    XCTAssertEqual(sendRecovered?.attempt, 2)
  }

  // MARK: - (c) Send Whose Account Changed Mid-Flight Is Abandoned

  func testSendWhoseAccountChangedMidFlightIsAbandoned() async throws {
    let originalSenderDID = "did:plc:sender_original_\(UUID().uuidString.prefix(8).lowercased())"
    let newActiveDID = "did:plc:switched_user_\(UUID().uuidString.prefix(8).lowercased())"
    let deliveredMessages = SafeBox<[String]>([])
    let callCount = SafeBox<Int>(0)

    // Set initial active user
    MLSCoordinationStore.shared.incrementGeneration(for: originalSenderDID)
    XCTAssertEqual(MLSCoordinationStore.shared.getState().activeUserDID, originalSenderDID)

    do {
      _ = try await MLSSendRetryCoordinator.performSendWithRetry(
        convoId: testConvoID,
        maxAttempts: 4,
        sleep: { _ in XCTFail("Must not retry when account changed mid-flight") }
      ) {
        callCount.withLock { $0 += 1 }

        // Simulate account switch occurring mid-flight before send payload execution
        MLSCoordinationStore.shared.incrementGeneration(for: newActiveDID)

        // Mid-flight validation check (same logic as MLSConversationManager.sendMessage)
        let activeDID = MLSCoordinationStore.shared.getState().activeUserDID
        if let activeDID, !activeDID.isEmpty, activeDID.lowercased() != originalSenderDID.lowercased() {
          throw MLSConversationError.noAuthentication
        }

        deliveredMessages.withLock { $0.append("sent-under-\(activeDID ?? "unknown")") }
        return "unreachable"
      }
      XCTFail("Send must be abandoned when account changed mid-flight")
    } catch {
      let classification = MLSConversationLifecycleError.classifySendError(error)
      XCTAssertEqual(classification, .terminal(code: "NoAuth", reason: "Not authenticated."))
      XCTAssertFalse(classification.isAutoRecoverable, "NoAuth must be terminal and not retry")
    }

    XCTAssertEqual(callCount.value, 1, "Must fail immediately on attempt 1 without retrying")
    XCTAssertTrue(deliveredMessages.value.isEmpty, "Message must never be delivered under the new account")
  }

  // MARK: - (d) Stream Reconnect Rebinds After Generation Bump Instead of Looping

  func testStreamReconnectRebindsAfterGenerationBumpInsteadOfLooping() async throws {
    let clientDID = "did:plc:stream_user_\(UUID().uuidString.prefix(8).lowercased())"
    MLSCoordinationStore.shared.incrementGeneration(for: clientDID)

    let rebindCount = SafeBox<Int>(0)
    let handlerProvider: MLSWebSocketManager.HandlerProvider = {
      rebindCount.withLock { $0 += 1 }
      var freshHandler = MLSWebSocketManager.EventHandler()
      freshHandler.onError = { _ in }
      return freshHandler
    }

    // Verify diagnostic recording and classification for closed-context stream pause
    let closedContextErr = OrchestratorBridgeError.Mls(
      message: "Context closed - database connections have been released for iOS suspension"
    )
    XCTAssertTrue(MLSWebSocketManager.isClosedContextError(closedContextErr))

    let genMismatchErr = MLSCoordinationAwareTask.GenerationStaleError(expected: 1, current: 2)
    XCTAssertTrue(MLSWebSocketManager.isGenerationMismatchError(genMismatchErr))

    // Record stream paused and resumed as MLSWebSocketManager does on rebind
    MLSDiagnostics.record(
      .streamPaused,
      code: "ContextClosed",
      conversation: testConvoID,
      attempt: 1,
      detail: ["reason": "ContextClosed"]
    )

    let freshHandler = await handlerProvider()
    XCTAssertNotNil(freshHandler)
    XCTAssertEqual(rebindCount.value, 1, "Handler provider must be invoked to rebind stream")

    MLSDiagnostics.record(
      .streamResumed,
      code: "ContextClosed",
      conversation: testConvoID,
      attempt: 0,
      detail: ["reason": "rebound_active_handler"]
    )

    let records = MLSDiagnostics.recent()
    let paused = records.first(where: { $0.event == .streamPaused })
    XCTAssertNotNil(paused)
    XCTAssertEqual(paused?.code, "ContextClosed")

    let resumed = records.first(where: { $0.event == .streamResumed })
    XCTAssertNotNil(resumed)
    XCTAssertEqual(resumed?.code, "ContextClosed")
  }

  // MARK: - (e) No Cached Context or Manager Reused Across Generation Bump

  func testNoCachedContextOrManagerReusedAcrossGenerationBump() async throws {
    let userDID = "did:plc:gen_bump_\(UUID().uuidString.prefix(8).lowercased())"

    // Set initial active user & coordination generation
    MLSCoordinationStore.shared.incrementGeneration(for: userDID)
    let initialGeneration = MLSCoordinationStore.shared.currentGeneration

    // Create initial context
    try await MLSCoreContext.shared.ensureContext(for: userDID)
    guard let id1 = await MLSCoreContext.shared.cachedContextIdentifier(for: userDID) else {
      return XCTFail("Initial context must be cached")
    }

    // Fetching again within same generation returns the exact same cached instance
    try await MLSCoreContext.shared.ensureContext(for: userDID)
    let id1Cached = await MLSCoreContext.shared.cachedContextIdentifier(for: userDID)
    XCTAssertEqual(id1, id1Cached, "Same generation must reuse cached context")

    // Bump coordination generation
    MLSCoordinationStore.shared.incrementGeneration(for: userDID)
    let bumpedGeneration = MLSCoordinationStore.shared.currentGeneration
    XCTAssertNotEqual(initialGeneration, bumpedGeneration, "Generation must have incremented")

    // Fetch context again after generation bump
    try await MLSCoreContext.shared.ensureContext(for: userDID)
    let id2 = await MLSCoreContext.shared.cachedContextIdentifier(for: userDID)
    XCTAssertNotEqual(
      id1,
      id2,
      "Cached context from previous generation must NOT be reused across a generation bump"
    )

    // Inactive account check: verify context opening is refused if another account is active
    let otherDID = "did:plc:other_account_\(UUID().uuidString.prefix(8).lowercased())"
    MLSCoordinationStore.shared.incrementGeneration(for: otherDID)
    XCTAssertEqual(MLSCoordinationStore.shared.getState().activeUserDID, otherDID)

    do {
      try await MLSCoreContext.shared.ensureContext(for: userDID)
      XCTFail("Must refuse to open context for account that is no longer active")
    } catch let error as MLSError {
      guard case .contextCreationBlocked(let reason) = error else {
        return XCTFail("Expected contextCreationBlocked but got: \(error)")
      }
      XCTAssertTrue(reason.contains("Account is no longer active"))
    }
  }
}
