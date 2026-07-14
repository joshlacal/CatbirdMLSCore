import Synchronization
import XCTest

@testable import CatbirdMLSCore

final class MLSContextFreeLifecycleSuspensionOwnerTests: XCTestCase {
  private var temporaryCoreStorageDirectory: URL?

  override func tearDown() {
    MLSClient.setNoUserResumeAfterCoreClearTestOverride(nil)
    MLSClient.setShutdownCoreCloseAfterIntentTestOverride(nil)
    MLSClient.setLegacyClearCoreCloseAfterIntentTestOverride(nil)
    MLSCoreContext.emergencyCloseAllContexts()
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    if let temporaryCoreStorageDirectory {
      try? FileManager.default.removeItem(at: temporaryCoreStorageDirectory)
    }
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    super.tearDown()
  }

  func testPendingShutdownCloseDeniesOwnerReleaseUntilHandoffCompletes() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    let closeIntentRecorded = expectation(description: "shutdown close intent recorded")
    let finishCoreClose = DispatchSemaphore(value: 0)
    owner.markSuspensionInProgress(reason: "pending shutdown close")
    MLSClient.setShutdownCoreCloseAfterIntentTestOverride {
      closeIntentRecorded.fulfill()
      finishCoreClose.wait()
    }

    let shutdown = Task {
      MLSClient.markShutdownInProgress(
        reason: "pending shutdown close",
        abandonmentOwnerDID: nil,
        abandonmentOwnerToken: UUID()
      )
    }
    await fulfillment(of: [closeIntentRecorded], timeout: 2)

    let releasedWhilePending = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertFalse(releasedWhilePending)
    assertBothGatesClosed()

    finishCoreClose.signal()
    await shutdown.value
    assertBothGatesClosed()
    let releasedAfterHandoff = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertFalse(releasedAfterHandoff)
    assertBothGatesClosed()
  }

  func testPendingLegacyCloseDeniesOwnerReleaseUntilHandoffCompletes() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    let closeIntentRecorded = expectation(description: "legacy close intent recorded")
    let finishCoreClose = DispatchSemaphore(value: 0)
    owner.markSuspensionInProgress(reason: "pending legacy close")
    MLSClient.setLegacyClearCoreCloseAfterIntentTestOverride {
      closeIntentRecorded.fulfill()
      finishCoreClose.wait()
    }

    let legacyClear = Task {
      MLSClient.clearSuspensionFlag(reason: "pending legacy denial")
    }
    await fulfillment(of: [closeIntentRecorded], timeout: 2)

    let releasedWhilePending = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertFalse(releasedWhilePending)
    assertBothGatesClosed()

    finishCoreClose.signal()
    let legacyCleared = await legacyClear.value
    XCTAssertFalse(legacyCleared)
    assertBothGatesClosed()
    let releasedAfterHandoff = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertTrue(releasedAfterHandoff)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testExactOwnerMarksAndReleasesContextFreeSuspension() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()

    owner.markSuspensionInProgress(reason: "context-free success")

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    let released = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertTrue(released)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testConcurrentSameOwnerReleaseIsSingleFlightAndKeepsGatesOpenAfterWinner() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    let firstReachedPostCoreClear = expectation(description: "first release cleared Core")
    let releaseFirst = DispatchSemaphore(value: 0)
    let invocationCount = Mutex(0)
    owner.markSuspensionInProgress(reason: "concurrent release")
    MLSClient.setNoUserResumeAfterCoreClearTestOverride {
      let isFirst = invocationCount.withLock { count in
        count += 1
        return count == 1
      }
      if isFirst {
        firstReachedPostCoreClear.fulfill()
        releaseFirst.wait()
      }
    }

    let firstRelease = Task { await owner.resumeSuspensionIfOwnedAndContextFree() }
    await fulfillment(of: [firstReachedPostCoreClear], timeout: 2)
    let duplicateReleased = await owner.resumeSuspensionIfOwnedAndContextFree()
    releaseFirst.signal()
    let firstReleased = await firstRelease.value

    XCTAssertEqual([firstReleased, duplicateReleased].filter { $0 }.count, 1)
    assertBothGatesOpenAndOrdinaryAdmissionAllowed()
  }

  func testShutdownPreservingSignalDuringReleaseKeepsBothGatesClosed() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    let releaseReachedPostCoreClear = expectation(description: "release cleared Core")
    let finishRelease = DispatchSemaphore(value: 0)
    let shutdownRecordedCloseIntent = expectation(description: "shutdown recorded close intent")
    let finishShutdownCoreClose = DispatchSemaphore(value: 0)
    owner.markSuspensionInProgress(reason: "shutdown-preserving release race")
    MLSClient.setNoUserResumeAfterCoreClearTestOverride {
      releaseReachedPostCoreClear.fulfill()
      finishRelease.wait()
    }
    MLSClient.setShutdownCoreCloseAfterIntentTestOverride {
      shutdownRecordedCloseIntent.fulfill()
      finishShutdownCoreClose.wait()
    }

    let release = Task { await owner.resumeSuspensionIfOwnedAndContextFree() }
    await fulfillment(of: [releaseReachedPostCoreClear], timeout: 2)
    let shutdown = Task {
      MLSClient.markShutdownInProgress(
        reason: "shutdown signal while release is held",
        abandonmentOwnerDID: nil,
        abandonmentOwnerToken: UUID()
      )
    }
    await fulfillment(of: [shutdownRecordedCloseIntent], timeout: 2)
    finishRelease.signal()
    let released = await release.value

    XCTAssertFalse(released)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(MLSClient._tryTrackedFFIAdmissionForTesting())
    MLSClient.setNoUserResumeAfterCoreClearTestOverride(nil)
    let retryDuringHandoff = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertFalse(retryDuringHandoff)
    finishShutdownCoreClose.signal()
    await shutdown.value
    assertBothGatesClosed()
  }

  func testLegacyClearDuringReleaseKeepsBothGatesClosed() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    let releaseReachedPostCoreClear = expectation(description: "release cleared Core")
    let finishRelease = DispatchSemaphore(value: 0)
    let legacyClearRecordedCloseIntent = expectation(
      description: "legacy clear recorded close intent"
    )
    let finishLegacyCoreClose = DispatchSemaphore(value: 0)
    owner.markSuspensionInProgress(reason: "legacy clear release race")
    MLSClient.setNoUserResumeAfterCoreClearTestOverride {
      releaseReachedPostCoreClear.fulfill()
      finishRelease.wait()
    }
    MLSClient.setLegacyClearCoreCloseAfterIntentTestOverride {
      legacyClearRecordedCloseIntent.fulfill()
      finishLegacyCoreClose.wait()
    }

    let release = Task { await owner.resumeSuspensionIfOwnedAndContextFree() }
    await fulfillment(of: [releaseReachedPostCoreClear], timeout: 2)
    let legacyClear = Task {
      MLSClient.clearSuspensionFlag(reason: "legacy clear while release is held")
    }
    await fulfillment(of: [legacyClearRecordedCloseIntent], timeout: 2)
    finishRelease.signal()
    let released = await release.value

    XCTAssertFalse(released)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(MLSClient._tryTrackedFFIAdmissionForTesting())
    MLSClient.setNoUserResumeAfterCoreClearTestOverride(nil)
    let retryDuringHandoff = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertFalse(retryDuringHandoff)
    finishLegacyCoreClose.signal()
    let legacyCleared = await legacyClear.value
    XCTAssertFalse(legacyCleared)
    assertBothGatesClosed()
  }

  func testWrongOwnerReleaseAfterSuccessfulReleaseDoesNotRecloseCoreGate() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    let wrongOwner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "wrong owner after success")
    let initiallyReleased = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertTrue(initiallyReleased)

    let wrongOwnerReleased = await wrongOwner.resumeSuspensionIfOwnedAndContextFree()

    XCTAssertFalse(wrongOwnerReleased)
    assertBothGatesOpenAndOrdinaryAdmissionAllowed()
  }

  func testDuplicateOwnerReleaseAfterSuccessfulReleaseDoesNotRecloseCoreGate() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "duplicate owner after success")
    let initiallyReleased = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertTrue(initiallyReleased)

    let duplicateReleased = await owner.resumeSuspensionIfOwnedAndContextFree()

    XCTAssertFalse(duplicateReleased)
    assertBothGatesOpenAndOrdinaryAdmissionAllowed()
  }

  func testWrongOwnerCannotReleaseContextFreeSuspension() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    let wrongOwner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "wrong-owner denial")

    let wrongOwnerReleased = await wrongOwner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertFalse(wrongOwnerReleased)
    assertBothGatesClosed()

    let ownerReleased = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertTrue(ownerReleased)
  }

  func testNewerSignalBeforeReleaseSupersedesOwner() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "older context-free signal")
    MLSClient.markSuspensionInProgress(reason: "newer generic signal")

    let released = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertFalse(released)
    assertBothGatesClosed()
  }

  func testNewerSignalAfterCoreClearKeepsBothGatesClosed() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "context-free CAS setup")
    MLSClient.setNoUserResumeAfterCoreClearTestOverride {
      MLSClient.markSuspensionInProgress(reason: "newer signal after Core clear")
    }

    let released = await owner.resumeSuspensionIfOwnedAndContextFree()
    XCTAssertFalse(released)
    assertBothGatesClosed()
  }

  func testLiveCoreContextDeniesReleaseAndKeepsBothGatesClosed() async throws {
    let did = "did:plc:context-free-owner-live-context"
    try useTemporaryCoreStorageDirectory()
    try await MLSCoreContext.shared.ensureContext(for: did)
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "live context denial")

    let released = await owner.resumeSuspensionIfOwnedAndContextFree()

    XCTAssertFalse(released)
    assertBothGatesClosed()
    await MLSCoreContext.shared.removeContext(for: did)
  }

  func testTrackedFFIOperationDeniesReleaseAndKeepsBothGatesClosed() async {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "tracked FFI denial")
    MLSClient._beginTrackedFFIOperationForTesting()
    defer { MLSClient._endTrackedFFIOperationForTesting() }

    let released = await owner.resumeSuspensionIfOwnedAndContextFree()

    XCTAssertFalse(released)
    assertBothGatesClosed()
  }

  func testShutdownLeaseDeniesReleaseAndKeepsBothGatesClosed() async throws {
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "shutdown lease denial")
    let lease = try XCTUnwrap(
      MLSClient.beginShutdownQuiescenceLease(
        abandonmentCapability: nil,
        excludingUserDID: nil
      )
    )
    defer { MLSClient.cancelShutdownQuiescenceLease(lease) }

    let released = await owner.resumeSuspensionIfOwnedAndContextFree()

    XCTAssertFalse(released)
    assertBothGatesClosed()
  }

  func testActiveBindingAndTransitionSnapshotDenyRelease() async throws {
    let did = "did:plc:context-free-owner-active"
    let api = ContextFreeOwnerDeviceAuthAPI(did: did)
    _ = try await installBinding(for: did, api: api)
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "active binding snapshot denial")

    let released = await owner.resumeSuspensionIfOwnedAndContextFree()

    XCTAssertFalse(released)
    XCTAssertTrue(MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: did))
    assertBothGatesClosed()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testPendingRebindAuthorityDeniesRelease() async throws {
    let did = "did:plc:context-free-owner-pending"
    let api = ContextFreeOwnerDeviceAuthAPI(did: did)
    _ = try await installBinding(for: did, api: api)
    _ = await MLSClient.shared.invalidateDeviceAuthBindingForReplacement(for: did)
    XCTAssertTrue(MLSClient.isDeviceAuthRebindObligationPendingForTesting(did))
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "pending rebind denial")

    let released = await owner.resumeSuspensionIfOwnedAndContextFree()

    XCTAssertFalse(released)
    assertBothGatesClosed()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testEnrollmentAuthorityDeniesRelease() async {
    let did = "did:plc:context-free-owner-enrollment"
    let api = ContextFreeOwnerDeviceAuthAPI(did: did, holdBegin: true)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    let binding = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { _ in Data(repeating: 1, count: 64) }
      )
    }
    await api.waitUntilBeginStarted()
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "enrollment denial")

    let released = await owner.resumeSuspensionIfOwnedAndContextFree()

    XCTAssertFalse(released)
    assertBothGatesClosed()
    await api.releaseBegin()
    binding.cancel()
    _ = try? await binding.value
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testCompletedRebindReceiptDeniesRelease() async throws {
    let did = "did:plc:context-free-owner-receipt"
    MLSClient.markSuspensionInProgress(reason: "receipt setup")
    let capability = try XCTUnwrap(MLSClient.beginSuspendedResumeCapability(for: did))
    XCTAssertTrue(
      MLSClient.recordCompletedDeviceAuthRebindForTesting(for: did, capability: capability)
    )
    MLSClient.cancelSuspendedResumeCapability(capability)
    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "receipt denial")

    let released = await owner.resumeSuspensionIfOwnedAndContextFree()

    XCTAssertFalse(released)
    assertBothGatesClosed()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  private func assertBothGatesClosed(
    file: StaticString = #filePath,
    line: UInt = #line
  ) {
    XCTAssertTrue(MLSClient.isSuspensionInProgress, file: file, line: line)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress, file: file, line: line)
    XCTAssertFalse(
      MLSClient._tryTrackedFFIAdmissionForTesting(),
      "A fresh ordinary MLS admission must remain denied",
      file: file,
      line: line
    )
  }

  private func assertBothGatesOpenAndOrdinaryAdmissionAllowed(
    file: StaticString = #filePath,
    line: UInt = #line
  ) {
    XCTAssertFalse(MLSClient.isSuspensionInProgress, file: file, line: line)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress, file: file, line: line)
    XCTAssertTrue(
      MLSClient._tryTrackedFFIAdmissionForTesting(),
      "A fresh ordinary MLS admission must remain allowed",
      file: file,
      line: line
    )
  }

  private func installBinding(
    for did: String,
    api: ContextFreeOwnerDeviceAuthAPI
  ) async throws -> MLSDeviceAuthBindingStatus {
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    return try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: api,
      signer: { _ in Data(repeating: 1, count: 64) }
    )
  }

  private func useTemporaryCoreStorageDirectory() throws {
    let directory = FileManager.default.temporaryDirectory
      .appendingPathComponent("context-free-lifecycle-owner-tests", isDirectory: true)
      .appendingPathComponent(UUID().uuidString, isDirectory: true)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    temporaryCoreStorageDirectory = directory
    MLSStoragePaths.setBaseDirectoryOverride(directory)
    MLSCoreContext.setSuspendedResumeReloadTestOverride(nil)
  }
}

private actor ContextFreeOwnerDeviceAuthAPI: MLSDeviceAuthBindingAPI {
  private let clientSnapshot: MLSDeviceAuthClientSnapshot
  private var shouldHoldBegin: Bool
  private var beginContinuation: CheckedContinuation<Void, Never>?
  private var beginStartedContinuation: CheckedContinuation<Void, Never>?

  init(did: String, holdBegin: Bool = false) {
    clientSnapshot = MLSDeviceAuthClientSnapshot(
      did: did,
      authenticationMode: .gateway,
      authenticationGeneration: 1
    )
    shouldHoldBegin = holdBegin
  }

  func snapshot() async -> MLSDeviceAuthClientSnapshot { clientSnapshot }

  func commitIfSnapshotMatches(
    _ expected: MLSDeviceAuthClientSnapshot,
    operation: @Sendable () -> Bool
  ) -> Bool {
    guard expected == clientSnapshot else { return false }
    return operation()
  }

  func begin(deviceID: String) async throws -> MLSDeviceAuthBindingChallenge {
    beginStartedContinuation?.resume()
    beginStartedContinuation = nil
    if shouldHoldBegin {
      await withCheckedContinuation { beginContinuation = $0 }
    }
    return MLSDeviceAuthBindingChallenge(
      challengeID: "challenge-1",
      challenge: Data([1, 2, 3]),
      expiresAt: Date().addingTimeInterval(60),
      bindingVersion: 1
    )
  }

  func begin(
    deviceID: String,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingChallenge {
    guard expected == clientSnapshot else { throw MLSDeviceAuthBindingError.sessionChanged }
    return try await begin(deviceID: deviceID)
  }

  func complete(challengeID: String, signature: Data) async throws
    -> MLSDeviceAuthBindingCompletion?
  {
    MLSDeviceAuthBindingCompletion(
      deviceID: "device-1",
      boundAt: Date(),
      bindingVersion: 1
    )
  }

  func complete(
    challengeID: String,
    signature: Data,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingCompletion? {
    guard expected == clientSnapshot else { throw MLSDeviceAuthBindingError.sessionChanged }
    return try await complete(challengeID: challengeID, signature: signature)
  }

  func waitUntilBeginStarted() async {
    if beginContinuation != nil { return }
    await withCheckedContinuation { beginStartedContinuation = $0 }
  }

  func releaseBegin() {
    shouldHoldBegin = false
    beginContinuation?.resume()
    beginContinuation = nil
  }
}
