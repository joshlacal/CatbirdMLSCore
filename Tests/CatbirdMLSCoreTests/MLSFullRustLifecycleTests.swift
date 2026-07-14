@preconcurrency import CatbirdMLS
import GRDB
import Petrel
import Synchronization
import XCTest

@testable import CatbirdMLSCore

final class MLSFullRustLifecycleTests: XCTestCase {
  private var temporaryCoreStorageDirectory: URL?

  override func setUpWithError() throws {
    MLSConversationManager.setSuspendedResumeStateRefreshOverride(nil)
    MLSConversationManager.setSuspendedResumeFinalReleaseOverride(nil)
    MLSConversationManager.setSuspendedResumeOtherBindingInvalidationOverride(nil)
    MLSConversationManager.setShutdownAfterSuspensionCapabilityTestOverride(nil)
    MLSConversationManager.setTrackedRustRuntimePostBodyTestOverride(nil)
    MLSCoreContext.setSuspendedResumeReloadTestOverride { _ in }
    MLSCoreContext.setSuspendedResumeContextAdmissionTestOverride(nil)
    MLSCoreContext.setContextPreInstallTestOverride(nil)
    MLSCoreContext.setContextPostInstallTestOverride(nil)
    MLSCoreContext.setContextCloseAttemptTestObserver(nil)
    MLSClient.setLegacyGateResumeAfterStateLockTestOverride(nil)
    MLSClient.setLegacyClearAfterCoreClearTestOverride(nil)
    MLSClient.setNoUserResumeAfterCoreClearTestOverride(nil)
    MLSClient.setShutdownCoreCloseAfterIntentTestOverride(nil)
    try super.setUpWithError()
  }

  override func tearDownWithError() throws {
    MLSConversationManager.setSuspendedResumeStateRefreshOverride(nil)
    MLSConversationManager.setSuspendedResumeFinalReleaseOverride(nil)
    MLSConversationManager.setSuspendedResumeOtherBindingInvalidationOverride(nil)
    MLSConversationManager.setShutdownAfterSuspensionCapabilityTestOverride(nil)
    MLSConversationManager.setTrackedRustRuntimePostBodyTestOverride(nil)
    MLSCoreContext.setSuspendedResumeReloadTestOverride(nil)
    MLSCoreContext.setSuspendedResumeContextAdmissionTestOverride(nil)
    MLSCoreContext.setContextPreInstallTestOverride(nil)
    MLSCoreContext.setContextPostInstallTestOverride(nil)
    MLSCoreContext.setContextCloseAttemptTestObserver(nil)
    MLSClient.setLegacyGateResumeAfterStateLockTestOverride(nil)
    MLSClient.setLegacyClearAfterCoreClearTestOverride(nil)
    MLSClient.setNoUserResumeAfterCoreClearTestOverride(nil)
    MLSClient.setShutdownCoreCloseAfterIntentTestOverride(nil)
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    if let temporaryCoreStorageDirectory {
      try? FileManager.default.removeItem(at: temporaryCoreStorageDirectory)
      self.temporaryCoreStorageDirectory = nil
    }
    MLSConversationManager.resetSuspensionStateForTesting()
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    try super.tearDownWithError()
  }

  func testRuntimeStorageLifecycleStatusWrapsBridgeResult() throws {
    let bridge = RecordingLifecycleBridge()
    bridge.storageLifecycleStatusResult = StorageLifecycleStatus(
      state: .open,
      interruptibleContexts: 2,
      isBusy: true,
      busyContexts: 1,
      lastOperationLabel: "encrypt_message"
    )
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:alice",
      mode: .rustFull,
      bridge: bridge
    )

    let status = runtime.storageLifecycleStatus()

    XCTAssertEqual(status.state, .open)
    XCTAssertEqual(status.interruptibleContexts, 2)
    XCTAssertTrue(status.isBusy)
    XCTAssertEqual(status.busyContexts, 1)
    XCTAssertEqual(status.lastOperationLabel, "encrypt_message")
  }

  func testRuntimePrepareForSuspendWrapsBridgeResult() throws {
    let bridge = RecordingLifecycleBridge()
    bridge.prepareResult = FfiSuspendResult(
      acceptingNewWork: false,
      interruptedContexts: 2
    )
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:alice",
      mode: .rustFull,
      bridge: bridge
    )

    let result = try runtime.prepareForSuspend(reason: "unit-test", deadlineMs: 250)

    XCTAssertEqual(bridge.prepareCalls, [LifecycleCall(reason: "unit-test", deadlineMs: 250)])
    XCTAssertFalse(result.acceptingNewWork)
    XCTAssertEqual(result.interruptedContexts, 2)
  }

  func testRustFullManagerSuspendsThroughRuntimeLifecycleBridge() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }

    XCTAssertTrue(rustPrepared)
    let postDrainPrepared = await manager.prepareRustRuntimeForSuspensionAfterDrain()
    XCTAssertTrue(postDrainPrepared)
    XCTAssertEqual(
      bridge.prepareCalls,
      [LifecycleCall(reason: "MLSConversationManager.suspendMLSOperations", deadlineMs: 1_500)]
    )
    XCTAssertNotNil(
      manager.orchestratorRuntime, "rustFull suspend should keep the runtime alive for resume")
  }

  func testResumeRecoversWhenLegacyCallerOpenedBothGatesWithoutRebindObligation() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    let coreRefreshCount = LifecycleLockedBox(0)
    MLSCoreContext.setSuspendedResumeReloadTestOverride { _ in
      coreRefreshCount.withValue { $0 += 1 }
    }
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }
    MLSClient.markSuspensionInProgress(reason: "ownerless legacy compatibility setup")
    XCTAssertFalse(manager.deviceAuthBindingRequiresResumeRebind)
    XCTAssertTrue(
      MLSClient.clearSuspensionFlag(reason: "legacy background-work compatibility caller")
    )
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .resumed)
    XCTAssertFalse(manager.isSuspending)
    XCTAssertFalse(manager.isSyncPaused)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertEqual(coreRefreshCount.value, 1)
    XCTAssertEqual(
      bridge.resumeCalls,
      [LifecycleReasonCall(reason: "MLSConversationManager.resumeMLSOperations")]
    )
  }

  func testLegacyClearRejectsUserOwnerBoundEmptySnapshotAndPreservesExactOwner() async throws {
    let userDID = "did:plc:legacy-clear-user-owner"
    let ownerToken = UUID()
    MLSClient.markSuspensionInProgress(
      reason: "user-owner legacy clear test",
      abandonmentOwnerDID: userDID,
      abandonmentOwnerToken: ownerToken
    )

    XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "unauthorized legacy user-owner clear"))
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    let ownerCapability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: userDID, ownerToken: ownerToken)
    )
    MLSClient.cancelSuspendedResumeCapability(ownerCapability)

    MLSClient.simulateLegacyGateClearPreservingOwnerForTesting()
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: userDID,
      deviceID: "device-legacy-open-window",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: userDID,
        deviceID: "device-legacy-open-window"
      )
    ) { _ in Data(repeating: 0x5A, count: 64) }
    XCTAssertNil(MLSClient.beginSuspendedResumeCapabilityAfterLegacyGateClear(for: userDID))
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    let preservedOwnerCapability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: userDID, ownerToken: ownerToken)
    )
    XCTAssertEqual(
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(
        for: userDID,
        capability: preservedOwnerCapability
      ),
      true
    )
    let userOwnerFinishedBeforeRebind = await MLSClient.shared.finishSuspendedResumeCapability(
      preservedOwnerCapability
    )
    XCTAssertFalse(userOwnerFinishedBeforeRebind)
    XCTAssertTrue(
      MLSClient.recordCompletedDeviceAuthRebindForTesting(
        for: userDID,
        capability: preservedOwnerCapability
      )
    )
    let userOwnerFinishedAfterRebind = await MLSClient.shared.finishSuspendedResumeCapability(
      preservedOwnerCapability
    )
    XCTAssertTrue(userOwnerFinishedAfterRebind)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
  }

  func testPendingShutdownCloseRevokesUserResumeUntilRenewedAfterHandoff() async throws {
    let userDID = "did:plc:pending-close-resume"
    let ownerToken = UUID()
    let closeIntentRecorded = expectation(description: "shutdown close intent recorded")
    let finishCoreClose = DispatchSemaphore(value: 0)
    MLSClient.markSuspensionInProgress(
      reason: "pending user resume close",
      abandonmentOwnerDID: userDID,
      abandonmentOwnerToken: ownerToken
    )
    let staleCapability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: userDID, ownerToken: ownerToken)
    )
    MLSClient.setShutdownCoreCloseAfterIntentTestOverride {
      closeIntentRecorded.fulfill()
      finishCoreClose.wait()
    }

    let shutdown = Task {
      MLSClient.markShutdownInProgress(
        reason: "pending user resume close",
        abandonmentOwnerDID: userDID,
        abandonmentOwnerToken: ownerToken
      )
    }
    await fulfillment(of: [closeIntentRecorded], timeout: 2)
    MLSCoreContext.clearSuspensionFlag()

    let staleFinished = await MLSClient.shared.finishSuspendedResumeCapability(staleCapability)
    XCTAssertFalse(staleFinished)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)

    finishCoreClose.signal()
    await shutdown.value
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    MLSClient.cancelSuspendedResumeCapability(staleCapability)
    XCTAssertNil(
      MLSClient.beginSuspendedResumeCapability(for: userDID, ownerToken: ownerToken),
      "shutdown phase must deny fresh user-resume authority after the close handoff"
    )
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testPendingShutdownCloseRevokesUserAbandonmentUntilRenewedLease() async throws {
    let userDID = "did:plc:pending-close-abandon"
    let ownerToken = UUID()
    let closeIntentRecorded = expectation(description: "shutdown close intent recorded")
    let finishCoreClose = DispatchSemaphore(value: 0)
    MLSClient.markSuspensionInProgress(
      reason: "pending user abandonment close",
      abandonmentOwnerDID: userDID,
      abandonmentOwnerToken: ownerToken
    )
    let staleCapability = try XCTUnwrap(
      MLSClient.ownedSuspensionAbandonmentCapability(for: userDID, ownerToken: ownerToken)
    )
    let staleLease = try XCTUnwrap(
      MLSClient.beginShutdownQuiescenceLease(
        abandonmentCapability: staleCapability,
        excludingUserDID: userDID
      )
    )
    MLSClient.setShutdownCoreCloseAfterIntentTestOverride {
      closeIntentRecorded.fulfill()
      finishCoreClose.wait()
    }

    let shutdown = Task {
      MLSClient.markShutdownInProgress(
        reason: "pending user abandonment close",
        abandonmentOwnerDID: userDID,
        abandonmentOwnerToken: ownerToken
      )
    }
    await fulfillment(of: [closeIntentRecorded], timeout: 2)

    let staleAbandoned = await MLSClient.abandonSuspensionAfterSafeShutdown(staleLease)
    XCTAssertFalse(staleAbandoned)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)

    finishCoreClose.signal()
    await shutdown.value
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    MLSClient.cancelShutdownQuiescenceLease(staleLease)
    let renewedCapability = try XCTUnwrap(
      MLSClient.ownedSuspensionAbandonmentCapability(for: userDID, ownerToken: ownerToken)
    )
    let renewedLease = try XCTUnwrap(
      MLSClient.beginShutdownQuiescenceLease(
        abandonmentCapability: renewedCapability,
        excludingUserDID: userDID
      )
    )
    let renewedAbandoned = await MLSClient.abandonSuspensionAfterSafeShutdown(renewedLease)
    XCTAssertTrue(renewedAbandoned)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testLegacyClearRejectsNoUserOwnerAndLegacyDIDReclosePreservesExactOwner() async throws {
    let noUserOwnerToken = UUID()
    MLSClient.markSuspensionInProgress(
      reason: "no-user owner legacy clear test",
      noUserOwnerToken: noUserOwnerToken
    )

    XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "unauthorized legacy no-user clear"))
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertNil(
      MLSClient.beginSuspendedResumeCapabilityAfterLegacyGateClear(
        for: "did:plc:legacy-did-reclose"
      )
    )
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    let preRecloseOwnerCapability = try XCTUnwrap(
      MLSClient.ownedNoUserSuspendedResumeCapability(ownerToken: noUserOwnerToken)
    )

    MLSClient.simulateLegacyGateClearPreservingOwnerForTesting()
    XCTAssertNil(
      MLSClient.beginSuspendedResumeCapabilityAfterLegacyGateClear(
        for: "did:plc:legacy-did-reclose"
      )
    )
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    let staleNoUserOwnerFinished = await MLSClient.finishNoUserSuspendedResumeCapability(
      preRecloseOwnerCapability
    )
    XCTAssertFalse(staleNoUserOwnerFinished)
    let renewedOwnerCapability = try XCTUnwrap(
      MLSClient.ownedNoUserSuspendedResumeCapability(ownerToken: noUserOwnerToken)
    )
    let renewedNoUserOwnerFinished = await MLSClient.finishNoUserSuspendedResumeCapability(
      renewedOwnerCapability
    )
    XCTAssertTrue(renewedNoUserOwnerFinished)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testLegacyClearCASLossToNewUserOwnerRestoresCoreGateAndPreservesOwner() throws {
    let userDID = "did:plc:legacy-clear-cas-owner"
    let ownerToken = UUID()
    MLSClient.markSuspensionInProgress(reason: "ownerless legacy clear CAS test")
    MLSClient.setLegacyClearAfterCoreClearTestOverride {
      MLSClient.markSuspensionInProgress(
        reason: "owner inserted during legacy clear",
        abandonmentOwnerDID: userDID,
        abandonmentOwnerToken: ownerToken
      )
    }

    XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "legacy clear losing owner CAS"))
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    let ownerCapability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: userDID, ownerToken: ownerToken)
    )
    MLSClient.cancelSuspendedResumeCapability(ownerCapability)
  }

  func testLegacyClearCASLossToNewNoUserOwnerRestoresCoreGateAndPreservesOwner() {
    let noUserOwnerToken = UUID()
    MLSClient.markSuspensionInProgress(reason: "ownerless no-user legacy clear CAS test")
    MLSClient.setLegacyClearAfterCoreClearTestOverride {
      MLSClient.markSuspensionInProgress(
        reason: "no-user owner inserted during legacy clear",
        noUserOwnerToken: noUserOwnerToken
      )
    }

    XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "legacy clear losing no-user owner CAS"))
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertNotNil(
      MLSClient.ownedNoUserSuspendedResumeCapability(ownerToken: noUserOwnerToken)
    )
  }

  func testLegacyClearPreservesOwnerlessEmptySnapshotCompatibility() {
    MLSClient.markSuspensionInProgress(reason: "ownerless legacy compatibility")

    XCTAssertTrue(MLSClient.clearSuspensionFlag(reason: "ownerless legacy compatibility"))
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testLegacyOpenedGateFallbackReclosesEveryGateWhenRebindIsOwed() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )

    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }
    MLSClient.markSuspensionInProgress(reason: "ownerless legacy rebind setup")
    XCTAssertTrue(
      MLSClient.clearSuspensionFlag(reason: "legacy background-work compatibility caller")
    )
    manager.deviceAuthBindingRequiresResumeRebind = true

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testLegacyOpenedGateFallbackReclosesEveryGateWhenCoreGateIsAlreadyClosed() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )

    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }
    MLSClient.markSuspensionInProgress(reason: "ownerless legacy split-gate setup")
    XCTAssertTrue(
      MLSClient.clearSuspensionFlag(reason: "legacy background-work compatibility caller")
    )
    MLSCoreContext.markSuspensionInProgress()

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testFailedDuplicateResumeDoesNotRevokeExistingExactCapability() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    let existingCapability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(
        for: "did:plc:testuser",
        ownerToken: manager.suspensionAbandonmentOwnerToken
      )
    )

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(
      MLSClient.isCurrentSuspendedResumeCapability(
        existingCapability,
        for: "did:plc:testuser"
      )
    )
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    MLSClient.cancelSuspendedResumeCapability(existingCapability)
  }

  func testSnapshotReadRejectsSupersededResumeCapabilityGeneration() throws {
    let userDID = "did:plc:testuser"
    MLSClient.markSuspensionInProgress(reason: "snapshot generation test")
    let capability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: userDID)
    )
    XCTAssertEqual(
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(
        for: userDID,
        capability: capability
      ),
      false
    )

    MLSClient.markSuspensionInProgress(reason: "superseding snapshot generation test")

    XCTAssertNil(
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(
        for: userDID,
        capability: capability
      )
    )
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testResumeCapabilityRejectsDIDOutsideSuspensionBindingSnapshot() async throws {
    let ownerDID = "did:plc:owner"
    let staleDID = "did:plc:stale"
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: ownerDID,
      deviceID: "device-owner",
      apiOverride: LifecycleDeviceAuthBindingAPI(did: ownerDID, deviceID: "device-owner")
    ) { _ in Data(repeating: 0x47, count: 64) }
    MLSClient.markSuspensionInProgress(reason: "resume permit owner test")

    let staleCapability = MLSClient.beginSuspendedResumeCapability(for: staleDID)
    if let staleCapability {
      MLSClient.cancelSuspendedResumeCapability(staleCapability)
    }
    let ownerCapability = MLSClient.beginSuspendedResumeCapability(for: ownerDID)

    XCTAssertNil(staleCapability)
    XCTAssertNotNil(ownerCapability)
    if let ownerCapability {
      MLSClient.cancelSuspendedResumeCapability(ownerCapability)
    }
    await MLSClient.shared.invalidateDeviceAuthBinding(for: ownerDID)
  }

  func testResumeCapabilityAllowsMemberOfMultiDIDSnapshot() async throws {
    let firstDID = "did:plc:first"
    let secondDID = "did:plc:second"
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: firstDID,
      deviceID: "device-first",
      apiOverride: LifecycleDeviceAuthBindingAPI(did: firstDID, deviceID: "device-first")
    ) { _ in Data(repeating: 0x48, count: 64) }
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: secondDID,
      deviceID: "device-second",
      apiOverride: LifecycleDeviceAuthBindingAPI(did: secondDID, deviceID: "device-second")
    ) { _ in Data(repeating: 0x49, count: 64) }
    MLSClient.markSuspensionInProgress(reason: "multi-DID resume permit test")

    let capability = MLSClient.beginSuspendedResumeCapability(for: secondDID)

    XCTAssertNotNil(capability)
    if let capability {
      MLSClient.cancelSuspendedResumeCapability(capability)
    }
    await MLSClient.shared.invalidateDeviceAuthBinding(for: firstDID)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: secondDID)
  }

  func testLegacyGateFallbackRefusalAtomicallyClosesClientGateForActiveAuthority() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    let bindingAPI = LifecycleDeviceAuthBindingAPI()
    manager.deviceAuthBindingAPIOverride = bindingAPI
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }
    _ = try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)

    let capability = MLSClient.beginSuspendedResumeCapabilityAfterLegacyGateClear(
      for: "did:plc:testuser"
    )

    XCTAssertNil(capability)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: "did:plc:testuser")
    )
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")
  }

  func testLegacyGateFallbackRefusesTransitionSnapshotAuthority() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    let bindingAPI = LifecycleDeviceAuthBindingAPI()
    manager.deviceAuthBindingAPIOverride = bindingAPI
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }
    _ = try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    MLSClient.markSuspensionInProgress(reason: "transition snapshot authority test")
    MLSCoreContext.clearSuspensionFlag()

    let capability = MLSClient.beginSuspendedResumeCapabilityAfterLegacyGateClear(
      for: "did:plc:testuser"
    )

    XCTAssertNil(capability)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: "did:plc:testuser")
    )
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")
  }

  func testLegacyGateFallbackCannotMintAuthorityOverTrackedFFI() throws {
    let userDID = "did:plc:testuser"
    MLSClient._beginTrackedFFIOperationForTesting()

    XCTAssertNil(
      MLSClient.beginSuspendedResumeCapabilityAfterLegacyGateClear(for: userDID)
    )

    MLSClient._endTrackedFFIOperationForTesting()
    let capability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapabilityAfterLegacyGateClear(for: userDID)
    )
    MLSClient.cancelSuspendedResumeCapability(capability)
  }

  func testLegacyGateFallbackBlocksClearAndAdmissionAfterAtomicClientTransition() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }
    MLSClient.markSuspensionInProgress(reason: "ownerless legacy interleaving setup")
    XCTAssertTrue(MLSClient.clearSuspensionFlag(reason: "legacy caller before interleaving"))
    let ffiAdmissionDuringInterleaving = LifecycleLockedBox<Bool?>(nil)
    MLSClient.setLegacyGateResumeAfterStateLockTestOverride {
      XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "interleaved legacy clear"))
      ffiAdmissionDuringInterleaving.withValue {
        $0 = MLSClient._tryTrackedFFIAdmissionForTesting()
      }
    }

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .resumed)
    XCTAssertFalse(manager.isSuspending)
    XCTAssertFalse(manager.isSyncPaused)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertNil(
      ffiAdmissionDuringInterleaving.value,
      "pre-drain gate restoration must bypass the legacy post-lock compatibility seam"
    )
  }

  func testPreCancelledLegacyGateFallbackNeverReleasesAdmission() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }
    MLSClient.markSuspensionInProgress(reason: "ownerless legacy cancellation setup")
    XCTAssertTrue(MLSClient.clearSuspensionFlag(reason: "legacy caller before cancellation"))

    let result = await Task {
      withUnsafeCurrentTask { $0?.cancel() }
      return await manager.resumeMLSOperations()
    }.value

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(MLSClient._tryTrackedFFIAdmissionForTesting())
    let replacementCapability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: "did:plc:testuser")
    )
    MLSClient.cancelSuspendedResumeCapability(replacementCapability)
  }

  func testRustAuthoritativeSuspendKeepsLegacyShutdownBehavior() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustAuthoritative)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustAuthoritative,
      bridge: bridge
    )

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    await manager.waitForRustShadowSuspensionTeardownForTesting()

    XCTAssertFalse(rustPrepared)
    XCTAssertTrue(bridge.prepareCalls.isEmpty)
    XCTAssertTrue(bridge.shutdownCalled)
    XCTAssertNil(manager.orchestratorRuntime)
  }

  func testRustFullSuspendFailureRequestsLegacyFallback() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    bridge.prepareError = TestLifecycleError.prepareFailed
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let rustPathAvailable = await MainActor.run { manager.suspendMLSOperations() }
    let rustPrepared = await manager.prepareRustRuntimeForSuspensionAfterDrain()

    XCTAssertTrue(rustPathAvailable)
    XCTAssertFalse(rustPrepared)
    XCTAssertEqual(
      bridge.prepareCalls,
      [LifecycleCall(reason: "MLSConversationManager.suspendMLSOperations", deadlineMs: 1_500)]
    )
    XCTAssertNotNil(
      manager.orchestratorRuntime,
      "prepare failure should preserve runtime so later close paths can still see it"
    )
  }

  func testRustFullSuspendWithoutRuntimeRequestsLegacyFallback() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }

    XCTAssertFalse(rustPrepared)
    XCTAssertNil(manager.orchestratorRuntime)
  }

  func testColdRuntimeInitializeParticipatesInSuspensionDrainAndCannotInstallAfterSuspension()
    async throws
  {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    let initializeStarted = DispatchSemaphore(value: 0)
    let releaseInitialize = DispatchSemaphore(value: 0)
    bridge.onInitialize = {
      initializeStarted.signal()
      releaseInitialize.wait()
    }
    manager.orchestratorRuntimeFactory = {
      MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: bridge
      )
    }

    let coldOperation = Task {
      try await manager.withRustAuthoritativeRuntime(operation: "heldColdInitialize") { _ in () }
    }
    XCTAssertEqual(initializeStarted.wait(timeout: .now() + 2), .success)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    _ = await MainActor.run { manager.suspendMLSOperations() }
    let prepareFinished = LifecycleLockedBox(false)
    let prepare = Task {
      let result = await manager.prepareRustRuntimeForSuspensionAfterDrain(timeout: 2)
      prepareFinished.withValue { $0 = true }
      return result
    }
    try await Task.sleep(nanoseconds: 100_000_000)

    XCTAssertFalse(prepareFinished.value)
    XCTAssertNil(manager.orchestratorRuntime)

    releaseInitialize.signal()
    do {
      _ = try await coldOperation.value
      XCTFail("Expected initialization superseded by suspension to abort")
    } catch {}
    let prepareResult = await prepare.value
    XCTAssertFalse(prepareResult)
    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertEqual(bridge.shutdownCallCount, 1)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testColdRuntimeCandidateRejectedBeforeAdmissionIsDiscardedWithoutDisturbingSuccessor()
    async throws
  {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .rustFull,
      migrateDatabase: true
    )
    let candidateContextIdentity = try await MLSCoreContext.shared.ensureContextIdentity(
      for: "did:plc:testuser"
    )
    let candidateBridge = RecordingLifecycleBridge()
    let candidateBuilt = AsyncStream<Void>.makeStream()
    let releaseCandidate = AsyncStream<Void>.makeStream()
    let shutdownStarted = DispatchSemaphore(value: 0)
    let releaseShutdown = DispatchSemaphore(value: 0)
    candidateBridge.onShutdown = {
      shutdownStarted.signal()
      releaseShutdown.wait()
    }
    manager.orchestratorRuntimeFactory = {
      let candidate = MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: candidateBridge,
        mlsContextIdentity: candidateContextIdentity
      )
      candidateBuilt.continuation.yield()
      for await _ in releaseCandidate.stream { break }
      return candidate
    }

    let coldOperation = Task {
      try await manager.withRustAuthoritativeRuntime(
        operation: "candidateBuiltBeforeSuspensionAdmission"
      ) { _ in () }
    }
    for await _ in candidateBuilt.stream { break }

    _ = await MainActor.run { manager.suspendMLSOperations() }
    releaseCandidate.continuation.yield()
    XCTAssertEqual(shutdownStarted.wait(timeout: .now() + 2), .success)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)
    XCTAssertThrowsError(
      try MLSClient.withSuspensionPreparationLease {}
    )
    releaseShutdown.signal()
    do {
      _ = try await coldOperation.value
      XCTFail("Expected the candidate to be rejected after suspension closed admission")
    } catch {}

    let candidateContextSurvived = await MLSCoreContext.shared.hasContext(
      for: "did:plc:testuser"
    )
    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertEqual(candidateBridge.shutdownCallCount, 1)
    XCTAssertFalse(candidateContextSurvived)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)

    MLSConversationManager.resetSuspensionStateForTesting()
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    let successorContextIdentity = try await MLSCoreContext.shared.ensureContextIdentity(
      for: "did:plc:testuser"
    )
    let successorBridge = RecordingLifecycleBridge()
    manager.orchestratorRuntimeFactory = {
      MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: successorBridge,
        mlsContextIdentity: successorContextIdentity
      )
    }

    let successor = await manager.ensureOrchestratorRuntime()

    XCTAssertNotNil(successor)
    XCTAssertTrue(manager.orchestratorRuntime === successor)
    XCTAssertEqual(successor?.mlsContextIdentity, successorContextIdentity)
    XCTAssertEqual(successorBridge.initializeCalls.count, 1)
    XCTAssertEqual(successorBridge.shutdownCallCount, 0)
    XCTAssertEqual(candidateBridge.shutdownCallCount, 1)
    let installedSuccessorContextIdentity = await MLSCoreContext.shared.contextIdentity(
      for: "did:plc:testuser"
    )
    XCTAssertEqual(installedSuccessorContextIdentity, successorContextIdentity)
    _ = await MLSCoreContext.shared.removeContext(for: "did:plc:testuser")
  }

  func testSwiftLegacyGuardDiscardsReservedCandidateUnderDrainVisibleRight() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      migrateDatabase: true
    )
    let contextIdentity = try await MLSCoreContext.shared.ensureContextIdentity(
      for: "did:plc:testuser"
    )
    let bridge = RecordingLifecycleBridge()
    let shutdownStarted = DispatchSemaphore(value: 0)
    let releaseShutdown = DispatchSemaphore(value: 0)
    bridge.onShutdown = {
      shutdownStarted.signal()
      releaseShutdown.wait()
    }
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge,
      mlsContextIdentity: contextIdentity
    )
    guard
      case .reserved(let initializationRight) = MLSClient.beginTrackedRuntimeInitialization(
        suspendedResumeCapability: nil,
        for: "did:plc:testuser",
        runtimeStorage: manager.orchestratorRuntimeStorage
      )
    else {
      return XCTFail("Expected a legacy-guard candidate reservation")
    }
    MLSClient.markSuspensionInProgress(reason: "legacy candidate rejection")

    let rejected = Task {
      try await manager.withTrackedRustRuntime(
        runtime,
        operation: "legacyCandidateRejection",
        runtimeInitializationRight: initializationRight
      ) { _ in () }
    }
    XCTAssertEqual(shutdownStarted.wait(timeout: .now() + 2), .success)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)
    XCTAssertThrowsError(try MLSClient.withSuspensionPreparationLease {})
    releaseShutdown.signal()
    do {
      _ = try await rejected.value
      XCTFail("Expected the Swift legacy guard to reject the Rust candidate")
    } catch {}

    XCTAssertEqual(bridge.shutdownCallCount, 1)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)
    let contextSurvived = await MLSCoreContext.shared.hasContext(for: "did:plc:testuser")
    XCTAssertFalse(contextSurvived)
  }

  func testResetDuringHeldFactoryAllowsSuccessorWithoutStaleFinalizationABA() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .rustFull,
      migrateDatabase: true
    )
    // A production runtime retains its MlsContext through its bridge and storage adapter. Keep the
    // fake candidate's context alive too, so ObjectIdentifier cannot be allocator-reused by the
    // replacement while the stale candidate is still in flight.
    let staleContext = try await MLSCoreContext.shared.getContext(for: "did:plc:testuser")
    let staleContextIdentity = ObjectIdentifier(staleContext)
    let staleBridge = RecordingLifecycleBridge()
    let successorBridge = RecordingLifecycleBridge()
    let staleBuilt = AsyncStream<Void>.makeStream()
    let releaseStale = AsyncStream<Void>.makeStream()
    let factoryCalls = LifecycleLockedBox(0)
    manager.orchestratorRuntimeFactory = {
      factoryCalls.withValue { $0 += 1 }
      let call = factoryCalls.value
      if call == 1 {
        let stale = MLSOrchestratorRuntime(
          userDID: "did:plc:testuser",
          mode: .rustFull,
          bridge: staleBridge,
          mlsContextIdentity: staleContextIdentity
        )
        staleBuilt.continuation.yield()
        for await _ in releaseStale.stream { break }
        return stale
      }
      let identity = try? await MLSCoreContext.shared.ensureContextIdentity(
        for: "did:plc:testuser"
      )
      guard let identity else { return nil }
      return MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: successorBridge,
        mlsContextIdentity: identity
      )
    }

    let staleInitialization = Task { await manager.ensureOrchestratorRuntime() }
    for await _ in staleBuilt.stream { break }
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    manager.resetOrchestratorRuntime(reason: "reset held stale initializer")
    let successorInitialization = Task { await manager.ensureOrchestratorRuntime() }
    try await Task.sleep(nanoseconds: 100_000_000)
    XCTAssertEqual(factoryCalls.value, 1)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    releaseStale.continuation.yield()
    let staleResult = await staleInitialization.value
    XCTAssertNil(staleResult)
    let successor = await successorInitialization.value
    XCTAssertNotNil(successor)
    XCTAssertTrue(manager.orchestratorRuntime === successor)
    XCTAssertTrue(staleBridge.initializeCalls.isEmpty)
    XCTAssertEqual(staleBridge.shutdownCallCount, 1)
    XCTAssertEqual(successorBridge.shutdownCallCount, 0)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)
    let survivingIdentity = await MLSCoreContext.shared.contextIdentity(for: "did:plc:testuser")
    XCTAssertEqual(survivingIdentity, successor?.mlsContextIdentity)
    withExtendedLifetime(staleContext) {}
    _ = await MLSCoreContext.shared.removeContext(for: "did:plc:testuser")
  }

  func testConcurrentColdRuntimeCallersShareOneInitializationAndInstalledRuntime() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    let initializeStarted = DispatchSemaphore(value: 0)
    let releaseInitialize = DispatchSemaphore(value: 0)
    let factoryCalls = LifecycleLockedBox(0)
    bridge.onInitialize = {
      initializeStarted.signal()
      releaseInitialize.wait()
    }
    manager.orchestratorRuntimeFactory = {
      factoryCalls.withValue { $0 += 1 }
      return MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: bridge
      )
    }

    let first = Task { await manager.ensureOrchestratorRuntime() }
    XCTAssertEqual(initializeStarted.wait(timeout: .now() + 2), .success)
    let second = Task { await manager.ensureOrchestratorRuntime() }
    try await Task.sleep(nanoseconds: 100_000_000)

    XCTAssertEqual(factoryCalls.value, 1)
    XCTAssertEqual(bridge.initializeCalls.count, 1)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    releaseInitialize.signal()
    let firstValue = await first.value
    let secondValue = await second.value
    let firstRuntime = try XCTUnwrap(firstValue)
    let secondRuntime = try XCTUnwrap(secondValue)

    XCTAssertTrue(firstRuntime === secondRuntime)
    XCTAssertTrue(manager.orchestratorRuntime === firstRuntime)
    XCTAssertEqual(factoryCalls.value, 1)
    XCTAssertEqual(bridge.initializeCalls.count, 1)
  }

  func testStaleInitializerCancellationCannotRevokeWaiterReservation() throws {
    let storage = MLSOrchestratorRuntimeStorage()
    guard case .reserved(let staleToken) = storage.beginInitialization() else {
      return XCTFail("Expected first initialization reservation")
    }
    storage.cancelInitialization(token: staleToken)
    guard case .reserved(let waiterToken) = storage.beginInitialization() else {
      return XCTFail("Expected waiter to acquire the next reservation")
    }
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )

    storage.cancelInitialization(token: staleToken)

    XCTAssertTrue(storage.installReserved(runtime, token: waiterToken))
    XCTAssertTrue(storage.load() === runtime)
  }

  func testCancelledResumeReattachReleasesItsExactInitializationReservation() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    MLSClient.markSuspensionInProgress(reason: "cancelled reattach reservation test")
    let capability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: "did:plc:testuser")
    )
    defer { MLSClient.cancelSuspendedResumeCapability(capability) }

    manager.orchestratorRuntimeResumeFactory = {
      MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: RecordingLifecycleBridge()
      )
    }
    let reattachGate = LifecycleInitializationGate()
    manager.orchestratorRuntimeBeforeReattachTestOverride = {
      await reattachGate.hold()
    }

    let restore = Task {
      await manager.restoreOrchestratorRuntimeAfterSuspendClose(
        reason: "cancelled exact-token test",
        suspendedResumeCapability: capability
      )
    }
    await reattachGate.waitUntilHeld()
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)
    restore.cancel()
    await reattachGate.release()

    let restoredRuntime = await restore.value
    XCTAssertNil(restoredRuntime)
    guard
      case .reserved(let replacementToken) =
        manager.orchestratorRuntimeStorage.beginInitialization()
    else {
      return XCTFail("Cancellation must release the exact reattach reservation")
    }
    manager.orchestratorRuntimeStorage.cancelInitialization(token: replacementToken)
  }

  func testRevokedResumeReservationCannotEnterReattachBody() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntimeResumeFactory = {
      MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: bridge
      )
    }
    MLSClient.markSuspensionInProgress(reason: "revoked resume reservation")
    let capability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: "did:plc:testuser")
    )
    let reattachGate = LifecycleInitializationGate()
    manager.orchestratorRuntimeBeforeReattachTestOverride = {
      await reattachGate.hold()
    }

    let restore = Task {
      await manager.restoreOrchestratorRuntimeAfterSuspendClose(
        reason: "revoked resume reservation",
        suspendedResumeCapability: capability
      )
    }
    await reattachGate.waitUntilHeld()
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)
    manager.resetOrchestratorRuntime(reason: "revoke held resume reservation")
    await reattachGate.release()

    let restoredRuntime = await restore.value
    XCTAssertNil(restoredRuntime)
    XCTAssertTrue(bridge.reattachCalls.isEmpty)
    XCTAssertEqual(bridge.shutdownCallCount, 1)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)
    MLSClient.cancelSuspendedResumeCapability(capability)
  }

  func testRuntimeResetCancelsPendingInitializationAndFencesStaleInstall() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    guard
      case .reserved(let staleToken) =
        manager.orchestratorRuntimeStorage.beginInitialization()
    else {
      return XCTFail("Expected reset test reservation")
    }
    let staleRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )

    manager.resetOrchestratorRuntime(reason: "pending initialization reset test")

    guard
      case .reserved(let replacementToken) =
        manager.orchestratorRuntimeStorage.beginInitialization()
    else {
      return XCTFail("Reset must release the pending initialization reservation")
    }
    XCTAssertFalse(
      manager.orchestratorRuntimeStorage.installReserved(staleRuntime, token: staleToken),
      "A pre-reset initializer must not install after reset"
    )
    manager.orchestratorRuntimeStorage.cancelInitialization(token: staleToken)
    let replacementRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    XCTAssertTrue(
      manager.orchestratorRuntimeStorage.installReserved(
        replacementRuntime,
        token: replacementToken
      ),
      "A stale reset initializer must not disturb the current reservation"
    )
    XCTAssertTrue(manager.orchestratorRuntime === replacementRuntime)
  }

  func testRuntimeInvalidationCancelsPendingInitializationAndFencesStaleInstall() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    guard
      case .reserved(let staleToken) =
        manager.orchestratorRuntimeStorage.beginInitialization()
    else {
      return XCTFail("Expected invalidation test reservation")
    }
    let staleRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )

    manager.invalidateOrchestratorRuntime(reason: "pending initialization invalidation test")

    guard
      case .reserved(let replacementToken) =
        manager.orchestratorRuntimeStorage.beginInitialization()
    else {
      return XCTFail("Invalidation must release the pending initialization reservation")
    }
    XCTAssertFalse(
      manager.orchestratorRuntimeStorage.installReserved(staleRuntime, token: staleToken),
      "A pre-invalidation initializer must not install after invalidation"
    )
    manager.orchestratorRuntimeStorage.cancelInitialization(token: staleToken)
    let replacementRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    XCTAssertTrue(
      manager.orchestratorRuntimeStorage.installReserved(
        replacementRuntime,
        token: replacementToken
      ),
      "A stale invalidated initializer must not disturb the current reservation"
    )
    XCTAssertTrue(manager.orchestratorRuntime === replacementRuntime)
  }

  func testRejectedAtomicRuntimeAdmissionReleasesOnlyItsOwnReservation() throws {
    let storage = MLSOrchestratorRuntimeStorage()
    guard
      case .reserved(let initializationRight) = MLSClient.beginTrackedRuntimeInitialization(
        suspendedResumeCapability: nil,
        for: "did:plc:testuser",
        runtimeStorage: storage
      )
    else {
      return XCTFail("Expected rejected operation reservation")
    }
    let bridge = RecordingLifecycleBridge()
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    var operationRan = false
    MLSClient.markSuspensionInProgress(reason: "reject atomic runtime install admission")

    XCTAssertThrowsError(
      try MLSClient.withTrackedFFIAdmissionAndAtomicRuntimeInstall(
        initializationRight: initializationRight,
        operation: { operationRan = true },
        runtime: runtime,
        runtimeStorage: storage
      )
    )
    XCTAssertFalse(operationRan)
    XCTAssertTrue(MLSClient.runtimeInitializationRightIsActive(initializationRight))
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)
    guard case .wait = storage.beginInitialization() else {
      return XCTFail("Rejected admission must retain its reservation through candidate teardown")
    }
    XCTAssertThrowsError(try MLSClient.withSuspensionPreparationLease {})

    runtime.shutdown()
    XCTAssertTrue(
      MLSClient.cancelTrackedRuntimeInitialization(
        initializationRight,
        runtimeStorage: storage
      )
    )
    XCTAssertEqual(bridge.shutdownCallCount, 1)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)
    guard case .reserved(let waiterToken) = storage.beginInitialization() else {
      return XCTFail("Completed teardown must release its exact reservation")
    }

    XCTAssertFalse(
      MLSClient.cancelTrackedRuntimeInitialization(
        initializationRight,
        runtimeStorage: storage
      ),
      "A stale right must not revoke its successor's reservation"
    )

    let replacement = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    XCTAssertTrue(storage.installReserved(replacement, token: waiterToken))
    XCTAssertTrue(storage.load() === replacement)
  }

  func testRuntimeInitializationRightsPermitDifferentDIDsConcurrently() throws {
    let firstStorage = MLSOrchestratorRuntimeStorage()
    let secondStorage = MLSOrchestratorRuntimeStorage()
    guard
      case .reserved(let firstRight) = MLSClient.beginTrackedRuntimeInitialization(
        suspendedResumeCapability: nil,
        for: "did:plc:first",
        runtimeStorage: firstStorage
      ),
      case .reserved(let secondRight) = MLSClient.beginTrackedRuntimeInitialization(
        suspendedResumeCapability: nil,
        for: "did:plc:second",
        runtimeStorage: secondStorage
      )
    else {
      return XCTFail("Different DIDs must retain independent initialization lanes")
    }

    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 2)
    XCTAssertTrue(
      MLSClient.cancelTrackedRuntimeInitialization(firstRight, runtimeStorage: firstStorage)
    )
    XCTAssertTrue(
      MLSClient.cancelTrackedRuntimeInitialization(secondRight, runtimeStorage: secondStorage)
    )
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)
  }

  func testHeldResumeReattachParticipatesInNewerSuspensionDrainAndCannotInstall() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    let reattachStarted = DispatchSemaphore(value: 0)
    let releaseReattach = DispatchSemaphore(value: 0)
    bridge.onReattach = {
      reattachStarted.signal()
      releaseReattach.wait()
    }
    manager.orchestratorRuntimeResumeFactory = {
      MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: bridge
      )
    }
    manager.rustRuntimeRequiresForegroundRestore = true
    await MainActor.run {
      manager.markSuspensionInProgressForLifecycleTransition(reason: "held reattach baseline")
    }

    let resume = Task { await manager.resumeMLSOperations() }
    XCTAssertEqual(reattachStarted.wait(timeout: .now() + 2), .success)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    _ = await MainActor.run { manager.suspendMLSOperations() }
    let prepareFinished = LifecycleLockedBox(false)
    let prepare = Task {
      let result = await manager.prepareRustRuntimeForSuspensionAfterDrain(timeout: 2)
      prepareFinished.withValue { $0 = true }
      return result
    }
    try await Task.sleep(nanoseconds: 100_000_000)

    XCTAssertFalse(prepareFinished.value)
    XCTAssertNil(manager.orchestratorRuntime)

    releaseReattach.signal()
    let resumeResult = await resume.value
    let prepareResult = await prepare.value

    XCTAssertEqual(resumeResult, .failedStillSuspended)
    XCTAssertFalse(prepareResult)
    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertEqual(bridge.shutdownCallCount, 1)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testRustShadowColdInitializeAndMirrorAreTrackedWithoutAuthoritativeMutationAccess()
    async throws
  {
    let manager = try await makeManager(protocolAuthorityMode: .rustShadow)
    let bridge = RecordingLifecycleBridge()
    let trackedCounts = LifecycleLockedBox<[Int]>([])
    bridge.onInitialize = {
      trackedCounts.withValue { $0.append(MLSClient.inFlightFFIOperationCount) }
    }
    bridge.onRecoveryState = {
      trackedCounts.withValue { $0.append(MLSClient.inFlightFFIOperationCount) }
    }
    manager.orchestratorRuntimeFactory = {
      MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustShadow,
        bridge: bridge
      )
    }
    let authoritativeBodyCalls = LifecycleLockedBox(0)

    do {
      _ = try await manager.withRustAuthoritativeRuntime(operation: "shadowMutationDenied") {
        _ in
        authoritativeBodyCalls.withValue { $0 += 1 }
      }
      XCTFail("Rust shadow must not gain authoritative mutation access")
    } catch {}

    await manager.mirrorRustRecoveryState(
      operation: "tracked-shadow-mirror",
      conversationId: "conversation-shadow",
      swiftDecision: .healthy
    )

    XCTAssertEqual(authoritativeBodyCalls.value, 0)
    XCTAssertEqual(bridge.initializeCalls, ["did:plc:testuser"])
    XCTAssertEqual(bridge.recoveryStateCalls, ["conversation-shadow"])
    XCTAssertEqual(trackedCounts.value, [1, 1])
    XCTAssertNotNil(manager.orchestratorRuntime)
  }

  func testRustShadowSuspensionDrainsHeldMirrorBeforeLeasedRuntimeShutdown() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustShadow)
    let bridge = RecordingLifecycleBridge()
    let mirrorStarted = DispatchSemaphore(value: 0)
    let releaseMirror = DispatchSemaphore(value: 0)
    bridge.onRecoveryState = {
      mirrorStarted.signal()
      releaseMirror.wait()
    }
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustShadow,
      bridge: bridge
    )

    let mirror = Task {
      await manager.mirrorRustRecoveryState(
        operation: "held-shadow-mirror",
        conversationId: "conversation-shadow-held",
        swiftDecision: .healthy
      )
    }
    XCTAssertEqual(mirrorStarted.wait(timeout: .now() + 2), .success)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    _ = await MainActor.run { manager.suspendMLSOperations() }
    try await Task.sleep(nanoseconds: 100_000_000)

    XCTAssertEqual(bridge.shutdownCallCount, 0)
    XCTAssertNotNil(manager.orchestratorRuntime)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    releaseMirror.signal()
    await mirror.value
    await manager.waitForRustShadowSuspensionTeardownForTesting()

    XCTAssertEqual(bridge.shutdownCallCount, 1)
    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testRustFullManagerResumesThroughRuntimeLifecycleBridge() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    MLSClient.markSuspensionInProgress(reason: "runtime lifecycle resume test")
    await MainActor.run {
      manager.isSuspending = true
    }
    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .resumed)
    XCTAssertEqual(
      bridge.resumeCalls,
      [LifecycleReasonCall(reason: "MLSConversationManager.resumeMLSOperations")]
    )
  }

  func testNoUserSuspendedManagerAbandonsEmptyAuthorityForNextAccount() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    _ = await MainActor.run { manager.suspendMLSOperations() }

    let shutdownWasSafe = await authorizedAccountSwitchShutdown(manager)

    XCTAssertTrue(shutdownWasSafe)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(manager.isSuspending)
    XCTAssertFalse(manager.isSyncPaused)
    XCTAssertTrue(MLSClient._tryTrackedFFIAdmissionForTesting())
    try await MLSCoreContext.shared.ensureContext(for: "did:plc:next-account")
    let nextContextExists = await MLSCoreContext.shared.hasContext(for: "did:plc:next-account")
    XCTAssertTrue(nextContextExists)
    await MLSCoreContext.shared.removeContext(for: "did:plc:next-account")
  }

  func testPreLoginManagerResumesExactEmptySuspensionForNextAccount() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )

    _ = await MainActor.run { manager.suspendMLSOperations() }

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .resumed)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(manager.isSuspending)
    XCTAssertFalse(manager.isSyncPaused)
    XCTAssertTrue(MLSClient._tryTrackedFFIAdmissionForTesting())
    try await MLSCoreContext.shared.ensureContext(for: "did:plc:next-login")
    let nextContextExists = await MLSCoreContext.shared.hasContext(for: "did:plc:next-login")
    XCTAssertTrue(nextContextExists)
    await MLSCoreContext.shared.removeContext(for: "did:plc:next-login")
  }

  func testStalePreLoginManagerCannotReleaseAnotherManagersSuspension() async throws {
    let owner = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    let stale = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    _ = await MainActor.run { owner.suspendMLSOperations() }

    let staleResult = await stale.resumeMLSOperations()

    XCTAssertEqual(staleResult, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    let ownerResult = await owner.resumeMLSOperations()

    XCTAssertEqual(ownerResult, .resumed)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testGenericSignalRevokesPreLoginSuspensionAuthority() async throws {
    let manager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    _ = await MainActor.run { manager.suspendMLSOperations() }
    MLSClient.markSuspensionInProgress(reason: "new generic suspension signal")

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
  }

  func testNewPreLoginManagerSignalRevokesPriorManagersAuthority() async throws {
    let first = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    let second = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    _ = await MainActor.run { first.suspendMLSOperations() }
    _ = await MainActor.run { second.suspendMLSOperations() }

    let firstResult = await first.resumeMLSOperations()

    XCTAssertEqual(firstResult, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    let secondResult = await second.resumeMLSOperations()

    XCTAssertEqual(secondResult, .resumed)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testOwnerBoundSignalSupersedesPreLoginSuspensionAuthority() async throws {
    let preLogin = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    let owner = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: "did:plc:owner"
    )
    _ = await MainActor.run { preLogin.suspendMLSOperations() }
    await MainActor.run {
      owner.markSuspensionInProgressForLifecycleTransition(
        reason: "owner-bound signal supersedes pre-login suspension"
      )
    }

    let staleResult = await preLogin.resumeMLSOperations()

    XCTAssertEqual(staleResult, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    let ownerResult = await owner.resumeMLSOperations()

    XCTAssertEqual(ownerResult, .resumed)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testOwnerBoundSignalDuringPreLoginFinalReleaseKeepsEveryGateClosed() async throws {
    let ownerDID = "did:plc:owner"
    let preLogin = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    let owner = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: ownerDID
    )
    MLSClient.setNoUserResumeAfterCoreClearTestOverride {
      MLSClient.markSuspensionInProgress(
        reason: "owner-bound signal during pre-login final release",
        abandonmentOwnerDID: ownerDID,
        abandonmentOwnerToken: owner.suspensionAbandonmentOwnerToken
      )
    }
    _ = await MainActor.run { preLogin.suspendMLSOperations() }

    let staleResult = await preLogin.resumeMLSOperations()

    XCTAssertEqual(staleResult, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(preLogin.isSuspending)
    XCTAssertTrue(preLogin.isSyncPaused)

    MLSClient.setNoUserResumeAfterCoreClearTestOverride(nil)
    let ownerResult = await owner.resumeMLSOperations()

    XCTAssertEqual(ownerResult, .resumed)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testLateContextRegistrationCannotEscapeNewEmergencySuspension() async throws {
    try useTemporaryCoreStorageDirectory()
    let installReached = AsyncStream<Void>.makeStream()
    let releaseInstall = AsyncStream<Void>.makeStream()
    MLSCoreContext.setContextPreInstallTestOverride { _ in
      installReached.continuation.yield()
      for await _ in releaseInstall.stream { break }
    }

    let creation = Task {
      try await MLSCoreContext.shared.ensureContext(for: "did:plc:late-registration")
    }
    for await _ in installReached.stream { break }

    MLSClient.emergencyCloseAllContexts(reason: "late registration regression")
    releaseInstall.continuation.yield()

    do {
      try await creation.value
      XCTFail("Expected the superseded context construction to fail")
    } catch {}
    let contextExists = await MLSCoreContext.shared.hasContext(
      for: "did:plc:late-registration"
    )
    XCTAssertFalse(contextExists)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testEmergencyCloseOwnsRegisteredContextExactlyOnceBeforePostValidation() async throws {
    try useTemporaryCoreStorageDirectory()
    let userDID = "did:plc:single-close-owner"
    let installReached = AsyncStream<Void>.makeStream()
    let releaseInstall = AsyncStream<Void>.makeStream()
    let closeAttempts = Mutex<[ObjectIdentifier: Int]>([:])
    MLSCoreContext.setContextPostInstallTestOverride { did in
      guard did == userDID else { return }
      installReached.continuation.yield()
      for await _ in releaseInstall.stream { break }
    }
    MLSCoreContext.setContextCloseAttemptTestObserver { did, identity in
      guard did == userDID else { return }
      closeAttempts.withLock { $0[identity, default: 0] += 1 }
    }

    let creation = Task {
      try await MLSCoreContext.shared.ensureContext(for: userDID)
    }
    for await _ in installReached.stream { break }

    MLSClient.emergencyCloseAllContexts(reason: "single close ownership regression")
    releaseInstall.continuation.yield()

    do {
      try await creation.value
      XCTFail("Expected emergency ownership to supersede context installation")
    } catch {}
    let recordedAttempts = closeAttempts.withLock { Array($0.values) }
    XCTAssertEqual(recordedAttempts, [1])
    let contextExists = await MLSCoreContext.shared.hasContext(for: userDID)
    XCTAssertFalse(contextExists)
  }

  func testNoUserResumeDuringShutdownCannotReleaseClosedGates() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    MLSClient.markSuspensionInProgress(reason: "no-user in-progress shutdown test")
    manager.userDid = nil
    manager.isShuttingDown = true
    await MainActor.run { manager.isSuspending = true }

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
  }

  func testNoUserShutdownConsumesItsExactOwnerAndReopensBothGates() async throws {
    let manager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )

    let shutdownWasSafe = await manager.shutdown()

    XCTAssertTrue(shutdownWasSafe)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(manager.isSuspending)
    XCTAssertFalse(manager.isSyncPaused)
  }

  func testNoUserShutdownCannotConsumeSupersededOwner() async throws {
    let manager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    await MainActor.run {
      manager.markSuspensionInProgressForLifecycleTransition(
        reason: "no-user shutdown supersession setup"
      )
    }
    MLSClient.markSuspensionInProgress(reason: "supersede no-user shutdown owner")

    let shutdownWasSafe = await manager.shutdown()

    XCTAssertFalse(shutdownWasSafe)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
  }

  func testNoUserShutdownCASLossAfterCoreClearKeepsBothGatesClosed() async throws {
    let manager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    MLSClient.setNoUserResumeAfterCoreClearTestOverride {
      MLSClient.markSuspensionInProgress(reason: "newer no-user shutdown CAS signal")
    }

    let shutdownWasSafe = await manager.shutdown()

    XCTAssertFalse(shutdownWasSafe)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
  }

  func testNoUserAbandonmentPreservesUnrelatedSuspensionSnapshot() async throws {
    let ownerDID = "did:plc:owner"
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: ownerDID,
      deviceID: "device-no-user-snapshot",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: ownerDID,
        deviceID: "device-no-user-snapshot"
      )
    ) { _ in Data(repeating: 0x4B, count: 64) }
    MLSClient.markSuspensionInProgress(reason: "no-user snapshot preservation test")
    let shutdownWasSafe = await authorizedAccountSwitchShutdown(manager)

    XCTAssertFalse(shutdownWasSafe)
    XCTAssertTrue(MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: ownerDID))
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: ownerDID)
  }

  func testNoUserShutdownRevokesEarlierUserResumeCapability() async throws {
    let ownerDID = "did:plc:owner"
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    MLSClient.markSuspensionInProgress(reason: "no-user capability preservation test")
    let capability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: ownerDID)
    )
    let shutdownWasSafe = await authorizedAccountSwitchShutdown(manager)

    XCTAssertFalse(shutdownWasSafe)
    XCTAssertFalse(MLSClient.isCurrentSuspendedResumeCapability(capability, for: ownerDID))
    XCTAssertNil(MLSClient.beginSuspendedResumeCapability(for: ownerDID))
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    MLSClient.cancelSuspendedResumeCapability(capability)
  }

  func testNoUserAbandonmentPreservesActiveBindingOutsideSnapshot() async throws {
    let ownerDID = "did:plc:owner"
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    MLSClient.markSuspensionInProgress(reason: "no-user active binding setup")
    let capability = try XCTUnwrap(
      MLSClient.beginSuspendedResumeCapability(for: ownerDID)
    )
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: ownerDID,
      deviceID: "device-no-user-active",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: ownerDID,
        deviceID: "device-no-user-active"
      ),
      suspendedResumePermit: capability
    ) { _ in Data(repeating: 0x4C, count: 64) }
    MLSClient.cancelSuspendedResumeCapability(capability)
    let shutdownWasSafe = await authorizedAccountSwitchShutdown(manager)

    XCTAssertFalse(shutdownWasSafe)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: ownerDID)
  }

  func testBackgroundShutdownCannotAbandonSuspensionWithoutAuthorization() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    _ = await MainActor.run { manager.suspendMLSOperations() }

    let shutdownWasSafe = await manager.shutdown()
    XCTAssertFalse(shutdownWasSafe)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
  }

  func testDefaultAccountSwitchShutdownAbandonsSuspensionItCreates() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)

    let shutdownWasSafe = await manager.shutdown()

    XCTAssertTrue(shutdownWasSafe)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(manager.isSuspending)
    XCTAssertFalse(manager.isSyncPaused)
  }

  func testDefaultAccountSwitchShutdownCannotAbandonSupersedingSuspension() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    MLSConversationManager.setShutdownAfterSuspensionCapabilityTestOverride {
      MLSClient.markSuspensionInProgress(reason: "superseding shutdown suspension")
    }

    let shutdownWasSafe = await manager.shutdown()

    XCTAssertFalse(shutdownWasSafe)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
  }

  func testCompletedRustMutationReturnsExactResultWhenSuspensionClosesFutureAdmission()
    async throws
  {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    let bodyCalls = Mutex(0)
    MLSConversationManager.setTrackedRustRuntimePostBodyTestOverride {
      MLSClient.markSuspensionInProgress(reason: "after committed Rust mutation")
    }

    let result = try await manager.withRustAuthoritativeRuntime(
      operation: "committedMutationResult"
    ) { _ in
      bodyCalls.withLock { $0 += 1 }
      return "committed"
    }

    XCTAssertEqual(result, "committed")
    XCTAssertEqual(bodyCalls.withLock { $0 }, 1)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    do {
      _ = try await manager.withRustAuthoritativeRuntime(operation: "futureMutationDenied") {
        _ in
        bodyCalls.withLock { $0 += 1 }
        return "unexpected"
      }
      XCTFail("Expected future Rust admission to remain closed")
    } catch {}
    XCTAssertEqual(bodyCalls.withLock { $0 }, 1)
  }

  func testNewerEmptySuspensionSignalInvalidatesAbandonmentCapability() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    _ = await MainActor.run { manager.suspendMLSOperations() }
    let authorization = await manager.authorizeSuspensionAbandonmentForAccountSwitch()
    XCTAssertNotNil(authorization)
    MLSClient.markSuspensionInProgress(reason: "newer empty suspension signal")

    let shutdownWasSafe = await manager.shutdown(
      accountSwitchSuspensionAuthorization: authorization
    )
    XCTAssertFalse(shutdownWasSafe)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
  }

  func testStaleManagerCannotAuthorizeAnotherManagersSuspensionAbandonment() async throws {
    let owner = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    let stale = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    _ = await MainActor.run { owner.suspendMLSOperations() }

    let staleAuthorization = await stale.authorizeSuspensionAbandonmentForAccountSwitch()

    XCTAssertNil(staleAuthorization)
    let staleShutdownWasSafe = await stale.shutdown()
    XCTAssertFalse(staleShutdownWasSafe)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(owner.isSuspending)
    let ownerAuthorization = await owner.authorizeSuspensionAbandonmentForAccountSwitch()
    XCTAssertNotNil(ownerAuthorization)
    let ownerShutdownWasSafe = await owner.shutdown(
      accountSwitchSuspensionAuthorization: ownerAuthorization
    )
    XCTAssertTrue(ownerShutdownWasSafe)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testAuthorizedAbandonmentPreservesUnrelatedCoreContext() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    try await MLSCoreContext.shared.ensureContext(for: "did:plc:unrelated-context")
    _ = await MainActor.run { manager.suspendMLSOperations() }

    let shutdownWasSafe = await authorizedAccountSwitchShutdown(manager)
    XCTAssertFalse(shutdownWasSafe)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    await MLSCoreContext.shared.removeContext(for: "did:plc:unrelated-context")
  }

  func testAuthorizedAbandonmentPreservesTrackedFFIWork() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    _ = await MainActor.run { manager.suspendMLSOperations() }
    let authorization = await manager.authorizeSuspensionAbandonmentForAccountSwitch()
    MLSClient._beginTrackedFFIOperationForTesting()
    defer { MLSClient._endTrackedFFIOperationForTesting() }

    let shutdownWasSafe = await manager.shutdown(
      accountSwitchSuspensionAuthorization: authorization,
      trackedFFIDrainTimeout: 0.05
    )
    XCTAssertFalse(shutdownWasSafe)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testShutdownWaitsForTrackedRustWorkBeforeRuntimeAndContextClose() async throws {
    try useTemporaryCoreStorageDirectory()
    let userDID = "did:plc:testuser"
    let manager = try await makeManager(protocolAuthorityMode: .rustFull, userDID: userDID)
    let bridge = RecordingLifecycleBridge()
    let operationStarted = DispatchSemaphore(value: 0)
    let releaseOperation = DispatchSemaphore(value: 0)
    let runtimeShutdown = Mutex(false)
    let contextCloseAttempts = Mutex(0)
    bridge.onStartupReconcile = {
      operationStarted.signal()
      releaseOperation.wait()
    }
    bridge.onShutdown = { runtimeShutdown.withLock { $0 = true } }
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: userDID,
      mode: .rustFull,
      bridge: bridge
    )
    try await MLSCoreContext.shared.ensureContext(for: userDID)
    MLSCoreContext.setContextCloseAttemptTestObserver { did, _ in
      guard did == userDID else { return }
      contextCloseAttempts.withLock { $0 += 1 }
    }

    let operation = Task {
      _ = try await manager.withRustAuthoritativeRuntime(operation: "heldShutdownRegression") {
        runtime in
        try runtime.startupReconcile()
      }
    }
    XCTAssertEqual(operationStarted.wait(timeout: .now() + 2), .success)
    let authorization = await MainActor.run {
      _ = manager.suspendMLSOperations()
      return manager.authorizeSuspensionAbandonmentForAccountSwitch()
    }
    XCTAssertNotNil(authorization)
    let shutdown = Task {
      await manager.shutdown(
        accountSwitchSuspensionAuthorization: authorization,
        trackedFFIDrainTimeout: 2
      )
    }
    try await Task.sleep(nanoseconds: 150_000_000)

    XCTAssertFalse(runtimeShutdown.withLock { $0 })
    XCTAssertEqual(contextCloseAttempts.withLock { $0 }, 0)

    releaseOperation.signal()
    _ = try? await operation.value
    let shutdownWasSafe = await shutdown.value
    XCTAssertTrue(shutdownWasSafe)
    XCTAssertTrue(runtimeShutdown.withLock { $0 })
    XCTAssertGreaterThan(contextCloseAttempts.withLock { $0 }, 0)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testShutdownDrainTimeoutInterruptsWithoutDestroyingRuntimeOrContext() async throws {
    try useTemporaryCoreStorageDirectory()
    let userDID = "did:plc:testuser"
    let manager = try await makeManager(protocolAuthorityMode: .rustFull, userDID: userDID)
    let bridge = RecordingLifecycleBridge()
    let operationStarted = DispatchSemaphore(value: 0)
    let releaseOperation = DispatchSemaphore(value: 0)
    let runtimeShutdown = Mutex(false)
    let contextCloseAttempts = Mutex(0)
    bridge.onStartupReconcile = {
      operationStarted.signal()
      releaseOperation.wait()
    }
    bridge.onShutdown = { runtimeShutdown.withLock { $0 = true } }
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: userDID,
      mode: .rustFull,
      bridge: bridge
    )
    try await MLSCoreContext.shared.ensureContext(for: userDID)
    MLSCoreContext.setContextCloseAttemptTestObserver { did, _ in
      guard did == userDID else { return }
      contextCloseAttempts.withLock { $0 += 1 }
    }

    let operation = Task {
      _ = try await manager.withRustAuthoritativeRuntime(operation: "heldShutdownTimeout") {
        runtime in
        try runtime.startupReconcile()
      }
    }
    XCTAssertEqual(operationStarted.wait(timeout: .now() + 2), .success)

    let shutdownWasSafe = await manager.shutdown(
      accountSwitchSuspensionAuthorization: nil,
      trackedFFIDrainTimeout: 0.05
    )

    XCTAssertFalse(shutdownWasSafe)
    XCTAssertFalse(runtimeShutdown.withLock { $0 })
    XCTAssertEqual(contextCloseAttempts.withLock { $0 }, 0)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    releaseOperation.signal()
    _ = try? await operation.value
    MLSCoreContext.emergencyCloseAllContexts()
  }

  func testShutdownLeaseBlocksNoUserGateReleaseUntilTeardownReservationEnds() async throws {
    let manager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    await MainActor.run {
      manager.markSuspensionInProgressForLifecycleTransition(
        reason: "no-user shutdown lease regression"
      )
    }
    let capability = try XCTUnwrap(
      MLSClient.ownedNoUserSuspendedResumeCapability(
        ownerToken: manager.suspensionAbandonmentOwnerToken
      )
    )
    let lease = try XCTUnwrap(
      MLSClient.beginShutdownQuiescenceLease(
        abandonmentCapability: nil,
        excludingUserDID: nil
      )
    )

    XCTAssertNil(
      MLSClient.ownedNoUserSuspendedResumeCapability(
        ownerToken: manager.suspensionAbandonmentOwnerToken
      )
    )
    let releasedDuringLease = await MLSClient.finishNoUserSuspendedResumeCapability(capability)
    XCTAssertFalse(releasedDuringLease)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    MLSClient.cancelShutdownQuiescenceLease(lease)
    XCTAssertNotNil(
      MLSClient.ownedNoUserSuspendedResumeCapability(
        ownerToken: manager.suspensionAbandonmentOwnerToken
      )
    )
  }

  func testStaleAbandonmentCapabilityCreatesUnboundShutdownLease() async throws {
    let userDID = "did:plc:testuser"
    let ownerToken = UUID()
    MLSClient.markSuspensionInProgress(
      reason: "stale shutdown capability setup",
      abandonmentOwnerDID: userDID,
      abandonmentOwnerToken: ownerToken
    )
    let capability = try XCTUnwrap(
      MLSClient.ownedSuspensionAbandonmentCapability(
        for: userDID,
        ownerToken: ownerToken
      )
    )
    MLSClient.markSuspensionInProgress(reason: "supersede shutdown capability")
    let lease = try XCTUnwrap(
      MLSClient.beginShutdownQuiescenceLease(
        abandonmentCapability: capability,
        excludingUserDID: userDID
      )
    )

    let abandoned = await MLSClient.abandonSuspensionAfterSafeShutdown(lease)
    XCTAssertFalse(abandoned)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    MLSClient.cancelShutdownQuiescenceLease(lease)
  }

  func testRapidForegroundResumeWaitsForTrackedFFIBeforeRuntimeRefreshAndRebind() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    let refreshCalls = Mutex(0)
    let rebindCalls = Mutex(0)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.deviceAuthResumeRebindOverride = {
      rebindCalls.withLock { $0 += 1 }
    }
    MLSConversationManager.setSuspendedResumeStateRefreshOverride { _, _ in
      refreshCalls.withLock { $0 += 1 }
    }
    MLSClient._beginTrackedFFIOperationForTesting()
    defer {
      if MLSClient.inFlightFFIOperationCount > 0 {
        MLSClient._endTrackedFFIOperationForTesting()
      }
    }
    _ = await MainActor.run { manager.suspendMLSOperations() }
    manager.deviceAuthBindingRequiresResumeRebind = true

    let resume = Task {
      await manager.resumeMLSOperations(trackedFFIDrainTimeout: 2)
    }
    try await Task.sleep(nanoseconds: 100_000_000)

    XCTAssertTrue(bridge.resumeCalls.isEmpty)
    XCTAssertEqual(refreshCalls.withLock { $0 }, 0)
    XCTAssertEqual(rebindCalls.withLock { $0 }, 0)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)

    MLSClient._endTrackedFFIOperationForTesting()
    let result = await resume.value
    XCTAssertEqual(result, .resumed)
    XCTAssertEqual(bridge.resumeCalls.count, 1)
    XCTAssertEqual(refreshCalls.withLock { $0 }, 1)
    XCTAssertEqual(rebindCalls.withLock { $0 }, 1)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testRapidForegroundResumeDrainTimeoutInterruptsAndKeepsEveryGateClosed() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    let refreshCalls = Mutex(0)
    let rebindCalls = Mutex(0)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.deviceAuthResumeRebindOverride = {
      rebindCalls.withLock { $0 += 1 }
    }
    MLSConversationManager.setSuspendedResumeStateRefreshOverride { _, _ in
      refreshCalls.withLock { $0 += 1 }
    }
    MLSClient._beginTrackedFFIOperationForTesting()
    defer { MLSClient._endTrackedFFIOperationForTesting() }
    _ = await MainActor.run { manager.suspendMLSOperations() }
    manager.deviceAuthBindingRequiresResumeRebind = true

    let result = await manager.resumeMLSOperations(trackedFFIDrainTimeout: 0.05)

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(bridge.resumeCalls.isEmpty)
    XCTAssertEqual(refreshCalls.withLock { $0 }, 0)
    XCTAssertEqual(rebindCalls.withLock { $0 }, 0)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
  }

  func testRapidForegroundResumeCannotConsumeNewerSuspensionWhileDraining() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    let refreshCalls = Mutex(0)
    let rebindCalls = Mutex(0)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.deviceAuthResumeRebindOverride = {
      rebindCalls.withLock { $0 += 1 }
    }
    MLSConversationManager.setSuspendedResumeStateRefreshOverride { _, _ in
      refreshCalls.withLock { $0 += 1 }
    }
    MLSClient._beginTrackedFFIOperationForTesting()
    defer {
      if MLSClient.inFlightFFIOperationCount > 0 {
        MLSClient._endTrackedFFIOperationForTesting()
      }
    }
    _ = await MainActor.run { manager.suspendMLSOperations() }
    manager.deviceAuthBindingRequiresResumeRebind = true
    let resume = Task {
      await manager.resumeMLSOperations(trackedFFIDrainTimeout: 2)
    }
    try await Task.sleep(nanoseconds: 100_000_000)

    _ = await MainActor.run { manager.suspendMLSOperations() }
    MLSClient._endTrackedFFIOperationForTesting()
    let result = await resume.value

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(bridge.resumeCalls.isEmpty)
    XCTAssertEqual(refreshCalls.withLock { $0 }, 0)
    XCTAssertEqual(rebindCalls.withLock { $0 }, 0)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
  }

  func testResumeCapabilityIssuanceRequiresAtomicTrackedFFIQuiescence() throws {
    let userDID = "did:plc:testuser"
    MLSClient.markSuspensionInProgress(reason: "resume capability quiescence regression")
    MLSClient._beginTrackedFFIOperationForTesting()

    XCTAssertNil(MLSClient.beginSuspendedResumeCapability(for: userDID))

    MLSClient._endTrackedFFIOperationForTesting()
    let capability = try XCTUnwrap(MLSClient.beginSuspendedResumeCapability(for: userDID))
    MLSClient.cancelSuspendedResumeCapability(capability)
  }

  func testOrdinaryRustAuthorityIsDeniedWhileSuspended() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    MLSClient.markSuspensionInProgress(reason: "ordinary rust denial test")

    do {
      _ = try await manager.withRustAuthoritativeRuntime(operation: "ordinaryDenied") { runtime in
        try runtime.startupReconcile()
      }
      XCTFail("Expected ordinary Rust authority to remain suspended")
    } catch {
      XCTAssertTrue(MLSClient.isSuspensionInProgress)
      XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
      XCTAssertEqual(bridge.startupReconcileCallCount, 0)
    }
  }

  func testDatabaseCoordinatorForegroundPreparationDoesNotReleaseGlobalGates() {
    MLSClient.markSuspensionInProgress(reason: "database coordinator gate test")

    MLSDatabaseCoordinator.shared.prepareStorageForForegroundResume()

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testSuspendedResumeCoreReloadFailureReleasesNoGate() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    MLSCoreContext.setSuspendedResumeReloadTestOverride { _ in
      throw TestLifecycleError.reloadFailed
    }
    MLSClient.markSuspensionInProgress(reason: "reload failure test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertEqual(bridge.startupReconcileCallCount, 0)
  }

  func testSuspendedResumeAuthoritativeRefreshFailureReleasesNoGate() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    bridge.startupReconcileError = TestLifecycleError.authoritativeRefreshFailed
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    MLSClient.markSuspensionInProgress(reason: "authoritative refresh failure test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertEqual(bridge.startupReconcileCallCount, 1)
  }

  func testSuspendedResumeCancellationReleasesNoGate() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    let refreshStarted = AsyncStream<Void>.makeStream()
    let holdRefresh = AsyncStream<Void>.makeStream()
    MLSConversationManager.setSuspendedResumeStateRefreshOverride { _, _ in
      refreshStarted.continuation.yield()
      for await _ in holdRefresh.stream { break }
    }
    MLSClient.markSuspensionInProgress(reason: "resume cancellation test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }

    let resume = Task { await manager.resumeMLSOperations() }
    for await _ in refreshStarted.stream { break }
    resume.cancel()
    let result = await resume.value

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
  }

  func testRustFullForceCloseRestoreUsesLifecycleReattachInsteadOfFullInitialize() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let staleBridge = RecordingLifecycleBridge()
    let rebuiltBridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: staleBridge
    )
    manager.orchestratorRuntimeResumeFactory = {
      return MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: rebuiltBridge
      )
    }

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)

    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "unit-test force close")
    }

    await manager.resumeMLSOperations()

    XCTAssertTrue(staleBridge.resumeCalls.isEmpty)
    XCTAssertTrue(rebuiltBridge.initializeCalls.isEmpty)
    XCTAssertEqual(
      rebuiltBridge.reattachCalls,
      [
        LifecycleUserReasonCall(
          userDID: "did:plc:testuser",
          reason: "MLSConversationManager.resumeMLSOperations"
        )
      ]
    )
    XCTAssertFalse(manager.rustRuntimeRequiresForegroundRestore)
  }

  func testRustFullForceCloseUsesAuthorizedRealContextBuildBeforeGateRelease() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .rustFull,
      migrateDatabase: true
    )
    let staleBridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: staleBridge
    )
    let rebindEvidence = LifecycleLockedBox<
      (runtime: Bool, context: Bool, identityMatches: Bool, gatesClosed: Bool)?
    >(nil)

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    manager.deviceAuthBindingRequiresResumeRebind = true
    manager.deviceAuthResumeRebindOverride = {
      let hasContext = await MLSCoreContext.shared.hasContext(for: "did:plc:testuser")
      let contextIdentity = await MLSCoreContext.shared.contextIdentity(for: "did:plc:testuser")
      rebindEvidence.withValue {
        $0 = (
          runtime: manager.orchestratorRuntime != nil,
          context: hasContext,
          identityMatches: manager.orchestratorRuntime?.mlsContextIdentity == contextIdentity,
          gatesClosed: MLSClient.isSuspensionInProgress
            && MLSCoreContext.isSuspensionInProgress
            && manager.isSuspending
            && manager.isSyncPaused
        )
      }
    }
    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "real authorized context rebuild test")
    }

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .resumed)
    XCTAssertEqual(rebindEvidence.value?.runtime, true)
    XCTAssertEqual(rebindEvidence.value?.context, true)
    XCTAssertEqual(rebindEvidence.value?.identityMatches, true)
    XCTAssertEqual(rebindEvidence.value?.gatesClosed, true)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    _ = await MLSCoreContext.shared.removeContext(for: "did:plc:testuser")
  }

  func testRustFullExistingRuntimeRebuildsOnAuthoritativelyReloadedCoreContext() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .rustFull,
      migrateDatabase: true
    )
    let originalContext = try await MLSCoreContext.shared.getContext(for: "did:plc:testuser")
    let contextIdentity = ObjectIdentifier(originalContext)
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge(),
      mlsContextIdentity: contextIdentity
    )
    manager.orchestratorRuntime = runtime
    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    let result = await manager.resumeMLSOperations()
    let installedIdentity = await MLSCoreContext.shared.contextIdentity(for: "did:plc:testuser")
    let resumedRuntime = manager.orchestratorRuntime

    XCTAssertEqual(result, .resumed)
    XCTAssertTrue((runtime.bridge as? RecordingLifecycleBridge)?.resumeCalls.isEmpty == true)
    XCTAssertTrue((runtime.bridge as? RecordingLifecycleBridge)?.reattachCalls.isEmpty == true)
    XCTAssertTrue((runtime.bridge as? RecordingLifecycleBridge)?.shutdownCalled == true)
    XCTAssertEqual(runtime.mlsContextIdentity, contextIdentity)
    XCTAssertNotEqual(installedIdentity, ObjectIdentifier(originalContext))
    XCTAssertEqual(resumedRuntime?.mlsContextIdentity, installedIdentity)
    XCTAssertFalse(resumedRuntime === runtime)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    _ = await MLSCoreContext.shared.removeContext(for: "did:plc:testuser")
  }

  func testRustFullRealContextBuildRevocationInstallsNoRuntimeAndReleasesNoGate() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .rustFull,
      migrateDatabase: true
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    let contextAcquired = AsyncStream<Void>.makeStream()
    let releaseContextAdmission = AsyncStream<Void>.makeStream()
    MLSCoreContext.setSuspendedResumeContextAdmissionTestOverride { _ in
      contextAcquired.continuation.yield()
      for await _ in releaseContextAdmission.stream { break }
    }

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "revoked authorized context rebuild test")
    }
    let resume = Task { await manager.resumeMLSOperations() }
    for await _ in contextAcquired.stream { break }

    MLSClient.markSuspensionInProgress(reason: "revoke capability during authorized context await")
    releaseContextAdmission.continuation.yield()
    let result = await resume.value

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    let hasContext = await MLSCoreContext.shared.hasContext(for: "did:plc:testuser")
    XCTAssertFalse(hasContext)
  }

  func testRustFullRealContextBuildCancellationInstallsNoRuntimeOrContext() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .rustFull,
      migrateDatabase: true
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    let contextAcquired = AsyncStream<Void>.makeStream()
    let releaseContextAdmission = AsyncStream<Void>.makeStream()
    MLSCoreContext.setSuspendedResumeContextAdmissionTestOverride { _ in
      contextAcquired.continuation.yield()
      for await _ in releaseContextAdmission.stream { break }
    }

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "cancelled context rebuild test")
    }
    let resume = Task { await manager.resumeMLSOperations() }
    for await _ in contextAcquired.stream { break }

    resume.cancel()
    releaseContextAdmission.continuation.yield()
    let result = await resume.value

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    let hasContext = await MLSCoreContext.shared.hasContext(for: "did:plc:testuser")
    XCTAssertFalse(hasContext)
  }

  func testRustFullPostRebuildRebindFailureDiscardsRuntimeAndContext() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .rustFull,
      migrateDatabase: true
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    manager.deviceAuthBindingRequiresResumeRebind = true
    manager.deviceAuthResumeRebindOverride = {
      throw TestLifecycleError.rebindFailed
    }
    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "post-rebuild rebind failure test")
    }

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    let hasContext = await MLSCoreContext.shared.hasContext(for: "did:plc:testuser")
    XCTAssertFalse(hasContext)
  }

  func testRustFullRuntimeBuildNilDiscardsAuthoritativeReloadContext() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .rustFull,
      migrateDatabase: true
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "runtime build nil cleanup test")
    }
    manager.database = try DatabaseQueue()

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    let hasContext = await MLSCoreContext.shared.hasContext(for: "did:plc:testuser")
    XCTAssertFalse(hasContext)
  }

  func testOldResumeFailureCleanupPreservesNewerGenerationContext() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .rustFull,
      migrateDatabase: true
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    let newerContextIdentity = LifecycleLockedBox<ObjectIdentifier?>(nil)

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    manager.deviceAuthBindingRequiresResumeRebind = true
    manager.deviceAuthResumeRebindOverride = {
      let oldRuntimeIdentity = manager.orchestratorRuntime?.mlsContextIdentity
      MLSClient.markSuspensionInProgress(reason: "newer generation context replacement")
      MLSCoreContext.emergencyCloseAllContexts()
      let newerCapability = try XCTUnwrap(
        MLSClient.beginSuspendedResumeCapability(for: "did:plc:testuser")
      )
      let installedIdentity = try await MLSCoreContext.shared.reloadContextForSuspendedResume(
        for: "did:plc:testuser",
        capability: newerCapability
      )
      XCTAssertNotEqual(installedIdentity, oldRuntimeIdentity)
      newerContextIdentity.withValue { $0 = installedIdentity }
      MLSClient.cancelSuspendedResumeCapability(newerCapability)
      throw TestLifecycleError.rebindFailed
    }
    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "newer generation preservation test")
    }
    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertNil(manager.orchestratorRuntime)
    let survivingContextIdentity = await MLSCoreContext.shared.contextIdentity(
      for: "did:plc:testuser"
    )
    XCTAssertEqual(survivingContextIdentity, newerContextIdentity.value)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    _ = await MLSCoreContext.shared.removeContext(for: "did:plc:testuser")
  }

  func testRustFullSyntheticResumeFailureUsesExplicitRestoreSeam() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let failingBridge = RecordingLifecycleBridge()
    failingBridge.resumeError = TestLifecycleError.resumeFailed
    let failedRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: failingBridge
    )
    let rebuiltBridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = failedRuntime
    manager.orchestratorRuntimeResumeFactory = {
      MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: rebuiltBridge
      )
    }

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    let result = await manager.resumeMLSOperations()
    let rebuiltRuntime = manager.orchestratorRuntime

    XCTAssertEqual(result, .resumed)
    XCTAssertEqual(failingBridge.resumeCalls.count, 1)
    XCTAssertTrue(failingBridge.shutdownCalled)
    XCTAssertEqual(rebuiltBridge.reattachCalls.count, 1)
    XCTAssertFalse(rebuiltRuntime === failedRuntime)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testOrdinaryRuntimeBuildCannotUseSuspendedResumeAdmission() async throws {
    try useTemporaryCoreStorageDirectory()
    let manager = try await makeManager(
      protocolAuthorityMode: .rustFull,
      migrateDatabase: true
    )
    MLSClient.markSuspensionInProgress(reason: "ordinary runtime build denial test")

    let runtime = await manager.buildOrchestratorRuntime()

    XCTAssertNil(runtime)
    let hasContext = await MLSCoreContext.shared.hasContext(for: "did:plc:testuser")
    XCTAssertFalse(hasContext)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testRustFullRestoreRejectsMismatchedRuntimeDIDAndKeepsGatesClosed() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: RecordingLifecycleBridge()
    )
    let mismatchedBridge = RecordingLifecycleBridge()
    manager.orchestratorRuntimeResumeFactory = {
      MLSOrchestratorRuntime(
        userDID: "did:plc:attacker",
        mode: .rustFull,
        bridge: mismatchedBridge
      )
    }

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "mismatched runtime DID test")
    }

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertTrue(mismatchedBridge.shutdownCalled)
    XCTAssertTrue(mismatchedBridge.reattachCalls.isEmpty)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
  }

  func testSuspendInvalidatesBindingAndRuntimeRestoreRebindsWithReplacementSigner() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let resumeOrder = LifecycleLockedBox<[String]>([])
    let staleBridge = RecordingLifecycleBridge()
    let rebuiltBridge = RecordingLifecycleBridge()
    let bindingAPI = LifecycleDeviceAuthBindingAPI()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: staleBridge
    )
    manager.orchestratorRuntimeResumeFactory = {
      resumeOrder.withValue { $0.append("runtimeRestore") }
      return MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: rebuiltBridge
      )
    }
    manager.deviceAuthBindingAPIOverride = bindingAPI
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }

    _ = try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    let initialBeginCount = await bindingAPI.beginCount()
    XCTAssertEqual(initialBeginCount, 1)
    XCTAssertEqual(staleBridge.deviceAuthChallenges.count, 1)
    MLSCoreContext.setSuspendedResumeReloadTestOverride { _ in
      resumeOrder.withValue { $0.append("coreReload") }
    }
    rebuiltBridge.onStartupReconcile = {
      resumeOrder.withValue { $0.append("authoritativeRefresh") }
    }
    await bindingAPI.setOnBegin {
      resumeOrder.withValue { $0.append("deviceAuthRebind") }
    }

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    let suspendedStatus = await MLSClient.shared.deviceAuthBindingStatus(
      for: "did:plc:testuser")
    XCTAssertNil(suspendedStatus)

    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "unit-test force close")
    }
    await bindingAPI.holdNextBegin()
    let resume = Task { await manager.resumeMLSOperations() }
    await bindingAPI.waitUntilBeginHeld()

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)

    await bindingAPI.releaseBegin()
    let result = await resume.value

    XCTAssertEqual(result, .resumed)
    let resumedBeginCount = await bindingAPI.beginCount()
    XCTAssertEqual(resumedBeginCount, 2)
    XCTAssertEqual(staleBridge.deviceAuthChallenges.count, 1)
    XCTAssertEqual(rebuiltBridge.deviceAuthChallenges.count, 1)
    let reboundStatus = await MLSClient.shared.deviceAuthBindingStatus(
      for: "did:plc:testuser")
    XCTAssertNotNil(reboundStatus)
    XCTAssertFalse(manager.isSuspending)
    XCTAssertEqual(
      resumeOrder.value,
      ["runtimeRestore", "coreReload", "authoritativeRefresh", "deviceAuthRebind"]
    )
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")
  }

  func testLegacyClearDuringHeldRuntimeRestoreRebindRevokesResumeAndFailsClosed() async throws {
    let userDID = "did:plc:testuser"
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let staleBridge = RecordingLifecycleBridge()
    let rebuiltBridge = RecordingLifecycleBridge()
    let bindingAPI = LifecycleDeviceAuthBindingAPI()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: userDID,
      mode: .rustFull,
      bridge: staleBridge
    )
    manager.orchestratorRuntimeResumeFactory = {
      MLSOrchestratorRuntime(
        userDID: userDID,
        mode: .rustFull,
        bridge: rebuiltBridge
      )
    }
    manager.deviceAuthBindingAPIOverride = bindingAPI
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }

    _ = try await manager.bindRustDeviceAuthentication(userDid: userDID)
    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)
    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "legacy-clear rebind race")
    }
    await bindingAPI.holdNextBegin()
    let resume = Task { await manager.resumeMLSOperations() }
    await bindingAPI.waitUntilBeginHeld()

    MLSCoreContext.clearSuspensionFlag()
    XCTAssertFalse(
      MLSClient.clearSuspensionFlag(reason: "legacy clear during held resume rebind")
    )
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    await bindingAPI.releaseBegin()
    let result = await resume.value
    let resumedBeginCount = await bindingAPI.beginCount()
    let reboundStatus = await MLSClient.shared.deviceAuthBindingStatus(for: userDID)

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertEqual(resumedBeginCount, 2)
    XCTAssertEqual(staleBridge.deviceAuthChallenges.count, 1)
    XCTAssertEqual(rebuiltBridge.deviceAuthChallenges.count, 0)
    XCTAssertNil(reboundStatus)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
  }

  func testRustResumeBindingFailureNeverReleasesGlobalSuspensionGate() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    let bindingAPI = LifecycleDeviceAuthBindingAPI()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.deviceAuthBindingAPIOverride = bindingAPI
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }

    _ = try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    let initialBeginCount = await bindingAPI.beginCount()
    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertEqual(initialBeginCount, 1)
    XCTAssertTrue(rustPrepared)

    await bindingAPI.holdNextBegin()
    await bindingAPI.failNextCompletion()
    let resume = Task { await manager.resumeMLSOperations() }
    await bindingAPI.waitUntilBeginHeld()

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)

    await bindingAPI.releaseBegin()
    await resume.value

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertTrue(manager.deviceAuthBindingRequiresResumeRebind)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")
  }

  func testNewSuspensionRevokesHeldRustResumeBindingPermit() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    let bindingAPI = LifecycleDeviceAuthBindingAPI()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.deviceAuthBindingAPIOverride = bindingAPI
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }

    _ = try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)

    await bindingAPI.holdNextBegin()
    let resume = Task { await manager.resumeMLSOperations() }
    await bindingAPI.waitUntilBeginHeld()
    MLSClient.markSuspensionInProgress(reason: "new suspension during held resume rebind")
    await bindingAPI.releaseBegin()
    await resume.value

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertTrue(manager.deviceAuthBindingRequiresResumeRebind)
    let status = await MLSClient.shared.deviceAuthBindingStatus(for: "did:plc:testuser")
    XCTAssertNil(status)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")
  }

  func testSwiftLegacyResumeReestablishesInvalidatedDeviceAuthentication() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    let recorder = LifecycleResumeRebindRecorder()
    manager.deviceAuthBindingRequiresResumeRebind = true
    manager.deviceAuthResumeRebindOverride = {
      await recorder.record()
    }
    MLSClient.markSuspensionInProgress(reason: "legacy resume rebind test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }

    await manager.resumeMLSOperations()

    let rebindCount = await recorder.count()
    XCTAssertEqual(rebindCount, 1)
    XCTAssertFalse(manager.deviceAuthBindingRequiresResumeRebind)
    XCTAssertFalse(manager.isSuspending)
  }

  func testExternalSuspensionSnapshotForcesRebindBeforeGateRelease() async throws {
    let userDID = "did:plc:testuser"
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: userDID,
      deviceID: "device-external-suspend",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: userDID,
        deviceID: "device-external-suspend"
      )
    ) { _ in Data(repeating: 0x45, count: 64) }
    let recorder = LifecycleResumeRebindRecorder()
    manager.deviceAuthResumeRebindOverride = {
      XCTAssertTrue(MLSClient.isSuspensionInProgress)
      XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
      await recorder.record()
    }

    MLSClient.markSuspensionInProgress(reason: "external suspension snapshot rebind test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }
    XCTAssertFalse(manager.deviceAuthBindingRequiresResumeRebind)

    let result = await manager.resumeMLSOperations()
    let rebindCount = await recorder.count()

    XCTAssertEqual(result, .resumed)
    XCTAssertEqual(rebindCount, 1)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(manager.isSuspending)
    XCTAssertFalse(manager.isSyncPaused)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: userDID)
  }

  func testExternalSuspensionSnapshotRebindFailureLeavesEveryGateClosed() async throws {
    let userDID = "did:plc:testuser"
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: userDID,
      deviceID: "device-external-suspend-failure",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: userDID,
        deviceID: "device-external-suspend-failure"
      )
    ) { _ in Data(repeating: 0x46, count: 64) }
    let recorder = LifecycleResumeRebindRecorder()
    manager.deviceAuthResumeRebindOverride = {
      XCTAssertTrue(MLSClient.isSuspensionInProgress)
      XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
      await recorder.record()
      throw TestLifecycleError.rebindFailed
    }

    MLSClient.markSuspensionInProgress(reason: "external suspension snapshot failure test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }
    XCTAssertFalse(manager.deviceAuthBindingRequiresResumeRebind)

    let result = await manager.resumeMLSOperations()
    let rebindCount = await recorder.count()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertEqual(rebindCount, 1)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: userDID)
  }

  func testStaleManagerCannotTakeOverBoundOwnerResumeCapability() async throws {
    let ownerDID = "did:plc:owner"
    let staleDID = "did:plc:stale"
    let ownerManager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: ownerDID
    )
    let staleManager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: staleDID
    )
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: ownerDID,
      deviceID: "device-owner-takeover",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: ownerDID,
        deviceID: "device-owner-takeover"
      )
    ) { _ in Data(repeating: 0x4A, count: 64) }
    let ownerRebind = LifecycleResumeRebindRecorder()
    ownerManager.deviceAuthResumeRebindOverride = {
      await ownerRebind.record()
    }
    let invalidatedDIDs = LifecycleLockedBox<[String]>([])
    MLSConversationManager.setSuspendedResumeOtherBindingInvalidationOverride { did in
      invalidatedDIDs.withValue { $0.append(did) }
    }

    MLSClient.markSuspensionInProgress(reason: "two-manager takeover test")
    await MainActor.run {
      ownerManager.isSuspending = true
      ownerManager.isSyncPaused = true
      staleManager.isSuspending = true
      staleManager.isSyncPaused = true
    }

    let staleResult = await staleManager.resumeMLSOperations()

    XCTAssertEqual(staleResult, .failedStillSuspended)
    XCTAssertTrue(invalidatedDIDs.value.isEmpty)
    XCTAssertTrue(
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: ownerDID)
    )
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(staleManager.isSuspending)
    XCTAssertTrue(staleManager.isSyncPaused)

    let ownerResult = await ownerManager.resumeMLSOperations()
    let ownerRebindCount = await ownerRebind.count()

    XCTAssertEqual(ownerResult, .resumed)
    XCTAssertEqual(ownerRebindCount, 1)
    XCTAssertTrue(invalidatedDIDs.value.isEmpty)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(ownerManager.isSuspending)
    XCTAssertFalse(ownerManager.isSyncPaused)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: ownerDID)
  }

  func testSameDIDNonOwnerCannotResumeOwnerBoundEmptySnapshot() async throws {
    let userDID = "did:plc:empty-snapshot-owner"
    let exactOwner = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: userDID
    )
    let sameDIDNonOwner = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: userDID
    )
    await MainActor.run {
      exactOwner.markSuspensionInProgressForLifecycleTransition(
        reason: "owner-bound empty snapshot"
      )
      sameDIDNonOwner.isSuspending = true
      sameDIDNonOwner.isSyncPaused = true
    }

    let stolenCapability = MLSClient.beginSuspendedResumeCapability(
      for: userDID,
      ownerToken: sameDIDNonOwner.suspensionAbandonmentOwnerToken
    )
    XCTAssertNil(
      stolenCapability,
      "Snapshot membership must not bypass the recorded lifecycle owner token"
    )
    if let stolenCapability {
      MLSClient.cancelSuspendedResumeCapability(stolenCapability)
    }

    let nonOwnerResult = await sameDIDNonOwner.resumeMLSOperations()

    XCTAssertEqual(nonOwnerResult, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(sameDIDNonOwner.isSuspending)
    XCTAssertTrue(sameDIDNonOwner.isSyncPaused)

    let ownerResult = await exactOwner.resumeMLSOperations()

    XCTAssertEqual(ownerResult, .resumed)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testSameDIDNonOwnerCannotResumeOwnerBoundActiveBindingSnapshot() async throws {
    let userDID = "did:plc:active-snapshot-owner"
    let exactOwner = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: userDID
    )
    let sameDIDNonOwner = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: userDID
    )
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: userDID,
      deviceID: "device-owner-token",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: userDID,
        deviceID: "device-owner-token"
      )
    ) { _ in Data(repeating: 0x5A, count: 64) }
    exactOwner.deviceAuthResumeRebindOverride = {}
    await MainActor.run {
      exactOwner.markSuspensionInProgressForLifecycleTransition(
        reason: "owner-bound active binding snapshot"
      )
      sameDIDNonOwner.isSuspending = true
      sameDIDNonOwner.isSyncPaused = true
    }

    let stolenCapability = MLSClient.beginSuspendedResumeCapability(
      for: userDID,
      ownerToken: sameDIDNonOwner.suspensionAbandonmentOwnerToken
    )
    XCTAssertNil(
      stolenCapability,
      "Active-binding snapshot membership must not bypass the recorded owner token"
    )
    if let stolenCapability {
      MLSClient.cancelSuspendedResumeCapability(stolenCapability)
    }

    let nonOwnerResult = await sameDIDNonOwner.resumeMLSOperations()

    XCTAssertEqual(nonOwnerResult, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    let ownerResult = await exactOwner.resumeMLSOperations()

    XCTAssertEqual(ownerResult, .resumed)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: userDID)
  }

  func testDIDManagerCannotResumeNoUserOwnedEmptySnapshot() async throws {
    let noUserOwner = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: nil
    )
    let didManager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: "did:plc:no-user-owner-claimant"
    )
    await MainActor.run {
      noUserOwner.markSuspensionInProgressForLifecycleTransition(
        reason: "no-user owner blocks DID resume"
      )
      didManager.isSuspending = true
      didManager.isSyncPaused = true
    }

    let result = await didManager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(didManager.isSuspending)
    XCTAssertTrue(didManager.isSyncPaused)
  }

  func testResumeKeepsGlobalSuspensionGateUntilDeviceAuthRebindSucceeds() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    let rebindStarted = AsyncStream<Void>.makeStream()
    let releaseRebind = AsyncStream<Void>.makeStream()
    manager.deviceAuthBindingRequiresResumeRebind = true
    manager.deviceAuthResumeRebindOverride = {
      rebindStarted.continuation.yield()
      for await _ in releaseRebind.stream { break }
    }
    MLSClient.markSuspensionInProgress(reason: "successful rebind gate test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }

    let resume = Task { await manager.resumeMLSOperations() }
    for await _ in rebindStarted.stream { break }

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)

    releaseRebind.continuation.yield()
    await resume.value

    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(manager.isSuspending)
    XCTAssertFalse(manager.isSyncPaused)
    XCTAssertFalse(manager.deviceAuthBindingRequiresResumeRebind)
  }

  func testResumeKeepsGlobalSuspensionGateWhenDeviceAuthRebindFails() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    let rebindStarted = AsyncStream<Void>.makeStream()
    let releaseRebind = AsyncStream<Void>.makeStream()
    manager.deviceAuthBindingRequiresResumeRebind = true
    manager.deviceAuthResumeRebindOverride = {
      rebindStarted.continuation.yield()
      for await _ in releaseRebind.stream { break }
      throw TestLifecycleError.rebindFailed
    }
    MLSClient.markSuspensionInProgress(reason: "failed rebind gate test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }

    let resume = Task { await manager.resumeMLSOperations() }
    for await _ in rebindStarted.stream { break }

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)

    releaseRebind.continuation.yield()
    await resume.value

    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertTrue(manager.deviceAuthBindingRequiresResumeRebind)
  }

  func testNewSuspensionAtFinalReleaseRestoresEveryGate() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    manager.deviceAuthBindingRequiresResumeRebind = true
    manager.deviceAuthResumeRebindOverride = {}
    MLSClient.markSuspensionInProgress(reason: "final release interleaving test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }
    MLSConversationManager.setSuspendedResumeFinalReleaseOverride {
      MLSClient.markSuspensionInProgress(reason: "new suspension at final release")
    }

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertTrue(manager.deviceAuthBindingRequiresResumeRebind)
  }

  func testBindingInvalidatedAtFinalReleaseKeepsEveryGateClosed() async throws {
    let userDID = "did:plc:testuser"
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: userDID,
      deviceID: "device-final-release",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: userDID,
        deviceID: "device-final-release"
      )
    ) { _ in Data(repeating: 0x4D, count: 64) }
    manager.deviceAuthResumeRebindOverride = {}
    _ = await MainActor.run { manager.suspendMLSOperations() }
    MLSConversationManager.setSuspendedResumeFinalReleaseOverride {
      await MLSClient.shared.invalidateDeviceAuthBinding(for: userDID)
    }

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    let bindingStatus = await MLSClient.shared.deviceAuthBindingStatus(for: userDID)
    XCTAssertNil(bindingStatus)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertTrue(manager.deviceAuthBindingRequiresResumeRebind)
  }

  func testResumeInvalidatesDormantAccountBindingBeforeOpeningGlobalGates() async throws {
    let activeDID = "did:plc:testuser"
    let dormantDID = "did:plc:dormant"
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    let activeAPI = LifecycleDeviceAuthBindingAPI(did: activeDID, deviceID: "device-active")
    let dormantAPI = LifecycleDeviceAuthBindingAPI(did: dormantDID, deviceID: "device-dormant")
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: activeDID,
      deviceID: "device-active",
      apiOverride: activeAPI
    ) { _ in Data(repeating: 0x41, count: 64) }
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: dormantDID,
      deviceID: "device-dormant",
      apiOverride: dormantAPI
    ) { _ in Data(repeating: 0x42, count: 64) }

    manager.deviceAuthBindingRequiresResumeRebind = true
    manager.deviceAuthResumeRebindOverride = {}
    MLSClient.markSuspensionInProgress(reason: "multi-account resume test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }
    let invalidationStarted = AsyncStream<String>.makeStream()
    let releaseInvalidation = AsyncStream<Void>.makeStream()
    MLSConversationManager.setSuspendedResumeOtherBindingInvalidationOverride { did in
      invalidationStarted.continuation.yield(did)
      for await _ in releaseInvalidation.stream { break }
    }

    let resume = Task { await manager.resumeMLSOperations() }
    var invalidatedDID: String?
    for await did in invalidationStarted.stream {
      invalidatedDID = did
      break
    }

    XCTAssertEqual(invalidatedDID, dormantDID)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    let heldDormantStatus = await MLSClient.shared.deviceAuthBindingStatus(for: dormantDID)
    XCTAssertNil(heldDormantStatus)

    releaseInvalidation.continuation.yield()
    let result = await resume.value

    XCTAssertEqual(result, .resumed)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(manager.isSuspending)
    XCTAssertFalse(manager.isSyncPaused)
    let resumedDormantStatus = await MLSClient.shared.deviceAuthBindingStatus(for: dormantDID)
    XCTAssertNil(resumedDormantStatus)

    await MLSClient.shared.invalidateDeviceAuthBinding(for: activeDID)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: dormantDID)
  }

  func testExactForegroundOwnerWithoutBindingCanInvalidateDormantSnapshotButOtherManagersCannot()
    async throws
  {
    let dormantDID = "did:plc:dormant-owner-tuple"
    let foregroundDID = "did:plc:foreground-owner-tuple"
    let staleDID = "did:plc:stale-owner-tuple"
    let exactOwner = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: foregroundDID
    )
    let sameDIDNonOwner = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: foregroundDID
    )
    let staleManager = try await makeManager(
      protocolAuthorityMode: .swiftLegacy,
      userDID: staleDID
    )
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: dormantDID,
      deviceID: "device-dormant-owner-tuple",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: dormantDID,
        deviceID: "device-dormant-owner-tuple"
      )
    ) { _ in Data(repeating: 0x55, count: 64) }
    let invalidatedDIDs = LifecycleLockedBox<[String]>([])
    MLSConversationManager.setSuspendedResumeOtherBindingInvalidationOverride { did in
      invalidatedDIDs.withValue { $0.append(did) }
    }
    await MainActor.run {
      exactOwner.markSuspensionInProgressForLifecycleTransition(
        reason: "foreground owner without binding"
      )
      sameDIDNonOwner.isSuspending = true
      sameDIDNonOwner.isSyncPaused = true
      staleManager.isSuspending = true
      staleManager.isSyncPaused = true
    }

    let staleResult = await staleManager.resumeMLSOperations()
    let sameDIDNonOwnerResult = await sameDIDNonOwner.resumeMLSOperations()
    XCTAssertEqual(staleResult, .failedStillSuspended)
    XCTAssertEqual(sameDIDNonOwnerResult, .failedStillSuspended)
    XCTAssertTrue(invalidatedDIDs.value.isEmpty)
    XCTAssertTrue(
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: dormantDID)
    )
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    let ownerResult = await exactOwner.resumeMLSOperations()
    let dormantStatusAfterOwnerResume = await MLSClient.shared.deviceAuthBindingStatus(
      for: dormantDID
    )

    XCTAssertEqual(ownerResult, .resumed)
    XCTAssertEqual(invalidatedDIDs.value, [dormantDID])
    XCTAssertNil(dormantStatusAfterOwnerResume)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
    XCTAssertFalse(exactOwner.isSuspending)
    XCTAssertFalse(exactOwner.isSyncPaused)
  }

  func testDormantAccountInvalidationFailureLeavesEveryGateClosed() async throws {
    let activeDID = "did:plc:testuser"
    let dormantDID = "did:plc:dormant-failure"
    let manager = try await makeManager(protocolAuthorityMode: .swiftLegacy)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: activeDID,
      deviceID: "device-active-failure",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: activeDID,
        deviceID: "device-active-failure"
      )
    ) { _ in Data(repeating: 0x43, count: 64) }
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: dormantDID,
      deviceID: "device-dormant-failure",
      apiOverride: LifecycleDeviceAuthBindingAPI(
        did: dormantDID,
        deviceID: "device-dormant-failure"
      )
    ) { _ in Data(repeating: 0x44, count: 64) }

    manager.deviceAuthBindingRequiresResumeRebind = true
    manager.deviceAuthResumeRebindOverride = {}
    MLSClient.markSuspensionInProgress(reason: "multi-account invalidation failure test")
    await MainActor.run {
      manager.isSuspending = true
      manager.isSyncPaused = true
    }
    MLSConversationManager.setSuspendedResumeOtherBindingInvalidationOverride { _ in
      throw TestLifecycleError.rebindFailed
    }

    let result = await manager.resumeMLSOperations()

    XCTAssertEqual(result, .failedStillSuspended)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    XCTAssertTrue(manager.isSuspending)
    XCTAssertTrue(manager.isSyncPaused)
    XCTAssertTrue(manager.deviceAuthBindingRequiresResumeRebind)
    let dormantStatus = await MLSClient.shared.deviceAuthBindingStatus(for: dormantDID)
    XCTAssertNil(dormantStatus)

    await MLSClient.shared.invalidateDeviceAuthBinding(for: activeDID)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: dormantDID)
  }

  func testRustFullForceCloseRestoreRerunsStartupReconcileOnRebuiltRuntime() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let staleBridge = RecordingLifecycleBridge()
    let rebuiltBridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: staleBridge
    )
    manager.orchestratorRuntimeResumeFactory = {
      MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: rebuiltBridge
      )
    }

    await manager.validateGroupStates()
    XCTAssertEqual(staleBridge.startupReconcileCallCount, 1)

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)

    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "unit-test force close")
    }

    await manager.resumeMLSOperations()
    await manager.validateGroupStates()

    XCTAssertEqual(rebuiltBridge.startupReconcileCallCount, 1)
  }

  func testRustFullForceCloseInvalidatesRuntimeAndAvoidsResumingStaleBridge() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let staleBridge = RecordingLifecycleBridge()
    let rebuiltBridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: staleBridge
    )
    manager.orchestratorRuntimeResumeFactory = {
      MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: rebuiltBridge
      )
    }

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)

    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "unit-test force close")
    }

    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertTrue(manager.rustRuntimeRequiresForegroundRestore)

    await manager.resumeMLSOperations()

    XCTAssertTrue(
      staleBridge.resumeCalls.isEmpty,
      "stale runtime must not be resumed after app-level force close"
    )
    XCTAssertTrue(
      rebuiltBridge.resumeCalls.isEmpty,
      "freshly rebuilt runtime should use lifecycle reattach, not resume the stale engine"
    )
    XCTAssertTrue(manager.orchestratorRuntime?.bridge === rebuiltBridge)
    XCTAssertFalse(manager.isSuspending)
  }

  func testReloadWhileSuspendedDefersPostReloadSyncUntilResume() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    MLSClient.markSuspensionInProgress(reason: "reload while suspended test")
    await MainActor.run {
      manager.isInitialized = true
      manager.isSuspending = true
      manager.isSyncPaused = true
    }

    await manager.reloadStateFromDisk()
    try? await Task.sleep(nanoseconds: 50_000_000)

    XCTAssertTrue(bridge.syncCalls.isEmpty)
    XCTAssertTrue(manager.postReloadSyncPending)

    await manager.resumeMLSOperations()
    try? await Task.sleep(nanoseconds: 50_000_000)

    XCTAssertEqual(
      bridge.resumeCalls,
      [LifecycleReasonCall(reason: "MLSConversationManager.resumeMLSOperations")]
    )
    XCTAssertFalse(manager.postReloadSyncPending)
  }

  func testRustFullResumeStaysSuspendedWhenRuntimeCannotBeRestored() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.orchestratorRuntimeResumeFactory = { nil }

    let rustPrepared = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPrepared)

    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "unit-test force close")
    }

    await manager.resumeMLSOperations()

    XCTAssertTrue(
      bridge.resumeCalls.isEmpty,
      "stale runtime must not be resumed once app-level force close invalidates it"
    )
    XCTAssertTrue(
      manager.isSuspending, "resume should not silently re-enable work without a runtime")
    XCTAssertNil(manager.orchestratorRuntime)
  }

  func testSwiftHostMaterialKeepsAppContentAndRustStorageSeparate() throws {
    let baseDirectory = FileManager.default.temporaryDirectory
      .appendingPathComponent(UUID().uuidString, isDirectory: true)
    try FileManager.default.createDirectory(at: baseDirectory, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(baseDirectory)
    defer {
      MLSStoragePaths.setBaseDirectoryOverride(nil)
      try? FileManager.default.removeItem(at: baseDirectory)
    }

    let manager = MLSGRDBManager()
    let appContentPath = manager.appContentDatabasePath(for: "did:plc:testuser")
    let rustStoragePath = manager.rustStateDatabasePath(for: "did:plc:testuser")

    XCTAssertNotEqual(appContentPath, rustStoragePath)
    XCTAssertTrue(appContentPath.path.contains("/MLS/"))
    XCTAssertTrue(rustStoragePath.path.contains("/mls-state/"))

    let context = try MlsContext(
      storagePath: rustStoragePath.path,
      encryptionKey: "test-key",
      keychain: MLSKeychainAccessBridge()
    )
    let status = context.storageLifecycleStatus()

    XCTAssertEqual(status.state, .open)
    XCTAssertFalse(status.isBusy)
    XCTAssertEqual(status.busyContexts, 0)
    XCTAssertEqual(status.interruptibleContexts, 2)
    XCTAssertTrue(FileManager.default.fileExists(atPath: rustStoragePath.path))
    XCTAssertFalse(
      FileManager.default.fileExists(atPath: appContentPath.path),
      "creating the Rust context should not synthesize a Swift projection database"
    )
  }

  private func authorizedAccountSwitchShutdown(
    _ manager: MLSConversationManager
  ) async -> Bool {
    let authorization = await manager.authorizeSuspensionAbandonmentForAccountSwitch()
    return await manager.shutdown(accountSwitchSuspensionAuthorization: authorization)
  }

  private func makeManager(
    protocolAuthorityMode: MLSProtocolAuthorityMode,
    migrateDatabase: Bool = false,
    userDID: String? = "did:plc:testuser"
  ) async throws -> MLSConversationManager {
    let databasePath = URL(fileURLWithPath: NSTemporaryDirectory())
      .appendingPathComponent(UUID().uuidString)
      .appendingPathExtension("sqlite")
      .path
    let database = try DatabasePool(path: databasePath)
    if migrateDatabase {
      try MLSGRDBManager.makeMigrator().migrate(database)
    }
    let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: atProtoClient,
      environment: .custom(serviceDID: "did:web:example.com#atproto_mls")
    )
    return MLSConversationManager(
      apiClient: apiClient,
      database: database,
      userDid: userDID,
      atProtoClient: atProtoClient,
      protocolAuthorityMode: protocolAuthorityMode
    )
  }

  private func useTemporaryCoreStorageDirectory() throws {
    let directory = FileManager.default.temporaryDirectory
      .appendingPathComponent("mls-full-rust-lifecycle-tests", isDirectory: true)
      .appendingPathComponent(UUID().uuidString, isDirectory: true)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    temporaryCoreStorageDirectory = directory
    MLSStoragePaths.setBaseDirectoryOverride(directory)
    MLSCoreContext.setSuspendedResumeReloadTestOverride(nil)
  }
}

private struct LifecycleCall: Equatable {
  let reason: String
  let deadlineMs: UInt64
}

private struct LifecycleReasonCall: Equatable {
  let reason: String
}

private struct LifecycleUserReasonCall: Equatable {
  let userDID: String
  let reason: String
}

private final class RecordingLifecycleBridge: OrchestratorBridge {
  var prepareResult = FfiSuspendResult(
    acceptingNewWork: false,
    interruptedContexts: 1
  )
  var storageLifecycleStatusResult = StorageLifecycleStatus(
    state: .open,
    interruptibleContexts: 2,
    isBusy: false,
    busyContexts: 0,
    lastOperationLabel: "initialized"
  )
  private(set) var initializeCalls: [String] = []
  private(set) var prepareCalls: [LifecycleCall] = []
  private(set) var reattachCalls: [LifecycleUserReasonCall] = []
  private(set) var resumeCalls: [LifecycleReasonCall] = []
  private(set) var syncCalls: [Bool] = []
  private(set) var interruptCalls: [LifecycleReasonCall] = []
  private(set) var emergencyCloseCalls: [LifecycleReasonCall] = []
  private(set) var startupReconcileCallCount = 0
  private(set) var deviceAuthChallenges: [Data] = []
  private(set) var recoveryStateCalls: [String] = []
  private(set) var shutdownCalled = false
  private(set) var shutdownCallCount = 0
  var prepareError: Error?
  var resumeError: Error?
  var startupReconcileError: Error?
  var onStartupReconcile: (@Sendable () -> Void)?
  var onShutdown: (@Sendable () -> Void)?
  var onInitialize: (@Sendable () -> Void)?
  var onReattach: (@Sendable () -> Void)?
  var onRecoveryState: (@Sendable () -> Void)?

  init() {
    super.init(noPointer: .init())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    super.init(unsafeFromRawPointer: pointer)
  }

  override func initialize(userDid: String) throws {
    initializeCalls.append(userDid)
    onInitialize?()
  }

  override func prepareForSuspend(reason: String, deadlineMs: UInt64) throws -> FfiSuspendResult {
    prepareCalls.append(LifecycleCall(reason: reason, deadlineMs: deadlineMs))
    if let prepareError {
      throw prepareError
    }
    return prepareResult
  }

  override func storageLifecycleStatus() -> StorageLifecycleStatus {
    storageLifecycleStatusResult
  }

  override func startupReconcile() throws -> FfiStartupReconcileReport {
    startupReconcileCallCount += 1
    onStartupReconcile?()
    if let startupReconcileError { throw startupReconcileError }
    return FfiStartupReconcileReport(
      scanned: 0,
      healthy: 0,
      needsRejoin: 0,
      resetPending: 0,
      unrecoverableLocal: 0
    )
  }

  override func reattachAfterSuspend(userDid: String, reason: String) throws {
    reattachCalls.append(LifecycleUserReasonCall(userDID: userDid, reason: reason))
    onReattach?()
  }

  override func resumeFromSuspend(reason: String) throws {
    resumeCalls.append(LifecycleReasonCall(reason: reason))
    if let resumeError { throw resumeError }
  }

  override func syncWithServer(fullSync: Bool) throws {
    syncCalls.append(fullSync)
  }

  override func signDeviceAuthChallenge(challenge: Data) throws -> Data {
    deviceAuthChallenges.append(challenge)
    return Data(repeating: 0x42, count: 64)
  }

  override func listConversations(userDid: String) throws -> [FfiConversationView] {
    []
  }

  override func getConversationRecoveryState(
    conversationId: String
  ) throws -> FfiConversationRecoveryState {
    recoveryStateCalls.append(conversationId)
    onRecoveryState?()
    return .healthy
  }

  override func interruptStorage(reason: String) throws {
    interruptCalls.append(LifecycleReasonCall(reason: reason))
  }

  override func emergencyClose(reason: String) throws {
    emergencyCloseCalls.append(LifecycleReasonCall(reason: reason))
  }

  override func shutdown() {
    shutdownCalled = true
    shutdownCallCount += 1
    onShutdown?()
  }
}

private actor LifecycleDeviceAuthBindingAPI: MLSDeviceAuthBindingAPI {
  private let snapshotDID: String
  private let completionDeviceID: String
  private var beginCalls = 0
  private var shouldHoldNextBegin = false
  private var beginIsHeld = false
  private var beginHeldContinuation: CheckedContinuation<Void, Never>?
  private var beginReleaseContinuation: CheckedContinuation<Void, Never>?
  private var completionError: Error?
  private var onBegin: (@Sendable () async -> Void)?

  init(did: String = "did:plc:testuser", deviceID: String = "device-1") {
    snapshotDID = did
    completionDeviceID = deviceID
  }

  func snapshot() -> MLSDeviceAuthClientSnapshot {
    MLSDeviceAuthClientSnapshot(
      did: snapshotDID,
      authenticationMode: .gateway,
      authenticationGeneration: 1
    )
  }

  func commitIfSnapshotMatches(
    _ expected: MLSDeviceAuthClientSnapshot,
    operation: @Sendable () -> Bool
  ) -> Bool {
    guard snapshot() == expected else { return false }
    return operation()
  }

  func begin(deviceID: String) async -> MLSDeviceAuthBindingChallenge {
    beginCalls += 1
    if let onBegin { await onBegin() }
    if shouldHoldNextBegin {
      shouldHoldNextBegin = false
      beginIsHeld = true
      beginHeldContinuation?.resume()
      beginHeldContinuation = nil
      await withCheckedContinuation { continuation in
        beginReleaseContinuation = continuation
      }
      beginIsHeld = false
    }
    return MLSDeviceAuthBindingChallenge(
      challengeID: "challenge-\(beginCalls)",
      challenge: Data([UInt8(beginCalls)]),
      expiresAt: Date().addingTimeInterval(60),
      bindingVersion: 1
    )
  }

  func begin(
    deviceID: String,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingChallenge {
    guard snapshot() == expected else { throw MLSDeviceAuthBindingError.sessionChanged }
    let challenge = await begin(deviceID: deviceID)
    guard snapshot() == expected else { throw MLSDeviceAuthBindingError.sessionChanged }
    return challenge
  }

  func complete(
    challengeID: String,
    signature: Data
  ) throws -> MLSDeviceAuthBindingCompletion? {
    if let completionError {
      self.completionError = nil
      throw completionError
    }
    return MLSDeviceAuthBindingCompletion(
      deviceID: completionDeviceID,
      boundAt: Date(),
      bindingVersion: 1
    )
  }

  func complete(
    challengeID: String,
    signature: Data,
    matching expected: MLSDeviceAuthClientSnapshot
  ) throws -> MLSDeviceAuthBindingCompletion? {
    guard snapshot() == expected else { throw MLSDeviceAuthBindingError.sessionChanged }
    return try complete(challengeID: challengeID, signature: signature)
  }

  func beginCount() -> Int { beginCalls }
  func setOnBegin(_ operation: (@Sendable () async -> Void)?) { onBegin = operation }
  func holdNextBegin() { shouldHoldNextBegin = true }
  func failNextCompletion() { completionError = TestLifecycleError.rebindFailed }

  func waitUntilBeginHeld() async {
    guard !beginIsHeld else { return }
    await withCheckedContinuation { continuation in
      beginHeldContinuation = continuation
    }
  }

  func releaseBegin() {
    beginReleaseContinuation?.resume()
    beginReleaseContinuation = nil
  }
}

private actor LifecycleResumeRebindRecorder {
  private var calls = 0

  func record() { calls += 1 }
  func count() -> Int { calls }
}

private actor LifecycleInitializationGate {
  private var isHeld = false
  private var heldContinuation: CheckedContinuation<Void, Never>?
  private var releaseContinuation: CheckedContinuation<Void, Never>?

  func hold() async {
    isHeld = true
    heldContinuation?.resume()
    heldContinuation = nil
    await withCheckedContinuation { continuation in
      releaseContinuation = continuation
    }
  }

  func waitUntilHeld() async {
    if isHeld { return }
    await withCheckedContinuation { continuation in
      heldContinuation = continuation
    }
  }

  func release() {
    releaseContinuation?.resume()
    releaseContinuation = nil
  }
}

private enum TestLifecycleError: Error {
  case prepareFailed
  case rebindFailed
  case reloadFailed
  case resumeFailed
  case authoritativeRefreshFailed
}

private final class LifecycleLockedBox<Value>: @unchecked Sendable {
  private let lock = NSLock()
  private var storedValue: Value

  init(_ value: Value) { storedValue = value }

  var value: Value {
    lock.withLock { storedValue }
  }

  func withValue(_ body: (inout Value) -> Void) {
    lock.withLock { body(&storedValue) }
  }
}
