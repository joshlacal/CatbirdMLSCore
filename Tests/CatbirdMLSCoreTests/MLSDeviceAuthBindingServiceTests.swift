import Foundation
import Petrel
import XCTest

@testable import CatbirdMLSCore

final class MLSDeviceAuthBindingServiceTests: XCTestCase {
  func testBindSignsExactChallengeAndCompletesOnce() async throws {
    let challenge = Data([0x00, 0x01, 0x7f, 0x80, 0xff])
    let signature = Data(repeating: 0x5a, count: 64)
    let api = DeviceAuthAPIStub(
      challenge: .init(
        challengeID: "challenge-1",
        challenge: challenge,
        expiresAt: Date().addingTimeInterval(60),
        bindingVersion: 1
      ),
      completion: .init(
        deviceID: "device-1",
        boundAt: Date(),
        bindingVersion: 1
      )
    )
    let signedBytes = LockedBox<Data?>(nil)
    let service = MLSDeviceAuthBindingService(
      expectedDID: "did:plc:alice",
      deviceID: "device-1",
      api: api,
      signer: { bytes in
        signedBytes.withValue { $0 = bytes }
        return signature
      }
    )

    let status = try await service.bind()

    XCTAssertEqual(signedBytes.value, challenge)
    let beginDeviceIDs = await api.beginDeviceIDs()
    let completedChallengeIDs = await api.completedChallengeIDs()
    let completedSignatures = await api.completedSignatures()
    let retainedStatus = await service.status()
    let beginSnapshots = await api.beginExpectedSnapshots()
    let completeSnapshots = await api.completeExpectedSnapshots()
    XCTAssertEqual(beginDeviceIDs, ["device-1"])
    XCTAssertEqual(completedChallengeIDs, ["challenge-1"])
    XCTAssertEqual(completedSignatures, [signature])
    XCTAssertEqual(beginSnapshots.count, 1)
    XCTAssertEqual(completeSnapshots.count, 1)
    XCTAssertEqual(beginSnapshots.first, completeSnapshots.first)
    XCTAssertEqual(beginSnapshots.first?.authenticationGeneration, 1)
    XCTAssertEqual(status.deviceID, "device-1")
    XCTAssertEqual(status.bindingVersion, 1)
    XCTAssertEqual(retainedStatus, status)
  }

  func testLegacyBindingAPIDefaultFailsClosedWithoutSending() async {
    let api = LegacyDeviceAuthAPIStub()
    let signCount = LockedBox(0)
    let service = MLSDeviceAuthBindingService(
      expectedDID: "did:plc:alice",
      deviceID: "device-1",
      api: api,
      signer: { _ in
        signCount.withValue { $0 += 1 }
        return Data(repeating: 1, count: 64)
      }
    )

    await assertBindingError(.sessionChanged) {
      _ = try await service.bind()
    }

    let beginCount = await api.beginCount()
    let completeCount = await api.completeCount()
    XCTAssertEqual(beginCount, 0)
    XCTAssertEqual(completeCount, 0)
    XCTAssertEqual(signCount.value, 0)
  }

  func testAuthDriftBeforeBeginExactOperationSendsNothing() async {
    let api = DeviceAuthAPIStub()
    let signer = DeviceAuthSignerRecorder()
    await api.setBeforeBeginExactOperation {
      await api.setSnapshot(
        .init(
          did: "did:plc:alice",
          authenticationMode: .gateway,
          authenticationGeneration: 2
        )
      )
    }
    let service = makeService(api: api) { challenge in
      try await signer.sign(challenge)
    }

    await assertBindingError(.sessionChanged) {
      _ = try await service.bind()
    }

    let beginDeviceIDs = await api.beginDeviceIDs()
    let completedChallengeIDs = await api.completedChallengeIDs()
    let signatureCount = await signer.count()
    let status = await service.status()
    XCTAssertEqual(beginDeviceIDs, [])
    XCTAssertEqual(completedChallengeIDs, [])
    XCTAssertEqual(signatureCount, 0)
    XCTAssertNil(status)
  }

  func testAuthDriftDuringBeginResponseDiscardsChallengeWithoutSigning() async throws {
    let api = DeviceAuthAPIStub()
    let signer = DeviceAuthSignerRecorder()
    await api.holdNextBegin()
    let service = makeService(api: api) { challenge in
      try await signer.sign(challenge)
    }
    let binding = Task { try await service.bind() }
    await api.waitUntilBeginStarted()
    await api.setSnapshot(
      .init(
        did: "did:plc:alice",
        authenticationMode: .gateway,
        authenticationGeneration: 2
      )
    )
    await api.releaseBegin()

    await assertBindingError(.sessionChanged) { _ = try await binding.value }
    let beginDeviceIDs = await api.beginDeviceIDs()
    let completedChallengeIDs = await api.completedChallengeIDs()
    let signatureCount = await signer.count()
    let status = await service.status()
    XCTAssertEqual(beginDeviceIDs, ["device-1"])
    XCTAssertEqual(completedChallengeIDs, [])
    XCTAssertEqual(signatureCount, 0)
    XCTAssertNil(status)
  }

  func testAuthDriftDuringCompleteResponseRejectsOldAuthResponseAndStatus() async throws {
    let api = DeviceAuthAPIStub()
    await api.holdNextComplete()
    let service = makeService(api: api)
    let binding = Task { try await service.bind() }
    await api.waitUntilCompleteStarted()
    await api.setSnapshot(
      .init(
        did: "did:plc:alice",
        authenticationMode: .gateway,
        authenticationGeneration: 2
      )
    )
    await api.releaseComplete()

    await assertBindingError(.sessionChanged) { _ = try await binding.value }
    let completedChallengeIDs = await api.completedChallengeIDs()
    let completeGenerations = await api.completeExpectedSnapshots().map(
      \.authenticationGeneration
    )
    let status = await service.status()
    XCTAssertEqual(completedChallengeIDs, ["challenge-1"])
    XCTAssertEqual(completeGenerations, [1])
    XCTAssertNil(status)
  }

  func testRejectsDirectAuthenticationBeforeBeginning() async {
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: "did:plc:alice",
        authenticationMode: .direct,
        authenticationGeneration: 1
      ))
    let service = makeService(api: api)

    await assertBindingError(.directAuthenticationUnsupported) {
      _ = try await service.bind()
    }
    let beginDeviceIDs = await api.beginDeviceIDs()
    XCTAssertEqual(beginDeviceIDs, [])
  }

  func testRejectsWrongDIDBeforeBeginning() async {
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: "did:plc:mallory",
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    let service = makeService(api: api)

    await assertBindingError(.sessionChanged) {
      _ = try await service.bind()
    }
    let beginDeviceIDs = await api.beginDeviceIDs()
    XCTAssertEqual(beginDeviceIDs, [])
  }

  func testRejectsInvalidChallengeShapesWithoutSigningOrCompleting() async {
    let invalidChallenges: [(MLSDeviceAuthBindingChallenge, MLSDeviceAuthBindingError)] = [
      (
        .init(
          challengeID: "c", challenge: Data(), expiresAt: Date().addingTimeInterval(60),
          bindingVersion: 1), .invalidChallenge
      ),
      (
        .init(
          challengeID: "c", challenge: Data(repeating: 1, count: 513),
          expiresAt: Date().addingTimeInterval(60), bindingVersion: 1), .invalidChallenge
      ),
      (
        .init(
          challengeID: "", challenge: Data([1]), expiresAt: Date().addingTimeInterval(60),
          bindingVersion: 1), .malformedResponse
      ),
      (
        .init(challengeID: "c", challenge: Data([1]), expiresAt: Date(), bindingVersion: 1),
        .challengeExpired
      ),
      (
        .init(
          challengeID: "c", challenge: Data([1]), expiresAt: Date().addingTimeInterval(60),
          bindingVersion: 2), .unsupportedVersion
      ),
    ]

    for (challenge, expectedError) in invalidChallenges {
      let api = DeviceAuthAPIStub(challenge: challenge)
      let signCount = LockedBox(0)
      let service = makeService(api: api) { _ in
        signCount.withValue { $0 += 1 }
        return Data(repeating: 1, count: 64)
      }

      await assertBindingError(expectedError) {
        _ = try await service.bind()
      }
      XCTAssertEqual(signCount.value, 0)
      let completedChallengeIDs = await api.completedChallengeIDs()
      XCTAssertEqual(completedChallengeIDs, [])
    }
  }

  func testRejectsInvalidSignatureLengthsWithoutCompleting() async {
    for count in [63, 65] {
      let api = DeviceAuthAPIStub()
      let service = makeService(api: api) { _ in Data(repeating: 1, count: count) }

      await assertBindingError(.invalidSignature) {
        _ = try await service.bind()
      }
      let completedChallengeIDs = await api.completedChallengeIDs()
      XCTAssertEqual(completedChallengeIDs, [])
    }
  }

  func testSanitizesSignerFailure() async {
    struct SensitiveSignerError: Error {}
    let api = DeviceAuthAPIStub()
    let service = makeService(api: api) { _ in throw SensitiveSignerError() }

    await assertBindingError(.signingFailed) {
      _ = try await service.bind()
    }
    let completedChallengeIDs = await api.completedChallengeIDs()
    XCTAssertEqual(completedChallengeIDs, [])
  }

  func testRechecksSessionAfterSigning() async {
    let api = DeviceAuthAPIStub()
    let service = makeService(api: api) { bytes in
      await api.setSnapshot(
        .init(
          did: "did:plc:alice",
          authenticationMode: .gateway,
          authenticationGeneration: 2
        ))
      return Data(repeating: UInt8(bytes.count), count: 64)
    }

    await assertBindingError(.sessionChanged) {
      _ = try await service.bind()
    }
    let completedChallengeIDs = await api.completedChallengeIDs()
    XCTAssertEqual(completedChallengeIDs, [])
    let completeSnapshots = await api.completeExpectedSnapshots()
    XCTAssertEqual(completeSnapshots, [])
  }

  func testRejectsMismatchedCompletionAndDoesNotRetainStatus() async {
    let apiWithNilCompletion = DeviceAuthAPIStub(completion: nil)
    let nilCompletionService = makeService(api: apiWithNilCompletion)
    await assertBindingError(.malformedResponse) {
      _ = try await nilCompletionService.bind()
    }
    let nilCompletionStatus = await nilCompletionService.status()
    XCTAssertNil(nilCompletionStatus)

    let mismatches: [MLSDeviceAuthBindingCompletion] = [
      .init(deviceID: "device-2", boundAt: Date(), bindingVersion: 1),
      .init(deviceID: "device-1", boundAt: Date(), bindingVersion: 2),
    ]

    for completion in mismatches {
      let api = DeviceAuthAPIStub(completion: completion)
      let service = makeService(api: api)

      await assertBindingError(.bindingMismatch) {
        _ = try await service.bind()
      }
      let status = await service.status()
      XCTAssertNil(status)
    }
  }

  func testRejectsSessionChangeAfterCompletionAndDoesNotClaimAuthority() async {
    let api = DeviceAuthAPIStub()
    await api.setAfterComplete {
      await api.setSnapshot(
        .init(
          did: "did:plc:alice",
          authenticationMode: .gateway,
          authenticationGeneration: 2
        ))
    }
    let service = makeService(api: api)

    await assertBindingError(.sessionChanged) {
      _ = try await service.bind()
    }
    let status = await service.status()
    XCTAssertNil(status)
  }

  func testStatusIsDiscardedWhenExactClientSessionChanges() async throws {
    let api = DeviceAuthAPIStub()
    let service = makeService(api: api)
    _ = try await service.bind()
    await api.setSnapshot(
      .init(
        did: "did:plc:alice",
        authenticationMode: .gateway,
        authenticationGeneration: 2
      ))

    let status = await service.status()

    XCTAssertNil(status)
  }

  func testStatusDoesNotReturnStaleEvidenceAfterInvalidationDuringSnapshot() async throws {
    let api = DeviceAuthAPIStub()
    let service = makeService(api: api)
    _ = try await service.bind()

    await api.holdNextSnapshot()
    let statusTask = Task { await service.status() }
    await api.waitUntilSnapshotHeld()
    await service.invalidate()
    await api.releaseSnapshot()

    let racedStatus = await statusTask.value
    let finalStatus = await service.status()
    XCTAssertNil(racedStatus)
    XCTAssertNil(finalStatus)
  }

  func testSuspendedStatusCannotClearAReplacementBinding() async throws {
    let api = DeviceAuthAPIStub()
    let service = makeService(api: api)
    _ = try await service.bind()

    await api.holdNextSnapshot()
    let staleStatusTask = Task { await service.status() }
    await api.waitUntilSnapshotHeld()
    let replacementStatus = try await service.bind()
    await api.releaseSnapshot()

    let staleStatus = await staleStatusTask.value
    let finalStatus = await service.status()
    XCTAssertNil(staleStatus)
    XCTAssertEqual(finalStatus, replacementStatus)
  }

  func testDeviceAndChallengeIdentifiersEnforceLexiconBounds() async {
    let oversizedDeviceService = MLSDeviceAuthBindingService(
      expectedDID: "did:plc:alice",
      deviceID: String(repeating: "é", count: 33),
      api: DeviceAuthAPIStub(),
      signer: { _ in Data(repeating: 1, count: 64) }
    )
    await assertBindingError(.malformedResponse) {
      _ = try await oversizedDeviceService.bind()
    }

    let oversizedChallengeAPI = DeviceAuthAPIStub(
      challenge: .init(
        challengeID: String(repeating: "é", count: 65),
        challenge: Data([1]),
        expiresAt: Date().addingTimeInterval(60),
        bindingVersion: 1
      )
    )
    await assertBindingError(.malformedResponse) {
      _ = try await self.makeService(api: oversizedChallengeAPI).bind()
    }
  }

  func testClientInvalidationWinsAgainstSuspendedExistingStatusCheck() async throws {
    let did = "did:plc:device-auth-race"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      )
    )
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: api,
      signer: { _ in Data(repeating: 1, count: 64) }
    )

    await api.holdNextSnapshot()
    let racedBind = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { _ in Data(repeating: 2, count: 64) }
      )
    }
    await api.waitUntilSnapshotHeld()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    await api.releaseSnapshot()

    do {
      _ = try await racedBind.value
      XCTFail("Expected invalidation to defeat suspended binding")
    } catch let error as MLSDeviceAuthBindingError {
      XCTAssertEqual(error, .sessionChanged)
    } catch {
      XCTFail("Expected sessionChanged, got \(error)")
    }
    let finalStatus = await MLSClient.shared.deviceAuthBindingStatus(for: did)
    XCTAssertNil(finalStatus)
  }

  func testClientStatusRejectsSuspensionDuringAwaitedSnapshot() async throws {
    let did = "did:plc:device-auth-status-suspension-race"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: api,
      signer: { _ in Data(repeating: 1, count: 64) }
    )

    await api.holdNextSnapshot()
    let statusTask = Task {
      await MLSClient.shared.deviceAuthBindingStatus(for: did)
    }
    await api.waitUntilSnapshotHeld()
    MLSClient.markSuspensionInProgress(reason: "status suspension race")
    await api.releaseSnapshot()

    let racedStatus = await statusTask.value
    XCTAssertNil(racedStatus)

    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testConfiguredClientReplacementWinsAgainstSuspendedBinding() async throws {
    let did = "did:plc:device-auth-replacement"
    let oldAPI = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      )
    )
    await MLSClient.shared.invalidateCachedClient(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: oldAPI,
      signer: { _ in Data(repeating: 1, count: 64) }
    )

    await oldAPI.holdNextSnapshot()
    let racedBind = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: oldAPI,
        signer: { _ in Data(repeating: 2, count: 64) }
      )
    }
    await oldAPI.waitUntilSnapshotHeld()

    let newATProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let newAPIClient = await MLSAPIClient(
      client: newATProtoClient,
      environment: .custom(serviceDID: "did:web:example.com#atproto_mls")
    )
    await MLSClient.shared.configure(
      for: did,
      apiClient: newAPIClient,
      atProtoClient: newATProtoClient
    )
    await oldAPI.releaseSnapshot()

    do {
      _ = try await racedBind.value
      XCTFail("Expected client replacement to defeat suspended binding")
    } catch let error as MLSDeviceAuthBindingError {
      XCTAssertEqual(error, .sessionChanged)
    } catch {
      XCTFail("Expected sessionChanged, got \(error)")
    }
    let finalStatus = await MLSClient.shared.deviceAuthBindingStatus(for: did)
    XCTAssertNil(finalStatus)
    await MLSClient.shared.invalidateCachedClient(for: did)
  }

  func testAuthGenerationChangeAfterFinalBindSnapshotCannotRecordResumeReceipt() async throws {
    let did = "did:plc:device-auth-final-generation-race"
    let generationOne = MLSDeviceAuthClientSnapshot(
      did: did,
      authenticationMode: .gateway,
      authenticationGeneration: 1
    )
    let api = DeviceAuthAPIStub(snapshot: generationOne)
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: api,
      signer: { _ in Data(repeating: 1, count: 64) }
    )
    MLSClient.markSuspensionInProgress(reason: "final auth generation receipt race")
    let capability = try XCTUnwrap(MLSClient.beginSuspendedResumeCapability(for: did))
    _ = await MLSClient.shared.invalidateDeviceAuthBinding(
      for: did,
      preservingSuspendedResumeCapability: capability
    )

    await api.holdThirdNextSnapshotAfterCapture()
    let rebind = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        suspendedResumePermit: capability,
        signer: { _ in Data(repeating: 2, count: 64) }
      )
    }
    await api.waitUntilSnapshotHeld()
    await api.setSnapshot(
      MLSDeviceAuthClientSnapshot(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 2
      ))
    await api.releaseSnapshot()

    do {
      _ = try await rebind.value
      XCTFail("Expected authentication generation drift to refuse the resume receipt")
    } catch let error as MLSDeviceAuthBindingError {
      XCTAssertEqual(error, .sessionChanged)
    } catch {
      XCTFail("Expected sessionChanged, got \(error)")
    }
    let finalStatus = await MLSClient.shared.deviceAuthBindingStatus(for: did)
    XCTAssertNil(finalStatus)
    let finishedStaleReceipt = await MLSClient.shared.finishSuspendedResumeCapability(capability)
    XCTAssertFalse(finishedStaleReceipt)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testAuthGenerationChangeAfterReceiptCannotReleaseSuspension() async throws {
    let did = "did:plc:device-auth-final-release-generation"
    let api = DeviceAuthAPIStub(
      snapshot: MLSDeviceAuthClientSnapshot(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: api,
      signer: { _ in Data(repeating: 1, count: 64) }
    )
    MLSClient.markSuspensionInProgress(reason: "final release auth generation race")
    let capability = try XCTUnwrap(MLSClient.beginSuspendedResumeCapability(for: did))
    _ = await MLSClient.shared.invalidateDeviceAuthBinding(
      for: did,
      preservingSuspendedResumeCapability: capability
    )
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: api,
      suspendedResumePermit: capability,
      signer: { _ in Data(repeating: 2, count: 64) }
    )

    await api.setSnapshot(
      MLSDeviceAuthClientSnapshot(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 2
      ))

    let finishedStaleReceipt = await MLSClient.shared.finishSuspendedResumeCapability(capability)
    XCTAssertFalse(finishedStaleReceipt)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testAuthGenerationChangeAfterFinalValidationCannotReleaseSuspension() async throws {
    let did = "did:plc:device-auth-post-validation-race"
    let api = DeviceAuthAPIStub(
      snapshot: MLSDeviceAuthClientSnapshot(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    defer {
      MLSClient.setDeviceAuthAfterFinalValidationTestOverride(nil)
      MLSClient.resetDeviceAuthSuspensionStateForTesting()
    }

    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: api,
      signer: { _ in Data(repeating: 1, count: 64) }
    )
    MLSClient.markSuspensionInProgress(reason: "post-validation auth generation race")
    let capability = try XCTUnwrap(MLSClient.beginSuspendedResumeCapability(for: did))
    _ = await MLSClient.shared.invalidateDeviceAuthBinding(
      for: did,
      preservingSuspendedResumeCapability: capability
    )
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: api,
      suspendedResumePermit: capability,
      signer: { _ in Data(repeating: 2, count: 64) }
    )

    let validationReached = DispatchSemaphore(value: 0)
    let allowConsume = DispatchSemaphore(value: 0)
    MLSClient.setDeviceAuthAfterFinalValidationTestOverride {
      validationReached.signal()
      allowConsume.wait()
    }
    let finish = Task.detached {
      await MLSClient.shared.finishSuspendedResumeCapability(capability)
    }
    await waitForDeviceAuthSemaphore(validationReached)
    await api.setSnapshot(
      MLSDeviceAuthClientSnapshot(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 2
      ))
    allowConsume.signal()

    let finishedStaleReceipt = await finish.value
    XCTAssertFalse(finishedStaleReceipt)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testConcurrentEnrollmentIsRejectedAndOnlyOneCompletionOccurs() async throws {
    let api = DeviceAuthAPIStub()
    await api.holdBegin()
    let service = makeService(api: api)
    let first = Task { try await service.bind() }
    await api.waitUntilBeginStarted()

    await assertBindingError(.concurrentEnrollment) {
      _ = try await service.bind()
    }

    await api.releaseBegin()
    _ = try await first.value
    let completedChallengeIDs = await api.completedChallengeIDs()
    XCTAssertEqual(completedChallengeIDs, ["challenge-1"])
  }

  func testClientCoalescesOverlappingEnrollmentForSameDeviceAndProvider() async throws {
    let did = "did:plc:device-auth-coalescing"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    await api.holdNextBegin()

    let first = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { _ in Data(repeating: 1, count: 64) }
      )
    }
    await api.waitUntilBeginStarted()

    let second = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { _ in Data(repeating: 2, count: 64) }
      )
    }
    for _ in 0..<100 {
      await Task.yield()
    }
    await api.releaseBegin()

    let firstStatus = try await first.value
    let secondStatus = try await second.value
    let beginDeviceIDs = await api.beginDeviceIDs()
    let completedChallengeIDs = await api.completedChallengeIDs()
    XCTAssertEqual(firstStatus, secondStatus)
    XCTAssertEqual(beginDeviceIDs, ["device-1"])
    XCTAssertEqual(completedChallengeIDs, ["challenge-1"])
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testSoleCanceledWaiterStopsEnrollmentBeforeSigningOrCompletion() async throws {
    let did = "did:plc:device-auth-sole-cancel"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    let signer = DeviceAuthSignerRecorder()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    await api.holdNextBegin()

    let binding = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { challenge in try await signer.sign(challenge) }
      )
    }
    await api.waitUntilBeginStarted()
    binding.cancel()
    for _ in 0..<100 { await Task.yield() }
    await api.releaseBegin()

    do {
      _ = try await binding.value
      XCTFail("Expected cancellation")
    } catch is CancellationError {
      // Expected.
    } catch {
      XCTFail("Expected CancellationError, got \(error)")
    }
    let signatureCount = await signer.count()
    let completions = await api.completedChallengeIDs()
    let status = await MLSClient.shared.deviceAuthBindingStatus(for: did)
    XCTAssertEqual(signatureCount, 0)
    XCTAssertTrue(completions.isEmpty)
    XCTAssertNil(status)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testCancellationAfterCompleteSendRetainsNoStatus() async throws {
    let api = DeviceAuthAPIStub()
    let signer = DeviceAuthSignerRecorder()
    await api.holdNextComplete()
    let service = makeService(api: api) { challenge in
      await signer.sign(challenge)
    }
    let binding = Task { try await service.bind() }
    await api.waitUntilCompleteStarted()
    binding.cancel()
    await api.releaseComplete()

    do {
      _ = try await binding.value
      XCTFail("Expected cancellation")
    } catch is CancellationError {
      // Expected.
    } catch {
      XCTFail("Expected CancellationError, got \(error)")
    }
    let begins = await api.beginDeviceIDs()
    let completions = await api.completedChallengeIDs()
    let signatureCount = await signer.count()
    let status = await service.status()
    XCTAssertEqual(begins, ["device-1"])
    XCTAssertEqual(completions, ["challenge-1"])
    XCTAssertEqual(signatureCount, 1)
    XCTAssertNil(status)
  }

  func testCancelledServiceBeforeBeginLaunchSendsNothing() async {
    let api = DeviceAuthAPIStub()
    let signer = DeviceAuthSignerRecorder()
    let launchGate = DeviceAuthCancellationLaunchGate()
    let service = makeService(api: api) { challenge in
      await signer.sign(challenge)
    }
    let binding = Task {
      await launchGate.waitForRelease()
      return try await service.bind()
    }
    await launchGate.waitUntilHeld()
    binding.cancel()
    await launchGate.release()

    await assertCancellation { _ = try await binding.value }
    let begins = await api.beginDeviceIDs()
    let completions = await api.completedChallengeIDs()
    let signatureCount = await signer.count()
    let status = await service.status()
    XCTAssertTrue(begins.isEmpty)
    XCTAssertTrue(completions.isEmpty)
    XCTAssertEqual(signatureCount, 0)
    XCTAssertNil(status)
  }

  func testCancelledServiceBeforeCompleteLaunchSendsOnlyBegin() async {
    let api = DeviceAuthAPIStub()
    let signerGate = DeviceAuthCancellationLaunchGate()
    let service = makeService(api: api) { _ in
      await signerGate.waitForRelease()
      return Data(repeating: 1, count: 64)
    }
    let binding = Task { try await service.bind() }
    await signerGate.waitUntilHeld()
    binding.cancel()
    await signerGate.release()

    await assertCancellation { _ = try await binding.value }
    let begins = await api.beginDeviceIDs()
    let completions = await api.completedChallengeIDs()
    let status = await service.status()
    XCTAssertEqual(begins, ["device-1"])
    XCTAssertTrue(completions.isEmpty)
    XCTAssertNil(status)
  }

  func testCancelingOneCoalescedWaiterPreservesEnrollmentForOtherWaiter() async throws {
    let did = "did:plc:device-auth-shared-cancel"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    let signer = DeviceAuthSignerRecorder()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    await api.holdNextBegin()

    let first = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { challenge in try await signer.sign(challenge) }
      )
    }
    await api.waitUntilBeginStarted()
    let second = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { challenge in try await signer.sign(challenge) }
      )
    }
    for _ in 0..<100 { await Task.yield() }
    first.cancel()
    for _ in 0..<100 { await Task.yield() }
    await api.releaseBegin()

    do {
      _ = try await first.value
      XCTFail("Expected the canceled waiter to fail")
    } catch is CancellationError {
      // Expected.
    } catch {
      XCTFail("Expected CancellationError, got \(error)")
    }
    _ = try await second.value
    let signatureCount = await signer.count()
    let completions = await api.completedChallengeIDs()
    XCTAssertEqual(signatureCount, 1)
    XCTAssertEqual(completions, ["challenge-1"])
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testExplicitInvalidationCancelsStoredEnrollmentTask() async throws {
    let did = "did:plc:device-auth-explicit-invalidate"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    let signer = DeviceAuthSignerRecorder()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    await api.holdNextBegin()

    let binding = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { challenge in try await signer.sign(challenge) }
      )
    }
    await api.waitUntilBeginStarted()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    await api.releaseBegin()

    do {
      _ = try await binding.value
      XCTFail("Expected cancellation")
    } catch is CancellationError {
      // Expected.
    } catch {
      XCTFail("Expected CancellationError, got \(error)")
    }
    let signatureCount = await signer.count()
    let completions = await api.completedChallengeIDs()
    XCTAssertEqual(signatureCount, 0)
    XCTAssertTrue(completions.isEmpty)
  }

  func testSynchronousSuspensionGateCancelsRegisteredEnrollmentTask() async throws {
    let did = "did:plc:device-auth-sync-suspend"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    let signer = DeviceAuthSignerRecorder()
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    await api.holdNextBegin()

    let binding = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { challenge in try await signer.sign(challenge) }
      )
    }
    await api.waitUntilBeginStarted()
    MLSClient.markSuspensionInProgress(reason: "sync device-auth regression")
    await api.releaseBegin()

    do {
      _ = try await binding.value
      XCTFail("Expected synchronous suspension cancellation")
    } catch is CancellationError {
      // Expected.
    } catch {
      XCTFail("Expected CancellationError, got \(error)")
    }
    let signatureCount = await signer.count()
    let completions = await api.completedChallengeIDs()
    XCTAssertEqual(signatureCount, 0)
    XCTAssertTrue(completions.isEmpty)

    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testBindingAdmissionFailsWhileSuspensionIsAlreadyActive() async throws {
    let did = "did:plc:device-auth-post-suspend-admission"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    MLSClient.markSuspensionInProgress(reason: "post-suspend admission test")

    await assertBindingError(.sessionChanged) {
      _ = try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { _ in Data(repeating: 1, count: 64) }
      )
    }
    let begins = await api.beginDeviceIDs()
    XCTAssertTrue(begins.isEmpty)

    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testExplicitClearSucceedsWhenNoDeviceAuthResumeIsOwed() async {
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    MLSClient.markSuspensionInProgress(reason: "no rebind owed clear test")

    let cleared = MLSClient.clearSuspensionFlag(reason: "explicit future BGTask check")

    XCTAssertTrue(cleared)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testEmergencyCloseCapturesCompletedBindingAndRefusesLegacyClear() async throws {
    let did = "did:plc:device-auth-emergency-close"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: api,
      signer: { _ in Data(repeating: 1, count: 64) }
    )

    MLSClient.emergencyCloseAllContexts(reason: "completed binding emergency close test")
    let capability = try XCTUnwrap(MLSClient.beginSuspendedResumeCapability(for: did))
    MLSClient.emergencyCloseAllContexts(reason: "emergency close revokes active resume capability")
    let revokedCapabilityFinished = await MLSClient.shared.finishSuspendedResumeCapability(
      capability)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    MLSClient.emergencyCloseAllContexts(reason: "idempotent emergency close preserves snapshot")
    let bindingWasCaptured = MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: did)
    let cleared = MLSClient.clearSuspensionFlag(reason: "must refuse emergency-close rebind bypass")

    XCTAssertTrue(bindingWasCaptured)
    XCTAssertFalse(revokedCapabilityFinished)
    XCTAssertFalse(cleared)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testEmergencyCloseCancelsRegisteredEnrollmentBeforeSigning() async throws {
    let did = "did:plc:device-auth-emergency-enrollment"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    let signer = DeviceAuthSignerRecorder()
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    await api.holdNextBegin()

    let binding = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { challenge in try await signer.sign(challenge) }
      )
    }
    await api.waitUntilBeginStarted()
    MLSClient.emergencyCloseAllContexts(reason: "in-flight enrollment emergency close test")
    await api.releaseBegin()

    do {
      _ = try await binding.value
      XCTFail("Expected emergency-close enrollment cancellation")
    } catch is CancellationError {
      // Expected.
    } catch {
      XCTFail("Expected CancellationError, got \(error)")
    }
    let signatureCount = await signer.count()
    let completions = await api.completedChallengeIDs()
    XCTAssertEqual(signatureCount, 0)
    XCTAssertTrue(completions.isEmpty)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)

    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testSuspensionSnapshotSurvivesBindingCleanupBeforeConsumption() async throws {
    let did = "did:plc:device-auth-suspension-snapshot"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: api,
      signer: { _ in Data(repeating: 1, count: 64) }
    )

    MLSClient.markSuspensionInProgress(reason: "suspension snapshot transition")
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    let preservedTransitionSnapshot =
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(
        for: did
      )

    XCTAssertTrue(preservedTransitionSnapshot)

    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testSuspensionBetweenBoundaryCheckAndAdmissionPreventsTaskLaunch() async throws {
    let did = "did:plc:device-auth-admission-race"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.setDeviceAuthBeforeEnrollmentAdmissionOverride {
      MLSClient.markSuspensionInProgress(reason: "admission race test")
    }

    await assertBindingError(.sessionChanged) {
      _ = try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: api,
        signer: { _ in Data(repeating: 1, count: 64) }
      )
    }
    let begins = await api.beginDeviceIDs()
    XCTAssertTrue(begins.isEmpty)

    await MLSClient.shared.setDeviceAuthBeforeEnrollmentAdmissionOverride(nil)
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testClientDoesNotReuseCompletedBindingAcrossProviders() async throws {
    let did = "did:plc:device-auth-provider-boundary"
    let snapshot = MLSDeviceAuthClientSnapshot(
      did: did,
      authenticationMode: .gateway,
      authenticationGeneration: 1
    )
    let firstAPI = DeviceAuthAPIStub(snapshot: snapshot)
    let secondAPI = DeviceAuthAPIStub(snapshot: snapshot)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)

    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: firstAPI,
      signer: { _ in Data(repeating: 1, count: 64) }
    )
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: secondAPI,
      signer: { _ in Data(repeating: 2, count: 64) }
    )

    let firstBegins = await firstAPI.beginDeviceIDs()
    let secondBegins = await secondAPI.beginDeviceIDs()
    let firstCompletions = await firstAPI.completedChallengeIDs()
    let secondCompletions = await secondAPI.completedChallengeIDs()
    XCTAssertEqual(firstBegins, ["device-1"])
    XCTAssertEqual(secondBegins, ["device-1"])
    XCTAssertEqual(firstCompletions, ["challenge-1"])
    XCTAssertEqual(secondCompletions, ["challenge-1"])
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testReplacementObligationSurvivesSuspensionWhileDetachedReplacementFails() async throws {
    let did = "did:plc:device-auth-held-replacement"
    let oldAPI = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    let replacementAPI = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    let detachGate = DeviceAuthReplacementDetachGate()
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: oldAPI,
      signer: { _ in Data(repeating: 1, count: 64) }
    )
    await MLSClient.shared.setDeviceAuthAfterReplacementDetachOverride {
      await detachGate.hold()
    }

    let replacement = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: replacementAPI,
        signer: { _ in Data(repeating: 2, count: 64) }
      )
    }
    await detachGate.waitUntilHeld()

    XCTAssertTrue(MLSClient.isDeviceAuthRebindObligationPendingForTesting(did))
    MLSClient.markSuspensionInProgress(reason: "held replacement suspension race")
    XCTAssertTrue(MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: did))
    XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "replacement obligation must stay closed"))

    await detachGate.release()
    await assertBindingError(.sessionChanged) { _ = try await replacement.value }
    XCTAssertTrue(MLSClient.isDeviceAuthRebindObligationPendingForTesting(did))
    XCTAssertTrue(MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: did))
    XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "failed replacement must stay closed"))

    await MLSClient.shared.setDeviceAuthAfterReplacementDetachOverride(nil)
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testSupersedingSuccessfulReplacementResolvesForceRebindObligation() async throws {
    let did = "did:plc:device-auth-superseded-replacement"
    let snapshot = MLSDeviceAuthClientSnapshot(
      did: did,
      authenticationMode: .gateway,
      authenticationGeneration: 1
    )
    let oldAPI = DeviceAuthAPIStub(snapshot: snapshot)
    let staleReplacementAPI = DeviceAuthAPIStub(snapshot: snapshot)
    let winningReplacementAPI = DeviceAuthAPIStub(snapshot: snapshot)
    let detachGate = DeviceAuthReplacementDetachGate()
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: oldAPI,
      signer: { _ in Data(repeating: 1, count: 64) }
    )
    await MLSClient.shared.setDeviceAuthAfterReplacementDetachOverride {
      await detachGate.hold()
    }
    let staleReplacement = Task {
      try await MLSClient.shared.bindDeviceAuthentication(
        for: did,
        deviceID: "device-1",
        apiOverride: staleReplacementAPI,
        signer: { _ in Data(repeating: 2, count: 64) }
      )
    }
    await detachGate.waitUntilHeld()
    XCTAssertTrue(MLSClient.isDeviceAuthRebindObligationPendingForTesting(did))

    await MLSClient.shared.setDeviceAuthAfterReplacementDetachOverride(nil)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: winningReplacementAPI,
      signer: { _ in Data(repeating: 3, count: 64) }
    )
    XCTAssertFalse(MLSClient.isDeviceAuthRebindObligationPendingForTesting(did))

    await detachGate.release()
    await assertBindingError(.sessionChanged) { _ = try await staleReplacement.value }
    XCTAssertFalse(MLSClient.isDeviceAuthRebindObligationPendingForTesting(did))
    await MLSClient.shared.invalidateDeviceAuthBinding(for: did)
  }

  func testProviderReplacementObligationIsDIDScopedAndCapturedBySuspension() async throws {
    let did = "did:plc:device-auth-provider-replacement"
    let unrelatedDID = "did:plc:device-auth-unrelated"
    let oldAPI = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateCachedClient(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: oldAPI,
      signer: { _ in Data(repeating: 1, count: 64) }
    )

    let replacementClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let replacementAPIClient = await MLSAPIClient(
      client: replacementClient,
      environment: .custom(serviceDID: "did:web:replacement.example#atproto_mls")
    )
    await MLSClient.shared.configure(
      for: did,
      apiClient: replacementAPIClient,
      atProtoClient: replacementClient
    )

    XCTAssertTrue(MLSClient.isDeviceAuthRebindObligationPendingForTesting(did))
    XCTAssertFalse(MLSClient.isDeviceAuthRebindObligationPendingForTesting(unrelatedDID))
    MLSClient.markSuspensionInProgress(reason: "provider replacement suspension")
    XCTAssertTrue(MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: did))
    XCTAssertFalse(
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: unrelatedDID))
    XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "provider replacement obligation"))

    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateCachedClient(for: did)
  }

  func testExplicitClientRevocationClearsPendingProviderReplacementObligation() async throws {
    let did = "did:plc:device-auth-provider-revocation"
    let oldAPI = DeviceAuthAPIStub(
      snapshot: .init(
        did: did,
        authenticationMode: .gateway,
        authenticationGeneration: 1
      ))
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateCachedClient(for: did)
    _ = try await MLSClient.shared.bindDeviceAuthentication(
      for: did,
      deviceID: "device-1",
      apiOverride: oldAPI,
      signer: { _ in Data(repeating: 1, count: 64) }
    )

    let replacementClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let replacementAPIClient = await MLSAPIClient(
      client: replacementClient,
      environment: .custom(serviceDID: "did:web:replacement.example#atproto_mls")
    )
    await MLSClient.shared.configure(
      for: did,
      apiClient: replacementAPIClient,
      atProtoClient: replacementClient
    )
    XCTAssertTrue(MLSClient.isDeviceAuthRebindObligationPendingForTesting(did))

    await MLSClient.shared.invalidateCachedClient(for: did)

    XCTAssertFalse(MLSClient.isDeviceAuthRebindObligationPendingForTesting(did))
    MLSClient.markSuspensionInProgress(reason: "explicit provider revocation")
    XCTAssertTrue(MLSClient.clearSuspensionFlag(reason: "no rebind owed after revocation"))
  }

  func testServiceEnvironmentSwitchInvalidatesCompletedAuthority() async throws {
    let serviceDIDA = "did:web:mls-a.example#atproto_mls"
    let serviceDIDB = "did:web:mls-b.example#atproto_mls"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: "did:plc:alice",
        authenticationMode: .gateway,
        authenticationGeneration: 1,
        mlsServiceDID: serviceDIDA
      )
    )
    let service = makeService(api: api)

    _ = try await service.bind()
    await api.setSnapshot(
      .init(
        did: "did:plc:alice",
        authenticationMode: .gateway,
        authenticationGeneration: 1,
        mlsServiceDID: serviceDIDB
      ))

    let status = await service.status()
    XCTAssertNil(status)
  }

  func testServiceEnvironmentSwitchDuringEnrollmentFailsClosedBeforeCompletion() async throws {
    let serviceDIDA = "did:web:mls-a.example#atproto_mls"
    let serviceDIDB = "did:web:mls-b.example#atproto_mls"
    let api = DeviceAuthAPIStub(
      snapshot: .init(
        did: "did:plc:alice",
        authenticationMode: .gateway,
        authenticationGeneration: 1,
        mlsServiceDID: serviceDIDA
      )
    )
    await api.holdNextBegin()
    let service = makeService(api: api)
    let binding = Task { try await service.bind() }
    await api.waitUntilBeginStarted()
    await api.setSnapshot(
      .init(
        did: "did:plc:alice",
        authenticationMode: .gateway,
        authenticationGeneration: 1,
        mlsServiceDID: serviceDIDB
      ))
    await api.releaseBegin()

    await assertBindingError(.sessionChanged) {
      _ = try await binding.value
    }
    let completions = await api.completedChallengeIDs()
    XCTAssertTrue(completions.isEmpty)
  }

  func testPetrelSnapshotTracksInPlaceMLSEnvironmentSwitch() async throws {
    let client = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: client,
      environment: .custom(serviceDID: "did:web:mls-a.example#atproto_mls")
    )
    let api = MLSDeviceAuthPetrelAPI(apiClient: apiClient)

    let before = await api.snapshot()
    await api.switchEnvironmentForTesting(
      .custom(serviceDID: "did:web:mls-b.example#atproto_mls"))
    let after = await api.snapshot()

    XCTAssertEqual(before.mlsServiceDID, "did:web:mls-a.example#atproto_mls")
    XCTAssertEqual(after.mlsServiceDID, "did:web:mls-b.example#atproto_mls")
    XCTAssertNotEqual(before, after)
  }

  func testPetrelBeginPreservesCancellationAfterExactRequestReturns() async {
    let serviceDID = "did:web:mls.example#atproto_mls"
    let requestReached = LockedBox(false)
    let client = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: client,
      environment: .custom(serviceDID: serviceDID)
    )
    let api = MLSDeviceAuthPetrelAPI(
      apiClient: apiClient,
      exactBeginRequest: { _, _, _ in
        requestReached.withValue { $0 = true }
        withUnsafeCurrentTask { $0?.cancel() }
        return .continuityChanged
      }
    )
    let expected = MLSDeviceAuthClientSnapshot(
      did: "did:plc:alice",
      authenticationMode: .gateway,
      authenticationGeneration: 1,
      mlsServiceDID: serviceDID
    )

    let operation = Task {
      try await api.begin(deviceID: "device-1", matching: expected)
    }

    do {
      _ = try await operation.value
      XCTFail("Expected cancellation")
    } catch is CancellationError {
      XCTAssertTrue(requestReached.value)
    } catch {
      XCTFail("Expected CancellationError, got \(error)")
    }
  }

  func testPetrelCancellationBeforeLaunchSendsNeitherExactRequest() async {
    let serviceDID = "did:web:mls.example#atproto_mls"
    let beginRequestCount = LockedBox(0)
    let completeRequestCount = LockedBox(0)
    let client = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: client,
      environment: .custom(serviceDID: serviceDID)
    )
    let api = MLSDeviceAuthPetrelAPI(
      apiClient: apiClient,
      exactBeginRequest: { _, _, _ in
        beginRequestCount.withValue { $0 += 1 }
        return .continuityChanged
      },
      exactCompleteRequest: { _, _, _ in
        completeRequestCount.withValue { $0 += 1 }
        return .continuityChanged
      }
    )
    let expected = MLSDeviceAuthClientSnapshot(
      did: "did:plc:alice",
      authenticationMode: .gateway,
      authenticationGeneration: 1,
      mlsServiceDID: serviceDID
    )
    let beginGate = DeviceAuthCancellationLaunchGate()
    let completeGate = DeviceAuthCancellationLaunchGate()
    let begin = Task {
      await beginGate.waitForRelease()
      return try await api.begin(deviceID: "device-1", matching: expected)
    }
    let complete = Task {
      await completeGate.waitForRelease()
      return try await api.complete(
        challengeID: "challenge-1",
        signature: Data(repeating: 1, count: 64),
        matching: expected
      )
    }
    await beginGate.waitUntilHeld()
    await completeGate.waitUntilHeld()
    begin.cancel()
    complete.cancel()
    await beginGate.release()
    await completeGate.release()

    await assertCancellation { _ = try await begin.value }
    await assertCancellation { _ = try await complete.value }
    XCTAssertEqual(beginRequestCount.value, 0)
    XCTAssertEqual(completeRequestCount.value, 0)
  }

  func testPetrelCompletePreservesCancellationFromExactRequestErrorPath() async {
    let serviceDID = "did:web:mls.example#atproto_mls"
    let requestReached = LockedBox(false)
    let client = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: client,
      environment: .custom(serviceDID: serviceDID)
    )
    let api = MLSDeviceAuthPetrelAPI(
      apiClient: apiClient,
      exactCompleteRequest: { _, _, _ in
        requestReached.withValue { $0 = true }
        withUnsafeCurrentTask { $0?.cancel() }
        throw URLError(.cancelled)
      }
    )
    let expected = MLSDeviceAuthClientSnapshot(
      did: "did:plc:alice",
      authenticationMode: .gateway,
      authenticationGeneration: 1,
      mlsServiceDID: serviceDID
    )

    let operation = Task {
      try await api.complete(
        challengeID: "challenge-1",
        signature: Data(repeating: 1, count: 64),
        matching: expected
      )
    }

    do {
      _ = try await operation.value
      XCTFail("Expected cancellation")
    } catch is CancellationError {
      XCTAssertTrue(requestReached.value)
    } catch {
      XCTFail("Expected CancellationError, got \(error)")
    }
  }

  func testInvalidationCancelsInFlightEnrollmentAndClearsVolatileStatus() async throws {
    let api = DeviceAuthAPIStub()
    let service = makeService(api: api)
    _ = try await service.bind()
    let initialStatus = await service.status()
    XCTAssertNotNil(initialStatus)
    await service.invalidate()
    let invalidatedStatus = await service.status()
    XCTAssertNil(invalidatedStatus)

    await api.holdBegin()
    let inFlight = Task { try await service.bind() }
    await api.waitUntilBeginCount(2)
    await service.invalidate()
    await api.releaseBegin()

    do {
      _ = try await inFlight.value
      XCTFail("Expected cancellation")
    } catch is CancellationError {
      // Expected.
    } catch {
      XCTFail("Expected CancellationError, got \(error)")
    }
    let finalStatus = await service.status()
    let completedChallengeIDs = await api.completedChallengeIDs()
    XCTAssertNil(finalStatus)
    XCTAssertEqual(completedChallengeIDs, ["challenge-1"])
  }

  private func makeService(
    api: DeviceAuthAPIStub,
    signer: @escaping MLSDeviceAuthChallengeSigner = { _ in Data(repeating: 1, count: 64) }
  ) -> MLSDeviceAuthBindingService {
    MLSDeviceAuthBindingService(
      expectedDID: "did:plc:alice",
      deviceID: "device-1",
      api: api,
      signer: signer
    )
  }

  private func assertBindingError(
    _ expected: MLSDeviceAuthBindingError,
    operation: () async throws -> Void
  ) async {
    do {
      try await operation()
      XCTFail("Expected \(expected)")
    } catch let error as MLSDeviceAuthBindingError {
      XCTAssertEqual(error, expected)
    } catch {
      XCTFail("Expected MLSDeviceAuthBindingError, got \(error)")
    }
  }

  private func assertCancellation(operation: () async throws -> Void) async {
    do {
      try await operation()
      XCTFail("Expected cancellation")
    } catch is CancellationError {
      // Expected.
    } catch {
      XCTFail("Expected CancellationError, got \(error)")
    }
  }
}

private func waitForDeviceAuthSemaphore(_ semaphore: DispatchSemaphore) async {
  await withCheckedContinuation { continuation in
    DispatchQueue.global().async {
      semaphore.wait()
      continuation.resume()
    }
  }
}

private actor LegacyDeviceAuthAPIStub: MLSDeviceAuthBindingAPI {
  private var beginCalls = 0
  private var completeCalls = 0

  func snapshot() -> MLSDeviceAuthClientSnapshot {
    .init(
      did: "did:plc:alice",
      authenticationMode: .gateway,
      authenticationGeneration: 1
    )
  }

  func commitIfSnapshotMatches(
    _ expected: MLSDeviceAuthClientSnapshot,
    operation: @Sendable () -> Bool
  ) -> Bool {
    snapshot() == expected && operation()
  }

  func begin(deviceID: String) -> MLSDeviceAuthBindingChallenge {
    beginCalls += 1
    return .init(
      challengeID: "legacy-challenge",
      challenge: Data([1]),
      expiresAt: Date().addingTimeInterval(60),
      bindingVersion: 1
    )
  }

  func complete(challengeID: String, signature: Data) -> MLSDeviceAuthBindingCompletion? {
    completeCalls += 1
    return .init(deviceID: "device-1", boundAt: Date(), bindingVersion: 1)
  }

  func beginCount() -> Int { beginCalls }
  func completeCount() -> Int { completeCalls }
}

private actor DeviceAuthAPIStub: MLSDeviceAuthBindingAPI {
  private var currentSnapshot: MLSDeviceAuthClientSnapshot
  private let challengeResponse: MLSDeviceAuthBindingChallenge
  private let completionResponse: MLSDeviceAuthBindingCompletion?
  private var beginCalls: [String] = []
  private var completeIDs: [String] = []
  private var completeSignatures: [Data] = []
  private var beginSnapshots: [MLSDeviceAuthClientSnapshot] = []
  private var completeSnapshots: [MLSDeviceAuthClientSnapshot] = []
  private var beginContinuation: CheckedContinuation<Void, Never>?
  private var beginStartedContinuation: CheckedContinuation<Void, Never>?
  private var shouldHoldBegin = false
  private var shouldHoldNextBegin = false
  private var completeContinuation: CheckedContinuation<Void, Never>?
  private var completeStartedContinuation: CheckedContinuation<Void, Never>?
  private var shouldHoldNextComplete = false
  private var shouldHoldNextSnapshot = false
  private var snapshotCallCount = 0
  private var holdSnapshotAfterCaptureAtCall: Int?
  private var snapshotContinuation: CheckedContinuation<Void, Never>?
  private var snapshotStartedContinuation: CheckedContinuation<Void, Never>?
  private var afterComplete: (@Sendable () async -> Void)?
  private var beforeBeginExactOperation: (@Sendable () async -> Void)?

  init(
    snapshot: MLSDeviceAuthClientSnapshot = .init(
      did: "did:plc:alice",
      authenticationMode: .gateway,
      authenticationGeneration: 1
    ),
    challenge: MLSDeviceAuthBindingChallenge = .init(
      challengeID: "challenge-1",
      challenge: Data([1, 2, 3]),
      expiresAt: Date().addingTimeInterval(60),
      bindingVersion: 1
    ),
    completion: MLSDeviceAuthBindingCompletion? = .init(
      deviceID: "device-1",
      boundAt: Date(),
      bindingVersion: 1
    )
  ) {
    currentSnapshot = snapshot
    challengeResponse = challenge
    completionResponse = completion
  }

  func snapshot() async -> MLSDeviceAuthClientSnapshot {
    snapshotCallCount += 1
    let capturedSnapshot = currentSnapshot
    if holdSnapshotAfterCaptureAtCall == snapshotCallCount {
      holdSnapshotAfterCaptureAtCall = nil
      snapshotStartedContinuation?.resume()
      snapshotStartedContinuation = nil
      await withCheckedContinuation { continuation in
        snapshotContinuation = continuation
      }
      return capturedSnapshot
    }
    if shouldHoldNextSnapshot {
      shouldHoldNextSnapshot = false
      snapshotStartedContinuation?.resume()
      snapshotStartedContinuation = nil
      await withCheckedContinuation { continuation in
        snapshotContinuation = continuation
      }
    }
    return currentSnapshot
  }

  func commitIfSnapshotMatches(
    _ expected: MLSDeviceAuthClientSnapshot,
    operation: @Sendable () -> Bool
  ) -> Bool {
    guard currentSnapshot == expected else { return false }
    return operation()
  }

  func begin(deviceID: String) async throws -> MLSDeviceAuthBindingChallenge {
    beginCalls.append(deviceID)
    beginStartedContinuation?.resume()
    beginStartedContinuation = nil
    if shouldHoldBegin || shouldHoldNextBegin {
      shouldHoldNextBegin = false
      await withCheckedContinuation { continuation in
        beginContinuation = continuation
      }
    }
    return challengeResponse
  }

  func begin(
    deviceID: String,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingChallenge {
    if let beforeBeginExactOperation {
      self.beforeBeginExactOperation = nil
      await beforeBeginExactOperation()
    }
    guard currentSnapshot == expected else {
      throw MLSDeviceAuthBindingError.sessionChanged
    }
    beginSnapshots.append(expected)
    let challenge = try await begin(deviceID: deviceID)
    guard currentSnapshot == expected else {
      throw MLSDeviceAuthBindingError.sessionChanged
    }
    return challenge
  }

  func complete(challengeID: String, signature: Data) async throws
    -> MLSDeviceAuthBindingCompletion?
  {
    completeIDs.append(challengeID)
    completeSignatures.append(signature)
    completeStartedContinuation?.resume()
    completeStartedContinuation = nil
    if shouldHoldNextComplete {
      shouldHoldNextComplete = false
      await withCheckedContinuation { continuation in
        completeContinuation = continuation
      }
    }
    if let afterComplete { await afterComplete() }
    return completionResponse
  }

  func complete(
    challengeID: String,
    signature: Data,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingCompletion? {
    guard currentSnapshot == expected else {
      throw MLSDeviceAuthBindingError.sessionChanged
    }
    completeSnapshots.append(expected)
    let completion = try await complete(challengeID: challengeID, signature: signature)
    guard currentSnapshot == expected else {
      throw MLSDeviceAuthBindingError.sessionChanged
    }
    return completion
  }

  func setSnapshot(_ snapshot: MLSDeviceAuthClientSnapshot) { currentSnapshot = snapshot }
  func setAfterComplete(_ operation: @escaping @Sendable () async -> Void) {
    afterComplete = operation
  }
  func setBeforeBeginExactOperation(_ operation: @escaping @Sendable () async -> Void) {
    beforeBeginExactOperation = operation
  }
  func beginDeviceIDs() -> [String] { beginCalls }
  func completedChallengeIDs() -> [String] { completeIDs }
  func completedSignatures() -> [Data] { completeSignatures }
  func beginExpectedSnapshots() -> [MLSDeviceAuthClientSnapshot] { beginSnapshots }
  func completeExpectedSnapshots() -> [MLSDeviceAuthClientSnapshot] { completeSnapshots }

  func holdBegin() { shouldHoldBegin = true }
  func holdNextBegin() { shouldHoldNextBegin = true }
  func holdNextComplete() { shouldHoldNextComplete = true }
  func holdNextSnapshot() { shouldHoldNextSnapshot = true }
  func holdThirdNextSnapshotAfterCapture() {
    holdSnapshotAfterCaptureAtCall = snapshotCallCount + 3
  }

  func waitUntilSnapshotHeld() async {
    if snapshotContinuation != nil { return }
    await withCheckedContinuation { continuation in
      snapshotStartedContinuation = continuation
    }
  }

  func releaseSnapshot() {
    snapshotContinuation?.resume()
    snapshotContinuation = nil
  }

  func waitUntilBeginStarted() async {
    await waitUntilBeginCount(1)
  }

  func waitUntilBeginCount(_ count: Int) async {
    if beginCalls.count >= count { return }
    await withCheckedContinuation { continuation in
      beginStartedContinuation = continuation
    }
  }

  func releaseBegin() {
    shouldHoldBegin = false
    beginContinuation?.resume()
    beginContinuation = nil
  }

  func waitUntilCompleteStarted() async {
    if !completeIDs.isEmpty { return }
    await withCheckedContinuation { continuation in
      completeStartedContinuation = continuation
    }
  }

  func releaseComplete() {
    completeContinuation?.resume()
    completeContinuation = nil
  }
}

private actor DeviceAuthSignerRecorder {
  private var challenges: [Data] = []

  func sign(_ challenge: Data) -> Data {
    challenges.append(challenge)
    return Data(repeating: 1, count: 64)
  }

  func count() -> Int { challenges.count }
}

private actor DeviceAuthReplacementDetachGate {
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

private actor DeviceAuthCancellationLaunchGate {
  private var isHeld = false
  private var heldContinuation: CheckedContinuation<Void, Never>?
  private var releaseContinuation: CheckedContinuation<Void, Never>?

  func waitForRelease() async {
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

private final class LockedBox<Value>: @unchecked Sendable {
  private let lock = NSLock()
  private var storage: Value

  init(_ value: Value) { storage = value }

  var value: Value {
    lock.lock()
    defer { lock.unlock() }
    return storage
  }

  func withValue(_ operation: (inout Value) -> Void) {
    lock.lock()
    defer { lock.unlock() }
    operation(&storage)
  }
}
