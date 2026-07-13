import CatbirdMLS
import Foundation
import GRDB
import Petrel
import Synchronization
import XCTest

@testable import CatbirdMLSCore

final class MLSDeviceAuthLifecycleTests: XCTestCase {
  func testLegacySignerRequiresExactRegisteredBareDID() throws {
    XCTAssertEqual(
      try MLSClient.deviceAuthSignerIdentity(
        expectedDID: " did:plc:alice ",
        registeredIdentity: "did:plc:alice"
      ),
      "did:plc:alice"
    )

    for identity in [nil, "", "did:plc:mallory", "did:plc:alice#device-1"] {
      XCTAssertThrowsError(
        try MLSClient.deviceAuthSignerIdentity(
          expectedDID: "did:plc:alice",
          registeredIdentity: identity
        )
      ) { error in
        XCTAssertEqual(error as? MLSDeviceAuthBindingError, .sessionChanged)
      }
    }
  }

  func testRustRuntimeSignsOnlyThroughDeviceAuthBridgeMethod() throws {
    let bridge = DeviceAuthRecordingBridge()
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:alice",
      mode: .rustFull,
      bridge: bridge
    )
    let challenge = Data([0x00, 0xff, 0x41])

    let signature = try runtime.signDeviceAuthChallenge(challenge)

    XCTAssertEqual(bridge.challenges, [challenge])
    XCTAssertEqual(signature, Data(repeating: 0x7a, count: 64))
  }

  func testRustBindingUsesOneRuntimeForDeviceResolutionAndChallengeSigning() async throws {
    let manager = try await makeManager()
    let originalBridge = DeviceAuthRecordingBridge()
    let replacementBridge = DeviceAuthRecordingBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: originalBridge
    )
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }
    let api = SuspendedDeviceAuthBindingAPI()
    manager.deviceAuthBindingAPIOverride = api

    let binding = Task {
      try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    }
    await api.waitUntilBeginHeld()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: replacementBridge
    )
    await api.releaseBegin()

    _ = try await binding.value
    XCTAssertEqual(originalBridge.challenges.count, 1)
    XCTAssertTrue(replacementBridge.challenges.isEmpty)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")
  }

  func testConcurrentRustRegistrationAndPushLookupShareOneEnrollment() async throws {
    let manager = try await makeManager()
    let bridge = DeviceAuthRecordingBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }
    let api = SuspendedDeviceAuthBindingAPI()
    manager.deviceAuthBindingAPIOverride = api

    let registration = Task {
      try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    }
    await api.waitUntilBeginHeld()
    let pushLookup = Task {
      try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    }
    for _ in 0..<100 { await Task.yield() }
    await api.releaseBegin()

    let registrationStatus = try await registration.value
    let pushStatus = try await pushLookup.value
    let beginCount = await api.beginCount()
    XCTAssertEqual(registrationStatus, pushStatus)
    XCTAssertEqual(beginCount, 1)
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")

    let repositoryRoot = URL(fileURLWithPath: #filePath)
      .deletingLastPathComponent()
      .deletingLastPathComponent()
      .deletingLastPathComponent()
    let authoritySource = try String(
      contentsOf: repositoryRoot.appendingPathComponent(
        "Sources/CatbirdMLSCore/Service/MLSConversationManager+ProtocolAuthority.swift"
      ),
      encoding: .utf8
    )
    XCTAssertFalse(
      authoritySource.contains("bindRustDeviceAuthentication(userDid: userDid, force: true)"),
      "Benign push-token lookups must coalesce with registration instead of resetting it"
    )
  }

  func testRustSuspendPrepareWaitsForSynchronousDeviceAuthSigning() async throws {
    let manager = try await makeManager()
    let bridge = DeviceAuthRecordingBridge(holdSigning: true)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }
    let api = SuspendedDeviceAuthBindingAPI()
    manager.deviceAuthBindingAPIOverride = api
    defer {
      bridge.releaseSigning()
      MLSConversationManager.resetSuspensionStateForTesting()
      MLSClient.resetDeviceAuthSuspensionStateForTesting()
    }

    let binding = Task {
      try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    }
    await api.waitUntilBeginHeld()
    await api.releaseBegin()
    XCTAssertTrue(bridge.waitUntilSigningStarts())
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    let rustPathAvailable = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPathAvailable)
    XCTAssertEqual(bridge.prepareCallCount, 0)

    let preparationFinished = Mutex(false)
    let preparation = Task { @MainActor in
      let prepared = await manager.prepareRustRuntimeForSuspensionAfterDrain(timeout: 2)
      preparationFinished.withLock { $0 = true }
      return prepared
    }
    try await Task.sleep(nanoseconds: 100_000_000)

    XCTAssertFalse(preparationFinished.withLock { $0 })
    XCTAssertEqual(bridge.prepareCallCount, 0)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)

    bridge.releaseSigning()
    let prepared = await preparation.value
    XCTAssertTrue(prepared)
    XCTAssertEqual(bridge.prepareCallCount, 1)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 0)
    _ = try? await binding.value
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")
  }

  func testRustSuspendPrepareTimeoutInterruptsWithoutPreparingOrOpeningGates() async throws {
    let manager = try await makeManager()
    let bridge = DeviceAuthRecordingBridge(holdSigning: true)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }
    let api = SuspendedDeviceAuthBindingAPI()
    manager.deviceAuthBindingAPIOverride = api
    defer {
      bridge.releaseSigning()
      MLSConversationManager.resetSuspensionStateForTesting()
      MLSClient.resetDeviceAuthSuspensionStateForTesting()
    }

    let binding = Task {
      try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    }
    await api.waitUntilBeginHeld()
    await api.releaseBegin()
    XCTAssertTrue(bridge.waitUntilSigningStarts())
    let rustPathAvailable = await MainActor.run { manager.suspendMLSOperations() }
    XCTAssertTrue(rustPathAvailable)

    let prepared = await manager.prepareRustRuntimeForSuspensionAfterDrain(timeout: 0.05)

    XCTAssertFalse(prepared)
    XCTAssertEqual(bridge.prepareCallCount, 0)
    XCTAssertEqual(MLSClient.inFlightFFIOperationCount, 1)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)

    bridge.releaseSigning()
    _ = try? await binding.value
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")
  }

  func testPublicForceRebindFailurePreservesDetachedAuthObligationAcrossSuspension()
    async throws
  {
    let manager = try await makeManager()
    let bridge = DeviceAuthRecordingBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }
    let api = SuspendedDeviceAuthBindingAPI()
    manager.deviceAuthBindingAPIOverride = api
    let initialBinding = Task {
      try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    }
    await api.waitUntilBeginHeld()
    await api.releaseBegin()
    _ = try await initialBinding.value

    let detachGate = DeviceAuthForceRebindGate()
    await MLSClient.shared.setDeviceAuthAfterReplacementDetachOverride {
      await detachGate.holdOnce()
    }
    let rebind = Task { try await manager.rebindCurrentDeviceAuthentication() }
    await detachGate.waitUntilHeld()

    XCTAssertTrue(
      MLSClient.isDeviceAuthRebindObligationPendingForTesting("did:plc:testuser"))
    MLSClient.markSuspensionInProgress(reason: "public force rebind failure race")
    XCTAssertTrue(
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: "did:plc:testuser"))
    XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "public force rebind held"))

    await detachGate.release()
    do {
      _ = try await rebind.value
      XCTFail("Expected force rebind to fail after suspension")
    } catch {}
    XCTAssertTrue(
      MLSClient.isDeviceAuthRebindObligationPendingForTesting("did:plc:testuser"))
    XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "failed public force rebind"))

    await MLSClient.shared.setDeviceAuthAfterReplacementDetachOverride(nil)
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")
  }

  func testCancelledPublicForceRebindPreservesDetachedAuthObligation() async throws {
    let manager = try await makeManager()
    let bridge = DeviceAuthRecordingBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )
    manager.deviceAuthDeviceIDProviderOverride = { "device-1" }
    let api = SuspendedDeviceAuthBindingAPI()
    manager.deviceAuthBindingAPIOverride = api
    let initialBinding = Task {
      try await manager.bindRustDeviceAuthentication(userDid: "did:plc:testuser")
    }
    await api.waitUntilBeginHeld()
    await api.releaseBegin()
    _ = try await initialBinding.value

    let detachGate = DeviceAuthForceRebindGate()
    await MLSClient.shared.setDeviceAuthAfterReplacementDetachOverride {
      await detachGate.holdOnce()
    }
    let rebind = Task { try await manager.rebindCurrentDeviceAuthentication() }
    await detachGate.waitUntilHeld()
    rebind.cancel()
    MLSClient.markSuspensionInProgress(reason: "cancelled public force rebind race")
    await detachGate.release()
    _ = try? await rebind.value

    XCTAssertTrue(
      MLSClient.isDeviceAuthRebindObligationPendingForTesting("did:plc:testuser"))
    XCTAssertTrue(
      MLSClient.deviceAuthBindingWasActiveAtSuspensionTransition(for: "did:plc:testuser"))
    XCTAssertFalse(MLSClient.clearSuspensionFlag(reason: "cancelled public force rebind"))

    await MLSClient.shared.setDeviceAuthAfterReplacementDetachOverride(nil)
    MLSClient.resetDeviceAuthSuspensionStateForTesting()
    await MLSClient.shared.invalidateDeviceAuthBinding(for: "did:plc:testuser")
  }

  func testRegistrationAndShutdownSourcesKeepBindingLifecycleWired() throws {
    let repositoryRoot = URL(fileURLWithPath: #filePath)
      .deletingLastPathComponent()
      .deletingLastPathComponent()
      .deletingLastPathComponent()
    let clientSource = try String(
      contentsOf: repositoryRoot.appendingPathComponent(
        "Sources/CatbirdMLSCore/Service/MLSClient.swift"),
      encoding: .utf8
    )
    let lifecycleSource = try String(
      contentsOf: repositoryRoot.appendingPathComponent(
        "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+Lifecycle.swift"
      ),
      encoding: .utf8
    )
    let bindingSource = try String(
      contentsOf: repositoryRoot.appendingPathComponent(
        "Sources/CatbirdMLSCore/Service/MLSDeviceAuthBindingService.swift"
      ),
      encoding: .utf8
    )

    XCTAssertTrue(clientSource.contains("disposition: .replacement"))
    XCTAssertTrue(clientSource.contains("bindLegacyDeviceAuthentication(for: normalizedDID)"))
    XCTAssertTrue(clientSource.contains("enrollment.task.cancel()"))
    XCTAssertTrue(lifecycleSource.contains("bindRustDeviceAuthentication"))
    XCTAssertTrue(lifecycleSource.contains("invalidateDeviceAuthBinding"))
    XCTAssertTrue(bindingSource.contains("client.authContinuitySnapshot()"))
    XCTAssertFalse(bindingSource.contains("ObjectIdentifier(client)"))
  }

  private func makeManager() async throws -> MLSConversationManager {
    let databasePath = URL(fileURLWithPath: NSTemporaryDirectory())
      .appendingPathComponent(UUID().uuidString)
      .appendingPathExtension("sqlite")
      .path
    let database = try DatabasePool(path: databasePath)
    let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: atProtoClient,
      environment: .custom(serviceDID: "did:web:example.com#atproto_mls")
    )
    return MLSConversationManager(
      apiClient: apiClient,
      database: database,
      userDid: "did:plc:testuser",
      atProtoClient: atProtoClient,
      protocolAuthorityMode: .rustFull
    )
  }
}

private final class DeviceAuthRecordingBridge: OrchestratorBridge {
  private let lock = NSLock()
  private var recordedChallenges: [Data] = []
  private var recordedPrepareCalls = 0
  private let holdSigning: Bool
  private let signingStarted = DispatchSemaphore(value: 0)
  private let releaseSigningSemaphore = DispatchSemaphore(value: 0)

  var challenges: [Data] { lock.withLock { recordedChallenges } }
  var prepareCallCount: Int { lock.withLock { recordedPrepareCalls } }

  init(holdSigning: Bool = false) {
    self.holdSigning = holdSigning
    super.init(noPointer: .init())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    holdSigning = false
    super.init(unsafeFromRawPointer: pointer)
  }

  override func signDeviceAuthChallenge(challenge: Data) throws -> Data {
    lock.withLock { recordedChallenges.append(challenge) }
    if holdSigning {
      signingStarted.signal()
      releaseSigningSemaphore.wait()
    }
    return Data(repeating: 0x7a, count: 64)
  }

  override func prepareForSuspend(reason: String, deadlineMs: UInt64) throws -> FfiSuspendResult {
    lock.withLock { recordedPrepareCalls += 1 }
    return FfiSuspendResult(acceptingNewWork: false, interruptedContexts: 0)
  }

  override func ensureDeviceRegistered() throws -> String {
    "did:plc:testuser"
  }

  func waitUntilSigningStarts() -> Bool {
    signingStarted.wait(timeout: .now() + 2) == .success
  }

  func releaseSigning() {
    releaseSigningSemaphore.signal()
  }

  override func shutdown() {}
}

private actor SuspendedDeviceAuthBindingAPI: MLSDeviceAuthBindingAPI {
  private var beginCalls = 0
  private var beginContinuation: CheckedContinuation<Void, Never>?
  private var beginHeldContinuation: CheckedContinuation<Void, Never>?

  func snapshot() -> MLSDeviceAuthClientSnapshot {
    MLSDeviceAuthClientSnapshot(
      did: "did:plc:testuser",
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
    beginHeldContinuation?.resume()
    beginHeldContinuation = nil
    await withCheckedContinuation { continuation in
      beginContinuation = continuation
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
    guard snapshot() == expected else { throw MLSDeviceAuthBindingError.sessionChanged }
    let challenge = await begin(deviceID: deviceID)
    guard snapshot() == expected else { throw MLSDeviceAuthBindingError.sessionChanged }
    return challenge
  }

  func complete(
    challengeID: String,
    signature: Data
  ) -> MLSDeviceAuthBindingCompletion? {
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
  ) throws -> MLSDeviceAuthBindingCompletion? {
    guard snapshot() == expected else { throw MLSDeviceAuthBindingError.sessionChanged }
    return complete(challengeID: challengeID, signature: signature)
  }

  func waitUntilBeginHeld() async {
    if beginContinuation != nil { return }
    await withCheckedContinuation { continuation in
      beginHeldContinuation = continuation
    }
  }

  func releaseBegin() {
    beginContinuation?.resume()
    beginContinuation = nil
  }

  func beginCount() -> Int { beginCalls }
}

private actor DeviceAuthForceRebindGate {
  private var didHold = false
  private var heldContinuation: CheckedContinuation<Void, Never>?
  private var releaseContinuation: CheckedContinuation<Void, Never>?

  func holdOnce() async {
    guard !didHold else { return }
    didHold = true
    heldContinuation?.resume()
    heldContinuation = nil
    await withCheckedContinuation { continuation in
      releaseContinuation = continuation
    }
  }

  func waitUntilHeld() async {
    if didHold { return }
    await withCheckedContinuation { continuation in
      heldContinuation = continuation
    }
  }

  func release() {
    releaseContinuation?.resume()
    releaseContinuation = nil
  }
}
