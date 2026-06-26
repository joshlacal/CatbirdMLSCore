import XCTest
import CatbirdMLS
import GRDB
import Petrel

@testable import CatbirdMLSCore

final class MLSFullRustLifecycleTests: XCTestCase {
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

    let rustPrepared = await MainActor.run {
      manager.suspendMLSOperations()
    }

    XCTAssertTrue(rustPrepared)
    XCTAssertEqual(
      bridge.prepareCalls,
      [LifecycleCall(reason: "MLSConversationManager.suspendMLSOperations", deadlineMs: 1_500)]
    )
    XCTAssertNotNil(manager.orchestratorRuntime, "rustFull suspend should keep the runtime alive for resume")
  }

  func testRustAuthoritativeSuspendKeepsLegacyShutdownBehavior() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustAuthoritative)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustAuthoritative,
      bridge: bridge
    )

    let rustPrepared = await MainActor.run {
      manager.suspendMLSOperations()
    }

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

    let rustPrepared = await MainActor.run {
      manager.suspendMLSOperations()
    }

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

    let rustPrepared = await MainActor.run {
      manager.suspendMLSOperations()
    }

    XCTAssertFalse(rustPrepared)
    XCTAssertNil(manager.orchestratorRuntime)
  }

  func testRustFullManagerResumesThroughRuntimeLifecycleBridge() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    await MainActor.run {
      manager.isSuspending = true
    }
    await manager.resumeMLSOperations()

    XCTAssertEqual(
      bridge.resumeCalls,
      [LifecycleReasonCall(reason: "MLSConversationManager.resumeMLSOperations")]
    )
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
      MLSOrchestratorRuntime(
        userDID: "did:plc:testuser",
        mode: .rustFull,
        bridge: rebuiltBridge
      )
    }

    let rustPrepared = await MainActor.run {
      manager.suspendMLSOperations()
    }
    XCTAssertTrue(rustPrepared)

    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "unit-test force close")
    }

    await manager.resumeMLSOperations()

    XCTAssertTrue(staleBridge.resumeCalls.isEmpty)
    XCTAssertTrue(rebuiltBridge.initializeCalls.isEmpty)
    XCTAssertEqual(
      rebuiltBridge.reattachCalls,
      [LifecycleUserReasonCall(
        userDID: "did:plc:testuser",
        reason: "MLSConversationManager.resumeMLSOperations"
      )]
    )
    XCTAssertFalse(manager.rustRuntimeRequiresForegroundRestore)
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

    let rustPrepared = await MainActor.run {
      manager.suspendMLSOperations()
    }
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

    let rustPrepared = await MainActor.run {
      manager.suspendMLSOperations()
    }
    XCTAssertTrue(rustPrepared)

    await MainActor.run {
      manager.markRustRuntimeClosedForSuspend(reason: "unit-test force close")
    }

    await manager.resumeMLSOperations()

    XCTAssertTrue(
      bridge.resumeCalls.isEmpty,
      "stale runtime must not be resumed once app-level force close invalidates it"
    )
    XCTAssertTrue(manager.isSuspending, "resume should not silently re-enable work without a runtime")
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

  private func makeManager(
    protocolAuthorityMode: MLSProtocolAuthorityMode
  ) async throws -> MLSConversationManager {
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
      protocolAuthorityMode: protocolAuthorityMode
    )
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
  private(set) var shutdownCalled = false
  var prepareError: Error?

  init() {
    super.init(noPointer: .init())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    super.init(unsafeFromRawPointer: pointer)
  }

  override func initialize(userDid: String) throws {
    initializeCalls.append(userDid)
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

  override func reattachAfterSuspend(userDid: String, reason: String) throws {
    reattachCalls.append(LifecycleUserReasonCall(userDID: userDid, reason: reason))
  }

  override func resumeFromSuspend(reason: String) throws {
    resumeCalls.append(LifecycleReasonCall(reason: reason))
  }

  override func syncWithServer(fullSync: Bool) throws {
    syncCalls.append(fullSync)
  }

  override func interruptStorage(reason: String) throws {
    interruptCalls.append(LifecycleReasonCall(reason: reason))
  }

  override func emergencyClose(reason: String) throws {
    emergencyCloseCalls.append(LifecycleReasonCall(reason: reason))
  }

  override func shutdown() {
    shutdownCalled = true
  }
}

private enum TestLifecycleError: Error {
  case prepareFailed
}
