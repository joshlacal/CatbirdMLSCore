import XCTest
import CatbirdMLS
import GRDB
import Petrel

@testable import CatbirdMLSCore

final class MLSFullRustLifecycleTests: XCTestCase {
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

    await MainActor.run {
      manager.suspendMLSOperations()
    }

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

    await MainActor.run {
      manager.suspendMLSOperations()
    }

    XCTAssertTrue(bridge.prepareCalls.isEmpty)
    XCTAssertTrue(bridge.shutdownCalled)
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
      manager.resumeMLSOperations()
    }

    XCTAssertEqual(
      bridge.resumeCalls,
      [LifecycleReasonCall(reason: "MLSConversationManager.resumeMLSOperations")]
    )
  }

  private func makeManager(
    protocolAuthorityMode: MLSProtocolAuthorityMode
  ) async throws -> MLSConversationManager {
    let database = try DatabaseQueue()
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

private final class RecordingLifecycleBridge: OrchestratorBridge {
  var prepareResult = FfiSuspendResult(
    acceptingNewWork: false,
    interruptedContexts: 1
  )
  private(set) var prepareCalls: [LifecycleCall] = []
  private(set) var resumeCalls: [LifecycleReasonCall] = []
  private(set) var interruptCalls: [LifecycleReasonCall] = []
  private(set) var emergencyCloseCalls: [LifecycleReasonCall] = []
  private(set) var shutdownCalled = false

  init() {
    super.init(noPointer: .init())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    super.init(unsafeFromRawPointer: pointer)
  }

  override func prepareForSuspend(reason: String, deadlineMs: UInt64) throws -> FfiSuspendResult {
    prepareCalls.append(LifecycleCall(reason: reason, deadlineMs: deadlineMs))
    return prepareResult
  }

  override func resumeFromSuspend(reason: String) throws {
    resumeCalls.append(LifecycleReasonCall(reason: reason))
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
