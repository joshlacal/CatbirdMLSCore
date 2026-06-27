import XCTest
import CatbirdMLS
import GRDB
import Petrel

@testable import CatbirdMLSCore

final class MLSRustAuthoritativeRecoveryTests: XCTestCase {
  func testRuntimeDebugWipeLocalGroupForRecoveryWrapsBridgeResult() throws {
    let bridge = RecordingJoinOrRejoinBridge(
      result: FfiJoinOrRejoinResult(epoch: 37, recoveryState: .resetPending)
    )
    bridge.debugWipeResult = FfiDebugWipeLocalGroupResult(
      conversationId: "convo-wipe",
      groupId: "abc123",
      deletedLocalGroup: true
    )
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:alice",
      mode: .rustFull,
      bridge: bridge
    )

    let result = try runtime.debugWipeLocalGroupForRecovery(conversationId: "convo-wipe")

    XCTAssertEqual(bridge.debugWipeCalls, ["convo-wipe"])
    XCTAssertEqual(result.conversationId, "convo-wipe")
    XCTAssertEqual(result.groupId, "abc123")
    XCTAssertTrue(result.deletedLocalGroup)
  }

  func testRuntimeJoinOrRejoinWrapsBridgeResult() throws {
    let bridge = RecordingJoinOrRejoinBridge(
      result: FfiJoinOrRejoinResult(epoch: 37, recoveryState: .resetPending)
    )
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:alice",
      mode: .rustAuthoritative,
      bridge: bridge
    )

    let result = try runtime.joinOrRejoin(conversationId: "convo-1")

    XCTAssertEqual(bridge.joinOrRejoinCalls, ["convo-1"])
    XCTAssertEqual(result.epoch, 37)
    XCTAssertEqual(result.recoveryState, .resetPending)
  }

  func testRustAuthoritativeManagerRoutesJoinOrRejoinThroughRuntime() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustAuthoritative)
    let bridge = RecordingJoinOrRejoinBridge(
      result: FfiJoinOrRejoinResult(epoch: 8, recoveryState: .healthy)
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustAuthoritative,
      bridge: bridge
    )

    let result = try await manager.joinOrRejoinWithRustAuthorityIfNeeded(
      conversationId: "convo-rust",
      operation: "test"
    )

    XCTAssertEqual(bridge.joinOrRejoinCalls, ["convo-rust"])
    XCTAssertEqual(result?.epoch, 8)
    XCTAssertEqual(result?.recoveryState, .healthy)
  }

  func testRustFullManagerRoutesDebugWipeThroughRuntime() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingJoinOrRejoinBridge(
      result: FfiJoinOrRejoinResult(epoch: 8, recoveryState: .healthy)
    )
    bridge.debugWipeResult = FfiDebugWipeLocalGroupResult(
      conversationId: "convo-debug-wipe",
      groupId: "fedcba",
      deletedLocalGroup: true
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let result = try await manager.debugWipeLocalGroupForRecovery(
      conversationId: "convo-debug-wipe"
    )

    XCTAssertEqual(bridge.debugWipeCalls, ["convo-debug-wipe"])
    XCTAssertEqual(result?.conversationId, "convo-debug-wipe")
    XCTAssertEqual(result?.groupId, "fedcba")
    XCTAssertEqual(result?.deletedLocalGroup, true)
  }

  func testPublicJoinOrRejoinConversationRoutesRustFullThroughRuntime() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingJoinOrRejoinBridge(
      result: FfiJoinOrRejoinResult(epoch: 21, recoveryState: .needsRejoin)
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let result = try await manager.joinOrRejoinConversation(conversationId: "convo-public")

    XCTAssertEqual(bridge.joinOrRejoinCalls, ["convo-public"])
    XCTAssertEqual(result?.epoch, 21)
    XCTAssertEqual(result?.recoveryState, .needsRejoin)
  }

  func testRustShadowDoesNotMutateByCallingJoinOrRejoin() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustShadow)
    let bridge = RecordingJoinOrRejoinBridge(
      result: FfiJoinOrRejoinResult(epoch: 8, recoveryState: .healthy)
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustShadow,
      bridge: bridge
    )

    let result = try await manager.joinOrRejoinWithRustAuthorityIfNeeded(
      conversationId: "convo-shadow",
      operation: "test"
    )

    XCTAssertNil(result)
    XCTAssertEqual(bridge.joinOrRejoinCalls, [])
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

private final class RecordingJoinOrRejoinBridge: OrchestratorBridge {
  private let result: FfiJoinOrRejoinResult
  private(set) var joinOrRejoinCalls: [String] = []
  private(set) var debugWipeCalls: [String] = []
  var debugWipeResult = FfiDebugWipeLocalGroupResult(
    conversationId: "",
    groupId: nil,
    deletedLocalGroup: false
  )

  init(result: FfiJoinOrRejoinResult) {
    self.result = result
    super.init(noPointer: .init())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    self.result = FfiJoinOrRejoinResult(epoch: 0, recoveryState: .healthy)
    super.init(unsafeFromRawPointer: pointer)
  }

  override func joinOrRejoin(convoId: String) throws -> FfiJoinOrRejoinResult {
    joinOrRejoinCalls.append(convoId)
    return result
  }

  override func debugWipeLocalGroupForRecovery(convoId: String) throws -> FfiDebugWipeLocalGroupResult {
    debugWipeCalls.append(convoId)
    return debugWipeResult
  }

  override func shutdown() {
  }

  override func getConversationRecoveryState(conversationId: String) throws -> FfiConversationRecoveryState {
    result.recoveryState
  }
}
