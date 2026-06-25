import XCTest
import CatbirdMLS
import GRDB
import Petrel

@testable import CatbirdMLSCore

final class MLSFullRustRecoveryRoutingTests: XCTestCase {
  func testRuntimeStartupReconcileWrapsBridgeResult() throws {
    let bridge = RecordingStartupReconcileBridge()
    bridge.report = FfiStartupReconcileReport(
      scanned: 4,
      healthy: 2,
      needsRejoin: 1,
      resetPending: 1,
      unrecoverableLocal: 0
    )
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:alice",
      mode: .rustFull,
      bridge: bridge
    )

    let report = try runtime.startupReconcile()

    XCTAssertEqual(bridge.startupReconcileCallCount, 1)
    XCTAssertEqual(report.scanned, 4)
    XCTAssertEqual(report.healthy, 2)
    XCTAssertEqual(report.needsRejoin, 1)
    XCTAssertEqual(report.resetPending, 1)
    XCTAssertEqual(report.unrecoverableLocal, 0)
  }

  func testRustFullValidateGroupStatesRoutesThroughRuntimeStartupReconcile() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingStartupReconcileBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    await manager.validateGroupStates()

    XCTAssertEqual(bridge.startupReconcileCallCount, 1)
  }

  func testRustAuthoritativeValidateGroupStatesDoesNotCallStartupReconcile() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustAuthoritative)
    let bridge = RecordingStartupReconcileBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustAuthoritative,
      bridge: bridge
    )

    await manager.validateGroupStates()

    XCTAssertEqual(bridge.startupReconcileCallCount, 0)
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

private final class RecordingStartupReconcileBridge: OrchestratorBridge {
  var report = FfiStartupReconcileReport(
    scanned: 0,
    healthy: 0,
    needsRejoin: 0,
    resetPending: 0,
    unrecoverableLocal: 0
  )
  private(set) var startupReconcileCallCount = 0

  init() {
    super.init(noPointer: .init())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    super.init(unsafeFromRawPointer: pointer)
  }

  override func startupReconcile() throws -> FfiStartupReconcileReport {
    startupReconcileCallCount += 1
    return report
  }

  override func shutdown() {
  }
}
