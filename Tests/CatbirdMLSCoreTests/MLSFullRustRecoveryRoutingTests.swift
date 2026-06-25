import XCTest
import CatbirdMLS
import GRDB
import Petrel

@testable import CatbirdMLSCore

final class MLSFullRustRecoveryRoutingTests: XCTestCase {
  func testRuntimeRunDeferredRecoveryWrapsBridgeResult() throws {
    let bridge = RecordingStartupReconcileBridge()
    bridge.deferredRecoveryReport = FfiDeferredRecoveryReport(
      scanned: 3,
      attempted: 1,
      recovered: 1,
      skipped: 1,
      failed: 0
    )
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:alice",
      mode: .rustFull,
      bridge: bridge
    )

    let report = try runtime.runDeferredRecovery(reason: "unit-test")

    XCTAssertEqual(bridge.runDeferredRecoveryCallCount, 1)
    XCTAssertEqual(bridge.lastDeferredRecoveryReason, "unit-test")
    XCTAssertEqual(report.scanned, 3)
    XCTAssertEqual(report.attempted, 1)
    XCTAssertEqual(report.recovered, 1)
    XCTAssertEqual(report.skipped, 1)
    XCTAssertEqual(report.failed, 0)
  }

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

  func testRustFullValidateGroupStatesDoesNotRunLegacyEpochDeleteSweep() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingStartupReconcileBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    try await manager.database.write { db in
      try MLSConversationModel(
        conversationID: "convo-missing",
        currentUserDID: "did:plc:testuser",
        groupID: Data([0xde, 0xad, 0xbe, 0xef])
      ).insert(db)
    }

    await manager.validateGroupStates()

    let conversation = try await manager.database.read { db in
      try MLSConversationModel.fetchOne(
        db,
        key: ["conversationID": "convo-missing", "currentUserDID": "did:plc:testuser"]
      )
    }

    XCTAssertEqual(bridge.startupReconcileCallCount, 1)
    XCTAssertEqual(conversation?.needsRejoin, false)
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

  func testRustFullRunDeferredEpochRecoveryRoutesThroughRuntimeOnly() async throws {
    let manager = try await makeAuthenticatedManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingStartupReconcileBridge()
    bridge.deferredRecoveryReport = FfiDeferredRecoveryReport(
      scanned: 1,
      attempted: 0,
      recovered: 0,
      skipped: 1,
      failed: 0
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    try await manager.database.write { db in
      try MLSConversationModel(
        conversationID: "convo-needs-rejoin",
        currentUserDID: "did:plc:testuser",
        groupID: Data([0xde, 0xad, 0xbe, 0xef])
      ).insert(db)
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET needsRejoin = 1
          WHERE conversationID = ? AND currentUserDID = ?;
        """,
        arguments: ["convo-needs-rejoin", "did:plc:testuser"]
      )
    }

    try await manager.runDeferredEpochRecovery()

    let conversation = try await manager.database.read { db in
      try MLSConversationModel.fetchOne(
        db,
        key: ["conversationID": "convo-needs-rejoin", "currentUserDID": "did:plc:testuser"]
      )
    }

    XCTAssertEqual(bridge.runDeferredRecoveryCallCount, 1)
    XCTAssertEqual(bridge.lastDeferredRecoveryReason, "swift-scheduler-request")
    XCTAssertEqual(conversation?.needsRejoin, true)
  }

  func testRustFullRunDeferredEpochRecoverySkipsInactiveAccountBeforeRuntime() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingStartupReconcileBridge()
    bridge.deferredRecoveryReport = FfiDeferredRecoveryReport(
      scanned: 1,
      attempted: 0,
      recovered: 0,
      skipped: 1,
      failed: 0
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    try await manager.database.write { db in
      try MLSConversationModel(
        conversationID: "convo-inactive",
        currentUserDID: "did:plc:testuser",
        groupID: Data([0xca, 0xfe, 0xba, 0xbe])
      ).insert(db)
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET needsRejoin = 1
          WHERE conversationID = ? AND currentUserDID = ?;
        """,
        arguments: ["convo-inactive", "did:plc:testuser"]
      )
    }

    try await manager.runDeferredEpochRecovery()

    let conversation = try await manager.database.read { db in
      try MLSConversationModel.fetchOne(
        db,
        key: ["conversationID": "convo-inactive", "currentUserDID": "did:plc:testuser"]
      )
    }

    XCTAssertEqual(bridge.runDeferredRecoveryCallCount, 0)
    XCTAssertNil(bridge.lastDeferredRecoveryReason)
    XCTAssertEqual(conversation?.needsRejoin, true)
  }

  private func makeManager(
    protocolAuthorityMode: MLSProtocolAuthorityMode
  ) async throws -> MLSConversationManager {
    let database = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(database)
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

  private func makeAuthenticatedManager(
    protocolAuthorityMode: MLSProtocolAuthorityMode
  ) async throws -> MLSConversationManager {
    let database = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(database)

    let userDid = "did:plc:testuser"
    let namespace = "MLSFullRustRecoveryRoutingTests.\(UUID().uuidString)"
    let storage = KeychainStorage(namespace: namespace)
    try await storage.saveAccount(
      Account(
        did: userDid,
        handle: "testuser.bsky.social",
        pdsURL: URL(string: "https://example.com")!
      ),
      for: userDid
    )
    try await storage.saveSession(
      Session(
        accessToken: "access-token",
        refreshToken: "refresh-token",
        createdAt: Date(),
        expiresIn: 3600,
        tokenType: .bearer,
        did: userDid
      ),
      for: userDid
    )
    try await storage.saveCurrentDID(userDid)

    let atProtoClient = try await ATProtoClient(
      baseURL: URL(string: "https://example.com")!,
      oauthConfig: OAuthConfig(
        clientId: "unit-test-client",
        redirectUri: "catbird://tests/oauth",
        scope: "atproto"
      ),
      namespace: namespace,
      authMode: .legacy
    )
    let apiClient = await MLSAPIClient(
      client: atProtoClient,
      environment: .custom(serviceDID: "did:web:example.com#atproto_mls")
    )
    return MLSConversationManager(
      apiClient: apiClient,
      database: database,
      userDid: userDid,
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
  var deferredRecoveryReport = FfiDeferredRecoveryReport(
    scanned: 0,
    attempted: 0,
    recovered: 0,
    skipped: 0,
    failed: 0
  )
  private(set) var startupReconcileCallCount = 0
  private(set) var runDeferredRecoveryCallCount = 0
  private(set) var lastDeferredRecoveryReason: String?

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

  override func runDeferredRecovery(reason: String) throws -> FfiDeferredRecoveryReport {
    runDeferredRecoveryCallCount += 1
    lastDeferredRecoveryReason = reason
    return deferredRecoveryReport
  }

  override func shutdown() {
  }
}
