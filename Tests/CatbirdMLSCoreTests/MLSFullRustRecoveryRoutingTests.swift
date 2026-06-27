import XCTest
import CatbirdMLS
import GRDB
import Petrel

@testable import CatbirdMLSCore

final class MLSFullRustRecoveryRoutingTests: XCTestCase {
  func testRuntimeEnsureConversationReadyWrapsBridgeResult() throws {
    let bridge = RecordingStartupReconcileBridge()
    bridge.conversationReadyResult = FfiConversationReadyResult(
      recoveryState: .recovering,
      epoch: 17,
      sendAllowed: false
    )
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:alice",
      mode: .rustFull,
      bridge: bridge
    )

    let result = try runtime.ensureConversationReady(conversationId: "convo-1")

    XCTAssertEqual(bridge.ensureConversationReadyCallCount, 1)
    XCTAssertEqual(bridge.lastEnsureConversationReadyConversationId, "convo-1")
    XCTAssertEqual(result.recoveryState, .recovering)
    XCTAssertEqual(result.epoch, 17)
    XCTAssertFalse(result.sendAllowed)
  }

  func testRustFullEnsureGroupInitializedCallsOnlyRustEnsureConversationReady() async throws {
    let manager = try await makeAuthenticatedManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-ready", on: manager)

    let bridge = RecordingStartupReconcileBridge()
    bridge.conversationReadyResult = FfiConversationReadyResult(
      recoveryState: .healthy,
      epoch: 5,
      sendAllowed: true
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    try await manager.ensureGroupInitialized(for: "convo-ready")

    XCTAssertEqual(bridge.ensureConversationReadyCallCount, 1)
    XCTAssertEqual(bridge.lastEnsureConversationReadyConversationId, "convo-ready")
    XCTAssertEqual(bridge.joinOrRejoinCallCount, 0)
  }

  func testRustFullEnsureGroupInitializedRejectsHealthyButNonSendableRustReadiness() async throws {
    let manager = try await makeAuthenticatedManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-not-ready", on: manager)

    let bridge = RecordingStartupReconcileBridge()
    bridge.conversationReadyResult = FfiConversationReadyResult(
      recoveryState: .healthy,
      epoch: 5,
      sendAllowed: false
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    await XCTAssertThrowsErrorAsync(try await manager.ensureGroupInitialized(for: "convo-not-ready")) { error in
      guard case MLSConversationError.groupNotInitialized = error else {
        return XCTFail("Expected groupNotInitialized, got \(error)")
      }
    }

    XCTAssertEqual(bridge.ensureConversationReadyCallCount, 1)
    XCTAssertEqual(bridge.lastEnsureConversationReadyConversationId, "convo-not-ready")
    XCTAssertEqual(bridge.joinOrRejoinCallCount, 0)
  }

  func testRustFullEnsureGroupInitializedSkipsInactiveAccountBeforeRuntime() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-inactive", on: manager)

    let bridge = RecordingStartupReconcileBridge()
    bridge.conversationReadyResult = FfiConversationReadyResult(
      recoveryState: .healthy,
      epoch: 9,
      sendAllowed: true
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    await XCTAssertThrowsErrorAsync(try await manager.ensureGroupInitialized(for: "convo-inactive")) { error in
      guard case MLSConversationError.groupNotInitialized = error else {
        return XCTFail("Expected groupNotInitialized, got \(error)")
      }
    }

    XCTAssertEqual(bridge.ensureConversationReadyCallCount, 0)
    XCTAssertEqual(bridge.joinOrRejoinCallCount, 0)
  }

  func testRustShadowEnsureGroupInitializedDoesNotShortCircuitOnShutdownBeforeModeSplit() async throws {
    let manager = try await makeAuthenticatedManager(protocolAuthorityMode: .rustShadow)
    manager.isShuttingDown = true

    await XCTAssertThrowsErrorAsync(try await manager.ensureGroupInitialized(for: "missing-convo")) { error in
      guard case MLSConversationError.conversationNotFound = error else {
        return XCTFail("Expected conversationNotFound, got \(error)")
      }
    }
  }

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

  func testRustFullStartupReconcileRunsOnlyOnceAcrossLifecycleHooks() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingStartupReconcileBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    await manager.validateGroupStates()
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

  func testRustFullDetectAndRejoinMissingConversationsRoutesThroughRuntimeStartupReconcile() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingStartupReconcileBridge()
    bridge.report = FfiStartupReconcileReport(
      scanned: 3,
      healthy: 1,
      needsRejoin: 1,
      resetPending: 1,
      unrecoverableLocal: 0
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    try await manager.detectAndRejoinMissingConversations()

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

  func testRustAuthoritativeSyncWithServerHonorsPauseBeforeRuntime() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustAuthoritative)
    let bridge = RecordingStartupReconcileBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustAuthoritative,
      bridge: bridge
    )
    manager.isSyncPaused = true

    try await manager.syncWithServer(fullSync: false)

    XCTAssertEqual(bridge.syncWithServerCallCount, 0)
    XCTAssertNil(bridge.lastSyncWithServerFullSync)
  }

  func testRustAuthoritativeSyncWithServerHonorsShutdownBeforeRuntime() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustAuthoritative)
    let bridge = RecordingStartupReconcileBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustAuthoritative,
      bridge: bridge
    )
    manager.isShuttingDown = true

    await XCTAssertThrowsErrorAsync(try await manager.syncWithServer(fullSync: false)) { error in
      guard case MLSConversationError.operationFailed = error else {
        return XCTFail("Expected operationFailed, got \(error)")
      }
    }

    XCTAssertEqual(bridge.syncWithServerCallCount, 0)
    XCTAssertNil(bridge.lastSyncWithServerFullSync)
  }

  func testRustFullSyncFallsBackToPersistedRustStorageWhenRustSnapshotsUnavailable() async throws {
    let manager = try await makeAuthenticatedManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingStartupReconcileBridge()
    bridge.listConversationsError = TestOrchestratorBridgeError.listConversationsUnavailable
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let conversationID = "rust-synced-convo"
    let groupID = Data([0xca, 0xfe, 0xba, 0xbe])
    try await manager.database.write { db in
      try MLSConversationModel(
        conversationID: conversationID,
        currentUserDID: "did:plc:testuser",
        groupID: groupID,
        epoch: 7,
        joinMethod: .welcome,
        joinEpoch: 7,
        title: "Hydrated"
      ).insert(db)

      try MLSMemberModel(
        memberID: "\(conversationID)_alice",
        conversationID: conversationID,
        currentUserDID: "did:plc:testuser",
        did: "did:plc:testuser",
        leafIndex: 0,
        role: .admin
      ).insert(db)
      try MLSMemberModel(
        memberID: "\(conversationID)_bob",
        conversationID: conversationID,
        currentUserDID: "did:plc:testuser",
        did: "did:plc:bob",
        leafIndex: 1,
        role: .member
      ).insert(db)
    }

    manager.conversations.removeAll()
    manager.groupStates.removeAll()

    try await manager.syncWithServer(fullSync: false)

    let hydrated = try XCTUnwrap(manager.conversations[conversationID])
    XCTAssertEqual(bridge.syncWithServerCallCount, 1)
    XCTAssertEqual(bridge.listConversationsCallCount, 1)
    XCTAssertEqual(hydrated.conversationId, conversationID)
    XCTAssertEqual(hydrated.groupId, groupID.hexEncodedString())
    XCTAssertEqual(hydrated.epoch, 7)
    XCTAssertEqual(hydrated.members.map(\.did.description).sorted(), ["did:plc:bob", "did:plc:testuser"])
    XCTAssertEqual(manager.groupStates[groupID.hexEncodedString()]?.convoId, conversationID)
    XCTAssertEqual(manager.groupStates[groupID.hexEncodedString()]?.epoch, 7)
  }

  func testRustFullSyncHydratesSwiftCachesFromRustConversationSnapshotsBeforeDatabaseFallback() async throws {
    let manager = try await makeAuthenticatedManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingStartupReconcileBridge()
    let conversationID = "rust-snapshot-convo"
    let groupIDHex = "cafebabefeedface"
    bridge.rustConversationSnapshots = [
      FfiConversationView(
        groupId: groupIDHex,
        conversationId: conversationID,
        epoch: 11,
        members: [
          FfiMemberView(did: "did:plc:testuser", role: "admin"),
          FfiMemberView(did: "did:plc:bob", role: "member"),
        ],
        name: "Rust Snapshot",
        description: nil,
        avatarUrl: nil,
        createdAt: "2026-06-26T12:00:00Z",
        updatedAt: "2026-06-26T12:05:00Z"
      )
    ]
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    manager.conversations.removeAll()
    manager.groupStates.removeAll()

    try await manager.syncWithServer(fullSync: true)

    let hydrated = try XCTUnwrap(manager.conversations[conversationID])
    XCTAssertEqual(bridge.syncWithServerCallCount, 1)
    XCTAssertEqual(bridge.listConversationsCallCount, 1)
    XCTAssertEqual(bridge.lastListConversationsUserDid, "did:plc:testuser")
    XCTAssertEqual(hydrated.conversationId, conversationID)
    XCTAssertEqual(hydrated.groupId, groupIDHex)
    XCTAssertEqual(hydrated.epoch, 11)
    XCTAssertEqual(hydrated.members.map(\.did.description).sorted(), ["did:plc:bob", "did:plc:testuser"])
    XCTAssertEqual(manager.groupStates[groupIDHex]?.convoId, conversationID)
    XCTAssertEqual(manager.groupStates[groupIDHex]?.epoch, 11)

    let persistedConversationCount = try await manager.database.read { db in
      try MLSConversationModel.fetchCount(db)
    }
    XCTAssertEqual(persistedConversationCount, 0)
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

  private func seedConversation(
    conversationID: String,
    on manager: MLSConversationManager
  ) async throws {
    let model = MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: "did:plc:testuser",
      groupID: Data([0xde, 0xad, 0xbe, 0xef])
    )
    try await manager.database.write { db in
      try model.insert(db)
    }
    manager.conversations[conversationID] = try XCTUnwrap(model.asConvoView())
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
  var conversationReadyResult = FfiConversationReadyResult(
    recoveryState: .healthy,
    epoch: 0,
    sendAllowed: true
  )
  private(set) var startupReconcileCallCount = 0
  private(set) var runDeferredRecoveryCallCount = 0
  private(set) var ensureConversationReadyCallCount = 0
  private(set) var joinOrRejoinCallCount = 0
  private(set) var syncWithServerCallCount = 0
  private(set) var listConversationsCallCount = 0
  private(set) var lastDeferredRecoveryReason: String?
  private(set) var lastEnsureConversationReadyConversationId: String?
  private(set) var lastSyncWithServerFullSync: Bool?
  private(set) var lastListConversationsUserDid: String?
  var rustConversationSnapshots: [FfiConversationView] = []
  var listConversationsError: Error?

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

  override func ensureConversationReady(convoId: String) throws -> FfiConversationReadyResult {
    ensureConversationReadyCallCount += 1
    lastEnsureConversationReadyConversationId = convoId
    return conversationReadyResult
  }

  override func joinOrRejoin(convoId: String) throws -> FfiJoinOrRejoinResult {
    joinOrRejoinCallCount += 1
    return FfiJoinOrRejoinResult(
      epoch: conversationReadyResult.epoch ?? 0,
      recoveryState: conversationReadyResult.recoveryState
    )
  }

  override func syncWithServer(fullSync: Bool) throws {
    syncWithServerCallCount += 1
    lastSyncWithServerFullSync = fullSync
  }

  override func listConversations(userDid: String) throws -> [FfiConversationView] {
    listConversationsCallCount += 1
    lastListConversationsUserDid = userDid
    if let listConversationsError {
      throw listConversationsError
    }
    return rustConversationSnapshots
  }

  override func shutdown() {
  }
}

private enum TestOrchestratorBridgeError: Error {
  case listConversationsUnavailable
}

private func XCTAssertThrowsErrorAsync<T>(
  _ expression: @autoclosure () async throws -> T,
  _ handler: (Error) -> Void
) async {
  do {
    _ = try await expression()
    XCTFail("Expected error to be thrown")
  } catch {
    handler(error)
  }
}
