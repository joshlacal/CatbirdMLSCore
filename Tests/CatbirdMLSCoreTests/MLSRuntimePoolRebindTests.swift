import CatbirdMLS
import GRDB
import Petrel
import XCTest
@testable import CatbirdMLSCore

final class MLSRuntimePoolRebindTests: XCTestCase {
  private var directory: URL!
  private var manager: MLSConversationManager!
  private var context: MlsContext!
  private var did: String!

  override func setUp() async throws {
    did = "did:plc:pool-rebind-\(UUID().uuidString.lowercased())"
    directory = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSRuntimePoolRebindTests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(directory)
    MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())
    let initialPool = try DatabasePool(path: directory.appendingPathComponent("initial.sqlite").path)
    let client = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let api = await MLSAPIClient(client: client,
      environment: .custom(serviceDID: "did:web:example.com#atproto_mls"))
    manager = MLSConversationManager(apiClient: api, database: initialPool,
      userDid: did, atProtoClient: client, protocolAuthorityMode: .rustFull)
    manager.database = try await manager.databaseManager.getDatabasePool(for: did)
    manager.membershipChangeObserver = MLSMembershipChangeObserver(database: manager.database, currentUserDID: did)
    await MLSClient.shared.configure(for: did, apiClient: api, atProtoClient: client)
    await MLSClient.shared.recovery(for: did)?.setPersistence(
      MLSRecoveryStateStore(database: manager.database, currentUserDID: did))
    try await manager.rustDeviceAuthorizationGate.ensure(scope: manager.rustDeviceAuthorizationScope(for: did)) { }
    context = try MlsContext(storagePath: directory.appendingPathComponent("native.sqlite").path,
      encryptionKey: String(repeating: "cd", count: 32), keychain: InMemoryKeychainAccess())
    try context.setContentRootKey(key: Data(repeating: 0x51, count: 32))
  }

  override func tearDown() async throws {
    MLSStorageCoordinator.shared.testPrePublicationHook = nil
    MLSConversationManager.resetSuspensionStateForTesting()
    MLSCoreContext.clearSuspensionFlag()
    MLSClient.clearSuspensionFlag(reason: "pool rebind test cleanup")
    NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
    if let manager { await manager.databaseManager.shutdownAllDatabases() }
    manager = nil
    context?.clearContentRootKey()
    try? context?.flushAndPrepareClose()
    context = nil
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
  }

  func testRefreshAdoptsAlreadyReopenedPoolBeforeStorageCallback() async throws {
    let oldPool = try XCTUnwrap(manager.database as? DatabasePool)
    let stale = MLSOrchestratorStorageAdapter(dbPool: oldPool, userDID: did, mlsContext: context)
    try stale.ensureConversationExists(userDid: did,
      conversationId: "550e8400-e29b-41d4-a716-446655440000", groupId: "deadbeef")
    XCTAssertEqual(try stale.listConversations(userDid: did).count, 1)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    XCTAssertThrowsError(try stale.listConversations(userDid: did))
    let currentPool = try await manager.databaseManager.getDatabasePool(for: did)
    XCTAssertFalse(currentPool === oldPool)

    try await manager.refreshDatabaseIfNeeded()

    XCTAssertTrue((manager.database as? DatabasePool) === currentPool)
    let rebound = MLSOrchestratorStorageAdapter(dbPool: try XCTUnwrap(manager.database as? DatabasePool),
      userDID: did, mlsContext: context)
    XCTAssertEqual(try rebound.listConversations(userDid: did).count, 1)
    try rebound.ensureConversationExists(userDid: did,
      conversationId: "550e8400-e29b-41d4-a716-446655440001", groupId: "cafefeed")
    XCTAssertEqual(try rebound.listConversations(userDid: did).count, 2)
  }

  func testRefreshRebindsObserverHandlersAndRecoveryPersistence() async throws {
    let conversationID = "550e8400-e29b-41d4-a716-446655440010"
    try MLSOrchestratorStorageAdapter(dbPool: manager.database as! DatabasePool,
      userDID: did, mlsContext: context).ensureConversationExists(userDid: did,
        conversationId: conversationID, groupId: "deadbeef")
    let observer = try XCTUnwrap(manager.membershipChangeObserver)
    let foundRecovery = await manager.mlsClient.recovery(for: did)
    let recovery = try XCTUnwrap(foundRecovery)
    let changes = expectation(description: "Original observer handler remains attached")
    await observer.setChangeHandler { _ in changes.fulfill() }
    await observer.setProfileLookup { _ in "Test member" }
    await recovery.recordFailedRejoin(convoId: "before-close")
    await recovery.flushPersistence()
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    let currentPool = try await manager.databaseManager.getDatabasePool(for: did)

    try await manager.refreshDatabaseIfNeeded()

    XCTAssertTrue(manager.membershipChangeObserver === observer)
    try await observer.processEpochTransition(conversationID: conversationID,
      oldEpoch: 0, newEpoch: 1, newMembers: [did], treeHash: nil)
    await fulfillment(of: [changes], timeout: 2)
    let events = try await observer.getRecentChanges(for: conversationID)
    XCTAssertEqual(events.count, 1)
    let roster = try await observer.getRoster(for: conversationID, at: 1)
    XCTAssertEqual(roster, [did])
    let detected = try await observer.detectChanges(conversationID: "profile-probe",
      oldEpoch: 0, newEpoch: 1, newMembers: [did])
    XCTAssertEqual(detected?.changes.first?.memberDisplayName, "Test member")

    await recovery.recordFailedRejoin(convoId: "after-close")
    await recovery.flushPersistence()
    let snapshot = try await MLSRecoveryStateStore(database: currentPool, currentUserDID: did).loadSnapshot()
    XCTAssertTrue(snapshot.conversations.contains { $0.conversationID == "before-close" })
    XCTAssertTrue(snapshot.conversations.contains { $0.conversationID == "after-close" })
    let stillBackedOff = await recovery.shouldSkipRejoin(convoId: "before-close")
    XCTAssertTrue(stillBackedOff, "Pool rebinding must preserve in-memory recovery policy")
  }

  func testRefreshFailsClosedDuringSuspension() async throws {
    let oldPool = try XCTUnwrap(manager.database as? DatabasePool)
    MLSClient.markSuspensionInProgress(reason: "pool binding test")
    MLSCoreContext.markSuspensionInProgress()
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    do {
      try await manager.refreshDatabaseIfNeeded()
      XCTFail("Ordinary pool binding must not bypass closed lifecycle gates")
    } catch { }
    XCTAssertTrue((manager.database as? DatabasePool) === oldPool)
    XCTAssertEqual(MLSGRDBManager.activeEmergencyPoolTokenCount(for: did), 0)
  }

  func testObserverRejectsOldProfileContinuationAfterRebind() async throws {
    let conversationID = "550e8400-e29b-41d4-a716-446655440011"
    let observer = try XCTUnwrap(manager.membershipChangeObserver)
    try MLSOrchestratorStorageAdapter(dbPool: manager.database as! DatabasePool,
      userDID: did, mlsContext: context).ensureConversationExists(userDid: did,
        conversationId: conversationID, groupId: "deadbeef")
    let barrier = PoolRebindBarrier()
    let lookupStarted = expectation(description: "Old pool profile lookup suspended")
    await observer.setProfileLookup { _ in
      lookupStarted.fulfill()
      await barrier.wait()
      return "Late profile"
    }
    let userDID = did!
    let transition = Task {
      try await observer.processEpochTransition(conversationID: conversationID,
        oldEpoch: 0, newEpoch: 1, newMembers: [userDID], treeHash: nil)
    }
    await fulfillment(of: [lookupStarted], timeout: 2)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    try await manager.refreshDatabaseIfNeeded()
    await barrier.release()
    do {
      try await transition.value
      XCTFail("An epoch transition from the previous pool must not write or publish after rebinding")
    } catch { }
    let roster = try await observer.getRoster(for: conversationID, at: 1)
    XCTAssertNil(roster)
    let events = try await observer.getRecentChanges(for: conversationID)
    XCTAssertTrue(events.isEmpty)
  }

  func testConcurrentRefreshRechecksOrdinaryReopenAfterDependentAwait() async throws {
    let activeManager = try XCTUnwrap(manager)
    let originalPool = try XCTUnwrap(activeManager.database as? DatabasePool)
    let foundRecovery = await activeManager.mlsClient.recovery(for: did)
    let recovery = try XCTUnwrap(foundRecovery)
    let entered = expectation(description: "Old recovery persistence tail is blocked")
    let blockedStore = PoolRebindBlockingStore(
      backing: MLSRecoveryStateStore(database: originalPool, currentUserDID: did), entered: entered)
    await recovery.setPersistence(blockedStore)
    await recovery.recordFailedRejoin(convoId: "old-tail")
    await fulfillment(of: [entered], timeout: 2)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    let firstReplacement = try await activeManager.databaseManager.getDatabasePool(for: did)
    let generation = MLSGRDBManager.storageLifecycleGeneration
    let rebindStarted = expectation(description: "Refresh bound P1 and awaits old recovery tail")
    await recovery.setPersistenceRebindHookForTesting { rebindStarted.fulfill() }
    let firstRefresh = Task { try await activeManager.refreshDatabaseIfNeeded() }
    await fulfillment(of: [rebindStarted], timeout: 2)
    await recovery.recordFailedRejoin(convoId: "during-rebind")
    let secondRefresh = Task { try await activeManager.refreshDatabaseIfNeeded() }
    let drained = await activeManager.databaseManager.closeDatabaseAndDrain(for: did)
    XCTAssertTrue(drained)
    let finalPool = try await activeManager.databaseManager.getDatabasePool(for: did)
    XCTAssertFalse(finalPool === firstReplacement)
    XCTAssertEqual(MLSGRDBManager.storageLifecycleGeneration, generation,
      "This regression requires ordinary replacement without an emergency generation change")
    await blockedStore.release()
    try await firstRefresh.value
    try await secondRefresh.value

    XCTAssertTrue((activeManager.database as? DatabasePool) === finalPool)
    let adapter = MLSOrchestratorStorageAdapter(dbPool: finalPool, userDID: did, mlsContext: context)
    try adapter.ensureConversationExists(userDid: did,
      conversationId: "550e8400-e29b-41d4-a716-446655440012", groupId: "deadbeef")
    XCTAssertEqual(try adapter.listConversations(userDid: did).count, 1)
    await recovery.recordFailedRejoin(convoId: "after-final-bind")
    await recovery.flushPersistence()
    let snapshot = try await MLSRecoveryStateStore(database: finalPool, currentUserDID: did).loadSnapshot()
    XCTAssertTrue(snapshot.conversations.contains { $0.conversationID == "after-final-bind" })
    let oldStoreCalls = await blockedStore.upsertCalls
    XCTAssertEqual(oldStoreCalls, 1, "New persistence enqueues must not retain the old writer")
  }

  func testRestoreAcquiresPoolBeforeConstructingRuntimeCallbacks() async throws {
    let oldPool = try XCTUnwrap(manager.database as? DatabasePool)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    let callbackProbe = PoolRebindBridge(did: did)
    manager.orchestratorRuntimeResumeFactory = { [self] in
      callbackProbe.storage = MLSOrchestratorStorageAdapter(
        dbPool: manager.database as! DatabasePool, userDID: did, mlsContext: context)
      return MLSOrchestratorRuntime(userDID: did, mode: .rustFull, bridge: callbackProbe)
    }

    let runtime = await manager.restoreOrchestratorRuntimeAfterSuspendClose(reason: "pool rebind regression")

    XCTAssertNotNil(runtime, "Restoration must bind an open pool before a bridge callback can run")
    XCTAssertFalse((manager.database as? DatabasePool) === oldPool)
    XCTAssertEqual(callbackProbe.successfulReads, 1)
    XCTAssertEqual(callbackProbe.reattachOnMainThread, false)
  }

  func testRestoreRejectsSupersededFactoryEvenAfterNewerGatesReopen() async throws {
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    let callbackProbe = PoolRebindBridge(did: did)
    manager.orchestratorRuntimeResumeFactory = { [self] in
      callbackProbe.storage = MLSOrchestratorStorageAdapter(
        dbPool: manager.database as! DatabasePool, userDID: did, mlsContext: context)
      MLSClient.markSuspensionInProgress(reason: "newer suspension during runtime factory")
      MLSCoreContext.markSuspensionInProgress()
      XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
      MLSClient.clearSuspensionFlag(reason: "newer foreground cycle")
      MLSCoreContext.clearSuspensionFlag()
      return MLSOrchestratorRuntime(userDID: did, mode: .rustFull, bridge: callbackProbe)
    }
    let runtime = await manager.restoreOrchestratorRuntimeAfterSuspendClose(reason: "superseded factory")
    XCTAssertNil(runtime)
    XCTAssertNil(manager.orchestratorRuntime)
    XCTAssertEqual(callbackProbe.successfulReads, 0)
  }

  func testResumeReportsActualClosedAdapterReconcileFailureWithGatesOpen() async throws {
    let stalePool = try XCTUnwrap(manager.database as? DatabasePool)
    let callbackProbe = PoolRebindBridge(did: did)
    callbackProbe.storage = MLSOrchestratorStorageAdapter(dbPool: stalePool, userDID: did, mlsContext: context)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    manager.orchestratorRuntime = MLSOrchestratorRuntime(userDID: did, mode: .rustFull, bridge: callbackProbe)
    manager.isSuspending = true

    let resumed = await manager.resumeMLSOperations()

    XCTAssertFalse(resumed)
    XCTAssertEqual(callbackProbe.startupCalls, 1)
    XCTAssertFalse(manager.rustStartupReconcileCompleted)
    XCTAssertFalse(MLSClient.isSuspensionInProgress)
    XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
  }

  func testResumeReconcilesUsingReboundPoolAfterOwnedSuspension() async throws {
    let callbackProbe = PoolRebindBridge(did: did)
    manager.orchestratorRuntimeResumeFactory = { [self] in
      XCTAssertFalse(MLSClient.isSuspensionInProgress)
      XCTAssertFalse(MLSCoreContext.isSuspensionInProgress)
      callbackProbe.storage = MLSOrchestratorStorageAdapter(
        dbPool: manager.database as! DatabasePool, userDID: did, mlsContext: context)
      return MLSOrchestratorRuntime(userDID: did, mode: .rustFull, bridge: callbackProbe)
    }
    _ = await manager.suspendMLSOperations()
    await manager.markRustRuntimeClosedForSuspend(reason: "pool rebind test")
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)

    let resumed = await manager.resumeMLSOperations()

    XCTAssertTrue(resumed)
    XCTAssertEqual(callbackProbe.successfulReads, 1)
    XCTAssertEqual(callbackProbe.startupCalls, 1)
    XCTAssertTrue(manager.rustStartupReconcileCompleted)
  }

  func testOldFailedResumePreservesNewerSuspensionOwner() async throws {
    let activeManager = try XCTUnwrap(manager)
    let barrier = PoolRebindBarrier()
    let factoryEntered = expectation(description: "Old resume reached runtime factory")
    activeManager.orchestratorRuntimeResumeFactory = {
      factoryEntered.fulfill()
      await barrier.wait()
      return nil
    }
    _ = await activeManager.suspendMLSOperations()
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    let oldResume = Task { await activeManager.resumeMLSOperations() }
    await fulfillment(of: [factoryEntered], timeout: 2)
    _ = await activeManager.suspendMLSOperations()
    let newerID = try XCTUnwrap(activeManager.activeSuspensionID)
    XCTAssertTrue(MLSGRDBManager.closeAllDatabasesForSuspension().isComplete)
    await barrier.release()

    let oldResult = await oldResume.value

    XCTAssertFalse(oldResult)
    XCTAssertEqual(activeManager.activeSuspensionID, newerID)
    XCTAssertTrue(MLSClient.isSuspensionInProgress)
    XCTAssertTrue(MLSCoreContext.isSuspensionInProgress)
    let callbackProbe = PoolRebindBridge(did: did)
    activeManager.orchestratorRuntimeResumeFactory = { [self] in
      callbackProbe.storage = MLSOrchestratorStorageAdapter(
        dbPool: activeManager.database as! DatabasePool, userDID: did, mlsContext: context)
      return MLSOrchestratorRuntime(userDID: did, mode: .rustFull, bridge: callbackProbe)
    }
    let newResult = await activeManager.resumeMLSOperations()
    XCTAssertTrue(newResult)
  }

  func testCanceledRuntimeFactoryCannotPublish() async throws {
    let activeManager = try XCTUnwrap(manager)
    let barrier = PoolRebindBarrier()
    let factoryEntered = expectation(description: "Cancelable runtime factory entered")
    let callbackProbe = PoolRebindBridge(did: did)
    activeManager.orchestratorRuntimeResumeFactory = { [self] in
      callbackProbe.storage = MLSOrchestratorStorageAdapter(
        dbPool: activeManager.database as! DatabasePool, userDID: did, mlsContext: context)
      factoryEntered.fulfill()
      await barrier.wait()
      return MLSOrchestratorRuntime(userDID: did, mode: .rustFull, bridge: callbackProbe)
    }
    let restore = Task {
      await activeManager.restoreOrchestratorRuntimeAfterSuspendClose(reason: "canceled factory")
    }
    await fulfillment(of: [factoryEntered], timeout: 2)
    restore.cancel()
    await barrier.release()
    let runtime = await restore.value
    XCTAssertNil(runtime)
    XCTAssertNil(activeManager.orchestratorRuntime)
    XCTAssertEqual(callbackProbe.successfulReads, 0)
  }
}

private final class PoolRebindBridge: OrchestratorBridge {
  var storage: MLSOrchestratorStorageAdapter?
  var successfulReads = 0
  var startupCalls = 0
  var reattachOnMainThread: Bool?

  init(did: String) {
    storageUserDID = did
    super.init(noPointer: .init())
  }
  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    super.init(unsafeFromRawPointer: pointer)
  }
  override func reattachAfterSuspend(userDid: String, reason: String) throws {
    reattachOnMainThread = Thread.isMainThread
    _ = try storage!.listConversations(userDid: userDid)
    successfulReads += 1
  }
  override func resumeFromSuspend(reason: String) throws { }
  override func startupReconcile() throws -> FfiStartupReconcileReport {
    startupCalls += 1
    _ = try storage!.listConversations(userDid: storageUserDID)
    return FfiStartupReconcileReport(scanned: 0, healthy: 0, needsRejoin: 0,
      resetPending: 0, unrecoverableLocal: 0)
  }
  var storageUserDID = "did:plc:pool-rebind"
  override func shutdown() { }
}

private actor PoolRebindBarrier {
  private var continuation: CheckedContinuation<Void, Never>?
  private var released = false
  func wait() async {
    if released { return }
    await withCheckedContinuation { continuation = $0 }
  }
  func release() {
    released = true
    continuation?.resume()
    continuation = nil
  }
}

private actor PoolRebindBlockingStore: MLSRecoveryStatePersisting {
  let backing: MLSRecoveryStateStore
  let entered: XCTestExpectation
  private let barrier = PoolRebindBarrier()
  private(set) var upsertCalls = 0
  init(backing: MLSRecoveryStateStore, entered: XCTestExpectation) {
    self.backing = backing
    self.entered = entered
  }
  func release() async { await barrier.release() }
  func upsertConversationState(conversationID: String, failedRejoinCount: Int,
    lastAttemptAtMs: Int64, quarantinedUntilMs: Int64?) async throws {
    upsertCalls += 1
    if upsertCalls == 1 { entered.fulfill() }
    await barrier.wait()
    try await backing.upsertConversationState(conversationID: conversationID,
      failedRejoinCount: failedRejoinCount, lastAttemptAtMs: lastAttemptAtMs,
      quarantinedUntilMs: quarantinedUntilMs)
  }
  func clearConversationState(conversationID: String) async throws {
    try await backing.clearConversationState(conversationID: conversationID)
  }
  func setGlobalLastRejoinAttempt(atMs: Int64) async throws {
    try await backing.setGlobalLastRejoinAttempt(atMs: atMs)
  }
  func loadSnapshot() async throws -> MLSRecoveryPersistenceSnapshot {
    try await backing.loadSnapshot()
  }
}
