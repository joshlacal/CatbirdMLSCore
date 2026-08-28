import XCTest
@testable import CatbirdMLSCore
private actor OrderTracker {
  private var order: [Int] = []
  func record(_ val: Int) {
    order.append(val)
  }
  func getOrder() -> [Int] {
    order
  }
}

private actor GateSignal {
  private var continuation: CheckedContinuation<Void, Never>?
  func wait() async {
    await withCheckedContinuation { cont in
      continuation = cont
    }
  }
  func signal() {
    continuation?.resume()
    continuation = nil
  }
}
final class MLSStorageLifecycleCutoverTests: XCTestCase {
  private var tempBaseDirectory: URL!
  private var fakeKeychain: MLSKeychainFakeStorage!

  override func setUp() async throws {
    try await super.setUp()
    tempBaseDirectory = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSStorageLifecycleCutoverTests-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: tempBaseDirectory, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(tempBaseDirectory)
    fakeKeychain = MLSKeychainFakeStorage()
    MLSKeychainManager.setFakeStorageOverrideForTesting(fakeKeychain)
  }

  override func tearDown() async throws {
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
    if let tempBaseDirectory {
      try? FileManager.default.removeItem(at: tempBaseDirectory)
    }
    tempBaseDirectory = nil
    fakeKeychain = nil
    try await super.tearDown()
  }

  func testCrossKindResetLeaseSerializesResets() async throws {
    let testDID = "did:plc:serial_reset_\(UUID().uuidString.lowercased())"
    let upperDID = testDID.uppercased()

    let tracker = OrderTracker()
    let acquiredSignal = GateSignal()

    async let task1: Void = {
      let leases = try await MLSStorageCoordinator.shared.acquireCrossKindResetLease(for: testDID)
      defer { for lease in leases.values { lease.release() } }
      await acquiredSignal.signal()
      try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms
      await tracker.record(1)
    }()

    async let task2: Void = {
      // Wait until task1 has acquired the lease before attempting (deterministic ordering)
      await acquiredSignal.wait()
      let leases = try await MLSStorageCoordinator.shared.acquireCrossKindResetLease(for: upperDID)
      defer { for lease in leases.values { lease.release() } }
      await tracker.record(2)
    }()

    _ = await (try? task1, try? task2)

    let order = await tracker.getOrder()
    XCTAssertEqual(order.count, 2)
    XCTAssertEqual(order, [1, 2], "Cross-kind reset lease must serialize concurrent resets for case-variant DIDs")
  }

  func testResetSentinelLifecycle() {
    let testDID = "did:plc:sentinel_test_\(UUID().uuidString)"

    XCTAssertFalse(MLSStoragePaths.isResetActive(for: testDID))

    MLSStoragePaths.setResetSentinel(for: testDID)
    XCTAssertTrue(MLSStoragePaths.isResetActive(for: testDID))
    XCTAssertTrue(MLSStoragePaths.isResetActive(for: testDID.uppercased()), "Sentinel check must normalize DID")

    MLSStoragePaths.clearResetSentinel(for: testDID.uppercased())
    XCTAssertFalse(MLSStoragePaths.isResetActive(for: testDID))
  }

  func testWelcomeGateIsolationForSimilarDIDs() async throws {
    let gate = MLSWelcomeGate.shared
    let did1 = "did:web:example.com"
    let did2 = "did:web:example_com"
    let convo = "convo_123"

    try await gate.beginWelcomeProcessing(for: convo, userDID: did1)
    let has1 = await gate.hasPendingWelcome(for: convo, userDID: did1)
    let has2 = await gate.hasPendingWelcome(for: convo, userDID: did2)
    XCTAssertTrue(has1)
    XCTAssertFalse(has2, "did:web:example.com and did:web:example_com must remain isolated")
    try await gate.clearAll(for: did1)
    let hasCleared = await gate.hasPendingWelcome(for: convo, userDID: did1)
    XCTAssertFalse(hasCleared)
  }

  func testEpochCheckpointDrainWriters() async throws {
    let checkpoint = MLSEpochCheckpoint.shared
    let testDID = "did:plc:epoch_test_\(UUID().uuidString.lowercased())"
    let groupData = Data([0x01, 0x02, 0x03, 0x04])

    await checkpoint.recordEpoch(userDID: testDID, groupId: groupData, epoch: 42, isNSE: false)
    let lastKnown = await checkpoint.getLastKnownEpoch(userDID: testDID, groupId: groupData)
    XCTAssertEqual(lastKnown, 42)

    try await checkpoint.clearAllCheckpoints(userDID: testDID)
    await checkpoint.resumeWrites(userDID: testDID)
    let cleared = await checkpoint.getLastKnownEpoch(userDID: testDID, groupId: groupData)
    XCTAssertNil(cleared)
  }

  func testEpochCheckpointWriterPausedBeforeDirectoryCreationRejectsWriteOnReset() async throws {
    let checkpoint = MLSEpochCheckpoint.shared
    let testDID = "did:plc:epoch_race_\(UUID().uuidString.lowercased())"
    let groupData = Data([0xaa, 0xbb, 0xcc, 0xdd])

    let enteredSignal = GateSignal()
    let allowContinuation = GateSignal()

    await checkpoint.setTestPreDirectoryCreationHook {
      await enteredSignal.signal()
      await allowContinuation.wait()
    }
    defer {
      Task { await checkpoint.setTestPreDirectoryCreationHook(nil) }
    }

    // Trigger writer task
    await checkpoint.recordEpoch(userDID: testDID, groupId: groupData, epoch: 100, isNSE: false)

    // Wait until writer task acquires lease and enters hook right before createDirectory
    await enteredSignal.wait()

    // While writer is paused inside hook, simulate reset setting sentinel and bumping generation:
    MLSStoragePaths.setResetSentinel(for: testDID)
    try MLSCoordinationStore.shared.incrementGenerationStrict(for: testDID)

    // Allow writer hook to continue
    await allowContinuation.signal()

    // Drain and clear checkpoints (which awaits the aborted writer task)
    try await checkpoint.clearAllCheckpoints(userDID: testDID)
    await checkpoint.resumeWrites(userDID: testDID)
    MLSStoragePaths.clearResetSentinel(for: testDID)

    let recordAfterRace = await checkpoint.getLastKnownEpoch(userDID: testDID, groupId: groupData)
    XCTAssertNil(recordAfterRace, "Writer must reject publication and not recreate checkpoint after reset")
  }

  func testDeterministicMultiTailEpochCheckpointResetDrainsAllTails() async throws {
    let checkpoint = MLSEpochCheckpoint.shared
    let testDID = "did:plc:multi_tail_\(UUID().uuidString.lowercased())"
    let g1 = Data([0x01, 0x01, 0x01])
    let g2 = Data([0x02, 0x02, 0x02])
    let g3 = Data([0x03, 0x03, 0x03])

    // Rapidly enqueue multiple chained writes on same DID
    await checkpoint.recordEpoch(userDID: testDID, groupId: g1, epoch: 10, isNSE: false)
    await checkpoint.recordEpoch(userDID: testDID, groupId: g2, epoch: 20, isNSE: false)
    await checkpoint.recordEpoch(userDID: testDID, groupId: g3, epoch: 30, isNSE: false)

    // clearAllCheckpoints awaits all chained tails and deletes cleanly without deadlock
    try await checkpoint.clearAllCheckpoints(userDID: testDID)
    await checkpoint.resumeWrites(userDID: testDID)

    let k1 = await checkpoint.getLastKnownEpoch(userDID: testDID, groupId: g1)
    let k2 = await checkpoint.getLastKnownEpoch(userDID: testDID, groupId: g2)
    let k3 = await checkpoint.getLastKnownEpoch(userDID: testDID, groupId: g3)

    XCTAssertNil(k1)
    XCTAssertNil(k2)
    XCTAssertNil(k3)
  }

  func testCoalescedOpenRegistersSingleTokenAtomicallyAndRejectsPostSnapshotPublication() async throws {
    let testDID = "did:plc:atomic_open_\(UUID().uuidString.lowercased())"
    let manager = MLSGRDBManager()

    // Concurrent first opens coalesce behind one owner and publish one token.
    try await withThrowingTaskGroup(of: Void.self) { group in
      for _ in 0..<8 {
        group.addTask {
          _ = try await manager.getDatabasePool(for: testDID)
        }
      }
      try await group.waitForAll()
    }
    let tokenCount1 = MLSGRDBManager.activeEmergencyPoolTokenCount(for: testDID)
    XCTAssertEqual(tokenCount1, 1, "coalescedOpen must register exactly one active emergency token")

    // Simulate an emergency snapshot: bump generation and clear active pools.
    MLSGRDBManager.emergencyCloseAllDatabases(mode: .passive)

    // A fresh open under the NEW generation must succeed and register exactly one token
    // (no double registration, no stale-cache publication from the old generation).
    _ = try await manager.getDatabasePool(for: testDID)
    let tokenCount2 = MLSGRDBManager.activeEmergencyPoolTokenCount(for: testDID)
    XCTAssertEqual(tokenCount2, 1, "post-snapshot open must register exactly one active emergency token (no double registration)")

    // Cleanup
    _ = await manager.closeDatabaseAndDrain(for: testDID)
  }

  func testRemoveContextReleasesOnlyItsOwnAdmissionLease() async throws {
    let testDID = "did:plc:context_lease_\(UUID().uuidString.lowercased())"
    let coordinator = MLSStorageCoordinator.shared

    try await MLSCoreContext.shared.ensureContext(for: testDID)
    _ = try await coordinator.coordinateOpen(for: .rustState, userDID: testDID) { _, _ in true }

    let removed = await MLSCoreContext.shared.removeContext(for: testDID.uppercased())
    XCTAssertTrue(removed)
    XCTAssertTrue(
      coordinator.hasActiveAdmissionLease(for: .rustState, userDID: testDID),
      "Removing the cached context must not release an independently owned lease"
    )

    coordinator.releaseAdmissionLease(for: .rustState, userDID: testDID)
    XCTAssertFalse(coordinator.hasActiveAdmissionLease(for: .rustState, userDID: testDID))
  }

  func testResetAndCloseContextReleasesOnlyItsOwnAdmissionLease() async throws {
    let testDID = "did:plc:context_reset_lease_\(UUID().uuidString.lowercased())"
    let coordinator = MLSStorageCoordinator.shared

    try await MLSCoreContext.shared.ensureContext(for: testDID)
    _ = try await coordinator.coordinateOpen(for: .rustState, userDID: testDID) { _, _ in true }

    try await MLSCoreContext.shared.resetAndCloseContext(for: testDID.uppercased())
    XCTAssertTrue(
      coordinator.hasActiveAdmissionLease(for: .rustState, userDID: testDID),
      "Resetting the cached context must not release an independently owned lease"
    )

    coordinator.releaseAdmissionLease(for: .rustState, userDID: testDID)
    XCTAssertFalse(coordinator.hasActiveAdmissionLease(for: .rustState, userDID: testDID))
  }

  func testCloseDatabaseRemovesEveryCaseAlias() async throws {
    let testDID = "did:plc:PoolAlias_\(UUID().uuidString)"
    let normalized = testDID.lowercased()
    let manager = MLSGRDBManager()

    _ = try await manager.getEphemeralDatabasePool(for: testDID)
    let originalWasOpen = await manager.isDatabaseOpen(for: testDID)
    let normalizedWasOpen = await manager.isDatabaseOpen(for: normalized)
    XCTAssertTrue(originalWasOpen)
    XCTAssertTrue(normalizedWasOpen)

    await manager.closeDatabase(for: testDID)

    let originalIsOpen = await manager.isDatabaseOpen(for: testDID)
    let normalizedIsOpen = await manager.isDatabaseOpen(for: normalized)
    XCTAssertFalse(originalIsOpen)
    XCTAssertFalse(normalizedIsOpen)
    XCTAssertFalse(
      MLSStorageCoordinator.shared.hasActiveAdmissionLease(for: .swiftGRDB, userDID: testDID)
    )
  }

  func testCloseAllLightweightQueuesEvictsClosedQueue() async throws {
    let testDID = "did:plc:queue_evict_\(UUID().uuidString.lowercased())"
    let seedManager = MLSGRDBManager()
    let seedPool = try await seedManager.getDatabasePool(for: testDID)
    try await seedPool.write { db in
      try db.execute(sql: "CREATE TABLE queue_eviction_probe (value TEXT NOT NULL);")
      try db.execute(sql: "INSERT INTO queue_eviction_probe (value) VALUES ('ok');")
    }
    let seedClosed = await seedManager.closeDatabaseAndDrain(for: testDID)
    XCTAssertTrue(seedClosed)

    let manager = MLSGRDBManager()
    let initial = try await manager.performLightweightRead(for: testDID) { db in
      try String.fetchOne(db, sql: "SELECT value FROM queue_eviction_probe LIMIT 1;")
    }
    XCTAssertEqual(initial, "ok")

    await manager.closeAllLightweightQueues()

    let reopened = try await manager.performLightweightRead(for: testDID) { db in
      try String.fetchOne(db, sql: "SELECT value FROM queue_eviction_probe LIMIT 1;")
    }
    XCTAssertEqual(reopened, "ok")
    await manager.closeAllLightweightQueues()
  }

}
