import XCTest

@testable import CatbirdMLSCore

private actor InMemoryRecoveryStateStore: MLSRecoveryStatePersisting {
  struct Entry: Sendable {
    var failedRejoinCount: Int
    var lastAttemptAtMs: Int64
    var quarantinedUntilMs: Int64?
  }

  private var entries: [String: Entry] = [:]
  private var globalLastRejoinAttemptAtMs: Int64?

  func upsertConversationState(
    conversationID: String,
    failedRejoinCount: Int,
    lastAttemptAtMs: Int64,
    quarantinedUntilMs: Int64?
  ) async throws {
    entries[conversationID] = Entry(
      failedRejoinCount: failedRejoinCount,
      lastAttemptAtMs: lastAttemptAtMs,
      quarantinedUntilMs: quarantinedUntilMs
    )
  }

  func clearConversationState(conversationID: String) async throws {
    entries.removeValue(forKey: conversationID)
  }

  func setGlobalLastRejoinAttempt(atMs: Int64) async throws {
    globalLastRejoinAttemptAtMs = atMs
  }

  func loadSnapshot() async throws -> MLSRecoveryPersistenceSnapshot {
    MLSRecoveryPersistenceSnapshot(
      conversations: entries.map { id, entry in
        MLSRecoveryPersistenceSnapshot.ConversationEntry(
          conversationID: id,
          failedRejoinCount: entry.failedRejoinCount,
          lastAttemptAtMs: entry.lastAttemptAtMs,
          quarantinedUntilMs: entry.quarantinedUntilMs
        )
      },
      lastGlobalRejoinAttemptAtMs: globalLastRejoinAttemptAtMs
    )
  }

  func entry(for conversationID: String) -> Entry? {
    entries[conversationID]
  }

  func globalStamp() -> Int64? {
    globalLastRejoinAttemptAtMs
  }
}

final class MLSRecoveryPersistenceTests: XCTestCase {
  private func epochMs(_ date: Date) -> Int64 {
    Int64(date.timeIntervalSince1970 * 1000)
  }

  func testHydrateRestoresRecentConversationCooldownAndGlobalFloor() async throws {
    let now = Date()
    let store = InMemoryRecoveryStateStore()
    try await store.upsertConversationState(
      conversationID: "convo-recent",
      failedRejoinCount: 1,
      lastAttemptAtMs: epochMs(now),
      quarantinedUntilMs: nil
    )
    try await store.setGlobalLastRejoinAttempt(atMs: epochMs(now))

    let manager = MLSRecoveryManager(persistence: store)
    await manager.hydrateFromDatabase()

    let skipsRecent = await manager.shouldSkipRejoin(convoId: "convo-recent")
    let skipsOther = await manager.shouldSkipRejoin(convoId: "other-convo")

    XCTAssertTrue(
      skipsRecent,
      "recent persisted per-conversation attempt should rehydrate the cooldown"
    )
    XCTAssertTrue(
      skipsOther,
      "recent persisted global floor should gate a different conversation after restart"
    )
  }

  func testHydrateIgnoresAndClearsExpiredConversationState() async throws {
    let expired = Date().addingTimeInterval(-(MLSRecoveryManager.persistedStateTTL + 60))
    let store = InMemoryRecoveryStateStore()
    try await store.upsertConversationState(
      conversationID: "convo-expired",
      failedRejoinCount: 2,
      lastAttemptAtMs: epochMs(expired),
      quarantinedUntilMs: nil
    )

    let manager = MLSRecoveryManager(persistence: store)
    await manager.hydrateFromDatabase()

    let skipsExpired = await manager.shouldSkipRejoin(convoId: "convo-expired")
    let expiredEntry = await store.entry(for: "convo-expired")

    XCTAssertFalse(skipsExpired)
    XCTAssertNil(
      expiredEntry,
      "expired persisted state should be deleted during hydration"
    )
  }

  func testAttemptCeilingPersistsQuarantineAcrossRestart() async throws {
    let store = InMemoryRecoveryStateStore()
    let manager = MLSRecoveryManager(persistence: store)

    for _ in 0..<MLSRecoveryManager.rejoinAttemptCeiling {
      await manager.recordFailedRejoin(convoId: "convo-ceiling")
    }
    await manager.flushPersistence()

    let persisted = await store.entry(for: "convo-ceiling")
    XCTAssertEqual(persisted?.failedRejoinCount, MLSRecoveryManager.rejoinAttemptCeiling)
    XCTAssertNotNil(persisted?.quarantinedUntilMs)

    let restarted = MLSRecoveryManager(persistence: store)
    await restarted.hydrateFromDatabase()

    let skipsCeiling = await restarted.shouldSkipRejoin(convoId: "convo-ceiling")
    let remaining = await restarted.remainingRejoinAttempts(convoId: "convo-ceiling")

    XCTAssertTrue(
      skipsCeiling,
      "ceiling quarantine must survive restart until the TTL horizon expires"
    )
    XCTAssertEqual(remaining, 0)
  }

  func testClearRejoinTrackingDeletesPersistedConversationState() async throws {
    let store = InMemoryRecoveryStateStore()
    let manager = MLSRecoveryManager(persistence: store)

    await manager.recordFailedRejoin(convoId: "convo-clear")
    await manager.flushPersistence()
    let entryBeforeClear = await store.entry(for: "convo-clear")
    XCTAssertNotNil(entryBeforeClear)

    await manager.clearRejoinTracking(convoId: "convo-clear")
    await manager.flushPersistence()

    let entryAfterClear = await store.entry(for: "convo-clear")
    let globalStamp = await store.globalStamp()

    XCTAssertNil(entryAfterClear)
    XCTAssertNotNil(
      globalStamp,
      "successful clear should persist the global rejoin floor"
    )
  }
}
