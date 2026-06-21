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

  // N39 (3-band collapse): the quarantine horizon arms when the failure
  // counter reaches MAX_REJOIN_ATTEMPTS (3) — the Rust contract
  // (`RecoveryTracker::record_failure` arms `lockout_until` at
  // `count >= max_attempts`, persisted as `quarantined_until_ms`;
  // catbird-mls/src/orchestrator/recovery.rs). The retired
  // `rejoinAttemptCeiling = 8` band this test used to pin had no Rust twin.
  func testMaxAttemptsPersistsQuarantineAcrossRestart() async throws {
    let store = InMemoryRecoveryStateStore()
    let manager = MLSRecoveryManager(persistence: store)

    for _ in 0..<MLSRecoveryManager.maxRejoinAttempts {
      await manager.recordFailedRejoin(convoId: "convo-maxed")
    }
    await manager.flushPersistence()

    let persisted = await store.entry(for: "convo-maxed")
    XCTAssertEqual(
      persisted?.failedRejoinCount, MLSRecoveryManager.maxRejoinAttempts,
      "counter must persist at the real attempt count — never inflated past it (N39)"
    )
    XCTAssertNotNil(
      persisted?.quarantinedUntilMs,
      "reaching MAX_REJOIN_ATTEMPTS must arm the persisted quarantine horizon (Rust record_failure parity)"
    )

    let restarted = MLSRecoveryManager(persistence: store)
    await restarted.hydrateFromDatabase()

    let skipsMaxed = await restarted.shouldSkipRejoin(convoId: "convo-maxed")
    let remaining = await restarted.remainingRejoinAttempts(convoId: "convo-maxed")

    XCTAssertTrue(
      skipsMaxed,
      "maxed-out quarantine must survive restart until the TTL horizon expires"
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

  func testClearRejoinTrackingAfterMaxAttemptsErasesPersistedBiasAcrossRestart() async throws {
    // N20 contract: rejoin paths that succeed (e.g. the
    // detectAndRejoinMissingConversations missing-convo loop) map success to
    // clearRejoinTracking. That clear must erase the *persisted* failure
    // counters and quarantine too — otherwise the next launch rehydrates the
    // stale bias and shouldSkipRejoin keeps gating a conversation that
    // already healed.
    // N39: the gate now arms at MAX_REJOIN_ATTEMPTS (3) — the single Rust
    // band — instead of the retired ceiling (8).
    let store = InMemoryRecoveryStateStore()
    let manager = MLSRecoveryManager(persistence: store)

    for _ in 0..<MLSRecoveryManager.maxRejoinAttempts {
      await manager.recordFailedRejoin(convoId: "convo-n20")
    }
    await manager.flushPersistence()

    let skipsAtMax = await manager.shouldSkipRejoin(convoId: "convo-n20")
    XCTAssertTrue(
      skipsAtMax,
      "maxed-out conversation must be gated before the successful rejoin"
    )

    // Successful rejoin outcome.
    await manager.clearRejoinTracking(convoId: "convo-n20")
    await manager.flushPersistence()

    let restarted = MLSRecoveryManager(persistence: store)
    await restarted.hydrateFromDatabase()

    let persistedEntry = await store.entry(for: "convo-n20")
    XCTAssertNil(
      persistedEntry,
      "successful rejoin must delete the persisted failure counters (N20)"
    )

    // shouldSkipRejoin is deliberately NOT asserted false here: clearRejoinTracking
    // stamps the global 30s rejoin floor (spec §8.4 / §10), which gates every
    // conversation immediately after a clear. Per-conversation bias is what N20
    // is about, so compare remaining attempts against a never-touched convo.
    let remainingCleared = await restarted.remainingRejoinAttempts(convoId: "convo-n20")
    let remainingFresh = await restarted.remainingRejoinAttempts(convoId: "convo-never-seen")
    XCTAssertEqual(
      remainingCleared, remainingFresh,
      "after a successful rejoin + restart, the conversation must look indistinguishable from a fresh one"
    )
    XCTAssertGreaterThan(remainingCleared, 0)
  }

  // MARK: - E7 parity fix set (P-1..P-6)

  func testFreshResetClearRemovesEntryAndRowWithoutTouchingGlobalFloor() async throws {
    let store = InMemoryRecoveryStateStore()
    let manager = MLSRecoveryManager(persistence: store)

    await manager.recordFailedRejoin(convoId: "convo-fresh")
    await manager.flushPersistence()
    let entryBefore = await store.entry(for: "convo-fresh")
    XCTAssertNotNil(entryBefore)
    let persistedStampBefore = await store.globalStamp()
    XCTAssertNotNil(persistedStampBefore, "failure outcome must persist the global stamp")
    let inMemoryFloorBefore = await manager.lastGlobalRejoinAttemptForTesting()
    XCTAssertNotNil(inMemoryFloorBefore)

    await manager.clearRejoinTrackingForFreshReset(convoId: "convo-fresh")
    await manager.flushPersistence()

    let entryAfter = await store.entry(for: "convo-fresh")
    XCTAssertNil(entryAfter, "fresh-reset clear must write-through-DELETE the persisted row")

    let persistedStampAfter = await store.globalStamp()
    XCTAssertEqual(
      persistedStampAfter, persistedStampBefore,
      "fresh-reset clear must NOT re-persist the global rejoin stamp"
    )
    let inMemoryFloorAfter = await manager.lastGlobalRejoinAttemptForTesting()
    XCTAssertEqual(
      inMemoryFloorAfter, inMemoryFloorBefore,
      "fresh-reset clear must not touch lastGlobalRejoinAttemptAt in either direction"
    )

    let remainingCleared = await manager.remainingRejoinAttempts(convoId: "convo-fresh")
    let remainingFresh = await manager.remainingRejoinAttempts(convoId: "convo-never-seen")
    XCTAssertEqual(remainingCleared, remainingFresh, "failure history must be wiped")

    let cooldown = await manager.successCooldownRemaining(convoId: "convo-fresh")
    XCTAssertNil(cooldown, "fresh-reset clear must NOT arm the successful-rejoin cooldown")
  }

  func testRuntimeLockoutExpiryClampsToMaxMinusOneAndReopensOneAttempt() async throws {
    let store = InMemoryRecoveryStateStore()
    let manager = MLSRecoveryManager(persistence: store)
    let now = Date()

    // Maxed-out entry (attempts == maxRejoinAttempts == 3) whose explicit
    // quarantine horizon lapsed one second ago.
    await manager.overrideRejoinTracking(
      convoId: "convo-lockout",
      attempts: 3,
      lastAttempt: now.addingTimeInterval(-3600),
      quarantinedUntil: now.addingTimeInterval(-1)
    )

    let skips = await manager.shouldSkipRejoin(convoId: "convo-lockout")
    XCTAssertFalse(skips, "lapsed lockout must clamp the count and re-open the gate")

    let remaining = await manager.remainingRejoinAttempts(convoId: "convo-lockout")
    XCTAssertEqual(remaining, 1, "runtime expiry re-opens EXACTLY one attempt")

    await manager.flushPersistence()
    let persisted = await store.entry(for: "convo-lockout")
    XCTAssertEqual(
      persisted?.failedRejoinCount, 2,
      "the clamped count must be written through so the clamp survives restart"
    )

    // The single re-opened attempt failing re-arms the maxed-out gate.
    await manager.recordFailedRejoin(convoId: "convo-lockout")
    let skipsAfterFailure = await manager.shouldSkipRejoin(convoId: "convo-lockout")
    XCTAssertTrue(skipsAfterFailure, "one failure after the clamp must re-arm the maxed-out gate")
  }

  func testHydrationClampsMaxedEntryWithExpiredOrAbsentQuarantine() async throws {
    let now = Date()
    let store = InMemoryRecoveryStateStore()
    try await store.upsertConversationState(
      conversationID: "convo-expired-quarantine",
      failedRejoinCount: 3,
      lastAttemptAtMs: epochMs(now.addingTimeInterval(-3600)),
      quarantinedUntilMs: epochMs(now.addingTimeInterval(-60))
    )
    try await store.upsertConversationState(
      conversationID: "convo-nil-quarantine",
      failedRejoinCount: 5,
      lastAttemptAtMs: epochMs(now.addingTimeInterval(-3600)),
      quarantinedUntilMs: nil
    )
    try await store.upsertConversationState(
      conversationID: "convo-active-quarantine",
      failedRejoinCount: 3,
      lastAttemptAtMs: epochMs(now.addingTimeInterval(-3600)),
      quarantinedUntilMs: epochMs(now.addingTimeInterval(3600))
    )

    let manager = MLSRecoveryManager(persistence: store)
    await manager.hydrateFromDatabase()

    let skipsExpired = await manager.shouldSkipRejoin(convoId: "convo-expired-quarantine")
    XCTAssertFalse(skipsExpired, "expired quarantine must clamp at hydration and re-open the gate")
    let remainingExpired = await manager.remainingRejoinAttempts(convoId: "convo-expired-quarantine")
    XCTAssertEqual(remainingExpired, 1, "hydration clamp re-opens exactly one attempt")

    let skipsNil = await manager.shouldSkipRejoin(convoId: "convo-nil-quarantine")
    XCTAssertFalse(skipsNil, "maxed entry with no quarantine horizon must clamp at hydration")
    let remainingNil = await manager.remainingRejoinAttempts(convoId: "convo-nil-quarantine")
    XCTAssertEqual(remainingNil, 1)

    let skipsActive = await manager.shouldSkipRejoin(convoId: "convo-active-quarantine")
    XCTAssertTrue(skipsActive, "an ACTIVE quarantine must still gate (honored, never clamped)")
  }

  func testSuccessCooldownArmsOnSuccessExpiresAndIsNotArmedByFreshReset() async throws {
    let manager = MLSRecoveryManager(persistence: nil)

    // Success-outcome clear arms the cooldown (sync-triggered rejoins gated).
    await manager.clearRejoinTracking(convoId: "convo-success")
    let remaining = await manager.successCooldownRemaining(convoId: "convo-success")
    XCTAssertNotNil(remaining, "successful rejoin must arm the sync-path cooldown")
    XCTAssertGreaterThan(
      remaining ?? 0,
      MLSRecoveryManager.successfulRejoinCooldownSec - 30,
      "freshly armed cooldown should be close to the full window"
    )

    // After the window elapses the cooldown stops gating.
    await manager.overrideSuccessfulRejoinTimestamp(
      convoId: "convo-success",
      to: Date().addingTimeInterval(-(MLSRecoveryManager.successfulRejoinCooldownSec + 1))
    )
    let expired = await manager.successCooldownRemaining(convoId: "convo-success")
    XCTAssertNil(expired, "cooldown must expire after successfulRejoinCooldownSec")

    // Fresh-reset clear is a non-attempt bookkeeping event — it must NOT arm.
    await manager.clearRejoinTrackingForFreshReset(convoId: "convo-reset")
    let resetCooldown = await manager.successCooldownRemaining(convoId: "convo-reset")
    XCTAssertNil(resetCooldown, "fresh-reset clear must NOT arm the success cooldown")

    // Local state loss is also not a successful recovery outcome, and it must
    // clear an existing success cooldown so Welcome/reissue recovery can run.
    await manager.clearRejoinTracking(convoId: "convo-local-loss")
    let localLossCooldownBefore = await manager.successCooldownRemaining(convoId: "convo-local-loss")
    XCTAssertNotNil(localLossCooldownBefore)

    await manager.clearRejoinTrackingAfterLocalStateLoss(convoId: "convo-local-loss")
    let localLossCooldownAfter = await manager.successCooldownRemaining(convoId: "convo-local-loss")
    XCTAssertNil(localLossCooldownAfter, "local-state-loss clear must remove the success cooldown")
  }

  func testHydrationDropsFutureDatedEntryAndDeletesRow() async throws {
    let now = Date()
    let store = InMemoryRecoveryStateStore()
    try await store.upsertConversationState(
      conversationID: "convo-future",
      failedRejoinCount: 2,
      lastAttemptAtMs: epochMs(now.addingTimeInterval(6 * 3600)),
      quarantinedUntilMs: nil
    )

    let manager = MLSRecoveryManager(persistence: store)
    await manager.hydrateFromDatabase()

    let skips = await manager.shouldSkipRejoin(convoId: "convo-future")
    XCTAssertFalse(skips, "future-dated entry must be ignored (under-gating never extends backoff)")
    let row = await store.entry(for: "convo-future")
    XCTAssertNil(row, "future-dated row must be DELETED at hydration (Rust drop+delete parity)")
  }

  func testHydrationBoundsFutureDatedGlobalStampSkew() async throws {
    let now = Date()
    let store = InMemoryRecoveryStateStore()
    try await store.setGlobalLastRejoinAttempt(atMs: epochMs(now.addingTimeInterval(7 * 24 * 3600)))

    let manager = MLSRecoveryManager(persistence: store)
    await manager.hydrateFromDatabase()

    let floor = await manager.lastGlobalRejoinAttemptForTesting()
    XCTAssertNotNil(floor, "future-dated global stamp is deliberate (Rust-gate projection) — keep it")
    XCTAssertLessThanOrEqual(
      floor?.timeIntervalSince(now) ?? .infinity,
      MLSRecoveryManager.maxHydratedGlobalStampSkew + 5,
      "forward clock skew in the persisted stamp must be bounded so it cannot wedge the global gate"
    )
    let skips = await manager.shouldSkipRejoin(convoId: "any-convo")
    XCTAssertTrue(skips, "a bounded future-projected global stamp must still gate right now")
  }
}
