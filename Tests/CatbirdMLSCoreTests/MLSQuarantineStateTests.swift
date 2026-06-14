import XCTest

@testable import CatbirdMLSCore

/// N39 (3-band collapse) + N40 (Layer-3 quarantine state seam) tests.
///
/// Contract under test, justified against the Rust twin
/// (`catbird-mls/src/orchestrator/recovery.rs` `RecoveryTracker`):
/// - the quarantine horizon arms when `failedRejoinCount` reaches
///   `MAX_REJOIN_ATTEMPTS` (3) — Rust `record_failure` arms `lockout_until`
///   at `count >= max_attempts`, persisted as `quarantined_until_ms`;
/// - quarantine ENTRY clears the failure counter (Rust `mark_quarantined`
///   does `failed_rejoins.remove`) — corruption/peer badness is STATE, never
///   count inflation (the retired 8/13 Swift-only bands);
/// - Layer-3 quarantine is the strongest gate in `shouldSkipRejoin` (Rust
///   `should_skip` checks `quarantined` first);
/// - indefinite quarantine exits only via `clearQuarantine` (event-driven,
///   Rust `clear_quarantine` / `QuarantineExitReason`).
@available(iOS 18.0, macOS 13.0, *)
final class MLSQuarantineStateTests: XCTestCase {

  private actor StoreBox: MLSRecoveryStatePersisting {
    struct Entry: Sendable {
      var failedRejoinCount: Int
      var lastAttemptAtMs: Int64
      var quarantinedUntilMs: Int64?
    }

    private var entries: [String: Entry] = [:]
    private var globalMs: Int64?

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
      globalMs = atMs
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
        lastGlobalRejoinAttemptAtMs: globalMs
      )
    }

    func entry(for conversationID: String) -> Entry? {
      entries[conversationID]
    }
  }

  // MARK: - N39: single arming threshold

  func testQuarantineHorizonArmsExactlyAtMaxRejoinAttempts() async throws {
    let store = StoreBox()
    let manager = MLSRecoveryManager(persistence: store)

    // Below max: no horizon persisted.
    for _ in 0..<(MLSRecoveryManager.maxRejoinAttempts - 1) {
      await manager.recordFailedRejoin(convoId: "convo-arm")
    }
    await manager.flushPersistence()
    let belowMax = await store.entry(for: "convo-arm")
    XCTAssertNil(
      belowMax?.quarantinedUntilMs,
      "horizon must NOT arm below MAX_REJOIN_ATTEMPTS (Rust: lockout_until is None for non-maxed entries)"
    )

    // At max: horizon armed, counter NOT inflated.
    await manager.recordFailedRejoin(convoId: "convo-arm")
    await manager.flushPersistence()
    let atMax = await store.entry(for: "convo-arm")
    XCTAssertNotNil(
      atMax?.quarantinedUntilMs,
      "horizon must arm at MAX_REJOIN_ATTEMPTS (Rust record_failure parity)"
    )
    XCTAssertEqual(
      atMax?.failedRejoinCount, MLSRecoveryManager.maxRejoinAttempts,
      "counter must stay at the real attempt count (no 8/13 inflation bands)"
    )

    let skips = await manager.shouldSkipRejoin(convoId: "convo-arm")
    XCTAssertTrue(skips)
  }

  // MARK: - N39: server corruption is state, not count inflation

  func testServerCorruptedEntersQuarantineStateAndClearsCounter() async throws {
    let store = StoreBox()
    let manager = MLSRecoveryManager(persistence: store)

    await manager.recordFailedRejoin(convoId: "convo-corrupt")
    await manager.recordFailedRejoin(convoId: "convo-corrupt")
    await manager.markConversationServerCorrupted(
      convoId: "convo-corrupt", errorMessage: "GroupInfo deserialization failed")
    await manager.flushPersistence()

    let corrupted = await manager.isConversationServerCorrupted(convoId: "convo-corrupt")
    XCTAssertTrue(corrupted, "corruption must be readable as quarantine state")

    let state = await manager.quarantineState(for: "convo-corrupt")
    XCTAssertEqual(state?.reason, .serverDataCorruption)
    XCTAssertNotNil(state?.until, "server corruption carries a 24h horizon, not an indefinite hold")

    // Enter-clears-counter (Rust mark_quarantined parity): the in-memory
    // failure counter is wiped — the quarantine state is the gate now.
    let remaining = await manager.remainingRejoinAttempts(convoId: "convo-corrupt")
    XCTAssertEqual(
      remaining, MLSRecoveryManager.maxRejoinAttempts,
      "quarantine entry must CLEAR the failure counter (enter-clears-counter), not inflate it to 13"
    )

    let skips = await manager.shouldSkipRejoin(convoId: "convo-corrupt")
    XCTAssertTrue(skips, "quarantined conversation must be gated")

    // Persisted as the Rust-portable maxed representation so the GATE
    // survives restart: count == max + quarantined_until_ms.
    let persisted = await store.entry(for: "convo-corrupt")
    XCTAssertEqual(persisted?.failedRejoinCount, MLSRecoveryManager.maxRejoinAttempts)
    XCTAssertNotNil(persisted?.quarantinedUntilMs)
  }

  func testServerCorruptedGateSurvivesRestartReasonDoesNot() async throws {
    let store = StoreBox()
    let manager = MLSRecoveryManager(persistence: store)

    await manager.markConversationServerCorrupted(
      convoId: "convo-restart", errorMessage: "endOfStream")
    await manager.flushPersistence()

    let restarted = MLSRecoveryManager(persistence: store)
    await restarted.hydrateFromDatabase()

    let skips = await restarted.shouldSkipRejoin(convoId: "convo-restart")
    XCTAssertTrue(
      skips,
      "the gating horizon must survive restart via the persisted maxed row"
    )
    let corrupted = await restarted.isConversationServerCorrupted(convoId: "convo-restart")
    XCTAssertFalse(
      corrupted,
      "the quarantine REASON is in-memory only (documented N39 trade-off — no production caller consumed it across restarts)"
    )
  }

  // MARK: - N40: Layer-3 state machine seam

  func testIndefiniteQuarantineGatesUntilEventDrivenExit() async throws {
    let store = StoreBox()
    let manager = MLSRecoveryManager(persistence: store)

    await manager.recordFailedRejoin(convoId: "convo-l3")
    await manager.flushPersistence()
    let rowBefore = await store.entry(for: "convo-l3")
    XCTAssertNotNil(rowBefore)

    await manager.markQuarantined(
      convoId: "convo-l3",
      reason: .peerBadCommit,
      suspectedDIDs: ["did:plc:badpeer"],
      until: nil
    )
    await manager.flushPersistence()

    let quarantined = await manager.isQuarantined(convoId: "convo-l3")
    XCTAssertTrue(quarantined)
    let state = await manager.quarantineState(for: "convo-l3")
    XCTAssertEqual(state?.reason, .peerBadCommit)
    XCTAssertEqual(state?.suspectedDIDs, ["did:plc:badpeer"])
    XCTAssertNil(state?.until, "peer-bad quarantine is indefinite (event-driven exit)")

    let skips = await manager.shouldSkipRejoin(convoId: "convo-l3")
    XCTAssertTrue(skips, "Layer-3 quarantine is the strongest gate")

    // Enter-clears-counter write-through: the persisted backoff row is
    // deleted (indefinite quarantine is deliberately NOT persisted — no
    // event-driven exit signal is wired on iOS yet, and a persisted
    // indefinite hold could permanently wedge a conversation).
    let rowAfter = await store.entry(for: "convo-l3")
    XCTAssertNil(rowAfter)

    // Event-driven exit re-opens the gate.
    let cleared = await manager.clearQuarantine(convoId: "convo-l3")
    XCTAssertTrue(cleared)
    let clearedAgain = await manager.clearQuarantine(convoId: "convo-l3")
    XCTAssertFalse(clearedAgain, "second clear must report nothing to clear")

    // Per-conversation state must be fully reset. (shouldSkipRejoin is
    // deliberately NOT asserted false here: the earlier recordFailedRejoin
    // armed the cross-conversation 30s global rejoin floor — spec §8.4 —
    // which gates EVERY conversation right now, same convention as the N20
    // test in MLSRecoveryPersistenceTests.)
    let stillQuarantined = await manager.isQuarantined(convoId: "convo-l3")
    XCTAssertFalse(stillQuarantined, "exit must drop the quarantine state")
    let remaining = await manager.remainingRejoinAttempts(convoId: "convo-l3")
    XCTAssertEqual(
      remaining, MLSRecoveryManager.maxRejoinAttempts,
      "counter was cleared on entry and must stay cleared after exit"
    )
  }

  func testHorizonQuarantineExpiresLazily() async throws {
    let manager = MLSRecoveryManager(persistence: nil)

    await manager.markQuarantined(
      convoId: "convo-lapsed",
      reason: .serverDataCorruption,
      until: Date().addingTimeInterval(-1)
    )

    let quarantined = await manager.isQuarantined(convoId: "convo-lapsed")
    XCTAssertFalse(quarantined, "a lapsed horizon must drop the state lazily")
    let corrupted = await manager.isConversationServerCorrupted(convoId: "convo-lapsed")
    XCTAssertFalse(corrupted)
    let skips = await manager.shouldSkipRejoin(convoId: "convo-lapsed")
    XCTAssertFalse(skips, "nothing should gate after the horizon lapses (TTL self-reset contract)")
  }

  func testFreshResetAndSuccessClearsExitQuarantine() async throws {
    let manager = MLSRecoveryManager(persistence: nil)

    // Server reset exit (Rust QuarantineExitReason::ServerReset analog).
    await manager.markQuarantined(convoId: "convo-reset", reason: .multiPeerBadCommits)
    await manager.clearRejoinTrackingForFreshReset(convoId: "convo-reset")
    let afterReset = await manager.isQuarantined(convoId: "convo-reset")
    XCTAssertFalse(afterReset, "server-pushed reset must exit quarantine")

    // Success exit (PeerCommitSucceeded / UserConfirmedReset analog).
    await manager.markQuarantined(convoId: "convo-success", reason: .repeatedFramingFailures)
    await manager.clearRejoinTracking(convoId: "convo-success")
    let afterSuccess = await manager.isQuarantined(convoId: "convo-success")
    XCTAssertFalse(afterSuccess, "successful recovery must exit quarantine")
  }

  func testPeerBadCounterTriggersQuarantineWithRustConvention() async throws {
    let manager = MLSRecoveryManager(persistence: nil)

    let first = await manager.recordPeerBadCommit(
      convoId: "convo-peer",
      messageID: "m1",
      suspectedDID: "did:plc:badpeer"
    )
    let second = await manager.recordPeerBadCommit(
      convoId: "convo-peer",
      messageID: "m2",
      suspectedDID: "did:plc:badpeer"
    )
    XCTAssertNil(first)
    XCTAssertNil(second)
    let notYetQuarantined = await manager.isQuarantined(convoId: "convo-peer")
    XCTAssertFalse(notYetQuarantined)

    let third = await manager.recordPeerBadCommit(
      convoId: "convo-peer",
      messageID: "m3",
      suspectedDID: "did:plc:badpeer"
    )
    XCTAssertEqual(third, .peerBadCommit)
    let state = await manager.quarantineState(for: "convo-peer")
    XCTAssertEqual(state?.reason, .peerBadCommit)
    XCTAssertEqual(state?.suspectedDIDs, ["did:plc:badpeer"])

    let manager2 = MLSRecoveryManager(persistence: nil)
    let onePeer = await manager2.recordPeerBadCommit(
      convoId: "convo-multi",
      messageID: "m1",
      suspectedDID: "did:plc:alpha"
    )
    let twoPeers = await manager2.recordPeerBadCommit(
      convoId: "convo-multi",
      messageID: "m2",
      suspectedDID: "did:plc:beta"
    )
    XCTAssertNil(onePeer)
    XCTAssertEqual(twoPeers, .multiPeerBadCommits)
    let multiPeerState = await manager2.quarantineState(for: "convo-multi")
    XCTAssertEqual(
      multiPeerState?.reason,
      .multiPeerBadCommits
    )

    let manager3 = MLSRecoveryManager(persistence: nil)
    _ = await manager3.recordPeerBadCommit(convoId: "convo-framing", messageID: "m1")
    _ = await manager3.recordPeerBadCommit(convoId: "convo-framing", messageID: "m2")
    let framing = await manager3.recordPeerBadCommit(convoId: "convo-framing", messageID: "m3")
    XCTAssertEqual(framing, .repeatedFramingFailures)
    let framingState = await manager3.quarantineState(for: "convo-framing")
    XCTAssertEqual(
      framingState?.reason,
      .repeatedFramingFailures
    )
  }
}
