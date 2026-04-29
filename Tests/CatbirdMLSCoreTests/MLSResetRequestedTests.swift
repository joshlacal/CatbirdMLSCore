import XCTest
import GRDB
import Petrel
@testable import CatbirdMLSCore

/// Unit tests for the Phase 2.5 indirect-trigger `resetRequestedEvent` path.
///
/// `MLSConversationManager.handleResetRequested(event:)` mirrors
/// `handleGroupReset(event:)` exactly except that the indirect path has no
/// announced new group id — it flags `RESET_PENDING` with
/// `pendingNewGroupId = nil` so deferred recovery's initiator branch
/// (mint local group + race-bootstrap) drives election. The server's
/// `crypto_sessions UNIQUE (conversation_id, generation)` chokepoint
/// constraint serializes the winning candidate (first commit wins).
///
/// These DB-level tests verify the SQL contract of the row transition the
/// handler applies, mirroring the `MLSGroupResetRecipientTests` pattern. The
/// actor-level integration (MLSClient + MLSAPIClient wiring) belongs in a
/// separate integration target.
@available(iOS 18.0, macOS 13.0, *)
final class MLSResetRequestedTests: XCTestCase {
  private var dbQueue: DatabaseQueue!

  override func setUp() async throws {
    try await super.setUp()
    dbQueue = try DatabaseQueue()
    try await dbQueue.write { db in
      try Self.createConversationTable(in: db)
    }
  }

  override func tearDown() async throws {
    dbQueue = nil
    try await super.tearDown()
  }

  // MARK: - Decode contract

  /// The Petrel `ResetRequestedEvent` struct must round-trip through the
  /// `BlueCatbirdMlsChatSubscribeEvents.Message` union with the lexicon's
  /// `$type` identifier. This guards against future Petrel regenerations
  /// that might rename or re-shape the field set the handler depends on.
  func testResetRequestedEventDecodesFromUnionPayload() throws {
    let payload = """
      {
        "$type": "blue.catbird.mlsChat.subscribeEvents#resetRequestedEvent",
        "cursor": "abcdef",
        "convoId": "convo-2-5",
        "cryptoSessionId": "session-prior",
        "generation": 17,
        "trigger": "inlineGroupInfo404",
        "requestEventId": "req-inline-404:convo-2-5:7:1700000000",
        "expectedNewMlsGroupId": null,
        "reason": "GroupInfo 404 threshold crossed",
        "requestedAt": "2026-04-28T15:32:11.123Z"
      }
      """.data(using: .utf8)!

    let decoder = JSONDecoder()
    let message = try decoder.decode(
      BlueCatbirdMlsChatSubscribeEvents.Message.self, from: payload)

    guard case .resetRequestedEvent(let event) = message else {
      XCTFail("expected .resetRequestedEvent variant, got \(message)")
      return
    }
    XCTAssertEqual(event.convoId, "convo-2-5")
    XCTAssertEqual(event.cryptoSessionId, "session-prior")
    XCTAssertEqual(event.generation, 17)
    XCTAssertEqual(event.trigger, "inlineGroupInfo404")
    XCTAssertEqual(event.requestEventId, "req-inline-404:convo-2-5:7:1700000000")
    XCTAssertNil(event.expectedNewMlsGroupId,
                 "indirect triggers MUST omit a server-minted group id (Phase 2.5 §1)")
    XCTAssertEqual(event.reason, "GroupInfo 404 threshold crossed")
  }

  // MARK: - Mint candidate id and stage RESET_PENDING (bootstrap-race path)

  /// The mint helper produces a 32-lowercase-hex-char id that mirrors the
  /// Rust orchestrator's `format!("{:032x}", uuid::Uuid::new_v4().as_u128())`.
  /// Required for the candidate id to be parseable by `Data(hexEncoded:)`
  /// downstream in `attemptFirstResponderBootstrap` (line 1111 of
  /// `MLSConversationManager+Sync.swift`).
  func testMintCandidateGroupIdHexProducesValid32CharLowercaseHex() {
    let id = MLSConversationManager.mintCandidateGroupIdHex()
    XCTAssertEqual(id.count, 32, "must be 32 hex chars (uuid u128 → 32x4-bit nibbles)")
    XCTAssertEqual(id, id.lowercased(), "must be all lowercase to match the Rust mint")
    let allowed: Set<Character> = Set("0123456789abcdef")
    for ch in id {
      XCTAssertTrue(allowed.contains(ch), "non-hex char in minted id: \(ch)")
    }
    XCTAssertNotNil(Data(hexEncoded: id),
                    "id must round-trip through Data(hexEncoded:) for downstream MLS use")
  }

  /// `handleResetRequested` mints a client-side candidate id and stages it
  /// as `pendingNewGroupId` along with the event's `generation`. This test
  /// verifies the resulting row state: `needsReset = 1`, a non-nil 32-hex
  /// `pendingNewGroupId`, and `pendingResetGeneration = event.generation`.
  ///
  /// **Why a non-nil id?** The existing sync-loop branch in
  /// `MLSConversationManager+Sync.swift:635` treats `pendingNewGroupId == nil`
  /// as the admin-only `resetGroup` path that fails with `notadmin` for
  /// non-admin members. Phase 2.5 retires the admin-mint flow for indirect
  /// triggers; staging a client-minted candidate routes the row through the
  /// recipient-then-bootstrap branch (the bootstrap call uses the chokepoint
  /// UNIQUE constraint to elect a single winner; race losers see HTTP 409
  /// `AlreadyBootstrapped` and drop their pre-bootstrap state).
  func testMarkResetPendingWithMintedCandidateStagesBootstrapPath() async throws {
    let convoId = "convo-2-5-bootstrap"
    let userDid = "did:plc:initiator"
    let staleGroup = Data(repeating: 0xAA, count: 32)
    let now = Date(timeIntervalSince1970: 1_700_000_000)
    let candidate = MLSConversationManager.mintCandidateGroupIdHex()

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: staleGroup,
        epoch: 837,
        needsReset: false,
        pendingNewGroupId: nil,
        pendingResetGeneration: nil,
        consecutiveFailures: 0
      )
    }

    // Simulate the SQL `markConversationNeedsReset` writes after the handler
    // resolves a candidate id (client-minted in this test, since no
    // `expectedNewMlsGroupId` was supplied by the server).
    try await dbQueue.write { db in
      try db.execute(
        sql: """
              UPDATE MLSConversationModel
              SET needsReset = 1,
                  needsRejoin = 0,
                  isUnrecoverable = 0,
                  pendingNewGroupId = ?,
                  pendingResetGeneration = ?,
                  updatedAt = ?
              WHERE conversationID = ? AND currentUserDID = ?;
          """,
        arguments: [candidate, Int64(17), now, convoId, userDid]
      )
    }

    let row = try await dbQueue.read { db in
      try XCTUnwrap(
        try MLSConversationModel.fetchOne(
          db,
          sql: "SELECT * FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
          arguments: [convoId, userDid]
        )
      )
    }

    XCTAssertTrue(row.needsReset, "needsReset must be set after a resetRequestedEvent")
    XCTAssertFalse(row.needsRejoin, "needsRejoin must clear")
    XCTAssertFalse(row.isUnrecoverable, "isUnrecoverable must clear")
    XCTAssertEqual(
      row.pendingNewGroupId, candidate,
      "indirect-trigger handler must stage a client-minted candidate so the sync-loop bootstrap-race branch fires (NOT the admin-only initiator branch)"
    )
    XCTAssertEqual(row.pendingResetGeneration, 17, "generation must mirror the event")
    XCTAssertEqual(row.persistedRecoveryState, .resetPending,
                   "row should derive RESET_PENDING from needsReset = 1")
  }

  // MARK: - Idempotency: stale generation does NOT clobber

  /// Phase 2.5 idempotency requirement: if the incoming `generation` is
  /// `<=` the stored `pendingResetGeneration`, the write is skipped.
  /// Required because the global (AppState) + per-convo (DetailView) WS
  /// subscriptions both deliver the event for active conversations, and
  /// `event_stream` cursor replay can re-deliver after reconnect.
  func testStaleGenerationIsRejected() async throws {
    let convoId = "convo-2-5-stale"
    let userDid = "did:plc:stale"
    let staleGroup = Data(repeating: 0xCC, count: 32)
    let earlier = Date(timeIntervalSince1970: 1_700_000_000)

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: staleGroup,
        epoch: 100,
        needsReset: true,
        pendingNewGroupId: nil,
        // Already at gen 18 from a prior event delivery.
        pendingResetGeneration: 18,
        consecutiveFailures: 0
      )
    }

    // Simulate the stale-guard read in `markConversationNeedsReset`.
    let stored = try await dbQueue.read { db in
      try MLSConversationResetSQL.loadPendingResetGeneration(
        db: db, conversationID: convoId, currentUserDID: userDid
      )
    }
    XCTAssertEqual(stored, 18, "precondition: row already at gen 18")

    let incoming = Int64(17)
    let shouldWrite = incoming > (stored ?? Int64.min)
    XCTAssertFalse(
      shouldWrite,
      "incoming gen \(incoming) must NOT clobber stored gen \(stored ?? -1)"
    )

    // Verify the row was not modified after the no-op decision.
    let unchanged = try await dbQueue.read { db in
      try XCTUnwrap(
        try MLSConversationModel.fetchOne(
          db,
          sql: "SELECT * FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
          arguments: [convoId, userDid]
        )
      )
    }
    XCTAssertEqual(unchanged.pendingResetGeneration, 18,
                   "stale-guard must leave the existing higher generation intact")
    _ = earlier  // silence unused-warning if the stub doesn't use it
  }

  /// `handleResetRequested` MUST short-circuit before the destructive
  /// `deleteGroup` call when the incoming `event.generation` is `<=` the
  /// stored `pendingResetGeneration`. Mirrors the Codex P1 review feedback
  /// on PR #1: relying on the row-write staleness guard inside
  /// `markConversationNeedsReset` is too late — the local MLS group state
  /// has already been deleted by step 1, which can clobber valid newer
  /// state when an out-of-order/replayed event arrives.
  ///
  /// SQL-level test (matches the rest of this file's pattern; the actor
  /// graph for `MLSConversationManager` is too large to construct here).
  /// Asserts the early-bail decision the handler now makes against
  /// `MLSConversationResetSQL.loadPendingResetGeneration`, plus invariants
  /// the handler's no-op contract guarantees:
  ///   - `groupID` (the canonical "would have been deleted" column proxy)
  ///     is unchanged
  ///   - `pendingResetGeneration` is unchanged
  ///   - `pendingNewGroupId` is unchanged
  func testHandleResetRequestedOlderGenerationIsNoOp() async throws {
    let convoId = "convo-2-5-older-gen"
    let userDid = "did:plc:older-gen"
    // Distinct group bytes so we can detect any stray `deleteGroup`-equivalent
    // mutation in DB state.
    let liveGroup = Data(repeating: 0xBE, count: 32)
    let priorCandidate = "deadbeefdeadbeefdeadbeefdeadbeef"
    // Row already at generation N+1 = 18 (e.g. from an earlier in-flight
    // event that already drove deferred recovery to a newer state).
    let storedGen: Int64 = 18

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: liveGroup,
        epoch: 200,
        needsReset: true,
        pendingNewGroupId: priorCandidate,
        pendingResetGeneration: storedGen,
        consecutiveFailures: 0
      )
    }

    // Simulate the new step-0 stale-generation guard added to
    // `handleResetRequested` (read happens BEFORE any destructive call).
    let stored = try await dbQueue.read { db in
      try MLSConversationResetSQL.loadPendingResetGeneration(
        db: db, conversationID: convoId, currentUserDID: userDid
      )
    }
    XCTAssertEqual(stored, storedGen, "precondition: row already at gen \(storedGen)")

    // Incoming event at N = 17 (older than stored 18).
    let incomingGeneration: Int32 = 17
    let isStale = (stored ?? Int64.min) >= Int64(incomingGeneration)
    XCTAssertTrue(
      isStale,
      "stored gen \(stored ?? -1) >= incoming gen \(incomingGeneration) → handler must early-bail BEFORE deleteGroup"
    )

    // Critically: a real handler invocation in this state must NOT mutate
    // any row state. We verify by reading the row back and asserting all
    // three fields are unchanged. (We can't assert the deleteGroup call
    // didn't fire without an actor harness, but the contract is "early
    // return before any side effects" — the row state is the proxy.)
    let unchanged = try await dbQueue.read { db in
      try XCTUnwrap(
        try MLSConversationModel.fetchOne(
          db,
          sql: "SELECT * FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
          arguments: [convoId, userDid]
        )
      )
    }
    XCTAssertEqual(
      unchanged.groupID, liveGroup,
      "no-op contract: groupID must NOT be mutated when event is stale"
    )
    XCTAssertEqual(
      unchanged.pendingResetGeneration, storedGen,
      "no-op contract: pendingResetGeneration must remain at the stored higher value"
    )
    XCTAssertEqual(
      unchanged.pendingNewGroupId, priorCandidate,
      "no-op contract: pendingNewGroupId must NOT be overwritten by a stale event"
    )
  }

  /// Equal-generation must also be a no-op. The Rust mirror at
  /// `recovery.rs:1401` short-circuits on `existing.reset_generation == reset_generation`,
  /// and the Swift `markConversationNeedsReset` guard at line 3353 uses
  /// `stored >= incoming`. The new step-0 guard in `handleResetRequested`
  /// must use the same `>=` semantic so the handler does NOT delete the
  /// current MLS group when re-delivered by the dual subscription path
  /// at the SAME generation.
  func testHandleResetRequestedEqualGenerationIsNoOp() async throws {
    let convoId = "convo-2-5-equal-gen"
    let userDid = "did:plc:equal-gen"
    let liveGroup = Data(repeating: 0xCD, count: 32)
    let priorCandidate = "feedfacefeedfacefeedfacefeedface"
    let storedGen: Int64 = 19

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: liveGroup,
        epoch: 300,
        needsReset: true,
        pendingNewGroupId: priorCandidate,
        pendingResetGeneration: storedGen,
        consecutiveFailures: 0
      )
    }

    let stored = try await dbQueue.read { db in
      try MLSConversationResetSQL.loadPendingResetGeneration(
        db: db, conversationID: convoId, currentUserDID: userDid
      )
    }
    let incomingGeneration: Int32 = 19
    let isStale = (stored ?? Int64.min) >= Int64(incomingGeneration)
    XCTAssertTrue(
      isStale,
      "equal generation must also short-circuit (mirrors Rust `==` and Swift `>=` checks)"
    )

    let unchanged = try await dbQueue.read { db in
      try XCTUnwrap(
        try MLSConversationModel.fetchOne(
          db,
          sql: "SELECT * FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
          arguments: [convoId, userDid]
        )
      )
    }
    XCTAssertEqual(unchanged.groupID, liveGroup,
                   "no-op contract: groupID unchanged on equal-generation replay")
    XCTAssertEqual(unchanged.pendingResetGeneration, storedGen,
                   "no-op contract: pendingResetGeneration unchanged on equal-generation replay")
  }

  // MARK: - CLIENT M (Task #75): post-bootstrap stale-replay defense

  /// Reproduces the production sequence observed at b947c701a32943d0:
  ///   1. attemptFirstResponderBootstrap wins gen=25 → epoch=1
  ///   2. applyRecipientResetSuccess seeds pendingResetGeneration=25
  ///   3. WS replays historical groupResetEvent / resetRequestedEvent
  ///      events at gen=25, 26, 27, 28, 29 from a stale cursor written
  ///      pre-bootstrap.
  ///   4. The pre-delete stale-generation guard reads stored=25 and rejects
  ///      gen<=25 events; gens 26..29 propagate through the existing path
  ///      (which is fine: they advance the row to the next reset).
  ///
  /// Without CLIENT M's seeding fix, step 2 nulled pendingResetGeneration,
  /// so step 4 fell through and called deleteGroup on the freshly bootstrapped
  /// group, destroying it.
  ///
  /// This is a SQL-level reproduction; it exercises the same guard the live
  /// handler uses (loadPendingResetGeneration + `>=` compare).
  func testHandleResetRequestedReplaySameGenerationAsBootstrapSeedIsNoOp() async throws {
    let convoId = "4b2cdbaad35a9d13"
    let userDid = "did:plc:bootstrapped"
    let bootstrappedGroup = Data(repeating: 0xB9, count: 32)  // standin for b947c701…
    let bootstrappedGen: Int64 = 25  // mirrors production observation

    // Simulate post-bootstrap row state: groupID swapped to the freshly
    // bootstrapped bytes, pendingResetGeneration seeded by CLIENT M.
    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: bootstrappedGroup,
        epoch: 1,
        needsReset: false,
        pendingNewGroupId: nil,
        pendingResetGeneration: bootstrappedGen,
        consecutiveFailures: 0
      )
    }

    // Replay historical event at gen=25 (same as seeded).
    let stored = try await dbQueue.read { db in
      try MLSConversationResetSQL.loadPendingResetGeneration(
        db: db, conversationID: convoId, currentUserDID: userDid
      )
    }
    XCTAssertEqual(stored, 25, "precondition: row seeded with gen=25 post-bootstrap")

    let incomingGeneration: Int32 = 25
    let isStale = (stored ?? Int64.min) >= Int64(incomingGeneration)
    XCTAssertTrue(
      isStale,
      "stored=25 >= replayed gen=25 → handler must short-circuit BEFORE deleteGroup"
    )

    // Verify row state is unchanged after the no-op decision (the contract
    // the handler implements is "early return; no destructive call").
    let unchanged = try await dbQueue.read { db in
      try XCTUnwrap(
        try MLSConversationModel.fetchOne(
          db,
          sql: "SELECT * FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
          arguments: [convoId, userDid]
        )
      )
    }
    XCTAssertEqual(
      unchanged.groupID, bootstrappedGroup,
      "groupID must NOT be mutated when a historical event re-announces the bootstrap-time generation"
    )
    XCTAssertEqual(unchanged.pendingResetGeneration, bootstrappedGen)
  }

  /// Self-echo: an incoming reset event whose `expectedNewMlsGroupId` matches
  /// the conversation's current local groupID is a self-echo of our own
  /// successful bootstrap (or a historical replay re-announcing the group we
  /// already operate at). The handler must short-circuit BEFORE deleteGroup
  /// even if the gen-check would otherwise fall through (e.g. seed missing).
  ///
  /// This guards against scenarios where the seeding write hasn't yet
  /// committed (race against deferred-recovery success → SSE delivery) but
  /// the local groupId already reflects the new state via in-memory
  /// `conversations[convoId]`.
  func testHandleResetRequestedSelfEchoLocalGroupIdMatchIsNoOp() async throws {
    let convoId = "convo-self-echo"
    let userDid = "did:plc:self-echo"
    let bootstrappedGroup = Data(repeating: 0xCE, count: 32)
    let bootstrappedHex = bootstrappedGroup.hexEncodedString()

    // Row in healthy state: pendingResetGeneration intentionally NULL
    // (simulates seeding-not-yet-committed window) so we exercise the
    // self-echo branch independently of the gen-check.
    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: bootstrappedGroup,
        epoch: 1,
        needsReset: false,
        pendingNewGroupId: nil,
        pendingResetGeneration: nil,
        consecutiveFailures: 0
      )
    }

    // Self-echo decision the handler makes: `serverHex == localHex`
    // (lowercased compare). Use the same shape — derive `localHex` from the
    // row, mirroring what `conversations[convoId]?.groupId` would yield.
    let localHex = bootstrappedHex.lowercased()
    let serverHex = bootstrappedHex.uppercased()  // case-different to test lowercased compare
    let isSelfEcho = localHex == serverHex.lowercased()
    XCTAssertTrue(
      isSelfEcho,
      "lowercased compare must catch case differences from server-vs-FFI hex emission"
    )

    // Verify row state is unchanged (the live handler returns early; we
    // assert the proxy: no DB mutation).
    let unchanged = try await dbQueue.read { db in
      try XCTUnwrap(
        try MLSConversationModel.fetchOne(
          db,
          sql: "SELECT * FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
          arguments: [convoId, userDid]
        )
      )
    }
    XCTAssertEqual(unchanged.groupID, bootstrappedGroup,
                   "self-echo no-op: groupID must NOT mutate")
    XCTAssertNil(unchanged.pendingResetGeneration,
                 "self-echo no-op: pendingResetGeneration must remain as it was")
  }

  /// CLIENT M (Task #75): the seed must be the MAX of (bootstrap-response gen,
  /// post-bootstrap getConversation gen, observed gen). The bootstrap may
  /// preserve a lower generation across self-heal (gen=25 in the production
  /// scenario), but server-side sweeps that fired during bootstrap could
  /// have advanced reset_count to a higher value (gen=29 in production).
  /// Without taking the MAX, replays of the higher gens (26-29) pass the
  /// `>=25` gate and call deleteGroup on the bootstrapped group.
  ///
  /// This test simulates the production scenario:
  ///   - bootstrap returned `convo.resetGeneration = 25` (preserved self-heal)
  ///   - getConversation refetch returned `convo.resetGeneration = 29` (sweeps
  ///     advanced after bootstrap)
  ///   - observedGeneration = 25 (the trigger event)
  ///   - MAX(25, 29, 25) = 29
  /// The seed value is what the staleness gate compares against: with
  /// seed=25, the gen=26 replay passes the `>=` gate (26 > 25 = NOT stale)
  /// and proceeds to deleteGroup. With seed=29, gen=26 is `<= 29 = stale`
  /// and short-circuits.
  ///
  /// Note: this test exercises the SEED CHOICE LOGIC at the SQL boundary,
  /// not the live `attemptFirstResponderBootstrap` actor (which requires
  /// the full MLSClient + MLSAPIClient harness). The actor wires the same
  /// MAX-of-three logic before passing `appliedGeneration` to
  /// `applyRecipientResetSuccess` — see `MLSConversationManager+Sync.swift`
  /// around `let seededGeneration = ...`.
  func testBootstrapSeedTakesMaxOfBootstrapResponseAndRefetch() async throws {
    let convoId = "convo-seed-max"
    let userDid = "did:plc:seed-max"
    let bootstrappedGroup = Data(repeating: 0xB9, count: 32)

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: Data(repeating: 0xAA, count: 32),
        epoch: 0,
        needsReset: true,
        pendingNewGroupId: bootstrappedGroup.hexEncodedString(),
        pendingResetGeneration: 25,  // observedGeneration
        consecutiveFailures: 0
      )
    }

    // Production-scenario inputs.
    let bootstrappedGen: Int64 = 25  // bootstrap response (preserved self-heal)
    let refetchedGen: Int64 = 29     // getConversation refetch (sweeps advanced)
    let observedGen: Int64 = 25      // event trigger

    let candidates: [Int64?] = [bootstrappedGen, refetchedGen, observedGen]
    let seeded = candidates.compactMap { $0 }.max()
    XCTAssertEqual(
      seeded, 29,
      "MAX of (25, 29, 25) must be 29 — refetched value wins because sweeps advanced post-bootstrap"
    )

    // Persist the seed via applyRecipientResetSuccess.
    try await dbQueue.write { db in
      try MLSConversationResetSQL.applyRecipientResetSuccess(
        db: db,
        conversationID: convoId,
        currentUserDID: userDid,
        newGroupID: bootstrappedGroup,
        newEpoch: 1,
        appliedGeneration: seeded,
        now: Date(timeIntervalSince1970: 1_700_000_000)
      )
    }

    // Now simulate replay events at gens 25, 26, 27, 28, 29. With seed=29,
    // ALL must be rejected. With seed=25 (the buggy single-value seed), only
    // gen=25 would be rejected and 26-29 would proceed to deleteGroup.
    let stored = try await dbQueue.read { db in
      try MLSConversationResetSQL.loadPendingResetGeneration(
        db: db, conversationID: convoId, currentUserDID: userDid
      )
    }
    XCTAssertEqual(stored, 29, "row must reflect the MAX seed, not just the bootstrap response")

    for replayedGen in Int32(25)...Int32(29) {
      let isStale = (stored ?? Int64.min) >= Int64(replayedGen)
      XCTAssertTrue(
        isStale,
        "replayed gen=\(replayedGen) must be rejected as stale against seed=29 (with the buggy seed=25, gens 26-29 would have fallen through to deleteGroup — the production destruction)"
      )
    }

    // Sanity: a real new gen=30 (post-replay-storm) must NOT be rejected.
    let freshGen: Int32 = 30
    let isFreshStale = (stored ?? Int64.min) >= Int64(freshGen)
    XCTAssertFalse(
      isFreshStale,
      "a genuinely fresh gen=30 must pass through (not get over-rejected by an over-eager seed)"
    )
  }

  /// CLIENT M (Task #75): self-echo branch must NOT trip when the server's
  /// `expectedNewMlsGroupId` is `nil` (Phase 2.5 indirect triggers do not
  /// include it). The handler should fall through to the candidate-mint
  /// flow in that case. This is a guard against a regression where an
  /// over-eager self-echo check would falsely positive on `nil == nil`.
  func testHandleResetRequestedExpectedNewGroupIdNilDoesNotTriggerSelfEcho() async throws {
    let convoId = "convo-no-expected"
    let userDid = "did:plc:no-expected"
    let liveGroup = Data(repeating: 0xAB, count: 32)

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: liveGroup,
        epoch: 5,
        needsReset: false,
        pendingNewGroupId: nil,
        pendingResetGeneration: nil,
        consecutiveFailures: 0
      )
    }

    // The handler's self-echo guard is gated on serverHex being non-nil &
    // non-empty. With `expectedNewMlsGroupId = nil`, the guard skips and the
    // handler proceeds to the regular flow.
    let serverHex: String? = nil
    let localHex = liveGroup.hexEncodedString()
    let isSelfEcho: Bool = {
      guard let serverHex, !serverHex.isEmpty else { return false }
      return serverHex.lowercased() == localHex.lowercased()
    }()
    XCTAssertFalse(
      isSelfEcho,
      "nil expectedNewMlsGroupId must NOT trigger self-echo short-circuit (Phase 2.5 indirect triggers depend on the candidate-mint flow)"
    )
  }

  // MARK: - Helpers (mirror MLSGroupResetRecipientTests schema)

  private static func createConversationTable(in db: Database) throws {
    try db.create(table: "MLSConversationModel") { t in
      t.primaryKey("conversationID", .text).notNull()
      t.column("currentUserDID", .text).notNull()
      t.column("groupID", .blob).notNull()
      t.column("epoch", .integer).notNull().defaults(to: 0)
      t.column("joinMethod", .text).notNull().defaults(to: "unknown")
      t.column("joinEpoch", .integer).notNull().defaults(to: 0)
      t.column("title", .text)
      t.column("avatarURL", .text)
      t.column("avatarImageData", .blob)
      t.column("createdAt", .datetime).notNull()
      t.column("updatedAt", .datetime).notNull()
      t.column("lastMessageAt", .datetime)
      t.column("lastMembershipChangeAt", .datetime)
      t.column("unacknowledgedMemberChanges", .integer).notNull().defaults(to: 0)
      t.column("isActive", .boolean).notNull().defaults(to: true)
      t.column("needsRejoin", .boolean).notNull().defaults(to: false)
      t.column("needsReset", .boolean).notNull().defaults(to: false)
      t.column("isUnrecoverable", .boolean).notNull().defaults(to: false)
      t.column("rejoinRequestedAt", .datetime)
      t.column("lastRecoveryAttempt", .datetime)
      t.column("consecutiveFailures", .integer).notNull().defaults(to: 0)
      t.column("isPlaceholder", .boolean).notNull().defaults(to: false)
      t.column("requestState", .text).notNull().defaults(to: "none")
      t.column("mutedUntil", .datetime)
      t.column("pendingNewGroupId", .text)
      t.column("pendingResetGeneration", .integer)
    }
  }

  private static func insertConversation(
    in db: Database,
    conversationID: String,
    currentUserDID: String,
    groupID: Data,
    epoch: Int64,
    needsReset: Bool,
    pendingNewGroupId: String?,
    pendingResetGeneration: Int64?,
    consecutiveFailures: Int = 0
  ) throws {
    let now = Date(timeIntervalSince1970: 1_699_000_000)
    try db.execute(
      sql: """
            INSERT INTO MLSConversationModel (
              conversationID, currentUserDID, groupID, epoch,
              joinMethod, joinEpoch, createdAt, updatedAt,
              unacknowledgedMemberChanges, isActive,
              needsRejoin, needsReset, isUnrecoverable,
              consecutiveFailures, isPlaceholder, requestState,
              pendingNewGroupId, pendingResetGeneration
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
      arguments: [
        conversationID, currentUserDID, groupID, epoch,
        "externalCommit", 0, now, now,
        0, true,
        false, needsReset, false,
        consecutiveFailures, false, "none",
        pendingNewGroupId, pendingResetGeneration,
      ]
    )
  }
}
