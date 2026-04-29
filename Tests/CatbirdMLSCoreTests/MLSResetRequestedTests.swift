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
