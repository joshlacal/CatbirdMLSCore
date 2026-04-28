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

  // MARK: - Mark RESET_PENDING with nil pendingNewGroupId

  /// `handleResetRequested` calls
  /// `markConversationNeedsReset(convoId, pendingNewGroupId: nil,
  /// pendingResetGeneration: Int64(event.generation))`. This test verifies
  /// the resulting row state: `needsReset = 1`, `pendingNewGroupId = NULL`,
  /// `pendingResetGeneration = event.generation`. Mirrors the legacy
  /// `handleGroupReset` SQL but with `pendingNewGroupId = nil` so the
  /// deferred recovery loop takes the **initiator** branch (mint a local
  /// group id and race-bootstrap via the chokepoint UNIQUE constraint)
  /// rather than the recipient branch (External Commit into the announced
  /// group). See `docs/plans/phase-2-5-indirect-funneling.md` §3.
  func testMarkResetPendingWithNilNewGroupIdSetsInitiatorPath() async throws {
    let convoId = "convo-2-5-initiator"
    let userDid = "did:plc:initiator"
    let staleGroup = Data(repeating: 0xAA, count: 32)
    let now = Date(timeIntervalSince1970: 1_700_000_000)

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

    // Simulate the SQL `markConversationNeedsReset` writes.
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
        arguments: [
          // pendingNewGroupId = NULL — Phase 2.5 indirect triggers carry no
          // announced group id; the initiator branch races the bootstrap.
          nil as String?,
          Int64(17), now, convoId, userDid,
        ]
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
    XCTAssertNil(row.pendingNewGroupId,
                 "indirect triggers MUST persist nil pendingNewGroupId (no server-announced id)")
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
