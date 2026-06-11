import GRDB
import XCTest

@testable import CatbirdMLSCore

/// N43: the two v32-table writers (`MLSRecoveryStateStore` and
/// `MLSOrchestratorStorageAdapter`) must key rows by the same DID
/// normalization (`MLSStorageHelpers.normalizeDID`), and rows written before
/// the fix (keyed by a raw mixed-case DID) must be re-keyed at hydration.
final class MLSRecoveryStateStoreTests: XCTestCase {
  private var dbQueue: DatabaseQueue!

  override func setUp() async throws {
    try await super.setUp()
    dbQueue = try DatabaseQueue()
    // Full production migration chain — same substrate the v32 tables get
    // in the real per-user database.
    try MLSGRDBManager.makeMigrator().migrate(dbQueue)
  }

  override func tearDown() async throws {
    dbQueue = nil
    try await super.tearDown()
  }

  func testMixedCaseDIDWritesAndReadsUnderNormalizedKey() async throws {
    let rawDID = "  did:web:Example.Com "
    let normalizedDID = MLSStorageHelpers.normalizeDID(rawDID)
    XCTAssertEqual(normalizedDID, "did:web:example.com")

    let store = MLSRecoveryStateStore(database: dbQueue, currentUserDID: rawDID)
    try await store.upsertConversationState(
      conversationID: "convo-1",
      failedRejoinCount: 2,
      lastAttemptAtMs: 1_000,
      quarantinedUntilMs: nil
    )
    try await store.setGlobalLastRejoinAttempt(atMs: 2_000)

    // Rows land under the normalized key — the same key the orchestrator
    // storage adapter uses.
    let storedKeys = try await dbQueue.read { db in
      try String.fetchAll(
        db, sql: "SELECT currentUserDID FROM MLSRecoveryAttemptStateModel")
        + String.fetchAll(db, sql: "SELECT currentUserDID FROM MLSRecoveryGlobalStateModel")
    }
    XCTAssertEqual(storedKeys, [normalizedDID, normalizedDID])

    // Round-trip: a store constructed with the already-normalized DID sees
    // the rows written by the mixed-case construction.
    let normalizedStore = MLSRecoveryStateStore(
      database: dbQueue, currentUserDID: normalizedDID)
    let snapshot = try await normalizedStore.loadSnapshot()
    XCTAssertEqual(snapshot.conversations.count, 1)
    XCTAssertEqual(snapshot.conversations.first?.conversationID, "convo-1")
    XCTAssertEqual(snapshot.conversations.first?.failedRejoinCount, 2)
    XCTAssertEqual(snapshot.lastGlobalRejoinAttemptAtMs, 2_000)

    // And the mixed-case store can clear what it wrote.
    try await store.clearConversationState(conversationID: "convo-1")
    let remaining = try await store.loadSnapshot()
    XCTAssertTrue(remaining.conversations.isEmpty)
  }

  func testLegacyRawDIDRowsAreRekeyedAtHydration() async throws {
    let rawDID = "did:plc:MixedCase"
    let normalizedDID = MLSStorageHelpers.normalizeDID(rawDID)

    // Simulate pre-N43 rows written under the raw key.
    try await dbQueue.write { db in
      try db.execute(
        sql: """
          INSERT INTO MLSRecoveryAttemptStateModel
            (conversationID, currentUserDID, failedRejoinCount, lastAttemptAtMs, quarantinedUntilMs)
          VALUES ('legacy-convo', ?, 3, 500, NULL)
          """,
        arguments: [rawDID]
      )
      try db.execute(
        sql: """
          INSERT INTO MLSRecoveryGlobalStateModel
            (currentUserDID, lastGlobalRejoinAttemptAtMs)
          VALUES (?, 750)
          """,
        arguments: [rawDID]
      )
    }

    let store = MLSRecoveryStateStore(database: dbQueue, currentUserDID: rawDID)
    let snapshot = try await store.loadSnapshot()

    XCTAssertEqual(snapshot.conversations.count, 1)
    XCTAssertEqual(snapshot.conversations.first?.conversationID, "legacy-convo")
    XCTAssertEqual(snapshot.conversations.first?.failedRejoinCount, 3)
    XCTAssertEqual(snapshot.lastGlobalRejoinAttemptAtMs, 750)

    // The legacy rows were re-keyed in place, not just read through.
    let keys = try await dbQueue.read { db in
      try String.fetchAll(
        db, sql: "SELECT currentUserDID FROM MLSRecoveryAttemptStateModel")
        + String.fetchAll(db, sql: "SELECT currentUserDID FROM MLSRecoveryGlobalStateModel")
    }
    XCTAssertEqual(keys, [normalizedDID, normalizedDID])
  }

  func testRekeyPrefersExistingNormalizedRowOnConflict() async throws {
    let rawDID = "did:plc:MixedCase"
    let normalizedDID = MLSStorageHelpers.normalizeDID(rawDID)

    try await dbQueue.write { db in
      // Stale pre-fix row under the raw key…
      try db.execute(
        sql: """
          INSERT INTO MLSRecoveryAttemptStateModel
            (conversationID, currentUserDID, failedRejoinCount, lastAttemptAtMs, quarantinedUntilMs)
          VALUES ('convo-x', ?, 1, 100, NULL)
          """,
        arguments: [rawDID]
      )
      // …and a newer post-fix row under the normalized key for the SAME
      // primary key (conversationID, currentUserDID-after-rekey).
      try db.execute(
        sql: """
          INSERT INTO MLSRecoveryAttemptStateModel
            (conversationID, currentUserDID, failedRejoinCount, lastAttemptAtMs, quarantinedUntilMs)
          VALUES ('convo-x', ?, 5, 900, NULL)
          """,
        arguments: [normalizedDID]
      )
      try db.execute(
        sql: """
          INSERT INTO MLSRecoveryGlobalStateModel
            (currentUserDID, lastGlobalRejoinAttemptAtMs)
          VALUES (?, 100), (?, 900)
          """,
        arguments: [rawDID, normalizedDID]
      )
    }

    let store = MLSRecoveryStateStore(database: dbQueue, currentUserDID: rawDID)
    let snapshot = try await store.loadSnapshot()

    // The newer normalized-key row wins; the stale raw-key row is dropped.
    XCTAssertEqual(snapshot.conversations.count, 1)
    XCTAssertEqual(snapshot.conversations.first?.failedRejoinCount, 5)
    XCTAssertEqual(snapshot.conversations.first?.lastAttemptAtMs, 900)
    XCTAssertEqual(snapshot.lastGlobalRejoinAttemptAtMs, 900)

    let rowCount = try await dbQueue.read { db in
      try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM MLSRecoveryAttemptStateModel") ?? -1
    }
    XCTAssertEqual(rowCount, 1)
  }
}
