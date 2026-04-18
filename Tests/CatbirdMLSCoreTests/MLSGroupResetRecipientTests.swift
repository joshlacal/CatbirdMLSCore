import XCTest
import GRDB
@testable import CatbirdMLSCore

/// Unit tests for the §8.5 Phase 1 recipient-path DB helpers.
///
/// These tests exercise `MLSConversationResetSQL` against an in-memory
/// DatabaseQueue to validate the row-level state transition that happens
/// when a non-admin device finishes an External Commit into the new group
/// announced by a `GroupResetEvent`.
///
/// Actor-level integration (MLSClient / MLSAPIClient wiring for the
/// recipient branch in `MLSConversationManager+Sync.runDeferredEpochRecovery`)
/// belongs in a separate integration test target — see
/// `MLSRecoveryStateMachineTests.swift` for the same rationale.
@available(iOS 18.0, macOS 13.0, *)
final class MLSGroupResetRecipientTests: XCTestCase {
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

  // MARK: - applyRecipientResetSuccess

  func testApplyRecipientResetSuccessSwapsGroupAndClearsPending() async throws {
    let convoId = "convo-recipient-1"
    let userDid = "did:plc:recipient"
    let staleGroup = Data(repeating: 0xAA, count: 32)
    let newGroup = Data(repeating: 0xBB, count: 32)

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: staleGroup,
        epoch: 742,
        needsReset: true,
        pendingNewGroupId: newGroup.hexEncodedString(),
        pendingResetGeneration: 3,
        consecutiveFailures: 5
      )
    }

    let now = Date(timeIntervalSince1970: 1_700_000_000)
    try await dbQueue.write { db in
      try MLSConversationResetSQL.applyRecipientResetSuccess(
        db: db,
        conversationID: convoId,
        currentUserDID: userDid,
        newGroupID: newGroup,
        newEpoch: 0,
        now: now
      )
    }

    let reloaded = try await dbQueue.read { db in
      try MLSConversationModel.fetchOne(
        db,
        sql: "SELECT * FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
        arguments: [convoId, userDid]
      )
    }

    let row = try XCTUnwrap(reloaded)
    XCTAssertEqual(row.groupID, newGroup, "groupID should swap to the new bytes")
    XCTAssertEqual(row.epoch, 0, "epoch should reset to the freshly landed value")
    XCTAssertEqual(row.joinEpoch, 0, "joinEpoch should track the new join point")
    XCTAssertFalse(row.needsReset, "needsReset must clear on success")
    XCTAssertFalse(row.needsRejoin, "needsRejoin must clear on success")
    XCTAssertFalse(row.isUnrecoverable, "isUnrecoverable must clear on success")
    XCTAssertNil(row.pendingNewGroupId, "pendingNewGroupId must be nulled")
    XCTAssertNil(row.pendingResetGeneration, "pendingResetGeneration must be nulled")
    XCTAssertEqual(row.consecutiveFailures, 0, "consecutiveFailures must reset")
    XCTAssertEqual(row.lastRecoveryAttempt, now)
    XCTAssertEqual(row.updatedAt, now)
    XCTAssertEqual(row.persistedRecoveryState, .healthy, "row should be HEALTHY after success")
  }

  // MARK: - clearPendingReset

  func testClearPendingResetLeavesNeedsResetIntact() async throws {
    let convoId = "convo-recipient-2"
    let userDid = "did:plc:recipient"
    let staleGroup = Data(repeating: 0xCC, count: 32)

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: staleGroup,
        epoch: 100,
        needsReset: true,
        pendingNewGroupId: "deadbeef",
        pendingResetGeneration: 2,
        consecutiveFailures: 1
      )
    }

    let now = Date(timeIntervalSince1970: 1_700_000_500)
    try await dbQueue.write { db in
      try MLSConversationResetSQL.clearPendingReset(
        db: db,
        conversationID: convoId,
        currentUserDID: userDid,
        now: now
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

    XCTAssertTrue(row.needsReset, "clearPendingReset must leave needsReset set")
    XCTAssertNil(row.pendingNewGroupId, "pendingNewGroupId should be nulled")
    XCTAssertNil(row.pendingResetGeneration, "pendingResetGeneration should be nulled")
    XCTAssertEqual(row.groupID, staleGroup, "groupID must not change on clearPendingReset")
    XCTAssertEqual(row.epoch, 100, "epoch must not change on clearPendingReset")
    XCTAssertEqual(row.persistedRecoveryState, .resetPending)
  }

  // MARK: - loadPendingResetGeneration

  func testLoadPendingResetGenerationReturnsStoredValue() async throws {
    let convoId = "convo-recipient-3"
    let userDid = "did:plc:recipient"

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: Data([0x01, 0x02, 0x03]),
        epoch: 5,
        needsReset: true,
        pendingNewGroupId: "abcd",
        pendingResetGeneration: 7
      )
    }

    let stored = try await dbQueue.read { db in
      try MLSConversationResetSQL.loadPendingResetGeneration(
        db: db, conversationID: convoId, currentUserDID: userDid
      )
    }
    XCTAssertEqual(stored, 7)

    let missing = try await dbQueue.read { db in
      try MLSConversationResetSQL.loadPendingResetGeneration(
        db: db, conversationID: "missing", currentUserDID: userDid
      )
    }
    XCTAssertNil(missing, "missing rows should return nil, not throw")
  }

  // MARK: - Helpers

  /// Build a MLSConversationModel table schema matching the post-v28 state.
  /// Kept in-test so we don't depend on the production MLSGRDBManager path
  /// (which uses SQLCipher + Keychain and requires an iOS device / simulator).
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
