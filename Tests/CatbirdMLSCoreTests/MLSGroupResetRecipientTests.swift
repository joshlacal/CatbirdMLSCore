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
      // CLIENT M (Task #75): pass `appliedGeneration: nil` to verify the
      // legacy "null on success" behavior is preserved when no generation
      // is supplied. Real callers in MLSConversationManager+Sync pass the
      // observed generation through (see seeds-pendingResetGeneration test).
      try MLSConversationResetSQL.applyRecipientResetSuccess(
        db: db,
        conversationID: convoId,
        currentUserDID: userDid,
        newGroupID: newGroup,
        newEpoch: 0,
        appliedGeneration: nil,
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
    XCTAssertNil(
      row.pendingResetGeneration,
      "pendingResetGeneration must be nulled when appliedGeneration is nil (back-compat)"
    )
    XCTAssertEqual(row.consecutiveFailures, 0, "consecutiveFailures must reset")
    XCTAssertEqual(row.lastRecoveryAttempt, now)
    XCTAssertEqual(row.updatedAt, now)
    XCTAssertEqual(row.persistedRecoveryState, .healthy, "row should be HEALTHY after success")
  }

  /// CLIENT M (Task #75): when `appliedGeneration` is supplied, the helper
  /// must persist it instead of nulling. This is the seed value used by the
  /// pre-delete stale-replay guards in `handleGroupReset` /
  /// `handleResetRequested`. A nil seed makes those guards vacuous and
  /// allows historical SSE/WS replays to call `deleteGroup` on the freshly
  /// bootstrapped group (the production bug observed at b947c701a32943d0).
  func testApplyRecipientResetSuccessSeedsPendingGenerationWhenSupplied() async throws {
    let convoId = "convo-recipient-seed"
    let userDid = "did:plc:seed"
    let staleGroup = Data(repeating: 0xAA, count: 32)
    let newGroup = Data(repeating: 0xCD, count: 32)

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: staleGroup,
        epoch: 1,
        needsReset: true,
        pendingNewGroupId: newGroup.hexEncodedString(),
        pendingResetGeneration: 25,
        consecutiveFailures: 0
      )
    }

    let now = Date(timeIntervalSince1970: 1_700_000_000)
    try await dbQueue.write { db in
      try MLSConversationResetSQL.applyRecipientResetSuccess(
        db: db,
        conversationID: convoId,
        currentUserDID: userDid,
        newGroupID: newGroup,
        newEpoch: 1,
        appliedGeneration: 25,
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
    XCTAssertEqual(row.groupID, newGroup, "groupID should swap to the bootstrapped bytes")
    XCTAssertEqual(row.epoch, 1, "epoch should track post-bootstrap value")
    XCTAssertFalse(row.needsReset, "needsReset must clear on success")
    XCTAssertFalse(row.needsRejoin, "needsRejoin must clear on success")
    XCTAssertNil(row.pendingNewGroupId, "pendingNewGroupId must be nulled (bootstrap landed)")
    XCTAssertEqual(
      row.pendingResetGeneration, 25,
      "appliedGeneration must seed pendingResetGeneration so subsequent stale replays at gen<=25 are rejected before deleteGroup runs"
    )
    XCTAssertEqual(row.persistedRecoveryState, .healthy)

    // Replay-defense check: simulate the next stale event arriving with the
    // same generation. The guard inside handleGroupReset / handleResetRequested
    // reads `loadPendingResetGeneration` and compares `stored >= incoming`.
    // With the seed, gen=25 (replayed) is rejected, NOT propagated to deleteGroup.
    let storedAfterSeed = try await dbQueue.read { db in
      try MLSConversationResetSQL.loadPendingResetGeneration(
        db: db, conversationID: convoId, currentUserDID: userDid
      )
    }
    XCTAssertEqual(
      storedAfterSeed, 25,
      "loadPendingResetGeneration must return the seeded value (the live read used by the pre-delete stale guard)"
    )
    XCTAssertTrue(
      (storedAfterSeed ?? Int64.min) >= Int64(25),
      "stored >= incoming(25) → pre-delete stale guard short-circuits the replayed event"
    )
  }

  func testApplyRecipientResetSuccessPreservesDisplayMetadata() async throws {
    let convoId = "convo-recipient-metadata"
    let userDid = "did:plc:metadata"
    let staleGroup = Data(repeating: 0xA1, count: 32)
    let newGroup = Data(repeating: 0xB2, count: 32)
    let avatarData = Data([0x01, 0x02, 0x03, 0x04])

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: staleGroup,
        epoch: 4,
        needsReset: true,
        pendingNewGroupId: newGroup.hexEncodedString(),
        pendingResetGeneration: 11,
        title: "Encrypted Group Title",
        avatarImageData: avatarData
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
        appliedGeneration: 11,
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

    XCTAssertEqual(row.groupID, newGroup)
    XCTAssertEqual(row.title, "Encrypted Group Title")
    XCTAssertEqual(row.avatarImageData, avatarData)
  }

  // MARK: - Metadata identity helpers

  func testMetadataUpdateResolvesStableConversationByRotatedGroupId() async throws {
    let convoId = "stable-conversation-id"
    let userDid = "did:plc:metadata"
    let rotatedGroup = Data(repeating: 0xC3, count: 32)
    let avatarData = Data([0x09, 0x08, 0x07])

    try await dbQueue.write { db in
      try Self.insertConversation(
        in: db,
        conversationID: convoId,
        currentUserDID: userDid,
        groupID: rotatedGroup,
        epoch: 0,
        needsReset: false,
        pendingNewGroupId: nil,
        pendingResetGeneration: nil,
        title: "Old Title"
      )
    }

    let updatedConversationID = try await dbQueue.write { db in
      try MLSConversationMetadataSQL.updateDecryptedMetadata(
        db: db,
        currentUserDID: userDid,
        groupIdHex: rotatedGroup.hexEncodedString(),
        title: "Restored Group Title",
        avatarImageData: avatarData,
        now: Date(timeIntervalSince1970: 1_700_000_100)
      )
    }

    XCTAssertEqual(updatedConversationID, convoId)

    let row = try await dbQueue.read { db in
      try XCTUnwrap(
        try MLSConversationModel.fetchOne(
          db,
          sql: "SELECT * FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
          arguments: [convoId, userDid]
        )
      )
    }

    XCTAssertEqual(row.title, "Restored Group Title")
    XCTAssertEqual(row.avatarImageData, avatarData)
    XCTAssertFalse(row.isPlaceholder)
  }

  func testMergedTitlePreservesExistingWhenRotatedMetadataUnavailable() {
    XCTAssertEqual(
      MLSConversationMetadataSQL.mergedTitle(
        encryptedTitle: nil,
        serverTitle: nil,
        existingTitle: "Existing Group Title"
      ),
      "Existing Group Title"
    )

    XCTAssertEqual(
      MLSConversationMetadataSQL.mergedTitle(
        encryptedTitle: "",
        serverTitle: "Server Group Title",
        existingTitle: "Existing Group Title"
      ),
      "Server Group Title"
    )
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
    consecutiveFailures: Int = 0,
    title: String? = nil,
    avatarImageData: Data? = nil
  ) throws {
    let now = Date(timeIntervalSince1970: 1_699_000_000)
    try db.execute(
      sql: """
            INSERT INTO MLSConversationModel (
              conversationID, currentUserDID, groupID, epoch,
              joinMethod, joinEpoch, title, avatarImageData, createdAt, updatedAt,
              unacknowledgedMemberChanges, isActive,
              needsRejoin, needsReset, isUnrecoverable,
              consecutiveFailures, isPlaceholder, requestState,
              pendingNewGroupId, pendingResetGeneration
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
      arguments: [
        conversationID, currentUserDID, groupID, epoch,
        "externalCommit", 0, title, avatarImageData, now, now,
        0, true,
        false, needsReset, false,
        consecutiveFailures, false, "none",
        pendingNewGroupId, pendingResetGeneration,
      ]
    )
  }
}
