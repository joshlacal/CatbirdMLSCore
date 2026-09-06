import Foundation
import GRDB
import XCTest
@testable import CatbirdMLS
@testable import CatbirdMLSCore

final class MLSAccountExitStorageProjectionTests: XCTestCase {
  private let account = "did:plc:receiver"
  private let cid = "550e8400-e29b-41d4-a716-446655440000"
  private let oldGroup = String(repeating: "11", count: 32)
  private let targetGroup = String(repeating: "22", count: 32)
  private var directory: URL!
  private var pool: DatabasePool!
  private var context: MlsContext!
  private var adapter: MLSOrchestratorStorageAdapter!

  override func setUp() async throws {
    try await super.setUp()
    directory = FileManager.default.temporaryDirectory.appendingPathComponent("MLSAccountExit-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    pool = try DatabasePool(path: directory.appendingPathComponent("messages.sqlite").path)
    try MLSGRDBManager.makeMigrator().migrate(pool)
    context = try MlsContext(storagePath: directory.appendingPathComponent("openmls.sqlite").path,
      encryptionKey: String(repeating: "cd", count: 32), keychain: InMemoryKeychainAccess())
    try context.setContentRootKey(key: Data(repeating: 0x51, count: 32))
    adapter = MLSOrchestratorStorageAdapter(dbPool: pool, userDID: account, mlsContext: context)
  }

  override func tearDown() async throws {
    adapter = nil
    if let context { context.clearContentRootKey(); try? context.flushAndPrepareClose() }
    context = nil
    pool = nil
    if let directory { try? FileManager.default.removeItem(at: directory) }
    directory = nil
    try await super.tearDown()
  }

  private func seed(resetGeneration: Int64? = 4) throws {
    try pool.write { db in
      try MLSConversationModel(conversationID: cid, currentUserDID: account,
        groupID: Data(hexEncoded: oldGroup)!, epoch: 3, title: "Saved title").insert(db)
      if let resetGeneration {
        try db.execute(sql: "UPDATE MLSConversationModel SET needsReset = 1, needsRejoin = 1, isUnrecoverable = 1, pendingNewGroupId = ?, pendingResetGeneration = ?, rejoinRequestedAt = ? WHERE conversationID = ? AND currentUserDID = ?",
          arguments: [targetGroup, resetGeneration, Date(), cid, account])
      }
      try MLSEpochKeyModel(epochKeyID: "saved-epoch", conversationID: cid,
        currentUserDID: account, epoch: 3, keyMaterial: Data(repeating: 0x7a, count: 32)).insert(db)
    }
    let payload = MLSMessagePayload.text("Saved encrypted history", embed: nil)
    try adapter.storeMessage(message: FfiMessage(
      id: "650e8400-e29b-41d4-a716-446655440000", conversationId: cid, senderDid: account,
      text: "Saved encrypted history", timestamp: "2026-09-05T01:02:03.000Z", epoch: 3,
      sequenceNumber: 7, isOwn: true, deliveryStatus: nil,
      payloadJson: String(decoding: try payload.encodeToJSON(), as: UTF8.self)))
    // A historical alias parent must remain byte-for-byte unchanged at this boundary.
    try pool.write { db in
      try MLSConversationModel(conversationID: oldGroup, currentUserDID: account,
        groupID: Data(hexEncoded: oldGroup)!, epoch: 3, title: "Saved alias").insert(db)
    }
  }

  private func model() throws -> MLSConversationModel {
    try pool.read { db in
      try XCTUnwrap(MLSConversationModel.filter(MLSConversationModel.Columns.conversationID == cid)
        .filter(MLSConversationModel.Columns.currentUserDID == account).fetchOne(db))
    }
  }

  private func retainedBytes() throws -> [Data] {
    try pool.read { db in
      let encoder = JSONEncoder()
      encoder.outputFormatting = [.sortedKeys]
      return [
        try encoder.encode(MLSMessageModel.order(MLSMessageModel.Columns.messageID).fetchAll(db)),
        try encoder.encode(MLSEpochKeyModel.fetchAll(db)),
        try encoder.encode(MLSConversationModel.filter(MLSConversationModel.Columns.conversationID == oldGroup).fetchAll(db))
      ]
    }
  }

  private func rejectProjectionWrites() throws {
    try pool.write { db in
      for table in ["MLSConversationModel", "mls_orchestrator_terminal_access"] {
        for operation in ["INSERT", "UPDATE", "DELETE"] {
          try db.execute(sql: "CREATE TRIGGER no_retry_\(table)_\(operation) BEFORE \(operation) ON \(table) BEGIN SELECT RAISE(ABORT, 'retry must not write'); END")
        }
      }
    }
  }

  func testVerifiedTerminalCallbackRetiresConsentAndRetainedTerminalRetryClearsNewLeaveConsent() throws {
    try seed(resetGeneration: nil)
    let before = try retainedBytes()
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET requestState = 'pendingInbound' WHERE conversationID = ? AND currentUserDID = ?", arguments: [cid, account])
      // The callback only retires these UI fields; JSON is retained byte-for-byte.
      try db.execute(sql: "INSERT INTO mls_orchestrator_canonical_policy VALUES (?, ?, 0, 1, 'saved policy bytes', 1)", arguments: [account, cid])
    }
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 3, terminalState: "device_removed"))
    XCTAssertEqual(try model().requestState, .none)
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET requestState = 'pendingInbound' WHERE conversationID = ? AND currentUserDID = ?", arguments: [cid, account])
      try db.execute(sql: "UPDATE mls_orchestrator_canonical_policy SET terminal_invitation_authorized = 1")
    }
    // A newly accepted own account exit can have the same crypto tuple. Native
    // verifies the new intent before this specialized callback; retire its UI.
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 3, terminalState: "device_removed"))
    XCTAssertEqual(try model().requestState, .none)
    let policy = try pool.read { db in
      try XCTUnwrap(Row.fetchOne(db, sql: "SELECT canonical_state_json, terminal_invitation_authorized FROM mls_orchestrator_canonical_policy"))
    }
    XCTAssertEqual(policy["canonical_state_json"] as String, "saved policy bytes")
    XCTAssertFalse(policy["terminal_invitation_authorized"] as Bool)
    XCTAssertEqual(try retainedBytes(), before)
    try rejectProjectionWrites()
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 3, terminalState: "device_removed"))
  }

  func testMatchingResetExitIsAtomicTerminalAndPreservesEncryptedHistoryKeysAndAlias() throws {
    try seed()
    let before = try retainedBytes()
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 4, terminalEpoch: 0, terminalState: "device_removed"))
    let saved = try model()
    XCTAssertEqual(saved.groupID, Data(hexEncoded: targetGroup))
    XCTAssertEqual(saved.epoch, 0)
    XCTAssertFalse(saved.needsReset || saved.needsRejoin || saved.isUnrecoverable)
    XCTAssertNil(saved.pendingNewGroupId)
    XCTAssertNil(saved.rejoinRequestedAt)
    XCTAssertEqual(saved.pendingResetGeneration, 4)
    XCTAssertEqual(saved.title, "Saved title")
    XCTAssertEqual(try adapter.getConversationState(conversationId: cid)?.state, "device_removed")
    XCTAssertEqual(try retainedBytes(), before)
    let messages = try adapter.getMessages(conversationId: cid, limit: 10, beforeSequence: nil)
    XCTAssertEqual(messages.first?.text, "Saved encrypted history")
    let encrypted = try pool.read { try MLSMessageModel.fetchOne($0) }
    XCTAssertNotNil(encrypted?.payloadEncrypted)
    XCTAssertNotNil(encrypted?.entryHMAC)
  }

  func testSameTerminalRetrySurvivesDatabaseReopenAndRetainsGenerationHighWater() throws {
    try seed()
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 4, terminalEpoch: 1, terminalState: "closed"))
    let before = try model()
    adapter = nil
    try pool.close()
    pool = try DatabasePool(path: directory.appendingPathComponent("messages.sqlite").path)
    adapter = MLSOrchestratorStorageAdapter(dbPool: pool, userDID: account, mlsContext: context)
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 4, terminalEpoch: 1, terminalState: "closed"))
    XCTAssertEqual(try model(), before)
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 3, terminalEpoch: 1, terminalState: "closed"))
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 4, terminalEpoch: 2, terminalState: "closed"))
    XCTAssertEqual(try model(), before)
  }

  func testNewerOrDifferentPendingResetRejectsWithoutTouchingAnySavedRows() throws {
    try seed(resetGeneration: 5)
    let before = try model()
    let retained = try retainedBytes()
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 4, terminalEpoch: 1, terminalState: "closed"))
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: 5, terminalEpoch: 4, terminalState: "closed"))
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 4, terminalState: "closed"))
    XCTAssertEqual(try model(), before)
    XCTAssertEqual(try retainedBytes(), retained)
    XCTAssertEqual(try adapter.getConversationState(conversationId: cid)?.state, "reset_pending")
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET groupID = ?, epoch = 20 WHERE conversationID = ? AND currentUserDID = ?", arguments: [Data(hexEncoded: targetGroup)!, cid, account])
    }
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 5, terminalEpoch: 1, terminalState: "closed"))
    XCTAssertEqual(try model().epoch, 20)
  }

  func testNoResetExitAndRetryAllowHistoricalHighWaterButRequireExactCurrentGroup() throws {
    try seed(resetGeneration: nil)
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET pendingResetGeneration = 7 WHERE conversationID = ? AND currentUserDID = ?", arguments: [cid, account])
    }
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: nil, terminalEpoch: 4, terminalState: "closed"))
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 2, terminalState: "closed"))
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 4, terminalState: "closed"))
    let saved = try model()
    let retained = try retainedBytes()
    XCTAssertEqual(saved.pendingResetGeneration, 7)
    try rejectProjectionWrites()
    for epoch: UInt64 in [3, 5] {
      XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
        expectedResetGeneration: nil, terminalEpoch: epoch, terminalState: "closed"))
    }
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 4, terminalState: "closed"))
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 4, terminalState: "device_removed"))
    XCTAssertEqual(try model(), saved)
    XCTAssertEqual(try retainedBytes(), retained)
    XCTAssertEqual(try adapter.getConversationState(conversationId: cid)?.state, "closed")
  }

  func testAcceptedNewExitMayAdvanceDeviceRemovedButClosedRemainsExactAndHistoryIsPreserved() throws {
    try seed(resetGeneration: nil)
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 4, terminalState: "device_removed"))
    let saved = try model()
    let retained = try retainedBytes()
    for state in ["device_removed", "closed"] {
      XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
        expectedResetGeneration: nil, terminalEpoch: 3, terminalState: state))
    }
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: nil, terminalEpoch: 5, terminalState: "closed"))
    XCTAssertEqual(try model(), saved)
    // These calls represent distinct accepted own requests validated by Rust.
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 5, terminalState: "device_removed"))
    XCTAssertEqual(try model().epoch, 5)
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 6, terminalState: "closed"))
    let closed = try model()
    XCTAssertEqual(closed.epoch, 6)
    try rejectProjectionWrites()
    for epoch: UInt64 in [5, 7] {
      XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
        expectedResetGeneration: nil, terminalEpoch: epoch, terminalState: "closed"))
    }
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 6, terminalState: "device_removed"))
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 6, terminalState: "closed"))
    XCTAssertEqual(try model(), closed)
    XCTAssertEqual(try retainedBytes(), retained)
    XCTAssertEqual(try adapter.getConversationState(conversationId: cid)?.state, "closed")
  }

  func testAcceptedNewExitMayRetireExactResetOverDeviceRemoved() throws {
    try seed(resetGeneration: nil)
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 4, terminalState: "device_removed"))
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET needsReset = 1, pendingNewGroupId = ?, pendingResetGeneration = 8 WHERE conversationID = ? AND currentUserDID = ?", arguments: [targetGroup, cid, account])
    }
    let retained = try retainedBytes()
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 7, terminalEpoch: 0, terminalState: "closed"))
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 8, terminalEpoch: 0, terminalState: "closed"))
    XCTAssertEqual(try model().groupID, Data(hexEncoded: targetGroup))
    XCTAssertEqual(try model().pendingResetGeneration, 8)
    XCTAssertEqual(try model().epoch, 0)
    XCTAssertEqual(try retainedBytes(), retained)
  }

  func testClosedCannotRetireAnotherLiveResetEvenWithAnExactTuple() throws {
    try seed()
    XCTAssertTrue(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 4, terminalEpoch: 1, terminalState: "closed"))
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET needsReset = 1, pendingNewGroupId = ? WHERE conversationID = ? AND currentUserDID = ?", arguments: [targetGroup, cid, account])
    }
    let saved = try model()
    try rejectProjectionWrites()
    for generation: Int32? in [nil, 4] {
      XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
        expectedResetGeneration: generation, terminalEpoch: 1, terminalState: "closed"))
    }
    XCTAssertEqual(try model(), saved)
  }

  func testMalformedProofAndCrossAccountOrAliasCannotMutateStorage() throws {
    try seed()
    let before = try model()
    for group in ["", "deadbeef", String(repeating: "AB", count: 32), String(repeating: "z", count: 64)] {
      XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: group,
        expectedResetGeneration: 4, terminalEpoch: 1, terminalState: "closed"))
    }
    for state in ["active", "Closed", "left", ""] {
      XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
        expectedResetGeneration: 4, terminalEpoch: 1, terminalState: state))
    }
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 0, terminalEpoch: 1, terminalState: "closed"))
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 4, terminalEpoch: UInt64.max, terminalState: "closed"))
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: oldGroup, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 4, terminalEpoch: 1, terminalState: "closed"))
    let other = MLSOrchestratorStorageAdapter(dbPool: pool, userDID: "did:plc:other", mlsContext: context)
    XCTAssertFalse(try other.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 4, terminalEpoch: 1, terminalState: "closed"))
    XCTAssertEqual(try model(), before)
  }

  func testPartialResetPayloadCannotBeBypassedByNoResetExit() throws {
    try seed(resetGeneration: nil)
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET needsReset = 1 WHERE conversationID = ? AND currentUserDID = ?", arguments: [cid, account])
    }
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 4, terminalState: "closed"))
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET needsReset = 0, pendingNewGroupId = ? WHERE conversationID = ? AND currentUserDID = ?", arguments: [targetGroup, cid, account])
    }
    XCTAssertFalse(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: oldGroup,
      expectedResetGeneration: nil, terminalEpoch: 4, terminalState: "closed"))
  }

  func testProjectionWriteFailureRollsBackTerminalInsertionAndResetRetirement() throws {
    try seed()
    let before = try model()
    try pool.write { db in
      try db.execute(sql: "CREATE TRIGGER fail_account_exit BEFORE UPDATE ON MLSConversationModel BEGIN SELECT RAISE(ABORT, 'fixture write failure'); END")
    }
    XCTAssertThrowsError(try adapter.completeAccountExit(conversationId: cid, expectedGroupIdHex: targetGroup,
      expectedResetGeneration: 4, terminalEpoch: 1, terminalState: "closed"))
    XCTAssertEqual(try model(), before)
    XCTAssertEqual(try adapter.getConversationState(conversationId: cid)?.state, "reset_pending")
    XCTAssertEqual(try pool.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM mls_orchestrator_terminal_access") }, 0)
  }
}
