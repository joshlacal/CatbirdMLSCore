import XCTest
import GRDB
@testable import CatbirdMLS
@testable import CatbirdMLSCore

final class MLSOrchestratorStorageAdapterTests: XCTestCase {
  private let stableConversationID = "550e8400-e29b-41d4-a716-446655440000"
  private var tempDir: URL!
  private var dbPool: DatabasePool!
  private var context: MlsContext!

  override func setUp() async throws {
    try await super.setUp()

    tempDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSOrchestratorStorageAdapterTests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)

    dbPool = try DatabasePool(path: tempDir.appendingPathComponent("messages.sqlite").path)
    try MLSGRDBManager.makeMigrator().migrate(dbPool)

    context = try MlsContext(
      storagePath: tempDir.appendingPathComponent("openmls.sqlite").path,
      encryptionKey: String(repeating: "cd", count: 32),
      keychain: InMemoryKeychainAccess()
    )
    try context.setContentRootKey(key: Data(repeating: 0x51, count: 32))
  }

  override func tearDown() async throws {
    if let context {
      context.clearContentRootKey()
      try? context.flushAndPrepareClose()
    }
    context = nil
    dbPool = nil
    if let tempDir {
      try? FileManager.default.removeItem(at: tempDir)
    }
    tempDir = nil

    try await super.tearDown()
  }

  func testMissingCanonicalReadsAllowAdoptionWithoutCreatingOrphanRows() throws {
    let did = "did:plc:receiver"
    let adapter = MLSOrchestratorStorageAdapter(dbPool: dbPool, userDID: did, mlsContext: context)
    XCTAssertNil(try adapter.getConversation(userDid: did, conversationId: stableConversationID))
    XCTAssertNil(try adapter.getConversationState(conversationId: stableConversationID))
    XCTAssertFalse(try adapter.needsRejoin(conversationId: stableConversationID))
    XCTAssertTrue(try adapter.getMessages(conversationId: stableConversationID, limit: 10, beforeSequence: nil).isEmpty)
    XCTAssertTrue(try adapter.getSequencerReceipts(conversationId: stableConversationID, sinceEpoch: nil).isEmpty)
    XCTAssertThrowsError(try adapter.getConversationState(conversationId: "invalid-conversation"))
    XCTAssertThrowsError(try adapter.setConversationState(conversationId: stableConversationID, state: "active"))
    XCTAssertThrowsError(try adapter.storePendingMessage(conversationId: stableConversationID, messageId: "must-not-insert"))
    try dbPool.read { db in
      XCTAssertEqual(try MLSConversationModel.fetchCount(db), 0)
      XCTAssertEqual(try MLSMessageModel.fetchCount(db), 0)
      XCTAssertEqual(try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM mls_orchestrator_pending_messages"), 0)
    }
    try adapter.ensureConversationExists(userDid: did, conversationId: stableConversationID, groupId: "deadbeef")
    XCTAssertNotNil(try adapter.getConversation(userDid: did, conversationId: stableConversationID))
  }

  func testRemovedStateSurvivesReopenAndServerEpochAdvanceUntilValidatedWelcome() throws {
    let did = "did:plc:receiver"
    try insertCanonicalConversation(userDID: did)
    var adapter: MLSOrchestratorStorageAdapter? = MLSOrchestratorStorageAdapter(dbPool: dbPool, userDID: did, mlsContext: context)
    try adapter?.setConversationState(conversationId: stableConversationID, state: "device_removed")
    adapter = nil
    try dbPool.close()
    dbPool = try DatabasePool(path: tempDir.appendingPathComponent("messages.sqlite").path)
    adapter = MLSOrchestratorStorageAdapter(dbPool: dbPool, userDID: did, mlsContext: context)
    XCTAssertEqual(try adapter?.getConversationState(conversationId: stableConversationID)?.state, "device_removed")
    try dbPool.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET epoch = 99, groupID = ? WHERE conversationID = ?",
        arguments: [Data(hexEncoded: "cafebabe")!, stableConversationID])
    }
    try adapter?.setConversationState(conversationId: stableConversationID, state: "active")
    try adapter?.setConversationState(conversationId: stableConversationID, state: "needs_rejoin")
    XCTAssertEqual(try adapter?.getConversationState(conversationId: stableConversationID)?.state, "device_removed")
    try adapter?.setConversationState(conversationId: stableConversationID, state: "active_after_welcome")
    XCTAssertEqual(try adapter?.getConversationState(conversationId: stableConversationID)?.state, "active")
  }

  func testRemovedMarkerIsScopedToTheOwningAccount() throws {
    let owner = "did:plc:receiver"
    let other = "did:plc:other"
    try insertCanonicalConversation(userDID: owner)
    let ownerAdapter = MLSOrchestratorStorageAdapter(dbPool: dbPool, userDID: owner, mlsContext: context)
    let otherAdapter = MLSOrchestratorStorageAdapter(dbPool: dbPool, userDID: other, mlsContext: context)
    try ownerAdapter.setConversationState(conversationId: stableConversationID, state: "device_removed")
    XCTAssertNil(try otherAdapter.getConversationState(conversationId: stableConversationID))
    XCTAssertNil(try otherAdapter.getConversation(userDid: other, conversationId: stableConversationID))
    XCTAssertThrowsError(try otherAdapter.getConversation(userDid: owner, conversationId: stableConversationID))
    XCTAssertThrowsError(try otherAdapter.setConversationState(conversationId: stableConversationID, state: "active_after_welcome"))
    XCTAssertEqual(try ownerAdapter.getConversationState(conversationId: stableConversationID)?.state, "device_removed")
  }

  func testClosedStateRetainsHistoryAndCannotBeReactivatedByWelcome() throws {
    let did = "did:plc:receiver"
    try insertCanonicalConversation(userDID: did)
    let adapter = MLSOrchestratorStorageAdapter(dbPool: dbPool, userDID: did, mlsContext: context)
    try adapter.setConversationState(conversationId: stableConversationID, state: "closed")
    try adapter.setConversationState(conversationId: stableConversationID, state: "active_after_welcome")
    XCTAssertEqual(try adapter.getConversationState(conversationId: stableConversationID)?.state, "closed")
    let model = try dbPool.read { try MLSConversationModel.fetchOne($0) }
    XCTAssertNotNil(model)
    XCTAssertTrue(model?.isActive == true)
  }

  func testEncryptedCallbackMessagesRoundTripAfterDatabaseAndContextReopen() throws {
    let did = "did:plc:receiver"
    try insertCanonicalConversation(userDID: did)
    var adapter: MLSOrchestratorStorageAdapter? = MLSOrchestratorStorageAdapter(dbPool: dbPool, userDID: did, mlsContext: context)
    let payload = MLSMessagePayload.text("restored encrypted history", embed: nil)
    let expected = FfiMessage(id: "reload-message", conversationId: stableConversationID,
      senderDid: "did:plc:sender", text: "restored encrypted history", timestamp: "2026-06-22T12:00:00Z",
      epoch: 3, sequenceNumber: 9, isOwn: false, deliveryStatus: nil,
      payloadJson: String(decoding: try payload.encodeToJSON(), as: UTF8.self))
    try adapter?.storeMessage(message: expected)
    let immediate = try XCTUnwrap(adapter?.getMessages(conversationId: stableConversationID, limit: 10, beforeSequence: nil).first)
    XCTAssertEqual(immediate.text, expected.text)
    XCTAssertNotNil(immediate.payloadJson)

    adapter = nil
    context.clearContentRootKey()
    try context.flushAndPrepareClose()
    context = nil
    try dbPool.close()
    dbPool = try DatabasePool(path: tempDir.appendingPathComponent("messages.sqlite").path)
    context = try MlsContext(storagePath: tempDir.appendingPathComponent("openmls.sqlite").path,
      encryptionKey: String(repeating: "cd", count: 32), keychain: InMemoryKeychainAccess())
    try context.setContentRootKey(key: Data(repeating: 0x51, count: 32))
    adapter = MLSOrchestratorStorageAdapter(dbPool: dbPool, userDID: did, mlsContext: context)
    let restored = try XCTUnwrap(adapter?.getMessages(conversationId: stableConversationID, limit: 10, beforeSequence: nil).first)
    XCTAssertEqual(restored.text, expected.text)
    XCTAssertEqual(restored.sequenceNumber, expected.sequenceNumber)
    let restoredPayload = try MLSMessagePayload.decodeFromJSON(Data(XCTUnwrap(restored.payloadJson).utf8))
    XCTAssertEqual(restoredPayload.text, expected.text)

    context.clearContentRootKey()
    XCTAssertThrowsError(try adapter?.getMessages(conversationId: stableConversationID, limit: 10, beforeSequence: nil),
      "A missing content key must fail instead of returning a blank message")
    try context.setContentRootKey(key: Data(repeating: 0x51, count: 32))
    try dbPool.write { db in
      try db.execute(sql: "UPDATE MLSMessageModel SET payloadEncrypted = ? WHERE messageID = ?",
        arguments: [Data([0]), expected.id])
    }
    XCTAssertThrowsError(try adapter?.getMessages(conversationId: stableConversationID, limit: 10, beforeSequence: nil),
      "Corrupt encrypted payloads must not fall back to plaintext columns")
  }

  func testStoreMessageUsesFieldEncryptedPayloadColumns() throws {
    let userDID = "did:plc:receiver"
    let payload = MLSMessagePayload.text("stored by rust", embed: nil)
    try insertCanonicalConversation(userDID: userDID)
    let adapter = MLSOrchestratorStorageAdapter(
      dbPool: dbPool,
      userDID: userDID,
      mlsContext: context
    )

    try adapter.storeMessage(
      message: FfiMessage(
        id: "server-msg-1",
        conversationId: stableConversationID,
        senderDid: "did:plc:sender",
        text: "stored by rust",
        timestamp: "2026-06-22T12:00:00Z",
        epoch: 3,
        sequenceNumber: 7,
        isOwn: false,
        deliveryStatus: nil,
        payloadJson: String(data: try payload.encodeToJSON(), encoding: .utf8)
      )
    )

    let row = try XCTUnwrap(dbPool.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "server-msg-1")
        .filter(MLSMessageModel.Columns.currentUserDID == MLSStorageHelpers.normalizeDID(userDID))
        .fetchOne(db)
    })

    XCTAssertNil(row.payloadJSON)
    XCTAssertNotNil(row.payloadEncrypted)
    XCTAssertNotNil(row.entryHMAC)
    XCTAssertEqual(row.payloadKeyVersion, 1)
    XCTAssertEqual(row.decryptedPayload(context: context)?.text, "stored by rust")
  }

  func testStoreMessagePersistsControlPayloadWithEmptyDisplayText() throws {
    let userDID = "did:plc:receiver"
    let payload = MLSMessagePayload.reaction(
      messageId: "parent-msg-1",
      emoji: "+1",
      action: .add
    )
    try insertCanonicalConversation(userDID: userDID)
    let adapter = MLSOrchestratorStorageAdapter(
      dbPool: dbPool,
      userDID: userDID,
      mlsContext: context
    )

    try adapter.storeMessage(
      message: FfiMessage(
        id: "reaction-msg-1",
        conversationId: stableConversationID,
        senderDid: "did:plc:sender",
        text: "",
        timestamp: "2026-06-22T12:00:00Z",
        epoch: 3,
        sequenceNumber: 8,
        isOwn: false,
        deliveryStatus: nil,
        payloadJson: String(data: try payload.encodeToJSON(), encoding: .utf8)
      )
    )

    let row = try XCTUnwrap(dbPool.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "reaction-msg-1")
        .filter(MLSMessageModel.Columns.currentUserDID == MLSStorageHelpers.normalizeDID(userDID))
        .fetchOne(db)
    })
    let decoded = try XCTUnwrap(row.decryptedPayload(context: context))

    XCTAssertNil(row.payloadJSON)
    XCTAssertNotNil(row.payloadEncrypted)
    XCTAssertEqual(decoded.messageType, .reaction)
    XCTAssertEqual(decoded.reaction?.messageId, "parent-msg-1")
    let reloaded = try XCTUnwrap(adapter.getMessages(conversationId: stableConversationID, limit: 10, beforeSequence: nil).first)
    XCTAssertEqual(reloaded.text, "")
    let reloadedPayload = try MLSMessagePayload.decodeFromJSON(Data(XCTUnwrap(reloaded.payloadJson).utf8))
    XCTAssertEqual(reloadedPayload.reaction?.messageId, "parent-msg-1")
  }

  private func insertCanonicalConversation(userDID: String) throws {
    let now = Date(timeIntervalSince1970: 1_700_000_000)
    try dbPool.write { db in
      try MLSConversationModel(
        conversationID: stableConversationID,
        currentUserDID: userDID,
        groupID: Data(hexEncoded: "deadbeef")!,
        epoch: 3,
        createdAt: now,
        updatedAt: now
      ).insert(db)
    }
  }

  func testStoreMessageRoutesExactRawAliasToCanonicalStableConversation() throws {
    let userDID = "did:plc:receiver"
    let stableID = "550e8400-e29b-41d4-a716-446655440000"
    let rawGroupID = "deadbeef"
    let now = Date(timeIntervalSince1970: 1_700_000_000)
    try dbPool.write { db in
      try MLSConversationModel(
        conversationID: stableID,
        currentUserDID: userDID,
        groupID: Data(hexEncoded: rawGroupID)!,
        epoch: 3,
        createdAt: now,
        updatedAt: now
      ).insert(db)
      try MLSConversationModel(
        conversationID: rawGroupID,
        currentUserDID: userDID,
        groupID: Data(hexEncoded: rawGroupID)!,
        epoch: 3,
        createdAt: now,
        updatedAt: now
      ).insert(db)
    }

    let adapter = MLSOrchestratorStorageAdapter(
      dbPool: dbPool,
      userDID: userDID,
      mlsContext: context
    )
    let payload = MLSMessagePayload.text("routed", embed: nil)
    try adapter.storeMessage(
      message: FfiMessage(
        id: "routed-message-1",
        conversationId: rawGroupID,
        senderDid: "did:plc:sender",
        text: "routed",
        timestamp: "2026-06-22T12:00:00Z",
        epoch: 3,
        sequenceNumber: 1,
        isOwn: false,
        deliveryStatus: nil,
        payloadJson: String(data: try payload.encodeToJSON(), encoding: .utf8)
      )
    )

    let rows = try dbPool.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "routed-message-1")
        .fetchAll(db)
    }
    XCTAssertEqual(rows.map(\.conversationID), [stableID])
  }

  func testPendingDeleteHandoffPreservesRawAndStableIntentKeysWithoutConversationRows() throws {
    let userDID = "did:plc:receiver"
    let rawGroupID = "deadbeef"
    let adapter = MLSOrchestratorStorageAdapter(
      dbPool: dbPool,
      userDID: userDID,
      mlsContext: context
    )

    try adapter.markPendingLocalDelete(
      conversationId: rawGroupID,
      groupIdHex: rawGroupID
    )
    try adapter.markPendingLocalDelete(
      conversationId: stableConversationID,
      groupIdHex: rawGroupID
    )

    let afterMark = try adapter.listPendingLocalDeletes().map(\.conversationId)
    XCTAssertEqual(Set(afterMark), [rawGroupID, stableConversationID])

    try adapter.clearPendingLocalDelete(conversationId: rawGroupID)
    let afterClear = try adapter.listPendingLocalDeletes().map(\.conversationId)
    XCTAssertEqual(afterClear, [stableConversationID])

    let rejectedIDs = [
      "DEADBEEF",                             // uppercase hex
      "+1",                                   // decodable but noncanonical hex
      "not-a-conversation-id",                // non-hex
      "550e8400-e29b-11d4-a716-446655440000", // UUIDv1 lookalike, not v4
    ]
    for rejected in rejectedIDs {
      XCTAssertThrowsError(
        try adapter.markPendingLocalDelete(conversationId: rejected, groupIdHex: rawGroupID),
        rejected
      )
    }
  }
}
