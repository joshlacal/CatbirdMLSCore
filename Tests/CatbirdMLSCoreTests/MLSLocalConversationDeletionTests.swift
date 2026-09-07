//
//  MLSLocalConversationDeletionTests.swift
//  CatbirdMLSCoreTests
//
//  Regression tests for local conversation deletion ("Delete for me")
//  and tenant/account isolation boundaries.
//

import Foundation
import GRDB
import XCTest
@testable import CatbirdMLSCore

final class MLSLocalConversationDeletionTests: XCTestCase, @unchecked Sendable {
  private let accountA = "did:plc:account_a_test"
  private let accountB = "did:plc:account_b_test"
  private let convoID = "550e8400-e29b-41d4-a716-446655440000"
  private let groupIDHex = String(repeating: "ab", count: 32)
  private var directory: URL!
  private var pool: DatabasePool!
  private var storage: MLSStorage!
  private var mlsContextA: MlsContext!
  private var mlsContextB: MlsContext!

  override func setUp() async throws {
    try await super.setUp()
    directory = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSDeleteForMeTests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    let dbPath = directory.appendingPathComponent("messages.sqlite").path
    pool = try DatabasePool(path: dbPath)
    try MLSGRDBManager.makeMigrator().migrate(pool)
    storage = MLSStorage.shared

    let openMlsPathA = directory.appendingPathComponent("openmls_a.sqlite").path
    mlsContextA = try MlsContext(
      storagePath: openMlsPathA,
      encryptionKey: String(repeating: "ab", count: 32),
      keychain: InMemoryKeychainAccess()
    )
    try mlsContextA.setContentRootKey(key: Data(repeating: 0x42, count: 32))

    let openMlsPathB = directory.appendingPathComponent("openmls_b.sqlite").path
    mlsContextB = try MlsContext(
      storagePath: openMlsPathB,
      encryptionKey: String(repeating: "cd", count: 32),
      keychain: InMemoryKeychainAccess()
    )
    try mlsContextB.setContentRootKey(key: Data(repeating: 0x43, count: 32))
  }

  override func tearDown() async throws {
    try? mlsContextA?.flushAndPrepareClose()
    try? mlsContextB?.flushAndPrepareClose()
    mlsContextA = nil
    mlsContextB = nil
    storage = nil
    pool = nil
    if let directory {
      try? FileManager.default.removeItem(at: directory)
    }
    directory = nil
    try await super.tearDown()
  }

  private func reopenDatabase() throws {
    let dbPath = directory.appendingPathComponent("messages.sqlite").path
    pool = nil
    pool = try DatabasePool(path: dbPath)
  }

  private func seedConversation(
    userDID: String,
    conversationID: String? = nil,
    groupIDHex: String? = nil,
    lastMessageAt: Date? = Date()
  ) throws {
    let convoIdentity = conversationID ?? self.convoID
    let groupIdentity = groupIDHex ?? self.groupIDHex
    try pool.write { db in
      let convo = MLSConversationModel(
        conversationID: convoIdentity,
        currentUserDID: userDID,
        groupID: Data(hexEncoded: groupIdentity)!,
        epoch: 1,
        title: "Test Group",
        createdAt: Date(),
        updatedAt: Date(),
        lastMessageAt: lastMessageAt,
        isActive: true
      )
      try convo.insert(db)

      let member = MLSMemberModel(
        memberID: "\(convoIdentity):\(userDID)",
        conversationID: convoIdentity,
        currentUserDID: userDID,
        did: userDID,
        handle: "test.bsky.social",
        leafIndex: 0,
        isActive: true,
        role: .member
      )
      try member.insert(db)

      let epochKey = MLSEpochKeyModel(
        epochKeyID: "epoch-1-\(userDID)",
        conversationID: convoIdentity,
        currentUserDID: userDID,
        epoch: 1,
        keyMaterial: Data(repeating: 0x42, count: 32)
      )
      try epochKey.insert(db)
    }
  }

  private func seedMessage(
    userDID: String,
    messageID: String,
    seq: Int64,
    timestamp: Date,
    conversationID: String? = nil
  ) throws {
    let convoIdentity = conversationID ?? self.convoID
    try pool.write { db in
      let msg = MLSMessageModel(
        messageID: messageID,
        currentUserDID: userDID,
        conversationID: convoIdentity,
        cryptoConversationID: convoIdentity,
        senderID: userDID,
        payloadJSON: nil,
        wireFormat: nil,
        contentType: "text/plain",
        timestamp: timestamp,
        epoch: 1,
        sequenceNumber: seq,
        authenticatedData: nil,
        signature: nil,
        isDelivered: true,
        isRead: true,
        isSent: true,
        sendAttempts: 1,
        error: nil,
        processingState: MLSMessageProcessingState.cached,
        gapBefore: false,
        payloadExpired: false,
        processingError: nil,
        processingAttempts: 0,
        validationFailureReason: nil,
        payloadEncrypted: Data("encrypted".utf8),
        entryHMAC: Data(repeating: 0x01, count: 32),
        payloadKeyVersion: 1
      )
      try msg.insert(db)
    }
  }

  private func replayWrite(
    userDID: String,
    messageID: String,
    seq: Int64,
    timestamp: Date,
    text: String = "Test text",
    conversationID: String? = nil
  ) throws {
    let ctx = userDID == accountB ? mlsContextB! : mlsContextA!
    let payload = MLSMessagePayload.text(text, embed: nil)
    try pool.write { db in
      try MLSStorageHelpers.savePayloadSync(
        context: ctx,
        in: db,
        messageID: messageID,
        conversationID: conversationID ?? self.convoID,
        currentUserDID: userDID,
        payload: payload,
        senderID: userDID,
        epoch: 1,
        sequenceNumber: seq,
        timestamp: timestamp
      )
    }
  }

  private func makeAdapter(userDID: String) -> MLSOrchestratorStorageAdapter {
    let ctx = userDID == accountB ? mlsContextB! : mlsContextA!
    return MLSOrchestratorStorageAdapter(dbPool: pool, userDID: userDID, mlsContext: ctx)
  }

  // MARK: - 1. Deletion purges messages and marker is recorded while preserving crypto

  func testDeleteConversationForMeRemovesLocalMessagesAndPreservesCrypto() async throws {
    try seedConversation(userDID: accountA)
    try seedMessage(userDID: accountA, messageID: "msg-1", seq: 1, timestamp: Date().addingTimeInterval(-100))
    try seedMessage(userDID: accountA, messageID: "msg-2", seq: 2, timestamp: Date().addingTimeInterval(-50))

    // Pre-condition
    let preMessages = try await pool.read { db in
      try MLSMessageModel.filter(MLSMessageModel.Columns.conversationID == self.convoID).fetchAll(db)
    }
    XCTAssertEqual(preMessages.count, 2)

    // Execute Delete for me
    try await storage.deleteConversationForMe(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    // Verify deletion marker exists and is hidden
    let marker = try await storage.fetchLocalConversationDeletionMarker(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertNotNil(marker)
    XCTAssertEqual(marker?.conversationID, convoID)
    XCTAssertEqual(marker?.currentUserDID, accountA)
    XCTAssertEqual(marker?.clearedThroughSequenceNumber, 2)
    XCTAssertEqual(marker?.isHiddenFromList, true)

    // Verify messages are purged locally
    let postMessages = try await pool.read { db in
      try MLSMessageModel.filter(MLSMessageModel.Columns.conversationID == self.convoID).fetchAll(db)
    }
    XCTAssertEqual(postMessages.count, 0)

    // Verify cryptographic state (epoch key) is PRESERVED
    let epochKeys = try await pool.read { db in
      try MLSEpochKeyModel.filter(MLSEpochKeyModel.Columns.conversationID == self.convoID).fetchAll(db)
    }
    XCTAssertEqual(epochKeys.count, 1, "Epoch keys must be preserved for continued cryptographic sync")
    XCTAssertEqual(epochKeys.first?.keyMaterial, Data(repeating: 0x42, count: 32))

    // Verify conversation row itself still exists in storage for state tracking
    let convoRow = try await pool.read { db in
      try MLSConversationModel.filter(MLSConversationModel.Columns.conversationID == self.convoID).fetchOne(db)
    }
    XCTAssertNotNil(convoRow)
  }

  // MARK: - 2. Conversation is hidden from active list

  func testConversationHiddenFromActiveListAfterDeleteForMe() async throws {
    try seedConversation(userDID: accountA)

    // Before deletion: present in active list
    let preActive = try await pool.read { db in
      try MLSStorageHelpers.fetchActiveConversationsSync(in: db, currentUserDID: self.accountA)
    }
    XCTAssertEqual(preActive.count, 1)

    // Delete for me
    try await storage.deleteConversationForMe(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    // After deletion: excluded from active list
    let postActive = try await pool.read { db in
      try MLSStorageHelpers.fetchActiveConversationsSync(in: db, currentUserDID: self.accountA)
    }
    XCTAssertEqual(postActive.count, 0, "Deleted conversation must be excluded from active list")

    // Check helper
    let isDeleted = try await storage.isConversationLocallyDeleted(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertTrue(isDeleted)
  }

  // MARK: - 3. Tenant isolation: Same conversation two DIDs

  func testTenantIsolationSameConvoTwoDIDs() async throws {
    // MLSConversationModel keys on conversationID alone; real multi-account
    // isolation lives in per-user database files. Use distinct conversation
    // and group identities per account so both rows coexist in this pool.
    let convoA = convoID
    let convoB = "550e8400-e29b-41d4-a716-446655440001"
    try seedConversation(userDID: accountA, conversationID: convoA, groupIDHex: groupIDHex)
    try seedConversation(
      userDID: accountB,
      conversationID: convoB,
      groupIDHex: String(repeating: "cd", count: 32)
    )
    try seedMessage(userDID: accountA, messageID: "msg-a-1", seq: 1, timestamp: Date().addingTimeInterval(-10), conversationID: convoA)
    try seedMessage(userDID: accountB, messageID: "msg-b-1", seq: 1, timestamp: Date().addingTimeInterval(-10), conversationID: convoB)

    // Account A deletes for me
    try await storage.deleteConversationForMe(
      conversationID: convoA,
      currentUserDID: accountA,
      database: pool
    )

    // Account A is deleted
    let aActive = try await pool.read { db in
      try MLSStorageHelpers.fetchActiveConversationsSync(in: db, currentUserDID: self.accountA)
    }
    XCTAssertEqual(aActive.count, 0)

    let aMarker = try await storage.fetchLocalConversationDeletionMarker(
      conversationID: convoA,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertNotNil(aMarker)
    XCTAssertEqual(aMarker?.isHiddenFromList, true)

    // Account B is completely untouched
    let bActive = try await pool.read { db in
      try MLSStorageHelpers.fetchActiveConversationsSync(in: db, currentUserDID: self.accountB)
    }
    XCTAssertEqual(bActive.count, 1, "Account B must remain active and unaffected by Account A's deletion")

    let bMarker = try await storage.fetchLocalConversationDeletionMarker(
      conversationID: convoB,
      currentUserDID: accountB,
      database: pool
    )
    XCTAssertNil(bMarker, "Account B must have no deletion marker")

    let bMessages = try await pool.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == convoB)
        .filter(MLSMessageModel.Columns.currentUserDID == self.accountB)
        .fetchAll(db)
    }
    XCTAssertEqual(bMessages.count, 1, "Account B's message history must remain intact")

    // Replay write for Account A at seq 1: must be suppressed
    try replayWrite(userDID: accountA, messageID: "replay-a-1", seq: 1, timestamp: Date().addingTimeInterval(-5), conversationID: convoA)
    let aMessagesAfterReplay = try await storage.fetchMessagesForConversation(
      convoA,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertEqual(aMessagesAfterReplay.count, 0, "Account A must suppress replayed message <= floor")

    // Replay write for Account B at seq 2: must succeed
    try replayWrite(userDID: accountB, messageID: "replay-b-2", seq: 2, timestamp: Date(), conversationID: convoB)
    let bMessagesAfterReplay = try await storage.fetchMessagesForConversation(
      convoB,
      currentUserDID: accountB,
      database: pool
    )
    XCTAssertEqual(bMessagesAfterReplay.count, 2, "Account B must accept new writes normally")
  }

  // MARK: - 4. Real DB Reopen and Server Sync cannot resurrect deleted conversation or history

  func testRealDBReopenAndSyncDoesNotResurrectDeletedConversationOrHistory() async throws {
    try seedConversation(userDID: accountA)
    try seedMessage(userDID: accountA, messageID: "msg-1", seq: 1, timestamp: Date().addingTimeInterval(-200))

    try await storage.deleteConversationForMe(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    // Perform REAL database reopen
    try reopenDatabase()

    // Verify state persists across reopen
    let isDeletedAfterReopen = try await storage.isConversationLocallyDeleted(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertTrue(isDeletedAfterReopen, "Locally deleted state must survive DB reopen")

    // Simulate server sync updating MLSConversationModel.lastMessageAt to a future/newer timestamp
    try await pool.write { db in
      let existing = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == self.convoID)
        .filter(MLSConversationModel.Columns.currentUserDID == self.accountA)
        .fetchOne(db)

      let newerTime = Date().addingTimeInterval(500)
      let resynced = MLSConversationModel.mergedServerSnapshot(
        conversationID: self.convoID,
        currentUserDID: self.accountA,
        groupID: Data(hexEncoded: self.groupIDHex)!,
        epoch: 2,
        createdAt: existing?.createdAt ?? Date(),
        updatedAt: Date(),
        title: "Resynced Title",
        existing: existing,
        lastMessageAt: newerTime,
        requestState: .none
      )
      try resynced.save(db)
    }

    // Query active conversations as relaunch / refresh does
    let active = try await pool.read { db in
      try MLSStorageHelpers.fetchActiveConversationsSync(in: db, currentUserDID: self.accountA)
    }
    XCTAssertEqual(active.count, 0, "Inventory lastMessageAt alone must NEVER resurrect a deleted conversation")

    // Fetch messages
    let messages = try await storage.fetchMessagesForConversation(
      convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertEqual(messages.count, 0, "Purged messages must not be resurrected")
  }

  // MARK: - 5. Old sequenced message with future timestamp cannot escape deletion

  func testFutureTimestampOldSeqDoesNotEscapeDeletion() async throws {
    try seedConversation(userDID: accountA)

    // Message with seq 1, but a future timestamp (e.g. clock skew)
    let futureTimestamp = Date().addingTimeInterval(3600)
    try seedMessage(userDID: accountA, messageID: "future-msg-1", seq: 1, timestamp: futureTimestamp)

    // Delete for me
    try await storage.deleteConversationForMe(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    // Verify messages purged
    let msgsAfterDelete = try await storage.fetchMessagesForConversation(
      convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertEqual(msgsAfterDelete.count, 0)

    // Attempt replay write with future timestamp and seq 1
    try replayWrite(
      userDID: accountA,
      messageID: "future-msg-1-replayed",
      seq: 1,
      timestamp: futureTimestamp
    )

    // Authoritative sequence floor must prevail: message must NOT be fetched
    let msgsAfterReplay = try await storage.fetchMessagesForConversation(
      convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertEqual(msgsAfterReplay.count, 0, "Old sequenced message with future timestamp must NOT escape deletion")

    let isStillDeleted = try await storage.isConversationLocallyDeleted(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertTrue(isStillDeleted)
  }

  // MARK: - 6. Repeated deletion is monotonic and does not regress floor on empty history

  func testRepeatedDeletionIsMonotonicAndDoesNotRegressFloorOnEmptyHistory() async throws {
    try seedConversation(userDID: accountA)
    try seedMessage(userDID: accountA, messageID: "msg-1", seq: 1, timestamp: Date().addingTimeInterval(-30))
    try seedMessage(userDID: accountA, messageID: "msg-2", seq: 2, timestamp: Date().addingTimeInterval(-20))
    try seedMessage(userDID: accountA, messageID: "msg-3", seq: 3, timestamp: Date().addingTimeInterval(-10))

    // First deletion
    try await storage.deleteConversationForMe(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    let marker1 = try await storage.fetchLocalConversationDeletionMarker(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertEqual(marker1?.clearedThroughSequenceNumber, 3)

    // Verify history is empty
    let emptyCount = try await pool.read { db in
      try MLSMessageModel.filter(MLSMessageModel.Columns.conversationID == self.convoID).fetchCount(db)
    }
    XCTAssertEqual(emptyCount, 0)

    // Second deletion ON EMPTY HISTORY (MAX(sequenceNumber) is null)
    try await storage.deleteConversationForMe(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    let marker2 = try await storage.fetchLocalConversationDeletionMarker(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertEqual(
      marker2?.clearedThroughSequenceNumber,
      3,
      "Repeat deletion on empty history must NOT regress the floor"
    )

    // Advance stored sequence state cursor
    try await storage.updateLastProcessedSeq(
      conversationID: convoID,
      currentUserDID: accountA,
      sequenceNumber: 7,
      database: pool
    )

    // Third deletion: must advance floor to stored cursor 7
    try await storage.deleteConversationForMe(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    let marker3 = try await storage.fetchLocalConversationDeletionMarker(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertEqual(marker3?.clearedThroughSequenceNumber, 7, "Deletion floor must advance monotonically")

    // Replay of message at seq 5 (< 7) must be suppressed
    try replayWrite(userDID: accountA, messageID: "replay-msg-5", seq: 5, timestamp: Date())
    let msgs = try await storage.fetchMessagesForConversation(
      convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertEqual(msgs.count, 0)
  }

  // MARK: - 7. Unhide then old replay does not resurrect purged history

  func testUnhideThenOldReplayDoesNotResurrectPurgedHistory() async throws {
    try seedConversation(userDID: accountA)
    try seedMessage(userDID: accountA, messageID: "old-msg-1", seq: 1, timestamp: Date().addingTimeInterval(-100))

    try await storage.deleteConversationForMe(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    let deletedBeforeUnhide = try await storage.isConversationLocallyDeleted(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertTrue(deletedBeforeUnhide)

    // User explicitly unhides / starts new chat -> clearLocalConversationDeletion
    try await storage.clearLocalConversationDeletion(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    // Conversation is now unhidden from active list
    let deletedAfterUnhide = try await storage.isConversationLocallyDeleted(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertFalse(deletedAfterUnhide)

    let active = try await pool.read { db in
      try MLSStorageHelpers.fetchActiveConversationsSync(in: db, currentUserDID: self.accountA)
    }
    XCTAssertEqual(active.count, 1, "Conversation must be visible again after unhide")

    // Marker must still preserve clearedThroughSequenceNumber
    let marker = try await storage.fetchLocalConversationDeletionMarker(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertNotNil(marker)
    XCTAssertEqual(marker?.isHiddenFromList, false)
    XCTAssertEqual(marker?.clearedThroughSequenceNumber, 1)

    // Replay old message (seq 1)
    try replayWrite(
      userDID: accountA,
      messageID: "old-msg-1-replayed",
      seq: 1,
      timestamp: Date().addingTimeInterval(-50)
    )

    // Purged history must NOT be resurrected
    let messages = try await storage.fetchMessagesForConversation(
      convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertEqual(messages.count, 0, "Old replayed history must NOT be resurrected after unhide")
  }

  // MARK: - 8. New accepted traffic resurfaces conversation safely

  func testNewAcceptedTrafficResurfacesConversationSafely() async throws {
    let t0 = Date().addingTimeInterval(-300)
    try seedConversation(userDID: accountA, lastMessageAt: t0)
    try seedMessage(userDID: accountA, messageID: "old-msg-1", seq: 1, timestamp: t0)

    try await storage.deleteConversationForMe(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    // Confirm hidden
    let preActive = try await pool.read { db in
      try MLSStorageHelpers.fetchActiveConversationsSync(in: db, currentUserDID: self.accountA)
    }
    XCTAssertEqual(preActive.count, 0)

    // New accepted message arrives beyond deletion fence: seq 2 > floor 1
    let t2 = Date()
    try replayWrite(
      userDID: accountA,
      messageID: "new-msg-2",
      seq: 2,
      timestamp: t2,
      text: "Hello from the future"
    )

    // Conversation must now surface in the active conversation list!
    let postActive = try await pool.read { db in
      try MLSStorageHelpers.fetchActiveConversationsSync(in: db, currentUserDID: self.accountA)
    }
    XCTAssertEqual(postActive.count, 1, "Conversation must surface when newer messages arrive")

    // Query messages: only the new message is returned, old message is NOT returned
    let messages = try await storage.fetchMessagesForConversation(
      convoID,
      currentUserDID: accountA,
      database: pool
    )
    XCTAssertEqual(messages.count, 1)
    XCTAssertEqual(messages.first?.messageID, "new-msg-2")
  }

  // MARK: - 9. Missing marker table fails transaction without deleting history

  func testMissingMarkerTableFailsTransactionWithoutDeletingHistory() async throws {
    try seedConversation(userDID: accountA)
    try seedMessage(userDID: accountA, messageID: "safe-msg-1", seq: 1, timestamp: Date())

    // Drop marker table to simulate missing migration / corruption
    try await pool.write { db in
      try db.drop(table: MLSConversationDeletionMarkerModel.databaseTableName)
    }

    // deleteConversationForMe must fail closed and NOT delete messages
    do {
      try await storage.deleteConversationForMe(
        conversationID: convoID,
        currentUserDID: accountA,
        database: pool
      )
      XCTFail("Must throw error when marker table is missing")
    } catch let error as MLSStorageError {
      switch error {
      case .requiredTableMissing:
        break // Expected!
      default:
        XCTFail("Unexpected MLSStorageError: \(error)")
      }
    } catch {
      XCTFail("Unexpected error type: \(error)")
    }

    // Verify messages were NOT deleted
    let remainingMessages = try await pool.read { db in
      try MLSMessageModel.filter(MLSMessageModel.Columns.conversationID == self.convoID).fetchAll(db)
    }
    XCTAssertEqual(remainingMessages.count, 1, "Messages must NOT be deleted if marker cannot be persisted")
  }

  // MARK: - 10. Storage adapter Rust projection filters deleted history

  func testStorageAdapterRustProjectionFiltersDeletedHistory() async throws {
    try seedConversation(userDID: accountA)
    try replayWrite(userDID: accountA, messageID: "adapter-msg-1", seq: 1, timestamp: Date().addingTimeInterval(-20))

    let adapter = makeAdapter(userDID: accountA)

    // Pre-condition: message visible to adapter
    let preMsgs = try adapter.getMessages(conversationId: convoID, limit: 10, beforeSequence: nil)
    XCTAssertEqual(preMsgs.count, 1)
    XCTAssertTrue(try adapter.messageExists(messageId: "adapter-msg-1"))

    // Delete for me
    try await storage.deleteConversationForMe(
      conversationID: convoID,
      currentUserDID: accountA,
      database: pool
    )

    // Post-condition: adapter returns 0 messages and messageExists returns false
    let postMsgs = try adapter.getMessages(conversationId: convoID, limit: 10, beforeSequence: nil)
    XCTAssertEqual(postMsgs.count, 0, "Rust adapter getMessages must not return purged messages")
    XCTAssertFalse(try adapter.messageExists(messageId: "adapter-msg-1"))

    // Attempt to store old message via adapter
    let oldFfi = FfiMessage(
      id: "replayed-adapter-msg-1",
      conversationId: convoID,
      senderDid: accountA,
      text: "Replayed",
      timestamp: ISO8601DateFormatter().string(from: Date().addingTimeInterval(-10)),
      epoch: 1,
      sequenceNumber: 1,
      isOwn: true,
      deliveryStatus: nil,
      payloadJson: nil
    )
    try adapter.storeMessage(message: oldFfi)

    let msgsAfterStore = try adapter.getMessages(conversationId: convoID, limit: 10, beforeSequence: nil)
    XCTAssertEqual(msgsAfterStore.count, 0, "Adapter storeMessage must suppress purged replayed message")
  }
}
