import XCTest
import GRDB
import Petrel
import PetrelCatbird
@testable import CatbirdMLSCore

/// Covers `MLSStorageHelpers.markMessageAsRead` / `markMessageAsUnread` /
/// `isNewestMessage` (storage layer) and
/// `MLSConversationManager.setMessageReadStatus` (service layer).
final class MLSMessageReadStateTests: XCTestCase {

  // MARK: - Storage layer: markMessageAsRead cascades to earlier messages

  func testMarkMessageAsReadCascadesToEarlierSequenceNumbers() async throws {
    let db = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(db)

    let conversationID = "convo-1"
    let currentUserDID = "did:plc:reader"
    try await seedMessages(
      in: db,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      sequenceNumbers: [1, 2, 3, 4]
    )

    let changed = try await MLSStorageHelpers.markMessageAsRead(
      in: db,
      messageID: "msg-3",
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )
    XCTAssertEqual(changed, 3) // msg-1, msg-2, msg-3

    let rows = try await fetchReadFlags(in: db, conversationID: conversationID, currentUserDID: currentUserDID)
    XCTAssertEqual(rows["msg-1"], true)
    XCTAssertEqual(rows["msg-2"], true)
    XCTAssertEqual(rows["msg-3"], true)
    XCTAssertEqual(rows["msg-4"], false) // later message untouched
  }

  func testMarkMessageAsReadDoesNotAffectOtherConversationsOrUsers() async throws {
    let db = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(db)

    let currentUserDID = "did:plc:reader"
    try await seedMessages(
      in: db,
      conversationID: "convo-a",
      currentUserDID: currentUserDID,
      sequenceNumbers: [1, 2],
      messagePrefix: "a-msg"
    )
    try await seedMessages(
      in: db,
      conversationID: "convo-b",
      currentUserDID: currentUserDID,
      sequenceNumbers: [1, 2],
      messagePrefix: "b-msg"
    )

    _ = try await MLSStorageHelpers.markMessageAsRead(
      in: db,
      messageID: "a-msg-2",
      conversationID: "convo-a",
      currentUserDID: currentUserDID
    )

    let convoBRows = try await fetchReadFlags(in: db, conversationID: "convo-b", currentUserDID: currentUserDID)
    XCTAssertEqual(convoBRows["b-msg-1"], false)
    XCTAssertEqual(convoBRows["b-msg-2"], false)
  }

  func testMarkMessageAsReadReturnsZeroForUnknownMessage() async throws {
    let db = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(db)

    let changed = try await MLSStorageHelpers.markMessageAsRead(
      in: db,
      messageID: "does-not-exist",
      conversationID: "convo-1",
      currentUserDID: "did:plc:reader"
    )
    XCTAssertEqual(changed, 0)
  }

  // MARK: - Storage layer: markMessageAsUnread is single-row only

  func testMarkMessageAsUnreadOnlyAffectsTargetRow() async throws {
    let db = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(db)

    let conversationID = "convo-1"
    let currentUserDID = "did:plc:reader"
    try await seedMessages(
      in: db,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      sequenceNumbers: [1, 2, 3],
      isRead: true
    )

    let changed = try await MLSStorageHelpers.markMessageAsUnread(
      in: db,
      messageID: "msg-2",
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )
    XCTAssertEqual(changed, 1)

    let rows = try await fetchReadFlags(in: db, conversationID: conversationID, currentUserDID: currentUserDID)
    XCTAssertEqual(rows["msg-1"], true, "earlier message must remain read")
    XCTAssertEqual(rows["msg-2"], false, "only the target row flips to unread")
    XCTAssertEqual(rows["msg-3"], true, "later message must remain read")
  }

  func testMarkMessageAsUnreadIsIdempotent() async throws {
    let db = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(db)

    let conversationID = "convo-1"
    let currentUserDID = "did:plc:reader"
    try await seedMessages(
      in: db,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      sequenceNumbers: [1],
      isRead: false
    )

    let changed = try await MLSStorageHelpers.markMessageAsUnread(
      in: db,
      messageID: "msg-1",
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )
    XCTAssertEqual(changed, 0, "row already unread — no rows should be touched")
  }

  // MARK: - Storage layer: isNewestMessage

  func testIsNewestMessageTrueForHighestSequenceNumber() async throws {
    let db = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(db)

    let conversationID = "convo-1"
    let currentUserDID = "did:plc:reader"
    try await seedMessages(
      in: db,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      sequenceNumbers: [1, 2, 3]
    )

    let isNewest = try await MLSStorageHelpers.isNewestMessage(
      in: db,
      messageID: "msg-3",
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )
    XCTAssertTrue(isNewest)
  }

  func testIsNewestMessageFalseForEarlierSequenceNumber() async throws {
    let db = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(db)

    let conversationID = "convo-1"
    let currentUserDID = "did:plc:reader"
    try await seedMessages(
      in: db,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      sequenceNumbers: [1, 2, 3]
    )

    let isNewest = try await MLSStorageHelpers.isNewestMessage(
      in: db,
      messageID: "msg-1",
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )
    XCTAssertFalse(isNewest)
  }

  func testIsNewestMessageFalseForUnknownMessage() async throws {
    let db = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(db)

    let isNewest = try await MLSStorageHelpers.isNewestMessage(
      in: db,
      messageID: "does-not-exist",
      conversationID: "convo-1",
      currentUserDID: "did:plc:reader"
    )
    XCTAssertFalse(isNewest)
  }

  // MARK: - Service layer: setMessageReadStatus

  func testSetMessageReadStatusMarksReadAndCascades() async throws {
    let manager = try await makeManager(userDid: "did:plc:testuser")
    try await seedMessages(
      in: manager.database,
      conversationID: "convo-1",
      currentUserDID: "did:plc:testuser",
      sequenceNumbers: [1, 2, 3]
    )

    try await manager.setMessageReadStatus(convoId: "convo-1", messageId: "msg-2", read: true)

    let rows = try await fetchReadFlags(in: manager.database, conversationID: "convo-1", currentUserDID: "did:plc:testuser")
    XCTAssertEqual(rows["msg-1"], true)
    XCTAssertEqual(rows["msg-2"], true)
    XCTAssertEqual(rows["msg-3"], false)
  }

  func testSetMessageReadStatusUnreadDoesNotCascade() async throws {
    let manager = try await makeManager(userDid: "did:plc:testuser")
    try await seedMessages(
      in: manager.database,
      conversationID: "convo-1",
      currentUserDID: "did:plc:testuser",
      sequenceNumbers: [1, 2, 3],
      isRead: true
    )

    try await manager.setMessageReadStatus(convoId: "convo-1", messageId: "msg-2", read: false)

    let rows = try await fetchReadFlags(in: manager.database, conversationID: "convo-1", currentUserDID: "did:plc:testuser")
    XCTAssertEqual(rows["msg-1"], true)
    XCTAssertEqual(rows["msg-2"], false)
    XCTAssertEqual(rows["msg-3"], true)
  }

  func testSetMessageReadStatusThrowsWhenNoCurrentUser() async throws {
    let manager = try await makeManager(userDid: nil)

    do {
      try await manager.setMessageReadStatus(convoId: "convo-1", messageId: "msg-1", read: true)
      XCTFail("Expected MLSError.noCurrentUser")
    } catch let error as MLSError {
      guard case .noCurrentUser = error else {
        return XCTFail("Expected .noCurrentUser, got \(error)")
      }
    }
  }

  /// The newest message in the conversation triggers a best-effort server
  /// cursor update (`MLSAPIClient.updateCursor`). The real API client here
  /// points at a throwaway HTTPS host (matching the construction pattern
  /// used by `MLSFullRustMessagingTests.makeManager`) so the update
  /// inevitably fails server-side (no such XRPC route exists there) —
  /// this asserts that failure is swallowed (`try?`) and never surfaces,
  /// while the local read-state change still lands. The decision of
  /// *which* message counts as "newest" is covered precisely and
  /// deterministically by the `isNewestMessage` storage tests above.
  func testSetMessageReadStatusNewestMessageTriggersCursorUpdateWithoutThrowing() async throws {
    let manager = try await makeManager(userDid: "did:plc:testuser")
    try await seedMessages(
      in: manager.database,
      conversationID: "convo-1",
      currentUserDID: "did:plc:testuser",
      sequenceNumbers: [1, 2, 3]
    )

    // msg-3 has the highest sequenceNumber -> newest.
    try await manager.setMessageReadStatus(convoId: "convo-1", messageId: "msg-3", read: true)

    let rows = try await fetchReadFlags(in: manager.database, conversationID: "convo-1", currentUserDID: "did:plc:testuser")
    XCTAssertEqual(rows["msg-3"], true)
  }

  func testSetMessageReadStatusNonNewestMessageDoesNotThrow() async throws {
    let manager = try await makeManager(userDid: "did:plc:testuser")
    try await seedMessages(
      in: manager.database,
      conversationID: "convo-1",
      currentUserDID: "did:plc:testuser",
      sequenceNumbers: [1, 2, 3]
    )

    // msg-1 is not the newest -> no cursor update should even be attempted.
    try await manager.setMessageReadStatus(convoId: "convo-1", messageId: "msg-1", read: true)

    let rows = try await fetchReadFlags(in: manager.database, conversationID: "convo-1", currentUserDID: "did:plc:testuser")
    XCTAssertEqual(rows["msg-1"], true)
  }

  // MARK: - Helpers

  private func makeManager(userDid: String?) async throws -> MLSConversationManager {
    let database = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(database)
    let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: atProtoClient,
      environment: .custom(serviceDID: "did:web:example.com#atproto_mls")
    )
    return MLSConversationManager(
      apiClient: apiClient,
      database: database,
      userDid: userDid,
      atProtoClient: atProtoClient,
      protocolAuthorityMode: .swiftLegacy
    )
  }

  /// Messages carry a `conversationID` foreign key into `MLSConversationModel`
  /// — seed (or reuse, via `INSERT OR IGNORE`) a minimal parent row so message
  /// inserts don't violate the FK constraint.
  private func ensureConversation(
    in database: MLSDatabase,
    conversationID: String,
    currentUserDID: String
  ) async throws {
    try await database.write { db in
      let model = MLSConversationModel(
        conversationID: conversationID,
        currentUserDID: currentUserDID,
        groupID: Data([0x01, 0x02, 0x03, 0x04])
      )
      try model.insert(db, onConflict: .ignore)
    }
  }

  @discardableResult
  private func seedMessages(
    in database: MLSDatabase,
    conversationID: String,
    currentUserDID: String,
    sequenceNumbers: [Int64],
    isRead: Bool = false,
    messagePrefix: String = "msg"
  ) async throws -> [MLSMessageModel] {
    try await ensureConversation(
      in: database,
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )

    let models: [MLSMessageModel] = sequenceNumbers.map { seq in
      MLSMessageModel(
        messageID: "\(messagePrefix)-\(seq)",
        currentUserDID: currentUserDID,
        conversationID: conversationID,
        senderID: "did:plc:sender",
        contentType: "text",
        timestamp: Date(timeIntervalSince1970: TimeInterval(seq)),
        epoch: 1,
        sequenceNumber: seq,
        isRead: isRead,
        processingState: "delivered"
      )
    }
    try await database.write { db in
      for model in models {
        try model.insert(db)
      }
    }
    return models
  }

  private func fetchReadFlags(
    in database: MLSDatabase,
    conversationID: String,
    currentUserDID: String
  ) async throws -> [String: Bool] {
    try await database.read { db in
      let rows = try Row.fetchAll(
        db,
        sql: "SELECT messageID, isRead FROM MLSMessageModel WHERE conversationID = ? AND currentUserDID = ?",
        arguments: [conversationID, currentUserDID]
      )
      var result: [String: Bool] = [:]
      for row in rows {
        let messageID: String = row["messageID"]
        let isRead: Bool = row["isRead"]
        result[messageID] = isRead
      }
      return result
    }
  }
}
