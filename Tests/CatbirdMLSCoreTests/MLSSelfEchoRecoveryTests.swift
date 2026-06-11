import XCTest
import GRDB
@testable import CatbirdMLSCore

final class MLSSelfEchoRecoveryTests: XCTestCase {
  private var dbQueue: DatabaseQueue!
  private let storage = MLSStorage.shared

  override func setUp() async throws {
    try await super.setUp()
    dbQueue = try DatabaseQueue()

    // N37: run the full production migration chain (v1…v32) instead of a
    // hand-rolled schema — the hand-rolled tables drifted from the v31
    // field-encryption columns (`payloadEncrypted` et al.) and broke every
    // query that references them.
    try MLSGRDBManager.makeMigrator().migrate(dbQueue)
  }

  override func tearDown() async throws {
    dbQueue = nil
    try await super.tearDown()
  }

  func testPendingSelfSendLookupDoesNotRequireSequenceZero() async throws {
    let conversationID = "convo-self-echo"
    let userDID = "did:plc:self-user"

    try await insertMessage(
      messageID: "local-pending",
      conversationID: conversationID,
      currentUserDID: userDID,
      senderDID: userDID,
      text: "optimistic",
      epoch: 4,
      sequenceNumber: 42,
      processingState: "pendingSelfSend",
      timestamp: Date(timeIntervalSince1970: 100)
    )

    let pending = try await storage.fetchOldestUnconfirmedSelfSentMessage(
      conversationID: conversationID,
      senderDID: userDID,
      currentUserDID: userDID,
      database: dbQueue
    )

    XCTAssertEqual(pending?.messageID, "local-pending")
    XCTAssertEqual(pending?.parsedPayload?.text, "optimistic")
  }

  func testServerPromotionClearsPendingSelfSendStateAndMovesPayload() async throws {
    let conversationID = "convo-promotion"
    let userDID = "did:plc:self-user"
    let receivedAt = Date(timeIntervalSince1970: 200)

    try await insertMessage(
      messageID: "local-pending",
      conversationID: conversationID,
      currentUserDID: userDID,
      senderDID: userDID,
      text: "cached plaintext",
      epoch: 4,
      sequenceNumber: 42,
      processingState: "pendingSelfSend",
      timestamp: Date(timeIntervalSince1970: 100)
    )

    try await storage.updateMessageMetadata(
      messageID: "local-pending",
      currentUserDID: userDID,
      epoch: 5,
      sequenceNumber: 108,
      timestamp: receivedAt,
      database: dbQueue,
      newMessageID: "server-row-id"
    )

    let rows = try await fetchMessages()

    XCTAssertNil(rows["local-pending"])
    XCTAssertEqual(rows["server-row-id"]?.processingState, "cached")
    XCTAssertEqual(rows["server-row-id"]?.epoch, 5)
    XCTAssertEqual(rows["server-row-id"]?.sequenceNumber, 108)
    XCTAssertEqual(rows["server-row-id"]?.parsedPayload?.text, "cached plaintext")
  }

  func testConfirmedOlderSelfMessageIsNotSelectedAsPendingEcho() async throws {
    let conversationID = "convo-confirmed-filter"
    let userDID = "did:plc:self-user"

    try await insertMessage(
      messageID: "confirmed-old",
      conversationID: conversationID,
      currentUserDID: userDID,
      senderDID: userDID,
      text: "confirmed",
      epoch: 2,
      sequenceNumber: 0,
      processingState: "cached",
      timestamp: Date(timeIntervalSince1970: 100)
    )

    try await insertMessage(
      messageID: "pending-new",
      conversationID: conversationID,
      currentUserDID: userDID,
      senderDID: userDID,
      text: "pending",
      epoch: 3,
      sequenceNumber: 12,
      processingState: "pendingSelfSend",
      timestamp: Date(timeIntervalSince1970: 200)
    )

    let pending = try await storage.fetchOldestUnconfirmedSelfSentMessage(
      conversationID: conversationID,
      senderDID: userDID,
      currentUserDID: userDID,
      database: dbQueue
    )

    XCTAssertEqual(pending?.messageID, "pending-new")
  }

  private func insertMessage(
    messageID: String,
    conversationID: String,
    currentUserDID: String,
    senderDID: String,
    text: String,
    epoch: Int64,
    sequenceNumber: Int64,
    processingState: String,
    timestamp: Date
  ) async throws {
    let payloadData = try MLSMessagePayload.text(text, embed: nil).encodeToJSON()
    try await dbQueue.write { db in
      // The production schema (unlike the old hand-rolled one) enforces
      // MLSMessageModel.conversationID → MLSConversationModel, so the parent
      // conversation row must exist before the message insert.
      try db.execute(
        sql: """
          INSERT OR IGNORE INTO MLSConversationModel (
            conversationID, currentUserDID, groupID, createdAt, updatedAt
          ) VALUES (?, ?, ?, ?, ?)
          """,
        arguments: [conversationID, currentUserDID, Data(), timestamp, timestamp]
      )
      try db.execute(
        sql: """
          INSERT INTO MLSMessageModel (
            messageID, currentUserDID, conversationID, senderID,
            payloadJSON, wireFormat, contentType, timestamp,
            epoch, sequenceNumber, authenticatedData, signature,
            isDelivered, isRead, isSent, sendAttempts, error,
            processingState, gapBefore, payloadExpired, processingError,
            processingAttempts, validationFailureReason
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """,
        arguments: [
          messageID, currentUserDID, conversationID, senderDID,
          payloadData, Data(), "application/json", timestamp,
          epoch, sequenceNumber, nil, nil,
          true, false, true, 0, nil,
          processingState, false, false, nil,
          0, nil,
        ]
      )
    }
  }

  private func fetchMessages() async throws -> [String: MLSMessageModel] {
    let rows = try await dbQueue.read { db in
      try MLSMessageModel.fetchAll(db)
    }
    return Dictionary(uniqueKeysWithValues: rows.map { ($0.messageID, $0) })
  }

}
