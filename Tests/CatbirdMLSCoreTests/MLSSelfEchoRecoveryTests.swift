import XCTest
import GRDB
@testable import CatbirdMLSCore

final class MLSSelfEchoRecoveryTests: XCTestCase {
  private var dbQueue: DatabaseQueue!
  private let storage = MLSStorage.shared

  override func setUp() async throws {
    try await super.setUp()
    dbQueue = try DatabaseQueue()

    try await dbQueue.write { db in
      try Self.createMessageTable(in: db)
      try Self.createReactionTable(in: db)
    }
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

  private static func createMessageTable(in db: Database) throws {
    try db.create(table: "MLSMessageModel") { t in
      t.primaryKey("messageID", .text).notNull()
      t.column("currentUserDID", .text).notNull()
      t.column("conversationID", .text).notNull()
      t.column("senderID", .text).notNull()
      t.column("payloadJSON", .blob)
      t.column("wireFormat", .blob)
      t.column("contentType", .text).notNull()
      t.column("timestamp", .datetime).notNull()
      t.column("epoch", .integer).notNull()
      t.column("sequenceNumber", .integer).notNull()
      t.column("authenticatedData", .blob)
      t.column("signature", .blob)
      t.column("isDelivered", .boolean).notNull().defaults(to: false)
      t.column("isRead", .boolean).notNull().defaults(to: false)
      t.column("isSent", .boolean).notNull().defaults(to: false)
      t.column("sendAttempts", .integer).notNull().defaults(to: 0)
      t.column("error", .text)
      t.column("processingState", .text).notNull()
      t.column("gapBefore", .boolean).notNull().defaults(to: false)
      t.column("payloadExpired", .boolean).notNull().defaults(to: false)
      t.column("processingError", .text)
      t.column("processingAttempts", .integer).notNull().defaults(to: 0)
      t.column("validationFailureReason", .text)
    }
  }

  private static func createReactionTable(in db: Database) throws {
    try db.create(table: "MLSMessageReactionModel") { t in
      t.primaryKey("reactionID", .text).notNull()
      t.column("messageID", .text).notNull().references("MLSMessageModel", onDelete: .cascade)
      t.column("conversationID", .text).notNull()
      t.column("currentUserDID", .text).notNull()
      t.column("actorDID", .text).notNull()
      t.column("emoji", .text).notNull()
      t.column("action", .text).notNull()
      t.column("timestamp", .datetime).notNull()
    }
  }
}
