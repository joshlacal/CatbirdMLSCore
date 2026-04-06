import XCTest
import GRDB
@testable import CatbirdMLSCore

final class MLSReadFrontierTests: XCTestCase {
  private var dbQueue: DatabaseQueue!

  override func setUp() async throws {
    try await super.setUp()
    dbQueue = try DatabaseQueue()

    try await dbQueue.write { db in
      try MLSReadFrontierModel.createTable(in: db)
      try MLSRemoteReadCursorModel.createTable(in: db)

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

      try db.create(table: "MLSOrphanedReactionModel") { t in
        t.primaryKey("reactionID", .text).notNull()
        t.column("messageID", .text).notNull()
        t.column("conversationID", .text).notNull()
        t.column("currentUserDID", .text).notNull()
        t.column("actorDID", .text).notNull()
        t.column("emoji", .text).notNull()
        t.column("action", .text).notNull()
        t.column("timestamp", .datetime).notNull()
      }
    }
  }

  func testReadFrontierOnlyAdvancesMonotonically() async throws {
    let conversationID = "convo-monotonic"
    let currentUserDID = "did:plc:reader"

    try await dbQueue.write { db in
      XCTAssertTrue(
        try MLSStorageHelpers.upsertReadFrontierSync(
          in: db,
          conversationID: conversationID,
          currentUserDID: currentUserDID,
          epoch: 4,
          sequenceNumber: 10
        )
      )
      XCTAssertFalse(
        try MLSStorageHelpers.upsertReadFrontierSync(
          in: db,
          conversationID: conversationID,
          currentUserDID: currentUserDID,
          epoch: 4,
          sequenceNumber: 9
        )
      )
      XCTAssertFalse(
        try MLSStorageHelpers.upsertReadFrontierSync(
          in: db,
          conversationID: conversationID,
          currentUserDID: currentUserDID,
          epoch: 3,
          sequenceNumber: 999
        )
      )
      XCTAssertTrue(
        try MLSStorageHelpers.upsertReadFrontierSync(
          in: db,
          conversationID: conversationID,
          currentUserDID: currentUserDID,
          epoch: 4,
          sequenceNumber: 11
        )
      )
      XCTAssertTrue(
        try MLSStorageHelpers.upsertReadFrontierSync(
          in: db,
          conversationID: conversationID,
          currentUserDID: currentUserDID,
          epoch: 5,
          sequenceNumber: 1
        )
      )

      let frontier = try MLSStorageHelpers.fetchReadFrontierSync(
        from: db,
        conversationID: conversationID,
        currentUserDID: currentUserDID
      )
      XCTAssertEqual(frontier?.epoch, 5)
      XCTAssertEqual(frontier?.sequenceNumber, 1)
    }
  }

  func testBackfilledPayloadAtOrBeforeFrontierIsAutoMarkedRead() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-backfill"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    try await dbQueue.write { db in
      XCTAssertTrue(
        try MLSStorageHelpers.upsertReadFrontierSync(
          in: db,
          conversationID: conversationID,
          currentUserDID: currentUserDID,
          epoch: 7,
          sequenceNumber: 20
        )
      )
    }

    _ = try await storage.savePayloadForMessage(
      messageID: "msg-old",
      conversationID: conversationID,
      payload: .text("old"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 7,
      sequenceNumber: 19,
      timestamp: Date(),
      database: dbQueue
    )

    _ = try await storage.savePayloadForMessage(
      messageID: "msg-new",
      conversationID: conversationID,
      payload: .text("new"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 7,
      sequenceNumber: 21,
      timestamp: Date(),
      database: dbQueue
    )

    let readStateByMessageID = try await dbQueue.read { db in
      let rows = try Row.fetchAll(
        db,
        sql: """
          SELECT messageID, isRead
          FROM MLSMessageModel
          WHERE conversationID = ? AND currentUserDID = ?
          """,
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

    XCTAssertEqual(readStateByMessageID["msg-old"], true)
    XCTAssertEqual(readStateByMessageID["msg-new"], false)
  }

  func testControlPayloadDoesNotIncreaseUnreadCount() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-control"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      messageID: "msg-reaction",
      conversationID: conversationID,
      payload: .reaction(messageId: "msg-target", emoji: "👍", action: .add),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    let unreadCount = try await dbQueue.read { db in
      let counts = try MLSStorageHelpers.getUnreadCountsForAllConversationsSync(
        from: db,
        currentUserDID: currentUserDID
      )
      return counts[conversationID] ?? 0
    }

    XCTAssertEqual(unreadCount, 0)
  }

  func testProtocolPlaceholderPayloadDoesNotIncreaseUnreadCount() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-protocol"
    let currentUserDID = "did:plc:reader"

    _ = try await storage.savePayloadForMessage(
      messageID: "msg-protocol",
      conversationID: conversationID,
      payload: .typing(isTyping: false),
      senderID: "unknown",
      currentUserDID: currentUserDID,
      epoch: 2,
      sequenceNumber: 5,
      timestamp: Date(),
      database: dbQueue
    )

    let unreadCount = try await dbQueue.read { db in
      let counts = try MLSStorageHelpers.getUnreadCountsForAllConversationsSync(
        from: db,
        currentUserDID: currentUserDID
      )
      return counts[conversationID] ?? 0
    }

    XCTAssertEqual(unreadCount, 0)
  }

  func testUnreadAndMarkReadIgnoreUndecryptableMessages() async throws {
    let conversationID = "convo-undecryptable"
    let currentUserDID = "did:plc:reader"

    try await dbQueue.write { db in
      try db.execute(
        sql: """
          INSERT INTO MLSMessageModel (
            messageID, currentUserDID, conversationID, senderID, payloadJSON, wireFormat,
            contentType, timestamp, epoch, sequenceNumber, authenticatedData, signature,
            isDelivered, isRead, isSent, sendAttempts, error, processingState, gapBefore,
            payloadExpired, processingError, processingAttempts, validationFailureReason
          ) VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """,
        arguments: [
          "msg-readable",
          currentUserDID,
          conversationID,
          "did:plc:sender",
          try MLSMessagePayload.text("hello").encodeToJSON(),
          Data(),
          "application/json",
          Date(),
          1,
          1,
          nil,
          nil,
          true,
          false,
          true,
          0,
          nil,
          "cached",
          false,
          false,
          nil,
          0,
          nil,
          "msg-error",
          currentUserDID,
          conversationID,
          "did:plc:sender",
          try MLSMessagePayload.text("broken").encodeToJSON(),
          Data(),
          "application/json",
          Date(),
          1,
          2,
          nil,
          nil,
          true,
          false,
          true,
          0,
          nil,
          "cached",
          false,
          false,
          "Decryption Failed",
          1,
          nil,
          "msg-expired",
          currentUserDID,
          conversationID,
          "did:plc:sender",
          try MLSMessagePayload.text("expired").encodeToJSON(),
          Data(),
          "application/json",
          Date(),
          1,
          3,
          nil,
          nil,
          true,
          false,
          true,
          0,
          nil,
          "cached",
          false,
          true,
          nil,
          0,
          nil,
        ]
      )
    }

    let unreadCount = try await MLSStorageHelpers.getUnreadCount(
      from: dbQueue,
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )
    XCTAssertEqual(unreadCount, 1)

    let markedCount = try await MLSStorageHelpers.markAllMessagesAsRead(
      in: dbQueue,
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )
    XCTAssertEqual(markedCount, 1)

    let stateByID = try await dbQueue.read { db in
      let rows = try Row.fetchAll(
        db,
        sql: "SELECT messageID, isRead FROM MLSMessageModel WHERE conversationID = ?",
        arguments: [conversationID]
      )
      return Dictionary(uniqueKeysWithValues: rows.map { row in
        let messageID: String = row["messageID"]
        let isRead: Bool = row["isRead"]
        return (messageID, isRead)
      })
    }

    XCTAssertEqual(stateByID["msg-readable"], true)
    XCTAssertEqual(stateByID["msg-error"], false)
    XCTAssertEqual(stateByID["msg-expired"], false)
  }

  func testFetchLastDecryptedMessageCursorSkipsErroredExpiredAndControlMessages() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-last-decrypted"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      messageID: "msg-text",
      conversationID: conversationID,
      payload: .text("visible"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    _ = try await storage.savePayloadForMessage(
      messageID: "msg-control",
      conversationID: conversationID,
      payload: .typing(isTyping: true),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 2,
      timestamp: Date(),
      database: dbQueue
    )

    _ = try await storage.savePayloadForMessage(
      messageID: "msg-error",
      conversationID: conversationID,
      payload: .text("broken"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 3,
      timestamp: Date(),
      database: dbQueue,
      processingError: "Decryption Failed"
    )

    let cursor = try await storage.fetchLastDecryptedMessageCursor(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      database: dbQueue
    )

    XCTAssertEqual(cursor?.messageID, "msg-text")
    XCTAssertEqual(cursor?.epoch, 1)
    XCTAssertEqual(cursor?.seq, 1)
  }

  func testRemoteReadCursorOnlyAdvancesForward() async throws {
    let conversationID = "convo-remote-read"
    let currentUserDID = "did:plc:reader"
    let readerDID = "did:plc:other"

    try await dbQueue.write { db in
      XCTAssertTrue(
        try MLSStorageHelpers.upsertRemoteReadCursorSync(
          in: db,
          conversationID: conversationID,
          currentUserDID: currentUserDID,
          readerDID: readerDID,
          epoch: 2,
          sequenceNumber: 10,
          messageID: "msg-10"
        )
      )
      XCTAssertFalse(
        try MLSStorageHelpers.upsertRemoteReadCursorSync(
          in: db,
          conversationID: conversationID,
          currentUserDID: currentUserDID,
          readerDID: readerDID,
          epoch: 2,
          sequenceNumber: 9,
          messageID: "msg-9"
        )
      )
      XCTAssertTrue(
        try MLSStorageHelpers.upsertRemoteReadCursorSync(
          in: db,
          conversationID: conversationID,
          currentUserDID: currentUserDID,
          readerDID: readerDID,
          epoch: 2,
          sequenceNumber: 11,
          messageID: "msg-11"
        )
      )
    }

    let cursors = try await MLSStorageHelpers.fetchRemoteReadCursors(
      from: dbQueue,
      conversationID: conversationID,
      currentUserDID: currentUserDID
    )

    XCTAssertEqual(cursors.count, 1)
    XCTAssertEqual(cursors.first?.epoch, 2)
    XCTAssertEqual(cursors.first?.sequenceNumber, 11)
    XCTAssertEqual(cursors.first?.messageID, "msg-11")
  }
}
