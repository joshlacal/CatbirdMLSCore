import XCTest
import GRDB
@testable import CatbirdMLSCore

final class MLSMessageStorageDisplayOrderTests: XCTestCase {
  private var dbQueue: DatabaseQueue!
  private let conversationID = "convo-rotated-groups"
  private let currentUserDID = "did:plc:viewer"

  override func setUp() async throws {
    try await super.setUp()
    dbQueue = try DatabaseQueue()
    try await dbQueue.write { db in
      try Self.createMessageTable(in: db)
    }
  }

  override func tearDown() async throws {
    dbQueue = nil
    try await super.tearDown()
  }

  func testFetchMessagesForConversationOrdersBySequenceAcrossEpochReset() async throws {
    try await insertMessage(id: "seq-10", epoch: 6, sequenceNumber: 10)
    try await insertMessage(id: "seq-11", epoch: 0, sequenceNumber: 11)
    try await insertMessage(id: "seq-12", epoch: 1, sequenceNumber: 12)

    let messages = try await MLSStorage.shared.fetchMessagesForConversation(
      conversationID,
      currentUserDID: currentUserDID,
      database: dbQueue,
      limit: 10
    )

    XCTAssertEqual(messages.map(\.messageID), ["seq-10", "seq-11", "seq-12"])
  }

  func testPaginationUsesSequenceRatherThanEpoch() async throws {
    try await insertMessage(id: "seq-09", epoch: 5, sequenceNumber: 9)
    try await insertMessage(id: "seq-10", epoch: 6, sequenceNumber: 10)
    try await insertMessage(id: "seq-11", epoch: 0, sequenceNumber: 11)

    let messages = try await MLSStorage.shared.fetchMessagesBeforeSequence(
      conversationId: conversationID,
      currentUserDID: currentUserDID,
      beforeEpoch: 0,
      beforeSeq: 11,
      database: dbQueue,
      limit: 10
    )

    XCTAssertEqual(messages.map(\.messageID), ["seq-09", "seq-10"])
  }

  func testLastCursorUsesHighestConversationSequenceAcrossEpochReset() async throws {
    try await insertMessage(id: "seq-10", epoch: 6, sequenceNumber: 10)
    try await insertMessage(id: "seq-11", epoch: 0, sequenceNumber: 11)
    try await insertMessage(id: "seq-12", epoch: 1, sequenceNumber: 12)

    let cursor = try await MLSStorage.shared.fetchLastMessageCursor(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      database: dbQueue
    )

    XCTAssertEqual(cursor?.messageID, "seq-12")
    XCTAssertEqual(cursor?.seq, 12)
  }

  private func insertMessage(id: String, epoch: Int64, sequenceNumber: Int64) async throws {
    let payload = try MLSMessagePayload.text(id, embed: nil).encodeToJSON()
    let message = MLSMessageModel(
      messageID: id,
      currentUserDID: MLSStorageHelpers.normalizeDID(currentUserDID),
      conversationID: conversationID,
      senderID: "did:plc:sender",
      payloadJSON: payload,
      wireFormat: nil,
      contentType: "application/json",
      timestamp: Date(timeIntervalSince1970: TimeInterval(sequenceNumber)),
      epoch: epoch,
      sequenceNumber: sequenceNumber,
      isDelivered: true,
      processingState: MLSMessageProcessingState.cached
    )

    try await dbQueue.write { db in
      try message.insert(db)
    }
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
}
