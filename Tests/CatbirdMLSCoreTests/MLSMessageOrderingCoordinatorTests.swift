import XCTest
import GRDB
@testable import CatbirdMLSCore

final class MLSMessageOrderingCoordinatorTests: XCTestCase {
  private var dbQueue: DatabaseQueue!
  private let coordinator = MLSMessageOrderingCoordinator()

  override func setUp() async throws {
    try await super.setUp()
    dbQueue = try DatabaseQueue()

    try await dbQueue.write { db in
      try Self.createMessageTable(in: db)
      try MLSConversationSequenceState.createTable(in: db)
      try MLSPendingMessageModel.createTable(in: db)
    }
  }

  override func tearDown() async throws {
    dbQueue = nil
    try await super.tearDown()
  }

  func testEncryptedHistoryRowAtOrBeforeLastProcessedIsReprocessed() async throws {
    let conversationID = "convo-seq-race"
    let currentUserDID = "did:plc:receiver"
    let messageID = "server-message-42"

    try await insertSequenceState(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      lastProcessedSeq: 57
    )

    try await insertMessage(
      messageID: messageID,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      senderDID: "did:plc:sender",
      payloadJSON: nil,
      wireFormat: Data([0x01, 0x02, 0x03]),
      epoch: 6,
      sequenceNumber: 42,
      processingState: "delivered",
      processingError: nil
    )

    let decision = try await coordinator.shouldProcessMessage(
      messageID: messageID,
      conversationID: conversationID,
      sequenceNumber: 42,
      messageEpoch: 6,
      localEpoch: 6,
      currentUserDID: currentUserDID,
      database: dbQueue
    )

    guard case .processNow = decision else {
      return XCTFail("Expected encrypted history row to be reprocessed, got \(decision)")
    }
  }

  func testDecodedPayloadAtOrBeforeLastProcessedIsSkipped() async throws {
    let conversationID = "convo-seq-duplicate"
    let currentUserDID = "did:plc:receiver"
    let messageID = "server-message-processed"
    let payload = try MLSMessagePayload.text("already decrypted", embed: nil).encodeToJSON()

    try await insertSequenceState(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      lastProcessedSeq: 57
    )

    try await insertMessage(
      messageID: messageID,
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      senderDID: "did:plc:sender",
      payloadJSON: payload,
      wireFormat: nil,
      epoch: 6,
      sequenceNumber: 42,
      processingState: MLSMessageProcessingState.cached,
      processingError: nil
    )

    let decision = try await coordinator.shouldProcessMessage(
      messageID: messageID,
      conversationID: conversationID,
      sequenceNumber: 42,
      messageEpoch: 6,
      localEpoch: 6,
      currentUserDID: currentUserDID,
      database: dbQueue
    )

    guard case .alreadyProcessed = decision else {
      return XCTFail("Expected decoded cached row to be skipped, got \(decision)")
    }
  }

  private func insertSequenceState(
    conversationID: String,
    currentUserDID: String,
    lastProcessedSeq: Int64
  ) async throws {
    try await dbQueue.write { db in
      let state = MLSConversationSequenceState(
        conversationID: conversationID,
        currentUserDID: MLSStorageHelpers.normalizeDID(currentUserDID),
        lastProcessedSeq: lastProcessedSeq,
        updatedAt: Date(timeIntervalSince1970: 100)
      )
      try state.insert(db)
    }
  }

  private func insertMessage(
    messageID: String,
    conversationID: String,
    currentUserDID: String,
    senderDID: String,
    payloadJSON: Data?,
    wireFormat: Data?,
    epoch: Int64,
    sequenceNumber: Int64,
    processingState: String,
    processingError: String?
  ) async throws {
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
          messageID,
          MLSStorageHelpers.normalizeDID(currentUserDID),
          conversationID,
          senderDID,
          payloadJSON,
          wireFormat,
          "application/mls",
          Date(timeIntervalSince1970: 200),
          epoch,
          sequenceNumber,
          nil,
          nil,
          true,
          false,
          false,
          0,
          nil,
          processingState,
          false,
          false,
          processingError,
          processingError == nil ? 0 : 1,
          nil,
        ]
      )
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
