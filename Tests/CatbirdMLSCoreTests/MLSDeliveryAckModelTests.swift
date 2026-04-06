import XCTest
import GRDB
@testable import CatbirdMLSCore

final class MLSDeliveryAckModelTests: XCTestCase {

  private var db: DatabaseQueue!

  override func setUp() async throws {
    db = try DatabaseQueue()
    try await db.write { db in
      try db.create(table: "MLSDeliveryAck") { t in
        t.column("messageId", .text).notNull()
        t.column("conversationId", .text).notNull()
        t.column("senderDID", .text).notNull()
        t.column("ackedAt", .datetime).notNull()
        t.column("currentUserDID", .text).notNull()
        t.primaryKey(["messageId", "senderDID", "currentUserDID"])
      }
    }
  }

  func testInsertAndFetch() async throws {
    let ack = MLSDeliveryAckModel(
      messageId: "msg-1",
      conversationId: "convo-1",
      senderDID: "did:plc:alice",
      ackedAt: Date(timeIntervalSince1970: 1000),
      currentUserDID: "did:plc:bob"
    )
    try await db.write { db in try ack.insert(db) }

    let fetched = try await db.read { db in
      try MLSDeliveryAckModel.fetchAll(db)
    }
    XCTAssertEqual(fetched.count, 1)
    XCTAssertEqual(fetched[0].messageId, "msg-1")
    XCTAssertEqual(fetched[0].senderDID, "did:plc:alice")
  }

  func testUpsertIsIdempotent() async throws {
    let ack = MLSDeliveryAckModel(
      messageId: "msg-1",
      conversationId: "convo-1",
      senderDID: "did:plc:alice",
      ackedAt: Date(timeIntervalSince1970: 1000),
      currentUserDID: "did:plc:bob"
    )
    try await db.write { db in try ack.save(db) }
    try await db.write { db in try ack.save(db) }  // second upsert — no error

    let count = try await db.read { db in try MLSDeliveryAckModel.fetchCount(db) }
    XCTAssertEqual(count, 1)
  }

  func testMultipleSendersForSameMessage() async throws {
    let ack1 = MLSDeliveryAckModel(messageId: "msg-1", conversationId: "c", senderDID: "did:plc:alice", ackedAt: Date(), currentUserDID: "did:plc:bob")
    let ack2 = MLSDeliveryAckModel(messageId: "msg-1", conversationId: "c", senderDID: "did:plc:carol", ackedAt: Date(), currentUserDID: "did:plc:bob")
    try await db.write { db in
      try ack1.insert(db)
      try ack2.insert(db)
    }
    let acks = try await db.read { db in
      try MLSDeliveryAckModel
        .filter(Column("messageId") == "msg-1")
        .fetchAll(db)
    }
    XCTAssertEqual(acks.count, 2)
  }
}
