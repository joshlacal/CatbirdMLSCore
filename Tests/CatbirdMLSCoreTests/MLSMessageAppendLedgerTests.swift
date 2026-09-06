import Foundation
import XCTest
import GRDB
@testable import CatbirdMLS
@testable import CatbirdMLSCore

final class MLSMessageAppendLedgerTests: XCTestCase {
  private let conversationID = "550e8400-e29b-41d4-a716-446655440000"
  private let userDID = "did:plc:receiver"
  private let senderDID = "did:plc:sender"
  private var directory: URL!
  private var pool: DatabasePool!
  private var context: MlsContext!

  override func setUp() async throws {
    directory = FileManager.default.temporaryDirectory.appendingPathComponent("MLSAppend-\(UUID())")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    try reopen()
    try MLSGRDBManager.makeMigrator().migrate(pool)
    try insertConversation()
  }

  private func insertConversation() throws {
    try pool.write { db in
      try MLSConversationModel(conversationID: conversationID, currentUserDID: userDID,
        groupID: Data([1, 2, 3]), epoch: 3, createdAt: Date(), updatedAt: Date()).insert(db)
    }
  }

  override func tearDown() async throws {
    try? context?.flushAndPrepareClose()
    context = nil
    try? pool?.close()
    pool = nil
    if let directory { try? FileManager.default.removeItem(at: directory) }
  }

  private func reopen() throws {
    pool = try DatabasePool(path: directory.appendingPathComponent("messages.sqlite").path)
    context = try MlsContext(storagePath: directory.appendingPathComponent("mls.sqlite").path,
      encryptionKey: String(repeating: "ab", count: 32), keychain: InMemoryKeychainAccess())
    try context.setContentRootKey(key: Data(repeating: 0x42, count: 32))
  }

  private func message(_ id: String, text: String = "original", sequence: UInt64 = 0,
    sender: String? = nil, epoch: UInt64 = 3) throws -> FfiMessage
  {
    FfiMessage(id: id, conversationId: conversationID, senderDid: sender ?? senderDID,
      text: text, timestamp: "2026-06-22T12:00:00Z", epoch: epoch, sequenceNumber: sequence,
      isOwn: false, deliveryStatus: nil,
      payloadJson: String(decoding: try MLSMessagePayload.text(text).encodeToJSON(), as: UTF8.self))
  }

  private func store(_ message: FfiMessage) throws {
    try MLSOrchestratorStorageAdapter(dbPool: pool, userDID: userDID,
      mlsContext: context).storeMessage(message: message)
  }

  private func row(_ id: String) throws -> MLSMessageModel {
    try pool.read { db in
      try XCTUnwrap(MLSMessageModel.filter(MLSMessageModel.Columns.messageID == id).fetchOne(db))
    }
  }

  private func proofRows() throws -> [Row] {
    try pool.read { db in
      try MLSMessageAppendLedger.verifiedEntries(context: context, in: db,
        userDID: userDID, cryptoConversationID: conversationID)
    }
  }

  private func ledgerColumns() throws -> [String] {
    try pool.read { try $0.columns(in: MLSMessageAppendLedger.table).map(\.name) }
  }

  func testSenderSequenceResetsAndDuplicatesPreserveAppendOrderAcrossReopen() throws {
    let first = try message("one", sequence: 40)
    try store(first)
    try store(message("two", sequence: 0, sender: "did:plc:second"))
    try store(message("three", sequence: 1))
    let original = try row("one")
    let initialProofs = try proofRows()
    XCTAssertEqual(initialProofs.map { $0["appendOrdinal"] as Int64 }, [1, 2, 3])
    var previous: Data?
    for id in ["one", "two", "three"] {
      let model = try row(id)
      XCTAssertTrue(try MLSFieldEncryption.verifyHMAC(context: context,
        conversationID: conversationID, previousHMAC: previous, messageID: id,
        payloadWire: XCTUnwrap(model.payloadEncrypted), expected: XCTUnwrap(model.entryHMAC)))
      previous = model.entryHMAC
    }
    try store(first)
    XCTAssertEqual(try row("one"), original)
    XCTAssertEqual(try proofRows(), initialProofs)

    try context.flushAndPrepareClose()
    context = nil
    try pool.close()
    pool = nil
    try reopen()
    try store(first)
    XCTAssertEqual(try row("one"), original)
    try store(message("four", sequence: 0))
    XCTAssertEqual(try proofRows().map { $0["appendOrdinal"] as Int64 }, [1, 2, 3, 4])
    XCTAssertTrue(try MLSFieldEncryption.verifyHMAC(context: context,
      conversationID: conversationID, previousHMAC: previous, messageID: "four",
      payloadWire: XCTUnwrap(row("four").payloadEncrypted), expected: XCTUnwrap(row("four").entryHMAC)))
  }

  func testMetadataEpochPromotionPreservesExactBodyDuplicates() async throws {
    try store(message("one", epoch: 3))
    let proofs = try proofRows()
    let original = try row("one")
    try await MLSStorage.shared.updateMessageMetadata(messageID: "one", currentUserDID: userDID,
      epoch: 4, sequenceNumber: 29, timestamp: Date(timeIntervalSince1970: 1_800_000_000),
      database: pool)
    let promoted = try row("one")
    XCTAssertEqual(promoted.epoch, 4)
    XCTAssertEqual(promoted.payloadEncrypted, original.payloadEncrypted)
    try store(message("one", epoch: 4))
    try store(message("one", epoch: 3))
    XCTAssertEqual(try row("one"), promoted)
    XCTAssertEqual(try proofRows(), proofs)
  }

  func testImmutableConflictsAndNoncontiguousOrdinalsFailWithoutWriting() throws {
    try store(message("one"))
    try store(message("two"))
    let original = try row("one")
    let proofs = try proofRows()
    XCTAssertThrowsError(try store(message("one", text: "conflict")))
    XCTAssertThrowsError(try store(message("one", sender: "did:plc:imposter")))
    XCTAssertEqual(try row("one"), original)
    XCTAssertEqual(try proofRows(), proofs)
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSMessageAppendLedgerV1 SET appendOrdinal = appendOrdinal + 10")
    }
    XCTAssertThrowsError(try store(message("three")))
    XCTAssertEqual(try pool.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM MLSMessageModel") }, 2)
  }

  func testMalformedOrdinalAndProofTamperingThrowWithoutWriting() throws {
    try store(message("one"))
    let original = try row("one")
    try pool.write { db in
      // SQLite affinity allows this text despite INTEGER CHECK (>0).
      try db.execute(sql: "UPDATE MLSMessageAppendLedgerV1 SET appendOrdinal = 'invalid'")
    }
    XCTAssertThrowsError(try store(message("two")))
    XCTAssertEqual(try row("one"), original)
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSMessageAppendLedgerV1 SET appendOrdinal = 1, proofHMAC = ?",
        arguments: [Data(repeating: 0x91, count: 32)])
    }
    XCTAssertThrowsError(try store(message("two")))
    XCTAssertEqual(try row("one"), original)
    try pool.write { db in
      try db.execute(sql: "UPDATE MLSMessageAppendLedgerV1 SET payloadDigest = ?",
        arguments: [String(repeating: "x", count: 32)])
    }
    XCTAssertThrowsError(try store(message("two")))
    XCTAssertEqual(try pool.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM MLSMessageModel") }, 1)
    XCTAssertEqual(try pool.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM MLSMessageAppendLedgerV1") }, 1)
  }

  func testEditAndUnsendDoNotLoseTheirProjectionOrRetainAnOriginalCiphertextCopy() async throws {
    let original = try message("one")
    try store(original)
    try store(message("two"))
    let proofs = try proofRows()
    _ = try await MLSStorage.shared.applyEdit(conversationID: conversationID,
      targetMessageID: "one", newText: "edited", editorUserDID: senderDID,
      editSeq: 20, currentUserDID: userDID, context: context, database: pool)
    let edited = try row("one")
    try store(original)
    XCTAssertEqual(try row("one"), edited)
    _ = try await MLSStorage.shared.applyTombstone(conversationID: conversationID,
      targetMessageID: "one", senderUserDID: senderDID, currentUserDID: userDID,
      context: context, database: pool)
    let deleted = try row("one")
    try store(original)
    XCTAssertEqual(try row("one"), deleted)
    XCTAssertEqual(deleted.isTombstone, 1)
    XCTAssertEqual(try proofRows(), proofs)
    let columns = try ledgerColumns()
    XCTAssertFalse(columns.contains("payloadEncrypted"))
    XCTAssertFalse(columns.contains("payloadJSON"))
    try store(message("three"))
    XCTAssertEqual(try proofRows().count, 3)
  }

  func testLegacyMalformedHistoryRemainsUntouchedAndNewChainStartsExplicitly() throws {
    try store(message("legacy"))
    try pool.write { db in
      // Reproduce a pre-v36 row whose historical HMAC was already malformed.
      try db.execute(sql: "DELETE FROM MLSMessageAppendLedgerV1")
      try db.execute(sql: "UPDATE MLSMessageModel SET entryHMAC = ? WHERE messageID = 'legacy'",
        arguments: [Data(repeating: 0x91, count: 32)])
    }
    let legacy = try row("legacy")
    try store(message("legacy"))
    XCTAssertEqual(try row("legacy"), legacy)
    XCTAssertTrue(try proofRows().isEmpty)
    try store(message("new"))
    XCTAssertEqual(try row("legacy"), legacy)
    XCTAssertEqual(try proofRows().map { $0["messageID"] as String }, ["new"])
    XCTAssertTrue(try MLSFieldEncryption.verifyHMAC(context: context,
      conversationID: conversationID, previousHMAC: nil, messageID: "new",
      payloadWire: XCTUnwrap(row("new").payloadEncrypted), expected: XCTUnwrap(row("new").entryHMAC)))
  }

  func testAsyncStorageSharesLedgerAndErrorPlaceholderRemainsReplaceable() async throws {
    func save(_ id: String, text: String, error: String? = nil) async throws {
      _ = try await MLSStorage.shared.savePayloadForMessage(context: context,
        messageID: id, conversationID: conversationID, payload: .text(text),
        senderID: senderDID, currentUserDID: userDID, epoch: 3,
        sequenceNumber: 0, timestamp: Date(), database: pool, processingError: error)
    }
    try await save("one", text: "error placeholder", error: "failed")
    XCTAssertTrue(try proofRows().isEmpty)
    try await save("one", text: "recovered")
    let original = try row("one")
    try await save("one", text: "recovered")
    XCTAssertEqual(try row("one"), original)
    try await save("one", text: "error placeholder", error: "late duplicate decrypt")
    XCTAssertEqual(try row("one"), original)
    XCTAssertEqual(try proofRows().count, 1)
    try store(message("two"))
    XCTAssertEqual(try proofRows().count, 2)
  }
}
