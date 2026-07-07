//
//  MLSEditUnsendTests.swift
//  CatbirdMLSCoreTests
//
//  Exercises message edit/unsend (spec §5.7 / §5.8): payload round-trip,
//  authorization, last-writer-wins, tombstone application, and orphan
//  persist-then-adopt — mirroring MLSReactionTests / MLSReadFrontierTests'
//  hermetic ephemeral-MlsContext test pattern.
//

import Foundation
import XCTest
import GRDB
@testable import CatbirdMLSCore
@testable import CatbirdMLS

final class MLSEditUnsendTests: XCTestCase {
  private var dbQueue: DatabaseQueue!
  private var mlsContext: MlsContext!
  private var tempStorageDir: URL!

  override func setUp() async throws {
    try await super.setUp()
    dbQueue = try DatabaseQueue()

    // Spin up an ephemeral MlsContext just for field-level encrypt + HMAC.
    tempStorageDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSEditUnsendTests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: tempStorageDir, withIntermediateDirectories: true)
    let storagePath = tempStorageDir.appendingPathComponent("ctx.db").path
    let encryptionKey = String(repeating: "ab", count: 32)
    mlsContext = try MlsContext(
      storagePath: storagePath,
      encryptionKey: encryptionKey,
      keychain: InMemoryKeychainAccess()
    )
    try mlsContext.setContentRootKey(key: Data(repeating: 0x42, count: 32))

    try await dbQueue.write { db in
      try MLSReadFrontierModel.createTable(in: db)

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
        // Field-level encryption columns (v31 in production migrator).
        t.column("payloadEncrypted", .blob)
        t.column("entryHMAC", .blob)
        t.column("payloadKeyVersion", .integer).notNull().defaults(to: 1)
        t.column("isTombstone", .integer).notNull().defaults(to: 0)
        t.column("deletedAt", .integer)
        // v34 — edit/unsend columns under test.
        t.column("isEdited", .integer).notNull().defaults(to: 0)
        t.column("editedAt", .datetime)
        t.column("appliedEditSeq", .integer)
      }

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

      try db.create(table: "MLSOrphanedMutationModel") { t in
        t.primaryKey("mutationID", .text).notNull()
        t.column("targetMessageID", .text).notNull()
        t.column("conversationID", .text).notNull()
        t.column("currentUserDID", .text).notNull()
        t.column("applierDID", .text).notNull()
        t.column("mutationType", .text).notNull()
        t.column("newText", .text)
        t.column("mutationSeq", .integer).notNull()
        t.column("createdAt", .datetime).notNull()
      }
    }
  }

  override func tearDown() async throws {
    if let ctx = mlsContext {
      ctx.clearContentRootKey()
      try? ctx.flushAndPrepareClose()
    }
    mlsContext = nil
    if let dir = tempStorageDir {
      try? FileManager.default.removeItem(at: dir)
    }
    tempStorageDir = nil
    dbQueue = nil
    try await super.tearDown()
  }

  // MARK: - Payload round-trip (wire format, spec §5.7.1 / §5.8.1)

  func testEditPayloadWireFormatRoundTrip() throws {
    let payload = MLSMessagePayload.edit(targetMessageId: "01ARZ3NDEKTSV4RRFFQ69G5FAV", newText: "corrected")
    let data = try payload.encodeToJSON()

    let json = try XCTUnwrap(try JSONSerialization.jsonObject(with: data) as? [String: Any])
    XCTAssertEqual(json["version"] as? Int, 1)
    XCTAssertEqual(json["messageType"] as? String, "edit")
    let edit = try XCTUnwrap(json["edit"] as? [String: Any])
    XCTAssertEqual(edit["targetMessageId"] as? String, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
    XCTAssertEqual(edit["newText"] as? String, "corrected")

    let decoded = try MLSMessagePayload.decodeFromJSON(data)
    XCTAssertEqual(decoded.messageType, .edit)
    XCTAssertEqual(decoded.edit?.targetMessageId, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
    XCTAssertEqual(decoded.edit?.newText, "corrected")
  }

  func testUnsendPayloadWireFormatRoundTrip() throws {
    let payload = MLSMessagePayload.unsend(targetMessageId: "01ARZ3NDEKTSV4RRFFQ69G5FAV")
    let data = try payload.encodeToJSON()

    let json = try XCTUnwrap(try JSONSerialization.jsonObject(with: data) as? [String: Any])
    XCTAssertEqual(json["messageType"] as? String, "delete")
    let delete = try XCTUnwrap(json["delete"] as? [String: Any])
    XCTAssertEqual(delete["targetMessageId"] as? String, "01ARZ3NDEKTSV4RRFFQ69G5FAV")

    let decoded = try MLSMessagePayload.decodeFromJSON(data)
    XCTAssertEqual(decoded.messageType, .delete)
    XCTAssertEqual(decoded.delete?.targetMessageId, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
  }

  // MARK: - Edit application

  func testApplyEditRewritesTextAndSetsEditedFlag() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-edit"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-1",
      conversationID: conversationID,
      payload: .text("original"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    let result = try await storage.applyEdit(
      conversationID: conversationID,
      targetMessageID: "msg-1",
      newText: "corrected",
      editorUserDID: senderDID,
      editSeq: 2,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertTrue(result.found)
    XCTAssertTrue(result.mutated, "genuine successful edit must report mutated — observers should be notified")

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-1")
        .fetchOne(db)
    }
    let row1 = try XCTUnwrap(row)
    XCTAssertEqual(row1.isEdited, 1)
    XCTAssertNotNil(row1.editedAt)
    XCTAssertEqual(row1.appliedEditSeq, 2)
    XCTAssertEqual(row1.decryptedPayload(context: mlsContext)?.text, "corrected")
  }

  func testApplyEditRejectsWrongSender() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-edit-auth"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"
    let attackerDID = "did:plc:attacker"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-1",
      conversationID: conversationID,
      payload: .text("original"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    // Target exists, so this reports `found: true` (spec: silently drop, no
    // error) — but `mutated` must be `false` (no observer notification) and
    // the text must be unchanged.
    let result = try await storage.applyEdit(
      conversationID: conversationID,
      targetMessageID: "msg-1",
      newText: "hijacked",
      editorUserDID: attackerDID,
      editSeq: 2,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertTrue(result.found)
    XCTAssertFalse(result.mutated, "rejected edit must not report mutated — no observer notification")

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-1")
        .fetchOne(db)
    }
    let row1 = try XCTUnwrap(row)
    XCTAssertEqual(row1.isEdited, 0)
    XCTAssertEqual(row1.decryptedPayload(context: mlsContext)?.text, "original")
  }

  /// Device-suffix normalization: an editor DID carrying an MLS credential
  /// device fragment (`did:plc:sender#device-1`) must still authorize against
  /// the bare-DID sender stored on the target row.
  func testApplyEditAuthorizesAcrossDeviceSuffix() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-edit-device"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-1",
      conversationID: conversationID,
      payload: .text("original"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    let result = try await storage.applyEdit(
      conversationID: conversationID,
      targetMessageID: "msg-1",
      newText: "corrected from another device",
      editorUserDID: "did:plc:sender#device-2",
      editSeq: 2,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertTrue(result.found)
    XCTAssertTrue(result.mutated)

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-1")
        .fetchOne(db)
    }
    XCTAssertEqual(row?.isEdited, 1)
    XCTAssertEqual(row?.decryptedPayload(context: mlsContext)?.text, "corrected from another device")
  }

  func testApplyEditLastWriterWinsDropsStaleEdit() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-edit-lww"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-1",
      conversationID: conversationID,
      payload: .text("original"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    // Apply the newer edit first (seq=5), then a stale one (seq=3) arrives late.
    _ = try await storage.applyEdit(
      conversationID: conversationID, targetMessageID: "msg-1", newText: "newer",
      editorUserDID: senderDID, editSeq: 5, currentUserDID: currentUserDID,
      context: mlsContext, database: dbQueue
    )
    let staleResult = try await storage.applyEdit(
      conversationID: conversationID, targetMessageID: "msg-1", newText: "stale",
      editorUserDID: senderDID, editSeq: 3, currentUserDID: currentUserDID,
      context: mlsContext, database: dbQueue
    )
    XCTAssertTrue(staleResult.found, "target existed, stale edit silently dropped")
    XCTAssertFalse(staleResult.mutated, "stale edit must not report mutated — no observer notification")

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-1")
        .fetchOne(db)
    }
    XCTAssertEqual(row?.decryptedPayload(context: mlsContext)?.text, "newer")
    XCTAssertEqual(row?.appliedEditSeq, 5)
  }

  func testApplyEditIsIdempotent() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-edit-idempotent"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-1",
      conversationID: conversationID,
      payload: .text("original"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    // First application actually mutates the row; the second (same editSeq)
    // is a stale no-op per last-writer-wins — `found` stays true both times,
    // but only the first application reports `mutated`.
    for iteration in 0..<2 {
      let result = try await storage.applyEdit(
        conversationID: conversationID, targetMessageID: "msg-1", newText: "corrected",
        editorUserDID: senderDID, editSeq: 2, currentUserDID: currentUserDID,
        context: mlsContext, database: dbQueue
      )
      XCTAssertTrue(result.found)
      XCTAssertEqual(result.mutated, iteration == 0, "only the first application should mutate")
    }

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-1")
        .fetchOne(db)
    }
    XCTAssertEqual(row?.decryptedPayload(context: mlsContext)?.text, "corrected")
    XCTAssertEqual(row?.appliedEditSeq, 2)
  }

  func testApplyEditReturnsFalseForMissingTarget() async throws {
    let storage = MLSStorage.shared
    let result = try await storage.applyEdit(
      conversationID: "convo-missing", targetMessageID: "does-not-exist", newText: "x",
      editorUserDID: "did:plc:sender", editSeq: 1, currentUserDID: "did:plc:reader",
      context: mlsContext, database: dbQueue
    )
    XCTAssertFalse(result.found)
    XCTAssertFalse(result.mutated)
  }

  // MARK: - Tombstone application

  func testApplyTombstoneBlanksTextAndRemovesReactions() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-delete"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-1",
      conversationID: conversationID,
      payload: .text("secret message"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )
    try await storage.saveReaction(
      MLSReactionModel(
        messageID: "msg-1", conversationID: conversationID, currentUserDID: currentUserDID,
        actorDID: "did:plc:reactor", emoji: "👍", action: "add"
      ),
      database: dbQueue
    )

    let result = try await storage.applyTombstone(
      conversationID: conversationID,
      targetMessageID: "msg-1",
      senderUserDID: senderDID,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertTrue(result.found)
    XCTAssertTrue(result.mutated, "genuine successful tombstone must report mutated — observers should be notified")

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-1")
        .fetchOne(db)
    }
    let row1 = try XCTUnwrap(row)
    XCTAssertEqual(row1.isTombstone, 1)
    XCTAssertNotNil(row1.deletedAt)
    XCTAssertEqual(row1.decryptedPayload(context: mlsContext)?.text, "")

    let reactions = try await dbQueue.read { db in try MLSReactionModel.fetchAll(db) }
    XCTAssertTrue(reactions.isEmpty, "reactions on a tombstoned message must be removed")

    // Tombstoned rows are hidden from user-facing read paths.
    let fetched = try await storage.fetchMessage(
      messageID: "msg-1", currentUserDID: currentUserDID, database: dbQueue
    )
    XCTAssertNil(fetched, "fetchMessage must hide tombstoned rows")
  }

  func testApplyTombstoneIsIdempotent() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-delete-idempotent"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-1",
      conversationID: conversationID,
      payload: .text("secret"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    // First application actually tombstones the row; the second is an
    // idempotent no-op — `found` stays true both times, but only the first
    // application reports `mutated`.
    for iteration in 0..<2 {
      let result = try await storage.applyTombstone(
        conversationID: conversationID, targetMessageID: "msg-1", senderUserDID: senderDID,
        currentUserDID: currentUserDID, context: mlsContext, database: dbQueue
      )
      XCTAssertTrue(result.found)
      XCTAssertEqual(result.mutated, iteration == 0, "only the first application should mutate")
    }
  }

  func testApplyTombstoneRejectsWrongSender() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-delete-auth"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-1",
      conversationID: conversationID,
      payload: .text("secret"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    let result = try await storage.applyTombstone(
      conversationID: conversationID, targetMessageID: "msg-1", senderUserDID: "did:plc:attacker",
      currentUserDID: currentUserDID, context: mlsContext, database: dbQueue
    )
    XCTAssertTrue(result.found, "target existed; mismatch is a silent drop")
    XCTAssertFalse(result.mutated, "rejected delete must not report mutated — no observer notification")

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-1")
        .fetchOne(db)
    }
    XCTAssertEqual(row?.isTombstone, 0)
    XCTAssertEqual(row?.decryptedPayload(context: mlsContext)?.text, "secret")
  }

  /// Spec §5.8.2: a tombstoned message's text can never be resurrected by a
  /// later-arriving edit, regardless of the edit's seq.
  func testEditCannotResurrectTombstonedMessage() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-delete-then-edit"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-1",
      conversationID: conversationID,
      payload: .text("secret"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )
    _ = try await storage.applyTombstone(
      conversationID: conversationID, targetMessageID: "msg-1", senderUserDID: senderDID,
      currentUserDID: currentUserDID, context: mlsContext, database: dbQueue
    )

    let result = try await storage.applyEdit(
      conversationID: conversationID, targetMessageID: "msg-1", newText: "resurrected",
      editorUserDID: senderDID, editSeq: 99, currentUserDID: currentUserDID,
      context: mlsContext, database: dbQueue
    )
    XCTAssertTrue(result.found, "target existed; edit silently dropped")
    XCTAssertFalse(result.mutated, "edit on tombstoned target must not report mutated")

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-1")
        .fetchOne(db)
    }
    XCTAssertEqual(row?.isTombstone, 1)
    XCTAssertEqual(row?.isEdited, 0)
    XCTAssertEqual(row?.decryptedPayload(context: mlsContext)?.text, "")
  }

  // MARK: - Orphan persist-then-adopt

  func testOrphanedEditPersistsThenAdoptsWhenTargetArrives() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-orphan-edit"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    let result = try await storage.applyOrPersistOrphanedEdit(
      conversationID: conversationID,
      targetMessageID: "msg-not-yet-arrived",
      newText: "edited before arrival",
      editorUserDID: senderDID,
      editSeq: 7,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertFalse(result.found)
    XCTAssertFalse(result.mutated)

    let orphans = try await dbQueue.read { db in try MLSOrphanedMutationModel.fetchAll(db) }
    XCTAssertEqual(orphans.count, 1)
    XCTAssertEqual(orphans.first?.targetMessageID, "msg-not-yet-arrived")
    XCTAssertEqual(orphans.first?.mutationType, MLSMutationKind.edit.rawValue)

    // Target message now arrives.
    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-not-yet-arrived",
      conversationID: conversationID,
      payload: .text("original"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    let adopted = try await storage.adoptOrphanedMutationsForMessage(
      "msg-not-yet-arrived",
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertEqual(adopted.count, 1)
    XCTAssertEqual(adopted.first?.kind, .edit)

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-not-yet-arrived")
        .fetchOne(db)
    }
    XCTAssertEqual(row?.decryptedPayload(context: mlsContext)?.text, "edited before arrival")
    XCTAssertEqual(row?.isEdited, 1)

    let remainingOrphans = try await dbQueue.read { db in try MLSOrphanedMutationModel.fetchAll(db) }
    XCTAssertTrue(remainingOrphans.isEmpty, "adopted orphan must be removed from the parking lot")
  }

  func testOrphanedDeleteWinsOverPendingOrphanedEdit() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-orphan-delete-wins"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.applyOrPersistOrphanedEdit(
      conversationID: conversationID,
      targetMessageID: "msg-not-yet-arrived",
      newText: "should never be shown",
      editorUserDID: senderDID,
      editSeq: 5,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    _ = try await storage.applyOrPersistOrphanedDelete(
      conversationID: conversationID,
      targetMessageID: "msg-not-yet-arrived",
      senderUserDID: senderDID,
      deleteSeq: 6,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )

    // Only the delete orphan should survive.
    let orphans = try await dbQueue.read { db in try MLSOrphanedMutationModel.fetchAll(db) }
    XCTAssertEqual(orphans.count, 1)
    XCTAssertEqual(orphans.first?.mutationType, MLSMutationKind.delete.rawValue)

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-not-yet-arrived",
      conversationID: conversationID,
      payload: .text("original"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    let adopted = try await storage.adoptOrphanedMutationsForMessage(
      "msg-not-yet-arrived",
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertEqual(adopted.count, 1)
    XCTAssertEqual(adopted.first?.kind, .delete)

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-not-yet-arrived")
        .fetchOne(db)
    }
    XCTAssertEqual(row?.isTombstone, 1)
    XCTAssertEqual(row?.isEdited, 0)
    XCTAssertEqual(row?.decryptedPayload(context: mlsContext)?.text, "")
  }

  // MARK: - Cache-hit recovery (review fix: an edit/delete whose own control
  // message row was already cached — e.g. by the NotificationServiceExtension
  // — must still mutate its target. `MLSConversationManager`'s three cache-hit
  // branches (`processServerMessageLegacy` x2, `processServerMessageLocked`)
  // previously mirrored `.reaction` recovery but fell through for `.edit` /
  // `.delete`, silently stranding the mutation. The fix routes all three
  // through `applyCachedMutationPayload`, which re-derives the same arguments
  // used here (sender DID + payload read off the already-cached row) and
  // applies them via the same storage entry points exercised below —
  // `MLSConversationManager` itself needs FFI/network scaffolding this
  // hermetic storage-level suite intentionally avoids.

  func testCacheHitRecoveryAppliesPendingEditToExistingTarget() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-cache-hit-edit"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    // Target message already persisted locally (as if the original send had
    // synced normally before the edit arrived).
    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-target",
      conversationID: conversationID,
      payload: .text("original"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    // The edit control message's OWN row is already cached (e.g. the NSE
    // decrypted it first) — this is the cache hit the fix recovers from. Prior
    // to the fix, hitting this cache short-circuited straight to
    // `.controlMessage`/`.application` without ever mutating the target.
    let editPayload = MLSMessagePayload.edit(
      targetMessageId: "msg-target", newText: "fixed via cache-hit recovery")
    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-edit-control",
      conversationID: conversationID,
      payload: editPayload,
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 2,
      timestamp: Date(),
      database: dbQueue
    )

    // Recovery: apply the mutation via the same entry point
    // `applyCachedMutationPayload` calls on a cache hit.
    let result = try await storage.applyOrPersistOrphanedEdit(
      conversationID: conversationID,
      targetMessageID: editPayload.edit!.targetMessageId,
      newText: editPayload.edit!.newText,
      editorUserDID: senderDID,
      editSeq: 2,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertTrue(result.found)
    XCTAssertTrue(
      result.mutated, "cache-hit recovery must actually apply the edit, not silently strand it")

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-target")
        .fetchOne(db)
    }
    XCTAssertEqual(row?.isEdited, 1)
    XCTAssertEqual(
      row?.decryptedPayload(context: mlsContext)?.text, "fixed via cache-hit recovery")
  }

  func testCacheHitRecoveryAppliesPendingDeleteToExistingTarget() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-cache-hit-delete"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-target",
      conversationID: conversationID,
      payload: .text("secret"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    // The delete control message's own row is already cached — same cache-hit
    // scenario as above, for `.delete` instead of `.edit`.
    let deletePayload = MLSMessagePayload.unsend(targetMessageId: "msg-target")
    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-delete-control",
      conversationID: conversationID,
      payload: deletePayload,
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 2,
      timestamp: Date(),
      database: dbQueue
    )

    let result = try await storage.applyOrPersistOrphanedDelete(
      conversationID: conversationID,
      targetMessageID: deletePayload.delete!.targetMessageId,
      senderUserDID: senderDID,
      deleteSeq: 2,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertTrue(result.found)
    XCTAssertTrue(
      result.mutated, "cache-hit recovery must actually apply the delete, not silently strand it")

    let row = try await dbQueue.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "msg-target")
        .fetchOne(db)
    }
    XCTAssertEqual(row?.isTombstone, 1)
  }

  // MARK: - Rejected mutation must not report `mutated` (observer notify gate)

  /// `MLSConversationManager`'s `.edit`/`.delete` notify call sites — the
  /// direct decrypt path, `handleRustAuthoritativePayloadSideEffects`, and the
  /// three cache-hit branches added above — all gate `notifyObservers(...)` on
  /// `result.mutated`. A rejected mutation reporting `found: true, mutated:
  /// false` is therefore sufficient evidence that no `.messageEdited` /
  /// `.messageUnsent` observer event fires for it.
  func testRejectedMutationNeverReportsMutatedGatingObserverNotify() async throws {
    let storage = MLSStorage.shared
    let conversationID = "convo-notify-gate"
    let currentUserDID = "did:plc:reader"
    let senderDID = "did:plc:sender"
    let attackerDID = "did:plc:attacker"

    _ = try await storage.savePayloadForMessage(
      context: mlsContext,
      messageID: "msg-1",
      conversationID: conversationID,
      payload: .text("original"),
      senderID: senderDID,
      currentUserDID: currentUserDID,
      epoch: 1,
      sequenceNumber: 1,
      timestamp: Date(),
      database: dbQueue
    )

    // Unauthorized edit: target found, but must not mutate (and therefore
    // must not notify).
    let editResult = try await storage.applyOrPersistOrphanedEdit(
      conversationID: conversationID,
      targetMessageID: "msg-1",
      newText: "hijacked",
      editorUserDID: attackerDID,
      editSeq: 2,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertTrue(editResult.found)
    XCTAssertFalse(editResult.mutated)

    // Unauthorized delete: same contract.
    let deleteResult = try await storage.applyOrPersistOrphanedDelete(
      conversationID: conversationID,
      targetMessageID: "msg-1",
      senderUserDID: attackerDID,
      deleteSeq: 3,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )
    XCTAssertTrue(deleteResult.found)
    XCTAssertFalse(deleteResult.mutated)
  }

  // MARK: - Orphaned-mutation reap (review fix: parking-lot tables must not
  // grow unbounded when a target message never arrives)

  func testReapExpiredOrphanedMutationsDeletesOldKeepsFresh() async throws {
    let storage = MLSStorage.shared
    let currentUserDID = "did:plc:reader"

    let oldOrphan = MLSOrphanedMutationModel(
      targetMessageID: "msg-old",
      conversationID: "convo-1",
      currentUserDID: currentUserDID,
      applierDID: "did:plc:sender",
      mutationType: MLSMutationKind.edit.rawValue,
      newText: "stale edit",
      mutationSeq: 1,
      createdAt: Date().addingTimeInterval(-40 * 86400)  // 40 days old
    )
    let freshOrphan = MLSOrphanedMutationModel(
      targetMessageID: "msg-fresh",
      conversationID: "convo-1",
      currentUserDID: currentUserDID,
      applierDID: "did:plc:sender",
      mutationType: MLSMutationKind.delete.rawValue,
      newText: nil,
      mutationSeq: 1,
      createdAt: Date()
    )
    try await dbQueue.write { db in
      try oldOrphan.insert(db)
      try freshOrphan.insert(db)
    }

    let threshold = Date().addingTimeInterval(-30 * 86400)  // 30-day retention
    try await storage.reapExpiredOrphanedMutations(
      userDID: currentUserDID, olderThan: threshold, database: dbQueue)

    let remaining = try await dbQueue.read { db in try MLSOrphanedMutationModel.fetchAll(db) }
    XCTAssertEqual(remaining.count, 1)
    XCTAssertEqual(remaining.first?.targetMessageID, "msg-fresh")
  }

  func testReapExpiredOrphanedReactionsDeletesOldKeepsFresh() async throws {
    let storage = MLSStorage.shared
    let currentUserDID = "did:plc:reader"

    let oldReaction = MLSOrphanedReactionModel(
      messageID: "msg-old",
      conversationID: "convo-1",
      currentUserDID: currentUserDID,
      actorDID: "did:plc:reactor",
      emoji: "👍",
      action: "add",
      timestamp: Date().addingTimeInterval(-40 * 86400)  // 40 days old
    )
    let freshReaction = MLSOrphanedReactionModel(
      messageID: "msg-fresh",
      conversationID: "convo-1",
      currentUserDID: currentUserDID,
      actorDID: "did:plc:reactor",
      emoji: "🔥",
      action: "add",
      timestamp: Date()
    )
    try await dbQueue.write { db in
      try oldReaction.insert(db)
      try freshReaction.insert(db)
    }

    let threshold = Date().addingTimeInterval(-30 * 86400)  // 30-day retention
    try await storage.reapExpiredOrphanedReactions(
      userDID: currentUserDID, olderThan: threshold, database: dbQueue)

    let remaining = try await dbQueue.read { db in try MLSOrphanedReactionModel.fetchAll(db) }
    XCTAssertEqual(remaining.count, 1)
    XCTAssertEqual(remaining.first?.messageID, "msg-fresh")
  }
}
