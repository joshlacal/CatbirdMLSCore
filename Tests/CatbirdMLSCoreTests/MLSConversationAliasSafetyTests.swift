import XCTest
import GRDB
import CatbirdMLS

@testable import CatbirdMLSCore

final class MLSConversationAliasSafetyTests: XCTestCase {
  private let userDID = "did:plc:testuser"
  private let rawGroupID = "deadbeef"
  private let stableConversationID = "550e8400-e29b-41d4-a716-446655440000"

  func testRejectsNonCanonicalStableIDWithoutTouchingRawAlias() throws {
    let invalidIDs = [
      "550E8400-E29B-41D4-A716-446655440000", // uppercase, not canonical lowercase
      "550e8400-e29b-11d4-a716-446655440000", // UUIDv1, not UUIDv4
      "550e8400-e29b-41d4-0716-446655440000", // non-RFC4122 variant
      "550e8400e29b41d4a716446655440000", // missing hyphens
    ]

    for invalidID in invalidIDs {
      let database = try makeDatabase()
      try insertConversation(id: rawGroupID, into: database)

      XCTAssertThrowsError(
        try database.write { db in
          _ = try MLSStorageHelpers.ensureConversationExistsSync(
            in: db,
            userDID: userDID,
            conversationID: invalidID,
            groupID: rawGroupID,
            isPlaceholder: false
          )
        },
        "(invalidID) must not be accepted as a canonical stable ID"
      )

      let ids = try database.read { db in
        try MLSConversationModel
          .filter(MLSConversationModel.Columns.currentUserDID == userDID)
          .fetchAll(db)
          .map(\.conversationID)
      }
      XCTAssertEqual(ids, [rawGroupID])
    }
  }

  func testRejectsAmbiguousSameGroupRowsWithoutDeletingUnrelatedStableState() throws {
    let unrelatedStableID = "550e8400-e29b-41d4-a716-446655440001"
    let database = try makeDatabase()
    try insertConversation(id: rawGroupID, into: database)
    try insertConversation(id: stableConversationID, into: database)
    try insertConversation(id: unrelatedStableID, into: database)

    XCTAssertThrowsError(
      try database.write { db in
        _ = try MLSStorageHelpers.ensureConversationExistsSync(
          in: db,
          userDID: userDID,
          conversationID: stableConversationID,
          groupID: rawGroupID,
          isPlaceholder: false
        )
      }
    )

    let ids = try database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .order(MLSConversationModel.Columns.conversationID)
        .fetchAll(db)
        .map(\.conversationID)
    }
    XCTAssertEqual(
      ids,
      [rawGroupID, stableConversationID, unrelatedStableID].sorted()
    )
  }

  func testReadOnlyResolverRejectsDifferentCanonicalWithoutMutation() throws {
    let otherStableID = "550e8400-e29b-41d4-a716-446655440001"
    let database = try makeDatabase()
    try insertConversation(id: rawGroupID, into: database)
    try insertConversation(id: stableConversationID, into: database)
    try insertConversation(id: otherStableID, into: database)

    XCTAssertThrowsError(
      try database.read { db in
        _ = try MLSStorageHelpers.resolveCanonicalConversationIDSync(
          in: db,
          userDID: userDID,
          conversationID: otherStableID,
          groupID: rawGroupID
        )
      }
    )

    let ids = try database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .order(MLSConversationModel.Columns.conversationID)
        .fetchAll(db)
        .map(\.conversationID)
    }
    XCTAssertEqual(ids, [rawGroupID, stableConversationID, otherStableID].sorted())
  }

  func testReadOnlyResolverFailsClosedBeforeStrandingStableParentBesideRawAlias() throws {
    let database = try makeDatabase()
    try insertConversation(id: rawGroupID, into: database)

    XCTAssertThrowsError(
      try database.read { db in
        _ = try MLSStorageHelpers.resolveCanonicalConversationIDSync(
          in: db,
          userDID: userDID,
          conversationID: stableConversationID,
          groupID: rawGroupID
        )
      }
    )

    let ids = try database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .fetchAll(db)
        .map(\.conversationID)
    }
    XCTAssertEqual(ids, [rawGroupID])
  }

  func testRejectsNonExactRawAliasAndPreservesUnrelatedSameGroupRow() throws {
    let lookalikeRawID = "deadbeef00"
    let database = try makeDatabase()
    try insertConversation(id: stableConversationID, into: database)
    try insertConversation(id: lookalikeRawID, into: database)

    XCTAssertThrowsError(
      try database.write { db in
        _ = try MLSStorageHelpers.ensureConversationExistsSync(
          in: db,
          userDID: userDID,
          conversationID: stableConversationID,
          groupID: rawGroupID,
          isPlaceholder: false
        )
      }
    )

    let ids = try database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.currentUserDID == userDID)
        .order(MLSConversationModel.Columns.conversationID)
        .fetchAll(db)
        .map(\.conversationID)
    }
    XCTAssertEqual(ids, [lookalikeRawID, stableConversationID].sorted())
  }

  func testSoleAliasInsertsCanonicalParentBeforeMovingForeignKeyChildren() throws {
    let database = try makeDatabase()
    try insertConversation(id: rawGroupID, into: database)

    try database.write { db in
      try MLSMemberModel(
        memberID: "\(rawGroupID)_did:plc:bob",
        conversationID: rawGroupID,
        currentUserDID: userDID,
        did: "did:plc:bob",
        handle: "bob",
        displayName: "Bob",
        leafIndex: 1,
        credentialData: Data([0x01]),
        signaturePublicKey: Data([0x02]),
        removedAt: Date(timeIntervalSince1970: 1_700_000_000),
        removedBy: "did:plc:former-admin",
        removalReason: "left",
        isActive: false,
        role: .member,
        avatarURL: "https://example.com/bob.png"
      ).insert(db)
      try MLSEpochKeyModel(
        epochKeyID: "\(rawGroupID)-7",
        conversationID: rawGroupID,
        currentUserDID: userDID,
        epoch: 7,
        keyMaterial: Data([0x0a, 0x0b])
      ).insert(db)
      try MLSMessageModel(
        messageID: "\(rawGroupID)-message",
        currentUserDID: userDID,
        conversationID: rawGroupID,
        senderID: "did:plc:bob",
        epoch: 7,
        sequenceNumber: 1,
        isDelivered: true,
        isSent: false
      ).insert(db)
    }

    let effectiveID = try database.write { db in
      try MLSStorageHelpers.ensureConversationExistsSync(
        in: db,
        userDID: userDID,
        conversationID: stableConversationID,
        groupID: rawGroupID,
        isPlaceholder: false
      )
    }
    XCTAssertEqual(effectiveID, stableConversationID)

    let rows = try database.read { db in
      let conversations = try MLSConversationModel.fetchAll(db)
      let members = try MLSMemberModel.fetchAll(db)
      let epochKeys = try MLSEpochKeyModel.fetchAll(db)
      let messages = try MLSMessageModel.fetchAll(db)
      return (conversations, members, epochKeys, messages)
    }

    XCTAssertEqual(rows.0.map(\.conversationID), [stableConversationID])
    XCTAssertEqual(rows.1.map(\.conversationID), [stableConversationID])
    XCTAssertEqual(rows.1.first?.memberID, "\(stableConversationID)_did:plc:bob")
    XCTAssertEqual(rows.2.map(\.conversationID), [stableConversationID])
    XCTAssertEqual(rows.2.first?.keyMaterial, Data([0x0a, 0x0b]))
    XCTAssertEqual(rows.3.map(\.conversationID), [stableConversationID])
  }

  func testEncryptedHistoryRetainsSourceBindingAfterAliasAdoption() throws {
    let tempStorageDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSConversationAliasSafety-\(UUID().uuidString)")
    try FileManager.default.createDirectory(
      at: tempStorageDir,
      withIntermediateDirectories: true
    )
    defer { try? FileManager.default.removeItem(at: tempStorageDir) }

    let context = try MlsContext(
      storagePath: tempStorageDir.appendingPathComponent("context.db").path,
      encryptionKey: String(repeating: "ab", count: 32),
      keychain: InMemoryKeychainAccess()
    )
    try context.setContentRootKey(key: Data(repeating: 0x42, count: 32))
    defer {
      context.clearContentRootKey()
      try? context.flushAndPrepareClose()
    }

    let payload = MLSMessagePayload.text("history survives alias healing")
    let payloadWire = try MLSFieldEncryption.encrypt(
      context: context,
      conversationID: rawGroupID,
      plaintext: try payload.encodeToJSON()
    )
    let entryHMAC = try MLSFieldEncryption.computeHMAC(
      context: context,
      conversationID: rawGroupID,
      previousHMAC: nil,
      messageID: "historical-message",
      payloadWire: payloadWire
    )

    let database = try makeDatabase()
    try insertConversation(id: rawGroupID, into: database)
    try database.write { db in
      try MLSMessageModel(
        messageID: "historical-message",
        currentUserDID: userDID,
        conversationID: rawGroupID,
        senderID: "did:plc:bob",
        epoch: 3,
        sequenceNumber: 1,
        isDelivered: true,
        payloadEncrypted: payloadWire,
        entryHMAC: entryHMAC,
        payloadKeyVersion: 1
      ).insert(db)
      try MLSReactionModel(
        reactionID: "historical-reaction",
        messageID: "historical-message",
        conversationID: rawGroupID,
        currentUserDID: userDID,
        actorDID: "did:plc:alice",
        emoji: "+1",
        action: "add",
        timestamp: Date(timeIntervalSince1970: 1_700_000_001)
      ).insert(db)
    }

    _ = try database.write { db in
      try MLSStorageHelpers.ensureConversationExistsSync(
        in: db,
        userDID: userDID,
        conversationID: stableConversationID,
        groupID: rawGroupID,
        isPlaceholder: false
      )
    }

    let row = try database.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "historical-message")
        .fetchOne(db)
    }
    XCTAssertEqual(row?.conversationID, stableConversationID)
    XCTAssertEqual(row?.cryptoConversationID, rawGroupID)
    XCTAssertEqual(row?.decryptedPayload(context: context)?.text, payload.text)
    XCTAssertTrue(
      try MLSFieldEncryption.verifyHMAC(
        context: context,
        conversationID: row?.cryptoConversationID ?? "",
        previousHMAC: nil,
        messageID: "historical-message",
        payloadWire: payloadWire,
        expected: try XCTUnwrap(row?.entryHMAC)
      )
    )

    let reactions = try database.read { db in
      try MLSReactionModel
        .filter(MLSReactionModel.Columns.messageID == "historical-message")
        .fetchAll(db)
    }
    XCTAssertEqual(reactions.count, 1)
    XCTAssertEqual(reactions.first?.conversationID, stableConversationID)
    XCTAssertEqual(reactions.first?.actorDID, "did:plc:alice")

    // A close/reopen cycle must retain both the source crypto binding and the
    // stable routing key; no plaintext fallback is allowed to mask a failed
    // authenticated decrypt.
    context.clearContentRootKey()
    try context.flushAndPrepareClose()
    let reopened = try MlsContext(
      storagePath: tempStorageDir.appendingPathComponent("context.db").path,
      encryptionKey: String(repeating: "ab", count: 32),
      keychain: InMemoryKeychainAccess()
    )
    try reopened.setContentRootKey(key: Data(repeating: 0x42, count: 32))
    defer {
      reopened.clearContentRootKey()
      try? reopened.flushAndPrepareClose()
    }
    XCTAssertEqual(row?.decryptedPayload(context: reopened)?.text, payload.text)

    // Even if a legacy plaintext copy is present, an authenticated ciphertext
    // row must not silently downgrade to it when decryption fails.
    let rowWithLegacyPlaintext = try XCTUnwrap(row).withPayload(payload)
    reopened.clearContentRootKey()
    XCTAssertNil(rowWithLegacyPlaintext.decryptedPayload(context: reopened))
  }

  func testAliasMigrationRollsBackParentAndChildrenWhenChildMoveFails() throws {
    let database = try makeDatabase()
    try insertConversation(id: rawGroupID, into: database)
    try database.write { db in
      try MLSMemberModel(
        memberID: "\(rawGroupID)_did:plc:bob",
        conversationID: rawGroupID,
        currentUserDID: userDID,
        did: "did:plc:bob",
        leafIndex: 0
      ).insert(db)
      try db.execute(sql: """
        CREATE TRIGGER injected_alias_move_failure
        BEFORE UPDATE OF conversationID ON MLSMemberModel
        WHEN OLD.conversationID = 'deadbeef'
        BEGIN SELECT RAISE(ABORT, 'injected alias migration failure'); END;
        """)
    }

    XCTAssertThrowsError(
      try database.write { db in
        _ = try MLSStorageHelpers.ensureConversationExistsSync(
          in: db,
          userDID: userDID,
          conversationID: stableConversationID,
          groupID: rawGroupID,
          isPlaceholder: false
        )
      }
    )

    let rows = try database.read { db in
      let conversations = try MLSConversationModel.fetchAll(db)
      let members = try MLSMemberModel.fetchAll(db)
      return (conversations, members)
    }
    XCTAssertEqual(rows.0.map(\.conversationID), [rawGroupID])
    XCTAssertEqual(rows.1.map(\.conversationID), [rawGroupID])
  }

  func testUnknownConversationScopedTableFailsClosedBeforeAliasDeletion() throws {
    let database = try makeDatabase()
    try insertConversation(id: rawGroupID, into: database)
    try database.write { db in
      try db.execute(sql: """
        CREATE TABLE unknown_conversation_state (
          stateID TEXT PRIMARY KEY NOT NULL,
          conversationID TEXT NOT NULL,
          payload BLOB
        )
        """)
      try db.execute(
        sql: "INSERT INTO unknown_conversation_state (stateID, conversationID, payload) VALUES (?, ?, ?)",
        arguments: ["state-1", rawGroupID, Data([0x01])]
      )
    }

    XCTAssertThrowsError(
      try database.write { db in
        _ = try MLSStorageHelpers.ensureConversationExistsSync(
          in: db,
          userDID: userDID,
          conversationID: stableConversationID,
          groupID: rawGroupID,
          isPlaceholder: false
        )
      }
    )

    let state = try database.read { db in
      try Row.fetchOne(
        db,
        sql: "SELECT conversationID, payload FROM unknown_conversation_state WHERE stateID = ?",
        arguments: ["state-1"]
      )
    }
    XCTAssertEqual(state?["conversationID"] as String?, rawGroupID)
    XCTAssertEqual(state?["payload"] as Data?, Data([0x01]))
    let ids = try database.read { db in
      try MLSConversationModel.fetchAll(db).map(\.conversationID)
    }
    XCTAssertEqual(ids, [rawGroupID])
  }

  private func makeDatabase() throws -> DatabaseQueue {
    let database = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(database)
    try database.write { db in
      try db.execute(sql: "PRAGMA foreign_keys = ON")
    }
    return database
  }

  private func insertConversation(id: String, into database: DatabaseQueue) throws {
    let now = Date(timeIntervalSince1970: 1_700_000_000)
    try database.write { db in
      try MLSConversationModel(
        conversationID: id,
        currentUserDID: userDID,
        groupID: Data(hexEncoded: rawGroupID)!,
        epoch: 3,
        title: id
      ).insert(db)
    }
    _ = now
  }
}
