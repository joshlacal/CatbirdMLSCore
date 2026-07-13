import XCTest
import GRDB
@testable import CatbirdMLS
@testable import CatbirdMLSCore

final class MLSOrchestratorStorageAdapterTests: XCTestCase {
  private var tempDir: URL!
  private var dbPool: DatabasePool!
  private var context: MlsContext!

  override func setUp() async throws {
    try await super.setUp()

    tempDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSOrchestratorStorageAdapterTests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)

    dbPool = try DatabasePool(path: tempDir.appendingPathComponent("messages.sqlite").path)
    try MLSGRDBManager.makeMigrator().migrate(dbPool)

    context = try MlsContext(
      storagePath: tempDir.appendingPathComponent("openmls.sqlite").path,
      encryptionKey: String(repeating: "cd", count: 32),
      keychain: InMemoryKeychainAccess()
    )
    try context.setContentRootKey(key: Data(repeating: 0x51, count: 32))
  }

  override func tearDown() async throws {
    if let context {
      context.clearContentRootKey()
      try? context.flushAndPrepareClose()
    }
    context = nil
    dbPool = nil
    if let tempDir {
      try? FileManager.default.removeItem(at: tempDir)
    }
    tempDir = nil

    try await super.tearDown()
  }

  func testStoreMessageUsesFieldEncryptedPayloadColumns() throws {
    let userDID = "did:plc:receiver"
    let payload = MLSMessagePayload.text("stored by rust", embed: nil)
    let adapter = try MLSOrchestratorStorageAdapter(
      dbPool: dbPool,
      userDID: userDID,
      mlsContext: context
    )

    try adapter.storeMessage(
      message: FfiMessage(
        id: "server-msg-1",
        conversationId: "convo-rust-store",
        senderDid: "did:plc:sender",
        text: "stored by rust",
        timestamp: "2026-06-22T12:00:00Z",
        epoch: 3,
        sequenceNumber: 7,
        isOwn: false,
        deliveryStatus: nil,
        payloadJson: String(data: try payload.encodeToJSON(), encoding: .utf8)
      )
    )

    let row = try XCTUnwrap(dbPool.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "server-msg-1")
        .filter(MLSMessageModel.Columns.currentUserDID == MLSStorageHelpers.normalizeDID(userDID))
        .fetchOne(db)
    })

    XCTAssertNil(row.payloadJSON)
    XCTAssertNotNil(row.payloadEncrypted)
    XCTAssertNotNil(row.entryHMAC)
    XCTAssertEqual(row.payloadKeyVersion, 1)
    XCTAssertEqual(row.decryptedPayload(context: context)?.text, "stored by rust")
  }

  func testStoreMessagePersistsControlPayloadWithEmptyDisplayText() throws {
    let userDID = "did:plc:receiver"
    let payload = MLSMessagePayload.reaction(
      messageId: "parent-msg-1",
      emoji: "+1",
      action: .add
    )
    let adapter = try MLSOrchestratorStorageAdapter(
      dbPool: dbPool,
      userDID: userDID,
      mlsContext: context
    )

    try adapter.storeMessage(
      message: FfiMessage(
        id: "reaction-msg-1",
        conversationId: "convo-rust-store",
        senderDid: "did:plc:sender",
        text: "",
        timestamp: "2026-06-22T12:00:00Z",
        epoch: 3,
        sequenceNumber: 8,
        isOwn: false,
        deliveryStatus: nil,
        payloadJson: String(data: try payload.encodeToJSON(), encoding: .utf8)
      )
    )

    let row = try XCTUnwrap(dbPool.read { db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == "reaction-msg-1")
        .filter(MLSMessageModel.Columns.currentUserDID == MLSStorageHelpers.normalizeDID(userDID))
        .fetchOne(db)
    })
    let decoded = try XCTUnwrap(row.decryptedPayload(context: context))

    XCTAssertNil(row.payloadJSON)
    XCTAssertNotNil(row.payloadEncrypted)
    XCTAssertEqual(decoded.messageType, .reaction)
    XCTAssertEqual(decoded.reaction?.messageId, "parent-msg-1")
  }

  func testSecurityStateAndReceiptRoundTripLosslessly() throws {
    let adapter = try makeAdapter()
    try adapter.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-security",
      groupId: "01020304"
    )
    try adapter.markResetPending(
      conversationId: "convo-security",
      newGroupIdHex: "05060708",
      resetGeneration: 4,
      notifiedAtMs: 1_752_345_678_800
    )

    let resetState = try XCTUnwrap(adapter.getConversationState(conversationId: "convo-security"))
    XCTAssertEqual(resetState.state, "reset_pending")
    XCTAssertEqual(resetState.newGroupId, "05060708")
    XCTAssertEqual(resetState.resetGeneration, 4)
    XCTAssertEqual(resetState.notifiedAtMs, 1_752_345_678_800)

    try adapter.markQuarantined(
      conversationId: "convo-security",
      reasonTag: "peer_bad_commit",
      sinceMs: 1_752_345_678_901
    )
    let restartedWhileQuarantined = try makeAdapter()
    let quarantined = try XCTUnwrap(
      restartedWhileQuarantined.getConversationState(conversationId: "convo-security")
    )
    XCTAssertEqual(quarantined.state, "quarantined")
    XCTAssertEqual(quarantined.quarantineReason, "peer_bad_commit")
    XCTAssertEqual(quarantined.quarantinedSinceMs, 1_752_345_678_901)

    let receipt = makeReceipt(
      conversationId: "convo-security",
      epoch: 9,
      sequencerTerm: 27
    )
    try adapter.storeSequencerReceipt(receipt: receipt)
    XCTAssertEqual(
      try adapter.getSequencerReceipts(conversationId: "convo-security", sinceEpoch: 9),
      [receipt]
    )

    try adapter.clearSequencerReceipts(conversationId: "convo-security")
    try adapter.clearQuarantine(conversationId: "convo-security")
    try adapter.clearResetPending(conversationId: "convo-security")
    XCTAssertTrue(
      try adapter.getSequencerReceipts(conversationId: "convo-security", sinceEpoch: nil).isEmpty
    )
    let restartedAdapter = try makeAdapter()
    let finalState = try XCTUnwrap(
      restartedAdapter.getConversationState(conversationId: "convo-security")
    )
    XCTAssertEqual(finalState.state, "active")
    XCTAssertNil(finalState.newGroupId)
    XCTAssertNil(finalState.resetGeneration)
    XCTAssertNil(finalState.notifiedAtMs)
    XCTAssertNil(finalState.quarantineReason)
  }

  func testActiveTransitionClearsResetMarkerAndPayloadAcrossRestart() throws {
    let adapter = try makeAdapter()
    try adapter.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-active-transition",
      groupId: "01020304"
    )
    try adapter.markResetPending(
      conversationId: "convo-active-transition",
      newGroupIdHex: "05060708",
      resetGeneration: 5,
      notifiedAtMs: 1_752_345_678_900
    )

    try adapter.setConversationState(
      conversationId: "convo-active-transition",
      state: "active"
    )

    let restartedAdapter = try makeAdapter()
    let state = try XCTUnwrap(
      restartedAdapter.getConversationState(conversationId: "convo-active-transition")
    )
    XCTAssertEqual(state.state, "active")
    XCTAssertNil(state.newGroupId)
    XCTAssertNil(state.resetGeneration)
    XCTAssertNil(state.notifiedAtMs)
  }

  func testUnrecoverableStateHydratesAsRustFailedBeforeActiveOrNeedsRejoin() throws {
    let adapter = try makeAdapter()
    try adapter.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-unrecoverable",
      groupId: "01020304"
    )
    try dbPool.write { db in
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET isActive = 1, needsRejoin = 1, isUnrecoverable = 1
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: ["convo-unrecoverable", "did:plc:receiver"]
      )
    }

    let restartedAdapter = try makeAdapter()
    let state = try XCTUnwrap(
      restartedAdapter.getConversationState(conversationId: "convo-unrecoverable")
    )
    XCTAssertEqual(state.state, "failed")
  }

  func testRustDoesNotEmitErrorConversationStateTag() throws {
    let adapter = try makeAdapter()
    try adapter.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-error-tag",
      groupId: "01020304"
    )

    assertThrowsStorageError("unsupported error tag") {
      try adapter.setConversationState(conversationId: "convo-error-tag", state: "error")
    }
    XCTAssertEqual(
      try adapter.getConversationState(conversationId: "convo-error-tag")?.state,
      "active"
    )
  }

  func testPendingMessageProtectionIsDurableIdempotentAndPrincipalScoped() throws {
    let receiver = try makeAdapter()
    let other = try makeAdapter(userDID: "did:plc:other")
    try receiver.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-receiver",
      groupId: "01020304"
    )
    try other.ensureConversationExists(
      userDid: "did:plc:other",
      conversationId: "convo-other",
      groupId: "05060708"
    )

    try receiver.storePendingMessage(conversationId: "convo-receiver", messageId: "msg-pending")
    try receiver.storePendingMessage(conversationId: "convo-receiver", messageId: "msg-pending")
    try other.storePendingMessage(conversationId: "convo-other", messageId: "msg-pending")

    XCTAssertTrue(try receiver.removePendingMessage(messageId: "msg-pending"))
    XCTAssertFalse(try receiver.removePendingMessage(messageId: "msg-pending"))
    XCTAssertTrue(try other.removePendingMessage(messageId: "msg-pending"))
  }

  func testSecurityStateDoesNotLeakAcrossBoundPrincipals() throws {
    let receiver = try makeAdapter()
    let other = try makeAdapter(userDID: "did:plc:other")
    try receiver.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "receiver-conversation",
      groupId: "01020304"
    )
    try other.ensureConversationExists(
      userDid: "did:plc:other",
      conversationId: "other-conversation",
      groupId: "05060708"
    )

    try receiver.markQuarantined(
      conversationId: "receiver-conversation",
      reasonTag: "receiver_reason",
      sinceMs: 10
    )
    try other.markQuarantined(
      conversationId: "other-conversation",
      reasonTag: "other_reason",
      sinceMs: 20
    )

    XCTAssertEqual(
      try receiver.getConversationState(conversationId: "receiver-conversation")?.quarantineReason,
      "receiver_reason"
    )
    XCTAssertEqual(
      try other.getConversationState(conversationId: "other-conversation")?.quarantineReason,
      "other_reason"
    )
    XCTAssertNil(try receiver.getConversationState(conversationId: "other-conversation"))
    XCTAssertNil(try other.getConversationState(conversationId: "receiver-conversation"))
  }

  func testRejectsCrossPrincipalRequestsAndUnknownConversationSecurityWrites() throws {
    let adapter = try makeAdapter()

    XCTAssertThrowsError(
      try adapter.ensureConversationExists(
        userDid: "did:plc:attacker",
        conversationId: "cross-principal",
        groupId: "01020304"
      )
    )
    XCTAssertThrowsError(try adapter.listConversations(userDid: "did:plc:attacker"))
    XCTAssertThrowsError(
      try adapter.markQuarantined(
        conversationId: "missing-conversation",
        reasonTag: "peer_bad_commit",
        sinceMs: 1
      )
    )
    XCTAssertThrowsError(
      try adapter.storePendingMessage(
        conversationId: "missing-conversation",
        messageId: "missing-message"
      )
    )
    XCTAssertThrowsError(
      try adapter.storeSequencerReceipt(
        receipt: makeReceipt(
          conversationId: "missing-conversation",
          epoch: 1,
          sequencerTerm: 1
        )
      )
    )
    XCTAssertThrowsError(
      try adapter.markPendingLocalDelete(
        conversationId: "missing-conversation",
        groupIdHex: "01020304"
      )
    )
  }

  func testPendingLocalDeleteRejectsEmptyOptionalGroupId() throws {
    let adapter = try makeAdapter()
    try adapter.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-pending-delete",
      groupId: "01020304"
    )

    for malformed in ["", " ", "\t\n"] {
      assertThrowsStorageError("empty optional group id") {
        try adapter.markPendingLocalDelete(
          conversationId: "convo-pending-delete",
          groupIdHex: malformed
        )
      }
    }
  }

  func testReceiptReplayRequiresEverySignedFieldToMatch() throws {
    let adapter = try makeAdapter()
    try adapter.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-replay",
      groupId: "01020304"
    )
    let receipt = makeReceipt(
      conversationId: "convo-replay",
      epoch: 3,
      sequencerTerm: 11
    )
    try adapter.storeSequencerReceipt(receipt: receipt)
    try adapter.storeSequencerReceipt(receipt: receipt)

    let conflictingTerm = makeReceipt(
      conversationId: receipt.convoId,
      epoch: receipt.epoch,
      sequencerTerm: receipt.sequencerTerm + 1
    )
    XCTAssertThrowsError(try adapter.storeSequencerReceipt(receipt: conflictingTerm))

    let conflictingHash = makeReceipt(
      conversationId: receipt.convoId,
      epoch: receipt.epoch,
      sequencerTerm: receipt.sequencerTerm,
      commitHash: Data(repeating: 0x44, count: 32)
    )
    XCTAssertThrowsError(try adapter.storeSequencerReceipt(receipt: conflictingHash))
    XCTAssertEqual(
      try adapter.getSequencerReceipts(conversationId: receipt.convoId, sinceEpoch: nil),
      [receipt]
    )
  }

  func testRejectsMalformedSequencerReceipts() throws {
    let adapter = try makeAdapter()
    try adapter.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-malformed",
      groupId: "01020304"
    )
    let malformed: [FfiSequencerReceipt] = [
      makeReceipt(
        conversationId: "convo-malformed",
        epoch: -1,
        sequencerTerm: 1
      ),
      makeReceipt(
        conversationId: "convo-malformed",
        epoch: 1,
        sequencerTerm: UInt64(Int64.max) + 1
      ),
      makeReceipt(
        conversationId: "convo-malformed",
        epoch: 1,
        sequencerTerm: 1,
        commitHash: Data([0x01])
      ),
      makeReceipt(
        conversationId: "convo-malformed",
        epoch: 1,
        sequencerTerm: 1,
        sequencerDID: "not-a-did"
      ),
      makeReceipt(
        conversationId: "convo-malformed",
        epoch: 1,
        sequencerTerm: 1,
        issuedAt: -1
      ),
      makeReceipt(
        conversationId: "convo-malformed",
        epoch: 1,
        sequencerTerm: 1,
        signature: Data([0x01])
      ),
    ]

    for receipt in malformed {
      XCTAssertThrowsError(try adapter.storeSequencerReceipt(receipt: receipt))
    }
    XCTAssertTrue(
      try adapter.getSequencerReceipts(conversationId: "convo-malformed", sinceEpoch: nil).isEmpty
    )
  }

  func testClearingQuarantinePreservesCoexistingResetState() throws {
    let adapter = try makeAdapter()
    try adapter.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-coexist",
      groupId: "01020304"
    )
    try adapter.markResetPending(
      conversationId: "convo-coexist",
      newGroupIdHex: "05060708",
      resetGeneration: 12,
      notifiedAtMs: 1_752_345_679_000
    )
    try adapter.markQuarantined(
      conversationId: "convo-coexist",
      reasonTag: "multi_peer_bad_commits",
      sinceMs: 1_752_345_679_100
    )
    try adapter.clearQuarantine(conversationId: "convo-coexist")

    let reset = try XCTUnwrap(adapter.getConversationState(conversationId: "convo-coexist"))
    XCTAssertEqual(reset.state, "reset_pending")
    XCTAssertEqual(reset.newGroupId, "05060708")
    XCTAssertEqual(reset.resetGeneration, 12)
    XCTAssertEqual(reset.notifiedAtMs, 1_752_345_679_000)
    XCTAssertNil(reset.quarantineReason)

    try adapter.markQuarantined(
      conversationId: "convo-coexist",
      reasonTag: "multi_peer_bad_commits",
      sinceMs: 1_752_345_679_200
    )
    try adapter.clearResetPending(conversationId: "convo-coexist")
    let quarantined = try XCTUnwrap(adapter.getConversationState(conversationId: "convo-coexist"))
    XCTAssertEqual(quarantined.state, "quarantined")
    XCTAssertEqual(quarantined.quarantineReason, "multi_peer_bad_commits")
    XCTAssertEqual(quarantined.quarantinedSinceMs, 1_752_345_679_200)
    XCTAssertNil(quarantined.notifiedAtMs)
  }

  func testResetReplayMustMatchAndGenerationMustAdvanceMonotonically() throws {
    let adapter = try makeAdapter()
    try adapter.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-reset-replay",
      groupId: "01020304"
    )
    try adapter.markResetPending(
      conversationId: "convo-reset-replay",
      newGroupIdHex: "05060708",
      resetGeneration: 4,
      notifiedAtMs: 100
    )
    try adapter.markResetPending(
      conversationId: "convo-reset-replay",
      newGroupIdHex: "05060708",
      resetGeneration: 4,
      notifiedAtMs: 100
    )
    XCTAssertThrowsError(
      try adapter.markResetPending(
        conversationId: "convo-reset-replay",
        newGroupIdHex: "090a0b0c",
        resetGeneration: 4,
        notifiedAtMs: 100
      )
    )
    XCTAssertThrowsError(
      try adapter.markResetPending(
        conversationId: "convo-reset-replay",
        newGroupIdHex: "01010101",
        resetGeneration: 3,
        notifiedAtMs: 90
      )
    )

    let state = try XCTUnwrap(
      adapter.getConversationState(conversationId: "convo-reset-replay")
    )
    XCTAssertEqual(state.newGroupId, "05060708")
    XCTAssertEqual(state.resetGeneration, 4)
    XCTAssertEqual(state.notifiedAtMs, 100)
  }

  func testEqualResetGenerationBackfillsNotificationForLegacyPayload() throws {
    let adapter = try makeAdapter()
    try adapter.ensureConversationExists(
      userDid: "did:plc:receiver",
      conversationId: "convo-reset-upgrade",
      groupId: "01020304"
    )
    try dbPool.write { db in
      try db.execute(
        sql: """
          UPDATE MLSConversationModel
          SET needsReset = 1,
              pendingNewGroupId = '05060708',
              pendingResetGeneration = 7
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: ["convo-reset-upgrade", "did:plc:receiver"]
      )
    }

    try adapter.markResetPending(
      conversationId: "convo-reset-upgrade",
      newGroupIdHex: "05060708",
      resetGeneration: 7,
      notifiedAtMs: 700
    )
    try adapter.markResetPending(
      conversationId: "convo-reset-upgrade",
      newGroupIdHex: "05060708",
      resetGeneration: 7,
      notifiedAtMs: 700
    )

    let restartedAdapter = try makeAdapter()
    let state = try XCTUnwrap(
      restartedAdapter.getConversationState(conversationId: "convo-reset-upgrade")
    )
    XCTAssertEqual(state.state, "reset_pending")
    XCTAssertEqual(state.newGroupId, "05060708")
    XCTAssertEqual(state.resetGeneration, 7)
    XCTAssertEqual(state.notifiedAtMs, 700)

    assertThrowsStorageError("conflicting reset notification") {
      try restartedAdapter.markResetPending(
        conversationId: "convo-reset-upgrade",
        newGroupIdHex: "05060708",
        resetGeneration: 7,
        notifiedAtMs: 701
      )
    }
  }

  func testRecoveryHydrationRejectsNegativeCountersAndTimestamps() throws {
    let adapter = try makeAdapter()
    let malformedRows: [(String, String)] = [
      (
        "negative failed rejoin count",
        """
        INSERT INTO MLSRecoveryAttemptStateModel
          (conversationID, currentUserDID, failedRejoinCount, lastAttemptAtMs, quarantinedUntilMs)
        VALUES ('convo-recovery', 'did:plc:receiver', -1, 100, 200)
        """
      ),
      (
        "negative last attempt timestamp",
        """
        INSERT INTO MLSRecoveryAttemptStateModel
          (conversationID, currentUserDID, failedRejoinCount, lastAttemptAtMs, quarantinedUntilMs)
        VALUES ('convo-recovery', 'did:plc:receiver', 1, -1, 200)
        """
      ),
      (
        "negative quarantine timestamp",
        """
        INSERT INTO MLSRecoveryAttemptStateModel
          (conversationID, currentUserDID, failedRejoinCount, lastAttemptAtMs, quarantinedUntilMs)
        VALUES ('convo-recovery', 'did:plc:receiver', 1, 100, -1)
        """
      ),
      (
        "negative global timestamp",
        """
        INSERT INTO MLSRecoveryGlobalStateModel
          (currentUserDID, lastGlobalRejoinAttemptAtMs)
        VALUES ('did:plc:receiver', -1)
        """
      ),
    ]

    for (label, insertSQL) in malformedRows {
      try dbPool.write { db in
        try db.execute(sql: "DELETE FROM MLSRecoveryAttemptStateModel")
        try db.execute(sql: "DELETE FROM MLSRecoveryGlobalStateModel")
        try db.execute(sql: insertSQL)
      }
      assertThrowsStorageError(label) {
        _ = try adapter.getRecoveryState()
      }
    }
  }

  func testConstructionFailsWhenRequiredSchemaCannotBeCreated() throws {
    let path = tempDir.appendingPathComponent("read-only.sqlite").path
    do {
      let writable = try DatabaseQueue(path: path)
      try writable.write { db in
        try db.execute(sql: "CREATE TABLE sentinel (id INTEGER PRIMARY KEY)")
      }
    }
    var configuration = Configuration()
    configuration.readonly = true
    let readOnlyPool = try DatabasePool(path: path, configuration: configuration)

    XCTAssertThrowsError(try makeAdapter(pool: readOnlyPool))
  }

  func testConstructionRejectsMalformedBoundPrincipal() throws {
    XCTAssertThrowsError(try makeAdapter(userDID: "not-a-did"))
  }

  func testConstructionRejectsIncompleteSecuritySchema() throws {
    let pool = try DatabasePool(
      path: tempDir.appendingPathComponent("incomplete-security-schema.sqlite").path
    )
    try MLSGRDBManager.makeMigrator().migrate(pool)
    try pool.write { db in
      try db.execute(sql: """
        CREATE TABLE mls_orchestrator_sequencer_receipts (
          conversation_id TEXT NOT NULL,
          user_did TEXT NOT NULL,
          epoch INTEGER NOT NULL,
          commit_hash BLOB NOT NULL,
          sequencer_did TEXT NOT NULL,
          issued_at INTEGER NOT NULL,
          signature BLOB NOT NULL
        )
        """)
    }

    XCTAssertThrowsError(try makeAdapter(pool: pool))
  }

  private func makeAdapter(
    pool: DatabasePool? = nil,
    userDID: String = "did:plc:receiver"
  ) throws -> MLSOrchestratorStorageAdapter {
    try MLSOrchestratorStorageAdapter(
      dbPool: pool ?? dbPool,
      userDID: userDID,
      mlsContext: context
    )
  }

  private func makeReceipt(
    conversationId: String,
    epoch: Int32,
    sequencerTerm: UInt64,
    commitHash: Data = Data(repeating: 0x11, count: 32),
    sequencerDID: String = "did:web:sequencer.example",
    issuedAt: Int64 = 1_752_345_678_999,
    signature: Data = Data(repeating: 0x22, count: 64)
  ) -> FfiSequencerReceipt {
    FfiSequencerReceipt(
      convoId: conversationId,
      epoch: epoch,
      sequencerTerm: sequencerTerm,
      commitHash: commitHash,
      sequencerDid: sequencerDID,
      issuedAt: issuedAt,
      signature: signature
    )
  }

  private func assertThrowsStorageError(
    _ label: String,
    file: StaticString = #filePath,
    line: UInt = #line,
    _ body: () throws -> Void
  ) {
    XCTAssertThrowsError(try body(), label, file: file, line: line) { error in
      guard case OrchestratorBridgeError.Storage = error else {
        return XCTFail("Expected typed storage error, got \(error)", file: file, line: line)
      }
    }
  }
}
