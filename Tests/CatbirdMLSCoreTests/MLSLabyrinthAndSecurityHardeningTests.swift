import CryptoKit
import Foundation
import GRDB
import Security
import Synchronization
import XCTest
@testable import CatbirdMLS
@testable import CatbirdMLSCore
@testable import Petrel

extension CatbirdMLS.MlsContext: @unchecked Sendable {}

final class MLSLabyrinthAndSecurityHardeningTests: XCTestCase {
  private var dbQueue: DatabaseQueue!
  private var mlsContext: MlsContext!
  private var tempDir: URL!

  private let currentUserDID = "did:plc:selfuser"
  private let aliceDID = "did:plc:alice"
  private let bobDID = "did:plc:bob"

  override func setUp() async throws {
    try await super.setUp()
    tempDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSLabyrinthSecurityHardening-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)

    dbQueue = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(dbQueue)

    mlsContext = try MlsContext(
      storagePath: tempDir.appendingPathComponent("ctx.db").path,
      encryptionKey: String(repeating: "cd", count: 32),
      keychain: InMemoryKeychainAccess()
    )
    try mlsContext.setContentRootKey(key: Data(repeating: 0x5A, count: 32))
  }

  override func tearDown() async throws {
    if let mlsContext {
      mlsContext.clearContentRootKey()
      try? mlsContext.flushAndPrepareClose()
    }
    mlsContext = nil
    dbQueue = nil
    if let tempDir {
      let trashRoot = FileManager.default.temporaryDirectory
        .appendingPathComponent("_trash-MLSLabyrinthSecurityHardening", isDirectory: true)
      try? FileManager.default.createDirectory(at: trashRoot, withIntermediateDirectories: true)
      try? FileManager.default.moveItem(
        at: tempDir,
        to: trashRoot.appendingPathComponent(tempDir.lastPathComponent)
      )
    }
    tempDir = nil
    try await super.tearDown()
  }

  // MARK: - Helpers

  private func insertConversation(
    id conversationID: String,
    epoch: Int64 = 1
  ) async throws {
    let now = Date(timeIntervalSince1970: 1_750_000_000)
    try await dbQueue.write { [currentUserDID] db in
      try db.execute(
        sql: """
          INSERT OR IGNORE INTO MLSConversationModel (
            conversationID, currentUserDID, groupID, epoch, createdAt, updatedAt
          ) VALUES (?, ?, ?, ?, ?, ?)
          """,
        arguments: [
          conversationID,
          MLSStorageHelpers.normalizeDID(currentUserDID),
          Data(conversationID.utf8),
          epoch,
          now,
          now,
        ]
      )
    }
  }

  private func insertEncryptedTextMessage(
    messageID: String,
    conversationID: String,
    senderDID: String,
    text: String,
    epoch: Int64 = 1,
    seq: Int64 = 1
  ) async throws {
    try await insertConversation(id: conversationID, epoch: epoch)
    let payloadData = try MLSMessagePayload.text(text).encodeToJSON()
    let encrypted = try MLSFieldEncryption.encrypt(
      context: mlsContext,
      conversationID: conversationID,
      plaintext: payloadData
    )
    let hmac = try MLSFieldEncryption.computeHMAC(
      context: mlsContext,
      conversationID: conversationID,
      previousHMAC: nil,
      messageID: messageID,
      payloadWire: encrypted
    )
    let normalizedUserDID = MLSStorageHelpers.normalizeDID(currentUserDID)
    let model = MLSMessageModel(
      messageID: messageID,
      currentUserDID: normalizedUserDID,
      conversationID: conversationID,
      cryptoConversationID: conversationID,
      senderID: MLSStorageHelpers.normalizeDID(senderDID),
      epoch: epoch,
      sequenceNumber: seq,
      isDelivered: true,
      payloadEncrypted: encrypted,
      entryHMAC: hmac,
      payloadKeyVersion: 1
    )
    try await dbQueue.write { db in
      try model.insert(db)
    }
  }

  // MARK: - A-1: Cross-Conversation Edit, Tombstone, and Orphan Isolation

  func testApplyEditFromDifferentConversationFailsClosedWithoutMutatingTarget() async throws {
    let convoA = "convo-a"
    let convoB = "convo-b"
    let targetMessageID = "msg-in-convo-a"

    try await insertEncryptedTextMessage(
      messageID: targetMessageID,
      conversationID: convoA,
      senderDID: aliceDID,
      text: "original secret in convo A"
    )
    try await insertConversation(id: convoB)

    let result = try await MLSStorage.shared.applyEdit(
      conversationID: convoB,
      targetMessageID: targetMessageID,
      newText: "spoofed cross-convo edit",
      editorUserDID: aliceDID,
      editSeq: 99,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )

    XCTAssertEqual(result, .dropped, "Cross-conversation edit must fail closed as .dropped")

    let stored = try await dbQueue.read { [currentUserDID] db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == targetMessageID)
        .filter(MLSMessageModel.Columns.currentUserDID == MLSStorageHelpers.normalizeDID(currentUserDID))
        .fetchOne(db)
    }
    let unwrapped = try XCTUnwrap(stored)
    XCTAssertEqual(unwrapped.isEdited, 0)
    XCTAssertEqual(unwrapped.decryptedPayload(context: mlsContext)?.text, "original secret in convo A")
  }

  func testApplyTombstoneFromDifferentConversationFailsClosedWithoutDeletingTarget() async throws {
    let convoA = "convo-a"
    let convoB = "convo-b"
    let targetMessageID = "msg-in-convo-a-delete"

    try await insertEncryptedTextMessage(
      messageID: targetMessageID,
      conversationID: convoA,
      senderDID: aliceDID,
      text: "must survive cross-convo delete"
    )
    try await insertConversation(id: convoB)

    let result = try await MLSStorage.shared.applyTombstone(
      conversationID: convoB,
      targetMessageID: targetMessageID,
      senderUserDID: aliceDID,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )

    XCTAssertEqual(result, .dropped, "Cross-conversation tombstone must fail closed as .dropped")

    let stored = try await dbQueue.read { [currentUserDID] db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == targetMessageID)
        .filter(MLSMessageModel.Columns.currentUserDID == MLSStorageHelpers.normalizeDID(currentUserDID))
        .fetchOne(db)
    }
    let unwrapped = try XCTUnwrap(stored)
    XCTAssertEqual(unwrapped.isTombstone, 0)
    XCTAssertEqual(unwrapped.decryptedPayload(context: mlsContext)?.text, "must survive cross-convo delete")
  }

  func testOrphanedMutationsDoNotCrossConversationsOnSaveOrAdoption() async throws {
    let convoA = "convo-orphan-a"
    let convoB = "convo-orphan-b"
    let targetMessageID = "delayed-msg-convo-a"

    try await insertConversation(id: convoA)
    try await insertConversation(id: convoB)

    // Attacker (or same sender) in convoB parks an orphaned delete + edit before convoA's message arrives.
    _ = try await MLSStorage.shared.applyOrPersistOrphanedDelete(
      conversationID: convoB,
      targetMessageID: targetMessageID,
      senderUserDID: aliceDID,
      deleteSeq: 50,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )

    // Legitimate edit orphan in convoA must NOT be suppressed by convoB's delete orphan.
    _ = try await MLSStorage.shared.applyOrPersistOrphanedEdit(
      conversationID: convoA,
      targetMessageID: targetMessageID,
      newText: "legitimate edit in convo A",
      editorUserDID: aliceDID,
      editSeq: 10,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )

    // Message arrives in convoA.
    try await insertEncryptedTextMessage(
      messageID: targetMessageID,
      conversationID: convoA,
      senderDID: aliceDID,
      text: "initial text in convo A",
      epoch: 2,
      seq: 5
    )

    let adopted = try await MLSStorage.shared.adoptOrphanedMutationsForMessage(
      targetMessageID,
      conversationID: convoA,
      currentUserDID: currentUserDID,
      context: mlsContext,
      database: dbQueue
    )

    XCTAssertEqual(adopted.map(\.kind), [.edit], "convoB delete orphan must not adopt onto convoA message")

    let stored = try await dbQueue.read { [currentUserDID] db in
      try MLSMessageModel
        .filter(MLSMessageModel.Columns.messageID == targetMessageID)
        .filter(MLSMessageModel.Columns.currentUserDID == MLSStorageHelpers.normalizeDID(currentUserDID))
        .fetchOne(db)
    }
    let unwrapped = try XCTUnwrap(stored)
    XCTAssertEqual(unwrapped.isTombstone, 0)
    XCTAssertEqual(unwrapped.isEdited, 1)
    XCTAssertEqual(unwrapped.decryptedPayload(context: mlsContext)?.text, "legitimate edit in convo A")
  }

  // MARK: - A-1: Recovery Request & Incoming Recovery Response Hardening

  func testRecoveryRequestValidationRejectsCrossConvoNonSelfNonTextPreJoinAndInjectedMessageIds() async throws {
    let convoA = "convo-rec-a"
    let convoB = "convo-rec-b"
    try await insertConversation(id: convoA, epoch: 5)
    try await insertConversation(id: convoB, epoch: 5)

    // Register Alice as an active member in convoA and convoB, joined at epoch 5.
    try await dbQueue.write { [currentUserDID, aliceDID] db in
      let normalizedUser = MLSStorageHelpers.normalizeDID(currentUserDID)
      try MLSMemberModel(
        memberID: "\(convoA)_\(aliceDID)",
        conversationID: convoA,
        currentUserDID: normalizedUser,
        did: aliceDID,
        leafIndex: 1,
        isActive: true
      ).insert(db)
      try MLSMemberModel(
        memberID: "\(convoB)_\(aliceDID)",
        conversationID: convoB,
        currentUserDID: normalizedUser,
        did: aliceDID,
        leafIndex: 1,
        isActive: true
      ).insert(db)
      try MLSMembershipEventModel(
        conversationID: convoA,
        currentUserDID: normalizedUser,
        memberDID: aliceDID,
        eventType: .joined,
        epoch: 5
      ).insert(db)
    }

    // 1. Self-authored message in convoA at epoch 5 (valid when requested in convoA by Alice).
    try await insertEncryptedTextMessage(
      messageID: "self-msg-epoch-5",
      conversationID: convoA,
      senderDID: currentUserDID,
      text: "self authored at epoch 5",
      epoch: 5,
      seq: 10
    )

    // 2. Third-party message authored by Bob in convoA at epoch 5 (must NEVER be re-sent by self).
    try await insertEncryptedTextMessage(
      messageID: "bob-msg-epoch-5",
      conversationID: convoA,
      senderDID: bobDID,
      text: "bob secret message",
      epoch: 5,
      seq: 11
    )

    // 3. Pre-join self-authored message in convoA at epoch 2 (before Alice joined at epoch 5).
    try await insertEncryptedTextMessage(
      messageID: "self-msg-prejoin-epoch-2",
      conversationID: convoA,
      senderDID: currentUserDID,
      text: "pre-join secret history",
      epoch: 2,
      seq: 2
    )

    // Valid candidate in convoA at epoch 5 succeeds:
    let validCandidate = await MLSConversationManager.validateAndFetchRecoveryCandidate(
      messageId: "self-msg-epoch-5",
      conversationId: convoA,
      requesterDID: aliceDID,
      currentUserDID: currentUserDID,
      mlsContext: mlsContext,
      currentVerifiedEpoch: 5,
      database: dbQueue
    )
    XCTAssertEqual(validCandidate?.payload.text, "self authored at epoch 5")

    // Cross-conversation exfiltration attempt from convoB targeting convoA message MUST fail:
    let crossConvo = await MLSConversationManager.validateAndFetchRecoveryCandidate(
      messageId: "self-msg-epoch-5",
      conversationId: convoB,
      requesterDID: aliceDID,
      currentUserDID: currentUserDID,
      mlsContext: mlsContext,
      currentVerifiedEpoch: 5,
      database: dbQueue
    )
    XCTAssertNil(crossConvo, "Cross-conversation recovery request must be rejected")

    // Third-party authored message MUST fail:
    let nonSelf = await MLSConversationManager.validateAndFetchRecoveryCandidate(
      messageId: "bob-msg-epoch-5",
      conversationId: convoA,
      requesterDID: aliceDID,
      currentUserDID: currentUserDID,
      mlsContext: mlsContext,
      currentVerifiedEpoch: 5,
      database: dbQueue
    )
    XCTAssertNil(nonSelf, "Recovery request for third-party message must be rejected")

    // Pre-join epoch message MUST fail:
    let preJoin = await MLSConversationManager.validateAndFetchRecoveryCandidate(
      messageId: "self-msg-prejoin-epoch-2",
      conversationId: convoA,
      requesterDID: aliceDID,
      currentUserDID: currentUserDID,
      mlsContext: mlsContext,
      currentVerifiedEpoch: 5,
      database: dbQueue
    )
    XCTAssertNil(preJoin, "Recovery request for pre-join epoch must be rejected")

    // Non-member requester MUST fail:
    let nonMember = await MLSConversationManager.validateAndFetchRecoveryCandidate(
      messageId: "self-msg-epoch-5",
      conversationId: convoA,
      requesterDID: "did:plc:mallory",
      currentUserDID: currentUserDID,
      mlsContext: mlsContext,
      currentVerifiedEpoch: 5,
      database: dbQueue
    )
    XCTAssertNil(nonMember, "Non-member recovery request must be rejected")

    // SQL wildcard / JSON quote / backslash / overlong messageId MUST fail validation:
    for badId in ["", "%", "msg_wildcard", "msg\"quote", "msg\\slash", String(repeating: "a", count: 129)] {
      XCTAssertFalse(
        MLSConversationManager.isValidRecoveryMessageId(badId),
        "Malformed messageId \(badId) must be rejected"
      )
    }

    // Wildcard injection in hasExistingRecoveryResponse must not match unrelated recovered messages:
    let realRecoveryPayload = try MLSMessagePayload(
      messageType: .text,
      text: "recovered content",
      recoveredMessageId: "legit-target-id"
    ).encodeToJSON()
    try await dbQueue.write { [currentUserDID] db in
      try MLSMessageModel(
        messageID: "recovery-carrier-1",
        currentUserDID: MLSStorageHelpers.normalizeDID(currentUserDID),
        conversationID: convoA,
        senderID: MLSStorageHelpers.normalizeDID(currentUserDID),
        payloadJSON: realRecoveryPayload,
        epoch: 5,
        sequenceNumber: 20
      ).insert(db)
    }
    let exactMatch = await MLSConversationManager.hasExistingRecoveryResponse(
      forMessageId: "legit-target-id",
      conversationId: convoA,
      currentUserDID: currentUserDID,
      mlsContext: mlsContext,
      database: dbQueue
    )
    XCTAssertTrue(exactMatch)

    let prefixWildcardMatch = await MLSConversationManager.hasExistingRecoveryResponse(
      forMessageId: "legit-%",
      conversationId: convoB,
      currentUserDID: currentUserDID,
      mlsContext: mlsContext,
      database: dbQueue
    )
    // Invalid messageId fails closed (returns true to suppress sending) while valid non-matching ID returns false:
    XCTAssertTrue(prefixWildcardMatch)
    let nonMatch = await MLSConversationManager.hasExistingRecoveryResponse(
      forMessageId: "legit-target",
      conversationId: convoA,
      currentUserDID: currentUserDID,
      mlsContext: mlsContext,
      database: dbQueue
    )
    XCTAssertFalse(nonMatch)
  }

  func testIncomingRecoveryResponseRequiresSameConversationAndMatchingOriginalSender() async throws {
    let convoA = "convo-in-rec-a"
    let convoB = "convo-in-rec-b"
    try await insertEncryptedTextMessage(
      messageID: "failed-alice-msg",
      conversationID: convoA,
      senderDID: aliceDID,
      text: "",
      epoch: 3,
      seq: 7
    )
    try await insertConversation(id: convoB, epoch: 3)

    // Matching conversation + matching sender root DID (even from sibling device) -> valid:
    let validSibling = await MLSConversationManager.isValidIncomingRecoveryResponse(
      recoveredMessageId: "failed-alice-msg",
      conversationId: convoA,
      senderDID: "\(aliceDID)#device-2",
      currentUserDID: currentUserDID,
      database: dbQueue
    )
    XCTAssertTrue(validSibling)

    // Wrong conversation -> rejected:
    let wrongConvo = await MLSConversationManager.isValidIncomingRecoveryResponse(
      recoveredMessageId: "failed-alice-msg",
      conversationId: convoB,
      senderDID: aliceDID,
      currentUserDID: currentUserDID,
      database: dbQueue
    )
    XCTAssertFalse(wrongConvo)

    // Wrong sender (Bob claiming to recover Alice's message) -> rejected:
    let wrongSender = await MLSConversationManager.isValidIncomingRecoveryResponse(
      recoveredMessageId: "failed-alice-msg",
      conversationId: convoA,
      senderDID: bobDID,
      currentUserDID: currentUserDID,
      database: dbQueue
    )
    XCTAssertFalse(wrongSender)
  }

  // MARK: - A-2: MLSEpochCheckpoint Path Traversal & Hashed Directory Containment

  func testEpochCheckpointRejectsPathTraversalAndKeysDirectoriesByDIDHash() async throws {
    let baseOverride = tempDir.appendingPathComponent("container-root", isDirectory: true)
    try FileManager.default.createDirectory(at: baseOverride, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(baseOverride)
    defer { MLSStoragePaths.setBaseDirectoryOverride(nil) }

    let checkpointsRoot = try MLSStoragePaths.checkpointsDirectory()
    try FileManager.default.createDirectory(at: checkpointsRoot, withIntermediateDirectories: true)

    // Create canary directory as a sibling of epoch-checkpoints-<suffix> inside container-root.
    // Unhashed "did:plc:abc/../../canary_dir" appended to checkpointsRoot resolves to baseOverride/canary_dir!
    let canaryDir = baseOverride.appendingPathComponent("canary_dir", isDirectory: true)
    try FileManager.default.createDirectory(at: canaryDir, withIntermediateDirectories: true)
    let canaryFile = canaryDir.appendingPathComponent("do-not-touch.txt")
    try Data("canary".utf8).write(to: canaryFile)

    // Pre-create intermediate segment so `checkpointsRoot/did:plc:abc/../../canary_dir` would
    // resolve on the real filesystem if path traversal were ever attempted.
    try FileManager.default.createDirectory(
      at: checkpointsRoot.appendingPathComponent("did:plc:abc", isDirectory: true),
      withIntermediateDirectories: true
    )

    let traversalDID = "did:plc:abc/../../canary_dir"
    let checkpoint = MLSEpochCheckpoint.shared

    XCTAssertFalse(MLSEpochCheckpoint.isSafeLegacyDirectoryComponent(MLSStoragePaths.normalizeDID(traversalDID)))
    XCTAssertTrue(MLSEpochCheckpoint.isSafeLegacyDirectoryComponent("did:plc:legacy_user-1.example"))

    let resolvedDir = try await checkpoint.checkpointDirectory(for: traversalDID)
    let expectedHash = MLSStoragePaths.didHash(for: traversalDID)
    let expectedDir = checkpointsRoot.appendingPathComponent(expectedHash, isDirectory: true)
    XCTAssertEqual(resolvedDir.standardizedFileURL, expectedDir.standardizedFileURL)
    XCTAssertTrue(
      resolvedDir.standardizedFileURL.path.hasPrefix(checkpointsRoot.standardizedFileURL.path + "/")
    )

    // 1. clearAllCheckpoints must NEVER delete canary_dir outside checkpointsRoot.
    try await checkpoint.clearAllCheckpoints(userDID: traversalDID)
    await checkpoint.resumeWrites(userDID: traversalDID)
    XCTAssertTrue(
      FileManager.default.fileExists(atPath: canaryFile.path),
      "clearAllCheckpoints must never traverse outside epoch_checkpoints to delete canary_dir"
    )

    // 2. Recording an epoch must persist strictly inside checkpointsRoot/<sha256(normalizedDID)>.
    let fakeKeychain = MLSKeychainFakeStorage()
    MLSKeychainManager.setFakeStorageOverrideForTesting(fakeKeychain)
    defer { MLSKeychainManager.setFakeStorageOverrideForTesting(nil) }

    let groupData = Data([0xde, 0xad, 0xbe, 0xef])
    await checkpoint.recordEpoch(userDID: traversalDID, groupId: groupData, epoch: 7, isNSE: false)
    try await Task.sleep(nanoseconds: 100_000_000)

    let expectedFile = expectedDir.appendingPathComponent("deadbeef.json")
    XCTAssertTrue(
      FileManager.default.fileExists(atPath: expectedFile.path),
      "Epoch checkpoint must be written under checkpointsRoot/<sha256>, not raw normalizedDID"
    )
    XCTAssertTrue(
      FileManager.default.fileExists(atPath: canaryFile.path),
      "canary_dir must remain completely untouched after recordEpoch"
    )

    // 3. Safe legacy directory cleanup works while keeping canary untouched.
    let legacyDID = "did:plc:legacy_safe_user"
    let legacyDir = checkpointsRoot.appendingPathComponent(legacyDID, isDirectory: true)
    try FileManager.default.createDirectory(at: legacyDir, withIntermediateDirectories: true)
    try Data("{}".utf8).write(to: legacyDir.appendingPathComponent("0102.json"))
    try await checkpoint.clearAllCheckpoints(userDID: legacyDID)
    await checkpoint.resumeWrites(userDID: legacyDID)
    XCTAssertFalse(FileManager.default.fileExists(atPath: legacyDir.path))

    // Cleanup via clearAllCheckpoints removes hashed dir and still leaves canary untouched.
    try await checkpoint.clearAllCheckpoints(userDID: traversalDID)
    await checkpoint.resumeWrites(userDID: traversalDID)
    XCTAssertFalse(FileManager.default.fileExists(atPath: expectedDir.path))
    XCTAssertTrue(FileManager.default.fileExists(atPath: canaryFile.path))
  }

  // MARK: - A-3 & A-5: Outbound HTTP SSRF, NSID Allowlist, & Query Encoding

  func testSSRFValidationRejectsPrivateLoopbackLocalNonDefaultPortAndFragmentURLs() throws {
    let blockedEndpoints = [
      "https://127.0.0.1",
      "https://192.168.1.1",
      "https://10.0.0.1",
      "https://172.16.0.1",
      "https://169.254.169.254",
      "https://100.64.0.1",
      "https://0.0.0.0",
      "https://[::1]",
      "https://[fe80::1]",
      "https://[fc00::1]",
      "https://[::ffff:127.0.0.1]",
      "https://localhost",
      "https://printer.local",
      "https://metadata.internal",
      "https://pds.example.com:8443",
      "https://pds.example.com#fragment",
      "http://pds.example.com",
      "https://user:pass@pds.example.com",
    ]

    let serviceDID = "did:web:chat.catbird.blue"
    let userDID = "did:plc:targetuser"

    for raw in blockedEndpoints {
      let dsDoc = DIDDocument(
        id: serviceDID,
        verificationMethod: [],
        service: [Service(id: "#catbird_labyrinth", type: "CatbirdLabyrinthService", serviceEndpoint: raw)]
      )
      XCTAssertThrowsError(
        try MLSAPIClient.labyrinthServiceEndpoint(in: dsDoc, did: serviceDID, fragment: "#catbird_labyrinth"),
        "labyrinthServiceEndpoint must reject SSRF/unsafe endpoint: \(raw)"
      )

      let pdsDoc = DIDDocument(
        id: userDID,
        verificationMethod: [],
        service: [Service(id: "#atproto_pds", type: "AtprotoPersonalDataServer", serviceEndpoint: raw)]
      )
      XCTAssertThrowsError(
        try MLSPublicPDSReader.currentPDS(in: pdsDoc, did: userDID),
        "currentPDS must reject SSRF/unsafe endpoint: \(raw)"
      )

      if let parsed = URL(string: raw) {
        XCTAssertNil(
          MLSPublicPDSReader.deviceRecordsListURL(pds: parsed, did: userDID),
          "deviceRecordsListURL must reject SSRF/unsafe endpoint: \(raw)"
        )
      }
    }

    // Valid public HTTPS endpoints (implicit 443 and explicit :443) succeed:
    for allowed in ["https://pds.example.com", "https://pds.example.com:443"] {
      let pdsDoc = DIDDocument(
        id: userDID,
        verificationMethod: [],
        service: [Service(id: "#atproto_pds", type: "AtprotoPersonalDataServer", serviceEndpoint: allowed)]
      )
      XCTAssertNoThrow(try MLSPublicPDSReader.currentPDS(in: pdsDoc, did: userDID))
      XCTAssertNotNil(MLSPublicPDSReader.deviceRecordsListURL(pds: URL(string: allowed)!, did: userDID))
    }
  }

  func testSubmitPreparedRequestRejectsDisallowedNSIDsAndPathTraversal() async throws {
    for allowedNSID in [
      "blue.catbird.chat.getOwnDevices",
      "blue.catbird.mlsChat.sendMessage",
      "blue.catbird.mls.getKeyPackages",
      "blue.catbird.labyrinth.appendBatch",
    ] {
      XCTAssertNoThrow(
        try MLSOutboundHTTPValidator.validatePreparedRequestNSID(allowedNSID),
        "Expected Catbird MLS/Labyrinth NSID to be allowed: \(allowedNSID)"
      )
    }

    let recorder = SecurityTestRequestRecorder()
    SecurityHardeningMockURLProtocol.handler = { request in
      recorder.record(request)
      return (200, Data("{}".utf8), ["Content-Type": "application/json"])
    }
    NetworkService.setNetworkTestProtocolClasses([SecurityHardeningMockURLProtocol.self])
    defer {
      NetworkService.setNetworkTestProtocolClasses(nil)
      SecurityHardeningMockURLProtocol.handler = nil
    }

    let atProto = await ATProtoClient(baseURL: URL(string: "https://pds.example.com")!)
    let apiClient = await MLSAPIClient(client: atProto)

    let disallowedNSIDs = [
      "com.atproto.server.createAppPassword",
      "com.atproto.repo.deleteRecord",
      "app.bsky.actor.getProfile",
      "..%2fcom.atproto.server.createAppPassword",
      "blue.catbird.mlsChat..%2f..%2fcom.atproto.server.createAppPassword",
      "blue.catbird.mlsChat.sendMessage/../../admin",
      "blue.catbird.labyrinth.",
      "blue.catbird.chat.getOwnDevices#frag",
    ]

    for badNSID in disallowedNSIDs {
      XCTAssertThrowsError(
        try MLSOutboundHTTPValidator.validatePreparedRequestNSID(badNSID),
        "validatePreparedRequestNSID must reject: \(badNSID)"
      )
      do {
        _ = try await apiClient.submitPreparedRequest(
          method: "POST",
          nsid: badNSID,
          body: Data("{}".utf8),
          query: nil
        )
        XCTFail("submitPreparedRequest must reject disallowed/malformed NSID: \(badNSID)")
      } catch let MLSAPIError.invalidResponse(message) {
        XCTAssertFalse(message.isEmpty)
      } catch {
        XCTFail("Expected MLSAPIError.invalidResponse pre-network rejection for \(badNSID), got \(error)")
      }
    }
    XCTAssertEqual(recorder.requests.count, 0, "Rejected NSIDs must never reach NetworkService")
  }

  func testDeviceProbeErrorCodeEncodesAmpersandAndHashWithoutQueryInjection() async throws {
    let recorder = SecurityTestRequestRecorder()
    SecurityHardeningMockURLProtocol.handler = { request in
      recorder.record(request)
      return (
        401,
        Data(#"{"error":"DeviceNotRegistered","message":"missing"}"#.utf8),
        ["Content-Type": "application/json"]
      )
    }
    NetworkService.setNetworkTestProtocolClasses([SecurityHardeningMockURLProtocol.self])
    defer {
      NetworkService.setNetworkTestProtocolClasses(nil)
      SecurityHardeningMockURLProtocol.handler = nil
    }

    let atProto = await ATProtoClient(baseURL: URL(string: "https://pds.example.com")!)
    let apiClient = await MLSAPIClient(client: atProto)

    let trickyDeviceId = "dev&injected=1#frag+plus"
    let code = await apiClient.deviceProbeErrorCode(actorDeviceId: trickyDeviceId)
    // Note: an unauthenticated test ATProtoClient may or may not attach credentials, so check
    // query encoding both via MLSOutboundHTTPValidator and on any dispatched URLRequest.
    var testComps = URLComponents()
    testComps.queryItems = [URLQueryItem(name: "actorDeviceId", value: trickyDeviceId)]
    let encodedQuery = try XCTUnwrap(testComps.percentEncodedQuery?.replacingOccurrences(of: "+", with: "%2B"))
    let parsedItems = try MLSOutboundHTTPValidator.parsePercentEncodedQueryItems(encodedQuery)
    XCTAssertEqual(parsedItems, [URLQueryItem(name: "actorDeviceId", value: trickyDeviceId)])

    if let sentRequest = recorder.requests.first {
      XCTAssertEqual(code, "DeviceNotRegistered")
      let sentURL = try XCTUnwrap(sentRequest.url)
      let comps = try XCTUnwrap(URLComponents(url: sentURL, resolvingAgainstBaseURL: false))
      XCTAssertNil(comps.fragment)
      XCTAssertEqual(
        comps.queryItems,
        [URLQueryItem(name: "actorDeviceId", value: trickyDeviceId)],
        "actorDeviceId containing &/#/+ must be encoded as a single query parameter without injection"
      )
    }
  }

  func testBoundedURLSessionBlocksRedirectsAndEnforcesStreamingByteCap() async throws {
    let recorder = SecurityTestRequestRecorder()
    let bounded = MLSBoundedURLSession(protocolClasses: [SecurityHardeningMockURLProtocol.self])
    defer {
      bounded.invalidateAndCancel()
      SecurityHardeningMockURLProtocol.handler = nil
    }

    // 1. 302 redirect must NOT be followed and must never forward Authorization: DPoP
    SecurityHardeningMockURLProtocol.handler = { request in
      recorder.record(request)
      if request.url?.host == "attacker.example" {
        XCTFail("Redirect to attacker.example must never be followed")
      }
      return (
        302,
        Data(),
        ["Location": "https://attacker.example/steal"]
      )
    }

    var dpopRequest = URLRequest(url: URL(string: "https://chat.catbird.blue/xrpc/blue.catbird.chat.sendMessage")!)
    dpopRequest.httpMethod = "POST"
    dpopRequest.setValue("DPoP secret-token", forHTTPHeaderField: "Authorization")
    dpopRequest.setValue("dpop-proof", forHTTPHeaderField: "DPoP")

    let (_, redirectResponse) = try await bounded.data(for: dpopRequest, maxBytes: 4096)
    XCTAssertEqual(redirectResponse.statusCode, 302)
    XCTAssertEqual(recorder.requests.count, 1, "Must not follow 3xx redirect")

    // 2. Oversized streaming response must abort immediately with BoundedFetchError.responseTooLarge
    SecurityHardeningMockURLProtocol.handler = { _ in
      return (200, Data(repeating: 0x41, count: 4096), ["Content-Type": "application/octet-stream"])
    }

    do {
      _ = try await bounded.data(
        for: URLRequest(url: URL(string: "https://pds.example.com/xrpc/com.atproto.repo.getRecord")!),
        maxBytes: 1024
      )
      XCTFail("Expected responseTooLarge error when body exceeds maxBytes")
    } catch let error as MLSBoundedURLSession.BoundedFetchError {
      XCTAssertEqual(error, .responseTooLarge(limit: 1024))
    }
  }

  func testBuildLabyrinthURLAndHeadersPreserveBasePathAndRejectFragmentsTraversalAndHeaderInjection() throws {
    let baseEndpoint = URL(string: "https://chat.catbird.blue/mls-prefix/")!
    let built = try MLSAPIClient.buildLabyrinthURL(
      endpoint: baseEndpoint,
      nsid: "blue.catbird.labyrinth.listBatches",
      path: "/xrpc/blue.catbird.labyrinth.listBatches?actorDeviceId=dev1&limit=10"
    )
    XCTAssertEqual(
      built.absoluteString,
      "https://chat.catbird.blue/mls-prefix/xrpc/blue.catbird.labyrinth.listBatches?actorDeviceId=dev1&limit=10"
    )

    let badPaths = [
      "/xrpc/blue.catbird.labyrinth.listBatches#frag",
      "/xrpc/blue.catbird.labyrinth.listBatches?limit=1#frag",
      "/xrpc/blue.catbird.labyrinth.listBatches/../admin",
      "/xrpc/blue.catbird.labyrinth.listBatches%2f..%2fadmin",
      "/xrpc/blue.catbird.labyrinth.listBatches\r\nInjected: 1",
    ]
    for badPath in badPaths {
      XCTAssertThrowsError(
        try MLSAPIClient.buildLabyrinthURL(
          endpoint: baseEndpoint,
          nsid: "blue.catbird.labyrinth.listBatches",
          path: badPath
        ),
        "buildLabyrinthURL must reject unsafe path: \(badPath)"
      )
    }

    var req = URLRequest(url: built)
    XCTAssertNoThrow(
      try MLSAPIClient.applyValidatedLabyrinthHeaders(
        [
          (name: "catbird-labyrinth-write", value: "abc123"),
          (name: "catbird-labyrinth-signature", value: "sig456"),
        ],
        to: &req
      )
    )
    XCTAssertEqual(req.value(forHTTPHeaderField: "catbird-labyrinth-write"), "abc123")

    let badHeaderSets: [[(name: String, value: String)]] = [
      [(name: "Host", value: "evil.example")],
      [(name: "catbird-labyrinth-", value: "empty-suffix")],
      [(name: "catbird-labyrinth-write", value: "good\r\nHost: evil.example")],
      [(name: "catbird-labyrinth-write", value: "good\nInjected: 1")],
      [(name: "catbird-labyrinth-write\r\nX-Evil", value: "1")],
    ]
    for badHeaders in badHeaderSets {
      var badReq = URLRequest(url: built)
      XCTAssertThrowsError(
        try MLSAPIClient.applyValidatedLabyrinthHeaders(badHeaders, to: &badReq),
        "applyValidatedLabyrinthHeaders must reject: \(badHeaders)"
      )
    }
  }

  func testDeleteLabyrinthMailboxKeysRejectsCursorLoopPageOverflowAndForeignOrMalformedURIs() async throws {
    let expectedDID = "did:plc:alice"

    // Valid page succeeds
    let validPage = Data(#"""
    {"records":[
      {"uri":"at://did:plc:alice/blue.catbird.labyrinth.mailboxKeys/self"},
      {"uri":"at://did:plc:alice/blue.catbird.labyrinth.mailboxKeys/dev-1"}
    ]}
    """#.utf8)
    let extracted = try MLSAPIClient.extractValidatedMailboxKeyRkeys(
      fromListRecords: validPage,
      expectedDID: expectedDID
    )
    XCTAssertEqual(extracted.rkeys, ["self", "dev-1"])
    XCTAssertNil(extracted.nextCursor)

    // Foreign DID, wrong collection, traversal rkeys must fail closed
    let invalidURIs = [
      "at://did:plc:bob/blue.catbird.labyrinth.mailboxKeys/self",
      "at://did:plc:alice/app.bsky.feed.post/3k1",
      "at://did:plc:alice/blue.catbird.labyrinth.mailboxKeys/..",
      "at://did:plc:alice/blue.catbird.labyrinth.mailboxKeys/a/b",
      "at://did:plc:alice/blue.catbird.labyrinth.mailboxKeys/bad%2frkey",
      "at://did:plc:alice/blue.catbird.labyrinth.mailboxKeys/bad#frag",
    ]
    for badURI in invalidURIs {
      let badData = try JSONSerialization.data(withJSONObject: ["records": [["uri": badURI]]])
      XCTAssertThrowsError(
        try MLSAPIClient.extractValidatedMailboxKeyRkeys(
          fromListRecords: badData,
          expectedDID: expectedDID
        ),
        "Must reject malformed/foreign mailboxKeys URI: \(badURI)"
      )
    }

    // Cursor loop must abort
    do {
      _ = try await MLSAPIClient.collectValidatedMailboxKeyRkeysToDelete(did: expectedDID) { _ in
        let loopPage = Data(#"""
        {"records":[{"uri":"at://did:plc:alice/blue.catbird.labyrinth.mailboxKeys/self"}],"cursor":"same"}
        """#.utf8)
        return (loopPage, 200)
      }
      XCTFail("Cursor loop in mailboxKeys deletion must throw")
    } catch {}

    // Exceeding 20 pages must abort
    var pageCounter = 0
    do {
      _ = try await MLSAPIClient.collectValidatedMailboxKeyRkeysToDelete(did: expectedDID) { _ in
        pageCounter += 1
        let page = Data("""
        {"records":[{"uri":"at://did:plc:alice/blue.catbird.labyrinth.mailboxKeys/k\(pageCounter)"}],"cursor":"c\(pageCounter)"}
        """.utf8)
        return (page, 200)
      }
      XCTFail("More than 20 pages in mailboxKeys deletion must throw")
    } catch {}
    XCTAssertEqual(pageCounter, 20, "Must stop after exactly 20 pages")
  }

  // MARK: - A-6: Sibling-Device Decryption & Authenticated Credential DID Enforcement

  private static let sharedSecurityRuntimeBaseDir: URL = {
    let dir = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSLabyrinthSecuritySharedRuntime-\(UUID().uuidString)", isDirectory: true)
    try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    return dir
  }()

  private func runBlockingFFI<T: Sendable>(
    _ operation: @Sendable @escaping () throws -> T
  ) async throws -> T {
    try await withCheckedThrowingContinuation { continuation in
      DispatchQueue.global(qos: .userInitiated).async {
        do {
          continuation.resume(returning: try operation())
        } catch {
          continuation.resume(throwing: error)
        }
      }
    }
  }

  func testDecryptAndStoreDecryptsUncachedSiblingDeviceMessageWhenOuterSenderMatchesUserDID() async throws {
    try FileManager.default.createDirectory(at: Self.sharedSecurityRuntimeBaseDir, withIntermediateDirectories: true)
    let isolatedDir = tempDir.appendingPathComponent("a6-sibling-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: isolatedDir, withIntermediateDirectories: true)
    let fakeKeychain = MLSKeychainFakeStorage()
    MLSStoragePaths.setBaseDirectoryOverride(Self.sharedSecurityRuntimeBaseDir)
    try FileManager.default.createDirectory(
      at: try MLSStoragePaths.grdbDatabaseDirectory(),
      withIntermediateDirectories: true
    )
    MLSKeychainManager.setFakeStorageOverrideForTesting(fakeKeychain)
    MLSCoreContext.clearSuspensionFlag()
    MLSClient.clearSuspensionFlag(reason: "a6-sibling setup")

    let siblingContext = try MlsContext(
      storagePath: isolatedDir.appendingPathComponent("sibling.db").path,
      encryptionKey: String(repeating: "ab", count: 32),
      keychain: InMemoryKeychainAccess()
    )
    try siblingContext.setEpochSecretStorage(storage: InMemoryEpochSecretStorage())

    defer {
      try? siblingContext.flushAndPrepareClose()
      MLSCoreContext.emergencyCloseAllContexts()
      _ = MLSGRDBManager.closeAllDatabasesForSuspension()
      MLSCoreContext.clearSuspensionFlag()
      MLSClient.clearSuspensionFlag(reason: "a6-sibling cleanup")
      NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
      MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
      MLSStoragePaths.setBaseDirectoryOverride(nil)
    }

    let userDID = "did:plc:a6sibling\(UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased().prefix(16))"
    await MLSGRDBManager.shared.setActiveUser(userDID)
    _ = try await MLSGRDBManager.shared.getDatabasePool(for: userDID)
    let primaryContext = try await MLSCoreContext.shared.getContext(for: userDID)

    let (groupId, encryptedCiphertext) = try await self.runBlockingFFI {
      let groupCreation = try primaryContext.createGroup(identityBytes: Data(userDID.utf8), config: nil)
      let groupId = groupCreation.groupId
      let siblingKP = try siblingContext.createKeyPackage(
        identityBytes: Data("\(userDID)#sibling-device".utf8)
      )
      let addResult = try primaryContext.addMembers(
        groupId: groupId,
        keyPackages: [KeyPackageData(data: siblingKP.keyPackageData)]
      )
      _ = try primaryContext.mergePendingCommit(groupId: groupId)
      _ = try siblingContext.processWelcome(
        welcomeBytes: addResult.welcomeData,
        identityBytes: Data("\(userDID)#sibling-device".utf8),
        config: nil
      )
      let payloadBytes = try MLSMessagePayload.text("Hello from sibling device!", embed: nil).encodeToJSON()
      let encrypted = try siblingContext.encryptMessage(groupId: groupId, plaintext: payloadBytes)
      return (groupId, encrypted.ciphertext)
    }

    // Delivery service / envelope carries bare `userDID` (without `#device`) as `senderID`.
    // Even though `senderID == userDID` and the message is not in local cache, OpenMLS can decrypt
    // the sibling leaf's message and must NOT drop it with a placeholder before `decryptMessage`.
    let plaintext = try await MLSCoreContext.shared.decryptAndStore(
      userDid: userDID,
      groupId: groupId,
      ciphertext: encryptedCiphertext,
      conversationID: groupId.hexEncodedString(),
      messageID: "sibling-msg-1",
      epoch: 1,
      sequenceNumber: 1,
      senderID: userDID
    )

    XCTAssertEqual(plaintext, "Hello from sibling device!")
  }

  func testDecryptAndStoreFailsClosedWhenAuthenticatedCredentialIdentityIsNotValidDID() async throws {
    try FileManager.default.createDirectory(at: Self.sharedSecurityRuntimeBaseDir, withIntermediateDirectories: true)
    let isolatedDir = tempDir.appendingPathComponent("a6-cred-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: isolatedDir, withIntermediateDirectories: true)
    let fakeKeychain = MLSKeychainFakeStorage()
    MLSStoragePaths.setBaseDirectoryOverride(Self.sharedSecurityRuntimeBaseDir)
    try FileManager.default.createDirectory(
      at: try MLSStoragePaths.grdbDatabaseDirectory(),
      withIntermediateDirectories: true
    )
    MLSKeychainManager.setFakeStorageOverrideForTesting(fakeKeychain)
    MLSCoreContext.clearSuspensionFlag()
    MLSClient.clearSuspensionFlag(reason: "a6-cred setup")

    let attackerContext = try MlsContext(
      storagePath: isolatedDir.appendingPathComponent("attacker.db").path,
      encryptionKey: String(repeating: "ef", count: 32),
      keychain: InMemoryKeychainAccess()
    )
    try attackerContext.setEpochSecretStorage(storage: InMemoryEpochSecretStorage())

    defer {
      try? attackerContext.flushAndPrepareClose()
      MLSCoreContext.emergencyCloseAllContexts()
      _ = MLSGRDBManager.closeAllDatabasesForSuspension()
      MLSCoreContext.clearSuspensionFlag()
      MLSClient.clearSuspensionFlag(reason: "a6-cred cleanup")
      NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
      MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
      MLSStoragePaths.setBaseDirectoryOverride(nil)
    }

    let userDID = "did:plc:a6victim\(UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased().prefix(16))"
    await MLSGRDBManager.shared.setActiveUser(userDID)
    _ = try await MLSGRDBManager.shared.getDatabasePool(for: userDID)
    let primaryContext = try await MLSCoreContext.shared.getContext(for: userDID)

    // Member whose MLS leaf credential identity is NOT a valid `did:` string
    let (groupId, encryptedCiphertext) = try await self.runBlockingFFI {
      let groupCreation = try primaryContext.createGroup(identityBytes: Data(userDID.utf8), config: nil)
      let groupId = groupCreation.groupId
      let malformedIdentity = Data("not-a-did-identity".utf8)
      let malformedKP = try attackerContext.createKeyPackage(identityBytes: malformedIdentity)
      let addResult = try primaryContext.addMembers(
        groupId: groupId,
        keyPackages: [KeyPackageData(data: malformedKP.keyPackageData)]
      )
      _ = try primaryContext.mergePendingCommit(groupId: groupId)
      _ = try attackerContext.processWelcome(
        welcomeBytes: addResult.welcomeData,
        identityBytes: malformedIdentity,
        config: nil
      )
      let payloadBytes = try MLSMessagePayload.text("Spoofed outer sender", embed: nil).encodeToJSON()
      let encrypted = try attackerContext.encryptMessage(groupId: groupId, plaintext: payloadBytes)
      return (groupId, encrypted.ciphertext)
    }

    do {
      _ = try await MLSCoreContext.shared.decryptAndStore(
        userDid: userDID,
        groupId: groupId,
        ciphertext: encryptedCiphertext,
        conversationID: groupId.hexEncodedString(),
        messageID: "malformed-cred-msg-1",
        epoch: 1,
        sequenceNumber: 1,
        senderID: "did:plc:impersonatedvictim"
      )
      XCTFail("decryptAndStore must fail closed when authenticated MLS credential identity is not a valid DID")
    } catch let error as MLSError {
      if case .invalidCredential = error {
        // Expected fail-closed rejection
      } else {
        XCTFail("Expected MLSError.invalidCredential, got \(error)")
      }
    }
  }

  // MARK: - A-8: MLSKeychain.deleteAllKeys GenericPassword Deletion

  func testMLSKeychainDeleteAllKeysDeletesGenericPasswordItemsWhenSkipDataProtectionIsFalse() throws {
    let fakeKeychain = MLSKeychainFakeStorage()
    let previousSkipDP = MLSKeychainManager.shared.skipDataProtection
    let capturedQueries = Mutex<[[String: String]]>([])

    MLSKeychainManager.setFakeStorageOverrideForTesting(fakeKeychain)
    MLSKeychainManager.shared.skipDataProtection = false
    MLSKeychain.setSecItemDeleteOverrideForTesting { rawQuery in
      var summary: [String: String] = [:]
      if let secClass = rawQuery[kSecClass as String] as? String {
        summary["class"] = secClass
      }
      if let service = rawQuery[kSecAttrService as String] as? String {
        summary["service"] = service
      }
      if let tag = rawQuery[kSecAttrApplicationTag as String] as? String {
        summary["tag"] = tag
      }
      capturedQueries.withLock { $0.append(summary) }
      return errSecItemNotFound
    }
    defer {
      MLSKeychain.setSecItemDeleteOverrideForTesting(nil)
      MLSKeychainManager.shared.skipDataProtection = previousSkipDP
      MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
    }

    let sigKey = Data(repeating: 0xAA, count: 64)
    let groupKey = Data(repeating: 0xBB, count: 32)
    let groupId = Data([0x01, 0x02, 0x03, 0x04])
    try MLSKeychain.storeSignatureKeyClean(sigKey, forIdentity: "did:plc:alice")
    try MLSKeychain.storeGroupKey(groupKey, forGroupId: groupId)
    XCTAssertEqual(try MLSKeychain.retrieveSignatureKeyClean(forIdentity: "did:plc:alice"), sigKey)
    XCTAssertEqual(try MLSKeychain.retrieveGroupKey(forGroupId: groupId), groupKey)

    try MLSKeychain.deleteAllKeys()

    XCTAssertNil(
      try MLSKeychain.retrieveSignatureKeyClean(forIdentity: "did:plc:alice"),
      "MLSKeychain.deleteAllKeys() must delete GenericPassword items under blue.catbird.mls.signature even when skipDataProtection is false"
    )
    XCTAssertThrowsError(
      try MLSKeychain.retrieveGroupKey(forGroupId: groupId),
      "MLSKeychain.deleteAllKeys() must delete GenericPassword items under blue.catbird.mls.groupkey"
    )

    let queries = capturedQueries.withLock { $0 }
    XCTAssertFalse(queries.isEmpty)
    XCTAssertTrue(
      queries.allSatisfy { $0["class"] == (kSecClassGenericPassword as String) && $0["tag"] == nil },
      "deleteAllKeys() must query kSecClassGenericPassword, never kSecClassKey: \(queries)"
    )
    let deletedServices = Set(queries.compactMap { $0["service"] })
    XCTAssertEqual(deletedServices, ["blue.catbird.mls.signature", "blue.catbird.mls.groupkey"])
  }

  // MARK: - A-9: Context-Free Suspension WAL Checkpoint on Encrypted SQLCipher DBs

  func testContextFreeEmergencyCloseFlushesEncryptedSQLCipherWALDatabase() async throws {
    try FileManager.default.createDirectory(at: Self.sharedSecurityRuntimeBaseDir, withIntermediateDirectories: true)
    let fakeKeychain = MLSKeychainFakeStorage()
    MLSStoragePaths.setBaseDirectoryOverride(Self.sharedSecurityRuntimeBaseDir)
    try FileManager.default.createDirectory(
      at: try MLSStoragePaths.grdbDatabaseDirectory(),
      withIntermediateDirectories: true
    )
    MLSKeychainManager.setFakeStorageOverrideForTesting(fakeKeychain)
    MLSContextFreeLifecycleSuspensionOwner.resetForTesting()
    MLSCoreContext.clearSuspensionFlag()
    MLSClient.clearSuspensionFlag(reason: "a9-wal setup")

    let grdbManager = MLSGRDBManager()
    let userDID = "did:plc:a9wal\(UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased().prefix(16))"

    defer {
      _ = MLSGRDBManager.closeAllDatabasesForSuspension()
      MLSContextFreeLifecycleSuspensionOwner.resetForTesting()
      MLSCoreContext.clearSuspensionFlag()
      MLSClient.clearSuspensionFlag(reason: "a9-wal cleanup")
      NotificationCenter.default.post(name: Database.resumeNotification, object: nil)
      MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
      MLSStoragePaths.setBaseDirectoryOverride(nil)
    }

    let pool = try await grdbManager.getDatabasePool(for: userDID)
    try await pool.write { db in
      try db.execute(sql: "CREATE TABLE wal_probe (id INTEGER PRIMARY KEY, body TEXT);")
      try db.execute(sql: "INSERT INTO wal_probe (body) VALUES ('encrypted-wal-row');")
    }

    // Confirm the GRDB SQLCipher database has an encrypted (random salt) header AND an active `-wal` sidecar.
    let header = try FileHandle(forReadingFrom: URL(fileURLWithPath: pool.path)).read(upToCount: 16) ?? Data()
    XCTAssertNotEqual(
      header,
      Data("SQLite format 3\0".utf8),
      "SQLCipher GRDB database header is random salt, not plaintext SQLite magic"
    )
    XCTAssertTrue(
      FileManager.default.fileExists(atPath: pool.path + "-wal"),
      "Expected active -wal file alongside encrypted SQLCipher database"
    )
    XCTAssertTrue(
      MLSContextFreeLifecycleSuspensionOwner.isDatabaseInWALMode(atPath: pool.path),
      "Encrypted SQLCipher DB with -wal sidecar must be detected as WAL mode"
    )

    // Also verify plaintext SQLite WAL header vs rollback journal header detection without -wal sidecar
    let plaintextWalPath = tempDir.appendingPathComponent("plaintext-wal.db")
    var walHeader = Data("SQLite format 3\0".utf8)
    walHeader.append(contentsOf: [0x10, 0x00, 0x02, 0x02])  // page size 4096, write/read format = 2 (WAL)
    try walHeader.write(to: plaintextWalPath)
    XCTAssertTrue(MLSContextFreeLifecycleSuspensionOwner.isDatabaseInWALMode(atPath: plaintextWalPath.path))

    let rollbackPath = tempDir.appendingPathComponent("rollback.db")
    var rollbackHeader = Data("SQLite format 3\0".utf8)
    rollbackHeader.append(contentsOf: [0x10, 0x00, 0x01, 0x01])  // write/read format = 1 (rollback journal)
    try rollbackHeader.write(to: rollbackPath)
    XCTAssertFalse(MLSContextFreeLifecycleSuspensionOwner.isDatabaseInWALMode(atPath: rollbackPath.path))

    let checkpointObserved = SecuritySQLCheckpointRecorder()
    try await pool.writeWithoutTransaction { db in
      db.trace { event in
        checkpointObserved.record(event.description)
      }
    }

    let owner = MLSContextFreeLifecycleSuspensionOwner()
    owner.markSuspensionInProgress(reason: "a9-background-suspend")
    XCTAssertTrue(owner.emergencyCloseAllContextsIfOwned(reason: "a9-background-suspend"))

    XCTAssertTrue(
      checkpointObserved.sawWALCheckpoint,
      "Context-free suspension emergency close must detect encrypted WAL DB via -wal sidecar and execute wal_checkpoint"
    )

    _ = await owner.resumeSuspensionIfOwnedAndContextFree()
    await grdbManager.shutdownAllDatabases()
  }
}

private final class SecuritySQLCheckpointRecorder: @unchecked Sendable {
  private let lock = NSLock()
  private var statements: [String] = []

  func record(_ sql: String) {
    lock.lock()
    statements.append(sql)
    lock.unlock()
  }

  var sawWALCheckpoint: Bool {
    lock.lock()
    defer { lock.unlock() }
    return statements.contains { $0.localizedCaseInsensitiveContains("wal_checkpoint") }
  }
}

private final class InMemoryEpochSecretStorage: EpochSecretStorage, @unchecked Sendable {
  private let store = Mutex<[String: Data]>([:])

  func storeEpochSecret(conversationId: String, epoch: UInt64, secretData: Data) async -> Bool {
    store.withLock { $0["\(conversationId):\(epoch)"] = secretData }
    return true
  }

  func getEpochSecret(conversationId: String, epoch: UInt64) async -> Data? {
    store.withLock { $0["\(conversationId):\(epoch)"] }
  }

  func deleteEpochSecret(conversationId: String, epoch: UInt64) async -> Bool {
    store.withLock { _ = $0.removeValue(forKey: "\(conversationId):\(epoch)") }
    return true
  }

  func deleteEpochsBefore(conversationId: String, cutoffEpoch: UInt64) async -> UInt32 {
    store.withLock { dict in
      let prefix = "\(conversationId):"
      var removed: UInt32 = 0
      for key in Array(dict.keys) where key.hasPrefix(prefix) {
        if let epoch = UInt64(key.dropFirst(prefix.count)), epoch < cutoffEpoch {
          dict.removeValue(forKey: key)
          removed += 1
        }
      }
      return removed
    }
  }
}

private final class SecurityTestRequestRecorder: @unchecked Sendable {
  private let lock = NSLock()
  private var stored: [URLRequest] = []

  func record(_ request: URLRequest) {
    lock.lock()
    stored.append(request)
    lock.unlock()
  }

  var requests: [URLRequest] {
    lock.lock()
    defer { lock.unlock() }
    return stored
  }
}

private final class SecurityHardeningMockURLProtocol: URLProtocol {
  private static let lock = NSLock()
  private nonisolated(unsafe) static var storedHandler:
    (@Sendable (URLRequest) throws -> (Int, Data, [String: String]))?
  private var isStopped = false

  static var handler: (@Sendable (URLRequest) throws -> (Int, Data, [String: String]))? {
    get { lock.lock(); defer { lock.unlock() }; return storedHandler }
    set { lock.lock(); storedHandler = newValue; lock.unlock() }
  }

  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

  override func startLoading() {
    guard let handler = Self.handler, let url = request.url else {
      client?.urlProtocol(self, didFailWithError: URLError(.notConnectedToInternet))
      return
    }
    do {
      let (status, data, headers) = try handler(request)
      if status >= 300, status < 400, let location = headers["Location"],
         let redirectURL = URL(string: location, relativeTo: url)?.absoluteURL {
        let redirectResponse = HTTPURLResponse(
          url: url, statusCode: status, httpVersion: "HTTP/1.1", headerFields: headers)!
        var newReq = request
        newReq.url = redirectURL
        client?.urlProtocol(self, wasRedirectedTo: newReq, redirectResponse: redirectResponse)
        if !isStopped {
          if !data.isEmpty {
            client?.urlProtocol(self, didLoad: data)
          }
          client?.urlProtocolDidFinishLoading(self)
        }
        return
      }
      let response = HTTPURLResponse(
        url: url, statusCode: status, httpVersion: "HTTP/1.1", headerFields: headers)!
      client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
      if !data.isEmpty && !isStopped {
        client?.urlProtocol(self, didLoad: data)
      }
      if !isStopped {
        client?.urlProtocolDidFinishLoading(self)
      }
    } catch {
      if !isStopped {
        client?.urlProtocol(self, didFailWithError: error)
      }
    }
  }

  override func stopLoading() {
    isStopped = true
  }
}
