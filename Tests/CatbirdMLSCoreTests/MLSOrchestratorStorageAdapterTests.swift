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
    let adapter = MLSOrchestratorStorageAdapter(
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
    let adapter = MLSOrchestratorStorageAdapter(
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
}
