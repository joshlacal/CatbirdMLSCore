import XCTest
import GRDB
@testable import CatbirdMLSCore

final class MLSEpochKeyStorageTests: XCTestCase {
  private var dbQueue: DatabaseQueue!
  private let storage = MLSStorage.shared

  override func setUp() async throws {
    try await super.setUp()
    dbQueue = try DatabaseQueue()

    try await dbQueue.write { db in
      try Self.createConversationTable(in: db)
      try Self.createEpochKeyTable(in: db)
    }
  }

  override func tearDown() async throws {
    dbQueue = nil
    try await super.tearDown()
  }

  func testRecordEpochKeyIsIdempotentForDuplicateEpoch() async throws {
    let conversationID = "convo-epoch-duplicate"
    let userDID = "did:plc:epoch-user"

    try await insertConversation(conversationID: conversationID, currentUserDID: userDID)

    try await storage.recordEpochKey(
      conversationID: conversationID,
      epoch: 5,
      userDID: userDID,
      database: dbQueue
    )

    try await storage.recordEpochKey(
      conversationID: conversationID,
      epoch: 5,
      userDID: userDID,
      database: dbQueue
    )

    let keys = try await fetchEpochKeys(conversationID: conversationID, currentUserDID: userDID)
    XCTAssertEqual(keys.count, 1)
    XCTAssertEqual(keys.first?.epoch, 5)
  }

  func testRecordEpochKeyDoesNotOverwriteSavedSecret() async throws {
    let conversationID = "convo-epoch-secret"
    let userDID = "did:plc:epoch-user"
    let secret = Data([0xCA, 0x7B, 0x1D])

    try await insertConversation(conversationID: conversationID, currentUserDID: userDID)

    try await storage.saveEpochSecret(
      userDID: userDID,
      conversationID: conversationID,
      epoch: 7,
      secretData: secret,
      database: dbQueue
    )

    try await storage.recordEpochKey(
      conversationID: conversationID,
      epoch: 7,
      userDID: userDID,
      database: dbQueue
    )

    let key = try await dbQueue.read { db in
      try MLSEpochKeyModel
        .filter(MLSEpochKeyModel.Columns.conversationID == conversationID)
        .filter(MLSEpochKeyModel.Columns.currentUserDID == userDID)
        .filter(MLSEpochKeyModel.Columns.epoch == 7)
        .fetchOne(db)
    }

    XCTAssertEqual(key?.keyMaterial, secret)
  }

  func testSaveEpochSecretDeduplicatesIdenticalConversationEpochSecret() async throws {
    let conversationID = "550e8400-e29b-41d4-a716-446655440000"
    let userDID = "did:plc:epoch-user"
    let secret = Data([0xCA, 0x7B, 0x1D])

    try await insertConversation(conversationID: conversationID, currentUserDID: userDID)
    try await storage.saveEpochSecret(
      userDID: userDID,
      conversationID: conversationID,
      epoch: 7,
      secretData: secret,
      database: dbQueue
    )
    try await storage.saveEpochSecret(
      userDID: userDID,
      conversationID: conversationID,
      epoch: 7,
      secretData: secret,
      database: dbQueue
    )

    let keys = try await fetchEpochKeys(conversationID: conversationID, currentUserDID: userDID)
    XCTAssertEqual(keys.count, 1)
    XCTAssertEqual(keys.first?.keyMaterial, secret)
  }

  func testSaveEpochSecretRejectsDifferentSecretForSameConversationEpoch() async throws {
    let conversationID = "550e8400-e29b-41d4-a716-446655440000"
    let userDID = "did:plc:epoch-user"
    let original = Data([0xCA, 0x7B, 0x1D])
    let conflicting = Data([0xEE, 0xAD, 0x01])

    try await insertConversation(conversationID: conversationID, currentUserDID: userDID)
    try await storage.saveEpochSecret(
      userDID: userDID,
      conversationID: conversationID,
      epoch: 7,
      secretData: original,
      database: dbQueue
    )

    var didThrow = false
    do {
      try await storage.saveEpochSecret(
        userDID: userDID,
        conversationID: conversationID,
        epoch: 7,
        secretData: conflicting,
        database: dbQueue
      )
    } catch {
      didThrow = true
    }
    XCTAssertTrue(didThrow)

    let keys = try await fetchEpochKeys(conversationID: conversationID, currentUserDID: userDID)
    XCTAssertEqual(keys.count, 1)
    XCTAssertEqual(keys.first?.keyMaterial, original)
  }

  func testEpochStoreGetAndDeleteResolveExactRawAliasToStableConversation() async throws {
    let rawGroupID = "deadbeef"
    let stableConversationID = "550e8400-e29b-41d4-a716-446655440000"
    let userDID = "did:plc:epoch-user"
    let secret = Data([0x01, 0x02, 0x03])

    try await insertConversation(
      conversationID: stableConversationID,
      currentUserDID: userDID,
      groupID: Data(hexEncoded: rawGroupID)
    )
    try await insertConversation(
      conversationID: rawGroupID,
      currentUserDID: userDID,
      groupID: Data(hexEncoded: rawGroupID)
    )

    try await storage.saveEpochSecret(
      userDID: userDID,
      conversationID: rawGroupID,
      epoch: 9,
      secretData: secret,
      database: dbQueue
    )

    let fetched = try await storage.getEpochSecret(
      userDID: userDID,
      conversationID: rawGroupID,
      epoch: 9,
      database: dbQueue
    )
    XCTAssertEqual(fetched, secret)
    let routedKeys = try await fetchEpochKeys(
      conversationID: stableConversationID,
      currentUserDID: userDID
    )
    XCTAssertEqual(routedKeys.map(\.epoch), [9])

    let epochStorage = storage
    let deleted = try await dbQueue.write { db in
      try epochStorage.deleteEpochsBeforeSync(
        userDID: userDID,
        conversationID: rawGroupID,
        cutoffEpoch: 10,
        db: db
      )
    }
    XCTAssertEqual(deleted, 1)
    let remaining = try await storage.getEpochSecret(
      userDID: userDID,
      conversationID: stableConversationID,
      epoch: 9,
      database: dbQueue
    )
    XCTAssertNil(remaining)
  }

  private func insertConversation(
    conversationID: String,
    currentUserDID: String,
    groupID: Data? = nil
  ) async throws {
    let now = Date(timeIntervalSince1970: 1_700_000_000)
    try await dbQueue.write { db in
      try db.execute(
        sql: """
          INSERT INTO MLSConversationModel (
            conversationID, currentUserDID, groupID, epoch,
            joinMethod, joinEpoch, createdAt, updatedAt,
            unacknowledgedMemberChanges, isActive,
            needsRejoin, needsReset, isUnrecoverable,
            consecutiveFailures, isPlaceholder, requestState
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """,
        arguments: [
          conversationID, currentUserDID, groupID ?? Data(conversationID.utf8), 0,
          "unknown", 0, now, now,
          0, true,
          false, false, false,
          0, false, "none",
        ]
      )
    }
  }

  private func fetchEpochKeys(
    conversationID: String,
    currentUserDID: String
  ) async throws -> [MLSEpochKeyModel] {
    try await dbQueue.read { db in
      try MLSEpochKeyModel
        .filter(MLSEpochKeyModel.Columns.conversationID == conversationID)
        .filter(MLSEpochKeyModel.Columns.currentUserDID == currentUserDID)
        .fetchAll(db)
    }
  }

  private static func createConversationTable(in db: Database) throws {
    try db.create(table: "MLSConversationModel") { t in
      t.primaryKey("conversationID", .text).notNull()
      t.column("currentUserDID", .text).notNull()
      t.column("groupID", .blob).notNull()
      t.column("epoch", .integer).notNull().defaults(to: 0)
      t.column("joinMethod", .text).notNull().defaults(to: "unknown")
      t.column("joinEpoch", .integer).notNull().defaults(to: 0)
      t.column("title", .text)
      t.column("description", .text)
      t.column("avatarURL", .text)
      t.column("avatarImageData", .blob)
      t.column("createdAt", .datetime).notNull()
      t.column("updatedAt", .datetime).notNull()
      t.column("lastMessageAt", .datetime)
      t.column("lastMembershipChangeAt", .datetime)
      t.column("unacknowledgedMemberChanges", .integer).notNull().defaults(to: 0)
      t.column("isActive", .boolean).notNull().defaults(to: true)
      t.column("needsRejoin", .boolean).notNull().defaults(to: false)
      t.column("needsReset", .boolean).notNull().defaults(to: false)
      t.column("isUnrecoverable", .boolean).notNull().defaults(to: false)
      t.column("rejoinRequestedAt", .datetime)
      t.column("lastRecoveryAttempt", .datetime)
      t.column("consecutiveFailures", .integer).notNull().defaults(to: 0)
      t.column("isPlaceholder", .boolean).notNull().defaults(to: false)
      t.column("requestState", .text).notNull().defaults(to: "none")
      t.column("mutedUntil", .datetime)
      t.column("pendingNewGroupId", .text)
      t.column("pendingResetGeneration", .integer)
    }
  }

  private static func createEpochKeyTable(in db: Database) throws {
    try db.create(table: "MLSEpochKeyModel") { t in
      t.primaryKey("epochKeyID", .text).notNull()
      t.column("conversationID", .text).notNull().references(
        "MLSConversationModel", onDelete: .cascade)
      t.column("currentUserDID", .text).notNull()
      t.column("epoch", .integer).notNull()
      t.column("keyMaterial", .blob).notNull()
      t.column("createdAt", .datetime).notNull()
      t.column("expiresAt", .datetime)
      t.column("isActive", .boolean).notNull().defaults(to: true)
      t.column("deletedAt", .datetime)
    }
  }
}
