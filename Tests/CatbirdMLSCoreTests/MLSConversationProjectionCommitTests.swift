@testable import CatbirdMLS
@testable import CatbirdMLSCore
import GRDB
import XCTest

final class MLSConversationProjectionCommitTests: XCTestCase {
    private let stableID = "550e8400-e29b-41d4-a716-446655440000"
    private var directory: URL!
    private var database: DatabasePool!
    private var context: MlsContext!

    override func setUp() async throws {
        try await super.setUp()
        directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("MLSConversationProjectionCommitTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        database = try DatabasePool(path: directory.appendingPathComponent("messages.sqlite").path)
        try MLSGRDBManager.makeMigrator().migrate(database)
        context = try MlsContext(
            storagePath: directory.appendingPathComponent("openmls.sqlite").path,
            encryptionKey: String(repeating: "cd", count: 32),
            keychain: InMemoryKeychainAccess()
        )
        try context.setContentRootKey(key: Data(repeating: 0x51, count: 32))
        _ = MLSOrchestratorStorageAdapter(
            dbPool: database,
            userDID: "did:plc:projection-test",
            mlsContext: context
        )
    }

    override func tearDown() async throws {
        context?.clearContentRootKey()
        try? context?.flushAndPrepareClose()
        context = nil
        database = nil
        if let directory {
            try? FileManager.default.removeItem(at: directory)
        }
        directory = nil
        try await super.tearDown()
    }

    func testRetiresAliasWithoutDeletingParentAndReplaysIdempotently() throws {
        let owner = "did:plc:receiver"
        let groupID = String(repeating: "ab", count: 32)
        try insertPair(owner: owner, groupID: groupID, epoch: 7)
        let adapter = MLSOrchestratorStorageAdapter(
            dbPool: database,
            userDID: owner,
            mlsContext: context
        )
        let live = try request(owner: owner, groupID: groupID, aliasEpoch: 7)

        XCTAssertEqual(try outcome(adapter.commitConversationProjectionJson(requestJson: live)), .retired(2))
        try database.read { db in
            XCTAssertEqual(try MLSConversationModel.fetchCount(db), 2)
            XCTAssertEqual(try Int.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM mls_conversation_projection_alias_retirement"
            ), 1)
            let protected: Data? = try Data.fetchOne(
                db,
                sql: "SELECT payload_encrypted FROM mls_conversation_projection_alias_retirement"
            )
            XCTAssertNotNil(protected)
            XCTAssertFalse(try String(data: XCTUnwrap(protected), encoding: .utf8)?.contains(owner) == true)
        }
        XCTAssertEqual(try adapter.listConversations(userDid: owner).map(\.conversationId), [stableID])
        XCTAssertEqual(try adapter.getConversation(userDid: owner, conversationId: groupID)?.conversationId, stableID)

        XCTAssertEqual(
            try outcome(adapter.commitConversationProjectionJson(requestJson: replay(from: live))),
            .alreadyAbsent(0)
        )
    }

    func testStaleAliasIncarnationReturnsTypedZeroWriteMismatch() throws {
        let owner = "did:plc:receiver"
        let groupID = String(repeating: "cd", count: 32)
        try insertPair(owner: owner, groupID: groupID, epoch: 7)
        let adapter = MLSOrchestratorStorageAdapter(
            dbPool: database,
            userDID: owner,
            mlsContext: context
        )

        XCTAssertEqual(
            try outcome(adapter.commitConversationProjectionJson(
                requestJson: request(owner: owner, groupID: groupID, aliasEpoch: 6)
            )),
            .fenceMismatch
        )
        try database.read { db in
            XCTAssertEqual(try Int.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM mls_conversation_projection_alias_retirement"
            ), 0)
            XCTAssertEqual(try Int.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM mls_conversation_projection_canonical"
            ), 0)
        }
    }

    func testObservedAliasEpochMayPrecedeCurrentCanonicalEpoch() throws {
        let owner = "did:plc:receiver"
        let groupID = String(repeating: "de", count: 32)
        try insertPair(
            owner: owner,
            groupID: groupID,
            canonicalEpoch: 9,
            aliasEpoch: 7,
            nativeConversationID: groupID
        )
        let adapter = MLSOrchestratorStorageAdapter(
            dbPool: database,
            userDID: owner,
            mlsContext: context
        )

        XCTAssertEqual(
            try outcome(adapter.commitConversationProjectionJson(
                requestJson: request(owner: owner, groupID: groupID, canonicalEpoch: 9, aliasEpoch: 7)
            )),
            .retired(2)
        )
    }

    func testCrossAccountOwnerIsRejectedBeforeWrites() throws {
        let adapter = MLSOrchestratorStorageAdapter(
            dbPool: database,
            userDID: "did:plc:receiver",
            mlsContext: context
        )
        XCTAssertThrowsError(try adapter.commitConversationProjectionJson(
            requestJson: request(
                owner: "did:plc:other",
                groupID: String(repeating: "ef", count: 32),
                aliasEpoch: 7
            )
        ))
        try database.read { db in
            XCTAssertEqual(try Int.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM mls_conversation_projection_canonical"
            ), 0)
        }
    }

    private func insertPair(
        owner: String,
        groupID: String,
        epoch: Int64
    ) throws {
        try insertPair(
            owner: owner,
            groupID: groupID,
            canonicalEpoch: epoch,
            aliasEpoch: epoch,
            nativeConversationID: stableID
        )
    }

    private func insertPair(
        owner: String,
        groupID: String,
        canonicalEpoch: Int64,
        aliasEpoch: Int64,
        nativeConversationID: String
    ) throws {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        try database.write { db in
            for (id, epoch) in [(stableID, canonicalEpoch), (groupID, aliasEpoch)] {
                try MLSConversationModel(
                    conversationID: id,
                    currentUserDID: owner,
                    groupID: Data(hexEncoded: groupID)!,
                    epoch: epoch,
                    createdAt: now,
                    updatedAt: now
                ).insert(db)
            }
            try db.execute(sql: """
            INSERT INTO mls_orchestrator_group_state
              (group_id, conversation_id, epoch, members_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """, arguments: [groupID, nativeConversationID, canonicalEpoch, Data("[]".utf8), now])
        }
    }

    private func request(
        owner: String,
        groupID: String,
        canonicalEpoch: UInt64 = 7,
        aliasEpoch: UInt64
    ) throws -> String {
        let unknown: [String: Any] = [
            "generation": NSNull(),
            "stateVersion": NSNull(),
            "resetGeneration": NSNull(),
            "resetPendingTargetGroupId": NSNull(),
        ]
        let canonical: [String: Any] = [
            "groupId": groupID,
            "conversationId": stableID,
            "epoch": canonicalEpoch,
            "members": [],
            "metadata": NSNull(),
            "createdAt": "2026-09-19T12:00:00Z",
            "updatedAt": "2026-09-19T12:00:01Z",
            "sequencerDid": NSNull(),
            "canonicalState": NSNull(),
        ]
        let alias: [String: Any] = [
            "ownerUserDid": owner,
            "canonicalConversationId": stableID,
            "canonicalGroupId": groupID,
            "aliasConversationId": groupID,
            "aliasGroupId": groupID,
            "canonicalIncarnation": canonicalEpoch,
            "aliasIncarnation": aliasEpoch,
            "canonicalLifecycle": unknown,
            "aliasLifecycle": unknown,
        ]
        return try String(decoding: JSONSerialization.data(withJSONObject: [
            "ownerUserDid": owner,
            "mode": "live",
            "canonical": canonical,
            "aliases": [alias],
            "canonicalLifecycle": unknown,
        ], options: [.sortedKeys]), as: UTF8.self)
    }

    private func replay(from live: String) throws -> String {
        var object = try XCTUnwrap(
            JSONSerialization.jsonObject(with: Data(live.utf8)) as? [String: Any]
        )
        object["mode"] = "replay"
        object["canonical"] = NSNull()
        return try String(
            decoding: JSONSerialization.data(withJSONObject: object, options: [.sortedKeys]),
            as: UTF8.self
        )
    }

    private func outcome(_ json: String) throws -> MLSConversationProjectionCommit.Outcome {
        try JSONDecoder().decode(
            MLSConversationProjectionCommit.Outcome.self,
            from: Data(json.utf8)
        )
    }
}
