import CatbirdMLS
import Foundation
import GRDB
import Petrel
import PetrelCatbird
import XCTest
@testable import CatbirdMLSCore

final class MLSCanonicalPolicyProjectionTests: XCTestCase {
  private let account = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
  private let otherAccount = "did:plc:cccccccccccccccccccccccc"
  private let cid = "550e8400-e29b-41d4-a716-446655440000"
  private var directory: URL!

  override func setUpWithError() throws {
    try super.setUpWithError()
    MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())
    directory = FileManager.default.temporaryDirectory.appendingPathComponent("mls-policy-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(directory)
  }

  override func tearDownWithError() throws {
    MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    try? FileManager.default.removeItem(at: directory)
    try super.tearDownWithError()
  }

  func testSwiftAPIRoutingAndNativePolicySnapshotsAlternateWithoutChangingConsent() async throws {
    let manager = try await makeManager()
    let pendingPolicy = try snapshot()
    let apiPending = try withSwiftRouting(pendingPolicy)
    expectTrue(try await apply(manager, json: apiPending))
    expectEqual(try await model(manager).requestState, .pendingInbound)
    // The shared native read codec validates/removes top-level routing hints;
    // the FFI display therefore carries the same policy without these fields.
    expectTrue(try await apply(manager, json: pendingPolicy, nativeProjection: true))
    expectTrue(try await pendingConsent(manager))
    expectTrue(try await apply(manager, json: apiPending))
    expectTrue(try await apply(manager, json: withSwiftRouting(pendingPolicy,
      sequencerDID: "did:web:next-sequencer.example", term: 1)))
    expectTrue(try await pendingConsent(manager))

    let activePolicy = try snapshot(version: 2, pending: false)
    expectTrue(try await apply(manager, json: withSwiftRouting(activePolicy)))
    expectTrue(try await apply(manager, json: activePolicy, nativeProjection: true))
    expectTrue(try await apply(manager, json: withSwiftRouting(activePolicy)))
    expectFalse(try await pendingConsent(manager))
    expectFalse(try await apply(manager, json: apiPending), "Routing cannot revive stale pending policy")
    do {
      _ = try await apply(manager, json: withSwiftRouting(snapshot(version: 2)))
      XCTFail("Routing cannot hide an equal-version participant conflict")
    } catch { }

    let revokedPolicy = try snapshot(version: 2, pending: false, revoked: true)
    let apiRevoked = try withSwiftRouting(revokedPolicy)
    expectTrue(try await apply(manager, json: apiRevoked))
    expectTrue(try await apply(manager, json: revokedPolicy, nativeProjection: true))
    expectTrue(try await apply(manager, json: apiRevoked))
    expectFalse(try await apply(manager, json: withSwiftRouting(snapshot(version: 2, pending: false, sequence: 50))),
      "Routing cannot revive a revoked device even on a newer application page")
    let savedJSON = try await manager.database.read { [account, cid] db in
      try String.fetchOne(db, sql: "SELECT canonical_state_json FROM mls_orchestrator_canonical_policy WHERE user_did = ? AND conversation_id = ?",
        arguments: [account, cid])
    }
    expectEqual(savedJSON, apiRevoked, "Persist the exact input JSON, including valid routing hints")
    expectEqual(try await retainedBytes(manager), [Data([1, 2, 3]), Data([4, 5, 6]), Data([7, 8, 9])])
  }

  func testRoutingOptionalsValidateOnlyTheirReadDomainAndAllowNullOrAbsence() throws {
    let policy = try snapshot()
    func withRawField(_ key: String, _ value: String) -> String {
      String(policy.dropLast()) + ",\"\(key)\":\(value)}"
    }
    for (key, value) in [("sequencerDid", "null"), ("sequencerTerm", "null"),
                         ("sequencerDid", "\"did:web:chat.catbird.blue\""),
                         ("sequencerTerm", "0"), ("sequencerTerm", "9007199254740991")] {
      XCTAssertNoThrow(try MLSCanonicalPolicyProjection.decode(withRawField(key, value),
        conversationID: cid, groupID: Data(repeating: 0xab, count: 32)))
    }
    for (key, value) in [("sequencerDid", "17"), ("sequencerDid", "{}"),
                         ("sequencerDid", "\"https://chat.catbird.blue\""),
                         ("sequencerTerm", "true"), ("sequencerTerm", "\"0\""),
                         ("sequencerTerm", "-1"), ("sequencerTerm", "9007199254740992"),
                         ("sequencerTerm", "0.0"), ("sequencerTerm", "0e0"), ("sequencerTerm", "1e1")] {
      XCTAssertThrowsError(try MLSCanonicalPolicyProjection.decode(withRawField(key, value),
        conversationID: cid, groupID: Data(repeating: 0xab, count: 32)), "Invalid routing value: \(key)=\(value)")
    }
  }

  func testPendingAcceptanceAtSameEpochSurvivesReopenAndRejectsStalePending() async throws {
    let manager = try await makeManager()
    expectTrue(try await apply(manager, json: snapshot()))
    expectEqual(try await model(manager).requestState, .pendingInbound)
    expectTrue(try await apply(manager, json: snapshot(version: 2, pending: false)))
    expectEqual(try await model(manager).requestState, .none)
    expectEqual(try await model(manager).epoch, 0)
    expectFalse(try await apply(manager, json: snapshot()))
    expectEqual(try await model(manager).requestState, .none)

    let reopened = try DatabaseQueue(path: directory.appendingPathComponent("messages.sqlite").path)
    let saved = try await reopened.read { [account, cid] db in
      try MLSCanonicalPolicyProjection.storedState(userDID: account, conversationID: cid, in: db)
    }
    expectEqual(saved?.coordinates.stateVersion, 2)
    expectEqual(saved?.participants.last?.status, .value_active)
    expectEqual(saved?.participants.last?.leafCount, 0, "Acceptance does not fabricate a device leaf")
    XCTAssertNotNil(saved?.participants.last?.invitationProvenance)
    expectEqual(try await retainedBytes(manager), [Data([1, 2, 3]), Data([4, 5, 6]), Data([7, 8, 9])])
  }

  func testSnapshotSequenceCanAdvanceWithoutChangingPolicyButEqualVersionConflictIsRejected() async throws {
    let manager = try await makeManager()
    expectTrue(try await apply(manager, json: snapshot()))
    expectTrue(try await apply(manager, json: snapshot(sequence: 9)))
    expectFalse(try await apply(manager, json: snapshot(sequence: 2)))
    do {
      _ = try await apply(manager, json: snapshot(pending: false, sequence: 10))
      XCTFail("Equal policy version must not change pending consent")
    } catch { }
    let saved = try await manager.database.read { [account, cid] db in
      try MLSCanonicalPolicyProjection.storedState(userDID: account, conversationID: cid, in: db)
    }
    expectEqual(saved?.snapshotSeq, 9)
    expectEqual(try await model(manager).requestState, .pendingInbound)
  }

  func testDirectoryRevocationOverlayDoesNotNeedPolicyVersionOrSequenceAndCannotRevive() async throws {
    let manager = try await makeManager()
    expectTrue(try await apply(manager, json: snapshot()))
    expectTrue(try await apply(manager, json: snapshot(revoked: true)))
    expectFalse(try await apply(manager, json: snapshot(sequence: 50)), "A newer page cannot revive a revoked device")
    expectTrue(try await apply(manager, json: snapshot(sequence: 50, revoked: true)))
    let saved = try await manager.database.read { [account, cid] db in
      try MLSCanonicalPolicyProjection.storedState(userDID: account, conversationID: cid, in: db)
    }
    expectEqual(saved?.coordinates.stateVersion, 1)
    expectEqual(saved?.snapshotSeq, 50)
    expectEqual(saved?.leaves.first?.deviceStatus, .value_revoked)
    expectEqual(try await model(manager).requestState, .pendingInbound)
    do {
      _ = try await apply(manager, json: snapshot(pending: false, sequence: 51, revoked: true))
      XCTFail("Revocation cannot smuggle an equal-version participant policy change")
    } catch { }
  }

  func testUnknownSnapshotCannotReplaceCanonicalPolicyOrClearRequestState() async throws {
    let manager = try await makeManager()
    expectTrue(try await apply(manager, json: snapshot()))
    let unknown = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationState.self,
      from: Data(snapshot(version: 99, pending: false).utf8))
    expectFalse(try await manager.applyRustConversationSnapshot(unknown,
      metadata: .init(title: "Untrusted legacy title")))
    expectEqual(try await model(manager).requestState, .pendingInbound)
    expectEqual(try await model(manager).title, "Saved history")
  }

  func testNativeDisplayEpochDoesNotRewriteHistoricalCanonicalCoordinates() async throws {
    let manager = try await makeManager(epoch: 8)
    expectTrue(try await apply(manager, json: snapshot(), nativeEpoch: 8))
    expectEqual(try await model(manager).epoch, 8)
    expectEqual(manager.groupStates[String(repeating: "ab", count: 32)]?.epoch, 8)
    expectEqual(manager.conversations[cid]?.coordinates.epoch, 0)
    let saved = try await manager.database.read { [account, cid] db in
      try MLSCanonicalPolicyProjection.storedState(userDID: account, conversationID: cid, in: db)
    }
    expectEqual(saved?.coordinates.epoch, 0)
    expectEqual(try await model(manager).requestState, .pendingInbound)
  }

  func testPendingResetAndAccountSwitchPreventPolicyWrites() async throws {
    let manager = try await makeManager()
    try await manager.database.write { [account, cid] db in
      try db.execute(sql: "UPDATE MLSConversationModel SET needsReset = 1, pendingNewGroupId = ?, pendingResetGeneration = 4 WHERE conversationID = ? AND currentUserDID = ?",
        arguments: [String(repeating: "cd", count: 32), cid, account])
    }
    expectFalse(try await apply(manager, json: snapshot()))
    expectTrue(try await model(manager).needsReset)
    expectEqual(try await model(manager).pendingResetGeneration, 4)
    manager.isShuttingDown = true
    do { _ = try await apply(manager, json: snapshot()); XCTFail("Shutdown must fence late projection") }
    catch { }
    manager.isShuttingDown = false
    manager.userDid = otherAccount
    do { _ = try await apply(manager, json: snapshot()); XCTFail("Foreign account is absent from canonical roster") }
    catch { }
    let saved = try await manager.database.read { [account, cid] db in
      try MLSCanonicalPolicyProjection.storedState(userDID: account, conversationID: cid, in: db)
    }
    XCTAssertNil(saved)
  }

  func testMalformedOrMismatchedCanonicalStateCannotAlterSavedData() async throws {
    let manager = try await makeManager()
    let state = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationState.self, from: Data(snapshot().utf8))
    for bad in try ["not-json", snapshot(conversationID: "650e8400-e29b-41d4-a716-446655440000"),
                snapshot(groupByte: 0xcd), snapshot(version: -1)] {
      do {
        _ = try await manager.applyRustConversationSnapshot(state, metadata: .init(canonicalStateJson: bad))
        XCTFail("Malformed or scope-mismatched canonical state must fail")
      } catch { }
    }
    expectEqual(try await model(manager).requestState, .none)
    expectEqual(try await model(manager).title, "Saved history")
    expectEqual(try await retainedBytes(manager), [Data([1, 2, 3]), Data([4, 5, 6]), Data([7, 8, 9])])
  }

  func testLegacyTerminalUpgradeRetainsHistoryAndUnknownPolicy() async throws {
    let manager = try await makeManager()
    try await manager.database.write { [account, cid] db in
      try db.execute(sql: "INSERT INTO mls_orchestrator_terminal_access VALUES (?, ?, ?, 'closed')",
        arguments: [account, cid, Data(repeating: 0xab, count: 32)])
    }
    let state = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationState.self, from: Data(snapshot().utf8))
    expectFalse(try await manager.applyRustConversationSnapshot(state,
      metadata: .init(title: "Replacement", canonicalStateJson: nil)))
    try await manager.hydrateSwiftCachesFromDatabaseAfterRustSync(reason: "legacy-terminal-upgrade")
    expectEqual(try await model(manager).title, "Saved history")
    expectEqual(try await model(manager).requestState, .none)
    XCTAssertNotNil(manager.conversations[cid], "Unknown policy must not hide retained legacy history")
    if case .active = manager.conversationStates[cid] { XCTFail("Closed crypto access stays terminal") }
    let saved = try await manager.database.read { [account, cid] db in
      try MLSCanonicalPolicyProjection.storedState(userDID: account, conversationID: cid, in: db)
    }
    XCTAssertNil(saved)
    expectEqual(try await retainedBytes(manager), [Data([1, 2, 3]), Data([4, 5, 6]), Data([7, 8, 9])])
  }

  func testNewDeviceRemovedInvitationAndAcceptanceChangeOnlyConsentProjection() async throws {
    let manager = try await makeManager()
    expectTrue(try await apply(manager, json: snapshot(version: 2, pending: false)))
    try await manager.database.write { [account, cid] db in
      try db.execute(sql: "INSERT INTO mls_orchestrator_terminal_access VALUES (?, ?, ?, 'device_removed')",
        arguments: [account, cid, Data(repeating: 0xab, count: 32)])
    }
    expectFalse(try await apply(manager, json: snapshot()))
    // Native has verified that this new invitation postdates its exact exit/removal evidence.
    expectFalse(try await apply(manager, json: snapshot(version: 3)), "Raw API projection cannot authorize a terminal invitation")
    expectTrue(try await apply(manager, json: snapshot(version: 3), nativeProjection: true))
    expectTrue(try await pendingConsent(manager))
    expectEqual(try await model(manager).requestState, .pendingInbound)
    expectTrue(try await apply(manager, json: snapshot(version: 4, pending: false), nativeProjection: true))
    expectFalse(try await pendingConsent(manager))
    expectEqual(try await model(manager).requestState, .none)
    let terminal = try await manager.database.read { [account, cid] db in
      try String.fetchOne(db, sql: "SELECT state FROM mls_orchestrator_terminal_access WHERE user_did = ? AND conversation_id = ?",
        arguments: [account, cid])
    }
    expectEqual(terminal, "device_removed")
    expectEqual(try await retainedBytes(manager), [Data([1, 2, 3]), Data([4, 5, 6]), Data([7, 8, 9])])
  }

  func testLegacyTerminalPendingAndResetPendingNeverExposeConsent() async throws {
    let manager = try await makeManager()
    expectTrue(try await apply(manager, json: snapshot()))
    expectTrue(try await pendingConsent(manager))
    try await manager.database.write { [account, cid] db in
      try db.execute(sql: "INSERT INTO mls_orchestrator_terminal_access VALUES (?, ?, ?, 'device_removed')",
        arguments: [account, cid, Data(repeating: 0xab, count: 32)])
    }
    expectFalse(try await pendingConsent(manager), "Upgrade defaults cannot revive an old pending invitation")
    let state = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationState.self, from: Data(snapshot().utf8))
    expectFalse(try await manager.applyRustConversationSnapshot(state, metadata: .init(canonicalStateJson: nil)))
    expectFalse(try await pendingConsent(manager), "Unknown native state cannot grant terminal invitation authority")
    try await manager.database.write { [account, cid] db in
      try db.execute(sql: "UPDATE mls_orchestrator_canonical_policy SET terminal_invitation_authorized = 1")
      try db.execute(sql: "UPDATE MLSConversationModel SET needsReset = 1, pendingNewGroupId = ? WHERE conversationID = ? AND currentUserDID = ?",
        arguments: [String(repeating: "cd", count: 32), cid, account])
    }
    expectFalse(try await pendingConsent(manager), "Reset fence overrides even a retained invitation flag")
    expectEqual(try await retainedBytes(manager), [Data([1, 2, 3]), Data([4, 5, 6]), Data([7, 8, 9])])
  }

  func testActualResetCallbacksCannotReviveOldTerminalInvitationAfterGroupChange() async throws {
    let manager = try await makeManager()
    expectTrue(try await apply(manager, json: snapshot(version: 2, pending: false)))
    try await manager.database.write { [account, cid] db in
      try db.execute(sql: "INSERT INTO mls_orchestrator_terminal_access VALUES (?, ?, ?, 'device_removed')",
        arguments: [account, cid, Data(repeating: 0xab, count: 32)])
    }
    expectTrue(try await apply(manager, json: snapshot(version: 3), nativeProjection: true))
    expectTrue(try await pendingConsent(manager))
    let before = try await retainedBytes(manager)
    let pool = try XCTUnwrap(manager.database as? DatabasePool)
    let context = try MlsContext(storagePath: directory.appendingPathComponent("reset-native.sqlite").path,
      encryptionKey: String(repeating: "ef", count: 32), keychain: InMemoryKeychainAccess())
    defer { try? context.flushAndPrepareClose() }
    let adapter = MLSOrchestratorStorageAdapter(dbPool: pool, userDID: account, mlsContext: context)
    try adapter.markResetPending(conversationId: cid, newGroupIdHex: String(repeating: "cd", count: 32),
      resetGeneration: 4, notifiedAtMs: 1)
    expectFalse(try await pendingConsent(manager))
    expectTrue(try adapter.completeResetPending(conversationId: cid, expectedGeneration: 4,
      expectedNewGroupIdHex: String(repeating: "cd", count: 32), landedEpoch: 0))
    expectFalse(try await pendingConsent(manager), "Reset completion must not revive a G0 invitation in G1")
    expectEqual(try await model(manager).requestState, .pendingInbound, "An old display value alone carries no authority")
    expectEqual(try await retainedBytes(manager), before)
  }

  private func makeManager(epoch: Int64 = 0) async throws -> MLSConversationManager {
    let db = try DatabasePool(path: directory.appendingPathComponent("messages.sqlite").path)
    try MLSGRDBManager.makeMigrator().migrate(db)
    try await db.write { [account, cid] db in
      try db.execute(sql: "CREATE TABLE mls_orchestrator_terminal_access (user_did TEXT NOT NULL, conversation_id TEXT NOT NULL, group_id BLOB NOT NULL, state TEXT NOT NULL, PRIMARY KEY(user_did, conversation_id))")
      try MLSConversationModel(conversationID: cid, currentUserDID: account,
        groupID: Data(repeating: 0xab, count: 32), epoch: epoch, title: "Saved history").insert(db)
      try MLSMessageModel(messageID: "stored-message", currentUserDID: account, conversationID: cid,
        senderID: account, epoch: 0, sequenceNumber: 1, payloadEncrypted: Data([1, 2, 3]), entryHMAC: Data([4, 5, 6])).insert(db)
      try MLSEpochKeyModel(epochKeyID: "stored-key", conversationID: cid, currentUserDID: account,
        epoch: 0, keyMaterial: Data([7, 8, 9])).insert(db)
    }
    let client = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let api = await MLSAPIClient(client: client, environment: .custom(serviceDID: "did:web:example.com#atproto_mls"))
    return MLSConversationManager(apiClient: api, database: db, userDid: account,
      atProtoClient: client, protocolAuthorityMode: .rustFull)
  }

  private func model(_ manager: MLSConversationManager) async throws -> MLSConversationModel {
    try await manager.database.read { [account, cid] db in
      try XCTUnwrap(MLSConversationModel.fetchOne(db,
        sql: "SELECT * FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
        arguments: [cid, account]))
    }
  }

  private func retainedBytes(_ manager: MLSConversationManager) async throws -> [Data] {
    try await manager.database.read { [account, cid] db in
      let message = try XCTUnwrap(MLSMessageModel.fetchOne(db,
        sql: "SELECT * FROM MLSMessageModel WHERE conversationID = ? AND currentUserDID = ?",
        arguments: [cid, account]))
      let key = try XCTUnwrap(Data.fetchOne(db,
        sql: "SELECT keyMaterial FROM MLSEpochKeyModel WHERE conversationID = ? AND currentUserDID = ?",
        arguments: [cid, account]))
      return [try XCTUnwrap(message.payloadEncrypted), try XCTUnwrap(message.entryHMAC), key]
    }
  }

  private func apply(_ manager: MLSConversationManager, json: String, nativeEpoch: UInt64? = nil,
                     nativeProjection: Bool = false) async throws -> Bool {
    let state = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationState.self, from: Data(json.utf8))
    let metadata: MLSConversationSnapshotMetadata
    if nativeProjection {
      metadata = .init(ffiConversation: FfiConversationView(
        canonicalStateJson: json,
        groupId: state.coordinates.groupId.data.hexEncodedString(),
        conversationId: cid,
        epoch: nativeEpoch ?? UInt64(state.coordinates.epoch),
        members: [],
        name: nil,
        description: nil,
        avatarUrl: nil,
        createdAt: "2026-09-05T01:02:03.000Z",
        updatedAt: nil))
    } else {
      metadata = .init(canonicalStateJson: json, nativeDisplayEpoch: nativeEpoch)
    }
    return try await manager.applyRustConversationSnapshot(state, metadata: metadata)
  }

  private func pendingConsent(_ manager: MLSConversationManager) async throws -> Bool {
    try await manager.database.read { [account, cid] db in
      let row = try XCTUnwrap(MLSConversationModel.fetchOne(db,
        sql: "SELECT * FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
        arguments: [cid, account]))
      return try row.hasPendingConsent(in: db)
    }
  }

  // Values are evaluated before these helpers, allowing real async storage reads.
  private func expectTrue(_ value: Bool, _ message: String = "", file: StaticString = #filePath, line: UInt = #line) {
    XCTAssertTrue(value, message, file: file, line: line)
  }
  private func expectFalse(_ value: Bool, _ message: String = "", file: StaticString = #filePath, line: UInt = #line) {
    XCTAssertFalse(value, message, file: file, line: line)
  }
  private func expectEqual<T: Equatable>(_ lhs: T, _ rhs: T, _ message: String = "", file: StaticString = #filePath, line: UInt = #line) {
    XCTAssertEqual(lhs, rhs, message, file: file, line: line)
  }

  private func withSwiftRouting(_ json: String, sequencerDID: String = "did:web:chat.catbird.blue", term: Int = 0) throws -> String {
    let state = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationState.self, from: Data(json.utf8))
    let routed = BlueCatbirdChatDefs.ConversationState(
      conversationKind: state.conversationKind, coordinates: state.coordinates, cipherSuite: state.cipherSuite,
      participants: state.participants, leaves: state.leaves, metadataSnapshot: state.metadataSnapshot,
      snapshotSeq: state.snapshotSeq, sequencerDid: try DID(didString: sequencerDID), sequencerTerm: term)
    // Same generated DTO encoder used by MLSOrchestratorAPIAdapter.conversationView.
    return String(decoding: try JSONEncoder().encode(routed), as: UTF8.self)
  }

  private func snapshot(version: Int = 1, pending: Bool = true, sequence: Int = 1,
                        conversationID: String? = nil, groupByte: UInt8? = nil, revoked: Bool = false) throws -> String {
    var object = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(Self.codecFixture.utf8)) as? [String: Any])
    var coordinates = try XCTUnwrap(object["coordinates"] as? [String: Any])
    coordinates["stateVersion"] = version
    if let conversationID { coordinates["conversationId"] = conversationID }
    if let groupByte { coordinates["groupId"] = ["$bytes": Data(repeating: groupByte, count: 32).base64EncodedString()] }
    object["coordinates"] = coordinates
    object["snapshotSeq"] = sequence
    var participants = try XCTUnwrap(object["participants"] as? [[String: Any]])
    participants[1]["status"] = pending ? "pending" : "active"
    object["participants"] = participants
    if revoked {
      var leaves = try XCTUnwrap(object["leaves"] as? [[String: Any]])
      leaves[0]["deviceStatus"] = "revoked"
      object["leaves"] = leaves
    }
    return String(decoding: try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys]), as: UTF8.self)
  }

  // Output from the real generated Swift ConversationState JSONEncoder codec,
  // round-tripped through the shared Rust schema-aware codec in integration.
  private static let codecFixture = #"""
{
  "$type" : "blue.catbird.chat.defs#conversationState",
  "cipherSuite" : "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
  "conversationKind" : "group",
  "coordinates" : {
    "$type" : "blue.catbird.chat.defs#conversationCoordinates",
    "confirmationTag" : {
      "$bytes" : "7+\/v7+\/v7+\/v7+\/v7+\/v7+\/v7+\/v7+\/v7+\/v7+\/v7+8="
    },
    "conversationId" : "550e8400-e29b-41d4-a716-446655440000",
    "epoch" : 0,
    "generation" : 0,
    "groupContextHash" : {
      "$bytes" : "zc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc0="
    },
    "groupId" : {
      "$bytes" : "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s="
    },
    "lifecycle" : "active",
    "stateVersion" : 1
  },
  "leaves" : [
    {
      "$type" : "blue.catbird.chat.defs#deviceLeafView",
      "deviceId" : "750e8400-e29b-41d4-a716-446655440000",
      "deviceStatus" : "active",
      "keyId" : "NHUPmL1Z_PyUbaRaqr6TO-FUpLUJThxKv0KGZQXzyX4",
      "leafOrigin" : "genesis",
      "userDid" : "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
    }
  ],
  "metadataSnapshot" : {
    "$type" : "blue.catbird.chat.defs#metadataSnapshot",
    "authorProof" : {
      "$type" : "blue.catbird.chat.defs#metadataAuthorProof",
      "authGenerationAtOrigin" : 1,
      "authorDeviceId" : "750e8400-e29b-41d4-a716-446655440000",
      "authorDid" : "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
      "authorKeyId" : "NHUPmL1Z_PyUbaRaqr6TO-FUpLUJThxKv0KGZQXzyX4",
      "deviceStatusAtOrigin" : "active",
      "originSeq" : 1,
      "originTransitionId" : "650e8400-e29b-41d4-a716-446655440000",
      "roleAtOrigin" : "admin",
      "signaturePublicKey" : {
        "$bytes" : "iojj3XQJ8ZX9UtstPLpdcspnCb8dlBIb83SIAbQPb1w="
      }
    },
    "ciphertext" : {
      "$bytes" : "QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="
    },
    "ciphertextSha256" : {
      "$bytes" : "Ql7U5KNrMOohuQ4hxxLGSeghTCm36vaAidEDnG5VOEw="
    },
    "ciphertextSize" : 32,
    "coordinate" : {
      "$type" : "blue.catbird.chat.defs#metadataCryptoContext",
      "confirmationTag" : {
        "$bytes" : "7+\/v7+\/v7+\/v7+\/v7+\/v7+\/v7+\/v7+\/v7+\/v7+\/v7+8="
      },
      "conversationId" : {
        "$bytes" : "VQ6EAOKbQdSnFkRmVUQAAA=="
      },
      "epoch" : 0,
      "generation" : 0,
      "groupContextHash" : {
        "$bytes" : "zc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc0="
      },
      "groupId" : {
        "$bytes" : "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s="
      }
    },
    "metadataVersion" : 1,
    "nonce" : {
      "$bytes" : "BwcHBwcHBwcHBwcH"
    },
    "originTransitionId" : "650e8400-e29b-41d4-a716-446655440000"
  },
  "participants" : [
    {
      "$type" : "blue.catbird.chat.defs#participantView",
      "leafCount" : 1,
      "role" : "admin",
      "status" : "active",
      "userDid" : "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
    },
    {
      "$type" : "blue.catbird.chat.defs#participantView",
      "invitationProvenance" : {
        "$type" : "blue.catbird.chat.defs#invitationProvenance",
        "invitationTransitionId" : "650e8400-e29b-41d4-a716-446655440000",
        "invitedByDeviceId" : "750e8400-e29b-41d4-a716-446655440000",
        "invitedByDid" : "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
      },
      "leafCount" : 0,
      "role" : "member",
      "status" : "pending",
      "userDid" : "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
    }
  ],
  "snapshotSeq" : 1
}
"""#
}
