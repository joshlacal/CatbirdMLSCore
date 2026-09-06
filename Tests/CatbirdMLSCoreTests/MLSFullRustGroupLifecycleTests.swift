import XCTest
import CatbirdMLS
import GRDB
import Petrel
import PetrelCatbird

@testable import CatbirdMLSCore

final class MLSFullRustGroupLifecycleTests: XCTestCase {
  private static let stableConversationID = "550e8400-e29b-41d4-a716-446655440000"
  private var storageDirectory: URL!

  override func setUpWithError() throws {
    try super.setUpWithError()
    MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())
    storageDirectory = FileManager.default.temporaryDirectory
      .appendingPathComponent("mls-group-lifecycle-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: storageDirectory, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(storageDirectory)
  }

  override func tearDownWithError() throws {
    MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    try FileManager.default.removeItem(at: storageDirectory)
    try super.tearDownWithError()
  }

  func testUnexpectedCallbackFailureHasHumanLifecycleCopy() {
    struct UnexpectedCallbackFailure: Error {}
    for operation: MLSConversationLifecycleError.Operation in [.open, .leave, .removeMembers] {
      let result = MLSConversationLifecycleError.presenting(UnexpectedCallbackFailure(), operation: operation)
      XCTAssertFalse(result.localizedDescription.contains("UnexpectedCallbackFailure"))
      XCTAssertTrue(result.localizedDescription.contains("try again"))
    }
  }

  func testHistoricalDepartureDenialOffersAnotherDeviceWithoutClaimingLeft() {
    let denied = OrchestratorBridgeError.ServerError(status: 400,
      body: #"{"error":"AccessOutsideMembershipInterval","message":"fixture"}"#)
    let leave = MLSConversationLifecycleError.presenting(denied, operation: .leave)
    XCTAssertTrue(leave.localizedDescription.contains("another device"))
    XCTAssertTrue(leave.localizedDescription.contains("saved messages are still here"))
    XCTAssertFalse(leave.localizedDescription.contains("You left"))
    XCTAssertFalse(MLSConversationLifecycleError.presenting(denied, operation: .open)
      .localizedDescription.contains("another device"))
    for body in ["AccessOutsideMembershipInterval", #"{"message":"AccessOutsideMembershipInterval"}"#,
                 #"{"error":"OtherError"}"#] {
      let other = OrchestratorBridgeError.ServerError(status: 400, body: body)
      XCTAssertFalse(MLSConversationLifecycleError.presenting(other, operation: .leave)
        .localizedDescription.contains("another device"))
    }
  }

  func testRuntimePendingMemberRemovalHasHumanStatus() throws {
    let bridge = RecordingGroupLifecycleBridge()
    bridge.removeError = OrchestratorBridgeError.InvalidInput(message: "conversation_member_removal_pending: confirmation pending")
    let runtime = MLSOrchestratorRuntime(userDID: "did:plc:testuser", mode: .rustFull, bridge: bridge)
    XCTAssertThrowsError(try runtime.removeMembers(conversationId: "convo-rust", memberDids: ["did:plc:bob"])) { error in
      XCTAssertFalse(error.localizedDescription.contains("conversation_member_removal_pending"))
      XCTAssertTrue(error.localizedDescription.contains("waiting"))
    }
  }

  func testRuntimeCreateConversationWrapsBridgeResult() throws {
    let bridge = RecordingGroupLifecycleBridge()
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let result = try runtime.createConversation(
      name: "Rust group",
      initialMemberDids: ["did:plc:bob"],
      description: "unit-test"
    )

    XCTAssertEqual(bridge.createConversationCallCount, 1)
    XCTAssertEqual(bridge.lastCreateConversationName, "Rust group")
    XCTAssertEqual(result.metadata.title, "Rust group")
    XCTAssertEqual(result.metadata.description, "unit-test")
    XCTAssertEqual(result.metadata.avatarUrl, "https://example.com/rust.png")
    XCTAssertEqual(result.conversation.conversationId, "convo-rust")
  }

  func testRustFullCreateGroupPersistsMetadataAndSkipsLegacyGroupCreationPath() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingGroupLifecycleBridge()
    bridge.createConversationResult = FfiCreateConversationResult(
      conversation: makeFFIConversationView(
        conversationID: "convo-rust",
        groupID: "deadbeef",
        epoch: 7,
        members: ["did:plc:testuser", "did:plc:bob"],
        name: "Rust group",
        description: "unit-test",
        avatarUrl: nil
      ),
      commitData: nil,
      welcomeData: nil
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let convo = try await manager.createGroup(
      initialMembers: [try DID(didString: "did:plc:bob")],
      name: "Rust group",
      description: "unit-test",
      avatarUrl: "https://example.com/request-avatar.png"
    )

    XCTAssertEqual(bridge.createConversationCallCount, 1)
    XCTAssertEqual(bridge.lastCreateConversationDescription, "unit-test")
    XCTAssertEqual(convo.conversationId, "convo-rust")
    XCTAssertEqual(manager.conversations["convo-rust"]?.conversationId, "convo-rust")
    XCTAssertEqual(manager.groupStates["deadbeef"]?.epoch, 7)
    let persisted = try await fetchConversation(conversationID: "convo-rust", on: manager)
    XCTAssertEqual(persisted?.title, "Rust group")
    XCTAssertEqual(persisted?.description, "unit-test")
    XCTAssertEqual(persisted?.avatarURL, "https://example.com/request-avatar.png")
  }

  func testRustFullCreateGroupPersistsParticipantRowsWithStableConversationIDAndRoles() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingGroupLifecycleBridge()
    bridge.createConversationResult = FfiCreateConversationResult(
      conversation: FfiConversationView(
        canonicalStateJson: nil,
        groupId: "deadbeef",
        conversationId: Self.stableConversationID,
        epoch: 7,
        members: [
          FfiMemberView(did: "did:plc:testuser", role: "admin"),
          FfiMemberView(did: "did:plc:bob", role: "member"),
        ],
        name: "Rust group",
        description: "unit-test",
        avatarUrl: nil,
        createdAt: ISO8601DateFormatter().string(from: Date()),
        updatedAt: nil
      ),
      commitData: nil,
      welcomeData: nil
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    _ = try await manager.createGroup(
      initialMembers: [try DID(didString: "did:plc:bob")],
      name: "Rust group"
    )

    let members = try await manager.database.read { db in
      try MLSMemberModel
        .filter(MLSMemberModel.Columns.conversationID == Self.stableConversationID)
        .filter(MLSMemberModel.Columns.currentUserDID == "did:plc:testuser")
        .order(MLSMemberModel.Columns.did)
        .fetchAll(db)
    }

    XCTAssertEqual(members.map(\.did), ["did:plc:bob", "did:plc:testuser"])
    XCTAssertEqual(members.map(\.conversationID), [Self.stableConversationID, Self.stableConversationID])
    XCTAssertEqual(members.map(\.role), [.member, .admin])
  }

  func testRustFullSnapshotRetiresRawGroupAliasWithoutDroppingEpochState() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let rawGroupID = "deadbeef"
    let canonicalSeed = MLSConversationModel(
      conversationID: Self.stableConversationID,
      currentUserDID: "did:plc:testuser",
      groupID: Data(hexEncoded: rawGroupID)!,
      epoch: 5,
      title: "Existing canonical"
    )
    let alias = MLSConversationModel(
      conversationID: rawGroupID,
      currentUserDID: "did:plc:testuser",
      groupID: Data(hexEncoded: rawGroupID)!,
      epoch: 3,
      title: "Raw alias"
    )
    let epochKey = MLSEpochKeyModel(
      epochKeyID: "raw-group-epoch-3",
      conversationID: rawGroupID,
      currentUserDID: "did:plc:testuser",
      epoch: 3,
      keyMaterial: Data([0x01, 0x02, 0x03])
    )
    try await manager.database.write { db in
      try canonicalSeed.insert(db)
      try alias.insert(db)
      try epochKey.insert(db)
    }

    let bridge = RecordingGroupLifecycleBridge()
    bridge.createConversationResult = FfiCreateConversationResult(
      conversation: FfiConversationView(
        canonicalStateJson: nil,
        groupId: rawGroupID,
        conversationId: Self.stableConversationID,
        epoch: 7,
        members: [FfiMemberView(did: "did:plc:testuser", role: "admin")],
        name: "Canonical group",
        description: nil,
        avatarUrl: nil,
        createdAt: ISO8601DateFormatter().string(from: Date()),
        updatedAt: nil
      ),
      commitData: nil,
      welcomeData: nil
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    _ = try await manager.createGroup(name: "Canonical group")

    let rows = try await manager.database.read { db in
      let conversations = try MLSConversationModel
        .filter(MLSConversationModel.Columns.currentUserDID == "did:plc:testuser")
        .fetchAll(db)
      let epochKeys = try MLSEpochKeyModel
        .filter(MLSEpochKeyModel.Columns.currentUserDID == "did:plc:testuser")
        .fetchAll(db)
      return (conversations, epochKeys)
    }

    XCTAssertEqual(rows.0.map(\.conversationID), [Self.stableConversationID])
    let canonical = try XCTUnwrap(rows.0.first)
    XCTAssertEqual(canonical.groupID, Data(hexEncoded: rawGroupID))
    XCTAssertEqual(canonical.epoch, 7)
    XCTAssertEqual(rows.1.map(\.conversationID), [Self.stableConversationID])
    XCTAssertEqual(rows.1.first?.keyMaterial, Data([0x01, 0x02, 0x03]))
    XCTAssertEqual(manager.groupStates[rawGroupID]?.convoId, Self.stableConversationID)
  }

  func testRustFullSnapshotPreservesLocalRecoveryMuteAvatarAndJoinFields() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let rawGroupID = "deadbeef"
    let rejoinRequestedAt = Date(timeIntervalSince1970: 1_700_000_010)
    let lastRecoveryAttempt = Date(timeIntervalSince1970: 1_700_000_020)
    let mutedUntil = Date(timeIntervalSince1970: 1_700_000_030)
    let existing = MLSConversationModel(
      conversationID: Self.stableConversationID,
      currentUserDID: "did:plc:testuser",
      groupID: Data(hexEncoded: rawGroupID)!,
      epoch: 4,
      joinMethod: .creator,
      joinEpoch: 1,
      title: "Local title",
      description: "Local description",
      avatarURL: "https://example.com/local.png",
      avatarImageData: Data([0x01, 0x02]),
      needsRejoin: true,
      needsReset: true,
      isUnrecoverable: true,
      rejoinRequestedAt: rejoinRequestedAt,
      lastRecoveryAttempt: lastRecoveryAttempt,
      consecutiveFailures: 4,
      isPlaceholder: true,
      requestState: .pendingInbound,
      mutedUntil: mutedUntil,
      pendingNewGroupId: "cafebabe",
      pendingResetGeneration: 9
    )
    try await manager.database.write { db in
      try existing.insert(db)
    }

    let bridge = RecordingGroupLifecycleBridge()
    bridge.createConversationResult = FfiCreateConversationResult(
      conversation: makeFFIConversationView(
        conversationID: Self.stableConversationID,
        groupID: rawGroupID,
        epoch: 8,
        members: ["did:plc:testuser"],
        name: "Server title",
        description: nil,
        avatarUrl: nil
      ),
      commitData: nil,
      welcomeData: nil
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    _ = try await manager.createGroup(name: "Server title")

    let persisted = try await fetchConversation(conversationID: Self.stableConversationID, on: manager)
    XCTAssertEqual(persisted?.epoch, 4, "Pending reset prevents snapshot epoch promotion")
    XCTAssertEqual(persisted?.joinMethod, .creator)
    XCTAssertEqual(persisted?.joinEpoch, 1)
    XCTAssertEqual(persisted?.description, "Local description")
    XCTAssertEqual(persisted?.avatarURL, "https://example.com/local.png")
    XCTAssertEqual(persisted?.avatarImageData, Data([0x01, 0x02]))
    XCTAssertEqual(persisted?.needsRejoin, true)
    XCTAssertEqual(persisted?.needsReset, true)
    XCTAssertEqual(persisted?.isUnrecoverable, true)
    XCTAssertEqual(persisted?.rejoinRequestedAt, rejoinRequestedAt)
    XCTAssertEqual(persisted?.lastRecoveryAttempt, lastRecoveryAttempt)
    XCTAssertEqual(persisted?.consecutiveFailures, 4)
    XCTAssertEqual(persisted?.requestState, .pendingInbound)
    XCTAssertEqual(persisted?.mutedUntil, mutedUntil)
    XCTAssertEqual(persisted?.pendingNewGroupId, "cafebabe")
    XCTAssertEqual(persisted?.pendingResetGeneration, 9)
  }

  func testRustFullSnapshotReactivatesMemberMergingProfileSecurityAndRemovalState() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let existing = MLSConversationModel(
      conversationID: Self.stableConversationID,
      currentUserDID: "did:plc:testuser",
      groupID: Data(hexEncoded: "deadbeef")!
    )
    let removedAt = Date(timeIntervalSince1970: 1_700_000_040)
    let existingMember = MLSMemberModel(
      memberID: "\(Self.stableConversationID)_did:plc:bob",
      conversationID: Self.stableConversationID,
      currentUserDID: "did:plc:testuser",
      did: "did:plc:bob",
      handle: "bob",
      displayName: "Bob",
      leafIndex: 4,
      credentialData: Data([0x11]),
      signaturePublicKey: Data([0x22]),
      removedAt: removedAt,
      removedBy: "did:plc:former-admin",
      removalReason: "left",
      isActive: false,
      role: .member,
      avatarURL: "https://example.com/bob.png"
    )
    try await manager.database.write { db in
      try existing.insert(db)
      try existingMember.insert(db)
    }

    let bridge = RecordingGroupLifecycleBridge()
    bridge.createConversationResult = FfiCreateConversationResult(
      conversation: makeFFIConversationView(
        conversationID: Self.stableConversationID,
        groupID: "deadbeef",
        epoch: 5,
        members: ["did:plc:bob"],
        name: "Rust group"
      ),
      commitData: nil,
      welcomeData: nil
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    _ = try await manager.createGroup(name: "Rust group")

    let member = try await manager.database.read { db in
      try MLSMemberModel
        .filter(MLSMemberModel.Columns.memberID == "\(Self.stableConversationID)_did:plc:bob")
        .fetchOne(db)
    }
    XCTAssertEqual(member?.conversationID, Self.stableConversationID)
    XCTAssertEqual(member?.handle, "bob")
    XCTAssertEqual(member?.displayName, "Bob")
    XCTAssertEqual(member?.avatarURL, "https://example.com/bob.png")
    XCTAssertEqual(member?.credentialData, Data([0x11]))
    XCTAssertEqual(member?.signaturePublicKey, Data([0x22]))
    XCTAssertEqual(member?.isActive, true)
    XCTAssertNil(member?.removedAt)
    XCTAssertNil(member?.removedBy)
    XCTAssertNil(member?.removalReason)
  }

  func testRustFullEmptySnapshotRetainsPreviouslyHydratedMembers() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let existing = MLSConversationModel(
      conversationID: Self.stableConversationID,
      currentUserDID: "did:plc:testuser",
      groupID: Data(hexEncoded: "deadbeef")!
    )
    let member = MLSMemberModel(
      memberID: "\(Self.stableConversationID)_did:plc:bob",
      conversationID: Self.stableConversationID,
      currentUserDID: "did:plc:testuser",
      did: "did:plc:bob",
      leafIndex: 1,
      role: .member
    )
    try await manager.database.write { db in
      try existing.insert(db)
      try member.insert(db)
    }

    let bridge = RecordingGroupLifecycleBridge()
    bridge.createConversationResult = FfiCreateConversationResult(
      conversation: makeFFIConversationView(
        conversationID: Self.stableConversationID,
        groupID: "deadbeef",
        epoch: 5,
        members: [],
        name: "Rust group"
      ),
      commitData: nil,
      welcomeData: nil
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    _ = try await manager.createGroup(name: "Rust group")

    let persistedMember = try await manager.database.read { db in
      try MLSMemberModel
        .filter(MLSMemberModel.Columns.memberID == "\(Self.stableConversationID)_did:plc:bob")
        .fetchOne(db)
    }
    XCTAssertEqual(persistedMember?.isActive, true)
  }

  func testRustFullAddMembersUsesRuntimeAndSkipsLegacyStageCommitPath() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-rust", on: manager)

    let bridge = RecordingGroupLifecycleBridge()
    bridge.groupMutationResult = makeGroupMutationResult(
      conversationID: "convo-rust",
      groupID: "deadbeef",
      epoch: 9,
      members: ["did:plc:testuser", "did:plc:bob"]
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    try await manager.addMembers(convoId: "convo-rust", memberDids: ["did:plc:bob"])

    XCTAssertEqual(bridge.addMembersCallCount, 1)
    XCTAssertEqual(manager.groupStates["deadbeef"]?.epoch, 9)
    XCTAssertEqual(
      Set(manager.groupStates["deadbeef"]?.members ?? []),
      Set(["did:plc:testuser", "did:plc:bob"])
    )
    let persisted = try await fetchConversation(conversationID: "convo-rust", on: manager)
    XCTAssertEqual(persisted?.title, "Rust group")
    XCTAssertEqual(persisted?.description, "unit-test")
    XCTAssertEqual(persisted?.avatarURL, "https://example.com/rust.png")
  }

  func testRustFullRemoveMemberUsesRuntimeAndSkipsLegacyStageCommitPath() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-rust", on: manager)

    let bridge = RecordingGroupLifecycleBridge()
    bridge.groupMutationResult = makeGroupMutationResult(
      conversationID: "convo-rust",
      groupID: "deadbeef",
      epoch: 11,
      members: ["did:plc:testuser"]
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    try await manager.removeMember(from: "convo-rust", memberDid: "did:plc:bob")

    XCTAssertEqual(bridge.removeMembersCallCount, 1)
    XCTAssertEqual(manager.groupStates["deadbeef"]?.epoch, 11)
    XCTAssertEqual(manager.groupStates["deadbeef"]?.members, ["did:plc:testuser"])
    let persisted = try await fetchConversation(conversationID: "convo-rust", on: manager)
    XCTAssertEqual(persisted?.title, "Rust group")
    XCTAssertEqual(persisted?.description, "unit-test")
    XCTAssertEqual(persisted?.avatarURL, "https://example.com/rust.png")
  }

  func testRustFullLeaveConversationUsesRuntimeAndSkipsLegacyServerPath() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-rust", on: manager)
    seedGroupState(conversationID: "convo-rust", groupID: "deadbeef", on: manager)

    let bridge = RecordingGroupLifecycleBridge()
    bridge.leaveResult = FfiLeaveResult(
      conversationId: "convo-rust",
      groupId: "deadbeef"
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    // The injected bridge stands in for the native path which first persists
    // verified terminal proof. Swift must not invent that proof from success.
    try await manager.database.write { db in
      try db.execute(sql: "INSERT INTO mls_orchestrator_terminal_access VALUES (?, ?, ?, ?)",
        arguments: ["did:plc:testuser", "convo-rust", Data([0xde, 0xad, 0xbe, 0xef]), "closed"])
    }
    let before = try await manager.database.read { db in
      try Data.fetchOne(db, sql: "SELECT payloadJSON FROM MLSMessageModel WHERE conversationID = 'convo-rust'")
    }
    try await manager.leaveConversation(convoId: "convo-rust")

    XCTAssertEqual(bridge.leaveConversationCallCount, 1)
    if case .active = manager.conversationStates["convo-rust"] { XCTFail("Confirmed close must stay read-only") }
    let remainingRows = try await countDurableConversationRows(conversationID: "convo-rust", on: manager)
    XCTAssertEqual(remainingRows, 4)
    let after = try await manager.database.read { db in
      try Data.fetchOne(db, sql: "SELECT payloadJSON FROM MLSMessageModel WHERE conversationID = 'convo-rust'")
    }
    XCTAssertEqual(after, before, "Confirmed leave must preserve exact stored message bytes")
  }

  func testSuccessfulLeaveWithoutPersistedNativeProofDoesNotAnnounceDepartureOrDelete() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-rust", on: manager)
    let bridge = RecordingGroupLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(userDID: "did:plc:testuser", mode: .rustFull, bridge: bridge)
    let observer = MLSStateObserver { event in
      if case .conversationLeft = event { XCTFail("Successful transport without proof must not announce departure") }
    }
    manager.addObserver(observer)
    await XCTAssertThrowsErrorAsync(try await manager.leaveConversation(convoId: "convo-rust")) { _ in }
    let rows = try await countDurableConversationRows(conversationID: "convo-rust", on: manager)
    XCTAssertEqual(rows, 4)
  }

  func testRustFullLeaveConversationPropagatesDurableCleanupFailure() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-rust", on: manager)
    seedGroupState(conversationID: "convo-rust", groupID: "deadbeef", on: manager)

    let bridge = RecordingGroupLifecycleBridge()
    bridge.leaveResult = FfiLeaveResult(
      conversationId: "convo-rust",
      groupId: "deadbeef"
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    try manager.database.close()

    await XCTAssertThrowsErrorAsync(try await manager.leaveConversation(convoId: "convo-rust")) { _ in }

    XCTAssertEqual(bridge.leaveConversationCallCount, 1)
    XCTAssertNotNil(manager.conversations["convo-rust"])
    XCTAssertNotNil(manager.groupStates["deadbeef"])
  }

  func testRustFullFailedLeavePreservesConversationMessagesMembersAndKeys() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-rust", on: manager)
    seedGroupState(conversationID: "convo-rust", groupID: "deadbeef", on: manager)
    let bridge = RecordingGroupLifecycleBridge()
    bridge.leaveError = OrchestratorBridgeError.Api(message: "connection lost")
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser", mode: .rustFull, bridge: bridge
    )
    let observer = MLSStateObserver { event in
      if case .conversationLeft = event { XCTFail("A failed leave must not announce departure") }
    }
    manager.addObserver(observer)

    await XCTAssertThrowsErrorAsync(try await manager.leaveConversation(convoId: "convo-rust")) { _ in }

    XCTAssertEqual(bridge.leaveConversationCallCount, 1)
    XCTAssertNotNil(manager.conversations["convo-rust"])
    XCTAssertNotNil(manager.groupStates["deadbeef"])
    let remainingRows = try await countDurableConversationRows(conversationID: "convo-rust", on: manager)
    XCTAssertEqual(remainingRows, 4, "A failed leave must preserve conversation, message, member, and epoch-key rows")
  }

  func testRustFullPendingLeavePreservesConversationMessagesMembersAndKeys() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-rust", on: manager)
    seedGroupState(conversationID: "convo-rust", groupID: "deadbeef", on: manager)
    let bridge = RecordingGroupLifecycleBridge()
    bridge.leaveError = OrchestratorBridgeError.InvalidInput(message: "conversation_leave_pending: Awaiting another member")
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser", mode: .rustFull, bridge: bridge
    )
    let observer = MLSStateObserver { event in
      if case .conversationLeft = event { XCTFail("A pending leave must not announce departure") }
    }
    manager.addObserver(observer)

    await XCTAssertThrowsErrorAsync(try await manager.leaveConversation(convoId: "convo-rust")) { error in
      XCTAssertTrue(error.localizedDescription.contains("leave request"))
      XCTAssertFalse(error.localizedDescription.contains("InvalidInput"))
      XCTAssertFalse(error.localizedDescription.contains("Failed"))
    }

    XCTAssertNotNil(manager.conversations["convo-rust"])
    XCTAssertNotNil(manager.groupStates["deadbeef"])
    let remainingRows = try await countDurableConversationRows(conversationID: "convo-rust", on: manager)
    XCTAssertEqual(remainingRows, 4)
  }

  func testRuntimeCreateConversationExplainsPendingDeviceAccessWithoutBridgeDetails() throws {
    let bridge = RecordingGroupLifecycleBridge()
    bridge.createError = OrchestratorBridgeError.InvalidInput(message: "conversation_device_access_pending: waiting")
    let runtime = MLSOrchestratorRuntime(userDID: "did:plc:testuser", mode: .rustFull, bridge: bridge)

    XCTAssertThrowsError(try runtime.createConversation(name: "", initialMemberDids: ["did:plc:bob"], description: nil)) { error in
      XCTAssertTrue(error.localizedDescription.contains("this device"))
      XCTAssertFalse(error.localizedDescription.contains("InvalidInput"))
      XCTAssertFalse(error.localizedDescription.contains("conversation_device_access_pending"))
    }
  }

  func testRustAuthoritativeAddMembersKeepsLegacyPreconditions() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustAuthoritative)
    try await seedConversation(conversationID: "convo-rust", on: manager)

    let bridge = RecordingGroupLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustAuthoritative,
      bridge: bridge
    )

    await XCTAssertThrowsErrorAsync(try await manager.addMembers(convoId: "convo-rust", memberDids: ["did:plc:bob"])) { error in
      guard case MLSConversationError.groupStateNotFound = error else {
        return XCTFail("Expected groupStateNotFound, got \(error)")
      }
    }

    XCTAssertEqual(bridge.addMembersCallCount, 0)
  }

  func testRustFullAcceptConversationRequestKeepsConsentUntilValidatedActivePolicy() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingGroupLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let convoId = "convo-pending-1"
    try await manager.database.write { db in
      let convo = MLSConversationModel(
        conversationID: convoId,
        currentUserDID: "did:plc:testuser",
        groupID: Data([0xde, 0xad, 0xbe, 0xef]),
        requestState: .pendingInbound
      )
      try convo.insert(db)
    }

    try await manager.acceptConversationRequest(convoId: convoId)

    XCTAssertEqual(bridge.acceptConversationCallCount, 1)
    XCTAssertEqual(bridge.lastAcceptConversationId, convoId)

    let updatedConvo = try await manager.database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == convoId)
        .fetchOne(db)
    }
    XCTAssertEqual(updatedConvo?.requestState, MLSRequestState.pendingInbound, "Accepted transport with no validated policy snapshot cannot fabricate active consent")
  }

  func testRustFullAcceptConversationRequestFailsClosedWhenRustThrows() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingGroupLifecycleBridge()
    bridge.shouldFailAcceptConversation = true
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let convoId = "convo-pending-2"
    try await manager.database.write { db in
      let convo = MLSConversationModel(
        conversationID: convoId,
        currentUserDID: "did:plc:testuser",
        groupID: Data([0xde, 0xad, 0xbe, 0xef]),
        requestState: .pendingInbound
      )
      try convo.insert(db)
    }

    await XCTAssertThrowsErrorAsync(try await manager.acceptConversationRequest(convoId: convoId)) { _ in
      // Expected failure
    }

    XCTAssertEqual(bridge.acceptConversationCallCount, 1)

    let unchangedConvo = try await manager.database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == convoId)
        .fetchOne(db)
    }
    XCTAssertEqual(unchangedConvo?.requestState, .pendingInbound, "Local row must NOT be flipped to accepted on failure")
  }

  func testRustSyncPersistsMetadataEpochAndAccountRosterBeforePublishingCache() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: Self.stableConversationID, on: manager)
    try await manager.database.write { db in
      try db.execute(sql: "UPDATE MLSMemberModel SET handle = 'saved.handle', displayName = 'Saved profile' WHERE conversationID = ?", arguments: [Self.stableConversationID])
    }
    let bridge = RecordingGroupLifecycleBridge()
    bridge.listResult = [FfiConversationView(
      canonicalStateJson: nil,
      groupId: "deadbeef",
      conversationId: Self.stableConversationID,
      epoch: 9,
      members: [FfiMemberView(did: "did:plc:testuser", role: "member"), FfiMemberView(did: "did:plc:bob", role: "admin")],
      name: "Current native title",
      description: "Current description",
      avatarUrl: nil,
      createdAt: nil,
      updatedAt: nil)]
    let runtime = MLSOrchestratorRuntime(userDID: "did:plc:testuser", mode: .rustFull, bridge: bridge)
    try await manager.hydrateSwiftCachesFromRustSnapshotsAfterRustSync(runtime: runtime, reason: "test")
    let model = try await fetchConversation(conversationID: Self.stableConversationID, on: manager)
    XCTAssertEqual(model?.epoch, 9)
    XCTAssertEqual(model?.title, "Current native title")
    XCTAssertEqual(model?.description, "Current description")
    let members = try await manager.database.read { db in
      try MLSMemberModel.filter(MLSMemberModel.Columns.conversationID == Self.stableConversationID)
        .filter(MLSMemberModel.Columns.isActive == true).order(MLSMemberModel.Columns.did).fetchAll(db)
    }
    XCTAssertEqual(members.map(\.did), ["did:plc:bob", "did:plc:testuser"])
    XCTAssertEqual(members.map(\.role), [.admin, .member])
    XCTAssertEqual(members.last?.handle, "saved.handle")
    XCTAssertEqual(members.last?.displayName, "Saved profile")
    XCTAssertEqual(manager.conversations[Self.stableConversationID]?.epoch, 9)
    XCTAssertEqual(manager.groupStates["deadbeef"]?.members, Set(["did:plc:testuser", "did:plc:bob"]))
    let rows = try await countDurableConversationRows(conversationID: Self.stableConversationID, on: manager)
    XCTAssertEqual(rows, 5, "Projection preserves the existing message and epoch key while adding one account row")
  }

  func testRustSnapshotCannotOverwriteTerminalCoordinatesRosterOrMetadata() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: Self.stableConversationID, on: manager)
    try await manager.database.write { db in
      try db.execute(sql: "INSERT INTO mls_orchestrator_terminal_access VALUES (?, ?, ?, ?)",
        arguments: ["did:plc:testuser", Self.stableConversationID, Data([0xde, 0xad, 0xbe, 0xef]), "device_removed"])
    }
    let bridge = RecordingGroupLifecycleBridge()
    bridge.listResult = [makeFFIConversationView(conversationID: Self.stableConversationID, groupID: "01020304", epoch: 99, members: ["did:plc:mallory"], name: "Stale title")]
    let runtime = MLSOrchestratorRuntime(userDID: "did:plc:testuser", mode: .rustFull, bridge: bridge)
    try await manager.hydrateSwiftCachesFromRustSnapshotsAfterRustSync(runtime: runtime, reason: "terminal-test")
    let model = try await fetchConversation(conversationID: Self.stableConversationID, on: manager)
    XCTAssertEqual(model?.groupID, Data([0xde, 0xad, 0xbe, 0xef]))
    XCTAssertEqual(model?.epoch, 0)
    XCTAssertNil(model?.title)
    if case .active = manager.conversationStates[Self.stableConversationID] { XCTFail("Terminal projection must not publish active") }
    let rows = try await countDurableConversationRows(conversationID: Self.stableConversationID, on: manager)
    XCTAssertEqual(rows, 4)
  }

  func testRustSnapshotRejectsOldEpochAndPendingResetWithoutChangingSavedData() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: Self.stableConversationID, on: manager)
    try await manager.database.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET epoch = 12, title = 'Keep title' WHERE conversationID = ?", arguments: [Self.stableConversationID])
    }
    let bridge = RecordingGroupLifecycleBridge()
    bridge.listResult = [makeFFIConversationView(conversationID: Self.stableConversationID, groupID: "deadbeef", epoch: 3, members: ["did:plc:mallory"])]
    let runtime = MLSOrchestratorRuntime(userDID: "did:plc:testuser", mode: .rustFull, bridge: bridge)
    try await manager.hydrateSwiftCachesFromRustSnapshotsAfterRustSync(runtime: runtime, reason: "stale-test")
    var model = try await fetchConversation(conversationID: Self.stableConversationID, on: manager)
    XCTAssertEqual(model?.epoch, 12)
    XCTAssertEqual(model?.title, "Keep title")
    try await manager.database.write { db in
      try db.execute(sql: "UPDATE MLSConversationModel SET needsReset = 1, pendingNewGroupId = '01020304', pendingResetGeneration = 7 WHERE conversationID = ?", arguments: [Self.stableConversationID])
    }
    bridge.listResult = [makeFFIConversationView(conversationID: Self.stableConversationID, groupID: "ffffffff", epoch: 99, members: ["did:plc:mallory"])]
    try await manager.hydrateSwiftCachesFromRustSnapshotsAfterRustSync(runtime: runtime, reason: "reset-test")
    model = try await fetchConversation(conversationID: Self.stableConversationID, on: manager)
    XCTAssertEqual(model?.epoch, 12)
    XCTAssertEqual(model?.pendingNewGroupId, "01020304")
    XCTAssertEqual(model?.pendingResetGeneration, 7)
    XCTAssertTrue(model?.needsReset == true)
    if case .active = manager.conversationStates[Self.stableConversationID] { XCTFail("Reset fence must not publish active") }
  }

  func testRustSnapshotCannotRebindDifferentGroupWithoutVerifiedAdoption() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: Self.stableConversationID, on: manager)
    let bridge = RecordingGroupLifecycleBridge()
    bridge.listResult = [makeFFIConversationView(conversationID: Self.stableConversationID, groupID: "01020304", epoch: 99, members: ["did:plc:mallory"], name: "Wrong generation")]
    let runtime = MLSOrchestratorRuntime(userDID: "did:plc:testuser", mode: .rustFull, bridge: bridge)
    try await manager.hydrateSwiftCachesFromRustSnapshotsAfterRustSync(runtime: runtime, reason: "generation-test")
    let model = try await fetchConversation(conversationID: Self.stableConversationID, on: manager)
    XCTAssertEqual(model?.groupID, Data([0xde, 0xad, 0xbe, 0xef]))
    XCTAssertEqual(model?.epoch, 0)
    XCTAssertNil(model?.title)
    XCTAssertEqual(manager.conversations[Self.stableConversationID]?.groupId, "deadbeef")
    XCTAssertNil(manager.groupStates["01020304"])
    let rows = try await countDurableConversationRows(conversationID: Self.stableConversationID, on: manager)
    XCTAssertEqual(rows, 4)
  }

  private func makeManager(
    protocolAuthorityMode: MLSProtocolAuthorityMode
  ) async throws -> MLSConversationManager {
    let database = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(database)
    // The real storage adapter owns this table; this fixture injects a bridge without it.
    try await database.write { db in
      try db.execute(sql: "CREATE TABLE mls_orchestrator_terminal_access (user_did TEXT NOT NULL, conversation_id TEXT NOT NULL, group_id BLOB NOT NULL, state TEXT NOT NULL, PRIMARY KEY(user_did, conversation_id))")
    }
    let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: atProtoClient,
      environment: .custom(serviceDID: "did:web:example.com#atproto_mls")
    )
    let manager = MLSConversationManager(
      apiClient: apiClient,
      database: database,
      userDid: "did:plc:testuser",
      atProtoClient: atProtoClient,
      protocolAuthorityMode: protocolAuthorityMode
    )
    // This fixture injects a fake Rust runtime without key custody. Authorization
    // failure/retry is exercised separately by MLSDeviceAuthorizationTests.
    try await manager.rustDeviceAuthorizationGate.ensure(
      scope: manager.rustDeviceAuthorizationScope(for: "did:plc:testuser")) {}
    return manager
  }

  private func seedConversation(
    conversationID: String,
    on manager: MLSConversationManager
  ) async throws {
    let model = MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: "did:plc:testuser",
      groupID: Data([0xde, 0xad, 0xbe, 0xef])
    )
    try await manager.database.write { db in
      try model.insert(db)
      try MLSMemberModel(
        memberID: "\(conversationID)_did:plc:testuser",
        conversationID: conversationID,
        currentUserDID: "did:plc:testuser",
        did: "did:plc:testuser",
        leafIndex: 0,
        role: .admin
      ).insert(db)
      try MLSEpochKeyModel(
        epochKeyID: "\(conversationID)_epoch_1",
        conversationID: conversationID,
        currentUserDID: "did:plc:testuser",
        epoch: 1,
        keyMaterial: Data([0x01, 0x02])
      ).insert(db)
      try MLSMessageModel(
        messageID: "\(conversationID)_message_1",
        currentUserDID: "did:plc:testuser",
        conversationID: conversationID,
        senderID: "did:plc:testuser",
        epoch: 1,
        sequenceNumber: 1,
        isDelivered: true,
        isSent: true
      ).insert(db)
    }
    manager.conversations[conversationID] = model.asConversationState()
  }

  private func fetchConversation(
    conversationID: String,
    on manager: MLSConversationManager
  ) async throws -> MLSConversationModel? {
    try await manager.database.read { db in
      try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == "did:plc:testuser")
        .fetchOne(db)
    }
  }

  private func countDurableConversationRows(
    conversationID: String,
    on manager: MLSConversationManager
  ) async throws -> Int {
    try await manager.database.read { db in
      let conversationCount = try MLSConversationModel
        .filter(MLSConversationModel.Columns.conversationID == conversationID)
        .filter(MLSConversationModel.Columns.currentUserDID == "did:plc:testuser")
        .fetchCount(db)
      let memberCount = try MLSMemberModel
        .filter(MLSMemberModel.Columns.conversationID == conversationID)
        .filter(MLSMemberModel.Columns.currentUserDID == "did:plc:testuser")
        .fetchCount(db)
      let epochKeyCount = try MLSEpochKeyModel
        .filter(MLSEpochKeyModel.Columns.conversationID == conversationID)
        .filter(MLSEpochKeyModel.Columns.currentUserDID == "did:plc:testuser")
        .fetchCount(db)
      let messageCount = try MLSMessageModel
        .filter(MLSMessageModel.Columns.conversationID == conversationID)
        .filter(MLSMessageModel.Columns.currentUserDID == "did:plc:testuser")
        .fetchCount(db)
      return conversationCount + memberCount + epochKeyCount + messageCount
    }
  }

  private func seedGroupState(
    conversationID: String,
    groupID: String,
    on manager: MLSConversationManager
  ) {
    manager.groupStates[groupID] = MLSGroupState(
      groupId: groupID,
      convoId: conversationID,
      epoch: 1,
      members: [],
      knownServerEpoch: nil
    )
  }

  private func makeGroupMutationResult(
    conversationID: String,
    groupID: String,
    epoch: UInt64,
    members: [String]
  ) -> FfiGroupMutationResult {
    FfiGroupMutationResult(
      conversation: makeFFIConversationView(
        conversationID: conversationID,
        groupID: groupID,
        epoch: epoch,
        members: members
      )
    )
  }

  private func makeFFIConversationView(
    conversationID: String,
    groupID: String,
    epoch: UInt64,
    members: [String],
    name: String = "Rust group",
    description: String? = "unit-test",
    avatarUrl: String? = "https://example.com/rust.png"
  ) -> FfiConversationView {
    FfiConversationView(
      canonicalStateJson: nil,
      groupId: groupID,
      conversationId: conversationID,
      epoch: epoch,
      members: members.map {
        FfiMemberView(
          did: $0,
          role: $0 == "did:plc:testuser" ? "admin" : "member"
        )
      },
      name: name,
      description: description,
      avatarUrl: avatarUrl,
      createdAt: ISO8601DateFormatter().string(from: Date()),
      updatedAt: nil
    )
  }
}

private final class RecordingGroupLifecycleBridge: OrchestratorBridge {
  var listResult: [FfiConversationView] = []

  override func listConversations(userDid: String) throws -> [FfiConversationView] { listResult }
  var createConversationResult = FfiCreateConversationResult(
    conversation: RecordingGroupLifecycleBridge.makeFFIConversationView(
      conversationID: "convo-rust",
      groupID: "deadbeef",
      epoch: 7,
      members: ["did:plc:testuser", "did:plc:bob"]
    ),
    commitData: nil,
    welcomeData: nil
  )
  var groupMutationResult = FfiGroupMutationResult(
    conversation: RecordingGroupLifecycleBridge.makeFFIConversationView(
      conversationID: "convo-rust",
      groupID: "deadbeef",
      epoch: 7,
      members: ["did:plc:testuser"]
    )
  )
  var leaveResult = FfiLeaveResult(
    conversationId: "convo-rust",
    groupId: "deadbeef"
  )

  private(set) var createConversationCallCount = 0
  private(set) var addMembersCallCount = 0
  private(set) var removeMembersCallCount = 0
  private(set) var leaveConversationCallCount = 0
  private(set) var lastCreateConversationName: String?
  private(set) var lastCreateConversationDescription: String?

  var shouldFailAcceptConversation = false
  var leaveError: Error?
  var createError: Error?
  var removeError: Error?
  private(set) var acceptConversationCallCount = 0
  private(set) var lastAcceptConversationId: String?

  init() {
    super.init(noPointer: .init())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    super.init(unsafeFromRawPointer: pointer)
  }

  override func createConversation(
    name: String,
    initialMembers: [String]?,
    description: String?
  ) throws -> FfiCreateConversationResult {
    createConversationCallCount += 1
    lastCreateConversationName = name
    lastCreateConversationDescription = description
    if let createError { throw createError }
    return createConversationResult
  }

  override func addMembersResult(
    conversationId: String,
    memberDids: [String]
  ) throws -> FfiGroupMutationResult {
    addMembersCallCount += 1
    return groupMutationResult
  }

  override func removeMembersResult(
    conversationId: String,
    memberDids: [String]
  ) throws -> FfiGroupMutationResult {
    removeMembersCallCount += 1
    if let removeError { throw removeError }
    return groupMutationResult
  }

  override func leaveConversation(
    conversationId: String
  ) throws -> FfiLeaveResult {
    leaveConversationCallCount += 1
    if let leaveError { throw leaveError }
    return leaveResult
  }

  override func acceptConversation(conversationId: String) throws {
    acceptConversationCallCount += 1
    lastAcceptConversationId = conversationId
    if shouldFailAcceptConversation {
      throw OrchestratorBridgeError.InvalidInput(message: "Simulated accept failure")
    }
  }

  override func shutdown() {
  }

  private static func makeFFIConversationView(
    conversationID: String,
    groupID: String,
    epoch: UInt64,
    members: [String],
    name: String = "Rust group",
    description: String? = "unit-test",
    avatarUrl: String? = "https://example.com/rust.png"
  ) -> FfiConversationView {
    FfiConversationView(
      canonicalStateJson: nil,
      groupId: groupID,
      conversationId: conversationID,
      epoch: epoch,
      members: members.map {
        FfiMemberView(
          did: $0,
          role: $0 == "did:plc:testuser" ? "admin" : "member"
        )
      },
      name: name,
      description: description,
      avatarUrl: avatarUrl,
      createdAt: ISO8601DateFormatter().string(from: Date()),
      updatedAt: nil
    )
  }
}

private func XCTAssertThrowsErrorAsync<T>(
  _ expression: @autoclosure () async throws -> T,
  _ handler: (Error) -> Void
) async {
  do {
    _ = try await expression()
    XCTFail("Expected error to be thrown")
  } catch {
    handler(error)
  }
}
