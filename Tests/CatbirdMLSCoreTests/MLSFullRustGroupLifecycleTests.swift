import XCTest
import CatbirdMLS
import GRDB
import Petrel
import PetrelCatbird

@testable import CatbirdMLSCore

final class MLSFullRustGroupLifecycleTests: XCTestCase {
  private static let stableConversationID = "550e8400-e29b-41d4-a716-446655440000"

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
    XCTAssertEqual(persisted?.epoch, 8)
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

    try await manager.leaveConversation(convoId: "convo-rust")

    XCTAssertEqual(bridge.leaveConversationCallCount, 1)
    XCTAssertNil(manager.conversations["convo-rust"])
    XCTAssertNil(manager.groupStates["deadbeef"])
    let remainingRows = try await countDurableConversationRows(conversationID: "convo-rust", on: manager)
    XCTAssertEqual(remainingRows, 0)
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

  func testRustFullAcceptConversationRequestCallsRustAndMarksAccepted() async throws {
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
    XCTAssertEqual(updatedConvo?.requestState, MLSRequestState.none)
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

  private func makeManager(
    protocolAuthorityMode: MLSProtocolAuthorityMode
  ) async throws -> MLSConversationManager {
    let database = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(database)
    let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let apiClient = await MLSAPIClient(
      client: atProtoClient,
      environment: .custom(serviceDID: "did:web:example.com#atproto_mls")
    )
    return MLSConversationManager(
      apiClient: apiClient,
      database: database,
      userDid: "did:plc:testuser",
      atProtoClient: atProtoClient,
      protocolAuthorityMode: protocolAuthorityMode
    )
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
    return groupMutationResult
  }

  override func leaveConversation(
    conversationId: String
  ) throws -> FfiLeaveResult {
    leaveConversationCallCount += 1
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
