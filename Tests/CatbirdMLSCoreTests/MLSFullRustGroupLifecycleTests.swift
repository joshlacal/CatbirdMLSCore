import XCTest
import CatbirdMLS
import GRDB
import Petrel
import PetrelCatbird

@testable import CatbirdMLSCore

final class MLSFullRustGroupLifecycleTests: XCTestCase {
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
    manager.conversations[conversationID] = try XCTUnwrap(model.asConvoView())
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
