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
    XCTAssertEqual(result.conversation.conversationId, "convo-rust")
  }

  func testRustFullCreateGroupUsesRuntimeAndSkipsLegacyGroupCreationPath() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    let bridge = RecordingGroupLifecycleBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let convo = try await manager.createGroup(
      initialMembers: [try DID(didString: "did:plc:bob")],
      name: "Rust group",
      description: "unit-test"
    )

    XCTAssertEqual(bridge.createConversationCallCount, 1)
    XCTAssertEqual(convo.conversationId, "convo-rust")
    XCTAssertEqual(manager.conversations["convo-rust"]?.conversationId, "convo-rust")
    XCTAssertEqual(manager.groupStates["deadbeef"]?.epoch, 7)
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
    }
    manager.conversations[conversationID] = try XCTUnwrap(model.asConvoView())
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
    members: [String]
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
      name: "Rust group",
      description: "unit-test",
      avatarUrl: nil,
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
    members: [String]
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
      name: "Rust group",
      description: "unit-test",
      avatarUrl: nil,
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
