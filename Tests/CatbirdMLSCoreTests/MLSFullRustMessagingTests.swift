import XCTest
import CatbirdMLS
import GRDB
import Petrel
import PetrelCatbird

@testable import CatbirdMLSCore

final class MLSFullRustMessagingTests: XCTestCase {
  private let stableConversationID = "550e8400-e29b-41d4-a716-446655440000"
  private let rawGroupID = "deadbeef"
  private var tempStorageDir: URL!

  override func setUpWithError() throws {
    try super.setUpWithError()
    tempStorageDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSFullRustMessagingTests-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: tempStorageDir, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(tempStorageDir)
  }

  override func tearDownWithError() throws {
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    if let tempStorageDir {
      try? FileManager.default.removeItem(at: tempStorageDir)
    }
    tempStorageDir = nil
    try super.tearDownWithError()
  }

  func testRustFullSendUsesResultBridgeAndSkipsLegacySendPath() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: stableConversationID, on: manager)

    let bridge = RecordingMessagingBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let result = try await manager.sendMessage(
      convoId: stableConversationID,
      plaintext: "hello"
    )

    XCTAssertEqual(bridge.sendPayloadResultCallCount, 1)
    XCTAssertEqual(bridge.sendPayloadJsonCallCount, 0)
    XCTAssertEqual(result.messageId, "msg-1")
    // Public orchestrator mutations use the stable conversation identity.
    // Rust resolves the current MLS group internally.
    XCTAssertEqual(bridge.lastSendPayloadResultConversationId, stableConversationID)
  }

  func testRustFullSendAppliesReturnedEngineEvents() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: stableConversationID, on: manager)
    seedGroupState(conversationID: stableConversationID, groupID: rawGroupID, on: manager)

    let bridge = RecordingMessagingBridge()
    bridge.sendResult = FfiSendResult(
      message: bridge.sendResult.message,
      events: rustResetEvents(conversationID: stableConversationID)
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    _ = try await manager.sendMessage(
      convoId: stableConversationID,
      plaintext: "hello"
    )

    XCTAssertEqual(bridge.sendPayloadResultCallCount, 1)
    XCTAssertEqual(bridge.lastSendPayloadResultConversationId, stableConversationID)
    XCTAssertNil(manager.groupStates[rawGroupID])
  }

  func testRustFullRawAliasSendUsesCanonicalConversationID() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedCanonicalAndRawAlias(on: manager)

    let bridge = RecordingMessagingBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    _ = try await manager.sendMessage(
      convoId: rawGroupID,
      plaintext: "hello",
      embed: .link(MLSLinkEmbed(url: "https://example.com/article"))
    )

    XCTAssertEqual(bridge.lastSendPayloadResultConversationId, stableConversationID)
  }

  func testRustFullRawAliasReactionUsesCanonicalConversationID() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedCanonicalAndRawAlias(on: manager)

    let bridge = RecordingMessagingBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    _ = try await manager.sendEncryptedReaction(
      convoId: rawGroupID,
      messageId: "message-1",
      emoji: "+1",
      action: .add
    )

    XCTAssertEqual(bridge.lastSendReactionConversationId, stableConversationID)
  }

  func testRustFullRawAliasEditAndUnsendUseCanonicalConversationID() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedCanonicalAndRawAlias(on: manager)
    try await seedOwnMessage(messageID: "message-1", conversationID: rawGroupID, on: manager)

    let bridge = RecordingMessagingBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    _ = try await manager.editMessage(
      convoId: rawGroupID,
      messageId: "message-1",
      newText: "edited"
    )
    XCTAssertEqual(bridge.lastSendPayloadResultConversationId, stableConversationID)

    _ = try await manager.unsendMessage(convoId: rawGroupID, messageId: "message-1")
    XCTAssertEqual(bridge.lastSendPayloadResultConversationId, stableConversationID)
  }

  func testRustFullRejectsUnrelatedNonCanonicalConversationIDBeforeMutation() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedCanonicalAndRawAlias(on: manager)

    let bridge = RecordingMessagingBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    do {
      _ = try await manager.sendMessage(convoId: "deadbeef00", plaintext: "must fail")
      XCTFail("A lookalike raw id must fail closed before Rust mutation")
    } catch {
      XCTAssertTrue(error is MLSStorageError)
    }
    XCTAssertEqual(bridge.sendPayloadResultCallCount, 0)
  }

  func testRustFullIncomingUsesResultBridgeBeforeLegacyProcessing() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-incoming", on: manager)

    let bridge = RecordingMessagingBridge()
    bridge.messageProcessingResult = FfiMessageProcessingResult(
      message: nil,
      events: []
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let message = BlueCatbirdChatDefs.ApplicationEntry(
      convoId: "convo-incoming",
      id: "msg-1",
      senderDid: try DID(didString: "did:plc:testuser"),
      senderDeviceDid: "did:plc:testuser#device",
      senderSeq: 1,
      ciphertext: Bytes(data: Data([0x01, 0x02, 0x03])),
      epoch: 7,
      seq: 44,
      createdAt: ATProtocolDate(date: Date())
    )

    let outcome = try await manager.processServerMessage(message, source: "unit-test")

    XCTAssertEqual(bridge.processIncomingMessageCallCount, 1)
    XCTAssertEqual(bridge.processIncomingCallCount, 0)
    XCTAssertEqual(bridge.lastProcessIncomingServerEpoch, 7)
    guard case .nonApplication = outcome else {
      return XCTFail("Expected nonApplication outcome, got \(outcome)")
    }
  }

  func testRustFullIncomingAppliesReturnedEngineEvents() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-incoming", on: manager)
    seedGroupState(conversationID: "convo-incoming", groupID: "deadbeef", on: manager)

    let bridge = RecordingMessagingBridge()
    bridge.messageProcessingResult = FfiMessageProcessingResult(
      message: nil,
      events: rustResetEvents(conversationID: "convo-incoming")
    )
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let message = BlueCatbirdChatDefs.ApplicationEntry(
      convoId: "convo-incoming",
      id: "msg-1",
      senderDid: try DID(didString: "did:plc:testuser"),
      senderDeviceDid: "did:plc:testuser#device",
      senderSeq: 1,
      ciphertext: Bytes(data: Data([0x01, 0x02, 0x03])),
      epoch: 7,
      seq: 44,
      createdAt: ATProtocolDate(date: Date())
    )

    _ = try await manager.processServerMessage(message, source: "unit-test")

    XCTAssertEqual(bridge.processIncomingMessageCallCount, 1)
    XCTAssertNil(manager.groupStates["deadbeef"])
  }

  func testRuntimeProcessServerEventWrapsBridgeEvents() throws {
    let bridge = RecordingMessagingBridge()
    bridge.serverEvents = [
      FfiEngineEvent(
        kind: .recoveryStateChanged,
        conversationId: "convo-1",
        messageId: nil,
        recoveryState: .resetPending
      ),
      FfiEngineEvent(
        kind: .needsUiRefresh,
        conversationId: "convo-1",
        messageId: nil,
        recoveryState: nil
      ),
    ]

    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let events = try runtime.processServerEvent(eventJson: #"{"type":"groupReset","convoId":"convo-1"}"#)

    XCTAssertEqual(bridge.processServerEventCallCount, 1)
    XCTAssertEqual(events.count, 2)
    XCTAssertEqual(events.first?.conversationId, "convo-1")
  }

  func testRustFullGroupResetUsesProcessServerEventAndAppliesEvents() async throws {
    let manager = try await makeAuthenticatedManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-reset", on: manager)
    seedGroupState(conversationID: "convo-reset", groupID: "deadbeef", on: manager)

    let bridge = RecordingMessagingBridge()
    bridge.serverEvents = rustResetEvents(conversationID: "convo-reset")
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let event = MLSConversationManager.MLSGroupResetEvent(
      convoId: "convo-reset",
      newGroupId: "00112233445566778899aabbccddeeff",
      resetGeneration: 3,
      resetBy: "did:plc:resetter",
      reason: "unit-test"
    )

    await manager.handleGroupReset(event: event)

    XCTAssertEqual(bridge.processServerEventCallCount, 1)
    XCTAssertEqual(bridge.recordGroupResetOutcomeCallCount, 0)
    XCTAssertEqual(bridge.lastServerEventJsonField("type"), "groupReset")
    XCTAssertEqual(bridge.lastServerEventJsonField("convoId"), "convo-reset")
    XCTAssertNil(manager.groupStates["deadbeef"])
  }

  func testRustFullResetRequestedUsesProcessServerEventAndAppliesEvents() async throws {
    let manager = try await makeAuthenticatedManager(protocolAuthorityMode: .rustFull)
    try await seedConversation(conversationID: "convo-reset-requested", on: manager)
    seedGroupState(conversationID: "convo-reset-requested", groupID: "deadbeef", on: manager)

    let bridge = RecordingMessagingBridge()
    bridge.serverEvents = rustResetEvents(conversationID: "convo-reset-requested")
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let event = MLSConversationManager.MLSResetRequestedEvent(
      convoId: "convo-reset-requested",
      generation: 4,
      trigger: "inlineGroupInfo404",
      requestEventId: "request-event-1",
      cryptoSessionId: "session-prior",
      expectedNewMlsGroupId: "00112233445566778899aabbccddeeff"
    )

    await manager.handleResetRequested(event: event)

    XCTAssertEqual(bridge.processServerEventCallCount, 1)
    XCTAssertEqual(bridge.recordResetRequestedOutcomeCallCount, 0)
    XCTAssertEqual(bridge.lastServerEventJsonField("type"), "resetRequested")
    XCTAssertEqual(bridge.lastServerEventJsonField("convoId"), "convo-reset-requested")
    XCTAssertEqual(
      bridge.lastServerEventJsonField("expectedNewMlsGroupIdHex"),
      "00112233445566778899aabbccddeeff"
    )
    XCTAssertNil(manager.groupStates["deadbeef"])
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

  private func makeAuthenticatedManager(
    protocolAuthorityMode: MLSProtocolAuthorityMode
  ) async throws -> MLSConversationManager {
    let database = try DatabaseQueue()
    try MLSGRDBManager.makeMigrator().migrate(database)

    let userDid = "did:plc:testuser"
    let namespace = "MLSFullRustMessagingTests.\(UUID().uuidString)"
    let storage = KeychainStorage(namespace: namespace)
    try await storage.saveAccount(
      Account(
        did: userDid,
        handle: "testuser.bsky.social",
        pdsURL: URL(string: "https://example.com")!
      ),
      for: userDid
    )
    try await storage.saveSession(
      Session(
        accessToken: "access-token",
        refreshToken: "refresh-token",
        createdAt: Date(),
        expiresIn: 3600,
        tokenType: .bearer,
        did: userDid
      ),
      for: userDid
    )
    try await storage.saveCurrentDID(userDid)

    let atProtoClient = try await ATProtoClient(
      baseURL: URL(string: "https://example.com")!,
      oauthConfig: OAuthConfig(
        clientId: "unit-test-client",
        redirectUri: "catbird://tests/oauth",
        scope: "atproto"
      ),
      namespace: namespace,
      authMode: .legacy
    )
    let apiClient = await MLSAPIClient(
      client: atProtoClient,
      environment: .custom(serviceDID: "did:web:example.com#atproto_mls")
    )
    return MLSConversationManager(
      apiClient: apiClient,
      database: database,
      userDid: userDid,
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
    manager.conversations[conversationID] = model.asConversationState()
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

  private func seedCanonicalAndRawAlias(on manager: MLSConversationManager) async throws {
    let now = Date(timeIntervalSince1970: 1_700_000_000)
    let stable = MLSConversationModel(
      conversationID: stableConversationID,
      currentUserDID: "did:plc:testuser",
      groupID: Data(hexEncoded: rawGroupID)!,
      epoch: 3,
      createdAt: now,
      updatedAt: now
    )
    let raw = MLSConversationModel(
      conversationID: rawGroupID,
      currentUserDID: "did:plc:testuser",
      groupID: Data(hexEncoded: rawGroupID)!,
      epoch: 3,
      createdAt: now,
      updatedAt: now
    )
    try await manager.database.write { db in
      try stable.insert(db)
      try raw.insert(db)
    }
    manager.conversations[stableConversationID] = stable.asConversationState()
    manager.conversations[rawGroupID] = raw.asConversationState()
  }

  private func seedOwnMessage(
    messageID: String,
    conversationID: String,
    on manager: MLSConversationManager
  ) async throws {
    let message = MLSMessageModel(
      messageID: messageID,
      currentUserDID: "did:plc:testuser",
      conversationID: conversationID,
      senderID: "did:plc:testuser",
      epoch: 3,
      sequenceNumber: 1,
      isDelivered: true,
      isSent: true
    )
    try await manager.database.write { db in
      try message.insert(db)
    }
  }

  private func rustResetEvents(conversationID: String) -> [FfiEngineEvent] {
    [
      FfiEngineEvent(
        kind: .recoveryStateChanged,
        conversationId: conversationID,
        messageId: nil,
        recoveryState: .resetPending
      ),
      FfiEngineEvent(
        kind: .needsUiRefresh,
        conversationId: conversationID,
        messageId: nil,
        recoveryState: nil
      ),
      FfiEngineEvent(
        kind: .messageInserted,
        conversationId: conversationID,
        messageId: "msg-1",
        recoveryState: nil
      ),
    ]
  }
}

private enum UnexpectedLegacyBridgeCall: Error {
  case processIncoming
  case sendPayloadJson
}

private final class RecordingMessagingBridge: OrchestratorBridge {
  var sendResult = FfiSendResult(
    message: FfiMessage(
      id: "msg-1",
      conversationId: "convo-send",
      senderDid: "did:plc:testuser",
      text: "hello",
      timestamp: ISO8601DateFormatter().string(from: Date()),
      epoch: 3,
      sequenceNumber: 1,
      isOwn: true,
      deliveryStatus: nil,
      payloadJson: try? String(
        data: MLSMessagePayload.text("hello", embed: nil).encodeToJSON(),
        encoding: .utf8
      )
    ),
    events: []
  )
  var sendPayloadJsonResult: FfiMessage?
  var messageProcessingResult = FfiMessageProcessingResult(
    message: nil,
    events: []
  )
  var serverEvents: [FfiEngineEvent] = []
  var resetRecordOutcome: FfiResetRecordOutcome = .staleOrDuplicate
  private(set) var sendPayloadJsonCallCount = 0
  private(set) var sendPayloadResultCallCount = 0
  private(set) var processIncomingCallCount = 0
  private(set) var processIncomingMessageCallCount = 0
  private(set) var processServerEventCallCount = 0
  private(set) var recordGroupResetOutcomeCallCount = 0
  private(set) var recordResetRequestedOutcomeCallCount = 0
  private(set) var syncWithServerCallCount = 0
  private(set) var lastProcessIncomingServerEpoch: UInt64?
  private(set) var lastProcessServerEventJson: String?
  private(set) var lastSendPayloadResultConversationId: String?
  private(set) var lastSendPayloadConversationId: String?
  private(set) var lastSendReactionConversationId: String?

  init() {
    super.init(noPointer: .init())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    super.init(unsafeFromRawPointer: pointer)
  }

  override func sendPayloadResultJson(
    conversationId: String,
    payloadJson: String
  ) throws -> FfiSendResult {
    sendPayloadResultCallCount += 1
    lastSendPayloadResultConversationId = conversationId
    return sendResult
  }

  override func sendPayloadJson(
    conversationId: String,
    payloadJson: String
  ) throws -> FfiMessage {
    sendPayloadJsonCallCount += 1
    lastSendPayloadConversationId = conversationId
    guard let sendPayloadJsonResult else {
      throw UnexpectedLegacyBridgeCall.sendPayloadJson
    }
    return sendPayloadJsonResult
  }

  override func sendReaction(
    conversationId: String,
    messageId: String,
    emoji: String,
    action: String
  ) throws -> FfiMessage {
    lastSendReactionConversationId = conversationId
    return sendResult.message
  }

  override func processIncoming(
    envelope: FfiIncomingEnvelope
  ) throws -> FfiMessage? {
    processIncomingCallCount += 1
    throw UnexpectedLegacyBridgeCall.processIncoming
  }

  override func processIncomingMessage(
    envelope: FfiIncomingEnvelope,
    serverEpoch: UInt64?
  ) throws -> FfiMessageProcessingResult {
    processIncomingMessageCallCount += 1
    lastProcessIncomingServerEpoch = serverEpoch
    return messageProcessingResult
  }

  override func processServerEvent(eventJson: String) throws -> [FfiEngineEvent] {
    processServerEventCallCount += 1
    lastProcessServerEventJson = eventJson
    return serverEvents
  }

  override func recordGroupResetOutcome(
    convoId: String,
    newGroupIdHex: String,
    resetGeneration: Int32
  ) throws -> FfiResetRecordOutcome {
    recordGroupResetOutcomeCallCount += 1
    return resetRecordOutcome
  }

  override func recordResetRequestedOutcome(
    convoId: String,
    cryptoSessionId: String,
    resetGeneration: Int32,
    trigger: String,
    requestEventId: String,
    expectedNewMlsGroupIdHex: String?
  ) throws -> FfiResetRecordOutcome {
    recordResetRequestedOutcomeCallCount += 1
    return resetRecordOutcome
  }

  override func syncWithServer(fullSync: Bool) throws {
    syncWithServerCallCount += 1
  }

  override func listConversations(userDid: String) throws -> [FfiConversationView] {
    []
  }

  func lastServerEventJsonField(_ field: String) -> String? {
    guard let data = lastProcessServerEventJson?.data(using: .utf8),
          let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
    else { return nil }
    return object[field] as? String
  }

  override func shutdown() {
  }
}
