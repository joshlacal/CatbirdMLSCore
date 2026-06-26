import XCTest
import CatbirdMLS
import GRDB
import Petrel
import PetrelCatbird

@testable import CatbirdMLSCore

final class MLSFullRustMessagingTests: XCTestCase {
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
    try await seedConversation(conversationID: "convo-send", on: manager)

    let bridge = RecordingMessagingBridge()
    manager.orchestratorRuntime = MLSOrchestratorRuntime(
      userDID: "did:plc:testuser",
      mode: .rustFull,
      bridge: bridge
    )

    let result = try await manager.sendMessage(
      convoId: "convo-send",
      plaintext: "hello"
    )

    XCTAssertEqual(bridge.sendPayloadResultCallCount, 1)
    XCTAssertEqual(bridge.sendPayloadJsonCallCount, 0)
    XCTAssertEqual(result.messageId, "msg-1")
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

    let message = BlueCatbirdMlsChatDefs.MessageView(
      id: "msg-1",
      convoId: "convo-incoming",
      ciphertext: Bytes(data: Data([0x01, 0x02, 0x03])),
      epoch: 7,
      seq: 44,
      createdAt: ATProtocolDate(date: Date()),
      messageType: "message"
    )

    let outcome = try await manager.processServerMessage(message, source: "unit-test")

    XCTAssertEqual(bridge.processIncomingMessageCallCount, 1)
    XCTAssertEqual(bridge.processIncomingCallCount, 0)
    XCTAssertEqual(bridge.lastProcessIncomingServerEpoch, 7)
    guard case .nonApplication = outcome else {
      return XCTFail("Expected nonApplication outcome, got \(outcome)")
    }
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
  private(set) var sendPayloadJsonCallCount = 0
  private(set) var sendPayloadResultCallCount = 0
  private(set) var processIncomingCallCount = 0
  private(set) var processIncomingMessageCallCount = 0
  private(set) var processServerEventCallCount = 0
  private(set) var lastProcessIncomingServerEpoch: UInt64?

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
    return sendResult
  }

  override func sendPayloadJson(
    conversationId: String,
    payloadJson: String
  ) throws -> FfiMessage {
    sendPayloadJsonCallCount += 1
    guard let sendPayloadJsonResult else {
      throw UnexpectedLegacyBridgeCall.sendPayloadJson
    }
    return sendPayloadJsonResult
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
    return serverEvents
  }

  override func shutdown() {
  }
}
