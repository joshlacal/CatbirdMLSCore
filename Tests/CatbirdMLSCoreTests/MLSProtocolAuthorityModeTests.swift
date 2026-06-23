import XCTest
import CatbirdMLS
import GRDB
import Petrel

@testable import CatbirdMLSCore

final class MLSProtocolAuthorityModeTests: XCTestCase {
  func testDefaultModeKeepsSwiftAuthoritative() {
    XCTAssertEqual(MLSProtocolAuthorityMode.defaultMode, .swiftLegacy)
    XCTAssertFalse(MLSProtocolAuthorityMode.swiftLegacy.mirrorsRustDecisions)
    XCTAssertFalse(MLSProtocolAuthorityMode.swiftLegacy.usesRustForDecisions)
  }

  func testShadowMirrorsWithoutDelegatingAuthority() {
    XCTAssertTrue(MLSProtocolAuthorityMode.rustShadow.mirrorsRustDecisions)
    XCTAssertFalse(MLSProtocolAuthorityMode.rustShadow.usesRustForDecisions)
  }

  func testAuthoritativeDelegatesDecisionsToRust() {
    XCTAssertTrue(MLSProtocolAuthorityMode.rustAuthoritative.mirrorsRustDecisions)
    XCTAssertTrue(MLSProtocolAuthorityMode.rustAuthoritative.usesRustForDecisions)
  }

  func testFFIRecoveryStateMapsToSwiftVocabulary() {
    XCTAssertEqual(ConversationRecoveryState(ffiRecoveryState: .healthy), .healthy)
    XCTAssertEqual(ConversationRecoveryState(ffiRecoveryState: .epochBehind), .epochBehind)
    XCTAssertEqual(ConversationRecoveryState(ffiRecoveryState: .groupMissing), .groupMissing)
    XCTAssertEqual(ConversationRecoveryState(ffiRecoveryState: .needsRejoin), .needsRejoin)
    XCTAssertEqual(ConversationRecoveryState(ffiRecoveryState: .recovering), .recovering)
    XCTAssertEqual(ConversationRecoveryState(ffiRecoveryState: .unrecoverableLocal), .unrecoverableLocal)
    XCTAssertEqual(ConversationRecoveryState(ffiRecoveryState: .resetPending), .resetPending)
  }

  func testFFIResetRecordOutcomeMapsToSwiftVocabulary() {
    XCTAssertEqual(MLSResetRecordOutcome(ffiOutcome: .recorded), .recorded)
    XCTAssertEqual(MLSResetRecordOutcome(ffiOutcome: .staleOrDuplicate), .staleOrDuplicate)
    XCTAssertEqual(MLSResetRecordOutcome(ffiOutcome: .selfEchoNoOp), .selfEchoNoOp)
  }

  func testRustResetFollowupsOnlyRunForRecordedOutcome() {
    XCTAssertTrue(MLSConversationManager.shouldRunRustResetFollowups(for: .recorded))
    XCTAssertFalse(MLSConversationManager.shouldRunRustResetFollowups(for: .staleOrDuplicate))
    XCTAssertFalse(MLSConversationManager.shouldRunRustResetFollowups(for: .selfEchoNoOp))
  }

  func testIncomingRustMessagePayloadJsonMapsToApplicationOutcome() throws {
    let payload = MLSMessagePayload.text("decoded from payload", embed: nil)
    let ffiMessage = FfiMessage(
      id: "msg-1",
      conversationId: "convo-1",
      senderDid: "did:plc:alice",
      text: "display fallback",
      timestamp: "2026-06-22T12:00:00Z",
      epoch: 9,
      sequenceNumber: 42,
      isOwn: false,
      deliveryStatus: nil,
      payloadJson: String(data: try payload.encodeToJSON(), encoding: .utf8)
    )

    let outcome = try MLSOrchestratorRuntime.messageProcessingOutcome(from: ffiMessage)

    guard case .application(let mappedPayload, let sender) = outcome else {
      return XCTFail("Expected application outcome")
    }
    XCTAssertEqual(mappedPayload.messageType, .text)
    XCTAssertEqual(mappedPayload.text, "decoded from payload")
    XCTAssertEqual(sender, "did:plc:alice")
  }

  func testIncomingRustLegacyTextMapsToTextPayload() throws {
    let ffiMessage = FfiMessage(
      id: "msg-legacy",
      conversationId: "convo-1",
      senderDid: "did:plc:bob",
      text: "legacy plaintext",
      timestamp: "2026-06-22T12:00:00Z",
      epoch: 1,
      sequenceNumber: 2,
      isOwn: false,
      deliveryStatus: nil,
      payloadJson: nil
    )

    let outcome = try MLSOrchestratorRuntime.messageProcessingOutcome(from: ffiMessage)

    guard case .application(let mappedPayload, let sender) = outcome else {
      return XCTFail("Expected application outcome")
    }
    XCTAssertEqual(mappedPayload.messageType, .text)
    XCTAssertEqual(mappedPayload.text, "legacy plaintext")
    XCTAssertEqual(sender, "did:plc:bob")
  }

  func testIncomingRustNilMessageMapsToNonApplicationOutcome() throws {
    let outcome = try MLSOrchestratorRuntime.messageProcessingOutcome(from: nil)

    guard case .nonApplication = outcome else {
      return XCTFail("Expected non-application outcome")
    }
  }

  func testManagerDefaultsToSwiftLegacyAuthority() async throws {
    let manager = try await makeManager()

    XCTAssertEqual(manager.protocolAuthorityMode, .swiftLegacy)
    XCTAssertFalse(manager.isRustProtocolAuthorityEnabled)
    XCTAssertNil(manager.orchestratorRuntime)
  }

  func testShadowModeDoesNotCreateRuntimeForNonPoolDatabase() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustShadow)

    XCTAssertTrue(manager.isRustProtocolAuthorityEnabled)
    let runtime = await manager.ensureOrchestratorRuntime()
    XCTAssertNil(runtime)
  }

  private func makeManager(
    protocolAuthorityMode: MLSProtocolAuthorityMode = .defaultMode
  ) async throws -> MLSConversationManager {
    let database = try DatabaseQueue()
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
}
