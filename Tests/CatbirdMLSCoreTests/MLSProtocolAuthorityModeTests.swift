import XCTest
import CatbirdMLS
import GRDB
import Petrel

@testable import CatbirdMLSCore

final class MLSProtocolAuthorityModeTests: XCTestCase {
  func testDefaultModeIsRustFull() {
    // Migration complete: rustFull is the default authority mode — Rust owns
    // protocol decisions and mutations by default. swiftLegacy remains for
    // explicit opt-out / rollback.
    XCTAssertEqual(MLSProtocolAuthorityMode.defaultMode, .rustFull)
    XCTAssertTrue(MLSProtocolAuthorityMode.defaultMode.usesRustForDecisions)
    XCTAssertTrue(MLSProtocolAuthorityMode.defaultMode.requiresRustOnlyProtocolMutations)
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

  func testRustFullModeParsesFromRuntimeValue() {
    XCTAssertEqual(MLSProtocolAuthorityMode(rawRuntimeValue: "rustFull"), .rustFull)
    XCTAssertEqual(MLSProtocolAuthorityMode(rawRuntimeValue: "fullRust"), .rustFull)
  }

  func testRustFullRequiresRustOnlyMutations() {
    XCTAssertTrue(MLSProtocolAuthorityMode.rustFull.mirrorsRustDecisions)
    XCTAssertTrue(MLSProtocolAuthorityMode.rustFull.usesRustForDecisions)
    XCTAssertTrue(MLSProtocolAuthorityMode.rustFull.requiresRustOnlyProtocolMutations)
    XCTAssertFalse(MLSProtocolAuthorityMode.rustAuthoritative.requiresRustOnlyProtocolMutations)
    XCTAssertFalse(MLSProtocolAuthorityMode.swiftLegacy.requiresRustOnlyProtocolMutations)
  }

  func testSharedAuthorityModeStateRoundTripsForExtensions() {
    MLSAuthorityModeSharedState.clearForTesting()
    defer { MLSAuthorityModeSharedState.clearForTesting() }

    XCTAssertEqual(MLSAuthorityModeSharedState.currentMode(), .defaultMode)
    // defaultMode is now .rustFull, so the cleared/fallback state is rustFull-enabled.
    XCTAssertTrue(MLSAuthorityModeSharedState.isRustFullEnabled)

    MLSAuthorityModeSharedState.setCurrentMode(.rustFull)

    XCTAssertEqual(MLSAuthorityModeSharedState.currentMode(), .rustFull)
    XCTAssertTrue(MLSAuthorityModeSharedState.isRustFullEnabled)
  }

  func testRustFullSharedStateBlocksDirectNotificationDecrypt() async throws {
    MLSAuthorityModeSharedState.clearForTesting()
    defer { MLSAuthorityModeSharedState.clearForTesting() }

    MLSAuthorityModeSharedState.setCurrentMode(.rustFull)

    do {
      _ = try await MLSCoreContext.shared.decryptForNotification(
        userDid: "did:plc:rustfull-notification-guard",
        groupId: Data([0x01, 0x02, 0x03]),
        ciphertext: Data([0x04, 0x05, 0x06]),
        conversationID: "convo-rustfull-notification-guard",
        messageID: "msg-rustfull-notification-guard"
      )
      XCTFail("Expected rustFull to block direct notification decrypt")
    } catch let error as MLSSQLCipherError {
      guard case .storageUnavailable(let reason) = error else {
        return XCTFail("Unexpected MLSSQLCipherError: \(error)")
      }
      XCTAssertTrue(reason.contains("rustFull"))
      XCTAssertTrue(reason.contains("notification decrypt"))
    }
  }

  func testRustFullSharedStateBlocksSwiftSilentRecovery() async throws {
    MLSAuthorityModeSharedState.clearForTesting()
    defer { MLSAuthorityModeSharedState.clearForTesting() }

    MLSAuthorityModeSharedState.setCurrentMode(.rustFull)
    let recoveryManager = MLSRecoveryManager(persistence: nil)

    do {
      try await recoveryManager.performSilentRecovery(
        for: "did:plc:rustfull-recovery-guard",
        conversationIds: ["convo-rustfull-recovery-guard"]
      )
      XCTFail("Expected rustFull to block Swift silent recovery")
    } catch let error as MLSRecoveryError {
      guard case .recoveryFailed(let underlying) = error,
        let storageError = underlying as? MLSSQLCipherError,
        case .storageUnavailable(let reason) = storageError
      else {
        return XCTFail("Unexpected MLSRecoveryError: \(error)")
      }
      XCTAssertTrue(reason.contains("rustFull"))
      XCTAssertTrue(reason.contains("silent recovery"))
    }
  }

  func testRustFullSharedStateBlocksSwiftDeviceReregistration() async throws {
    MLSAuthorityModeSharedState.clearForTesting()
    defer { MLSAuthorityModeSharedState.clearForTesting() }

    MLSAuthorityModeSharedState.setCurrentMode(.rustFull)

    do {
      _ = try await MLSClient.shared.reregisterDevice(for: "did:plc:rustfull-reregister-guard")
      XCTFail("Expected rustFull to block Swift device re-registration")
    } catch let error as MLSSQLCipherError {
      guard case .storageUnavailable(let reason) = error else {
        return XCTFail("Unexpected MLSSQLCipherError: \(error)")
      }
      XCTAssertTrue(reason.contains("rustFull"))
      XCTAssertTrue(reason.contains("device re-registration"))
    }
  }

  func testRustFullSharedStateBlocksLowLevelSwiftProtocolMutations() async throws {
    MLSAuthorityModeSharedState.clearForTesting()
    defer { MLSAuthorityModeSharedState.clearForTesting() }

    MLSAuthorityModeSharedState.setCurrentMode(.rustFull)

    try await assertRustFullBlocksProtocolMutation("createGroup") {
      _ = try await MLSClient.shared.createGroup(for: "did:plc:rustfull-protocol-guard")
    }
    try await assertRustFullBlocksProtocolMutation("joinByExternalCommit") {
      _ = try await MLSClient.shared.joinByExternalCommit(
        for: "did:plc:rustfull-protocol-guard",
        convoId: "convo-rustfull-protocol-guard"
      )
    }
    try await assertRustFullBlocksProtocolMutation("deleteGroup") {
      try await MLSClient.shared.deleteGroup(
        for: "did:plc:rustfull-protocol-guard",
        groupId: Data([0x01, 0x02, 0x03])
      )
    }
    try await assertRustFullBlocksProtocolMutation("clearPendingCommit") {
      try await MLSClient.shared.clearPendingCommit(
        for: "did:plc:rustfull-protocol-guard",
        groupId: Data([0x01, 0x02, 0x03])
      )
    }
    try await assertRustFullBlocksProtocolMutation("commitPendingProposals") {
      _ = try await MLSClient.shared.commitPendingProposals(
        for: "did:plc:rustfull-protocol-guard",
        groupId: Data([0x01, 0x02, 0x03])
      )
    }
  }

  func testRustFullSharedStateBlocksLowLevelSwiftKeyPackageMutations() async throws {
    MLSAuthorityModeSharedState.clearForTesting()
    defer { MLSAuthorityModeSharedState.clearForTesting() }

    MLSAuthorityModeSharedState.setCurrentMode(.rustFull)

    try await assertRustFullBlocksKeyPackageMutation("ensureDeviceRegistered") {
      _ = try await MLSClient.shared.ensureDeviceRegistered(
        userDid: "did:plc:rustfull-keypackage-guard"
      )
    }
    try await assertRustFullBlocksKeyPackageMutation("batchCreateKeyPackages") {
      _ = try await MLSClient.shared.batchCreateKeyPackages(
        for: "did:plc:rustfull-keypackage-guard",
        identity: "did:plc:rustfull-keypackage-guard",
        count: 1
      )
    }
    try await assertRustFullBlocksKeyPackageMutation("createKeyPackage") {
      _ = try await MLSClient.shared.createKeyPackage(
        for: "did:plc:rustfull-keypackage-guard",
        identity: "did:plc:rustfull-keypackage-guard"
      )
    }
    try await assertRustFullBlocksKeyPackageMutation("monitorAndReplenishBundles") {
      _ = try await MLSClient.shared.monitorAndReplenishBundles(
        for: "did:plc:rustfull-keypackage-guard"
      )
    }
    try await assertRustFullBlocksKeyPackageMutation("syncKeyPackageHashes") {
      _ = try await MLSClient.shared.syncKeyPackageHashes(
        for: "did:plc:rustfull-keypackage-guard"
      )
    }
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

  func testManagerDefaultsToRustFullAuthority() async throws {
    let manager = try await makeManager()

    XCTAssertEqual(manager.protocolAuthorityMode, .rustFull)
    XCTAssertTrue(manager.isRustProtocolAuthorityEnabled)
    // Runtime is created lazily and not for a non-pool DatabaseQueue, so it is
    // still nil at init until ensureOrchestratorRuntime() runs against a pool DB.
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

  private func assertRustFullBlocksProtocolMutation(
    _ operationName: String,
    operation: () async throws -> Void
  ) async throws {
    do {
      try await operation()
      XCTFail("Expected rustFull to block \(operationName)")
    } catch let error as MLSSQLCipherError {
      guard case .storageUnavailable(let reason) = error else {
        return XCTFail("Unexpected MLSSQLCipherError for \(operationName): \(error)")
      }
      XCTAssertTrue(reason.contains("rustFull"), operationName)
      XCTAssertTrue(reason.contains("Swift MLS protocol mutation"), operationName)
    }
  }

  private func assertRustFullBlocksKeyPackageMutation(
    _ operationName: String,
    operation: () async throws -> Void
  ) async throws {
    do {
      try await operation()
      XCTFail("Expected rustFull to block \(operationName)")
    } catch let error as MLSSQLCipherError {
      guard case .storageUnavailable(let reason) = error else {
        return XCTFail("Unexpected MLSSQLCipherError for \(operationName): \(error)")
      }
      XCTAssertTrue(reason.contains("rustFull"), operationName)
      XCTAssertTrue(reason.contains("Swift MLS key package mutation"), operationName)
    }
  }
}
