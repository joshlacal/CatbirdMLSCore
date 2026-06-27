import XCTest
import GRDB
import Petrel

@testable import CatbirdMLSCore

final class MLSFullRustAuthorityGuardTests: XCTestCase {
  func testRustFullBlocksSwiftProtocolMutations() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustFull)

    do {
      try manager.assertSwiftProtocolMutationAllowed("testRustFullBlocksSwiftProtocolMutations")
      XCTFail("Expected rustFull to block Swift protocol mutations")
    } catch let error as MLSConversationError {
      guard case .operationFailed(let message) = error else {
        return XCTFail("Unexpected MLSConversationError: \(error)")
      }
      XCTAssertEqual(message, "Swift MLS protocol mutation blocked in rustFull mode")
    }
  }

  func testRustAuthoritativeStillAllowsSwiftProtocolMutations() async throws {
    let manager = try await makeManager(protocolAuthorityMode: .rustAuthoritative)

    XCTAssertNoThrow(try manager.assertSwiftProtocolMutationAllowed("testRustAuthoritativeStillAllowsSwiftProtocolMutations"))
  }

  func testRustFullDeliveryAckEntryPointReturnsBeforeSwiftSend() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager+DeliveryAcks.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(signature: "func enqueueDeliveryAck(messageId: String, conversationId: String)", from: source)
    )
    let rustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: body)
    )

    XCTAssertTrue(rustFullBranch.contains("return"))
    XCTAssertTrue(body.contains("sendDeliveryAck(messageId: messageId"))
    let rustFullGuardIndex = try XCTUnwrap(
      body.range(of: "protocolAuthorityMode == .rustFull")
    ).lowerBound
    let swiftSendIndex = try XCTUnwrap(
      body.range(of: "sendDeliveryAck(messageId: messageId")
    ).lowerBound
    XCTAssertLessThan(
      rustFullGuardIndex,
      swiftSendIndex
    )
  }

  func testDeliveryAckSendAssertsSwiftProtocolMutationAllowed() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager+DeliveryAcks.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(
        signature: "private func sendDeliveryAck(messageId: String, conversationId: String, userDid: String) async throws",
        from: source
      )
    )

    XCTAssertTrue(body.contains("assertSwiftProtocolMutationAllowed(\"sendDeliveryAck\")"))
  }

  func testRustFullRecoveryRequestSendPathReturnsBeforeSwiftSend() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager+DeliveryAcks.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(
        signature: "func sendRecoveryRequest(",
        from: source
      )
    )
    let rustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: body)
    )

    XCTAssertTrue(rustFullBranch.contains("return"))
    XCTAssertTrue(body.contains("assertSwiftProtocolMutationAllowed(\"sendRecoveryRequest\")"))
    XCTAssertLessThan(
      try XCTUnwrap(body.range(of: "protocolAuthorityMode == .rustFull")).lowerBound,
      try XCTUnwrap(body.range(of: "sendQueueCoordinator.enqueueSend")).lowerBound
    )
  }

  func testRustFullRecoveryRequestResponsePathReturnsBeforeSwiftSend() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager+DeliveryAcks.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(
        signature: "func handleRecoveryRequest(",
        from: source
      )
    )
    let rustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: body)
    )

    XCTAssertTrue(rustFullBranch.contains("return"))
    XCTAssertTrue(body.contains("assertSwiftProtocolMutationAllowed(\"handleRecoveryRequest\")"))
    XCTAssertLessThan(
      try XCTUnwrap(body.range(of: "protocolAuthorityMode == .rustFull")).lowerBound,
      try XCTUnwrap(body.range(of: "sendQueueCoordinator.enqueueSend")).lowerBound
    )
  }

  func testRustFullInitializeUsesRustDeviceAndKeyPackageReadinessBeforeSwiftMaintenance() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+Lifecycle.swift"),
      encoding: .utf8
    )
    let initializeBody = try XCTUnwrap(
      extractFunctionBody(signature: "public func initialize() async throws", from: source)
    )
    let rustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: initializeBody)
    )

    XCTAssertTrue(rustFullBranch.contains("prepareRustFullStartupDeviceAndKeyPackages"))
    XCTAssertFalse(rustFullBranch.contains("MLSClient.shared.ensureDeviceRegistered"))
    XCTAssertFalse(rustFullBranch.contains("MLSClient.shared.getKeyPackageBundleCount"))
    XCTAssertFalse(rustFullBranch.contains("smartRefreshKeyPackages"))

    let rustFullGuardIndex = try XCTUnwrap(
      initializeBody.range(of: "protocolAuthorityMode == .rustFull")
    ).lowerBound
    let legacyStartupIndex = try XCTUnwrap(
      initializeBody.range(of: "prepareSwiftLegacyStartupDeviceAndKeyPackages")
    ).lowerBound
    let initialSwiftRefreshIndex = try XCTUnwrap(
      initializeBody.range(of: "smartRefreshKeyPackages")
    ).lowerBound

    XCTAssertLessThan(rustFullGuardIndex, legacyStartupIndex)
    XCTAssertLessThan(rustFullGuardIndex, initialSwiftRefreshIndex)

    let rustFullStartupBody = try XCTUnwrap(
      extractFunctionBody(
        signature: "internal func prepareRustFullStartupDeviceAndKeyPackages(",
        from: source
      )
    )
    XCTAssertTrue(rustFullStartupBody.contains("runRustStartupReconcileIfNeeded"))
    XCTAssertTrue(rustFullStartupBody.contains("runtime.ensureDeviceRegistered()"))
    XCTAssertTrue(rustFullStartupBody.contains("runtime.replenishKeyPackagesIfNeeded()"))
    XCTAssertFalse(rustFullStartupBody.contains("MLSClient.shared"))
    XCTAssertFalse(rustFullStartupBody.contains("smartRefreshKeyPackages"))

    let swiftLegacyStartupBody = try XCTUnwrap(
      extractFunctionBody(
        signature: "private func prepareSwiftLegacyStartupDeviceAndKeyPackages(",
        from: source
      )
    )
    XCTAssertTrue(swiftLegacyStartupBody.contains("MLSClient.shared.ensureDeviceRegistered"))
    XCTAssertTrue(swiftLegacyStartupBody.contains("MLSClient.shared.getKeyPackageBundleCount"))
    XCTAssertTrue(swiftLegacyStartupBody.contains("MLSClient.shared.reconcileKeyPackagesWithServer"))
  }

  func testRustFullInitializeSkipsSwiftDeviceSyncMutationLoop() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+Lifecycle.swift"),
      encoding: .utf8
    )
    let initializeBody = try XCTUnwrap(
      extractFunctionBody(signature: "public func initialize() async throws", from: source)
    )
    let deviceSyncSection = try XCTUnwrap(
      extractSection(
        startMarker: "consumptionTracker = MLSConsumptionTracker",
        endMarker: "keyPackageRefreshTask = Task",
        from: initializeBody
      )
    )
    let rustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: deviceSyncSection)
    )

    XCTAssertTrue(rustFullBranch.contains("Skipping Swift device sync manager"))
    XCTAssertFalse(rustFullBranch.contains("deviceSyncManager.configure"))
    XCTAssertFalse(rustFullBranch.contains("deviceSyncManager.startPolling"))
    XCTAssertFalse(rustFullBranch.contains("addDeviceWithKeyPackage"))
    XCTAssertTrue(deviceSyncSection.contains("else if let deviceSyncManager = deviceSyncManager"))
    XCTAssertTrue(deviceSyncSection.contains("deviceSyncManager.configure"))
    XCTAssertTrue(deviceSyncSection.contains("deviceSyncManager.startPolling"))
    XCTAssertTrue(deviceSyncSection.contains("return try await self.addDeviceWithKeyPackage"))
  }

  func testAddDeviceWithKeyPackageAssertsSwiftProtocolMutationAllowed() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(
        signature: "public func addDeviceWithKeyPackage(",
        from: source
      )
    )

    XCTAssertTrue(body.contains("assertSwiftProtocolMutationAllowed(\"addDeviceWithKeyPackage\")"))
    XCTAssertLessThan(
      try XCTUnwrap(body.range(of: "assertSwiftProtocolMutationAllowed(\"addDeviceWithKeyPackage\")")).lowerBound,
      try XCTUnwrap(body.range(of: "mlsClient.stageCommit(")).lowerBound
    )
  }

  func testRustFullEnsureDeviceRecordPublishedReturnsBeforeSwiftDeviceRecordService() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(
        signature: "public func ensureDeviceRecordPublished() async throws",
        from: source
      )
    )
    let rustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: body)
    )

    XCTAssertTrue(rustFullBranch.contains("return"))
    XCTAssertTrue(rustFullBranch.contains("Skipping Swift device record publish"))
    XCTAssertLessThan(
      try XCTUnwrap(body.range(of: "protocolAuthorityMode == .rustFull")).lowerBound,
      try XCTUnwrap(body.range(of: "deviceRecordService.ensureDeviceRecordPublished")).lowerBound
    )
  }

  func testResumeDeviceRecordPublishUsesManagerAuthorityGuard() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+Lifecycle.swift"),
      encoding: .utf8
    )
    let resumeBody = try XCTUnwrap(
      extractFunctionBody(signature: "public func resumeMLSOperations() async", from: source)
    )
    let deviceRecordSection = try XCTUnwrap(
      extractSection(
        startMarker: "if userDid != nil, !configuration.skipDeviceRecordPublishing",
        endMarker: "// Note: missingConversationsTask",
        from: resumeBody
      )
    )

    XCTAssertTrue(deviceRecordSection.contains("self.ensureDeviceRecordPublished()"))
    XCTAssertFalse(deviceRecordSection.contains("self.deviceRecordService.ensureDeviceRecordPublished"))
  }

  func testRustFullDirectWelcomeJoinIsBlockedBeforeSwiftWelcomeProcessing() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+Groups.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(
        signature: "public func joinGroup(welcomeMessage: String) async throws",
        from: source
      )
    )

    XCTAssertTrue(body.contains("assertSwiftProtocolMutationAllowed(\"joinGroup(welcomeMessage:)\")"))
    XCTAssertLessThan(
      try XCTUnwrap(body.range(of: "assertSwiftProtocolMutationAllowed(\"joinGroup(welcomeMessage:)\")")).lowerBound,
      try XCTUnwrap(body.range(of: "processWelcome(welcomeData:")).lowerBound
    )
  }

  func testRustFullJoinViaExternalCommitRoutesThroughRustJoinOrRejoin() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(
        signature: "public func joinViaExternalCommit(convoId: String) async throws",
        from: source
      )
    )
    let rustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: body)
    )

    XCTAssertTrue(rustFullBranch.contains("joinOrRejoinConversation(conversationId: convoId)"))
    XCTAssertTrue(rustFullBranch.contains("return"))
    XCTAssertLessThan(
      try XCTUnwrap(body.range(of: "protocolAuthorityMode == .rustFull")).lowerBound,
      try XCTUnwrap(body.range(of: "attemptExternalCommitFallback")).lowerBound
    )
  }

  func testRuntimeCurrentDeviceInfoUsesStoredDeviceUuidAndBridgeListDevices() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSOrchestratorRuntime.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(signature: "public func currentDeviceInfo() throws -> MLSRegisteredDeviceInfo?", from: source)
    )

    XCTAssertTrue(body.contains("credentialAdapter?.getDeviceUuid(userDid: userDID)"))
    XCTAssertTrue(body.contains("bridge.listDevices()"))
    XCTAssertTrue(body.contains("device.deviceUuid == deviceUuid"))
  }

  func testRegisteredDeviceInfoForPushTokenUsesRustRuntimeInRustFull() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager+ProtocolAuthority.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(
        signature: "func registeredDeviceInfoForPushTokenRegistration() async throws -> MLSRegisteredDeviceInfo?",
        from: source
      )
    )
    let rustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: body)
    )

    XCTAssertTrue(rustFullBranch.contains("runtime.ensureDeviceRegistered()"))
    XCTAssertTrue(rustFullBranch.contains("runtime.currentDeviceInfo()"))
    XCTAssertFalse(rustFullBranch.contains("MLSClient.shared"))
    XCTAssertLessThan(
      try XCTUnwrap(body.range(of: "protocolAuthorityMode == .rustFull")).lowerBound,
      try XCTUnwrap(body.range(of: "mlsClient.ensureDeviceRegistered")).lowerBound
    )
  }

  func testRustFullSkipsSwiftGroupInfoRefreshTask() throws {
    let source = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager+Messaging.swift"),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(signature: "internal func startGroupInfoRefreshTask()", from: source)
    )
    let rustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: body)
    )

    XCTAssertTrue(rustFullBranch.contains("return"))
    XCTAssertLessThan(
      try XCTUnwrap(body.range(of: "protocolAuthorityMode == .rustFull")).lowerBound,
      try XCTUnwrap(body.range(of: "groupInfoRefreshTask = Task")).lowerBound
    )
  }

  func testRustFullGroupInfoPublishEntrypointsReturnBeforeSwiftPublish() throws {
    let messagingSource = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager+Messaging.swift"),
      encoding: .utf8
    )
    let refreshAllBody = try XCTUnwrap(
      extractFunctionBody(signature: "internal func refreshAllGroupInfo() async", from: messagingSource)
    )
    let refreshAllRustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: refreshAllBody)
    )
    XCTAssertTrue(refreshAllRustFullBranch.contains("return"))
    XCTAssertLessThan(
      try XCTUnwrap(refreshAllBody.range(of: "protocolAuthorityMode == .rustFull")).lowerBound,
      try XCTUnwrap(refreshAllBody.range(of: "mlsClient.publishGroupInfo")).lowerBound
    )

    let managerSource = try String(
      contentsOf: sourceFileURL(relativePath: "Sources/CatbirdMLSCore/Service/MLSConversationManager.swift"),
      encoding: .utf8
    )
    let requestBody = try XCTUnwrap(
      extractFunctionBody(signature: "public func handleGroupInfoRefreshRequest(convoId: String) async", from: managerSource)
    )
    let requestRustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: requestBody)
    )

    XCTAssertTrue(requestRustFullBranch.contains("return"))
    XCTAssertLessThan(
      try XCTUnwrap(requestBody.range(of: "protocolAuthorityMode == .rustFull")).lowerBound,
      try XCTUnwrap(requestBody.range(of: "mlsClient.publishGroupInfo")).lowerBound
    )
  }

  func testRustFullWelcomeReissueResponderReturnsBeforeSwiftCommitMutation() throws {
    let source = try String(
      contentsOf: sourceFileURL(
        relativePath: "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+WelcomeRecovery.swift"
      ),
      encoding: .utf8
    )

    let handleBody = try XCTUnwrap(
      extractFunctionBody(
        signature: "public func handleWelcomeReissueRequested(",
        from: source
      )
    )
    let handleRustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: handleBody)
    )

    XCTAssertTrue(handleRustFullBranch.contains("return"))
    XCTAssertLessThan(
      try XCTUnwrap(handleBody.range(of: "protocolAuthorityMode == .rustFull")).lowerBound,
      try XCTUnwrap(handleBody.range(of: "respondToWelcomeReissueRequest")).lowerBound
    )

    let respondBody = try XCTUnwrap(
      extractFunctionBody(
        signature: "func respondToWelcomeReissueRequest(",
        from: source
      )
    )
    let respondRustFullBranch = try XCTUnwrap(
      extractConditionalBranchBody(matching: "if protocolAuthorityMode == .rustFull", from: respondBody)
    )

    XCTAssertTrue(respondRustFullBranch.contains("return"))
    let rustFullGuardIndex = try XCTUnwrap(
      respondBody.range(of: "protocolAuthorityMode == .rustFull")
    ).lowerBound
    for swiftMutation in [
      "mlsClient.clearPendingCommit",
      "mlsClient.stageCommit",
      "mlsClient.exportPostCommitGroupInfo",
      "mlsClient.confirmCommit",
    ] {
      XCTAssertLessThan(
        rustFullGuardIndex,
        try XCTUnwrap(respondBody.range(of: swiftMutation)).lowerBound
      )
    }
  }

  func testRustFullGroupMetadataUpdateAssertsBeforeSwiftMutation() throws {
    let source = try String(
      contentsOf: sourceFileURL(
        relativePath: "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+Groups.swift"
      ),
      encoding: .utf8
    )
    let body = try XCTUnwrap(
      extractFunctionBody(
        signature: "public func updateGroupMetadataEncrypted(",
        from: source
      )
    )

    XCTAssertTrue(body.contains("assertSwiftProtocolMutationAllowed(\"updateGroupMetadataEncrypted\")"))
    let guardIndex = try XCTUnwrap(
      body.range(of: "assertSwiftProtocolMutationAllowed(\"updateGroupMetadataEncrypted\")")
    ).lowerBound
    for swiftMutation in [
      "mlsClient.updateGroupMetadataEncrypted",
      "mlsClient.clearPendingCommit",
      "mlsClient.mergePendingCommit",
      "publishLatestGroupInfo",
    ] {
      XCTAssertLessThan(
        guardIndex,
        try XCTUnwrap(body.range(of: swiftMutation)).lowerBound
      )
    }
  }

  func testRustFullManagerEntryPointsCompileGateLegacyLowLevelMLSClientCalls() throws {
    let forbiddenCalls = [
      ".getEpoch(",
      ".deleteGroup(",
      ".joinByExternalCommit(",
      ".processCommit(",
      ".mergeIncomingCommit(",
      ".stageCommit(",
    ]

    let guardedFunctionsByFile = [
      "Sources/CatbirdMLSCore/Service/MLSConversationManager.swift": [
        "public func handleGroupReset(",
        "public func handleResetRequested(",
      ],
      "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+Sync.swift": [
        "public func syncWithServer(fullSync: Bool = false) async throws",
        "internal func runDeferredEpochRecovery() async throws",
      ],
      "Sources/CatbirdMLSCore/Service/MLSConversationManager+Messaging.swift": [
        "public func sendMessage(",
        "internal func processServerMessage(",
        "func ensureGroupInitialized(for convoId: String) async throws",
      ],
      "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+Lifecycle.swift": [
        "internal func validateGroupStates() async",
        "public func detectAndRejoinMissingConversations() async throws",
      ],
    ]

    let rustFullSafeHelpersBySignature = [
      "public func handleGroupReset(": [
        "handleRustEngineEvents",
        "rustServerEventJson",
        "withRustAuthoritativeRuntime",
      ],
      "public func handleResetRequested(": [
        "handleRustEngineEvents",
        "rustServerEventJson",
        "withRustAuthoritativeRuntime",
      ],
      "public func syncWithServer(fullSync: Bool = false) async throws": [
        "hydrateSwiftCachesFromDatabaseAfterRustSync",
        "hydrateSwiftCachesFromRustSnapshotsAfterRustSync",
        "withRustAuthoritativeRuntime",
      ],
      "internal func runDeferredEpochRecovery() async throws": [
        "ensureActiveAccount",
        "withRustAuthoritativeRuntime",
      ],
      "public func sendMessage(": [
        "handleRustEngineEvents",
        "withRustAuthoritativeRuntime",
      ],
      "internal func processServerMessage(": [
        "nextProcessingAttemptID",
        "processServerMessageWithFullRust",
        "throwIfShuttingDown",
        "validateSessionGeneration",
      ],
      "func ensureGroupInitialized(for convoId: String) async throws": [
        "ensureActiveAccount",
        "withRustAuthoritativeRuntime",
      ],
      "internal func validateGroupStates() async": [
        "runRustStartupReconcileIfNeeded",
        "withRustAuthoritativeRuntime",
      ],
      "public func detectAndRejoinMissingConversations() async throws": [
        "withRustAuthoritativeRuntime",
      ],
    ]

    let legacyHelpersByFile = [
      "Sources/CatbirdMLSCore/Service/MLSConversationManager.swift": [
        "private func handleGroupResetLegacy(",
        "private func handleResetRequestedLegacy(",
      ],
      "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+Sync.swift": [
        "private func syncWithServerLegacy(fullSync: Bool = false) async throws",
        "private func runDeferredEpochRecoveryLegacy() async throws",
      ],
      "Sources/CatbirdMLSCore/Service/MLSConversationManager+Messaging.swift": [
        "private func sendMessageLegacy(",
        "private func processServerMessageLegacy(",
        "private func ensureGroupInitializedLegacy(",
      ],
      "Sources/CatbirdMLSCore/Service/Extensions/MLSConversationManager+Lifecycle.swift": [
        "private func validateGroupStatesLegacy() async",
        "private func detectAndRejoinMissingConversationsLegacy() async throws",
      ],
    ]

    for (file, functionSignatures) in guardedFunctionsByFile {
      let source = try String(contentsOf: sourceFileURL(relativePath: file), encoding: .utf8)
      let declaredFunctions = declaredFunctionNames(in: source)
      for signature in functionSignatures {
        let currentFunctionName = try XCTUnwrap(
          extractFunctionName(from: signature),
          "Missing function name in signature \(signature)"
        )
        let body = try XCTUnwrap(
          extractFunctionBody(signature: signature, from: source),
          "Missing function body for \(signature) in \(file)"
        )
        XCTAssertTrue(
          body.contains(".rustFull"),
          "\(file) \(signature) must keep an explicit rustFull guard"
        )
        XCTAssertTrue(
          body.contains("#if MLS_SWIFT_LEGACY_PROTOCOL"),
          "\(file) \(signature) must compile-gate its legacy Swift protocol path"
        )

        let rustFullBranch = try XCTUnwrap(
          extractConditionalBranchBody(
            matching: "if protocolAuthorityMode == .rustFull",
            from: body
          ),
          "\(file) \(signature) must keep an extractable rustFull branch"
        )

        let sameFileHelperCalls = invokedFunctionNames(
          in: rustFullBranch,
          declaredFunctionNames: declaredFunctions
        ).subtracting([currentFunctionName])
        let allowedHelpers = Set(rustFullSafeHelpersBySignature[signature] ?? [])
        let disallowedHelpers = sameFileHelperCalls.subtracting(allowedHelpers)
        XCTAssertTrue(
          disallowedHelpers.isEmpty,
          "\(file) \(signature) rustFull branch calls non-allowlisted same-file helper(s): \(disallowedHelpers.sorted().joined(separator: ", "))"
        )

        let violations = forbiddenCalls.filter { call in
          hasUngatedForbiddenCall(call, in: body)
        }
        XCTAssertTrue(
          violations.isEmpty,
          "\(file) \(signature) has forbidden low-level MLSClient call(s) outside the legacy compile gate: \(violations.joined(separator: ", "))"
        )
      }
    }

    for (file, helperSignatures) in legacyHelpersByFile {
      let source = try String(contentsOf: sourceFileURL(relativePath: file), encoding: .utf8)
      for signature in helperSignatures {
        let body = try XCTUnwrap(
          extractFunctionBody(signature: signature, from: source),
          "Missing helper body for \(signature) in \(file)"
        )
        XCTAssertFalse(
          body.contains("protocolAuthorityMode == .rustFull"),
          "\(file) \(signature) should not keep a dead rustFull branch inside a legacy helper"
        )
      }
    }
  }

  private func makeManager(
    protocolAuthorityMode: MLSProtocolAuthorityMode
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

  private func sourceFileURL(relativePath: String) -> URL {
    let testsDirectory = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
    let packageRoot = testsDirectory.deletingLastPathComponent().deletingLastPathComponent()
    return packageRoot.appendingPathComponent(relativePath)
  }

  private func extractFunctionBody(signature: String, from source: String) -> String? {
    guard let signatureRange = source.range(of: signature),
          let bodyStart = source[signatureRange.upperBound...].firstIndex(of: "{")
    else {
      return nil
    }

    var depth = 0
    var currentIndex = bodyStart
    while currentIndex < source.endIndex {
      let character = source[currentIndex]
      if character == "{" {
        depth += 1
      } else if character == "}" {
        depth -= 1
        if depth == 0 {
          return String(source[bodyStart...currentIndex])
        }
      }
      currentIndex = source.index(after: currentIndex)
    }

    return nil
  }

  private func extractConditionalBranchBody(
    matching prefix: String,
    from functionBody: String
  ) -> String? {
    guard let conditionalRange = functionBody.range(of: prefix),
          let bodyStart = functionBody[conditionalRange.upperBound...].firstIndex(of: "{")
    else {
      return nil
    }

    return extractBraceBody(startingAt: bodyStart, in: functionBody)
  }

  private func declaredFunctionNames(in source: String) -> Set<String> {
    let pattern = #"\bfunc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("#
    let regex = try! NSRegularExpression(pattern: pattern)
    let nsSource = source as NSString
    let range = NSRange(location: 0, length: nsSource.length)
    let matches = regex.matches(in: source, range: range)
    return Set(matches.compactMap { match in
      guard match.numberOfRanges > 1 else { return nil }
      return nsSource.substring(with: match.range(at: 1))
    })
  }

  private func invokedFunctionNames(
    in source: String,
    declaredFunctionNames: Set<String>
  ) -> Set<String> {
    Set(
      declaredFunctionNames.filter { functionName in
        source.contains("\(functionName)(")
      }
    )
  }

  private func extractFunctionName(from signature: String) -> String? {
    let pattern = #"func\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("#
    let regex = try! NSRegularExpression(pattern: pattern)
    let nsSignature = signature as NSString
    let range = NSRange(location: 0, length: nsSignature.length)
    guard let match = regex.firstMatch(in: signature, range: range), match.numberOfRanges > 1 else {
      return nil
    }
    return nsSignature.substring(with: match.range(at: 1))
  }

  private func extractBraceBody(
    startingAt bodyStart: String.Index,
    in source: String
  ) -> String? {
    var depth = 0
    var currentIndex = bodyStart
    while currentIndex < source.endIndex {
      let character = source[currentIndex]
      if character == "{" {
        depth += 1
      } else if character == "}" {
        depth -= 1
        if depth == 0 {
          return String(source[bodyStart...currentIndex])
        }
      }
      currentIndex = source.index(after: currentIndex)
    }

    return nil
  }

  private func extractSection(
    startMarker: String,
    endMarker: String,
    from source: String
  ) -> String? {
    guard let start = source.range(of: startMarker)?.lowerBound,
          let end = source.range(of: endMarker, range: start..<source.endIndex)?.lowerBound
    else {
      return nil
    }
    return String(source[start..<end])
  }

  private func hasUngatedForbiddenCall(_ forbiddenCall: String, in functionBody: String) -> Bool {
    var legacyGateDepth = 0

    for line in functionBody.split(separator: "\n", omittingEmptySubsequences: false) {
      let trimmed = line.trimmingCharacters(in: .whitespaces)

      if trimmed.hasPrefix("#if ") && trimmed.contains("MLS_SWIFT_LEGACY_PROTOCOL") {
        legacyGateDepth += 1
      }

      if trimmed.contains(forbiddenCall), legacyGateDepth == 0 {
        return true
      }

      if trimmed.hasPrefix("#endif"), legacyGateDepth > 0 {
        legacyGateDepth -= 1
      }
    }

    return false
  }
}
