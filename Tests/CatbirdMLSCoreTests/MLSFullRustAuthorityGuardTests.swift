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

    for (file, functionSignatures) in guardedFunctionsByFile {
      let source = try String(contentsOf: sourceFileURL(relativePath: file), encoding: .utf8)
      for signature in functionSignatures {
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

        let violations = forbiddenCalls.filter { call in
          hasUngatedForbiddenCall(call, in: body)
        }
        XCTAssertTrue(
          violations.isEmpty,
          "\(file) \(signature) has forbidden low-level MLSClient call(s) outside the legacy compile gate: \(violations.joined(separator: ", "))"
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
