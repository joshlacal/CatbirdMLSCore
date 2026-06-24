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
}
