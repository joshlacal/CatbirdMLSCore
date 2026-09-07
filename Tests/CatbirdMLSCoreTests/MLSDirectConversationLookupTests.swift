import Foundation
import GRDB
import Petrel
import XCTest
@testable import CatbirdMLSCore

@MainActor
final class MLSDirectConversationLookupTests: XCTestCase {
  private func lookup(
    terminalState: String? = nil,
    terminalUser: String = "did:plc:alice",
    addNewerClosed: Bool = false
  ) async throws -> String? {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let database = try DatabasePool(path: directory.appendingPathComponent("lookup.sqlite").path)
    defer { try? database.close() }
    try await database.write { db in
      try db.execute(sql: """
        CREATE TABLE MLSConversationModel (
          currentUserDID TEXT, conversationID TEXT, isActive INTEGER, updatedAt INTEGER);
        CREATE TABLE MLSMemberModel (
          currentUserDID TEXT, conversationID TEXT, did TEXT, isActive INTEGER);
        CREATE TABLE mls_orchestrator_terminal_access (
          user_did TEXT, conversation_id TEXT, group_id BLOB, state TEXT);
        INSERT INTO MLSConversationModel VALUES ('did:plc:alice', 'old', 1, 1);
        INSERT INTO MLSMemberModel VALUES ('did:plc:alice', 'old', 'did:plc:alice', 1);
        INSERT INTO MLSMemberModel VALUES ('did:plc:alice', 'old', 'did:plc:bob', 1);
        """)
      if let terminalState {
        try db.execute(sql: "INSERT INTO mls_orchestrator_terminal_access VALUES (?, 'old', x'01', ?)",
                       arguments: [terminalUser, terminalState])
      }
      if addNewerClosed {
        try db.execute(sql: """
          INSERT INTO MLSConversationModel VALUES ('did:plc:alice', 'new', 1, 2);
          INSERT INTO MLSMemberModel VALUES ('did:plc:alice', 'new', 'did:plc:alice', 1);
          INSERT INTO MLSMemberModel VALUES ('did:plc:alice', 'new', 'did:plc:bob', 1);
          INSERT INTO mls_orchestrator_terminal_access VALUES ('did:plc:alice', 'new', x'02', 'closed');
          """)
      }
    }
    let client = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let api = await MLSAPIClient(client: client, environment: .custom(serviceDID: "did:web:example.com#atproto_mls"))
    let manager = MLSConversationManager(apiClient: api, database: database,
      userDid: "did:plc:alice", atProtoClient: client, protocolAuthorityMode: .swiftLegacy)
    return try await manager.findDirectConversation(with: DID(didString: "did:plc:bob"))
  }

  func testClosedHistoryIsNotReusedDespiteActiveMembers() async throws {
    let result = try await lookup(terminalState: "closed")
    XCTAssertNil(result)
  }

  func testNewerClosedHistoryDoesNotHideActiveConversation() async throws {
    let result = try await lookup(addNewerClosed: true)
    XCTAssertEqual(result, "old")
  }

  func testOtherAccountClosureDoesNotBlockReuse() async throws {
    let result = try await lookup(terminalState: "closed", terminalUser: "did:plc:other")
    XCTAssertEqual(result, "old")
  }

  func testRemovedDeviceReusesAccountConversationForAccessRepair() async throws {
    let result = try await lookup(terminalState: "device_removed")
    XCTAssertEqual(result, "old")
  }

  func testActiveDirectIsReused() async throws {
    let result = try await lookup()
    XCTAssertEqual(result, "old")
  }
}
