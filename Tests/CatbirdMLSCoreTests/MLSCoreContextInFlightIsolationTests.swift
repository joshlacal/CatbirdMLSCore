import XCTest
@testable import CatbirdMLSCore

final class MLSCoreContextInFlightIsolationTests: XCTestCase {
  
  func testInFlightDecryptionIsolationBetweenAccounts() async throws {
    let tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: tempDir) }
    
    let config = MLSCoreContext.Configuration(
      storageDirectory: tempDir,
      keychainAccessGroup: nil,
      disableDarwinNotifications: true,
      loggerSubsystem: "blue.catbird.test"
    )
    let context = MLSCoreContext(configuration: config)
    
    let userA = "did:plc:alice_\(UUID().uuidString)"
    let userB = "did:plc:bob_\(UUID().uuidString)"
    let sharedMessageID = "colliding-msg-\(UUID().uuidString)"
    let convoA = "convo-alice"
    let convoB = "convo-bob"
    
    // Check coordination for User A
    let resultA = await context.checkOrAwaitDecryption(
      messageID: sharedMessageID,
      userDID: userA,
      conversationID: convoA
    )
    
    // Check coordination for User B with identical messageID
    let resultB = await context.checkOrAwaitDecryption(
      messageID: sharedMessageID,
      userDID: userB,
      conversationID: convoB
    )
    
    // Both should safely determine they should proceed independently
    guard case .shouldProceed = resultA else {
      XCTFail("Expected shouldProceed for user A, got \(resultA)")
      return
    }
    guard case .shouldProceed = resultB else {
      XCTFail("Expected shouldProceed for user B, got \(resultB)")
      return
    }
  }
}
