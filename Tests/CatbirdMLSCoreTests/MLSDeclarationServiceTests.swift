import XCTest
import Petrel
@testable import CatbirdMLSCore

final class MLSDeclarationServiceTests: XCTestCase {
  func testDeterministicRkeyRoundTrip() {
    let formatted = MLSDeclarationService.formatDeterministicRkey(epoch: 42, seq: 7)
    XCTAssertEqual(formatted, "e00000000000000000042-s00000000000000000007")

    let parsed = MLSDeclarationService.parseDeterministicRkey(formatted)
    XCTAssertEqual(parsed?.epoch, 42)
    XCTAssertEqual(parsed?.seq, 7)
  }

  func testDeterministicRkeyRejectsMalformedValues() {
    XCTAssertNil(MLSDeclarationService.parseDeterministicRkey("invalid"))
    XCTAssertNil(MLSDeclarationService.parseDeterministicRkey("e42-s7"))
    XCTAssertNil(MLSDeclarationService.parseDeterministicRkey("e00000000000000000042-snotanumber000000000"))
    XCTAssertNil(MLSDeclarationService.parseDeterministicRkey("x00000000000000000042-s00000000000000000007"))
  }

  func testChatPolicyUpdateUnionDecodes() throws {
    let json = """
    {
      "$type": "blue.catbird.mlsChat.declaration#chatPolicyUpdate",
      "allowFollowersBypass": true,
      "allowFollowingBypass": false,
      "autoExpireDays": 14
    }
    """

    let data = try XCTUnwrap(json.data(using: .utf8))
    let decoded = try JSONDecoder().decode(
      BlueCatbirdMlsChatDeclaration.BlueCatbirdMlsChatDeclarationEventUnion.self,
      from: data
    )

    switch decoded {
    case .blueCatbirdMlsChatDeclarationChatPolicyUpdate(let update):
      XCTAssertTrue(update.allowFollowersBypass)
      XCTAssertFalse(update.allowFollowingBypass)
      XCTAssertEqual(update.autoExpireDays, 14)
    default:
      XCTFail("Expected chatPolicyUpdate variant")
    }
  }

  func testDeclarationFrictionErrorDescription() {
    let error = MLSConversationError.declarationUserConfirmationRequired(
      targetDid: "did:plc:abc123",
      operation: "selectKeyPackages",
      warning: "No declaration chain found"
    )

    let description = error.errorDescription ?? ""
    XCTAssertTrue(description.contains("did:plc:abc123"))
    XCTAssertTrue(description.contains("selectKeyPackages"))
    XCTAssertTrue(description.contains("No declaration chain found"))
  }

  func testMissingChainDecisionByRolloutMode() {
    let shadow = MLSDeclarationService.authorizationDecision(
      for: .missingChain,
      rolloutMode: .shadow,
      securitySensitive: true
    )
    XCTAssertTrue(shadow.allowed)
    XCTAssertFalse(shadow.requiresUserFriction)
    XCTAssertNotNil(shadow.warning)

    let softSensitive = MLSDeclarationService.authorizationDecision(
      for: .missingChain,
      rolloutMode: .soft,
      securitySensitive: true
    )
    XCTAssertTrue(softSensitive.allowed)
    XCTAssertTrue(softSensitive.requiresUserFriction)
    XCTAssertNotNil(softSensitive.warning)

    let softPassive = MLSDeclarationService.authorizationDecision(
      for: .missingChain,
      rolloutMode: .soft,
      securitySensitive: false
    )
    XCTAssertTrue(softPassive.allowed)
    XCTAssertFalse(softPassive.requiresUserFriction)

    let full = MLSDeclarationService.authorizationDecision(
      for: .missingChain,
      rolloutMode: .full,
      securitySensitive: true
    )
    XCTAssertFalse(full.allowed)
    XCTAssertNotNil(full.failureReason)
  }

  func testUnavailableDecisionByRolloutMode() {
    let shadow = MLSDeclarationService.authorizationDecision(
      for: .unavailable,
      rolloutMode: .shadow,
      securitySensitive: true
    )
    XCTAssertTrue(shadow.allowed)
    XCTAssertNotNil(shadow.warning)

    let soft = MLSDeclarationService.authorizationDecision(
      for: .unavailable,
      rolloutMode: .soft,
      securitySensitive: true
    )
    XCTAssertFalse(soft.allowed)
    XCTAssertNotNil(soft.failureReason)

    let full = MLSDeclarationService.authorizationDecision(
      for: .unavailable,
      rolloutMode: .full,
      securitySensitive: false
    )
    XCTAssertFalse(full.allowed)
    XCTAssertNotNil(full.failureReason)
  }
}
