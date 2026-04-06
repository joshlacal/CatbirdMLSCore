import XCTest
@testable import CatbirdMLSCore

final class MLSNotificationRoutingPolicyTests: XCTestCase {
  private let testUserDID = "did:plc:notification-routing-test"

  override func setUp() {
    super.setUp()
    resetSharedState()
  }

  override func tearDown() {
    resetSharedState()
    super.tearDown()
  }

  func testActiveForegroundRoutesToCacheOnly() {
    MLSNotificationCoordinator.setRoutingV2EnabledForTesting(true)

    let decision = MLSNotificationCoordinator.routingDecision(
      context: .appForegroundActive,
      recipientUserDID: testUserDID
    )

    XCTAssertEqual(decision.action, .cacheOnly)
    XCTAssertEqual(decision.owner, .appSync)
    XCTAssertEqual(decision.reason, .foregroundActiveUsesSync)
  }

  func testInactiveForegroundRoutesToDecrypt() {
    MLSNotificationCoordinator.setRoutingV2EnabledForTesting(true)

    let decision = MLSNotificationCoordinator.routingDecision(
      context: .appForegroundInactive,
      recipientUserDID: testUserDID
    )

    XCTAssertEqual(decision.action, .decrypt)
    XCTAssertEqual(decision.owner, .appNotification)
    XCTAssertEqual(decision.reason, .foregroundInactiveDecrypt)
  }

  func testNSEYieldsWhenMainAppActiveForRecipient() {
    MLSNotificationCoordinator.setRoutingV2EnabledForTesting(true)
    MLSNotificationCoordinator.setMainAppActive(true, activeUserDID: testUserDID)

    let decision = MLSNotificationCoordinator.routingDecision(
      context: .nseBackground,
      recipientUserDID: testUserDID
    )

    XCTAssertEqual(decision.action, .skip)
    XCTAssertNil(decision.owner)
    XCTAssertEqual(decision.reason, .nseYieldMainAppActive)
  }

  func testRoutingSkipsDuringAccountSwitch() {
    MLSNotificationCoordinator.setRoutingV2EnabledForTesting(true)
    MLSNotificationCoordinator.beginAccountSwitch(from: "did:plc:old-user", to: testUserDID)

    let decision = MLSNotificationCoordinator.routingDecision(
      context: .appForegroundInactive,
      recipientUserDID: testUserDID
    )

    XCTAssertEqual(decision.action, .skip)
    XCTAssertNil(decision.owner)
    XCTAssertEqual(decision.reason, .accountSwitchInProgress)
  }

  func testLegacyRoutingModeAllowsForegroundDecrypt() {
    MLSNotificationCoordinator.setRoutingV2EnabledForTesting(false)

    let decision = MLSNotificationCoordinator.routingDecision(
      context: .appForegroundActive,
      recipientUserDID: testUserDID
    )

    XCTAssertEqual(decision.action, .decrypt)
    XCTAssertEqual(decision.owner, .appNotification)
    XCTAssertEqual(decision.reason, .legacyRoutingDisabled)
  }

  func testMutationPublishIncrementsVersionWhenRequested() {
    let before = MLSStateVersionManager.shared.getDiskVersion(for: testUserDID)

    let published = MLSNotificationCoordinator.publishMutation(
      userDID: testUserDID,
      source: "unit_test",
      incrementVersion: true,
      postStateChanged: false
    )

    let after = MLSStateVersionManager.shared.getDiskVersion(for: testUserDID)
    XCTAssertEqual(after, before + 1)
    XCTAssertEqual(published, after)
  }

  func testMutationPublishLeavesVersionUnchangedWhenDisabled() {
    let before = MLSStateVersionManager.shared.getDiskVersion(for: testUserDID)

    let published = MLSNotificationCoordinator.publishMutation(
      userDID: testUserDID,
      source: "unit_test_no_increment",
      incrementVersion: false,
      postStateChanged: false
    )

    let after = MLSStateVersionManager.shared.getDiskVersion(for: testUserDID)
    XCTAssertEqual(after, before)
    XCTAssertNil(published)
  }

  private func resetSharedState() {
    if let defaults = UserDefaults(suiteName: "group.blue.catbird.shared") {
      defaults.removeObject(forKey: MLSNotificationRoutingPolicy.routingV2UserDefaultsKey)
      defaults.synchronize()
    }

    MLSNotificationCoordinator.setMainAppActive(false, activeUserDID: nil)
    MLSNotificationCoordinator.setShuttingDown(false, userDID: nil)
    MLSNotificationCoordinator.endAccountSwitch()
  }
}
