import XCTest
@testable import CatbirdMLSCore

final class MLSChatEndpointCatalogTests: XCTestCase {
  func testEveryLegacyTransportRouteHasAnExplicitCanonicalDisposition() {
    let routes = Set(MLSChatEndpointCatalog.routes.map(\.legacy))
    let expectedLegacyLexicons = Set([
      "beginDeviceAuthBinding", "beginTransitionAttestation", "bootstrapResetGroup",
      "checkBlocks", "commitGroupChange", "completeDeviceAuthBinding", "createConvo",
      "deleteBlob", "device", "finalizeGroupChange", "getBlob", "getBlobUsage",
      "getBlockStatus", "getConvoSettings", "getConvos", "getGroupMetadataBlob",
      "getGroupState", "getKeyPackageStatus", "getKeyPackages", "getMessages",
      "getPendingDevices", "getSubscriptionTicket", "invalidateKeyPackage", "leaveConvo",
      "listDevices", "optIn", "policy", "publishKeyPackages", "putGroupMetadataBlob",
      "reconcileKeyPackages", "registerDevice", "reissueWelcome", "reissueWelcomeRespond",
      "removeDevice", "reportRecoveryFailure", "reportSpam", "requestFailover", "resetGroup",
      "sendMessage", "subscribeEvents", "updateConvo", "updateCursor", "uploadBlob"
    ].map { "blue.catbird.mlsChat.\($0)" })

    XCTAssertTrue(expectedLegacyLexicons.isSubset(of: routes))
    XCTAssertEqual(expectedLegacyLexicons.count, 43)
    XCTAssertEqual(
      routes.subtracting(expectedLegacyLexicons),
      [
        "blue.catbird.mlsChat.blockChatSender",
        "blue.catbird.mlsChat.declaration",
        "blue.catbird.mlsChat.getKeyPackageStats",
        "blue.catbird.mlsChat.message"
      ]
    )
    XCTAssertEqual(
      MLSChatEndpointCatalog.excludedLegacyGeneratedTypes,
      ["blue.catbird.mlsChat.defs"]
    )
    XCTAssertTrue(routes.contains("blue.catbird.mlsChat.sendMessage"))
    XCTAssertTrue(routes.contains("blue.catbird.mlsChat.commitGroupChange"))
    XCTAssertTrue(routes.contains("blue.catbird.mlsChat.device"))

    for route in MLSChatEndpointCatalog.routes {
      if let canonical = route.canonical {
        XCTAssertTrue(
          canonical.hasPrefix("blue.catbird.chat."),
          "canonical route escaped clean-chat namespace: \(canonical)"
        )
      }
    }
  }

  func testDirectCompatibilityRoutesUseCanonicalGeneratedNames() {
    let direct = Dictionary(
      uniqueKeysWithValues: MLSChatEndpointCatalog.routes.compactMap { route in
        route.disposition == .direct ? (route.legacy, route.canonical) : nil
      }
    )

    XCTAssertEqual(
      direct["blue.catbird.mlsChat.getBlob"],
      "blue.catbird.chat.getBlob"
    )
    XCTAssertNil(direct["blue.catbird.mlsChat.getSubscriptionTicket"])
    XCTAssertNil(direct["blue.catbird.mlsChat.subscribeEvents"])
  }

  func testSignedMutationRoutesAreNotPretendedToBeWireCompatible() {
    let signed = MLSChatEndpointCatalog.routes.filter {
      $0.disposition == .signedAdapterRequired
    }

    XCTAssertTrue(signed.contains { $0.legacy.hasSuffix(".sendMessage") })
    XCTAssertTrue(signed.contains { $0.legacy.hasSuffix(".commitGroupChange") })
    XCTAssertTrue(signed.contains { $0.legacy.hasSuffix(".registerDevice") })
    XCTAssertTrue(signed.allSatisfy(\.requiresSignedRequest))
  }

  func testRecoveryAndRecordRoutesRemainExplicitlyBlockedUntilAdaptersExist() {
    let blocked = MLSChatEndpointCatalog.routes.filter {
      $0.disposition == .adapterBlocked
    }

    XCTAssertTrue(blocked.contains { $0.legacy.hasSuffix(".reportRecoveryFailure") })
    XCTAssertTrue(blocked.contains { $0.legacy.hasSuffix(".device") })
    XCTAssertTrue(blocked.allSatisfy { $0.canonical == nil || $0.requiresSignedRequest })
  }
}
