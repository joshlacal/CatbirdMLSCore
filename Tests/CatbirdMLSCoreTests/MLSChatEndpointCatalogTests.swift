import XCTest
@testable import CatbirdMLSCore

final class MLSChatEndpointCatalogTests: XCTestCase {
  func testEveryLegacyTransportRouteHasAnExplicitCanonicalDisposition() {
    let routes = Set(MLSChatEndpointCatalog.routes.map(\.legacy))

    XCTAssertEqual(routes.count, 35)
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
