import XCTest
@testable import CatbirdMLSCore

final class MLSChatEndpointCatalogTests: XCTestCase {
  func testCanonicalCatalogContainsAllCleanChatEndpoints() {
    let routes = Set(MLSChatEndpointCatalog.routes.map(\.canonical))
    let expectedCanonicalEndpoints = Set([
      "acceptConversation", "acknowledgeWelcome", "activateReset",
      "cancelLeafRecovery", "cancelLeave", "closeConversation",
      "createConversation", "deleteBlob", "enrollDevice",
      "getBlob", "getBlobUsage", "getConversationState",
      "getConversations", "getDevices", "getEntries",
      "getLeafRecoveryInbox", "getOwnDevices", "getPendingWelcomes",
      "getSubscriptionTicket", "prepareBlobUpload", "publishTyping",
      "rejectWelcome", "replenishKeyPackages",
      "requestLeafRecovery", "requestLeave", "requestReset",
      "revokeDevice", "sendMessage", "submitTransition",
      "subscribeEvents", "uploadBlob"
    ].map { "blue.catbird.chat.\($0)" })

    XCTAssertEqual(routes, expectedCanonicalEndpoints)
    XCTAssertEqual(MLSChatEndpointCatalog.canonicalNamespace, "blue.catbird.chat")

    for route in MLSChatEndpointCatalog.routes {
      XCTAssertTrue(
        route.canonical.hasPrefix("blue.catbird.chat."),
        "canonical route escaped clean-chat namespace: \(route.canonical)"
      )
    }
  }

  func testCanonicalRouteLookup() {
    let sendMessageRoute = try! XCTUnwrap(
      MLSChatEndpointCatalog.route(forEndpoint: "sendMessage")
    )
    XCTAssertEqual(sendMessageRoute.canonical, "blue.catbird.chat.sendMessage")
    XCTAssertTrue(sendMessageRoute.requiresSignedRequest)

    let fullLookup = try! XCTUnwrap(
      MLSChatEndpointCatalog.route(forEndpoint: "blue.catbird.chat.sendMessage")
    )
    XCTAssertEqual(fullLookup, sendMessageRoute)

    let getEntriesRoute = try! XCTUnwrap(
      MLSChatEndpointCatalog.route(forEndpoint: "getEntries")
    )
    XCTAssertEqual(getEntriesRoute.canonical, "blue.catbird.chat.getEntries")
    XCTAssertFalse(getEntriesRoute.requiresSignedRequest)
  }

  func testSignedMutationRoutesRequireSignedRequest() {
    let signedEndpoints = [
      "sendMessage", "submitTransition", "createConversation",
      "closeConversation", "acceptConversation", "requestLeave",
      "cancelLeave", "enrollDevice", "revokeDevice",
      "replenishKeyPackages",
      "requestLeafRecovery", "cancelLeafRecovery", "requestReset",
      "activateReset", "acknowledgeWelcome", "rejectWelcome",
      "deleteBlob"
    ]

    for name in signedEndpoints {
      let route = try! XCTUnwrap(
        MLSChatEndpointCatalog.route(forEndpoint: name),
        "missing route for \(name)"
      )
      XCTAssertTrue(route.requiresSignedRequest, "\(name) should require signed request")
    }
  }

  func testReadAndTicketRoutesDoNotRequireSignedRequest() {
    let readEndpoints = [
      "getConversations", "getConversationState", "getEntries",
      "getPendingWelcomes", "getLeafRecoveryInbox", "getDevices",
      "getOwnDevices", "getBlobUsage", "getBlob",
      "prepareBlobUpload", "uploadBlob", "publishTyping",
      "getSubscriptionTicket", "subscribeEvents"
    ]

    for name in readEndpoints {
      let route = try! XCTUnwrap(
        MLSChatEndpointCatalog.route(forEndpoint: name),
        "missing route for \(name)"
      )
      XCTAssertFalse(route.requiresSignedRequest, "\(name) should not require signed request")
    }
  }
}
