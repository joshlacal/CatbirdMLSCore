import Foundation

/// Catalog of canonical `blue.catbird.chat.*` endpoints and routing metadata.
public struct MLSChatEndpointRoute: Sendable, Equatable {
  public let endpoint: String
  public let canonical: String
  public let requiresSignedRequest: Bool

  public init(
    endpoint: String,
    canonical: String,
    requiresSignedRequest: Bool
  ) {
    self.endpoint = endpoint
    self.canonical = canonical
    self.requiresSignedRequest = requiresSignedRequest
  }
}

public enum MLSChatEndpointCatalog {
  public static let canonicalNamespace = "blue.catbird.chat"

  public static let routes: [MLSChatEndpointRoute] = [
    route("acceptConversation", requiresSignedRequest: true),
    route("acknowledgeWelcome", requiresSignedRequest: true),
    route("activateReset", requiresSignedRequest: true),
    route("cancelLeafRecovery", requiresSignedRequest: true),
    route("cancelLeave", requiresSignedRequest: true),
    route("closeConversation", requiresSignedRequest: true),
    route("createConversation", requiresSignedRequest: true),
    route("deleteBlob", requiresSignedRequest: true),
    route("enrollDevice", requiresSignedRequest: true),
    route("getBlob", requiresSignedRequest: false),
    route("getBlobUsage", requiresSignedRequest: false),
    route("getConversationState", requiresSignedRequest: false),
    route("getConversations", requiresSignedRequest: false),
    route("getDevices", requiresSignedRequest: false),
    route("getEntries", requiresSignedRequest: false),
    route("getLeafRecoveryInbox", requiresSignedRequest: false),
    route("getOwnDevices", requiresSignedRequest: false),
    route("getPendingWelcomes", requiresSignedRequest: false),
    route("getSubscriptionTicket", requiresSignedRequest: false),
    route("prepareBlobUpload", requiresSignedRequest: false),
    route("publishTyping", requiresSignedRequest: false),
    route("rejectWelcome", requiresSignedRequest: true),
    route("replenishKeyPackages", requiresSignedRequest: true),
    route("requestLeafRecovery", requiresSignedRequest: true),
    route("requestLeave", requiresSignedRequest: true),
    route("requestReset", requiresSignedRequest: true),
    route("revokeDevice", requiresSignedRequest: true),
    route("sendMessage", requiresSignedRequest: true),
    route("submitTransition", requiresSignedRequest: true),
    route("subscribeEvents", requiresSignedRequest: false),
    route("uploadBlob", requiresSignedRequest: false)
  ]

  public static func route(forEndpoint endpoint: String) -> MLSChatEndpointRoute? {
    routes.first { $0.endpoint == endpoint || $0.canonical == endpoint }
  }

  private static func canonicalName(_ name: String) -> String {
    "\(canonicalNamespace).\(name)"
  }

  private static func route(_ name: String, requiresSignedRequest: Bool) -> MLSChatEndpointRoute {
    MLSChatEndpointRoute(
      endpoint: name,
      canonical: canonicalName(name),
      requiresSignedRequest: requiresSignedRequest
    )
  }
}
