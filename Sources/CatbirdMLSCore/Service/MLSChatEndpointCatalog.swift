import Foundation

/// Migration inventory for the pre-cutover MLS chat API.
///
/// The clean-chat generated procedures are intentionally not source-compatible
/// with the old procedures: every mutation carries a signed request, and the
/// read models use canonical conversation/entry projections. Keeping that
/// distinction explicit prevents a legacy DTO from being sent to a clean
/// endpoint by accident while the signing and projection adapters are landed.
public enum MLSChatRouteDisposition: String, Sendable {
  /// Input and output can be forwarded through the generated clean procedure.
  case direct
  /// A generated clean procedure exists, but a canonical signed request must
  /// be built first.
  case signedAdapterRequired
  /// More than one clean procedure is needed to preserve the old operation.
  case compoundAdapterRequired
  /// No semantically equivalent clean procedure exists yet.
  case adapterBlocked
}

public struct MLSChatEndpointRoute: Sendable, Equatable {
  public let legacy: String
  public let canonical: String?
  public let disposition: MLSChatRouteDisposition
  public let requiresSignedRequest: Bool

  public init(
    legacy: String,
    canonical: String?,
    disposition: MLSChatRouteDisposition,
    requiresSignedRequest: Bool
  ) {
    self.legacy = legacy
    self.canonical = canonical
    self.disposition = disposition
    self.requiresSignedRequest = requiresSignedRequest
  }
}

/// A single source of truth for route migration. This is deliberately data,
/// rather than a fallback transport: legacy call sites must not silently
/// downgrade a clean request when a required adapter is unavailable.
public enum MLSChatEndpointCatalog {
  public static let legacyNamespace = "blue.catbird.mlsChat"
  public static let canonicalNamespace = "blue.catbird.chat"

  /// `Defs` is a generated shared-type container, not a transport route. It
  /// is intentionally excluded from `routes`; all 43 legacy procedures and
  /// records are represented below. Four additional call-site-only names
  /// (`getKeyPackageStats`, `blockChatSender`, `declaration`, and `message`)
  /// are retained because they occur in core source but have no checked-in
  /// legacy generated file.
  public static let excludedLegacyGeneratedTypes = ["\(legacyNamespace).defs"]

  public static let routes: [MLSChatEndpointRoute] = [
    direct("getBlob", "getBlob"),
    signed("sendMessage", "sendMessage"),
    signed("createConvo", "createConversation"),
    signed("commitGroupChange", "submitTransition"),
    signed("registerDevice", "enrollDevice"),
    signed("removeDevice", "revokeDevice"),
    signed("publishKeyPackages", "replenishKeyPackages"),
    signed("leaveConvo", "requestLeave"),
    signed("resetGroup", "requestReset"),
    compound("bootstrapResetGroup", "activateReset"),
    compound("beginDeviceAuthBinding", "rebindDeviceAuthentication"),
    compound("completeDeviceAuthBinding", "rebindDeviceAuthentication"),
    signed("beginTransitionAttestation", "submitTransition"),
    signed("finalizeGroupChange", "submitTransition"),
    signed("deleteBlob", "deleteBlob"),
    unsignedAdapter("uploadBlob", "uploadBlob"),
    adapter("getConvos", "getConversations"),
    adapter("getMessages", "getEntries"),
    adapter("getGroupState", "getConversationState"),
    adapter("getBlobUsage", "getBlobUsage"),
    adapter("getSubscriptionTicket", "getSubscriptionTicket"),
    adapter("subscribeEvents", "subscribeEvents"),
    adapter("listDevices", "getOwnDevices"),
    adapter("getPendingDevices", "getOwnDevices"),
    adapter("getKeyPackages", nil),
    adapter("getKeyPackageStatus", nil),
    adapter("getGroupMetadataBlob", "getBlob"),
    compound("putGroupMetadataBlob", "prepareBlobUpload"),
    adapter("getConvoSettings", nil),
    adapter("getKeyPackageStats", nil),
    adapter("blockChatSender", nil),
    adapter("checkBlocks", nil),
    adapter("getBlockStatus", nil),
    adapter("optIn", nil),
    adapter("updateConvo", nil),
    adapter("updateCursor", nil),
    adapter("reconcileKeyPackages", nil),
    adapter("invalidateKeyPackage", nil),
    adapter("reissueWelcome", nil),
    adapter("reissueWelcomeRespond", nil),
    signedAdapter("reportRecoveryFailure", "requestLeafRecovery"),
    adapter("reportSpam", nil),
    adapter("requestFailover", nil),
    record("device"),
    record("declaration"),
    record("policy"),
    record("message")
  ]

  public static func route(forLegacyEndpoint endpoint: String) -> MLSChatEndpointRoute? {
    routes.first { $0.legacy == endpoint }
  }

  private static func legacyName(_ name: String) -> String {
    "\(legacyNamespace).\(name)"
  }

  private static func canonicalName(_ name: String) -> String {
    "\(canonicalNamespace).\(name)"
  }

  private static func direct(_ legacy: String, _ canonical: String) -> MLSChatEndpointRoute {
    MLSChatEndpointRoute(
      legacy: legacyName(legacy), canonical: canonicalName(canonical), disposition: .direct,
      requiresSignedRequest: false
    )
  }

  private static func signed(_ legacy: String, _ canonical: String) -> MLSChatEndpointRoute {
    MLSChatEndpointRoute(
      legacy: legacyName(legacy), canonical: canonicalName(canonical),
      disposition: .signedAdapterRequired, requiresSignedRequest: true
    )
  }

  private static func adapter(_ legacy: String, _ canonical: String?) -> MLSChatEndpointRoute {
    MLSChatEndpointRoute(
      legacy: legacyName(legacy), canonical: canonical.map(canonicalName),
      disposition: .adapterBlocked, requiresSignedRequest: false
    )
  }

  private static func signedAdapter(_ legacy: String, _ canonical: String) -> MLSChatEndpointRoute {
    MLSChatEndpointRoute(
      legacy: legacyName(legacy), canonical: canonicalName(canonical),
      disposition: .adapterBlocked, requiresSignedRequest: true
    )
  }

  private static func unsignedAdapter(_ legacy: String, _ canonical: String) -> MLSChatEndpointRoute {
    MLSChatEndpointRoute(
      legacy: legacyName(legacy), canonical: canonicalName(canonical),
      disposition: .adapterBlocked, requiresSignedRequest: false
    )
  }

  private static func compound(_ legacy: String, _ canonical: String) -> MLSChatEndpointRoute {
    MLSChatEndpointRoute(
      legacy: legacyName(legacy), canonical: canonicalName(canonical),
      disposition: .compoundAdapterRequired, requiresSignedRequest: true
    )
  }

  private static func record(_ collection: String) -> MLSChatEndpointRoute {
    MLSChatEndpointRoute(
      legacy: legacyName(collection), canonical: nil, disposition: .adapterBlocked,
      requiresSignedRequest: false
    )
  }
}
