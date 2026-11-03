import CatbirdMLS
import Foundation
import Petrel
import PetrelCatbird

/// Thin network transport implementing `PlatformTransport` and `PushFetchTransport` (`Spec §6.3`, `D1`, `Gate 7`, `A-3`, `A-5`, `I10`).
///
/// Preserves `Atproto-Space-Mac` and `Atproto-Space-Credential` headers verbatim across FFI,
/// blocks all `3xx` redirects, enforces SSRF/NSID checks, and never refreshes OAuth inside NSE (`I10`).
public final class PetrelPlatformTransport: PlatformTransport, PushFetchTransport, @unchecked Sendable {
  public static let maxHttpResponseBytes = 16 * 1024 * 1024

  private let atProtoClient: ATProtoClient?
  private let homeDsEndpoint: URL?
  private let homeDsDid: String
  private let actorDeviceId: String?
  private let boundedSession: MLSBoundedURLSession
  private let allowLoopbackForTesting: Bool
  private let httpOverride: (@Sendable (PlatformHttpRequest) throws -> PlatformHttpResponse)?
  private let lock = NSLock()
  private var webSocketTask: URLSessionWebSocketTask?
  private var webSocketSession: URLSession?

  public init(
    atProtoClient: ATProtoClient? = nil,
    homeDsEndpoint: URL? = URL(string: "https://chat.catbird.blue"),
    homeDsDid: String = "did:web:chat.catbird.blue",
    actorDeviceId: String? = nil,
    allowLoopbackForTesting: Bool = false,
    httpOverride: (@Sendable (PlatformHttpRequest) throws -> PlatformHttpResponse)? = nil
  ) {
    self.atProtoClient = atProtoClient
    self.homeDsEndpoint = homeDsEndpoint
    self.homeDsDid = Self.stripDidFragment(homeDsDid)
    self.actorDeviceId = actorDeviceId?.trimmingCharacters(in: .whitespacesAndNewlines)
    self.boundedSession = .shared
    self.allowLoopbackForTesting = allowLoopbackForTesting
    self.httpOverride = httpOverride
  }

  init(
    atProtoClient: ATProtoClient? = nil,
    homeDsEndpoint: URL? = URL(string: "https://chat.catbird.blue"),
    homeDsDid: String = "did:web:chat.catbird.blue",
    actorDeviceId: String? = nil,
    boundedSession: MLSBoundedURLSession,
    allowLoopbackForTesting: Bool = false,
    httpOverride: (@Sendable (PlatformHttpRequest) throws -> PlatformHttpResponse)? = nil
  ) {
    self.atProtoClient = atProtoClient
    self.homeDsEndpoint = homeDsEndpoint
    self.homeDsDid = Self.stripDidFragment(homeDsDid)
    self.actorDeviceId = actorDeviceId?.trimmingCharacters(in: .whitespacesAndNewlines)
    self.boundedSession = boundedSession
    self.allowLoopbackForTesting = allowLoopbackForTesting
    self.httpOverride = httpOverride
  }

  public func executeHttp(request: PlatformHttpRequest) throws -> PlatformHttpResponse {
    precondition(!Thread.isMainThread, "PetrelPlatformTransport.executeHttp must never block the main thread")
    if let httpOverride {
      return try httpOverride(request)
    }

    guard let url = URL(string: request.url) else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Invalid request URL: \(request.url)")
    }
    let mailboxKeysDeletionRepo: String? = try Self.extractXrpcNsid(url) == MLSOutboundHTTPValidator.deleteRecordNSID
      ? MLSOutboundHTTPValidator.mailboxKeysDeletionRepo(method: request.method, body: request.body)
      : nil
    try validateOutboundURLAndNSID(url, permittingMailboxKeysDeletion: mailboxKeysDeletionRepo != nil)

    var urlRequest = URLRequest(url: url)
    urlRequest.httpMethod = request.method
    urlRequest.httpBody = request.body
    urlRequest.timeoutInterval = 20.0
    var hasAuthorizationHeader = false
    for header in request.headers {
      if header.name.caseInsensitiveCompare("Authorization") == .orderedSame {
        hasAuthorizationHeader = true
      }
      urlRequest.setValue(header.value, forHTTPHeaderField: header.name)
    }

    if !hasAuthorizationHeader, let atProtoClient = self.atProtoClient {
      let immutableRequest = urlRequest
      let configuredHomeDs = self.homeDsEndpoint
      let configuredHomeDsDid = self.homeDsDid
      let bridged = Self.runAsyncAuthBridge(timeoutSeconds: 25.0) { () -> PlatformHttpResponse? in
        let clientBaseURL = await atProtoClient.networkService.baseURL
        if Self.sameOrigin(url, clientBaseURL) {
          if let mailboxKeysDeletionRepo, try await atProtoClient.getDid() != mailboxKeysDeletionRepo {
            throw MLSOutboundHTTPValidator.ValidationError.disallowedNSID(MLSOutboundHTTPValidator.deleteRecordNSID)
          }
          let (data, http) = try await atProtoClient.networkService.performRequestReturningHTTPErrorResponses(
            immutableRequest,
            skipTokenRefresh: false
          )
          return Self.makePlatformHttpResponse(data: data, http: http)
        }
        if let configuredHomeDs,
           Self.sameOrigin(url, configuredHomeDs),
           let nsid = Self.extractXrpcNsid(url),
           nsid.hasPrefix("blue.catbird.chat.")
        {
          let (code, authOut) = try await atProtoClient.com.atproto.server.getServiceAuth(
            input: .init(
              aud: configuredHomeDsDid,
              exp: nil,
              lxm: try NSID(nsidString: nsid)
            )
          )
          if (200..<300).contains(code), let token = authOut?.token, !token.isEmpty {
            var authedRequest = immutableRequest
            authedRequest.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
            let (data, http) = try await self.boundedSession.data(
              for: authedRequest,
              maxBytes: Self.maxHttpResponseBytes
            )
            return Self.makePlatformHttpResponse(data: data, http: http)
          }
        }
        return nil
      }
      if let directResponse = try bridged.get() {
        return directResponse
      }
    }
    // A mailboxKeys deletion only runs against the signed-in account's own PDS session.
    if mailboxKeysDeletionRepo != nil {
      throw MLSOutboundHTTPValidator.ValidationError.disallowedNSID(MLSOutboundHTTPValidator.deleteRecordNSID)
    }

    do {
      let (data, http) = try self.boundedSession.performSync(
        for: urlRequest,
        maxBytes: Self.maxHttpResponseBytes,
        timeoutSeconds: 25.0
      )
      return Self.makePlatformHttpResponse(data: data, http: http)
    } catch let err as MLSBoundedURLSession.BoundedFetchError {
      if case .timedOut = err {
        throw CatbirdMLS.MlsError.Internal(message: "HTTP request timed out")
      }
      throw CatbirdMLS.MlsError.Internal(message: err.localizedDescription)
    } catch {
      throw CatbirdMLS.MlsError.Internal(message: error.localizedDescription)
    }
  }

  public func connectRealtimeStream(homeDsEndpoint: String, cursorSeq: Int64?) throws {
    guard let baseURL = URL(string: homeDsEndpoint) else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Invalid homeDsEndpoint: \(homeDsEndpoint)")
    }
    if !self.allowLoopbackForTesting {
      try MLSOutboundHTTPValidator.validatePublicHTTPSServiceEndpoint(baseURL)
    }

    var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false)
    components?.scheme = (baseURL.scheme == "http" && self.allowLoopbackForTesting) ? "ws" : "wss"
    components?.path = "/xrpc/blue.catbird.chat.subscribeEvents"
    if let cursorSeq {
      components?.queryItems = [URLQueryItem(name: "cursor", value: String(cursorSeq))]
    }
    guard let wsURL = components?.url else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Failed to construct realtime WebSocket URL")
    }

    self.lock.lock()
    defer { self.lock.unlock() }
    self.webSocketTask?.cancel(with: .goingAway, reason: nil)
    self.webSocketSession?.invalidateAndCancel()

    let config = URLSessionConfiguration.ephemeral
    config.httpCookieStorage = nil
    config.urlCredentialStorage = nil
    let session = URLSession(configuration: config)
    let task = session.webSocketTask(with: wsURL)
    self.webSocketSession = session
    self.webSocketTask = task
    task.resume()
  }

  public func disconnectRealtimeStream() throws {
    self.lock.lock()
    let task = self.webSocketTask
    let session = self.webSocketSession
    self.webSocketTask = nil
    self.webSocketSession = nil
    self.lock.unlock()

    task?.cancel(with: .normalClosure, reason: nil)
    session?.invalidateAndCancel()
  }

  // MARK: - PushFetchTransport (Zero OAuth Refresh, I10 < 2s Budget)

  public func fetchMissingEntries(
    conversationId: String,
    afterSeq: Int64,
    chatDpopToken: String
  ) throws -> PushFetchBundle {
    precondition(!Thread.isMainThread, "PetrelPlatformTransport.fetchMissingEntries must never block the main thread")
    let trimmedToken = chatDpopToken.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmedToken.isEmpty else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Missing cached chat-dpop token")
    }
    guard let base = self.homeDsEndpoint else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Missing home DS endpoint")
    }
    if !self.allowLoopbackForTesting {
      try MLSOutboundHTTPValidator.validatePublicHTTPSServiceEndpoint(base)
    }

    var components = URLComponents(url: base, resolvingAgainstBaseURL: false)
    components?.path = "/xrpc/blue.catbird.chat.getEntries"
    var queryItems = [
      URLQueryItem(name: "conversationId", value: conversationId),
      URLQueryItem(name: "afterSeq", value: String(afterSeq)),
    ]
    if let actorDeviceId = self.actorDeviceId, !actorDeviceId.isEmpty {
      queryItems.append(URLQueryItem(name: "actorDeviceId", value: actorDeviceId))
    }
    components?.queryItems = queryItems
    guard let url = components?.url else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Malformed getEntries URL")
    }

    var req = URLRequest(url: url)
    req.httpMethod = "GET"
    req.timeoutInterval = 1.5
    let authHeader: String
    if trimmedToken.lowercased().hasPrefix("bearer ") || trimmedToken.lowercased().hasPrefix("dpop ") {
      authHeader = trimmedToken
    } else {
      authHeader = "Bearer \(trimmedToken)"
    }
    req.setValue(authHeader, forHTTPHeaderField: "Authorization")

    do {
      let (data, http) = try self.boundedSession.performSync(
        for: req,
        maxBytes: MLSBoundedURLSession.labyrinthMaxBytes,
        timeoutSeconds: 1.6
      )
      guard (200..<300).contains(http.statusCode) else {
        throw CatbirdMLS.MlsError.Internal(message: "Push getEntries returned HTTP \(http.statusCode)")
      }
      return try Self.decodePushGetEntriesBundle(from: data)
    } catch let mlsErr as CatbirdMLS.MlsError {
      throw mlsErr
    } catch let err as MLSBoundedURLSession.BoundedFetchError {
      if case .timedOut = err {
        throw CatbirdMLS.MlsError.Internal(message: "Push catch-up timed out (I10 < 2s budget)")
      }
      throw CatbirdMLS.MlsError.Internal(message: err.localizedDescription)
    } catch {
      throw CatbirdMLS.MlsError.Internal(message: error.localizedDescription)
    }
  }

  public func getAuthorizedDeviceKeys(
    did: String,
    chatDpopToken _: String?
  ) throws -> [AuthorizedPeerDeviceKey] {
    precondition(!Thread.isMainThread, "PetrelPlatformTransport.getAuthorizedDeviceKeys must never block the main thread")
    do {
      let records = try MLSPublicPDSReader.fetchAuthorizedDeviceRecordsSync(
        did: did,
        boundedSession: self.boundedSession,
        timeoutSeconds: 1.5
      )
      return records.map { record in
        let deviceId = record.uri.split(separator: "/").last.map(String.init) ?? record.uri
        return AuthorizedPeerDeviceKey(
          did: did,
          deviceId: deviceId,
          signaturePublicKey: record.publicKey
        )
      }
    } catch let err as MLSBoundedURLSession.BoundedFetchError {
      if case .timedOut = err {
        throw CatbirdMLS.MlsError.Internal(message: "Authorized device key fetch timed out")
      }
      throw CatbirdMLS.MlsError.Internal(message: err.localizedDescription)
    } catch {
      throw CatbirdMLS.MlsError.Internal(message: error.localizedDescription)
    }
  }

  // MARK: - Canonical `blue.catbird.chat.getEntries` & Fixture Wire Decoding

  static func decodePushGetEntriesBundle(from data: Data) throws -> PushFetchBundle {
    guard let root = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
          let rawEntries = root["entries"] as? [[String: Any]]
    else {
      throw CatbirdMLS.MlsError.Internal(message: "Malformed getEntries JSON response")
    }

    var fetchedEntries: [FetchedPushEntry] = []
    fetchedEntries.reserveCapacity(rawEntries.count)
    var latestSnapshot: FetchedMetadataSnapshot? = decodeMetadataSnapshotObject(
      root["metadataSnapshot"] as? [String: Any]
    )

    for raw in rawEntries {
      let seq = parseInt64(raw["seq"]) ?? 0
      let rawType = raw["$type"] as? String
      let signedRequest = raw["signedRequest"] as? [String: Any]
      let signedBody = signedRequest?["body"] as? [String: Any]

      let entryKind = resolveEntryKind(raw: raw, rawType: rawType)
      let priorEpoch = parseInt64((signedBody?["prior"] as? [String: Any])?["epoch"])
      let nextEpoch = parseInt64((signedBody?["next"] as? [String: Any])?["epoch"])
      let epoch = parseInt64(raw["epoch"])
        ?? parseInt64(signedBody?["epoch"])
        ?? priorEpoch
        ?? nextEpoch
        ?? 0

      let senderDid = (raw["senderDid"] as? String)
        ?? (raw["sender_did"] as? String)
        ?? (signedBody?["actorDid"] as? String)
        ?? ""

      let entryId = (signedBody?["messageId"] as? String)
        ?? (raw["entryId"] as? String)
        ?? (raw["messageId"] as? String)
        ?? (raw["message_id"] as? String)
        ?? "entry-\(seq)"

      let payloadPruned = (raw["payloadPruned"] as? Bool) == true
      let mlsBytes: Data
      if payloadPruned {
        mlsBytes = Data()
      } else {
        let appMsg = signedBody?["applicationMessage"] as? [String: Any]
        let commitObj = signedBody?["commit"] as? [String: Any]
        mlsBytes = decodeBytesField(appMsg?["bytes"])
          ?? decodeBytesField(commitObj?["bytes"])
          ?? decodeBytesField(signedBody?["ciphertext"])
          ?? decodeBytesField(raw["mlsBytes"])
          ?? decodeBytesField(raw["ciphertext"])
          ?? Data()
      }

      if let bodySnapshot = decodeMetadataSnapshotObject(signedBody?["metadataSnapshot"] as? [String: Any]) {
        latestSnapshot = bodySnapshot
      }

      fetchedEntries.append(
        FetchedPushEntry(
          entryId: entryId,
          seq: seq,
          epoch: epoch,
          entryKind: entryKind,
          senderDid: senderDid,
          mlsBytes: mlsBytes
        )
      )
    }

    return PushFetchBundle(
      entries: fetchedEntries,
      metadataSnapshot: latestSnapshot,
      authorizedDeviceKeys: []
    )
  }

  private static func resolveEntryKind(raw: [String: Any], rawType: String?) -> String {
    if let explicit = (raw["entryKind"] as? String) ?? (raw["entry_kind"] as? String), !explicit.isEmpty {
      return explicit
    }
    guard let rawType else { return "application" }
    let fragment = rawType.split(separator: "#").last.map(String.init) ?? rawType
    switch fragment {
    case "applicationEntry":
      return "application"
    case "commitEntry", "leafRecoveryFulfillmentEntry", "leaveCommitFulfillmentEntry":
      return "commit"
    case "metadataEntry":
      return "metadata"
    default:
      if fragment.hasSuffix("Entry") {
        return String(fragment.dropLast("Entry".count))
      }
      return fragment
    }
  }

  private static func decodeMetadataSnapshotObject(_ obj: [String: Any]?) -> FetchedMetadataSnapshot? {
    guard let obj,
          let metadataVersion = parseInt64(obj["metadataVersion"]),
          let ciphertext = decodeBytesField(obj["ciphertext"])
    else {
      return nil
    }
    let epoch = parseInt64(obj["epoch"])
      ?? parseInt64((obj["coordinate"] as? [String: Any])?["epoch"])
      ?? 0
    let nonce = decodeBytesField(obj["nonce"]) ?? Data(repeating: 0, count: 12)
    return FetchedMetadataSnapshot(
      epoch: epoch,
      metadataVersion: metadataVersion,
      nonce: nonce,
      ciphertext: ciphertext
    )
  }

  private static func decodeBytesField(_ value: Any?) -> Data? {
    if let dict = value as? [String: Any], let b64 = dict["$bytes"] as? String {
      return decodeFlexibleBase64(b64)
    }
    if let b64 = value as? String {
      return decodeFlexibleBase64(b64)
    }
    return nil
  }

  private static func decodeFlexibleBase64(_ raw: String) -> Data? {
    let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else { return nil }
    if let direct = Data(base64Encoded: trimmed) {
      return direct
    }
    let standard = trimmed
      .replacingOccurrences(of: "-", with: "+")
      .replacingOccurrences(of: "_", with: "/")
    let padded = standard + String(repeating: "=", count: (4 - standard.utf8.count % 4) % 4)
    return Data(base64Encoded: padded)
  }

  private static func parseInt64(_ value: Any?) -> Int64? {
    if let i64 = value as? Int64 { return i64 }
    if let i = value as? Int { return Int64(i) }
    if let num = value as? NSNumber { return num.int64Value }
    if let str = value as? String { return Int64(str) }
    return nil
  }

  fileprivate static func stripDidFragment(_ did: String) -> String {
    let trimmed = did.trimmingCharacters(in: .whitespacesAndNewlines)
    return trimmed.split(separator: "#", maxSplits: 1).first.map(String.init) ?? trimmed
  }

  private static func sameOrigin(_ lhs: URL, _ rhs: URL) -> Bool {
    guard let lScheme = lhs.scheme?.lowercased(),
          let rScheme = rhs.scheme?.lowercased(),
          lScheme == rScheme,
          let lHost = lhs.host?.lowercased(),
          let rHost = rhs.host?.lowercased(),
          lHost == rHost
    else {
      return false
    }
    let defaultPort: Int? = (lScheme == "https") ? 443 : (lScheme == "http" ? 80 : nil)
    return (lhs.port ?? defaultPort) == (rhs.port ?? defaultPort)
  }

  private static func extractXrpcNsid(_ url: URL) -> String? {
    guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
          components.path.hasPrefix("/xrpc/")
    else {
      return nil
    }
    let nsid = String(components.path.dropFirst("/xrpc/".count))
    return nsid.isEmpty ? nil : nsid
  }

  private static func makePlatformHttpResponse(data: Data, http: HTTPURLResponse) -> PlatformHttpResponse {
    var headers: [PlatformHttpHeader] = []
    for (rawKey, rawVal) in http.allHeaderFields {
      if let name = rawKey as? String, let value = rawVal as? String {
        headers.append(PlatformHttpHeader(name: name, value: value))
      }
    }
    return PlatformHttpResponse(
      status: UInt16(clamping: http.statusCode),
      headers: headers,
      body: data
    )
  }

  private static func runAsyncAuthBridge<T: Sendable>(
    timeoutSeconds: TimeInterval,
    _ operation: @escaping @Sendable () async throws -> T
  ) -> Result<T, Error> {
    let box = AsyncAuthBridgeBox<T>()
    let task = Task {
      let outcome = await Result { try await operation() }
      box.complete(outcome)
    }
    let nanos = Int64(max(1.0, timeoutSeconds * 1_000_000_000.0))
    if box.sema.wait(timeout: .now() + .nanoseconds(Int(clamping: nanos))) == .timedOut {
      task.cancel()
      return .failure(CatbirdMLS.MlsError.Internal(message: "Authenticated HTTP bridge timed out"))
    }
    return box.readResult() ?? .failure(CatbirdMLS.MlsError.Internal(message: "Authenticated HTTP bridge missing result"))
  }

  private func validateOutboundURLAndNSID(_ url: URL, permittingMailboxKeysDeletion: Bool) throws {
    if !allowLoopbackForTesting {
      try MLSOutboundHTTPValidator.validatePublicHTTPSServiceEndpoint(url)
    }
    if let components = URLComponents(url: url, resolvingAgainstBaseURL: false) {
      if let rawQuery = components.percentEncodedQuery, !rawQuery.isEmpty {
        _ = try MLSOutboundHTTPValidator.parsePercentEncodedQueryItems(rawQuery)
      }
      if components.path.hasPrefix("/xrpc/") {
        let nsid = String(components.path.dropFirst("/xrpc/".count))
        if !(permittingMailboxKeysDeletion && nsid == MLSOutboundHTTPValidator.deleteRecordNSID) {
          try MLSOutboundHTTPValidator.validatePreparedRequestNSID(nsid)
        }
      }
    }
  }
}

/// Centralized service-DID & endpoint routing for home `mls-ds` (`D1`, `D2`).
public enum MLSEnvironment: Sendable, Equatable {
  case production
  case custom(serviceDID: String)

  public var serviceDID: String {
    switch self {
    case .production:
      return "did:web:chat.catbird.blue#atproto_mls"
    case let .custom(did):
      return did
    }
  }

  /// Bare `did:web:...` identifier without `#fragment` as required by `mls-ds` `BareDid::parse` and `getServiceAuth(aud:)`.
  public var bareServiceDID: String {
    PetrelPlatformTransport.stripDidFragment(serviceDID)
  }

  public var endpointURL: URL {
    switch self {
    case .production:
      return URL(string: "https://chat.catbird.blue")!
    case let .custom(did):
      let bare = PetrelPlatformTransport.stripDidFragment(did)
      let parts = bare.split(separator: ":", maxSplits: 3)
      if parts.count >= 3, parts[0] == "did", parts[1] == "web" {
        let host = String(parts[2]).replacingOccurrences(of: "%3A", with: ":")
          .replacingOccurrences(of: "%3a", with: ":")
        if let url = URL(string: "https://\(host)") {
          return url
        }
      }
      return URL(string: "https://chat.catbird.blue")!
    }
  }
}

/// Direct XRPC client for home `mls-ds` (`D1`, `D2`).
/// Mints `com.atproto.server.getServiceAuth` against `environment.bareServiceDID` and sends requests
/// directly to `environment.endpointURL` (never proxying `blue.catbird.chat.*` through `nest`).
public final class MLSAPIClient: @unchecked Sendable {
  public let client: ATProtoClient
  public let environment: MLSEnvironment

  public init(client: ATProtoClient, environment: MLSEnvironment = .production) async {
    self.client = client
    self.environment = environment
  }

  public func authenticatedUserDID() async -> String? {
    try? await client.getDid()
  }

  private func mintServiceAuthToken(lxm: String) async throws -> String {
    let (code, output) = try await client.com.atproto.server.getServiceAuth(
      input: .init(
        aud: environment.bareServiceDID,
        exp: nil,
        lxm: try NSID(nsidString: lxm)
      )
    )
    guard (200..<300).contains(code), let token = output?.token, !token.isEmpty else {
      throw CatbirdMLS.MlsError.Internal(message: "getServiceAuth(\(lxm)) failed with HTTP \(code)")
    }
    return token
  }

  @discardableResult
  public func registerPushGateway(
    rawJsonData: Data
  ) async throws -> BlueCatbirdChatRegisterPushGateway.Output? {
    let token = try await mintServiceAuthToken(lxm: "blue.catbird.chat.registerPushGateway")
    var components = URLComponents(url: environment.endpointURL, resolvingAgainstBaseURL: false)
    components?.path = "/xrpc/blue.catbird.chat.registerPushGateway"
    guard let url = components?.url else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Invalid registerPushGateway URL")
    }
    var req = URLRequest(url: url)
    req.httpMethod = "POST"
    req.httpBody = rawJsonData
    req.timeoutInterval = 15.0
    req.setValue("application/json", forHTTPHeaderField: "Content-Type")
    req.setValue("application/json", forHTTPHeaderField: "Accept")
    req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

    let (data, http) = try await MLSBoundedURLSession.shared.data(
      for: req,
      maxBytes: MLSBoundedURLSession.pdsRecordMaxBytes
    )
    guard (200..<300).contains(http.statusCode) else {
      throw CatbirdMLS.MlsError.Internal(message: "registerPushGateway failed with HTTP \(http.statusCode)")
    }
    guard !data.isEmpty else { return nil }
    return try? JSONDecoder().decode(BlueCatbirdChatRegisterPushGateway.Output.self, from: data)
  }

  @discardableResult
  public func registerPushGateway(
    input: BlueCatbirdChatRegisterPushGateway.Input
  ) async throws -> BlueCatbirdChatRegisterPushGateway.Output? {
    let body = try JSONEncoder().encode(input)
    return try await registerPushGateway(rawJsonData: body)
  }

  public func getOwnDevices(
    actorDeviceId: String
  ) async throws -> BlueCatbirdChatGetOwnDevices.Output? {
    let token = try await mintServiceAuthToken(lxm: "blue.catbird.chat.getOwnDevices")
    var components = URLComponents(url: environment.endpointURL, resolvingAgainstBaseURL: false)
    components?.path = "/xrpc/blue.catbird.chat.getOwnDevices"
    components?.queryItems = [URLQueryItem(name: "actorDeviceId", value: actorDeviceId)]
    guard let url = components?.url else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Invalid getOwnDevices URL")
    }
    var req = URLRequest(url: url)
    req.httpMethod = "GET"
    req.timeoutInterval = 15.0
    req.setValue("application/json", forHTTPHeaderField: "Accept")
    req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

    let (data, http) = try await MLSBoundedURLSession.shared.data(
      for: req,
      maxBytes: MLSBoundedURLSession.pdsRecordMaxBytes
    )
    guard (200..<300).contains(http.statusCode) else {
      throw CatbirdMLS.MlsError.Internal(message: "getOwnDevices failed with HTTP \(http.statusCode)")
    }
    return try JSONDecoder().decode(BlueCatbirdChatGetOwnDevices.Output.self, from: data)
  }
}

private final class AsyncAuthBridgeBox<T>: @unchecked Sendable {
  private let lock = NSLock()
  let sema = DispatchSemaphore(value: 0)
  private var result: Result<T, Error>?

  init() {}

  func complete(_ value: Result<T, Error>) {
    lock.withLock {
      result = value
    }
    sema.signal()
  }

  func readResult() -> Result<T, Error>? {
    lock.withLock { result }
  }
}
