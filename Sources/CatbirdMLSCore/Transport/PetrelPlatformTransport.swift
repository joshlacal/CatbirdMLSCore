import CatbirdMLS
import Foundation
import Petrel
import PetrelCatbird

private struct PushGetEntriesWireResponse: Decodable {
  struct WireEntry: Decodable {
    let entryId: String?
    let messageId: String?
    let seq: Int64
    let epoch: Int64
    let entryKind: String
    let senderDid: String?
    let mlsBytes: Data?
    let ciphertext: Data?
  }

  struct WireMetadataSnapshot: Decodable {
    let metadataVersion: Int64
    let epoch: Int64
    let nonce: Data?
    let ciphertext: Data
  }

  let entries: [WireEntry]
  let metadataSnapshot: WireMetadataSnapshot?
}

/// Thin network transport implementing `PlatformTransport` and `PushFetchTransport` (`Spec §6.3`, `D1`, `Gate 7`, `A-3`, `A-5`, `I10`).
///
/// Preserves `Atproto-Space-Mac` and `Atproto-Space-Credential` headers verbatim across FFI,
/// blocks all `3xx` redirects, enforces SSRF/NSID checks, and never refreshes OAuth inside NSE (`I10`).
public final class PetrelPlatformTransport: PlatformTransport, PushFetchTransport, @unchecked Sendable {
  public static let maxHttpResponseBytes = 16 * 1024 * 1024

  private let atProtoClient: ATProtoClient?
  private let homeDsEndpoint: URL?
  private let boundedSession: MLSBoundedURLSession
  private let allowLoopbackForTesting: Bool
  private let httpOverride: (@Sendable (PlatformHttpRequest) throws -> PlatformHttpResponse)?
  private let lock = NSLock()
  private var webSocketTask: URLSessionWebSocketTask?
  private var webSocketSession: URLSession?

  public init(
    atProtoClient: ATProtoClient? = nil,
    homeDsEndpoint: URL? = URL(string: "https://chat.catbird.blue"),
    allowLoopbackForTesting: Bool = false,
    httpOverride: (@Sendable (PlatformHttpRequest) throws -> PlatformHttpResponse)? = nil
  ) {
    self.atProtoClient = atProtoClient
    self.homeDsEndpoint = homeDsEndpoint
    self.boundedSession = .shared
    self.allowLoopbackForTesting = allowLoopbackForTesting
    self.httpOverride = httpOverride
  }

  init(
    atProtoClient: ATProtoClient? = nil,
    homeDsEndpoint: URL? = URL(string: "https://chat.catbird.blue"),
    boundedSession: MLSBoundedURLSession,
    allowLoopbackForTesting: Bool = false,
    httpOverride: (@Sendable (PlatformHttpRequest) throws -> PlatformHttpResponse)? = nil
  ) {
    self.atProtoClient = atProtoClient
    self.homeDsEndpoint = homeDsEndpoint
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
    try validateOutboundURLAndNSID(url)

    var urlRequest = URLRequest(url: url)
    urlRequest.httpMethod = request.method
    urlRequest.httpBody = request.body
    urlRequest.timeoutInterval = 20.0
    for header in request.headers {
      urlRequest.setValue(header.value, forHTTPHeaderField: header.name)
    }

    do {
      let (data, http) = try self.boundedSession.performSync(
        for: urlRequest,
        maxBytes: Self.maxHttpResponseBytes,
        timeoutSeconds: 25.0
      )
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
    guard !chatDpopToken.isEmpty else {
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
    components?.queryItems = [
      URLQueryItem(name: "conversationId", value: conversationId),
      URLQueryItem(name: "afterSeq", value: String(afterSeq)),
    ]
    guard let url = components?.url else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Malformed getEntries URL")
    }

    var req = URLRequest(url: url)
    req.httpMethod = "GET"
    req.timeoutInterval = 1.5
    req.setValue("Bearer \(chatDpopToken)", forHTTPHeaderField: "Authorization")

    do {
      let (data, http) = try self.boundedSession.performSync(
        for: req,
        maxBytes: MLSBoundedURLSession.labyrinthMaxBytes,
        timeoutSeconds: 1.6
      )
      guard (200..<300).contains(http.statusCode) else {
        throw CatbirdMLS.MlsError.Internal(message: "Push getEntries returned HTTP \(http.statusCode)")
      }
      let decoded = try JSONDecoder().decode(PushGetEntriesWireResponse.self, from: data)
      let entries = decoded.entries.map {
        FetchedPushEntry(
          entryId: $0.entryId ?? $0.messageId ?? "entry-\($0.seq)",
          seq: $0.seq,
          epoch: $0.epoch,
          entryKind: $0.entryKind,
          senderDid: $0.senderDid ?? "",
          mlsBytes: $0.mlsBytes ?? $0.ciphertext ?? Data()
        )
      }
      let snapshot = decoded.metadataSnapshot.map {
        FetchedMetadataSnapshot(
          epoch: $0.epoch,
          metadataVersion: $0.metadataVersion,
          nonce: $0.nonce ?? Data(repeating: 0, count: 12),
          ciphertext: $0.ciphertext
        )
      }
      return PushFetchBundle(
        entries: entries,
        metadataSnapshot: snapshot,
        authorizedDeviceKeys: []
      )
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

  private func validateOutboundURLAndNSID(_ url: URL) throws {
    if !allowLoopbackForTesting {
      try MLSOutboundHTTPValidator.validatePublicHTTPSServiceEndpoint(url)
    }
    if let components = URLComponents(url: url, resolvingAgainstBaseURL: false) {
      if let rawQuery = components.percentEncodedQuery, !rawQuery.isEmpty {
        _ = try MLSOutboundHTTPValidator.parsePercentEncodedQueryItems(rawQuery)
      }
      if components.path.hasPrefix("/xrpc/") {
        let nsid = String(components.path.dropFirst("/xrpc/".count))
        try MLSOutboundHTTPValidator.validatePreparedRequestNSID(nsid)
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

  public var endpointURL: URL {
    switch self {
    case .production:
      return URL(string: "https://chat.catbird.blue")!
    case let .custom(did):
      let bare = did.split(separator: "#", maxSplits: 1).first.map(String.init) ?? did
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

/// Stateless XRPC service-DID router for home `mls-ds` push-gateway registration (`D2`).
/// All MLS group state, storage, and encryption live exclusively in Rust `CatbirdMlsClient`.
public final class MLSAPIClient: @unchecked Sendable {
  public let client: ATProtoClient
  public let environment: MLSEnvironment

  public init(client: ATProtoClient, environment: MLSEnvironment = .production) async {
    self.client = client
    self.environment = environment
    await client.setServiceDID(environment.serviceDID, for: "blue.catbird.chat")
  }

  public func authenticatedUserDID() async -> String? {
    try? await client.getDid()
  }

  @discardableResult
  public func registerPushGateway(
    input: BlueCatbirdChatRegisterPushGateway.Input
  ) async throws -> BlueCatbirdChatRegisterPushGateway.Output? {
    await client.setServiceDID(environment.serviceDID, for: "blue.catbird.chat")
    let (code, output) = try await client.blue.catbird.chat.registerPushGateway(input: input)
    guard (200..<300).contains(code) else {
      throw CatbirdMLS.MlsError.Internal(message: "registerPushGateway failed with HTTP \(code)")
    }
    return output
  }
}
