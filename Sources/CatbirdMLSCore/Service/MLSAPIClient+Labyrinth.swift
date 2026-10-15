//
//  MLSAPIClient+Labyrinth.swift
//  CatbirdMLSCore
//
//  Transport for the Rust orchestrator's Labyrinth history-backup callbacks
//  (catbird-mls feature `labyrinth`).
//
//  `blue.catbird.labyrinth.*` procedures carry the device-signed write body in
//  two custom headers (`catbird-labyrinth-write`, `...-signature`). A PDS
//  proxies `atproto-proxy` requests with a fixed header allowlist and drops
//  them, so these calls go straight to the delivery service with a
//  service-auth token minted by `com.atproto.server.getServiceAuth`, the same
//  token (aud = service DID, lxm = NSID) the PDS would mint when proxying.
//

import Foundation
import Petrel

extension MLSAPIClient {
  static let labyrinthNamespacePrefix = "blue.catbird.labyrinth."
  static let labyrinthMailboxKeysCollection = "blue.catbird.labyrinth.mailboxKeys"
  private static let labyrinthEndpointCacheLifetime: TimeInterval = 600
  private static let labyrinthPublicReadLimits = NetworkResponseLimits(
    maximumWireBytes: 1_048_576, maximumDecodedBytes: 1_048_576)

  /// Errors from the Labyrinth transport. The adapter maps them to
  /// `OrchestratorBridgeError.Api`.
  public enum LabyrinthTransportError: Error, LocalizedError, Equatable {
    case invalidRequest(String)
    case serviceUnresolved(String)
    case serviceAuthFailed(Int)
    case pdsFailure(String)

    public var errorDescription: String? {
      switch self {
      case let .invalidRequest(detail): return "labyrinth request rejected: \(detail)"
      case let .serviceUnresolved(detail): return "labyrinth service unresolved: \(detail)"
      case let .serviceAuthFailed(status): return "labyrinth service auth failed (status \(status))"
      case let .pdsFailure(detail): return "labyrinth PDS call failed: \(detail)"
      }
    }
  }

  /// `did:web:chat.catbird.blue#atproto_mls` → (`did:web:chat.catbird.blue`, `#atproto_mls`).
  static func splitServiceDID(_ serviceDID: String) -> (did: String, fragment: String) {
    guard let hash = serviceDID.firstIndex(of: "#") else { return (serviceDID, "#atproto_mls") }
    return (String(serviceDID[..<hash]), String(serviceDID[hash...]))
  }

  /// Validates what Rust asked for: only Labyrinth NSIDs, and a path that is
  /// exactly `/xrpc/{nsid}` plus an optional query string.
  static func validateLabyrinthRequest(method: String, nsid: String, path: String) throws {
    guard method == "GET" || method == "POST" else {
      throw LabyrinthTransportError.invalidRequest("method \(method)")
    }
    guard nsid.hasPrefix(labyrinthNamespacePrefix),
          nsid.count > labyrinthNamespacePrefix.count,
          nsid.allSatisfy({ $0.isASCII && ($0.isLetter || $0.isNumber || $0 == ".") })
    else {
      throw LabyrinthTransportError.invalidRequest("nsid \(nsid)")
    }
    let route = "/xrpc/\(nsid)"
    guard path == route || path.hasPrefix(route + "?") else {
      throw LabyrinthTransportError.invalidRequest("path does not match \(nsid)")
    }
  }

  /// The delivery service's HTTPS endpoint from its DID document.
  static func labyrinthServiceEndpoint(
    in document: DIDDocument, did: String, fragment: String
  ) throws -> URL {
    guard document.id == did else {
      throw LabyrinthTransportError.serviceUnresolved("DID document id mismatch")
    }
    let matches = document.service.filter { $0.id == fragment || $0.id == did + fragment }
    guard matches.count == 1, let service = matches.first,
          let endpoint = URL(string: service.serviceEndpoint),
          endpoint.scheme == "https", endpoint.host != nil,
          endpoint.user == nil, endpoint.password == nil, endpoint.fragment == nil
    else {
      throw LabyrinthTransportError.serviceUnresolved("no unique https \(fragment) service")
    }
    return endpoint
  }

  private func resolveLabyrinthServiceEndpoint() async throws -> URL {
    let serviceDID = mlsServiceDID
    if let cached = LabyrinthEndpointCache.shared.endpoint(for: serviceDID) { return cached }
    let (did, fragment) = Self.splitServiceDID(serviceDID)
    let document = try await SpaceHostResolver.fetchDIDDocument(
      for: did,
      limits: Self.labyrinthPublicReadLimits)
    let endpoint = try Self.labyrinthServiceEndpoint(in: document, did: did, fragment: fragment)
    LabyrinthEndpointCache.shared.store(
      endpoint, for: serviceDID, lifetime: Self.labyrinthEndpointCacheLifetime)
    return endpoint
  }

  /// Sends one `blue.catbird.labyrinth.*` XRPC to the delivery service and
  /// returns the raw response, including non-2xx statuses: Rust maps the
  /// atproto error names.
  public func submitLabyrinthRequest(
    method: String,
    nsid: String,
    path: String,
    body: Data?,
    headers: [(name: String, value: String)]
  ) async throws -> (data: Data, response: HTTPURLResponse) {
    try Self.validateLabyrinthRequest(method: method, nsid: nsid, path: path)
    let endpoint = try await resolveLabyrinthServiceEndpoint()
    guard let url = URL(string: path, relativeTo: endpoint)?.absoluteURL,
          url.host == endpoint.host
    else {
      throw LabyrinthTransportError.invalidRequest("path \(path)")
    }

    let (audience, _) = Self.splitServiceDID(mlsServiceDID)
    // No `exp`: the PDS then sets both `iat` and `exp` (iat + 60 s) from its own
    // clock, the most mls-ds accepts. An `exp` from this device's clock would
    // fail whenever the device runs fast.
    let (status, output) = try await client.com.atproto.server.getServiceAuth(
      input: .init(aud: audience, lxm: try NSID(nsidString: nsid)))
    guard (200...299).contains(status), let token = output?.token, !token.isEmpty else {
      throw LabyrinthTransportError.serviceAuthFailed(status)
    }

    var request = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData)
    request.httpMethod = method
    request.timeoutInterval = 30
    request.setValue("*/*", forHTTPHeaderField: "Accept")
    request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
    if let body {
      request.httpBody = body
      request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    }
    for header in headers {
      let name = header.name.lowercased()
      guard name.hasPrefix("catbird-labyrinth-") else {
        throw LabyrinthTransportError.invalidRequest("header \(header.name)")
      }
      request.setValue(header.value, forHTTPHeaderField: header.name)
    }

    let (data, response) = try await LabyrinthEndpointCache.session.data(for: request)
    guard let http = response as? HTTPURLResponse else {
      throw LabyrinthTransportError.invalidRequest("non-HTTP response")
    }
    return (data, http)
  }

  // MARK: - PDS mailboxKeys records

  /// `com.atproto.repo.putRecord` of a `blue.catbird.labyrinth.mailboxKeys`
  /// record (`$type` included) into the signed-in account's repo.
  public func putLabyrinthMailboxKeys(rkey: String, record: Data) async throws {
    guard let did = await authenticatedUserDID() else { throw MLSAPIError.noAuthentication }
    let value = try JSONSerialization.jsonObject(with: record)
    let body = try JSONSerialization.data(withJSONObject: [
      "repo": did, "collection": Self.labyrinthMailboxKeysCollection,
      "rkey": rkey, "record": value, "validate": false,
    ])
    let (data, response) = try await authenticatedRepoCall(
      method: "POST", endpoint: "com.atproto.repo.putRecord", body: body, query: nil)
    guard (200...299).contains(response.statusCode) else {
      throw LabyrinthTransportError.pdsFailure(
        "putRecord \(response.statusCode): \(String(decoding: data.prefix(512), as: UTF8.self))")
    }
  }

  /// `com.atproto.repo.getRecord` from `did`'s own PDS, unauthenticated.
  /// Returns the record value JSON, or nil when the record or repo is absent.
  /// The PDS URL comes from the peer's own DID document, so the read goes
  /// through Petrel's bounded public fetch (public addresses only, no
  /// redirects, 1 MiB cap).
  public func getLabyrinthMailboxKeys(did: String, rkey: String) async throws -> Data? {
    let pds: URL
    do {
      pds = try await MLSPublicPDSReader.resolveCurrentPDS(did: did)
    } catch let error where Self.isAbsentPDS(error) {
      return nil
    }
    guard let url = Self.mailboxKeysGetRecordURL(pds: pds, did: did, rkey: rkey) else {
      throw LabyrinthTransportError.pdsFailure("bad PDS URL")
    }
    let (data, http) = try await SpaceHostResolver.fetchBoundedPublicJSON(
      from: url, limits: Self.labyrinthPublicReadLimits,
      fixtureSession: MLSPublicPDSReader.authorizationSession)
    return try Self.mailboxKeysValue(fromGetRecord: data, status: http.statusCode)
  }

  /// The unauthenticated `getRecord` URL on `pds`, keeping any base path.
  static func mailboxKeysGetRecordURL(pds: URL, did: String, rkey: String) -> URL? {
    guard pds.scheme?.lowercased() == "https",
          var components = URLComponents(url: pds, resolvingAgainstBaseURL: false)
    else { return nil }
    var basePath = components.path
    if basePath.hasSuffix("/") { basePath.removeLast() }
    components.path = basePath + "/xrpc/com.atproto.repo.getRecord"
    components.queryItems = [
      URLQueryItem(name: "repo", value: did),
      URLQueryItem(name: "collection", value: labyrinthMailboxKeysCollection),
      URLQueryItem(name: "rkey", value: rkey),
    ]
    return components.url
  }

  /// True when resolving `did`'s PDS failed because there is none to read: the
  /// DID document is gone (404, or 410 for a PLC tombstone) or names no usable
  /// `#atproto_pds` service. Network and parse failures stay errors.
  static func isAbsentPDS(_ error: Error) -> Bool {
    if let reader = error as? MLSPublicPDSReader.ReaderError, case .invalidEndpoint = reader {
      return true
    }
    if let resolution = error as? SpaceHostResolutionError,
       case let .networkError(message) = resolution {
      return message == "HTTP status 404" || message == "HTTP status 410"
    }
    return false
  }

  /// Maps a `getRecord` response to the record value JSON (nil = absent).
  static func mailboxKeysValue(fromGetRecord data: Data, status: Int) throws -> Data? {
    switch status {
    case 200:
      let object = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
      guard let value = object?["value"] as? [String: Any] else {
        throw LabyrinthTransportError.pdsFailure("getRecord without a value")
      }
      return try JSONSerialization.data(withJSONObject: value)
    case 400, 404:
      // The query is fixed and well formed, so these mean the record or its
      // repo is not there. A PDS answers a deleted, deactivated or taken-down
      // repo with 400 InvalidRequest ("Could not locate record"), and the
      // AppView it pipes to with "Could not find repo". Rust then falls back
      // to the delivery service's directory for that member.
      return nil
    default:
      throw LabyrinthTransportError.pdsFailure("getRecord \(status)")
    }
  }

  /// Deletes every `blue.catbird.labyrinth.mailboxKeys` record in the
  /// signed-in account's repo (account deletion).
  public func deleteLabyrinthMailboxKeys() async throws {
    guard let did = await authenticatedUserDID() else { throw MLSAPIError.noAuthentication }
    var cursor: String?
    var rkeys: [String] = []
    repeat {
      var query = [
        URLQueryItem(name: "repo", value: did),
        URLQueryItem(name: "collection", value: Self.labyrinthMailboxKeysCollection),
        URLQueryItem(name: "limit", value: "100"),
      ]
      if let cursor { query.append(URLQueryItem(name: "cursor", value: cursor)) }
      let (data, response) = try await authenticatedRepoCall(
        method: "GET", endpoint: "com.atproto.repo.listRecords", body: nil, query: query)
      guard response.statusCode == 200,
            let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
      else {
        throw LabyrinthTransportError.pdsFailure("listRecords \(response.statusCode)")
      }
      let records = object["records"] as? [[String: Any]] ?? []
      rkeys += records.compactMap { ($0["uri"] as? String)?.split(separator: "/").last.map(String.init) }
      cursor = records.isEmpty ? nil : object["cursor"] as? String
    } while cursor != nil

    for rkey in rkeys {
      let body = try JSONSerialization.data(withJSONObject: [
        "repo": did, "collection": Self.labyrinthMailboxKeysCollection, "rkey": rkey,
      ])
      let (data, response) = try await authenticatedRepoCall(
        method: "POST", endpoint: "com.atproto.repo.deleteRecord", body: body, query: nil)
      guard (200...299).contains(response.statusCode) else {
        throw LabyrinthTransportError.pdsFailure(
          "deleteRecord \(response.statusCode): \(String(decoding: data.prefix(512), as: UTF8.self))")
      }
    }
  }

  /// An authenticated call to the signed-in account's own PDS (no proxy).
  private func authenticatedRepoCall(
    method: String, endpoint: String, body: Data?, query: [URLQueryItem]?
  ) async throws -> (Data, HTTPURLResponse) {
    var headers = ["Accept": "application/json"]
    if body != nil { headers["Content-Type"] = "application/json" }
    let request = try await client.networkService.createURLRequest(
      endpoint: endpoint, method: method, headers: headers, body: body, queryItems: query)
    return try await client.networkService.performRequestReturningHTTPErrorResponses(
      request, skipTokenRefresh: false)
  }
}

/// Process-wide cache of the delivery service endpoint, and the redirect-free
/// session the Labyrinth transport uses.
final class LabyrinthEndpointCache: @unchecked Sendable {
  static let shared = LabyrinthEndpointCache()

  static let session: URLSession = {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.httpCookieStorage = nil
    configuration.urlCredentialStorage = nil
    configuration.httpShouldSetCookies = false
    configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
    configuration.timeoutIntervalForResource = LabyrinthCallbackGate.deadline
    return URLSession(configuration: configuration, delegate: NoRedirects(), delegateQueue: nil)
  }()

  private final class NoRedirects: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(
      _ session: URLSession, task: URLSessionTask,
      willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest,
      completionHandler: @escaping (URLRequest?) -> Void
    ) {
      completionHandler(nil)
    }
  }

  private let lock = NSLock()
  private var entries: [String: (endpoint: URL, expires: Date)] = [:]

  func endpoint(for serviceDID: String) -> URL? {
    lock.lock()
    defer { lock.unlock() }
    guard let entry = entries[serviceDID], entry.expires > Date() else { return nil }
    return entry.endpoint
  }

  func store(_ endpoint: URL, for serviceDID: String, lifetime: TimeInterval) {
    lock.lock()
    defer { lock.unlock() }
    entries[serviceDID] = (endpoint, Date().addingTimeInterval(lifetime))
  }
}
