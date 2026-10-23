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
  /// Only the explicit local framework produced by the verified rebuild
  /// enables Spaces. The older public binary remains a release gate.
  static var labyrinthSpaceTransportAvailable: Bool {
    #if CATBIRD_MLS_VERIFIED_SPACE_FFI
    true
    #else
    false
    #endif
  }
  static let labyrinthNamespacePrefix = "blue.catbird.labyrinth."
  static let labyrinthMailboxKeysCollection = "blue.catbird.labyrinth.mailboxKeys"
  /// mls-ds rejects service-auth tokens living longer than 60 s (exp - iat).
  /// `exp` uses this device's clock and `iat` the PDS's, so leave margin.
  private static let labyrinthServiceAuthLifetime: TimeInterval = 50
  private static let labyrinthEndpointCacheLifetime: TimeInterval = 600

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
      limits: NetworkResponseLimits(maximumWireBytes: 1_048_576, maximumDecodedBytes: 1_048_576))
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
    let expiry = Int(Date().addingTimeInterval(Self.labyrinthServiceAuthLifetime).timeIntervalSince1970)
    let (status, output) = try await client.com.atproto.server.getServiceAuth(
      input: .init(aud: audience, exp: expiry, lxm: try NSID(nsidString: nsid)))
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

  /// Uses owner OAuth for this account's records and verified Space
  /// credentials for a foreign sender's conversation ciphertext.
  public func submitLabyrinthSpaceRequest(
    method: String, nsid: String, path: String, body: Data?
  ) async throws -> (data: Data, response: HTTPURLResponse) {
    guard Self.labyrinthSpaceTransportAvailable else {
      throw LabyrinthTransportError.invalidRequest("this native artifact does not enable Space history")
    }
    guard let did = await authenticatedUserDID() else { throw MLSAPIError.noAuthentication }
    if nsid == "com.atproto.space.listSpaces" {
      let query = try Self.validateConversationSpaceInventory(method: method, path: path, body: body)
      return try await authenticatedRepoCall(method: method, endpoint: nsid, body: nil, query: query)
    }
    if let conversation = try Self.conversationSpaceRequest(
      did: did, method: method, nsid: nsid, path: path, body: body) {
      if method == "GET", conversation.repo != did {
        // Petrel verifies the foreign record's fresh response MAC, URI and
        // DRISL CID. Rust then binds that CID to the DS sequence reference.
        let pds = try await MLSPublicPDSReader.resolveCurrentPDS(did: conversation.repo)
        guard var url = URLComponents(
          url: pds.appendingPathComponent("xrpc/\(nsid)"), resolvingAgainstBaseURL: false) else {
          throw LabyrinthTransportError.invalidRequest("invalid conversation repo host")
        }
        url.queryItems = conversation.query
        guard let target = url.url else {
          throw LabyrinthTransportError.invalidRequest("invalid conversation record URL")
        }
        let credentials = SpaceCredentialManager(client: client, authorityHostProvider: { authority in
          let doc = try await SpaceHostResolver.fetchDIDDocument(for: authority)
          guard doc.id == authority else {
            throw LabyrinthTransportError.serviceUnresolved("authority DID mismatch")
          }
          return try SpaceAuthorityEndpoints.extract(from: doc).spaceHost
        })
        return try await credentials.get(url: target, space: conversation.space, audience: conversation.repo)
      }
      return try await authenticatedRepoCall(
        method: method, endpoint: nsid, body: body, query: conversation.query)
    }
    let query = try Self.validateLabyrinthSpaceRequest(
      did: did, method: method, nsid: nsid, path: path, body: body)
    return try await authenticatedRepoCall(method: method, endpoint: nsid, body: body, query: query)
  }

  struct ConversationSpaceRequest {
    let space: SpaceRef
    let repo: String
    let query: [URLQueryItem]?
  }

  static func validateConversationSpaceInventory(
    method: String, path: String, body: Data?
  ) throws -> [URLQueryItem] {
    guard method == "GET", body == nil, let url = URLComponents(string: path),
          url.scheme == nil, url.host == nil, url.fragment == nil,
          url.percentEncodedPath == "/xrpc/com.atproto.space.listSpaces" else {
      throw LabyrinthTransportError.invalidRequest("invalid conversation inventory route")
    }
    var params: [String: String] = [:]
    for item in url.queryItems ?? [] {
      guard let value = item.value, params.updateValue(value, forKey: item.name) == nil else {
        throw LabyrinthTransportError.invalidRequest("duplicate conversation inventory parameter")
      }
    }
    guard params["spaceType"] == "blue.catbird.labyrinth.conversation",
          Set(params.keys).isSubset(of: ["spaceType", "limit", "cursor"]) else {
      throw LabyrinthTransportError.invalidRequest("conversation inventory is not scoped")
    }
    return url.queryItems ?? []
  }

  /// An exact conversation-record capability. Space placement and generation
  /// have already been authenticated and pinned by the Rust sequencer client.
  static func conversationSpaceRequest(
    did: String, method: String, nsid: String, path: String, body: Data?
  ) throws -> ConversationSpaceRequest? {
    guard let url = URLComponents(string: path), url.scheme == nil, url.host == nil,
          url.fragment == nil, url.percentEncodedPath == "/xrpc/\(nsid)" else {
      throw LabyrinthTransportError.invalidRequest("invalid Space operation route")
    }
    let params: [String: Any]
    if method == "GET" {
      guard body == nil else { throw LabyrinthTransportError.invalidRequest("Space query body") }
      var values: [String: String] = [:]
      for item in url.queryItems ?? [] {
        guard let value = item.value, values.updateValue(value, forKey: item.name) == nil else {
          throw LabyrinthTransportError.invalidRequest("duplicate Space query parameter")
        }
      }
      params = values
    } else {
      guard method == "POST", url.query == nil, let body,
            let value = try JSONSerialization.jsonObject(with: body) as? [String: Any] else {
        throw LabyrinthTransportError.invalidRequest("invalid Space write")
      }
      params = value
    }
    guard let ref = params["space"] as? String,
          let space = try? SpaceRef(uriString: ref) else {
      throw LabyrinthTransportError.invalidRequest("invalid Space reference")
    }
    guard space.spaceType == "blue.catbird.labyrinth.conversation" else { return nil }
    guard let generation = UUID(uuidString: space.skey),
          generation.uuidString.lowercased() == space.skey,
          let repo = params["repo"] as? String, (try? DID(didString: repo)) != nil else {
      throw LabyrinthTransportError.invalidRequest("invalid conversation generation or repo")
    }
    if method == "GET" {
      guard params["collection"] as? String == "blue.catbird.labyrinth.batch" else {
        throw LabyrinthTransportError.invalidRequest("invalid conversation record read")
      }
      if nsid == "com.atproto.space.getRecord" {
        guard let rkey = params["rkey"] as? String, (try? RecordKey(keyString: rkey)) != nil,
              Set(params.keys).isSubset(of: ["space", "repo", "collection", "rkey"]) else {
          throw LabyrinthTransportError.invalidRequest("invalid conversation record key")
        }
      } else {
        guard nsid == "com.atproto.space.listRecords", repo == did,
              Set(params.keys).isSubset(of: ["space", "repo", "collection", "limit", "excludeValues"]) else {
          throw LabyrinthTransportError.invalidRequest("conversation cleanup may list only owned records")
        }
      }
    } else {
      guard repo == did else {
        throw LabyrinthTransportError.invalidRequest("conversation write is not owned by this account")
      }
      if nsid == "com.atproto.space.deleteRecord" {
        guard params["collection"] as? String == "blue.catbird.labyrinth.batch",
              let rkey = params["rkey"] as? String, (try? RecordKey(keyString: rkey)) != nil else {
          throw LabyrinthTransportError.invalidRequest("invalid owned conversation record deletion")
        }
      } else {
        guard nsid == "com.atproto.space.applyWrites",
              let writes = params["writes"] as? [[String: Any]], writes.count == 1,
            let write = writes.first,
            write["$type"] as? String == "com.atproto.space.applyWrites#create",
            write["collection"] as? String == "blue.catbird.labyrinth.batch",
            let rkey = write["rkey"] as? String, (try? RecordKey(keyString: rkey)) != nil else {
          throw LabyrinthTransportError.invalidRequest("invalid immutable conversation write")
        }
      }
    }
    return ConversationSpaceRequest(space: space, repo: repo, query: method == "GET" ? url.queryItems : nil)
  }

  /// The native callback is an account-bound mailbox capability, not a
  /// general authenticated Space client. In particular a foreign repo or a
  /// conversation Space must use the separately verified conversation path.
  static func validateLabyrinthSpaceRequest(
    did: String, method: String, nsid: String, path: String, body: Data?
  ) throws -> [URLQueryItem]? {
    let methods = [
      "com.atproto.space.getRecord": "GET",
      "com.atproto.space.listRecords": "GET",
      "com.atproto.space.applyWrites": "POST",
      "com.atproto.space.deleteRecord": "POST",
    ]
    guard methods[nsid] == method,
          let components = URLComponents(string: path),
          components.scheme == nil, components.host == nil, components.fragment == nil,
          components.percentEncodedPath == "/xrpc/\(nsid)" else {
      throw LabyrinthTransportError.invalidRequest("unsupported personal Space operation")
    }
    let parameters: [String: Any]
    if method == "GET" {
      guard body == nil else {
        throw LabyrinthTransportError.invalidRequest("Space query has a body")
      }
      var values: [String: String] = [:]
      for item in components.queryItems ?? [] {
        guard let value = item.value, values.updateValue(value, forKey: item.name) == nil else {
          throw LabyrinthTransportError.invalidRequest("duplicate or empty Space query parameter")
        }
      }
      parameters = values
    } else {
      guard components.query == nil, let body,
            let object = try JSONSerialization.jsonObject(with: body) as? [String: Any] else {
        throw LabyrinthTransportError.invalidRequest("invalid Space write body")
      }
      parameters = object
    }
    guard parameters["repo"] as? String == did,
          parameters["space"] as? String == "at://\(did)/space/blue.catbird.labyrinth.mailbox/self" else {
      throw LabyrinthTransportError.invalidRequest("Space operation is outside the account's mailbox")
    }
    let collections: Set<String> = [
      "blue.catbird.labyrinth.device", "blue.catbird.labyrinth.epoch",
      "blue.catbird.labyrinth.epochMember", "blue.catbird.labyrinth.virtualDevice",
      "blue.catbird.labyrinth.ownerBatch",
    ]
    if nsid == "com.atproto.space.applyWrites" {
      guard let writes = parameters["writes"] as? [[String: Any]],
            !writes.isEmpty, writes.count <= 200,
            writes.allSatisfy({ write in
              guard let collection = write["collection"] as? String,
                    let type = write["$type"] as? String else { return false }
              return collections.contains(collection) && [
                "com.atproto.space.applyWrites#create", "com.atproto.space.applyWrites#update",
                "com.atproto.space.applyWrites#delete",
              ].contains(type)
            }) else {
        throw LabyrinthTransportError.invalidRequest("invalid mailbox writes")
      }
    } else {
      guard let collection = parameters["collection"] as? String,
            collections.contains(collection) else {
        throw LabyrinthTransportError.invalidRequest("invalid mailbox collection")
      }
    }
    return method == "GET" ? components.queryItems : nil
  }

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
  public func getLabyrinthMailboxKeys(did: String, rkey: String) async throws -> Data? {
    let pds = try await MLSPublicPDSReader.resolveCurrentPDS(did: did)
    guard var components = URLComponents(
      url: pds.appendingPathComponent("xrpc/com.atproto.repo.getRecord"),
      resolvingAgainstBaseURL: false)
    else {
      throw LabyrinthTransportError.pdsFailure("bad PDS URL")
    }
    components.queryItems = [
      URLQueryItem(name: "repo", value: did),
      URLQueryItem(name: "collection", value: Self.labyrinthMailboxKeysCollection),
      URLQueryItem(name: "rkey", value: rkey),
    ]
    guard let url = components.url else { throw LabyrinthTransportError.pdsFailure("bad query") }
    var request = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData)
    request.setValue("application/json", forHTTPHeaderField: "Accept")
    request.timeoutInterval = 15
    let (data, response) = try await MLSPublicPDSReader.authorizationSession.data(for: request)
    guard let http = response as? HTTPURLResponse else {
      throw LabyrinthTransportError.pdsFailure("non-HTTP response")
    }
    return try Self.mailboxKeysValue(fromGetRecord: data, status: http.statusCode)
  }

  /// Maps a `getRecord` response to the record value JSON (nil = absent).
  static func mailboxKeysValue(fromGetRecord data: Data, status: Int) throws -> Data? {
    let object = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
    switch status {
    case 200:
      guard let value = object?["value"] as? [String: Any] else {
        throw LabyrinthTransportError.pdsFailure("getRecord without a value")
      }
      return try JSONSerialization.data(withJSONObject: value)
    case 400, 404:
      let error = object?["error"] as? String
      if error == "RecordNotFound" || error == "RepoNotFound" || (status == 404 && error == nil) {
        return nil
      }
      throw LabyrinthTransportError.pdsFailure("getRecord \(status) \(error ?? "")")
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
