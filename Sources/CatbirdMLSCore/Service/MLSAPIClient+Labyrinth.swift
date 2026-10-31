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
          (try? MLSOutboundHTTPValidator.validatePreparedRequestNSID(nsid)) != nil
    else {
      throw LabyrinthTransportError.invalidRequest("nsid \(nsid)")
    }
    guard !path.utf8.contains(where: {
      $0 <= 0x20 || $0 >= 0x7F || $0 == UInt8(ascii: "#") || $0 == UInt8(ascii: "\\")
    }) else {
      throw LabyrinthTransportError.invalidRequest("path \(path)")
    }
    let route = "/xrpc/\(nsid)"
    guard path == route || path.hasPrefix(route + "?") else {
      throw LabyrinthTransportError.invalidRequest("path does not match \(nsid)")
    }
    if path.hasPrefix(route + "?") {
      let queryPart = String(path.dropFirst((route + "?").count))
      guard (try? MLSOutboundHTTPValidator.parsePercentEncodedQueryItems(queryPart)) != nil else {
        throw LabyrinthTransportError.invalidRequest("query in path \(path)")
      }
    }
  }

  /// Builds a validated Labyrinth XRPC URL on `endpoint`, preserving any base path on `endpoint`
  /// and rejecting SSRF targets, `#` fragments, `..` traversal, and control characters.
  static func buildLabyrinthURL(endpoint: URL, nsid: String, path: String) throws -> URL {
    do {
      try MLSOutboundHTTPValidator.validatePublicHTTPSServiceEndpoint(endpoint)
    } catch {
      throw LabyrinthTransportError.serviceUnresolved(error.localizedDescription)
    }
    guard nsid.hasPrefix(labyrinthNamespacePrefix),
          nsid.count > labyrinthNamespacePrefix.count,
          (try? MLSOutboundHTTPValidator.validatePreparedRequestNSID(nsid)) != nil
    else {
      throw LabyrinthTransportError.invalidRequest("nsid \(nsid)")
    }
    guard !path.utf8.contains(where: {
      $0 <= 0x20 || $0 >= 0x7F || $0 == UInt8(ascii: "#") || $0 == UInt8(ascii: "\\")
    }) else {
      throw LabyrinthTransportError.invalidRequest("path \(path)")
    }

    let route = "/xrpc/\(nsid)"
    guard path == route || path.hasPrefix(route + "?"),
          let pathComponents = URLComponents(string: path),
          pathComponents.scheme == nil,
          pathComponents.host == nil,
          pathComponents.user == nil,
          pathComponents.password == nil,
          pathComponents.port == nil,
          pathComponents.fragment == nil,
          pathComponents.percentEncodedFragment == nil,
          pathComponents.percentEncodedPath == route,
          pathComponents.path == route
    else {
      throw LabyrinthTransportError.invalidRequest("path \(path)")
    }

    if let rawQuery = pathComponents.percentEncodedQuery {
      guard (try? MLSOutboundHTTPValidator.parsePercentEncodedQueryItems(rawQuery)) != nil else {
        throw LabyrinthTransportError.invalidRequest("query in path \(path)")
      }
    }

    guard var endpointComponents = URLComponents(url: endpoint, resolvingAgainstBaseURL: false) else {
      throw LabyrinthTransportError.invalidRequest("endpoint")
    }
    var basePath = endpointComponents.percentEncodedPath
    while basePath.hasSuffix("/") {
      basePath.removeLast()
    }
    endpointComponents.percentEncodedPath = basePath + route
    endpointComponents.percentEncodedQuery = pathComponents.percentEncodedQuery
    endpointComponents.fragment = nil

    guard let builtURL = endpointComponents.url,
          builtURL.scheme?.lowercased() == "https",
          builtURL.host?.lowercased() == endpoint.host?.lowercased(),
          builtURL.port == endpoint.port,
          builtURL.fragment == nil
    else {
      throw LabyrinthTransportError.invalidRequest("path \(path)")
    }
    return builtURL
  }

  /// Applies strictly validated `catbird-labyrinth-*` headers, rejecting CR/LF/NUL injection,
  /// non-token characters, and `Host` / `Authorization` / `Atproto-Proxy` overrides.
  static func applyValidatedLabyrinthHeaders(
    _ headers: [(name: String, value: String)],
    to request: inout URLRequest
  ) throws {
    let requiredPrefix = "catbird-labyrinth-"
    for header in headers {
      guard !header.name.utf8.contains(where: { $0 <= 0x20 || $0 >= 0x7F }),
            !header.value.utf8.contains(where: {
              $0 == 0x0D || $0 == 0x0A || $0 == 0x00 || ($0 < 0x20 && $0 != 0x09) || $0 == 0x7F
            })
      else {
        throw LabyrinthTransportError.invalidRequest("header \(header.name)")
      }
      let lowerName = header.name.lowercased()
      guard lowerName != "host",
            lowerName != "authorization",
            lowerName != "atproto-proxy",
            lowerName.hasPrefix(requiredPrefix),
            lowerName.count > requiredPrefix.count,
            lowerName.utf8.allSatisfy({
              ($0 >= 0x61 && $0 <= 0x7A) || ($0 >= 0x30 && $0 <= 0x39) || $0 == 0x2D
            })
      else {
        throw LabyrinthTransportError.invalidRequest("header \(header.name)")
      }
      request.setValue(header.value, forHTTPHeaderField: header.name)
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
          MLSOutboundHTTPValidator.isValidPublicHTTPSServiceEndpoint(endpoint)
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
    let url = try Self.buildLabyrinthURL(endpoint: endpoint, nsid: nsid, path: path)

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
    try Self.applyValidatedLabyrinthHeaders(headers, to: &request)

    return try await MLSBoundedURLSession.shared.data(
      for: request,
      maxBytes: MLSBoundedURLSession.labyrinthMaxBytes
    )
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
    guard let did = await authenticatedUserDID() else {
      clearLabyrinthSpaceCredentials()
      throw MLSAPIError.noAuthentication
    }
    if nsid == "com.atproto.space.listSpaces" {
      let query = try Self.validateConversationSpaceInventory(method: method, path: path, body: body)
      return try await authenticatedRepoCall(method: method, endpoint: nsid, body: nil, query: query)
    }
    if let conversation = try Self.conversationSpaceRequest(
      did: did, method: method, nsid: nsid, path: path, body: body) {
      if method == "GET", conversation.repo != did {
        // Petrel verifies the foreign record's fresh response MAC, URI and
        // DRISL CID. Rust then binds that CID to the DS sequence reference.
        let target = try await labyrinthForeignRecordURL(nsid: nsid, conversation: conversation)
        let credentials = labyrinthSpaceCredentialManager(for: did)
        do {
          return try await credentials.get(url: target, space: conversation.space, audience: conversation.repo)
        } catch let error as SpaceCredentialError {
          return try Self.labyrinthSpaceCredentialResponse(for: error, url: target)
        }
      }
      return try await authenticatedRepoCall(
        method: method, endpoint: nsid, body: body, query: conversation.query)
    }
    let query = try Self.validateLabyrinthSpaceRequest(
      did: did, method: method, nsid: nsid, path: path, body: body)
    return try await authenticatedRepoCall(method: method, endpoint: nsid, body: body, query: query)
  }

  /// The URL of a foreign conversation record on the repo's current PDS.
  /// The PDS and the author KEM key (labyrinthSpaceCredentialManager) come
  /// from one cached DID document, so a read fetches it once rather than twice.
  func labyrinthForeignRecordURL(
    nsid: String, conversation: ConversationSpaceRequest
  ) async throws -> URL {
    let document = try await labyrinthDIDDocuments.document(for: conversation.repo)
    let pds = try MLSPublicPDSReader.currentPDS(in: document, did: conversation.repo)
    guard var url = URLComponents(
      url: pds.appendingPathComponent("xrpc/\(nsid)"), resolvingAgainstBaseURL: false) else {
      throw LabyrinthTransportError.invalidRequest("invalid conversation repo host")
    }
    url.queryItems = conversation.query
    guard let target = url.url else {
      throw LabyrinthTransportError.invalidRequest("invalid conversation record URL")
    }
    return target
  }

  /// The Space credential manager of signed-in account `did`. Petrel caches
  /// credentials (and their keys) on the manager until they near expiry, so
  /// one manager is kept per account instead of exchanging a fresh credential
  /// for every foreign record read. Another account replaces it.
  func labyrinthSpaceCredentialManager(for did: String) -> SpaceCredentialManager {
    labyrinthSpaceCredentials.manager(for: did) { [client, labyrinthDIDDocuments] in
      SpaceCredentialManager(
        client: client,
        authorityHostProvider: { authority in
          let doc = try await SpaceHostResolver.fetchDIDDocument(for: authority)
          guard doc.id == authority else {
            throw LabyrinthTransportError.serviceUnresolved("authority DID mismatch")
          }
          return try SpaceAuthorityEndpoints.extract(from: doc).spaceHost
        },
        delegationTokenProvider: { space in
          // The cache is keyed by account: never cache a credential that the
          // client minted after switching to another account.
          guard try await client.getDid() == did else { throw MLSAPIError.noAuthentication }
          let (_, output) = try await client.com.atproto.space.getDelegationToken(input: .init(space: space))
          guard let token = output?.token else { throw SpaceCredentialError.missingDelegationToken }
          guard try await client.getDid() == did else { throw MLSAPIError.noAuthentication }
          return token
        },
        authorKEMKeyProvider: { repo, forceRefresh in
          try await Self.labyrinthAuthorKEMKey(
            repo: repo, forceRefresh: forceRefresh, documents: labyrinthDIDDocuments)
        })
    }
  }

  /// The author KEM key for a foreign conversation read, from the DID
  /// document its PDS was resolved from. Petrel passes `forceRefresh` on its
  /// retry after a response MAC mismatch; that always refetches, so a rotated
  /// KEM key is still picked up.
  static func labyrinthAuthorKEMKey(
    repo: String, forceRefresh: Bool, documents: LabyrinthDIDDocumentCache
  ) async throws -> String {
    let document = try await documents.document(for: repo, forceRefresh: forceRefresh)
    return try SpaceHostResolver.authorKEMKey(from: document, expectedDID: repo)
  }

  /// Drops the cached Space credentials (logout, account or environment switch).
  public func clearLabyrinthSpaceCredentials() {
    labyrinthSpaceCredentials.clear()
    labyrinthDIDDocuments.clear()
  }

  /// A credential failure as the raw response the catbird-mls callback
  /// contract asks for: a non-2xx status and an atproto `{"error","message"}`
  /// body. Labyrinth maps `SpaceDeleted` and 403 to its terminal `Forbidden`;
  /// as an untyped bridge error it saw only status 0 and no error name.
  static func labyrinthSpaceCredentialResponse(
    for error: SpaceCredentialError, url: URL
  ) throws -> (data: Data, response: HTTPURLResponse) {
    let (status, name, message) = labyrinthSpaceCredentialOutcome(error)
    var body = ["message": message ?? error.errorDescription ?? "Space credential exchange failed"]
    if let name, !name.isEmpty,
       name.allSatisfy({ $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "_") }) {
      body["error"] = name
    }
    guard let response = HTTPURLResponse(
      url: url, statusCode: status, httpVersion: "HTTP/1.1",
      headerFields: ["Content-Type": "application/json"]) else { throw error }
    return (try JSONSerialization.data(withJSONObject: body, options: [.sortedKeys]), response)
  }

  /// The authority's status, error name and message for each Petrel
  /// credential failure. Petrel keeps the error name but drops the status of
  /// refusals, so those use the closest authority answer.
  static func labyrinthSpaceCredentialOutcome(
    _ error: SpaceCredentialError
  ) -> (status: Int, name: String?, message: String?) {
    switch error {
    case let .spaceDeleted(_, message):
      // Upstream and Swan: 400 SpaceDeleted, the renewal deletion signal.
      return (400, "SpaceDeleted", message)
    case let .authorizationRefused(_, name, message) where name == "SpaceNotFound":
      // Petrel's form of the authority's SpaceNotFound (400 upstream, 404 in
      // Swan). Since the October 29 Spaces release the reference authority
      // answers SpaceNotFound to a user its policy does not authorize, so on
      // the credential path it means "not found or not authorized". The DS
      // pins this conversation Space, so treat it as a read refusal: 403
      // without the name, which labyrinth maps to Forbidden (owner-archive
      // fallback) instead of NotFound("space"). The name stays in the message.
      let detail = message ?? error.errorDescription ?? "Space not found or not authorized"
      return (403, nil, "\(name): \(detail)")
    case let .authorizationRefused(_, name, message):
      // This reader was refused by name (UserNotAuthorized, AppNotAuthorized
      // or NotAuthorized; other authorities may still send them), or any 403:
      // labyrinth's read refusal, on which history degrades.
      return (403, name, message)
    case let .tokenRejected(_, name, message, _):
      // InvalidDelegationToken or InvalidClientAttestation (400 upstream and
      // in Swan). Not an access denial: the next read mints a new token.
      return (400, name, message)
    case let .exchangeFailed(status, message):
      // Petrel keeps the status and the message, or the raw body when the
      // authority sent no message; recover an error name from such a body.
      // SpaceNotFound never arrives here: Petrel reports it at 400 or 404 as
      // authorizationRefused (above), where it is answered as a refusal.
      guard (300...599).contains(status) else { return (502, "UpstreamFailure", message) }
      let body = (try? JSONSerialization.jsonObject(with: Data(message.utf8))) as? [String: Any]
      return (status, body?["error"] as? String, body?["message"] as? String ?? message)
    case .missingDelegationToken, .invalidToken, .invalidResponse, .insecureURL:
      // No usable answer from the account's PDS, the authority or the repo host.
      return (502, "UpstreamFailure", nil)
    case .invalidKey:
      return (500, "InternalServerError", nil)
    case .invalidSpaceRef:
      return (400, "InvalidRequest", nil)
    }
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
    guard Self.isValidMailboxKeyRkey(rkey) else {
      throw LabyrinthTransportError.invalidRequest("invalid mailboxKeys rkey")
    }
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
    let (data, http) = try await MLSBoundedURLSession.fetchData(
      for: request,
      maxBytes: MLSBoundedURLSession.pdsRecordMaxBytes,
      session: MLSPublicPDSReader.authorizationSession
    )
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

  static let maxMailboxKeysDeletionPages = 20

  static func isValidMailboxKeyRkey(_ rkey: String) -> Bool {
    guard !rkey.isEmpty,
          rkey.count <= 512,
          rkey != ".",
          rkey != "..",
          !rkey.utf8.contains(where: {
            $0 <= 0x20 || $0 >= 0x7F
              || $0 == UInt8(ascii: "/")
              || $0 == UInt8(ascii: "\\")
              || $0 == UInt8(ascii: "%")
              || $0 == UInt8(ascii: "#")
              || $0 == UInt8(ascii: "?")
          }),
          (try? RecordKey(keyString: rkey)) != nil
    else {
      return false
    }
    return true
  }

  /// Extracts and strictly validates `at://<expectedDID>/blue.catbird.labyrinth.mailboxKeys/<rkey>`
  /// URIs and optional pagination cursor from a `com.atproto.repo.listRecords` page.
  static func extractValidatedMailboxKeyRkeys(
    fromListRecords data: Data,
    expectedDID: String
  ) throws -> (rkeys: [String], nextCursor: String?) {
    guard let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
      throw LabyrinthTransportError.pdsFailure("listRecords invalid JSON")
    }
    let records: [[String: Any]]
    if let rawRecords = object["records"] {
      guard let typed = rawRecords as? [[String: Any]] else {
        throw LabyrinthTransportError.pdsFailure("listRecords invalid records array")
      }
      records = typed
    } else {
      records = []
    }

    let expectedPrefix = "at://\(expectedDID)/\(labyrinthMailboxKeysCollection)/"
    var validatedRkeys: [String] = []
    validatedRkeys.reserveCapacity(records.count)

    for record in records {
      guard let uri = record["uri"] as? String,
            uri.hasPrefix(expectedPrefix)
      else {
        throw LabyrinthTransportError.pdsFailure("listRecords URI outside expected mailboxKeys collection")
      }
      let rkey = String(uri.dropFirst(expectedPrefix.count))
      guard isValidMailboxKeyRkey(rkey) else {
        throw LabyrinthTransportError.pdsFailure("listRecords invalid mailboxKeys rkey")
      }
      validatedRkeys.append(rkey)
    }

    let nextCursor: String?
    if records.isEmpty {
      nextCursor = nil
    } else if let rawCursor = object["cursor"], !(rawCursor is NSNull) {
      guard let cursorString = rawCursor as? String, !cursorString.isEmpty else {
        throw LabyrinthTransportError.pdsFailure("listRecords invalid cursor")
      }
      nextCursor = cursorString
    } else {
      nextCursor = nil
    }

    return (validatedRkeys, nextCursor)
  }

  static func collectValidatedMailboxKeyRkeysToDelete(
    did: String,
    maxPages: Int = maxMailboxKeysDeletionPages,
    fetchPage: (String?) async throws -> (data: Data, statusCode: Int)
  ) async throws -> [String] {
    var cursor: String?
    var seenCursors = Set<String>()
    var seenRkeys = Set<String>()
    var rkeys: [String] = []
    var pagesFetched = 0

    repeat {
      pagesFetched += 1
      guard pagesFetched <= maxPages else {
        throw LabyrinthTransportError.pdsFailure("listRecords exceeded max page bound (\(maxPages))")
      }

      let (data, statusCode) = try await fetchPage(cursor)
      guard statusCode == 200 else {
        throw LabyrinthTransportError.pdsFailure("listRecords \(statusCode)")
      }

      let parsed = try extractValidatedMailboxKeyRkeys(fromListRecords: data, expectedDID: did)
      for rkey in parsed.rkeys where seenRkeys.insert(rkey).inserted {
        rkeys.append(rkey)
      }

      if let next = parsed.nextCursor {
        guard seenCursors.insert(next).inserted else {
          throw LabyrinthTransportError.pdsFailure("listRecords cursor loop detected")
        }
        guard pagesFetched < maxPages else {
          throw LabyrinthTransportError.pdsFailure("listRecords exceeded max page bound (\(maxPages))")
        }
        cursor = next
      } else {
        cursor = nil
      }
    } while cursor != nil

    return rkeys
  }

  /// Deletes every `blue.catbird.labyrinth.mailboxKeys` record in the
  /// signed-in account's repo (account deletion).
  public func deleteLabyrinthMailboxKeys() async throws {
    guard let did = await authenticatedUserDID() else { throw MLSAPIError.noAuthentication }
    let rkeys = try await Self.collectValidatedMailboxKeyRkeysToDelete(did: did) { cursor in
      var query = [
        URLQueryItem(name: "repo", value: did),
        URLQueryItem(name: "collection", value: Self.labyrinthMailboxKeysCollection),
        URLQueryItem(name: "limit", value: "100"),
      ]
      if let cursor { query.append(URLQueryItem(name: "cursor", value: cursor)) }
      let (data, response) = try await self.authenticatedRepoCall(
        method: "GET", endpoint: "com.atproto.repo.listRecords", body: nil, query: query)
      return (data, response.statusCode)
    }

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

  static var session: URLSession {
    MLSBoundedURLSession.shared.underlyingSession
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

/// One Space credential manager, bound to the account DID it was made for.
/// Asking for another DID drops it, so credentials never cross accounts.
final class LabyrinthSpaceCredentialStore: @unchecked Sendable {
  private let lock = NSLock()
  private var entry: (did: String, manager: SpaceCredentialManager)?

  func manager(
    for did: String, make: () -> SpaceCredentialManager
  ) -> SpaceCredentialManager {
    lock.lock()
    defer { lock.unlock() }
    if let entry, entry.did == did { return entry.manager }
    let manager = make()
    entry = (did, manager)
    return manager
  }

  func clear() {
    lock.lock()
    defer { lock.unlock() }
    entry = nil
  }
}

/// Recently fetched DID documents of foreign conversation repos, each already
/// checked to be its DID's own document. A foreign record read needs the
/// repo's PDS and its author KEM key; both come from one fetch through here.
///
/// Bounded to `capacity` DIDs and short-lived (`lifetime` seconds). A forced
/// lookup always refetches and replaces the entry; if that refetch fails, the
/// entry is dropped. Failed fetches are not cached. Each caller still applies
/// its own checks to the document.
final class LabyrinthDIDDocumentCache: @unchecked Sendable {
  typealias Fetch = @Sendable (String) async throws -> DIDDocument

  static let defaultLifetime: TimeInterval = 60
  static let defaultCapacity = 32

  private let lock = NSLock()
  private var entries: [String: (document: DIDDocument, expires: Date)] = [:]
  private let lifetime: TimeInterval
  private let capacity: Int
  private let now: @Sendable () -> Date
  private let fetch: Fetch

  init(
    lifetime: TimeInterval = LabyrinthDIDDocumentCache.defaultLifetime,
    capacity: Int = LabyrinthDIDDocumentCache.defaultCapacity,
    now: @escaping @Sendable () -> Date = { Date() },
    fetch: @escaping Fetch = { did in try await MLSPublicPDSReader.fetchValidatedDIDDocument(did: did) }
  ) {
    self.lifetime = lifetime
    self.capacity = max(1, capacity)
    self.now = now
    self.fetch = fetch
  }

  func document(for did: String, forceRefresh: Bool = false) async throws -> DIDDocument {
    if !forceRefresh, let cached = cachedDocument(for: did) { return cached }
    do {
      let document = try await fetch(did)
      // The fetch checks this too; the cache never holds another DID's document.
      guard document.id == did else { throw MLSPublicPDSReader.ReaderError.invalidResponse }
      store(document, for: did)
      return document
    } catch {
      // A forced refresh follows a response MAC mismatch, so the cached
      // document is suspect: never serve it again once its refetch failed.
      if forceRefresh { remove(did) }
      throw error
    }
  }

  func clear() {
    lock.lock()
    defer { lock.unlock() }
    entries.removeAll()
  }

  private func remove(_ did: String) {
    lock.lock()
    defer { lock.unlock() }
    entries.removeValue(forKey: did)
  }

  private func cachedDocument(for did: String) -> DIDDocument? {
    lock.lock()
    defer { lock.unlock() }
    guard let entry = entries[did] else { return nil }
    guard entry.expires > now() else {
      entries.removeValue(forKey: did)
      return nil
    }
    return entry.document
  }

  private func store(_ document: DIDDocument, for did: String) {
    lock.lock()
    defer { lock.unlock() }
    let current = now()
    if entries[did] == nil, entries.count >= capacity {
      entries = entries.filter { $0.value.expires > current }
      if entries.count >= capacity,
         let oldest = entries.min(by: { $0.value.expires < $1.value.expires })?.key {
        entries.removeValue(forKey: oldest)
      }
    }
    entries[did] = (document, current.addingTimeInterval(lifetime))
  }
}
