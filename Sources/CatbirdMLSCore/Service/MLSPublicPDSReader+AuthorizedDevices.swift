import Foundation
import Petrel
import PetrelCatbird

extension MLSPublicPDSReader {
  static let directRequestReceiptAuthorityDID = "did:web:chat.catbird.blue"

  static var authorizationSession: URLSession {
    #if DEBUG && canImport(Network) && canImport(Security)
    if let fixture = DebugFixtureTransport.current {
      do { return try fixture.makeSession() }
      catch { preconditionFailure("Invalid debug fixture authorization session") }
    }
    #endif
    return defaultAuthorizationSession
  }

  private static let defaultAuthorizationSession: URLSession = {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.httpCookieStorage = nil
    configuration.urlCredentialStorage = nil
    configuration.httpShouldSetCookies = false
    configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
    return URLSession(configuration: configuration, delegate: AuthorizationRedirectPolicy(), delegateQueue: nil)
  }()

  private final class AuthorizationRedirectPolicy: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(_ session: URLSession, task: URLSessionTask,
      willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest,
      completionHandler: @escaping (URLRequest?) -> Void) {
      completionHandler(nil)
    }
  }

  /// Strict canonical URL normalization for production trust-path endpoint matching.
  /// Builds URLComponents for both URLs, normalizes default HTTPS port 443 to nil,
  /// and requires the full normalized URLs to be strictly equal (preserving scheme, host,
  /// path, query, user, password, and fragment).
  static func normalizeForTrustEvaluation(_ url: URL?) -> URL? {
    guard let url, var components = URLComponents(url: url, resolvingAgainstBaseURL: true) else {
      return nil
    }
    if components.scheme?.lowercased() == "https", components.port == 443 {
      components.port = nil
    }
    return components.url
  }

  static func isMatchingEndpoint(_ actual: URL?, expected: URL) -> Bool {
    guard let normActual = normalizeForTrustEvaluation(actual),
          let normExpected = normalizeForTrustEvaluation(expected) else {
      return false
    }
    return normActual == normExpected
  }

  /// Bypass Petrel's non-expiring DID-to-PDS cache for authorization. The
  /// native key-set cache is the only cache on this trust path.
  static func resolveCurrentPDS(did: String, session: URLSession = authorizationSession) async throws -> URL {
    try currentPDS(in: try await fetchValidatedDIDDocument(did: did, session: session), did: did)
  }

  /// A fresh fetch of `did`'s DID document, checked to be that DID's own
  /// document. Never cached here.
  static func fetchValidatedDIDDocument(
    did: String, session: URLSession = authorizationSession
  ) async throws -> DIDDocument {
    guard !did.contains("#"), (try? DID(didString: did)) != nil else { throw ReaderError.invalidResponse }
    let url = try SpaceHostResolver.didDocumentURL(for: did)
    let limits = NetworkResponseLimits(
      maximumWireBytes: 1_048_576, maximumDecodedBytes: 1_048_576)
    let document: DIDDocument
    #if DEBUG && canImport(Network) && canImport(Security)
    if let fixture = DebugFixtureTransport.current, fixture.permits(url) {
      var request = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData)
      request.setValue("application/json", forHTTPHeaderField: "Accept")
      let (data, response) = try await session.data(for: request)
      guard let http = response as? HTTPURLResponse, http.statusCode == 200,
            isMatchingEndpoint(http.url, expected: url), data.count <= 1_048_576 else {
        throw ReaderError.invalidResponse
      }
      document = try JSONDecoder().decode(DIDDocument.self, from: data)
    } else {
      document = try await SpaceHostResolver.fetchDIDDocument(for: did, limits: limits)
    }
    #else
    document = try await SpaceHostResolver.fetchDIDDocument(for: did, limits: limits)
    #endif
    guard document.id == did else { throw ReaderError.invalidResponse }
    return document
  }

  /// The single HTTPS `#atproto_pds` endpoint of `did`'s own DID document.
  static func currentPDS(in document: DIDDocument, did: String) throws -> URL {
    guard document.id == did else { throw ReaderError.invalidResponse }
    let services = document.service.filter {
      ($0.id == "#atproto_pds" || $0.id == "\(did)#atproto_pds") && $0.type == "AtprotoPersonalDataServer"
    }
    guard services.count == 1, let service = services.first,
          let endpoint = URL(string: service.serviceEndpoint), endpoint.scheme == "https",
          endpoint.host != nil, endpoint.user == nil, endpoint.password == nil,
          endpoint.fragment == nil else { throw ReaderError.invalidEndpoint }
    return endpoint
  }

  /// Fetch only the coordinator DID selected by Rust, never a receipt-supplied URL.
  /// The fixed local authority retains its existing fixture route; other authorities
  /// use Petrel's public-address-validated and redirect-free DID resolver.
  static func fetchDirectRequestReceiptAuthorityDocument(
    authorityDid did: String,
    session: URLSession = authorizationSession
  ) async throws -> Data {
    guard did.hasPrefix("did:web:") else { throw ReaderError.invalidResponse }
    if did != directRequestReceiptAuthorityDID {
      let url = try SpaceHostResolver.didDocumentURL(for: did)
      guard let host = url.host?.lowercased(),
            host != "localhost", host != "127.0.0.1", host != "::1"
      else { throw ReaderError.invalidEndpoint }
      let limits = NetworkResponseLimits(
        maximumWireBytes: 1_048_576, maximumDecodedBytes: 1_048_576)
      let document: DIDDocument
      #if DEBUG && canImport(Network) && canImport(Security)
      if let fixture = Petrel.DebugFixtureTransport.current {
        guard fixture.permits(url) else { throw ReaderError.invalidEndpoint }
        document = try await SpaceHostResolver.fetchDIDDocument(
          for: did, urlSession: session, limits: limits)
      } else {
        document = try await SpaceHostResolver.fetchDIDDocument(for: did, limits: limits)
      }
      #else
      document = try await SpaceHostResolver.fetchDIDDocument(for: did, limits: limits)
      #endif
      guard document.id == did else { throw ReaderError.invalidResponse }
      let data = try JSONEncoder().encode(document)
      guard data.count <= 1_048_576 else { throw ReaderError.invalidResponse }
      return data
    }
    #if DEBUG && canImport(Network) && canImport(Security)
    if let fixture = Petrel.DebugFixtureTransport.current {
      let fixtureURL = fixture.origin.appendingPathComponent(".well-known/did.json")
      var request = URLRequest(url: fixtureURL, cachePolicy: .reloadIgnoringLocalCacheData)
      request.setValue("application/json", forHTTPHeaderField: "Accept")
      request.setValue("chat.catbird.blue", forHTTPHeaderField: "Host")
      request.timeoutInterval = 10.0
      let (data, response) = try await session.data(for: request)
      guard let http = response as? HTTPURLResponse,
            http.statusCode == 200,
            data.count <= 1_048_576
      else { throw ReaderError.invalidResponse }
      let document = try JSONDecoder().decode(DIDDocument.self, from: data)
      guard document.id == did else { throw ReaderError.invalidResponse }
      return data
    }
    #endif
    let url = try SpaceHostResolver.didDocumentURL(for: did)
    var request = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData)
    request.setValue("application/json", forHTTPHeaderField: "Accept")
    request.timeoutInterval = 10.0
    let (data, response) = try await session.data(for: request)
    guard let http = response as? HTTPURLResponse,
          http.statusCode == 200,
          isMatchingEndpoint(http.url, expected: url),
          data.count <= 1_048_576
    else { throw ReaderError.invalidResponse }
    let document = try JSONDecoder().decode(DIDDocument.self, from: data)
    guard document.id == did else { throw ReaderError.invalidResponse }
    return data
  }

  struct AuthorizedDeviceRecord: Equatable {
    let uri: String
    let publicKey: Data
  }

  /// ADR-009: the account's resolved PDS supplies authorization, never the DS.
  /// Malformed or incomplete reads are errors; only a complete empty listing
  /// represents a successfully resolved account with no authorized keys.
  static func decodeAuthorizedDevicePage(_ data: Data, did: String) throws
    -> (records: [AuthorizedDeviceRecord], cursor: String?) {
    guard data.count <= 1_048_576,
          let envelope = try JSONSerialization.jsonObject(with: data) as? [String: Any],
          let rows = envelope["records"] as? [[String: Any]], rows.count <= 100 else {
      throw ReaderError.invalidResponse
    }
    var seen = Set<String>()
    let prefix = "at://\(did)/\(deviceCollection)/"
    let records = try rows.map { row -> AuthorizedDeviceRecord in
      guard let uri = row["uri"] as? String, uri.hasPrefix(prefix),
            !uri.dropFirst(prefix.count).isEmpty,
            (try? RecordKey(keyString: String(uri.dropFirst(prefix.count)))) != nil,
            seen.insert(uri).inserted,
            let value = row["value"] as? [String: Any],
            value["$type"] as? String == deviceCollection,
            let algorithm = value["algorithm"] as? String,
            algorithm.lowercased() == "ed25519",
            let bytes = value["mlsSignaturePublicKey"] as? [String: Any],
            let encoded = bytes["$bytes"] as? String,
            let key = Data(base64Encoded: encoded + String(repeating: "=", count: (4 - encoded.utf8.count % 4) % 4)), key.count == 32 else {
        throw ReaderError.invalidResponse
      }
      // Generated decoding validates createdAt as an AT Protocol date.
      var normalizedValue = value
      normalizedValue["mlsSignaturePublicKey"] = ["$bytes": key.base64EncodedString()]
      _ = try JSONDecoder().decode(BlueCatbirdChatDevice.self,
        from: JSONSerialization.data(withJSONObject: normalizedValue))
      return AuthorizedDeviceRecord(uri: uri, publicKey: key)
    }
    let cursor: String?
    if let raw = envelope["cursor"], !(raw is NSNull) {
      guard let value = raw as? String, !value.isEmpty else { throw ReaderError.invalidResponse }
      cursor = value
    } else { cursor = nil }
    return (records, cursor)
  }

  static func fetchAuthorizedDeviceRecords(
    did: String,
    resolvePDS: @Sendable (String) async throws -> URL,
    session: URLSession = authorizationSession
  ) async throws -> [AuthorizedDeviceRecord] {
    guard !did.contains("#"), (try? DID(didString: did)) != nil else {
      throw ReaderError.invalidResponse
    }
    let pds = try await resolvePDS(did)
    guard let baseURL = deviceRecordsListURL(pds: pds, did: did, limit: 100),
          pds.user == nil, pds.password == nil else { throw ReaderError.invalidEndpoint }
    var records: [AuthorizedDeviceRecord] = []
    var seenURIs = Set<String>()
    var seenCursors = Set<String>()
    var cursor: String?
    for _ in 0..<100 {
      try Task.checkCancellation()
      var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false)!
      if let cursor { components.queryItems?.append(URLQueryItem(name: "cursor", value: cursor)) }
      var request = URLRequest(url: components.url!, cachePolicy: .reloadIgnoringLocalCacheData)
      request.setValue("application/json", forHTTPHeaderField: "Accept")
      let (data, response) = try await session.data(for: request)
      guard let http = response as? HTTPURLResponse,
            http.url?.scheme == baseURL.scheme, http.url?.host == baseURL.host,
            http.url?.port == baseURL.port else { throw ReaderError.invalidResponse }
      guard (200...299).contains(http.statusCode) else { throw ReaderError.httpStatus(http.statusCode) }
      let page = try decodeAuthorizedDevicePage(data, did: did)
      for record in page.records {
        guard seenURIs.insert(record.uri).inserted else { throw ReaderError.invalidResponse }
        records.append(record)
      }
      guard let next = page.cursor else { return records }
      guard seenCursors.insert(next).inserted else { throw ReaderError.invalidResponse }
      cursor = next
    }
    throw ReaderError.invalidResponse
  }

  static func fetchAuthorizedDeviceSignatureKeys(
    did: String, resolvePDS: @Sendable (String) async throws -> URL,
    session: URLSession = authorizationSession
  ) async throws -> [Data] {
    let records = try await fetchAuthorizedDeviceRecords(did: did, resolvePDS: resolvePDS, session: session)
    var seen = Set<Data>()
    return records.map(\.publicKey).filter { seen.insert($0).inserted }
  }
}
