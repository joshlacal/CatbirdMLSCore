import Foundation
import Petrel
import PetrelCatbird

extension MLSPublicPDSReader {
  static let authorizationSession: URLSession = {
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

  /// Bypass Petrel's non-expiring DID-to-PDS cache for authorization. The
  /// native key-set cache is the only cache on this trust path.
  static func resolveCurrentPDS(did: String, session: URLSession = authorizationSession) async throws -> URL {
    guard !did.contains("#"), (try? DID(didString: did)) != nil else { throw ReaderError.invalidResponse }
    let url = try SpaceHostResolver.didDocumentURL(for: did)
    var request = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData)
    request.setValue("application/json", forHTTPHeaderField: "Accept")
    let (data, response) = try await session.data(for: request)
    guard let http = response as? HTTPURLResponse, http.statusCode == 200,
          http.url == url, data.count <= 1_048_576 else { throw ReaderError.invalidResponse }
    let document = try JSONDecoder().decode(DIDDocument.self, from: data)
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
