import Foundation
import OSLog
import Petrel
import PetrelCatbird

/// Reads PUBLIC `blue.catbird.mlsChat.device` records directly from a DID's hosting PDS.
///
/// W6 root cause: foreign-repo reads (another member's device records) were issued through the
/// BFF (nest) via `com.atproto.repo.listRecords`. Nest's XRPC proxy only routes
/// `com.atproto.repo.*` to the *session user's* PDS, so a foreign `repo` resolves to a PDS that
/// does not host it → `400 InvalidRequest "Could not find repo"` → empty result → silent
/// TOFU/warn-only credential binding.
///
/// Device records are public, so the correct fix is to resolve the target's real PDS
/// (plc.directory / `.well-known/did.json`, independent of nest) and read its records
/// unauthenticated. `BlueCatbirdMlsChatDevice` (and its `Bytes`/`ATProtocolDate` fields) decode
/// from a `listRecords` `value` object with a vanilla `JSONDecoder`, so no decoder registry is
/// needed.
enum MLSPublicPDSReader {
  static let deviceCollection = "blue.catbird.mlsChat.device"

  private static let logger = Logger(subsystem: "blue.catbird", category: "MLSPublicPDSReader")

  enum ReaderError: Error {
    case invalidEndpoint
    case invalidResponse
    case httpStatus(Int)
  }

  /// Build the unauthenticated `listRecords` URL for a DID's device records on its hosting PDS.
  /// Returns nil for a non-HTTPS endpoint (defense against downgraded/SSRF endpoints).
  static func deviceRecordsListURL(pds: URL, did: String, limit: Int = 50) -> URL? {
    guard pds.scheme?.lowercased() == "https" else { return nil }
    guard var comps = URLComponents(url: pds, resolvingAgainstBaseURL: false) else { return nil }

    // Preserve any base path the PDS endpoint carries, then append the XRPC method.
    var basePath = comps.path
    if basePath.hasSuffix("/") { basePath.removeLast() }
    comps.path = basePath + "/xrpc/com.atproto.repo.listRecords"
    comps.queryItems = [
      URLQueryItem(name: "repo", value: did),
      URLQueryItem(name: "collection", value: deviceCollection),
      URLQueryItem(name: "limit", value: String(limit)),
    ]
    return comps.url
  }

  /// Decode device signature public keys from a `com.atproto.repo.listRecords` JSON response.
  /// Lenient: records whose `value` is not a well-formed device record are skipped.
  static func decodeDeviceSignatureKeys(fromListRecordsJSON data: Data) -> [Data] {
    guard let envelope = try? JSONDecoder().decode(ListRecordsEnvelope.self, from: data) else {
      return []
    }
    return envelope.records.compactMap { $0.value?.mlsSignaturePublicKey.data }
  }

  /// Resolve the DID's hosting PDS and fetch its public device signature keys, unauthenticated.
  ///
  /// Throws on resolution / network / non-2xx failures so callers can preserve the
  /// fetch-failed (→ TOFU/warn-only) vs. published-none (→ empty `[]`) distinction. A 2xx
  /// response with no records returns an empty array.
  static func fetchDeviceSignatureKeys(
    did: String,
    resolvePDS: @Sendable (String) async throws -> URL,
    session: URLSession = .shared
  ) async throws -> [Data] {
    let pds = try await resolvePDS(did)
    guard let url = deviceRecordsListURL(pds: pds, did: did) else {
      throw ReaderError.invalidEndpoint
    }

    var request = URLRequest(url: url)
    request.httpMethod = "GET"
    request.setValue("application/json", forHTTPHeaderField: "Accept")

    let (data, response) = try await session.data(for: request)
    guard let http = response as? HTTPURLResponse else {
      throw ReaderError.invalidResponse
    }
    guard (200...299).contains(http.statusCode) else {
      throw ReaderError.httpStatus(http.statusCode)
    }
    return decodeDeviceSignatureKeys(fromListRecordsJSON: data)
  }

  // MARK: - JSON shapes

  private struct ListRecordsEnvelope: Decodable {
    let records: [RecordItem]
  }

  private struct RecordItem: Decodable {
    let value: BlueCatbirdMlsChatDevice?

    enum CodingKeys: String, CodingKey { case value }

    init(from decoder: Decoder) throws {
      let container = try decoder.container(keyedBy: CodingKeys.self)
      // Lenient: a `value` that isn't a device record decodes to nil instead of throwing,
      // so one foreign/legacy record can't poison the whole batch.
      value = try? container.decode(BlueCatbirdMlsChatDevice.self, forKey: .value)
    }
  }
}
