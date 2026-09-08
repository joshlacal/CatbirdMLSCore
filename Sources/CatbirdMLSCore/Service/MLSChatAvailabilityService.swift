import Foundation
import OSLog
import Petrel

/// Service to check if users are available for MLS encrypted chat
/// Uses the server's getOptInStatus endpoint to check if users have opted in
public actor MLSChatAvailabilityService {
  private let logger = Logger(subsystem: "blue.catbird", category: "MLSChatAvailability")
  private let apiClient: MLSAPIClient

  public init(apiClient: MLSAPIClient) {
    self.apiClient = apiClient
  }

  /// Check availability state for a single user
  public func checkAvailability(with did: String) async -> MLSAPIClient.MLSChatAvailability {
    guard let normalizedDid = try? DID(didString: did) else {
      logger.error("Failed to normalize DID for chat availability: \(did)")
      return .unavailable
    }

    let statuses = await apiClient.getChatAvailability(dids: [normalizedDid])
    return statuses.first?.availability ?? .unknown
  }

  /// Batch check availability state for multiple users
  public func checkAvailability(withDids dids: [String]) async -> [String: MLSAPIClient.MLSChatAvailability] {
    var results: [String: MLSAPIClient.MLSChatAvailability] = [:]
    var validDIDs: [DID] = []
    var didMap: [String: [String]] = [:] // canonical -> original input strings

    for did in dids {
      guard let normalizedDid = try? DID(didString: did) else {
        logger.error("Invalid DID string provided for chat availability: \(did)")
        results[did] = .unavailable
        continue
      }
      let canonical = normalizedDid.description
      if didMap[canonical] == nil {
        validDIDs.append(normalizedDid)
      }
      didMap[canonical, default: []].append(did)
    }

    guard !validDIDs.isEmpty else {
      return results
    }

    let statuses = await apiClient.getChatAvailability(dids: validDIDs)
    for status in statuses {
      let canonical = status.did.description
      if let originals = didMap[canonical] {
        for original in originals {
          results[original] = status.availability
        }
      }
    }

    for did in dids where results[did] == nil {
      results[did] = .unknown
    }

    return results
  }

  /// Check if a single user can receive MLS messages (legacy boolean)
  public func canChat(with did: String) async -> Bool {
    await checkAvailability(with: did) == .available
  }

  /// Batch check for multiple users (efficient for profile lists)
  public func canChat(withDids dids: [String]) async -> [String: Bool] {
    let statuses = await checkAvailability(withDids: dids)
    var results: [String: Bool] = [:]
    for (did, availability) in statuses {
      results[did] = (availability == .available)
    }
    return results
  }

  /// Clear the cache (e.g., on logout)
  public func clearCache() async {
    await apiClient.clearAvailabilityCache()
  }

  /// Invalidate cache for a specific user
  public func invalidate(did: String) async {
    await apiClient.invalidateAvailability(for: did)
  }
}
