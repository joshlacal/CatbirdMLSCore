import Foundation

/// Protocol for checking whether a DID is "trusted" for chat request purposes.
///
/// A trusted sender means the conversation should appear in the main inbox.
/// An untrusted sender means it should appear in Requests until accepted.
///
/// Trust can be determined by:
/// - Whether the current user follows the sender
/// - Whether there's an existing accepted conversation with the sender
/// - App-specific criteria (mutual follows, verified accounts, etc.)
public protocol MLSTrustChecker: Sendable {
  /// Check if the current user trusts the given DID for messaging.
  ///
  /// - Parameter did: The DID to check
  /// - Returns: `true` if the DID is trusted (messages go to inbox), `false` if untrusted (messages go to Requests)
  func isTrusted(did: String) async -> Bool
}

/// Default implementation that trusts everyone (for backwards compatibility)
public struct AlwaysTrustChecker: MLSTrustChecker {
  public init() {}
  
  public func isTrusted(did: String) async -> Bool {
    return true
  }
}
