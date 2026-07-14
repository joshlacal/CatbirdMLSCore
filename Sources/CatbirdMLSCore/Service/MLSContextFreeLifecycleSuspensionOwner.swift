import Foundation

/// Opaque authority for one context-free MLS lifecycle suspension transition.
///
/// The private owner token is bound to the exact MLSClient suspension generation and signal
/// serial. Release delegates to the existing two-sided context-free compare-and-swap and cannot
/// clear user-bound authority, live MLS work, or a newer lifecycle transition.
public final class MLSContextFreeLifecycleSuspensionOwner: Sendable {
  private let ownerToken: UUID

  public init() {
    ownerToken = UUID()
  }

  public nonisolated func markSuspensionInProgress(reason: String = "context-free lifecycle") {
    MLSClient.markSuspensionInProgress(
      reason: reason,
      noUserOwnerToken: ownerToken
    )
  }

  /// Emergency-closes Core contexts while preserving this exact context-free suspension owner.
  /// Returns false without changing lifecycle state if a newer owner has replaced this instance.
  @discardableResult
  public nonisolated func emergencyCloseAllContextsIfOwned(
    reason: String = "context-free lifecycle expiration"
  ) -> Bool {
    MLSClient.emergencyCloseAllContextsIfOwned(
      noUserOwnerToken: ownerToken,
      reason: reason
    )
  }

  @discardableResult
  public func resumeSuspensionIfOwnedAndContextFree() async -> Bool {
    guard
      let capability = MLSClient.ownedNoUserSuspendedResumeCapability(
        ownerToken: ownerToken
      )
    else {
      return false
    }
    return await MLSClient.finishNoUserSuspendedResumeCapability(capability)
  }
}
