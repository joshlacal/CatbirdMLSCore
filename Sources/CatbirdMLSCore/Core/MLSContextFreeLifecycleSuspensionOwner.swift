//
//  MLSContextFreeLifecycleSuspensionOwner.swift
//  CatbirdMLSCore
//

import Foundation
import Synchronization
import OSLog

/// Coordinates context-free MLS lifecycle suspension when no `MLSConversationManager` instance is active.
/// Rotates per suspension to ensure that stale foreground tasks or expired background tasks cannot
/// accidentally close or resume an in-flight successor's suspension state.
public final class MLSContextFreeLifecycleSuspensionOwner: Sendable {
  public let id: UUID

  private struct SharedState: Sendable {
    var activeOwnerId: UUID?
    #if DEBUG
    var onPostDecisionHookForTesting: (@Sendable () -> Void)?
    #endif
  }

  private static let sharedState = Mutex(SharedState())
  private static let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSContextFreeLifecycleSuspensionOwner")

  public init() {
    self.id = UUID()
  }

  /// Mark suspension in progress for this owner, setting both client and core suspension flags.
  public func markSuspensionInProgress(reason: String) {
    Self.sharedState.withLock { state in
      state.activeOwnerId = self.id
      Self.logger.info("🚨 [ContextFreeOwner] Suspension marked in progress for owner \(self.id, privacy: .public): \(reason, privacy: .public)")
      MLSCoreContext.markSuspensionInProgress()
      MLSClient.markSuspensionInProgress(reason: "ContextFreeOwner(\(self.id)): \(reason)")
    }
  }

  /// Synchronously emergency closes all contexts if this instance is the currently active owner.
  @discardableResult
  public func emergencyCloseAllContextsIfOwned(reason: String) -> Bool {
    let isOwner = Self.sharedState.withLock { state -> Bool in
      state.activeOwnerId == self.id
    }
    guard isOwner else {
      Self.logger.warning("⚠️ [ContextFreeOwner] emergencyClose skipped: owner \(self.id, privacy: .public) is not active")
      return false
    }

    Self.logger.warning("🚨 [ContextFreeOwner] Emergency closing all contexts for active owner \(self.id, privacy: .public): \(reason, privacy: .public)")
    MLSCoreContext.interruptAllContexts()
    MLSClient.interruptAllContexts()
    MLSCoreContext.emergencyCloseAllContexts()
    MLSClient.emergencyCloseAllContexts(reason: "ContextFreeOwner(\(self.id)): \(reason)")
    return true
  }

  /// Resumes suspension if this instance is the active owner or if the suspension has no owner.
  /// If a different live owner holds the suspension, resume is refused.
  @discardableResult
  public func resumeSuspensionIfOwnedAndContextFree() async -> Bool {
    enum ResumeDecision {
      case owned
      case unowned
      case foreignOwner(UUID)
    }

    #if DEBUG
    var testHook: (@Sendable () -> Void)?
    #endif

    let decision = Self.sharedState.withLock { state -> ResumeDecision in
      #if DEBUG
      testHook = state.onPostDecisionHookForTesting
      state.onPostDecisionHookForTesting = nil
      #endif
      guard let activeOwnerId = state.activeOwnerId else {
        MLSCoreContext.clearSuspensionFlag()
        MLSClient.clearSuspensionFlag(reason: "ContextFreeOwner unowned resume")
        return .unowned
      }
      if activeOwnerId == self.id {
        state.activeOwnerId = nil
        MLSCoreContext.clearSuspensionFlag()
        MLSClient.clearSuspensionFlag(reason: "ContextFreeOwner(\(self.id)) resume")
        return .owned
      }
      return .foreignOwner(activeOwnerId)
    }

    #if DEBUG
    testHook?()
    #endif

    switch decision {
    case .owned:
      Self.logger.info("✅ [ContextFreeOwner] Cleared suspension flags for active owner \(self.id, privacy: .public)")
      return true

    case .unowned:
      Self.logger.info("✅ [ContextFreeOwner] Cleared unowned suspension flags (no active owner)")
      return true

    case .foreignOwner(let activeOwnerId):
      Self.logger.warning("⚠️ [ContextFreeOwner] resume skipped: owner \(self.id, privacy: .public) is not active (held by \(activeOwnerId, privacy: .public))")
      return false
    }
  }

  internal static func resetForTesting() {
    sharedState.withLock { state in
      state.activeOwnerId = nil
      #if DEBUG
      state.onPostDecisionHookForTesting = nil
      #endif
    }
  }

  #if DEBUG
  internal static func setPostDecisionHookForTesting(_ hook: (@Sendable () -> Void)?) {
    sharedState.withLock { state in
      state.onPostDecisionHookForTesting = hook
    }
  }
  #endif
}
