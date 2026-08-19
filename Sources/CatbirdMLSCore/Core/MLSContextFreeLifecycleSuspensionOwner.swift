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
    }
    Self.logger.info("🚨 [ContextFreeOwner] Suspension marked in progress for owner \(self.id, privacy: .public): \(reason, privacy: .public)")
    MLSCoreContext.markSuspensionInProgress()
    MLSClient.markSuspensionInProgress(reason: "ContextFreeOwner(\(self.id)): \(reason)")
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

  /// Resumes suspension if this instance is still the active context-free owner.
  @discardableResult
  public func resumeSuspensionIfOwnedAndContextFree() async -> Bool {
    let isOwner = Self.sharedState.withLock { state -> Bool in
      if state.activeOwnerId == self.id {
        state.activeOwnerId = nil
        return true
      }
      return false
    }
    guard isOwner else {
      Self.logger.debug("🔄 [ContextFreeOwner] resume skipped: owner \(self.id, privacy: .public) is not active")
      return false
    }

    Self.logger.info("✅ [ContextFreeOwner] Clearing suspension flags for active owner \(self.id, privacy: .public)")
    MLSCoreContext.clearSuspensionFlag()
    MLSClient.clearSuspensionFlag(reason: "ContextFreeOwner(\(self.id)) resume")
    return true
  }
}
