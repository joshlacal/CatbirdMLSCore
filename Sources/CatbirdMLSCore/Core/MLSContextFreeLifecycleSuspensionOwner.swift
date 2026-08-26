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

  private enum ActiveSuspensionOwner: Equatable, Sendable {
    case contextFree(UUID)
    case manager(UUID)
  }

  private struct SharedState: Sendable {
    var activeOwner: ActiveSuspensionOwner?
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
      state.activeOwner = .contextFree(self.id)
      Self.logger.info("🚨 [ContextFreeOwner] Suspension marked in progress for owner \(self.id, privacy: .public): \(reason, privacy: .public)")
      MLSCoreContext.markSuspensionInProgress()
      MLSClient.markSuspensionInProgress(reason: "ContextFreeOwner(\(self.id)): \(reason)")
    }
  }

  /// Records a manager-owned suspension under the shared state lock,
  /// updating the active owner to `.manager(id)` and asserting both global gates.
  @discardableResult
  public static func recordManagerSuspension(id: UUID = UUID(), reason: String) -> UUID {
    sharedState.withLock { state in
      state.activeOwner = .manager(id)
      logger.info("🚨 [ContextFreeOwner] Manager suspension marked in progress for owner \(id, privacy: .public): \(reason, privacy: .public)")
      MLSCoreContext.markSuspensionInProgress()
      MLSClient.markSuspensionInProgress(reason: reason)
      return id
    }
  }

  /// Reasserts manager-owned suspension after a failed foreground restore, but only
  /// when no newer lifecycle transition owns the gates.
  ///
  /// A foreground restore must release its own global gates before rebuilding a
  /// force-closed Rust context. If that rebuild fails, this method restores the
  /// fail-closed state without overwriting a newer inactive/background owner.
  @discardableResult
  static func recordManagerSuspensionIfUnowned(
    id: UUID = UUID(),
    reason: String
  ) -> UUID? {
    sharedState.withLock { state in
      guard state.activeOwner == nil else {
        logger.warning(
          "⚠️ [ContextFreeOwner] Manager suspension reassert skipped for owner \(id, privacy: .public): a newer owner is active"
        )
        return nil
      }
      state.activeOwner = .manager(id)
      logger.warning(
        "🚨 [ContextFreeOwner] Manager suspension reasserted for owner \(id, privacy: .public): \(reason, privacy: .public)"
      )
      MLSCoreContext.markSuspensionInProgress()
      MLSClient.markSuspensionInProgress(reason: reason)
      return id
    }
  }

  /// Records a manager-owned resume under the shared state lock,
  /// clearing the active owner if it was owned by this manager ID and releasing both global gates.
  /// If a different live owner holds the suspension, both gates remain asserted and false is returned.
  @discardableResult
  public static func recordManagerResume(id: UUID, reason: String) -> Bool {
    enum ResumeDecision {
      case owned
      case foreignOwner(ActiveSuspensionOwner?)
    }

    #if DEBUG
    var testHook: (@Sendable () -> Void)?
    #endif

    let decision = sharedState.withLock { state -> ResumeDecision in
      #if DEBUG
      testHook = state.onPostDecisionHookForTesting
      state.onPostDecisionHookForTesting = nil
      #endif

      if state.activeOwner == .manager(id) {
        state.activeOwner = nil
        MLSCoreContext.clearSuspensionFlag()
        MLSClient.clearSuspensionFlag(reason: reason)
        return .owned
      } else {
        return .foreignOwner(state.activeOwner)
      }
    }

    #if DEBUG
    testHook?()
    #endif

    switch decision {
    case .owned:
      logger.info("✅ [ContextFreeOwner] Manager suspension resumed for owner \(id, privacy: .public): \(reason, privacy: .public)")
      return true
    case .foreignOwner(let activeOwner):
      logger.warning("⚠️ [ContextFreeOwner] Manager resume skipped for owner \(id, privacy: .public): not active owner (held by \(String(describing: activeOwner), privacy: .public))")
      return false
    }
  }

  /// Synchronously emergency closes all contexts if this instance is the currently active owner.
  @discardableResult
  public func emergencyCloseAllContextsIfOwned(reason: String) -> Bool {
    let isOwner = Self.sharedState.withLock { state -> Bool in
      state.activeOwner == .contextFree(self.id)
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
      case managerOwned(UUID)
    }

    #if DEBUG
    var testHook: (@Sendable () -> Void)?
    #endif

    let decision = Self.sharedState.withLock { state -> ResumeDecision in
      #if DEBUG
      testHook = state.onPostDecisionHookForTesting
      state.onPostDecisionHookForTesting = nil
      #endif
      guard let activeOwner = state.activeOwner else {
        MLSCoreContext.clearSuspensionFlag()
        MLSClient.clearSuspensionFlag(reason: "ContextFreeOwner unowned resume")
        return .unowned
      }
      switch activeOwner {
      case .contextFree(let ownerId):
        if ownerId == self.id {
          state.activeOwner = nil
          MLSCoreContext.clearSuspensionFlag()
          MLSClient.clearSuspensionFlag(reason: "ContextFreeOwner(\(self.id)) resume")
          return .owned
        } else {
          return .foreignOwner(ownerId)
        }
      case .manager(let managerId):
        return .managerOwned(managerId)
      }
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

    case .managerOwned(let managerId):
      Self.logger.warning("⚠️ [ContextFreeOwner] resume skipped: owner \(self.id, privacy: .public) is not active (held by manager \(managerId, privacy: .public))")
      return false
    }
  }

  internal static func resetForTesting() {
    sharedState.withLock { state in
      state.activeOwner = nil
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
