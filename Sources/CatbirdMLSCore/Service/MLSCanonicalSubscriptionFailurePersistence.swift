import Foundation

/// Core-owned revision of the generated event-envelope union. This is a
/// compiled capability identity, not a label supplied by a caller.
internal enum MLSCanonicalGeneratedUnionRevision: String, Codable, Equatable, Sendable {
  case eventEnvelopeV1 = "blue.catbird.chat.defs.eventEnvelope.v1"
}

/// Core-owned action-table revisions. A revision is meaningful only when the
/// action table is complete for every generated event arm.
internal enum MLSCanonicalActionTableRevision: String, Codable, Equatable, Sendable {
  case v1 = "canonical-durable-actions-v1"
  case v2 = "canonical-durable-actions-v2"

  internal static let current: Self = .v2
}

internal struct MLSCanonicalSubscriptionCapability: Codable, Equatable, Sendable {
  internal let generatedUnionRevision: MLSCanonicalGeneratedUnionRevision
  internal let actionTableRevision: MLSCanonicalActionTableRevision

  internal static let current = Self(
    generatedUnionRevision: .eventEnvelopeV1,
    actionTableRevision: .current
  )

  /// Stable key material for the manager-owned pending lifecycle map. This is
  /// derived only from Core-owned compiled capabilities; callers cannot supply
  /// a label that would replace or merge another capability's poison.
  internal var storageIdentifier: String {
    [generatedUnionRevision.rawValue, actionTableRevision.rawValue]
      .map { Data($0.utf8).base64EncodedString() }
      .joined(separator: ".")
  }
}

/// Exact durable namespace for a canonical subscription terminal state.
/// Every component is required so a blocked subscription cannot deny another
/// account, environment, device, or conversation.
internal struct MLSCanonicalSubscriptionScope: Codable, Equatable, Sendable {
  internal let accountIdentifier: String
  internal let environmentIdentifier: String
  internal let deviceIdentifier: String
  internal let subscriptionIdentifier: String

  internal init(
    accountIdentifier: String,
    environmentIdentifier: String,
    deviceIdentifier: String,
    subscriptionIdentifier: String
  ) {
    self.accountIdentifier = accountIdentifier.trimmingCharacters(in: .whitespacesAndNewlines)
    self.environmentIdentifier = environmentIdentifier.trimmingCharacters(in: .whitespacesAndNewlines)
    self.deviceIdentifier = deviceIdentifier.trimmingCharacters(in: .whitespacesAndNewlines)
    self.subscriptionIdentifier = subscriptionIdentifier.trimmingCharacters(in: .whitespacesAndNewlines)
  }

  internal var hasExactComponents: Bool {
    !accountIdentifier.isEmpty &&
      !environmentIdentifier.isEmpty &&
      !deviceIdentifier.isEmpty &&
      !subscriptionIdentifier.isEmpty
  }

  /// CursorStore already partitions by account; this additional encoded
  /// identity keeps the durable record exact even when a store is reused.
  internal var storageIdentifier: String {
    [accountIdentifier, environmentIdentifier, deviceIdentifier, subscriptionIdentifier]
      .map { Data($0.utf8).base64EncodedString() }
      .joined(separator: ".")
  }
}

internal struct MLSCanonicalSubscriptionFailureRecord: Codable, Equatable, Sendable {
  internal let scope: MLSCanonicalSubscriptionScope
  internal let capability: MLSCanonicalSubscriptionCapability
  internal let failure: MLSCanonicalSubscriptionTerminalFailure?

  internal init(
    scope: MLSCanonicalSubscriptionScope,
    capability: MLSCanonicalSubscriptionCapability,
    failure: MLSCanonicalSubscriptionTerminalFailure?
  ) {
    self.scope = scope
    self.capability = capability
    self.failure = failure
  }
}

internal enum MLSCanonicalSubscriptionFailureConfigurationError:
  Error, Equatable, LocalizedError
{
  case missingScope
  case missingStorage
  case incompleteActionTable
  case persistenceUnavailable

  internal var errorDescription: String? {
    switch self {
    case .missingScope:
      return "Canonical subscription requires an exact account, environment, device, and subscription scope"
    case .missingStorage:
      return "Canonical subscription requires durable cursor storage"
    case .incompleteActionTable:
      return "Canonical subscription requires a complete typed durable-action table"
    case .persistenceUnavailable:
      return "Canonical subscription durable failure storage is unavailable"
    }
  }
}

internal enum MLSCanonicalSubscriptionFailurePersistenceError:
  Error, Equatable, LocalizedError
{
  case invalidRecord

  internal var errorDescription: String? {
    switch self {
    case .invalidRecord:
      return "The persisted canonical subscription failure record is invalid"
    }
  }
}

/// Uses the existing protected cursor store as the durable Core seam. The
/// event-type namespace is deliberately separate from message cursors, and
/// malformed or cross-scope records fail closed rather than being silently
/// interpreted as a capability transition.
internal enum MLSCanonicalSubscriptionFailurePersistence {
  internal static let eventType = "canonicalSubscriptionFailure.v1"

  internal static func load(
    scope: MLSCanonicalSubscriptionScope,
    capability: MLSCanonicalSubscriptionCapability,
    store: MLSEventCursorStore
  ) async throws -> MLSCanonicalSubscriptionTerminalFailure? {
    let raw = try await MainActor.run {
      try store.getCursor(
        for: scope.storageIdentifier,
        eventType: eventType
      )
    }
    guard let raw else { return nil }
    guard let data = raw.data(using: .utf8) else {
      throw MLSCanonicalSubscriptionFailurePersistenceError.invalidRecord
    }
    let record: MLSCanonicalSubscriptionFailureRecord
    do {
      record = try JSONDecoder().decode(
        MLSCanonicalSubscriptionFailureRecord.self,
        from: data
      )
    } catch {
      throw MLSCanonicalSubscriptionFailurePersistenceError.invalidRecord
    }
    guard record.scope == scope else {
      throw MLSCanonicalSubscriptionFailurePersistenceError.invalidRecord
    }
    guard record.capability == capability else {
      // A different Core-owned compiled capability is the only implicit
      // recovery transition. The old record remains scoped to its old
      // capability and cannot block the newly installed complete table.
      return nil
    }
    return record.failure
  }

  internal static func save(
    failure: MLSCanonicalSubscriptionTerminalFailure?,
    scope: MLSCanonicalSubscriptionScope,
    capability: MLSCanonicalSubscriptionCapability,
    store: MLSEventCursorStore
  ) async throws {
    let record = MLSCanonicalSubscriptionFailureRecord(
      scope: scope,
      capability: capability,
      failure: failure
    )
    let data: Data
    do {
      data = try JSONEncoder().encode(record)
    } catch {
      throw MLSCanonicalSubscriptionFailurePersistenceError.invalidRecord
    }
    guard let encoded = String(data: data, encoding: .utf8) else {
      throw MLSCanonicalSubscriptionFailurePersistenceError.invalidRecord
    }
    try await MainActor.run {
      try store.updateCursor(
        for: scope.storageIdentifier,
        cursor: encoded,
        eventType: eventType
      )
    }
  }
}

/// Lifecycle state used by each manager. Construction requires the complete
/// scope, complete typed action table, and durable store; there is no
/// process-local fallback that could make a subscription appear safe.
internal struct MLSCanonicalSubscriptionFailureCoordinator {
  internal let scope: MLSCanonicalSubscriptionScope
  internal let capability: MLSCanonicalSubscriptionCapability
  internal let store: MLSEventCursorStore
  internal private(set) var latch: MLSCanonicalSubscriptionFailureLatch
  internal private(set) var persistenceUnavailable = false

  internal init(
    scope: MLSCanonicalSubscriptionScope?,
    capability: MLSCanonicalSubscriptionCapability?,
    store: MLSEventCursorStore?,
    initialFailure: MLSCanonicalSubscriptionTerminalFailure? = nil,
    persistenceUnavailable: Bool = false
  ) throws {
    guard let scope, scope.hasExactComponents else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.missingScope
    }
    guard let capability else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.incompleteActionTable
    }
    guard let store else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.missingStorage
    }
    self.scope = scope
    self.capability = capability
    self.store = store
    self.latch = MLSCanonicalSubscriptionFailureLatch(terminalFailure: initialFailure)
    self.persistenceUnavailable = persistenceUnavailable
  }

  internal var terminalFailure: MLSCanonicalSubscriptionTerminalFailure? {
    latch.terminalFailure
  }

  /// A terminal record that failed to reach the protected store is carried by
  /// the manager lifecycle after its run task exits. Only this exact scope and
  /// Core-owned capability may be retried on a later reconnect.
  internal var pendingFailure: MLSCanonicalSubscriptionPendingFailure? {
    guard persistenceUnavailable, let failure = latch.terminalFailure else {
      return nil
    }
    return MLSCanonicalSubscriptionPendingFailure(
      scope: scope,
      capability: capability,
      failure: failure
    )
  }

  internal mutating func load() async throws {
    do {
      let failure = try await MLSCanonicalSubscriptionFailurePersistence.load(
        scope: scope,
        capability: capability,
        store: store
      )
      latch.restore(failure)
      persistenceUnavailable = false
    } catch let error as MLSCanonicalSubscriptionFailurePersistenceError {
      throw error
    } catch {
      persistenceUnavailable = true
      throw MLSCanonicalSubscriptionFailureConfigurationError.persistenceUnavailable
    }
  }

  /// Retry the retained terminal record against the coordinator's current
  /// protected store. The manager invokes this before inventory/ticket/stream
  /// setup on reconnect; failure keeps the latch and the unavailable state.
  internal mutating func retryPendingPersistence() async throws {
    guard let failure = latch.terminalFailure else {
      persistenceUnavailable = false
      return
    }
    do {
      try await MLSCanonicalSubscriptionFailurePersistence.save(
        failure: failure,
        scope: scope,
        capability: capability,
        store: store
      )
      persistenceUnavailable = false
    } catch {
      persistenceUnavailable = true
      throw MLSCanonicalSubscriptionFailureConfigurationError.persistenceUnavailable
    }
  }

  @discardableResult
  internal mutating func record(_ error: Error) async throws -> Bool {
    if persistenceUnavailable, let failure = latch.terminalFailure {
      do {
        try await MLSCanonicalSubscriptionFailurePersistence.save(
          failure: failure,
          scope: scope,
          capability: capability,
          store: store
        )
        persistenceUnavailable = false
        return true
      } catch {
        persistenceUnavailable = true
        throw MLSCanonicalSubscriptionFailureConfigurationError.persistenceUnavailable
      }
    }

    guard latch.record(error), let failure = latch.terminalFailure else {
      return false
    }
    do {
      try await MLSCanonicalSubscriptionFailurePersistence.save(
        failure: failure,
        scope: scope,
        capability: capability,
        store: store
      )
    } catch {
      // Preserve the in-memory terminal block. The manager must stop and
      // remain unavailable until a later explicit durable write succeeds.
      persistenceUnavailable = true
      throw MLSCanonicalSubscriptionFailureConfigurationError.persistenceUnavailable
    }
    persistenceUnavailable = false
    return true
  }

  internal mutating func clear(
    after transition: MLSCanonicalSubscriptionRecoveryTransition
  ) async throws {
    guard !persistenceUnavailable else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.persistenceUnavailable
    }
    let previous = latch.terminalFailure
    latch.clear(after: transition)
    do {
      try await MLSCanonicalSubscriptionFailurePersistence.save(
        failure: nil,
        scope: scope,
        capability: capability,
        store: store
      )
      persistenceUnavailable = false
    } catch {
      latch.restore(previous)
      persistenceUnavailable = true
      throw MLSCanonicalSubscriptionFailureConfigurationError.persistenceUnavailable
    }
  }
}

// The coordinator is owned by one manager task and crosses only to the
// MainActor-protected cursor store for I/O. Its mutable latch is never shared
// between tasks; this conformance documents that ownership boundary for Swift
// 6 region isolation when a manager awaits persistence.
extension MLSCanonicalSubscriptionFailureCoordinator: @unchecked Sendable {}

/// Pending terminal poison retained by a manager after its subscription task
/// exits because durable failure persistence was unavailable. This is a value,
/// not a store reference: a later public reconnect can use the manager's
/// current protected store after storage recovery without writing to an old
/// account/device/environment scope.
internal struct MLSCanonicalSubscriptionPendingFailure: Equatable, Sendable {
  internal let scope: MLSCanonicalSubscriptionScope
  internal let capability: MLSCanonicalSubscriptionCapability
  internal let failure: MLSCanonicalSubscriptionTerminalFailure

  internal var storageIdentifier: String {
    "\(scope.storageIdentifier)|\(capability.storageIdentifier)"
  }
}

/// Manager-owned lifecycle for terminal persistence failures. Both the
/// WebSocket and SSE actors keep one instance so a run-task exit cannot erase
/// an unwritten terminal record. A pending value is consumed only after its
/// exact record has been written successfully to the current protected store.
internal actor MLSCanonicalSubscriptionFailureLifecycle {
  private var pending: [String: MLSCanonicalSubscriptionPendingFailure] = [:]

  internal func remember(
    _ coordinator: MLSCanonicalSubscriptionFailureCoordinator
  ) {
    guard let failure = coordinator.pendingFailure else { return }
    pending[failure.storageIdentifier] = failure
  }

  internal func restorePendingIfNeeded(
    _ coordinator: MLSCanonicalSubscriptionFailureCoordinator
  ) async throws -> MLSCanonicalSubscriptionFailureCoordinator {
    let key = "\(coordinator.scope.storageIdentifier)|\(coordinator.capability.storageIdentifier)"
    guard let pendingFailure = pending[key] else {
      return coordinator
    }

    var restored = try MLSCanonicalSubscriptionFailureCoordinator(
      scope: coordinator.scope,
      capability: coordinator.capability,
      store: coordinator.store,
      initialFailure: pendingFailure.failure,
      persistenceUnavailable: true
    )
    do {
      try await restored.retryPendingPersistence()
    } catch {
      // Keep the exact pending value for the next public reconnect. In
      // particular, do not let a failed retry fall through to inventory.
      remember(restored)
      throw error
    }
    pending.removeValue(forKey: key)
    return restored
  }
}
