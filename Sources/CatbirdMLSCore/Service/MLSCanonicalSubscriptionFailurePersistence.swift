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
    initialFailure: MLSCanonicalSubscriptionTerminalFailure? = nil
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
  }

  internal var terminalFailure: MLSCanonicalSubscriptionTerminalFailure? {
    latch.terminalFailure
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
