import Foundation

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
    self.accountIdentifier = accountIdentifier
    self.environmentIdentifier = environmentIdentifier
    self.deviceIdentifier = deviceIdentifier
    self.subscriptionIdentifier = subscriptionIdentifier
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
  internal let supportRevision: String
  internal let failure: MLSCanonicalSubscriptionTerminalFailure?
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
/// malformed records fail closed rather than being silently ignored.
internal enum MLSCanonicalSubscriptionFailurePersistence {
  internal static let eventType = "canonicalSubscriptionFailure.v1"

  internal static func load(
    scope: MLSCanonicalSubscriptionScope,
    supportRevision: String,
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
    guard record.scope == scope, record.supportRevision == supportRevision else {
      // A different reviewed support revision is an explicit capability
      // transition. Its old record remains scoped to that old revision and
      // cannot block the newly installed action table.
      return nil
    }
    return record.failure
  }

  internal static func save(
    failure: MLSCanonicalSubscriptionTerminalFailure?,
    scope: MLSCanonicalSubscriptionScope,
    supportRevision: String,
    store: MLSEventCursorStore
  ) async throws {
    let record = MLSCanonicalSubscriptionFailureRecord(
      scope: scope,
      supportRevision: supportRevision,
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

/// Lifecycle state used by each manager. Recreating this value is safe: load
/// restores the same-version durable latch, while a different reviewed
/// support revision intentionally starts clear. The manager's normal
/// reconnect path never calls clear.
internal struct MLSCanonicalSubscriptionFailureCoordinator {
  internal let scope: MLSCanonicalSubscriptionScope?
  internal let supportRevision: String
  internal let store: MLSEventCursorStore?
  internal private(set) var latch: MLSCanonicalSubscriptionFailureLatch

  internal init(
    scope: MLSCanonicalSubscriptionScope?,
    supportRevision: String,
    store: MLSEventCursorStore?,
    initialFailure: MLSCanonicalSubscriptionTerminalFailure? = nil
  ) {
    self.scope = scope
    self.supportRevision = supportRevision
    self.store = store
    self.latch = MLSCanonicalSubscriptionFailureLatch(terminalFailure: initialFailure)
  }

  internal var terminalFailure: MLSCanonicalSubscriptionTerminalFailure? {
    latch.terminalFailure
  }

  internal mutating func load() async throws {
    guard let scope, let store else { return }
    let failure = try await MLSCanonicalSubscriptionFailurePersistence.load(
      scope: scope,
      supportRevision: supportRevision,
      store: store
    )
    latch.restore(failure)
  }

  @discardableResult
  internal mutating func record(_ error: Error) async throws -> Bool {
    let previous = latch.terminalFailure
    guard latch.record(error), let failure = latch.terminalFailure else {
      return false
    }
    guard let scope, let store else { return true }
    do {
      try await MLSCanonicalSubscriptionFailurePersistence.save(
        failure: failure,
        scope: scope,
        supportRevision: supportRevision,
        store: store
      )
    } catch {
      // Do not claim a durable latch when the protected write failed. The
      // in-memory attempt is restored and the caller receives the storage
      // error so the next replay can retry the write.
      latch.restore(previous)
      throw error
    }
    return true
  }

  internal mutating func clear(
    after transition: MLSCanonicalSubscriptionRecoveryTransition
  ) async throws {
    let previous = latch.terminalFailure
    latch.clear(after: transition)
    guard let scope, let store else { return }
    do {
      try await MLSCanonicalSubscriptionFailurePersistence.save(
        failure: nil,
        scope: scope,
        supportRevision: supportRevision,
        store: store
      )
    } catch {
      latch.restore(previous)
      throw error
    }
  }
}
