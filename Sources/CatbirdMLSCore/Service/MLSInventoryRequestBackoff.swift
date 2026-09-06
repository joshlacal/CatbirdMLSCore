import Foundation

/// Shared admission state for fresh inventory allocations. Retained capabilities
/// and streams do not participate. A denied call fails promptly so an FFI caller
/// cannot hold a native recovery lock while waiting for capacity.
internal final class MLSInventoryRequestBackoff: @unchecked Sendable {
  internal struct Scope: Hashable, Sendable {
    let service: String
    let did: String
    let deviceId: String
    // Own snapshots replace a complete internal row; shared inventories retain
    // generations. A denial of one allocation kind says nothing about the other.
    let endpoint: String
  }

  // Client retry policy, matching the existing stream reconnect sequence.
  // These delays never assert that server capacity or authority has expired.
  private static let localRetryDelays = [1, 2, 4, 8, 16, 30]
  private static let maximumServerRetryAfterSeconds = 901
  // Keep escalation across local retries, but retire idle account/device state
  // after the existing maximum hint horizon. This is only memory cleanup.
  private static let idleHistorySeconds = maximumServerRetryAfterSeconds

  private struct State {
    var deadline: ContinuousClock.Instant?
    var fallbackStep = 0
    var wave = UUID()
    var responseRevision = UUID()
    var lastActivity: ContinuousClock.Instant
  }
  private struct Attempt {
    let wave: UUID
    let responseRevision: UUID
  }
  private let now: @Sendable () -> ContinuousClock.Instant
  private let lock = NSLock()
  private var states: [Scope: State] = [:]
  internal init(now: @escaping @Sendable () -> ContinuousClock.Instant = { ContinuousClock().now }) {
    self.now = now
  }

  internal static func allocationDevice(method: String, endpoint: String, query: [URLQueryItem]?) -> String? {
    guard method == "GET",
          endpoint == "blue.catbird.chat.getConversations" || endpoint == "blue.catbird.chat.getOwnDevices",
          let query,
          !query.contains(where: { $0.name == "pageCursor" || $0.name == "inventorySessionId" }) else { return nil }
    let actors = query.filter { $0.name == "actorDeviceId" }
    guard actors.count == 1, let actor = actors.first?.value, !actor.isEmpty else { return nil }
    return actor
  }

  internal func perform(
    scope: Scope?,
    currentScope: () async throws -> Scope?,
    request: () async throws -> (Data, HTTPURLResponse)
  ) async throws -> (Data, HTTPURLResponse) {
    try Task.checkCancellation()
    guard let scope else { return try await request() }
    guard try await currentScope() == scope else {
      throw MLSAPIError.invalidResponse(message: "Inventory account or device changed before request")
    }
    try Task.checkCancellation()
    let instant = now()
    let attempt = try lock.withLock {
      pruneIdleStates(at: instant)
      var state = states[scope] ?? State(lastActivity: instant)
      state.lastActivity = instant
      states[scope] = state
      if let deadline = state.deadline, deadline > instant {
        let duration = instant.duration(to: deadline).components
        let remaining = ceil(Double(duration.seconds) + Double(duration.attoseconds) / 1e18)
        throw MLSAPIError.rateLimited(retryAfter: remaining)
      }
      return Attempt(wave: state.wave, responseRevision: state.responseRevision)
    }

    // No lock spans the operation, authentication awaits, or a timer. Native
    // callbacks fail promptly instead of sleeping while retaining Rust locks.
    let result = try await request()
    if result.1.statusCode == 429 {
      let receivedAt = now()
      let serverDelay = Self.retryAfterSeconds(result.1.value(forHTTPHeaderField: "Retry-After"))
      lock.withLock {
        pruneIdleStates(at: receivedAt)
        var state = states[scope] ?? State(lastActivity: receivedAt)
        let firstDenialInWave = state.wave == attempt.wave
        let delay: Int
        if let serverDelay {
          delay = serverDelay
        } else {
          // Concurrent requests admitted before the same denial count as one
          // escalation, even if their responses finish in a different order.
          if firstDenialInWave { state.fallbackStep += 1 }
          state.fallbackStep = min(max(1, state.fallbackStep), Self.localRetryDelays.count)
          delay = Self.localRetryDelays[state.fallbackStep - 1]
        }
        if firstDenialInWave { state.wave = UUID() }
        // Every actual denial invalidates a success that started before it,
        // including a late response belonging to an already-counted wave.
        state.responseRevision = UUID()
        state.lastActivity = receivedAt
        let deadline = receivedAt.advanced(by: .seconds(delay))
        state.deadline = max(state.deadline ?? deadline, deadline)
        states[scope] = state
      }
    } else if (200...299).contains(result.1.statusCode) {
      lock.withLock {
        if states[scope]?.responseRevision == attempt.responseRevision {
          states.removeValue(forKey: scope)
        }
      }
    }
    return result
  }

  // Called only while holding the short state lock.
  private func pruneIdleStates(at instant: ContinuousClock.Instant) {
    states = states.filter {
      ($0.value.deadline.map { $0 > instant } ?? false)
        || $0.value.lastActivity.advanced(by: .seconds(Self.idleHistorySeconds)) > instant
    }
  }

  /// The producer uses positive delta seconds. Current retained grants are at
  /// most 15 minutes plus the subsecond publication boundary, hence 901. A
  /// malformed/unsupported hint selects client policy, never a server deadline.
  internal static func retryAfterSeconds(_ header: String?) -> Int? {
    guard let header, header.utf8.count <= 16 else { return nil }
    let value = header.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !value.isEmpty, value.utf8.allSatisfy({ (48...57).contains($0) }),
          let seconds = Int(value), (1...maximumServerRetryAfterSeconds).contains(seconds) else { return nil }
    return seconds
  }
}
