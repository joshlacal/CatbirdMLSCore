import Foundation

/// W5: Coalesces device-wide key-package publish/replenish operations so that
/// concurrent or rapid-fire callers collapse into a single in-flight request
/// instead of stampeding the server's `publishKeyPackages` quota.
///
/// ## Why this exists
/// `publishKeyPackages` is a **device-wide** operation, but it had many
/// independent triggers (device bootstrap, per-conversation Welcome recovery,
/// group create, send-time replenish, internal reconciliation) with **no
/// in-flight guard**. During a recovery-heavy bootstrap, dozens of
/// conversations each call `monitorAndReplenishBundles` before any of them has
/// finished publishing — so each one observes `available == 0` and fires its
/// full batch loop (target 25 / batch 5 ⇒ 5 HTTP publishes per call). N
/// concurrent callers ⇒ 5·N publishes, instantly exhausting the server's
/// per-DID quota (20 req/min ⇒ HTTP 429 with `Retry-After`). A production +
/// E2E capture observed **32 × 429, all from `publishKeyPackages`**.
///
/// ## What this does
/// Keyed by normalized DID (the publish is per-account/per-device):
/// 1. **Single-flight** — concurrent callers await the same in-flight `Task`
///    rather than launching new publishes (mirrors `inFlightExternalCommits`).
/// 2. **Debounce** — if a publish *succeeded* within `debounceWindow`, later
///    callers skip (the pool was just replenished to target — re-publishing is
///    redundant, not needed).
/// 3. **Backoff** — on a 429 / rate-limit error, honor `Retry-After` (or a
///    conservative default) and refuse to re-publish until the window passes,
///    so callers back off instead of tight-looping the quota.
///
/// The publish decision is factored into the pure, dependency-free
/// ``decide(now:lastSuccess:debounceWindow:backoffUntil:)`` so it is unit
/// testable without the network.
actor KeyPackagePublishCoordinator {

  /// Result of a (possibly coalesced/skipped) replenish.
  struct ReplenishOutcome: Sendable, Equatable {
    let available: Int
    let uploaded: Int
  }

  /// The scheduling decision for a publish attempt. Pure value type so the
  /// policy can be exhaustively unit tested.
  enum Decision: Equatable, Sendable {
    /// No recent success and no active backoff — run the publish.
    case proceed
    /// A publish succeeded within `debounceWindow`; reuse its result instead of
    /// firing a redundant device-wide publish.
    case skipRecentSuccess(lastAvailable: Int)
    /// Server is rate-limiting; do not publish until `until`.
    case backoff(until: Date)
  }

  /// Pure decision function. `backoff` takes precedence over `skip` (an active
  /// server backoff must always be honored), which takes precedence over
  /// `proceed`.
  static func decide(
    now: Date,
    lastSuccess: (date: Date, available: Int)?,
    debounceWindow: TimeInterval,
    backoffUntil: Date?
  ) -> Decision {
    if let backoffUntil, backoffUntil > now {
      return .backoff(until: backoffUntil)
    }
    if let lastSuccess, now.timeIntervalSince(lastSuccess.date) < debounceWindow {
      return .skipRecentSuccess(lastAvailable: lastSuccess.available)
    }
    return .proceed
  }

  /// Extracts a Retry-After hint (seconds) from a rate-limit error, or `nil`
  /// when the error is not a rate-limit. The outer optional distinguishes
  /// "not rate-limited" (`nil`) from "rate-limited, no explicit hint"
  /// (`.some(nil)`).
  static func rateLimitRetryAfter(for error: Error) -> TimeInterval?? {
    if case MLSError.rateLimited(let seconds) = error {
      return .some(TimeInterval(seconds))
    }
    if case MLSAPIError.rateLimited(let after) = error {
      return .some(after)
    }
    return nil
  }

  private let debounceWindow: TimeInterval
  private let defaultBackoff: TimeInterval
  private let now: @Sendable () -> Date

  private var inFlight: [String: Task<ReplenishOutcome, any Error>] = [:]
  private var lastSuccess: [String: (date: Date, available: Int)] = [:]
  private var backoffUntil: [String: Date] = [:]

  /// - Parameters:
  ///   - debounceWindow: How long after a successful publish to skip redundant
  ///     publishes. Default 30s comfortably covers a bootstrap/recovery burst
  ///     without starving a genuinely-later refresh.
  ///   - defaultBackoff: Backoff applied on a 429 with no explicit Retry-After.
  ///     Default 10s: the server quota is 20/min (~3s/token), so 10s ensures the
  ///     burst drains rather than re-firing into a still-saturated window.
  ///   - now: Injectable clock for deterministic tests.
  init(
    debounceWindow: TimeInterval = 30,
    defaultBackoff: TimeInterval = 10,
    now: @escaping @Sendable () -> Date = { Date() }
  ) {
    self.debounceWindow = debounceWindow
    self.defaultBackoff = defaultBackoff
    self.now = now
  }

  /// Run `operation` (the actual device-wide publish) under single-flight +
  /// debounce + backoff for `key` (normalized DID).
  ///
  /// - On `.skipRecentSuccess`, returns the last known availability with
  ///   `uploaded: 0` without invoking `operation`.
  /// - On `.backoff`, throws `MLSError.rateLimited` without invoking
  ///   `operation`.
  /// - On `.proceed`, either awaits the existing in-flight task or starts a new
  ///   one; on a rate-limit failure, arms a backoff window so subsequent callers
  ///   short-circuit instead of hammering the quota.
  func run(
    key: String,
    operation: @Sendable @escaping () async throws -> ReplenishOutcome
  ) async throws -> ReplenishOutcome {
    // NOTE: everything up to `await task.value` runs without suspension, so the
    // decide → in-flight check → task install sequence is atomic on this actor.
    switch Self.decide(
      now: now(),
      lastSuccess: lastSuccess[key],
      debounceWindow: debounceWindow,
      backoffUntil: backoffUntil[key]
    ) {
    case .skipRecentSuccess(let lastAvailable):
      return ReplenishOutcome(available: lastAvailable, uploaded: 0)
    case .backoff(let until):
      let seconds = max(1, until.timeIntervalSince(now())).rounded(.up)
      throw MLSError.rateLimited(retryAfterSeconds: Int(seconds))
    case .proceed:
      break
    }

    if let existing = inFlight[key] {
      return try await existing.value
    }

    let task = Task<ReplenishOutcome, any Error> { try await operation() }
    inFlight[key] = task

    do {
      let outcome = try await task.value
      if inFlight[key] == task { inFlight[key] = nil }
      lastSuccess[key] = (now(), outcome.available)
      return outcome
    } catch {
      if inFlight[key] == task { inFlight[key] = nil }
      if case .some(let hint) = Self.rateLimitRetryAfter(for: error) {
        backoffUntil[key] = now().addingTimeInterval(hint ?? defaultBackoff)
      }
      throw error
    }
  }
}
