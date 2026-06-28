import Foundation
import Testing

@testable import CatbirdMLSCore

/// W5: Verifies the device-wide key-package publish coalescer collapses the
/// bootstrap/recovery 429 storm — single-flight, debounce, and Retry-After
/// backoff — without starving a genuinely-needed publish.
@Suite("KeyPackagePublishCoordinator")
struct KeyPackagePublishCoordinatorTests {

  typealias Coordinator = KeyPackagePublishCoordinator
  typealias Outcome = KeyPackagePublishCoordinator.ReplenishOutcome

  /// Sendable counter for observing how many times an injected operation runs.
  private actor Counter {
    private(set) var value = 0
    func increment() { value += 1 }
  }

  // MARK: - Pure decision

  @Test("Proceeds with no prior state")
  func decideProceedsFromClean() {
    let d = Coordinator.decide(
      now: Date(), lastSuccess: nil, debounceWindow: 30, backoffUntil: nil)
    #expect(d == .proceed)
  }

  @Test("Skips a redundant publish within the debounce window")
  func decideSkipsWithinDebounce() {
    let now = Date()
    let d = Coordinator.decide(
      now: now,
      lastSuccess: (date: now.addingTimeInterval(-5), available: 25),
      debounceWindow: 30,
      backoffUntil: nil)
    #expect(d == .skipRecentSuccess(lastAvailable: 25))
  }

  @Test("Proceeds again once the debounce window has elapsed")
  func decideProceedsAfterDebounce() {
    let now = Date()
    let d = Coordinator.decide(
      now: now,
      lastSuccess: (date: now.addingTimeInterval(-31), available: 25),
      debounceWindow: 30,
      backoffUntil: nil)
    #expect(d == .proceed)
  }

  @Test("Backoff takes precedence over a recent success")
  func decideBackoffWins() {
    let now = Date()
    let until = now.addingTimeInterval(8)
    let d = Coordinator.decide(
      now: now,
      lastSuccess: (date: now.addingTimeInterval(-1), available: 25),
      debounceWindow: 30,
      backoffUntil: until)
    #expect(d == .backoff(until: until))
  }

  @Test("Expired backoff no longer blocks")
  func decideExpiredBackoff() {
    let now = Date()
    let d = Coordinator.decide(
      now: now,
      lastSuccess: nil,
      debounceWindow: 30,
      backoffUntil: now.addingTimeInterval(-1))
    #expect(d == .proceed)
  }

  // MARK: - Rate-limit extraction

  @Test("Recognizes both rate-limit error types and ignores others")
  func rateLimitExtraction() {
    // MLSError carries an explicit hint.
    let mlsHint = Coordinator.rateLimitRetryAfter(for: MLSError.rateLimited(retryAfterSeconds: 3))
    #expect(mlsHint == .some(.some(3)))
    // MLSAPIError rate-limit with no hint: recognized (outer .some) but no value.
    let apiHint = Coordinator.rateLimitRetryAfter(for: MLSAPIError.rateLimited(retryAfter: nil))
    #expect(apiHint == .some(TimeInterval?.none))
    // Non-rate-limit error: not recognized.
    #expect(Coordinator.rateLimitRetryAfter(for: MLSError.operationFailed) == nil)
  }

  // MARK: - Coalescing behavior

  @Test("Concurrent callers collapse into a single publish")
  func coalescesConcurrentCallers() async throws {
    let coordinator = Coordinator(debounceWindow: 30, defaultBackoff: 10)
    let counter = Counter()

    let outcomes = try await withThrowingTaskGroup(of: Outcome.self) { group in
      for _ in 0..<12 {
        group.addTask {
          try await coordinator.run(key: "did:plc:alice") {
            await counter.increment()
            // Hold the in-flight task open so siblings pile onto it.
            try await Task.sleep(for: .milliseconds(50))
            return Outcome(available: 25, uploaded: 25)
          }
        }
      }
      var collected: [Outcome] = []
      for try await o in group { collected.append(o) }
      return collected
    }

    #expect(outcomes.count == 12)
    // The single-flight guard means exactly one real publish ran.
    #expect(await counter.value == 1)
    #expect(outcomes.allSatisfy { $0 == Outcome(available: 25, uploaded: 25) })
  }

  @Test("A successful publish suppresses a redundant follow-up within the window")
  func debounceSkipsFollowUp() async throws {
    let coordinator = Coordinator(debounceWindow: 30, defaultBackoff: 10)
    let counter = Counter()

    let first = try await coordinator.run(key: "did:plc:bob") {
      await counter.increment()
      return Outcome(available: 25, uploaded: 25)
    }
    let second = try await coordinator.run(key: "did:plc:bob") {
      await counter.increment()
      return Outcome(available: 99, uploaded: 99)
    }

    #expect(first == Outcome(available: 25, uploaded: 25))
    // Second call is skipped: reuses last availability, uploads nothing, never
    // invokes the operation.
    #expect(second == Outcome(available: 25, uploaded: 0))
    #expect(await counter.value == 1)
  }

  @Test("Distinct DIDs are not coalesced together")
  func distinctKeysAreIndependent() async throws {
    let coordinator = Coordinator(debounceWindow: 30, defaultBackoff: 10)
    let counter = Counter()

    _ = try await coordinator.run(key: "did:plc:alice") {
      await counter.increment()
      return Outcome(available: 25, uploaded: 25)
    }
    _ = try await coordinator.run(key: "did:plc:bob") {
      await counter.increment()
      return Outcome(available: 25, uploaded: 25)
    }

    #expect(await counter.value == 2)
  }

  @Test("A 429 arms a backoff window that short-circuits subsequent callers")
  func rateLimitArmsBackoff() async throws {
    let coordinator = Coordinator(debounceWindow: 30, defaultBackoff: 10)
    let counter = Counter()

    // First call fails with a rate-limit error.
    await #expect(throws: MLSAPIError.self) {
      _ = try await coordinator.run(key: "did:plc:carol") {
        await counter.increment()
        throw MLSAPIError.rateLimited(retryAfter: 5)
      }
    }

    // Second call must NOT invoke the operation — it is inside the backoff
    // window and should throw a rate-limit error instead of hammering.
    await #expect(throws: MLSError.self) {
      _ = try await coordinator.run(key: "did:plc:carol") {
        await counter.increment()
        return Outcome(available: 25, uploaded: 25)
      }
    }

    #expect(await counter.value == 1)
  }

  @Test("Backoff expires and a later publish proceeds")
  func backoffExpiresWithInjectedClock() async throws {
    // Drive time forward deterministically through the injected clock.
    let clock = ClockBox(start: Date(timeIntervalSince1970: 1_000))
    let coordinator = Coordinator(
      debounceWindow: 30, defaultBackoff: 10, now: { clock.read() })
    let counter = Counter()

    await #expect(throws: MLSAPIError.self) {
      _ = try await coordinator.run(key: "did:plc:dave") {
        await counter.increment()
        throw MLSAPIError.rateLimited(retryAfter: 5)  // backoff until t+5
      }
    }

    // Advance past the backoff and the debounce: the next publish proceeds.
    clock.advance(by: 40)
    let outcome = try await coordinator.run(key: "did:plc:dave") {
      await counter.increment()
      return Outcome(available: 25, uploaded: 25)
    }

    #expect(outcome == Outcome(available: 25, uploaded: 25))
    #expect(await counter.value == 2)
  }

  /// Thread-safe mutable clock for deterministic time control in tests.
  private final class ClockBox: @unchecked Sendable {
    private let lock = NSLock()
    private var current: Date
    init(start: Date) { current = start }
    func read() -> Date { lock.lock(); defer { lock.unlock() }; return current }
    func advance(by seconds: TimeInterval) {
      lock.lock(); defer { lock.unlock() }; current = current.addingTimeInterval(seconds)
    }
  }
}
