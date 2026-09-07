import XCTest

@testable import CatbirdMLSCore

/// The startup fan-in (UI reloads, stream events, notifications, reconcile)
/// used to issue one `getConversations` pagination each and earn HTTP 429,
/// which paused the live event stream and delayed message delivery.
final class MLSServerSyncCoalescerTests: XCTestCase {
  private actor RunCounter {
    private(set) var count = 0
    func increment() { count += 1 }
  }

  func testConcurrentRequestsShareOneServerRoundTrip() async throws {
    let coalescer = MLSServerSyncCoalescer()
    let counter = RunCounter()

    await withThrowingTaskGroup(of: Void.self) { group in
      for _ in 0..<8 {
        group.addTask {
          try await coalescer.run(fullSync: false) {
            await counter.increment()
            try await Task.sleep(nanoseconds: 80_000_000)
          }
        }
      }
      while (try? await group.next()) != nil {}
    }

    let runs = await counter.count
    XCTAssertEqual(runs, 1)
  }

  /// A nested `syncWithServer` from inside a running sync (the send preflight
  /// "group not in FFI → sync-fix" path) must run, not await its own parent.
  func testNestedSyncInsideRunningSyncRunsWithoutDeadlock() async throws {
    let coalescer = MLSServerSyncCoalescer()
    let counter = RunCounter()

    let nested = Task {
      try await coalescer.run(fullSync: false) {
        try await coalescer.run(fullSync: false) { await counter.increment() }
        await counter.increment()
      }
    }
    let timeout = Task {
      try await Task.sleep(nanoseconds: 3_000_000_000)
      nested.cancel()
    }
    _ = try await nested.value
    timeout.cancel()

    let runs = await counter.count
    XCTAssertEqual(runs, 2)
  }

  func testSequentialRequestsEachSync() async throws {
    let coalescer = MLSServerSyncCoalescer()
    let counter = RunCounter()

    for _ in 0..<3 {
      let performed = try await coalescer.run(fullSync: false) { await counter.increment() }
      XCTAssertTrue(performed)
    }

    let runs = await counter.count
    XCTAssertEqual(runs, 3)
  }

  func testFullSyncIsNotSatisfiedByInFlightIncrementalSync() async throws {
    let coalescer = MLSServerSyncCoalescer()
    let incrementalStarted = expectation(description: "incremental started")
    let counter = RunCounter()

    let incremental = Task {
      try await coalescer.run(fullSync: false) {
        incrementalStarted.fulfill()
        await counter.increment()
        try await Task.sleep(nanoseconds: 120_000_000)
      }
    }
    await fulfillment(of: [incrementalStarted], timeout: 2)

    let performed = try await coalescer.run(fullSync: true) { await counter.increment() }
    _ = try await incremental.value

    XCTAssertTrue(performed)
    let runs = await counter.count
    XCTAssertEqual(runs, 2)
  }

  func testIncrementalSyncJoinsInFlightFullSync() async throws {
    let coalescer = MLSServerSyncCoalescer()
    let fullStarted = expectation(description: "full started")
    let counter = RunCounter()

    let full = Task {
      try await coalescer.run(fullSync: true) {
        fullStarted.fulfill()
        await counter.increment()
        try await Task.sleep(nanoseconds: 120_000_000)
      }
    }
    await fulfillment(of: [fullStarted], timeout: 2)

    let performed = try await coalescer.run(fullSync: false) { await counter.increment() }
    _ = try await full.value

    XCTAssertFalse(performed)
    let runs = await counter.count
    XCTAssertEqual(runs, 1)
  }

  func testFailurePropagatesToJoinedRequests() async throws {
    struct SyncFailure: Error {}
    let coalescer = MLSServerSyncCoalescer()
    let started = expectation(description: "first sync started")

    let first = Task {
      try await coalescer.run(fullSync: false) {
        started.fulfill()
        try await Task.sleep(nanoseconds: 100_000_000)
        throw SyncFailure()
      }
    }
    await fulfillment(of: [started], timeout: 2)

    var joinerFailed = false
    do {
      _ = try await coalescer.run(fullSync: false) {
        XCTFail("joined request must not issue its own sync")
      }
    } catch is SyncFailure {
      joinerFailed = true
    }

    var ownerFailed = false
    do { _ = try await first.value } catch is SyncFailure { ownerFailed = true }

    XCTAssertTrue(joinerFailed)
    XCTAssertTrue(ownerFailed)
  }

  func testFailureDoesNotPoisonLaterSyncs() async throws {
    struct SyncFailure: Error {}
    let coalescer = MLSServerSyncCoalescer()
    let counter = RunCounter()

    do {
      _ = try await coalescer.run(fullSync: false) { throw SyncFailure() }
      XCTFail("expected failure")
    } catch is SyncFailure {}

    let performed = try await coalescer.run(fullSync: false) { await counter.increment() }

    XCTAssertTrue(performed)
    let runs = await counter.count
    XCTAssertEqual(runs, 1)
  }
}
