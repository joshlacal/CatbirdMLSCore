import Foundation

/// Async admission gate bounding the number of concurrent synchronous Rust orchestrator
/// bridge calls occupying OS threads simultaneously.
///
/// Bounding concurrent execution to a small fixed limit (4) prevents thread pool
/// saturation in GCD's workqueue pool and cooperative thread starvation in the Swift
/// runtime when Rust threads block on UniFFI callbacks (`MLSOrchestratorAPIAdapter.blocking`)
/// or internal locks (`direct_request_operation_lock`). Callers beyond the limit wait as
/// suspended Swift async Tasks without holding any OS threads.
internal actor RustAuthorityAdmissionGate {
  /// Maximum number of synchronous Rust orchestrator operations allowed to occupy
  /// threads simultaneously.
  ///
  /// Value: 4.
  /// Justification:
  /// 1. Hardware alignment: Apple Silicon devices typically have 4-6 high-performance/efficiency
  ///    cores. 4 threads efficiently utilize core capacity without inducing thread contention.
  /// 2. Tokio worker alignment: The underlying `catbird-mls` Tokio multi-thread runtime defaults to
  ///    4 worker threads. Admitting more than 4 concurrent `Runtime::block_on` callers causes
  ///    thread queuing in Tokio without increasing crypto or disk throughput.
  /// 3. Swift cooperative pool protection: The Swift cooperative thread pool allocates 1 thread
  ///    per logical core. When synchronous Rust operations invoke synchronous UniFFI callbacks
  ///    bridging into async Swift networking (`MLSOrchestratorAPIAdapter.blocking`), each detached
  ///    Task requires a free cooperative thread. Limiting blocking workers to at most 4 guarantees
  ///    cooperative worker threads remain available to schedule and complete detached tasks.
  internal static let maxConcurrentRustAuthoritativeOperations = 4

  private let maxConcurrent: Int
  private var activeCount: Int = 0
  private var waiters: [CheckedContinuation<Void, Never>] = []

  internal init(maxConcurrent: Int = RustAuthorityAdmissionGate.maxConcurrentRustAuthoritativeOperations) {
    self.maxConcurrent = maxConcurrent
  }

  internal func withPermit<T>(
    operation: () async throws -> T
  ) async rethrows -> T {
    await acquire()
    defer { release() }
    return try await operation()
  }

  private func acquire() async {
    if activeCount < maxConcurrent {
      activeCount += 1
      return
    }
    await withCheckedContinuation { continuation in
      waiters.append(continuation)
    }
  }

  private func release() {
    if !waiters.isEmpty {
      let next = waiters.removeFirst()
      next.resume()
    } else {
      activeCount = max(0, activeCount - 1)
    }
  }

  #if DEBUG
  internal var currentActiveCount: Int { activeCount }
  internal var currentWaiterCount: Int { waiters.count }
  #endif
}
