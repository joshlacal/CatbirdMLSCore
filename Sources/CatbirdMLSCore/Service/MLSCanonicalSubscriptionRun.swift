import Foundation
import Synchronization

/// A lifetime fence shared across executors. Replacement invalidates the old
/// run even when its synchronous Rust callback cannot observe cancellation.
internal final class MLSCanonicalSubscriptionRun: Sendable {
  private let valid = Mutex(true)

  var isValid: Bool { valid.withLock { $0 } }

  func invalidate() { valid.withLock { $0 = false } }

  func check() throws {
    try Task.checkCancellation()
    guard isValid else { throw CancellationError() }
  }

  /// Cursor persistence is synchronous under this fence. Invalidation cannot
  /// race the MainActor hop between a validity check and the durable write.
  func whileValid<T>(_ operation: () throws -> T) throws -> T {
    try Task.checkCancellation()
    return try valid.withLock { valid in
      guard valid else { throw CancellationError() }
      return try operation()
    }
  }
}
