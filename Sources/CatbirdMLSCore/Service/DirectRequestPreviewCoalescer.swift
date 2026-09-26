@preconcurrency import CatbirdMLS
import Foundation

extension DirectRequestView: @unchecked Sendable {}

/// Request coalescer for in-flight direct request preview refreshes.
///
/// Prevents concurrent callers for the same conversation ID from independently
/// storming the Rust engine and contending for `direct_request_operation_lock`.
/// Concurrent callers for the same conversation ID share the result of a single
/// in-flight refresh operation.
internal actor DirectRequestPreviewCoalescer {
  private var inFlight: [String: Task<DirectRequestView, Error>] = [:]

  internal init() {}

  internal func coalesce(
    conversationId: String,
    operation: @Sendable @escaping () async throws -> DirectRequestView
  ) async throws -> DirectRequestView {
    if let existing = inFlight[conversationId] {
      return try await existing.value
    }
    let task = Task<DirectRequestView, Error> {
      try await operation()
    }
    inFlight[conversationId] = task
    do {
      let result = try await task.value
      inFlight.removeValue(forKey: conversationId)
      return result
    } catch {
      inFlight.removeValue(forKey: conversationId)
      throw error
    }
  }

  #if DEBUG
  internal var inFlightCount: Int { inFlight.count }
  #endif
}
