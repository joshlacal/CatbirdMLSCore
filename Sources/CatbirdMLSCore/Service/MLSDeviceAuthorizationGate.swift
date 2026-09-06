import Foundation

/// Publication is scoped to the active account, device and manager session.
/// A failed attempt cannot unlock protocol work; the next operation retries.
internal actor MLSDeviceAuthorizationGate {
  private var readyScope: String?
  private var pending: (scope: String, task: Task<Void, Error>)?
  func ensure(scope: String, _ publish: @escaping @Sendable () async throws -> Void) async throws {
    try Task.checkCancellation()
    if readyScope == scope { return }
    if let pending {
      guard pending.scope == scope else { throw CancellationError() }
      try await pending.task.value
      try Task.checkCancellation()
      return
    }
    let task = Task.detached { try await publish() }
    pending = (scope, task)
    do {
      try await task.value
      readyScope = scope
      pending = nil
    } catch {
      pending = nil
      throw error
    }
    try Task.checkCancellation()
  }
}
