import Foundation

enum MLSUserOperationContext {
  @TaskLocal static var heldUserDIDs: Set<String> = []
}

struct MLSUserOperationPermit: Sendable {
  fileprivate let userDID: String
  fileprivate let id: UUID
}

enum MLSUserOperationCoordinatorError: Error, Sendable {
  case timeout
}

/// Serializes MLS operations that mutate per-user state (Rust MLS SQLite + Swift SQLCipher).
///
/// This is intentionally process-local; cross-process safety is provided separately by
/// `MLSDatabaseCoordinator` (NSFileCoordinator) using per-user lock files.
actor MLSUserOperationCoordinator {
  static let shared = MLSUserOperationCoordinator()

  private var owners: [String: UUID] = [:]

  private enum WaiterContinuation {
    case nonThrowing(CheckedContinuation<MLSUserOperationPermit, Never>)
    case throwing(CheckedContinuation<MLSUserOperationPermit, Error>)
  }

  private struct Waiter {
    let id: UUID
    let continuation: WaiterContinuation
  }

  private var waiters: [String: [Waiter]] = [:]

  private init() {}

  /// Acquire an exclusive permit for a user's MLS state.
  func acquire(for userDID: String) async -> MLSUserOperationPermit {
    if owners[userDID] == nil {
      let permit = MLSUserOperationPermit(userDID: userDID, id: UUID())
      owners[userDID] = permit.id
      return permit
    }

    return await withCheckedContinuation { continuation in
      let waiter = Waiter(id: UUID(), continuation: .nonThrowing(continuation))
      waiters[userDID, default: []].append(waiter)
    }
  }

  /// Acquire an exclusive permit with a bounded timeout.
  ///
  /// This is cancellation-safe: if the caller is cancelled while waiting, it will
  /// be removed from the queue and will not "leak" a continuation.
  func acquire(for userDID: String, timeout: Duration) async throws -> MLSUserOperationPermit {
    if owners[userDID] == nil {
      let permit = MLSUserOperationPermit(userDID: userDID, id: UUID())
      owners[userDID] = permit.id
      return permit
    }

    let waiterID = UUID()

    return try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        let waiter = Waiter(id: waiterID, continuation: .throwing(continuation))
        waiters[userDID, default: []].append(waiter)

        Task { [weak self] in
          do {
            try await Task.sleep(for: timeout)
          } catch {
            return
          }
          await self?.timeoutWaiter(userDID: userDID, waiterID: waiterID)
        }
      }
    } onCancel: {
      Task { [weak self] in
        await self?.cancelWaiter(userDID: userDID, waiterID: waiterID)
      }
    }
  }

  private func cancelWaiter(userDID: String, waiterID: UUID) {
    guard var queue = waiters[userDID] else { return }
    guard let index = queue.firstIndex(where: { $0.id == waiterID }) else { return }

    let waiter = queue.remove(at: index)
    waiters[userDID] = queue.isEmpty ? nil : queue

    if case .throwing(let continuation) = waiter.continuation {
      continuation.resume(throwing: CancellationError())
    }
  }

  private func timeoutWaiter(userDID: String, waiterID: UUID) {
    guard var queue = waiters[userDID] else { return }
    guard let index = queue.firstIndex(where: { $0.id == waiterID }) else { return }

    let waiter = queue.remove(at: index)
    waiters[userDID] = queue.isEmpty ? nil : queue

    if case .throwing(let continuation) = waiter.continuation {
      continuation.resume(throwing: MLSUserOperationCoordinatorError.timeout)
    }
  }

  func release(_ permit: MLSUserOperationPermit) {
    guard owners[permit.userDID] == permit.id else { return }

    if var queue = waiters[permit.userDID], !queue.isEmpty {
      let next = MLSUserOperationPermit(userDID: permit.userDID, id: UUID())
      owners[permit.userDID] = next.id

      let waiter = queue.removeFirst()
      waiters[permit.userDID] = queue.isEmpty ? nil : queue

      switch waiter.continuation {
      case .nonThrowing(let continuation):
        continuation.resume(returning: next)
      case .throwing(let continuation):
        continuation.resume(returning: next)
      }
    } else {
      owners[permit.userDID] = nil
      waiters[permit.userDID] = nil
    }
  }
}

/// Acquire a per-user permit without breaking actor isolation in the caller.
///
/// This function does *not* require a @Sendable closure, so it can be used inside actors
/// (e.g. MLSCoreContext / MLSGRDBManager / MLSClient) without triggering isolation errors.
func withMLSUserPermit<T>(
  for userDID: String,
  operation: () async throws -> T
) async rethrows -> T {
  let held = MLSUserOperationContext.heldUserDIDs
  if held.contains(userDID) {
    return try await operation()
  }

  let permit = await MLSUserOperationCoordinator.shared.acquire(for: userDID)
  defer { Task { await MLSUserOperationCoordinator.shared.release(permit) } }

  return try await MLSUserOperationContext.$heldUserDIDs.withValue(held.union([userDID])) {
    try await operation()
  }
}
