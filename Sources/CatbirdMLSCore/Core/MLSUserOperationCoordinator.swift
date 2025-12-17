import Foundation

public enum MLSUserOperationContext {
  @TaskLocal public static var heldUserDIDs: Set<String> = []
}

public struct MLSUserOperationPermit: Sendable {
  fileprivate let userDID: String
  fileprivate let id: UUID
}

/// Serializes MLS operations that mutate per-user state (Rust MLS SQLite + Swift SQLCipher).
///
/// This is intentionally process-local; cross-process safety is provided separately by
/// `MLSDatabaseCoordinator` (NSFileCoordinator) using per-user lock files.
public actor MLSUserOperationCoordinator {
  public static let shared = MLSUserOperationCoordinator()

  private var owners: [String: UUID] = [:]
  private var waiters: [String: [CheckedContinuation<MLSUserOperationPermit, Never>]] = [:]

  private init() {}

  /// Acquire an exclusive permit for a user's MLS state.
  public func acquire(for userDID: String) async -> MLSUserOperationPermit {
    if owners[userDID] == nil {
      let permit = MLSUserOperationPermit(userDID: userDID, id: UUID())
      owners[userDID] = permit.id
      return permit
    }

    return await withCheckedContinuation { continuation in
      waiters[userDID, default: []].append(continuation)
    }
  }

  public func release(_ permit: MLSUserOperationPermit) {
    guard owners[permit.userDID] == permit.id else { return }

    if var queue = waiters[permit.userDID], !queue.isEmpty {
      let next = MLSUserOperationPermit(userDID: permit.userDID, id: UUID())
      owners[permit.userDID] = next.id
      let cont = queue.removeFirst()
      waiters[permit.userDID] = queue.isEmpty ? nil : queue
      cont.resume(returning: next)
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
public func withMLSUserPermit<T>(
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
