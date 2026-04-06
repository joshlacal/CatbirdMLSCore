import Foundation
import OSLog

public enum MLSExclusiveAccessContext {
  @TaskLocal public static var heldUserDIDs: Set<String> = []
}

public enum MLSExclusivePurpose: String, Sendable {
  case decrypt
  case decryptBatch
  case ffiMutation
  case checkpoint
  case closeAndDrain
  case accountSwitch
  case maintenance
  case other
}

public enum MLSExclusiveAccessError: Error, LocalizedError, Sendable {
  public enum Phase: String, Sendable {
    case permit
    /// Deprecated: Advisory locks removed. Only permit phase is used now.
    @available(*, deprecated, message: "Advisory locks removed. SQLite WAL handles concurrent access.")
    case advisoryLock
  }

  case timedOut(userDID: String, purpose: MLSExclusivePurpose, phase: Phase)

  public var errorDescription: String? {
    switch self {
    case .timedOut(_, let purpose, let phase):
      return "MLS exclusive access timed out during \(phase.rawValue) acquisition (\(purpose.rawValue))"
    }
  }
}

@inlinable
public func isMLSExclusiveAccessHeld(for userDID: String) -> Bool {
  MLSExclusiveAccessContext.heldUserDIDs.contains(userDID)
}

public func assertMLSExclusiveAccessHeld(
  for userDID: String,
  file: StaticString = #fileID,
  line: UInt = #line
) {
  #if DEBUG
    precondition(
      isMLSExclusiveAccessHeld(for: userDID),
      "MLS exclusive access not held for userDID",
      file: file,
      line: line
    )
  #endif
}

/// The only supported way to perform MLS operations that can mutate on-disk state.
///
/// Coordination:
/// 1) process-local per-user permit (prevents intra-process races)
/// 2) caller opens/uses/closes database handles (SQLite WAL handles concurrent access)
/// 3) caller checkpoints (when needed)
///
/// Cross-process coordination uses `MLSStateChangeNotifier` / `MLSNotificationCoordinator`
/// instead of file locks.
/// This avoids 0xdead10cc crashes when iOS suspends the app with held file locks.
public func withMLSExclusiveAccess<T>(
  userDID: String,
  purpose: MLSExclusivePurpose,
  timeout: Duration = .seconds(15),
  _ operation: () async throws -> T
) async throws -> T {
  let held = MLSExclusiveAccessContext.heldUserDIDs
  if held.contains(userDID) {
    return try await operation()
  }

  let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSExclusiveAccess")
  let clock = ContinuousClock()
  let started = clock.now
  let deadline = started.advanced(by: timeout)

  func timeRemaining() -> Duration {
    max(.zero, clock.now.duration(to: deadline))
  }

  let permit: MLSUserOperationPermit
  do {
    let permitStart = clock.now
    permit = try await MLSUserOperationCoordinator.shared.acquire(for: userDID, timeout: timeRemaining())
    let waited = permitStart.duration(to: clock.now)
    if waited > .zero {
      logger.debug(
        "🎟️ [Exclusive] Permit acquired for \(userDID.prefix(20), privacy: .private) in \(String(describing: waited)) (\(purpose.rawValue, privacy: .public))"
      )
    }
  } catch is CancellationError {
    throw CancellationError()
  } catch MLSUserOperationCoordinatorError.timeout {
    logger.warning(
      "⏱️ [Exclusive] Permit timeout for \(userDID.prefix(20), privacy: .private) (\(purpose.rawValue, privacy: .public))"
    )
    throw MLSExclusiveAccessError.timedOut(userDID: userDID, purpose: purpose, phase: .permit)
  } catch {
    logger.error(
      "🚫 [Exclusive] Permit acquisition failed for \(userDID.prefix(20), privacy: .private): \(error.localizedDescription, privacy: .public)"
    )
    throw error
  }

  defer { Task { await MLSUserOperationCoordinator.shared.release(permit) } }

  // No advisory lock needed - SQLite WAL handles concurrent access
  // Cross-process coordination uses `MLSStateChangeNotifier` / `MLSNotificationCoordinator`

  return try await MLSExclusiveAccessContext.$heldUserDIDs.withValue(held.union([userDID])) {
    try await operation()
  }
}

/// Probe cross-process storage exclusivity.
///
/// This function previously used advisory file locks to coordinate between processes.
/// Now it always returns true since SQLite WAL mode handles concurrent access and
/// cross-process coordination uses `MLSStateChangeNotifier` / `MLSNotificationCoordinator`.
///
/// Kept for API compatibility with existing callers.
@available(*, deprecated, message: "Advisory locks removed. SQLite WAL handles concurrent access.")
public func tryAcquireMLSCrossProcessStorageGate(userDID: String) -> Bool {
  // Always return true - SQLite WAL handles concurrent access
  // Cross-process coordination uses `MLSStateChangeNotifier` / `MLSNotificationCoordinator`
  return true
}

// MARK: - Database Operation Wrapper

/// Execute a database operation with automatic operation ticket management.
///
/// This is the recommended way to perform MLS database operations. It:
/// 1. Acquires an operation ticket (atomically rejected during shutdown)
/// 2. Executes the operation with exclusive access coordination
/// 3. Releases the ticket when complete
///
/// - Parameters:
///   - userDID: User's decentralized identifier
///   - purpose: The purpose of this operation (for logging/debugging)
///   - operation: The async operation to perform
/// - Returns: The result of the operation
/// - Throws: `MLSGateError` if the gate is closing/closed,
///           or any error from the operation or exclusives access coordination
public func withMLSDatabaseOperation<T: Sendable>(
  for userDID: String,
  purpose: MLSExclusivePurpose = .other,
  _ operation: @Sendable () async throws -> T
) async throws -> T {
  // 1. Acquire connection token (atomic rejection during gate closing/closed)
  let token = try await MLSDatabaseGate.shared.acquireConnection(for: userDID)

  // Ensure token is released even on error/cancellation
  defer {
    Task {
      await MLSDatabaseGate.shared.releaseConnection(token)
    }
  }

  // 2. Use existing exclusive access coordination
  return try await withMLSExclusiveAccess(userDID: userDID, purpose: purpose) {
    try await operation()
  }
}
