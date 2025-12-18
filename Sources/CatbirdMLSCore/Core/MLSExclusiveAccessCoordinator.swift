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
/// Lock order is enforced:
/// 1) process-local per-user permit
/// 2) cross-process POSIX advisory lock
/// 3) caller opens/uses/closes database handles
/// 4) caller checkpoints (when needed)
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

  do {
    let lockStart = clock.now
    try await acquireAdvisoryLock(
      userDID: userDID,
      purpose: purpose,
      deadline: deadline,
      clock: clock,
      logger: logger
    )
    let waited = lockStart.duration(to: clock.now)
    if waited > .zero {
      logger.debug(
        "🔐 [Exclusive] Advisory lock acquired for \(userDID.prefix(20), privacy: .private) in \(String(describing: waited)) (\(purpose.rawValue, privacy: .public))"
      )
    }
  } catch is CancellationError {
    throw CancellationError()
  } catch let error as MLSExclusiveAccessError {
    throw error
  } catch {
    logger.error(
      "🚫 [Exclusive] Advisory lock acquisition failed for \(userDID.prefix(20), privacy: .private): \(error.localizedDescription, privacy: .public)"
    )
    throw error
  }

  defer { MLSAdvisoryLockCoordinator.shared.releaseExclusiveLock(for: userDID) }

  return try await MLSExclusiveAccessContext.$heldUserDIDs.withValue(held.union([userDID])) {
    try await operation()
  }
}

private func acquireAdvisoryLock(
  userDID: String,
  purpose: MLSExclusivePurpose,
  deadline: ContinuousClock.Instant,
  clock: ContinuousClock,
  logger: Logger
) async throws {
  var delay = Duration.milliseconds(10)
  let maxDelay = Duration.milliseconds(250)

  while clock.now < deadline {
    try Task.checkCancellation()

    if MLSAdvisoryLockCoordinator.shared.tryAcquireExclusiveLock(for: userDID) {
      return
    }

    // Backoff with jitter, bounded by remaining time.
    let jittered = delay + .milliseconds(Int.random(in: 0...20))
    let remaining = clock.now.duration(to: deadline)
    if remaining <= .zero {
      break
    }

    try await Task.sleep(for: min(jittered, remaining))
    delay = min(delay + delay, maxDelay)
  }

  logger.warning(
    "⏱️ [Exclusive] Advisory lock timeout for \(userDID.prefix(20), privacy: .private) (\(purpose.rawValue, privacy: .public))"
  )
  throw MLSExclusiveAccessError.timedOut(userDID: userDID, purpose: purpose, phase: .advisoryLock)
}

/// Probe cross-process storage exclusivity without holding the lock.
///
/// This is intended as a "gate" (e.g., for the NSE) to decide whether to proceed with
/// MLS/SQLCipher work when the main app may be active. It acquires the underlying advisory
/// lock non-blocking and immediately releases it.
public func tryAcquireMLSCrossProcessStorageGate(userDID: String) -> Bool {
  if MLSAdvisoryLockCoordinator.shared.tryAcquireExclusiveLock(for: userDID) {
    MLSAdvisoryLockCoordinator.shared.releaseExclusiveLock(for: userDID)
    return true
  }
  return false
}
