import Foundation
import Darwin
import OSLog

/// Cross-process coordinator using BSD flock on a shared App Group file.
public final class ProcessCoordinator {
  public static let shared = ProcessCoordinator()

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "ProcessCoordinator")
  private let lockURL: URL

  private init() {
    if let container = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared"
    ) {
      lockURL = container.appendingPathComponent("mls_state.lock")
    } else {
      // Fallback: prevents crashes in environments without the App Group entitlement (e.g. unit tests).
      let base =
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
        ?? FileManager.default.temporaryDirectory
      let fallbackDir = base.appendingPathComponent("CatbirdMLSCore", isDirectory: true)
      try? FileManager.default.createDirectory(at: fallbackDir, withIntermediateDirectories: true)
      lockURL = fallbackDir.appendingPathComponent("mls_state.lock")
      logger.warning("⚠️ [ProcessCoordinator] App Group container not available; using fallback lock at \(self.lockURL.path, privacy: .private)")
    }
  }

  /// Perform an operation under exclusive lock.
  @discardableResult
  public func performExclusive<T>(_ block: () throws -> T) throws -> T {
    let fd = open(lockURL.path, O_CREAT | O_RDWR, 0o666)
    if fd < 0 {
      throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno))
    }
    defer {
      flock(fd, LOCK_UN)
      close(fd)
    }

    if flock(fd, LOCK_EX) != 0 {
      throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno))
    }

    return try block()
  }

  /// Try to acquire exclusive lock without blocking.
  ///
  /// Use this to check if NSE is currently holding the lock (to show spinner).
  ///
  /// - Returns: true if lock is available (no one else holds it), false if busy
  public func tryExclusiveNonBlocking() -> Bool {
    let fd = open(lockURL.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      return true  // Can't open = assume available
    }

    // LOCK_NB = Non-blocking
    let result = flock(fd, LOCK_EX | LOCK_NB)
    if result == 0 {
      // We got the lock - release it immediately
      flock(fd, LOCK_UN)
      close(fd)
      return true
    } else if errno == EWOULDBLOCK {
      // Lock is held by another process
      close(fd)
      return false
    } else {
      // Some other error - assume available
      close(fd)
      return true
    }
  }
}
