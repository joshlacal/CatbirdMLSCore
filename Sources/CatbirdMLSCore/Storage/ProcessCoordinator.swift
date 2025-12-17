import Foundation
import Darwin

/// Cross-process coordinator using BSD flock on a shared App Group file.
public final class ProcessCoordinator {
  public static let shared = ProcessCoordinator()

  private let lockURL: URL
  private var fd: Int32 = -1

  private init() {
    let container = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared")!
    lockURL = container.appendingPathComponent("mls_state.lock")
  }

  /// Acquire an exclusive lock (blocking).
  private func enterExclusive() throws {
    fd = open(lockURL.path, O_CREAT | O_RDWR, 0o666)
    if fd < 0 {
      throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno))
    }
    if flock(fd, LOCK_EX) != 0 {
      let code = errno
      close(fd)
      fd = -1
      throw NSError(domain: NSPOSIXErrorDomain, code: Int(code))
    }
  }

  /// Release the lock.
  private func exitExclusive() {
    guard fd >= 0 else { return }
    flock(fd, LOCK_UN)
    close(fd)
    fd = -1
  }

  /// Perform an operation under exclusive lock.
  @discardableResult
  public func performExclusive<T>(_ block: () throws -> T) throws -> T {
    try enterExclusive()
    defer { exitExclusive() }
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
