//
//  MLSAdvisoryLockCoordinator.swift
//  CatbirdMLSCore
//
//  POSIX Advisory Lock Coordinator for cross-process database synchronization.
//
//  ═══════════════════════════════════════════════════════════════════════════
//  SQLCipher NSE/App Database Coordination (Phase 4)
//  ═══════════════════════════════════════════════════════════════════════════
//
//  Problem: NSFileCoordinator coordinates at the file system level, but SQLite's
//  WAL mode uses internal file locking (fcntl) that operates independently.
//  This causes HMAC check failures when NSE and main app access the database
//  concurrently.
//
//  Solution: Implement cross-process coordination at the same level as SQLite's
//  internal locking using POSIX advisory locks (fcntl with F_SETLK).
//
//  Lock files are stored in the App Group container:
//  group.blue.catbird.shared/mls_locks/<sanitized_userDID>.lock
//
//  ═══════════════════════════════════════════════════════════════════════════

import Foundation
import OSLog

/// Coordinates exclusive database access between the main app and NSE using POSIX advisory locks.
///
/// Usage from NSE (before closing database):
/// ```swift
/// let locked = MLSAdvisoryLockCoordinator.shared.acquireExclusiveLock(for: userDID, timeout: 2.0)
/// defer { if locked { MLSAdvisoryLockCoordinator.shared.releaseExclusiveLock(for: userDID) } }
/// // ... perform checkpoint and close ...
/// ```
///
/// Usage from Main App (before opening database):
/// ```swift
/// let locked = MLSAdvisoryLockCoordinator.shared.acquireExclusiveLock(for: userDID, timeout: 3.0)
/// defer { if locked { MLSAdvisoryLockCoordinator.shared.releaseExclusiveLock(for: userDID) } }
/// // ... open database ...
/// ```
final class MLSAdvisoryLockCoordinator: @unchecked Sendable {
  
  // MARK: - Singleton
  
  static let shared = MLSAdvisoryLockCoordinator()
  
  // MARK: - Properties
  
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "AdvisoryLockCoordinator")
  
  /// Directory for lock files in the App Group container
  private let lockDirectory: URL
  
  /// Active file descriptors for held locks (keyed by userDID)
  private struct HeldLock {
    var fd: Int32
    var refCount: Int
  }

  private var heldLocks: [String: HeldLock] = [:]
  
  /// Serial queue for thread-safe lock management
  private let queue = DispatchQueue(label: "blue.catbird.mls.advisoryLockCoordinator")
  
  // MARK: - Initialization
  
  private init() {
    // Use App Group container for cross-process visibility
    if let sharedContainer = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared"
    ) {
      lockDirectory = sharedContainer.appendingPathComponent("mls_locks", isDirectory: true)
    } else {
      // Fallback to Application Support (won't work for NSE coordination, but prevents crash)
      let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
      lockDirectory = appSupport.appendingPathComponent("mls_locks", isDirectory: true)
      logger.warning("⚠️ [AdvisoryLock] App Group container not available - locks won't work for NSE")
    }
    
    // Create lock directory if needed
    do {
      try FileManager.default.createDirectory(at: lockDirectory, withIntermediateDirectories: true)
      logger.debug("🔐 [AdvisoryLock] Lock directory ready: \(self.lockDirectory.path)")
    } catch {
      logger.error("❌ [AdvisoryLock] Failed to create lock directory: \(error.localizedDescription)")
    }
  }
  
  // MARK: - Public API
  
  /// Acquire an exclusive lock for database access.
  ///
  /// This uses POSIX advisory locks (fcntl with F_SETLK) which operate at the same level
  /// as SQLite's internal locking, ensuring proper coordination.
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - timeout: Maximum time to wait for the lock (seconds)
  /// - Returns: true if lock was acquired, false if timed out
  func acquireExclusiveLock(for userDID: String, timeout: TimeInterval) -> Bool {
    return queue.sync {
      logger.info("🔐 [AdvisoryLock] Acquiring exclusive lock for: \(userDID.prefix(20), privacy: .private)")
      let startTime = Date()
      
      // Check if we already hold this lock
      if var held = heldLocks[userDID] {
        held.refCount += 1
        heldLocks[userDID] = held
        logger.debug("   Already holding lock for this user")
        return true
      }
      
      let lockPath = lockFilePath(for: userDID)
      
      // Open or create the lock file
      let fd = open(lockPath.path, O_RDWR | O_CREAT, 0o644)
      guard fd >= 0 else {
        logger.error("❌ [AdvisoryLock] Failed to open lock file: \(String(cString: strerror(errno)))")
        return false
      }
      
      // Configure the lock structure for exclusive lock on entire file
      var lockInfo = flock()
      lockInfo.l_type = Int16(F_WRLCK)   // Exclusive (write) lock
      lockInfo.l_whence = Int16(SEEK_SET)
      lockInfo.l_start = 0
      lockInfo.l_len = 0                  // 0 means lock entire file
      
      // Attempt to acquire lock with polling for timeout
      let pollInterval: TimeInterval = 0.05  // 50ms
      
      while true {
        // Try non-blocking lock acquisition
        let result = fcntl(fd, F_SETLK, &lockInfo)
        
        if result == 0 {
          // Lock acquired successfully
          heldLocks[userDID] = HeldLock(fd: fd, refCount: 1)
          let duration = Date().timeIntervalSince(startTime)
          let durationStr = String(format: "%.2f", duration)
          logger.info("🔐 [AdvisoryLock] Acquired exclusive lock in \(durationStr)s for: \(userDID.prefix(20), privacy: .private)")
          return true
        }
        
        // Check if we've timed out
        let elapsed = Date().timeIntervalSince(startTime)
        if elapsed >= timeout {
          close(fd)
          let timeoutStr = String(format: "%.1f", timeout)
          logger.warning("⏱️ [AdvisoryLock] Lock timeout after \(timeoutStr)s for: \(userDID.prefix(20), privacy: .private)")
          return false
        }
        
        // Wait before retrying
        Thread.sleep(forTimeInterval: pollInterval)
      }
    }
  }
  
  /// Try to acquire an exclusive lock for database access without blocking.
  ///
  /// Use this for the NSE to detect that the main app currently holds the lock and
  /// must be the sole writer/ratchet-advancer.
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: true if lock was acquired, false if another process holds it
  func tryAcquireExclusiveLock(for userDID: String) -> Bool {
    return queue.sync {
      // Check if we already hold this lock
      if var held = heldLocks[userDID] {
        held.refCount += 1
        heldLocks[userDID] = held
        return true
      }

      let lockPath = lockFilePath(for: userDID)
      let fd = open(lockPath.path, O_RDWR | O_CREAT, 0o644)
      guard fd >= 0 else {
        logger.error("❌ [AdvisoryLock] Failed to open lock file: \(String(cString: strerror(errno)))")
        return false
      }

      var lockInfo = flock()
      lockInfo.l_type = Int16(F_WRLCK)
      lockInfo.l_whence = Int16(SEEK_SET)
      lockInfo.l_start = 0
      lockInfo.l_len = 0

      let result = fcntl(fd, F_SETLK, &lockInfo)
      if result == 0 {
        heldLocks[userDID] = HeldLock(fd: fd, refCount: 1)
        logger.debug("🔐 [AdvisoryLock] Acquired (non-blocking) for: \(userDID.prefix(20), privacy: .private)")
        return true
      }

      close(fd)
      logger.debug("🔒 [AdvisoryLock] Busy (non-blocking) for: \(userDID.prefix(20), privacy: .private)")
      return false
    }
  }

  /// Release an exclusive lock for database access.
  ///
  /// - Parameter userDID: User's decentralized identifier
  func releaseExclusiveLock(for userDID: String) {
    queue.sync {
      guard var held = heldLocks[userDID] else {
        logger.debug("🔓 [AdvisoryLock] No lock held for: \(userDID.prefix(20), privacy: .private)")
        return
      }

      held.refCount -= 1
      if held.refCount > 0 {
        heldLocks[userDID] = held
        logger.debug("🔓 [AdvisoryLock] Decremented refCount=\(held.refCount) for: \(userDID.prefix(20), privacy: .private)")
        return
      }

      let fd = held.fd
      heldLocks.removeValue(forKey: userDID)
      
      // Configure unlock structure
      var lockInfo = flock()
      lockInfo.l_type = Int16(F_UNLCK)
      lockInfo.l_whence = Int16(SEEK_SET)
      lockInfo.l_start = 0
      lockInfo.l_len = 0
      
      // Release the lock
      _ = fcntl(fd, F_SETLK, &lockInfo)
      
      // Close the file descriptor
      close(fd)
      
      logger.info("🔓 [AdvisoryLock] Released lock for: \(userDID.prefix(20), privacy: .private)")
    }
  }
  
  /// Check if we currently hold a lock for the specified user.
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: true if we hold the lock
  func isLockHeld(for userDID: String) -> Bool {
    return queue.sync {
      return heldLocks[userDID] != nil
    }
  }
  
  /// Release all held locks.
  /// Call this during cleanup/shutdown.
  func releaseAllLocks() {
    queue.sync {
      for (userDID, held) in heldLocks {
        let fd = held.fd
        var lockInfo = flock()
        lockInfo.l_type = Int16(F_UNLCK)
        lockInfo.l_whence = Int16(SEEK_SET)
        lockInfo.l_start = 0
        lockInfo.l_len = 0
        
        _ = fcntl(fd, F_SETLK, &lockInfo)
        close(fd)
        
        logger.debug("🔓 [AdvisoryLock] Released lock during cleanup: \(userDID.prefix(20), privacy: .private)")
      }
      heldLocks.removeAll()
      logger.info("🔓 [AdvisoryLock] All locks released")
    }
  }
  
  // MARK: - Private Helpers
  
  /// Get the lock file path for a user.
  private func lockFilePath(for userDID: String) -> URL {
    let sanitized = sanitizeDID(userDID)
    return lockDirectory.appendingPathComponent("\(sanitized).lock")
  }
  
  /// Sanitize DID for filesystem compatibility.
  private func sanitizeDID(_ userDID: String) -> String {
    userDID
      .replacingOccurrences(of: ":", with: "-")
      .replacingOccurrences(of: "/", with: "-")
      .replacingOccurrences(of: "#", with: "-")
      .replacingOccurrences(of: "?", with: "-")
  }
}
