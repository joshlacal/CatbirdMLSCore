//
//  MLSStateVersionManager.swift
//  CatbirdMLSCore
//
//  Monotonic state versioning for cross-process MLS state synchronization.
//
//  ═══════════════════════════════════════════════════════════════════════════
//  MONOTONIC STATE VERSIONING (2024-12)
//  ═══════════════════════════════════════════════════════════════════════════
//
//  Problem: The NSE and main app can have divergent in-memory MLS state.
//  When NSE decrypts a message, it advances the ratchet (Epoch N → N+1) on disk.
//  The main app's in-memory context still holds Epoch N and tries to decrypt
//  with a stale key, causing SecretReuseError or DecryptionFailed.
//
//  Solution: Monotonic State Versioning
//  - Every MLS state change (ratchet advance, commit, etc.) increments a version counter
//  - Version is stored in UserDefaults (fast cross-process read, no SQLite locking)
//  - Before decryption, app checks: diskVersion > memoryVersion → reload context
//  - This is FASTER than waiting for Darwin notifications (which have latency)
//
//  Architecture:
//  1. NSE: After any MLS state change → increment version
//  2. App: Before MLS operation → check version → reload if stale
//  3. Both: Best-effort atomic increment (no file locks to avoid 0xdead10cc)
//
//  Version storage:
//  - Per-user: "mls_state_version.<userDID_hash>" in shared UserDefaults suite
//  - Global: "mls_global_state_version" for any-user changes
//
//  ═══════════════════════════════════════════════════════════════════════════

import CryptoKit
import Foundation
import OSLog

/// Notification posted when MLS state version is incremented locally.
/// Apps can observe this to trigger immediate state reload.
public let MLSStateVersionDidChangeNotification = Notification.Name("MLSStateVersionDidChange")

/// Manager for monotonic MLS state versioning across processes.
///
/// This enables the main app to detect when NSE has advanced the MLS ratchet
/// without waiting for Darwin notification latency.
///
/// Usage from NSE (after decrypting):
/// ```swift
/// MLSStateVersionManager.shared.incrementVersion(for: userDID)
/// ```
///
/// Usage from Main App (before decrypting):
/// ```swift
/// if MLSStateVersionManager.shared.isContextStale(for: userDID, memoryVersion: context.version) {
///     await reloadContext(for: userDID)
/// }
/// ```
public final class MLSStateVersionManager: @unchecked Sendable {

  // MARK: - Singleton

  public static let shared = MLSStateVersionManager()

  // MARK: - Constants

  private static let suiteName = "group.blue.catbird.shared"
  private static let globalVersionKey = "mls_global_state_version"
  private static let userVersionKeyPrefix = "mls_state_version."
  private static let lastKnownVersionKeyPrefix = "mls_last_known_version."

  // MARK: - Properties

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "StateVersionManager")

  /// Shared UserDefaults suite for cross-process access
  private let sharedDefaults: UserDefaults

  /// In-memory cache of last known versions per user (for detecting changes)
  /// Key: userDID, Value: last version we observed
  private var lastKnownVersions: [String: Int] = [:]

  /// Lock for thread-safe version cache access
  private let cacheLock = NSLock()

  // MARK: - Initialization

  private init() {
    if let defaults = UserDefaults(suiteName: Self.suiteName) {
      sharedDefaults = defaults
    } else {
      // Fallback to standard UserDefaults (won't work cross-process, but prevents crash)
      sharedDefaults = UserDefaults.standard
      logger.warning("⚠️ [StateVersion] Shared UserDefaults suite not available - versioning won't work cross-process")
    }
    logger.debug("MLSStateVersionManager initialized")
  }

  // MARK: - Public API: Version Reading

  /// Get the current disk state version for a user.
  ///
  /// This is a fast read from UserDefaults (no database access).
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: Current state version on disk (0 if never set)
  public func getDiskVersion(for userDID: String) -> Int {
    let key = versionKey(for: userDID)
    return sharedDefaults.integer(forKey: key)
  }

  /// Get the global state version (any user's state changed).
  ///
  /// Useful for detecting any MLS activity across all users.
  ///
  /// - Returns: Global state version (0 if never set)
  public func getGlobalDiskVersion() -> Int {
    return sharedDefaults.integer(forKey: Self.globalVersionKey)
  }

  /// Get the last known version we observed for a user (in-memory).
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: Last known version, or nil if we haven't observed any
  public func getLastKnownVersion(for userDID: String) -> Int? {
    cacheLock.lock()
    defer { cacheLock.unlock() }
    return lastKnownVersions[userDID]
  }

  /// Check if the in-memory context is stale compared to disk.
  ///
  /// Call this before any MLS decryption or state-sensitive operation.
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - memoryVersion: The version currently held in memory (from last context load)
  /// - Returns: true if disk version > memory version (context needs reload)
  public func isContextStale(for userDID: String, memoryVersion: Int) -> Bool {
    let diskVersion = getDiskVersion(for: userDID)
    let isStale = diskVersion > memoryVersion
    if isStale {
      logger.info("🔄 [StateVersion] Context stale for \(userDID.prefix(20))...: disk=\(diskVersion), memory=\(memoryVersion)")
    }
    return isStale
  }

  /// Check if state has changed since we last observed it.
  ///
  /// This updates the internal cache if a change is detected.
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: true if version changed since last observation
  public func hasVersionChanged(for userDID: String) -> Bool {
    let diskVersion = getDiskVersion(for: userDID)

    cacheLock.lock()
    let lastKnown = lastKnownVersions[userDID]
    let changed = lastKnown == nil || diskVersion > lastKnown!

    if changed {
      lastKnownVersions[userDID] = diskVersion
    }
    cacheLock.unlock()

    if changed {
      logger.info("🔄 [StateVersion] Version changed for \(userDID.prefix(20))...: \(lastKnown ?? 0) → \(diskVersion)")
    }
    return changed
  }

  // MARK: - Public API: Version Writing

  /// Increment the state version for a user.
  ///
  /// Call this AFTER any MLS state mutation:
  /// - Message decryption (ratchet advance)
  /// - Commit processing (epoch advance)
  /// - Welcome processing (group join)
  /// - Member add/remove
  ///
  /// This operation uses best-effort atomic increment (no file locks for 0xdead10cc prevention).
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: The new version number
  @discardableResult
  public func incrementVersion(for userDID: String) -> Int {
    let key = versionKey(for: userDID)

    // ═══════════════════════════════════════════════════════════════════════════
    // ADVISORY LOCKS REMOVED (2026-02): Signal-style 0xdead10cc prevention
    // ═══════════════════════════════════════════════════════════════════════════
    // Advisory locks cause 0xdead10cc crashes. Version increment is now best-effort.
    // Even if we read a slightly stale value, the version will still increase,
    // which is sufficient to trigger context reload on the other process.
    // ═══════════════════════════════════════════════════════════════════════════
    let current = sharedDefaults.integer(forKey: key)
    let newVersion = current + 1
    sharedDefaults.set(newVersion, forKey: key)

    // Also increment global version
    let globalCurrent = sharedDefaults.integer(forKey: Self.globalVersionKey)
    sharedDefaults.set(globalCurrent + 1, forKey: Self.globalVersionKey)

    // Force synchronize to ensure cross-process visibility
    sharedDefaults.synchronize()

    // Update our local cache
    cacheLock.lock()
    lastKnownVersions[userDID] = newVersion
    cacheLock.unlock()

    logger.info("📈 [StateVersion] Incremented version for \(userDID.prefix(20))...: → \(newVersion)")

    // Post local notification for immediate in-process handling
    NotificationCenter.default.post(
      name: MLSStateVersionDidChangeNotification,
      object: nil,
      userInfo: ["userDID": userDID, "version": newVersion]
    )

    return newVersion
  }

  /// Set a specific version for a user (used during context initialization).
  ///
  /// - Parameters:
  ///   - version: Version to set
  ///   - userDID: User's decentralized identifier
  public func setVersion(_ version: Int, for userDID: String) {
    let key = versionKey(for: userDID)
    sharedDefaults.set(version, forKey: key)
    sharedDefaults.synchronize()

    cacheLock.lock()
    lastKnownVersions[userDID] = version
    cacheLock.unlock()

    logger.debug("📝 [StateVersion] Set version for \(userDID.prefix(20))...: \(version)")
  }

  /// Update the last known version without incrementing disk version.
  ///
  /// Call this after successfully reloading context from disk.
  ///
  /// - Parameter userDID: User's decentralized identifier
  public func syncLastKnownVersion(for userDID: String) {
    let diskVersion = getDiskVersion(for: userDID)

    cacheLock.lock()
    lastKnownVersions[userDID] = diskVersion
    cacheLock.unlock()

    logger.debug("🔄 [StateVersion] Synced last known version for \(userDID.prefix(20))...: \(diskVersion)")
  }

  // MARK: - Public API: Lock Status

  /// Check if MLS operations are likely busy.
  ///
  /// ADVISORY LOCKS REMOVED (2026-02): Signal-style 0xdead10cc prevention.
  /// This method now always returns true since we no longer use file locks.
  /// The UI can instead observe Darwin notifications or state version changes
  /// to detect NSE activity.
  ///
  /// - Returns: Always true (locks removed)
  public func isLockAvailable() -> Bool {
    // Advisory locks removed - always return true
    // UI should use Darwin notification observation instead
    return true
  }

  // MARK: - Public API: Cleanup

  /// Clear all version data for a user (e.g., on logout).
  ///
  /// - Parameter userDID: User's decentralized identifier
  public func clearVersion(for userDID: String) {
    let key = versionKey(for: userDID)
    sharedDefaults.removeObject(forKey: key)
    sharedDefaults.synchronize()

    cacheLock.lock()
    lastKnownVersions.removeValue(forKey: userDID)
    cacheLock.unlock()

    logger.info("🗑️ [StateVersion] Cleared version for \(userDID.prefix(20))...")
  }

  /// Clear all version data (e.g., on app reset).
  public func clearAllVersions() {
    // Remove all per-user versions
    let allKeys = sharedDefaults.dictionaryRepresentation().keys
    for key in allKeys where key.hasPrefix(Self.userVersionKeyPrefix) {
      sharedDefaults.removeObject(forKey: key)
    }

    // Remove global version
    sharedDefaults.removeObject(forKey: Self.globalVersionKey)
    sharedDefaults.synchronize()

    cacheLock.lock()
    lastKnownVersions.removeAll()
    cacheLock.unlock()

    logger.info("🗑️ [StateVersion] Cleared all versions")
  }

  // MARK: - Private Helpers

  /// Generate a stable, short key for a userDID.
  private func versionKey(for userDID: String) -> String {
    // Hash the DID to keep key length reasonable
    let digest = SHA256.hash(data: Data(userDID.utf8))
    let hex = digest.compactMap { String(format: "%02x", $0) }.joined()
    return "\(Self.userVersionKeyPrefix)\(hex.prefix(16))"
  }
}
