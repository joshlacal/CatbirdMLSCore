//
//  MLSGRDBManager.swift
//  Catbird
//
//  GRDB DatabasePool manager with SQLCipher encryption for MLS storage.
//  Uses DatabasePool for concurrent read access and serialized writes.
//  Actor isolation ensures thread-safe access with Swift 6 concurrency.
//
//  OOM FIX NOTES (2024-12):
//  SQLite error 7 "out of memory" in SQLCipher is often NOT actual memory exhaustion.
//  Common causes:
//  1. Account switching race conditions - database opened before previous one closes
//  2. WAL file corruption from incomplete checkpoints
//  3. HMAC verification failure (unrecoverable; hard reset/quarantine required)
//  4. Connection pool exhaustion with multiple accounts
//
//  Key fixes implemented:
//  - MLSDatabaseGate prevents open-while-closing race
//  - closeDatabaseAndDrain() must complete before opening new database
//  - Increased cache_size from 1MB to 2MB for SQLCipher overhead
//  - Disabled wal_autocheckpoint (budget-based TRUNCATE checkpoints instead)
//  - Transient errors (busy, locked) no longer trigger destructive recovery
//  - Periodic WAL checkpoints during polling loops
//
//  CRITICAL FIX (2024-12-15): progressiveRepair() now distinguishes transient vs corruption
//  - SQLITE_BUSY (error 5) and SQLITE_LOCKED (error 6) are TRANSIENT - wait and retry
//  - These errors should NEVER escalate to FULL DATABASE RESET
//  - Only true corruption (malformed disk image, HMAC failure) should trigger reset
//  - New parameter `lastError` allows repair logic to preserve data during lock contention
//

import Foundation
import GRDB
import os
import OSLog

/// Checkpoint mode for emergency close — controls how aggressively WAL is flushed.
public enum EmergencyCheckpointMode: String, Sendable {
  /// TRUNCATE: Exclusive WAL access, resets WAL to zero. Use ONLY from the main app
  /// during suspension. NEVER from NSE — TRUNCATE from two processes corrupts the WAL.
  case truncate
  /// PASSIVE: Non-blocking, checkpoints as many frames as possible without waiting.
  /// Safe for cross-process use. Use from NSE and any non-owning process.
  case passive
}

/// Result of a safe checkpoint operation for app suspension.
public enum CheckpointResult: Sendable {
  /// Checkpoint completed successfully, all WAL pages written to database
  case success(pagesCheckpointed: Int, totalPages: Int)
  /// Checkpoint completed partially - some pages written, others still pending
  /// This is acceptable for suspension as critical data was flushed
  case partial(pagesCheckpointed: Int, totalPages: Int)
  /// Checkpoint was skipped (no database, already closed, etc.)
  case skipped(reason: String)
  /// Checkpoint failed with an error
  case failed(error: Error)

  /// Whether the checkpoint is safe enough for app suspension
  public var isSafeForSuspension: Bool {
    switch self {
    case .success, .partial, .skipped:
      return true
    case .failed:
      return false
    }
  }
}

/// Manages encrypted GRDB DatabasePool instances with per-user isolation.
/// Actor provides thread-safe access and automatic isolation.
/// Uses DatabasePool for better read concurrency (multiple concurrent readers, single writer).
public actor MLSGRDBManager {

  // MARK: - Shared Instance

  /// Process-wide shared instance for database access.
  ///
  /// IMPORTANT: Cross-process coordination (main app vs NSE) uses Darwin notifications for lockless signaling,
  /// NOT actor isolation or file locks. Each process has its own `.shared` instance.
  ///
  /// For code that has direct access to an MLSConversationManager, prefer using
  /// the manager's owned database instance for proper lifecycle management.
  public static let shared = MLSGRDBManager()

  // MARK: - Emergency Suspension Close (0xdead10cc Prevention)

  /// Non-actor-isolated storage for emergency suspension close.
  /// This is necessary because iOS can suspend us at any point after scenePhase changes
  /// and we MUST release SQLite file handles synchronously or face 0xdead10cc termination.
  private nonisolated(unsafe) static var emergencyDatabases: [String: DatabasePool] = [:]
  private nonisolated(unsafe) static var emergencyDatabasesLock = NSLock()

  /// Flag indicating emergency close happened - actor cache is now stale
  private nonisolated(unsafe) static var emergencyCacheInvalidated = false

  // MARK: - Checkpoint Timeout (Signal-style thread-local)

  /// Thread-local key for checkpoint busy timeout.
  ///
  /// Signal pattern: the GRDB busy callback retries forever for normal writes,
  /// but during checkpoints we set a short timeout via thread-local storage.
  /// The busy callback checks this and aborts early during checkpoints.
  ///
  /// IMPORTANT: We CANNOT use `PRAGMA busy_timeout` for this because it
  /// internally calls `sqlite3_busy_timeout()` which CLEARS any
  /// `sqlite3_busy_handler()` set by GRDB's `busyMode = .callback`.
  /// This was the root cause of persistent 0xdead10cc crashes.
  private nonisolated static let checkpointTimeoutKey = "MLSGRDBManager.checkpointBusyRetries"
  private nonisolated static let staticLogger = Logger(subsystem: "Catbird", category: "MLSGRDBManager")

  /// Create a Signal-style busy callback with configurable max retries.
  ///
  /// - Parameter maxRetries: Maximum number of retries before giving up (each retry sleeps 25ms)
  /// - Returns: GRDB BusyMode.callback configured with checkpoint abort support
  private static func nseBusyMode(maxRetries: Int) -> Database.BusyMode {
    .callback { numberOfTries in
      // Checkpoint abort: same thread-local pattern as the main pool
      if let checkpointMax = Thread.current.threadDictionary[MLSGRDBManager.checkpointTimeoutKey] as? Int {
        if numberOfTries >= checkpointMax {
          return false
        }
      }
      // Hard cap for NSE: never block longer than maxRetries * 25ms
      if numberOfTries >= maxRetries {
        return false
      }
      Thread.sleep(forTimeInterval: 0.025)
      return true
    }
  }

  /// Emergency synchronous close of all GRDB database pools for 0xdead10cc prevention.
  /// Call this SYNCHRONOUSLY when transitioning to inactive/background.
  /// This is safe to call from any thread and does not require actor isolation.
  ///
  /// - Parameter mode: Checkpoint mode. Main app uses `.truncate` for 0xdead10cc prevention.
  ///   NSE MUST use `.passive` — TRUNCATE from a non-owning process causes WAL corruption.
  public nonisolated static func emergencyCloseAllDatabases(
    mode: EmergencyCheckpointMode = .truncate
  ) {
    let pid = ProcessInfo.processInfo.processIdentifier
    let processTag = Bundle.main.bundlePath.hasSuffix(".appex") ? "NSE" : "APP"

    // Wait for any in-progress budget checkpoint to finish before we close pools.
    // This prevents the race where a background PASSIVE checkpoint holds a connection
    // while we TRUNCATE + close underneath it.
    var spinCount = 0
    while checkpointBudgetLock.withLock({ $0.checkpointInProgress }) && spinCount < 20 {
      Thread.sleep(forTimeInterval: 0.005)  // 5ms per spin, 100ms max
      spinCount += 1
    }
    if spinCount > 0 {
      staticLogger.debug("[0xdead10cc-FIX] Waited \(spinCount * 5)ms for in-progress checkpoint to finish")
    }

    emergencyDatabasesLock.lock()
    defer { emergencyDatabasesLock.unlock() }

    staticLogger.warning(
      "🚨 [0xdead10cc-FIX] [\(processTag)/\(pid)] Emergency closing \(emergencyDatabases.count) GRDB pools (mode=\(mode.rawValue))"
    )

    for (userDID, pool) in emergencyDatabases {
      let checkpointSQL: String
      switch mode {
      case .truncate:
        checkpointSQL = "PRAGMA wal_checkpoint(TRUNCATE);"
      case .passive:
        checkpointSQL = "PRAGMA wal_checkpoint(PASSIVE);"
      }

      // Log WAL state BEFORE checkpoint for corruption diagnostics
      do {
        try pool.read { db in
          let walInfo = try Row.fetchOne(db, sql: "PRAGMA wal_checkpoint(PASSIVE);")
          let busy = walInfo?["busy"] as? Int ?? -1
          let log = walInfo?["log"] as? Int ?? -1
          let checkpointed = walInfo?["checkpointed"] as? Int ?? -1
          staticLogger.warning(
            "📊 [\(processTag)/\(pid)] PRE-checkpoint WAL state for \(userDID.prefix(20), privacy: .private): busy=\(busy) log=\(log) checkpointed=\(checkpointed)"
          )
        }
      } catch {
        staticLogger.warning(
          "📊 [\(processTag)/\(pid)] PRE-checkpoint WAL probe failed for \(userDID.prefix(20), privacy: .private): \(error)"
        )
      }

      // Best-effort checkpoint before close
      do {
        try pool.writeWithoutTransaction { db in
          Thread.current.threadDictionary[checkpointTimeoutKey] = 2  // ~50ms max
          defer { Thread.current.threadDictionary.removeObject(forKey: checkpointTimeoutKey) }
          try db.execute(sql: checkpointSQL)
        }
        staticLogger.debug(
          "✅ [0xdead10cc-FIX] [\(processTag)/\(pid)] GRDB \(mode.rawValue) checkpoint for \(userDID.prefix(20), privacy: .private)"
        )
      } catch {
        staticLogger.warning(
          "⚠️ [0xdead10cc-FIX] [\(processTag)/\(pid)] GRDB \(mode.rawValue) checkpoint failed for \(userDID.prefix(20), privacy: .private): \(error)"
        )
      }

      // Close the pool
      do {
        try pool.close()
        staticLogger.debug(
          "✅ [0xdead10cc-FIX] [\(processTag)/\(pid)] GRDB pool closed for \(userDID.prefix(20), privacy: .private)"
        )
      } catch {
        staticLogger.warning(
          "⚠️ [0xdead10cc-FIX] [\(processTag)/\(pid)] GRDB pool close failed for \(userDID.prefix(20), privacy: .private): \(error)"
        )
      }
    }
    emergencyDatabases.removeAll()

    // Mark actor cache as stale - must be cleared on next access
    emergencyCacheInvalidated = true

    staticLogger.debug("✅ [0xdead10cc-FIX] [\(processTag)/\(pid)] All GRDB database pools emergency closed")
  }

  /// Best-effort PASSIVE checkpoint on all registered pools WITHOUT closing them.
  /// Safe to call before GRDB suspension — shrinks the WAL so the NSE has less
  /// data to contend with, without disrupting GRDB's pool lifecycle.
  /// Unlike emergencyCloseAllDatabases, this preserves pool state for resume.
  public nonisolated static func flushWALOnAllDatabases() {
    let pid = ProcessInfo.processInfo.processIdentifier
    let processTag = Bundle.main.bundlePath.hasSuffix(".appex") ? "NSE" : "APP"

    emergencyDatabasesLock.lock()
    let pools = emergencyDatabases
    emergencyDatabasesLock.unlock()

    for (userDID, pool) in pools {
      do {
        try pool.writeWithoutTransaction { db in
          Thread.current.threadDictionary[checkpointTimeoutKey] = 2
          defer { Thread.current.threadDictionary.removeObject(forKey: checkpointTimeoutKey) }
          try db.execute(sql: "PRAGMA wal_checkpoint(PASSIVE);")
        }
        staticLogger.debug(
          "✅ [\(processTag)/\(pid)] Pre-suspension PASSIVE checkpoint for \(userDID.prefix(20), privacy: .private)"
        )
      } catch {
        staticLogger.debug(
          "⚠️ [\(processTag)/\(pid)] Pre-suspension PASSIVE checkpoint skipped for \(userDID.prefix(20), privacy: .private): \(error)"
        )
      }
    }
  }

  /// Synchronous TRUNCATE checkpoint at app launch to clear any leftover WAL from previous session.
  ///
  /// Signal-style: at launch, aggressively checkpoint all known databases to start with a clean WAL.
  /// This handles the case where a previous session was terminated before budget checkpoints ran.
  /// Safe to call from `init()` before any async work begins.
  public nonisolated static func syncTruncatingCheckpointAtLaunch() {
    // Verify plaintext headers on all databases FIRST
    // If any show encrypted headers, iOS won't recognize them as SQLite → 0xdead10cc
    if let appGroup = FileManager.default.containerURL(forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared") {
      let mlsDir = appGroup.appendingPathComponent("Application Support/MLS", isDirectory: true)
      let rustDir = appGroup.appendingPathComponent("Application Support/mls-state", isDirectory: true)
      verifyPlaintextHeaders(in: mlsDir)
      verifyPlaintextHeaders(in: rustDir)
    }

    emergencyDatabasesLock.lock()
    let pools = emergencyDatabases
    emergencyDatabasesLock.unlock()

    guard !pools.isEmpty else {
      staticLogger.debug("[0xdead10cc-FIX] Launch checkpoint: no GRDB pools registered yet")
      return
    }

    staticLogger.debug("[0xdead10cc-FIX] Launch checkpoint: checkpointing \(pools.count) GRDB pool(s)")

    for (userDID, pool) in pools {
      do {
        try pool.writeWithoutTransaction { db in
          // Signal uses 3s timeout for launch checkpoint (120 retries * 25ms)
          Thread.current.threadDictionary[checkpointTimeoutKey] = 120
          defer { Thread.current.threadDictionary.removeObject(forKey: checkpointTimeoutKey) }
          try db.execute(sql: "PRAGMA wal_checkpoint(TRUNCATE);")
        }
        staticLogger.debug("  ✅ Launch TRUNCATE checkpoint succeeded for \(userDID.prefix(20), privacy: .private)...")
      } catch {
        // SQLITE_BUSY is expected if NSE is active; log and continue
        staticLogger.warning("  ⚠️ Launch TRUNCATE checkpoint skipped for \(userDID.prefix(20), privacy: .private)...: \(error.localizedDescription)")
      }
    }
  }

  /// Verify plaintext headers on all database files in the MLS directory.
  /// iOS only exempts SQLite WAL locks from 0xdead10cc if the file starts with "SQLite format 3\0".
  /// If cipher_plaintext_header_size was added after database creation, the header remains encrypted.
  public nonisolated static func verifyPlaintextHeaders(in directory: URL) {
    let fm = FileManager.default
    guard let files = try? fm.contentsOfDirectory(atPath: directory.path) else {
      os_log(.fault, "[0xdead10cc-DIAG] Cannot list directory: %{public}@", directory.path)
      return
    }

    let dbFiles = files.filter { $0.hasSuffix(".db") }
    let sqliteMagic = Data("SQLite format 3\0".utf8)

    for dbFile in dbFiles {
      let fullPath = directory.appendingPathComponent(dbFile)
      guard let handle = try? FileHandle(forReadingFrom: fullPath) else {
        os_log(.fault, "[0xdead10cc-DIAG] Cannot open: %{public}@", dbFile)
        continue
      }
      defer { try? handle.close() }

      guard let header = try? handle.read(upToCount: 16) else {
        os_log(.fault, "[0xdead10cc-DIAG] Cannot read header: %{public}@", dbFile)
        continue
      }

      let isPlaintext = header.prefix(16) == sqliteMagic
      let hexHeader = header.map { String(format: "%02x", $0) }.joined(separator: " ")

      if isPlaintext {
        os_log(.fault, "[0xdead10cc-DIAG] ✅ PLAINTEXT header: %{public}@ → %{public}@", dbFile, hexHeader)
      } else {
        os_log(.fault, "[0xdead10cc-DIAG] ❌ ENCRYPTED header: %{public}@ → %{public}@", dbFile, hexHeader)
      }
    }
  }

  /// Register a database pool for emergency close.
  /// Called internally when a pool is created or retrieved.
  private nonisolated static func registerForEmergencyClose(_ pool: DatabasePool, for userDID: String) {
    emergencyDatabasesLock.lock()
    defer { emergencyDatabasesLock.unlock() }
    emergencyDatabases[userDID] = pool
  }

  /// Unregister a database pool from emergency close.
  /// Called internally when a pool is explicitly closed.
  private nonisolated static func unregisterFromEmergencyClose(for userDID: String) {
    emergencyDatabasesLock.lock()
    defer { emergencyDatabasesLock.unlock() }
    emergencyDatabases.removeValue(forKey: userDID)
  }

  // MARK: - Properties

  /// Logger for database operations
  private nonisolated let logger = Logger(subsystem: "Catbird", category: "MLSGRDBManager")

  /// Active database pools per user DID (upgraded from DatabaseQueue for better concurrency)
  private var databases: [String: DatabasePool] = [:]

  /// Cached lightweight queues to prevent rapid open/close cycles that corrupt SHM.
  /// Each entry is evicted after 5 seconds of inactivity.
  private var cachedLightweightQueues: [String: (queue: DatabaseQueue, lastUsed: Date)] = [:]
  private let lightweightQueueTTL: TimeInterval = 5.0

  /// Encryption manager
  private let encryption = MLSSQLCipherEncryption.shared

  /// In-memory key fingerprint per user (helps detect key reuse/mismatch within a process)
  private var keyFingerprints: [String: String] = [:]

  /// Base directory for all user databases
  private nonisolated let databaseDirectory: URL

  /// Database file extension
  private nonisolated let fileExtension = "db"

  private nonisolated var isRunningInExtension: Bool {
    Bundle.main.bundleURL.pathExtension == "appex"
  }



  /// Tracks uncached ephemeral database pools for cleanup during account switch
  /// Key: userDID, Value: DatabasePool (not in main cache)
  private var uncachedEphemeralPools: [String: DatabasePool] = [:]

  private struct DatabaseAccessSuspension: Sendable {
    let until: Date
    let reason: String
    let operation: String
  }

  private struct ForegroundResumePreparation: Sendable {
    let startedAt: Date
  }

  public enum ForegroundResumePreparationResult: Sendable {
    case ready
    case repaired
    case reset
  }

  private enum QuickIntegrityCheckClassification {
    case ok
    case transient(String)
    case corruption(String)
    case unexpected(String)
  }

  /// Temporary fail-fast guard for a user whose database is returning corruption or
  /// resource-exhaustion errors. This prevents notification decrypt, polling, and UI
  /// refresh loops from repeatedly reopening and probing the same broken store.
  private var databaseAccessSuspensions: [String: DatabaseAccessSuspension] = [:]

  private let resourceExhaustionSuspensionDuration: TimeInterval = 8
  private let corruptionSuspensionDuration: TimeInterval = 30

  /// Tracks WAL/SHM repair state per user.
  /// `repairsDone`: how many WAL repairs have been performed since last successful DB open.
  /// `lastRepairTime`: when the last repair was done, to debounce rapid-fire errors.
  /// When WAL repair "succeeds" but corruption persists (main .db is corrupt),
  /// we escalate to automatic quarantine + salvage after maxWALRepairsBeforeSalvage repairs.
  private var walRepairState: [String: (repairsDone: Int, lastRepairTime: Date)] = [:]
  private let maxWALRepairsBeforeSalvage = 3
  /// Minimum interval between counting separate WAL repair attempts (seconds).
  /// Rapid-fire errors from ephemeral lightweight queues hitting the same corrupt file
  /// should count as ONE repair attempt, not multiple.
  private let walRepairDebounceInterval: TimeInterval = 5.0
  private var foregroundResumePreparations: [String: ForegroundResumePreparation] = [:]
  private let foregroundResumeWaitTimeout: TimeInterval = 2.0
  private let foregroundResumeWaitPollIntervalNanos: UInt64 = 50_000_000
  private let foregroundResumeTransientRetryCount = 6
  private let foregroundResumeBaseRetryDelayNanos: UInt64 = 100_000_000

  private enum ConnectionState: String {
    case closed
    case opening
    case open
    case closing
  }

  /// Lifecycle tracking per user to prevent re-entry while closing
  private var connectionStates: [String: ConnectionState] = [:]

  /// The currently "active" user DID - only one database should be actively used at a time
  /// Other databases are kept in cache but should not have active operations
  private var activeUserDID: String?

  /// Public accessor for current active DID (for validation in MLSStorage)
  public var currentActiveDID: String? {
    return activeUserDID
  }

  /// Tracks whether the database manager has been closed
  private var isClosed = false

  /// The currently "active" user DID - only one database should be actively used at a time

  // MARK: - Coordination Generation

  /// Current coordination generation (increments on every account switch/drain)
  /// This is used to invalidate pending tasks that might try to touch the DB
  /// after it has been closed or switched to a new user.
  private var coordinationGeneration: [String: Int] = [:]

  public func getCoordinationGeneration(for userDID: String) -> Int {
    return coordinationGeneration[userDID] ?? 0
  }

  private func incrementCoordinationGeneration(for userDID: String) {
    let current = coordinationGeneration[userDID] ?? 0
    coordinationGeneration[userDID] = current + 1
    logger.info(
      "🔢 [GEN] Incremented coordination generation to \(current + 1) for \(userDID.prefix(16))...")
  }

  private func currentDatabaseAccessSuspension(for userDID: String) -> DatabaseAccessSuspension? {
    guard let suspension = databaseAccessSuspensions[userDID] else {
      return nil
    }

    if suspension.until <= Date() {
      databaseAccessSuspensions.removeValue(forKey: userDID)
      return nil
    }

    return suspension
  }

  private func requireDatabaseAccess(for userDID: String, operation: String) throws {
    guard let suspension = currentDatabaseAccessSuspension(for: userDID) else {
      return
    }

    let remaining = max(1, Int(suspension.until.timeIntervalSinceNow.rounded(.up)))
    logger.warning(
      "⏸️ [DB-GUARD] Rejecting \(operation) for \(userDID.prefix(20), privacy: .private) - \(remaining)s remaining (\(suspension.reason, privacy: .public))"
    )
    throw MLSSQLCipherError.storageUnavailable(
      reason: "MLS storage temporarily unavailable for \(remaining)s: \(suspension.reason)"
    )
  }

  private func clearDatabaseAccessSuspensionIfNeeded(for userDID: String, operation: String) {
    guard databaseAccessSuspensions.removeValue(forKey: userDID) != nil else {
      return
    }

    logger.info(
      "✅ [DB-GUARD] Cleared storage suspension for \(userDID.prefix(20), privacy: .private) via \(operation, privacy: .public)"
    )
  }

  private func recordDatabaseAccessFailure(for userDID: String, error: Error, operation: String) {
    let isResourceExhaustion = isSQLiteError7(error)
    let isCorruption = isRecoverableCodecError(error)
    guard isResourceExhaustion || isCorruption else {
      return
    }

    let duration = isCorruption ? corruptionSuspensionDuration : resourceExhaustionSuspensionDuration
    let now = Date()
    let previousUntil = databaseAccessSuspensions[userDID]?.until ?? now
    let baseTime = max(previousUntil.timeIntervalSince1970, now.timeIntervalSince1970)
    let until = Date(timeIntervalSince1970: baseTime + duration)
    let reason = error.localizedDescription

    databaseAccessSuspensions[userDID] = DatabaseAccessSuspension(
      until: until,
      reason: reason,
      operation: operation
    )

    let remaining = max(1, Int(until.timeIntervalSinceNow.rounded(.up)))
    logger.error(
      "🚨 [DB-GUARD] Suspending \(operation, privacy: .public) for \(userDID.prefix(20), privacy: .private) for \(remaining)s after database failure: \(reason, privacy: .public)"
    )

    if let ephemeralPool = uncachedEphemeralPools.removeValue(forKey: userDID) {
      do {
        try ephemeralPool.close()
        logger.info(
          "🧹 [DB-GUARD] Closed uncached ephemeral pool for \(userDID.prefix(20), privacy: .private)"
        )
      } catch {
        logger.warning(
          "⚠️ [DB-GUARD] Failed to close uncached ephemeral pool for \(userDID.prefix(20), privacy: .private): \(error.localizedDescription)"
        )
      }
    }

    closeDatabase(for: userDID)

    // CRITICAL FIX: When corruption is detected (error 11), delete WAL/SHM files
    // immediately so the next connection attempt opens cleanly.
    // If recovery succeeds, clear the suspension — the database is ready to use.
    //
    // DEBOUNCE: Multiple rapid-fire lightweight reads hitting the same corrupt file
    // should count as ONE repair attempt, not N. We only increment the repair count
    // if enough time has passed since the last repair (walRepairDebounceInterval).
    if isCorruption {
      let now = Date()
      let state = walRepairState[userDID] ?? (repairsDone: 0, lastRepairTime: .distantPast)
      let timeSinceLastRepair = now.timeIntervalSince(state.lastRepairTime)

      if timeSinceLastRepair < walRepairDebounceInterval {
        // Within debounce window — this is likely the same corruption event.
        // Don't increment counter, don't repair again (already done), just suspend.
        logger.debug(
          "🔧 [DB-GUARD] Corruption re-hit within debounce window (\(Int(timeSinceLastRepair))s) for \(userDID.prefix(20), privacy: .private) - skipping duplicate repair"
        )
        return
      }

      let newRepairCount = state.repairsDone + 1
      walRepairState[userDID] = (repairsDone: newRepairCount, lastRepairTime: now)

      if newRepairCount >= maxWALRepairsBeforeSalvage && !isRunningInExtension {
        // WAL repair has been attempted multiple times but corruption persists.
        // The main .db file is corrupt. Escalate to the coordinated Swift/Rust reset path.
        logger.error(
          "🚨 [DB-GUARD] WAL repair attempted \(newRepairCount)x for \(userDID.prefix(20), privacy: .private) - escalating to coordinated reset"
        )
        walRepairState.removeValue(forKey: userDID)

        Task { [self] in
          await triggerCoordinatedResetIfNeeded(
            for: userDID,
            operation: operation,
            trigger: "persistent corruption after \(newRepairCount) WAL repairs"
          )
        }
      } else {
        logger.warning(
          "🔧 [DB-GUARD] Corruption detected (\(newRepairCount)/\(self.maxWALRepairsBeforeSalvage)) - attempting WAL/SHM repair for \(userDID.prefix(20), privacy: .private)"
        )
        Task { [self] in
          let recovered = await attemptSoftRecovery(for: userDID)
          if recovered {
            logger.info(
              "🧪 [DB-GUARD] WAL/SHM cleanup finished for \(userDID.prefix(20), privacy: .private) - verifying readability before shortening suspension"
            )
            await verifySoftRecoveryOutcome(for: userDID, operation: operation)
          }
        }
      }
    }
  }

  private func verifySoftRecoveryOutcome(for userDID: String, operation: String) async {
    let verification = await runQuickIntegrityCheckWithRetry(for: userDID)

    switch verification {
    case .ok:
      let shortSuspension = Date().addingTimeInterval(2.0)
      databaseAccessSuspensions[userDID] = DatabaseAccessSuspension(
        until: shortSuspension,
        reason: "WAL repair verified, retrying shortly",
        operation: operation
      )
      logger.info(
        "✅ [DB-GUARD] WAL/SHM repair verified for \(userDID.prefix(20), privacy: .private) - short suspension (2s) before retry"
      )

    case .transient(let description):
      logger.warning(
        "⏳ [DB-GUARD] Soft-recovery verification still transient for \(userDID.prefix(20), privacy: .private): \(description, privacy: .public)"
      )

    case .unexpected(let description):
      logger.error(
        "❌ [DB-GUARD] Soft-recovery verification failed unexpectedly for \(userDID.prefix(20), privacy: .private): \(description, privacy: .public)"
      )

    case .corruption(let description):
      // WAL repair "succeeded" (files deleted) but the .db file itself is corrupt.
      // Do NOT trigger immediate reset — this would wipe the database on the first
      // corruption event. Instead, keep the 30s suspension and let the walRepairState
      // debounce counter accumulate naturally. After 3 repair attempts all fail
      // verification, the counter in recordDatabaseAccessFailure will escalate to
      // auto-salvage with proper quarantine and data recovery.
      logger.error(
        "🚨 [DB-GUARD] WAL/SHM cleanup did not restore readability for \(userDID.prefix(20), privacy: .private): \(description, privacy: .public)"
      )
      logger.warning(
        "⏳ [DB-GUARD] Keeping suspension active for \(userDID.prefix(20), privacy: .private) - walRepairState counter will escalate if corruption persists"
      )
    }
  }

  private func triggerCoordinatedResetIfNeeded(
    for userDID: String,
    operation: String,
    trigger: String
  ) async {
    guard !isRunningInExtension else {
      return
    }

    guard coordinatedResetsInProgress.insert(userDID).inserted else {
      logger.debug(
        "🧰 [DB-GUARD] Coordinated reset already running for \(userDID.prefix(20), privacy: .private)"
      )
      return
    }

    databaseAccessSuspensions[userDID] = DatabaseAccessSuspension(
      until: Date().addingTimeInterval(5.0),
      reason: "MLS storage reset in progress",
      operation: operation
    )

    logger.error(
      "🧰 [DB-GUARD] Triggering coordinated Swift/Rust MLS reset for \(userDID.prefix(20), privacy: .private) (\(trigger, privacy: .public))"
    )

    defer {
      coordinatedResetsInProgress.remove(userDID)
    }

    do {
      try await MLSClient.shared.clearStorage(for: userDID)
      clearRepairState(for: userDID)
      clearHardResetFlag(for: userDID)
      clearDatabaseAccessSuspensionIfNeeded(for: userDID, operation: "coordinated-reset")
      logger.info(
        "✅ [DB-GUARD] Coordinated MLS storage reset completed for \(userDID.prefix(20), privacy: .private)"
      )
    } catch {
      logger.error(
        "❌ [DB-GUARD] Coordinated MLS storage reset failed for \(userDID.prefix(20), privacy: .private): \(error.localizedDescription)"
      )
    }
  }

  private func waitForForegroundResumePreparationIfNeeded(for userDID: String, operation: String)
    async throws
  {
    guard let state = foregroundResumePreparations[userDID] else {
      return
    }

    logger.info(
      "⏳ [RESUME-GUARD] Waiting for foreground resume preparation before \(operation, privacy: .public) for \(userDID.prefix(20), privacy: .private)"
    )

    let deadline = Date().addingTimeInterval(foregroundResumeWaitTimeout)

    while let currentState = foregroundResumePreparations[userDID] {
      if Date() >= deadline {
        let elapsedMs = Int(Date().timeIntervalSince(currentState.startedAt) * 1000)
        logger.error(
          "🚫 [RESUME-GUARD] Timed out after \(elapsedMs)ms waiting for foreground resume preparation before \(operation, privacy: .public) for \(userDID.prefix(20), privacy: .private)"
        )
        throw MLSSQLCipherError.storageUnavailable(
          reason: "MLS foreground resume still in progress"
        )
      }

      // If the state changed while we were waiting, reset the deadline to give the
      // active preparation a full window to finish cleanly.
      if currentState.startedAt != state.startedAt {
        return try await waitForForegroundResumePreparationIfNeeded(
          for: userDID,
          operation: operation
        )
      }

      try await Task.sleep(nanoseconds: foregroundResumeWaitPollIntervalNanos)
    }
  }

  public func isDatabaseAccessSuspended(for userDID: String) -> Bool {
    currentDatabaseAccessSuspension(for: userDID) != nil
  }

  public func databaseAccessSuspensionDescription(for userDID: String) -> String? {
    guard let suspension = currentDatabaseAccessSuspension(for: userDID) else {
      return nil
    }

    let remaining = max(1, Int(suspension.until.timeIntervalSinceNow.rounded(.up)))
    return "\(remaining)s remaining after \(suspension.operation): \(suspension.reason)"
  }

  public func suspendDatabaseAccessForTesting(
    for userDID: String,
    duration: TimeInterval = 60,
    reason: String = "test",
    operation: String = "test"
  ) {
    databaseAccessSuspensions[userDID] = DatabaseAccessSuspension(
      until: Date().addingTimeInterval(duration),
      reason: reason,
      operation: operation
    )
  }

  public func clearDatabaseAccessSuspensionForTesting(for userDID: String) {
    databaseAccessSuspensions.removeValue(forKey: userDID)
  }

  public func beginForegroundResumePreparationForTesting(for userDID: String) {
    foregroundResumePreparations[userDID] = ForegroundResumePreparation(startedAt: Date())
  }

  public func endForegroundResumePreparationForTesting(for userDID: String) {
    foregroundResumePreparations.removeValue(forKey: userDID)
  }

  // MARK: - Periodic Checkpointing (fast switches)

  private var periodicCheckpointTask: Task<Void, Never>?
  private let periodicCheckpointInterval: TimeInterval = 30.0

  // MARK: - Budget-Based TRUNCATE Checkpoints (Signal-style)
  //
  // Signal checkpoints every ~32 writes with TRUNCATE mode, keeping WAL perpetually small.
  // This prevents WAL growth and reduces lock contention at suspension time.
  // Reference: Signal's GRDBDatabaseStorageAdapter

  /// Thread-safe checkpoint budget state for budget-based checkpoints.
  /// Uses nonisolated(unsafe) with OSAllocatedUnfairLock for synchronous access from any thread.
  private struct CheckpointBudgetState: Sendable {
    /// Number of writes remaining before triggering a checkpoint.
    /// Starts at 1 so the first write triggers a baseline checkpoint.
    var budget: Int = 1

    /// Number of writes between checkpoints (after baseline)
    static let normalBudget: Int = 32

    /// Retry budget when checkpoint fails (try sooner)
    static let retryBudget: Int = 8

    /// Whether a checkpoint is currently in progress.
    /// Prevents queued checkpoint Tasks from racing each other or with emergency close.
    var checkpointInProgress: Bool = false
  }

  /// Nonisolated checkpoint budget tracker using unfair lock for thread safety.
  /// This must be nonisolated(unsafe) because we need synchronous access without async/await.
  private nonisolated(unsafe) static var checkpointBudgetLock = OSAllocatedUnfairLock(
    initialState: CheckpointBudgetState()
  )

  /// Called after every successful write operation to decrement the checkpoint budget
  /// and trigger a PASSIVE checkpoint when the budget reaches zero.
  ///
  /// This implements Signal's pattern of frequent, small checkpoints to keep the WAL file
  /// small, rather than waiting for app suspension to checkpoint.
  ///
  /// Uses PASSIVE mode during normal operation (non-blocking, doesn't interfere with writers).
  /// TRUNCATE mode is reserved for suspension (emergencyCloseAllDatabases) where we MUST
  /// release WAL file handles to avoid 0xdead10cc termination.
  private nonisolated func didCompleteWrite(for userDID: String) {
    let shouldCheckpoint = Self.checkpointBudgetLock.withLock { state -> Bool in
      state.budget -= 1
      if state.budget <= 0 && !state.checkpointInProgress {
        state.checkpointInProgress = true
        return true
      }
      return false
    }

    if shouldCheckpoint {
      // Perform checkpoint on background queue to avoid blocking the write caller
      DispatchQueue.global(qos: .utility).async { [weak self] in
        guard let self else {
          Self.checkpointBudgetLock.withLock { $0.checkpointInProgress = false }
          return
        }
        Task {
          await self.performPassiveCheckpoint(for: userDID)
        }
      }
    }
  }

  /// Performs a PASSIVE checkpoint (non-blocking).
  ///
  /// PASSIVE mode:
  /// - Copies WAL pages back to the main database without blocking readers or writers
  /// - Does NOT require exclusive access — safe under concurrent write pressure
  /// - WAL file is not truncated (that's reserved for suspension via emergencyCloseAllDatabases)
  ///
  /// This prevents the corruption vector where TRUNCATE checkpoints race with concurrent writes
  /// or emergency close during high-throughput message processing.
  private func performPassiveCheckpoint(for userDID: String) async {
    defer {
      Self.checkpointBudgetLock.withLock { $0.checkpointInProgress = false }
    }

    guard let pool = databases[userDID] else {
      Self.checkpointBudgetLock.withLock { state in
        state.budget = CheckpointBudgetState.normalBudget
      }
      return
    }

    do {
      try await pool.writeWithoutTransaction { db in
        // PASSIVE checkpoint: non-blocking, copies what it can without waiting for locks
        try db.execute(sql: "PRAGMA wal_checkpoint(PASSIVE);")
      }

      Self.checkpointBudgetLock.withLock { state in
        state.budget = CheckpointBudgetState.normalBudget
      }
      logger.debug("✅ [Checkpoint] PASSIVE checkpoint succeeded for \(userDID.prefix(20), privacy: .private)")

    } catch {
      Self.checkpointBudgetLock.withLock { state in
        state.budget = CheckpointBudgetState.retryBudget
      }
      logger.debug("⚠️ [Checkpoint] PASSIVE checkpoint deferred for \(userDID.prefix(20), privacy: .private): \(error.localizedDescription)")
    }
  }
  
  // MARK: - Diagnostics Tracking (OOM Fix 2024-12)
  
  /// Tracks force-closed pools for debugging resource exhaustion issues
  /// Contains (userDID prefix, timestamp) pairs for recent force closes
  private var forceClosedPools: [(userDID: String, timestamp: Date)] = []
  
  /// Maximum number of force close entries to keep for diagnostics
  private let maxForceCloseHistory = 10

  // MARK: - Safe Recovery Configuration (HMAC Fix 2024-12)

  /// Recovery policy for HMAC/NOTADB failures.
  ///
  /// CRITICAL: Auto-reset on first HMAC failure is dangerous because HMAC/NOTADB
  /// can occur due to transient conditions (WAL race, switching, keychain timing)
  /// not just true corruption. This policy controls the behavior.
  public enum RecoveryPolicy: Sendable {
    /// Never auto-reset. Mark as needing reset and require user action via Diagnostics.
    /// This is the safest option and prevents data loss from false-positive corruption detection.
    case requireUserConfirmation

    /// Quarantine files and mark as needing reset, but don't auto-reopen.
    /// User data is preserved in quarantine for potential recovery.
    case quarantineOnly

    /// Allow auto-reset only after multiple consecutive failures (legacy behavior, not recommended).
    /// Only use this if you understand the risks of false-positive corruption detection.
    case autoResetAfterRetries(maxRetries: Int)
  }

  /// Current recovery policy. Default is safest: require user confirmation.
  private var recoveryPolicy: RecoveryPolicy = .requireUserConfirmation

  /// Configure the recovery policy for HMAC/NOTADB failures.
  public func setRecoveryPolicy(_ policy: RecoveryPolicy) {
    recoveryPolicy = policy
    logger.info("🔧 [Recovery] Set recovery policy to: \(String(describing: policy))")
  }

  /// Tracks consecutive HMAC failure count per user (for autoResetAfterRetries policy)
  private var consecutiveHMACFailures: [String: Int] = [:]

  // MARK: - Hard Reset Tracking (HMAC Corruption Fix 2024-12)

  /// Tracks users whose databases require a manual reset due to persistent HMAC corruption.
  /// When soft recovery (WAL/SHM deletion) fails, this flag is set to indicate
  /// the main .db file is corrupted and must be reset from Diagnostics.
  private var usersNeedingHardReset: Set<String> = []

  /// Tracks coordinated resets already running for a DID so corruption storms do not
  /// trigger duplicate Swift/Rust storage resets.
  private var coordinatedResetsInProgress: Set<String> = []

  /// Check if a user's database needs hard reset
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: true if the database has persistent HMAC corruption
  public func needsHardReset(for userDID: String) -> Bool {
    return usersNeedingHardReset.contains(userDID)
  }

  /// Mark a user as needing hard reset
  private func markNeedsHardReset(for userDID: String) {
    usersNeedingHardReset.insert(userDID)
    logger.critical(
      "🚨 [HARD-RESET] Marked user \(userDID.prefix(20), privacy: .private) as needing hard reset")
    logger.critical("   HMAC corruption detected - hard reset required")
    logger.critical("   Use Settings ▸ Diagnostics ▸ Reset MLS Storage to recover")
  }

  /// Clear the hard reset flag for a user (after successful manual reset)
  public func clearHardResetFlag(for userDID: String) {
    usersNeedingHardReset.remove(userDID)
    consecutiveHMACFailures.removeValue(forKey: userDID)
    logger.info("✅ [Recovery] Cleared hard reset flag for \(userDID.prefix(20), privacy: .private)")
  }

  // MARK: - Initialization

  /// Initialize a new database manager instance (per-user ownership model)
  /// Each MLSConversationManager should create its own instance for proper lifecycle management
  public init() {
    // Create base directory for MLS databases
    let baseDirectory = MLSStoragePaths.baseContainerURL()
    self.databaseDirectory = baseDirectory.appendingPathComponent("MLS", isDirectory: true)

    // Create directory if it doesn't exist
    do {
      try FileManager.default.createDirectory(
        at: databaseDirectory, withIntermediateDirectories: true)
    } catch {
      logger.error("Failed to create database directory: \(error.localizedDescription)")
    }
  }

  /// Cleanup resources when the database manager is deallocated
  /// Ensures all database connections are properly closed
  deinit {
    // Close all database connections
    // Note: deinit in actors runs synchronously on the actor's executor
    for (userDID, db) in databases {
      do {
        try db.close()
        logger.info("🧹 [deinit] Closed database for user: \(userDID.prefix(20), privacy: .private)")
      } catch {
        logger.warning(
          "⚠️ [deinit] Failed to close database for \(userDID.prefix(20), privacy: .private): \(error.localizedDescription)"
        )
      }
    }
    databases.removeAll()
  }

  // MARK: - Lock Helpers
  // NOTE: Advisory file locks have been removed (2026-02) to prevent 0xdead10cc crashes.
  // SQLite WAL mode handles concurrent access. `MLSStateChangeNotifier`/`MLSNotificationCoordinator`
  // coordinate cache invalidation across processes instead.

  private func updateConnectionState(_ state: ConnectionState, for userDID: String) {
    connectionStates[userDID] = state
  }

  private func currentConnectionState(for userDID: String) -> ConnectionState {
    connectionStates[userDID] ?? .closed
  }

  private func keyFingerprint(_ key: Data) -> String {
    Data(key.prefix(8)).base64EncodedString()
  }

  // MARK: - Public API
  
  // MARK: - Smart Database Access (Auto-routes Active vs Inactive Users)

  /// Perform a database read operation with automatic routing based on user state.
  ///
  /// This is the **RECOMMENDED** method for all database reads. It automatically:
  /// - Uses DatabasePool for the active user (full performance)
  /// - Uses lightweight DatabaseQueue for inactive users (prevents OOM)
  /// - Uses lightweight DatabaseQueue when running in NSE
  ///
  /// This prevents the Error 7 (OOM) → Error 11 (Corruption) cascade that occurs
  /// when a heavy DatabasePool is opened for an inactive user while another user
  /// is already active.
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - work: The read operation to perform
  /// - Returns: The result of the operation
  public func read<T: Sendable>(
    for userDID: String,
    _ work: @Sendable @escaping (Database) throws -> T
  ) async throws -> T {
    try await waitForForegroundResumePreparationIfNeeded(for: userDID, operation: "read")
    try requireDatabaseAccess(for: userDID, operation: "read")

    // ═══════════════════════════════════════════════════════════════════════════
    // SMART ROUTING: Signal-style single-pool-per-database
    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL: In the main app, NEVER create ephemeral DatabaseQueues for a
    // database that is (or was) managed by a DatabasePool. Mixing connection
    // types on the same file causes rapid SHM open/close cycles that corrupt
    // the WAL coordination state, turning transient lock contention into
    // permanent Error 11 (database disk image is malformed).
    //
    // Main app: Always use DatabasePool (lazy-opened via getDatabasePool).
    // NSE only: Use lightweight DatabaseQueue (separate process, memory limited).
    // ═══════════════════════════════════════════════════════════════════════════

    let inExtension = isRunningInExtension
    let pooledDatabase = reusablePooledDatabaseForSmartRoute(for: userDID)

    // Determine if this user is inactive (not the app's foreground account).
    // We check activeUserDID first, but also handle the transient nil case:
    // during NSE handshake, activeUserDID is temporarily cleared. If another
    // user has an open pool, this user is inactive even when activeUserDID is nil.
    let isInactive: Bool
    if let activeDID = activeUserDID {
      isInactive = activeDID != userDID
    } else if pooledDatabase == nil {
      // activeUserDID is nil (transient handshake window) and this user has no pool.
      // Check if ANY other user has an open pool — if so, this user is inactive.
      let otherPoolExists = databases.contains { $0.key != userDID && $0.value != nil }
      isInactive = otherPoolExists
    } else {
      isInactive = false
    }

    do {
      let result: T
      if let pooledDatabase {
        // Existing pool available — always prefer it
        result = try await pooledDatabase.read(work)
      } else if inExtension {
        // NSE: Must use lightweight DatabaseQueue (separate process, 24MB memory limit)
        result = try await performLightweightRead(for: userDID, work)
      } else if isInactive {
        // Main app, inactive user: use ephemeral DatabasePool (not DatabaseQueue).
        // getEphemeralDatabasePool returns a DatabasePool — same connection type as the
        // active user — so no SHM corruption from mixing connection types in one process.
        let pool = try await getEphemeralDatabasePool(for: userDID)
        result = try await pool.read(work)
      } else {
        // Main app, active user, no pool cached: open a full DatabasePool.
        // Rapid ephemeral DatabaseQueue open/close cycles corrupt the SHM coordination state,
        // turning transient lock contention into permanent database corruption (Error 11).
        let pool = try await getDatabasePool(for: userDID)
        result = try await pool.read(work)
      }

      clearDatabaseAccessSuspensionIfNeeded(for: userDID, operation: "read")
      return result
    } catch {
      recordDatabaseAccessFailure(for: userDID, error: error, operation: "read")
      throw error
    }
  }

  /// Perform a database write operation with automatic routing based on user state.
  ///
  /// This is the **RECOMMENDED** method for all database writes. It automatically:
  /// - Uses DatabasePool for the active user (full performance)
  /// - Uses lightweight DatabaseQueue for inactive users (prevents OOM)
  /// - Uses lightweight DatabaseQueue when running in NSE
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - work: The write operation to perform
  /// - Returns: The result of the operation
  public func write<T: Sendable>(
    for userDID: String,
    _ work: @Sendable @escaping (Database) throws -> T
  ) async throws -> T {
    try await waitForForegroundResumePreparationIfNeeded(for: userDID, operation: "write")
    try requireDatabaseAccess(for: userDID, operation: "write")

    let inExtension = isRunningInExtension
    let pooledDatabase = reusablePooledDatabaseForSmartRoute(for: userDID)

    // Same transient-nil-safe inactive check as read(for:) — see comment there.
    let isInactive: Bool
    if let activeDID = activeUserDID {
      isInactive = activeDID != userDID
    } else if pooledDatabase == nil {
      let otherPoolExists = databases.contains { $0.key != userDID && $0.value != nil }
      isInactive = otherPoolExists
    } else {
      isInactive = false
    }

    do {
      let result: T
      if let pooledDatabase {
        result = try await pooledDatabase.write(work)
      } else if inExtension {
        // NSE: Must use lightweight DatabaseQueue (separate process, 24MB memory limit)
        result = try await performLightweightWrite(for: userDID, work)
      } else if isInactive {
        // Main app, inactive user: use ephemeral DatabasePool (same connection type, no SHM corruption)
        let pool = try await getEphemeralDatabasePool(for: userDID)
        result = try await pool.write(work)
      } else {
        // Main app, active user: open the primary DatabasePool
        let pool = try await getDatabasePool(for: userDID)
        result = try await pool.write(work)
      }

      // Signal-style budget checkpoint: decrement budget and trigger TRUNCATE checkpoint when needed
      didCompleteWrite(for: userDID)

      clearDatabaseAccessSuspensionIfNeeded(for: userDID, operation: "write")
      return result
    } catch {
      recordDatabaseAccessFailure(for: userDID, error: error, operation: "write")
      throw error
    }
  }

  // MARK: - NSE-Optimized Database Access

  /// Perform a database read operation optimized for Notification Service Extension.
  ///
  /// CRITICAL: NSE has a ~24MB memory limit. This method uses DatabaseQueue (single connection)
  /// instead of DatabasePool (multiple connections) to stay within memory constraints.
  ///
  /// Use this method in the NSE instead of getDatabasePool().read().
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - work: The read operation to perform
  /// - Returns: The result of the operation
  public func nseRead<T: Sendable>(
    for userDID: String,
    _ work: @Sendable @escaping (Database) throws -> T
  ) async throws -> T {
    try requireDatabaseAccess(for: userDID, operation: "nseRead")

    do {
      let result = try await performLightweightRead(for: userDID, work)
      clearDatabaseAccessSuspensionIfNeeded(for: userDID, operation: "nseRead")
      return result
    } catch {
      recordDatabaseAccessFailure(for: userDID, error: error, operation: "nseRead")
      throw error
    }
  }

  /// Perform a database write operation optimized for Notification Service Extension.
  ///
  /// CRITICAL: NSE has a ~24MB memory limit. This method uses DatabaseQueue (single connection)
  /// instead of DatabasePool (multiple connections) to stay within memory constraints.
  ///
  /// Use this method in the NSE instead of getDatabasePool().write().
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - work: The write operation to perform
  /// - Returns: The result of the operation
  public func nseWrite<T: Sendable>(
    for userDID: String,
    _ work: @Sendable @escaping (Database) throws -> T
  ) async throws -> T {
    try requireDatabaseAccess(for: userDID, operation: "nseWrite")

    do {
      let result = try await performLightweightWrite(for: userDID, work)

      // Signal-style budget checkpoint: decrement budget and trigger TRUNCATE checkpoint when needed
      // Note: In NSE we still track the budget, but the checkpoint may be deferred if no active pool exists
      didCompleteWrite(for: userDID)

      clearDatabaseAccessSuspensionIfNeeded(for: userDID, operation: "nseWrite")
      return result
    } catch {
      recordDatabaseAccessFailure(for: userDID, error: error, operation: "nseWrite")
      throw error
    }
  }

  /// Get or create encrypted DatabasePool for a user (actor isolation)
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: Encrypted GRDB DatabasePool
  /// - Throws: MLSSQLCipherError if creation fails or if called for inactive user
  /// - Important: Consider using `read(for:)` or `write(for:)` instead - they auto-route to lightweight access for inactive users.
  /// - Warning: DO NOT use this in NSE! Use nseRead/nseWrite instead.
  /// - Warning: This method now BLOCKS access for inactive users to prevent OOM → corruption.
  public func getDatabasePool(for userDID: String) async throws -> DatabasePool {
    try await waitForForegroundResumePreparationIfNeeded(
      for: userDID,
      operation: "getDatabasePool"
    )
    try requireDatabaseAccess(for: userDID, operation: "getDatabasePool")

    do {
    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL: BLOCK DatabasePool for inactive users
    // ═══════════════════════════════════════════════════════════════════════════
    // Opening a Pool for an inactive user while another user is active causes:
    // 1. Memory spike (2 Pools × 6MB = 12MB+ SQLCipher overhead)
    // 2. Error 7/21 (OOM) during checkpoint/transaction
    // 3. Error 11 (Corruption) from partial write
    //
    // This is no longer just a warning - we now BLOCK this dangerous operation.
    // Callers should use read(for:)/write(for:) which auto-route to lightweight
    // DatabaseQueue for inactive users.
    // ═══════════════════════════════════════════════════════════════════════════
    if isRunningInExtension {
      logger.error(
        "🛑 [NSE] BLOCKED: getDatabasePool called from extension - use nseRead/nseWrite instead")
      throw MLSSQLCipherError.storageUnavailable(
        reason: "NSE must use nseRead/nseWrite for memory safety")
    }
    
    if let activeDID = activeUserDID, activeDID != userDID {
      logger.error(
        """
        🛑 [OOM-BLOCKED] getDatabasePool BLOCKED for INACTIVE user!
        Requested: \(userDID.prefix(20), privacy: .private)
        Active: \(activeDID.prefix(20), privacy: .private)
        This would cause Error 7/21 (OOM) → Error 11 (Corruption).
        Use read(for:)/write(for:) instead - they auto-route to lightweight access.
        """)
      #if DEBUG
        logger.critical("getDatabasePool called for inactive user. Use read(for:)/write(for:) instead.")
      #endif
      throw MLSSQLCipherError.storageUnavailable(
        reason:
          "Cannot open heavy DatabasePool for inactive user. Use read(for:)/write(for:) instead."
      )
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // SINGLE GATE ARCHITECTURE: Check gate as the PRIMARY guard
    // - If gate is CLOSED: This is a new login - open the gate automatically
    // - If gate is CLOSING: Shutdown in progress - reject immediately
    // - If gate is OPEN: Proceed normally
    // ═══════════════════════════════════════════════════════════════════════════
    let gateState = await MLSDatabaseGate.shared.gateState(for: userDID)
    switch gateState {
    case .closing:
      logger.warning(
        "🚪 [Gate] getDatabasePool rejected - gate is closing for \(userDID.prefix(16))...")
      throw MLSSQLCipherError.storageUnavailable(reason: "Account switch in progress")
    case .closed:
      // Auto-open the gate for a new login
      await MLSDatabaseGate.shared.openGate(for: userDID)
      logger.info("🚪 [Gate] Auto-opened gate for new user: \(userDID.prefix(16), privacy: .private)...")
    case .open:
      break  // Already open, proceed
    }

    // CRITICAL FIX: Track which user is currently being accessed to detect key mismatch scenarios
    // Log the access for debugging account switching issues
    let currentGeneration = coordinationGeneration[userDID] ?? 0
    let connectionState = currentConnectionState(for: userDID)

    logger.debug(
      """
      📀 [DB-READ] getDatabasePool requested
      Requesting user: \(userDID.prefix(20), privacy: .private)...
      Active user: \(self.activeUserDID?.prefix(20) ?? "none", privacy: .private)...
      Generation: \(currentGeneration, privacy: .public)
      Connection state: \(connectionState.rawValue, privacy: .public)
      Thread: \(Thread.current.description, privacy: .public)
      """)

    // CRITICAL VALIDATION: Check if database manager has been closed
    guard !isClosed else {
      logger.error(
        "🚨 DB RACE PREVENTED: Database manager is closed, but \(userDID.prefix(20), privacy: .private) tried to access."
      )
      throw MLSSQLCipherError.databaseClosed
    }

    // CRITICAL: Clear stale cache after emergency suspension close
    // Emergency close happens synchronously from a nonisolated context and can't clear the actor's cache directly.
    // So we check the flag here and clear the cache if needed.
    if Self.emergencyCacheInvalidated {
      Self.emergencyCacheInvalidated = false
      logger.debug("🔄 [0xdead10cc-FIX] Clearing stale database cache after emergency close")
      databases.removeAll()
      uncachedEphemeralPools.removeAll()
    }

    // Allow concurrent pools for multiple users; log if this differs from active user.
    if let activeDID = activeUserDID, activeDID != userDID {
      logger.warning(
        """
        ⚠️ [DB-MULTI] Requesting pool for non-active user
        Requested: \(userDID.prefix(20), privacy: .private)...
        Active: \(activeDID.prefix(20), privacy: .private)...
        Operation: getDatabasePool
        Stack trace: \(Thread.callStackSymbols.prefix(5), privacy: .public)
        """)
    }

    // Track primary active user without blocking multi-user access.
    if activeUserDID == nil {
      activeUserDID = userDID
    } else if let previousUser = activeUserDID, previousUser != userDID {
      logger.info(
        """
        ℹ️ [DB-MULTI] Keeping active user unchanged for concurrent access
        Requested: \(userDID.prefix(20), privacy: .private)...
        Active: \(previousUser.prefix(20), privacy: .private)...
        Generation: \(currentGeneration, privacy: .public)
        """)
    }

    // Check cache first (actor isolation provides thread-safety)
    if let existingDatabase = databases[userDID] {
      // Validate the cached connection is still healthy.
      // IMPORTANT: Do not treat task cancellations as corruption; cancellation is expected in iOS lifecycle.
      do {
        _ = try await existingDatabase.read { db in
          // Touch sqlite_master so we catch codec/key/corruption issues (not just "can SQLite run a constant query").
          try Int.fetchOne(db, sql: "SELECT 1 FROM sqlite_master LIMIT 1;")
        }
        // Success - reset failure counter
        consecutiveHMACFailures.removeValue(forKey: userDID)
        return existingDatabase
      } catch let error as CancellationError {
        throw error
      } catch {
        // SAFE RECOVERY LADDER: HMAC failures are NOT auto-reset anymore.
        // Instead, we follow the configured recovery policy.
        if isHMACFailure(error) {
          logger.critical("💥 HMAC check failed during cached pool validation")
          logger.critical("   User: \(userDID.prefix(20), privacy: .private)")

          guard discardCachedDatabaseForRecovery(
            for: userDID,
            reason: "HMAC failure during cached pool validation"
          ) else {
            throw MLSSQLCipherError.storageUnavailable(
              reason: "MLS storage is busy while recycling a broken pooled connection"
            )
          }

          // Use safe recovery ladder instead of auto-reset
          return try await handleHMACFailure(
            for: userDID,
            error: error,
            mode: .primary,
            context: "cached pool validation"
          )
        } else if isSQLiteError7(error) {
          // ═══════════════════════════════════════════════════════════════════
          // CRITICAL FIX: SQLite error 7 (NOMEM) means connection is exhausted
          // Close and reopen WITHOUT deleting files
          // ═══════════════════════════════════════════════════════════════════
          logger.warning(
            "⚠️ [Error7] Cached connection returned SQLITE_NOMEM - closing and reopening")
          logger.warning("   This usually means file descriptor or mlock() quota exhaustion")

          guard discardCachedDatabaseForRecovery(
            for: userDID,
            reason: "cached pool returned SQLITE_NOMEM"
          ) else {
            throw MLSSQLCipherError.storageUnavailable(
              reason: "MLS storage is busy while recycling an exhausted pooled connection"
            )
          }
          
          // Brief pause to let resources free up
          try? await Task.sleep(nanoseconds: 50_000_000)  // 50ms

          // Fall through to create a fresh connection below
        } else if isRecoverableCodecError(error) {
          // Only treat known SQLCipher/SQLite corruption/codec failures as "unhealthy".
          // Avoid destructive recovery for transient errors (locks, timeouts, etc.).
          logger.warning(
            "⚠️ Cached database connection unhealthy, reconnecting: \(error.localizedDescription)")
          guard discardCachedDatabaseForRecovery(
            for: userDID,
            reason: "cached pool failed validation with recoverable codec error"
          ) else {
            throw MLSSQLCipherError.storageUnavailable(
              reason: "MLS storage is busy while recycling an unhealthy pooled connection"
            )
          }

          // Repair WAL/SHM files
          try? await repairDatabase(for: userDID)
        } else {
          logger.debug(
            "Database validation query failed (non-recoverable), reusing existing connection: \(error.localizedDescription)"
          )
          return existingDatabase
        }
      }
    }

    updateConnectionState(.opening, for: userDID)

    // ═══════════════════════════════════════════════════════════════════════════
    // CRITICAL: 0xdead10cc Migration - Check if database needs recreation
    // ═══════════════════════════════════════════════════════════════════════════
    // Databases created before cipher_plaintext_header_size=32 was added have
    // encrypted headers, causing iOS to fail to identify them as SQLite WAL
    // databases. This prevents automatic checkpointing during suspension,
    // leading to 0xdead10cc termination.
    //
    // This migration deletes old databases so they can be recreated with the
    // plaintext header. MLS conversation history will be lost, but this is
    // necessary to prevent 0xdead10cc crashes.
    // ═══════════════════════════════════════════════════════════════════════════
    let migrationPerformed = MLSPlaintextHeaderMigration.ensurePlaintextHeaderMigration(
      for: userDID,
      databaseType: .swiftGRDB
    )
    if migrationPerformed {
      logger.warning("🔧 [0xdead10cc] GRDB database was recreated for plaintext header migration")
    }

    // Check if database file already exists
    let dbPath = databasePath(for: userDID)
    let isNewDatabase = !FileManager.default.fileExists(atPath: dbPath.path)

    // NOTE: Advisory lock removed (2026-02) - SQLite WAL handles concurrent access.
    // `MLSStateChangeNotifier`/`MLSNotificationCoordinator` coordinate cache invalidation.

    // Create/open database (runs off main thread via actor isolation)
    do {
      let database = try await createDatabase(for: userDID)

      // Cache the database (actor isolation provides thread-safety)
      databases[userDID] = database
      Self.registerForEmergencyClose(database, for: userDID)
      updateConnectionState(.open, for: userDID)
      startPeriodicCheckpointingIfNeeded()

      // Successfully opened - clear any corruption counters
      walRepairState.removeValue(forKey: userDID)

      if isNewDatabase {
        logger.info("✨ Created new database pool for user: \(userDID, privacy: .private)")
      } else {
        logger.info("📂 Opened existing database pool for user: \(userDID, privacy: .private)")
      }

      return database
    } catch {
      updateConnectionState(.closed, for: userDID)
      // SAFE RECOVERY LADDER: HMAC failures use configured policy, not auto-reset.
      if isHMACFailure(error) {
        logger.critical("🚨 MLS storage open failed with HMAC/NOTADB")
        logger.critical("   Error: \(error.localizedDescription)")

        return try await handleHMACFailure(
          for: userDID,
          error: error,
          mode: .primary,
          context: "database open"
        )
      }
      
      // If database creation fails with other corruption error, attempt progressive repair
      // This will escalate from WAL/SHM repair to full reset if needed
      if isRecoverableCodecError(error) {
        logger.warning(
          "⚠️ Database creation failed, attempting progressive repair: \(error.localizedDescription)"
        )

        // Use progressive repair which handles escalation automatically
        // Pass the original error so it can distinguish transient vs corruption
        return try await progressiveRepair(for: userDID, lastError: error)
      }

      // Re-throw if not a recoverable error
      throw error
    }
    } catch {
      recordDatabaseAccessFailure(for: userDID, error: error, operation: "getDatabasePool")
      throw error
    }
  }
  /// Execute a block with safe, gated database access.
  ///
  /// This is the PREFERRED method for all database operations. It ensures:
  /// 1. The gate is open (auto-opens if needed)
  /// 2. The operation is tracked by the gate (preventing shutdown while running)
  /// 3. The operation is drained before shutdown completes
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - block: Async block that uses the database
  /// - Returns: Result of the block
  public func withDatabase<T>(
    for userDID: String,
    perform block: @Sendable (DatabasePool) async throws -> T
  ) async throws -> T {
    // Check if this is the active user - if not, we can't provide a DatabasePool
    // (Callers should use read(for:)/write(for:) which auto-route to lightweight access)
    if let activeDID = activeUserDID, activeDID != userDID {
      logger.error(
        """
        🛑 [OOM-BLOCKED] withDatabase BLOCKED for INACTIVE user!
        Requested: \(userDID.prefix(20), privacy: .private)
        Active: \(activeDID.prefix(20), privacy: .private)
        Use read(for:)/write(for:) instead - they auto-route to lightweight access.
        """)
      #if DEBUG
        logger.critical("withDatabase called for inactive user. Use read(for:)/write(for:) instead.")
      #endif
      throw MLSSQLCipherError.storageUnavailable(
        reason: "Cannot use withDatabase for inactive user. Use read(for:)/write(for:) instead."
      )
    }

    // 1. Ensure database is available (handles auto-open)
    let db = try await getDatabasePool(for: userDID)

    // 2. Acquire connection token to block shutdown while we work
    let token = try await MLSDatabaseGate.shared.acquireConnection(for: userDID)

    defer {
      // 3. Release token when done
      Task { await MLSDatabaseGate.shared.releaseConnection(token) }
    }

    // 4. Perform the work
    return try await block(db)
  }


  /// Legacy method for backwards compatibility - returns DatabaseQueue interface
  /// New code should use getDatabasePool(for:) instead
  @available(
    *, deprecated, renamed: "getDatabasePool(for:)",
    message: "Use getDatabasePool for better concurrency"
  )
  public func getDatabaseQueue(for userDID: String) async throws -> DatabasePool {
    return try await getDatabasePool(for: userDID)
  }

  // MARK: - Ephemeral Database Access (Notification Decryption)

  /// Get database pool for a user WITHOUT triggering active database switching
  ///
  /// CRITICAL: Use this method for notification decryption when the notification
  /// is for a user OTHER than the currently active user.
  ///
  /// This method:
  /// - Does NOT checkpoint the currently active database
  /// - Does NOT change the activeUserDID tracking
  /// - DOES cache the connection for reuse
  /// - DOES allow concurrent access to multiple user databases
  /// - DOES perform key validation before full open (Phase 2)
  /// - DOES retry with exponential backoff on resource exhaustion (SQLite error 7)
  ///
  /// This prevents the "database locked" error when:
  /// 1. User A is active and using their database (e.g., polling, UI operations)
  /// 2. A push notification arrives for User B
  /// 3. The notification handler needs to decrypt User B's message
  ///
  /// Previously, this would try to checkpoint User A's database, which fails
  /// if User A has active read/write operations in progress.
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: Encrypted GRDB DatabasePool for read/write operations
  /// - Throws: MLSSQLCipherError if creation fails or if called for inactive user
  /// - Warning: This method now BLOCKS access for inactive users to prevent OOM → corruption.
  ///   Use `read(for:)`/`write(for:)` instead which auto-route to lightweight access.
  public func getEphemeralDatabasePool(for userDID: String) async throws -> DatabasePool {
    try await waitForForegroundResumePreparationIfNeeded(
      for: userDID,
      operation: "getEphemeralDatabasePool"
    )
    try requireDatabaseAccess(for: userDID, operation: "getEphemeralDatabasePool")

    do {
    // ═══════════════════════════════════════════════════════════════════════════
    // EPHEMERAL ACCESS: Lightweight access for inactive users
    // ═══════════════════════════════════════════════════════════════════════════
    // This method is specifically designed for:
    // 1. NSE accessing any user's database
    // 2. Main app accessing inactive user's data (e.g., notification decryption)
    //
    // For INACTIVE users: We reuse their existing cached pool if available,
    // otherwise we proceed carefully with gate checks.
    //
    // The heavy blocking is only in getDatabasePool() - this method is the
    // safe path for cross-user access.
    // ═══════════════════════════════════════════════════════════════════════════
    let isInactive = activeUserDID != nil && activeUserDID != userDID
    if isInactive {
      logger.info(
        """
        📀 [Ephemeral] Accessing INACTIVE user database
        Requested: \(userDID.prefix(20), privacy: .private)
        Active: \(self.activeUserDID?.prefix(20) ?? "none", privacy: .private)
        Strategy: Reuse existing pool if available, otherwise open with care
        """)
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SINGLE GATE ARCHITECTURE: Check gate as the PRIMARY guard
    // - If gate is CLOSED: Auto-open for NSE access to inactive user
    // - If gate is CLOSING: Main app is switching accounts - reject
    // ═══════════════════════════════════════════════════════════════════════════
    let gateState = await MLSDatabaseGate.shared.gateState(for: userDID)
    switch gateState {
    case .closing:
      logger.warning(
        "🚪 [Gate] getEphemeralDatabasePool rejected - gate is closing for \(userDID.prefix(16))...")
      throw MLSSQLCipherError.storageUnavailable(reason: "Account switch in progress")
    case .closed:
      // Auto-open for NSE access to inactive user's database
      await MLSDatabaseGate.shared.openGate(for: userDID)
      logger.info("🚪 [Gate] Auto-opened gate for ephemeral access: \(userDID.prefix(16), privacy: .private)...")
    case .open:
      break
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 3: Exponential Backoff Retry for SQLite error 7 (resource exhaustion)
    // ═══════════════════════════════════════════════════════════════════════════
    let maxRetries = 3
    let baseDelayNanos: UInt64 = 100_000_000  // 100ms
    
    for attempt in 0..<maxRetries {
      do {
        return try await _getEphemeralDatabasePoolInternal(for: userDID)
      } catch {
        if isSQLiteError7(error) && attempt < maxRetries - 1 {
          let delayNanos = baseDelayNanos * (1 << attempt)  // 100ms, 200ms, 400ms
          let delayMs = delayNanos / 1_000_000
          logger.warning(
            "🔄 [Retry] Resource exhaustion on attempt \(attempt + 1)/\(maxRetries), backoff: \(delayMs)ms"
          )

          try await Task.sleep(nanoseconds: delayNanos)
          continue
        }
        throw error
      }
    }
    
    // Should never reach here, but satisfy compiler
    throw MLSSQLCipherError.databaseCreationFailed(
      underlying: NSError(
        domain: "MLSGRDBManager",
        code: -6,
        userInfo: [NSLocalizedDescriptionKey: "Max retries exceeded"]
      ))
    } catch {
      recordDatabaseAccessFailure(for: userDID, error: error, operation: "getEphemeralDatabasePool")
      throw error
    }
  }
  
  /// Internal implementation of getEphemeralDatabasePool
  private func _getEphemeralDatabasePoolInternal(for userDID: String) async throws -> DatabasePool {
    logger.debug(
      "📀 [Ephemeral] getDatabasePool requested for user: \(userDID.prefix(20), privacy: .private)..."
    )
    logger.debug("   Active user: \(self.activeUserDID?.prefix(20) ?? "none", privacy: .private)")
    logger.debug("   Strategy: NO checkpoint, NO active user switch")

    // CRITICAL: Clear stale cache after emergency suspension close
    // Emergency close happens synchronously from a nonisolated context and can't clear the actor's cache directly.
    // Without this check, closed pools remain in `databases` and get returned to callers.
    if Self.emergencyCacheInvalidated {
      Self.emergencyCacheInvalidated = false
      logger.debug("🔄 [0xdead10cc-FIX] Clearing stale database cache after emergency close (ephemeral path)")
      databases.removeAll()
      uncachedEphemeralPools.removeAll()
    }

    // NOTE: We intentionally DO NOT checkpoint or switch the active user here
    // This allows concurrent access to multiple user databases

    // Check cache first
    if let existingDatabase = databases[userDID] {
      // Validate the connection is healthy
      do {
        _ = try await existingDatabase.read { db in
          try Int.fetchOne(db, sql: "SELECT 1 FROM sqlite_master LIMIT 1;")
        }
        logger.debug(
          "✅ [Ephemeral] Reusing cached database for user: \(userDID.prefix(20), privacy: .private)"
        )
        return existingDatabase
      } catch is CancellationError {
        throw CancellationError()
      } catch {
        // Connection unhealthy - remove and recreate
        if isHMACFailure(error) {
          logger.critical("🔐 [Ephemeral] HMAC check failed")
          guard discardCachedDatabaseForRecovery(
            for: userDID,
            reason: "ephemeral cached validation hit HMAC failure"
          ) else {
            throw MLSSQLCipherError.storageUnavailable(
              reason: "MLS storage is busy while recycling a broken pooled connection"
            )
          }

          return try await handleHMACFailure(
            for: userDID,
            error: error,
            mode: .ephemeral,
            context: "ephemeral cached validation"
          )
        } else if isSQLiteError7(error) {
          // SQLite error 7 - close and reopen WITHOUT deleting files
          logger.warning("⚠️ [Ephemeral] SQLITE_NOMEM - closing and reopening")
          guard discardCachedDatabaseForRecovery(
            for: userDID,
            reason: "ephemeral cached pool returned SQLITE_NOMEM"
          ) else {
            throw MLSSQLCipherError.storageUnavailable(
              reason: "MLS storage is busy while recycling an exhausted pooled connection"
            )
          }
          try? await Task.sleep(nanoseconds: 50_000_000)  // 50ms
          // Fall through to create fresh connection
        } else if isRecoverableCodecError(error) {
          logger.warning("⚠️ [Ephemeral] Cached connection unhealthy, reconnecting")
          guard discardCachedDatabaseForRecovery(
            for: userDID,
            reason: "ephemeral cached pool failed validation with recoverable codec error"
          ) else {
            throw MLSSQLCipherError.storageUnavailable(
              reason: "MLS storage is busy while recycling an unhealthy pooled connection"
            )
          }
          try? await repairDatabase(for: userDID)
        } else {
          // Connection is broken (e.g. SQLITE_MISUSE after emergency close) - discard and recreate
          logger.warning("⚠️ [Ephemeral] Cached connection failed validation (code=\((error as NSError).code)): \(error.localizedDescription) - discarding")
          guard discardCachedDatabaseForRecovery(
            for: userDID,
            reason: "ephemeral cached pool failed validation unexpectedly"
          ) else {
            throw MLSSQLCipherError.storageUnavailable(
              reason: "MLS storage is busy while recycling an invalid pooled connection"
            )
          }
          // Fall through to create fresh connection
        }
      }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 2: Key Validation Before Full Open
    // ═══════════════════════════════════════════════════════════════════════════
    // Perform lightweight validation to catch HMAC issues early
    let encryptionKey = try await ensureKeyForDatabase(for: userDID)
    let keyValid = try await validateKeyBeforeOpen(for: userDID, key: encryptionKey)
    
    if !keyValid {
      logger.critical("🔑 [Ephemeral] Key validation failed")
      return try await handleHMACFailure(
        for: userDID,
        error: MLSSQLCipherError.invalidEncryptionKey(reason: "Key validation failed"),
        mode: .ephemeral,
        context: "key validation"
      )
    }

    // NOTE: Advisory lock removed (2026-02) - SQLite WAL handles concurrent access.

    // Create new database connection
    // CRITICAL FIX: Do NOT cache ephemeral connections for non-active users
    // Caching creates memory pressure from accumulated mlock() allocations
    do {
      let database = try await createDatabase(for: userDID)
      let cachedDatabase = await cacheEphemeralDatabase(database, for: userDID)
      // Success - reset failure counter
      consecutiveHMACFailures.removeValue(forKey: userDID)
      logger.info("✅ [Retry] Success on attempt")
      return cachedDatabase
    } catch {
      if isHMACFailure(error) {
        return try await handleHMACFailure(
          for: userDID,
          error: error,
          mode: .ephemeral,
          context: "ephemeral open"
        )
      }
      if isRecoverableCodecError(error) {
        logger.warning("⚠️ [Ephemeral] Database creation failed, attempting repair")
        return try await progressiveRepair(for: userDID, lastError: error)
      }
      throw error
    }
  }

  /// Check if a user is the currently active database user
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: true if this user is the active database user
  public func isActiveUser(_ userDID: String) -> Bool {
    return activeUserDID == userDID
  }

  private func reusablePooledDatabaseForSmartRoute(for userDID: String) -> DatabasePool? {
    guard !isRunningInExtension, let existingPool = databases[userDID] else {
      return nil
    }

    if activeUserDID == nil {
      logger.warning(
        "🔧 [Active] Restoring active user tracking from pooled database for: \(userDID.prefix(20), privacy: .private)"
      )
      activeUserDID = userDID
    } else if
      let activeDID = activeUserDID,
      activeDID != userDID,
      databases[activeDID] == nil
    {
      logger.warning(
        """
        🔧 [Active] Repairing stale active user tracking from pooled database
        Previous active: \(activeDID.prefix(20), privacy: .private)
        Repaired active: \(userDID.prefix(20), privacy: .private)
        """
      )
      activeUserDID = userDID
    }

    return existingPool
  }
  
  /// Set a user as the active user for database access.
  ///
  /// Call this BEFORE `getDatabasePool()` during account switching or initial login.
  /// This allows `getDatabasePool()` to succeed for the new active user.
  ///
  /// - Parameter userDID: User's decentralized identifier (or nil to clear)
  public func setActiveUser(_ userDID: String?) {
    let oldUser = activeUserDID?.prefix(20) ?? "nil"
    let newUser = userDID?.prefix(20) ?? "nil"
    logger.info("🔄 [Active] Switching active user: \(oldUser) → \(newUser)")
    activeUserDID = userDID
  }

  @discardableResult
  private func discardCachedDatabaseForRecovery(
    for userDID: String,
    reason: String
  ) -> Bool {
    guard databases[userDID] != nil else {
      return true
    }

    logger.warning(
      """
      🧹 [Recovery] Discarding cached database before recovery
      User: \(userDID.prefix(20), privacy: .private)
      Reason: \(reason, privacy: .public)
      """
    )
    closeDatabase(for: userDID)

    guard databases[userDID] == nil else {
      logger.warning(
        """
        🚫 [Recovery] Cached database remained open after close attempt
        User: \(userDID.prefix(20), privacy: .private)
        Reason: \(reason, privacy: .public)
        """
      )
      return false
    }

    return true
  }

  // MARK: - Lightweight Ephemeral Access (Notifications)
  
  /// Perform a lightweight read-only database task for a user without caching the connection
  ///
  /// CRITICAL: Use this for notification decryption when the notification is for a user
  /// OTHER than the currently active user. This method:
  /// - Uses DatabaseQueue (lighter weight than DatabasePool)
  /// - Does NOT cache the connection (closes immediately after use)
  /// - Does NOT trigger active user switching
  /// - Minimizes memory lock pressure from SQLCipher
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - work: The read-only database operation to perform
  /// - Returns: The result of the database operation
  /// - Throws: Error if database cannot be opened or operation fails
  public func performLightweightRead<T: Sendable>(
    for userDID: String,
    _ work: @Sendable @escaping (Database) throws -> T
  ) async throws -> T {
    logger.debug("📀 [Lightweight] Read requested for user: \(userDID.prefix(20), privacy: .private)")

    // NOTE: Advisory lock removed (2026-02) - SQLite WAL handles concurrent access.

    // ═══════════════════════════════════════════════════════════════════════════
    // NSE MEMORY OPTIMIZATION: Always use DatabaseQueue in extension context
    // ═══════════════════════════════════════════════════════════════════════════
    // NSE has ~24MB memory limit. DatabasePool creates multiple connections,
    // each with SQLCipher encryption buffers (~2MB+ per connection).
    // DatabaseQueue uses a single serial connection, drastically reducing memory.
    // ═══════════════════════════════════════════════════════════════════════════

    // If we already have a running pool for this user AND we're not in NSE, use it
    if !isRunningInExtension, let existingPool = databases[userDID] {
      logger.debug("   Using existing cached pool")
      return try await existingPool.read(work)
    }
    
    // For NSE or non-active users, create a lightweight queue, use it, and close immediately
    logger.debug("   Creating lightweight DatabaseQueue (no caching)")

    // CRITICAL: 0xdead10cc Migration - ensure database has plaintext header
    _ = MLSPlaintextHeaderMigration.ensurePlaintextHeaderMigration(
      for: userDID,
      databaseType: .swiftGRDB
    )

    let dbPath = databasePath(for: userDID)

    // Get encryption key and salt
    let encryptionKey = try await ensureKeyForDatabase(for: userDID)
    let salt = try await ensureSaltForDatabase(for: userDID, dbPath: dbPath)

    // Configure ultra-lightweight connection for NSE
    var config = Configuration()
    config.readonly = true  // Read-only for safety
    config.busyMode = Self.nseBusyMode(maxRetries: 20)  // 20 * 25ms = 500ms max
    config.observesSuspensionNotifications = true

    config.prepareDatabase { db in
      // CRITICAL: Set memory security OFF first, before ANY other cipher operation
      // This prevents mlock() calls that can exhaust NSE memory limits
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")

      // CRITICAL: PRAGMA key MUST be the first cipher operation on the connection.
      // All other cipher_* pragmas (plaintext_header_size, salt, page_size, etc.)
      // are silently ignored if set before the key.
      let hexKey = encryptionKey.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA key = \"x'\(hexKey)'\";")

      // iOS Shared Container Fix: Leave header unencrypted so iOS recognizes
      // the file as SQLite and exempts WAL locks from 0xDEAD10CC termination.
      // MUST be after PRAGMA key or it has no effect!
      try db.execute(sql: "PRAGMA cipher_plaintext_header_size = 32;")

      // Explicit salt (REQUIRED when using plaintext header)
      let hexSalt = salt.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA cipher_salt = \"x'\(hexSalt)'\";")

      // SQLCipher 4 settings
      try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
      try db.execute(sql: "PRAGMA kdf_iter = 256000;")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")

      // ═══════════════════════════════════════════════════════════════════════════
      // NSE MEMORY OPTIMIZATION: Aggressive cache reduction
      // ═══════════════════════════════════════════════════════════════════════════
      // Default cache can be 4MB+. For NSE with 24MB limit, use 256KB max.
      // This is sufficient for simple read operations.
      try db.execute(sql: "PRAGMA cache_size = -256;")  // 256KB only

      // Disable memory-mapped I/O to prevent shared memory exhaustion
      try db.execute(sql: "PRAGMA mmap_size = 0;")
    }
    
    // Reuse a cached lightweight queue if available (prevents rapid open/close SHM corruption).
    let queue: DatabaseQueue
    if let cached = cachedLightweightQueues[userDID],
       Date().timeIntervalSince(cached.lastUsed) < lightweightQueueTTL {
      queue = cached.queue
      logger.debug("   Reusing cached lightweight queue")
    } else {
      // Close any expired cached queue before creating a new one
      if let old = cachedLightweightQueues.removeValue(forKey: userDID) {
        try? old.queue.close()
      }
      queue = try DatabaseQueue(path: dbPath.path, configuration: config)
      logger.debug("   Created new lightweight queue (will cache for reuse)")
    }

    // Update last-used timestamp
    cachedLightweightQueues[userDID] = (queue: queue, lastUsed: Date())

    // Schedule eviction after TTL (only if no other read refreshes the timestamp)
    let ttl = self.lightweightQueueTTL
    Task { [self] in
      try? await Task.sleep(nanoseconds: UInt64(ttl * 1_500_000_000))  // 1.5x TTL
      if let cached = cachedLightweightQueues[userDID],
         Date().timeIntervalSince(cached.lastUsed) >= ttl {
        cachedLightweightQueues.removeValue(forKey: userDID)
        try? cached.queue.close()
        logger.debug("✅ [Lightweight] Evicted cached queue for: \(userDID.prefix(20), privacy: .private)")
      }
    }

    return try await queue.read(work)
  }

  // MARK: - NSE Lightweight Write Access

  /// Perform a lightweight database write operation using DatabaseQueue.
  ///
  /// CRITICAL: This method is designed for Notification Service Extension (NSE) use.
  /// NSE has a ~24MB memory limit. This method uses a single-connection DatabaseQueue
  /// instead of DatabasePool to minimize memory footprint.
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - work: The database write operation to perform
  /// - Returns: The result of the database operation
  /// - Throws: Error if database cannot be opened or operation fails
  public func performLightweightWrite<T: Sendable>(
    for userDID: String,
    _ work: @Sendable @escaping (Database) throws -> T
  ) async throws -> T {
    logger.debug(
      "📀 [Lightweight] Write requested for user: \(userDID.prefix(20), privacy: .private)")

    // Block destructive operations in extension
    if isRunningInExtension {
      logger.debug("   Running in extension context - using lightweight queue")
    }

    // No advisory lock needed - SQLite WAL handles concurrent access
    // Cross-process coordination uses `MLSStateChangeNotifier`/`MLSNotificationCoordinator`

    // If we have a running pool for this user AND we're not in NSE, use it
    if !isRunningInExtension, let existingPool = databases[userDID] {
      logger.debug("   Using existing cached pool for write")
      return try await existingPool.write(work)
    }

    // For NSE or non-active users, create a lightweight queue
    logger.debug("   Creating lightweight DatabaseQueue for write")

    // CRITICAL: 0xdead10cc Migration - ensure database has plaintext header
    _ = MLSPlaintextHeaderMigration.ensurePlaintextHeaderMigration(
      for: userDID,
      databaseType: .swiftGRDB
    )

    let dbPath = databasePath(for: userDID)

    // Get encryption key and salt
    let encryptionKey = try await ensureKeyForDatabase(for: userDID)
    let salt = try await ensureSaltForDatabase(for: userDID, dbPath: dbPath)

    // Configure ultra-lightweight connection for NSE (Signal-style)
    var config = Configuration()
    // Note: defaultTransactionKind is auto-managed in GRDB 7.0+
    config.allowsUnsafeTransactions = true  // Allow checkpoint-without-transaction
    config.maximumReaderCount = 4  // Signal uses 4 for extensions
    config.readonly = false
    config.busyMode = Self.nseBusyMode(maxRetries: 80)  // 80 * 25ms = 2s max
    config.observesSuspensionNotifications = true

    config.prepareDatabase { db in
      // CRITICAL: Set memory security OFF first
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")

      // CRITICAL: PRAGMA key MUST be the first cipher operation on the connection.
      // All other cipher_* pragmas are silently ignored if set before the key.
      let hexKey = encryptionKey.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA key = \"x'\(hexKey)'\";")

      // iOS Shared Container Fix — MUST be after PRAGMA key!
      try db.execute(sql: "PRAGMA cipher_plaintext_header_size = 32;")

      // Explicit salt
      let hexSalt = salt.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA cipher_salt = \"x'\(hexSalt)'\";")

      // SQLCipher 4 settings
      try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
      try db.execute(sql: "PRAGMA kdf_iter = 256000;")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")

      // NSE memory optimization
      try db.execute(sql: "PRAGMA cache_size = -512;")  // 512KB for writes
      try db.execute(sql: "PRAGMA mmap_size = 0;")

      // WAL mode - disable auto-checkpoints (main app's budget system handles it)
      try db.execute(sql: "PRAGMA journal_mode = WAL;")
      try db.execute(sql: "PRAGMA wal_autocheckpoint = 0;")
      try db.execute(sql: "PRAGMA synchronous = NORMAL;")
      // Ensure NSE checkpoint pages are fully fsynced to disk before transfer.
      // Without this, a PASSIVE checkpoint from the NSE can leave partially-written
      // WAL pages that corrupt the database if iOS kills the extension mid-checkpoint.
      try db.execute(sql: "PRAGMA checkpoint_fullfsync = ON;")
    }

    // Open, write, close immediately
    let queue = try DatabaseQueue(path: dbPath.path, configuration: config)
    defer {
      do {
        try queue.close()
        logger.debug(
          "✅ [Lightweight] Closed write queue for: \(userDID.prefix(20), privacy: .private)")
      } catch {
        logger.warning("⚠️ [Lightweight] Failed to close write queue: \(error.localizedDescription)")
      }
    }

    return try await queue.write(work)
  }

  private func startPeriodicCheckpointingIfNeeded() {
    guard periodicCheckpointTask == nil else { return }

    periodicCheckpointTask = Task { [weak self] in
      while let self {
        try? await Task.sleep(nanoseconds: UInt64(self.periodicCheckpointInterval * 1_000_000_000))
        await self.performPeriodicCheckpoint()
      }
    }
  }

  private func performPeriodicCheckpoint() async {
    // Best-effort optimization to keep WAL small so switch-time drain is fast.
    let didToCheckpoint = activeUserDID ?? databases.keys.first
    guard let didToCheckpoint, let db = databases[didToCheckpoint] else { return }

    // No advisory lock needed - SQLite WAL PASSIVE checkpoint is safe for concurrent access
    // PASSIVE mode won't block readers/writers and won't be blocked by them

    do {
      // Direct checkpoint - no redundant NSFileCoordinator wrapper needed
      try await db.writeWithoutTransaction { db in
        try db.execute(sql: "PRAGMA wal_checkpoint(PASSIVE);")
      }
      logger.debug("✅ Periodic WAL checkpoint(PASSIVE) for \(didToCheckpoint.prefix(20), privacy: .private)")
    } catch {
      logger.debug("⏭️ Periodic checkpoint skipped: \(error.localizedDescription)")
    }
  }

  /// Close database for a user
  /// - Parameter userDID: User's decentralized identifier
  public func closeDatabase(for userDID: String) {
    let wasActiveUser = (activeUserDID == userDID)

    // Also close any cached lightweight queue to prevent stale SHM references
    if let cached = cachedLightweightQueues.removeValue(forKey: userDID) {
      try? cached.queue.close()
    }

    guard let db = databases[userDID] else {
      logger.debug("No database to close for user: \(userDID, privacy: .private)")
      if wasActiveUser {
        activeUserDID = nil
      }
      updateConnectionState(.closed, for: userDID)
      return
    }

    logger.info("Closing database for user: \(userDID, privacy: .private)")
    updateConnectionState(.closing, for: userDID)

    // Best-effort checkpoint to truncate WAL before closing.
    do {
      try db.writeWithoutTransaction { database in
        try database.execute(sql: "PRAGMA wal_checkpoint(TRUNCATE);")
      }
      logger.debug(
        "✅ [Checkpoint] TRUNCATE completed before close for: \(userDID.prefix(20), privacy: .private)"
      )
    } catch {
      logger.debug("⏭️ [Checkpoint] TRUNCATE skipped before close: \(error.localizedDescription)")
    }

    // Fail-closed: do not interrupt in-flight queries; if close cannot complete, keep the pool alive.
    do {
      try db.close()
      databases.removeValue(forKey: userDID)
      Self.unregisterFromEmergencyClose(for: userDID)
      if wasActiveUser {
        activeUserDID = nil
      }
      updateConnectionState(.closed, for: userDID)
      logger.info("✅ Database pool closed for user: \(userDID, privacy: .private)")
    } catch {
      updateConnectionState(.open, for: userDID)
      if wasActiveUser {
        activeUserDID = userDID
      }
      logger.warning("⚠️ Database pool close deferred (busy): \(error.localizedDescription)")
    }
  }

  /// Close database and wait for all pending operations to complete
  /// Use this during account switching to ensure clean state before opening new database
  /// - Parameter userDID: User's decentralized identifier
  /// - Parameter timeout: Maximum time to wait for pending operations (seconds)
  /// - Returns: True if close completed successfully, false if timed out
  @discardableResult
  public func closeDatabaseAndDrain(for userDID: String, timeout: TimeInterval = 5.0) async -> Bool {
    let duration = Duration.milliseconds(Int(timeout * 1000))
    let currentGeneration = coordinationGeneration[userDID] ?? 0

    do {
      return try await withMLSExclusiveAccess(
        userDID: userDID,
        purpose: .closeAndDrain,
        timeout: duration
      ) { [self] in
        logger.info(
          """
          🛑 [DB-CLOSE] Closing and draining database
          User: \(userDID.prefix(20), privacy: .private)...
          Generation: \(currentGeneration, privacy: .public)
          Timeout: \(timeout, privacy: .public)s
          Thread: \(Thread.current.description, privacy: .public)
          """)

        // Prevent new opens while we're trying to close.
        updateConnectionState(.closing, for: userDID)
        let wasActiveUser = (activeUserDID == userDID)

        // CRITICAL FIX: Close any uncached ephemeral pools for this user
        // These are created during notification handling for non-active users
        if let ephemeralPool = uncachedEphemeralPools.removeValue(forKey: userDID) {
          logger.info(
            "🧹 [Ephemeral] Closing uncached ephemeral pool for: \(userDID.prefix(20), privacy: .private)"
          )
          // DEFENSIVE TIMEOUT: Wrap ephemeral checkpoint in 2-second timeout
          let checkpointOk = await withTaskGroup(of: Bool.self) { group in
            group.addTask {
              do {
                try await ephemeralPool.writeWithoutTransaction { db in
                  try db.execute(sql: "PRAGMA wal_checkpoint(TRUNCATE);")
                }
                return true
              } catch {
                return false
              }
            }
            group.addTask {
              try? await Task.sleep(nanoseconds: 2_000_000_000)  // 2 seconds
              return false
            }
            let result = await group.next() ?? false
            group.cancelAll()
            return result
          }

          do {
            try ephemeralPool.close()
            if checkpointOk {
              logger.info(
                "✅ [Ephemeral] Closed ephemeral pool for: \(userDID.prefix(20), privacy: .private)")
            } else {
              logger.warning(
                "⏱️ [Ephemeral] Checkpoint timed out - forced close for: \(userDID.prefix(20), privacy: .private)")
            }
          } catch {
            logger.warning(
              "⚠️ [Ephemeral] Failed to close ephemeral pool: \(error.localizedDescription)")
          }
        }

        guard let db = databases[userDID] else {
          logger.debug("   No database to close for user: \(userDID, privacy: .private)")
          if wasActiveUser {
            activeUserDID = nil
          }
          updateConnectionState(.closed, for: userDID)
          return true
        }

        do {
          logger.info("📀 [Checkpoint] Starting TRUNCATE checkpoint for: \(userDID.prefix(20), privacy: .private)")
          let checkpointStart = Date()

          // DEFENSIVE TIMEOUT: Wrap checkpoint in 3-second timeout to prevent indefinite blocking
          // If GRDB is waiting for a writer connection that will never be available (due to
          // corruption, lock contention, or FFI holding the connection), we skip checkpoint
          // and proceed to force-close. Data may be lost but app won't freeze.
          let checkpointSucceeded = await withTaskGroup(of: Bool.self) { group in
            group.addTask {
              do {
                try await db.writeWithoutTransaction { database in
                  try database.execute(sql: "PRAGMA wal_checkpoint(TRUNCATE);")
                }
                return true
              } catch {
                return false
              }
            }
            group.addTask {
              try? await Task.sleep(nanoseconds: 3_000_000_000)  // 3 seconds
              return false
            }
            let result = await group.next() ?? false
            group.cancelAll()
            return result
          }

          let duration = Date().timeIntervalSince(checkpointStart)
          let durationStr = String(format: "%.2f", duration)

          if checkpointSucceeded {
            logger.info("✅ [Checkpoint] TRUNCATE completed in \(durationStr)s")
          } else {
            logger.warning("⏱️ [Checkpoint] Checkpoint timed out or failed after \(durationStr)s - proceeding with forced close")
          }

          // Fail-closed: do not interrupt in-flight queries; if close cannot complete, report busy.
          try db.close()
          databases.removeValue(forKey: userDID)
          if wasActiveUser {
            activeUserDID = nil
          }
          updateConnectionState(.closed, for: userDID)
          logger.info("✅ Database closed and drained for user: \(userDID, privacy: .private)")
          return true

        } catch {
          updateConnectionState(.open, for: userDID)
          if wasActiveUser {
            activeUserDID = userDID
          }
          logger.warning("🚨 Database drain/checkpoint did not complete for \(userDID.prefix(20), privacy: .private): \(error.localizedDescription)")
          return false
        }
      }
    } catch is MLSExclusiveAccessError {
      logger.warning("🔒 [Close] Exclusive access busy - close/drain cancelled for \(userDID.prefix(20), privacy: .private)")
      return false
    } catch {
      logger.warning("🔒 [Close] Exclusive access error: \(error.localizedDescription)")
      return false
    }
  }
  
  /// Release database connection WITHOUT checkpointing.
  ///
  /// Use this when another process (like NSE) will handle the checkpoint.
  /// This method simply interrupts running queries and closes the connection,
  /// releasing file locks so the other process can checkpoint.
  ///
  /// - Parameter userDID: User's decentralized identifier
  @discardableResult
  public func releaseConnectionWithoutCheckpoint(for userDID: String) async -> Bool {
    do {
      return try await withMLSExclusiveAccess(
        userDID: userDID,
        purpose: .closeAndDrain,
        timeout: .seconds(2)
      ) { [self] in
        logger.info("🔓 [Release] Releasing connection WITHOUT checkpoint for: \(userDID.prefix(20), privacy: .private)")
        let wasActiveUser = (activeUserDID == userDID)

        guard let db = databases[userDID] else {
          logger.debug("   No database connection to release for user: \(userDID.prefix(20), privacy: .private)")
          if wasActiveUser {
            activeUserDID = nil
          }
          updateConnectionState(.closed, for: userDID)
          return true
        }

        updateConnectionState(.closing, for: userDID)
        incrementCoordinationGeneration(for: userDID)

        // Fail-closed: do not interrupt queries. If we can't close cleanly, don't acknowledge.
        do {
          try db.close()
          databases.removeValue(forKey: userDID)
          if wasActiveUser {
            activeUserDID = nil
          }
          updateConnectionState(.closed, for: userDID)
          logger.info("🔓 [Release] Connection released for: \(userDID.prefix(20), privacy: .private)")
          return true
        } catch {
          updateConnectionState(.open, for: userDID)
          if wasActiveUser {
            activeUserDID = userDID
          }
          logger.warning("⚠️ [Release] Could not close connection (busy): \(error.localizedDescription)")
          return false
        }
      }
    } catch is MLSExclusiveAccessError {
      logger.warning("🔒 [Release] Exclusive access busy - not releasing connection for \(userDID.prefix(20), privacy: .private)")
      return false
    } catch {
      logger.warning("🔒 [Release] Exclusive access error: \(error.localizedDescription)")
      return false
    }
  }
  
  /// Force close a database pool, even if operations are pending
  /// Use this when graceful drain times out to prevent zombie connections
  /// - Parameters:
  ///   - pool: The DatabasePool to force close
  ///   - userDID: User DID for logging
  private func forceClosePool(_ pool: DatabasePool, for userDID: String) {
    // Track for diagnostics
    let userPrefix = String(userDID.prefix(20))
    forceClosedPools.append((userDID: userPrefix, timestamp: Date()))
    
    // Keep history bounded
    if forceClosedPools.count > maxForceCloseHistory {
      forceClosedPools.removeFirst()
    }
    
    // Best-effort: do not interrupt queries; attempt close and record for diagnostics.
    do {
      try pool.close()
      logger.info("🛑 Closed database pool for: \(userDID.prefix(20), privacy: .private)")
    } catch {
      logger.warning("⚠️ Pool close deferred (busy): \(error.localizedDescription)")
    }
  }
  
  /// Get recent force close events for diagnostics
  /// - Returns: Array of (userDID prefix, timestamp) tuples
  public var recentForceCloses: [(userDID: String, timestamp: Date)] {
    forceClosedPools
  }

  // MARK: - Connection Health & Recovery

  /// Check if an error is a recoverable SQLCipher/SQLite error
  /// These errors often manifest as "out of memory" but are really codec context failures
  /// Also includes disk I/O errors and database malformation
  ///
  /// NOTE: HMAC verification failures are NOT considered recoverable here.
  /// The `getDatabasePool` method handles HMAC failures separately via hard reset.
  ///
  /// - Parameter error: The error to check
  /// - Returns: True if the error is a recoverable database error (NOT including key mismatch)
  public nonisolated func isRecoverableCodecError(_ error: Error) -> Bool {
    let description = error.localizedDescription.lowercased()

    // HMAC failures require hard reset, not WAL/SHM repair.
    if isHMACFailure(error) {
      return false
    }

    // OOM FIX: Exclude transient errors that should NOT trigger recovery
    // These are temporary conditions that resolve themselves
    let isTransientError =
      description.contains("database is locked") || description.contains("sqlite_busy")
      || description.contains("sqlite error 5")  // SQLITE_BUSY
      || description.contains("sqlite error 6")  // SQLITE_LOCKED
      || description.contains("interrupted") || description.contains("cancelled")
      || description.contains("timeout")

    if isTransientError {
      return false  // NOT recoverable via repair - just retry later
    }
    
    // ═══════════════════════════════════════════════════════════════════════════════
    // 🚨 CRITICAL FIX: SQLite Error 7 (SQLITE_NOMEM) is NOT always corruption!
    // ═══════════════════════════════════════════════════════════════════════════════
    //
    // On iOS, SQLITE_NOMEM (error 7) usually means FILE DESCRIPTOR EXHAUSTION, not
    // actual RAM exhaustion. This happens when:
    // 1. Rapid account switching doesn't close old connections fast enough
    // 2. NSE and main app both have connections open
    // 3. Too many DatabasePool reader connections accumulate
    //
    // DELETING THE WAL FILE FOR ERROR 7 IS CATASTROPHIC because:
    // - Recent messages and encryption keys may be in the WAL
    // - The main .db file may not have the latest checkpointed data
    // - You lose conversations and decrypt capability
    //
    // Error 7 should be treated as TRANSIENT - retry after closing other connections.
    // ═══════════════════════════════════════════════════════════════════════════════
    let isResourceExhaustion =
      description.contains("out of memory") || description.contains("sqlite error 7")
    
    if isResourceExhaustion {
      return false  // NOT recoverable via repair - NEVER delete files for error 7!
    }

    // Only these are TRUE corruption that might benefit from WAL/SHM repair:
    // SQLite error 10 (SQLITE_IOERR) - disk I/O error
    // SQLite error 11 (SQLITE_CORRUPT) - database disk image is malformed
    // SQLite error 26 (SQLITE_NOTADB) - file is not a database (wrong key or corruption)
    return description.contains("sqlite error 10") || description.contains("disk i/o error")
      || description.contains("sqlite error 11") || description.contains("sqlite error 26")
      || description.contains("database disk image is malformed")
      || description.contains("database is malformed")
      || description.contains("file is not a database")
      || (description.contains("sqlite") && description.contains("pragma"))
  }

  /// Check if an error is transient and should be retried without repair
  /// - Parameter error: The error to check
  /// - Returns: True if the error is likely transient and will resolve with retry
  public nonisolated func isTransientError(_ error: Error) -> Bool {
    let description = error.localizedDescription.lowercased()

    // 🚨 CRITICAL: SQLite Error 7 (SQLITE_NOMEM) is TRANSIENT on iOS!
    // It usually means file descriptor exhaustion from too many open connections.
    // The fix is to close other connections, NOT delete the database.
    let isResourceExhaustion =
      description.contains("out of memory") || description.contains("sqlite error 7")

    return isResourceExhaustion
      || description.contains("database is locked") || description.contains("sqlite_busy")
      || description.contains("sqlite error 5")  // SQLITE_BUSY
      || description.contains("sqlite error 6")  // SQLITE_LOCKED
      || description.contains("interrupted") || description.contains("cancelled")
      || description.contains("timeout") || description.contains("connection refused")
      || description.contains("network")
  }

  /// Validate that a database connection is healthy by running a simple query
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: True if the connection is healthy
  public func validateConnection(for userDID: String) async -> Bool {
    guard let database = databases[userDID] else {
      return false
    }

    do {
      _ = try await database.read { db in
        try Int.fetchOne(db, sql: "SELECT 1;")
      }
      return true
    } catch {
      logger.warning(
        "⚠️ Connection validation failed for user: \(userDID, privacy: .private) - \(error.localizedDescription)"
      )
      return false
    }
  }

  /// Force reconnection to the database, closing any existing connection
  /// Use this after detecting a codec error to recover the connection
  /// Uses progressive repair strategy: WAL/SHM repair first, then full reset if needed
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - triggeringError: The error that caused the reconnection (helps distinguish transient vs corruption)
  /// - Returns: Fresh database pool
  /// - Throws: MLSSQLCipherError if reconnection fails
  public func reconnectDatabase(for userDID: String, triggeringError: Error? = nil) async throws
    -> DatabasePool
  {
    logger.warning("🔄 Force reconnecting database for user: \(userDID, privacy: .private)")

    // Close existing connection
    closeDatabase(for: userDID)

    if let triggeringError, isHMACFailure(triggeringError) {
      return try await handleHMACFailure(
        for: userDID,
        error: triggeringError,
        mode: .primary,
        context: "reconnect (triggering error)"
      )
    }

    // Use progressive repair which applies escalating strategies
    // Pass the triggering error so it can distinguish transient vs corruption
    do {
      return try await progressiveRepair(for: userDID, lastError: triggeringError)
    } catch {
      if isHMACFailure(error) {
        return try await handleHMACFailure(
          for: userDID,
          error: error,
          mode: .primary,
          context: "reconnect"
        )
      }
      throw error
    }
  }

  /// Execute a database operation with automatic recovery on codec errors
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - maxRetries: Maximum number of retry attempts (default: 2)
  ///   - operation: The async database operation to execute
  /// - Returns: The result of the operation
  /// - Throws: The original error if all retries fail
  public func executeWithRecovery<T>(
    for userDID: String,
    maxRetries: Int = 2,
    operation: @escaping (DatabasePool) async throws -> T
  ) async throws -> T {
    var lastError: Error?

    for attempt in 0...maxRetries {
      do {
        let database = try await getDatabasePool(for: userDID)
        return try await operation(database)
      } catch {
        lastError = error

        // CRITICAL FIX: Check if this is a transient error FIRST
        // Transient errors should just wait and retry, not escalate to repair
        if isTransientError(error) && attempt < maxRetries {
          logger.warning(
            "⏳ Transient error detected (attempt \(attempt + 1)/\(maxRetries + 1)), waiting before retry..."
          )
          // Longer wait for transient errors - give time for lock to clear
          try? await Task.sleep(nanoseconds: 500_000_000)  // 500ms
          continue
        }

        if isRecoverableCodecError(error) && attempt < maxRetries {
          logger.warning(
            "⚠️ Codec error detected (attempt \(attempt + 1)/\(maxRetries + 1)), attempting recovery..."
          )

          // Force reconnection - pass the error so repair can distinguish transient vs corruption
          _ = try? await reconnectDatabase(for: userDID, triggeringError: error)

          // Small delay before retry
          try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms
        } else {
          throw error
        }
      }
    }

    throw lastError
      ?? MLSSQLCipherError.databaseCreationFailed(
        underlying: NSError(
          domain: "MLSGRDBManager", code: -1,
          userInfo: [NSLocalizedDescriptionKey: "Unknown error after retries"]))
  }

  /// Close all databases
  func closeAllDatabases() {
    let userDIDs = Array(databases.keys)
    for userDID in userDIDs {
      closeDatabase(for: userDID)
    }
    activeUserDID = nil
    logger.info("Closed all databases")
  }

  /// Close all databases except the specified user (for account switching)
  /// This is more aggressive than LRU eviction - immediately closes all non-active databases
  /// - Parameter keepUserDID: The user DID to keep open (the new active account)
  public func closeAllExcept(keepUserDID: String) async {
    logger.info("🧹 Closing all databases except: \(keepUserDID.prefix(20), privacy: .private)")

    let usersToClose = databases.keys.filter { $0 != keepUserDID }

    for userDID in usersToClose {
      // Use synchronous close with checkpoint
      await closeDatabaseAndDrain(for: userDID, timeout: 3.0)
    }

    activeUserDID = keepUserDID
    logger.info("✅ Closed \(usersToClose.count) inactive database(s)")
  }

  /// Get the count of currently open databases (for diagnostics)
  public var openDatabaseCount: Int {
    databases.count
  }

  /// Get list of user DIDs with open databases (for diagnostics)
  public var openDatabaseUsers: [String] {
    Array(databases.keys)
  }

  /// Check if a specific user's database is currently open
  public func isDatabaseOpen(for userDID: String) -> Bool {
    databases[userDID] != nil
  }



  /// Delete database file for a user (when removing account)
  /// - Parameter userDID: User's decentralized identifier
  /// - Throws: MLSSQLCipherError if deletion fails
  public func deleteDatabase(for userDID: String) async throws {
    if isRunningInExtension {
      logger.error(
        "🛑 [MLS] deleteDatabase blocked in extension for \(userDID.prefix(20), privacy: .private)")
      throw MLSSQLCipherError.storageUnavailable(
        reason: "MLS storage deletion is not allowed from extensions."
      )
    }

    // Close database first
    closeDatabase(for: userDID)

    // Delete database file
    let dbPath = databasePath(for: userDID)

    if FileManager.default.fileExists(atPath: dbPath.path) {
      do {
        try FileManager.default.removeItem(at: dbPath)
        logger.info("Deleted database for user: \(userDID, privacy: .private)")
      } catch {
        throw MLSSQLCipherError.databaseCreationFailed(underlying: error)
      }
    }

    // Delete encryption key
    try await encryption.deleteKey(for: userDID)
  }

  /// Tracks repair attempts per user to prevent infinite repair loops
  private var repairAttempts: [String: (count: Int, lastAttempt: Date)] = [:]

  /// Maximum number of repair attempts before forcing a full database reset
  private let maxRepairAttempts = 3

  /// Cooldown period between repair attempts (15 minutes)
  private let repairCooldown: TimeInterval = 900

  /// Repair corrupted database by removing WAL and SHM files
  /// Call this if you get SQLITE_NOMEM or other corruption errors
  /// - Parameter userDID: User's decentralized identifier
  public func repairDatabase(for userDID: String) async throws {
    if isRunningInExtension {
      logger.warning(
        "🛑 [MLS] repairDatabase blocked in extension for \(userDID.prefix(20), privacy: .private)")
      throw MLSSQLCipherError.storageUnavailable(
        reason: "MLS storage repair is not allowed from extensions."
      )
    }

    logger.warning("⚠️ Attempting to repair database for user: \(userDID, privacy: .private)")

    // Close the cached pool first so we get a fresh connection.
    closeDatabase(for: userDID)

    let dbPath = databasePath(for: userDID)
    let walPath = URL(fileURLWithPath: dbPath.path + "-wal")
    let shmPath = URL(fileURLWithPath: dbPath.path + "-shm")

    // Check WAL size before deciding what to do.
    let walSize = (try? FileManager.default.attributesOfItem(atPath: walPath.path)[.size] as? Int64) ?? 0

    // CHECKPOINT-FIRST STRATEGY: Never blindly delete WAL files containing data.
    // A WAL file with uncommitted pages is the ONLY copy of recent transactions.
    // Deleting it permanently corrupts the main .db file.
    if walSize > 0 {
      logger.info("🔧 [Repair] WAL file has \(walSize) bytes — attempting checkpoint before cleanup")

      // Try to checkpoint via a temporary connection to merge WAL into main DB.
      let checkpointed = await checkpointWALSafely(dbPath: dbPath, userDID: userDID)

      if checkpointed {
        // Checkpoint succeeded — WAL data is now in the main .db file.
        // Safe to delete WAL/SHM.
        logger.info("✅ [Repair] Checkpoint succeeded — WAL merged into main DB, cleaning up files")
        deleteWALAndSHM(walPath: walPath, shmPath: shmPath)
      } else {
        // Checkpoint failed — the DB or WAL may have real corruption.
        // Do NOT delete the WAL. Leave files intact for the escalation path
        // (walRepairState counter → quarantine + salvage) to handle properly.
        logger.warning(
          "⚠️ [Repair] Checkpoint failed — leaving WAL intact to preserve data. Escalation path will handle if corruption persists."
        )
      }
    } else {
      // WAL is empty or missing — safe to clean up SHM for a fresh start.
      logger.info("🔧 [Repair] WAL is empty/missing — cleaning up SHM only")
      deleteWALAndSHM(walPath: walPath, shmPath: shmPath)
    }

    logger.info("✅ Database repair completed for user: \(userDID, privacy: .private)")
  }

  /// Attempt a PASSIVE checkpoint on the WAL file via a temporary SQLite connection.
  /// Returns true if the checkpoint succeeded (WAL data merged into main DB).
  private func checkpointWALSafely(dbPath: URL, userDID: String) async -> Bool {
    do {
      let key = try await encryption.getOrCreateKey(for: userDID)
      guard let salt = try await encryption.getSalt(for: userDID) else {
        logger.error("❌ [Checkpoint] No salt found for \(userDID.prefix(20), privacy: .private)")
        return false
      }
      let hexKey = key.map { String(format: "%02x", $0) }.joined()
      let hexSalt = salt.map { String(format: "%02x", $0) }.joined()

      var config = Configuration()
      config.prepareDatabase { db in
        try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")
        try db.execute(sql: "PRAGMA key = \"x'\(hexKey)'\";")
        try db.execute(sql: "PRAGMA cipher_plaintext_header_size = 32;")
        try db.execute(sql: "PRAGMA cipher_salt = \"x'\(hexSalt)'\";")
        try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
        try db.execute(sql: "PRAGMA kdf_iter = 256000;")
        try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
        try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
      }
      // Use a temporary DatabaseQueue (not pool) for the checkpoint.
      let tempQueue = try DatabaseQueue(path: dbPath.path, configuration: config)
      defer { try? tempQueue.close() }

      var logPages: Int32 = -1
      var checkpointedPages: Int32 = -1
      var busyPages: Int32 = -1
      try await tempQueue.write { db in
        // PASSIVE checkpoint: merges what it can without blocking other writers.
        // This is the safest checkpoint mode — it won't wait for locks.
        let result = try Row.fetchOne(db, sql: "PRAGMA wal_checkpoint(PASSIVE)")
        busyPages = result?["busy"] as? Int32 ?? -1
        checkpointedPages = result?["checkpointed"] as? Int32 ?? -1
        logPages = result?["log"] as? Int32 ?? -1
      }
      logger.info(
        "🔧 [Checkpoint] PASSIVE result for \(userDID.prefix(20), privacy: .private): log=\(logPages), checkpointed=\(checkpointedPages), busy=\(busyPages)"
      )

      return true
    } catch {
      logger.error(
        "❌ [Checkpoint] Failed for \(userDID.prefix(20), privacy: .private): \(error.localizedDescription)"
      )
      return false
    }
  }

  /// Delete WAL and SHM files. Only call this when WAL is empty or after a successful checkpoint.
  private func deleteWALAndSHM(walPath: URL, shmPath: URL) {
    for path in [walPath, shmPath] {
      if FileManager.default.fileExists(atPath: path.path) {
        do {
          try FileManager.default.removeItem(at: path)
          logger.info("   ✅ Deleted: \(path.lastPathComponent)")
        } catch {
          logger.error("   ❌ Failed to delete \(path.lastPathComponent): \(error.localizedDescription)")
        }
      }
    }
  }
  
  /// Attempt soft recovery for WAL/SHM desync issues (non-HMAC).
  /// Uses a CHECKPOINT-FIRST strategy: merges WAL data into the main .db before cleanup.
  /// Never blindly deletes WAL files that may contain the only copy of recent transactions.
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: true if soft recovery was performed, false if it failed or was skipped
  private func attemptSoftRecovery(for userDID: String) async -> Bool {
    if isRunningInExtension {
      logger.warning(
        "🛑 [SoftRecovery] Skipped in extension for \(userDID.prefix(20), privacy: .private)")
      return false
    }

    logger.warning("🔧 [SoftRecovery] Attempting checkpoint-first recovery for user: \(userDID.prefix(20), privacy: .private)")

    // Close any cached connection first to release file locks.
    closeDatabase(for: userDID)

    let dbPath = databasePath(for: userDID)
    let walPath = URL(fileURLWithPath: dbPath.path + "-wal")
    let shmPath = URL(fileURLWithPath: dbPath.path + "-shm")

    // Check WAL file size to understand what we're dealing with.
    let walExists = FileManager.default.fileExists(atPath: walPath.path)
    let walSize: Int64
    if walExists {
      walSize = (try? FileManager.default.attributesOfItem(atPath: walPath.path)[.size] as? Int64) ?? 0
    } else {
      walSize = 0
    }

    logger.info("🔧 [SoftRecovery] WAL exists=\(walExists), size=\(walSize) bytes")

    if walSize > 0 {
      // WAL has data — try to checkpoint it safely before any file operations.
      let checkpointed = await checkpointWALSafely(dbPath: dbPath, userDID: userDID)

      if checkpointed {
        // WAL data merged into main DB — now safe to clean up.
        logger.info("✅ [SoftRecovery] Checkpoint succeeded — safe to clean WAL/SHM")
        deleteWALAndSHM(walPath: walPath, shmPath: shmPath)
      } else {
        // Checkpoint failed — real corruption may exist. Do NOT delete WAL.
        // The WAL might be the only copy of recent data. Leave it for the
        // escalation path (quarantine + salvage) which can recover rows from both files.
        logger.warning(
          "⚠️ [SoftRecovery] Checkpoint failed for \(userDID.prefix(20), privacy: .private) — preserving WAL (\(walSize) bytes) to avoid permanent data loss"
        )
        // Still return true so the caller proceeds with verification.
        // The verification will detect if corruption persists.
      }
    } else {
      // WAL is empty or missing — just clean up SHM for a fresh lock state.
      deleteWALAndSHM(walPath: walPath, shmPath: shmPath)
    }

    // Verify the main .db file still exists.
    if FileManager.default.fileExists(atPath: dbPath.path) {
      logger.info("🔧 [SoftRecovery] ✅ Main database file preserved: \(dbPath.lastPathComponent)")
      return true
    } else {
      logger.warning("🔧 [SoftRecovery] ⚠️ Main database file missing - will create new one")
      return true
    }
  }

  // MARK: - Phase 2: Key Validation Before Open
  
  /// Validate the encryption key before performing a full database open.
  ///
  /// This is a lightweight read-only check that catches HMAC issues early,
  /// allowing a hard reset before the full database open attempt.
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - key: The encryption key to validate
  /// - Returns: true if validation passed, false if HMAC check failed
  /// - Throws: Non-HMAC errors (e.g., file not found, permissions)
  private func validateKeyBeforeOpen(for userDID: String, key: Data) async throws -> Bool {
    let dbPath = databasePath(for: userDID)
    
    // If database file doesn't exist, validation passes (new database)
    guard FileManager.default.fileExists(atPath: dbPath.path) else {
      logger.debug("🔑 [KeyValidation] No database file exists - skipping validation")
      return true
    }
    
    logger.debug("🔑 [KeyValidation] Validating key before open for: \(userDID.prefix(20), privacy: .private)")

    // No advisory lock needed - read-only validation is safe with WAL concurrent access

    // Get salt for this user
    let salt = try await ensureSaltForDatabase(for: userDID, dbPath: dbPath)

    // Configure lightweight read-only connection
    var config = Configuration()
    config.readonly = true
    config.busyMode = .timeout(2.0)  // Short timeout for validation
    config.observesSuspensionNotifications = true

    config.prepareDatabase { db in
      // CRITICAL: Set memory security OFF first, before ANY other cipher operation
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")

      // CRITICAL: PRAGMA key MUST be the first cipher operation on the connection.
      let hexKey = key.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA key = \"x'\(hexKey)'\";")

      // iOS Shared Container Fix — MUST be after PRAGMA key!
      try db.execute(sql: "PRAGMA cipher_plaintext_header_size = 32;")

      // Explicit salt (REQUIRED when using plaintext header)
      let hexSalt = salt.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA cipher_salt = \"x'\(hexSalt)'\";")

      // SQLCipher 4 settings
      try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
      try db.execute(sql: "PRAGMA kdf_iter = 256000;")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")

      // Minimal cache for lightweight validation
      try db.execute(sql: "PRAGMA cache_size = -256;")  // 256KB only
    }
    
    do {
      // Open lightweight queue, validate, close immediately
      let queue = try DatabaseQueue(path: dbPath.path, configuration: config)
      defer {
        do {
          try queue.close()
        } catch {
          logger.debug("🔑 [KeyValidation] Queue close threw: \(error.localizedDescription)")
        }
      }
      
      // Try to read sqlite_master - this triggers HMAC validation
        _ = try await queue.read { db in
        try Int.fetchOne(db, sql: "SELECT 1 FROM sqlite_master LIMIT 1;")
      }
      
      logger.debug("🔑 [KeyValidation] ✅ Key validation passed")
      return true
      
    } catch {
      if isHMACFailure(error) {
        logger.warning("🔑 [KeyValidation] HMAC failure detected - hard reset required")
        return false
      }
      
      // Non-HMAC error - propagate it
      logger.error("🔑 [KeyValidation] Non-HMAC error: \(error.localizedDescription)")
      throw error
    }
  }
  
  /// Check if an error indicates a SQLCipher HMAC failure / NOTADB condition.
  private nonisolated func isHMACFailure(_ error: Error) -> Bool {
    if let mlsError = error as? MLSSQLCipherError {
      switch mlsError {
      case .databaseCreationFailed(let underlying):
        return isHMACFailure(underlying)
      default:
        break
      }
    }

    if let dbError = error as? DatabaseError {
      // SQLITE_NOTADB (26) is a clear indicator of wrong key/corruption
      // NOTE: SQLITE_ERROR (1) is too generic and can be transient - don't treat as HMAC failure
      if dbError.resultCode == .SQLITE_NOTADB || dbError.resultCode.rawValue == 26 {
        return true
      }
    }

    let desc = error.localizedDescription.lowercased()
    return desc.contains("hmac check failed")
      || desc.contains("hmac verification")
      || desc.contains("hmac_check")
      || desc.contains("sqlcipher_page_cipher")
      || desc.contains("cipher_page_cipher")
      || (desc.contains("hmac") && desc.contains("pgno"))
      || (desc.contains("hmac") && desc.contains("page"))
      || desc.contains("file is encrypted or is not a database")
      || desc.contains("not a database")
  }

  /// Check if an error indicates SQLite error 7 / resource exhaustion.
  private nonisolated func isSQLiteError7(_ error: Error) -> Bool {
    let desc = error.localizedDescription.lowercased()
    return desc.contains("sqlite error 7") || desc.contains("out of memory")
  }

  // MARK: - Database Format Detection

  /// Check if a database file has a plain SQLite header (first 16 bytes).
  ///
  /// When using `cipher_plaintext_header_size = 32`, the SQLite header is unencrypted.
  /// If this returns false, the file may be from an older encrypted-header format
  /// and requires migration rather than deletion.
  ///
  /// - Parameter url: Path to the database file
  /// - Returns: true if the file starts with "SQLite format 3\0"
  private nonisolated func hasPlainSQLiteHeader(_ url: URL) -> Bool {
    guard FileManager.default.fileExists(atPath: url.path) else { return false }
    guard let fh = try? FileHandle(forReadingFrom: url) else { return false }
    defer { try? fh.close() }

    guard let data = try? fh.read(upToCount: 16), data.count >= 16 else { return false }

    // SQLite magic header: "SQLite format 3\0"
    let magic = Data("SQLite format 3\0".utf8)
    return data.prefix(16) == magic
  }

  /// Determine if an HMAC failure might be due to database format mismatch rather than corruption.
  ///
  /// This helps distinguish between:
  /// 1. Old encrypted-header format (pre-plaintext-header migration) → migrate, don't delete
  /// 2. Wrong salt/key timing issue → retry, don't delete
  /// 3. True corruption → quarantine and require user action
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: A diagnostic hint about the likely cause
  private func diagnoseHMACFailure(for userDID: String) async -> HMACFailureDiagnosis {
    let dbPath = databasePath(for: userDID)

    guard FileManager.default.fileExists(atPath: dbPath.path) else {
      return .noDatabase
    }

    // Check if this is an old-format database (encrypted header)
    if !hasPlainSQLiteHeader(dbPath) {
      logger.warning("🔍 [Diagnosis] Database has encrypted header - may need format migration")
      return .encryptedHeaderFormat
    }

    // Check if salt exists in Keychain
    if await (try? encryption.getSalt(for: userDID)) == nil {
      logger.warning("🔍 [Diagnosis] Salt missing from Keychain for existing database")
      return .missingSalt
    }

    // Check if encryption key exists
    if await (try? encryption.getKey(for: userDID)) == nil {
      logger.warning("🔍 [Diagnosis] Encryption key missing from Keychain for existing database")
      return .missingKey
    }

    // Has plain header + has salt + has key = likely true corruption or WAL race
    return .likelyCorruptionOrRace
  }

  /// Diagnosis of why an HMAC failure occurred
  private enum HMACFailureDiagnosis {
    case noDatabase
    case encryptedHeaderFormat
    case missingSalt
    case missingKey
    case likelyCorruptionOrRace
  }

  // MARK: - Data Salvage from Corrupt Database

  /// Result of a salvage attempt from a corrupt database.
  public struct SalvageResult: Sendable {
    public let conversationsRecovered: Int
    public let messagesRecovered: Int
    public let membersRecovered: Int
    public let errors: [String]
  }

  /// Attempts to extract readable data from a corrupt database into a fresh one.
  ///
  /// Opens the corrupt DB read-only and copies each table row-by-row into the target pool.
  /// Rows on corrupted pages will fail individually but won't prevent other rows from being saved.
  ///
  /// - Parameters:
  ///   - corruptPath: Path to the corrupt database file
  ///   - targetPool: Fresh database pool to copy data into
  ///   - userDID: User's DID (for encryption key lookup)
  /// - Returns: SalvageResult describing what was recovered
  private func salvageDataFromCorruptDatabase(
    corruptPath: URL,
    targetPool: DatabasePool,
    userDID: String
  ) async -> SalvageResult {
    var conversationsRecovered = 0
    var messagesRecovered = 0
    var membersRecovered = 0
    var errors: [String] = []

    guard FileManager.default.fileExists(atPath: corruptPath.path) else {
      return SalvageResult(conversationsRecovered: 0, messagesRecovered: 0, membersRecovered: 0, errors: ["Database file not found"])
    }

    do {
      // Open the corrupt database read-only with the same encryption settings
      let encryptionKey: Data
      let salt: Data
      do {
        encryptionKey = try await encryption.getOrCreateKey(for: userDID)
        guard let s = try await encryption.getSalt(for: userDID) else {
          return SalvageResult(conversationsRecovered: 0, messagesRecovered: 0, membersRecovered: 0, errors: ["No salt found for user"])
        }
        salt = s
      } catch {
        return SalvageResult(conversationsRecovered: 0, messagesRecovered: 0, membersRecovered: 0, errors: ["Cannot retrieve encryption key/salt: \(error.localizedDescription)"])
      }

      let hexKey = encryptionKey.map { String(format: "%02x", $0) }.joined()
      let hexSalt = salt.map { String(format: "%02x", $0) }.joined()

      var config = Configuration()
      config.readonly = true
      config.prepareDatabase { db in
        try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")
        try db.execute(sql: "PRAGMA key = \"x'\(hexKey)'\";")
        try db.execute(sql: "PRAGMA cipher_plaintext_header_size = 32;")
        try db.execute(sql: "PRAGMA cipher_salt = \"x'\(hexSalt)'\";")
        try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
        try db.execute(sql: "PRAGMA kdf_iter = 256000;")
        try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
        try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
      }

      let corruptQueue = try DatabaseQueue(path: corruptPath.path, configuration: config)
      defer { try? corruptQueue.close() }

      // Use GRDB's ATTACH DATABASE to copy data directly via SQL.
      // This avoids Row/Sendable issues and is more efficient than row-by-row copying.
      // We read from the corrupt DB and insert into the target using raw SQL.

      // Run integrity check to understand what's damaged
      do {
        let integrityResults = try await corruptQueue.read { db in
          try String.fetchAll(db, sql: "PRAGMA integrity_check(20);")
        }
        for line in integrityResults where line != "ok" {
          errors.append("integrity: \(line)")
        }
      } catch {
        errors.append("integrity_check failed: \(error.localizedDescription)")
      }

      // Salvage each table: read from corrupt DB, then write to fresh DB.
      // Using model types with synchronous GRDB access (we're already actor-isolated).

      let tablesToSalvage: [(name: String, label: String)] = [
        ("MLSConversationModel", "conversations"),
        ("MLSMessageModel", "messages"),
        ("MLSMemberModel", "members"),
        ("MLSConversationSequenceState", "sequence_state"),
        ("MLSEpochKeyModel", "epoch_keys"),
        ("MLSReactionModel", "reactions"),
      ]

      for (tableName, label) in tablesToSalvage {
        do {
          // Phase 1: Read all rows as raw dictionaries from corrupt DB
          let rowDicts: [[String: DatabaseValue]] = try await corruptQueue.read { db in
            let rows = try Row.fetchAll(db, sql: "SELECT * FROM \(tableName)")
            return rows.map { row in
              var dict: [String: DatabaseValue] = [:]
              for column in row.columnNames {
                dict[column] = row[column]
              }
              return dict
            }
          }

          guard !rowDicts.isEmpty else { continue }

          // Phase 2: Insert into fresh target DB
          try await targetPool.write { targetDb in
            for dict in rowDicts {
              do {
                let columns = dict.keys.joined(separator: ", ")
                let placeholders = dict.keys.map { _ in "?" }.joined(separator: ", ")
                let values = Array(dict.values) as [DatabaseValueConvertible?]
                try targetDb.execute(
                  sql: "INSERT OR IGNORE INTO \(tableName) (\(columns)) VALUES (\(placeholders))",
                  arguments: StatementArguments(values)
                )
                switch label {
                case "conversations": conversationsRecovered += 1
                case "messages": messagesRecovered += 1
                case "members": membersRecovered += 1
                default: break
                }
              } catch {
                errors.append("\(label): \(error.localizedDescription)")
              }
            }
          }
        } catch {
          errors.append("\(label) table: \(error.localizedDescription)")
        }
      }

    } catch {
      errors.append("Failed to open corrupt database: \(error.localizedDescription)")
    }

    logger.info("📦 [Salvage] Recovered \(conversationsRecovered) conversations, \(messagesRecovered) messages, \(membersRecovered) members (\(errors.count) errors)")
    return SalvageResult(
      conversationsRecovered: conversationsRecovered,
      messagesRecovered: messagesRecovered,
      membersRecovered: membersRecovered,
      errors: errors
    )
  }

  /// Reset MLS storage by quarantining existing files and recreating a fresh database.
  /// Attempts to salvage readable data from the corrupt database before quarantining.
  ///
  /// Called automatically after repeated WAL repair failures indicate main .db corruption,
  /// or manually from Settings > Diagnostics > Reset MLS Storage.
  public func quarantineAndResetDatabase(for userDID: String) async throws {
    if isRunningInExtension {
      logger.error(
        "🛑 [Diagnostics] Quarantine reset blocked in extension for \(userDID.prefix(20), privacy: .private)"
      )
      throw MLSSQLCipherError.storageUnavailable(
        reason: "MLS storage reset is not allowed from extensions."
      )
    }

    try await withMLSExclusiveAccess(userDID: userDID, purpose: .maintenance, timeout: .seconds(10)) { [self] in
      logger.error("🧰 [Diagnostics] Quarantining + resetting MLS storage for: \(userDID.prefix(20), privacy: .private)")

      MLSStateChangeNotifier.postNSEStop()
      try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms grace period

      // Ensure the pool is fully closed first (fail-closed; do not interrupt queries).
      let closed = await closeDatabaseAndDrain(for: userDID, timeout: 10.0)
      guard closed else {
        throw MLSSQLCipherError.storageUnavailable(reason: "MLS storage is busy; try again")
      }

      // Clear any stale active user to avoid account mismatch on the next open.
      activeUserDID = nil

      let dbPath = databasePath(for: userDID)
      let walPath = URL(fileURLWithPath: dbPath.path + "-wal")
      let shmPath = URL(fileURLWithPath: dbPath.path + "-shm")
      let journalPath = URL(fileURLWithPath: dbPath.path + "-journal")

      // Create quarantine directory
      let formatter = ISO8601DateFormatter()
      formatter.formatOptions = [.withInternetDateTime, .withDashSeparatorInDate, .withColonSeparatorInTime]
      let timestamp = formatter.string(from: Date())

      let didTag = userDID.data(using: .utf8)?.base64EncodedString()
        .replacingOccurrences(of: "/", with: "_")
        .replacingOccurrences(of: "+", with: "-")
        .replacingOccurrences(of: "=", with: "")
        .prefix(16) ?? "unknown"

      let quarantineDir = databaseDirectory
        .appendingPathComponent("Quarantine", isDirectory: true)
        .appendingPathComponent("\(timestamp)_\(didTag)", isDirectory: true)

      try FileManager.default.createDirectory(at: quarantineDir, withIntermediateDirectories: true, attributes: nil)

      for path in [dbPath, walPath, shmPath, journalPath] {
        guard FileManager.default.fileExists(atPath: path.path) else { continue }
        let destination = quarantineDir.appendingPathComponent(path.lastPathComponent)
        do {
          try FileManager.default.moveItem(at: path, to: destination)
          logger.info("   📦 Quarantined: \(path.lastPathComponent)")
        } catch {
          logger.error("   ❌ Failed to quarantine \(path.lastPathComponent): \(error.localizedDescription)")
          throw MLSSQLCipherError.databaseCreationFailed(underlying: error)
        }
      }

      // Clear repair state and access suspension now that the old files are quarantined.
      // The suspension was set against the OLD corrupt database — the fresh database is healthy,
      // so continuing to block access would deadlock re-initialization.
      repairAttempts.removeValue(forKey: userDID)
      walRepairState.removeValue(forKey: userDID)
      usersNeedingHardReset.remove(userDID)
      consecutiveHMACFailures.removeValue(forKey: userDID)
      databaseAccessSuspensions.removeValue(forKey: userDID)
      updateConnectionState(.closed, for: userDID)

      // Recreate a fresh database.
      updateConnectionState(.opening, for: userDID)
      let database = try await createDatabase(for: userDID)
      databases[userDID] = database
      Self.registerForEmergencyClose(database, for: userDID)
      updateConnectionState(.open, for: userDID)
      activeUserDID = userDID
      startPeriodicCheckpointingIfNeeded()

      // Attempt to salvage readable data from the quarantined (corrupt) database.
      // This runs after the fresh database is created, so failures here are non-fatal.
      let quarantinedDbPath = quarantineDir.appendingPathComponent(dbPath.lastPathComponent)
      if FileManager.default.fileExists(atPath: quarantinedDbPath.path) {
        logger.info("🔧 [Salvage] Attempting to recover data from quarantined database...")
        let result = await salvageDataFromCorruptDatabase(
          corruptPath: quarantinedDbPath,
          targetPool: database,
          userDID: userDID
        )
        if result.messagesRecovered > 0 || result.conversationsRecovered > 0 {
          logger.info("✅ [Salvage] Recovered \(result.conversationsRecovered) conversations, \(result.messagesRecovered) messages from corrupt database")
        }
        if !result.errors.isEmpty {
          logger.warning("⚠️ [Salvage] \(result.errors.count) errors during recovery (some data may be unrecoverable)")
          for error in result.errors.prefix(5) {
            logger.debug("   [Salvage] \(error)")
          }
        }
      }

      logger.info("✅ [Diagnostics] MLS storage reset complete (old files quarantined)")
    }
  }

  @available(*, deprecated, message: "Use quarantineAndResetDatabase(for:) from Diagnostics")
  public func resetDatabase(for userDID: String) async throws {
    try await quarantineAndResetDatabase(for: userDID)
  }

  /// Attempt progressive database recovery - starts with WAL/SHM repair, escalates to full reset if needed
  /// Tracks attempts and applies exponential backoff between retries
  ///
  /// CRITICAL FIX: This method now distinguishes between TRANSIENT errors (locked, busy) and
  /// actual CORRUPTION. Transient errors will NOT escalate to database deletion.
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Parameter lastError: The error that triggered recovery (used to determine if transient)
  /// - Returns: Fresh database pool if recovery was successful
  /// - Throws: MLSSQLCipherError if all recovery options fail
  public func progressiveRepair(for userDID: String, lastError: Error? = nil) async throws
    -> DatabasePool
  {
    // CRITICAL FIX: Check if the triggering error was transient (locked, busy)
    // Transient errors should NOT escalate to database deletion
    if let error = lastError, isTransientError(error) {
      logger.warning("⏳ [progressiveRepair] Transient error detected - waiting for lock to clear")
      logger.warning("   Error: \(error.localizedDescription)")
      
      // 🚨 SPECIAL HANDLING FOR ERROR 7 (SQLITE_NOMEM / Resource Exhaustion)
      // Try to free up file descriptors by closing OTHER databases first
      let errorDesc = error.localizedDescription.lowercased()
      let isResourceExhaustion = errorDesc.contains("out of memory") || errorDesc.contains("sqlite error 7")
      
      if isResourceExhaustion {
        logger.warning("🚨 [progressiveRepair] SQLITE_NOMEM detected - attempting to free file descriptors")
        logger.warning("   Strategy: Close ALL other database connections, then retry")
        
        // Close all databases EXCEPT the one we're trying to open
        let usersToClose = databases.keys.filter { $0 != userDID }
        for otherUser in usersToClose {
          logger.debug("   Closing database for: \(otherUser.prefix(20), privacy: .private)")
          closeDatabase(for: otherUser)
        }
        
        if !usersToClose.isEmpty {
          logger.info("   ✅ Closed \(usersToClose.count) other database(s) to free file descriptors")
          // Give OS time to release file handles
          try await Task.sleep(nanoseconds: 200_000_000)  // 200ms
        }
      }
      
      logger.warning("   Strategy: Wait and retry (NO database deletion)")

      // Wait for lock to clear (500ms intervals, up to 5 attempts = 2.5s total)
      for attempt in 1...5 {
        try await Task.sleep(nanoseconds: 500_000_000)  // 500ms

        do {
          let db = try await createDatabase(for: userDID)
          databases[userDID] = db
          Self.registerForEmergencyClose(db, for: userDID)

          // Verify it's actually working
          _ = try await db.read { database in
            try Int.fetchOne(database, sql: "SELECT 1 FROM sqlite_master LIMIT 1;")
          }

          logger.info("✅ Database opened after waiting for lock (attempt \(attempt))")
          return db
        } catch let retryError {
          if isTransientError(retryError) && attempt < 5 {
            logger.debug("⏳ Still locked, waiting... (attempt \(attempt)/5)")
            continue
          }
          // If it's no longer a transient error, fall through to normal repair
          logger.warning("⚠️ Error changed from transient to: \(retryError.localizedDescription)")
          break
        }
      }

      // If we exhausted retries for a transient error, generally throw without deleting.
      // Exception: SQLite error 7 during initial header reads can be SQLCipher header corruption (fake OOM).
      let finalDesc = (lastError?.localizedDescription ?? "").lowercased()
      let looksLikeFakeOOM =
        (finalDesc.contains("out of memory") || finalDesc.contains("sqlite error 7"))
        && finalDesc.contains("sqlite_master")

      if looksLikeFakeOOM {
        logger.critical("🚨 [progressiveRepair] Persistent SQLITE_NOMEM during sqlite_master read")
        logger.critical("   Treating as suspected SQLCipher header corruption; attempting auto-heal")

        // Step 1: Non-destructive cleanup (WAL/SHM only)
        _ = await attemptSoftRecovery(for: userDID)

        do {
          let db = try await createDatabase(for: userDID)
          databases[userDID] = db
          Self.registerForEmergencyClose(db, for: userDID)
          _ = try await db.read { database in
            try Int.fetchOne(database, sql: "SELECT 1 FROM sqlite_master LIMIT 1;")
          }
          logger.info("✅ [progressiveRepair] Opened database after WAL/SHM cleanup")
          return db
        } catch {
          // Fail-closed: never perform destructive reset automatically.
          logger.critical("🚨 [progressiveRepair] WAL/SHM cleanup did not restore access")
          throw MLSSQLCipherError.needsUserAction(
            reason: "MLS storage could not be recovered automatically. Use Settings ▸ Diagnostics ▸ Reset MLS Storage."
          )
        }
      }

      logger.error("❌ Database remained locked after 2.5s - aborting (preserving data)")
      throw MLSSQLCipherError.databaseCreationFailed(
        underlying: NSError(
          domain: "MLSGRDBManager",
          code: -4,
          userInfo: [
            NSLocalizedDescriptionKey: "Database locked by another process. Please restart the app."
          ]
        ))
    }

    // Check current repair state
    let currentState = repairAttempts[userDID] ?? (count: 0, lastAttempt: .distantPast)
    let timeSinceLastAttempt = Date().timeIntervalSince(currentState.lastAttempt)

    // Apply cooldown if we've had recent failures (exponential backoff)
    let requiredCooldown = min(
      repairCooldown * pow(2.0, Double(max(0, currentState.count - 1))), 3600)  // Max 1 hour
    // Only apply cooldown *after* we have already exhausted the maximum repair strategies.
    // Otherwise we can get stuck for 15+ minutes after a single failed WAL/SHM repair.
    if currentState.count >= maxRepairAttempts && timeSinceLastAttempt < requiredCooldown {
      let remaining = Int(requiredCooldown - timeSinceLastAttempt)
      logger.warning(
        "⏳ Database repair on cooldown for \(remaining) more seconds (attempt \(currentState.count)/\(self.maxRepairAttempts))"
      )
      throw MLSSQLCipherError.databaseCreationFailed(
        underlying: NSError(
          domain: "MLSGRDBManager",
          code: -2,
          userInfo: [
            NSLocalizedDescriptionKey:
              "Database repair on cooldown (\(remaining)s remaining). Please try again later."
          ]
        ))
    }

    // Increment attempt counter
    repairAttempts[userDID] = (count: currentState.count + 1, lastAttempt: Date())
    let attemptNumber = currentState.count + 1

    logger.info(
      "🔄 Progressive repair attempt \(attemptNumber)/\(self.maxRepairAttempts) for user: \(userDID, privacy: .private)"
    )

    // Strategy based on attempt number:
    // Attempt 1: Try WAL/SHM repair
    // Attempt 2+: Coordinated Swift/Rust reset (ONLY for true corruption, not transient errors)

    if attemptNumber == 1 {
      // Try standard WAL/SHM repair
      logger.info("📝 Strategy: WAL/SHM file repair (attempt \(attemptNumber))")
      try? await repairDatabase(for: userDID)

      // Try to open the database
      do {
        let db = try await createDatabase(for: userDID)
        databases[userDID] = db
        Self.registerForEmergencyClose(db, for: userDID)

        // Verify it's actually working
        _ = try await db.read { database in
          try Int.fetchOne(database, sql: "SELECT 1 FROM sqlite_master LIMIT 1;")
        }

        // Success! Reset the counter
        repairAttempts.removeValue(forKey: userDID)
        walRepairState.removeValue(forKey: userDID)
        logger.info("✅ Database recovered via WAL/SHM repair")
        return db
      } catch {
        logger.error("❌ WAL/SHM repair failed: \(error.localizedDescription)")
        // Continue to escalate on next attempt
        throw error
      }
    } else {
      // CRITICAL FIX: Before doing a full reset, verify this is TRUE corruption
      // not a transient lock that persisted across attempts
      if let error = lastError, isTransientError(error) {
        logger.error("🛑 [progressiveRepair] BLOCKING FULL RESET - error is still transient!")
        logger.error("   Transient errors should NOT cause data loss")
        logger.error("   Error: \(error.localizedDescription)")
        throw MLSSQLCipherError.databaseCreationFailed(
          underlying: NSError(
            domain: "MLSGRDBManager",
            code: -5,
            userInfo: [
              NSLocalizedDescriptionKey:
                "Database access blocked. Please restart the app to clear stale connections."
            ]
          ))
      }

      guard !isRunningInExtension else {
        logger.critical(
          "🚨 [progressiveRepair] Persistent corruption detected in extension for \(userDID.prefix(20), privacy: .private)"
        )
        throw MLSSQLCipherError.needsUserAction(
          reason: "MLS storage requires main-app reset. Reopen the app to continue recovery."
        )
      }

      logger.critical(
        "🚨 [progressiveRepair] Persistent corruption after WAL/SHM repair for \(userDID.prefix(20), privacy: .private) - triggering coordinated reset"
      )

      try await MLSClient.shared.clearStorage(for: userDID)
      clearRepairState(for: userDID)
      clearHardResetFlag(for: userDID)

      if let reopenedDatabase = databases[userDID] {
        logger.info("✅ [progressiveRepair] Coordinated reset restored database access")
        return reopenedDatabase
      }

      let reopenedDatabase = try await MLSGRDBManager.shared.getDatabasePool(for: userDID)
      logger.info("✅ [progressiveRepair] Reopened database after coordinated reset")
      return reopenedDatabase
    }
  }

  /// Check if database is in a failed state requiring reset
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: True if database has exceeded repair attempts and needs manual intervention
  public nonisolated func isInFailedState(for userDID: String) async -> Bool {
    let state = await repairAttempts[userDID]
    return state?.count ?? 0 >= maxRepairAttempts
  }

  /// Clear the repair attempt counter for a user (call after successful operation)
  /// - Parameter userDID: User's decentralized identifier
  public func clearRepairState(for userDID: String) {
    if repairAttempts.removeValue(forKey: userDID) != nil {
      logger.info("🧹 Cleared repair state for user: \(userDID, privacy: .private)")
    }
    walRepairState.removeValue(forKey: userDID)
  }
  
  private enum DatabaseOpenMode {
    case primary
    case ephemeral
  }

  // MARK: - Safe HMAC Recovery Ladder

  /// Handle an HMAC/NOTADB failure using the configured recovery policy.
  ///
  /// This implements a safe recovery ladder that NEVER auto-deletes on first failure:
  /// 1. Diagnose the likely cause (format mismatch, missing salt, WAL race, etc.)
  /// 2. Retry if policy allows
  /// 3. Quarantine files if retry limit exceeded
  /// 4. Require user confirmation for actual reset
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - error: The HMAC/NOTADB error that occurred
  ///   - mode: Whether this is a primary or ephemeral database open
  ///   - context: Description of where the error occurred (for logging)
  /// - Returns: A fresh DatabasePool if recovery succeeds
  /// - Throws: MLSSQLCipherError if recovery requires user action
  private func handleHMACFailure(
    for userDID: String,
    error: Error,
    mode: DatabaseOpenMode,
    context: String
  ) async throws -> DatabasePool {
    // 1. Diagnose the failure
    let diagnosis = await diagnoseHMACFailure(for: userDID)
    logger.critical(
      "🔍 [HMAC-LADDER] Diagnosing failure for \(userDID.prefix(20), privacy: .private)")
    logger.critical("   Context: \(context)")
    logger.critical("   Diagnosis: \(String(describing: diagnosis))")
    logger.critical("   Error: \(error.localizedDescription)")

    // 2. Special handling for missing salt/key (fail-closed, don't auto-recover)
    switch diagnosis {
    case .noDatabase:
      // No database file - this shouldn't be an HMAC failure, something is wrong
      logger.error("🚨 [HMAC-LADDER] HMAC failure but no database file exists")
      throw MLSSQLCipherError.storageUnavailable(reason: "Database file not found")

    case .encryptedHeaderFormat:
      // Old format database - requires migration, not deletion
      logger.critical("🚨 [HMAC-LADDER] Database has encrypted header - migration required")
      markNeedsHardReset(for: userDID)
      throw MLSSQLCipherError.needsUserAction(
        reason:
          "Database requires format migration. Use Settings ▸ Diagnostics ▸ Reset MLS Storage."
      )

    case .missingSalt, .missingKey:
      // Missing credentials for existing database - fail closed
      logger.critical("🚨 [HMAC-LADDER] Keychain credentials missing for existing database")
      markNeedsHardReset(for: userDID)
      throw MLSSQLCipherError.needsUserAction(
        reason:
          "MLS encryption credentials are missing. Use Settings ▸ Diagnostics ▸ Reset MLS Storage."
      )

    case .likelyCorruptionOrRace:
      // Could be transient (WAL race) or true corruption - follow recovery policy
      break
    }

    // 3. Increment failure counter
    let failureCount = (consecutiveHMACFailures[userDID] ?? 0) + 1
    consecutiveHMACFailures[userDID] = failureCount
    logger.warning(
      "🔢 [HMAC-LADDER] Consecutive HMAC failures for \(userDID.prefix(20), privacy: .private): \(failureCount)"
    )

    // 4. Apply recovery policy
    switch recoveryPolicy {
    case .requireUserConfirmation:
      // Never auto-reset - require user to go to Diagnostics
      logger.critical("🛑 [HMAC-LADDER] Recovery policy requires user confirmation")
      markNeedsHardReset(for: userDID)
      throw MLSSQLCipherError.needsUserAction(
        reason: "MLS storage needs repair. Use Settings ▸ Diagnostics ▸ Reset MLS Storage."
      )

    case .quarantineOnly:
      // Quarantine files but don't reopen - require user action to continue
      logger.critical("📦 [HMAC-LADDER] Quarantining files without auto-reopen")
      _ = await performHardReset(for: userDID)
      markNeedsHardReset(for: userDID)
      throw MLSSQLCipherError.needsUserAction(
        reason:
          "MLS storage was quarantined for investigation. Use Settings ▸ Diagnostics to continue."
      )

    case .autoResetAfterRetries(let maxRetries):
      // Legacy behavior: allow auto-reset after N failures
      if failureCount >= maxRetries {
        logger.warning(
          "⚠️ [HMAC-LADDER] Max retries (\(maxRetries)) exceeded - performing hard reset")
        return try await hardResetAndReopenDatabase(
          for: userDID,
          mode: mode,
          context: context
        )
      } else {
        // Not enough failures yet - mark as needing reset and fail
        // This allows the user/app to retry first
        logger.info(
          "🔄 [HMAC-LADDER] Failure \(failureCount)/\(maxRetries) - will retry before reset")
        markNeedsHardReset(for: userDID)
        throw MLSSQLCipherError.needsUserAction(
          reason:
            "MLS storage error (attempt \(failureCount)/\(maxRetries)). Will auto-repair after \(maxRetries) failures."
        )
      }
    }
  }

  private func hardResetAndReopenDatabase(
    for userDID: String,
    mode: DatabaseOpenMode,
    context: String
  ) async throws -> DatabasePool {
    if isRunningInExtension {
      logger.error(
        "🛑 [HARD-RESET] Blocked in extension for \(userDID.prefix(20), privacy: .private)")
      throw MLSSQLCipherError.storageUnavailable(
        reason: "MLS storage reset is not allowed from extensions."
      )
    }

    logger.critical(
      "💥 [HMAC] Forcing quarantine reset for \(userDID.prefix(20), privacy: .private)...")
    logger.critical("   Context: \(context, privacy: .public)")

    let resetSucceeded = await performHardReset(for: userDID)
    guard resetSucceeded else {
      markNeedsHardReset(for: userDID)
      throw MLSSQLCipherError.storageUnavailable(
        reason: "MLS storage locked; unable to quarantine corrupted files"
      )
    }

    updateConnectionState(.opening, for: userDID)
    let database = try await createDatabase(for: userDID)

    switch mode {
    case .primary:
      databases[userDID] = database
      Self.registerForEmergencyClose(database, for: userDID)
      updateConnectionState(.open, for: userDID)
      if activeUserDID == nil {
        activeUserDID = userDID
      }
      startPeriodicCheckpointingIfNeeded()
      logger.info(
        "✅ [HARD-RESET] Recreated primary database for user: \(userDID.prefix(20), privacy: .private)..."
      )
      return database
    case .ephemeral:
      let cachedDatabase = await cacheEphemeralDatabase(database, for: userDID)
      updateConnectionState(.open, for: userDID)
      logger.info(
        "✅ [HARD-RESET] Recreated ephemeral database for user: \(userDID.prefix(20), privacy: .private)..."
      )
      return cachedDatabase
    }
  }

  private func cacheEphemeralDatabase(
    _ database: DatabasePool,
    for userDID: String
  ) async -> DatabasePool {
    // Only cache if this user is the active user or if we don't have an active user yet
    // For truly ephemeral access (notifications for other accounts), don't cache
    if activeUserDID == nil || activeUserDID == userDID {
      databases[userDID] = database
      Self.registerForEmergencyClose(database, for: userDID)
      startPeriodicCheckpointingIfNeeded()
      logger.info(
        "✅ [Ephemeral] Created and cached database for user: \(userDID.prefix(20), privacy: .private)"
      )
      return database
    }

    // For non-active users, track for cleanup but don't add to main cache
    // This allows cleanup during account switch while avoiding cache pollution
    uncachedEphemeralPools[userDID] = database
    // CRITICAL: Also register for emergency close (0xdead10cc prevention)
    Self.registerForEmergencyClose(database, for: userDID)

    // CRITICAL FIX: Checkpoint WAL before returning to prevent corruption
    // Ephemeral connections may not be properly closed, leaving WAL in bad state
    do {
      try await database.writeWithoutTransaction { db in
        try db.execute(sql: "PRAGMA wal_checkpoint(PASSIVE);")
      }
      logger.debug(
        "✅ [Ephemeral] WAL checkpointed for uncached pool: \(userDID.prefix(20), privacy: .private)"
      )
    } catch {
      logger.warning(
        "⚠️ [Ephemeral] WAL checkpoint failed (non-fatal): \(error.localizedDescription)")
    }

    logger.info(
      "✅ [Ephemeral] Created UNCACHED database for user: \(userDID.prefix(20), privacy: .private)")
    logger.info("   (Tracked for cleanup during account switch)")
    return database
  }

  // MARK: - Hard Reset (Quarantine Reset)

  /// Perform a hard reset of the database for a user.
  ///
  /// This method quarantines the database files (.db, -wal, -shm) and all associated state.
  /// Use this when HMAC/NOTADB indicates unrecoverable corruption.
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: true if reset was successful, false if files couldn't be quarantined
  @discardableResult
  public func performHardReset(for userDID: String) async -> Bool {
    if isRunningInExtension {
      logger.error(
        "🛑 [HARD-RESET] Blocked in extension for \(userDID.prefix(20), privacy: .private)")
      return false
    }

    logger.critical(
      "🔥 [HARD-RESET] Performing quarantine reset for: \(userDID.prefix(20), privacy: .private)")
    logger.critical("   ⚠️ Preserving data by moving files into quarantine")

    // 1. Ask NSE to release handles before quarantine
    MLSStateChangeNotifier.postNSEStop()
    try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms grace period

    // 2. No advisory lock needed - SQLite WAL handles concurrent access
    // NSE stop signal above gives grace period for extension to close handles
    // `MLSStateChangeNotifier` publishes state changes after reset completes

    // 3. Close any open connections
    let released = await releaseConnectionWithoutCheckpoint(for: userDID)
    if !released {
      closeDatabase(for: userDID)
    }

    // 4. Close any ephemeral pools
    if let ephemeralPool = uncachedEphemeralPools.removeValue(forKey: userDID) {
      do {
        try ephemeralPool.close()
        logger.info("   Closed ephemeral pool")
      } catch {
        logger.warning("   Failed to close ephemeral pool: \(error.localizedDescription)")
      }
    }

    // 5. Get database file paths
    let dbPath = databasePath(for: userDID)
    let walPath = URL(fileURLWithPath: dbPath.path + "-wal")
    let shmPath = URL(fileURLWithPath: dbPath.path + "-shm")
    let journalPath = URL(fileURLWithPath: dbPath.path + "-journal")

    var quarantinedFiles: [String] = []
    var failedFiles: [String] = []

    // 6. Quarantine ALL database files (main .db, -wal, -shm, -journal)
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [
      .withInternetDateTime, .withDashSeparatorInDate, .withColonSeparatorInTime,
    ]

    let timestamp = formatter.string(from: Date())
    let didTag =
      userDID.data(using: .utf8)?.base64EncodedString()
      .replacingOccurrences(of: "/", with: "_")
      .replacingOccurrences(of: "+", with: "-")
      .replacingOccurrences(of: "=", with: "")
      .prefix(16) ?? "unknown"

    let quarantineDir =
      databaseDirectory
      .appendingPathComponent("Quarantine", isDirectory: true)
      .appendingPathComponent("\(timestamp)_\(didTag)", isDirectory: true)

    do {
      try FileManager.default.createDirectory(
        at: quarantineDir, withIntermediateDirectories: true, attributes: nil)
    } catch {
      logger.error("   ❌ Failed to create quarantine directory: \(error.localizedDescription)")
      return false
    }

    for path in [dbPath, walPath, shmPath, journalPath] {
      guard FileManager.default.fileExists(atPath: path.path) else { continue }
      do {
        let attrs = try? FileManager.default.attributesOfItem(atPath: path.path)
        let fileSize = (attrs?[.size] as? Int64) ?? 0
        let destination = quarantineDir.appendingPathComponent(path.lastPathComponent)

        try FileManager.default.moveItem(at: path, to: destination)
        quarantinedFiles.append("\(path.lastPathComponent) (\(fileSize) bytes)")
        logger.info("   📦 Quarantined: \(path.lastPathComponent) (\(fileSize) bytes)")
      } catch {
        failedFiles.append(path.lastPathComponent)
        logger.error(
          "   ❌ Failed to quarantine \(path.lastPathComponent): \(error.localizedDescription)")
      }
    }

    // 7. Clear all state for this user
    repairAttempts.removeValue(forKey: userDID)
    walRepairState.removeValue(forKey: userDID)
    usersNeedingHardReset.remove(userDID)
    connectionStates.removeValue(forKey: userDID)
    coordinationGeneration.removeValue(forKey: userDID)
    keyFingerprints.removeValue(forKey: userDID)
    if activeUserDID == userDID {
      activeUserDID = nil
    }

    if !quarantinedFiles.isEmpty {
      logger.critical("🔥 [HARD-RESET] Quarantined: \(quarantinedFiles.joined(separator: ", "))")
    }

    if !failedFiles.isEmpty {
      logger.error("🔥 [HARD-RESET] Failed to quarantine: \(failedFiles.joined(separator: ", "))")
      return false
    }

    logger.critical("🔥 [HARD-RESET] ✅ Complete - data preserved in quarantine")
    return true
  }

  /// Force a WAL checkpoint to consolidate the WAL file into the main database
  /// This can help prevent memory exhaustion by reducing the size of auxiliary files
  /// - Parameter userDID: User's decentralized identifier
  public func checkpointDatabase(for userDID: String) async throws {
    guard let database = databases[userDID] else {
      logger.debug("No active database for checkpoint: \(userDID, privacy: .private)")
      return
    }

    try await withMLSExclusiveAccess(userDID: userDID, purpose: .checkpoint, timeout: .seconds(5)) {
      let checkpointStart = Date()
      // Direct checkpoint - already inside withMLSExclusiveAccess which holds advisory lock
      try await database.writeWithoutTransaction { db in
        try db.execute(sql: "PRAGMA wal_checkpoint(TRUNCATE);")
      }
      let duration = Date().timeIntervalSince(checkpointStart)
      let durationStr = String(format: "%.2f", duration)
      logger.info("✅ [Checkpoint] TRUNCATE completed in \(durationStr)s for user: \(userDID, privacy: .private)")
    }
  }

  /// Perform a safe checkpoint for app suspension/backgrounding.
  ///
  /// This method is designed to be resilient to concurrent database operations.
  /// It uses a multi-phase approach:
  /// 1. First tries PASSIVE mode (non-blocking, best-effort)
  /// 2. If PASSIVE succeeds but WAL not fully checkpointed, waits briefly and retries
  /// 3. Falls back gracefully if checkpoint can't complete (better than blocking)
  ///
  /// This prevents SQLite error 6 (SQLITE_LOCKED) that occurs when trying to
  /// checkpoint while other operations are using the database.
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: A result indicating the checkpoint outcome
  public func safeCheckpointForSuspension(for userDID: String) async -> CheckpointResult {
    guard let database = databases[userDID] else {
      logger.debug("No active database for safe checkpoint: \(userDID.prefix(20), privacy: .private)")
      return .skipped(reason: "No active database")
    }

    let startTime = Date()
    
    // APPROACH: Best-effort checkpoint with very short timeout
    // If the database is busy, we just skip the checkpoint - the WAL will be consistent
    // and iOS can safely suspend the app. The checkpoint will happen next time the app
    // enters foreground or during idle maintenance.
    //
    // This is preferable to blocking or retrying because:
    // 1. The user is backgrounding the app - they want it to happen NOW
    // 2. SQLite WAL is crash-safe even without checkpointing
    // 3. A failed checkpoint doesn't prevent clean suspension
    
    // Try with a short timeout - if we can't get access quickly, skip it
    let checkpointTask = Task {
      try await database.read { db -> (Int, Int) in
        // PRAGMA wal_checkpoint(PASSIVE) checkpoints what it can without blocking
        let row = try Row.fetchOne(db, sql: "PRAGMA wal_checkpoint(PASSIVE);")
        let logPages = row?["log"] as? Int ?? 0
        let checkpointedPages = row?["checkpointed"] as? Int ?? 0
        return (logPages, checkpointedPages)
      }
    }
    
    // Wait max 200ms for checkpoint - if busy, just move on
    let timeoutTask = Task {
      try await Task.sleep(nanoseconds: 200_000_000)  // 200ms
      return (0, 0)  // Timeout result
    }
    
    do {
      // Race between checkpoint and timeout
      let result = try await withTaskGroup(of: (Bool, Int, Int).self) { group in
        group.addTask {
          do {
            let (log, checkpointed) = try await checkpointTask.value
            return (true, log, checkpointed)  // success, log pages, checkpointed pages
          } catch {
            return (false, 0, 0)  // error
          }
        }
        
        group.addTask {
          do {
            _ = try await timeoutTask.value
            return (false, -1, -1)  // timeout (distinguished by -1)
          } catch {
            return (false, 0, 0)  // cancelled
          }
        }
        
        // Return first completed result
        if let first = await group.next() {
          group.cancelAll()
          return first
        }
        return (false, 0, 0)
      }
      
      let (success, totalPages, pagesCheckpointed) = result
      
      // Timeout case
      if totalPages == -1 {
        logger.debug("⏭️ [SafeCheckpoint] Skipped - database busy (timeout after 200ms)")
        return .skipped(reason: "Database busy - skipped for fast suspension")
      }
      
      if success {
        let duration = Date().timeIntervalSince(startTime)
        let durationStr = String(format: "%.2f", duration)
        
        if pagesCheckpointed >= totalPages || totalPages == 0 {
          logger.info("✅ [SafeCheckpoint] Completed in \(durationStr)s (\(pagesCheckpointed)/\(totalPages) pages)")
          return .success(pagesCheckpointed: pagesCheckpointed, totalPages: totalPages)
        } else {
          logger.info("✅ [SafeCheckpoint] Partial in \(durationStr)s (\(pagesCheckpointed)/\(totalPages) pages)")
          return .partial(pagesCheckpointed: pagesCheckpointed, totalPages: totalPages)
        }
      } else {
        logger.debug("⏭️ [SafeCheckpoint] Skipped - database access failed")
        return .skipped(reason: "Database access failed")
      }
    } catch {
      logger.debug("⏭️ [SafeCheckpoint] Skipped - \(error.localizedDescription)")
      return .skipped(reason: error.localizedDescription)
    }
  }

  /// Check if database exists for user
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: True if database file exists
  func databaseExists(for userDID: String) -> Bool {
    let dbPath = databasePath(for: userDID)
    return FileManager.default.fileExists(atPath: dbPath.path)
  }

  /// Get database file size
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: Size in bytes, or nil if database doesn't exist
  func databaseSize(for userDID: String) -> Int64? {
    let dbPath = databasePath(for: userDID)

    guard let attributes = try? FileManager.default.attributesOfItem(atPath: dbPath.path),
      let fileSize = attributes[.size] as? Int64
    else {
      return nil
    }

    return fileSize
  }

  // MARK: - Private Methods

  /// Execute database write operation with cross-process coordination.
  ///
  /// Uses NSFileCoordinator(.forMerging) around pool creation — the same approach Signal uses.
  /// The coordinator is only held for the duration of the block (milliseconds for DatabasePool init),
  /// NOT across ongoing transactions, so it cannot trigger 0xdead10cc (which requires holding a
  /// file lock while the process is suspended by RunningBoard).
  ///
  /// Previously this was gutted to a no-op passthrough, which removed the only cross-process
  /// handshake between the main app and NSE when both open the same SQLCipher WAL file.
  /// Without it, concurrent pool creation could corrupt the WAL/SHM mapping.
  private func coordinatedWrite<T>(to url: URL, _ block: () throws -> T) throws -> T {
    let coordinator = NSFileCoordinator(filePresenter: nil)
    var coordinatorError: NSError?
    var result: T?
    var blockError: Error?
    coordinator.coordinate(
      writingItemAt: url,
      options: .forMerging,
      error: &coordinatorError,
      byAccessor: { _ in
        do {
          result = try block()
        } catch {
          blockError = error
        }
      }
    )
    if let error = blockError ?? coordinatorError {
      throw error
    }
    // The coordinator always invokes the accessor synchronously, so result is always set
    // if neither blockError nor coordinatorError was thrown.
    return result!
  }

  /// Create new encrypted database with GRDB DatabasePool (runs off main thread via actor isolation)
  private func createDatabase(for userDID: String) async throws -> DatabasePool {
    let encryptionKey = try await ensureKeyForDatabase(for: userDID)

    // Generate key fingerprint for logging (first 8 bytes, base64 encoded - NEVER log full key!)
    let keyFingerprint = Data(encryptionKey.prefix(8)).base64EncodedString()
    let currentGeneration = coordinationGeneration[userDID] ?? 0

    logger.info(
      """
      📂 [DB-OPEN] Opening database
      User: \(userDID.prefix(20), privacy: .private)...
      Key fingerprint: \(keyFingerprint, privacy: .public)
      Generation: \(currentGeneration, privacy: .public)
      Thread: \(Thread.current.description, privacy: .public)
      Active user: \(self.activeUserDID?.prefix(20) ?? "none", privacy: .private)
      """)

    // Get database file path
    let dbPath = databasePath(for: userDID)

    // Get salt for plaintext header mode
    let salt = try await ensureSaltForDatabase(for: userDID, dbPath: dbPath)

    // Configure GRDB with SQLCipher encryption (Signal-style)
    var config = Configuration()

    // Note: defaultTransactionKind is auto-managed in GRDB 7.0+ (no longer settable).
    // GRDB handles IMMEDIATE vs DEFERRED automatically based on operation type.

    // Required for checkpoint-without-transaction (our budget-based TRUNCATE checkpoints
    // call PRAGMA wal_checkpoint outside of any transaction).
    config.allowsUnsafeTransactions = true

    // Signal uses 10 readers for main app. More readers = less contention on concurrent reads.
    // With SQLCipher overhead, each reader costs ~2MB, so 10 readers ≈ 20MB (acceptable on M-series).
    config.maximumReaderCount = 10

    // Keep: iOS suspension notification support for GRDB's internal bookkeeping
    config.observesSuspensionNotifications = true
    config.readonly = false
    config.qos = .userInitiated

    // Signal-style busy handler: 25ms sleep between retries, retry forever for writes.
    // For checkpoints, a thread-local timeout limits retries to ~50ms (2 retries).
    //
    // CRITICAL: Do NOT set `PRAGMA busy_timeout` anywhere - it calls
    // `sqlite3_busy_timeout()` which CLEARS this handler via `sqlite3_busy_handler(db, nil)`.
    // This was the root cause of 0xdead10cc: the 10s PRAGMA timeout replaced this
    // callback, allowing auto-checkpoints to hold WAL locks for up to 10 seconds.
    config.busyMode = .callback { numberOfTries in
      // Signal pattern: during checkpoints, abort after a short timeout.
      // The checkpoint caller sets a thread-local retry limit before invoking
      // PRAGMA wal_checkpoint, then clears it after.
      if let maxRetries = Thread.current.threadDictionary[MLSGRDBManager.checkpointTimeoutKey] as? Int {
        if numberOfTries >= maxRetries {
          return false  // Abort checkpoint - will retry with smaller budget
        }
      }

      if numberOfTries == 1 || numberOfTries % 40 == 0 {
        os_log(.info, log: .default, "GRDB busy retry #%d (25ms intervals)", numberOfTries)
      }
      Thread.sleep(forTimeInterval: 0.025)
      return true  // Retry forever for normal writes
    }

    // Configure encryption using GRDB's prepareDatabase
    config.prepareDatabase { db in
      // ═══════════════════════════════════════════════════════════════════════════
      // CRITICAL FIX (2024-12): Disable SQLCipher memory security FIRST
      // ═══════════════════════════════════════════════════════════════════════════
      // This MUST be set BEFORE the key is applied!
      // cipher_memory_security = ON causes SQLCipher to lock memory pages using
      // mlock() to prevent sensitive data from being swapped to disk. However,
      // on iOS this often triggers:
      // - SQLITE_NOMEM (error 7) when the mlock quota is exhausted
      // - "out of memory" errors during rapid account switching
      // - Connection failures when multiple databases are open
      //
      // iOS already encrypts swap via Data Protection, so this is redundant.
      // Disabling reduces memory pressure significantly during account switching.
      // ═══════════════════════════════════════════════════════════════════════════
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")

      // ═══════════════════════════════════════════════════════════════════════════
      // CRITICAL: PRAGMA key MUST be the first cipher operation on the connection.
      // All other cipher_* pragmas (plaintext_header_size, salt, page_size, etc.)
      // are silently ignored if set before the key. This was the root cause of
      // encrypted headers persisting even after database recreation.
      //
      // References:
      // - https://github.com/groue/GRDB.swift/issues/302
      // - https://github.com/sqlcipher/sqlcipher/issues/255
      // - Signal-iOS: GRDBDatabaseStorageAdapter.swift (key first, then header size)
      // ═══════════════════════════════════════════════════════════════════════════
      let hexKey = encryptionKey.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA key = \"x'\(hexKey)'\";")

      // iOS Shared Container 0xDEAD10CC Fix: Leave first 32 bytes unencrypted
      // so iOS recognizes the file as SQLite and exempts WAL locks.
      // MUST be set AFTER PRAGMA key or it has no effect!
      try db.execute(sql: "PRAGMA cipher_plaintext_header_size = 32;")

      // Explicit salt (REQUIRED when using plaintext header)
      // When cipher_plaintext_header_size > 0, SQLCipher cannot store the salt
      // in the encrypted header, so we must provide it explicitly.
      let hexSalt = salt.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA cipher_salt = \"x'\(hexSalt)'\";")

      // SQLCipher 4 settings
      try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
      try db.execute(sql: "PRAGMA kdf_iter = 256000;")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")

      // Enable WAL mode for better concurrency (critical for DatabasePool)
      try db.execute(sql: "PRAGMA journal_mode = WAL;")
      // Disable SQLite's automatic checkpoints - we manage checkpoints ourselves
      // via budget-based TRUNCATE checkpoints (every ~32 writes).
      // Auto-checkpoints are dangerous because they run inside the busy handler's
      // timeout context. If a checkpoint stalls, it holds WAL locks for the full
      // busy timeout duration, which can trigger 0xdead10cc during suspension.
      try db.execute(sql: "PRAGMA wal_autocheckpoint = 0;")
      try db.execute(sql: "PRAGMA synchronous = NORMAL;")  // NORMAL is sufficient with WAL

      // Signal-style checkpoint durability: ensure checkpoints are fully flushed to disk.
      // This prevents data loss if the device loses power during/after a checkpoint.
      // Critical for our budget-based TRUNCATE checkpoint strategy.
      try db.execute(sql: "PRAGMA checkpoint_fullfsync = ON;")

      // F_FULLFSYNC for all fsync operations (not just checkpoints).
      // Signal uses F_BARRIERFSYNC via a patched SQLCipher; stock SQLCipher maps
      // PRAGMA fullfsync to the stronger F_FULLFSYNC. Either way, this ensures WAL
      // frames are fully flushed to stable storage before the write returns,
      // preventing cross-process WAL corruption when the NSE and main app alternate.
      try db.execute(sql: "PRAGMA fullfsync = ON;")

      // OOM FIX: Increase SQLite's page cache for SQLCipher overhead
      // SQLCipher requires additional memory per page for encryption/decryption buffers
      // Previous: -1000 (1MB) was too small, causing codec allocation failures
      // New: -2000 (2MB) per connection provides headroom for encryption overhead
      // With 2 reader connections + 1 writer = ~6MB total cache
      try db.execute(sql: "PRAGMA cache_size = -2000;")

      // Limit temp store to memory with size constraint
      try db.execute(sql: "PRAGMA temp_store = MEMORY;")
      try db.execute(sql: "PRAGMA temp_store_directory = '';")  // Use in-memory temp

    // Enable foreign keys
    try db.execute(sql: "PRAGMA foreign_keys = ON;")

      // NOTE: Do NOT set `PRAGMA busy_timeout` here. It calls sqlite3_busy_timeout()
      // which CLEARS the sqlite3_busy_handler() set by config.busyMode = .callback above.
      // The callback handler (25ms retry forever) is the correct busy strategy.
      // See: https://www.sqlite.org/c3ref/busy_timeout.html
      // "Setting a new busy handler clears any previously set handler."

      // Disable mmap to avoid shared-kernel handles across app/NSE that survive pool closes
      try db.execute(sql: "PRAGMA mmap_size = 0;")

      // Memory-mapped I/O disabled to prevent "out of memory" errors on iOS
      // iOS has strict memory limits; large mmap regions can exhaust available memory
      // especially when multiple database connections are open during polling/sync
      // The default mmap_size of 0 (disabled) is safe and performant enough
      // try db.execute(sql: "PRAGMA mmap_size = 268435456;")  // DISABLED - was 256MB
    }

    // Create DatabasePool for concurrent reads
    do {
      return try coordinatedWrite(to: dbPath) {
        do {
          let database = try DatabasePool(path: dbPath.path, configuration: config)

          // Set file protection (iOS Data Protection)
          try setFileProtection(for: dbPath)

          // Exclude from backups
          try excludeFromBackup(dbPath)

          // Run migrations
          try runMigrations(database)

          return database
        } catch let error as DatabaseError {
          if isHMACFailure(error) {
            logger.critical(
              "🚨 [MLS] Unable to open encrypted DB (HMAC/NOTADB) for \(userDID.prefix(20), privacy: .private)")
          }
          throw error
        }
      }
    } catch {
      if isHMACFailure(error) {
        throw error
      }
      throw MLSSQLCipherError.databaseCreationFailed(underlying: error)
    }
  }

  /// Ensure a key exists.
  ///
  /// Fail-closed: if the key is missing while a database file exists, we must not try to "self-heal" by deleting.
  /// Instead, require explicit user action (Diagnostics reset) so data is preserved/quarantined.
  private func ensureKeyForDatabase(for userDID: String) async throws -> Data {
    if let existing = try? await encryption.getKey(for: userDID) {
      let fp = keyFingerprint(existing)
      if let prior = keyFingerprints[userDID], prior != fp {
        logger.critical("🚨 [MLS] Encryption key changed for \(userDID.prefix(20), privacy: .private)")
        throw MLSSQLCipherError.needsUserAction(
          reason: "MLS encryption key changed. Use Settings ▸ Diagnostics ▸ Reset MLS Storage."
        )
      }
      keyFingerprints[userDID] = fp
      logger.debug("🔑 [MLS] Key id for \(userDID.prefix(20), privacy: .private): \(fp, privacy: .private)")
      return existing
    }

    let dbPath = databasePath(for: userDID)
    if FileManager.default.fileExists(atPath: dbPath.path) {
      logger.critical(
        "🚨 [MLS] Encryption key missing but database exists for \(userDID.prefix(20), privacy: .private)"
      )
      throw MLSSQLCipherError.needsUserAction(
        reason: "MLS encryption key is missing. Use Settings ▸ Diagnostics ▸ Reset MLS Storage."
      )
    }

    let key = try await encryption.getOrCreateKey(for: userDID)
    keyFingerprints[userDID] = keyFingerprint(key)
    return key
  }

  /// Ensure a salt exists.
  ///
  /// Fail-closed: if the salt is missing while a database file exists, do not generate a new one.
  /// This prevents an unrecoverable HMAC/NOTADB on existing storage.
  private func ensureSaltForDatabase(for userDID: String, dbPath: URL? = nil) async throws -> Data {
    if let existing = try? await encryption.getSalt(for: userDID) {
      return existing
    }

    let resolvedPath = dbPath ?? databasePath(for: userDID)
    if FileManager.default.fileExists(atPath: resolvedPath.path) {
      logger.critical(
        "🚨 [MLS] Encryption salt missing but database exists for \(userDID.prefix(20), privacy: .private)"
      )
      throw MLSSQLCipherError.needsUserAction(
        reason: "MLS encryption salt is missing. Use Settings ▸ Diagnostics ▸ Reset MLS Storage."
      )
    }

    let salt = try await encryption.getOrCreateSalt(for: userDID)
    logger.debug(
      "🧂 [MLS] Generated new SQLCipher salt for \(userDID.prefix(20), privacy: .private)")
    return salt
  }

  /// Run database migrations using DatabaseMigrator
  private func runMigrations(_ db: DatabasePool) throws {
    var migrator = DatabaseMigrator()

    // MARK: v1 - Initial schema
    migrator.registerMigration("v1_initial_schema") { db in
      // Create all MLS tables using GRDB's native table creation
      try db.create(table: "MLSConversationModel") { t in
        t.primaryKey("conversationID", .text).notNull()
        t.column("currentUserDID", .text).notNull()
        t.column("groupID", .blob).notNull()
        t.column("epoch", .integer).notNull().defaults(to: 0)
        t.column("joinMethod", .text).notNull().defaults(to: "unknown")
        t.column("joinEpoch", .integer).notNull().defaults(to: 0)
        t.column("title", .text)
        t.column("avatarURL", .text)
        t.column("createdAt", .datetime).notNull()
        t.column("updatedAt", .datetime).notNull()
        t.column("lastMessageAt", .datetime)
        t.column("isActive", .boolean).notNull().defaults(to: true)
        t.column("needsRejoin", .boolean).notNull().defaults(to: false)
        t.column("rejoinRequestedAt", .datetime)
      }

      try db.create(table: "MLSMessageModel") { t in
        t.primaryKey("messageID", .text).notNull()
        t.column("currentUserDID", .text).notNull()
        t.column("conversationID", .text).notNull().references(
          "MLSConversationModel", onDelete: .cascade)
        t.column("senderID", .text).notNull()
        t.column("payloadJSON", .blob)  // Full MLSMessagePayload as JSON
        t.column("wireFormat", .blob)
        t.column("contentType", .text).notNull()
        t.column("timestamp", .datetime).notNull()
        t.column("epoch", .integer).notNull()
        t.column("sequenceNumber", .integer).notNull()
        t.column("authenticatedData", .blob)
        t.column("signature", .blob)
        t.column("isDelivered", .boolean).notNull().defaults(to: false)
        t.column("isRead", .boolean).notNull().defaults(to: false)
        t.column("isSent", .boolean).notNull().defaults(to: false)
        t.column("sendAttempts", .integer).notNull().defaults(to: 0)
        t.column("error", .text)
        t.column("processingState", .text).notNull()
        t.column("gapBefore", .boolean).notNull().defaults(to: false)
        t.column("payloadExpired", .boolean).notNull().defaults(to: false)
        t.column("processingError", .text)
        t.column("processingAttempts", .integer).notNull().defaults(to: 0)
        t.column("validationFailureReason", .text)
      }

      try db.create(table: "MLSMemberModel") { t in
        t.primaryKey("memberID", .text).notNull()
        t.column("conversationID", .text).notNull().references(
          "MLSConversationModel", onDelete: .cascade)
        t.column("currentUserDID", .text).notNull()
        t.column("did", .text).notNull()
        t.column("handle", .text)
        t.column("displayName", .text)
        t.column("leafIndex", .integer).notNull()
        t.column("credentialData", .blob)
        t.column("signaturePublicKey", .blob)
        t.column("addedAt", .datetime).notNull()
        t.column("updatedAt", .datetime).notNull()
        t.column("removedAt", .datetime)
        t.column("isActive", .boolean).notNull().defaults(to: true)
        t.column("role", .text).notNull()
        t.column("capabilities", .blob)
      }

      try db.create(table: "MLSKeyPackageModel") { t in
        t.primaryKey("keyPackageID", .text).notNull()
        t.column("currentUserDID", .text).notNull()
        t.column("keyPackageData", .blob).notNull()
        t.column("credentialData", .blob).notNull()
        t.column("createdAt", .datetime).notNull()
        t.column("expiresAt", .datetime)
        t.column("isPublished", .boolean).notNull().defaults(to: false)
        t.column("isUsed", .boolean).notNull().defaults(to: false)
      }

      try db.create(table: "MLSEpochKeyModel") { t in
        t.primaryKey("epochKeyID", .text).notNull()
        t.column("conversationID", .text).notNull().references(
          "MLSConversationModel", onDelete: .cascade)
        t.column("currentUserDID", .text).notNull()
        t.column("epoch", .integer).notNull()
        t.column("keyMaterial", .blob).notNull()
        t.column("createdAt", .datetime).notNull()
        t.column("expiresAt", .datetime)
        t.column("isActive", .boolean).notNull().defaults(to: true)
      }

      try db.create(table: "MLSMessageReactionModel") { t in
        t.primaryKey("reactionID", .text).notNull()
        t.column("messageID", .text).notNull().references("MLSMessageModel", onDelete: .cascade)
        t.column("conversationID", .text).notNull()
        t.column("currentUserDID", .text).notNull()
        t.column("actorDID", .text).notNull()
        t.column("emoji", .text).notNull()
        t.column("action", .text).notNull()
        t.column("timestamp", .datetime).notNull()
      }

      try db.create(table: "MLSStorageBlobModel") { t in
        t.primaryKey("blobID", .text).notNull()
        t.column("conversationID", .text).references("MLSConversationModel", onDelete: .cascade)
        t.column("currentUserDID", .text).notNull()
        t.column("blobType", .text).notNull()
        t.column("blobData", .blob).notNull()
        t.column("mimeType", .text).notNull()
        t.column("size", .integer).notNull()
        t.column("createdAt", .datetime).notNull()
        t.column("updatedAt", .datetime).notNull()
      }

      try db.create(table: "MLSAdminRosterModel") { t in
        t.primaryKey("convo_id", .text).notNull().references(
          "MLSConversationModel", onDelete: .cascade)
        t.column("version", .integer).notNull()
        t.column("roster_hash", .text).notNull()
        t.column("encrypted_roster", .blob).notNull()
        t.column("updated_at", .datetime).notNull()
      }
    }

    // MARK: v2 - Performance indexes
    migrator.registerMigration("v2_performance_indexes") { db in
      // MLSMessageModel indexes - frequently queried by conversation and timestamp
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_message_conversation_timestamp
            ON MLSMessageModel(conversationID, timestamp DESC);
          """)

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_message_user
            ON MLSMessageModel(currentUserDID);
          """)

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_message_sender
            ON MLSMessageModel(senderID);
          """)

      // MLSConversationModel indexes - frequently queried by user and activity
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_conversation_user_active
            ON MLSConversationModel(currentUserDID, isActive, lastMessageAt DESC);
          """)

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_conversation_updated
            ON MLSConversationModel(updatedAt DESC);
          """)

      // MLSMemberModel indexes - frequently queried by conversation
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_member_conversation
            ON MLSMemberModel(conversationID);
          """)

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_member_did
            ON MLSMemberModel(did);
          """)

      // MLSEpochKeyModel indexes - queried by conversation and epoch
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_epoch_conversation_epoch
            ON MLSEpochKeyModel(conversationID, epoch DESC);
          """)

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_epoch_created
            ON MLSEpochKeyModel(createdAt DESC);
          """)

      // MLSMessageReactionModel indexes - queried by message
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_reaction_message
            ON MLSMessageReactionModel(messageID);
          """)

      // MLSStorageBlobModel indexes - queried by user and type
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_blob_user_type
            ON MLSStorageBlobModel(currentUserDID, blobType);
          """)

      // MLSAdminRosterModel - primary key index already covers convo_id lookups
    }

    // MARK: v3 - Consumption tracking
    migrator.registerMigration("v3_consumption_tracking") { db in
      // Create consumption record table
      try db.create(table: "MLSConsumptionRecordModel") { t in
        t.primaryKey("recordID", .text).notNull()
        t.column("currentUserDID", .text).notNull()
        t.column("timestamp", .datetime).notNull()
        t.column("packagesConsumed", .integer).notNull()
        t.column("operation", .text).notNull()
        t.column("context", .text)
      }

      // Indexes for consumption queries
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_consumption_user_timestamp
            ON MLSConsumptionRecordModel(currentUserDID, timestamp DESC);
          """)

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_consumption_operation
            ON MLSConsumptionRecordModel(currentUserDID, operation);
          """)
    }

    // MARK: v4 - Fix message ordering index
    migrator.registerMigration("v4_fix_message_ordering_index") { db in
      // Drop old timestamp-based index that doesn't match query ordering
      try db.execute(
        sql: """
            DROP INDEX IF EXISTS idx_message_conversation_timestamp;
          """)

      // Create new index matching actual query ordering (epoch, sequenceNumber)
      // This prevents messages from appearing out of order during epoch transitions
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_message_conversation_epoch_seq
            ON MLSMessageModel(conversationID, epoch ASC, sequenceNumber ASC);
          """)

      // Keep separate timestamp index for catchup and time-based queries
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_message_timestamp
            ON MLSMessageModel(timestamp DESC);
          """)
    }

    // MARK: v5 - Error tracking and recovery fields
    migrator.registerMigration("v5_error_tracking_recovery") { db in
      // Add error tracking fields to MLSMessageModel (if not already present from initial schema)
      let messageColumns = try db.columns(in: "MLSMessageModel").map { $0.name }

      if !messageColumns.contains("processingError") {
        try db.execute(
          sql: """
              ALTER TABLE MLSMessageModel
              ADD COLUMN processingError TEXT;
            """)
      }

      if !messageColumns.contains("processingAttempts") {
        try db.execute(
          sql: """
              ALTER TABLE MLSMessageModel
              ADD COLUMN processingAttempts INTEGER NOT NULL DEFAULT 0;
            """)
      }

      if !messageColumns.contains("validationFailureReason") {
        try db.execute(
          sql: """
              ALTER TABLE MLSMessageModel
              ADD COLUMN validationFailureReason TEXT;
            """)
      }

      // Add recovery tracking fields to MLSConversationModel (if not already present)
      let convoColumns = try db.columns(in: "MLSConversationModel").map { $0.name }

      if !convoColumns.contains("lastRecoveryAttempt") {
        try db.execute(
          sql: """
              ALTER TABLE MLSConversationModel
              ADD COLUMN lastRecoveryAttempt DATETIME;
            """)
      }

      if !convoColumns.contains("consecutiveFailures") {
        try db.execute(
          sql: """
              ALTER TABLE MLSConversationModel
              ADD COLUMN consecutiveFailures INTEGER NOT NULL DEFAULT 0;
            """)
      }

      // Create index for finding conversations needing recovery
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_conversation_recovery
            ON MLSConversationModel(consecutiveFailures DESC, lastRecoveryAttempt);
          """)

      // Create index for finding messages with processing errors
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_message_processing_error
            ON MLSMessageModel(conversationID, processingError);
          """)
    }

    // MARK: v6 - Membership history and visibility tracking
    migrator.registerMigration("v6_membership_history") { db in
      // Add membership change tracking to MLSConversationModel (if not already present)
      let convoColumns = try db.columns(in: "MLSConversationModel").map { $0.name }

      if !convoColumns.contains("lastMembershipChangeAt") {
        try db.execute(
          sql: """
              ALTER TABLE MLSConversationModel
              ADD COLUMN lastMembershipChangeAt DATETIME;
            """)
      }

      if !convoColumns.contains("unacknowledgedMemberChanges") {
        try db.execute(
          sql: """
              ALTER TABLE MLSConversationModel
              ADD COLUMN unacknowledgedMemberChanges INTEGER NOT NULL DEFAULT 0;
            """)
      }

      // Check if MLSMemberModel needs removedBy and removalReason columns
      // (these were added in Phase 1.2, but migration handles databases without them)
      let hasRemovedBy = try db.columns(in: "MLSMemberModel")
        .contains { $0.name == "removedBy" }
      let hasRemovalReason = try db.columns(in: "MLSMemberModel")
        .contains { $0.name == "removalReason" }

      if !hasRemovedBy {
        try db.execute(
          sql: """
              ALTER TABLE MLSMemberModel
              ADD COLUMN removedBy TEXT;
            """)
      }

      if !hasRemovalReason {
        try db.execute(
          sql: """
              ALTER TABLE MLSMemberModel
              ADD COLUMN removalReason TEXT;
            """)
      }

      // Create membership event audit log table
      try db.create(table: "MLSMembershipEventModel") { t in
        t.primaryKey("id", .text).notNull()
        t.column("conversationID", .text).notNull()
          .references("MLSConversationModel", onDelete: .cascade)
        t.column("currentUserDID", .text).notNull()
        t.column("memberDID", .text).notNull()
        t.column("eventType", .text).notNull()
        t.column("timestamp", .datetime).notNull()
        t.column("actorDID", .text)
        t.column("epoch", .integer).notNull()
        t.column("metadata", .blob)
      }

      // Create indexes for membership event queries
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_membership_events_conversation_timestamp
            ON MLSMembershipEventModel(conversationID, timestamp DESC);
          """)

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_membership_events_member
            ON MLSMembershipEventModel(memberDID, timestamp DESC);
          """)
    }

    // MARK: v7 - Roster snapshots and tree hash pinning for E2EE validation
    migrator.registerMigration("v7_validation_hardening") { db in
      // Create roster snapshot table for membership change detection
      try db.create(table: "MLSRosterSnapshotModel") { t in
        t.primaryKey("snapshotID", .text).notNull()
        t.column("conversationID", .text).notNull()
          .references("MLSConversationModel", onDelete: .cascade)
        t.column("epoch", .integer).notNull()
        t.column("memberDIDs", .blob).notNull()  // JSON-encoded [String]
        t.column("treeHash", .blob)
        t.column("timestamp", .datetime).notNull()
        t.column("previousSnapshotID", .text)
      }

      // Create indexes for roster snapshot queries
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_roster_snapshots_conversation_epoch
            ON MLSRosterSnapshotModel(conversationID, epoch DESC);
          """)

      try db.execute(
        sql: """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_roster_snapshots_conversation_epoch_unique
            ON MLSRosterSnapshotModel(conversationID, epoch);
          """)

      // Create tree hash pin table for state divergence detection
      try db.create(table: "MLSTreeHashPinModel") { t in
        t.primaryKey("pinID", .text).notNull()
        t.column("conversationID", .text).notNull()
          .references("MLSConversationModel", onDelete: .cascade)
        t.column("epoch", .integer).notNull()
        t.column("treeHash", .blob).notNull()
        t.column("pinnedAt", .datetime).notNull()
        t.column("source", .text).notNull()
        t.column("verified", .boolean).notNull().defaults(to: false)
      }

      // Create indexes for tree hash pin queries
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_tree_hash_pins_conversation_epoch
            ON MLSTreeHashPinModel(conversationID, epoch DESC);
          """)

      try db.execute(
        sql: """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tree_hash_pins_conversation_epoch_unique
            ON MLSTreeHashPinModel(conversationID, epoch);
          """)

      // Create validation audit log table for security decisions
      try db.create(table: "MLSValidationAuditLog") { t in
        t.primaryKey("id", .text).notNull()
        t.column("conversationID", .text).notNull()
        t.column("timestamp", .datetime).notNull()
        t.column("operationType", .text).notNull()
        t.column("credentialDID", .text)
        t.column("epoch", .integer).notNull()
        t.column("decision", .text).notNull()  // "allowed", "denied", "requires_approval"
        t.column("reason", .text)
        t.column("metadata", .blob)
      }

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_validation_audit_conversation_timestamp
            ON MLSValidationAuditLog(conversationID, timestamp DESC);
          """)
    }

    // MARK: v8 - Add deletedAt to MLSEpochKeyModel
    migrator.registerMigration("v8_add_deleted_at_to_epoch_keys") { db in
      // Add deletedAt column to MLSEpochKeyModel if it doesn't exist
      let hasDeletedAt = try db.columns(in: "MLSEpochKeyModel")
        .contains { $0.name == "deletedAt" }

      if !hasDeletedAt {
        try db.execute(
          sql: """
              ALTER TABLE MLSEpochKeyModel
              ADD COLUMN deletedAt DATETIME;
            """)
      }

      // Add index for cleanup queries
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_epoch_deleted
            ON MLSEpochKeyModel(currentUserDID, deletedAt);
          """)
    }

    // MARK: v9 - Track join method/epoch (External Commit partial history)
    migrator.registerMigration("v9_join_tracking") { db in
      let hasJoinMethod = try db.columns(in: "MLSConversationModel")
        .contains { $0.name == "joinMethod" }
      let hasJoinEpoch = try db.columns(in: "MLSConversationModel")
        .contains { $0.name == "joinEpoch" }

      if !hasJoinMethod {
        try db.execute(
          sql: """
              ALTER TABLE MLSConversationModel
              ADD COLUMN joinMethod TEXT NOT NULL DEFAULT 'unknown';
            """)
      }

      if !hasJoinEpoch {
        try db.execute(
          sql: """
              ALTER TABLE MLSConversationModel
              ADD COLUMN joinEpoch INTEGER NOT NULL DEFAULT 0;
            """)
      }
    }

    // MARK: v10 - Placeholder conversation support for NSE FK fix
    // When NSE decrypts a message for a new conversation, it may not have the
    // conversation metadata yet. This flag allows creating a minimal placeholder
    // that the main app will heal with full metadata on next sync.
    migrator.registerMigration("v10_placeholder_conversations") { db in
      let hasIsPlaceholder = try db.columns(in: "MLSConversationModel")
        .contains { $0.name == "isPlaceholder" }

      if !hasIsPlaceholder {
        try db.execute(
          sql: """
              ALTER TABLE MLSConversationModel
              ADD COLUMN isPlaceholder INTEGER NOT NULL DEFAULT 0;
            """)
      }
    }

    // MARK: v11 - Migrate plaintext + embedData to payloadJSON
    // Consolidates separate plaintext and embedData columns into a single payloadJSON column
    // that stores the full MLSMessagePayload as JSON.
    migrator.registerMigration("v11_payload_consolidation") { db in
      let messageColumns = try db.columns(in: "MLSMessageModel").map { $0.name }

      // Check if we need to migrate (has old columns but not new)
      let hasPlaintext = messageColumns.contains("plaintext")
      let hasPayloadJSON = messageColumns.contains("payloadJSON")
      let hasPlaintextExpired = messageColumns.contains("plaintextExpired")
      let hasPayloadExpired = messageColumns.contains("payloadExpired")

      if hasPlaintext && !hasPayloadJSON {
        // Add new payloadJSON column
        try db.execute(
          sql: """
              ALTER TABLE MLSMessageModel
              ADD COLUMN payloadJSON BLOB;
            """)

        // Migrate existing data: create payloadJSON from plaintext + embedData
        // For each message with plaintext, create a JSON payload with type "text"
        try db.execute(
          sql: """
              UPDATE MLSMessageModel
              SET payloadJSON = 
                CASE 
                  WHEN embedData IS NOT NULL THEN 
                    json_object(
                      'type', 'text',
                      'text', plaintext,
                      'embed', json(embedData)
                    )
                  ELSE 
                    json_object('type', 'text', 'text', plaintext)
                END
              WHERE plaintext IS NOT NULL;
            """)
      }

      if hasPlaintextExpired && !hasPayloadExpired {
        // Add new payloadExpired column
        try db.execute(
          sql: """
              ALTER TABLE MLSMessageModel
              ADD COLUMN payloadExpired INTEGER NOT NULL DEFAULT 0;
            """)

        // Copy values from plaintextExpired
        try db.execute(
          sql: """
              UPDATE MLSMessageModel
              SET payloadExpired = plaintextExpired;
            """)
      }
    }

    // MARK: v12 - Orphaned Reaction persistence
    // Creates a table for reactions that arrive before their parent messages.
    // These do NOT have foreign key constraints on messageID.
    migrator.registerMigration("v12_orphan_reactions") { db in
      try db.create(table: "MLSOrphanedReactionModel") { t in
        t.primaryKey("reactionID", .text).notNull()
        // Note: No ForeignKey on messageID as that's the whole point
        t.column("messageID", .text).notNull()
        t.column("conversationID", .text).notNull()
        t.column("currentUserDID", .text).notNull()
        t.column("actorDID", .text).notNull()
        t.column("emoji", .text).notNull()
        t.column("action", .text).notNull()
        t.column("timestamp", .datetime).notNull()
      }

      // Index for efficient adoption when messages arrive
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_orphaned_reaction_message
            ON MLSOrphanedReactionModel(messageID, currentUserDID);
          """)
    }

    // v13: Message ordering tables for cross-process coordination
    migrator.registerMigration("v13_message_ordering") { db in
      // Sequence state tracking per conversation
      try db.create(table: "mls_conversation_sequence_state", ifNotExists: true) { t in
        t.column("conversationID", .text).notNull()
        t.column("currentUserDID", .text).notNull()
        t.column("lastProcessedSeq", .integer).notNull().defaults(to: -1)
        t.column("updatedAt", .datetime).notNull()
        t.primaryKey(["conversationID", "currentUserDID"])
      }

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_seq_state_user
            ON mls_conversation_sequence_state(currentUserDID);
          """)

      // Pending message buffer for out-of-order messages
      try db.create(table: "mls_pending_messages", ifNotExists: true) { t in
        t.column("messageID", .text).notNull()
        t.column("currentUserDID", .text).notNull()
        t.column("conversationID", .text).notNull()
        t.column("sequenceNumber", .integer).notNull()
        t.column("epoch", .integer).notNull()
        t.column("messageViewJSON", .blob).notNull()
        t.column("receivedAt", .datetime).notNull()
        t.column("processAttempts", .integer).notNull().defaults(to: 0)
        t.column("source", .text).notNull()
        t.primaryKey(["messageID", "currentUserDID"])
      }

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_pending_msg_convo_seq
            ON mls_pending_messages(currentUserDID, conversationID, sequenceNumber);
          """)

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_pending_msg_received
            ON mls_pending_messages(receivedAt);
          """)
    }

    // v14: Decryption receipt ledger for cross-process deduplication
    migrator.registerMigration("v14_decryption_receipts") { db in
      // Create the decryption receipt table using the model's schema
      try MLSDecryptionReceiptModel.createTable(in: db)
    }

    // v15: Chat request state tracking (local-only, server stores no request metadata)
    // Tracks whether an inbound conversation is pending acceptance or has been accepted
    migrator.registerMigration("v15_chat_request_state") { db in
      // Add requestState column to conversations
      // Values: "none" (default - own convos or accepted), "pendingInbound" (needs acceptance)
      try db.alter(table: "MLSConversationModel") { t in
        t.add(column: "requestState", .text).notNull().defaults(to: "none")
      }

      // Index for efficient filtering of pending requests
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_conversation_request_state
            ON MLSConversationModel(currentUserDID, requestState)
            WHERE requestState != 'none';
          """)
    }

    // v16: Declaration verification cache (scoped by local account + target DID)
    migrator.registerMigration("v16_declaration_cache") { db in
      try db.create(table: "MLSDeclarationCache", ifNotExists: true) { t in
        t.column("localAccountDID", .text).notNull()
        t.column("targetDID", .text).notNull()
        t.column("chainState", .text).notNull()
        t.column("verifiedAt", .double).notNull()
        t.column("headEpoch", .integer)
        t.column("headSeq", .integer)
        t.column("headCID", .text)
        t.column("headURI", .text)
        t.column("onlineRootPublicKey", .blob)
        t.column("onlineRootAlg", .text)
        t.column("recoveryRootPublicKey", .blob)
        t.column("recoveryRootAlg", .text)
        t.column("authorizedDeviceKeysJSON", .text).notNull().defaults(to: "[]")
        t.column("hadRecovery", .boolean).notNull().defaults(to: false)
        t.column("lastError", .text)
        t.primaryKey(["localAccountDID", "targetDID"])
      }

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_declaration_cache_local_verified
            ON MLSDeclarationCache(localAccountDID, verifiedAt DESC);
          """)
    }

    // v17: Declaration chat policy
    migrator.registerMigration("v17_declaration_policy") { db in
      try db.alter(table: "MLSDeclarationCache") { t in
        t.add(column: "chatPolicyJSON", .text)
      }
    }

    // v18: Per-conversation read frontier for monotonic local read state
    migrator.registerMigration("v18_read_frontier") { db in
      try MLSReadFrontierModel.createTable(in: db)
    }

    // v19: Control/protocol payloads should not remain unread.
    migrator.registerMigration("v19_control_messages_read") { db in
      try db.execute(
        sql: """
          UPDATE MLSMessageModel
          SET isRead = 1
          WHERE isRead = 0
            AND json_valid(CAST(payloadJSON AS TEXT)) = 1
            AND COALESCE(
              json_extract(CAST(payloadJSON AS TEXT), '$.messageType'),
              json_extract(CAST(payloadJSON AS TEXT), '$.type')
            ) IS NOT NULL
            AND COALESCE(
              json_extract(CAST(payloadJSON AS TEXT), '$.messageType'),
              json_extract(CAST(payloadJSON AS TEXT), '$.type')
            ) != 'text';
          """
      )
    }

    // v20: Drop legacy declaration cache (replaced by device record service)
    migrator.registerMigration("v20_drop_declaration_cache") { db in
      try db.drop(table: "MLSDeclarationCache")
    }

    // v21: Store decrypted group avatar image data locally
    migrator.registerMigration("v21_group_avatar_image_data") { db in
      let hasAvatarImageData = try db.columns(in: "MLSConversationModel")
        .contains { $0.name == "avatarImageData" }
      if !hasAvatarImageData {
        try db.alter(table: "MLSConversationModel") { t in
          t.add(column: "avatarImageData", .blob)
        }
      }
    }

    // v22: Per-conversation timed mute
    migrator.registerMigration("v22_conversation_muted_until") { db in
      let hasMutedUntil = try db.columns(in: "MLSConversationModel")
        .contains { $0.name == "mutedUntil" }
      if !hasMutedUntil {
        try db.alter(table: "MLSConversationModel") { t in
          t.add(column: "mutedUntil", .datetime)
        }
      }
    }

    migrator.registerMigration("v23_member_avatar_url") { db in
      let hasAvatarURL = try db.columns(in: "MLSMemberModel")
        .contains { $0.name == "avatarURL" }
      if !hasAvatarURL {
        try db.alter(table: "MLSMemberModel") { t in
          t.add(column: "avatarURL", .text)
        }
      }
    }

    migrator.registerMigration("v24_remote_read_cursors") { db in
      try MLSRemoteReadCursorModel.createTable(in: db)
    }

    // MARK: v25 - Delivery acks
    migrator.registerMigration("v25_delivery_acks") { db in
      try db.create(table: "MLSDeliveryAck", ifNotExists: true) { t in
        t.column("messageId", .text).notNull()
        t.column("conversationId", .text).notNull()
        t.column("senderDID", .text).notNull()
        t.column("ackedAt", .datetime).notNull()
        t.column("currentUserDID", .text).notNull()
        t.primaryKey(["messageId", "senderDID", "currentUserDID"])
      }
      try db.execute(sql: """
        CREATE INDEX IF NOT EXISTS idx_delivery_ack_conversation
        ON MLSDeliveryAck(conversationId, currentUserDID)
      """)
    }

    // Execute all migrations
    try migrator.migrate(db)
  }

  /// Set iOS Data Protection on database file
  private func setFileProtection(for fileURL: URL) throws {
    #if targetEnvironment(macCatalyst)
      // Skip file protection on Mac Catalyst - macOS has its own security model (FileVault, etc.)
      logger.debug("Skipping file protection on Mac Catalyst")
    #else
      do {
        // Use a background-safe protection class to avoid iOS locking the DB
        // while we are checkpointing during background transitions.
        let protection = FileProtectionType.completeUntilFirstUserAuthentication

        // Apply protection to the main DB file and any sidecar files (WAL/SHM/JOURNAL).
        let relatedPaths = [
          fileURL,
          URL(fileURLWithPath: fileURL.path + "-wal"),
          URL(fileURLWithPath: fileURL.path + "-shm"),
          URL(fileURLWithPath: fileURL.path + "-journal"),
        ]

        for url in relatedPaths where FileManager.default.fileExists(atPath: url.path) {
          try FileManager.default.setAttributes(
            [.protectionKey: protection],
            ofItemAtPath: url.path
          )
        }
      } catch {
        throw MLSSQLCipherError.fileProtectionFailed(underlying: error)
      }
    #endif
  }

  /// Exclude database from iCloud/iTunes backup
  private func excludeFromBackup(_ fileURL: URL) throws {
    var url = fileURL
    do {
      var resourceValues = URLResourceValues()
      resourceValues.isExcludedFromBackup = true
      try url.setResourceValues(resourceValues)
    } catch {
      throw MLSSQLCipherError.backupExclusionFailed(underlying: error)
    }
  }

  // MARK: - WAL Corruption Diagnostics

  /// Probe WAL health for a user's database. Logs file sizes, lock state, and integrity.
  ///
  /// Call this before and after NSE operations to detect corruption windows.
  /// Safe to call from any context — uses its own lightweight connection.
  public nonisolated static func probeWALHealth(
    for userDID: String,
    label: String,
    file: String = #fileID,
    line: Int = #line
  ) {
    let pid = ProcessInfo.processInfo.processIdentifier
    let processTag = Bundle.main.bundlePath.hasSuffix(".appex") ? "NSE" : "APP"
    let didPrefix = String(userDID.prefix(20))

    guard let appGroup = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared"
    ) else {
      staticLogger.warning("📊 [WAL-DIAG] [\(processTag)/\(pid)] Cannot access app group container")
      return
    }

    // Probe both GRDB and Rust databases
    let grdbDir = appGroup.appendingPathComponent("Application Support/MLS", isDirectory: true)
    let rustDir = appGroup.appendingPathComponent("Application Support/mls-state", isDirectory: true)

    probeWALFiles(in: grdbDir, dbType: "GRDB", userDID: didPrefix, label: label, processTag: processTag, pid: pid)
    probeWALFiles(in: rustDir, dbType: "Rust", userDID: didPrefix, label: label, processTag: processTag, pid: pid)
  }

  private nonisolated static func probeWALFiles(
    in directory: URL,
    dbType: String,
    userDID: String,
    label: String,
    processTag: String,
    pid: Int32
  ) {
    let fm = FileManager.default
    guard let files = try? fm.contentsOfDirectory(atPath: directory.path) else { return }

    let dbFiles = files.filter { $0.hasSuffix(".db") }
    for dbFile in dbFiles {
      let dbPath = directory.appendingPathComponent(dbFile)
      let walPath = URL(fileURLWithPath: dbPath.path + "-wal")
      let shmPath = URL(fileURLWithPath: dbPath.path + "-shm")

      let dbSize = (try? fm.attributesOfItem(atPath: dbPath.path)[.size] as? Int64) ?? 0
      let walSize = (try? fm.attributesOfItem(atPath: walPath.path)[.size] as? Int64) ?? 0
      let shmSize = (try? fm.attributesOfItem(atPath: shmPath.path)[.size] as? Int64) ?? 0
      let walExists = fm.fileExists(atPath: walPath.path)

      // Check POSIX file lock state on the main DB file
      let lockState = probePOSIXLock(at: dbPath)

      staticLogger.warning(
        """
        📊 [WAL-DIAG] [\(processTag)/\(pid)] \(label) | \(dbType)/\(dbFile) \
        db=\(dbSize)B wal=\(walExists ? "\(walSize)B" : "NONE") shm=\(shmSize)B \
        lock=\(lockState)
        """
      )
    }
  }

  /// Probe POSIX advisory lock state on a file.
  /// Returns a human-readable string describing the lock holder (if any).
  private nonisolated static func probePOSIXLock(at url: URL) -> String {
    guard let fd = fopen(url.path, "r") else {
      return "cannot_open"
    }
    defer { fclose(fd) }

    let fileno = fileno(fd)
    var flock = Darwin.flock()
    flock.l_type = Int16(F_WRLCK)
    flock.l_whence = Int16(SEEK_SET)
    flock.l_start = 0
    flock.l_len = 0  // Entire file

    // F_GETLK checks if a lock WOULD block, and fills in the conflicting lock info
    if fcntl(fileno, F_GETLK, &flock) == -1 {
      return "probe_failed(errno=\(errno))"
    }

    if flock.l_type == Int16(F_UNLCK) {
      return "unlocked"
    }

    let lockType = flock.l_type == Int16(F_RDLCK) ? "READ" : "WRITE"
    return "\(lockType)_by_pid=\(flock.l_pid)"
  }

  /// Run a quick integrity check on a database file using a fresh ephemeral connection.
  /// Returns "ok" on success, or the error description on failure.
  ///
  /// IMPORTANT: This opens its own connection — do not call during emergency close.
  public func quickIntegrityCheck(for userDID: String) async -> String {
    do {
      let result = try await performLightweightRead(for: userDID) { db -> String in
        // quick_check is faster than full integrity_check — stops at first error
        if let check = try String.fetchOne(db, sql: "PRAGMA quick_check;") {
          return check
        }
        return "no_result"
      }
      return result
    } catch {
      return "error: \(error.localizedDescription)"
    }
  }

  private func classifyQuickIntegrityCheckResult(_ result: String) -> QuickIntegrityCheckClassification {
    let normalized = result.trimmingCharacters(in: .whitespacesAndNewlines)
    if normalized.lowercased() == "ok" {
      return .ok
    }

    let error = NSError(
      domain: "MLSGRDBManager.ForegroundResume",
      code: -1,
      userInfo: [NSLocalizedDescriptionKey: normalized]
    )

    if isTransientError(error) {
      return .transient(normalized)
    }

    if isRecoverableCodecError(error) {
      return .corruption(normalized)
    }

    return .unexpected(normalized)
  }

  private func runQuickIntegrityCheckWithRetry(for userDID: String) async -> QuickIntegrityCheckClassification {
    let maxAttempts = self.foregroundResumeTransientRetryCount
    let baseDelayNanos = self.foregroundResumeBaseRetryDelayNanos

    for attempt in 1...maxAttempts {
      let result = await quickIntegrityCheck(for: userDID)
      let classification = classifyQuickIntegrityCheckResult(result)

      switch classification {
      case .ok, .corruption, .unexpected:
        return classification
      case .transient(let description):
        if attempt == maxAttempts {
          return .transient(description)
        }

        let delayNanos = baseDelayNanos * UInt64(attempt)
        let delayMs = delayNanos / 1_000_000
        logger.warning(
          "⏳ [RESUME] Transient integrity-check result (\(attempt)/\(maxAttempts)) for \(userDID.prefix(20), privacy: .private): \(description, privacy: .public). Retrying in \(delayMs)ms"
        )
        try? await Task.sleep(nanoseconds: delayNanos)
      }
    }

    return .unexpected("unknown integrity check state")
  }

  public func prepareForForegroundResume(
    for userDID: String,
    allowCoordinatedReset: Bool = true
  ) async throws -> ForegroundResumePreparationResult {
    if foregroundResumePreparations[userDID] != nil {
      try await waitForForegroundResumePreparationIfNeeded(
        for: userDID,
        operation: "prepareForForegroundResume"
      )
      return .ready
    }

    foregroundResumePreparations[userDID] = ForegroundResumePreparation(startedAt: Date())
    logger.info(
      "🚦 [RESUME-GUARD] Starting foreground resume preparation for \(userDID.prefix(20), privacy: .private)"
    )

    defer {
      foregroundResumePreparations.removeValue(forKey: userDID)
      logger.info(
        "✅ [RESUME-GUARD] Finished foreground resume preparation for \(userDID.prefix(20), privacy: .private)"
      )
    }

    let initialCheck = await runQuickIntegrityCheckWithRetry(for: userDID)

    switch initialCheck {
    case .ok:
      clearRepairState(for: userDID)
      clearHardResetFlag(for: userDID)
      clearDatabaseAccessSuspensionIfNeeded(for: userDID, operation: "prepareForForegroundResume")
      return .ready

    case .transient(let description):
      // Transient errors (SQLITE_NOMEM, locked) after retries usually mean the pool's
      // internal reader connections are stale from suspension. Close and reopen the pool
      // to get fresh connections, then retry one more time.
      logger.warning(
        "🔄 [RESUME] Transient errors persisted after retries - closing and reopening pool for \(userDID.prefix(20), privacy: .private)"
      )
      closeDatabase(for: userDID)
      // Clear any suspension state that might block the reopen
      databaseAccessSuspensions.removeValue(forKey: userDID)

      let retryCheck = await runQuickIntegrityCheckWithRetry(for: userDID)
      switch retryCheck {
      case .ok:
        clearRepairState(for: userDID)
        clearHardResetFlag(for: userDID)
        logger.info("✅ [RESUME] Pool reopen resolved transient errors")
        return .repaired
      case .corruption(let corruptionDesc):
        logger.error("🚨 [RESUME] Pool reopen revealed corruption: \(corruptionDesc)")
        // Fall through to repair path below
        try await repairDatabase(for: userDID)
        let postRepairCheck = await runQuickIntegrityCheckWithRetry(for: userDID)
        if case .ok = postRepairCheck {
          clearRepairState(for: userDID)
          clearHardResetFlag(for: userDID)
          clearDatabaseAccessSuspensionIfNeeded(for: userDID, operation: "prepareForForegroundResume")
          return .repaired
        }
        throw MLSSQLCipherError.storageUnavailable(
          reason: "MLS storage corruption persists after pool reopen + WAL repair: \(description)"
        )
      default:
        throw MLSSQLCipherError.storageUnavailable(
          reason: "MLS foreground resume failed after pool reopen: \(description)"
        )
      }

    case .unexpected(let description):
      throw MLSSQLCipherError.storageUnavailable(
        reason: "MLS foreground resume integrity check failed: \(description)"
      )

    case .corruption(let description):
      logger.error(
        "🚨 [RESUME] Verified corruption before foreground reopen for \(userDID.prefix(20), privacy: .private): \(description, privacy: .public)"
      )
      logger.warning(
        "🔧 [RESUME] Performing WAL/SHM cleanup before any foreground database reopen"
      )
      try await repairDatabase(for: userDID)

      let verificationCheck = await runQuickIntegrityCheckWithRetry(for: userDID)

      switch verificationCheck {
      case .ok:
        clearRepairState(for: userDID)
        clearHardResetFlag(for: userDID)
        clearDatabaseAccessSuspensionIfNeeded(for: userDID, operation: "prepareForForegroundResume")
        logger.info("✅ [RESUME] Foreground corruption resolved after WAL/SHM cleanup")
        return .repaired

      case .transient(let verificationDescription):
        throw MLSSQLCipherError.storageUnavailable(
          reason:
            "MLS foreground resume is still waiting on storage after repair: \(verificationDescription)"
        )

      case .unexpected(let verificationDescription):
        throw MLSSQLCipherError.storageUnavailable(
          reason: "MLS foreground verification failed after repair: \(verificationDescription)"
        )

      case .corruption(let verificationDescription):
        // WAL repair didn't fix page-level corruption. Do NOT wipe immediately.
        // Let the walRepairState counter in recordDatabaseAccessFailure handle
        // escalation after multiple failed attempts. This prevents data loss
        // on transient corruption that resolves after a full pool close/reopen cycle.
        logger.error(
          "🚨 [RESUME] Persistent corruption after WAL/SHM cleanup for \(userDID.prefix(20), privacy: .private): \(verificationDescription, privacy: .public)"
        )
        logger.warning(
          "⏳ [RESUME] Not wiping database — walRepairState counter will escalate if corruption persists across attempts"
        )
        throw MLSSQLCipherError.storageUnavailable(
          reason: "MLS storage corruption persists after WAL repair. Will retry on next access."
        )
      }
    }
  }

  /// Sanitize DID for filesystem compatibility
  /// Removes characters that are problematic in file paths
  private nonisolated func sanitizeDID(_ userDID: String) -> String {
    userDID
      .replacingOccurrences(of: ":", with: "-")
      .replacingOccurrences(of: "/", with: "-")
      .replacingOccurrences(of: "#", with: "-")
      .replacingOccurrences(of: "?", with: "-")
  }

  /// Get database file path for user
  private func databasePath(for userDID: String) -> URL {
    // Sanitize DID string for use in filesystem path (doesn't modify the actual DID)
    let sanitizedDID = sanitizeDID(userDID)

    // Create filename: mls_messages_{DID}.db
    let filename = "mls_messages_\(sanitizedDID).\(fileExtension)"

    return databaseDirectory.appendingPathComponent(filename)
  }
  
  // MARK: - WAL Monitoring & Health Checks
  
  /// Threshold for WAL file size warning (5MB)
  /// WAL files larger than this may indicate checkpoint failures
  private nonisolated let walSizeWarningThreshold: Int64 = 5 * 1024 * 1024
  
  /// Threshold for WAL file size critical alert (20MB)
  /// WAL files this large are a serious problem and may cause OOM
  private nonisolated let walSizeCriticalThreshold: Int64 = 20 * 1024 * 1024
  
  /// WAL health status for monitoring
  public struct WALHealthStatus: Sendable {
    public let userDID: String
    public let walSize: Int64
    public let shmSize: Int64
    public let dbSize: Int64
    public let status: WALStatus
    public let message: String
    
    public enum WALStatus: String, Sendable {
      case healthy = "healthy"
      case warning = "warning"
      case critical = "critical"
      case missing = "missing"
    }
  }
  
  /// Check WAL file health for a specific user
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: WAL health status with size information and recommendations
  public nonisolated func checkWALHealth(for userDID: String) -> WALHealthStatus {
    let sanitizedDID = sanitizeDID(userDID)
    let dbFilename = "mls_messages_\(sanitizedDID).\(fileExtension)"
    let dbPath = databaseDirectory.appendingPathComponent(dbFilename)
    let walPath = URL(fileURLWithPath: dbPath.path + "-wal")
    let shmPath = URL(fileURLWithPath: dbPath.path + "-shm")
    
    let fm = FileManager.default
    
    // Get file sizes
    let dbSize = (try? fm.attributesOfItem(atPath: dbPath.path)[.size] as? Int64) ?? 0
    let walSize = (try? fm.attributesOfItem(atPath: walPath.path)[.size] as? Int64) ?? 0
    let shmSize = (try? fm.attributesOfItem(atPath: shmPath.path)[.size] as? Int64) ?? 0
    
    // Determine status
    let status: WALHealthStatus.WALStatus
    let message: String
    
    if !fm.fileExists(atPath: dbPath.path) {
      status = .missing
      message = "Database file does not exist"
    } else if walSize >= walSizeCriticalThreshold {
      status = .critical
      message = "WAL file critically large (\(walSize / 1024 / 1024)MB) - checkpoint likely failing. " +
                "This may cause SQLite error 7. Recommend force checkpoint or database repair."
    } else if walSize >= walSizeWarningThreshold {
      status = .warning
      message = "WAL file growing large (\(walSize / 1024 / 1024)MB) - checkpoint may be delayed. " +
                "Consider triggering manual checkpoint during idle time."
    } else {
      status = .healthy
      message = "WAL size normal (\(walSize / 1024)KB)"
    }
    
    logger.debug("🔍 WAL Health Check for \(userDID.prefix(20), privacy: .private): status=\(status.rawValue), walSize=\(walSize), dbSize=\(dbSize)")
    
    return WALHealthStatus(
      userDID: userDID,
      walSize: walSize,
      shmSize: shmSize,
      dbSize: dbSize,
      status: status,
      message: message
    )
  }
  
  /// Check WAL health for all open databases
  /// - Returns: Array of WAL health statuses
  public func checkAllWALHealth() -> [WALHealthStatus] {
    var results: [WALHealthStatus] = []
    
    for userDID in databases.keys {
      results.append(checkWALHealth(for: userDID))
    }
    
    // Log summary
    let criticalCount = results.filter { $0.status == .critical }.count
    let warningCount = results.filter { $0.status == .warning }.count
    
    if criticalCount > 0 {
      logger.error("🚨 WAL Health Summary: \(criticalCount) CRITICAL, \(warningCount) warning, \(results.count) total")
    } else if warningCount > 0 {
      logger.warning("⚠️ WAL Health Summary: \(warningCount) warning, \(results.count) total")
    } else {
      logger.debug("✅ WAL Health Summary: All \(results.count) databases healthy")
    }
    
    return results
  }
  
  // MARK: - Connection Pool Metrics
  
  /// Connection pool statistics for monitoring
  public struct ConnectionPoolMetrics: Sendable {
    public let activeConnections: Int
    public let openDatabaseCount: Int
    public let pendingCloseCount: Int
    public let recentForceCloseCount: Int
    public let status: PoolStatus
    
    public enum PoolStatus: String, Sendable {
      case healthy = "healthy"
      case busy = "busy"
      case exhausted = "exhausted"
    }
  }
  
  /// Get connection pool metrics for monitoring
  /// - Returns: Current connection pool statistics
  public func getConnectionPoolMetrics() -> ConnectionPoolMetrics {
    let openCount = databases.count
    // Pending close operations tracking removed
    
    // Count recent force closes (last 5 minutes)
    let fiveMinutesAgo = Date().addingTimeInterval(-300)
    let recentForceCloses = forceClosedPools.filter { $0.timestamp > fiveMinutesAgo }.count
    
    // Determine status
    let status: ConnectionPoolMetrics.PoolStatus
    if recentForceCloses >= 3 {
      status = .exhausted
      logger.error("🚨 Connection pool EXHAUSTED: \(recentForceCloses) force closes in last 5 minutes")
    } else if openCount > 3 {
      status = .busy
      logger.warning("⚠️ Connection pool busy: \(openCount) open")
    } else {
      status = .healthy
    }
    
    return ConnectionPoolMetrics(
      activeConnections: openCount,
      openDatabaseCount: openCount,
      pendingCloseCount: 0,
      recentForceCloseCount: recentForceCloses,
      status: status
    )
  }
  
  // MARK: - Automatic Maintenance
  
  /// Perform idle-time maintenance on all open databases
  /// Call this during app idle periods (e.g., after UI becomes inactive)
  /// 
  /// Maintenance includes:
  /// - WAL checkpoint (PASSIVE mode - doesn't block readers)
  /// - Connection pool cleanup for inactive databases
  /// - WAL health logging
  ///
  /// - Parameter aggressiveCheckpoint: If true, uses TRUNCATE mode (blocks briefly but resets WAL)
  public func performIdleMaintenance(aggressiveCheckpoint: Bool = false) async {
    let dbCount = self.databases.count
    logger.info("🧹 Starting idle maintenance for \(dbCount) open database(s)")
    
    let startTime = Date()
    var checkpointedCount = 0
    var failedCount = 0
    
    for (userDID, database) in self.databases {
      // Check WAL health first
      let health = checkWALHealth(for: userDID)
      
      if health.status == .critical || health.status == .warning || aggressiveCheckpoint {
        // Attempt checkpoint
        do {
          try await database.write { db in
            let checkpointMode = aggressiveCheckpoint ? "TRUNCATE" : "PASSIVE"
            try db.execute(sql: "PRAGMA wal_checkpoint(\(checkpointMode));")
          }
          checkpointedCount += 1
          logger.debug("✅ Checkpointed database for \(userDID.prefix(20), privacy: .private)")
        } catch {
          failedCount += 1
          let errorDesc = error.localizedDescription.lowercased()
          if errorDesc.contains("locked") || errorDesc.contains("busy") {
            logger.debug("⏳ Checkpoint deferred (database busy) for \(userDID.prefix(20), privacy: .private)")
          } else {
            logger.warning("⚠️ Checkpoint failed for \(userDID.prefix(20), privacy: .private): \(error.localizedDescription)")
          }
        }
      }
    }
    
    // Log pool metrics
    let metrics = getConnectionPoolMetrics()
    
    let elapsed = Date().timeIntervalSince(startTime)
    let elapsedStr = String(format: "%.2f", elapsed)
    let statusStr = metrics.status.rawValue
    logger.info("🧹 Idle maintenance complete in \(elapsedStr)s: \(checkpointedCount) checkpointed, \(failedCount) deferred, pool status: \(statusStr)")
  }
  
  /// Close databases that haven't been accessed recently
  /// Call this to free resources when app is low on memory or backgrounded
  /// - Parameter keepActiveUser: If true, keeps the active user's database open
  /// - Returns: Number of databases closed
  @discardableResult
  public func closeInactiveDatabases(keepActiveUser: Bool = true) async -> Int {
    var closedCount = 0
    
    let usersToClose = self.databases.keys.filter { userDID in
      if keepActiveUser && userDID == self.activeUserDID {
        return false
      }
      return true
    }
    
    for userDID in usersToClose {
      let success = await closeDatabaseAndDrain(for: userDID, timeout: 3.0)
      if success {
        closedCount += 1
        logger.debug("🗑️ Closed inactive database for \(userDID.prefix(20), privacy: .private)")
      }
    }
    
    if closedCount > 0 {
      let remainingCount = self.databases.count
      logger.info("🗑️ Closed \(closedCount) inactive database(s), kept \(remainingCount) open")
    }
    
    return closedCount
  }
  
  /// Emergency cleanup when system is low on resources
  /// Aggressively closes all databases except the active one
  /// - Returns: Number of databases closed
  @discardableResult
  public func emergencyCleanup() async -> Int {
    logger.warning("🚨 Emergency cleanup triggered - closing non-essential databases")
    
    // First, check pool metrics
    let metrics = getConnectionPoolMetrics()
    let statusStr = metrics.status.rawValue
    logger.warning("   Pool status before cleanup: \(statusStr), open=\(metrics.openDatabaseCount), pending=\(metrics.pendingCloseCount)")
    
    // Close all but active
    let closedCount = await closeInactiveDatabases(keepActiveUser: true)
    
    // If active database has critical WAL, try aggressive checkpoint
    if let activeUser = activeUserDID {
      let health = checkWALHealth(for: activeUser)
      if health.status == .critical {
        logger.warning("   Active user has critical WAL - attempting aggressive checkpoint")
        await performIdleMaintenance(aggressiveCheckpoint: true)
      }
    }
    
    return closedCount
  }
}

// MARK: - Debug Helpers

#if DEBUG
  extension MLSGRDBManager {
    /// List all database files
    func listDatabases() -> [String] {
      guard let files = try? FileManager.default.contentsOfDirectory(atPath: databaseDirectory.path)
      else {
        return []
      }

      return files.filter { $0.hasSuffix(".\(fileExtension)") }
    }

    /// Get table statistics for a user's database
    func getTableStats(for userDID: String) async throws -> [String: Int] {
      let db = try await getDatabasePool(for: userDID)

      return try await db.read { database in
        var stats: [String: Int] = [:]

        // Get all table names
        let tables = try String.fetchAll(
          database,
          sql: "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        )

        // Count rows in each table
        for table in tables {
          if let count = try Int.fetchOne(database, sql: "SELECT COUNT(*) FROM \(table);") {
            stats[table] = count
          }
        }

        return stats
      }
    }

    /// Export database to unencrypted file (for debugging only)
    func exportUnencrypted(for userDID: String, to destinationPath: String) async throws {
      let db = try await getDatabasePool(for: userDID)

      try await db.write { database in
        // Use SQLCipher's ATTACH and export
        try database.execute(
          sql: """
              ATTACH DATABASE '\(destinationPath)' AS plaintext KEY '';
              SELECT sqlcipher_export('plaintext');
              DETACH DATABASE plaintext;
            """)
      }

      logger.warning(
        "⚠️ Exported UNENCRYPTED database to \(destinationPath) - DELETE after debugging!")
    }

    /// Verify database encryption
    func verifyEncryption(for userDID: String) async throws -> Bool {
      let db = try await getDatabasePool(for: userDID)

      // Try to query sqlite_master (should succeed with correct key)
      return try await db.read { database in
        _ = try Int.fetchOne(database, sql: "SELECT COUNT(*) FROM sqlite_master;")
        return true
      }
    }

    /// Run integrity check on database
    func checkIntegrity(for userDID: String) async throws -> Bool {
      let db = try await getDatabasePool(for: userDID)

      return try await db.read { database in
        if let result = try String.fetchOne(database, sql: "PRAGMA integrity_check;") {
          return result == "ok"
        }
        return false
      }
    }

    /// Optimize database (vacuum and analyze)
    func optimize(for userDID: String) async throws {
      let db = try await getDatabasePool(for: userDID)

      try await db.write { database in
        try database.execute(sql: "VACUUM;")
        try database.execute(sql: "ANALYZE;")
      }

      logger.info("Optimized database for user: \(userDID, privacy: .private)")
    }
  }
#endif
