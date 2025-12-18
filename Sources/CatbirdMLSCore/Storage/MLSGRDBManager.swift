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
//  3. HMAC verification failure (wrong encryption key)
//  4. Connection pool exhaustion with multiple accounts
//
//  Key fixes implemented:
//  - pendingCloseOperations tracking prevents open-while-closing race
//  - closeDatabaseAndDrain() must complete before opening new database
//  - Increased cache_size from 1MB to 2MB for SQLCipher overhead
//  - Reduced wal_autocheckpoint from 500 to 200 pages
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
import OSLog

/// Manages encrypted GRDB DatabasePool instances with per-user isolation.
/// Actor provides thread-safe access and automatic isolation.
/// Uses DatabasePool for better read concurrency (multiple concurrent readers, single writer).
public actor MLSGRDBManager {

  // MARK: - Properties

  /// Shared singleton instance
  public static let shared = MLSGRDBManager()

  /// Logger for database operations
  private nonisolated let logger = Logger(subsystem: "Catbird", category: "MLSGRDBManager")

  /// Active database pools per user DID (upgraded from DatabaseQueue for better concurrency)
  private var databases: [String: DatabasePool] = [:]

  /// Encryption manager
  private let encryption = MLSSQLCipherEncryption.shared

  /// In-memory key fingerprint per user (helps detect key reuse/mismatch within a process)
  private var keyFingerprints: [String: String] = [:]

  /// Base directory for all user databases
  private nonisolated let databaseDirectory: URL

  /// Database file extension
  private nonisolated let fileExtension = "db"

  // MARK: - Account Switch Serialization (OOM Fix)

  /// Tracks users currently undergoing database close operations
  /// Used to prevent opening a database while it's being closed (race condition fix)
  private var pendingCloseOperations: Set<String> = []

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

  /// Maximum time to wait for a pending close operation before timing out
  private let closeOperationTimeout: TimeInterval = 10.0

  // MARK: - Periodic Checkpointing (fast switches)

  private var periodicCheckpointTask: Task<Void, Never>?
  private let periodicCheckpointInterval: TimeInterval = 30.0
  
  // MARK: - Diagnostics Tracking (OOM Fix 2024-12)
  
  /// Tracks force-closed pools for debugging resource exhaustion issues
  /// Contains (userDID prefix, timestamp) pairs for recent force closes
  private var forceClosedPools: [(userDID: String, timestamp: Date)] = []
  
  /// Maximum number of force close entries to keep for diagnostics
  private let maxForceCloseHistory = 10

  // MARK: - Initialization

  private init() {
    // Create base directory for MLS databases
    let appSupport: URL
    if let sharedContainer = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared")
    {
      appSupport = sharedContainer
    } else {
      appSupport =
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
    }
    self.databaseDirectory = appSupport.appendingPathComponent("MLS", isDirectory: true)

    // Create directory if it doesn't exist
    do {
      try FileManager.default.createDirectory(
        at: databaseDirectory, withIntermediateDirectories: true)
    } catch {
      logger.error("Failed to create database directory: \(error.localizedDescription)")
    }
  }

  // MARK: - Lock Helpers

  private struct AdvisoryLockGuard {
    let userDID: String
    func release() {
      MLSAdvisoryLockCoordinator.shared.releaseExclusiveLock(for: userDID)
    }
  }

  private func requireAdvisoryLock(
    for userDID: String,
    timeout: TimeInterval,
    context: String
  ) throws -> AdvisoryLockGuard {
    let acquired = MLSAdvisoryLockCoordinator.shared.acquireExclusiveLock(
      for: userDID,
      timeout: timeout
    )
    guard acquired else {
      throw MLSSQLCipherError.storageUnavailable(
        reason: "Unable to acquire storage lock for \(context)"
      )
    }
    return AdvisoryLockGuard(userDID: userDID)
  }

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

  /// Get or create encrypted DatabasePool for a user (actor isolation)
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: Encrypted GRDB DatabasePool
  /// - Throws: MLSSQLCipherError if creation fails
  /// - Important: Caller should verify userDID matches the currently active account to prevent key mismatch
  public func getDatabasePool(for userDID: String) async throws -> DatabasePool {
    // CRITICAL FIX: Track which user is currently being accessed to detect key mismatch scenarios
    // Log the access for debugging account switching issues
    logger.debug(
      "📀 getDatabasePool requested for user: \(userDID.prefix(20), privacy: .private)...")

    if currentConnectionState(for: userDID) == .closing {
      throw MLSSQLCipherError.storageUnavailable(reason: "MLS storage is closing")
    }

    // OOM FIX #1: Wait for any pending close operations on this user's database
    // This prevents the race condition where we try to open while still closing
    if pendingCloseOperations.contains(userDID) {
      logger.warning(
        "⏳ Waiting for pending close operation on database: \(userDID.prefix(20), privacy: .private)..."
      )
      let startWait = Date()

      // Poll until the close operation completes or timeout
      while pendingCloseOperations.contains(userDID) {
        if Date().timeIntervalSince(startWait) > closeOperationTimeout {
          logger.error(
            "❌ Timeout waiting for database close operation: \(userDID.prefix(20), privacy: .private)"
          )
          throw MLSSQLCipherError.databaseCreationFailed(
            underlying: NSError(
              domain: "MLSGRDBManager",
              code: -3,
              userInfo: [
                NSLocalizedDescriptionKey:
                  "Database close operation timed out - account switch may be in progress"
              ]
            ))
        }
        try await Task.sleep(nanoseconds: 50_000_000)  // 50ms polling
      }
      logger.info("✅ Pending close completed, proceeding with database open")
    }

    // Track the active user; switching safety is handled by explicit close/drain + advisory locks.
    if activeUserDID != userDID {
      if let previousUser = activeUserDID {
        logger.debug(
          "🔄 Switching active database from \(previousUser.prefix(20), privacy: .private) to \(userDID.prefix(20), privacy: .private)"
        )
      }
      activeUserDID = userDID
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
        return existingDatabase
      } catch let error as CancellationError {
        throw error
      } catch {
        // CRITICAL FIX (Issue B): Differentiate between key mismatch (HMAC failure) and true corruption
        // SQLCipher HMAC check failure means wrong key, NOT corruption - don't delete the database!
        let errorDesc = error.localizedDescription.lowercased()
        let isHMACFailure =
          errorDesc.contains("hmac check failed")
          || errorDesc.contains("hmac verification")
          || errorDesc.contains("hmac_check")
          || errorDesc.contains("sqlcipher_page_cipher")
          || (errorDesc.contains("hmac") && errorDesc.contains("pgno"))
          || (errorDesc.contains("hmac") && errorDesc.contains("page"))

        if isHMACFailure {
          logger.error("🔐 HMAC check failed - attempting SOFT RECOVERY (WAL/SHM cleanup only)")
          logger.error("   This typically happens when WAL file is out of sync with main DB")
          logger.error("   User requested: \(userDID.prefix(20), privacy: .private)...")

          // Remove from cache
          databases.removeValue(forKey: userDID)
          
          // ═══════════════════════════════════════════════════════════════════════════
          // SOFT RECOVERY: Only delete WAL and SHM files, preserve the main .db file
          // ═══════════════════════════════════════════════════════════════════════════
          // HMAC failures often occur when:
          // 1. App was force-killed while WAL was being written
          // 2. Account switch interrupted a checkpoint operation
          // 3. NSE and main app had conflicting WAL states
          //
          // In these cases, deleting the WAL/SHM allows the DB to open with whatever
          // data was already committed to the main .db file. This may lose some
          // recent messages but preserves the vast majority of conversation history.
          // ═══════════════════════════════════════════════════════════════════════════
          let softRecoverySuccess = attemptSoftRecovery(for: userDID)
          
          if softRecoverySuccess {
            logger.info("✅ Soft recovery completed - retrying database open...")
            // Fall through to normal database creation below
          } else {
            logger.error("❌ Soft recovery failed - database may be truly corrupted")
            throw MLSSQLCipherError.encryptionKeyMismatch(
              message: "Database HMAC verification failed and soft recovery unsuccessful"
            )
          }
        } else {
          // Only treat known SQLCipher/SQLite corruption/codec failures as "unhealthy".
          // Avoid destructive recovery for transient errors (locks, timeouts, etc.).
          if isRecoverableCodecError(error) {
            logger.warning(
              "⚠️ Cached database connection unhealthy, reconnecting: \(error.localizedDescription)")
            databases.removeValue(forKey: userDID)

            // Repair WAL/SHM files
            try? repairDatabase(for: userDID)
          } else {
            logger.debug(
              "Database validation query failed (non-recoverable), reusing existing connection: \(error.localizedDescription)"
            )
            return existingDatabase
          }
        }
      }
    }

    updateConnectionState(.opening, for: userDID)

    // Check if database file already exists
    let dbPath = databasePath(for: userDID)
    let isNewDatabase = !FileManager.default.fileExists(atPath: dbPath.path)

    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 4: Acquire advisory lock before opening database
    // ═══════════════════════════════════════════════════════════════════════════
    // This ensures we don't try to open while NSE is performing a TRUNCATE checkpoint.
    // The lock is released after successful open.
    // ═══════════════════════════════════════════════════════════════════════════
    logger.debug("🔐 [Open] Acquiring advisory lock for: \(userDID.prefix(20), privacy: .private)")
    let lock = try requireAdvisoryLock(for: userDID, timeout: 3.0, context: "open")
    defer { lock.release() }

    // Create/open database (runs off main thread via actor isolation)
    do {
      let database = try await createDatabase(for: userDID)

      // Cache the database (actor isolation provides thread-safety)
      databases[userDID] = database
      updateConnectionState(.open, for: userDID)
      startPeriodicCheckpointingIfNeeded()

      if isNewDatabase {
        logger.info("✨ Created new database pool for user: \(userDID, privacy: .private)")
      } else {
        logger.info("📂 Opened existing database pool for user: \(userDID, privacy: .private)")
      }

      return database
    } catch {
      updateConnectionState(.closed, for: userDID)
      // Check if this is an HMAC/corruption error that persists after soft recovery
      let errorDesc = error.localizedDescription.lowercased()
      let isHMACError = errorDesc.contains("hmac") 
        || errorDesc.contains("cipher_page_cipher")
        || errorDesc.contains("file is encrypted or is not a database")
        || errorDesc.contains("not a database")
      
      // Fail-closed: never delete or recreate databases automatically.
      // If soft recovery (WAL/SHM cleanup) did not restore readability, require explicit user action.
      if isHMACError {
        logger.critical("🚨 MLS storage open failed after soft recovery")
        logger.critical("   Error: \(error.localizedDescription)")
        throw MLSSQLCipherError.needsUserAction(
          reason: "MLS storage could not be opened. Use Settings ▸ Diagnostics ▸ Reset MLS Storage."
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
  /// - DOES retry with exponential backoff on HMAC failures (Phase 3)
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
  /// - Throws: MLSSQLCipherError if creation fails
  public func getEphemeralDatabasePool(for userDID: String) async throws -> DatabasePool {
    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 3: Exponential Backoff Retry for HMAC/Error-7 failures
    // ═══════════════════════════════════════════════════════════════════════════
    let maxRetries = 3
    let baseDelayNanos: UInt64 = 100_000_000  // 100ms
    
    for attempt in 0..<maxRetries {
      do {
        return try await _getEphemeralDatabasePoolInternal(for: userDID)
      } catch {
        if isHMACOrError7(error) && attempt < maxRetries - 1 {
          let delayNanos = baseDelayNanos * (1 << attempt)  // 100ms, 200ms, 400ms
          let delayMs = delayNanos / 1_000_000
          logger.warning("🔄 [Retry] HMAC failure on attempt \(attempt + 1)/\(maxRetries), backoff: \(delayMs)ms")
          
          // Clean WAL/SHM files before retry
          _ = attemptSoftRecovery(for: userDID)
          
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
  }
  
  /// Internal implementation of getEphemeralDatabasePool
  private func _getEphemeralDatabasePoolInternal(for userDID: String) async throws -> DatabasePool {
    logger.debug(
      "📀 [Ephemeral] getDatabasePool requested for user: \(userDID.prefix(20), privacy: .private)..."
    )
    logger.debug("   Active user: \(self.activeUserDID?.prefix(20) ?? "none", privacy: .private)")
    logger.debug("   Strategy: NO checkpoint, NO active user switch")

    // Wait for any pending close operations on this user's database
    if pendingCloseOperations.contains(userDID) {
      logger.warning("⏳ [Ephemeral] Waiting for pending close operation...")
      let startWait = Date()

      while pendingCloseOperations.contains(userDID) {
        if Date().timeIntervalSince(startWait) > closeOperationTimeout {
          logger.error("❌ [Ephemeral] Timeout waiting for database close operation")
          throw MLSSQLCipherError.databaseCreationFailed(
            underlying: NSError(
              domain: "MLSGRDBManager",
              code: -3,
              userInfo: [NSLocalizedDescriptionKey: "Database close operation timed out"]
            ))
        }
        try await Task.sleep(nanoseconds: 50_000_000)  // 50ms polling
      }
      logger.info("✅ [Ephemeral] Pending close completed, proceeding")
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
        let errorDesc = error.localizedDescription.lowercased()
        let isHMACFailure =
          errorDesc.contains("hmac check failed")
          || errorDesc.contains("hmac verification")
          || errorDesc.contains("hmac_check")
          || errorDesc.contains("sqlcipher_page_cipher")
          || (errorDesc.contains("hmac") && errorDesc.contains("pgno"))
          || (errorDesc.contains("hmac") && errorDesc.contains("page"))

        if isHMACFailure {
          logger.error("🔐 [Ephemeral] HMAC check failed - attempting soft recovery")
          databases.removeValue(forKey: userDID)
          
          // Try soft recovery (WAL/SHM cleanup only) before failing
          let softRecoverySuccess = attemptSoftRecovery(for: userDID)
          if !softRecoverySuccess {
            throw MLSSQLCipherError.encryptionKeyMismatch(
              message: "Database key mismatch for ephemeral access and soft recovery failed"
            )
          }
          // Fall through to retry database creation below
        } else if isRecoverableCodecError(error) {
          logger.warning("⚠️ [Ephemeral] Cached connection unhealthy, reconnecting")
          databases.removeValue(forKey: userDID)
          try? repairDatabase(for: userDID)
        } else {
          // Non-fatal error, reuse existing connection
          return existingDatabase
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
      logger.warning("🔑 [Ephemeral] Key validation failed - attempting soft recovery")
      let softRecoverySuccess = attemptSoftRecovery(for: userDID)
      if !softRecoverySuccess {
        throw MLSSQLCipherError.encryptionKeyMismatch(
          message: "Key validation failed and soft recovery unsuccessful"
        )
      }
      // Fall through to create database after soft recovery
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // PHASE 4: Acquire advisory lock before opening database
    // ═══════════════════════════════════════════════════════════════════════════
    logger.debug("🔐 [Ephemeral] Acquiring advisory lock for: \(userDID.prefix(20), privacy: .private)")
    let lock = try requireAdvisoryLock(for: userDID, timeout: 3.0, context: "ephemeral open")
    defer { lock.release() }

    // Create new database connection
    // CRITICAL FIX: Do NOT cache ephemeral connections for non-active users
    // Caching creates memory pressure from accumulated mlock() allocations
    do {
      let database = try await createDatabase(for: userDID)
      
      // Only cache if this user is the active user or if we don't have an active user yet
      // For truly ephemeral access (notifications for other accounts), don't cache
      if activeUserDID == nil || activeUserDID == userDID {
        databases[userDID] = database
        startPeriodicCheckpointingIfNeeded()
        logger.info(
          "✅ [Ephemeral] Created and cached database for user: \(userDID.prefix(20), privacy: .private)")
      } else {
        // For non-active users, return without caching
        // The caller is responsible for the connection lifetime
        logger.info(
          "✅ [Ephemeral] Created UNCACHED database for user: \(userDID.prefix(20), privacy: .private)")
        logger.info("   (Not caching to reduce memory pressure during account switch)")
      }
      
      logger.info("✅ [Retry] Success on attempt")
      return database
    } catch {
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

    let lock = try requireAdvisoryLock(for: userDID, timeout: 2.0, context: "lightweight read")
    defer { lock.release() }
    
    // If we already have a running pool for this user (active account), use it
    if let existingPool = databases[userDID] {
      logger.debug("   Using existing cached pool")
      return try await existingPool.read(work)
    }
    
    // For non-active users, create a lightweight queue, use it, and close it immediately
    logger.debug("   Creating lightweight DatabaseQueue (no caching)")
    
    let dbPath = databasePath(for: userDID)
    
    // Get encryption key
    let encryptionKey = try await ensureKeyForDatabase(for: userDID)
    
    // Configure lightweight connection
    var config = Configuration()
    config.readonly = true  // Read-only for safety
    config.busyMode = .timeout(5.0)  // Shorter timeout for lightweight access
    
    config.prepareDatabase { db in
      // CRITICAL: Set memory security OFF first, before key
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")
      
      // Set encryption key
      let hexKey = encryptionKey.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA key = \"x'\(hexKey)'\";")
      
      // SQLCipher 4 settings
      try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
      try db.execute(sql: "PRAGMA kdf_iter = 256000;")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
      
      // Minimal cache for lightweight access
      try db.execute(sql: "PRAGMA cache_size = -500;")  // 500KB only
    }
    
    // Open, read, close immediately - no caching
    let queue = try DatabaseQueue(path: dbPath.path, configuration: config)
    defer {
      do {
        try queue.close()
        logger.debug("✅ [Lightweight] Closed ephemeral queue for: \(userDID.prefix(20), privacy: .private)")
      } catch {
        logger.warning("⚠️ [Lightweight] Failed to close queue: \(error.localizedDescription)")
      }
    }
    
      return try await queue.read(work)
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

    // Best-effort: only checkpoint when we can coordinate with other processes.
    let lockAcquired = MLSAdvisoryLockCoordinator.shared.acquireExclusiveLock(for: didToCheckpoint, timeout: 0.05)
    guard lockAcquired else {
      logger.debug("⏭️ Periodic checkpoint skipped (advisory lock busy)")
      return
    }
    defer { MLSAdvisoryLockCoordinator.shared.releaseExclusiveLock(for: didToCheckpoint) }

    do {
      try await MLSDatabaseCoordinator.shared.performWrite(for: didToCheckpoint, timeout: 1.0) {
        try await db.writeWithoutTransaction { db in
          try db.execute(sql: "PRAGMA wal_checkpoint(PASSIVE);")
        }
      }
      logger.debug("✅ Periodic WAL checkpoint(PASSIVE) for \(didToCheckpoint.prefix(20), privacy: .private)")
    } catch {
      logger.debug("⏭️ Periodic checkpoint skipped: \(error.localizedDescription)")
    }
  }

  /// Close database for a user
  /// - Parameter userDID: User's decentralized identifier
  public func closeDatabase(for userDID: String) {
    // OOM FIX: Track that we're closing this database
    pendingCloseOperations.insert(userDID)
    defer { pendingCloseOperations.remove(userDID) }

    // Clear active user if this was the active database
    if activeUserDID == userDID {
      activeUserDID = nil
    }

    guard let db = databases[userDID] else {
      logger.debug("No database to close for user: \(userDID, privacy: .private)")
      updateConnectionState(.closed, for: userDID)
      return
    }

    logger.info("Closing database for user: \(userDID, privacy: .private)")
    updateConnectionState(.closing, for: userDID)

    // Fail-closed: do not interrupt in-flight queries; if close cannot complete, keep the pool alive.
    do {
      try db.close()
      databases.removeValue(forKey: userDID)
      updateConnectionState(.closed, for: userDID)
      logger.info("✅ Database pool closed for user: \(userDID, privacy: .private)")
    } catch {
      updateConnectionState(.open, for: userDID)
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

    do {
      return try await withMLSExclusiveAccess(
        userDID: userDID,
        purpose: .closeAndDrain,
        timeout: duration
      ) { [self] in
        logger.info("🛑 Closing and draining database for user: \(userDID, privacy: .private)")

        // Prevent new opens while we're trying to close.
        pendingCloseOperations.insert(userDID)
        updateConnectionState(.closing, for: userDID)
        defer {
          pendingCloseOperations.remove(userDID)
        }

        // Clear active user if this was the active database
        if activeUserDID == userDID {
          activeUserDID = nil
        }

        guard let db = databases[userDID] else {
          logger.debug("   No database to close for user: \(userDID, privacy: .private)")
          updateConnectionState(.closed, for: userDID)
          return true
        }

        do {
          logger.info("📀 [Checkpoint] Starting TRUNCATE checkpoint for: \(userDID.prefix(20), privacy: .private)")
          let checkpointStart = Date()

          try await MLSDatabaseCoordinator.shared.performWrite(for: userDID, timeout: timeout) {
            try db.writeWithoutTransaction { database in
              try database.execute(sql: "PRAGMA wal_checkpoint(TRUNCATE);")
            }
          }

          let duration = Date().timeIntervalSince(checkpointStart)
          let durationStr = String(format: "%.2f", duration)
          logger.info("✅ [Checkpoint] TRUNCATE completed in \(durationStr)s")

          // Fail-closed: do not interrupt in-flight queries; if close cannot complete, report busy.
          try db.close()
          databases.removeValue(forKey: userDID)
          updateConnectionState(.closed, for: userDID)
          logger.info("✅ Database closed and drained for user: \(userDID, privacy: .private)")
          return true

        } catch {
          updateConnectionState(.open, for: userDID)
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

        // Clear active user if this was the active database
        if activeUserDID == userDID {
          activeUserDID = nil
        }

        guard let db = databases[userDID] else {
          logger.debug("   No database connection to release for user: \(userDID.prefix(20), privacy: .private)")
          updateConnectionState(.closed, for: userDID)
          return true
        }

        updateConnectionState(.closing, for: userDID)

        // Fail-closed: do not interrupt queries. If we can't close cleanly, don't acknowledge.
        do {
          try db.close()
          databases.removeValue(forKey: userDID)
          updateConnectionState(.closed, for: userDID)
          logger.info("🔓 [Release] Connection released for: \(userDID.prefix(20), privacy: .private)")
          return true
        } catch {
          updateConnectionState(.open, for: userDID)
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
  /// NOTE: HMAC verification failures are NOT considered recoverable here because they
  /// typically indicate a wrong encryption key (e.g., during account switching), not actual
  /// database corruption. The `getDatabasePool` method handles HMAC failures separately
  /// to avoid data loss from incorrect recovery attempts.
  ///
  /// - Parameter error: The error to check
  /// - Returns: True if the error is a recoverable database error (NOT including key mismatch)
  public nonisolated func isRecoverableCodecError(_ error: Error) -> Bool {
    let description = error.localizedDescription.lowercased()

    // CRITICAL FIX: HMAC failures indicate WRONG KEY, not corruption
    // Don't treat these as recoverable - they need the correct key, not database repair
    // Check for various HMAC failure patterns that indicate encryption key mismatch
    let isHMACFailure =
      description.contains("hmac check failed") 
      || description.contains("hmac verification")
      || description.contains("hmac_check")
      || description.contains("sqlcipher_page_cipher")
      || (description.contains("hmac") && description.contains("pgno"))
      || (description.contains("hmac") && description.contains("page"))

    if isHMACFailure {
      return false  // NOT recoverable via repair - needs correct key
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

    // Use progressive repair which applies escalating strategies
    // Pass the triggering error so it can distinguish transient vs corruption
    return try await progressiveRepair(for: userDID, lastError: triggeringError)
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

  /// Check if there's a pending close operation for a user
  public func hasPendingCloseOperation(for userDID: String) -> Bool {
    pendingCloseOperations.contains(userDID)
  }

  /// Delete database file for a user (when removing account)
  /// - Parameter userDID: User's decentralized identifier
  /// - Throws: MLSSQLCipherError if deletion fails
  public func deleteDatabase(for userDID: String) async throws {
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
  public func repairDatabase(for userDID: String) throws {
    logger.warning("⚠️ Attempting to repair database for user: \(userDID, privacy: .private)")

    // Close database first
    closeDatabase(for: userDID)

    let dbPath = databasePath(for: userDID)
    let walPath = URL(fileURLWithPath: dbPath.path + "-wal")
    let shmPath = URL(fileURLWithPath: dbPath.path + "-shm")

    // Delete WAL and SHM files (will be recreated)
    for path in [walPath, shmPath] {
      if FileManager.default.fileExists(atPath: path.path) {
        do {
          try FileManager.default.removeItem(at: path)
          logger.info("Deleted corrupted file: \(path.lastPathComponent)")
        } catch {
          logger.error("Failed to delete \(path.lastPathComponent): \(error.localizedDescription)")
        }
      }
    }

    logger.info("✅ Database repair completed for user: \(userDID, privacy: .private)")
  }
  
  /// Attempt soft recovery for HMAC/WAL desync issues
  /// This ONLY deletes WAL and SHM files, preserving the main .db file
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: true if soft recovery was performed, false if files couldn't be deleted
  private func attemptSoftRecovery(for userDID: String) -> Bool {
    logger.warning("🔧 [SoftRecovery] Attempting WAL/SHM cleanup for user: \(userDID.prefix(20), privacy: .private)")
    
    // Close any cached connection first
    closeDatabase(for: userDID)
    
    let dbPath = databasePath(for: userDID)
    let walPath = URL(fileURLWithPath: dbPath.path + "-wal")
    let shmPath = URL(fileURLWithPath: dbPath.path + "-shm")
    
    var deletedFiles: [String] = []
    var failedFiles: [String] = []
    
    // Only delete WAL and SHM files - NEVER the main .db file
    for path in [walPath, shmPath] {
      if FileManager.default.fileExists(atPath: path.path) {
        do {
          // Get file size for logging
          let attrs = try? FileManager.default.attributesOfItem(atPath: path.path)
          let fileSize = (attrs?[.size] as? Int64) ?? 0
          
          try FileManager.default.removeItem(at: path)
          deletedFiles.append("\(path.lastPathComponent) (\(fileSize) bytes)")
          logger.info("   ✅ Deleted: \(path.lastPathComponent) (\(fileSize) bytes)")
        } catch {
          failedFiles.append(path.lastPathComponent)
          logger.error("   ❌ Failed to delete \(path.lastPathComponent): \(error.localizedDescription)")
        }
      }
    }
    
    if !deletedFiles.isEmpty {
      logger.info("🔧 [SoftRecovery] Cleaned up: \(deletedFiles.joined(separator: ", "))")
    }
    
    if !failedFiles.isEmpty {
      logger.warning("🔧 [SoftRecovery] Failed to clean: \(failedFiles.joined(separator: ", "))")
      return false
    }
    
    // Verify the main .db file still exists
    if FileManager.default.fileExists(atPath: dbPath.path) {
      logger.info("🔧 [SoftRecovery] ✅ Main database file preserved: \(dbPath.lastPathComponent)")
      return true
    } else {
      logger.warning("🔧 [SoftRecovery] ⚠️ Main database file missing - will create new one")
      return true  // Still return true since cleanup was successful
    }
  }

  // MARK: - Phase 2: Key Validation Before Open
  
  /// Validate the encryption key before performing a full database open.
  ///
  /// This is a lightweight read-only check that catches HMAC issues early,
  /// allowing for soft recovery before the full database open attempt.
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

    let lock = try requireAdvisoryLock(for: userDID, timeout: 2.0, context: "key validation")
    defer { lock.release() }
    
    // Configure lightweight read-only connection
    var config = Configuration()
    config.readonly = true
    config.busyMode = .timeout(2.0)  // Short timeout for validation
    
    config.prepareDatabase { db in
      // CRITICAL: Set memory security OFF first, before key
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")
      
      // Set encryption key
      let hexKey = key.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA key = \"x'\(hexKey)'\";")
      
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
      let errorDesc = error.localizedDescription.lowercased()
      
      // Check for HMAC failure patterns
      let isHMACFailure =
        errorDesc.contains("hmac check failed")
        || errorDesc.contains("hmac verification")
        || errorDesc.contains("hmac_check")
        || errorDesc.contains("sqlcipher_page_cipher")
        || (errorDesc.contains("hmac") && errorDesc.contains("pgno"))
        || (errorDesc.contains("hmac") && errorDesc.contains("page"))
        || errorDesc.contains("file is encrypted or is not a database")
        || errorDesc.contains("not a database")
      
      if isHMACFailure {
        logger.warning("🔑 [KeyValidation] HMAC failure detected, triggering soft recovery")
        return false
      }
      
      // Non-HMAC error - propagate it
      logger.error("🔑 [KeyValidation] Non-HMAC error: \(error.localizedDescription)")
      throw error
    }
  }
  
  /// Check if an error indicates HMAC failure or SQLite error 7
  private nonisolated func isHMACOrError7(_ error: Error) -> Bool {
    let desc = error.localizedDescription.lowercased()
    
    // HMAC failure patterns
    let isHMAC = desc.contains("hmac check failed")
      || desc.contains("hmac verification")
      || desc.contains("hmac_check")
      || desc.contains("sqlcipher_page_cipher")
      || (desc.contains("hmac") && desc.contains("pgno"))
      || (desc.contains("hmac") && desc.contains("page"))
    
    // SQLite error 7 (often HMAC-related in SQLCipher context)
    let isError7 = desc.contains("sqlite error 7") || desc.contains("out of memory")
    
    return isHMAC || isError7
  }

  /// Manually reset MLS storage by quarantining existing files and recreating a fresh database.
  ///
  /// IMPORTANT: This MUST NEVER run automatically.
  public func quarantineAndResetDatabase(for userDID: String) async throws {
    try await withMLSExclusiveAccess(userDID: userDID, purpose: .maintenance, timeout: .seconds(10)) { [self] in
      logger.error("🧰 [Diagnostics] Quarantining + resetting MLS storage for: \(userDID.prefix(20), privacy: .private)")

      // Ensure the pool is fully closed first (fail-closed; do not interrupt queries).
      let closed = await closeDatabaseAndDrain(for: userDID, timeout: 10.0)
      guard closed else {
        throw MLSSQLCipherError.storageUnavailable(reason: "MLS storage is busy; try again")
      }

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

      // Clear repair state now that the old files are quarantined.
      repairAttempts.removeValue(forKey: userDID)
      updateConnectionState(.closed, for: userDID)

      // Recreate a fresh database.
      updateConnectionState(.opening, for: userDID)
      let database = try await createDatabase(for: userDID)
      databases[userDID] = database
      updateConnectionState(.open, for: userDID)
      startPeriodicCheckpointingIfNeeded()

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
        _ = attemptSoftRecovery(for: userDID)

        do {
          let db = try await createDatabase(for: userDID)
          databases[userDID] = db
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
    // Attempt 1-2: Try WAL/SHM repair
    // Attempt 3+: Full database reset (ONLY for true corruption, not transient errors)

    if attemptNumber <= 2 {
      // Try standard WAL/SHM repair
      logger.info("📝 Strategy: WAL/SHM file repair (attempt \(attemptNumber))")
      try? repairDatabase(for: userDID)

      // Try to open the database
      do {
        let db = try await createDatabase(for: userDID)
        databases[userDID] = db

        // Verify it's actually working
        _ = try await db.read { database in
          try Int.fetchOne(database, sql: "SELECT 1 FROM sqlite_master LIMIT 1;")
        }

        // Success! Reset the counter
        repairAttempts.removeValue(forKey: userDID)
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

      // Fail-closed: never perform destructive reset automatically.
      logger.critical(
        "🚨 [progressiveRepair] Max repair attempts exceeded for \(userDID.prefix(20), privacy: .private)"
      )
      throw MLSSQLCipherError.needsUserAction(
        reason: "MLS storage requires manual reset. Use Settings ▸ Diagnostics ▸ Reset MLS Storage."
      )
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
      try await MLSDatabaseCoordinator.shared.performWrite(for: userDID, timeout: 5.0) {
        try await database.writeWithoutTransaction { db in
          try db.execute(sql: "PRAGMA wal_checkpoint(TRUNCATE);")
        }
      }
      let duration = Date().timeIntervalSince(checkpointStart)
      let durationStr = String(format: "%.2f", duration)
      logger.info("✅ [Checkpoint] TRUNCATE completed in \(durationStr)s for user: \(userDID, privacy: .private)")
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

  /// Coordinate writes across App / NSE to avoid WAL/header races
  private func coordinatedWrite<T>(to url: URL, _ block: () throws -> T) throws -> T {
    try ProcessCoordinator.shared.performExclusive {
      let coordinator = NSFileCoordinator(filePresenter: nil)
      var result: Result<T, Error>?
      var coordError: NSError?

      coordinator.coordinate(writingItemAt: url, options: [], error: &coordError) { _ in
        result = Result { try block() }
      }

      if let coordError { throw coordError }
      switch result {
      case .success(let value):
        return value
      case .failure(let error):
        throw error
      case .none:
        throw MLSSQLCipherError.databaseCreationFailed(
          underlying: NSError(
            domain: "MLSGRDBManager",
            code: -8,
            userInfo: [NSLocalizedDescriptionKey: "File coordination failed"]))
      }
    }
  }

  /// Create new encrypted database with GRDB DatabasePool (runs off main thread via actor isolation)
  private func createDatabase(for userDID: String) async throws -> DatabasePool {
    let encryptionKey = try await ensureKeyForDatabase(for: userDID)

    // Get database file path
    let dbPath = databasePath(for: userDID)

    // Configure GRDB with SQLCipher encryption
    var config = Configuration()

    // Enable busyMode for better concurrency handling
    // INCREASED from 5s to 10s for NSE/App contention scenarios
    // The NSE has ~30s execution limit, so 10s gives 3 retry opportunities
    config.busyMode = .timeout(10.0)  // Wait up to 10 seconds for locks

    // Note: defaultTransactionKind is automatically managed by GRDB

    // Enable readonly connections for readers (DatabasePool feature)
    config.readonly = false

    // Set QoS to userInitiated to match main thread priority
    // This prevents priority inversion when UI threads await database operations
    config.qos = .userInitiated

    // CRITICAL FIX: Limit maximum reader connections to prevent memory exhaustion
    // iOS has limited file descriptors and memory; too many concurrent readers
    // can exhaust SQLite's resources causing "out of memory" errors
    // Reduced from 4 to 2 to minimize connection pool pressure on SQLCipher codec
    config.maximumReaderCount = 1

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
      
      // Convert key to hex string for SQLCipher
      // SQLCipher expects: PRAGMA key = "x'hexstring'"
      let hexKey = encryptionKey.map { String(format: "%02x", $0) }.joined()

      // Set encryption key using quoted hex literal format
      try db.execute(sql: "PRAGMA key = \"x'\(hexKey)'\";")

      // Try SQLCipher 4 settings first
      try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
      try db.execute(sql: "PRAGMA kdf_iter = 256000;")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")

      // Test if database can be read with these settings
      do {
        _ = try Int.fetchOne(db, sql: "SELECT 1 FROM sqlite_master LIMIT 1;")
      } catch {
        // If SQLCipher 4 settings fail, try SQLCipher 3 compatibility mode
        // This handles existing databases created with older settings
        try db.execute(sql: "PRAGMA cipher_page_size = 1024;")
        try db.execute(sql: "PRAGMA kdf_iter = 64000;")
        try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA1;")
        try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA1;")

        // Verify it works now
        _ = try Int.fetchOne(db, sql: "SELECT 1 FROM sqlite_master LIMIT 1;")
      }

      // Enable WAL mode for better concurrency (critical for DatabasePool)
      try db.execute(sql: "PRAGMA journal_mode = WAL;")
      // OOM FIX: Reduce checkpoint threshold to prevent WAL file from growing too large
      // (200 pages * 4KB = 800KB threshold before automatic checkpoint)
      // Lower threshold means more frequent checkpoints, reducing memory pressure
      // Reduced from 500 to 200 to be more aggressive about WAL management
      try db.execute(sql: "PRAGMA wal_autocheckpoint = 200;")
      try db.execute(sql: "PRAGMA synchronous = NORMAL;")  // NORMAL is sufficient with WAL

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

      // OOM FIX: Explicitly set busy timeout to handle lock contention gracefully
      // This prevents "database is locked" errors from being misinterpreted as OOM
      // INCREASED from 5s to 10s for NSE/App concurrent access scenarios
      try db.execute(sql: "PRAGMA busy_timeout = 10000;")  // 10 seconds

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
          let code = error.resultCode
          let raw = code.rawValue
          let isHMACOrNotADB = raw == 7 || raw == 21 || raw == 26 || code == .SQLITE_NOTADB
          if isHMACOrNotADB {
            logger.critical("🚨 [MLS] Unable to open encrypted DB (\(raw)) for \(userDID.prefix(20), privacy: .private)")
            throw MLSSQLCipherError.needsUserAction(
              reason: "MLS storage could not be opened. Use Settings ▸ Diagnostics ▸ Reset MLS Storage."
            )
          }
          throw error
        }
      }
    } catch {
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
        t.column("plaintext", .text)
        t.column("embedData", .blob)
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
        t.column("plaintextExpired", .boolean).notNull().defaults(to: false)
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

      try db.create(table: "MLSReportModel") { t in
        t.primaryKey("id", .text).notNull()
        t.column("convo_id", .text).notNull().references("MLSConversationModel", onDelete: .cascade)
        t.column("reporter_did", .text).notNull()
        t.column("reported_did", .text).notNull()
        t.column("reason", .text).notNull()
        t.column("details", .text)
        t.column("status", .text).notNull().defaults(to: "pending")
        t.column("action", .text)
        t.column("resolution_notes", .text)
        t.column("created_at", .datetime).notNull()
        t.column("resolved_at", .datetime)
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

      // MLSReportModel indexes - frequently queried by conversation and status
      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_reports_convo
            ON MLSReportModel(convo_id);
          """)

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_reports_status
            ON MLSReportModel(status);
          """)

      try db.execute(
        sql: """
            CREATE INDEX IF NOT EXISTS idx_reports_convo_status
            ON MLSReportModel(convo_id, status, created_at DESC);
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
      // Add error tracking fields to MLSMessageModel
      try db.execute(
        sql: """
            ALTER TABLE MLSMessageModel
            ADD COLUMN processingError TEXT;
          """)

      try db.execute(
        sql: """
            ALTER TABLE MLSMessageModel
            ADD COLUMN processingAttempts INTEGER NOT NULL DEFAULT 0;
          """)

      try db.execute(
        sql: """
            ALTER TABLE MLSMessageModel
            ADD COLUMN validationFailureReason TEXT;
          """)

      // Add recovery tracking fields to MLSConversationModel
      try db.execute(
        sql: """
            ALTER TABLE MLSConversationModel
            ADD COLUMN lastRecoveryAttempt DATETIME;
          """)

      try db.execute(
        sql: """
            ALTER TABLE MLSConversationModel
            ADD COLUMN consecutiveFailures INTEGER NOT NULL DEFAULT 0;
          """)

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
      // Add membership change tracking to MLSConversationModel
      try db.execute(
        sql: """
            ALTER TABLE MLSConversationModel
            ADD COLUMN lastMembershipChangeAt DATETIME;
          """)

      try db.execute(
        sql: """
            ALTER TABLE MLSConversationModel
            ADD COLUMN unacknowledgedMemberChanges INTEGER NOT NULL DEFAULT 0;
          """)

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
    
    logger.debug("🔍 WAL Health Check for \(userDID.prefix(20)): status=\(status.rawValue), walSize=\(walSize), dbSize=\(dbSize)")
    
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
    let pendingCount = pendingCloseOperations.count
    
    // Count recent force closes (last 5 minutes)
    let fiveMinutesAgo = Date().addingTimeInterval(-300)
    let recentForceCloses = forceClosedPools.filter { $0.timestamp > fiveMinutesAgo }.count
    
    // Determine status
    let status: ConnectionPoolMetrics.PoolStatus
    if recentForceCloses >= 3 {
      status = .exhausted
      logger.error("🚨 Connection pool EXHAUSTED: \(recentForceCloses) force closes in last 5 minutes")
    } else if openCount > 3 || pendingCount > 0 {
      status = .busy
      logger.warning("⚠️ Connection pool busy: \(openCount) open, \(pendingCount) pending close")
    } else {
      status = .healthy
    }
    
    return ConnectionPoolMetrics(
      activeConnections: openCount,
      openDatabaseCount: openCount,
      pendingCloseCount: pendingCount,
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
          logger.debug("✅ Checkpointed database for \(userDID.prefix(20))")
        } catch {
          failedCount += 1
          let errorDesc = error.localizedDescription.lowercased()
          if errorDesc.contains("locked") || errorDesc.contains("busy") {
            logger.debug("⏳ Checkpoint deferred (database busy) for \(userDID.prefix(20))")
          } else {
            logger.warning("⚠️ Checkpoint failed for \(userDID.prefix(20)): \(error.localizedDescription)")
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
        logger.debug("🗑️ Closed inactive database for \(userDID.prefix(20))")
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
