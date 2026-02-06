//
//  MLSShutdownCoordinator.swift
//  CatbirdMLSCore
//
//  Centralized shutdown coordinator for MLS state management.
//
//  ═══════════════════════════════════════════════════════════════════════════
//  CRITICAL: Correct Shutdown Sequence for Account Switching
//  ═══════════════════════════════════════════════════════════════════════════
//
//  Problem: When switching accounts, the following components must be closed
//  in a STRICT ORDER to prevent SQLite error 21 (out of memory) and state desync:
//
//  1. Swift Concurrency tasks advancing the ratchet
//  2. Rust FFI context holding SQLite handles
//  3. Swift GRDB database pool
//  4. OS memory locks (mlock pages from SQLCipher)
//
//  If closed in wrong order, the new user's database open races with:
//  - Old user's uncommitted WAL data
//  - Old user's mlocked memory pages
//  - Old user's Rust FFI file handles
//
//  Solution: This coordinator enforces the correct shutdown sequence:
//
//  1. **STOP** all new operations (exclusive lock)
//  2. **WAIT** for current operations to finish (drain)
//  3. **CLOSE FFI** (Rust `flushAndPrepareClose`) - releases Rust SQLite handles
//  4. **CHECKPOINT WAL** (Swift `WAL_CHECKPOINT(PASSIVE)`) - flush pending writes
//  5. **CLOSE SWIFT DB** (`closeDatabaseAndDrain`) - close GRDB pool
//  6. **SLEEP 200ms** (the "Magic Sleep") - let OS reclaim mlocked memory
//  7. **SIGNAL SAFE** - return success
//
//  ═══════════════════════════════════════════════════════════════════════════
//  Memory Pressure Detection (Replaces "Magic Sleep")
//  ═══════════════════════════════════════════════════════════════════════════
//
//  SQLCipher uses `mlock()` to prevent encryption keys from being swapped to disk.
//  On iOS, these locked memory pages aren't immediately reclaimed when the database
//  is closed. If we immediately open a new database, iOS may report "out of memory"
//  (SQLite error 7) because the old user's mlocked pages haven't been released yet.
//
//  Instead of a fixed 200ms delay, we now use memory pressure detection to verify
//  that sufficient memory is available before allowing a new database to open.
//  This is more reliable across different devices and memory pressure scenarios.
//
//  ═══════════════════════════════════════════════════════════════════════════
//  5. MLSAppActivityState (Cross-process shutdown flag)
//
//  ═══════════════════════════════════════════════════════════════════════════

import Foundation
import OSLog

/// Result of a shutdown operation
public enum MLSShutdownResult: Sendable {
  /// Shutdown completed successfully within timeout
  case success(durationMs: Int)
  
  /// Shutdown completed but some steps had warnings
  case successWithWarnings(durationMs: Int, warnings: [String])
  
  /// Shutdown timed out but proceeded anyway
  case timedOut(durationMs: Int, phase: MLSShutdownPhase)
  
  /// Shutdown failed completely
  case failed(Error)
  
  public var wasSuccessful: Bool {
    switch self {
    case .success, .successWithWarnings:
      return true
    case .timedOut, .failed:
      return false
    }
  }
}

/// Phases of the shutdown sequence (for timeout reporting)
public enum MLSShutdownPhase: String, Sendable {
  case acquiringLock = "acquiring_lock"
  case drainOperations = "drain_operations"
  case closingFFI = "closing_ffi"
  case checkpointingWAL = "checkpointing_wal"
  case closingSwiftDB = "closing_swift_db"
  case memoryReclaim = "memory_reclaim"
  case complete = "complete"
}

/// Centralized coordinator for MLS shutdown operations.
///
/// This coordinator enforces the correct shutdown sequence to prevent
/// SQLite errors during account switching. All shutdown operations should
/// go through this coordinator rather than calling individual components directly.
///
/// Usage:
/// ```swift
/// let result = await MLSShutdownCoordinator.shared.shutdown(for: userDID)
/// if !result.wasSuccessful {
///     logger.warning("Shutdown had issues but proceeding anyway")
/// }
/// ```
public actor MLSShutdownCoordinator {
  
  // MARK: - Singleton
  
  public static let shared = MLSShutdownCoordinator()
  
  // MARK: - Properties
  
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "ShutdownCoordinator")
  
  /// Memory threshold for SQLCipher (50MB should be sufficient)
  private let memoryThreshold: UInt64 = 50_000_000

  /// Maximum time to wait for memory reclamation
  private let memoryReclaimTimeout: Duration = .seconds(2)

  /// Default timeout for the entire shutdown sequence
  private let defaultTimeout: TimeInterval = 10.0

  /// Timeout for individual phases
  private let phaseTimeouts: [MLSShutdownPhase: TimeInterval] = [
    .acquiringLock: 2.0,
    .drainOperations: 5.0,
    .closingFFI: 2.0,
    .checkpointingWAL: 3.0,
    .closingSwiftDB: 3.0,
    .memoryReclaim: 2.0  // Memory pressure detection timeout
  ]
  
  /// Track users currently being shut down to prevent concurrent shutdowns
  private var activeShutdowns: Set<String> = []

  /// Public accessor to check if any shutdown is in progress
  public var isShuttingDown: Bool {
    !activeShutdowns.isEmpty
  }
  
  // MARK: - Initialization
  
  private init() {
    logger.debug("MLSShutdownCoordinator initialized")
  }
  
  // MARK: - Public API
  
  /// Perform a complete, ordered shutdown of all MLS resources for a user.
  ///
  /// This is the **only** method that should be used to shut down MLS state
  /// during account switching. It enforces the correct sequence to prevent
  /// SQLite errors and state corruption.
  ///
  /// The shutdown sequence is:
  /// 1. Stop all new operations (exclusive lock)
  /// 2. Wait for current operations to finish (drain)
  /// 3. Close FFI context (release Rust SQLite handles)
  /// 4. Checkpoint WAL (flush pending writes)
  /// 5. Close Swift database (close GRDB pool)
  /// 6. Sleep 200ms (let OS reclaim mlocked memory)
  ///
  /// - Parameters:
  ///   - userDID: The user's DID to shut down
  ///   - databaseManager: The database manager instance to shut down
  ///   - timeout: Maximum time for the entire shutdown (default: 10s)
  /// - Returns: Result indicating success, warnings, or failure
  public func shutdown(
    for userDID: String,
    databaseManager: MLSGRDBManager,
    timeout: TimeInterval? = nil
  ) async -> MLSShutdownResult {
    let effectiveTimeout = timeout ?? defaultTimeout
    let startTime = ContinuousClock.now
    
    logger.info("🛑 [SHUTDOWN] Starting ordered shutdown for user: \(userDID.prefix(20), privacy: .private)...")
    
    // Prevent concurrent shutdowns for the same user
    guard !activeShutdowns.contains(userDID) else {
      logger.warning("⚠️ [SHUTDOWN] Already shutting down user: \(userDID.prefix(20), privacy: .private)")
      return .successWithWarnings(durationMs: 0, warnings: ["Shutdown already in progress"])
    }
    activeShutdowns.insert(userDID)
    defer {
      activeShutdowns.remove(userDID)
      MLSCoordinationStore.shared.updatePhase(.active)
      // Always ensure shutdown flag is cleared on exit
      Task { await self.signalShutdownComplete(for: userDID) }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 0: STOP THE WORLD - Atomically reject new operations
    // ═══════════════════════════════════════════════════════════════════════════
    // This is the critical first step: once called, no new database operations
    // can start. Existing operations will complete normally.
    //
    // SINGLE GATE ARCHITECTURE: The gate is now the PRIMARY shutdown mechanism.
    // We close it FIRST to atomically block all new getDatabasePool calls.
    // ═══════════════════════════════════════════════════════════════════════════
    logger.info("🛑 [SHUTDOWN] Phase 0: Stopping new operations...")
    
    // CRITICAL: Close the gate FIRST - this atomically blocks all getDatabasePool calls
    do {
      try await MLSDatabaseGate.shared.closeGateAndDrain(for: userDID, timeout: .seconds(5))
      logger.info("🚪 [SHUTDOWN] Gate closed and connections drained")
    } catch let error as MLSGateError {
      switch error {
      case .drainTimeout(_, let count):
        logger.warning("⚠️ [SHUTDOWN] Gate drain timeout with \(count) connections - forcing close")
        await MLSDatabaseGate.shared.forceCloseGate(for: userDID)
      default:
        logger.warning("⚠️ [SHUTDOWN] Gate close error: \(error.localizedDescription)")
      }
    } catch {
      logger.warning("⚠️ [SHUTDOWN] Unexpected gate error: \(error.localizedDescription)")
    }
    
    // Signal cross-process shutdown (NSE will yield)
    signalShutdownStarting(for: userDID)
    await requestNSEStop(for: userDID)

    MLSCoordinationStore.shared.updatePhase(.switching)
    MLSCoordinationStore.shared.incrementGeneration(for: userDID)
    logger.info("✅ [SHUTDOWN] Phase 0 complete - new operations blocked")
    
    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 1: Acquire exclusive access (stop new operations)
    // ═══════════════════════════════════════════════════════════════════════════
    logger.info("🔒 [SHUTDOWN] Phase 1: Acquiring exclusive access...")

    do {
      return try await withMLSExclusiveAccess(
        userDID: userDID,
        purpose: .accountSwitch,
        timeout: .seconds(Int64(effectiveTimeout))
      ) { [self] in
        return await performShutdownSequence(
          userDID: userDID,
          databaseManager: databaseManager,
          startTime: startTime,
          hadExclusiveLock: true
        )
      }
    } catch is MLSExclusiveAccessError {
      logger.warning("⏱️ [SHUTDOWN] Timed out acquiring exclusive access")
      
      // Even if we can't get exclusive access, try to proceed with shutdown
      // This is a "fail-soft" approach - better to have some cleanup than none
      logger.warning("⚠️ [SHUTDOWN] Proceeding with best-effort shutdown (no exclusive lock)")
      
      return await performShutdownSequence(
        userDID: userDID,
        databaseManager: databaseManager,
        startTime: startTime,
        hadExclusiveLock: false
      )
    } catch {
      logger.error("❌ [SHUTDOWN] Failed: \(error.localizedDescription)")
      return .failed(error)
    }
  }
  
  // MARK: - Private Implementation
  
  private func performShutdownSequence(
    userDID: String,
    databaseManager: MLSGRDBManager,
    startTime: ContinuousClock.Instant,
    hadExclusiveLock: Bool
  ) async -> MLSShutdownResult {
    var warnings: [String] = []
    
    if !hadExclusiveLock {
      warnings.append("Could not acquire exclusive lock - proceeding anyway")
    }
    

    
    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 3: Close FFI context (CRITICAL: Before Swift DB)
    // ═══════════════════════════════════════════════════════════════════════════
    // The Rust FFI context holds its own SQLite connection. If we close the Swift
    // GRDB pool first, Rust may still have an open handle, causing WAL conflicts.
    //
    // NOTE: We call MLSCoreContext.shared.removeContext() which handles the core
    // package's context cache. The app layer (MLSClient) may have its own cache
    // that it manages separately before calling this coordinator.
    // ═══════════════════════════════════════════════════════════════════════════
    logger.info("🦀 [SHUTDOWN] Phase 3: Closing FFI context (CatbirdMLSCore)...")
    
      await MLSCoreContext.shared.removeContext(for: userDID)
    logger.info("✅ [SHUTDOWN] FFI context closed (CatbirdMLSCore)")
    
    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 4: Checkpoint WAL (flush pending writes to main DB file)
    // ═══════════════════════════════════════════════════════════════════════════
    // Use PASSIVE mode which doesn't block other connections. If there are still
    // active readers, they can continue - we just want to flush what we can.
    // ═══════════════════════════════════════════════════════════════════════════
    logger.info("📝 [SHUTDOWN] Phase 4: Checkpointing WAL...")

    do {
      try await databaseManager.checkpointDatabase(for: userDID)
      logger.info("✅ [SHUTDOWN] WAL checkpoint completed")
    } catch {
      // Checkpoint failure is not fatal - proceed with shutdown
      logger.warning("⚠️ [SHUTDOWN] WAL checkpoint failed: \(error.localizedDescription)")
      warnings.append("WAL checkpoint failed: \(error.localizedDescription)")
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 5: Close Swift GRDB database pool
    // ═══════════════════════════════════════════════════════════════════════════
    logger.info("📀 [SHUTDOWN] Phase 5: Closing Swift database pool...")

    let closeTimeout = phaseTimeouts[.closingSwiftDB] ?? 3.0
    let closeSuccess = await databaseManager.closeDatabaseAndDrain(
      for: userDID,
      timeout: closeTimeout
    )
    
    if closeSuccess {
      logger.info("✅ [SHUTDOWN] Swift database closed")
    } else {
      logger.warning("⚠️ [SHUTDOWN] Swift database close timed out")
      warnings.append("Database close timed out")
    }
    
    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 6: Memory pressure detection (replaces "Magic Sleep")
    // ═══════════════════════════════════════════════════════════════════════════
    // This is CRITICAL for preventing SQLite error 7 during account switching.
    // SQLCipher uses mlock() to protect encryption keys. When we close a database,
    // the munlock() calls don't immediately release the memory pages. If we open
    // a new database too quickly, iOS may report "out of memory" because the old
    // pages haven't been fully released yet.
    //
    // Instead of a fixed delay, we now poll for available memory or attempt a
    // test database connection to verify memory is available.
    // ═══════════════════════════════════════════════════════════════════════════
    logger.info("🧠 [SHUTDOWN] Phase 6: Waiting for memory reclamation...")

    await waitForMemoryReclamation(timeout: memoryReclaimTimeout)

    logger.info("✅ [SHUTDOWN] Memory reclamation verified")

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 7: Release shutdown lock (allow new operations)
    // ═══════════════════════════════════════════════════════════════════════════
    await signalShutdownComplete(for: userDID)
    
    // Calculate total duration
    let elapsed = startTime.duration(to: ContinuousClock.now)
    let durationMs = Int(elapsed.components.seconds * 1000 + elapsed.components.attoseconds / 1_000_000_000_000_000)
    
    if warnings.isEmpty {
      logger.info("✅ [SHUTDOWN] Complete in \(durationMs)ms - safe to switch accounts")
      MLSCoordinationStore.shared.updatePhase(.closed)
      return .success(durationMs: durationMs)
    } else {
      logger.warning("⚠️ [SHUTDOWN] Complete in \(durationMs)ms with \(warnings.count) warning(s)")
      return .successWithWarnings(durationMs: durationMs, warnings: warnings)
    }
  }
  
  /// Perform a quick shutdown for the Notification Service Extension.
  ///
  /// The NSE has a limited execution window (~30 seconds) and may be terminated
  /// at any time. This method performs a faster shutdown that prioritizes
  /// flushing the ratchet state to disk over complete cleanup.
  ///
  /// - Parameters:
  ///   - userDID: The user's DID to shut down
  ///   - databaseManager: The database manager instance to shut down
  /// - Returns: Result indicating success or failure
  public func quickShutdownForNSE(for userDID: String, databaseManager: MLSGRDBManager) async -> MLSShutdownResult {
    let startTime = ContinuousClock.now
    var warnings: [String] = []
    
    logger.info("⚡ [NSE-SHUTDOWN] Quick shutdown for: \(userDID.prefix(20), privacy: .private)...")
    
    // ═══════════════════════════════════════════════════════════════════════════
    // NSE Shutdown Sequence (optimized for speed)
    // ═══════════════════════════════════════════════════════════════════════════
    // 1. Flush FFI state to disk (critical - saves ratchet advancement)
    // 2. Close FFI context
    // 3. Checkpoint WAL (best-effort, non-blocking)
    // 4. Skip memory reclaim delay (NSE is being terminated anyway)
    // ═══════════════════════════════════════════════════════════════════════════
    
    // Phase 1: Flush and close FFI (critical for ratchet state)
      await MLSCoreContext.shared.removeContext(for: userDID)
    logger.info("✅ [NSE-SHUTDOWN] FFI context flushed and closed")
    
    // Phase 2: Best-effort WAL checkpoint (non-blocking)
    do {
      try await databaseManager.checkpointDatabase(for: userDID)
      logger.info("✅ [NSE-SHUTDOWN] WAL checkpoint completed")
    } catch {
      logger.warning("⚠️ [NSE-SHUTDOWN] WAL checkpoint skipped: \(error.localizedDescription)")
      warnings.append("WAL checkpoint skipped")
    }

    // Phase 3: Release database (don't wait for drain - NSE is being terminated)
    _ = await databaseManager.releaseConnectionWithoutCheckpoint(for: userDID)
    
    // Calculate duration
    let elapsed = startTime.duration(to: ContinuousClock.now)
    let durationMs = Int(elapsed.components.seconds * 1000 + elapsed.components.attoseconds / 1_000_000_000_000_000)
    
    if warnings.isEmpty {
      logger.info("✅ [NSE-SHUTDOWN] Complete in \(durationMs)ms")
      return .success(durationMs: durationMs)
    } else {
      logger.warning("⚠️ [NSE-SHUTDOWN] Complete in \(durationMs)ms with warnings")
      return .successWithWarnings(durationMs: durationMs, warnings: warnings)
    }
  }
  
  /// Signal that the app is preparing to shut down for a user.
  ///
  /// This posts a cross-process notification so the NSE knows to yield
  /// database access to the main app during account switching.
  ///
  /// - Parameter userDID: The user's DID being shut down
  public func signalShutdownStarting(for userDID: String) {
    logger.info("📢 [SHUTDOWN] Signaling shutdown starting for: \(userDID.prefix(20), privacy: .private)...")
    MLSAppActivityState.setShuttingDown(true, userDID: userDID)
  }
  
  /// Signal that shutdown is complete for a user.
  ///
  /// This clears the cross-process shutdown flag.
  ///
  /// - Parameter userDID: The user's DID that was shut down
  public func signalShutdownComplete(for userDID: String) {
    logger.info("📢 [SHUTDOWN] Signaling shutdown complete for: \(userDID.prefix(20), privacy: .private)...")
    MLSAppActivityState.setShuttingDown(false, userDID: nil)
  }

  /// Ask the NSE to release database handles before destructive operations.
  private func requestNSEStop(for userDID: String) async {
    logger.info("📢 [SHUTDOWN] Requesting NSE stop for: \(userDID.prefix(20), privacy: .private)...")
    MLSStateChangeNotifier.postNSEStop()
    try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms grace period
  }

  // MARK: - Memory Pressure Detection

  /// Wait for memory to be reclaimed after database close.
  ///
  /// Uses `os_proc_available_memory()` to verify that sufficient memory is available
  /// for opening a new SQLCipher database. This replaces the fixed 200ms "magic sleep"
  /// with actual memory availability detection.
  ///
  /// - Parameter timeout: Maximum time to wait for memory reclamation
  private func waitForMemoryReclamation(timeout: Duration) async {
    let start = ContinuousClock.now
    let clock = ContinuousClock()

    // First, try a quick check - memory might already be available
    if hasAvailableMemory() {
      logger.debug("⚡ [SHUTDOWN] Memory immediately available")
      return
    }

    // Poll until memory is available or timeout
    while clock.now - start < timeout {
      // Give the OS a moment to reclaim memory
      try? await Task.sleep(for: .milliseconds(50))

      if hasAvailableMemory() {
        let elapsed = start.duration(to: clock.now)
        logger.info("✅ [SHUTDOWN] Memory reclaimed after \(String(describing: elapsed))")
        return
      }
    }

    // Timeout - log warning but proceed anyway (may still work)
    logger.warning("⚠️ [SHUTDOWN] Memory reclaim timeout - proceeding anyway")
  }

  /// Check if sufficient memory is available for SQLCipher operations.
  ///
  /// Uses Darwin's `os_proc_available_memory()` to get available memory in bytes.
  /// SQLCipher typically needs ~10-20MB for its mlock'd pages, so we use a 50MB
  /// threshold to be safe.
  private nonisolated func hasAvailableMemory() -> Bool {
    #if os(iOS) || os(tvOS) || os(watchOS)
    // os_proc_available_memory() returns the number of bytes available
    // before the OS would start killing processes
    let available = os_proc_available_memory()
    return available > memoryThreshold
    #else
    // os_proc_available_memory() is not available on macOS
    // Memory pressure is less of a concern on desktop, so always return true
    return true
    #endif
  }
}
