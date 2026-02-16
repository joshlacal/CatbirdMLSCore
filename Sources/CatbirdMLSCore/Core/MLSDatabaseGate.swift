//
//  MLSDatabaseGate.swift
//  CatbirdMLSCore
//
//  ═══════════════════════════════════════════════════════════════════════════
//  Single Gate Architecture for MLS Database Access
//  ═══════════════════════════════════════════════════════════════════════════
//
//  This actor is the SINGLE SOURCE OF TRUTH for database access control.
//  It replaces the 5 separate coordination mechanisms that previously existed:
//
//  REPLACED MECHANISMS:
//  1. pendingCloseOperations (MLSGRDBManager)
//  2. activeUserDID (MLSGRDBManager)
//  3. MLSAdvisoryLockCoordinator (file locks) - REMOVED for 0xdead10cc prevention
//  4. MLSDatabaseAccessController (operation tickets)
//  5. MLSAppActivityState.isShuttingDown (UserDefaults)
//
//  DESIGN PRINCIPLES:
//  - One gate per user
//  - Gate check and connection increment are ATOMIC
//  - Drain waits for ALL connections, not just "tracked" ones
//  - No operation can access database after gate closes
//
//  ═══════════════════════════════════════════════════════════════════════════

import Foundation
import OSLog

// MARK: - Gate State

/// The current state of a user's database gate.
public enum MLSGateState: Sendable, Equatable {
  /// Gate is open - database operations allowed
  case open
  
  /// Gate is closing - no new operations, waiting for drain
  case closing
  
  /// Gate is closed - no operations allowed
  case closed
}

// MARK: - Gate Errors

/// Errors from database gate operations.
public enum MLSGateError: Error, LocalizedError, Sendable {
  /// The gate is closed - operation rejected
  case gateClosed(userDID: String)
  
  /// The gate is closing - operation rejected
  case gateClosing(userDID: String)
  
  /// Drain timed out with active connections
  case drainTimeout(userDID: String, activeConnections: Int)
  
  public var errorDescription: String? {
    switch self {
    case .gateClosed(let userDID):
      return "Database gate closed for user: \(userDID.prefix(16))..."
    case .gateClosing(let userDID):
      return "Database gate closing for user: \(userDID.prefix(16))..."
    case .drainTimeout(let userDID, let count):
      return "Drain timeout for \(userDID.prefix(16))..., \(count) connections still active"
    }
  }
}

// MARK: - Connection Token

/// A token representing an active database connection.
///
/// This is returned by `acquireConnection()` and must be released via `releaseConnection()`.
/// The token includes a generation number to detect stale operations.
public struct MLSConnectionToken: Sendable, Hashable {
  /// Unique identifier for this connection
  public let id: UUID
  
  /// User DID this connection is for
  public let userDID: String
  
  /// Generation number at time of acquisition (for staleness detection)
  public let generation: UInt64
  
  /// When this token was issued
  public let issuedAt: ContinuousClock.Instant
  
  internal init(userDID: String, generation: UInt64) {
    self.id = UUID()
    self.userDID = userDID
    self.generation = generation
    self.issuedAt = ContinuousClock.now
  }
}

// MARK: - Database Gate

/// Single Gate Architecture for MLS database access control.
///
/// This actor provides atomic gate checks with connection counting.
/// It ensures that:
/// 1. No operation can start after the gate begins closing
/// 2. Drain waits for ALL active connections
/// 3. Gate state changes are atomic and visible immediately
///
/// ## Usage
///
/// ```swift
/// // Acquire connection (fails if gate is closed/closing)
/// let token = try await MLSDatabaseGate.shared.acquireConnection(for: userDID)
/// defer { await MLSDatabaseGate.shared.releaseConnection(token) }
///
/// // Use database...
/// ```
///
/// ## Shutdown Flow
///
/// ```swift
/// // 1. Close gate and wait for drain
/// try await MLSDatabaseGate.shared.closeGateAndDrain(for: userDID, timeout: .seconds(5))
///
/// // 2. Safe to checkpoint and close database
/// await MLSGRDBManager.shared.closeDatabaseAndDrain(for: userDID)
///
/// // 3. Reopen gate for next login
/// await MLSDatabaseGate.shared.openGate(for: userDID)
/// ```
public actor MLSDatabaseGate {
  
  // MARK: - Singleton
  
  public static let shared = MLSDatabaseGate()
  
  // MARK: - Properties
  
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "DatabaseGate")
  
  /// Gate state per user
  private var gateStates: [String: MLSGateState] = [:]
  
  /// Active connection count per user
  private var activeConnections: [String: Int] = [:]
  
  /// Generation counter per user (increments on each close cycle)
  private var generations: [String: UInt64] = [:]
  
  /// Continuations waiting for drain to complete
  private var drainContinuations: [String: [CheckedContinuation<Void, Never>]] = [:]
  
  // MARK: - Initialization
  
  private init() {
    logger.info("🚪 [Gate] MLSDatabaseGate initialized")
  }
  
  // MARK: - Gate Control
  
  /// Open the gate for a user.
  ///
  /// Call this when a user logs in or after database recovery.
  /// If the gate was previously closed, this also increments the generation.
  ///
  /// - Parameter userDID: User's decentralized identifier
  public func openGate(for userDID: String) {
    let previousState = gateStates[userDID] ?? .closed
    gateStates[userDID] = .open
    
    if previousState == .closed {
      // Increment generation so stale operations fail
      generations[userDID, default: 0] += 1
    }
    
    let gen = generations[userDID] ?? 1
    logger.info("🚪 [Gate] OPENED for user: \(userDID.prefix(16), privacy: .private)... (gen=\(gen))")
  }
  
  /// Close the gate and wait for all active connections to drain.
  ///
  /// This method:
  /// 1. Sets gate to .closing (rejects new connections immediately)
  /// 2. Waits for active connections to reach 0
  /// 3. Sets gate to .closed
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - timeout: Maximum time to wait for drain
  /// - Throws: `MLSGateError.drainTimeout` if connections don't drain in time
  public func closeGateAndDrain(for userDID: String, timeout: Duration = .seconds(5)) async throws {
    let currentState = gateStates[userDID] ?? .closed
    
    // Already closed? Nothing to do
    if currentState == .closed {
      logger.debug("🚪 [Gate] Already closed for: \(userDID.prefix(16), privacy: .private)...")
      return
    }
    
    // Already closing? Wait for existing drain
    if currentState == .closing {
      logger.debug("🚪 [Gate] Already closing, waiting for drain: \(userDID.prefix(16), privacy: .private)...")
      try await waitForDrain(for: userDID, timeout: timeout)
      return
    }
    
    // Begin closing
    gateStates[userDID] = .closing
    let activeCount = activeConnections[userDID] ?? 0
    logger.info("🚪 [Gate] CLOSING for user: \(userDID.prefix(16), privacy: .private)... (\(activeCount) active connections)")
    
    // Wait for drain
    try await waitForDrain(for: userDID, timeout: timeout)
    
    // Mark as closed
    gateStates[userDID] = .closed
    logger.info("🚪 [Gate] CLOSED for user: \(userDID.prefix(16), privacy: .private)...")
  }
  
  /// Force close the gate without waiting for drain.
  ///
  /// Use this only as a last resort when drain times out.
  ///
  /// - Parameter userDID: User's decentralized identifier
  public func forceCloseGate(for userDID: String) {
    let activeCount = activeConnections[userDID] ?? 0
    gateStates[userDID] = .closed
    
    // Clear connection count
    activeConnections.removeValue(forKey: userDID)
    
    // Resume any waiting drain continuations
    if let continuations = drainContinuations.removeValue(forKey: userDID) {
      for continuation in continuations {
        continuation.resume()
      }
    }
    
    logger.warning("🚪 [Gate] FORCE CLOSED for: \(userDID.prefix(16), privacy: .private)... (abandoned \(activeCount) connections)")
  }
  
  // MARK: - Connection Lifecycle
  
  /// Acquire a connection token (fails if gate is not open).
  ///
  /// The caller MUST call `releaseConnection()` when done.
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: A connection token
  /// - Throws: `MLSGateError` if gate is closed or closing
  public func acquireConnection(for userDID: String) throws -> MLSConnectionToken {
    let state = gateStates[userDID] ?? .closed
    
    switch state {
    case .closed:
      throw MLSGateError.gateClosed(userDID: userDID)
    case .closing:
      throw MLSGateError.gateClosing(userDID: userDID)
    case .open:
      break
    }
    
    // Atomically increment connection count and create token
    activeConnections[userDID, default: 0] += 1
    let gen = generations[userDID] ?? 1
    let token = MLSConnectionToken(userDID: userDID, generation: gen)
    
    let count = activeConnections[userDID] ?? 0
    logger.debug("📈 [Gate] Connection acquired (count=\(count)) for: \(userDID.prefix(16), privacy: .private)...")
    
    return token
  }
  
  /// Release a connection token.
  ///
  /// - Parameter token: The token from `acquireConnection()`
  public func releaseConnection(_ token: MLSConnectionToken) {
    guard var count = activeConnections[token.userDID], count > 0 else {
      logger.warning("⚠️ [Gate] Release with no active connections: \(token.userDID.prefix(16), privacy: .private)...")
      return
    }
    
    count -= 1
    
    if count == 0 {
      activeConnections.removeValue(forKey: token.userDID)
      
      // Signal drain if gate is closing
      if gateStates[token.userDID] == .closing {
        if let continuations = drainContinuations.removeValue(forKey: token.userDID) {
          logger.info("✅ [Gate] All connections drained for: \(token.userDID.prefix(16), privacy: .private)...")
          for continuation in continuations {
            continuation.resume()
          }
        }
      }
    } else {
      activeConnections[token.userDID] = count
    }
    
    logger.debug("📉 [Gate] Connection released (remaining=\(count)) for: \(token.userDID.prefix(16), privacy: .private)...")
  }
  
  // MARK: - Queries
  
  /// Check if the gate is open for a user.
  public func isOpen(for userDID: String) -> Bool {
    gateStates[userDID] == .open
  }
  
  /// Get the current gate state for a user.
  public func gateState(for userDID: String) -> MLSGateState {
    gateStates[userDID] ?? .closed
  }
  
  /// Get the current generation for a user.
  public func generation(for userDID: String) -> UInt64 {
    generations[userDID] ?? 0
  }
  
  /// Get the active connection count for a user.
  public func connectionCount(for userDID: String) -> Int {
    activeConnections[userDID] ?? 0
  }
  
  /// Validate that a token is still valid (correct generation).
  public func isTokenValid(_ token: MLSConnectionToken) -> Bool {
    guard gateStates[token.userDID] == .open else { return false }
    guard let currentGen = generations[token.userDID] else { return false }
    return token.generation == currentGen
  }
  
  // MARK: - Private Helpers
  
  private func waitForDrain(for userDID: String, timeout: Duration) async throws {
    // Fast path: already drained
    guard let count = activeConnections[userDID], count > 0 else {
      logger.debug("⚡ [Gate] No connections to drain for: \(userDID.prefix(16), privacy: .private)...")
      return
    }
    
    logger.info("⏳ [Gate] Waiting for \(count) connections to drain: \(userDID.prefix(16), privacy: .private)...")
    
    // Create drain task
    let drainTask = Task {
      await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
        // Re-check in case connections drained between check and now
        if (activeConnections[userDID] ?? 0) == 0 {
          continuation.resume()
        } else {
          drainContinuations[userDID, default: []].append(continuation)
        }
      }
    }
    
    // Race against timeout
    let result = await withTaskGroup(of: Bool.self) { group in
      group.addTask {
        await drainTask.value
        return true
      }
      
      group.addTask {
        try? await Task<Never, Never>.sleep(for: timeout)
        return false
      }
      
      let first = await group.next() ?? false
      
      // If timeout wins, ensure the drain waiter can complete; otherwise the task group will hang
      // waiting for the cancelled drain wait task.
      if !first {
        drainTask.cancel()
        if let orphanedContinuations = drainContinuations.removeValue(forKey: userDID) {
          logger.debug("🧹 [Gate] Resuming \(orphanedContinuations.count) orphaned drain continuations")
          for continuation in orphanedContinuations {
            continuation.resume()
          }
        }
      }
      group.cancelAll()
      return first
    }
    
    if !result {
      drainTask.cancel()

      let remaining = activeConnections[userDID] ?? 0
      logger.warning("⏱️ [Gate] Drain timeout, \(remaining) connections remaining: \(userDID.prefix(16), privacy: .private)...")
      throw MLSGateError.drainTimeout(userDID: userDID, activeConnections: remaining)
    }
  }
}

// MARK: - Convenience Extensions

extension MLSDatabaseGate {
  
  /// Execute work with automatic connection management.
  ///
  /// This is the preferred way to access the database as it handles
  /// token acquisition and release automatically.
  ///
  /// - Parameters:
  ///   - userDID: User's decentralized identifier
  ///   - work: The work to perform
  /// - Returns: The result of the work
  /// - Throws: `MLSGateError` if gate is not open, or any error from work
  public func withConnection<T>(
    for userDID: String,
    _ work: @Sendable () async throws -> T
  ) async throws -> T {
    let token = try acquireConnection(for: userDID)
    
    do {
      let result = try await work()
      releaseConnection(token)
      return result
    } catch {
      releaseConnection(token)
      throw error
    }
  }
}
