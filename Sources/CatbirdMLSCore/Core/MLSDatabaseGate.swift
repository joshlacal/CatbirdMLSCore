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
  
  /// User DID this connection is for (canonical lowercase)
  public let userDID: String
  
  /// Generation number at time of acquisition (for staleness detection)
  public let generation: UInt64
  
  /// When this token was issued
  public let issuedAt: ContinuousClock.Instant
  
  internal init(userDID: String, generation: UInt64) {
    self.id = UUID()
    self.userDID = MLSStoragePaths.normalizeDID(userDID)
    self.generation = generation
    self.issuedAt = ContinuousClock.now
  }
}

// MARK: - Database Gate

/// Single Gate Architecture for MLS database access control.
public actor MLSDatabaseGate {
  
  // MARK: - Singleton
  
  public static let shared = MLSDatabaseGate()
  
  // MARK: - Properties
  
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "DatabaseGate")
  
  /// Gate state per user (keyed by normalized DID)
  private var gateStates: [String: MLSGateState] = [:]
  
  /// Active tokens per user (keyed by normalized DID). The set count is the live
  /// connection count; no separate counter is kept so the two cannot diverge.
  private var activeTokens: [String: Set<UUID>] = [:]
  
  /// Generation counter per user (increments on each close cycle, keyed by normalized DID)
  private var generations: [String: UInt64] = [:]
  
  /// Continuations waiting for drain to complete (keyed by normalized DID)
  private var drainContinuations: [String: [CheckedContinuation<Void, Never>]] = [:]
  
  // MARK: - Initialization
  
  private init() {
    logger.info("🚪 [Gate] MLSDatabaseGate initialized")
  }
  
  // MARK: - Gate Control
  
  /// Open the gate for a user.
  public func openGate(for userDID: String) {
    let normalized = MLSStoragePaths.normalizeDID(userDID)
    let previousState = gateStates[normalized] ?? .closed
    gateStates[normalized] = .open
    
    if previousState == .closed {
      generations[normalized, default: 0] += 1
    }
    
    let gen = generations[normalized] ?? 1
    logger.info("🚪 [Gate] OPENED for user: \(normalized.prefix(16), privacy: .private)... (gen=\(gen))")
  }
  
  /// Close the gate and wait for all active connections to drain.
  public func closeGateAndDrain(for userDID: String, timeout: Duration = .seconds(5)) async throws {
    let normalized = MLSStoragePaths.normalizeDID(userDID)
    let currentState = gateStates[normalized] ?? .closed
    
    if currentState == .closed {
      logger.debug("🚪 [Gate] Already closed for: \(normalized.prefix(16), privacy: .private)...")
      return
    }
    
    if currentState == .closing {
      logger.debug("🚪 [Gate] Already closing, waiting for drain: \(normalized.prefix(16), privacy: .private)...")
      try await waitForDrain(for: normalized, timeout: timeout)
      return
    }
    
    gateStates[normalized] = .closing
    let activeCount = activeTokens[normalized]?.count ?? 0
    logger.info("🚪 [Gate] CLOSING for user: \(normalized.prefix(16), privacy: .private)... (\(activeCount) active connections)")
    
    try await waitForDrain(for: normalized, timeout: timeout)
    
    gateStates[normalized] = .closed
    logger.info("🚪 [Gate] CLOSED for user: \(normalized.prefix(16), privacy: .private)...")
  }
  
  /// Force close the gate without waiting for drain.
  public func forceCloseGate(for userDID: String) {
    let normalized = MLSStoragePaths.normalizeDID(userDID)
    let activeCount = activeTokens[normalized]?.count ?? 0
    gateStates[normalized] = .closed
    
    activeTokens.removeValue(forKey: normalized)
    
    if let continuations = drainContinuations.removeValue(forKey: normalized) {
      for continuation in continuations {
        continuation.resume()
      }
    }
    
    logger.warning("🚪 [Gate] FORCE CLOSED for: \(normalized.prefix(16), privacy: .private)... (abandoned \(activeCount) connections)")
  }
  
  // MARK: - Connection Lifecycle
  
  /// Acquire a connection token (fails if gate is not open).
  public func acquireConnection(for userDID: String) throws -> MLSConnectionToken {
    let normalized = MLSStoragePaths.normalizeDID(userDID)
    let state = gateStates[normalized] ?? .closed
    
    switch state {
    case .closed:
      throw MLSGateError.gateClosed(userDID: normalized)
    case .closing:
      throw MLSGateError.gateClosing(userDID: normalized)
    case .open:
      break
    }
    
    let gen = generations[normalized] ?? 1
    let token = MLSConnectionToken(userDID: normalized, generation: gen)
    
    activeTokens[normalized, default: []].insert(token.id)
    let count = activeTokens[normalized]?.count ?? 1
    
    logger.debug("📈 [Gate] Connection acquired (count=\(count)) for: \(normalized.prefix(16), privacy: .private)...")
    
    return token
  }
  
  /// Release a connection token conditionally on exact token presence.
  public func releaseConnection(_ token: MLSConnectionToken) {
    let normalized = token.userDID  // tokens are minted with a canonical DID
    
    guard var tokenSet = activeTokens[normalized], tokenSet.contains(token.id) else {
      logger.debug("⚠️ [Gate] Release ignored for untracked/stale token: \(token.id) user: \(normalized.prefix(16), privacy: .private)...")
      return
    }
    
    tokenSet.remove(token.id)
    
    if tokenSet.isEmpty {
      activeTokens.removeValue(forKey: normalized)
      
      if gateStates[normalized] == .closing {
        if let continuations = drainContinuations.removeValue(forKey: normalized) {
          logger.info("✅ [Gate] All connections drained for: \(normalized.prefix(16), privacy: .private)...")
          for continuation in continuations {
            continuation.resume()
          }
        }
      }
    } else {
      activeTokens[normalized] = tokenSet
    }
    
    let remaining = activeTokens[normalized]?.count ?? 0
    logger.debug("📉 [Gate] Connection released (remaining=\(remaining)) for: \(normalized.prefix(16), privacy: .private)...")
  }
  
  // MARK: - Queries
  
  public func isOpen(for userDID: String) -> Bool {
    let normalized = MLSStoragePaths.normalizeDID(userDID)
    return gateStates[normalized] == .open
  }
  
  public func gateState(for userDID: String) -> MLSGateState {
    let normalized = MLSStoragePaths.normalizeDID(userDID)
    return gateStates[normalized] ?? .closed
  }
  
  public func generation(for userDID: String) -> UInt64 {
    let normalized = MLSStoragePaths.normalizeDID(userDID)
    return generations[normalized] ?? 0
  }
  
  public func connectionCount(for userDID: String) -> Int {
    let normalized = MLSStoragePaths.normalizeDID(userDID)
    return activeTokens[normalized]?.count ?? 0
  }
  
  public func isTokenValid(_ token: MLSConnectionToken) -> Bool {
    let normalized = token.userDID  // tokens are minted with a canonical DID
    guard gateStates[normalized] == .open else { return false }
    guard let currentGen = generations[normalized] else { return false }
    return token.generation == currentGen && (activeTokens[normalized]?.contains(token.id) == true)
  }
  
  // MARK: - Private Helpers
  
  private func waitForDrain(for userDID: String, timeout: Duration) async throws {
    let normalized = MLSStoragePaths.normalizeDID(userDID)
    let count = activeTokens[normalized]?.count ?? 0
    guard count > 0 else {
      logger.debug("⚡ [Gate] No connections to drain for: \(normalized.prefix(16), privacy: .private)...")
      return
    }
    
    logger.info("⏳ [Gate] Waiting for \(count) connections to drain: \(normalized.prefix(16), privacy: .private)...")
    
    let drainTask = Task {
      await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
        if (self.activeTokens[normalized]?.count ?? 0) == 0 {
          continuation.resume()
        } else {
          self.drainContinuations[normalized, default: []].append(continuation)
        }
      }
    }
    
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
      if !first {
        drainTask.cancel()
        if let orphanedContinuations = self.drainContinuations.removeValue(forKey: normalized) {
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
      let remaining = activeTokens[normalized]?.count ?? 0
      logger.warning("⏱️ [Gate] Drain timeout, \(remaining) connections remaining: \(normalized.prefix(16), privacy: .private)...")
      throw MLSGateError.drainTimeout(userDID: normalized, activeConnections: remaining)
    }
  }
}

// MARK: - Convenience Extensions

extension MLSDatabaseGate {
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
