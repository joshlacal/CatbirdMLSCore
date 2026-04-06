//
//  MLSDatabaseCoordinator.swift
//  CatbirdMLSCore
//
//  Database operation coordinator with timeout protection.
//
//  NOTE: NSFileCoordinator was REMOVED to prevent 0xdead10cc crashes.
//  iOS kills apps that hold file coordination locks during suspension.
//  SQLite WAL mode with budget-based checkpoints handles concurrency safely.
//
//  Architecture:
//  - Provides timeout mechanism to prevent deadlocks
//  - Operations execute on background queue
//  - No file coordination locks acquired
//  - Relies on SQLite WAL mode for cross-process safety
//

import Foundation
import OSLog

/// Coordinates database access with timeout protection.
/// NOTE: File coordination removed to prevent 0xdead10cc crashes.
public final class MLSDatabaseCoordinator: @unchecked Sendable {

  // MARK: - Singleton

  /// Shared coordinator instance
  public static let shared = MLSDatabaseCoordinator()

  // MARK: - Properties

  private let logger = Logger(subsystem: "Catbird", category: "MLSDatabaseCoordinator")

  /// Default timeout for operations (seconds)
  private let defaultTimeout: TimeInterval = 15.0

  /// Tracks if we're currently performing a write (for debugging)
  private var isPerformingWrite = false

  // MARK: - Initialization

  private init() {
    // No lock files needed - SQLite WAL mode handles concurrency
  }
  
  // MARK: - Suspension Handling

  /// Prepare for app suspension.
  /// NOTE: No-op - WAL mode handles suspension safely without intervention.
  public func prepareForSuspension() {
    logger.debug("prepareForSuspension is no-op - WAL handles suspension safely")
  }

  /// Resume normal operation after returning from background.
  /// Clears the MLS suspension flag so Rust FFI contexts can be re-created on demand.
  public func resumeFromSuspension() {
    MLSCoreContext.clearSuspensionFlag()
    MLSClient.clearSuspensionFlag(reason: "MLSDatabaseCoordinator.resumeFromSuspension")
    logger.debug("Resumed from suspension - cleared MLS suspension flags")
  }

  // MARK: - Public API

  /// Perform a read operation with timeout protection.
  /// - Parameters:
  ///   - timeout: Maximum time for operation (default: 15 seconds)
  ///   - operation: The read operation to perform
  /// - Returns: The result of the operation
  /// - Throws: Timeout or errors from the operation
  public func performRead<T: Sendable>(
    timeout: TimeInterval? = nil,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    try await performRead(for: nil, timeout: timeout, operation: operation)
  }

  /// Perform a read operation with timeout protection for a specific user.
  public func performRead<T: Sendable>(
    for userDID: String?,
    timeout: TimeInterval? = nil,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    let effectiveTimeout = timeout ?? defaultTimeout
    return try await withTimeout(
      forWriting: false,
      timeout: effectiveTimeout,
      operation: operation
    )
  }

  /// Perform a write operation with timeout protection.
  /// - Parameters:
  ///   - timeout: Maximum time for operation (default: 15 seconds)
  ///   - operation: The write operation to perform
  /// - Returns: The result of the operation
  /// - Throws: Timeout or errors from the operation
  public func performWrite<T: Sendable>(
    timeout: TimeInterval? = nil,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    try await performWrite(for: nil, timeout: timeout, operation: operation)
  }

  /// Perform a write operation with timeout protection for a specific user.
  public func performWrite<T: Sendable>(
    for userDID: String?,
    timeout: TimeInterval? = nil,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    let effectiveTimeout = timeout ?? defaultTimeout
    return try await withTimeout(
      forWriting: true,
      timeout: effectiveTimeout,
      operation: operation
    )
  }

  /// Perform a synchronous write operation with timeout protection.
  /// Use this for operations that cannot be async.
  public func performWriteSync<T>(
    timeout: TimeInterval? = nil,
    operation: @escaping () throws -> T
  ) throws -> T {
    try performWriteSync(for: nil, timeout: timeout, operation: operation)
  }

  /// Perform a synchronous write operation with timeout protection for a specific user.
  public func performWriteSync<T>(
    for userDID: String?,
    timeout: TimeInterval? = nil,
    operation: @escaping () throws -> T
  ) throws -> T {
    let effectiveTimeout = timeout ?? defaultTimeout
    return try withTimeoutSync(
      forWriting: true,
      timeout: effectiveTimeout,
      operation: operation
    )
  }
  
  // MARK: - Private Implementation

  /// Perform an async operation with timeout protection (no file coordination).
  private func withTimeout<T: Sendable>(
    forWriting: Bool,
    timeout: TimeInterval,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<T, Error>) in
      let resumeLock = NSLock()
      var didResume = false
      var operationTask: Task<T, Error>?

      func resumeOnce(_ result: Result<T, Error>) {
        resumeLock.lock()
        defer { resumeLock.unlock() }
        guard !didResume else { return }
        didResume = true
        operationTask?.cancel()

        switch result {
        case .success(let value):
          continuation.resume(returning: value)
        case .failure(let error):
          continuation.resume(throwing: error)
        }
      }

      // Timeout for the operation
      let timeoutWorkItem = DispatchWorkItem { [weak self] in
        self?.logger.error("Operation timed out after \(timeout)s")
        resumeOnce(.failure(MLSDatabaseCoordinatorError.timeout))
      }
      DispatchQueue.global().asyncAfter(deadline: .now() + timeout, execute: timeoutWorkItem)

      // Run operation on background queue (important for main-actor callers)
      DispatchQueue.global(qos: .userInitiated).async { [weak self] in
        if forWriting {
          self?.isPerformingWrite = true
        }

        let semaphore = DispatchSemaphore(value: 0)
        var opResult: Result<T, Error>?

        let task = Task { try await operation() }
        resumeLock.lock()
        operationTask = task
        resumeLock.unlock()

        Task {
          defer { semaphore.signal() }
          do {
            opResult = .success(try await task.value)
          } catch {
            opResult = .failure(error)
          }
        }

        // Wait for operation to complete
        semaphore.wait()

        if forWriting {
          self?.isPerformingWrite = false
        }

        timeoutWorkItem.cancel()
        resumeOnce(opResult ?? .failure(MLSDatabaseCoordinatorError.timeout))
      }
    }
  }
  
  /// Perform a synchronous operation with timeout protection (no file coordination).
  private func withTimeoutSync<T>(
    forWriting: Bool,
    timeout: TimeInterval,
    operation: @escaping () throws -> T
  ) throws -> T {
    var result: Result<T, Error>?
    let semaphore = DispatchSemaphore(value: 0)

    // Run operation on background queue
    DispatchQueue.global(qos: .userInitiated).async { [weak self] in
      if forWriting {
        self?.isPerformingWrite = true
      }

      do {
        let value = try operation()
        result = .success(value)
      } catch {
        result = .failure(error)
      }

      if forWriting {
        self?.isPerformingWrite = false
      }

      semaphore.signal()
    }

    // Wait for operation with timeout
    let waitResult = semaphore.wait(timeout: .now() + timeout)

    if waitResult == .timedOut {
      logger.error("[Sync] Operation timed out after \(timeout)s")
      throw MLSDatabaseCoordinatorError.timeout
    }

    switch result {
    case .success(let value):
      return value
    case .failure(let error):
      throw error
    case .none:
      throw MLSDatabaseCoordinatorError.operationNotCompleted
    }
  }
}

// MARK: - Errors

/// Errors that can occur during database operations
enum MLSDatabaseCoordinatorError: Error, LocalizedError {
  case timeout
  case operationNotCompleted

  public var errorDescription: String? {
    switch self {
    case .timeout:
      return "Database operation timed out"
    case .operationNotCompleted:
      return "Database operation did not complete"
    }
  }
}
