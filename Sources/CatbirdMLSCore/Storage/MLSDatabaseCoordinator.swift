//
//  MLSDatabaseCoordinator.swift
//  CatbirdMLSCore
//
//  Cross-process file coordination for MLS database access.
//
//  CRITICAL: The Notification Service Extension (NSE) and main app run as separate
//  processes with separate memory spaces. SQLite's internal locking mechanisms
//  are not always sufficient to prevent corruption when both processes attempt
//  to write to an encrypted (SQLCipher) database simultaneously.
//
//  This coordinator uses NSFileCoordinator to serialize access across processes,
//  preventing the "HMAC check failed" and "out of memory" errors that occur when
//  the NSE and main app write to the database at the same time.
//
//  Architecture:
//  - Uses a dedicated lock file (not the database itself) for coordination
//  - Supports both read and write coordination
//  - Integrates with async/await via continuations
//  - Provides timeout mechanism to prevent deadlocks
//

import CryptoKit
import Foundation
import OSLog

/// Coordinates database access across the main app and Notification Service Extension
/// to prevent corruption from concurrent writes to the encrypted SQLCipher database.
final class MLSDatabaseCoordinator: @unchecked Sendable {
  
  // MARK: - Singleton
  
  /// Shared coordinator instance
  static let shared = MLSDatabaseCoordinator()
  
  // MARK: - Properties
  
  private let logger = Logger(subsystem: "Catbird", category: "MLSDatabaseCoordinator")
  
  /// Directory containing lock files used for coordination (App Group container when available)
  private let lockDirectoryURL: URL
  
  /// Global lock file used when a specific userDID is not available
  private let globalLockFileURL: URL
  
  /// Default timeout for acquiring coordination (seconds)
  private let defaultTimeout: TimeInterval = 15.0
  
  /// Tracks if we're currently holding the write lock (for debugging)
  private var isHoldingWriteLock = false
  
  // MARK: - Initialization
  
  private init() {
    // Use the shared App Group container for lock files
    if let container = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: "group.blue.catbird.shared"
    ) {
      self.lockDirectoryURL = container
      self.globalLockFileURL = container.appendingPathComponent(".mls_database.lock")
    } else {
      // Fallback to Application Support (won't work for cross-process, but prevents crash)
      let appSupport = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
      self.lockDirectoryURL = appSupport
      self.globalLockFileURL = appSupport.appendingPathComponent(".mls_database.lock")
      logger.warning("⚠️ App Group container not available - cross-process coordination will not work")
    }

    // Ensure the global lock file exists
    createLockFileIfNeeded(at: globalLockFileURL)
  }
  
  // MARK: - Public API
  
  /// Perform a read operation with coordination
  ///
  /// Multiple processes can read simultaneously, but reads are blocked while a write is in progress.
  /// - Parameters:
  ///   - timeout: Maximum time to wait for coordination (default: 15 seconds)
  ///   - operation: The read operation to perform
  /// - Returns: The result of the operation
  /// - Throws: Coordination errors or errors from the operation
  func performRead<T: Sendable>(
    timeout: TimeInterval? = nil,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    try await performRead(for: nil, timeout: timeout, operation: operation)
  }

  /// Perform a read operation with coordination for a specific user database.
  ///
  /// Uses a per-user lock file so unrelated accounts don't block each other.
  func performRead<T: Sendable>(
    for userDID: String?,
    timeout: TimeInterval? = nil,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    let effectiveTimeout = timeout ?? defaultTimeout
    return try await withCoordination(
      lockFileURL: lockFileURL(for: userDID),
      forWriting: false,
      timeout: effectiveTimeout,
      operation: operation
    )
  }
  
  /// Perform a write operation with exclusive coordination
  ///
  /// Only one process can write at a time. Reads and other writes are blocked.
  /// - Parameters:
  ///   - timeout: Maximum time to wait for coordination (default: 15 seconds)
  ///   - operation: The write operation to perform
  /// - Returns: The result of the operation
  /// - Throws: Coordination errors or errors from the operation
  func performWrite<T: Sendable>(
    timeout: TimeInterval? = nil,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    try await performWrite(for: nil, timeout: timeout, operation: operation)
  }

  /// Perform a write operation with exclusive coordination for a specific user database.
  ///
  /// Uses a per-user lock file so unrelated accounts don't block each other.
  func performWrite<T: Sendable>(
    for userDID: String?,
    timeout: TimeInterval? = nil,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    let effectiveTimeout = timeout ?? defaultTimeout
    return try await withCoordination(
      lockFileURL: lockFileURL(for: userDID),
      forWriting: true,
      timeout: effectiveTimeout,
      operation: operation
    )
  }
  
  /// Perform a write operation synchronously with exclusive coordination
  ///
  /// Use this for operations that cannot be async (e.g., in synchronous callbacks).
  /// - Parameters:
  ///   - timeout: Maximum time to wait for coordination (default: 15 seconds)
  ///   - operation: The write operation to perform
  /// - Returns: The result of the operation
  /// - Throws: Coordination errors or errors from the operation
  func performWriteSync<T>(
    timeout: TimeInterval? = nil,
    operation: @escaping () throws -> T
  ) throws -> T {
    try performWriteSync(for: nil, timeout: timeout, operation: operation)
  }

  func performWriteSync<T>(
    for userDID: String?,
    timeout: TimeInterval? = nil,
    operation: @escaping () throws -> T
  ) throws -> T {
    let effectiveTimeout = timeout ?? defaultTimeout
    return try withCoordinationSync(
      lockFileURL: lockFileURL(for: userDID),
      forWriting: true,
      timeout: effectiveTimeout,
      operation: operation
    )
  }
  
  // MARK: - Private Implementation
  
  /// Create a lock file if it doesn't exist
  private func createLockFileIfNeeded(at url: URL) {
    if !FileManager.default.fileExists(atPath: url.path) {
      do {
        try FileManager.default.createDirectory(
          at: url.deletingLastPathComponent(),
          withIntermediateDirectories: true
        )
        FileManager.default.createFile(atPath: url.path, contents: nil)
        logger.debug("Created lock file at: \(url.path)")
      } catch {
        logger.error("Failed to create lock file: \(error.localizedDescription)")
      }
    }
  }

  private func lockFileURL(for userDID: String?) -> URL {
    guard let userDID else { return globalLockFileURL }

    // Keep filenames short and filesystem-safe; hash provides uniqueness.
    let digest = SHA256.hash(data: Data(userDID.utf8))
    let hex = digest.compactMap { String(format: "%02x", $0) }.joined()
    let filename = ".mls_database.\(hex.prefix(16)).lock"

    let url = lockDirectoryURL.appendingPathComponent(filename)
    createLockFileIfNeeded(at: url)
    return url
  }
  
  /// Perform an async operation with file coordination
  private func withCoordination<T: Sendable>(
    lockFileURL: URL,
    forWriting: Bool,
    timeout: TimeInterval,
    operation: @Sendable @escaping () async throws -> T
  ) async throws -> T {
    let coordinator = NSFileCoordinator(filePresenter: nil)

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

      // Timeout the *entire* coordinated operation (acquire + work) and ensure we only resume once.
      let timeoutWorkItem = DispatchWorkItem { [weak self] in
        self?.logger.error("❌ File coordination timed out after \(timeout)s")
        resumeOnce(.failure(MLSDatabaseCoordinatorError.timeout))
      }
      DispatchQueue.global().asyncAfter(deadline: .now() + timeout, execute: timeoutWorkItem)

      // Run coordination off the caller thread (important for main-actor callers).
      DispatchQueue.global(qos: .userInitiated).async { [weak self] in
        var coordinatorError: NSError?

        let coordinationBlock: (URL) -> Void = { [weak self] _ in
          if forWriting {
            self?.isHoldingWriteLock = true
            self?.logger.debug("🔒 Acquired write lock")
          } else {
            self?.logger.debug("🔓 Acquired read lock")
          }

          // Hold the coordinated access until the async operation finishes.
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

          semaphore.wait()

          if forWriting {
            self?.isHoldingWriteLock = false
            self?.logger.debug("🔓 Released write lock")
          }

          timeoutWorkItem.cancel()
          resumeOnce(opResult ?? .failure(MLSDatabaseCoordinatorError.timeout))
        }

        if forWriting {
          coordinator.coordinate(
            writingItemAt: lockFileURL,
            options: [],
            error: &coordinatorError,
            byAccessor: coordinationBlock
          )
        } else {
          coordinator.coordinate(
            readingItemAt: lockFileURL,
            options: [],
            error: &coordinatorError,
            byAccessor: coordinationBlock
          )
        }

        if let error = coordinatorError {
          timeoutWorkItem.cancel()
          self?.logger.error("❌ File coordination failed: \(error.localizedDescription)")
          resumeOnce(.failure(MLSDatabaseCoordinatorError.coordinationFailed(error)))
        }
      }
    }
  }
  
  /// Perform a synchronous operation with file coordination
  private func withCoordinationSync<T>(
    lockFileURL: URL,
    forWriting: Bool,
    timeout: TimeInterval,
    operation: @escaping () throws -> T
  ) throws -> T {
    let coordinator = NSFileCoordinator(filePresenter: nil)
    var coordinatorError: NSError?
    var result: Result<T, Error>?
    
    let semaphore = DispatchSemaphore(value: 0)
    
    let coordinationBlock: (URL) -> Void = { [weak self] _ in
      if forWriting {
        self?.isHoldingWriteLock = true
        self?.logger.debug("🔒 [Sync] Acquired write lock")
      }
      
      do {
        let value = try operation()
        result = .success(value)
      } catch {
        result = .failure(error)
      }
      
      if forWriting {
        self?.isHoldingWriteLock = false
        self?.logger.debug("🔓 [Sync] Released write lock")
      }
      
      semaphore.signal()
    }
    
    // Start coordination on background queue
    DispatchQueue.global(qos: .userInitiated).async {
      if forWriting {
        coordinator.coordinate(
          writingItemAt: lockFileURL,
          options: [],
          error: &coordinatorError,
          byAccessor: coordinationBlock
        )
      } else {
        coordinator.coordinate(
          readingItemAt: lockFileURL,
          options: [],
          error: &coordinatorError,
          byAccessor: coordinationBlock
        )
      }
    }
    
    // Wait with timeout
    let waitResult = semaphore.wait(timeout: .now() + timeout)
    
    if waitResult == .timedOut {
      logger.error("❌ [Sync] File coordination timed out after \(timeout)s")
      throw MLSDatabaseCoordinatorError.timeout
    }
    
    if let error = coordinatorError {
      logger.error("❌ [Sync] File coordination failed: \(error.localizedDescription)")
      throw MLSDatabaseCoordinatorError.coordinationFailed(error)
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

/// Errors that can occur during database coordination
enum MLSDatabaseCoordinatorError: Error, LocalizedError {
  case timeout
  case coordinationFailed(NSError)
  case operationNotCompleted
  
  public var errorDescription: String? {
    switch self {
    case .timeout:
      return "Database coordination timed out - another process may be holding the lock"
    case .coordinationFailed(let error):
      return "File coordination failed: \(error.localizedDescription)"
    case .operationNotCompleted:
      return "Database operation did not complete"
    }
  }
}
