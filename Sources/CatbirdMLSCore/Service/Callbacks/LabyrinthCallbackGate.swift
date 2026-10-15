//
//  LabyrinthCallbackGate.swift
//  CatbirdMLSCore
//
//  Bounds the synchronous callbacks Rust makes while it holds its Labyrinth
//  slot lock: the Labyrinth requests and mailboxKeys records, and the
//  canonical reads the archive flush makes. Suspending the app takes that
//  same lock on the main thread, so a callback must not wait out a slow
//  network: each one gets a deadline, and suspension cancels the ones in
//  flight and refuses new ones. The shared gate follows MLSClient's FFI
//  suspension flag, which clears on foreground or when a BGTask runs MLS
//  work. The runtime calls that take that lock on the calling thread
//  (suspend, shutdown, emergency close) also hold it shut until they return,
//  whatever the flag does meanwhile. Rust retries the archive later. A failed Labyrinth request or canonical read is a transport error
//  there; a failed mailboxKeys read counts towards quarantine until the FFI
//  carries catbird-mls #20.
//

import Foundation

enum LabyrinthCallbackError: Error, LocalizedError, Equatable {
  case suspended
  case timedOut(TimeInterval)

  var errorDescription: String? {
    switch self {
    case .suspended: return "labyrinth callback refused while MLS is suspended"
    case let .timedOut(deadline): return "labyrinth callback exceeded \(Int(deadline)) s"
    }
  }
}

final class LabyrinthCallbackGate: @unchecked Sendable {
  static let shared = LabyrinthCallbackGate()
  /// The longest one Labyrinth callback may hold the calling Rust thread.
  static let deadline: TimeInterval = 20
  /// The same for a canonical read, which sync also makes and which can carry
  /// a large page: the network layer's own idle timeout, so only a hung read
  /// is cut short. Suspension still cancels it at once.
  static let readDeadline: TimeInterval = 120
  /// The same for retracting the account's `mailboxKeys`, which lists and
  /// deletes every per-epoch record one request at a time. Suspension still
  /// cancels it at once, and a retraction cut short keeps what it deleted:
  /// the purge records its remote half as pending and retries it.
  static let retractDeadline: TimeInterval = 120

  private let lock = NSLock()
  private var suspended = false
  private var holds = 0
  private var cancellers: [UUID: @Sendable () -> Void] = [:]

  /// Refuses new callbacks and fails the ones in flight at once.
  func suspend() {
    shut().forEach { $0() }
  }

  /// Refuses new callbacks and returns the cancellers of the ones in flight,
  /// for a caller that shuts the gate under its own lock: it runs them once
  /// that lock is released.
  func shut() -> [@Sendable () -> Void] {
    lock.lock()
    suspended = true
    let pending = Array(cancellers.values)
    lock.unlock()
    return pending
  }

  func resume() {
    lock.lock()
    suspended = false
    lock.unlock()
  }

  /// Keeps the gate shut until the matching `endSuspensionHold()`, even if
  /// `resume()` runs meanwhile, and fails the callbacks in flight at once.
  func beginSuspensionHold() {
    lock.lock()
    holds += 1
    let pending = Array(cancellers.values)
    lock.unlock()
    pending.forEach { $0() }
  }

  func endSuspensionHold() {
    lock.lock()
    holds = max(0, holds - 1)
    lock.unlock()
  }

  func resetForTesting() {
    lock.lock()
    suspended = false
    holds = 0
    lock.unlock()
  }

  /// Runs `operation` for a synchronous callback and waits at most `deadline`.
  func run<T>(
    deadline: TimeInterval = LabyrinthCallbackGate.deadline,
    _ operation: @escaping () async throws -> T
  ) throws -> T {
    let call = Call<T>()
    let id = UUID()
    lock.lock()
    if suspended || holds > 0 {
      lock.unlock()
      throw LabyrinthCallbackError.suspended
    }
    let task = Task.detached {
      do {
        call.finish(.success(try await operation()))
      } catch {
        call.finish(.failure(error))
      }
    }
    cancellers[id] = {
      task.cancel()
      call.finish(.failure(LabyrinthCallbackError.suspended))
    }
    lock.unlock()
    defer {
      lock.lock()
      cancellers[id] = nil
      lock.unlock()
    }

    guard let outcome = call.wait(timeout: deadline) else {
      task.cancel()
      throw LabyrinthCallbackError.timedOut(deadline)
    }
    return try outcome.get()
  }

  /// One callback's result. The first outcome wins: a cancelled task that
  /// finishes later does not replace the suspension error.
  private final class Call<T>: @unchecked Sendable {
    private let semaphore = DispatchSemaphore(value: 0)
    private let lock = NSLock()
    private var result: Result<T, Error>?

    func finish(_ outcome: Result<T, Error>) {
      lock.lock()
      let isFirst = result == nil
      if isFirst { result = outcome }
      lock.unlock()
      if isFirst { semaphore.signal() }
    }

    func wait(timeout: TimeInterval) -> Result<T, Error>? {
      guard semaphore.wait(timeout: .now() + timeout) == .success else { return nil }
      lock.lock()
      defer { lock.unlock() }
      return result
    }
  }
}
