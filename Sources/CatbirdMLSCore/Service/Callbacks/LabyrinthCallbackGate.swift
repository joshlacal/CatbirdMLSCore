//
//  LabyrinthCallbackGate.swift
//  CatbirdMLSCore
//
//  Bounds the synchronous Labyrinth callbacks. Rust holds its Labyrinth slot
//  lock while one runs, and suspending the app takes that same lock on the
//  main thread, so a callback must not wait out a slow network: each one gets
//  a deadline, and suspension cancels the ones in flight and refuses new ones
//  until the app resumes. Rust treats either failure as a transport error and
//  retries the archive later.
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
  /// The longest one callback may hold the calling Rust thread.
  static let deadline: TimeInterval = 20

  private let lock = NSLock()
  private var suspended = false
  private var cancellers: [UUID: @Sendable () -> Void] = [:]

  /// Refuses new callbacks and fails the ones in flight at once.
  func suspend() {
    lock.lock()
    suspended = true
    let pending = Array(cancellers.values)
    lock.unlock()
    pending.forEach { $0() }
  }

  func resume() {
    lock.lock()
    suspended = false
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
    if suspended {
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
