//
//  MergedCommitTracker.swift
//  CatbirdMLSCore
//
//  Phase D-Swift, Task D-S.2 — in-memory short-circuit for already-merged commits.
//
//  The decrypt path can be invited to re-process the same commit ciphertext in
//  several ways:
//
//    1. The SSE / WebSocket stream redelivers an envelope after a reconnect.
//    2. EPOCH-RECOVERY's `fetchAndProcessMissingCommits` pulls a commit by epoch
//       range, processes it, and then the same epoch range is requested again
//       from a different code path before `currentEpoch` was updated on the
//       caller stack.
//    3. Pre-send sync and catch-up both run against an overlapping epoch range.
//
//  The existing `commit.epoch <= currentEpoch` skip in
//  `fetchAndProcessMissingCommits` (Messaging.swift:1524) handles most cases,
//  but it can't catch:
//
//    - The same epoch reached via TWO commits (fork resolution / sequencer hiccup).
//    - A redelivery that arrives before the post-merge epoch read has propagated
//      to the caller's loop variable.
//
//  This tracker is a small in-memory `Set<String>` keyed by SHA-256 hash of the
//  commit ciphertext. It is NOT persisted across app restarts — the existing
//  `epoch <= currentEpoch` guard handles restart-time idempotency cheaply, since
//  the OpenMLS group's persisted epoch survives restarts. The tracker exists
//  purely to catch in-process double-merges that the epoch skip can't see.
//
//  Entries are evicted after a TTL (default 10 minutes) to bound memory growth;
//  the chance of seeing the same commit hash after 10 minutes is effectively
//  zero (commit hashes are 256 bits).
//

import Foundation
import CryptoKit

/// In-memory tracker for commits that have already been merged into the local
/// MLS state during the current process lifetime. Used to short-circuit the
/// idempotent-but-expensive `MLSClient.processCommit` FFI call when the same
/// commit ciphertext is re-fed through the decrypt path.
///
/// Thread-safety: backed by an `NSLock`. The tracker is intentionally a
/// reference type (`final class`) so that callers can share a single instance
/// across actors without needing to round-trip the data through actor message
/// passing for every check.
public final class MergedCommitTracker: @unchecked Sendable {

  // MARK: - State

  private var entries: [String: Date] = [:]
  private let lock = NSLock()

  // MARK: - Construction

  public init() {}

  // MARK: - Public API

  /// Mark `commitHash` as merged. Returns `true` if the hash was already
  /// present (i.e. the caller should treat this as a duplicate and skip
  /// further processing), `false` if it was added fresh.
  ///
  /// The "if new" semantic encodes atomic check-and-insert: a subsequent
  /// call with the same hash always reports `true` until eviction, so a
  /// well-formed caller pattern is:
  ///
  /// ```swift
  /// if tracker.markMergedIfNew(commitHash: hash) {
  ///   return // already merged elsewhere; nothing to do
  /// }
  /// try await mlsClient.processCommit(...)
  /// ```
  @discardableResult
  public func markMergedIfNew(commitHash: String) -> Bool {
    lock.lock()
    defer { lock.unlock() }
    if entries[commitHash] != nil {
      return true
    }
    entries[commitHash] = Date()
    return false
  }

  /// Returns whether the given commit hash has been marked merged.
  /// Mostly useful for tests; production code should call `markMergedIfNew`
  /// to keep the check-and-insert atomic.
  public func contains(commitHash: String) -> Bool {
    lock.lock()
    defer { lock.unlock() }
    return entries[commitHash] != nil
  }

  /// Drop entries older than `ttl` seconds (relative to `now`). The caller is
  /// expected to invoke this periodically — there is no background timer.
  ///
  /// Default policy: `ttl` of 10 minutes (600s) bounded memory growth at
  /// `~32 bytes * (commits/10min)` which for any realistic conversation
  /// volume stays under a few KB.
  public func evictOlderThan(ttl: TimeInterval, now: Date = Date()) {
    lock.lock()
    defer { lock.unlock() }
    entries = entries.filter { now.timeIntervalSince($0.value) < ttl }
  }

  // MARK: - Pure-function helpers

  /// Canonical commit hash: lowercase hex SHA-256 of the ciphertext. This is
  /// the same encoding used by `MLSConversationManager.trackOwnCommit` /
  /// `isOwnCommit`, so the two tracking systems can interoperate if needed.
  public static func commitHash(for ciphertext: Data) -> String {
    SHA256.hash(data: ciphertext)
      .compactMap { String(format: "%02x", $0) }
      .joined()
  }

  // MARK: - Test affordances

  /// Insert a commit hash with a specific timestamp. Used by tests to seed
  /// "old" entries for eviction checks. NOT intended for production use.
  internal func unsafeInsertForTest(commitHash: String, at date: Date) {
    lock.lock()
    defer { lock.unlock() }
    entries[commitHash] = date
  }
}
