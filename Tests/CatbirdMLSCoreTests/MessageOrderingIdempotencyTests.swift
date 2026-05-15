//
//  MessageOrderingIdempotencyTests.swift
//  CatbirdMLSCoreTests
//
//  Phase D-Swift, Task D-S.2 — idempotency of catch-up buffer drain and
//  merged-commit short-circuit.
//
//  These tests cover the two May 2026 reproduction symptoms targeted by D-S.2:
//
//  1. **Merged-commit tracker idempotency.** A `MergedCommitTracker` is a
//     pure in-memory `Set<String>` keyed by commit hash; once a commit is
//     marked merged the tracker reports it as already merged on subsequent
//     calls. This is the within-run dedup that complements the existing
//     `commit.epoch <= currentEpoch` skip — the latter can't catch the case
//     where two distinct commits both target the same epoch (fork resolution
//     or sequencer hiccup), or where the same commit hash is re-fed before
//     the post-merge epoch read has propagated to the loop variable.
//
//  2. **Atomic buffer drain.** The new `drainBufferedMessages` method on
//     `MLSMessageOrderingCoordinator` swaps the buffer atomically: it
//     reads all pending rows AND deletes them within the same returned
//     sequence, so a caller iterating the returned list cannot cause a
//     successor sync pass to re-fetch and re-buffer the same messages.
//

import XCTest
import GRDB
@testable import CatbirdMLSCore

final class MergedCommitTrackerTests: XCTestCase {

  func testFirstMarkReturnsFalse_SecondMarkReturnsTrue() {
    let tracker = MergedCommitTracker()
    let hash = "abc123"
    XCTAssertFalse(tracker.markMergedIfNew(commitHash: hash),
                   "First mark for a commit hash must return false (not already merged)")
    XCTAssertTrue(tracker.markMergedIfNew(commitHash: hash),
                  "Second mark for the same commit hash must return true (already merged)")
  }

  func testDistinctHashesAreIndependent() {
    let tracker = MergedCommitTracker()
    XCTAssertFalse(tracker.markMergedIfNew(commitHash: "hash-A"))
    XCTAssertFalse(tracker.markMergedIfNew(commitHash: "hash-B"))
    XCTAssertTrue(tracker.markMergedIfNew(commitHash: "hash-A"),
                  "hash-A should still be marked after hash-B was added")
  }

  func testCommitHashFromBytesIsStable() {
    let payload = Data([0x01, 0x02, 0x03, 0x04])
    let h1 = MergedCommitTracker.commitHash(for: payload)
    let h2 = MergedCommitTracker.commitHash(for: payload)
    XCTAssertEqual(h1, h2, "Hash of identical bytes must match")

    let differentPayload = Data([0x01, 0x02, 0x03, 0x05])
    let h3 = MergedCommitTracker.commitHash(for: differentPayload)
    XCTAssertNotEqual(h1, h3, "Hash of different bytes must differ")
  }

  func testEvictionRemovesEntriesOlderThanTTL() {
    let tracker = MergedCommitTracker()
    let oldHash = "old-hash"

    // Insert with a clock reference 10 minutes in the past.
    let past = Date(timeIntervalSinceNow: -600)
    tracker.unsafeInsertForTest(commitHash: oldHash, at: past)

    // Sanity: already-merged check at t==past+1s still sees it.
    XCTAssertTrue(tracker.contains(commitHash: oldHash),
                  "Entry should be present immediately after insert")

    // After evicting entries older than 5 minutes from now, the entry is gone.
    tracker.evictOlderThan(ttl: 300, now: Date())
    XCTAssertFalse(tracker.contains(commitHash: oldHash),
                   "Entry inserted 10 minutes ago must be evicted under a 5-minute TTL")
  }

  func testEvictionPreservesRecentEntries() {
    let tracker = MergedCommitTracker()
    let recentHash = "recent-hash"
    tracker.unsafeInsertForTest(commitHash: recentHash, at: Date())

    tracker.evictOlderThan(ttl: 300, now: Date())
    XCTAssertTrue(tracker.contains(commitHash: recentHash),
                  "Entry inserted just now must survive eviction under a 5-minute TTL")
  }
}

// MARK: - Atomic Buffer Drain

final class MLSMessageOrderingCoordinatorAtomicDrainTests: XCTestCase {

  private var dbQueue: DatabaseQueue!
  private let coordinator = MLSMessageOrderingCoordinator()
  private let conversationID = "convo-drain-idempotency"
  private let currentUserDID = "did:plc:receiver"

  override func setUp() async throws {
    try await super.setUp()
    dbQueue = try DatabaseQueue()

    try await dbQueue.write { db in
      try MLSPendingMessageModel.createTable(in: db)
    }
  }

  override func tearDown() async throws {
    dbQueue = nil
    try await super.tearDown()
  }

  func testDrainReturnsAllPendingMessages() async throws {
    try await insertPending(messageID: "msg-1", seq: 1)
    try await insertPending(messageID: "msg-2", seq: 2)
    try await insertPending(messageID: "msg-3", seq: 3)

    let drained = try await coordinator.drainBufferedMessages(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      database: dbQueue
    )

    XCTAssertEqual(drained.count, 3, "Drain should return all pending messages")
    XCTAssertEqual(drained.map { $0.sequenceNumber }, [1, 2, 3],
                   "Drain should return messages in sequence order")
  }

  func testDrainRemovesMessagesFromBufferAtomically() async throws {
    try await insertPending(messageID: "msg-1", seq: 1)
    try await insertPending(messageID: "msg-2", seq: 2)

    _ = try await coordinator.drainBufferedMessages(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      database: dbQueue
    )

    // After drain, the buffer must be empty — the messages should NOT be re-fetchable.
    let remaining = try await dbQueue.read { db in
      try MLSPendingMessageModel.fetchAll(db)
    }
    XCTAssertTrue(remaining.isEmpty,
                  "Drain must remove all returned messages from the pending table")
  }

  func testSecondDrainIsEmpty() async throws {
    try await insertPending(messageID: "msg-1", seq: 1)
    try await insertPending(messageID: "msg-2", seq: 2)

    let first = try await coordinator.drainBufferedMessages(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      database: dbQueue
    )
    XCTAssertEqual(first.count, 2, "First drain returns the buffered messages")

    let second = try await coordinator.drainBufferedMessages(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      database: dbQueue
    )
    XCTAssertTrue(second.isEmpty,
                  "Second drain must return zero — first drain already cleared the buffer")
  }

  func testDrainScopedByConversation() async throws {
    let otherConvoID = "convo-other"

    try await insertPending(messageID: "this-1", seq: 1, conversationID: conversationID)
    try await insertPending(messageID: "other-1", seq: 1, conversationID: otherConvoID)

    let drained = try await coordinator.drainBufferedMessages(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      database: dbQueue
    )

    XCTAssertEqual(drained.count, 1, "Drain must only return rows for the requested conversation")
    XCTAssertEqual(drained.first?.messageID, "this-1")

    // The other conversation's row must still be in the table.
    let others = try await dbQueue.read { db in
      try MLSPendingMessageModel
        .filter(MLSPendingMessageModel.Columns.conversationID == otherConvoID)
        .fetchAll(db)
    }
    XCTAssertEqual(others.count, 1,
                   "Other conversation's pending row must NOT be removed by another conversation's drain")
  }

  func testDrainScopedByCurrentUserDID() async throws {
    let otherUserDID = "did:plc:other-user"

    try await insertPending(messageID: "mine-1", seq: 1, currentUserDID: currentUserDID)
    try await insertPending(messageID: "theirs-1", seq: 1, currentUserDID: otherUserDID)

    let drained = try await coordinator.drainBufferedMessages(
      conversationID: conversationID,
      currentUserDID: currentUserDID,
      database: dbQueue
    )

    XCTAssertEqual(drained.count, 1, "Drain must only return rows for the requested user")
    XCTAssertEqual(drained.first?.messageID, "mine-1")
  }

  // MARK: - Helpers

  private func insertPending(
    messageID: String,
    seq: Int64,
    conversationID: String? = nil,
    currentUserDID: String? = nil
  ) async throws {
    let resolvedConvo = conversationID ?? self.conversationID
    let resolvedUser = currentUserDID ?? self.currentUserDID
    let dummyJSON = try JSONEncoder().encode(["seq": seq])
    let pending = MLSPendingMessageModel(
      messageID: messageID,
      currentUserDID: MLSStorageHelpers.normalizeDID(resolvedUser),
      conversationID: resolvedConvo,
      sequenceNumber: seq,
      epoch: 0,
      messageViewJSON: dummyJSON,
      receivedAt: Date(),
      processAttempts: 0,
      source: "test"
    )
    try await dbQueue.write { db in
      try pending.save(db, onConflict: .replace)
    }
  }
}
