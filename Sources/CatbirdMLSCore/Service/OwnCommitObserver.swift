//
//  OwnCommitObserver.swift
//  CatbirdMLSCore
//
//  Phase D-Swift, Task D-S.3 — observer protocol for commit producers.
//
//  The `MLSConversationManager.ownCommits` map records SHA-256 hashes of
//  commits we produced locally, so when the server fans the same ciphertext
//  back over WebSocket / SSE we can short-circuit at the decrypt path
//  instead of attempting to decrypt our own message (which would fail
//  with `CannotDecryptOwnMessage` per MLS forward secrecy).
//
//  Three of the four own-commit producers (create-group, addMembers, group
//  setup) already record into that map directly because they live in
//  `MLSConversationManager` extensions and have access to `trackOwnCommit`.
//  The FOURTH producer — `MLSClient.joinByExternalCommit` (External Commit
//  / rejoin path) — lives in a different type (`MLSClient` actor) that
//  doesn't reference `MLSConversationManager`, and historically failed to
//  record commits at all (the bug confirmed in D-S.0 orientation,
//  exactly mirroring the Rust orchestrator's own_commits insert-gap).
//
//  This protocol is the seam: `MLSClient` notifies a registered observer
//  whenever it produces a commit. `MLSConversationManager` conforms to the
//  protocol and forwards into its existing `trackOwnCommit(_:)` method,
//  closing the producer gap without giving `MLSClient` a reference back
//  to the manager (which would create a circular ownership graph between
//  two singleton-like instances).
//

import Foundation

/// Receives notifications about commits the local client has just produced.
/// Conformers record the commit bytes for later receive-side dedup.
///
/// Conformers SHOULD be reference types so that a single observer can be
/// registered across multiple producer actor instances; `MLSClient` keeps
/// the observer weakly to avoid extending its lifetime.
public protocol OwnCommitObserver: AnyObject, Sendable {
  /// Called when a local commit has been produced and accepted by the
  /// server. The commit bytes are the same `commit_data` blob that will
  /// later arrive on the receive path; recording their SHA-256 hash lets
  /// the receiver short-circuit.
  ///
  /// Implementations should be cheap and non-blocking. The producer awaits
  /// the call before returning to its own caller, so a slow observer
  /// would delay the External Commit flow.
  func ownCommitProduced(commitBytes: Data) async
}

/// Reference-typed adapter that forwards `OwnCommitObserver` calls into a
/// Sendable closure. Useful for hosts (like `MLSConversationManager`) that
/// can't easily conform to `OwnCommitObserver` directly because of
/// concurrency / property-wrapper constraints (e.g. `@Observable` classes),
/// but still want to plumb the callback through to one of their own
/// instance methods. The closure captures `self` weakly at the call site.
public final class OwnCommitClosureObserver: OwnCommitObserver, @unchecked Sendable {
  private let handler: @Sendable (Data) -> Void

  public init(handler: @escaping @Sendable (Data) -> Void) {
    self.handler = handler
  }

  public func ownCommitProduced(commitBytes: Data) async {
    handler(commitBytes)
  }
}
