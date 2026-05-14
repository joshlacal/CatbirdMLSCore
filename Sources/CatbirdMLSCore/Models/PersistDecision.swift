//
//  PersistDecision.swift
//  CatbirdMLSCore
//
//  Phase D-Swift, Task D-S.4 — typed decision for whether the atomic-block
//  in `MLSConversationManager.persistProcessedPayload` should write a row,
//  skip persistence entirely, or surface a typed error.
//
//  Before D-S.4 the atomic block was unconditional: every message routed
//  through `processServerMessage` -> `persistProcessedPayload` ended up
//  with a row in `MLSMessageModel`. For own-message echoes and
//  wrong-epoch old messages, the row was written with a non-nil
//  `processingError` field, which the UI then rendered as a failed-
//  decrypt error bubble — the "hasError=true placeholders" symptom in
//  the May 2026 iOS reproduction.
//
//  The fix is decision-driven persistence:
//
//    .persist                     -> write the decrypted payload row
//    .skipPersist(reason:)        -> write NOTHING (caller short-circuits)
//    .surfaceError(MLSProcessError) -> write a SINGLE typed-error row that
//                                    the UI can show as actionable
//
//  The decision is computed by a pure function `PersistDecisionPolicy.decide`
//  so it can be unit-tested without instantiating a full
//  `MLSConversationManager` (which depends on the singleton `MLSClient`).
//
//  ## Extension points reserved for Phase E.iOS
//
//  Phase E.iOS will wire `WelcomeRecoveryDecision` (Accept / RequestReissue
//  / ExternalCommitWithHistoryGap / Surrender) into the receive path. Two
//  additional `SkipReason` cases are reserved (commented out below) for
//  that wave:
//
//    .awaitingReissue           — the message arrived during a reissue
//                                 window; we haven't observed the
//                                 successor Welcome yet, so the decrypt
//                                 attempt is deferred rather than failed.
//    .surrenderedAtRecipient    — recipient surrendered their place in
//                                 the group; we should not even attempt
//                                 to process further messages for this
//                                 group at this device until the user
//                                 resurfaces the group via a new join.
//
//  Do NOT uncomment these here — adding the cases now means D-Swift has
//  to also wire the producer logic for them, which is out of scope. E.iOS
//  uncommenting + wiring is the test of the right scope split.
//

import Foundation

/// What the atomic-block should do with the result of a single message
/// processing attempt. The decision is computed by
/// `PersistDecisionPolicy.decide` and then dispatched by
/// `MLSConversationManager.persistProcessedPayload`.
public enum PersistDecision: Sendable, Equatable {
  /// Decrypt succeeded; write the row with the decoded payload.
  case persist

  /// Don't write a row at all. The reason captures *why* so logs / metrics
  /// can distinguish the (legitimate) cases.
  case skipPersist(reason: SkipReason)

  /// Decrypt failed in a way the user can act on (e.g., reissue is
  /// available). Write a SINGLE row with a typed error variant so the
  /// UI can render an actionable affordance. USE SPARINGLY — most decrypt
  /// failures should map to either `.skipPersist` (transient, recoverable
  /// without user action) or be re-routed to recovery state machinery.
  case surfaceError(MLSProcessError)
}

/// The reason why a message's processing did not produce a persisted row.
/// Each case corresponds to a known, expected condition; the UI MUST NOT
/// treat any of these as failures to display.
public enum SkipReason: Sendable, Equatable {
  /// The message's sender device DID matches our local device DID. We
  /// already have the plaintext (cached at send time); persisting again
  /// would either dedupe via the message ID or — worse — produce a stale
  /// "Cannot decrypt own message" placeholder.
  case ownMessage

  /// A commit message we produced locally, fanned back to us via SSE /
  /// WebSocket. Our FFI state already reflects the commit; we recorded
  /// the hash in `ownCommits` at send time and `isOwnCommit(_:)`
  /// short-circuited `processCommit`.
  case ownCommitConfirmed

  /// The message's epoch is strictly older than the group's current
  /// epoch. We've already moved past this message's epoch via a commit
  /// (or several), so the ratchet key for decryption is gone. There is
  /// no recovery path for this; we silently drop.
  case wrongEpochOldMessage(messageEpoch: UInt64, groupEpoch: UInt64)

  /// We've already processed this exact commit hash during the current
  /// process lifetime (recorded by `MergedCommitTracker`). Redelivery
  /// from the server should not produce a duplicate row.
  case alreadyMerged

  /// The message predates our join epoch — we joined the group at some
  /// epoch `joinedAtEpoch`, and this message comes from an epoch where
  /// we weren't a member yet. We cannot decrypt and should NOT persist
  /// a placeholder; the UI can render a "you joined the conversation"
  /// affordance at `joinedAtEpoch` if it wants context.
  case preJoinGap(joinedAtEpoch: UInt64)

  // MARK: - Reserved for Phase E.iOS — DO NOT UNCOMMENT HERE.
  //
  // case awaitingReissue
  //   The producer wave (Phase E.iOS) will wire this case when it lands
  //   `WelcomeRecoveryDecision.RequestReissue`. Adding the case here
  //   without the producer logic would create an unhandled-case warning
  //   on every `switch SkipReason` site, forcing E.iOS to retrofit them.
  //
  // case surrenderedAtRecipient
  //   Likewise reserved for E.iOS's `Surrender` arm. Surrendering is
  //   the "this device is giving up its slot in the group" action;
  //   subsequent messages should not produce error rows because the
  //   user has explicitly opted out of seeing them.
}

/// Inputs to `PersistDecisionPolicy.decide(...)`. Kept as a flat struct so
/// the policy function is pure and easy to test — no need to construct a
/// real `MessageView` / `MLSConversationManager` to exercise the logic.
public struct PersistDecisionInput: Sendable, Equatable {
  /// The sender's device DID as reported by the message metadata. This
  /// is the per-device DID (e.g. `did:plc:alice#dev1`), not the base
  /// account DID — the difference matters for cross-device echoes from
  /// the same user.
  public let senderDeviceDid: String

  /// The current device's DID at the time of processing. Compared
  /// against `senderDeviceDid` to detect own messages.
  public let localDeviceDid: String

  /// The message's MLS epoch as reported by the server (`MessageView.epoch`).
  public let messageEpoch: UInt64

  /// The group's current MLS epoch as reported by the FFI (`MlsContext.getEpoch`).
  public let groupEpoch: UInt64

  /// Whether the FFI decrypt attempt succeeded. The policy does NOT
  /// itself attempt decryption; it consumes the result.
  public let decryptSucceeded: Bool

  /// Whether this is a commit message rather than an application
  /// message. Affects whether own-sender means `.ownMessage` (app) vs
  /// `.ownCommitConfirmed` (commit).
  public let isCommit: Bool

  /// Whether the caller has already populated a placeholder payload
  /// (e.g. via `recoverSelfSentMessage` adopting a pre-cached send).
  /// Reserved for future use; in D-S.4 only consulted via the
  /// `decryptSucceeded` axis.
  public let hasPlaceholderPayload: Bool

  public init(
    senderDeviceDid: String,
    localDeviceDid: String,
    messageEpoch: UInt64,
    groupEpoch: UInt64,
    decryptSucceeded: Bool,
    isCommit: Bool,
    hasPlaceholderPayload: Bool
  ) {
    self.senderDeviceDid = senderDeviceDid
    self.localDeviceDid = localDeviceDid
    self.messageEpoch = messageEpoch
    self.groupEpoch = groupEpoch
    self.decryptSucceeded = decryptSucceeded
    self.isCommit = isCommit
    self.hasPlaceholderPayload = hasPlaceholderPayload
  }
}

/// Pure-function policy for `PersistDecision`. The decision is fully
/// determined by `PersistDecisionInput` — no I/O, no FFI calls, no clock
/// reads. This makes the decision trivially testable and easy to reason
/// about in isolation from the surrounding atomic-block dispatch.
public enum PersistDecisionPolicy {

  /// Compute the persistence decision for one message processing attempt.
  ///
  /// Decision order (first match wins):
  ///   1. Sender is our own device → `.skipPersist(.ownMessage)` for app,
  ///      `.skipPersist(.ownCommitConfirmed)` for commit.
  ///   2. Message epoch < group epoch → `.skipPersist(.wrongEpochOldMessage)`.
  ///   3. Message epoch > group epoch → `.skipPersist(.preJoinGap)` —
  ///      we don't yet know enough to distinguish "future epoch" (handled
  ///      by buffering upstream) from "you joined later" (genuine
  ///      pre-join gap). Treating both as skip-not-error is the safe
  ///      default.
  ///   4. Decrypt succeeded → `.persist`.
  ///   5. Decrypt failed at the current epoch → `.surfaceError(.decryptFailed)`
  ///      so the UI can show an actionable affordance.
  public static func decide(_ input: PersistDecisionInput) -> PersistDecision {
    // Rule 1: our own message? Skip without persisting.
    // The sender DID is extracted from the MLS AAD after decrypt; callers
    // at sites that don't yet have a decrypt result (wrong-epoch catch,
    // etc.) pass an empty string. We refuse to match an empty sender
    // against an empty local DID — both being absent means "we don't know
    // who sent this", not "we sent it ourselves".
    let normalizedSender = normalize(input.senderDeviceDid)
    let normalizedLocal = normalize(input.localDeviceDid)
    if !normalizedSender.isEmpty,
       !normalizedLocal.isEmpty,
       normalizedSender == normalizedLocal {
      return .skipPersist(reason: input.isCommit ? .ownCommitConfirmed : .ownMessage)
    }

    // Rule 2: strictly older epoch — message is from before our last
    // commit; key material is gone.
    if input.messageEpoch < input.groupEpoch {
      return .skipPersist(
        reason: .wrongEpochOldMessage(
          messageEpoch: input.messageEpoch,
          groupEpoch: input.groupEpoch
        )
      )
    }

    // Rule 3: strictly newer epoch — message is from an epoch we haven't
    // caught up to yet. Upstream ordering logic should have buffered
    // this; if it slipped through, we still must NOT persist an error
    // row. `preJoinGap` is the closest available label.
    if input.messageEpoch > input.groupEpoch {
      return .skipPersist(reason: .preJoinGap(joinedAtEpoch: input.groupEpoch))
    }

    // Rule 4: same epoch + decrypt succeeded -> persist.
    if input.decryptSucceeded {
      return .persist
    }

    // Rule 5: same epoch + decrypt failed -> surface the error so UI can
    // show an actionable affordance (reissue available, etc.).
    return .surfaceError(.decryptFailed)
  }

  /// Lowercase + trim a DID for comparison. The transport may have
  /// re-encoded the device DID with mixed case (DPoP / WebSocket frame
  /// re-encoding has been observed to do this for the device-suffix
  /// portion). Comparing case-insensitively prevents a same-device
  /// echo from being treated as a peer message.
  private static func normalize(_ did: String) -> String {
    did.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
  }
}

/// Typed errors that `PersistDecision.surfaceError` can carry. Each variant
/// must have a UI affordance — there is no "generic error" case, because
/// the whole point of `.surfaceError` (vs. `.skipPersist`) is that the
/// user can act on this.
public enum MLSProcessError: Sendable, Equatable {
  /// FFI decrypt failed at the current epoch with no recovery hint.
  /// UI typically renders this as "couldn't read message — tap to
  /// request reissue" if reissue is available in the group's policy.
  case decryptFailed
}
