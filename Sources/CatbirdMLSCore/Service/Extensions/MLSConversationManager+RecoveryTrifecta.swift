import Foundation
import OSLog

// MARK: - Phase 2 Stage 3: Trifecta D1 Dispatch
//
// Implements the operationally-unrecoverable trifecta detection layered on top
// of the per-conversation MLSRecoveryManager attempt counter. The trifecta is
// a fast-path that fires Mode B (`group_state_unrecoverable`) reports the
// moment three operationally-unrecoverable rejoin attempts cluster within
// 10 minutes for the same convo — without waiting for
// MAX_REJOIN_ATTEMPTS to exhaust on the slow path.
//
// Spec reference: docs/superpowers/specs/2026-04-26-mls-auto-reset-phase2-design.md
// (§"Client-Side Changes → iOS"). ADR-008 D1 mode definitions live in
// MLS_CLIENT_PROTOCOL.md §8.6.1.
//
// IMPORTANT: this fast-path does NOT replace the slow-path dispatch in
// MLSRecoveryManager.recordFailedRejoin. Both paths coexist; the server
// dedupes by (did, convoId, failureMode). A reader who concludes this
// fast-path is redundant should read this comment first.

extension MLSConversationManager {
  /// Pure threshold evaluator for the trifecta sliding window.
  ///
  /// Filters `window` to entries newer than `now - windowSeconds`, appends
  /// `now`, and returns whether the resulting count meets `threshold`. The
  /// caller decides what to do with the new window (typically: persist if
  /// `shouldDispatch` is false; clear after dispatch).
  ///
  /// Pure & static so unit tests can exercise it without standing up a
  /// `MLSConversationManager` (matches the package's test convention —
  /// see `MLSRecoveryManager.cooldownForAttempts` and
  /// `MLSConversationManager.evaluateRejoinGate`).
  static func evaluateTrifectaThreshold(
    window: [Date],
    now: Date,
    windowSeconds: TimeInterval = MLSConversationManager.trifectaWindow,
    threshold: Int = MLSConversationManager.trifectaThreshold
  ) -> (newWindow: [Date], shouldDispatch: Bool) {
    let cutoff = now.addingTimeInterval(-windowSeconds)
    var pruned = window.filter { $0 > cutoff }
    pruned.append(now)
    return (newWindow: pruned, shouldDispatch: pruned.count >= threshold)
  }

  /// Record a single trifecta hit for `convoId` and dispatch a D1 Mode B
  /// report if the threshold is met within the sliding window.
  ///
  /// Caller (typically `attemptRejoinWithWelcomeFallback` after the EC
  /// catch block detects 409/stale-GroupInfo on top of a missing-Welcome
  /// signal) is responsible for confirming the trifecta conditions before
  /// invoking this method, and for capturing `epochAuthenticatorHex`
  /// upstream of the `deleteGroup` call (see Sync.swift / Lifecycle.swift
  /// `preDeleteAuthHex` capture pattern).
  ///
  /// On dispatch, the window is cleared so subsequent attempts must
  /// re-accumulate before another report fires (server-side dedup is the
  /// secondary guard).
  internal func recordTrifectaFailure(
    convoId: String,
    epochAuthenticatorHex: String? = nil
  ) async {
    let now = Date()
    let shouldDispatch: Bool = {
      recoveryFailureWindowLock.lock()
      defer { recoveryFailureWindowLock.unlock() }
      let prior = recoveryFailureWindow[convoId, default: []]
      let (newWindow, dispatch) = Self.evaluateTrifectaThreshold(window: prior, now: now)
      if dispatch {
        // Reset the window post-dispatch so we don't spam the server even
        // if the recovery loop continues to hit the trifecta on subsequent
        // ticks (the in-flight Mode B vote has already been cast).
        recoveryFailureWindow[convoId] = []
      } else {
        recoveryFailureWindow[convoId] = newWindow
      }
      return dispatch
    }()

    if !shouldDispatch {
      logger.info(
        "📊 [D1-TRIFECTA] Recorded trifecta hit for \(convoId.prefix(16)) — below threshold (\(self.recoveryFailureWindow[convoId]?.count ?? 0)/\(MLSConversationManager.trifectaThreshold))"
      )
      return
    }

    logger.warning(
      "🚨 [D1-TRIFECTA] Threshold reached for \(convoId.prefix(16)) — dispatching Mode B (group_state_unrecoverable) report (auth=\(epochAuthenticatorHex != nil ? "present" : "nil"))"
    )
    await dispatchD1Report(convoId: convoId, epochAuthenticatorHex: epochAuthenticatorHex)
  }

  /// Send a Mode B (`group_state_unrecoverable`) recovery-failure report
  /// to the server. Non-fatal: errors are logged and swallowed so the
  /// recovery loop continues regardless. The Stage 4 server-side sweep
  /// will eventually catch convos whose initial dispatch dropped on the
  /// wire.
  ///
  /// **Authenticator semantics** (per
  /// `mls-ds/server/src/handlers/mls_chat/report_recovery_failure.rs:167-190`):
  /// when `epochAuthenticatorHex` is nil/empty, the server short-circuits
  /// before dispatching to the ConversationActor and returns
  /// `recorded: false, reason: "missing_authenticator"` — the call does
  /// NOT count toward quorum. For DM auto-reset to fire on a single
  /// report from this client (`quorum_threshold_dm = 1`), callers MUST
  /// pass a non-nil hex authenticator captured from the pre-delete local
  /// group state via `MLSClient.epochAuthenticatorHex(for:groupId:)`.
  internal func dispatchD1Report(
    convoId: String,
    epochAuthenticatorHex: String? = nil
  ) async {
    do {
      let output = try await apiClient.reportRecoveryFailure(
        convoId: convoId,
        failureMode: "group_state_unrecoverable",
        failureType: "trifecta_external_commit_409",
        epochAuthenticator: epochAuthenticatorHex
      )
      logger.info(
        "✅ [D1-TRIFECTA] Mode B report dispatched for \(convoId.prefix(16)) — recorded=\(output.recorded), autoReset=\(output.autoResetTriggered), reason=\(output.reason ?? "nil")"
      )
      if epochAuthenticatorHex == nil {
        logger.warning(
          "⚠️ [D1-TRIFECTA] Mode B dispatched without authenticator for \(convoId.prefix(16)) — server short-circuits as missing_authenticator and the vote will NOT count toward DM quorum (server reset-vote pyramid: see `report_recovery_failure.rs:167-190`)"
        )
      }
    } catch {
      logger.error(
        "⚠️ [D1-TRIFECTA] Mode B report failed for \(convoId.prefix(16)): \(error.localizedDescription) — non-fatal, server sweep will catch it"
      )
    }
  }

  /// Sentinel reason string emitted by `classifyWelcomeRejoinFailure` in
  /// `MLSConversationManager+Lifecycle.swift` when the server returns 200
  /// for the Welcome fetch but the response body has no `welcome` blob.
  /// This is distinct from 404/410 (device-sync signals) and is the only
  /// Welcome-failure variant that satisfies the trifecta's
  /// "Welcome fetch returned 200 with no welcome blob" condition.
  internal static let trifectaMissingWelcomeMarker =
    "Welcome unavailable (missing welcome in 200 response)"

  /// Whether `fallbackReason` (from `WelcomeRejoinResult.fallbackToExternalCommit`)
  /// indicates the server returned a 200 with no Welcome blob — the
  /// "Welcome staged but empty" sentinel that satisfies trifecta condition #2.
  ///
  /// Pure helper; safe to unit-test without instantiating the manager.
  static func isTrifectaWelcomeReason(_ fallbackReason: String) -> Bool {
    return fallbackReason.contains("missing welcome in 200 response")
  }

  /// Whether `error` thrown by `attemptExternalCommitFallback` matches the
  /// EC failure modes that satisfy trifecta condition #3 — HTTP 409
  /// (epoch race) or a stale-GroupInfo error.
  ///
  /// Pure helper; safe to unit-test without instantiating the manager.
  static func isTrifectaExternalCommitError(_ error: Error) -> Bool {
    if let apiError = error as? MLSAPIError,
      case .httpError(let code, _) = apiError,
      code == 409
    {
      return true
    }
    let message = error.localizedDescription.lowercased()
    if message.contains("groupinfo expired") || message.contains("stale")
      || message.contains("expired")
    {
      return true
    }
    return false
  }
}
