import Foundation
import OSLog
import Petrel

/// Manages silent auto-recovery from MLS desync situations
///
/// When key package desync is detected (local vs server mismatch) or other
/// irrecoverable MLS errors occur, this manager silently:
/// 1. Deletes device from server
/// 2. Clears local MLS storage
/// 3. Re-registers device with fresh key packages
/// 4. Persists affected conversations for deferred rejoin recovery
/// 5. Leaves External Commit attempts to the conversation recovery queue
@available(iOS 18.0, macOS 13.0, *)
public actor MLSRecoveryManager {
  public typealias DeferredRejoinHandler = @Sendable (
    _ userDid: String,
    _ conversationIds: [String]
  ) async -> Void

  // MARK: - Properties

  private let mlsClient: MLSClient
  private let mlsAPIClient: MLSAPIClient
  private let logger = Logger(subsystem: "blue.catbird", category: "MLSRecoveryManager")

  /// Tracks users currently undergoing recovery to prevent concurrent operations
  private var recoveryInProgress: Set<String> = []

  /// Persists rejoin intent in the app-owned conversation database.
  ///
  /// MLSRecoveryManager intentionally does not own GRDB access or launch
  /// External Commits directly. The app layer wires this handler so automatic
  /// recovery flows through the same persisted `needsRejoin` queue as sync-time
  /// epoch recovery.
  private var deferredRejoinHandler: DeferredRejoinHandler?

  // MARK: - Per-Conversation Recovery Tracking (Prevents Infinite Loops)

  /// Tracks failed rejoins per conversation to prevent infinite recovery loops.
  ///
  /// This is an **internal implementation detail** of the
  /// `ConversationRecoveryState` machine — callers observing state should go
  /// through `recoveryState(for:)` and the transition helpers, not read
  /// `failedRejoins` directly. Kept here (instead of rolled into the enum)
  /// because the spec §8.2 counter semantics (5 commit failures, 3 decryption
  /// failures, 3 rejoin attempts) are orthogonal to state identity.
  private var failedRejoins: [String: (attempts: Int, lastAttempt: Date)] = [:]

  /// Transient recovery state for conversations (per spec §8.1). Only holds
  /// the non-persisted states — `.epochBehind`, `.groupMissing`, `.recovering`.
  /// The absence of an entry means "defer to the persisted state on the
  /// conversation model, or `.healthy` if none." Persisted states
  /// (`.needsRejoin`, `.unrecoverableLocal`, `.resetPending`) live in GRDB
  /// via `MLSConversationModel` boolean columns and are read via the model's
  /// `persistedRecoveryState` computed property.
  ///
  /// Upholds invariants S1 / S1.1 (`docs/program/01-INVARIANTS.md`) by making
  /// the recovery state machine the single source of truth for "can this
  /// conversation attempt recovery right now?"
  private var transientStates: [String: ConversationRecoveryState] = [:]

  /// Maximum rejoin attempts per conversation before giving up. Spec §8.2:
  /// "attempts >= MAX_REJOIN_ATTEMPTS → UNRECOVERABLE_LOCAL".
  private let maxRejoinAttempts = 3

  /// Threshold for transitioning `.healthy → .needsRejoin` on repeated commit
  /// failures. Spec §8.2: "5 consecutive commit failures".
  private let commitFailureThreshold = 5

  /// Threshold for transitioning `.healthy → .needsRejoin` on repeated
  /// decryption failures. Spec §8.2: "3 decryption failures".
  private let decryptionFailureThreshold = 3

  /// Per-conversation commit failure counter (transitions into `.needsRejoin`
  /// once it hits `commitFailureThreshold`).
  private var commitFailureCounts: [String: Int] = [:]

  /// Per-conversation decryption failure counter (transitions into
  /// `.needsRejoin` once it hits `decryptionFailureThreshold`).
  private var decryptionFailureCounts: [String: Int] = [:]

  // MARK: - GroupInfo 404 Circuit Breaker (Spec §8.3, §10)

  /// Tracks consecutive GroupInfo 404 responses per conversation.
  /// Spec §10: GROUPINFO_404_CIRCUIT_BREAKER = 3
  private var groupInfo404Counts: [String: Int] = [:]

  /// Maximum consecutive 404s before tripping the circuit breaker
  private let groupInfo404MaxStrikes = 3

  // MARK: - Global Rejoin Floor (Spec §8.4 / §10)

  /// GLOBAL minimum interval between External Commit rejoin attempts across
  /// ALL conversations. Mirrors the Rust `RecoveryTracker.last_global_rejoin_at`
  /// invariant from spec §8.4 / §10 (`MIN_REJOIN_INTERVAL_SEC = 30`). Without
  /// this floor, multiple conversations going `.needsRejoin` simultaneously
  /// can each fire External Commits within milliseconds, inflating epoch
  /// counters and burning server quota.
  ///
  /// Per-conversation cooldown (`failedRejoins` + `cooldownForAttempts`) and
  /// the one-at-a-time gating still apply on top of this floor — this is an
  /// additional cross-conversation guard, not a replacement.
  static let minGlobalRejoinIntervalSec: TimeInterval = 30

  /// In-memory timestamp of the most recent External Commit rejoin attempt
  /// (success or failure) across all conversations. Set inside
  /// `shouldSkipRejoin` at the moment the gate decides to proceed, so both
  /// success and failure paths share the same cooldown. In-memory only —
  /// matches the Rust core; on app restart the natural cooldown applies
  /// from the next attempt.
  private var lastGlobalRejoinAttemptAt: Date?

  /// Calculate exponential backoff cooldown based on attempt count
  /// - Parameter attempts: Number of failed attempts so far
  /// - Returns: Cooldown duration in seconds
  internal static func cooldownForAttempts(_ attempts: Int) -> TimeInterval {
    switch attempts {
    case 0:
      return 0  // First attempt: no cooldown
    case 1:
      return 30  // After 1st failure: 30 seconds
    case 2:
      return 120  // After 2nd failure: 2 minutes
    case 3:
      return 600  // After 3rd failure: 10 minutes
    default:
      return 3600  // After 4+ failures: 1 hour
    }
  }

  internal static func isInBackoffCooldown(
    attempts: Int,
    lastAttempt: Date,
    now: Date
  ) -> Bool {
    guard attempts > 0 else { return false }
    let cooldown = cooldownForAttempts(attempts)
    let elapsed = now.timeIntervalSince(lastAttempt)
    return elapsed < cooldown
  }

  // MARK: - Initialization

  public init(mlsClient: MLSClient, mlsAPIClient: MLSAPIClient) {
    self.mlsClient = mlsClient
    self.mlsAPIClient = mlsAPIClient
  }

  public func setDeferredRejoinHandler(_ handler: DeferredRejoinHandler?) {
    deferredRejoinHandler = handler
  }

  // MARK: - ConversationRecoveryState (Spec §8.1 / Invariants S1, S1.1)

  /// Return the fully-resolved recovery state for a conversation, overlaying
  /// transient in-memory state on top of whatever persistent state the model
  /// carries. Transient state takes precedence — e.g. an in-flight
  /// `.recovering` supersedes a persisted `.needsRejoin`.
  ///
  /// - Parameters:
  ///   - convoId: Conversation identifier.
  ///   - model: The persisted `MLSConversationModel` if known. Pass `nil` to
  ///     query transient state only (e.g. during inline recovery before the
  ///     model row exists).
  /// - Returns: The current `ConversationRecoveryState` per spec §8.1.
  public func recoveryState(
    for convoId: String,
    model: MLSConversationModel? = nil
  ) -> ConversationRecoveryState {
    if let transient = transientStates[convoId] {
      return transient
    }
    return model?.persistedRecoveryState ?? .healthy
  }

  /// Transition a conversation's transient recovery state.
  ///
  /// Only transient states (`.epochBehind`, `.groupMissing`, `.recovering`)
  /// and `.healthy` may be set via this method — persistent states are owned
  /// by the GRDB layer (`MLSConversationModel.needsRejoin`/`needsReset`/
  /// `isUnrecoverable`) and should be written directly via the DB. Attempts
  /// to set a persistent state here are rejected so the two sources of truth
  /// don't disagree.
  ///
  /// The transition is validated against §8.2 via
  /// `ConversationRecoveryState.canTransition(to:)`. Illegal transitions are
  /// logged and dropped.
  ///
  /// - Parameters:
  ///   - convoId: Conversation identifier.
  ///   - next: Target transient state.
  ///   - currentPersistedState: The current persisted state from the model, if
  ///     known. Used as the "from" state when no transient state is set.
  /// - Returns: `true` if the transition was applied, `false` if rejected.
  @discardableResult
  public func setTransientState(
    convoId: String,
    to next: ConversationRecoveryState,
    currentPersistedState: ConversationRecoveryState = .healthy
  ) -> Bool {
    guard !next.isPersisted || next == .healthy else {
      logger.warning(
        "⚠️ [RecoveryState] Refusing to set persistent state \(next.rawValue) via transient API for \(convoId.prefix(16)) — use DB write instead"
      )
      return false
    }

    let current = transientStates[convoId] ?? currentPersistedState
    guard current.canTransition(to: next) else {
      logger.warning(
        "⚠️ [RecoveryState] Illegal transition \(current.rawValue) → \(next.rawValue) for \(convoId.prefix(16))"
      )
      return false
    }

    if next == .healthy {
      transientStates.removeValue(forKey: convoId)
    } else {
      transientStates[convoId] = next
    }
    logger.debug(
      "🔁 [RecoveryState] \(convoId.prefix(16)): \(current.rawValue) → \(next.rawValue)")
    return true
  }

  /// Clear any transient state for a conversation, returning it to the
  /// persisted baseline (`.healthy` unless the model says otherwise).
  public func clearTransientState(convoId: String) {
    if transientStates.removeValue(forKey: convoId) != nil {
      logger.debug("🧹 [RecoveryState] Cleared transient state for \(convoId.prefix(16))")
    }
  }

  /// Record a commit failure. After `commitFailureThreshold` consecutive
  /// failures (spec §8.2: 5), the conversation is flagged `.needsRejoin`.
  ///
  /// - Parameter convoId: Conversation identifier.
  /// - Returns: `true` if the threshold was reached on this call (caller
  ///   should persist `needsRejoin = true` to the model).
  @discardableResult
  public func recordCommitFailure(convoId: String) -> Bool {
    let count = (commitFailureCounts[convoId] ?? 0) + 1
    commitFailureCounts[convoId] = count
    logger.debug(
      "📝 [RecoveryState] Commit failure \(count)/\(self.commitFailureThreshold) for \(convoId.prefix(16))"
    )

    if count >= commitFailureThreshold {
      logger.warning(
        "🔄 [RecoveryState] Commit failure threshold reached for \(convoId.prefix(16)) — flagging .needsRejoin"
      )
      commitFailureCounts[convoId] = 0  // reset to allow the next cycle
      // Transient state moves to .epochBehind and the caller is expected to
      // persist `.needsRejoin` on the model row. Transient state is cleared
      // so the persistent state can take over without a stale shadow.
      transientStates.removeValue(forKey: convoId)
      return true
    }
    return false
  }

  /// Record a decryption failure. After `decryptionFailureThreshold`
  /// consecutive failures (spec §8.2: 3), the conversation is flagged
  /// `.needsRejoin`.
  ///
  /// - Parameter convoId: Conversation identifier.
  /// - Returns: `true` if the threshold was reached on this call (caller
  ///   should persist `needsRejoin = true` to the model).
  @discardableResult
  public func recordDecryptionFailure(convoId: String) -> Bool {
    let count = (decryptionFailureCounts[convoId] ?? 0) + 1
    decryptionFailureCounts[convoId] = count
    logger.debug(
      "📝 [RecoveryState] Decryption failure \(count)/\(self.decryptionFailureThreshold) for \(convoId.prefix(16))"
    )

    if count >= decryptionFailureThreshold {
      logger.warning(
        "🔄 [RecoveryState] Decryption failure threshold reached for \(convoId.prefix(16)) — flagging .needsRejoin"
      )
      decryptionFailureCounts[convoId] = 0
      transientStates.removeValue(forKey: convoId)
      return true
    }
    return false
  }

  /// Reset per-conversation failure counters after a successful operation.
  /// Called whenever a commit/decrypt succeeds to prevent aging failures from
  /// accumulating indefinitely.
  public func clearFailureCounters(convoId: String) {
    commitFailureCounts.removeValue(forKey: convoId)
    decryptionFailureCounts.removeValue(forKey: convoId)
  }

  /// Current commit failure counter (primarily for diagnostics and tests).
  public func commitFailureCount(convoId: String) -> Int {
    commitFailureCounts[convoId] ?? 0
  }

  /// Current decryption failure counter (primarily for diagnostics and tests).
  public func decryptionFailureCount(convoId: String) -> Int {
    decryptionFailureCounts[convoId] ?? 0
  }

  /// Check whether the given persisted-then-transient state permits a recovery
  /// attempt right now (ignoring cooldowns, which are checked separately via
  /// `shouldSkipRejoin`). Used by the deferred recovery hook to filter
  /// candidates.
  public func canAttemptRecovery(
    convoId: String,
    persistedState: ConversationRecoveryState
  ) -> Bool {
    let resolved = transientStates[convoId] ?? persistedState
    return resolved.allowsRecoveryAttempt
  }

  // MARK: - GroupInfo 404 Circuit Breaker (Spec §8.3)

  /// Check if the GroupInfo 404 circuit breaker is tripped for a conversation.
  /// Spec §8.3: "IF tripped (>= GROUPINFO_404_CIRCUIT_BREAKER consecutive 404s): return FAILED"
  /// - Parameter convoId: Conversation identifier
  /// - Returns: `true` if the circuit breaker is tripped (should skip External Commit attempt)
  public func isGroupInfo404CircuitBreakerTripped(convoId: String) -> Bool {
    let count = groupInfo404Counts[convoId] ?? 0
    let maxStrikes = self.groupInfo404MaxStrikes
    if count >= maxStrikes {
      logger.warning(
        "🚫 [GroupInfo404CB] Circuit breaker TRIPPED for \(convoId.prefix(16)) — \(count) consecutive 404s (max: \(maxStrikes))"
      )
      return true
    }
    return false
  }

  /// Record a GroupInfo 404 response for a conversation.
  /// - Parameter convoId: Conversation identifier
  public func recordGroupInfo404(convoId: String) {
    let current = groupInfo404Counts[convoId] ?? 0
    groupInfo404Counts[convoId] = current + 1
    let maxStrikes = self.groupInfo404MaxStrikes
    logger.warning(
      "📝 [GroupInfo404CB] Recorded 404 for \(convoId.prefix(16)) — now \(current + 1)/\(maxStrikes)"
    )
  }

  /// Clear the GroupInfo 404 counter for a conversation (on successful GroupInfo fetch).
  /// - Parameter convoId: Conversation identifier
  public func clearGroupInfo404(convoId: String) {
    if groupInfo404Counts.removeValue(forKey: convoId) != nil {
      logger.info("✅ [GroupInfo404CB] Cleared 404 counter for \(convoId.prefix(16))")
    }
  }

  // MARK: - Per-Conversation Rejoin Tracking

  /// Check if a conversation should be skipped during rejoin (max attempts exceeded or on cooldown)
  ///
  /// Layered gates (each can short-circuit to skip):
  /// 1. Per-conversation max attempts (spec §8.2 — MAX_REJOIN_ATTEMPTS = 3)
  /// 2. Per-conversation exponential backoff (30s/2m/10m/1h)
  /// 3. **Global** rejoin floor (spec §8.4 / §10 — MIN_REJOIN_INTERVAL_SEC = 30,
  ///    applies across all conversations, mirrors Rust
  ///    `RecoveryTracker.last_global_rejoin_at`)
  ///
  /// When the gate decides to proceed (returns `false`), the global timestamp
  /// is stamped immediately so both success and failure paths share the same
  /// cooldown — failure does not allow immediate retry.
  public func shouldSkipRejoin(convoId: String) -> Bool {
    if let record = failedRejoins[convoId] {
      // Skip if max attempts exceeded
      if record.attempts >= maxRejoinAttempts {
        logger.info(
          "⏭️ [MLSRecoveryManager] Skipping \(convoId.prefix(16)) - max attempts (\(self.maxRejoinAttempts)) exceeded"
        )
        return true
      }

      // Skip if on cooldown (exponential backoff based on attempts)
      let now = Date()
      if Self.isInBackoffCooldown(
        attempts: record.attempts,
        lastAttempt: record.lastAttempt,
        now: now
      ) {
        let cooldown = Self.cooldownForAttempts(record.attempts)
        let elapsed = now.timeIntervalSince(record.lastAttempt)
        let remaining = Int(cooldown - elapsed)
        logger.info(
          "⏭️ [MLSRecoveryManager] Skipping \(convoId.prefix(16)) - on cooldown (\(remaining)s remaining, attempt \(record.attempts))"
        )
        return true
      }
    }

    // Global rejoin floor (spec §8.4 / §10) — applies across ALL conversations.
    // Prevents two convos going NEEDS_REJOIN simultaneously from both firing
    // External Commits within milliseconds.
    let now = Date()
    if let lastGlobal = lastGlobalRejoinAttemptAt {
      let elapsed = now.timeIntervalSince(lastGlobal)
      if elapsed < Self.minGlobalRejoinIntervalSec {
        let remaining = Int(Self.minGlobalRejoinIntervalSec - elapsed)
        logger.info(
          "⏭️ [MLSRecoveryManager] Skipping \(convoId.prefix(16)) - global rejoin floor active (\(remaining)s remaining)"
        )
        return true
      }
    }

    // Gate decided to proceed: stamp the global timestamp BEFORE the External
    // Commit is issued. Both success and failure paths now share the cooldown,
    // so a failed attempt cannot immediately retry across other conversations.
    lastGlobalRejoinAttemptAt = now
    return false
  }

  /// Record a failed rejoin attempt for a conversation.
  ///
  /// - Parameters:
  ///   - convoId: The conversation identifier whose rejoin attempt failed.
  ///   - epochAuthenticatorHex: Optional hex-encoded RFC 9420 §8.7
  ///     `epoch_authenticator` for the reporter's current view of the group.
  ///     Required for the server-side A7 reset-vote pyramid to count this
  ///     client's vote toward quorum — per
  ///     `mls-ds/server/src/handlers/mls_chat/report_recovery_failure.rs:141-159`,
  ///     calls without this field succeed (HTTP 200) but are short-circuited
  ///     as `reason: "missing_authenticator"` and do not contribute to
  ///     quorum. Callers should compute it via
  ///     `MLSContext.epochAuthenticator(groupId:)` and hex-encode when the
  ///     group is still present locally; pass `nil` only when the group
  ///     state is unavailable (warning log will fire).
  ///   - failureType: Spec §8.6 / ADR-008 D1 failure-type discriminator.
  ///     Defaults to `"external_commit_exhausted"` (Mode A — local rejoin
  ///     attempts gave up). Pass `"remote_data_error"` from call sites that
  ///     observed a corrupt/inconsistent server-fetched payload (Mode B —
  ///     group state is unrecoverable at the network layer).
  public func recordFailedRejoin(
    convoId: String,
    epochAuthenticatorHex: String? = nil,
    failureType: String = "external_commit_exhausted"
  ) {
    let existing = failedRejoins[convoId] ?? (attempts: 0, lastAttempt: Date.distantPast)
    failedRejoins[convoId] = (attempts: existing.attempts + 1, lastAttempt: Date())
    logger.warning(
      "📝 [MLSRecoveryManager] Recorded failed rejoin for \(convoId.prefix(16)) - attempt \(existing.attempts + 1)/\(self.maxRejoinAttempts) failureType=\(failureType)"
    )

    // Spec §8.6: Report to server when transitioning to UNRECOVERABLE_LOCAL
    if existing.attempts + 1 >= maxRejoinAttempts {
      logger.error(
        "🚨 [MLSRecoveryManager] Max rejoin attempts reached for \(convoId.prefix(16)) — reporting to server (failureType=\(failureType))"
      )
      // TODO(§8.1 escalation hardening): this dispatch is fire-and-forget; if
      // the server is unreachable at the exact moment the 3rd attempt
      // exhausts, the escalation is lost with no retry queue. Persist a
      // pending-escalation intent to DB and drain on next sync tick.
      if epochAuthenticatorHex == nil {
        logger.warning(
          "⚠️ [MLSRecoveryManager] reportRecoveryFailure missing epochAuthenticator for \(convoId.prefix(16)) — server will short-circuit vote (reason: missing_authenticator). Caller should pass MLSContext.epochAuthenticator(groupId:) hex."
        )
      }
      let authenticator = epochAuthenticatorHex
      // ADR-008 D1 (spec §8.6.1): mirror the Rust orchestrator's
      // `report_unrecoverable_local` classifier
      // (`catbird-mls/src/orchestrator/recovery.rs:855-865`).
      //   - "remote_data_error"           → Mode B  (group_state_unrecoverable)
      //   - "external_commit_exhausted"   → Mode A  (local_state_loss)
      //   - anything else                 → omit (server treats as Mode A)
      let failureMode: String?
      switch failureType {
      case "remote_data_error":
        failureMode = "group_state_unrecoverable"
      case "external_commit_exhausted":
        failureMode = "local_state_loss"
      default:
        failureMode = nil
      }
      let resolvedFailureType = failureType
      Task {
        do {
          let input = BlueCatbirdMlsChatReportRecoveryFailure.Input(
            convoId: convoId,
            failureType: resolvedFailureType,  // Spec §8.6
            failureMode: failureMode,  // ADR-008 D1
            epochAuthenticator: authenticator
          )
          let (code, output) = try await self.mlsAPIClient.client.blue.catbird.mlschat.reportRecoveryFailure(input: input)
          self.logger.info(
            "📡 [MLSRecoveryManager] Reported recovery failure for \(convoId.prefix(16)) — code=\(code) recorded=\(output?.recorded ?? false) autoReset=\(output?.autoResetTriggered ?? false) authenticator=\(authenticator != nil ? "present" : "nil") failureType=\(resolvedFailureType) failureMode=\(failureMode ?? "nil")"
          )
        } catch {
          // Swallowing this was masking §8.6 escalation failures. Keep the
          // conversation locked out client-side (caller already treats it as
          // UNRECOVERABLE_LOCAL), but surface the root cause at .error so ops
          // can see the reason. Persistent retry-intent (new column + backoff
          // drained on next sync) is tracked separately as task #21.
          self.logger.error(
            "❌ [MLSRecoveryManager] Failed to report recovery failure for \(convoId.prefix(16)): \(error.localizedDescription) — errorType=\(String(describing: type(of: error)))"
          )
        }
      }
    }
  }

  /// Clear rejoin tracking for a conversation (on success).
  ///
  /// Also clears transient state and failure counters so the conversation
  /// is fully returned to `.healthy` (spec §8.2: NEEDS_REJOIN → HEALTHY on
  /// successful External Commit).
  public func clearRejoinTracking(convoId: String) {
    let hadTracking = failedRejoins.removeValue(forKey: convoId) != nil
    transientStates.removeValue(forKey: convoId)
    commitFailureCounts.removeValue(forKey: convoId)
    decryptionFailureCounts.removeValue(forKey: convoId)
    if hadTracking {
      logger.info("✅ [MLSRecoveryManager] Cleared rejoin tracking for \(convoId.prefix(16))")
    }
  }

  /// Get the number of remaining rejoin attempts for a conversation
  public func remainingRejoinAttempts(convoId: String) -> Int {
    guard let record = failedRejoins[convoId] else {
      return maxRejoinAttempts
    }
    return max(0, maxRejoinAttempts - record.attempts)
  }

  /// Load persisted unrecoverable state from database on startup.
  /// This prevents retry loops after app restart for conversations that
  /// already exhausted all rejoin attempts.
  ///
  /// ADR-002 A7 escalation retry: because hydrate sets attempts past
  /// maxRejoinAttempts, `recordFailedRejoin` will never fire again for these
  /// convos, which means the initial `reportRecoveryFailure` dispatch (also
  /// fire-and-forget) is the ONLY shot at getting the server a vote. If that
  /// first attempt failed (network blip, server down at the moment), the
  /// escalation is lost forever — until quorum can't form and the convo is
  /// permanently dead client-side.
  ///
  /// Mitigation: re-fire the escalation on each hydrate (once per app launch
  /// per convo). Server-side is idempotent via 24h per-DID rate limit +
  /// ON CONFLICT DO NOTHING, so duplicates are cheap. Still fire-and-forget
  /// here; a durable pending-escalation queue remains backlog task.
  ///
  /// - Parameters:
  ///   - unrecoverableConvoIds: Convo IDs that crossed MAX_REJOIN_ATTEMPTS in
  ///     a prior session and are persisted as UNRECOVERABLE_LOCAL.
  ///   - epochAuthenticatorsByConvoId: Optional per-convo hex-encoded RFC
  ///     9420 §8.7 `epoch_authenticator`. When non-nil for a convo, the
  ///     server's A7 reset-vote pyramid will count this client's vote toward
  ///     quorum; when nil (or not provided), the call succeeds but the
  ///     server short-circuits as `reason: "missing_authenticator"`. Callers
  ///     should compute via `MLSContext.epochAuthenticator(groupId:)` +
  ///     hex-encode when the group is still present locally.
  public func hydrateFromDatabase(
    unrecoverableConvoIds: [String],
    epochAuthenticatorsByConvoId: [String: String]? = nil
  ) {
    for convoId in unrecoverableConvoIds {
      failedRejoins[convoId] = (attempts: maxRejoinAttempts + 1, lastAttempt: Date.distantPast)
      logger.info("📥 [MLSRecoveryManager] Hydrated unrecoverable state for \(convoId.prefix(16))")

      // Re-fire escalation on every hydrate. Cheap server-side (rate-limited)
      // and ensures a missed first dispatch eventually reaches the server.
      logger.warning(
        "📡 [MLSRecoveryManager] Re-firing recovery escalation on hydrate for \(convoId.prefix(16))"
      )
      let authenticator = epochAuthenticatorsByConvoId?[convoId]
      if authenticator == nil {
        logger.warning(
          "⚠️ [MLSRecoveryManager] hydrate re-fire missing epochAuthenticator for \(convoId.prefix(16)) — server will short-circuit vote (reason: missing_authenticator). Caller should pass MLSContext.epochAuthenticator(groupId:) hex in epochAuthenticatorsByConvoId."
        )
      }
      Task {
        do {
          let input = BlueCatbirdMlsChatReportRecoveryFailure.Input(
            convoId: convoId,
            failureType: "external_commit_exhausted",  // Spec §8.6
            epochAuthenticator: authenticator
          )
          let (code, output) = try await self.mlsAPIClient.client.blue.catbird.mlschat.reportRecoveryFailure(input: input)
          self.logger.info(
            "📡 [MLSRecoveryManager] Re-fired recovery escalation for \(convoId.prefix(16)) — code=\(code) recorded=\(output?.recorded ?? false) autoReset=\(output?.autoResetTriggered ?? false) authenticator=\(authenticator != nil ? "present" : "nil")"
          )
        } catch {
          self.logger.error(
            "❌ [MLSRecoveryManager] Failed to re-fire recovery escalation for \(convoId.prefix(16)): \(error.localizedDescription)"
          )
        }
      }
    }
  }

  // MARK: - Desync Detection

  /// Check if there's a key package desync between local and server
  /// Returns severity level: none, minor (recoverable), severe (requires re-registration)
  ///
  /// Note: This method requires server-side bundle count comparison, which may not always
  /// be available. If server status cannot be retrieved, assumes severe desync to be safe.
  public func checkDesyncSeverity(for userDid: String, serverBundleCount: Int? = nil) async throws
    -> DesyncSeverity
  {
    logger.info("🔍 [MLSRecoveryManager] Checking desync severity for \(userDid.prefix(20))...")

    do {
      // Get local bundle count
      let localCount = try await mlsClient.getKeyPackageBundleCount(for: userDid)

      // Use provided server count or default to unknown (assume severe)
      let serverCount = serverBundleCount ?? 0

      logger.info(
        "📊 [MLSRecoveryManager] Bundle counts - Local: \(localCount), Server: \(serverCount)")

      // If we don't have server count, assume based on local count
      if serverBundleCount == nil {
        if localCount == 0 {
          // CRITICAL FIX: Check if device is even registered before declaring desync
          // "0 bundles" is normal if the device isn't registered yet!
          let isRegistered = await mlsClient.isDeviceRegisteredAsync(for: userDid)

          if !isRegistered {
            logger.info(
              "✅ [MLSRecoveryManager] Device not locally registered - 0 bundles is expected")
            return .none
          }

          logger.error("🚨 [MLSRecoveryManager] No local bundles - severe desync")
          return .severe(localCount: 0, serverCount: 0, difference: 0)
        } else {
          logger.info("✅ [MLSRecoveryManager] Local bundles exist, server count unknown")
          return .none
        }
      }

      // Calculate desync
      let difference = abs(Int(localCount) - serverCount)
      let percentageDiff = localCount > 0 ? Double(difference) / Double(localCount) * 100 : 100

      if difference == 0 {
        logger.info("✅ [MLSRecoveryManager] No desync detected")
        return .none
      } else if percentageDiff < 20 && localCount > 10 {
        // Minor desync - can be fixed by uploading missing packages
        logger.warning(
          "⚠️ [MLSRecoveryManager] Minor desync: \(difference) packages (\(Int(percentageDiff))% difference)"
        )
        return .minor(localCount: Int(localCount), serverCount: serverCount, difference: difference)
      } else {
        // Severe desync - requires full re-registration
        logger.error(
          "🚨 [MLSRecoveryManager] Severe desync: \(difference) packages (\(Int(percentageDiff))% difference)"
        )
        return .severe(
          localCount: Int(localCount), serverCount: serverCount, difference: difference)
      }
    } catch {
      logger.error("❌ [MLSRecoveryManager] Failed to check desync: \(error.localizedDescription)")
      throw error
    }
  }

  // MARK: - Silent Recovery

  /// Perform silent recovery for a user
  /// This is the main entry point for automatic recovery
  public func performSilentRecovery(for userDid: String, conversationIds: [String] = []) async throws {
    logger.info("🔄 [MLSRecoveryManager] Starting silent recovery for \(userDid.prefix(20))...")

    // Prevent concurrent recovery for same user
    guard !recoveryInProgress.contains(userDid) else {
      logger.warning(
        "⚠️ [MLSRecoveryManager] Recovery already in progress for \(userDid.prefix(20))")
      return
    }

    recoveryInProgress.insert(userDid)
    defer { recoveryInProgress.remove(userDid) }

    do {
      // Use the unified reregisterDevice flow which handles:
      // 1. Delete device from server
      // 2. Clear local MLS storage
      // 3. Re-register with fresh key packages
      logger.info(
        "🔄 [MLSRecoveryManager] Step 1/2: Re-registering device (atomic cleanup + registration)...")
      _ = try await mlsClient.reregisterDevice(for: userDid)
      logger.info("✅ [MLSRecoveryManager] Device re-registered successfully")

      // Step 2: Persist conversations for deferred rejoin recovery.
      logger.info(
        "🔄 [MLSRecoveryManager] Step 2/2: Marking \(conversationIds.count) conversations for deferred rejoin..."
      )
      if !conversationIds.isEmpty {
        if let deferredRejoinHandler {
          await deferredRejoinHandler(userDid, conversationIds)
        } else {
          logger.warning(
            "⚠️ [MLSRecoveryManager] No deferred rejoin handler configured; caller must persist needsRejoin for \(conversationIds.count) conversation(s)"
          )
        }
      }

      logger.info("✅ [MLSRecoveryManager] Silent recovery complete for \(userDid.prefix(20))")
    } catch {
      logger.error("❌ [MLSRecoveryManager] Recovery failed: \(error.localizedDescription)")
      throw MLSRecoveryError.recoveryFailed(underlying: error)
    }
  }

  // MARK: - Device Management

  /// Delete device from server
  /// Note: This uses the MLSDeviceManager's reregisterDevice flow which handles
  /// server-side cleanup and re-registration atomically
  private func deleteDeviceFromServer(for userDid: String) async throws {
    logger.debug("🗑️ [MLSRecoveryManager] Device cleanup will be handled during re-registration")
    // Device deletion is handled atomically during re-registration by MLSDeviceManager
    // The reregisterDevice() method in MLSDeviceManager:
    // 1. Deletes existing device from server
    // 2. Clears local key packages
    // 3. Creates fresh key packages
    // 4. Registers new device
    //
    // So we just log here and let the re-registration handle cleanup
    logger.info("✅ [MLSRecoveryManager] Device will be deleted during re-registration")
  }

  // MARK: - Recovery Triggers

  /// Called when NoMatchingKeyPackage error occurs during Welcome processing
  /// Determines if silent recovery should be triggered
  ///
  /// CRITICAL: This method distinguishes between LOCAL corruption (recoverable via re-registration)
  /// and REMOTE/SERVER corruption (NOT recoverable by wiping local state).
  ///
  /// - Parameters:
  ///   - error: The error that occurred
  ///   - userDid: The user DID
  ///   - convoId: Optional conversation ID - if provided, checks if max attempts exceeded
  ///   - isRemoteDataError: If true, the error originated from server-fetched data (GroupInfo, Welcome)
  ///                        and should NOT trigger destructive local recovery
  public func shouldTriggerRecovery(
    for error: Error, userDid: String, convoId: String? = nil, isRemoteDataError: Bool = false
  ) -> Bool {
    let errorString = String(describing: error).lowercased()

    // CRITICAL FIX: Errors from REMOTE data (GroupInfo, Welcome from server) should NOT trigger
    // local database wipe - the problem is on the server, not locally.
    // These errors indicate the server is serving corrupted/truncated data.
    let remoteDataErrorPatterns = [
      "invalidvectorlength",  // GroupInfo deserialization failure
      "endofstream",  // Truncated data from server
      "deseriali",  // General deserialization issues
      "malformed",  // Malformed protocol data
      "truncat",  // Truncated data
    ]

    // If this is flagged as a remote data error, check if the pattern matches remote issues
    if isRemoteDataError {
      for pattern in remoteDataErrorPatterns {
        if errorString.contains(pattern) {
          logger.error(
            "🚫 [MLSRecoveryManager] Detected SERVER-SIDE data corruption: \(pattern)")
          logger.error(
            "   ❌ NOT triggering local recovery - wiping local DB won't fix server data!")
          logger.error(
            "   📋 Conversation \(convoId?.prefix(16) ?? "unknown") should be marked as broken")
          logger.error(
            "   🔧 Server team needs to investigate GroupInfo storage for this conversation")

          // Record this as a failed rejoin to prevent retry loops.
          // ADR-008 D1: server-side data corruption is Mode B — pass
          // `remote_data_error` so the eventual recoveryFailure report sets
          // `failureMode = "group_state_unrecoverable"` (counts toward
          // server-side quorum auto-reset rather than self-heal).
          if let convoId = convoId {
            recordFailedRejoin(convoId: convoId, failureType: "remote_data_error")
          }

          return false
        }
      }
    }

    // Check for known LOCAL errors that ARE recoverable via re-registration
    // These indicate local state corruption, not server issues
    let localRecoverablePatterns = [
      "nomatchingkeypackage",  // Local key package inventory desync
      "keypackagenotfound",  // Key package missing from local storage
      "secretreuseerror",  // Ratchet state corruption
      "sqlite out of memory",  // Local database issues
    ]

    for pattern in localRecoverablePatterns {
      if errorString.contains(pattern) {
        // Check if this specific conversation has exceeded max recovery attempts
        if let convoId = convoId, shouldSkipRejoin(convoId: convoId) {
          logger.warning(
            "🔍 [MLSRecoveryManager] Detected \(pattern) but conversation \(convoId.prefix(16)) exceeded max attempts - not triggering recovery"
          )
          return false
        }

        logger.warning("🔍 [MLSRecoveryManager] Detected LOCAL recovery trigger: \(pattern)")
        return true
      }
    }

    // LEGACY: Check for invalidvectorlength WITHOUT remote flag (for backwards compatibility)
    // Only trigger if NOT explicitly marked as remote data error
    if !isRemoteDataError && errorString.contains("invalidvectorlength") {
      // Log a warning but still allow recovery for backwards compat
      // Callers should set isRemoteDataError=true for External Commit errors
      logger.warning(
        "⚠️ [MLSRecoveryManager] InvalidVectorLength detected without remote flag")
      logger.warning(
        "   Caller should set isRemoteDataError=true if from External Commit/Welcome")

      if let convoId = convoId, shouldSkipRejoin(convoId: convoId) {
        return false
      }
      return true
    }

    return false
  }

  /// Attempt recovery if needed and return whether recovery was performed
  ///
  /// CRITICAL: This method now distinguishes between LOCAL and REMOTE data errors.
  /// Remote data errors (from GroupInfo/Welcome) should NOT trigger destructive recovery.
  ///
  /// - Parameters:
  ///   - error: The error that occurred
  ///   - userDid: The user DID
  ///   - convoIds: Conversation IDs to rejoin after recovery
  ///   - triggeringConvoId: The specific conversation that triggered recovery (for tracking)
  ///   - isRemoteDataError: If true, error originated from server-fetched data (don't wipe local)
  @discardableResult
  public func attemptRecoveryIfNeeded(
    for error: Error,
    userDid: String,
    convoIds: [String] = [],
    triggeringConvoId: String? = nil,
    isRemoteDataError: Bool = false
  ) async -> Bool {
    // Use the triggering convoId if provided, otherwise check first convoId
    let checkConvoId = triggeringConvoId ?? convoIds.first

    guard
      shouldTriggerRecovery(
        for: error, userDid: userDid, convoId: checkConvoId, isRemoteDataError: isRemoteDataError)
    else {
      return false
    }

    logger.info("🔄 [MLSRecoveryManager] Triggering silent recovery due to LOCAL error...")

    do {
      try await performSilentRecovery(for: userDid, conversationIds: convoIds)
      return true
    } catch {
      logger.error("❌ [MLSRecoveryManager] Auto-recovery failed: \(error.localizedDescription)")
      // Record the failure for the triggering conversation
      if let convoId = checkConvoId {
        recordFailedRejoin(convoId: convoId)
      }
      return false
    }
  }

  // MARK: - Server Data Corruption Handling

  /// Mark a conversation as having corrupted server data
  /// This prevents retry loops when the server is serving bad GroupInfo
  /// The conversation will remain inaccessible until server data is fixed
  public func markConversationServerCorrupted(convoId: String, errorMessage: String) {
    logger.error(
      "🚫 [MLSRecoveryManager] Marking conversation \(convoId.prefix(16)) as SERVER-CORRUPTED")
    logger.error("   Error: \(errorMessage)")
    logger.error("   This conversation cannot be joined until server data is repaired")

    // Record max failures immediately to prevent any retry attempts
    failedRejoins[convoId] = (attempts: maxRejoinAttempts + 10, lastAttempt: Date())
  }

  /// Check if a conversation is marked as having server-side corruption
  public func isConversationServerCorrupted(convoId: String) -> Bool {
    guard let record = failedRejoins[convoId] else { return false }
    // Conversations with more than maxRejoinAttempts + 5 are considered server-corrupted
    return record.attempts > maxRejoinAttempts + 5
  }

  // MARK: - GroupInfo Health Check (Fix #4)

  /// 🔒 FIX #4: Verify GroupInfo health after epoch advancement
  ///
  /// Call this after publishing GroupInfo to verify the stored data is valid.
  /// If verification fails, attempts to republish.
  ///
  /// - Parameters:
  ///   - convoId: Conversation identifier
  ///   - expectedSize: Expected minimum size of GroupInfo
  ///   - maxRetries: Maximum republish attempts (default: 2)
  /// - Returns: true if GroupInfo is healthy, false if repair failed
  public func verifyGroupInfoHealth(
    convoId: String,
    expectedSize: Int,
    maxRetries: Int = 2
  ) async -> Bool {
    logger.info(
      "🔍 [MLSRecoveryManager.verifyGroupInfoHealth] Checking GroupInfo for \(convoId.prefix(16))..."
    )

    for attempt in 1...maxRetries {
      do {
        // Fetch stored GroupInfo
        let (storedData, epoch, _) = try await mlsAPIClient.getGroupInfo(convoId: convoId)

        // Check size
        if storedData.count < 100 {
          logger.error(
            "❌ [verifyGroupInfoHealth] GroupInfo too small: \(storedData.count) bytes (attempt \(attempt))"
          )

          if attempt < maxRetries {
            // Request republish from active members
            logger.info("🔄 [verifyGroupInfoHealth] Requesting GroupInfo refresh...")
            _ = try await mlsAPIClient.groupInfoRefresh(convoId: convoId)
            try await Task.sleep(for: .seconds(2))
            continue
          }
          return false
        }

        // Check it's not base64 encoded (should be binary)
        let isAsciiOnly = storedData.allSatisfy { byte in
          (byte >= 0x20 && byte <= 0x7E) || byte == 0x0A || byte == 0x0D
        }
        if isAsciiOnly && storedData.count > 50 {
          logger.error("❌ [verifyGroupInfoHealth] GroupInfo appears to be base64 text, not binary!")
          return false
        }

        // Size matches expectation (with 20% tolerance for different epochs)
        let sizeDiff = abs(storedData.count - expectedSize)
        let tolerance = expectedSize / 5  // 20%
        if sizeDiff > tolerance && expectedSize > 0 {
          logger.warning(
            "⚠️ [verifyGroupInfoHealth] Size mismatch: stored \(storedData.count), expected ~\(expectedSize)"
          )
          // Not a failure, just a warning - epoch changes can affect size
        }

        logger.info(
          "✅ [verifyGroupInfoHealth] GroupInfo healthy - size: \(storedData.count) bytes, epoch: \(epoch)"
        )
        return true

      } catch {
        logger.error(
          "❌ [verifyGroupInfoHealth] Failed to fetch GroupInfo (attempt \(attempt)): \(error.localizedDescription)"
        )
        if attempt < maxRetries {
          try? await Task.sleep(for: .seconds(1))
        }
      }
    }

    logger.error(
      "❌ [verifyGroupInfoHealth] GroupInfo health check FAILED after \(maxRetries) attempts")
    return false
  }
}

// MARK: - Supporting Types

/// Severity level of key package desync
public enum DesyncSeverity {
  case none
  case minor(localCount: Int, serverCount: Int, difference: Int)
  case severe(localCount: Int, serverCount: Int, difference: Int)

  public var isRecoverable: Bool {
    switch self {
    case .none, .minor:
      return true
    case .severe:
      return false
    }
  }
}

/// Errors specific to MLS recovery operations
public enum MLSRecoveryError: LocalizedError {
  case recoveryFailed(underlying: Error)
  case recoveryInProgress
  case deviceDeletionFailed
  case maxRetriesExceeded

  public var errorDescription: String? {
    switch self {
    case .recoveryFailed(let underlying):
      return "MLS recovery failed: \(underlying.localizedDescription)"
    case .recoveryInProgress:
      return "Recovery already in progress"
    case .deviceDeletionFailed:
      return "Failed to delete device from server"
    case .maxRetriesExceeded:
      return "Maximum retry attempts exceeded"
    }
  }
}

// MARK: - SQLite Retry Utility

/// Utility for retrying operations with exponential backoff
/// Used for SQLite and other transient error recovery
@available(iOS 18.0, macOS 13.0, *)
public enum RetryUtility {
  private static let logger = Logger(subsystem: "blue.catbird", category: "RetryUtility")

  /// Execute an async operation with exponential backoff retry
  /// - Parameters:
  ///   - maxAttempts: Maximum number of retry attempts (default: 3)
  ///   - initialDelay: Initial delay in seconds before first retry (default: 0.5)
  ///   - maxDelay: Maximum delay between retries (default: 10 seconds)
  ///   - shouldRetry: Closure to determine if error is retryable
  ///   - operation: The operation to execute
  /// - Returns: The result of the successful operation
  public static func withExponentialBackoff<T>(
    maxAttempts: Int = 3,
    initialDelay: TimeInterval = 0.5,
    maxDelay: TimeInterval = 10,
    shouldRetry: @escaping (Error) -> Bool = isRetryableError,
    operation: () async throws -> T
  ) async throws -> T {
    var lastError: Error?
    var currentDelay = initialDelay

    for attempt in 1...maxAttempts {
      do {
        return try await operation()
      } catch {
        lastError = error

        if !shouldRetry(error) {
          logger.debug(
            "🔄 [Retry] Non-retryable error on attempt \(attempt): \(error.localizedDescription)")
          throw error
        }

        if attempt == maxAttempts {
          logger.error("❌ [Retry] Max attempts (\(maxAttempts)) exceeded")
          break
        }

        logger.warning(
          "⚠️ [Retry] Attempt \(attempt)/\(maxAttempts) failed: \(error.localizedDescription)")
        logger.info("⏳ [Retry] Waiting \(String(format: "%.1f", currentDelay))s before retry...")

        try await Task.sleep(nanoseconds: UInt64(currentDelay * 1_000_000_000))

        // Exponential backoff with jitter
        currentDelay = min(currentDelay * 2 + Double.random(in: 0...0.5), maxDelay)
      }
    }

    throw lastError ?? MLSRecoveryError.maxRetriesExceeded
  }

  /// Determine if an error is likely transient and retryable
  public static func isRetryableError(_ error: Error) -> Bool {
    let errorString = String(describing: error).lowercased()

    // SQLite transient errors
    let retryablePatterns = [
      "sqlite_busy",
      "database is locked",
      "out of memory",
      "unable to open database",
      "disk i/o error",
      "database disk image is malformed",
      "connection refused",
      "network is unreachable",
      "timed out",
      "timeout",
    ]

    for pattern in retryablePatterns {
      if errorString.contains(pattern) {
        return true
      }
    }

    // NSError codes for common transient errors
    if let nsError = error as NSError? {
      switch nsError.code {
      case NSURLErrorTimedOut,
        NSURLErrorNetworkConnectionLost,
        NSURLErrorNotConnectedToInternet:
        return true
      default:
        break
      }
    }

    return false
  }

  /// Execute a storage operation with SQLite-specific retry settings
  public static func withSQLiteRetry<T>(
    operation: () async throws -> T
  ) async throws -> T {
    return try await withExponentialBackoff(
      maxAttempts: 3,
      initialDelay: 0.2,
      maxDelay: 2.0,
      shouldRetry: isRetryableError,
      operation: operation
    )
  }
}
