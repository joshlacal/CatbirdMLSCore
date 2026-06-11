import Foundation
import OSLog
import Petrel
import PetrelCatbird

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

  /// Optional so the rejoin-tracking core stays constructible in tests
  /// (restart-persistence tests recreate the manager on the same DB without
  /// a live MLSClient/MLSAPIClient). Production paths always pass both via
  /// the public initializer; reporting/recovery methods guard loudly.
  private let mlsClient: MLSClient?
  private let mlsAPIClient: MLSAPIClient?
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
  ///
  /// N39 (3-band collapse): this is the SINGLE arming threshold — reaching it
  /// arms the persisted 24h `quarantinedUntil` horizon, exactly like the Rust
  /// twin (`RecoveryTracker::record_failure` arms `lockout_until` at
  /// `count >= max_attempts`, persisted as `quarantined_until_ms`;
  /// `catbird-mls/src/orchestrator/constants.rs` `MAX_REJOIN_ATTEMPTS = 3`).
  /// The old Swift-only 8 (ceiling) and 13 (server-corrupted) bands are
  /// retired; server corruption is now quarantine STATE (`markQuarantined`),
  /// not counter inflation.
  public static let maxRejoinAttempts = 3

  /// Instance alias for `Self.maxRejoinAttempts` (keeps existing call sites
  /// unchanged).
  private var maxRejoinAttempts: Int { Self.maxRejoinAttempts }

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

  // MARK: - Unbridgeable Epoch Gap (bootstrap commit-not-on-wire)

  /// Per-conversation consecutive `serverDataGap` counter from
  /// `fetchAndProcessMissingCommits`. When it returns `.serverDataGap`
  /// repeatedly for the same convo, the local FFI is stuck at an epoch the
  /// wire commit history cannot reach. Typical fingerprint: bootstrap
  /// creator's `confirmCommit` was lost to a context-reload race; the
  /// addMembers commit only ever existed in the Welcome bytes, never on the
  /// wire (`mls-ds/server/src/handlers/mls_chat/bootstrap_reset_group.rs`
  /// has no `INSERT INTO messages`). The only recovery path is a
  /// server-side group reset.
  private var serverDataGapCounts: [String: Int] = [:]

  /// Threshold for transitioning to "report unrecoverable to server" on
  /// repeated `serverDataGap` returns. Matches existing thresholds
  /// (`commitFailureThreshold`, `decryptionFailureThreshold`,
  /// `groupInfo404MaxStrikes`).
  private let serverDataGapMaxStrikes = 3

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

  /// In-memory timestamp of the most recent External Commit rejoin OUTCOME
  /// (success or failure) across all conversations. Stamped only on outcome
  /// paths — `recordFailedRejoin` (failure) and `clearRejoinTracking`
  /// (success) — so `shouldSkipRejoin` remains a pure read. This mirrors the
  /// Rust orchestrator (`catbird-mls/src/orchestrator/recovery.rs` —
  /// `record_failure` / `clear` set `last_global_rejoin_at`, while
  /// `should_skip` only reads it). In-memory only — on app restart the
  /// natural cooldown applies from the next attempt.
  ///
  /// Critical invariant: this MUST NOT be written from `shouldSkipRejoin`.
  /// Writing it inside the gate makes the gate non-idempotent — a second
  /// call within the same logical rejoin attempt (e.g. nested EC fallback
  /// re-entering the gate) would observe the floor it just set and skip,
  /// which would be misclassified as a failed attempt by the caller.
  ///
  /// WS-6.4 (E7, N21): no longer in-memory-only — write-through persisted via
  /// `persistence` on every outcome stamp and rehydrated at startup, so a
  /// restart cannot reset the global rejoin floor.
  private var lastGlobalRejoinAttemptAt: Date?

  // MARK: - Successful-Rejoin Cooldown (Spec §10 SUCCESSFUL_REJOIN_COOLDOWN)

  /// Suppress SYNC-TRIGGERED rejoin attempts on a conversation for this
  /// duration after a SUCCESSFUL rejoin. E7 twin of Rust
  /// `SUCCESSFUL_REJOIN_COOLDOWN` (`catbird-mls/src/orchestrator/constants.rs`)
  /// — breaks the rejoin-loop/epoch-inflation spiral where a client
  /// successfully rejoins, then loses local MLS state, then sync re-detects
  /// the missing group and rejoins again, 409-ing every sibling device.
  /// Decrypt-triggered and user-initiated recovery are NOT gated by this.
  public static let successfulRejoinCooldownSec: TimeInterval = 300

  /// Per-conversation timestamp of the most recent SUCCESSFUL rejoin.
  /// Recorded by `clearRejoinTracking` (the success-outcome path) but NOT by
  /// `clearRejoinTrackingForFreshReset` (non-attempt bookkeeping). In-memory
  /// only, matching the Rust twin (`RecoveryTracker.successful_rejoins` is
  /// not persisted either).
  private var successfulRejoins: [String: Date] = [:]

  /// Upper bound applied to a hydrated FUTURE-dated global rejoin stamp:
  /// 30s global floor + 300s maximum Rust-gate backoff projection window
  /// (`recordRejoinOutcome` deliberately writes future stamps to mirror the
  /// Rust gate). Bounding here means unbounded forward clock skew in a
  /// persisted stamp cannot wedge the global gate. E7 contract.
  internal static let maxHydratedGlobalStampSkew: TimeInterval = 330

  // MARK: - Persisted Recovery Counters (WS-6.4 / E7 / N21)

  /// TTL for persisted recovery-counter entries. E7 coordinated semantics
  /// with the Rust `RecoveryTracker` twin: entries whose `last_attempt_at`
  /// is older than this are IGNORED at hydration, so persisted state can
  /// suppress automated rejoin for at most 24h — it can never permanently
  /// wedge a conversation.
  public static let persistedStateTTL: TimeInterval = 24 * 60 * 60

  /// Persistence seam (GRDB-backed in production via
  /// `MLSRecoveryStateStore`). Wired by the single startup caller
  /// (`MLSConversationManager.initialize()`); nil in contexts that never run
  /// the rejoin loop (e.g. NSE).
  private var persistence: MLSRecoveryStatePersisting?

  /// Explicit per-conversation quarantine horizon (max-attempts lockout,
  /// server-corruption marks). Checked by `shouldSkipRejoin` in addition to
  /// the attempts-derived backoff. Hydration honors a persisted horizon but
  /// never extends it. N39: armed when `failedRejoinCount` reaches
  /// `maxRejoinAttempts` (3) — the single 3-band Rust convention
  /// (`FailedRejoinEntry.lockout_until` / persisted `quarantined_until_ms`).
  private var quarantinedUntil: [String: Date] = [:]

  // MARK: - Layer-3 Quarantine State (N40, Rust `RecoveryTracker.quarantined` twin)

  /// Per-conversation Layer-3 quarantine state. Swift twin of the Rust
  /// orchestrator's `RecoveryTracker.quarantined` map
  /// (`catbird-mls/src/orchestrator/recovery.rs`): the STRONGEST recovery
  /// gate — `shouldSkipRejoin` checks it before every other band, and an
  /// indefinite entry exits only via `clearQuarantine` (event-driven:
  /// server reset, healthy peer commit, user-confirmed reset — Rust
  /// `QuarantineExitReason`).
  ///
  /// Entry convention (E7, coordinated with Rust `mark_quarantined`):
  /// entering quarantine CLEARS the failed-rejoin counter — quarantine
  /// replaces the counter as the gate; corruption/peer-badness is tracked as
  /// STATE, never as count inflation (the old `maxRejoinAttempts + 10` band
  /// is retired, N39/N40).
  ///
  /// Trigger status (2026-06-11): the Rust N38 typed decrypt-error classes
  /// that drive `record_peer_bad_commit` classification are NOT yet exported
  /// through the iOS xcframework, and the DS pushes no quarantine events, so
  /// the only production entry path today is `markConversationServerCorrupted`
  /// (`.serverDataCorruption`, 24h horizon). The peer-bad reasons land with
  /// the next FFI rebuild after N38.
  ///
  /// In-memory only for the quarantine REASON; the gating effect of a
  /// horizon-carrying entry survives restart via the Rust-portable persisted
  /// row (`failedRejoinCount == maxRejoinAttempts` + `quarantinedUntilMs`).
  /// Indefinite entries are deliberately NOT persisted: iOS has no
  /// event-driven exit signal wired yet, and a persisted indefinite
  /// quarantine could permanently wedge a conversation (violating the E7
  /// "persisted state can never permanently wedge" contract).
  private var quarantineStates: [String: MLSQuarantineState] = [:]

  /// Tail of the write-through chain. Each persistence op is appended here
  /// so writes apply in the order the state changes happened. Failures are
  /// logged at `.error` (never silent) and do not affect in-memory state,
  /// which stays authoritative for the current process.
  private var persistTail: Task<Void, Never>?

  /// Extract a `retryAfter` hint (in milliseconds) from an MLSAPIError or its
  /// localized description. The Petrel-generated client does not currently
  /// expose response headers, so we look in two places:
  ///   1. `MLSAPIError.rateLimited(retryAfter:)` — when the upstream decoder
  ///      stamped one (rare today, but future-proofed).
  ///   2. The error's `localizedDescription` for `retryAfterSeconds=N`,
  ///      `retryAfterSeconds: N`, or `Retry-After: N` patterns — covers cases
  ///      where Petrel surfaces the JSON body verbatim in an
  ///      `MLSAPIError.httpError(_, message:)`.
  /// Returns `nil` if no hint was found.
  internal static func extractRetryAfterMs(_ error: Error) -> Int? {
    if let apiError = error as? MLSAPIError,
       case .rateLimited(let retryAfter) = apiError,
       let seconds = retryAfter
    {
      return Int(seconds * 1000)
    }
    let description = (error as? LocalizedError)?.errorDescription
      ?? (error as NSError).localizedDescription
    if let seconds = extractRetryAfterNumber(in: description, keys: ["retryAfterSeconds", "Retry-After"]) {
      return seconds * 1000
    }
    return nil
  }

  /// Find the first numeric value following any of `keys` separated by an
  /// optional run of whitespace and a `:` or `=`. Used by
  /// `extractRetryAfterMs` to read retry hints out of plain-text error
  /// descriptions where regex literals would be overkill.
  private static func extractRetryAfterNumber(in haystack: String, keys: [String]) -> Int? {
    let lowered = haystack.lowercased()
    for key in keys {
      let keyLower = key.lowercased()
      guard let keyRange = lowered.range(of: keyLower) else { continue }
      var idx = keyRange.upperBound
      while idx < lowered.endIndex, lowered[idx].isWhitespace { idx = lowered.index(after: idx) }
      guard idx < lowered.endIndex, lowered[idx] == ":" || lowered[idx] == "=" else { continue }
      idx = lowered.index(after: idx)
      while idx < lowered.endIndex, lowered[idx].isWhitespace { idx = lowered.index(after: idx) }
      var digits = ""
      while idx < lowered.endIndex, lowered[idx].isASCII, lowered[idx].isNumber {
        digits.append(lowered[idx])
        idx = lowered.index(after: idx)
      }
      if let value = Int(digits) { return value }
    }
    return nil
  }

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

  /// Test-only initializer: rejoin tracking + persistence without live
  /// MLS/API clients. Server-reporting and silent-recovery paths guard the
  /// nil dependencies loudly. Used by the restart-persistence tests, which
  /// recreate the manager on the same DB to mirror the Rust twin's
  /// kill-orchestrator-mid-backoff tests.
  internal init(persistence: MLSRecoveryStatePersisting?) {
    self.mlsClient = nil
    self.mlsAPIClient = nil
    self.persistence = persistence
  }

  public func setDeferredRejoinHandler(_ handler: DeferredRejoinHandler?) {
    deferredRejoinHandler = handler
  }

  // MARK: - Persistence Plumbing (WS-6.4 / E7)

  /// Wire the persistence seam. Called once at startup by
  /// `MLSConversationManager.initialize()` (the single startup caller),
  /// immediately before `hydrateFromDatabase()`.
  public func setPersistence(_ store: MLSRecoveryStatePersisting?) {
    persistence = store
  }

  /// Await completion of all enqueued write-through operations. Primarily
  /// for tests and orderly shutdown; production callers don't need to wait.
  public func flushPersistence() async {
    await persistTail?.value
  }

  /// Append a persistence op to the ordered write-through chain. LOUD on
  /// failure (E7: persistence failures in recovery paths must be observable,
  /// never silent) — but in-memory state stays authoritative either way.
  private func enqueuePersist(
    _ label: String,
    _ op: @escaping @Sendable () async throws -> Void
  ) {
    let previous = persistTail
    let log = logger
    persistTail = Task {
      await previous?.value
      do {
        try await op()
      } catch {
        log.error(
          "❌ [MLSRecoveryManager] PERSISTENCE FAILURE (\(label)): \(error.localizedDescription) — recovery counters may not survive restart (E7 write-through violated for this change)"
        )
      }
    }
  }

  /// One-time-per-process flag for the persistence==nil misconfiguration log
  /// (P-7 / E7): the nil guards below are intentional for the NSE (which
  /// never runs the rejoin loop) but otherwise indistinguishable from a
  /// missed `setPersistence(_:)` call. Log loudly exactly once.
  private var loggedMissingPersistence = false

  /// True when running inside an app extension (e.g. the NSE), where a nil
  /// persistence store is intentional.
  private static let isAppExtension = Bundle.main.bundlePath.hasSuffix(".appex")

  /// Surface a missing persistence store at `.error` once per process —
  /// outside the NSE this means recovery counters will NOT survive restart
  /// (E7 write-through silently skipped). Keep behavior identical otherwise.
  private func notePersistenceUnavailable(_ label: String) {
    guard !Self.isAppExtension, !loggedMissingPersistence else { return }
    loggedMissingPersistence = true
    logger.error(
      "❌ [MLSRecoveryManager] PERSISTENCE UNAVAILABLE (\(label)): no recovery persistence store wired in this process (setPersistence(_:) never called?). Intentional only in the NSE — in the main app recovery counters will NOT survive restart (E7 write-through skipped). Logged once per process."
    )
  }

  /// Write-through the current in-memory entry for a conversation.
  private func persistConversationEntry(convoId: String) {
    guard let persistence else {
      notePersistenceUnavailable("upsert \(convoId.prefix(16))")
      return
    }
    guard let record = failedRejoins[convoId] else { return }
    let attempts = record.attempts
    let lastMs = Int64(record.lastAttempt.timeIntervalSince1970 * 1000)
    let quarantineMs = quarantinedUntil[convoId].map { Int64($0.timeIntervalSince1970 * 1000) }
    enqueuePersist("upsert \(convoId.prefix(16))") {
      try await persistence.upsertConversationState(
        conversationID: convoId,
        failedRejoinCount: attempts,
        lastAttemptAtMs: lastMs,
        quarantinedUntilMs: quarantineMs
      )
    }
  }

  /// Write-through deletion of a conversation's persisted entry (success path).
  private func persistClearConversationEntry(convoId: String) {
    guard let persistence else {
      notePersistenceUnavailable("clear \(convoId.prefix(16))")
      return
    }
    enqueuePersist("clear \(convoId.prefix(16))") {
      try await persistence.clearConversationState(conversationID: convoId)
    }
  }

  /// Write-through the global rejoin-floor timestamp.
  private func persistGlobalStamp(_ date: Date) {
    guard let persistence else {
      notePersistenceUnavailable("global stamp")
      return
    }
    let ms = Int64(date.timeIntervalSince1970 * 1000)
    enqueuePersist("global stamp") {
      try await persistence.setGlobalLastRejoinAttempt(atMs: ms)
    }
  }

  private static func date(fromEpochMs ms: Int64) -> Date {
    Date(timeIntervalSince1970: Double(ms) / 1000.0)
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

  /// Check if a conversation should be skipped during rejoin (max attempts exceeded or on cooldown).
  ///
  /// **Read-mostly, with one lazy-expiry side effect.** When the lockout
  /// horizon of a maxed-out entry has lapsed (explicit `quarantinedUntil`,
  /// else the implicit `lastAttempt + persistedStateTTL`), this method clamps
  /// the failed-rejoin count to `maxRejoinAttempts - 1` — re-opening exactly
  /// one attempt — and enqueues a persistence write-through so the clamp
  /// survives restart (E7 runtime lockout expiry; Swift twin of Rust
  /// `RecoveryTracker::expire_lapsed_lockout`). That mutation is idempotent
  /// and can only flip the answer from "skip" to "don't skip" (never the
  /// reverse), so the method remains safe to call multiple times for the
  /// same logical rejoin attempt (e.g. nested External Commit fallback
  /// re-entering the gate). It never stamps attempt outcomes:
  /// `lastGlobalRejoinAttemptAt` and the failure counters are mutated only
  /// in `recordFailedRejoin` / `clearRejoinTracking`, matching the Rust
  /// orchestrator contract in `catbird-mls/src/orchestrator/recovery.rs`
  /// (`should_skip` does not stamp the global timestamp; `record_failure` /
  /// `clear` do).
  ///
  /// Layered gates (each can short-circuit to skip):
  /// 0. Layer-3 quarantine state (N40 — strongest gate, Rust
  ///    `RecoveryTracker::should_skip` checks `quarantined` first; exit is
  ///    event-driven via `clearQuarantine`, or lazy horizon expiry)
  /// 1. Per-conversation max attempts (spec §8.2 — MAX_REJOIN_ATTEMPTS = 3;
  ///    N39: reaching it arms the 24h `quarantinedUntil` lockout horizon)
  /// 2. Per-conversation exponential backoff (30s/2m/10m/1h)
  /// 3. **Global** rejoin floor (spec §8.4 / §10 — MIN_REJOIN_INTERVAL_SEC = 30,
  ///    applies across all conversations, mirrors Rust
  ///    `RecoveryTracker.last_global_rejoin_at`)
  public func shouldSkipRejoin(convoId: String) -> Bool {
    // Layer 3 (N40): quarantine state is the strongest gate — never
    // auto-rejoin a quarantined conversation (Rust parity: quarantine is
    // checked before every other band in `should_skip`). Horizon-carrying
    // states (server corruption) expire lazily inside
    // `activeQuarantineState`; indefinite states exit only via
    // `clearQuarantine`.
    if let state = activeQuarantineState(convoId: convoId) {
      logger.error(
        "⛔️ [MLSRecoveryManager] Skipping \(convoId.prefix(16)) - quarantined (reason=\(state.reason.rawValue), since=\(state.since)) — automated rejoin disabled"
      )
      return true
    }

    // N39 maxed-out lockout horizon: armed when the failure counter reaches
    // maxRejoinAttempts (possibly hydrated from a prior session).
    // Honored while active; expires naturally — never extended here.
    if let quarantine = quarantinedUntil[convoId], Date() < quarantine {
      logger.error(
        "⛔️ [MLSRecoveryManager] Skipping \(convoId.prefix(16)) - quarantined until \(quarantine) (circuit breaker)"
      )
      return true
    }

    // E7 runtime lockout expiry: a maxed-out entry whose lockout horizon
    // lapsed clamps to maxRejoinAttempts - 1 so exactly one attempt
    // re-opens. Without this, removing the expired quarantine was cosmetic —
    // the attempts-derived ceiling/max gates below still blocked, wedging a
    // long-lived process past the 24h lockout.
    expireLapsedLockoutIfNeeded(convoId: convoId, now: Date())

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

    // Gate decided to proceed. Do NOT stamp `lastGlobalRejoinAttemptAt` here —
    // that's the caller's outcome path's responsibility (`recordFailedRejoin`
    // on failure, `clearRejoinTracking` on success). Stamping inside the gate
    // would make this method non-idempotent and self-skip nested re-entries
    // within the same logical rejoin attempt.
    return false
  }

  /// E7 runtime lockout expiry — Swift twin of Rust
  /// `RecoveryTracker::expire_lapsed_lockout`
  /// (`catbird-mls/src/orchestrator/recovery.rs`). A maxed-out entry's
  /// lockout horizon is the explicit `quarantinedUntil` when set, else the
  /// implicit `lastAttempt + persistedStateTTL` (the same 24h value
  /// `recordFailedRejoin` persists at the ceiling). Once that horizon
  /// lapses, the failed-rejoin count clamps to `maxRejoinAttempts - 1`,
  /// re-opening exactly one attempt, and the clamped entry is written
  /// through. Mutations here are idempotent and only ever LOOSEN the gate.
  private func expireLapsedLockoutIfNeeded(convoId: String, now: Date) {
    guard let record = failedRejoins[convoId] else {
      // Stale quarantine with no failure entry: drop it and clear the
      // persisted row (pre-existing lazy-expiry behavior).
      if let quarantine = quarantinedUntil[convoId], now >= quarantine {
        quarantinedUntil.removeValue(forKey: convoId)
        persistClearConversationEntry(convoId: convoId)
      }
      return
    }

    guard record.attempts >= maxRejoinAttempts else {
      // Below the maxed-out band the attempts-derived backoff governs; an
      // expired explicit quarantine simply falls away.
      if let quarantine = quarantinedUntil[convoId], now >= quarantine {
        quarantinedUntil.removeValue(forKey: convoId)
        persistConversationEntry(convoId: convoId)
      }
      return
    }

    let lockout =
      quarantinedUntil[convoId]
      ?? record.lastAttempt.addingTimeInterval(Self.persistedStateTTL)
    guard now >= lockout else { return }

    quarantinedUntil.removeValue(forKey: convoId)
    let clamped = maxRejoinAttempts - 1
    if clamped <= 0 {
      // maxRejoinAttempts == 1: mirroring hydration, a zero count is not
      // tracked at all.
      failedRejoins.removeValue(forKey: convoId)
      persistClearConversationEntry(convoId: convoId)
    } else {
      failedRejoins[convoId] = (attempts: clamped, lastAttempt: record.lastAttempt)
      persistConversationEntry(convoId: convoId)
    }
    logger.warning(
      "⏰ [MLSRecoveryManager] Rejoin lockout lapsed for \(convoId.prefix(16)) — clamping failed-rejoin count \(record.attempts) → \(clamped) (one attempt re-opens; E7 runtime expiry)"
    )
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
    let now = Date()
    let newAttempts = existing.attempts + 1
    failedRejoins[convoId] = (attempts: newAttempts, lastAttempt: now)
    // Stamp the global rejoin floor on the failure outcome (spec §8.4 / §10,
    // mirrors Rust `RecoveryTracker::record_failure`). Any completed attempt
    // — success or failure — counts as the most recent global attempt for
    // cross-conversation cooldown.
    lastGlobalRejoinAttemptAt = now
    logger.warning(
      "📝 [MLSRecoveryManager] Recorded failed rejoin for \(convoId.prefix(16)) - attempt \(newAttempts)/\(self.maxRejoinAttempts) failureType=\(failureType)"
    )

    // N39 (3-band collapse, Rust `record_failure` parity): reaching
    // MAX_REJOIN_ATTEMPTS arms the 24h lockout horizon — Rust sets
    // `lockout_until = now + RECOVERY_BACKOFF_TTL` whenever
    // `count >= max_attempts`, persisted as `quarantined_until_ms`. Like the
    // Rust twin this RE-ARMS on every maxed failure OUTCOME (a real new
    // attempt extends the lockout); hydration still honors-never-extends —
    // that contract applies to persisted state at startup, not runtime
    // outcomes. The TTL guarantees the breaker self-resets (runtime clamp /
    // hydration clamp re-open exactly one attempt after expiry).
    if newAttempts >= maxRejoinAttempts {
      quarantinedUntil[convoId] = now.addingTimeInterval(Self.persistedStateTTL)
      logger.error(
        "⛔️ [MLSRecoveryManager] Max rejoin attempts (\(self.maxRejoinAttempts)) reached for \(convoId.prefix(16)) — automated rejoin disabled, quarantined for 24h (circuit breaker)"
      )
    }

    // WS-6.4 / E7: write-through on every state change.
    persistConversationEntry(convoId: convoId)
    persistGlobalStamp(now)

    // Spec §8.6: Report to server when transitioning to UNRECOVERABLE_LOCAL.
    // N39: vote-once-at-3 — fire only on the TRANSITION into the maxed band
    // (existing < max, new >= max), once per exhaustion cycle. A runtime/
    // hydration clamp re-opening one attempt that then fails re-crosses the
    // boundary and legitimately re-fires (same as pre-collapse behavior).
    if existing.attempts < maxRejoinAttempts, newAttempts >= maxRejoinAttempts {
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
      guard let apiClient = mlsAPIClient else {
        logger.error(
          "❌ [MLSRecoveryManager] Cannot report recovery failure for \(convoId.prefix(16)) — no API client configured (test/headless context)"
        )
        return
      }
      Task {
        do {
          let input = BlueCatbirdMlsChatReportRecoveryFailure.Input(
            convoId: convoId,
            failureType: resolvedFailureType,  // Spec §8.6
            failureMode: failureMode,  // ADR-008 D1
            epochAuthenticator: authenticator
          )
          let (code, output) = try await apiClient.client.blue.catbird.mlsChat.reportRecoveryFailure(input: input)
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

  // MARK: - Rust-gate-aware outcome routing (2026-05-02 deadlock fix)

  /// Parses Rust's
  /// `OrchestratorError::RecoveryFailed("Rejoin suppressed for X: <kind>
  /// (Ns remaining)")` shape into a `TimeInterval` of seconds remaining.
  /// Returns nil for any other error.
  ///
  /// Mirrors the Kotlin `extractRustGateRemainingMs` in
  /// `MLSRecoveryManager.kt`. When a rejoin attempt is rejected by Rust's
  /// `RecoveryTracker` (no real network attempt happened), we should NOT
  /// stamp our local `lastGlobalRejoinAttemptAt` to now — that doubles
  /// the gate window unnecessarily and causes both gates to re-arm in
  /// lockstep.
  ///
  /// Rust source: `catbird-mls/src/orchestrator/recovery.rs:551,572`.
  internal static func extractRustGateRemainingSec(_ error: Error) -> TimeInterval? {
    let message = String(describing: error)
    guard message.contains("Rejoin suppressed") else { return nil }
    // Match `(N seconds remaining)` or `(Ns remaining)` shape — Rust emits
    // `(Ns remaining)` and the more verbose form is defensive future-proofing.
    let pattern = #"\((\d+)s? ?(?:seconds? )?remaining\)"#
    guard
      let regex = try? NSRegularExpression(pattern: pattern),
      let match = regex.firstMatch(
        in: message,
        range: NSRange(message.startIndex..., in: message)
      ),
      let range = Range(match.range(at: 1), in: message),
      let seconds = Double(message[range])
    else { return nil }
    return seconds
  }

  /// Record the OUTCOME of a rejoin attempt that may have been a real
  /// failure OR may have been a Rust-orchestrator gate suppression. If
  /// the error is a gate suppression (no real attempt happened), project
  /// `lastGlobalRejoinAttemptAt` so it clears at the same time as Rust's
  /// gate, instead of stomping it to now and creating the gate-thrash
  /// the 2026-05-02 prod incident exhibited.
  ///
  /// Use this in catch blocks that wrap calls into the Rust orchestrator
  /// (`handleGroupReset`, `joinOrRejoin`, `bootstrapResetGroup`, etc.).
  /// Existing callers that pass a real failure error see identical
  /// behavior to `recordFailedRejoin` (since the gate-suppression check
  /// short-circuits to false on those).
  public func recordRejoinOutcome(
    convoId: String,
    error: Error,
    epochAuthenticatorHex: String? = nil,
    failureType: String = "external_commit_exhausted"
  ) {
    if let remainingSec = Self.extractRustGateRemainingSec(error) {
      // Project local gate to clear at the same wall-clock instant as Rust's.
      // Subtract `minGlobalRejoinIntervalSec` because `shouldSkipRejoin`
      // compares against `(now - lastGlobalRejoinAttemptAt) >= interval`,
      // so we need to write a future timestamp such that the inequality
      // flips when Rust's gate also clears. Clamp to 0 to keep the timestamp
      // realistic when Rust reports < 30 s remaining.
      let projected = Date().addingTimeInterval(
        max(0, remainingSec - Self.minGlobalRejoinIntervalSec)
      )
      lastGlobalRejoinAttemptAt = projected
      // WS-6.4 / E7: write-through the projected global stamp so a restart
      // mid-gate doesn't forget the Rust-side cooldown projection.
      persistGlobalStamp(projected)
      logger.debug(
        "⏸️ [MLSRecoveryManager] Rust rejoin gate active for \(convoId.prefix(16)) (\(Int(remainingSec))s remaining); projecting local gate, NOT stamping recordFailedRejoin (no real attempt happened)"
      )
      // Do NOT increment per-convo failure counter — Rust gate suppression
      // means no real network attempt happened. Counting it would falsely
      // accelerate the convo toward MAX_REJOIN_ATTEMPTS and trip the
      // §8.6 escalation prematurely.
      return
    }
    recordFailedRejoin(
      convoId: convoId,
      epochAuthenticatorHex: epochAuthenticatorHex,
      failureType: failureType
    )
  }

  /// Record a retryable bootstrap stall caused by missing peer key packages.
  ///
  /// This is not an External Commit exhaustion and must not contribute a reset
  /// vote. We still stamp normal cooldown state so the reset-pending loop waits
  /// for peer replenishment instead of hammering `getKeyPackages`.
  public func recordPeerKeyPackagesMissing(convoId: String) {
    let existing = failedRejoins[convoId] ?? (attempts: 0, lastAttempt: Date.distantPast)
    let nextAttempts: Int
    if existing.attempts >= maxRejoinAttempts {
      nextAttempts = existing.attempts
    } else {
      nextAttempts = min(existing.attempts + 1, maxRejoinAttempts - 1)
    }

    let now = Date()
    failedRejoins[convoId] = (attempts: nextAttempts, lastAttempt: now)
    // E7 parity: do NOT arm `lastGlobalRejoinAttemptAt` here. A key-package
    // stall is not an External Commit attempt — arming the cross-conversation
    // 30s floor from it would gate UNRELATED conversations' rejoins off a
    // non-attempt event (the same non-arming contract as
    // `clearRejoinTrackingForFreshReset`). The per-conversation cooldown
    // above is the intended throttle for the reset-pending loop.
    logger.warning(
      "⏳ [MLSRecoveryManager] Waiting for peer key packages for \(convoId.prefix(16)) - cooldown attempt \(nextAttempts)/\(self.maxRejoinAttempts - 1)"
    )
    // WS-6.4 / E7: write-through on every state change.
    persistConversationEntry(convoId: convoId)
  }

  /// Record an unbridgeable epoch gap (`fetchAndProcessMissingCommits`
  /// returned `.serverDataGap` despite a known epoch gap).
  ///
  /// When this fires N times in a row for the same convo, the local FFI is
  /// stuck at an epoch the wire commit history cannot reach — the typical
  /// fingerprint is a bootstrap creator whose `confirmCommit` was lost to a
  /// concurrent context reload, since the addMembers commit only ever
  /// existed in the Welcome bytes (no `INSERT INTO messages` in
  /// `bootstrap_reset_group.rs`). Standard epoch-recovery cannot bridge it,
  /// so the only fix is a server-side group reset.
  ///
  /// At threshold (`serverDataGapMaxStrikes`), reports recovery failure as
  /// Mode B (`group_state_unrecoverable`) so 1:1 convos auto-reset on a
  /// single vote and group convos progress toward quorum. Resets the counter
  /// after dispatch so we don't spam the server every sync — re-enters only
  /// if `serverDataGapMaxStrikes` more consecutive gaps occur post-report
  /// (i.e. the reset didn't actually fire / hasn't propagated yet).
  ///
  /// Caller should compute `epochAuthenticatorHex` via
  /// `MLSClient.epochAuthenticatorHex(for:groupId:)` so the vote counts
  /// toward quorum (server short-circuits as `missing_authenticator`
  /// otherwise).
  public func recordServerDataGap(
    convoId: String,
    epochAuthenticatorHex: String? = nil
  ) {
    let count = (serverDataGapCounts[convoId] ?? 0) + 1
    serverDataGapCounts[convoId] = count
    logger.warning(
      "📊 [MLSRecoveryManager] serverDataGap strike \(count)/\(self.serverDataGapMaxStrikes) for \(convoId.prefix(16)) — epoch recovery cannot bridge gap"
    )
    guard count >= serverDataGapMaxStrikes else { return }

    serverDataGapCounts[convoId] = 0

    logger.error(
      "🚨 [MLSRecoveryManager] Unbridgeable epoch gap confirmed for \(convoId.prefix(16)) — reporting Mode B (group_state_unrecoverable, failureType=bootstrap_commit_unbridgeable)"
    )
    if epochAuthenticatorHex == nil {
      logger.warning(
        "⚠️ [MLSRecoveryManager] recordServerDataGap missing epochAuthenticator for \(convoId.prefix(16)) — server will short-circuit vote (reason: missing_authenticator). Caller should pass MLSContext.epochAuthenticator(groupId:) hex."
      )
    }
    let authenticator = epochAuthenticatorHex
    guard let apiClient = mlsAPIClient else {
      logger.error(
        "❌ [MLSRecoveryManager] Cannot report unbridgeable epoch gap for \(convoId.prefix(16)) — no API client configured (test/headless context)"
      )
      return
    }
    Task {
      do {
        let input = BlueCatbirdMlsChatReportRecoveryFailure.Input(
          convoId: convoId,
          failureType: "bootstrap_commit_unbridgeable",
          failureMode: "group_state_unrecoverable",
          epochAuthenticator: authenticator
        )
        let (code, output) = try await apiClient.client.blue.catbird.mlsChat
          .reportRecoveryFailure(input: input)
        self.logger.info(
          "📡 [MLSRecoveryManager] Reported unbridgeable epoch gap for \(convoId.prefix(16)) — code=\(code) recorded=\(output?.recorded ?? false) autoReset=\(output?.autoResetTriggered ?? false) reason=\(output?.reason ?? "nil")"
        )
      } catch {
        self.logger.error(
          "❌ [MLSRecoveryManager] Failed to report unbridgeable epoch gap for \(convoId.prefix(16)): \(error.localizedDescription)"
        )
      }
    }
  }

  /// Clear the unbridgeable-epoch-gap counter (called on successful epoch
  /// advance / rejoin).
  public func clearServerDataGap(convoId: String) {
    if serverDataGapCounts.removeValue(forKey: convoId) != nil {
      logger.debug(
        "[MLSRecoveryManager] Cleared serverDataGap counter for \(convoId.prefix(16))"
      )
    }
  }

  /// Clear rejoin tracking for a conversation (on success).
  ///
  /// Also clears transient state and failure counters so the conversation
  /// is fully returned to `.healthy` (spec §8.2: NEEDS_REJOIN → HEALTHY on
  /// successful External Commit).
  ///
  /// This is the SUCCESS-OUTCOME path: it stamps the global rejoin floor and
  /// records the per-conversation successful-rejoin cooldown (spec §10
  /// `SUCCESSFUL_REJOIN_COOLDOWN`, mirrors Rust `RecoveryTracker::clear`).
  /// For non-attempt bookkeeping (server resets, stale-flag clears, race
  /// rollbacks) use `clearRejoinTrackingForFreshReset` instead — routing
  /// those through here re-arms the 30s global gate from events that are not
  /// attempts (the 2026-05-02 reset-bootstrap deadlock class).
  public func clearRejoinTracking(convoId: String) {
    let hadTracking = failedRejoins.removeValue(forKey: convoId) != nil
    quarantinedUntil.removeValue(forKey: convoId)
    // N40 quarantine exit: a successful rejoin / user-confirmed recovery is
    // an event-driven exit (Rust `QuarantineExitReason::PeerCommitSucceeded`
    // / `UserConfirmedReset` analog).
    quarantineStates.removeValue(forKey: convoId)
    transientStates.removeValue(forKey: convoId)
    commitFailureCounts.removeValue(forKey: convoId)
    decryptionFailureCounts.removeValue(forKey: convoId)
    serverDataGapCounts.removeValue(forKey: convoId)
    // Stamp the global rejoin floor on the success outcome (spec §8.4 / §10,
    // mirrors Rust `RecoveryTracker::clear` which stamps unconditionally —
    // see comment at `catbird-mls/src/orchestrator/recovery.rs` near line 108:
    // "the minimum interval still applies to prevent rapid successive
    // rejoins even when they succeed"). Stamped regardless of whether prior
    // failure tracking existed, so a clean rejoin success still enforces the
    // 30s floor against subsequent EC bursts on other conversations.
    let now = Date()
    lastGlobalRejoinAttemptAt = now
    // Spec §10 SUCCESSFUL_REJOIN_COOLDOWN: record the success so
    // sync-triggered rejoins on this conversation are suppressed for 300s
    // (spiral protection — see `successCooldownRemaining(convoId:)`).
    successfulRejoins[convoId] = now
    persistClearConversationEntry(convoId: convoId)
    persistGlobalStamp(now)
    if hadTracking {
      logger.info("✅ [MLSRecoveryManager] Cleared rejoin tracking for \(convoId.prefix(16))")
    }
  }

  /// Clear per-conversation rejoin tracking for a server-initiated reset or
  /// other NON-ATTEMPT bookkeeping event. E7 twin of Rust
  /// `RecoveryTracker::clear_for_fresh_reset` / `clear_stale_flag`
  /// (`catbird-mls/src/orchestrator/recovery.rs`).
  ///
  /// Unlike `clearRejoinTracking` (the success-outcome path), this:
  /// - does NOT touch or persist `lastGlobalRejoinAttemptAt` — a
  ///   server-pushed reset is not an attempt by THIS client and must not
  ///   gate the imminent first-responder bootstrap that follows (the
  ///   2026-05-02 prod deadlock: two clients sat behind their own global
  ///   gates waiting for the other to bootstrap, sometimes for 24+ minutes);
  /// - does NOT record a successful rejoin — no rejoin happened, so the
  ///   `SUCCESSFUL_REJOIN_COOLDOWN` sync suppression must not arm.
  ///
  /// It still wipes the per-conversation failure history (server reset
  /// trumps client retry counters), quarantine, transient state, and
  /// counters, and write-through-DELETEs the persisted row.
  public func clearRejoinTrackingForFreshReset(convoId: String) {
    let hadTracking = failedRejoins.removeValue(forKey: convoId) != nil
    quarantinedUntil.removeValue(forKey: convoId)
    // N40 quarantine exit: a server-pushed reset is an event-driven exit
    // (Rust `QuarantineExitReason::ServerReset` — the server has declared the
    // old, possibly-poisoned group dead).
    quarantineStates.removeValue(forKey: convoId)
    transientStates.removeValue(forKey: convoId)
    commitFailureCounts.removeValue(forKey: convoId)
    decryptionFailureCounts.removeValue(forKey: convoId)
    serverDataGapCounts.removeValue(forKey: convoId)
    // Intentionally do NOT touch `lastGlobalRejoinAttemptAt` or
    // `successfulRejoins` — see doc comment above.
    persistClearConversationEntry(convoId: convoId)
    if hadTracking {
      logger.info(
        "🧹 [MLSRecoveryManager] Cleared rejoin tracking for \(convoId.prefix(16)) (fresh reset / bookkeeping — global floor NOT armed)"
      )
    }
  }

  /// Remaining `SUCCESSFUL_REJOIN_COOLDOWN` imposed by a recent SUCCESSFUL
  /// rejoin on this conversation, or `nil` when none is active. Applies to
  /// SYNC-TRIGGERED rejoin attempts only — callers on decrypt-triggered or
  /// user-initiated recovery paths must not consult this. E7 twin of Rust
  /// `RecoveryTracker::success_cooldown_remaining`.
  public func successCooldownRemaining(convoId: String) -> TimeInterval? {
    guard let last = successfulRejoins[convoId] else { return nil }
    let elapsed = Date().timeIntervalSince(last)
    guard elapsed < Self.successfulRejoinCooldownSec else { return nil }
    return Self.successfulRejoinCooldownSec - elapsed
  }

  /// Test seam: backdate (or otherwise override) a successful-rejoin
  /// timestamp so cooldown expiry is testable without waiting 300s.
  internal func overrideSuccessfulRejoinTimestamp(convoId: String, to date: Date) {
    successfulRejoins[convoId] = date
  }

  /// Test seam: install raw rejoin-tracking state so runtime lockout expiry
  /// is testable without waiting out real 24h horizons.
  internal func overrideRejoinTracking(
    convoId: String,
    attempts: Int,
    lastAttempt: Date,
    quarantinedUntil quarantine: Date?
  ) {
    failedRejoins[convoId] = (attempts: attempts, lastAttempt: lastAttempt)
    if let quarantine {
      quarantinedUntil[convoId] = quarantine
    } else {
      quarantinedUntil.removeValue(forKey: convoId)
    }
  }

  /// Test seam: read the in-memory global rejoin floor (no public getter —
  /// production callers must route through `shouldSkipRejoin`).
  internal func lastGlobalRejoinAttemptForTesting() -> Date? {
    lastGlobalRejoinAttemptAt
  }

  /// Get the number of remaining rejoin attempts for a conversation
  public func remainingRejoinAttempts(convoId: String) -> Int {
    guard let record = failedRejoins[convoId] else {
      return maxRejoinAttempts
    }
    return max(0, maxRejoinAttempts - record.attempts)
  }

  /// Load persisted recovery counters and global cooldown state.
  ///
  /// N35 note: a legacy
  /// `hydrateFromDatabase(unrecoverableConvoIds:epochAuthenticatorsByConvoId:)`
  /// overload used to live here. It stamped attempts past `maxRejoinAttempts`
  /// at hydration and re-fired the ADR-002 A7 `reportRecoveryFailure`
  /// escalation once per launch (so a network blip at the moment of the
  /// original 3rd-failure dispatch could not orphan the server-side reset
  /// vote forever). It was deleted with zero callers: this method is the
  /// startup contract now, the 24h TTL + hydration/runtime clamps re-open
  /// exactly one attempt after a lapsed lockout (whose failure re-fires the
  /// §8.6 vote via `recordFailedRejoin` — covering the A7 re-fire intent),
  /// and N39 tracks server corruption as quarantine state instead of count
  /// inflation. A durable pending-escalation queue remains a backlog task.
  ///
  /// This is the WS-6.4/N21 startup contract: call after `setPersistence(_:)`
  /// and before any deferred rejoin loop runs. E7 coordinated semantics with
  /// the Rust twin (`RecoveryTracker::hydrate_from_persisted`,
  /// `catbird-mls/src/orchestrator/recovery.rs`):
  /// - entries older than the 24h TTL are ignored AND deleted;
  /// - future-dated entries (wall clock moved backwards since the write) are
  ///   ignored AND deleted — a kept row would dodge the TTL gate and pin a
  ///   quarantine far past 24h of real time, and would re-log its drop
  ///   warning on every restart;
  /// - zero-count entries are dead rows — ignored AND deleted;
  /// - a maxed-out entry whose quarantine is nil-or-expired clamps to
  ///   `maxRejoinAttempts - 1` (honor the lockout, never extend it — exactly
  ///   one attempt re-opens after the normal per-attempt cooldown);
  /// - the global stamp re-arms `MIN_REJOIN_INTERVAL` for whatever window
  ///   remains; a future-dated global stamp is honored (it is a deliberate
  ///   Rust-gate projection from `recordRejoinOutcome`) but bounded by
  ///   `maxHydratedGlobalStampSkew` so forward clock skew cannot wedge the
  ///   global gate.
  public func hydrateFromDatabase() async {
    guard let persistence else {
      logger.debug("📥 [MLSRecoveryManager] No recovery persistence store configured")
      return
    }

    do {
      let snapshot = try await persistence.loadSnapshot()
      let now = Date()

      for entry in snapshot.conversations {
        let lastAttempt = Self.date(fromEpochMs: entry.lastAttemptAtMs)

        // Future-dated entry: drop + delete (Rust parity — under-gating
        // never extends backoff; the next real attempt rewrites the row).
        if lastAttempt > now {
          logger.warning(
            "⚠️ [MLSRecoveryManager] Persisted rejoin backoff for \(entry.conversationID.prefix(16)) is future-dated (wall clock moved backwards) — dropping entry and deleting row"
          )
          try await persistence.clearConversationState(conversationID: entry.conversationID)
          continue
        }

        guard now.timeIntervalSince(lastAttempt) <= Self.persistedStateTTL else {
          logger.info(
            "🧹 [MLSRecoveryManager] Ignoring expired persisted recovery state for \(entry.conversationID.prefix(16))"
          )
          try await persistence.clearConversationState(conversationID: entry.conversationID)
          continue
        }

        var attempts = entry.failedRejoinCount
        if attempts <= 0 {
          // Dead row (no failure state to carry) — Rust parity: reject and
          // delete rather than hydrating a meaningless zero-count entry.
          try await persistence.clearConversationState(conversationID: entry.conversationID)
          continue
        }

        var quarantine: Date?
        if let quarantineMs = entry.quarantinedUntilMs {
          let candidate = Self.date(fromEpochMs: quarantineMs)
          if candidate > now {
            quarantine = candidate
          }
        }

        if attempts >= maxRejoinAttempts, quarantine == nil {
          // E7 hydration clamp (Rust twin: `hydrate_from_persisted`): the
          // lockout expired (or was never recorded) — honor it, don't
          // extend. Clamping below maxRejoinAttempts re-opens exactly one
          // attempt (after the normal per-attempt cooldown) instead of
          // re-arming the indefinite maxed-out gate.
          let clamped = maxRejoinAttempts - 1
          logger.warning(
            "⏰ [MLSRecoveryManager] Hydrating \(entry.conversationID.prefix(16)) with lapsed lockout — clamping failed-rejoin count \(attempts) → \(clamped) (one attempt re-opens; E7 hydration clamp)"
          )
          attempts = clamped
          if attempts <= 0 {
            // maxRejoinAttempts == 1: a zero count is not tracked at all;
            // delete the row so it cannot resurrect on a later restart.
            try await persistence.clearConversationState(conversationID: entry.conversationID)
            continue
          }
        }

        failedRejoins[entry.conversationID] = (
          attempts: attempts,
          lastAttempt: lastAttempt
        )
        if let quarantine {
          quarantinedUntil[entry.conversationID] = quarantine
        }

        logger.info(
          "📥 [MLSRecoveryManager] Hydrated recovery state for \(entry.conversationID.prefix(16)) attempts=\(attempts)"
        )
      }

      if let globalMs = snapshot.lastGlobalRejoinAttemptAtMs {
        let global = Self.date(fromEpochMs: globalMs)
        if global > now {
          // Deliberate: `recordRejoinOutcome` writes future-dated stamps to
          // project the Rust orchestrator's gate. Honor the projection, but
          // bound the forward skew (E7: persisted state can never wedge the
          // global gate beyond the largest legitimate projection window).
          let bound = now.addingTimeInterval(Self.maxHydratedGlobalStampSkew)
          if global > bound {
            logger.warning(
              "⚠️ [MLSRecoveryManager] Hydrated global rejoin stamp is \(Int(global.timeIntervalSince(now)))s in the future — clamping to +\(Int(Self.maxHydratedGlobalStampSkew))s (E7 skew bound)"
            )
          }
          lastGlobalRejoinAttemptAt = min(global, bound)
          logger.info("📥 [MLSRecoveryManager] Hydrated global rejoin floor (future-projected)")
        } else if now.timeIntervalSince(global) <= Self.persistedStateTTL {
          lastGlobalRejoinAttemptAt = global
          logger.info("📥 [MLSRecoveryManager] Hydrated global rejoin floor")
        }
      }
    } catch {
      logger.error(
        "❌ [MLSRecoveryManager] Failed to hydrate persisted recovery state: \(error.localizedDescription)"
      )
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
    guard let mlsClient else {
      logger.error(
        "❌ [MLSRecoveryManager] Cannot check desync severity — no MLS client configured (test/headless context)"
      )
      let serverCount = serverBundleCount ?? 0
      return .severe(localCount: 0, serverCount: serverCount, difference: serverCount)
    }

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
    guard let mlsClient else {
      logger.error(
        "❌ [MLSRecoveryManager] Cannot perform silent recovery — no MLS client configured (test/headless context)"
      )
      throw MLSRecoveryError.recoveryFailed(underlying: MLSRecoveryError.deviceDeletionFailed)
    }

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
          // Gate-aware via recordRejoinOutcome: a Rust orchestrator gate
          // suppression must NOT be misclassified as remote-data corruption.
          if let convoId = convoId {
            recordRejoinOutcome(
              convoId: convoId,
              error: error,
              failureType: "remote_data_error"
            )
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
      // Record the failure for the triggering conversation. Gate-aware:
      // if `performSilentRecovery` propagated a Rust orchestrator gate
      // suppression, project our local gate instead of stamping it.
      if let convoId = checkConvoId {
        recordRejoinOutcome(convoId: convoId, error: error)
      }
      return false
    }
  }

  // MARK: - Layer-3 Quarantine (N40, Rust `RecoveryTracker` Layer-3 twin)

  /// Resolve the active quarantine state for a conversation, lazily dropping
  /// a horizon-carrying state whose `until` has lapsed (mirrors the lazy
  /// expiry pattern used for `quarantinedUntil`). Indefinite states
  /// (`until == nil`) never expire here — they exit only via
  /// `clearQuarantine`.
  private func activeQuarantineState(convoId: String, now: Date = Date()) -> MLSQuarantineState? {
    guard let state = quarantineStates[convoId] else { return nil }
    if let until = state.until, now >= until {
      quarantineStates.removeValue(forKey: convoId)
      logger.warning(
        "⏰ [MLSRecoveryManager] Quarantine horizon lapsed for \(convoId.prefix(16)) (reason=\(state.reason.rawValue)) — state dropped"
      )
      return nil
    }
    return state
  }

  /// Enter Layer-3 quarantine for a conversation. Swift twin of Rust
  /// `RecoveryTracker::mark_quarantined`
  /// (`catbird-mls/src/orchestrator/recovery.rs`).
  ///
  /// Entry convention (E7): entering quarantine CLEARS the failed-rejoin
  /// counter — quarantine replaces the counter as the gate (Rust:
  /// `self.failed_rejoins.remove(convo_id)` on entry). Corruption/peer
  /// badness is tracked as STATE, never as count inflation.
  ///
  /// Persistence:
  /// - `until != nil` (horizon-bounded, e.g. server corruption): persists
  ///   the Rust-portable representation — `failedRejoinCount ==
  ///   maxRejoinAttempts` + `quarantinedUntilMs` — so the GATING effect
  ///   survives restart (hydration treats it as a maxed entry with an active
  ///   lockout). The quarantine REASON is in-memory only.
  /// - `until == nil` (indefinite, peer-bad classes): NOT persisted; the
  ///   persisted backoff row is write-through-DELETED (the enter-clears-
  ///   counter write-through). iOS has no event-driven exit signal wired
  ///   yet, so persisting an indefinite hold could permanently wedge a
  ///   conversation across restarts — the in-memory hold protects the
  ///   current process, which is where an epoch-storm actually unfolds.
  ///
  /// Trigger status (2026-06-11): no caller can classify peer-bad commits
  /// yet (Rust N38 typed decrypt errors are not in the iOS xcframework);
  /// production entry is `markConversationServerCorrupted` only.
  public func markQuarantined(
    convoId: String,
    reason: MLSQuarantineReason,
    suspectedDIDs: [String] = [],
    until: Date? = nil
  ) {
    let now = Date()
    // Enter-clears-counter (Rust parity).
    failedRejoins.removeValue(forKey: convoId)
    quarantineStates[convoId] = MLSQuarantineState(
      reason: reason,
      since: now,
      suspectedDIDs: suspectedDIDs,
      until: until
    )
    logger.error(
      "⛔️ [MLSRecoveryManager] QUARANTINED \(convoId.prefix(16)) — reason=\(reason.rawValue) horizon=\(until.map { "\($0)" } ?? "indefinite (event-driven exit)") suspectedDIDs=\(suspectedDIDs.count)"
    )

    if let until {
      quarantinedUntil[convoId] = until
      // Persist the Rust-portable maxed-entry representation so the gate
      // survives restart (count >= max + quarantined_until_ms is exactly
      // what the Rust twin's `record_rejoin_failure` persists when maxed).
      guard let persistence else {
        notePersistenceUnavailable("quarantine \(convoId.prefix(16))")
        return
      }
      let lastMs = Int64(now.timeIntervalSince1970 * 1000)
      let untilMs = Int64(until.timeIntervalSince1970 * 1000)
      let attempts = Self.maxRejoinAttempts
      enqueuePersist("quarantine \(convoId.prefix(16))") {
        try await persistence.upsertConversationState(
          conversationID: convoId,
          failedRejoinCount: attempts,
          lastAttemptAtMs: lastMs,
          quarantinedUntilMs: untilMs
        )
      }
    } else {
      quarantinedUntil.removeValue(forKey: convoId)
      // Indefinite quarantine: in-memory only (see doc comment). The
      // enter-clears-counter convention still writes through — the persisted
      // backoff row no longer reflects live state.
      persistClearConversationEntry(convoId: convoId)
    }
  }

  /// Event-driven quarantine exit (Rust `RecoveryTracker::clear_quarantine`).
  /// Call on server reset, healthy peer commit, or user-confirmed manual
  /// reset (`QuarantineExitReason` analogs). The bulk clears
  /// (`clearRejoinTracking`, `clearRejoinTrackingForFreshReset`) already
  /// perform this exit as part of their wipe.
  ///
  /// - Returns: `true` when a quarantine state was actually cleared.
  @discardableResult
  public func clearQuarantine(convoId: String) -> Bool {
    guard quarantineStates.removeValue(forKey: convoId) != nil else { return false }
    quarantinedUntil.removeValue(forKey: convoId)
    persistClearConversationEntry(convoId: convoId)
    logger.info(
      "✅ [MLSRecoveryManager] Quarantine cleared for \(convoId.prefix(16)) (event-driven exit)"
    )
    return true
  }

  /// Whether the conversation is currently Layer-3 quarantined (lazy horizon
  /// expiry applies). Rust `RecoveryTracker::is_quarantined` twin.
  public func isQuarantined(convoId: String) -> Bool {
    activeQuarantineState(convoId: convoId) != nil
  }

  /// Snapshot of the current quarantine state, if any (lazy horizon expiry
  /// applies). Rust `RecoveryTracker::quarantine_snapshot` twin.
  public func quarantineState(for convoId: String) -> MLSQuarantineState? {
    activeQuarantineState(convoId: convoId)
  }

  // MARK: - Server Data Corruption Handling

  /// Mark a conversation as having corrupted server data.
  /// This prevents retry loops when the server is serving bad GroupInfo;
  /// the conversation remains gated until server data is fixed or the 24h
  /// horizon lapses.
  ///
  /// N39 rework: this used to inflate the failure counter to
  /// `maxRejoinAttempts + 10` (the retired 13-band). It now enters Layer-3
  /// quarantine with `.serverDataCorruption` and a 24h horizon — the same
  /// enter-clears-counter convention the Rust twin uses (corruption is
  /// STATE, not count inflation). Gating across restart is preserved via
  /// the Rust-portable persisted row (`markQuarantined` horizon path).
  public func markConversationServerCorrupted(convoId: String, errorMessage: String) {
    logger.error(
      "🚫 [MLSRecoveryManager] Marking conversation \(convoId.prefix(16)) as SERVER-CORRUPTED")
    logger.error("   Error: \(errorMessage)")
    logger.error("   This conversation cannot be joined until server data is repaired")

    markQuarantined(
      convoId: convoId,
      reason: .serverDataCorruption,
      until: Date().addingTimeInterval(Self.persistedStateTTL)
    )
  }

  /// Check if a conversation is marked as having server-side corruption.
  ///
  /// N39: reads the Layer-3 quarantine state (reason ==
  /// `.serverDataCorruption`) instead of the retired `attempts > max + 5`
  /// count band. The reason is in-memory only — after a restart this returns
  /// `false` while the persisted horizon keeps gating rejoins (the gate is
  /// what matters; no production caller consumed the reason across restarts).
  public func isConversationServerCorrupted(convoId: String) -> Bool {
    activeQuarantineState(convoId: convoId)?.reason == .serverDataCorruption
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
    guard let apiClient = mlsAPIClient else {
      logger.error(
        "❌ [MLSRecoveryManager.verifyGroupInfoHealth] Cannot verify GroupInfo — no API client configured (test/headless context)"
      )
      return false
    }

    for attempt in 1...maxRetries {
      do {
        // Fetch stored GroupInfo
        let (storedData, epoch, _) = try await apiClient.getGroupInfo(convoId: convoId)

        // Check size
        if storedData.count < 100 {
          logger.error(
            "❌ [verifyGroupInfoHealth] GroupInfo too small: \(storedData.count) bytes (attempt \(attempt))"
          )

          if attempt < maxRetries {
            // Request republish from active members
            logger.info("🔄 [verifyGroupInfoHealth] Requesting GroupInfo refresh...")
            _ = try await apiClient.groupInfoRefresh(convoId: convoId)
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

/// Why a conversation entered Layer-3 quarantine. Mirrors the Rust
/// `QuarantineReason` (`catbird-mls/src/orchestrator/types.rs:267`, same
/// snake_case wire tags) plus the iOS-only `serverDataCorruption` case that
/// replaces the retired count-inflation band in
/// `markConversationServerCorrupted` (N39).
public enum MLSQuarantineReason: String, Sendable, Equatable {
  /// One peer produced 3+ classified peer-bad failures (Rust Signal C).
  /// Trigger gap: requires the N38 typed decrypt-error classes, not yet in
  /// the iOS xcframework.
  case peerBadCommit = "peer_bad_commit"
  /// 2+ distinct peers produced peer-bad failures within 60s (Rust Signal C
  /// multi-peer). Same N38 trigger gap.
  case multiPeerBadCommits = "multi_peer_bad_commits"
  /// 3+ distinct message IDs failed framing within 120s (Rust Signal D).
  /// Same N38 trigger gap.
  case repeatedFramingFailures = "repeated_framing_failures"
  /// Server served corrupt/truncated group data (GroupInfo deserialization
  /// failures etc.). iOS-only reason; carries a 24h horizon rather than an
  /// indefinite hold.
  case serverDataCorruption = "server_data_corruption"
}

/// Snapshot of a conversation's Layer-3 quarantine state. Swift twin of the
/// Rust `QuarantineState` (`catbird-mls/src/orchestrator/types.rs:310`),
/// extended with an optional `until` horizon for the iOS
/// `serverDataCorruption` case.
public struct MLSQuarantineState: Sendable, Equatable {
  public let reason: MLSQuarantineReason
  /// When the quarantine was entered.
  public let since: Date
  /// DIDs the classifier attributed the bad commits to. Empty when only the
  /// framing signal tripped, or for server-corruption entries.
  public let suspectedDIDs: [String]
  /// Expiry horizon. `nil` means indefinite — exit is event-driven only
  /// (`clearQuarantine`: server reset, healthy peer commit, user-confirmed
  /// reset), matching the Rust Layer-3 convention.
  public let until: Date?

  public init(reason: MLSQuarantineReason, since: Date, suspectedDIDs: [String], until: Date?) {
    self.reason = reason
    self.since = since
    self.suspectedDIDs = suspectedDIDs
    self.until = until
  }
}

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
