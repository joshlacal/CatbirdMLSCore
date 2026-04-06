import Foundation

/// Shared app-group activity flag used to prevent the Notification Service Extension (NSE)
/// from advancing the MLS ratchet while the main app is actively running for the same user.
public enum MLSAppActivityState {
  private static let suiteName = "group.blue.catbird.shared"
  private static let isActiveKey = "mls_main_app_is_active"
  private static let activeUserDIDKey = "mls_main_app_active_user_did"
  private static let updatedAtKey = "mls_main_app_activity_updated_at"

  private static var defaults: UserDefaults? { UserDefaults(suiteName: suiteName) }

  public static func setMainAppActive(_ isActive: Bool, activeUserDID: String?) {
    guard let defaults else { return }
    defaults.set(isActive, forKey: isActiveKey)
    defaults.set(activeUserDID, forKey: activeUserDIDKey)
    defaults.set(Date().timeIntervalSince1970, forKey: updatedAtKey)
  }

  public static func updateActiveUserDID(_ userDID: String?) {
    guard let defaults else { return }
    defaults.set(userDID, forKey: activeUserDIDKey)
    defaults.set(Date().timeIntervalSince1970, forKey: updatedAtKey)
  }

  /// Returns false when the main app is (likely) active for this recipient DID.
  /// A stale timeout avoids permanently suppressing NSE decryption after crashes.
  public static func shouldNSEDecrypt(
    recipientUserDID: String,
    staleAfter seconds: TimeInterval = 30
  ) -> Bool {
    guard let defaults else { return true }

    let isActive = defaults.bool(forKey: isActiveKey)
    let activeUser = defaults.string(forKey: activeUserDIDKey)
    let updatedAt = defaults.double(forKey: updatedAtKey)

    if updatedAt > 0, Date().timeIntervalSince1970 - updatedAt > seconds {
      return true
    }

    return !(isActive && activeUser == recipientUserDID)
  }

  // MARK: - Shutdown Signaling (Cross-Process Coordination)

  private static let isShuttingDownKey = "mls_main_app_is_shutting_down"

  /// Signal that the main app is shutting down (starting account switch)
  /// NSE should yield database access during this period
  public static func setShuttingDown(_ isShuttingDown: Bool, userDID: String?) {
    guard let defaults else { return }
    defaults.set(isShuttingDown, forKey: isShuttingDownKey)
    defaults.set(userDID, forKey: activeUserDIDKey)
    defaults.set(Date().timeIntervalSince1970, forKey: updatedAtKey)
  }

  /// Check if the main app is shutting down for a specific user
  /// Returns false if the shutdown flag is stale (older than timeout)
  public static func isShuttingDown(for userDID: String, staleAfter seconds: TimeInterval = 30)
    -> Bool
  {
    guard let defaults else { return false }

    let shuttingDown = defaults.bool(forKey: isShuttingDownKey)
    let activeUser = defaults.string(forKey: activeUserDIDKey)
    let updatedAt = defaults.double(forKey: updatedAtKey)

    // Stale check - don't block forever if app crashed during shutdown
    if updatedAt > 0, Date().timeIntervalSince1970 - updatedAt > seconds {
      return false
    }

    return shuttingDown && activeUser == userDID
  }

  // MARK: - Account Switching Phase (Pre-Shutdown Coordination)

  private static let isSwitchingKey = "mls_main_app_is_switching"
  private static let switchingFromUserKey = "mls_switching_from_user"
  private static let switchingToUserKey = "mls_switching_to_user"
  private static let accountSwitchEpochKey = "mls_account_switch_epoch"

  /// Get the current account switch epoch counter.
  /// This monotonically increases with each account switch.
  /// Operations can check this before/after critical sections to detect concurrent switches.
  public static func getAccountSwitchEpoch() -> UInt64 {
    guard let defaults else { return 0 }
    return defaults.object(forKey: accountSwitchEpochKey) as? UInt64 ?? 0
  }

  /// Increment the account switch epoch counter.
  /// Call this at the START of every account switch to signal other processes.
  private static func incrementAccountSwitchEpoch() {
    guard let defaults else { return }
    let current = getAccountSwitchEpoch()
    defaults.set(current + 1, forKey: accountSwitchEpochKey)
  }

  /// Signal that an account switch is starting (BEFORE any MLS work stops)
  /// This is set at the very beginning of account switch, before shutdown signals.
  /// NSE should yield database access for BOTH the old and new user during this period.
  public static func beginAccountSwitch(from fromDID: String?, to toDID: String) {
    guard let defaults else { return }
    // Increment epoch FIRST to signal all processes immediately
    incrementAccountSwitchEpoch()
    defaults.set(true, forKey: isSwitchingKey)
    defaults.set(fromDID, forKey: switchingFromUserKey)
    defaults.set(toDID, forKey: switchingToUserKey)
    defaults.set(Date().timeIntervalSince1970, forKey: updatedAtKey)
  }

  /// Signal that account switch is complete
  /// Call this after the new account is fully initialized and ready
  public static func endAccountSwitch() {
    guard let defaults else { return }
    defaults.set(false, forKey: isSwitchingKey)
    defaults.removeObject(forKey: switchingFromUserKey)
    defaults.removeObject(forKey: switchingToUserKey)
    defaults.set(Date().timeIntervalSince1970, forKey: updatedAtKey)
  }

  /// Check if an account switch is in progress that affects this user
  /// Returns true if the user is either the source or destination of an active switch.
  /// This is more aggressive than isShuttingDown - it blocks BOTH accounts during the transition.
  public static func isSwitchingAffecting(userDID: String, staleAfter seconds: TimeInterval = 30)
    -> Bool
  {
    guard let defaults else { return false }

    let switching = defaults.bool(forKey: isSwitchingKey)
    let fromUser = defaults.string(forKey: switchingFromUserKey)
    let toUser = defaults.string(forKey: switchingToUserKey)
    let updatedAt = defaults.double(forKey: updatedAtKey)

    // Stale check - don't block forever if app crashed during switch
    if updatedAt > 0, Date().timeIntervalSince1970 - updatedAt > seconds {
      return false
    }

    return switching && (fromUser == userDID || toUser == userDID)
  }

  /// Check if any account switch is in progress (regardless of which users)
  /// Useful for blocking all MLS operations globally during transitions
  public static func isAnySwitchInProgress(staleAfter seconds: TimeInterval = 30) -> Bool {
    guard let defaults else { return false }

    let switching = defaults.bool(forKey: isSwitchingKey)
    let updatedAt = defaults.double(forKey: updatedAtKey)

    // Stale check
    if updatedAt > 0, Date().timeIntervalSince1970 - updatedAt > seconds {
      return false
    }

    return switching
  }

  // MARK: - NSE Ratchet Advancement Signal (Fix #3: Race Condition Prevention)

  private static let nseProcessedMessageKey = "mls_nse_processed_message_for_user"
  private static let nseProcessedAtKey = "mls_nse_processed_at"

  /// Signal that NSE has processed (decrypted) a message for a user.
  /// The main app should reload MLS state from disk when it resumes.
  /// - Parameter userDID: The user whose MLS ratchet was advanced
  public static func signalNSEProcessed(for userDID: String) {
    guard let defaults else { return }
    defaults.set(userDID, forKey: nseProcessedMessageKey)
    defaults.set(Date().timeIntervalSince1970, forKey: nseProcessedAtKey)
  }

  /// Check if NSE has processed a message for this user since the app was last active.
  /// If true, the main app should force a state reload before processing messages.
  /// - Parameter userDID: The user to check
  /// - Parameter staleAfter: Ignore flags older than this (prevents false positives from old runs)
  /// - Returns: True if NSE processed a message and the flag hasn't been cleared yet
  public static func hasNSEProcessed(for userDID: String, staleAfter seconds: TimeInterval = 60)
    -> Bool
  {
    guard let defaults else { return false }

    let processedUser = defaults.string(forKey: nseProcessedMessageKey)
    let processedAt = defaults.double(forKey: nseProcessedAtKey)

    // Stale check - ignore very old flags
    if processedAt > 0, Date().timeIntervalSince1970 - processedAt > seconds {
      return false
    }

    return processedUser == userDID
  }

  /// Clear the NSE-processed flag after the main app has reloaded state.
  /// Call this after successfully calling reloadStateFromDisk().
  public static func clearNSEProcessedFlag() {
    guard let defaults else { return }
    defaults.removeObject(forKey: nseProcessedMessageKey)
    defaults.removeObject(forKey: nseProcessedAtKey)
  }
}