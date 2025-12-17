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
    staleAfter seconds: TimeInterval = 300
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
}
