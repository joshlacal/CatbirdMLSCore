import Foundation
import OSLog
/// Cross-process authority-mode hint shared from the main app to extensions.
///
/// The main Catbird process still owns mode parsing. Extensions read this narrow
/// app-group value so rustFull can avoid direct Swift/OpenMLS protocol work.
public enum MLSAuthorityModeSharedState {
  private static let suiteName = "group.blue.catbird.shared"
  public static let userDefaultsKey = "mls.protocol_authority_mode.\(MLSStoragePaths.cleanSuffix)"
  private static let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSAuthorityMode")


  private static var defaults: UserDefaults {
    guard let defaults = UserDefaults(suiteName: suiteName) else {
      fatalError("Required App Group suite \(suiteName) unavailable for MLSAuthorityModeSharedState")
    }
    return defaults
  }

  public static func setCurrentMode(_ mode: MLSProtocolAuthorityMode) {
    defaults.set(mode.rawValue, forKey: userDefaultsKey)
    defaults.synchronize()
  }

  public static func currentMode() -> MLSProtocolAuthorityMode {
    guard let obj = defaults.object(forKey: userDefaultsKey) else {
      return .defaultMode
    }
    guard let rawValue = obj as? String,
      let mode = MLSProtocolAuthorityMode(rawRuntimeValue: rawValue)
    else {
      logger.critical("🚨 [AuthorityMode] Corrupt or invalid authority mode in shared defaults: \(String(describing: obj)), returning .defaultMode")
      return .defaultMode
    }
    return mode
  }

  public static var isRustFullEnabled: Bool {
    currentMode() == .rustFull
  }

  public static func clearForTesting() {
    defaults.removeObject(forKey: userDefaultsKey)
    defaults.synchronize()
  }
}
