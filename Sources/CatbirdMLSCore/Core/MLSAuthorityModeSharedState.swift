import Foundation

/// Cross-process authority-mode hint shared from the main app to extensions.
///
/// The main Catbird process still owns mode parsing. Extensions read this narrow
/// app-group value so rustFull can avoid direct Swift/OpenMLS protocol work.
public enum MLSAuthorityModeSharedState {
  private static let suiteName = "group.blue.catbird.shared"
  public static let userDefaultsKey = "mls.protocol_authority_mode"

  private static var defaults: UserDefaults {
    UserDefaults(suiteName: suiteName) ?? .standard
  }

  public static func setCurrentMode(_ mode: MLSProtocolAuthorityMode) {
    defaults.set(mode.rawValue, forKey: userDefaultsKey)
    defaults.synchronize()
  }

  public static func currentMode() -> MLSProtocolAuthorityMode {
    guard let rawValue = defaults.string(forKey: userDefaultsKey),
      let mode = MLSProtocolAuthorityMode(rawRuntimeValue: rawValue)
    else {
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
