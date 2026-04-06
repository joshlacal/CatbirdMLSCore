import Foundation

public enum MLSNotificationExecutionContext: String, Sendable {
  case appForegroundActive = "app_foreground_active"
  case appForegroundInactive = "app_foreground_inactive"
  case nseBackground = "nse_background"
}

public enum MLSNotificationAction: String, Sendable {
  case cacheOnly = "cache_only"
  case decrypt = "decrypt"
  case skip = "skip"
}

public enum MLSNotificationDecryptionOwner: String, Sendable {
  case appSync = "app_sync"
  case appNotification = "app_notification"
  case nse = "nse"
}

public enum MLSNotificationRoutingReason: String, Sendable {
  case missingRecipient = "missing_recipient"
  case accountSwitchInProgress = "account_switch_in_progress"
  case appShuttingDown = "app_shutting_down"
  case nseYieldMainAppActive = "nse_yield_main_app_active"
  case foregroundActiveUsesSync = "foreground_active_uses_sync"
  case foregroundInactiveDecrypt = "foreground_inactive_decrypt"
  case nseBackgroundDecrypt = "nse_background_decrypt"
  case legacyRoutingDisabled = "legacy_routing_disabled"
}

public struct MLSNotificationRoutingDecision: Sendable {
  public let action: MLSNotificationAction
  public let owner: MLSNotificationDecryptionOwner?
  public let reason: MLSNotificationRoutingReason

  public init(
    action: MLSNotificationAction,
    owner: MLSNotificationDecryptionOwner?,
    reason: MLSNotificationRoutingReason
  ) {
    self.action = action
    self.owner = owner
    self.reason = reason
  }
}

public enum MLSNotificationRoutingPolicy {
  private static let sharedSuiteName = "group.blue.catbird.shared"
  public static let routingV2UserDefaultsKey = "mls.notification.routing.v2"

  private static var sharedDefaults: UserDefaults? {
    UserDefaults(suiteName: sharedSuiteName)
  }

  public static var isRoutingV2Enabled: Bool {
    guard let defaults = sharedDefaults else { return true }
    if defaults.object(forKey: routingV2UserDefaultsKey) == nil {
      return true
    }
    return defaults.bool(forKey: routingV2UserDefaultsKey)
  }

  public static func setRoutingV2EnabledForTesting(_ enabled: Bool) {
    sharedDefaults?.set(enabled, forKey: routingV2UserDefaultsKey)
  }

  public static func decide(
    context: MLSNotificationExecutionContext,
    recipientUserDID: String?
  ) -> MLSNotificationRoutingDecision {
    guard let recipientUserDID, !recipientUserDID.isEmpty else {
      return MLSNotificationRoutingDecision(
        action: .skip,
        owner: nil,
        reason: .missingRecipient
      )
    }

    if !isRoutingV2Enabled {
      return legacyDecision(context: context)
    }

    if MLSAppActivityState.isSwitchingAffecting(userDID: recipientUserDID) {
      return MLSNotificationRoutingDecision(
        action: .skip,
        owner: nil,
        reason: .accountSwitchInProgress
      )
    }

    if MLSAppActivityState.isShuttingDown(for: recipientUserDID) {
      return MLSNotificationRoutingDecision(
        action: .skip,
        owner: nil,
        reason: .appShuttingDown
      )
    }

    switch context {
    case .appForegroundActive:
      return MLSNotificationRoutingDecision(
        action: .cacheOnly,
        owner: .appSync,
        reason: .foregroundActiveUsesSync
      )

    case .appForegroundInactive:
      return MLSNotificationRoutingDecision(
        action: .decrypt,
        owner: .appNotification,
        reason: .foregroundInactiveDecrypt
      )

    case .nseBackground:
      if !MLSAppActivityState.shouldNSEDecrypt(recipientUserDID: recipientUserDID) {
        return MLSNotificationRoutingDecision(
          action: .skip,
          owner: nil,
          reason: .nseYieldMainAppActive
        )
      }

      return MLSNotificationRoutingDecision(
        action: .decrypt,
        owner: .nse,
        reason: .nseBackgroundDecrypt
      )
    }
  }

  private static func legacyDecision(
    context: MLSNotificationExecutionContext
  ) -> MLSNotificationRoutingDecision {
    switch context {
    case .appForegroundActive, .appForegroundInactive:
      return MLSNotificationRoutingDecision(
        action: .decrypt,
        owner: .appNotification,
        reason: .legacyRoutingDisabled
      )
    case .nseBackground:
      return MLSNotificationRoutingDecision(
        action: .decrypt,
        owner: .nse,
        reason: .legacyRoutingDisabled
      )
    }
  }
}
