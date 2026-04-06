import Foundation
import OSLog

public enum MLSNotificationCoordinator {
  private static let logger = Logger(subsystem: "blue.catbird.mls", category: "NotificationCoordinator")

  public static var isRoutingV2Enabled: Bool {
    MLSNotificationRoutingPolicy.isRoutingV2Enabled
  }

  public static func setRoutingV2EnabledForTesting(_ enabled: Bool) {
    MLSNotificationRoutingPolicy.setRoutingV2EnabledForTesting(enabled)
  }

  public static func routingDecision(
    context: MLSNotificationExecutionContext,
    recipientUserDID: String?
  ) -> MLSNotificationRoutingDecision {
    let decision = MLSNotificationRoutingPolicy.decide(
      context: context,
      recipientUserDID: recipientUserDID
    )

    logger.info(
      "🧭 [MLS Routing] context=\(context.rawValue, privacy: .public) routing_action=\(decision.action.rawValue, privacy: .public) policy_reason=\(decision.reason.rawValue, privacy: .public) decryption_owner=\(decision.owner?.rawValue ?? "none", privacy: .public)"
    )

    return decision
  }

  @MainActor
  public static func configureAppObservers(
    onStateChanged: @escaping @MainActor () async -> Void,
    onNSEWillClose: @escaping @MainActor (MLSNSEWillCloseRequest) async -> Bool
  ) {
    MLSStateChangeNotifier.shared.observeWithAsyncHandler(onStateChanged)
    MLSStateChangeNotifier.shared.observeNSEWillCloseWithAsyncHandler(onNSEWillClose)
  }

  public static func stopAppObservers() {
    MLSStateChangeNotifier.shared.stopObserving()
  }

  public static func setMainAppActive(_ isActive: Bool, activeUserDID: String?) {
    MLSAppActivityState.setMainAppActive(isActive, activeUserDID: activeUserDID)
  }

  public static func updateActiveUserDID(_ userDID: String?) {
    MLSAppActivityState.updateActiveUserDID(userDID)
  }

  public static func setShuttingDown(_ isShuttingDown: Bool, userDID: String?) {
    MLSAppActivityState.setShuttingDown(isShuttingDown, userDID: userDID)
  }

  public static func beginAccountSwitch(from fromDID: String?, to toDID: String) {
    MLSAppActivityState.beginAccountSwitch(from: fromDID, to: toDID)
  }

  public static func endAccountSwitch() {
    MLSAppActivityState.endAccountSwitch()
  }

  public static func recordNSEProcessed(for userDID: String) {
    MLSAppActivityState.signalNSEProcessed(for: userDID)
  }

  @discardableResult
  public static func prepareNSECloseHandshake(userDID: String) -> UInt64 {
    let token = MLSStateChangeNotifier.postNSEWillClose(userDID: userDID)
    logger.info(
      "🤝 [MLS Handshake] nse_will_close token=\(token, privacy: .public) user=\(userDID.prefix(20), privacy: .private)"
    )
    return token
  }

  public static func waitForAppAcknowledgment(
    userDID: String,
    token: UInt64,
    timeout: Duration
  ) async -> Bool {
    let acked = await MLSStateChangeNotifier.waitForAppAcknowledgment(
      userDID: userDID,
      token: token,
      timeout: timeout
    )

    if !acked {
      MLSNotificationMetrics.increment(
        .handshakeTimeout,
        owner: .nse,
        context: .nseBackground,
        source: "nse_handshake"
      )
      logger.warning(
        "⏱️ [MLS Handshake] timeout token=\(token, privacy: .public) user=\(userDID.prefix(20), privacy: .private)"
      )
    }

    return acked
  }

  public static func postStateChanged() {
    MLSStateChangeNotifier.postStateChanged()
  }

  @discardableResult
  public static func publishMutation(
    userDID: String,
    source: String,
    decryptionOwner: MLSNotificationDecryptionOwner? = nil,
    routingAction: MLSNotificationAction? = nil,
    routingReason: MLSNotificationRoutingReason? = nil,
    incrementVersion: Bool = true,
    postStateChanged: Bool = true
  ) -> Int? {
    MLSStateMutationNotifier.publish(
      userDID: userDID,
      source: source,
      decryptionOwner: decryptionOwner,
      routingAction: routingAction,
      routingReason: routingReason,
      incrementVersion: incrementVersion,
      postStateChanged: postStateChanged
    )
  }
}
