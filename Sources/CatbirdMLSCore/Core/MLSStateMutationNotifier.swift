import Foundation
import OSLog

public enum MLSStateMutationNotifier {
  private static let logger = Logger(subsystem: "blue.catbird.mls", category: "StateMutationNotifier")

  @discardableResult
  public static func publish(
    userDID: String,
    source: String,
    decryptionOwner: MLSNotificationDecryptionOwner? = nil,
    routingAction: MLSNotificationAction? = nil,
    routingReason: MLSNotificationRoutingReason? = nil,
    incrementVersion: Bool = true,
    postStateChanged: Bool = true
  ) -> Int? {
    let ownerRaw = decryptionOwner?.rawValue ?? "none"
    let actionRaw = routingAction?.rawValue ?? "none"
    let reasonRaw = routingReason?.rawValue ?? "none"

    let versionBefore = MLSStateVersionManager.shared.getDiskVersion(for: userDID)
    let versionAfter: Int
    if incrementVersion {
      versionAfter = MLSStateVersionManager.shared.incrementVersion(for: userDID)
    } else {
      versionAfter = versionBefore
    }

    if postStateChanged {
      MLSStateChangeNotifier.postStateChanged()
    }

    logger.info(
      "📡 [MLS Mutation] source=\(source, privacy: .public) decryption_owner=\(ownerRaw, privacy: .public) routing_action=\(actionRaw, privacy: .public) policy_reason=\(reasonRaw, privacy: .public) version_before=\(versionBefore, privacy: .public) version_after=\(versionAfter, privacy: .public) state_changed=\(postStateChanged, privacy: .public)"
    )

    return incrementVersion ? versionAfter : nil
  }
}
