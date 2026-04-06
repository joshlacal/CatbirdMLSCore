import Foundation
import OSLog

public enum MLSNotificationMetric: String, Sendable {
  case cacheHitBeforeDecrypt = "cache_hit_before_decrypt"
  case decryptAttempt = "decrypt_attempt"
  case secretReuseRecovery = "secret_reuse_recovery"
  case handshakeTimeout = "handshake_timeout"
}

public enum MLSNotificationMetrics {
  private static let logger = Logger(subsystem: "blue.catbird.mls", category: "NotificationMetrics")
  private static let lock = NSLock()
  private static var counters: [String: Int] = [:]

  public static func increment(
    _ metric: MLSNotificationMetric,
    owner: MLSNotificationDecryptionOwner? = nil,
    context: MLSNotificationExecutionContext? = nil,
    source: String? = nil
  ) {
    let ownerRaw = owner?.rawValue ?? "none"
    let contextRaw = context?.rawValue ?? "none"
    let sourceRaw = source ?? "none"
    let key = "\(metric.rawValue)|owner=\(ownerRaw)|context=\(contextRaw)|source=\(sourceRaw)"

    lock.lock()
    counters[key, default: 0] += 1
    let value = counters[key] ?? 0
    lock.unlock()

    logger.debug(
      "📊 [MLS Metrics] metric=\(metric.rawValue, privacy: .public) owner=\(ownerRaw, privacy: .public) context=\(contextRaw, privacy: .public) source=\(sourceRaw, privacy: .public) value=\(value, privacy: .public)"
    )
  }

  public static func snapshot() -> [String: Int] {
    lock.lock()
    let snapshot = counters
    lock.unlock()
    return snapshot
  }
}
