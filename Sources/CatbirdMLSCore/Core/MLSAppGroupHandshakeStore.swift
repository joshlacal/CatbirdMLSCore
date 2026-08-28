import CryptoKit
import Foundation
import OSLog

public struct MLSNSEWillCloseRequest: Codable, Sendable, Hashable {
  public let userDID: String
  public let token: UInt64
  public let createdAt: TimeInterval

  public init(userDID: String, token: UInt64, createdAt: TimeInterval) {
    self.userDID = userDID
    self.token = token
    self.createdAt = createdAt
  }
}

public struct MLSAppAcknowledgment: Codable, Sendable, Hashable {
  public let userDID: String
  public let token: UInt64
  public let acknowledgedAt: TimeInterval

  public init(userDID: String, token: UInt64, acknowledgedAt: TimeInterval) {
    self.userDID = userDID
    self.token = token
    self.acknowledgedAt = acknowledgedAt
  }
}

public final class MLSAppGroupHandshakeStore: @unchecked Sendable {
  public static let shared = MLSAppGroupHandshakeStore()

  private static let suiteName = "group.blue.catbird.shared"

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSHandshakeStore")
  private var defaults: UserDefaults {
    guard let defaults = UserDefaults(suiteName: Self.suiteName) else {
      fatalError("Required App Group suite \(Self.suiteName) unavailable for MLSAppGroupHandshakeStore")
    }
    return defaults
  }

  private init() {}

  // MARK: - Public API

  @discardableResult
  public func issueWillCloseRequest(for userDID: String) -> MLSNSEWillCloseRequest {
    let now = Date().timeIntervalSince1970
    let counterKey = counterKey(for: userDID)
    let current: UInt64
    if let obj = defaults.object(forKey: counterKey) {
      if let number = obj as? NSNumber {
        current = number.uint64Value
      } else {
        logger.critical("🚨 [Handshake] Non-numeric counter found in defaults for key: \(counterKey), treating as 0")
        current = 0
      }
    } else {
      current = 0
    }
    let token = current &+ 1
    defaults.set(token, forKey: counterKey)
    defaults.synchronize()
    let request = MLSNSEWillCloseRequest(userDID: userDID, token: token, createdAt: now)
    set(request, forKey: requestKey(for: userDID))
    logger.debug("📌 [Handshake] Issued willClose token=\(token, privacy: .public) for \(userDID.prefix(20), privacy: .private)")
    return request
  }

  public func acknowledge(userDID: String, token: UInt64) {
    let now = Date().timeIntervalSince1970
    let key = ackKey(for: userDID)

    let nextToken = max(currentAckToken(for: userDID) ?? 0, token)
    let ack = MLSAppAcknowledgment(userDID: userDID, token: nextToken, acknowledgedAt: now)
    set(ack, forKey: key)
    logger.debug("✅ [Handshake] Ack token=\(nextToken, privacy: .public) for \(userDID.prefix(20), privacy: .private)")
  }

  public func currentRequest(for userDID: String) -> MLSNSEWillCloseRequest? {
    get(MLSNSEWillCloseRequest.self, forKey: requestKey(for: userDID))
  }

  public func currentAcknowledgment(for userDID: String) -> MLSAppAcknowledgment? {
    get(MLSAppAcknowledgment.self, forKey: ackKey(for: userDID))
  }

  public func isAcknowledged(userDID: String, token: UInt64) -> Bool {
    (currentAckToken(for: userDID) ?? 0) >= token
  }

  public func allRequests() -> [MLSNSEWillCloseRequest] {
    let dict = defaults.dictionaryRepresentation()
    var results: [MLSNSEWillCloseRequest] = []
    results.reserveCapacity(4)

    for (key, value) in dict where key.hasPrefix("mls_handshake_request.") && key.hasSuffix(".\(MLSStoragePaths.cleanSuffix)") {
      guard let data = value as? Data else {
        logger.critical("🚨 [Handshake] Skipping non-Data handshake request for key: \(key)")
        continue
      }
      if let record = decode(MLSNSEWillCloseRequest.self, from: data) {
        results.append(record)
      }
    }
    return results
  }

  public func waitForAcknowledgment(
    userDID: String,
    token: UInt64,
    timeout: Duration
  ) async -> Bool {
    if isAcknowledged(userDID: userDID, token: token) {
      return true
    }

    let logger = self.logger
    let clock = ContinuousClock()
    let started = clock.now
    let deadline = clock.now.advanced(by: timeout)

    var delay = Duration.milliseconds(20)
    let maxDelay = Duration.milliseconds(200)
    var attempts = 0

    while clock.now < deadline {
      attempts += 1
      if isAcknowledged(userDID: userDID, token: token) {
        let waited = started.duration(to: clock.now)
        if waited > .milliseconds(50) {
          logger.info("🤝 [Handshake] Acknowledged after \(String(describing: waited), privacy: .public) (\(attempts, privacy: .public) polls)")
        }
        return true
      }

      try? await Task.sleep(for: delay)
      delay = min(delay * 2, maxDelay)
    }

    let waited = started.duration(to: clock.now)
    logger.warning("⚠️ [Handshake] Timed out waiting for app ack (token=\(token, privacy: .public), user=\(userDID.prefix(20), privacy: .private))")
    logger.warning("   waited=\(String(describing: waited), privacy: .public) attempts=\(attempts, privacy: .public)")
    return false
  }

  func clearAll(for userDID: String) {
    defaults.removeObject(forKey: counterKey(for: userDID))
    defaults.removeObject(forKey: requestKey(for: userDID))
    defaults.removeObject(forKey: ackKey(for: userDID))
    defaults.synchronize()
  }

  // MARK: - Key Helpers

  private func counterKey(for userDID: String) -> String {
    "mls_handshake_counter.\(userKeySuffix(for: userDID)).\(MLSStoragePaths.cleanSuffix)"
  }

  private func requestKey(for userDID: String) -> String {
    "mls_handshake_request.\(userKeySuffix(for: userDID)).\(MLSStoragePaths.cleanSuffix)"
  }

  private func ackKey(for userDID: String) -> String {
    "mls_handshake_ack.\(userKeySuffix(for: userDID)).\(MLSStoragePaths.cleanSuffix)"
  }

  private func userKeySuffix(for userDID: String) -> String {
    let digest = SHA256.hash(data: Data(userDID.utf8))
    let hex = digest.compactMap { String(format: "%02x", $0) }.joined()
    return String(hex.prefix(16))
  }

  // MARK: - Storage Helpers

  private func currentAckToken(for userDID: String) -> UInt64? {
    currentAcknowledgment(for: userDID)?.token
  }

  private func set<T: Encodable>(_ value: T, forKey key: String) {
    let encoder = JSONEncoder()
    do {
      let data = try encoder.encode(value)
      defaults.set(data, forKey: key)
      defaults.synchronize()
    } catch {
      logger.critical("🚨 [Handshake] Failed to encode handshake record for key \(key): \(error.localizedDescription)")
    }
  }

  private func get<T: Decodable>(_ type: T.Type, forKey key: String) -> T? {
    guard let obj = defaults.object(forKey: key) else { return nil }
    guard let data = obj as? Data else {
      logger.critical("🚨 [Handshake] Non-Data object in defaults for key \(key)")
      return nil
    }
    return decode(type, from: data)
  }

  private func decode<T: Decodable>(_ type: T.Type, from data: Data) -> T? {
    let decoder = JSONDecoder()
    do {
      return try decoder.decode(type, from: data)
    } catch {
      logger.critical("🚨 [Handshake] Failed to decode handshake record: \(error.localizedDescription)")
      return nil
    }
  }
}
