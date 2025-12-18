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
  private let defaults: UserDefaults

  private init() {
    if let defaults = UserDefaults(suiteName: Self.suiteName) {
      self.defaults = defaults
    } else {
      self.defaults = UserDefaults.standard
      logger.warning("⚠️ [Handshake] Shared UserDefaults suite not available - token handshake won't work cross-process")
    }
  }

  // MARK: - Public API

  /// Issue (or replace) the current NSE will-close request for a user and return its token.
  ///
  /// Tokens are per-user and monotonic. If a newer token is acknowledged, it implicitly
  /// acknowledges all older tokens for the same user.
  @discardableResult
  public func issueWillCloseRequest(for userDID: String) -> MLSNSEWillCloseRequest {
    let now = Date().timeIntervalSince1970

    var token: UInt64
    do {
      token = try ProcessCoordinator.shared.performExclusive {
        let counterKey = counterKey(for: userDID)
        let current: UInt64
        if let number = defaults.object(forKey: counterKey) as? NSNumber {
          current = number.uint64Value
        } else {
          current = UInt64(defaults.integer(forKey: counterKey))
        }
        let next = current &+ 1
        defaults.set(next, forKey: counterKey)
        defaults.synchronize()
        return next
      }
    } catch {
      logger.error("🚫 [Handshake] Failed to acquire token counter lock: \(error.localizedDescription)")
      // Best-effort fallback: still attempt to advance the counter even without the lock.
      // This preserves monotonicity and avoids generating a token that might appear "already acked".
      let counterKey = counterKey(for: userDID)
      let current: UInt64
      if let number = defaults.object(forKey: counterKey) as? NSNumber {
        current = number.uint64Value
      } else {
        current = UInt64(defaults.integer(forKey: counterKey))
      }
      token = current &+ 1
      defaults.set(token, forKey: counterKey)
      defaults.synchronize()
    }

    let request = MLSNSEWillCloseRequest(userDID: userDID, token: token, createdAt: now)
    set(request, forKey: requestKey(for: userDID))
    logger.debug("📌 [Handshake] Issued willClose token=\(token, privacy: .public) for \(userDID.prefix(20), privacy: .private)")
    return request
  }

  /// Record an app acknowledgment for a token. This is idempotent and monotonic:
  /// the stored acknowledgment token only ever increases.
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

  /// Enumerate all current will-close requests written by the NSE.
  ///
  /// This is used by the main app upon receiving the Darwin doorbell.
  public func allRequests() -> [MLSNSEWillCloseRequest] {
    let dict = defaults.dictionaryRepresentation()
    let prefix = Self.requestKeyPrefix

    var results: [MLSNSEWillCloseRequest] = []
    results.reserveCapacity(4)

    for (key, value) in dict where key.hasPrefix(prefix) {
      guard let data = value as? Data else { continue }
      if let record = decode(MLSNSEWillCloseRequest.self, from: data) {
        results.append(record)
      }
    }

    return results
  }

  /// Wait until the app acknowledgment token is >= the requested token.
  ///
  /// This is intended for the NSE after posting the doorbell notification.
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
          logger.debug(
            "✅ [Handshake] Ack observed for token=\(token, privacy: .public) in \(String(describing: waited)) (attempts=\(attempts, privacy: .public))"
          )
        }
        return true
      }

      do {
        try Task.checkCancellation()
      } catch {
        return false
      }

      let remaining = clock.now.duration(to: deadline)
      if remaining <= .zero {
        break
      }

      let jittered = delay + .milliseconds(Int.random(in: 0...30))
      do {
        try await Task.sleep(for: min(jittered, remaining))
      } catch {
        return false
      }

      delay = min(delay + delay, maxDelay)
    }

    let waited = started.duration(to: clock.now)
    logger.warning("⏱️ [Handshake] Ack wait timed out for token=\(token, privacy: .public) \(userDID.prefix(20), privacy: .private)")
    logger.warning("   waited=\(String(describing: waited), privacy: .public) attempts=\(attempts, privacy: .public)")
    return false
  }

  // MARK: - Key Helpers

  private static let counterKeyPrefix = "mls_handshake_counter."
  private static let requestKeyPrefix = "mls_handshake_request."
  private static let ackKeyPrefix = "mls_handshake_ack."

  private func counterKey(for userDID: String) -> String {
    "\(Self.counterKeyPrefix)\(userKeySuffix(for: userDID))"
  }

  private func requestKey(for userDID: String) -> String {
    "\(Self.requestKeyPrefix)\(userKeySuffix(for: userDID))"
  }

  private func ackKey(for userDID: String) -> String {
    "\(Self.ackKeyPrefix)\(userKeySuffix(for: userDID))"
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
    guard let data = try? encoder.encode(value) else { return }
    defaults.set(data, forKey: key)
    defaults.synchronize()
  }

  private func get<T: Decodable>(_ type: T.Type, forKey key: String) -> T? {
    guard let data = defaults.data(forKey: key) else { return nil }
    return decode(type, from: data)
  }

  private func decode<T: Decodable>(_ type: T.Type, from data: Data) -> T? {
    let decoder = JSONDecoder()
    return try? decoder.decode(type, from: data)
  }
}
