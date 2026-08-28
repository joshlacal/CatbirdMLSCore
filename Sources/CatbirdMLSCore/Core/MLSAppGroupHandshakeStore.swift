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
  public func issueWillCloseRequest(for userDID: String) throws -> MLSNSEWillCloseRequest {
    try withHandshakeMutationLock {
      let now = Date().timeIntervalSince1970
      let counterKey = counterKey(for: userDID)
      let current: UInt64
      if let obj = defaults.object(forKey: counterKey) {
        if let number = obj as? NSNumber {
          current = number.uint64Value
        } else {
          logger.critical("🚨 [Handshake] Non-numeric counter found in defaults for key: \(counterKey)")
          throw MLSStorageInitializationError.unreadableState(
            details: "Corrupt non-numeric handshake counter for \(userDID)"
          )
        }
      } else {
        current = 0
      }
      guard current < UInt64.max else {
        throw MLSStorageInitializationError.validationFailed(
          details: "Handshake counter overflow for \(userDID)"
        )
      }
      let token = current + 1
      guard token > 0 else {
        throw MLSStorageInitializationError.validationFailed(
          details: "Invalid zero handshake token for \(userDID)"
        )
      }

      // Update generation-scoped active request index
      var activeDIDs: [String]
      if let obj = defaults.object(forKey: activeRequestsIndexKey) {
        guard let arr = obj as? [String] else {
          throw MLSStorageInitializationError.unreadableState(
            details: "Corrupt active requests index in defaults"
          )
        }
        activeDIDs = arr
      } else {
        activeDIDs = []
      }
      if !activeDIDs.contains(userDID) {
        activeDIDs.append(userDID)
        defaults.set(activeDIDs, forKey: activeRequestsIndexKey)
      }

      defaults.set(token, forKey: counterKey)
      let request = MLSNSEWillCloseRequest(userDID: userDID, token: token, createdAt: now)
      set(request, forKey: requestKey(for: userDID))
      defaults.synchronize()

      logger.debug("📌 [Handshake] Issued willClose token=\(token, privacy: .public) for \(userDID.prefix(20), privacy: .private)")
      return request
    }
  }

  public func acknowledge(userDID: String, token: UInt64) {
    guard token > 0 else { return }
    let now = Date().timeIntervalSince1970
    let key = ackKey(for: userDID)

    let currentToken = currentAckToken(for: userDID) ?? 0
    let nextToken = max(currentToken, token)
    let ack = MLSAppAcknowledgment(userDID: userDID, token: nextToken, acknowledgedAt: now)
    set(ack, forKey: key)
    logger.debug("✅ [Handshake] Ack token=\(nextToken, privacy: .public) for \(userDID.prefix(20), privacy: .private)")
  }

  public func currentRequest(for userDID: String) -> MLSNSEWillCloseRequest? {
    guard let req = get(MLSNSEWillCloseRequest.self, forKey: requestKey(for: userDID)) else {
      return nil
    }
    let normReqDID = req.userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let normExpectedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    guard req.token > 0 && normReqDID == normExpectedDID else {
      logger.critical("🚨 [Handshake] Corrupt/mismatched current request record for \(userDID)")
      return nil
    }
    return req
  }

  public func currentAcknowledgment(for userDID: String) -> MLSAppAcknowledgment? {
    guard let ack = get(MLSAppAcknowledgment.self, forKey: ackKey(for: userDID)) else {
      return nil
    }
    let normAckDID = ack.userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let normExpectedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    guard ack.token > 0 && normAckDID == normExpectedDID else {
      logger.critical("🚨 [Handshake] Corrupt/mismatched acknowledgment record for \(userDID)")
      return nil
    }
    return ack
  }

  public func isAcknowledged(userDID: String, token: UInt64) -> Bool {
    guard token > 0 else { return false }
    return (currentAckToken(for: userDID) ?? 0) >= token
  }

  public func allRequests() throws -> [MLSNSEWillCloseRequest] {
    let activeDIDs: [String]
    if let obj = defaults.object(forKey: activeRequestsIndexKey) {
      guard let arr = obj as? [String] else {
        throw MLSStorageInitializationError.unreadableState(
          details: "Corrupt non-array active requests index in defaults"
        )
      }
      activeDIDs = arr
    } else {
      activeDIDs = []
    }

    var results: [MLSNSEWillCloseRequest] = []
    results.reserveCapacity(activeDIDs.count)

    for userDID in activeDIDs {
      let key = requestKey(for: userDID)
      guard let obj = defaults.object(forKey: key) else {
        continue
      }
      guard let data = obj as? Data else {
        logger.critical("🚨 [Handshake] Non-Data handshake request for key: \(key)")
        throw MLSStorageInitializationError.unreadableState(
          details: "Corrupt non-Data handshake request for key: \(key)"
        )
      }
      guard let record = decode(MLSNSEWillCloseRequest.self, from: data) else {
        logger.critical("🚨 [Handshake] Failed to decode handshake record for key: \(key)")
        throw MLSStorageInitializationError.unreadableState(
          details: "Corrupt undecodable handshake request for key: \(key)"
        )
      }
      let normReqDID = record.userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
      let normExpectedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
      guard record.token > 0 && normReqDID == normExpectedDID else {
        throw MLSStorageInitializationError.unreadableState(
          details: "Corrupt/mismatched handshake request record for \(userDID) (token=\(record.token), reqDID=\(record.userDID))"
        )
      }
      results.append(record)
    }
    return results
  }

  public func waitForAcknowledgment(
    userDID: String,
    token: UInt64,
    timeout: Duration
  ) async -> Bool {
    guard token > 0 else { return false }
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

  func clearAll(for userDID: String) throws {
    try withHandshakeMutationLock {
      defaults.removeObject(forKey: counterKey(for: userDID))
      defaults.removeObject(forKey: requestKey(for: userDID))
      defaults.removeObject(forKey: ackKey(for: userDID))
      if let obj = defaults.object(forKey: activeRequestsIndexKey) {
        guard var activeDIDs = obj as? [String] else {
          throw MLSStorageInitializationError.unreadableState(
            details: "Corrupt non-array active requests index in defaults"
          )
        }
        let normalizedTarget = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        activeDIDs.removeAll { $0.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() == normalizedTarget }
        if activeDIDs.isEmpty {
          defaults.removeObject(forKey: activeRequestsIndexKey)
        } else {
          defaults.set(activeDIDs, forKey: activeRequestsIndexKey)
        }
      }
      defaults.synchronize()
    }
  }
  // MARK: - Key Helpers

  private var activeRequestsIndexKey: String {
    "mls_handshake_active_requests.\(MLSStoragePaths.cleanSuffix)"
  }

  private func withHandshakeMutationLock<T>(_ block: () throws -> T) throws -> T {
    let coordinationDir = try MLSStoragePaths.coordinationDirectory()
    let lockURL = coordinationDir.appendingPathComponent("handshake_index.\(MLSStoragePaths.cleanSuffix).lease")
    try FileManager.default.createDirectory(at: coordinationDir, withIntermediateDirectories: true)

    let fd = open(lockURL.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open handshake lock file: \(errno)")
    }
    defer { close(fd) }

    let startTime = ContinuousClock.now
    let deadline = startTime.advanced(by: .seconds(15))
    while true {
      if flock(fd, LOCK_EX | LOCK_NB) == 0 {
        defer { flock(fd, LOCK_UN) }
        return try block()
      }
      let err = errno
      if err != EWOULDBLOCK && err != EAGAIN && err != EINTR {
        throw MLSStorageInitializationError.admissionDenied(details: "flock handshake mutation failed with errno: \(err)")
      }
      if ContinuousClock.now >= deadline {
        throw MLSStorageInitializationError.admissionDenied(details: "Timed out waiting for handshake mutation lock")
      }
      usleep(25000)
    }
  }

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
