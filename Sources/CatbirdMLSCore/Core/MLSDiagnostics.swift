//
//  MLSDiagnostics.swift
//  CatbirdMLSCore
//

import CatbirdMLS
import Foundation
import OSLog

public enum MLSDiagnosticEvent: String, Codable, Sendable, CaseIterable {
  case sendFailed
  case sendRecovered
  case streamPaused
  case streamResumed
  case rejoinWaiting
  case conversationLoadFailed
  case decryptRefused
}

public struct MLSDiagnosticRecord: Sendable, Codable, Equatable {
  public let event: MLSDiagnosticEvent
  public let code: String
  public let conversationIDPrefix: String?
  public let epoch: UInt64?
  public let generation: UInt64?
  public let stateVersion: UInt64?
  public let retryAfter: TimeInterval?
  public let occurredAt: Date
  public let attempt: Int?
  public let detail: [String: String]

  public init(
    event: MLSDiagnosticEvent,
    code: String,
    conversationIDPrefix: String? = nil,
    epoch: UInt64? = nil,
    generation: UInt64? = nil,
    stateVersion: UInt64? = nil,
    retryAfter: TimeInterval? = nil,
    occurredAt: Date = Date(),
    attempt: Int? = nil,
    detail: [String: String] = [:]
  ) {
    self.event = event
    self.code = code
    self.conversationIDPrefix = conversationIDPrefix
    self.epoch = epoch
    self.generation = generation
    self.stateVersion = stateVersion
    self.retryAfter = retryAfter
    self.occurredAt = occurredAt
    self.attempt = attempt
    self.detail = detail
  }
}

public enum MLSDiagnostics {
  public typealias Reporter = @Sendable (MLSDiagnosticRecord) -> Void

  private static let reporterLock = NSLock()
  private static nonisolated(unsafe) var _reporter: Reporter?

  public static var reporter: Reporter? {
    get {
      reporterLock.lock()
      defer { reporterLock.unlock() }
      return _reporter
    }
    set {
      reporterLock.lock()
      defer { reporterLock.unlock() }
      _reporter = newValue
    }
  }

  public static func record(
    _ event: MLSDiagnosticEvent,
    code: String,
    conversation: String?,
    detail: [String: String]
  ) {
    let convoPrefix = conversation.map { String($0.prefix(16)) }
    let epoch = detail["epoch"].flatMap { UInt64($0) }
    let generation = detail["generation"].flatMap { UInt64($0) }
    let stateVersion = detail["stateVersion"].flatMap { UInt64($0) }
    let retryAfter = detail["retryAfter"].flatMap { TimeInterval($0) }
      ?? detail["retryAfterSeconds"].flatMap { TimeInterval($0) }
    let attempt = detail["attempt"].flatMap { Int($0) }

    let record = MLSDiagnosticRecord(
      event: event,
      code: sanitizeCode(code),
      conversationIDPrefix: convoPrefix,
      epoch: epoch,
      generation: generation,
      stateVersion: stateVersion,
      retryAfter: retryAfter,
      occurredAt: Date(),
      attempt: attempt,
      detail: sanitize(detail: detail)
    )
    self.record(record)
  }

  public static func record(
    _ event: MLSDiagnosticEvent,
    code: String,
    conversation: String? = nil,
    epoch: UInt64? = nil,
    generation: UInt64? = nil,
    stateVersion: UInt64? = nil,
    retryAfter: TimeInterval? = nil,
    attempt: Int? = nil,
    detail: [String: String] = [:]
  ) {
    let convoPrefix = conversation.map { String($0.prefix(16)) }
    let record = MLSDiagnosticRecord(
      event: event,
      code: sanitizeCode(code),
      conversationIDPrefix: convoPrefix,
      epoch: epoch,
      generation: generation,
      stateVersion: stateVersion,
      retryAfter: retryAfter,
      occurredAt: Date(),
      attempt: attempt,
      detail: sanitize(detail: detail)
    )
    self.record(record)
  }

  /// Record straight from an error so the trail always names the real failure:
  /// the code identifies the class, `errorSummary` carries the redacted text
  /// for whatever the mapping did not recognise.
  public static func record(
    _ event: MLSDiagnosticEvent,
    error: Error,
    conversation: String? = nil,
    epoch: UInt64? = nil,
    generation: UInt64? = nil,
    stateVersion: UInt64? = nil,
    retryAfter: TimeInterval? = nil,
    attempt: Int? = nil,
    detail: [String: String] = [:]
  ) {
    var enriched = detail
    enriched["errorType"] = String(describing: type(of: error))
    enriched["errorSummary"] = errorSummary(from: error)
    record(
      event,
      code: errorCode(from: error),
      conversation: conversation,
      epoch: epoch,
      generation: generation,
      stateVersion: stateVersion,
      retryAfter: retryAfter ?? extractRetryAfter(from: error),
      attempt: attempt,
      detail: enriched
    )
  }

  private struct RepeatKey: Hashable {
    let event: MLSDiagnosticEvent
    let code: String
    let conversation: String?
    /// Distinct attempts are distinct facts: a retry ladder must stay legible
    /// while an unnumbered failure loop collapses.
    let attempt: Int?
  }

  private static let repeatLock = NSLock()
  private static nonisolated(unsafe) var lastRecorded: (key: RepeatKey, at: Date)?

  /// A failing loop must not bury the trail. Identical failures for the same
  /// conversation inside this window are dropped rather than appended, so a
  /// storm leaves one entry instead of flooding the buffer and Sentry.
  private static let repeatSuppressionWindow: TimeInterval = 30

  public static func record(_ record: MLSDiagnosticRecord) {
    let key = RepeatKey(
      event: record.event,
      code: record.code,
      conversation: record.conversationIDPrefix,
      attempt: record.attempt
    )
    let isRepeat: Bool = repeatLock.withLock {
      if let last = lastRecorded, last.key == key,
         record.occurredAt.timeIntervalSince(last.at) < repeatSuppressionWindow {
        return true
      }
      lastRecorded = (key: key, at: record.occurredAt)
      return false
    }
    if isRepeat { return }

    // 1. Persist to App Group ring buffer
    MLSSuspensionFlightRecorder.shared.recordDiagnosticRecord(record)

    // 2. Dispatch to registered reporter sink (e.g. Sentry bridge)
    let currentReporter: Reporter? = {
      reporterLock.lock()
      defer { reporterLock.unlock() }
      return _reporter
    }()
    currentReporter?(record)
  }

  public static func recent(limit: Int = 50) -> [MLSDiagnosticRecord] {
    MLSSuspensionFlightRecorder.shared.getDiagnosticRecords(limit: limit)
  }

  public static func clear() {
    MLSSuspensionFlightRecorder.shared.clearDiagnosticRecords()
  }

  public static func exportText() -> String {
    let records = recent(limit: 200)
    if records.isEmpty {
      return "No MLS diagnostic records."
    }
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime]

    var lines: [String] = []
    lines.append("=== MLS Diagnostics Trail (\(records.count) records) ===")

    for record in records {
      var parts: [String] = []
      parts.append(formatter.string(from: record.occurredAt))
      parts.append("[\(record.event.rawValue)]")
      parts.append("code=\(sanitizeCode(record.code))")
      if let convo = record.conversationIDPrefix, !convo.isEmpty {
        parts.append("convo=\(String(convo.prefix(16)))")
      }
      if let epoch = record.epoch {
        parts.append("epoch=\(epoch)")
      }
      if let gen = record.generation {
        parts.append("gen=\(gen)")
      }
      if let sv = record.stateVersion {
        parts.append("stateVersion=\(sv)")
      }
      if let attempt = record.attempt {
        parts.append("attempt=\(attempt)")
      }
      if let retryAfter = record.retryAfter {
        parts.append(String(format: "retryAfter=%.1fs", retryAfter))
      }
      lines.append(parts.joined(separator: " "))
    }
    return lines.joined(separator: "\n")
  }

  // MARK: - Sanitization & Privacy

  private static func sanitizeCode(_ code: String) -> String {
    if code.hasPrefix("did:") {
      return String(code.prefix(16))
    }
    if code.hasPrefix("eyJ") || code.count > 64 {
      return "[REDACTED_CODE]"
    }
    return code
  }

  private static func sanitize(detail: [String: String]) -> [String: String] {
    var sanitized: [String: String] = [:]
    for (key, value) in detail {
      let lowerKey = key.lowercased()
      // Exclude plaintext, token, password, secret, ciphertext, bearer
      if lowerKey.contains("plaintext") || lowerKey.contains("text") || lowerKey.contains("message")
        || lowerKey.contains("cipher") || lowerKey.contains("token") || lowerKey.contains("bearer")
        || lowerKey.contains("secret") || lowerKey.contains("password") || lowerKey.contains("auth")
      {
        continue
      }
      if lowerKey.contains("did") || value.hasPrefix("did:") {
        sanitized[key] = String(value.prefix(16))
      } else if value.hasPrefix("eyJ") || value.count > 128 {
        sanitized[key] = "[REDACTED]"
      } else {
        sanitized[key] = value
      }
    }
    return sanitized
  }

  // MARK: - Error Mapping & Coordinate Extraction

  public static func errorCode(from error: Error) -> String {
    if let bridgeError = error as? OrchestratorBridgeError {
      return errorCode(fromBridgeError: bridgeError)
    }
    if let lifecycleError = error as? MLSConversationLifecycleError {
      switch lifecycleError {
      case .rateLimited:
        return "RateLimited"
      case .deviceAccessPending:
        return "RecipientNotReady"
      case .leavePending:
        return "LeavePending"
      case .memberRemovalPending:
        return "MemberRemovalPending"
      case .deviceAuthorizationUnavailable:
        return "DeviceAuthorizationUnavailable"
      case .unavailable(_, let underlying):
        return errorCode(from: underlying)
      }
    }
    if let apiError = error as? MLSAPIError {
      switch apiError {
      case .rateLimited:
        return "RateLimited"
      case .conversationNotFound:
        return "ConversationNotFound"
      case .noAuthentication:
        return "NotAuthenticated"
      case .httpError(let status, let message):
        if status == 429 { return "RateLimited" }
        if status == 502 { return "HTTP_502" }
        if let canonical = canonicalErrorCode(message) { return canonical }
        return "HTTP_\(status)"
      default:
        return "MLSAPIError"
      }
    }
    if let mlsError = error as? MLSError {
      switch mlsError {
      case .rateLimited:
        return "RateLimited"
      case .secretReuseSkipped:
        return "SecretReuseSkipped"
      default:
        let desc = String(describing: mlsError)
        if desc.contains("SecretReuse") { return "SecretReuse" }
        return "MLSError"
      }
    }
    let desc = error.localizedDescription
    if let canonical = canonicalErrorCode(desc) {
      return canonical
    }
    if desc.contains("StaleCoordinates") { return "StaleCoordinates" }
    if desc.contains("RateLimited") || desc.contains("rate limited") || desc.contains("429") {
      return "RateLimited"
    }
    if desc.contains("RecipientNotReady") || desc.contains("device_access_pending") {
      return "RecipientNotReady"
    }
    if desc.contains("ConversationAlreadyExists") { return "ConversationAlreadyExists" }
    if desc.contains("RecoveryFailed") { return "RecoveryFailed" }
    if desc.contains("SecretReuse") { return "SecretReuse" }
    if desc.contains("generation mismatch") || desc.contains("Generation mismatch") {
      return "GenerationMismatch"
    }
    if desc.contains("502") { return "HTTP_502" }
    // Never report an anonymous failure: a code of "UnknownError" tells nobody
    // anything. Fall back to the concrete error type, which stays stable enough
    // to fingerprint, and let `errorSummary` carry the redacted detail.
    return "Unmapped_\(String(describing: type(of: error)))"
  }

  /// Redacted one-line description for an error whose code came out unmapped,
  /// so a diagnostics trail names the real failure instead of a placeholder.
  public static func errorSummary(from error: Error) -> String {
    let raw = String(describing: error)
      .replacingOccurrences(of: "\n", with: " ")
      .trimmingCharacters(in: .whitespacesAndNewlines)
    let collapsed = raw.split(separator: " ", omittingEmptySubsequences: true).joined(separator: " ")
    return sanitizeSummary(collapsed)
  }

  private static func sanitizeSummary(_ summary: String) -> String {
    var redacted = summary
    if let didRange = redacted.range(of: "did:plc:") {
      let tail = redacted[didRange.upperBound...].prefix(8)
      redacted = redacted.replacingOccurrences(
        of: "did:plc:" + tail, with: "did:plc:" + tail + "…"
      )
    }
    let words = redacted.split(separator: " ").map { word -> String in
      // Anything long and opaque is a token, capability or ciphertext blob.
      word.count > 44 || word.hasPrefix("eyJ") ? "[REDACTED]" : String(word)
    }
    return String(words.joined(separator: " ").prefix(180))
  }

  private static func errorCode(fromBridgeError bridgeError: OrchestratorBridgeError) -> String {
    switch bridgeError {
    case .ServerError(let status, let body):
      if let canonical = canonicalErrorCode(body) {
        return canonical
      }
      if status == 429 { return "RateLimited" }
      if status == 502 { return "HTTP_502" }
      if body.contains("StaleCoordinates") { return "StaleCoordinates" }
      if body.contains("RateLimited") || body.contains("Rate limit") { return "RateLimited" }
      if body.contains("RecipientNotReady") { return "RecipientNotReady" }
      if body.contains("ConversationAlreadyExists") { return "ConversationAlreadyExists" }
      return "HTTP_\(status)"

    case .RecoveryFailed:
      return "RecoveryFailed"

    case .ConversationNotFound:
      return "ConversationNotFound"

    case .EpochMismatch:
      return "EpochMismatch"

    case .DeviceLimitReached:
      return "DeviceLimitReached"

    case .NotAuthenticated:
      return "NotAuthenticated"

    case .ShuttingDown:
      return "ShuttingDown"

    case .ConversationQuarantined:
      return "ConversationQuarantined"

    case .MissingSecurityCapability:
      return "MissingSecurityCapability"

    case .Storage:
      return "StorageError"

    case .Api(let message):
      if message.contains("generation mismatch")
        || message.contains("Coordination generation mismatch")
      {
        return "GenerationMismatch"
      }
      if message.contains("502") {
        return "HTTP_502"
      }
      return "ApiError"

    case .Mls(let message):
      if message.contains("SecretReuse") { return "SecretReuse" }
      return "MlsError"
    case .InvalidInput(let message):
      if message.contains("conversation_device_access_pending")
        || message.contains("not been added to it")
      {
        return "RecipientNotReady"
      }
      if message.contains("conversation_member_removal_pending") {
        return "MemberRemovalPending"
      }
      if message.contains("conversation_leave_pending") {
        return "LeavePending"
      }
      if message.contains("DID/device credential binding rejected")
        || message.contains("authorized device key")
      {
        return "DeviceAuthorizationUnavailable"
      }
      return "InvalidInput"

    case .Credential:
      return "CredentialError"

    case .Voice:
      return "VoiceError"
    }
  }

  public static func extractCoordinates(from error: Error) -> (
    epoch: UInt64?, generation: UInt64?, stateVersion: UInt64?
  ) {
    if let bridgeError = error as? OrchestratorBridgeError {
      switch bridgeError {
      case .EpochMismatch(_, let remote):
        return (epoch: remote, generation: nil, stateVersion: nil)
      case .ServerError(_, let body):
        return parseCoordinates(from: body)
      default:
        break
      }
    }
    return parseCoordinates(from: error.localizedDescription)
  }

  public static func extractRetryAfter(from error: Error) -> TimeInterval? {
    if let lifecycleError = error as? MLSConversationLifecycleError {
      return lifecycleError.retryAfter
    }
    if case MLSAPIError.rateLimited(let seconds) = error {
      return seconds
    }
    if case MLSError.rateLimited(let seconds) = error {
      return TimeInterval(seconds)
    }
    if let bridgeError = error as? OrchestratorBridgeError,
      case .ServerError(let status, let body) = bridgeError, status == 429
    {
      return parseRetryAfter(from: body)
    }
    return parseRetryAfter(from: error.localizedDescription)
  }

  public static func extractDetail(from error: Error) -> [String: String] {
    var detail: [String: String] = [:]
    if let bridgeError = error as? OrchestratorBridgeError {
      switch bridgeError {
      case .RecoveryFailed(let message):
        if message.contains("SecretReuse without durable envelope evidence") {
          detail["subcode"] = "SecretReuseWithoutDurableEnvelope"
        }
      case .ServerError(let status, _):
        detail["status"] = String(status)
      case .EpochMismatch(let local, let remote):
        detail["localEpoch"] = String(local)
        detail["remoteEpoch"] = String(remote)
      case .ConversationQuarantined(_, let reason):
        detail["quarantineReason"] = String(reason.prefix(64))
      default:
        break
      }
    }
    return detail
  }

  private static func canonicalErrorCode(_ body: String) -> String? {
    guard let data = body.data(using: .utf8),
      let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
    else {
      return nil
    }
    return object["error"] as? String
  }

  private static func parseCoordinates(from text: String) -> (
    epoch: UInt64?, generation: UInt64?, stateVersion: UInt64?
  ) {
    var epoch: UInt64?
    var generation: UInt64?
    var stateVersion: UInt64?

    if let data = text.data(using: .utf8),
      let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
    {
      if let ep = object["epoch"] as? UInt64 ?? (object["epoch"] as? Int).flatMap({ UInt64($0) })
        ?? (object["serverEpoch"] as? UInt64)
      {
        epoch = ep
      }
      if let gen = object["generation"] as? UInt64
        ?? (object["generation"] as? Int).flatMap({ UInt64($0) })
      {
        generation = gen
      }
      if let sv = object["stateVersion"] as? UInt64
        ?? (object["stateVersion"] as? Int).flatMap({ UInt64($0) })
      {
        stateVersion = sv
      }
    }
    if stateVersion == nil && text.contains("stateVersion 0") {
      stateVersion = 0
    }
    return (epoch, generation, stateVersion)
  }

  private static func parseRetryAfter(from text: String) -> TimeInterval? {
    if let data = text.data(using: .utf8),
      let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
      let seconds = object["retryAfter"] as? Double
    {
      return seconds
    }
    let prefix = "Retry after "
    if let range = text.range(of: prefix) {
      let remainder = text[range.upperBound...]
      let digits = remainder.prefix(while: { $0.isNumber || $0 == "." })
      if let sec = TimeInterval(digits), sec > 0 {
        return sec
      }
    }
    return nil
  }
}
