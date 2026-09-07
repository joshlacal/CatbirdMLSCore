//
//  MLSSendFailureClassification.swift
//  CatbirdMLSCore
//

import Foundation
import CatbirdMLS
import OSLog

private let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSSendFailureClassification")

/// Structured classification of MLS send and pipeline failures.
public enum MLSSendFailureClassification: Equatable, Sendable {
  /// The local coordinates (epoch, generation, stateVersion) are stale or mismatched.
  /// Recoverable automatically by re-deriving fresh coordinates from authoritative state.
  case staleCoordinates(epoch: UInt64?, generation: UInt64?, stateVersion: UInt64?)

  /// Gateway or service rate limiting with optional retry-after cooldown.
  /// Recoverable automatically by waiting for the cooldown.
  case rateLimited(retryAfter: TimeInterval?)

  /// Recipient or direct peer has no active MLS leaf (acceptance, recovery, or device gap).
  /// Waiting state: peer must act. Does NOT spin automatically.
  case recipientNotReady(detail: String?)

  /// Conversation requires recovery or access from another device (device access pending,
  /// leaf recovery waiting, conversation already exists, secret reuse without durable envelope).
  /// Waiting state: peer device must act. Does NOT spin automatically.
  case peerActionRequired(reason: String)

  /// Transient network error, upstream gateway timeout (e.g. 502), or coordination generation mismatch.
  /// Recoverable automatically with bounded backoff.
  case transientNetwork(status: Int?, message: String)

  /// Genuinely terminal failure (not a member, account switched, invalid group, permanent 4xx, retries exhausted).
  /// Non-recoverable automatically.
  case terminal(code: String, reason: String)
}

public extension MLSSendFailureClassification {
  /// True if this failure class can be recovered automatically without user interaction.
  var isAutoRecoverable: Bool {
    switch self {
    case .staleCoordinates, .rateLimited, .transientNetwork:
      return true
    case .recipientNotReady, .peerActionRequired, .terminal:
      return false
    }
  }

  /// True if this failure requires action by another device or participant before messaging can proceed.
  var isWaitingForPeer: Bool {
    switch self {
    case .recipientNotReady, .peerActionRequired:
      return true
    case .staleCoordinates, .rateLimited, .transientNetwork, .terminal:
      return false
    }
  }

  /// Diagnostic code for telemetry and logging.
  var diagnosticCode: String {
    switch self {
    case .staleCoordinates:
      return "StaleCoordinates"
    case .rateLimited:
      return "RateLimited"
    case .recipientNotReady:
      return "RecipientNotReady"
    case .peerActionRequired(let reason):
      if reason.contains("SecretReuse") { return "SecretReuse" }
      if reason.contains("access") || reason.contains("Access") { return "DeviceAccessPending" }
      if reason.contains("rejoin") || reason.contains("Rejoin") { return "RejoinWaiting" }
      return "PeerActionRequired"
    case .transientNetwork(let status, _):
      if let status { return "HTTP_\(status)" }
      return "TransientNetwork"
    case .terminal(let code, _):
      return code
    }
  }

  /// Human-readable explanation of the current state for UI indicators and banners.
  var userVisibleReason: String {
    switch self {
    case .staleCoordinates:
      return "Updating conversation security keys"
    case .rateLimited(let retryAfter):
      if let seconds = retryAfter, seconds.isFinite && seconds > 0 {
        return "Sending temporarily paused. Retrying in \(Int(ceil(seconds)))s"
      }
      return "Sending temporarily paused"
    case .recipientNotReady:
      return "Waiting for recipient to accept or activate their device"
    case .peerActionRequired(let reason):
      return reason
    case .transientNetwork(let status, let message):
      if let status {
        return "Server temporarily unavailable (\(status))"
      }
      return message
    case .terminal(_, let reason):
      return reason
    }
  }

  /// Headline suitable for full-screen or card error presentations (e.g. pipelineError overlay).
  var presentationHeadline: String {
    switch self {
    case .staleCoordinates:
      return "Security State Desync"
    case .rateLimited:
      return "Rate Limited"
    case .recipientNotReady:
      return "Recipient Not Ready"
    case .peerActionRequired:
      return "Waiting for Device Access"
    case .transientNetwork(let status, _):
      if let status {
        return "Connection Issue (\(status))"
      }
      return "Connection Issue"
    case .terminal:
      return "Couldn't Load Messages"
    }
  }

  /// Detail body suitable for full-screen or card error presentations.
  var presentationDetail: String {
    switch self {
    case .staleCoordinates:
      return "The conversation security keys need to be refreshed. Tap Retry to sync with the server."
    case .rateLimited(let retryAfter):
      if let seconds = retryAfter, seconds.isFinite && seconds > 0 {
        return "Secure chat is temporarily rate limited. Try again in \(Int(ceil(seconds))) seconds."
      }
      return "Secure chat is temporarily rate limited. Please wait a moment before trying again."
    case .recipientNotReady:
      return "The recipient has not accepted the conversation or has no active devices ready for secure chat."
    case .peerActionRequired(let reason):
      return reason
    case .transientNetwork(let status, let message):
      if let status {
        return "Could not reach the secure chat service (\(status)). Check your connection and try again."
      }
      return "\(message) Check your connection and try again."
    case .terminal(_, let reason):
      return reason
    }
  }

  /// Central classification entrypoint. Maps any error into a typed send failure classification.
  static func classify(_ error: Error) -> MLSSendFailureClassification {
    // 1. Direct typed MLSConversationLifecycleError
    if let lifecycleError = error as? MLSConversationLifecycleError {
      switch lifecycleError {
      case .rateLimited(let retryAfter):
        return .rateLimited(retryAfter: retryAfter)
      case .deviceAccessPending:
        return .peerActionRequired(
          reason: "This conversation already exists, and this device is waiting for secure access. Open Catbird on an active device, then try again."
        )
      case .leavePending:
        return .terminal(
          code: "LeavePending",
          reason: "Your leave request is still waiting for confirmation."
        )
      case .memberRemovalPending:
        return .terminal(
          code: "MemberRemovalPending",
          reason: "Member removal is still waiting for confirmation."
        )
      case .deviceAuthorizationUnavailable:
        return .terminal(
          code: "DeviceAuthorizationUnavailable",
          reason: "Catbird cannot verify this device's authorization right now. Check your connection and try again."
        )
      case .unavailable(let message, let underlying):
        let sub = classify(underlying)
        if case .terminal = sub {
          return .terminal(code: "Unavailable", reason: message)
        }
        return sub
      }
    }

    // 2. OrchestratorBridgeError
    if let bridgeError = error as? OrchestratorBridgeError {
      switch bridgeError {
      case .ServerError(let status, let body):
        let intStatus = Int(status)
        if intStatus == 429 {
          return .rateLimited(retryAfter: MLSDiagnostics.extractRetryAfter(from: error))
        }
        let code = MLSDiagnostics.errorCode(from: error)
        if code == "StaleCoordinates" || body.contains("StaleCoordinates") || body.contains("stale coordinate") {
          let coords = MLSDiagnostics.extractCoordinates(from: error)
          return .staleCoordinates(epoch: coords.epoch, generation: coords.generation, stateVersion: coords.stateVersion)
        }
        if code == "RecipientNotReady" || body.contains("RecipientNotReady") {
          return .recipientNotReady(detail: body)
        }
        if code == "ConversationAlreadyExists" || body.contains("ConversationAlreadyExists") {
          return .peerActionRequired(
            reason: "This conversation already exists on another device. Open Catbird on an active device to grant access."
          )
        }
        if intStatus >= 500 && intStatus <= 599 {
          return .transientNetwork(status: intStatus, message: "Server error (\(intStatus)). Upstream service may be temporarily unavailable.")
        }
        return .terminal(code: code, reason: "Server rejected the request: \(code)")

      case .EpochMismatch(_, let remote):
        return .staleCoordinates(epoch: remote, generation: nil, stateVersion: nil)

      case .RecoveryFailed(let message):
        if message.contains("SecretReuse") || message.contains("leaf") || message.contains("recovery") || message.contains("Rejoin") {
          return .peerActionRequired(
            reason: "Secure conversation requires recovery by an existing device (\(message))."
          )
        }
        return .terminal(code: "RecoveryFailed", reason: message)

      case .InvalidInput(let detail):
        if detail.contains("StaleCoordinates") || detail.contains("stale") {
          let coords = MLSDiagnostics.extractCoordinates(from: error)
          return .staleCoordinates(epoch: coords.epoch, generation: coords.generation, stateVersion: coords.stateVersion)
        }
        if detail.contains("RecipientNotReady") || detail.contains("not ready") {
          return .recipientNotReady(detail: detail)
        }
        if detail.hasPrefix("conversation_device_access_pending")
          || detail.contains("waiting for secure access")
          || detail.contains("not been added to it") {
          return .peerActionRequired(
            reason: "This conversation already exists, and this device is waiting for secure access. Open Catbird on an active device, then try again."
          )
        }
        if detail.contains("DID/device credential binding rejected") || detail.contains("authorized device key") {
          return .terminal(code: "DeviceAuthRejected", reason: "Device authorization credential binding was rejected.")
        }
        return .terminal(code: "InvalidInput", reason: detail)

      case .NotAuthenticated, .Credential:
        return .terminal(code: "NotAuthenticated", reason: "Sign in again to continue using secure conversations.")

      case .DeviceLimitReached:
        return .terminal(code: "DeviceLimitReached", reason: "Your account has reached its device limit. Remove an unused device before adding this one.")

      case .ConversationQuarantined:
        return .peerActionRequired(reason: "This conversation is quarantined and requires recovery before it can be used.")

      case .ConversationNotFound:
        return .terminal(code: "ConversationNotFound", reason: "This conversation is not available on this device.")

      case .ShuttingDown:
        return .transientNetwork(status: nil, message: "Secure messaging service is restarting.")

      case .Api(let message):
        if message.contains("502") {
          return .transientNetwork(status: 502, message: "Upstream gateway timed out (502).")
        }
        if message.contains("generation mismatch") || message.contains("Coordination generation mismatch") {
          return .transientNetwork(status: nil, message: "Coordination generation mismatch.")
        }
        return .terminal(code: "ApiError", reason: message)

      case .Storage(let message):
        return .terminal(code: "StorageError", reason: message)

      default:
        let code = MLSDiagnostics.errorCode(from: error)
        return .terminal(code: code, reason: error.localizedDescription)
      }
    }

    // 3. MLSAPIError
    if let apiError = error as? MLSAPIError {
      switch apiError {
      case .rateLimited(let seconds):
        return .rateLimited(retryAfter: seconds)
      case .serverUnavailable:
        return .transientNetwork(status: 503, message: "MLS server is currently unavailable.")
      case .httpError(let code, let message):
        if code == 429 {
          return .rateLimited(retryAfter: MLSDiagnostics.extractRetryAfter(from: error))
        }
        if code == 409 {
          return .staleCoordinates(epoch: nil, generation: nil, stateVersion: nil)
        }
        if message.contains("StaleCoordinates") {
          let coords = MLSDiagnostics.extractCoordinates(from: error)
          return .staleCoordinates(epoch: coords.epoch, generation: coords.generation, stateVersion: coords.stateVersion)
        }
        if message.contains("RecipientNotReady") {
          return .recipientNotReady(detail: message)
        }
        if message.contains("ConversationAlreadyExists") {
          return .peerActionRequired(reason: "Conversation already exists and is awaiting access from another device.")
        }
        if code >= 500 && code <= 599 {
          return .transientNetwork(status: code, message: "Server error (\(code)). Upstream service may be temporarily unavailable.")
        }
        return .terminal(code: "HTTP_\(code)", reason: message)

      default:
        return .terminal(code: "APIError", reason: error.localizedDescription)
      }
    }

    // 4. MLSError
    if let mlsError = error as? MLSError {
      switch mlsError {
      case .rateLimited(let seconds):
        return .rateLimited(retryAfter: TimeInterval(seconds))
      case .staleStateDetected(_, let diskEpoch):
        return .staleCoordinates(epoch: diskEpoch, generation: nil, stateVersion: nil)
      case .ratchetStateDesync:
        return .peerActionRequired(reason: "Secure conversation state is out of sync and requires recovery.")
      default:
        break
      }
    }

    // 5. MLSConversationError
    if let convoError = error as? MLSConversationError {
      switch convoError {
      case .duplicateMessage:
        return .terminal(code: "DuplicateMessage", reason: "Message already sent.")
      case .groupNotInitialized:
        return .peerActionRequired(reason: "Secure conversation group is not active on this device.")
      case .noAuthentication:
        return .terminal(code: "NoAuth", reason: "Not authenticated.")
      case .invalidGroupId:
        return .terminal(code: "InvalidGroup", reason: "Invalid conversation identifier.")
      default:
        break
      }
    }

    // 6. URLError
    if let urlError = error as? URLError {
      switch urlError.code {
      case .timedOut:
        return .transientNetwork(status: nil, message: "Connection timed out.")
      case .cannotConnectToHost, .networkConnectionLost, .notConnectedToInternet, .dnsLookupFailed:
        return .transientNetwork(status: nil, message: "Network connection lost.")
      default:
        return .transientNetwork(status: nil, message: urlError.localizedDescription)
      }
    }

    // 7. General string inspection fallback
    let desc = error.localizedDescription
    if desc.contains("StaleCoordinates") || desc.contains("stale coordinate") {
      let coords = MLSDiagnostics.extractCoordinates(from: error)
      return .staleCoordinates(epoch: coords.epoch, generation: coords.generation, stateVersion: coords.stateVersion)
    }
    if desc.contains("RecipientNotReady") {
      return .recipientNotReady(detail: desc)
    }
    if desc.contains("RateLimited") || desc.contains("rate limited") || desc.contains("Rate limited") {
      let retryAfter = MLSDiagnostics.extractRetryAfter(from: error)
      return .rateLimited(retryAfter: retryAfter)
    }
    if desc.contains("ConversationAlreadyExists") || desc.contains("device_access_pending") {
      return .peerActionRequired(reason: "This conversation already exists, and this device is waiting for secure access.")
    }
    if desc.contains("SecretReuse") {
      return .peerActionRequired(reason: "Secure conversation requires recovery by an existing device (SecretReuse).")
    }
    if desc.contains("502") || desc.contains("Bad Gateway") {
      return .transientNetwork(status: 502, message: "Upstream gateway timed out (502).")
    }
    if desc.contains("generation mismatch") || desc.contains("GenerationStaleError") {
      return .transientNetwork(status: nil, message: "Coordination generation mismatch.")
    }

    return .terminal(code: MLSDiagnostics.errorCode(from: error), reason: desc)
  }
}

public extension MLSConversationLifecycleError {
  /// Classifies a send failure for retry and user-facing presentation.
  static func classifySendError(_ error: Error) -> MLSSendFailureClassification {
    MLSSendFailureClassification.classify(error)
  }

  /// Classifies a pipeline error for user-facing presentation.
  static func classifyPipelineError(_ error: Error) -> MLSSendFailureClassification {
    MLSSendFailureClassification.classify(error)
  }
}

/// Progress reported before each automatic retry attempt.
public struct MLSSendRetryProgress: Sendable, Equatable {
  public let attempt: Int
  public let maxAttempts: Int
  public let nextAttemptAt: Date
  public let delay: TimeInterval
  public let classification: MLSSendFailureClassification

  public init(
    attempt: Int,
    maxAttempts: Int,
    nextAttemptAt: Date,
    delay: TimeInterval,
    classification: MLSSendFailureClassification
  ) {
    self.attempt = attempt
    self.maxAttempts = maxAttempts
    self.nextAttemptAt = nextAttemptAt
    self.delay = delay
    self.classification = classification
  }
}

/// Manages bounded automatic retries for recoverable MLS send operations.
public enum MLSSendRetryCoordinator {
  public static let defaultMaxAttempts = 4

  /// Executes an async send operation with bounded automatic retry and status updates.
  ///
  /// - Parameters:
  ///   - convoId: Target conversation ID.
  ///   - maxAttempts: Maximum attempts (default 4 = 1 initial + 3 retries).
  ///   - onRetryProgress: Optional callback invoked before each retry with progress details.
  ///   - sleep: Optional sleep function for testing. Defaults to `Task.sleep`.
  ///   - operation: The send operation to execute. Re-derives fresh state on each attempt.
  /// - Returns: Result of the send operation.
  public static func performSendWithRetry<T: Sendable>(
    convoId: String,
    maxAttempts: Int = defaultMaxAttempts,
    onRetryProgress: (@Sendable (MLSSendRetryProgress) -> Void)? = nil,
    sleep: (@Sendable (TimeInterval) async throws -> Void)? = nil,
    operation: @Sendable () async throws -> T
  ) async throws -> T {
    var attempt = 1
    while true {
      try Task.checkCancellation()
      do {
        let result = try await operation()
        if attempt > 1 {
          MLSDiagnostics.record(
            .sendRecovered,
            code: "SendRecovered",
            conversation: convoId,
            attempt: attempt,
            detail: ["attempts": String(attempt)]
          )
        }
        return result
      } catch {
        let classification = MLSSendFailureClassification.classify(error)
        let coords = MLSDiagnostics.extractCoordinates(from: error)
        let retryAfter = MLSDiagnostics.extractRetryAfter(from: error)
        var detail = MLSDiagnostics.extractDetail(from: error)
        detail["attempt"] = String(attempt)
        detail["classification"] = classification.diagnosticCode

        MLSDiagnostics.record(
          .sendFailed,
          code: classification.diagnosticCode,
          conversation: convoId,
          epoch: coords.epoch,
          generation: coords.generation,
          stateVersion: coords.stateVersion,
          retryAfter: retryAfter,
          attempt: attempt,
          detail: detail
        )

        // Peer action / waiting states do NOT spin
        if classification.isWaitingForPeer {
          MLSDiagnostics.record(
            .rejoinWaiting,
            code: classification.diagnosticCode,
            conversation: convoId,
            epoch: coords.epoch,
            generation: coords.generation,
            stateVersion: coords.stateVersion,
            attempt: attempt,
            detail: detail
          )
          throw error
        }

        guard classification.isAutoRecoverable, attempt < maxAttempts else {
          // Terminal error or retries exhausted
          throw error
        }

        let delay: TimeInterval
        switch classification {
        case .rateLimited(let serverHint):
          delay = serverHint ?? min(pow(2.0, Double(attempt)), 15.0)
        case .staleCoordinates:
          delay = min(0.5 * Double(attempt), 5.0)
        case .transientNetwork:
          delay = min(pow(2.0, Double(attempt - 1)) * 1.0, 15.0)
        default:
          delay = 1.0
        }

        let nextAttemptAt = Date().addingTimeInterval(delay)
        onRetryProgress?(
          MLSSendRetryProgress(
            attempt: attempt,
            maxAttempts: maxAttempts,
            nextAttemptAt: nextAttemptAt,
            delay: delay,
            classification: classification
          )
        )

        if let sleep {
          try await sleep(delay)
        } else {
          try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
        }

        attempt += 1
      }
    }
  }
}
