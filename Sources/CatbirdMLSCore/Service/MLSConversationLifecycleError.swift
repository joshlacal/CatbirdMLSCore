import CatbirdMLS
import Foundation

/// Host-facing outcomes for conversation lifecycle operations. The bridge keeps
/// its existing ABI; stable pending prefixes identify nonterminal Rust outcomes.
public enum MLSConversationLifecycleError: Error, LocalizedError {
  case rateLimited(retryAfter: TimeInterval?)
  case leavePending
  case deviceAccessPending
  case memberRemovalPending
  case deviceAuthorizationUnavailable
  case unavailable(message: String, underlyingError: Error)

  public var errorDescription: String? {
    switch self {
    case .rateLimited(let retryAfter):
      if let seconds = Self.validRetryAfter(retryAfter) {
        return "Secure chat is temporarily rate limited. Try again in \(Int(ceil(seconds))) seconds."
      }
      return "Secure chat is temporarily rate limited. Please wait a little before trying again."
    case .leavePending:
      return "Your leave request is waiting for another member to finish removing you. This conversation and its messages will stay on this device until that completes."
    case .deviceAccessPending:
      return "This conversation already exists, and this device is waiting for secure access. Open Catbird on a device that can already use the conversation, then try again."
    case .memberRemovalPending:
      return "The removal request is still waiting for confirmation. The member list will update when the removal finishes."
    case .deviceAuthorizationUnavailable:
      return "Catbird cannot verify this device's authorization right now. Check your connection and try again. Your messages and keys have been kept."
    case .unavailable(let message, _):
      return message
    }
  }

  /// Validated server cooldown, when one is available. No hint means callers
  /// should present the rate limit without scheduling an immediate retry.
  public var retryAfter: TimeInterval? {
    guard case .rateLimited(let seconds) = self else { return nil }
    return Self.validRetryAfter(seconds)
  }

  private static func validRetryAfter(_ seconds: TimeInterval?) -> TimeInterval? {
    // Match MLSInventoryRequestBackoff's accepted server hint horizon.
    guard let seconds, seconds.isFinite, seconds >= 1, seconds <= 901 else { return nil }
    return seconds
  }

  private static func bridgeRetryAfter(_ body: String) -> TimeInterval? {
    struct Payload: Decodable { let retryAfter: Double }
    if let payload = try? JSONDecoder().decode(Payload.self, from: Data(body.utf8)) {
      return validRetryAfter(payload.retryAfter)
    }
    // This exact sentence is emitted locally by MLSAPIError.errorDescription
    // and preserved by the native bridge. Never display arbitrary server text.
    let prefix = "Rate limited. Retry after "
    let suffix = " seconds."
    guard body.hasPrefix(prefix), body.hasSuffix(suffix) else { return nil }
    let digits = body.dropFirst(prefix.count).dropLast(suffix.count)
    guard !digits.isEmpty, digits.allSatisfy({ $0 >= "0" && $0 <= "9" }),
          let seconds = TimeInterval(digits) else { return nil }
    return validRetryAfter(seconds)
  }

  internal static func isPendingDeviceAccess(_ result: MLSConversationReadyResult) -> Bool {
    !result.sendAllowed && (result.recoveryState == .needsRejoin || result.recoveryState == .groupMissing)
  }

  internal enum Operation: Equatable {
    case open
    case leave
    case removeMembers
  }

  internal static func presentingDeviceAuthorization(_ error: Error) -> Error {
    guard error is OrchestratorBridgeError else { return error }
    let detail = String(describing: error)
    if detail.contains("device_authorization_unavailable: ")
      || detail.contains("DID/device credential binding rejected")
      || detail.contains("authorized device key") || detail.contains("authorized-device-key")
      || detail.contains("device-key resolution") {
      return Self.deviceAuthorizationUnavailable
    }
    return error
  }

  internal static func presenting(_ error: Error, operation: Operation) -> Error {
    if case MLSAPIError.rateLimited(let seconds) = error {
      return Self.rateLimited(retryAfter: validRetryAfter(seconds))
    }
    if case MLSError.rateLimited(let seconds) = error {
      return Self.rateLimited(retryAfter: validRetryAfter(TimeInterval(seconds)))
    }
    if let mapped = presentingDeviceAuthorization(error) as? Self { return mapped }
    guard let bridgeError = error as? OrchestratorBridgeError else {
      if let localized = error as? LocalizedError, localized.errorDescription != nil { return error }
      let message: String
      switch operation {
      case .open: message = "Could not open the secure conversation. Please try again."
      case .leave: message = "Your departure could not be confirmed. Your conversation and messages have been kept. Please try again."
      case .removeMembers: message = "The member's removal could not be confirmed. Please try again."
      }
      return Self.unavailable(message: message, underlyingError: error)
    }
    let message: String
    switch bridgeError {
    case .ServerError(let status, let body) where status == 429:
      return Self.rateLimited(retryAfter: bridgeRetryAfter(body))
    case .ServerError(let status, let body) where status == 400
      && operation == .leave && canonicalErrorCode(body) == "AccessOutsideMembershipInterval":
      message = "This device no longer has access to complete the request. Open the conversation on another device to leave it. Your saved messages are still here."
    case .InvalidInput(let detail) where detail.hasPrefix("conversation_member_removal_pending: "):
      return Self.memberRemovalPending
    case .InvalidInput(let detail) where detail.hasPrefix("conversation_leave_pending: "):
      return Self.leavePending
    case .InvalidInput(let detail) where detail.hasPrefix("conversation_device_access_pending: ")
      || detail == "a conversation with that person exists but this device has not been added to it":
      return Self.deviceAccessPending
    case .InvalidInput(let detail) where detail.contains("DID/device credential binding rejected")
      || detail.contains("authorized device key") || detail.contains("authorized-device-key"):
      return Self.deviceAuthorizationUnavailable
    case .NotAuthenticated, .Credential:
      message = "Sign in again to continue using secure conversations."
    case .DeviceLimitReached:
      message = "Your account has reached its device limit. Remove an unused device before adding this one."
    case .ShuttingDown:
      message = "Secure chat is restarting. Please try again in a moment."
    case .ConversationQuarantined:
      message = "This conversation needs recovery before it can be used. Open the conversation to review its recovery options."
    case .ConversationNotFound:
      message = "This conversation is not available on this device. Refresh your conversations and try again."
    default:
      switch operation {
      case .open:
        message = "Could not open the secure conversation. Check your connection and try again."
      case .removeMembers:
        message = "The member’s removal could not be confirmed. Refresh the conversation and try again."
      case .leave:
        message = "Your departure could not be confirmed. Your conversation and messages have been kept. Please try again."
      }
    }
    return Self.unavailable(message: message, underlyingError: error)
  }

  private static func canonicalErrorCode(_ body: String) -> String? {
    guard let object = try? JSONSerialization.jsonObject(with: Data(body.utf8)) as? [String: Any] else { return nil }
    return object["error"] as? String
  }
}
