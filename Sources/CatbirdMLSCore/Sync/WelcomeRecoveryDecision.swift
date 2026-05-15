import Foundation

enum WelcomeRecoveryFailure: Equatable, Sendable {
  case none
  case noMatchingKeyPackage
  case welcomeUnavailable
  case welcomeExpired
  case transient(String)
  case irrecoverable(String)

  var reason: String {
    switch self {
    case .none:
      return "none"
    case .noMatchingKeyPackage:
      return "NoMatchingKeyPackage"
    case .welcomeUnavailable:
      return "Welcome unavailable"
    case .welcomeExpired:
      return "Welcome expired"
    case .transient(let reason), .irrecoverable(let reason):
      return reason
    }
  }
}

enum WelcomeRecoveryDecision: Equatable, Sendable {
  case accept
  case requestReissue(reason: String, nextAttempt: Int)
  case externalCommitWithHistoryGap(lastSeenEpoch: UInt64)
  case surrender(reason: String, retryAfter: TimeInterval?)
}

enum WelcomeRecoveryPolicy {
  static let maxReissueAttempts = 3

  static func decide(
    welcomeFailure: WelcomeRecoveryFailure,
    priorReissueAttempts: Int,
    hasCurrentAdmin: Bool,
    groupInfoUnrecoverable: Bool,
    externalCommitAlreadyFailed: Bool,
    lastSeenEpoch: UInt64,
    retryAfter: TimeInterval? = nil
  ) -> WelcomeRecoveryDecision {
    if case .none = welcomeFailure {
      return .accept
    }

    if groupInfoUnrecoverable {
      return .surrender(reason: "GroupInfo unrecoverable", retryAfter: retryAfter)
    }

    if externalCommitAlreadyFailed {
      return .surrender(reason: "External Commit recovery failed", retryAfter: retryAfter)
    }

    if case .irrecoverable(let reason) = welcomeFailure {
      return .surrender(reason: reason, retryAfter: retryAfter)
    }

    if case .noMatchingKeyPackage = welcomeFailure,
      hasCurrentAdmin,
      priorReissueAttempts < maxReissueAttempts
    {
      return .requestReissue(
        reason: welcomeFailure.reason,
        nextAttempt: priorReissueAttempts + 1
      )
    }

    return .externalCommitWithHistoryGap(lastSeenEpoch: lastSeenEpoch)
  }
}
