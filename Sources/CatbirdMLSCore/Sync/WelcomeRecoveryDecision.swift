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
        case let .transient(reason), let .irrecoverable(reason):
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
        serverListsCurrentUserAsMember: Bool = false,
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

        if case let .irrecoverable(reason) = welcomeFailure {
            return .surrender(reason: reason, retryAfter: retryAfter)
        }

        let canRequestReissue: Bool
        switch welcomeFailure {
        case .noMatchingKeyPackage, .welcomeUnavailable, .welcomeExpired:
            canRequestReissue = true
        case .none, .transient, .irrecoverable:
            canRequestReissue = false
        }

        if canRequestReissue {
            if priorReissueAttempts < maxReissueAttempts,
               serverListsCurrentUserAsMember || hasCurrentAdmin
            {
                return .requestReissue(
                    reason: welcomeFailure.reason,
                    nextAttempt: priorReissueAttempts + 1
                )
            }

            if serverListsCurrentUserAsMember {
                return .surrender(
                    reason: "Welcome reissue attempts exhausted",
                    retryAfter: retryAfter
                )
            }
        }

        return .externalCommitWithHistoryGap(lastSeenEpoch: lastSeenEpoch)
    }
}
