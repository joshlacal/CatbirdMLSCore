@testable import CatbirdMLSCore
import XCTest

final class WelcomeRecoveryDecisionTests: XCTestCase {
    func testNoMatchingKeyPackageRequestsReissueBeforeLimit() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .noMatchingKeyPackage,
            priorReissueAttempts: 2,
            hasCurrentAdmin: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 7
        )

        XCTAssertEqual(
            decision,
            .requestReissue(reason: "NoMatchingKeyPackage", nextAttempt: 3)
        )
    }

    func testMissingWelcomeRequestsReissueBeforeExternalCommitWhenAdminExists() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .welcomeUnavailable,
            priorReissueAttempts: 0,
            hasCurrentAdmin: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 1
        )

        XCTAssertEqual(
            decision,
            .requestReissue(reason: "Welcome unavailable", nextAttempt: 1)
        )
    }

    func testExpiredWelcomeRequestsReissueBeforeExternalCommitWhenAdminExists() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .welcomeExpired,
            priorReissueAttempts: 1,
            hasCurrentAdmin: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 4
        )

        XCTAssertEqual(
            decision,
            .requestReissue(reason: "Welcome expired", nextAttempt: 2)
        )
    }

    func testNoMatchingKeyPackageEscalatesToExternalCommitAfterThreeAttempts() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .noMatchingKeyPackage,
            priorReissueAttempts: 3,
            hasCurrentAdmin: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 9
        )

        XCTAssertEqual(decision, .externalCommitWithHistoryGap(lastSeenEpoch: 9))
    }

    func testMissingWelcomeRequestsReissueWhenServerListsCurrentUserAsMemberEvenWithoutAdmin() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .welcomeUnavailable,
            priorReissueAttempts: 0,
            hasCurrentAdmin: false,
            serverListsCurrentUserAsMember: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 10
        )

        XCTAssertEqual(
            decision,
            .requestReissue(reason: "Welcome unavailable", nextAttempt: 1)
        )
    }

    func testNoMatchingKeyPackageRequestsReissueWhenServerListsCurrentUserAsMemberEvenWithoutAdmin() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .noMatchingKeyPackage,
            priorReissueAttempts: 1,
            hasCurrentAdmin: false,
            serverListsCurrentUserAsMember: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 8
        )

        XCTAssertEqual(
            decision,
            .requestReissue(reason: "NoMatchingKeyPackage", nextAttempt: 2)
        )
    }

    func testExpiredWelcomeRequestsReissueWhenServerListsCurrentUserAsMemberEvenWithoutAdmin() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .welcomeExpired,
            priorReissueAttempts: 2,
            hasCurrentAdmin: false,
            serverListsCurrentUserAsMember: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 6
        )

        XCTAssertEqual(
            decision,
            .requestReissue(reason: "Welcome expired", nextAttempt: 3)
        )
    }

    func testMissingWelcomeDoesNotExternalCommitWhenServerListedMemberExhaustsReissues() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .welcomeUnavailable,
            priorReissueAttempts: 3,
            hasCurrentAdmin: false,
            serverListsCurrentUserAsMember: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 10
        )

        XCTAssertEqual(
            decision,
            .surrender(reason: "Welcome reissue attempts exhausted", retryAfter: nil)
        )
    }

    func testNoMatchingKeyPackageDoesNotExternalCommitWhenServerListedMemberExhaustsReissues() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .noMatchingKeyPackage,
            priorReissueAttempts: 3,
            hasCurrentAdmin: false,
            serverListsCurrentUserAsMember: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 14
        )

        XCTAssertEqual(
            decision,
            .surrender(reason: "Welcome reissue attempts exhausted", retryAfter: nil)
        )
    }

    func testExpiredWelcomeDoesNotExternalCommitWhenServerListedMemberExhaustsReissues() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .welcomeExpired,
            priorReissueAttempts: 3,
            hasCurrentAdmin: false,
            serverListsCurrentUserAsMember: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 15
        )

        XCTAssertEqual(
            decision,
            .surrender(reason: "Welcome reissue attempts exhausted", retryAfter: nil)
        )
    }

    func testNoMatchingKeyPackageEscalatesToExternalCommitWhenNoAdminCanReissue() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .noMatchingKeyPackage,
            priorReissueAttempts: 0,
            hasCurrentAdmin: false,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 11
        )

        XCTAssertEqual(decision, .externalCommitWithHistoryGap(lastSeenEpoch: 11))
    }

    func testUnrecoverableGroupInfoSurrendersInsteadOfExternalCommitLoop() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .welcomeExpired,
            priorReissueAttempts: 3,
            hasCurrentAdmin: true,
            groupInfoUnrecoverable: true,
            externalCommitAlreadyFailed: false,
            lastSeenEpoch: 12
        )

        XCTAssertEqual(
            decision,
            .surrender(reason: "GroupInfo unrecoverable", retryAfter: nil)
        )
    }

    func testExternalCommitFailureAfterExhaustedReissueSurrenders() {
        let decision = WelcomeRecoveryPolicy.decide(
            welcomeFailure: .noMatchingKeyPackage,
            priorReissueAttempts: 3,
            hasCurrentAdmin: true,
            groupInfoUnrecoverable: false,
            externalCommitAlreadyFailed: true,
            lastSeenEpoch: 13
        )

        XCTAssertEqual(
            decision,
            .surrender(reason: "External Commit recovery failed", retryAfter: nil)
        )
    }
}
