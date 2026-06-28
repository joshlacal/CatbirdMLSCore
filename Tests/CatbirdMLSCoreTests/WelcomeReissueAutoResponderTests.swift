import GRDB
import CatbirdMLS
import Petrel
import PetrelCatbird
import XCTest

@testable import CatbirdMLSCore

final class WelcomeReissueAutoResponderTests: XCTestCase {
    func testHandleWelcomeReissueRequestedIgnoresDuplicateRequestIDAlreadyInFlight() async throws {
        let manager = try await makeManager()
        let requestID = "request-in-flight"
        let event = makeEvent(requestID: requestID)

        manager.welcomeReissueResponseState.withLock { handled in
            _ = handled.insert(requestID)
        }

        await manager.handleWelcomeReissueRequested(event: event)

        let handledIDs = manager.welcomeReissueResponseState.withLock { handled in
            handled
        }
        XCTAssertEqual(handledIDs, [requestID])
    }

    func testHandleWelcomeReissueRequestedClearsRequestIDAfterFailureSoRetryCanProceed() async throws {
        let manager = try await makeManager()
        let requestID = "request-retry"
        let event = makeEvent(requestID: requestID)

        await manager.handleWelcomeReissueRequested(event: event)

        let stateAfterFirstFailure = manager.welcomeReissueResponseState.withLock { handled in
            handled
        }
        XCTAssertFalse(stateAfterFirstFailure.contains(requestID))

        await manager.handleWelcomeReissueRequested(event: event)

        let stateAfterRetry = manager.welcomeReissueResponseState.withLock { handled in
            handled
        }
        XCTAssertFalse(stateAfterRetry.contains(requestID))
    }

    func testRemoveIdentitiesTargetsOnlyRequestedDeviceQualifiedIdentity() {
        let members = [
            makeMember("did:plc:recipient#phone"),
            makeMember("did:plc:recipient#ipad"),
            makeMember("did:plc:other#phone"),
        ]

        let removeDids = MLSConversationManager.removeIdentities(
            forRecipientDeviceDid: "did:plc:recipient#phone",
            from: members
        )

        XCTAssertEqual(removeDids, ["did:plc:recipient#phone"])
    }

    func testRemoveIdentitiesFallsBackToBareDidOnlyForLegacyMemberIdentity() {
        let members = [
            makeMember("did:plc:recipient"),
            makeMember("did:plc:recipient#ipad"),
            makeMember("did:plc:other#phone"),
        ]

        let removeDids = MLSConversationManager.removeIdentities(
            forRecipientDeviceDid: "did:plc:recipient#phone",
            from: members
        )

        XCTAssertEqual(removeDids, ["did:plc:recipient"])
    }

    func testRemoveIdentitiesKeepsBareDidRequestLegacyUserWide() {
        let members = [
            makeMember("did:plc:recipient#phone"),
            makeMember("did:plc:recipient#ipad"),
            makeMember("did:plc:other#phone"),
        ]

        let removeDids = MLSConversationManager.removeIdentities(
            forRecipientDeviceDid: "did:plc:recipient",
            from: members
        )

        XCTAssertEqual(removeDids, ["did:plc:recipient#phone", "did:plc:recipient#ipad"])
    }

    func testCurrentDeviceDetectionAllowsSameUserOtherDevice() {
        XCTAssertFalse(
            MLSConversationManager.isWelcomeReissueRequestForCurrentDevice(
                recipientDeviceDid: "did:plc:recipient#ipad",
                currentDeviceDid: "did:plc:recipient#phone"
            )
        )
    }

    func testCurrentDeviceDetectionIgnoresExactCurrentDevice() {
        XCTAssertTrue(
            MLSConversationManager.isWelcomeReissueRequestForCurrentDevice(
                recipientDeviceDid: "did:plc:recipient#phone",
                currentDeviceDid: "did:plc:recipient#phone"
            )
        )
    }

    private func makeManager() async throws -> MLSConversationManager {
        let database = try DatabaseQueue()
        let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
        let apiClient = await MLSAPIClient(
            client: atProtoClient,
            environment: .custom(serviceDID: "did:web:example.com#atproto_mls")
        )
        // These tests exercise the swiftLegacy Welcome auto-responder
        // (respondToWelcomeReissueRequest staging a Swift commit). Pin the mode
        // explicitly so they keep testing swiftLegacy behavior independent of the
        // now-rustFull defaultMode.
        return MLSConversationManager(
            apiClient: apiClient,
            database: database,
            atProtoClient: atProtoClient,
            protocolAuthorityMode: .swiftLegacy
        )
    }

    private func makeEvent(requestID: String) -> BlueCatbirdMlsChatSubscribeEvents.WelcomeReissueRequestedEvent {
        BlueCatbirdMlsChatSubscribeEvents.WelcomeReissueRequestedEvent(
            cursor: "cursor-\(requestID)",
            convoId: "convo-\(requestID)",
            recipientDeviceDid: "did:plc:recipient#device",
            requestedAt: ATProtocolDate(date: Date()),
            requestId: requestID
        )
    }

    private func makeMember(_ identity: String) -> GroupMemberDebugInfo {
        GroupMemberDebugInfo(
            leafIndex: 0,
            credentialIdentity: Data(identity.utf8),
            credentialType: "basic"
        )
    }
}
