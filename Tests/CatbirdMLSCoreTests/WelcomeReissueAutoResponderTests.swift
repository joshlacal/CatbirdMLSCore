import GRDB
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

    private func makeManager() async throws -> MLSConversationManager {
        let database = try DatabaseQueue()
        let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
        let apiClient = await MLSAPIClient(
            client: atProtoClient,
            environment: .custom(serviceDID: "did:web:example.com#atproto_mls")
        )
        return MLSConversationManager(
            apiClient: apiClient,
            database: database,
            atProtoClient: atProtoClient
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
}
