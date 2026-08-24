import Foundation
import Petrel
import PetrelCatbird
import Testing
@testable import CatbirdMLSCore

@Suite("MLSAPIClient Stub Audit")
struct MLSAPIClientStubAuditTests {

    @Test("registerDeviceToken throws methodNotImplemented instead of returning hardcoded true")
    func registerDeviceToken_throwsMethodNotImplemented() async throws {
        let client = ATProtoClient()
        let apiClient = await MLSAPIClient(client: client)

        await #expect(throws: MLSAPIError.self) {
            _ = try await apiClient.registerDeviceToken(
                deviceId: "test-device",
                pushToken: "0123456789abcdef",
                deviceName: "Test Device"
            )
        }
    }

    @Test("unregisterDeviceToken throws methodNotImplemented instead of returning hardcoded true")
    func unregisterDeviceToken_throwsMethodNotImplemented() async throws {
        let client = ATProtoClient()
        let apiClient = await MLSAPIClient(client: client)

        await #expect(throws: MLSAPIError.self) {
            _ = try await apiClient.unregisterDeviceToken(deviceId: "test-device")
        }
    }

    @Test("leaveConversation throws methodNotImplemented on legacy stub")
    func leaveConversation_throwsMethodNotImplemented() async throws {
        let client = ATProtoClient()
        let apiClient = await MLSAPIClient(client: client)

        await #expect(throws: MLSAPIError.self) {
            _ = try await apiClient.leaveConversation(convoId: "test-convo")
        }
    }

    @Test("addMembers throws methodNotImplemented on legacy stub")
    func addMembers_throwsMethodNotImplemented() async throws {
        let client = ATProtoClient()
        let apiClient = await MLSAPIClient(client: client)

        await #expect(throws: MLSAPIError.self) {
            _ = try await apiClient.addMembers(
                convoId: "test-convo",
                didList: [try DID(didString: "did:plc:targetuser")]
            )
        }
    }

    @Test("processExternalCommit throws methodNotImplemented on legacy stub")
    func processExternalCommit_throwsMethodNotImplemented() async throws {
        let client = ATProtoClient()
        let apiClient = await MLSAPIClient(client: client)

        await #expect(throws: MLSAPIError.self) {
            _ = try await apiClient.processExternalCommit(
                convoId: "test-convo",
                externalCommit: Data([0x01, 0x02, 0x03])
            )
        }
    }

    @Test("getPendingDeviceAdditions throws methodNotImplemented")
    func getPendingDeviceAdditions_throwsMethodNotImplemented() async throws {
        let client = ATProtoClient()
        let apiClient = await MLSAPIClient(client: client)

        await #expect(throws: MLSAPIError.self) {
            _ = try await apiClient.getPendingDeviceAdditions()
        }
    }

    @Test("claimPendingDeviceAddition throws methodNotImplemented")
    func claimPendingDeviceAddition_throwsMethodNotImplemented() async throws {
        let client = ATProtoClient()
        let apiClient = await MLSAPIClient(client: client)

        await #expect(throws: MLSAPIError.self) {
            _ = try await apiClient.claimPendingDeviceAddition(pendingAdditionId: "test-addition")
        }
    }

    @Test("completePendingDeviceAddition throws methodNotImplemented")
    func completePendingDeviceAddition_throwsMethodNotImplemented() async throws {
        let client = ATProtoClient()
        let apiClient = await MLSAPIClient(client: client)

        await #expect(throws: MLSAPIError.self) {
            _ = try await apiClient.completePendingDeviceAddition(
                pendingAdditionId: "test-addition",
                newEpoch: 2
            )
        }
    }

    @Test("publishKeyPackagesBatch throws methodNotImplemented")
    func publishKeyPackagesBatch_throwsMethodNotImplemented() async throws {
        let client = ATProtoClient()
        let apiClient = await MLSAPIClient(client: client)

        await #expect(throws: MLSAPIError.self) {
            _ = try await apiClient.publishKeyPackagesBatch([])
        }
    }
}
