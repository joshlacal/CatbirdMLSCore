import Testing
@testable import CatbirdMLSCore

@Suite("MLSBlockReconciler")
struct MLSBlockReconcilerTests {

    @Test("leaves groups containing any blocked DID")
    func reconcile_leavesGroupsWithBlockedMembers() async throws {
        let fake = FakeManager(snapshots: [
            .init(id: "c1", memberDids: ["did:me", "did:bob"]),
            .init(id: "c2", memberDids: ["did:me", "did:charlie"]),
            .init(id: "c3", memberDids: ["did:me", "did:dave", "did:bob"]),
        ])
        let reconciler = MLSBlockReconciler()

        let left = try await reconciler.reconcile(
            blockedDids: ["did:bob"],
            using: fake
        )

        #expect(Set(left) == Set(["c1", "c3"]))
        #expect(Set(fake.leftIds) == Set(["c1", "c3"]))
    }

    @Test("idempotent: running twice on same state does nothing second time")
    func reconcile_idempotent() async throws {
        let fake = FakeManager(snapshots: [
            .init(id: "c1", memberDids: ["did:me", "did:bob"]),
        ])
        let reconciler = MLSBlockReconciler()

        _ = try await reconciler.reconcile(blockedDids: ["did:bob"], using: fake)
        fake.removeConversation("c1")  // simulate the leave settling
        let secondPass = try await reconciler.reconcile(blockedDids: ["did:bob"], using: fake)

        #expect(secondPass.isEmpty)
    }

    @Test("empty block list is a no-op")
    func reconcile_emptyBlockListIsNoop() async throws {
        let fake = FakeManager(snapshots: [
            .init(id: "c1", memberDids: ["did:me", "did:bob"]),
        ])
        let reconciler = MLSBlockReconciler()

        let left = try await reconciler.reconcile(blockedDids: [], using: fake)

        #expect(left.isEmpty)
        #expect(fake.leftIds.isEmpty)
    }

    @Test("leave failure on one group does not abort others")
    func reconcile_tolerantToIndividualFailures() async throws {
        let fake = FakeManager(
            snapshots: [
                .init(id: "c1", memberDids: ["did:me", "did:bob"]),
                .init(id: "c2", memberDids: ["did:me", "did:bob"]),
            ],
            leaveFailures: ["c1"]
        )
        let reconciler = MLSBlockReconciler()

        let left = try await reconciler.reconcile(blockedDids: ["did:bob"], using: fake)

        #expect(left == ["c2"])
    }
}

// MARK: - Fake

private final class FakeManager: MLSGroupReconcilable, @unchecked Sendable {
    private var snapshots: [MLSConversationSnapshot]
    private(set) var leftIds: [String] = []
    private let failFor: Set<String>

    init(snapshots: [MLSConversationSnapshot], leaveFailures: Set<String> = []) {
        self.snapshots = snapshots
        self.failFor = leaveFailures
    }

    func listConversationSnapshots() async throws -> [MLSConversationSnapshot] { snapshots }

    func leaveConversation(convoId: String) async throws {
        if failFor.contains(convoId) {
            struct TestError: Error {}
            throw TestError()
        }
        leftIds.append(convoId)
        snapshots.removeAll { $0.id == convoId }
    }

    func removeConversation(_ id: String) {
        snapshots.removeAll { $0.id == id }
    }
}
