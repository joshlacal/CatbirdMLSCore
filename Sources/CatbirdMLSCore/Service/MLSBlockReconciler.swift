import Foundation
import os.log

public struct MLSBlockReconciler: Sendable {
    private let logger = Logger(subsystem: "blue.catbird", category: "MLSBlockReconciler")

    public init() {}

    /// Leave any conversation whose members contain at least one DID in `blockedDids`.
    /// Returns the IDs of groups that were left.
    ///
    /// Tolerates per-group leave failures (logs + continues) so one bad group
    /// does not abort cleanup of the others.
    @discardableResult
    public func reconcile(
        blockedDids: Set<String>,
        using manager: some MLSGroupReconcilable
    ) async throws -> [String] {
        guard !blockedDids.isEmpty else { return [] }
        let snapshots = try await manager.listConversationSnapshots()
        var leftIds: [String] = []
        for convo in snapshots {
            guard let offendingDid = convo.memberDids.first(where: blockedDids.contains) else {
                continue
            }
            do {
                try await manager.leaveConversation(convoId: convo.id)
                leftIds.append(convo.id)
                logger.info(
                    "Reconciled: left \(convo.id) because member \(offendingDid, privacy: .public) is blocked"
                )
            } catch {
                logger.error(
                    "Reconcile leave failed for \(convo.id): \(String(describing: error))"
                )
            }
        }
        return leftIds
    }

    /// Convenience overload taking an array (no need for callers to build a Set).
    @discardableResult
    public func reconcile(
        blockedDids: [String],
        using manager: some MLSGroupReconcilable
    ) async throws -> [String] {
        try await reconcile(blockedDids: Set(blockedDids), using: manager)
    }
}
