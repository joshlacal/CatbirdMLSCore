import Foundation

extension MLSConversationManager {
    /// Returns a snapshot of every conversation currently in the in-memory
    /// store, with members deduplicated by user DID (the server stores one
    /// `MemberView` per device, but block semantics operate on users).
    public func listConversationSnapshots() async throws -> [MLSConversationSnapshot] {
        let views = conversations.values
        return views.map { convo in
            var seen = Set<String>()
            var ordered: [String] = []
            for member in convo.members {
                let did = member.userDid.didString()
                if seen.insert(did).inserted {
                    ordered.append(did)
                }
            }
            return MLSConversationSnapshot(id: convo.conversationId, memberDids: ordered)
        }
    }
}

extension MLSConversationManager: MLSGroupReconcilable {}
