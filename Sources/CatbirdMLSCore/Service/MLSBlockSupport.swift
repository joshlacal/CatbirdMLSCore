import Foundation

/// Lightweight, Sendable view of an MLS conversation used by block-aware
/// features that need only the conversation ID and its member DIDs.
///
/// Kept intentionally tiny so it can be passed across isolation boundaries
/// (main-actor UI, background reconcilers) without dragging the full
/// `ConvoView` lexicon type with it.
public struct MLSConversationSnapshot: Sendable, Equatable {
    public let id: String
    public let memberDids: [String]

    public init(id: String, memberDids: [String]) {
        self.id = id
        self.memberDids = memberDids
    }
}

/// Read/leave interface used by block-aware features
/// (`MLSBlockCoordinator`, `MLSBlockReconciler`).
///
/// Abstracting these two calls behind a protocol lets Catbird's coordinator
/// and the reconciler (Task 7.1) be unit-tested without spinning up a real
/// `MLSConversationManager`.
public protocol MLSGroupReconcilable: Sendable {
    /// Returns a snapshot of every conversation currently known to the
    /// manager, with members deduplicated by user DID (members are stored
    /// per-device in MLS, but block semantics operate on users).
    func listConversationSnapshots() async throws -> [MLSConversationSnapshot]

    /// Leave the conversation identified by `convoId`. Implementations are
    /// expected to be idempotent — leaving a conversation the user is not
    /// in should not throw.
    func leaveConversation(convoId: String) async throws
}
