import Foundation
import Petrel

/// State change events
public enum MLSStateEvent {
  case conversationCreated(BlueCatbirdMlsChatDefs.ConvoView)
  case conversationJoined(BlueCatbirdMlsChatDefs.ConvoView)
  case conversationLeft(String)
  case conversationRequestAccepted(String)  // convoId - chat request was accepted
  case membersAdded(String, [DID])
  case messageSent(String, ATProtocolDate)
  case epochUpdated(String, Int)
  case syncCompleted(Int)
  case syncFailed(Error)
  case membershipChanged(convoId: String, did: DID, action: MembershipAction)
  case kickedFromConversation(convoId: String, by: DID, reason: String?)
  case conversationNeedsRecovery(convoId: String, reason: RecoveryReason)
  case reactionReceived(
    convoId: String, messageId: String, emoji: String, senderDID: String, action: String)
  case groupReset(convoId: String, newGroupId: String, resetGeneration: Int, resetBy: DID?, reason: String?)

  public var description: String {
    switch self {
    case .conversationCreated(let convo):
      return "Conversation created: \(convo.conversationId)"
    case .conversationJoined(let convo):
      return "Conversation joined: \(convo.conversationId)"
    case .conversationLeft(let id):
      return "Conversation left: \(id)"
    case .conversationRequestAccepted(let id):
      return "Chat request accepted: \(id)"
    case .membersAdded(let convoId, let members):
      return "Members added to \(convoId): \(members.count)"
    case .messageSent(let msgId, _):
      return "Message sent: \(msgId)"
    case .epochUpdated(let convoId, let epoch):
      return "Epoch updated for \(convoId): \(epoch)"
    case .syncCompleted(let count):
      return "Sync completed: \(count) conversations"
    case .syncFailed(let error):
      return "Sync failed: \(error.localizedDescription)"
    case .membershipChanged(let convoId, let did, let action):
      return "Membership changed in \(convoId): \(did) - \(action.rawValue)"
    case .kickedFromConversation(let convoId, let by, let reason):
      return "Kicked from \(convoId) by \(by)\(reason.map { ": \($0)" } ?? "")"
    case .conversationNeedsRecovery(let convoId, let reason):
      return "Conversation \(convoId) needs recovery: \(reason.rawValue)"
    case .reactionReceived(let convoId, let messageId, let emoji, let senderDID, let action):
      return "Reaction \(action) in \(convoId): \(emoji) on \(messageId) by \(senderDID)"
    case .groupReset(let convoId, let newGroupId, let resetGeneration, let resetBy, let reason):
      return "Group reset in \(convoId): newGroup=\(newGroupId), gen=\(resetGeneration), by=\(resetBy)\(reason.map { ": \($0)" } ?? "")"
    }
  }
}

/// State change observer
public class MLSStateObserver {
  public let id: UUID
  public let onStateChange: (MLSStateEvent) -> Void

  public init(id: UUID = UUID(), onStateChange: @escaping (MLSStateEvent) -> Void) {
    self.id = id
    self.onStateChange = onStateChange
  }
}
