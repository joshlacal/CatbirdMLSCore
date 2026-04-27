import Foundation

enum MLSConversationIdentity {
  static func matches(requestedId: String, conversationId: String, groupId: String) -> Bool {
    let requestedId = requestedId.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !requestedId.isEmpty else {
      return false
    }

    return conversationId == requestedId || groupId == requestedId
  }
}
