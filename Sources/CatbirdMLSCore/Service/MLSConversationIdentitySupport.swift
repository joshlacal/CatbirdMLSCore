import Foundation

enum MLSConversationIdentity {
  static func matches(requestedId: String, conversationId: String, groupId: String) -> Bool {
    let requestedId = requestedId.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !requestedId.isEmpty else {
      return false
    }

    return conversationId == requestedId || groupId == requestedId
  }

  static func processingGroupIdHex(conversationID: String, groupId: String?) -> String {
    let trimmedGroupId = groupId?.trimmingCharacters(in: .whitespacesAndNewlines)
    guard let trimmedGroupId, !trimmedGroupId.isEmpty else {
      return conversationID
    }

    return trimmedGroupId
  }
}
