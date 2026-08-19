import Foundation
import Petrel
import PetrelCatbird

public extension MLSConversationModel {
  /// Create from ConversationState
  init(
    state: BlueCatbirdChatDefs.ConversationState,
    currentUserDID: String
  ) {
    let now = Date()
    self.conversationID = state.coordinates.conversationId.description
    self.currentUserDID = currentUserDID
    self.groupID = state.coordinates.groupId.data
    self.epoch = Int64(state.coordinates.epoch)
    self.joinMethod = .welcome
    self.joinEpoch = Int64(state.coordinates.epoch)
    self.title = nil
    self.description = nil
    self.avatarURL = nil
    self.avatarImageData = nil
    self.createdAt = now
    self.updatedAt = now
    self.lastMessageAt = nil
    self.lastMembershipChangeAt = nil
    self.unacknowledgedMemberChanges = 0
    self.isActive = true
    self.needsRejoin = false
    self.needsReset = false
    self.isUnrecoverable = false
    self.rejoinRequestedAt = nil
    self.lastRecoveryAttempt = nil
    self.consecutiveFailures = 0
    self.isPlaceholder = false
    self.requestState = .none
    self.mutedUntil = nil
    self.pendingNewGroupId = nil
    self.pendingResetGeneration = Int64(state.coordinates.generation)
  }
}

public extension BlueCatbirdChatDefs.ConversationState {
  var conversationId: String { coordinates.conversationId.description }
  var groupId: String { coordinates.groupId.data.hexEncodedString() }
  var epoch: Int { coordinates.epoch }
}
