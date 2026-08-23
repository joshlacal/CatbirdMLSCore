import Foundation
import Petrel
import PetrelCatbird

public extension MLSConversationModel {
  /// Merge an authoritative Rust projection into an existing local row.
  /// Rust owns group coordinates and epoch; local-only recovery, reset,
  /// request, mute, profile, and join bookkeeping survives projection refresh.
  static func mergedRustSnapshot(
    state: BlueCatbirdChatDefs.ConversationState,
    currentUserDID: String,
    existing: MLSConversationModel?
  ) -> MLSConversationModel {
    let incoming = MLSConversationModel(state: state, currentUserDID: currentUserDID)
    return MLSConversationModel(
      conversationID: incoming.conversationID,
      currentUserDID: incoming.currentUserDID,
      groupID: incoming.groupID,
      epoch: incoming.epoch,
      joinMethod: existing?.joinMethod ?? incoming.joinMethod,
      joinEpoch: existing?.joinEpoch ?? incoming.joinEpoch,
      title: existing?.title,
      description: existing?.description,
      avatarURL: existing?.avatarURL,
      avatarImageData: existing?.avatarImageData,
      createdAt: existing?.createdAt ?? incoming.createdAt,
      updatedAt: incoming.updatedAt,
      lastMessageAt: existing?.lastMessageAt,
      lastMembershipChangeAt: existing?.lastMembershipChangeAt,
      unacknowledgedMemberChanges: existing?.unacknowledgedMemberChanges ?? 0,
      isActive: true,
      needsRejoin: existing?.needsRejoin ?? false,
      needsReset: existing?.needsReset ?? false,
      isUnrecoverable: existing?.isUnrecoverable ?? false,
      rejoinRequestedAt: existing?.rejoinRequestedAt,
      lastRecoveryAttempt: existing?.lastRecoveryAttempt,
      consecutiveFailures: existing?.consecutiveFailures ?? 0,
      isPlaceholder: false,
      requestState: existing?.requestState ?? .none,
      mutedUntil: existing?.mutedUntil,
      pendingNewGroupId: existing?.pendingNewGroupId,
      pendingResetGeneration: existing?.pendingResetGeneration ?? incoming.pendingResetGeneration
    )
  }

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
