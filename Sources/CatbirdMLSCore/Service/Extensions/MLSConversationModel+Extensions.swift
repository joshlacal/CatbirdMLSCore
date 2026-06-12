import Foundation
import Petrel
import PetrelCatbird

public extension MLSConversationModel {
  /// Convert local database model to API ConvoView
  /// Note: Some fields are approximated since the local model doesn't store all API fields
  public func asConvoView() -> BlueCatbirdMlsChatDefs.ConvoView? {
    // Create DID from stored currentUserDID string
    guard let creatorDID = try? DID(didString: currentUserDID) else {
      return nil
    }

    return BlueCatbirdMlsChatDefs.ConvoView(
      conversationId: conversationID,
      groupId: groupID.hexEncodedString(),
      creator: creatorDID,
      members: [],  // Members not stored in local model, fetched separately when needed
      epoch: Int(epoch),
      cipherSuite: "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
      createdAt: ATProtocolDate(date: createdAt),
      lastMessageAt: lastMessageAt.map { ATProtocolDate(date: $0) },
      confirmationTag: nil,
      // WS-4 rung 2: not stored in the local model; iOS consumption of
      // sequencerDid is rung 3 (E7).
      resetGeneration: nil,
      sequencerDid: nil
    )
  }
}
