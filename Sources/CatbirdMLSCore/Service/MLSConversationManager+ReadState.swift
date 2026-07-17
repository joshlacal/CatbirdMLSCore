//
//  MLSConversationManager+ReadState.swift
//  CatbirdMLSCore
//
//  Per-message read/unread state, plus a best-effort server cursor sync
//  when the newest message in a conversation is marked read.
//

import Foundation
import GRDB
import OSLog

extension MLSConversationManager {

  /// Sets the read/unread state of a single message.
  ///
  /// Marking a message read cascades to every earlier-sequenced message in
  /// the conversation (messages-domain semantics). Marking a message unread
  /// only affects that single row.
  ///
  /// When `read == true` and the message is the newest in the conversation
  /// (highest `sequenceNumber`), this also fires a best-effort server cursor
  /// update via `MLSAPIClient.updateCursor(convoId:cursor:)`. That call is
  /// intentionally swallowed on failure — cursor sync is advisory and must
  /// never surface as a failure of the local read-state change.
  ///
  /// - Parameters:
  ///   - convoId: Conversation the message belongs to
  ///   - messageId: The message whose read state is changing
  ///   - read: `true` to mark read, `false` to mark unread
  public func setMessageReadStatus(
    convoId: String,
    messageId: String,
    read: Bool
  ) async throws {
    guard let userDid else {
      throw MLSError.noCurrentUser
    }

    if read {
      try await MLSStorageHelpers.markMessageAsRead(
        in: database,
        messageID: messageId,
        conversationID: convoId,
        currentUserDID: userDid
      )
    } else {
      try await MLSStorageHelpers.markMessageAsUnread(
        in: database,
        messageID: messageId,
        conversationID: convoId,
        currentUserDID: userDid
      )
    }

    guard read else { return }

    let isNewest = (try? await MLSStorageHelpers.isNewestMessage(
      in: database,
      messageID: messageId,
      conversationID: convoId,
      currentUserDID: userDid
    )) ?? false
    guard isNewest else { return }

    // Best-effort only — the local read-state change already succeeded, so
    // a cursor-sync failure here must never surface as a thrown error.
    if (try? await apiClient.updateCursor(convoId: convoId, cursor: messageId)) == nil {
      logger.debug("setMessageReadStatus: cursor update failed for \(convoId, privacy: .public)")
    }
  }
}
