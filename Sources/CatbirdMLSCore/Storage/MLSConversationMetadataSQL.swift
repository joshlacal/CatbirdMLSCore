import Foundation
import GRDB

/// SQL helpers for applying group display metadata to the stable
/// conversation row while MLS crypto operations continue to use `groupID`.
enum MLSConversationMetadataSQL {
  struct LocalDisplayMetadata: Equatable, Sendable {
    let conversationID: String
    let title: String?
    let avatarImageData: Data?
  }

  /// Resolve the stable conversation ID for a mutable MLS group ID.
  ///
  /// During initial group creation the two identifiers are the same, but
  /// server-side reset/rotation keeps `conversationID` stable and swaps
  /// `groupID`. Prefer the current `groupID` column, then fall back to the
  /// legacy "conversationID == groupIdHex" shape for old rows/new groups.
  static func resolveConversationID(
    db: Database,
    currentUserDID: String,
    groupIdHex: String
  ) throws -> String? {
    if let groupIdData = Data(hexEncoded: groupIdHex),
      let conversationID = try String.fetchOne(
        db,
        sql: """
          SELECT conversationID
          FROM MLSConversationModel
          WHERE currentUserDID = ? AND groupID = ?
          LIMIT 1;
          """,
        arguments: [currentUserDID, groupIdData]
      )
    {
      return conversationID
    }

    return try String.fetchOne(
      db,
      sql: """
        SELECT conversationID
        FROM MLSConversationModel
        WHERE currentUserDID = ? AND conversationID = ?
        LIMIT 1;
        """,
      arguments: [currentUserDID, groupIdHex]
    )
  }

  static func loadLocalDisplayMetadata(
    db: Database,
    currentUserDID: String,
    groupIdHex: String
  ) throws -> LocalDisplayMetadata? {
    guard
      let conversationID = try resolveConversationID(
        db: db,
        currentUserDID: currentUserDID,
        groupIdHex: groupIdHex
      )
    else {
      return nil
    }

    guard
      let row = try Row.fetchOne(
        db,
        sql: """
          SELECT title, avatarImageData
          FROM MLSConversationModel
          WHERE conversationID = ? AND currentUserDID = ?
          LIMIT 1;
          """,
        arguments: [conversationID, currentUserDID]
      )
    else {
      return nil
    }

    return LocalDisplayMetadata(
      conversationID: conversationID,
      title: row["title"],
      avatarImageData: row["avatarImageData"]
    )
  }

  @discardableResult
  static func updateDecryptedMetadata(
    db: Database,
    currentUserDID: String,
    groupIdHex: String,
    title: String?,
    avatarImageData: Data?,
    now: Date
  ) throws -> String? {
    guard
      let conversationID = try resolveConversationID(
        db: db,
        currentUserDID: currentUserDID,
        groupIdHex: groupIdHex
      )
    else {
      return nil
    }

    try db.execute(
      sql: """
        UPDATE MLSConversationModel
        SET title = ?, avatarImageData = ?, updatedAt = ?, isPlaceholder = 0
        WHERE conversationID = ? AND currentUserDID = ?;
        """,
      arguments: [title, avatarImageData, now, conversationID, currentUserDID]
    )

    return conversationID
  }

  /// Prefer fresh encrypted metadata, then server metadata, then the existing
  /// stable-row display title so reset/rotation sync cannot blank a group name
  /// while the new MLS group metadata is still being bootstrapped.
  static func mergedTitle(
    encryptedTitle: String?,
    serverTitle: String?,
    existingTitle: String?
  ) -> String? {
    nonEmpty(encryptedTitle) ?? nonEmpty(serverTitle) ?? nonEmpty(existingTitle)
  }

  private static func nonEmpty(_ value: String?) -> String? {
    guard let value, !value.isEmpty else { return nil }
    return value
  }
}
