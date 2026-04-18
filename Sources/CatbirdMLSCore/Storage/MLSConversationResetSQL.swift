import Foundation
import GRDB

/// Pure-SQL helpers for applying group-reset state transitions on
/// `MLSConversationModel` rows. Extracted from `MLSConversationManager` so the
/// DB-level behavior of §8.5 recipient recovery can be unit-tested without
/// constructing the full actor graph (MLSClient, MLSAPIClient, etc.).
///
/// All helpers take a live `GRDB.Database` handle and are intended to be
/// invoked inside a `database.write { db in ... }` closure.
public enum MLSConversationResetSQL {

  /// Apply the successful recipient-side group reset (§8.5 Phase 1).
  ///
  /// Updates the row identified by `(conversationID, currentUserDID)` to point
  /// at the new MLS group: replaces `groupID`, resets `epoch` to the freshly
  /// landed value, clears `needsReset` / `needsRejoin` / `isUnrecoverable`, and
  /// nulls the `pendingNewGroupId` + `pendingResetGeneration` staging columns.
  /// Also resets the recovery counters so the next failure is treated as the
  /// first one.
  ///
  /// Generation staleness must be checked by the caller before invoking this
  /// helper — see `MLSConversationResetSQL.loadPendingResetGeneration`.
  public static func applyRecipientResetSuccess(
    db: Database,
    conversationID: String,
    currentUserDID: String,
    newGroupID: Data,
    newEpoch: Int64,
    now: Date
  ) throws {
    try db.execute(
      sql: """
            UPDATE MLSConversationModel
            SET groupID = ?,
                epoch = ?,
                joinEpoch = ?,
                needsReset = 0,
                needsRejoin = 0,
                isUnrecoverable = 0,
                pendingNewGroupId = NULL,
                pendingResetGeneration = NULL,
                consecutiveFailures = 0,
                lastRecoveryAttempt = ?,
                updatedAt = ?
            WHERE conversationID = ? AND currentUserDID = ?;
        """,
      arguments: [newGroupID, newEpoch, newEpoch, now, now, conversationID, currentUserDID]
    )
  }

  /// Clear only the pending-reset staging columns, leaving `needsReset` intact.
  /// Used when verification fails (e.g. server returned a different group ID
  /// than the one the SSE event announced) and we want to drop the stale
  /// pointer while remaining in RESET_PENDING for another attempt.
  public static func clearPendingReset(
    db: Database,
    conversationID: String,
    currentUserDID: String,
    now: Date
  ) throws {
    try db.execute(
      sql: """
            UPDATE MLSConversationModel
            SET pendingNewGroupId = NULL,
                pendingResetGeneration = NULL,
                updatedAt = ?
            WHERE conversationID = ? AND currentUserDID = ?;
        """,
      arguments: [now, conversationID, currentUserDID]
    )
  }

  /// Read the current `pendingResetGeneration` for the row. Used by the
  /// recipient recovery path to detect mid-flight races: if a newer event
  /// bumped the generation while the External Commit was in progress, the
  /// caller should leave the RESET_PENDING flag set rather than clear it.
  public static func loadPendingResetGeneration(
    db: Database,
    conversationID: String,
    currentUserDID: String
  ) throws -> Int64? {
    try Int64.fetchOne(
      db,
      sql: """
            SELECT pendingResetGeneration FROM MLSConversationModel
            WHERE conversationID = ? AND currentUserDID = ?;
        """,
      arguments: [conversationID, currentUserDID]
    )
  }
}
