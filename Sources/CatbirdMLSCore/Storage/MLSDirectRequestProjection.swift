import CatbirdMLS
import Foundation
import GRDB

/// Whether an Apple conversation may use the legacy ordinary-message path.
/// Only a retained canonical v1 policy can grant that classification. Missing
/// local state remains unknown so an inventory or notification race cannot
/// bypass Rust request consent.
public enum MLSDirectRequestConversationClassification: String, Sendable, Equatable {
  case verifiedRequest
  case requestAwaitingProjection
  case legacyV1
  case unknown
}

/// Replayable host projection of Rust-owned encrypted request state. The
/// authoritative journal remains native; these rows feed Apple presentation
/// and record whether the accepted introduction reached normal history.
enum MLSDirectRequestProjection {
  enum ProjectionError: Error {
    case invalidView
    case staleOrConflictingView
    case conflictingHistory
  }

  private struct ProtectedPreview: Codable, Equatable {
    struct Invitation: Codable, Equatable {
      let authorityDID: String
      let conversationID: String
      let invitationTransitionID: String
      let invitedByDID: String
      let invitedByDeviceID: String
      let recipientDID: String
    }

    let text: String?
    let unavailableCode: String?
    let invitation: Invitation?
  }

  static func createTables(in db: Database) throws {
    try db.execute(sql: """
      CREATE TABLE IF NOT EXISTS mls_direct_request_projection (
        current_user_did TEXT NOT NULL,
        conversation_id TEXT NOT NULL,
        first_message_id TEXT,
        consent TEXT NOT NULL,
        crypto TEXT NOT NULL,
        protected_preview BLOB,
        can_preview INTEGER NOT NULL,
        can_accept INTEGER NOT NULL,
        can_close INTEGER NOT NULL,
        can_send INTEGER NOT NULL,
        can_emit_receipts INTEGER NOT NULL,
        can_administer INTEGER NOT NULL,
        state_version INTEGER NOT NULL,
        generation INTEGER NOT NULL,
        introduction_sender_did TEXT,
        introduction_sender_device_id TEXT,
        introduction_received_at TEXT,
        introduction_epoch INTEGER,
        introduction_sequence_number INTEGER,
        history_projected_message_id TEXT,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (current_user_did, conversation_id)
      );
      CREATE INDEX IF NOT EXISTS idx_direct_request_projection_consent
        ON mls_direct_request_projection(current_user_did, consent, updated_at);

      CREATE TABLE IF NOT EXISTS mls_direct_request_operation_projection (
        current_user_did TEXT NOT NULL,
        draft_id TEXT NOT NULL,
        recipient_did TEXT,
        outcome TEXT NOT NULL,
        request_id TEXT,
        conversation_id TEXT,
        message_id TEXT,
        code TEXT,
        requires_acceptance INTEGER,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (current_user_did, draft_id)
      );
      """)
  }

  static func classifyConversation(
    userDID: String,
    conversationID: String,
    in db: Database
  ) throws -> MLSDirectRequestConversationClassification {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDID)
    if try db.tableExists("mls_direct_request_projection"),
       try Bool.fetchOne(
         db,
         sql: """
           SELECT EXISTS(
             SELECT 1 FROM mls_direct_request_projection
             WHERE current_user_did = ? AND conversation_id = ?
           )
           """,
         arguments: [normalizedDID, conversationID]
       ) == true {
      return .requestAwaitingProjection
    }
    guard let policy = try MLSCanonicalPolicyProjection.storedState(
      userDID: normalizedDID,
      conversationID: conversationID,
      in: db
    ) else {
      return .unknown
    }
    return policy.directRequest == nil ? .legacyV1 : .requestAwaitingProjection
  }

  static func persist(
    _ view: DirectRequestView,
    userDID: String,
    context: MlsContext,
    in db: Database
  ) throws {
    try createTables(in: db)
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDID)
    guard Int64(exactly: view.stateVersion) != nil,
          Int64(exactly: view.generation) != nil else {
      throw ProjectionError.invalidView
    }

    if let previous = try Row.fetchOne(
      db,
      sql: """
        SELECT *
        FROM mls_direct_request_projection
        WHERE current_user_did = ? AND conversation_id = ?
        """,
      arguments: [normalizedDID, view.conversationId]
    ) {
      let previousGeneration: Int64 = previous["generation"]
      let previousVersion: Int64 = previous["state_version"]
      let incomingGeneration = Int64(view.generation)
      let incomingVersion = Int64(view.stateVersion)
      guard incomingGeneration >= previousGeneration else { return }
      if incomingGeneration == previousGeneration {
        guard incomingVersion >= previousVersion else { return }
        if incomingVersion == previousVersion {
          guard try isEquivalent(previous, to: view, context: context) else {
            throw ProjectionError.staleOrConflictingView
          }
        }
      }
      let previousConsent: String = previous["consent"]
      if previousConsent == "closed", consentString(view.consent) != "closed" {
        throw ProjectionError.staleOrConflictingView
      }
      if let projectedMessageID: String = previous["history_projected_message_id"],
         view.firstMessageId != projectedMessageID {
        throw ProjectionError.conflictingHistory
      }
    }

    let protectedPreview = try encryptedPreview(view.preview, conversationID: view.conversationId,
      context: context)
    let introduction = view.introduction
    if let introduction {
      guard Int64(exactly: introduction.epoch) != nil,
            Int64(exactly: introduction.sequenceNumber) != nil,
            parseCanonicalTimestamp(introduction.receivedAt) != nil else {
        throw ProjectionError.invalidView
      }
    }

    try db.execute(sql: """
      INSERT INTO mls_direct_request_projection (
        current_user_did, conversation_id, first_message_id, consent, crypto,
        protected_preview, can_preview, can_accept, can_close, can_send,
        can_emit_receipts, can_administer, state_version, generation,
        introduction_sender_did, introduction_sender_device_id,
        introduction_received_at, introduction_epoch,
        introduction_sequence_number, history_projected_message_id, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
      ON CONFLICT(current_user_did, conversation_id) DO UPDATE SET
        first_message_id = excluded.first_message_id,
        consent = excluded.consent,
        crypto = excluded.crypto,
        protected_preview = excluded.protected_preview,
        can_preview = excluded.can_preview,
        can_accept = excluded.can_accept,
        can_close = excluded.can_close,
        can_send = excluded.can_send,
        can_emit_receipts = excluded.can_emit_receipts,
        can_administer = excluded.can_administer,
        state_version = excluded.state_version,
        generation = excluded.generation,
        introduction_sender_did = excluded.introduction_sender_did,
        introduction_sender_device_id = excluded.introduction_sender_device_id,
        introduction_received_at = excluded.introduction_received_at,
        introduction_epoch = excluded.introduction_epoch,
        introduction_sequence_number = excluded.introduction_sequence_number,
        updated_at = excluded.updated_at
      """, arguments: [
        normalizedDID,
        view.conversationId,
        view.firstMessageId,
        consentString(view.consent),
        cryptoString(view.crypto),
        protectedPreview,
        view.capabilities.canPreview,
        view.capabilities.canAccept,
        view.capabilities.canClose,
        view.capabilities.canSend,
        view.capabilities.canEmitReceipts,
        view.capabilities.canAdminister,
        Int64(view.stateVersion),
        Int64(view.generation),
        introduction?.senderDid,
        introduction?.senderDeviceId,
        introduction?.receivedAt,
        introduction.map { Int64($0.epoch) },
        introduction.map { Int64($0.sequenceNumber) },
        Date(),
      ])

    try projectAcceptedIntroductionIfReady(
      view,
      userDID: normalizedDID,
      context: context,
      in: db
    )
  }

  static func persistOperation(
    _ outcome: DirectRequestOutcome,
    draftID: String,
    recipientDID: String?,
    userDID: String,
    in db: Database
  ) throws {
    try createTables(in: db)
    let fields: (
      outcome: String,
      requestID: String?,
      conversationID: String?,
      messageID: String?,
      code: String?,
      requiresAcceptance: Bool?
    )
    switch outcome {
    case let .requestSent(conversationId, messageId, requestId):
      fields = ("request_sent", requestId, conversationId, messageId, nil, nil)
    case let .outcomeUnknown(requestId):
      fields = ("outcome_unknown", requestId, nil, nil, nil, nil)
    case let .existingDirect(conversationId, requiresAcceptance):
      fields = ("existing_direct", nil, conversationId, nil, nil, requiresAcceptance)
    case let .retryable(code):
      fields = ("retryable", nil, nil, nil, code, nil)
    case let .terminalNotPublished(requestId, code):
      fields = ("terminal_not_published", requestId, nil, nil, code, nil)
    }

    try db.execute(sql: """
      INSERT INTO mls_direct_request_operation_projection (
        current_user_did, draft_id, recipient_did, outcome, request_id,
        conversation_id, message_id, code, requires_acceptance, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(current_user_did, draft_id) DO UPDATE SET
        recipient_did = COALESCE(excluded.recipient_did, recipient_did),
        outcome = excluded.outcome,
        request_id = COALESCE(excluded.request_id, request_id),
        conversation_id = COALESCE(excluded.conversation_id, conversation_id),
        message_id = COALESCE(excluded.message_id, message_id),
        code = excluded.code,
        requires_acceptance = excluded.requires_acceptance,
        updated_at = excluded.updated_at
      """, arguments: [
        MLSStorageHelpers.normalizeDID(userDID), draftID, recipientDID,
        fields.outcome, fields.requestID, fields.conversationID,
        fields.messageID, fields.code, fields.requiresAcceptance, Date(),
      ])
  }

  private static func projectAcceptedIntroductionIfReady(
    _ view: DirectRequestView,
    userDID: String,
    context: MlsContext,
    in db: Database
  ) throws {
    guard case .accepted = view.consent,
          let messageID = view.firstMessageId,
          let introduction = view.introduction,
          let preview = view.preview,
          case let .ready(text, _) = preview,
          let timestamp = parseCanonicalTimestamp(introduction.receivedAt),
          let epoch = Int64(exactly: introduction.epoch),
          let sequence = Int64(exactly: introduction.sequenceNumber)
    else { return }

    guard try MLSConversationModel
      .filter(MLSConversationModel.Columns.currentUserDID == userDID)
      .filter(MLSConversationModel.Columns.conversationID == view.conversationId)
      .fetchCount(db) == 1 else { return }

    let existing = try MLSMessageModel
      .filter(MLSMessageModel.Columns.currentUserDID == userDID)
      .filter(MLSMessageModel.Columns.messageID == messageID)
      .fetchOne(db)
    if existing == nil {
      try MLSStorageHelpers.savePayloadSync(
        context: context,
        in: db,
        messageID: messageID,
        conversationID: view.conversationId,
        currentUserDID: userDID,
        payload: .text(text),
        senderID: introduction.senderDid,
        epoch: epoch,
        sequenceNumber: sequence,
        timestamp: timestamp
      )
      try db.execute(sql: """
        UPDATE MLSMessageModel
        SET isDelivered = 1, isSent = ?, processingState = ?
        WHERE messageID = ? AND currentUserDID = ?
        """, arguments: [
          MLSStorageHelpers.normalizeDID(introduction.senderDid) == userDID,
          MLSMessageProcessingState.cached,
          messageID,
          userDID,
        ])
    }

    // `savePayloadSync` may intentionally suppress a row at a durable deletion
    // fence. Never record projection success unless the exact accepted
    // introduction is now present; otherwise a later retry must remain able to
    // repair the host projection without asking Rust to publish again.
    guard let materialized = try MLSMessageModel
      .filter(MLSMessageModel.Columns.currentUserDID == userDID)
      .filter(MLSMessageModel.Columns.messageID == messageID)
      .fetchOne(db),
      materialized.conversationID == view.conversationId,
      materialized.senderID == MLSStorageHelpers.normalizeDID(introduction.senderDid),
      materialized.epoch == epoch,
      materialized.sequenceNumber == sequence,
      materialized.timestamp == timestamp,
      materialized.decryptedPayload(context: context)?.text == text
    else {
      throw ProjectionError.conflictingHistory
    }

    try db.execute(sql: """
      UPDATE mls_direct_request_projection
      SET history_projected_message_id = ?
      WHERE current_user_did = ? AND conversation_id = ?
        AND (history_projected_message_id IS NULL OR history_projected_message_id = ?)
      """, arguments: [messageID, userDID, view.conversationId, messageID])
  }

  private static func encryptedPreview(
    _ preview: RequestPreview?,
    conversationID: String,
    context: MlsContext
  ) throws -> Data? {
    guard let protected = protectedPreview(preview) else { return nil }
    return try MLSFieldEncryption.encrypt(
      context: context,
      conversationID: conversationID,
      plaintext: try JSONEncoder().encode(protected)
    )
  }

  private static func protectedPreview(_ preview: RequestPreview?) -> ProtectedPreview? {
    switch preview {
    case let .ready(text, invitation):
      return ProtectedPreview(
        text: text,
        unavailableCode: nil,
        invitation: invitation.map {
          ProtectedPreview.Invitation(
            authorityDID: $0.authorityDid,
            conversationID: $0.conversationId,
            invitationTransitionID: $0.invitationTransitionId,
            invitedByDID: $0.invitedByDid,
            invitedByDeviceID: $0.invitedByDeviceId,
            recipientDID: $0.recipientDid
          )
        }
      )
    case let .unavailable(code):
      return ProtectedPreview(text: nil, unavailableCode: code, invitation: nil)
    case nil:
      return nil
    }
  }

  private static func isEquivalent(
    _ row: Row,
    to view: DirectRequestView,
    context: MlsContext
  ) throws -> Bool {
    let storedPreview: ProtectedPreview?
    if let encrypted: Data = row["protected_preview"] {
      let plaintext = try MLSFieldEncryption.decrypt(
        context: context,
        conversationID: view.conversationId,
        wire: encrypted
      )
      storedPreview = try JSONDecoder().decode(ProtectedPreview.self, from: plaintext)
    } else {
      storedPreview = nil
    }
    let introduction = view.introduction
    return (row["first_message_id"] as String?) == view.firstMessageId
      && (row["consent"] as String) == consentString(view.consent)
      && (row["crypto"] as String) == cryptoString(view.crypto)
      && storedPreview == protectedPreview(view.preview)
      && (row["can_preview"] as Bool) == view.capabilities.canPreview
      && (row["can_accept"] as Bool) == view.capabilities.canAccept
      && (row["can_close"] as Bool) == view.capabilities.canClose
      && (row["can_send"] as Bool) == view.capabilities.canSend
      && (row["can_emit_receipts"] as Bool) == view.capabilities.canEmitReceipts
      && (row["can_administer"] as Bool) == view.capabilities.canAdminister
      && (row["introduction_sender_did"] as String?) == introduction?.senderDid
      && (row["introduction_sender_device_id"] as String?) == introduction?.senderDeviceId
      && (row["introduction_received_at"] as String?) == introduction?.receivedAt
      && (row["introduction_epoch"] as Int64?) == introduction.map { Int64($0.epoch) }
      && (row["introduction_sequence_number"] as Int64?)
        == introduction.map { Int64($0.sequenceNumber) }
  }

  private static func consentString(_ consent: RequestConsent) -> String {
    switch consent {
    case .incomingPending: return "incoming_pending"
    case .outgoingPending: return "outgoing_pending"
    case .accepted: return "accepted"
    case .closed: return "closed"
    }
  }

  private static func cryptoString(_ crypto: RequestCrypto) -> String {
    switch crypto {
    case .welcomeAvailable: return "welcome_available"
    case .importing: return "importing"
    case .ready: return "ready"
    case .unavailable: return "unavailable"
    }
  }

  private static func parseCanonicalTimestamp(_ value: String) -> Date? {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return formatter.date(from: value)
  }
}
