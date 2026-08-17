import CatbirdMLS
import Foundation
import Petrel
import PetrelCatbird
/// Narrow iOS seam for the Rust clean-chat transport.
///
/// Petrel owns every request and response DTO. This adapter only turns those
/// generated Codable values into the bytes expected by UniFFI and decodes the
/// bytes returned by Rust; it does not define a second wire schema or execute
/// HTTP itself. Uploads and subscriptions remain deliberately outside this
/// seam until their dedicated platform transports are migrated.
public enum MLSCanonicalTransportAdapter {
  public static func prepare<Input: Encodable>(
    auth: CleanChatAuthContextFfi,
    operation: CleanChatOperationFfi,
    input: Input
  ) throws -> CleanChatPreparedRequestFfi {
    let requestJSON = try JSONEncoder().encode(input)
    return try prepareCleanChatRequest(
      auth: auth,
      operation: operation,
      requestJson: requestJSON
    )
  }

  public static func decode<Response: Decodable>(
    _ response: Data,
    operation: CleanChatOperationFfi,
    as _: Response.Type = Response.self
  ) throws -> Response {
    let canonicalJSON = try decodeCleanChatResponse(
      operation: operation,
      responseJson: response
    )
    return try JSONDecoder().decode(Response.self, from: canonicalJSON)
  }

  public static func decodeBlob(_ response: Data) throws -> Data {
    try decodeCleanChatBlob(responseBytes: response)
  }
  /// Project canonical conversation state into the compatibility view used by
  /// existing manager orchestration. The source remains the generated
  /// clean-chat DTO; no legacy endpoint is consulted.
  public static func projectConversationView(
    from state: BlueCatbirdChatDefs.ConversationState
  ) -> BlueCatbirdMlsChatDefs.ConvoView? {
    let creatorString = state.metadataSnapshot.authorProof.authorDid.description
    guard let creator = try? DID(didString: creatorString) else {
      return nil
    }
    let joinedAt = ATProtocolDate(date: .distantPast)
    let members = state.participants.compactMap { participant -> BlueCatbirdMlsChatDefs.MemberView? in
      guard let did = try? DID(didString: participant.userDid.description) else {
        return nil
      }
      return BlueCatbirdMlsChatDefs.MemberView(
        did: did,
        userDid: did,
        deviceId: nil,
        deviceName: nil,
        joinedAt: joinedAt,
        isAdmin: participant.userDid.description == creatorString,
        isModerator: nil,
        promotedAt: nil,
        promotedBy: nil,
        leafIndex: nil,
        credential: nil
      )
    }
    return BlueCatbirdMlsChatDefs.ConvoView(
      conversationId: state.coordinates.conversationId.description,
      groupId: state.coordinates.groupId.data.hexEncodedString(),
      creator: creator,
      members: members,
      epoch: state.coordinates.epoch,
      cipherSuite: state.cipherSuite.rawValue,
      createdAt: joinedAt,
      lastMessageAt: nil,
      confirmationTag: state.coordinates.confirmationTag,
      resetGeneration: state.coordinates.generation,
      sequencerDid: nil
    )
  }

  /// Project one canonical entry into a compatibility message view without
  /// changing its semantic type. A requested type is a filter, never a relabel.
  public static func projectMessageView(
    from entry: BlueCatbirdChatDefs.ConversationEntry,
    messageType: BlueCatbirdMlsChatDefs.MessageViewMessageType? = nil
  ) -> BlueCatbirdMlsChatDefs.MessageView? {
    switch entry {
    case let .blueCatbirdChatDefsApplicationEntry(application):
      guard messageType == nil || messageType == .value_app,
            case let .blueCatbirdChatDefsApplicationSendBody(body) = application.signedRequest.body
      else {
        return nil
      }
      return BlueCatbirdMlsChatDefs.MessageView(
        id: String(describing: application.entryId),
        convoId: String(describing: application.conversationId),
        ciphertext: body.applicationMessage.bytes,
        epoch: body.prior.epoch,
        seq: application.seq,
        createdAt: ATProtocolDate(date: application.receivedAt.date),
        messageType: .value_app
      )

    case let .blueCatbirdChatDefsCommitEntry(commit):
      guard messageType == nil || messageType == .value_commit,
            case let .blueCatbirdChatDefsCommitTransitionBody(body) = commit.signedRequest.body
      else {
        return nil
      }
      return BlueCatbirdMlsChatDefs.MessageView(
        id: String(describing: commit.entryId),
        convoId: String(describing: commit.conversationId),
        ciphertext: body.commit.bytes,
        epoch: body.prior.epoch,
        seq: commit.seq,
        createdAt: ATProtocolDate(date: commit.receivedAt.date),
        messageType: .value_commit
      )

    default:
      return nil
    }
  }

  /// The inventory snapshot is the ticket's cursor fence. A persisted event
  /// cursor may be older than that fence after reconnect; explicitly reconcile
  /// it to the fresh snapshot cursor instead of logging it and silently
  /// discarding it.
  internal static func reconciledResumeCursor(
    savedCursor: String?,
    inventorySnapshotCursor: String
  ) -> String {
    inventorySnapshotCursor
  }

  /// Every durable payload emitted by the generated clean-chat union. Keeping
  /// the generated payload as the associated value prevents the client from
  /// silently flattening status, identity, or terminal-sequence fields.
  public enum MLSCanonicalDurableEvent {
    /// Typing is a generated subscription variant but intentionally has no
    /// durable cursor. It is surfaced here so a known best-effort message is
    /// not mistaken for an unsupported durable payload.
    case typing(BlueCatbirdChatDefs.TypingEvent)
    case messageAvailable(
      BlueCatbirdChatDefs.MessageAvailableEvent,
      cursor: String,
      messages: [BlueCatbirdMlsChatDefs.MessageView]
    )
    case conversationChanged(BlueCatbirdChatDefs.ConversationChangedEvent)
    case conversationClosed(BlueCatbirdChatDefs.ConversationClosedEvent)
    case welcomeAvailable(BlueCatbirdChatDefs.WelcomeAvailableEvent)
    case welcomeDisposition(BlueCatbirdChatDefs.WelcomeDispositionEvent)
    case resetRequested(BlueCatbirdChatDefs.ResetRequestedEvent)
    case leafRecovery(BlueCatbirdChatDefs.LeafRecoveryEvent)
    case leaveRequest(BlueCatbirdChatDefs.LeaveRequestEvent)
    case accessEnded(BlueCatbirdChatDefs.AccessEndedEvent)
    case watermark(BlueCatbirdChatDefs.WatermarkEvent)
  }

  internal struct MLSUnsupportedDurableEventError: Error, LocalizedError, Equatable {
    internal let typeIdentifier: String

    internal var errorDescription: String? {
      "Unsupported durable event payload: \(typeIdentifier)"
    }
  }

  internal enum MLSCanonicalStreamHandlingResult: Equatable {
    case handled
    case reconnect(Error)

    static func == (
      lhs: MLSCanonicalStreamHandlingResult,
      rhs: MLSCanonicalStreamHandlingResult
    ) -> Bool {
      switch (lhs, rhs) {
      case (.handled, .handled):
        return true
      case let (.reconnect(lhsError), .reconnect(rhsError)):
        return String(describing: lhsError) == String(describing: rhsError)
      default:
        return false
      }
    }
  }

  /// Handle one generated durable envelope. The cursor is committed only once
  /// the typed handler (and any message-entry reconciliation) has succeeded.
  /// A handler failure or unknown payload requests reconnect and leaves the
  /// cursor unchanged so replay can retry the exact durable event.
  @discardableResult
  internal static func handleCanonicalStreamMessage(
    _ message: BlueCatbirdChatSubscribeEvents.Message,
    subscriptionKey: String,
    loadEntries: @escaping (String, Int) async throws -> [BlueCatbirdChatDefs.ConversationEntry],
    onDurableEvent: @escaping (MLSCanonicalDurableEvent) async throws -> Void,
    saveCursor: @escaping (String) -> Void
  ) async -> MLSCanonicalStreamHandlingResult {
    guard case let .blueCatbirdChatDefsEventEnvelope(envelope) = message else {
      if case let .blueCatbirdChatDefsTypingEvent(typing) = message {
        do {
          try await onDurableEvent(.typing(typing))
          return .handled
        } catch {
          return .reconnect(error)
        }
      }
      return .reconnect(
        MLSUnsupportedDurableEventError(typeIdentifier: "blue.catbird.chat.defs#eventEnvelope")
      )
    }

    let event: MLSCanonicalDurableEvent
    switch envelope.payload {
    case let .blueCatbirdChatDefsMessageAvailableEvent(available):
      let conversationID = String(describing: available.conversationId)
      guard subscriptionKey == "__global__" || subscriptionKey == conversationID else {
        saveCursor(envelope.cursor)
        return .handled
      }
      do {
        let entries = try await loadEntries(conversationID, max(0, available.seq - 1))
        let messages = entries.compactMap { entry -> BlueCatbirdMlsChatDefs.MessageView? in
          guard let message = projectMessageView(from: entry),
                message.convoId == conversationID
          else {
            return nil
          }
          return message
        }
        event = .messageAvailable(available, cursor: envelope.cursor, messages: messages)
      } catch {
        return .reconnect(error)
      }

    case let .blueCatbirdChatDefsConversationChangedEvent(changed):
      guard subscriptionKey == "__global__"
        || String(describing: changed.conversationId) == subscriptionKey
      else {
        saveCursor(envelope.cursor)
        return .handled
      }
      event = .conversationChanged(changed)

    case let .blueCatbirdChatDefsConversationClosedEvent(closed):
      guard subscriptionKey == "__global__"
        || String(describing: closed.conversationId) == subscriptionKey
      else {
        saveCursor(envelope.cursor)
        return .handled
      }
      event = .conversationClosed(closed)

    case let .blueCatbirdChatDefsWelcomeAvailableEvent(available):
      guard subscriptionKey == "__global__"
        || String(describing: available.conversationId) == subscriptionKey
      else {
        saveCursor(envelope.cursor)
        return .handled
      }
      event = .welcomeAvailable(available)

    case let .blueCatbirdChatDefsWelcomeDispositionEvent(disposition):
      event = .welcomeDisposition(disposition)

    case let .blueCatbirdChatDefsResetRequestedEvent(reset):
      guard subscriptionKey == "__global__"
        || String(describing: reset.conversationId) == subscriptionKey
      else {
        saveCursor(envelope.cursor)
        return .handled
      }
      event = .resetRequested(reset)

    case let .blueCatbirdChatDefsLeafRecoveryEvent(recovery):
      guard subscriptionKey == "__global__"
        || String(describing: recovery.conversationId) == subscriptionKey
      else {
        saveCursor(envelope.cursor)
        return .handled
      }
      event = .leafRecovery(recovery)

    case let .blueCatbirdChatDefsLeaveRequestEvent(leave):
      guard subscriptionKey == "__global__"
        || String(describing: leave.conversationId) == subscriptionKey
      else {
        saveCursor(envelope.cursor)
        return .handled
      }
      event = .leaveRequest(leave)

    case let .blueCatbirdChatDefsAccessEndedEvent(accessEnded):
      guard subscriptionKey == "__global__"
        || String(describing: accessEnded.conversationId) == subscriptionKey
      else {
        saveCursor(envelope.cursor)
        return .handled
      }
      event = .accessEnded(accessEnded)

    case let .blueCatbirdChatDefsWatermarkEvent(watermark):
      event = .watermark(watermark)

    case let .unexpected(container):
      let typeIdentifier: String
      switch container {
      case let .unknownType(type, _):
        typeIdentifier = type
      case let .decodeError(message):
        typeIdentifier = message
      default:
        typeIdentifier = "unknown"
      }
      return .reconnect(
        MLSUnsupportedDurableEventError(typeIdentifier: typeIdentifier)
      )
    }

    do {
      try await onDurableEvent(event)
    } catch {
      return .reconnect(error)
    }
    saveCursor(envelope.cursor)
    return .handled
  }

  /// Shared canonical stream-handler seam used by both WebSocket and SSE.
  /// This compatibility overload exposes the legacy message callback while the
  /// typed overload above remains the source of truth for all durable events.
  internal static func handleCanonicalStreamMessage(
    _ message: BlueCatbirdChatSubscribeEvents.Message,
    subscriptionKey: String,
    loadEntries: @escaping (String, Int) async throws -> [BlueCatbirdChatDefs.ConversationEntry],
    onMessage: @escaping (BlueCatbirdMlsChatSubscribeEvents.MessageEvent) async -> Void,
    onError: @escaping (Error) async -> Void,
    saveCursor: @escaping (String) -> Void
  ) async {
    let result = await handleCanonicalStreamMessage(
      message,
      subscriptionKey: subscriptionKey,
      loadEntries: loadEntries,
      onDurableEvent: { event in
        guard case let .messageAvailable(_, _, messages) = event,
              case let .blueCatbirdChatDefsEventEnvelope(envelope) = message
        else {
          return
        }
        for message in messages {
          await onMessage(
            BlueCatbirdMlsChatSubscribeEvents.MessageEvent(
              cursor: envelope.cursor,
              message: message,
              ephemeral: nil,
              epoch: message.epoch
            )
          )
        }
      },
      saveCursor: saveCursor
    )
    if case let .reconnect(error) = result {
      await onError(error)
    }
  }
}

// Keep the failure type discoverable at module scope for focused callers and
// tests while retaining the adapter's implementation-local declaration.
internal typealias MLSUnsupportedDurableEventError =
  MLSCanonicalTransportAdapter.MLSUnsupportedDurableEventError
