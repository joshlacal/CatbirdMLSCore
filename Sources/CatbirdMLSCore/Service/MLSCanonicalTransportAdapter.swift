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

  /// Shared canonical stream-handler seam used by both WebSocket and SSE.
  /// Durable envelopes advance the cursor exactly once; entries are filtered
  /// by the subscribed conversation and unknown variants are skipped.
  internal static func handleCanonicalStreamMessage(
    _ message: BlueCatbirdChatSubscribeEvents.Message,
    subscriptionKey: String,
    loadEntries: @escaping (String, Int) async throws -> [BlueCatbirdChatDefs.ConversationEntry],
    onMessage: @escaping (BlueCatbirdMlsChatSubscribeEvents.MessageEvent) async -> Void,
    onError: @escaping (Error) async -> Void,
    saveCursor: @escaping (String) -> Void
  ) async {
    guard case let .blueCatbirdChatDefsEventEnvelope(envelope) = message else {
      return
    }

    guard case let .blueCatbirdChatDefsMessageAvailableEvent(available) = envelope.payload else {
      saveCursor(envelope.cursor)
      return
    }

    let availableConversationId = String(describing: available.conversationId)
    guard subscriptionKey == "__global__" || subscriptionKey == availableConversationId else {
      saveCursor(envelope.cursor)
      return
    }

    do {
      let entries = try await loadEntries(availableConversationId, max(0, available.seq - 1))
      for entry in entries {
        guard let message = projectMessageView(from: entry),
              message.convoId == availableConversationId
        else {
          continue
        }
        await onMessage(
          BlueCatbirdMlsChatSubscribeEvents.MessageEvent(
            cursor: envelope.cursor,
            message: message,
            ephemeral: nil,
            epoch: message.epoch
          )
        )
      }
      saveCursor(envelope.cursor)
    } catch {
      await onError(error)
    }
  }
}
