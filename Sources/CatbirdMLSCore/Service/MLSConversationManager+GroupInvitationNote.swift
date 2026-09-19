import Foundation

extension MLSConversationManager {
  /// Sends one durable invitation note through the Rust typed-payload path.
  /// This deliberately performs one publication attempt: callers persist the
  /// attempt before entry and explicitly reconcile an uncertain outcome.
  public func sendGroupInvitationNote(
    conversationId: String,
    text: String,
    reference: MLSGroupInvitationReference
  ) async throws -> String {
    try throwIfShuttingDown("sendGroupInvitationNote")
    guard let senderDID = userDid else { throw MLSConversationError.noAuthentication }
    let capturedGeneration = sessionGeneration
    guard MLSCoordinationStore.shared.getState().activeUserDID.map({
      MLSStorageHelpers.normalizeDID($0) == MLSStorageHelpers.normalizeDID(senderDID)
    }) ?? true else { throw MLSConversationError.noAuthentication }

    let payload = try MLSMessagePayload.groupInvitationNote(text: text, reference: reference)
    let stableConversationID = try await rustConversationID(for: conversationId)
    let result = try await withRustAuthoritativeRuntime(
      operation: "sendGroupInvitationNote"
    ) { runtime in
      try runtime.sendPayloadResult(
        conversationId: stableConversationID,
        payload: payload
      )
    }
    try validateSessionGeneration(capturedGeneration: capturedGeneration)
    guard MLSCoordinationStore.shared.getState().activeUserDID.map({
      MLSStorageHelpers.normalizeDID($0) == MLSStorageHelpers.normalizeDID(senderDID)
    }) ?? true else { throw MLSConversationError.noAuthentication }
    await handleRustEngineEvents(result.events, source: "sendGroupInvitationNote")
    try validateSessionGeneration(capturedGeneration: capturedGeneration)
    return result.message.id
  }
}
