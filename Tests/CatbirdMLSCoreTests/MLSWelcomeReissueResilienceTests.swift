import Foundation
import Testing
@testable import CatbirdMLS
@testable import CatbirdMLSCore

/// W3 #3: the admin-side Welcome-reissue responder must distinguish failures it
/// can never recover from on THIS device (no local group state → can't seal a
/// Welcome) from transient ones. The former must stop re-attempting (so the
/// device doesn't no-op-loop every event); the latter must still retry.
struct MLSWelcomeReissueResilienceTests {

  @Test func unfulfillableForConversationNotFound() {
    // After a hydration attempt, a still-missing conversation means this user
    // can't fulfill it here.
    #expect(
      MLSConversationManager.isWelcomeReissueUnfulfillableHere(
        MLSConversationError.conversationNotFound))
  }

  @Test func unfulfillableForGroupStateNotFound() {
    #expect(
      MLSConversationManager.isWelcomeReissueUnfulfillableHere(
        MLSConversationError.groupStateNotFound))
  }

  @Test func unfulfillableForMlsGroupNotFound() {
    let error = MlsError.GroupNotFound(message: "Group not found: 849021ceacb4ab33")
    #expect(MLSConversationManager.isWelcomeReissueUnfulfillableHere(error))
  }

  @Test func unfulfillableForRecipientNotInLocalTreeMessage() {
    // respondToWelcomeReissueRequest throws this when removeIdentities is empty.
    struct Wrapped: LocalizedError {
      var errorDescription: String? { "Recipient is not present in local MLS group state" }
    }
    #expect(MLSConversationManager.isWelcomeReissueUnfulfillableHere(Wrapped()))
  }

  @Test func transientErrorsStillRetry() {
    // Network/timeouts and unrelated errors must NOT be treated as
    // unfulfillable — they should clear the handled marker and retry.
    struct Net: LocalizedError { var errorDescription: String? { "The request timed out" } }
    #expect(!MLSConversationManager.isWelcomeReissueUnfulfillableHere(Net()))
    #expect(
      !MLSConversationManager.isWelcomeReissueUnfulfillableHere(
        MLSConversationError.noAuthentication))
  }
}
