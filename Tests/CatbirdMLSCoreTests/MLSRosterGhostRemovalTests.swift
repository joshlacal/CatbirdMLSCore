import Foundation
import Testing
@testable import CatbirdMLS
@testable import CatbirdMLSCore

/// W4 (iOS): classification for the roster-ghost removal fallback. When a member
/// is on the server roster but never joined the MLS tree (`leaf_index IS NULL`),
/// `stageCommit(.removeMembers)` / the rustFull runtime fail with
/// `MlsError.InvalidInput("No members found to remove")`. The client must
/// recognize exactly that case and fall back to a commitless removeMember —
/// without misclassifying unrelated failures into a server roster mutation.
struct MLSRosterGhostRemovalTests {

  @Test func detectsTypedInvalidInputNoMembers() {
    let error = MlsError.InvalidInput(message: "No members found to remove")
    #expect(MLSConversationManager.isRosterGhostRemovalError(error))
  }

  @Test func detectsTypedInvalidInputNoValidMembers() {
    // The Rust warn log and the FFI error use slightly different wording.
    let error = MlsError.InvalidInput(message: "No valid members found to remove")
    #expect(MLSConversationManager.isRosterGhostRemovalError(error))
  }

  @Test func detectsViaWrappedErrorMessage() {
    // A wrapped/rethrown error (e.g. from the rustFull runtime) still classifies
    // through its localizedDescription.
    struct Wrapped: LocalizedError {
      var errorDescription: String? { "stageCommit failed: No members found to remove" }
    }
    #expect(MLSConversationManager.isRosterGhostRemovalError(Wrapped()))
  }

  @Test func ignoresOtherInvalidInput() {
    // A generic InvalidInput with unrelated text must NOT be treated as a ghost —
    // otherwise an unrelated failure would silently trigger a server roster mutation.
    let error = MlsError.InvalidInput(message: "Key package signature key not authorized")
    #expect(!MLSConversationManager.isRosterGhostRemovalError(error))
  }

  @Test func ignoresUnrelatedError() {
    #expect(!MLSConversationManager.isRosterGhostRemovalError(MLSConversationError.conversationNotFound))
  }

  @Test func messageMatcherIsCaseInsensitiveAndNarrow() {
    #expect(MLSConversationManager.messageIndicatesNoTreeLeaf("NO MEMBERS FOUND TO REMOVE"))
    #expect(MLSConversationManager.messageIndicatesNoTreeLeaf("error: No Valid Members Found To Remove"))
    #expect(!MLSConversationManager.messageIndicatesNoTreeLeaf("member successfully removed"))
    #expect(!MLSConversationManager.messageIndicatesNoTreeLeaf(""))
  }
}
