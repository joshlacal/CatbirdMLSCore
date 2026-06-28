import Foundation
import Testing
@testable import CatbirdMLS
@testable import CatbirdMLSCore

/// W3 #1: in rustFull the admin device must route the Welcome-reissue mutation
/// into the Rust orchestrator (swap_members + idempotency key), while swiftLegacy
/// keeps staging the commit in Swift. The routing decision is a pure function so
/// it can be asserted without an MLS engine. The "unfulfillable here"
/// classification must still apply on the rustFull path so a genuinely-absent
/// group stops re-attempting instead of looping forever.
struct MLSWelcomeReissueRustFullRoutingTests {
  @Test func rustFullRoutesToRuntime() {
    #expect(MLSConversationManager.welcomeReissueRouting(mode: .rustFull) == .rustRuntime)
  }

  @Test func swiftLegacyRoutesToSwiftResponder() {
    #expect(MLSConversationManager.welcomeReissueRouting(mode: .swiftLegacy) == .swiftResponder)
  }

  @Test func unfulfillableClassificationStillApplies() {
    // The rustFull path must still treat "no local group state" as unfulfillable
    // so it stops re-attempting on every event.
    #expect(
      MLSConversationManager.isWelcomeReissueUnfulfillableHere(
        MLSConversationError.groupStateNotFound))
  }
}
