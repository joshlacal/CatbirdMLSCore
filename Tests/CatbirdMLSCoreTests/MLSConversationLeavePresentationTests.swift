import CatbirdMLS
import Foundation
import Petrel
import PetrelCatbird
import Testing
@testable import CatbirdMLSCore

struct MLSConversationLeavePresentationTests {
  private let cid = "550e8400-e29b-41d4-a716-446655440000"
  private let device = "ec85d622-d53d-4cf8-b24e-adc3c10d32fc"
  private let requestID = "9f0b254a-2ff8-49e3-8994-02e726200001"
  private let account = "did:plc:oq3qa6f332ergklpj2dvd3up"

  private func output(requester: String? = nil, requestCID: String? = nil, expiry: String = "2030-01-02T00:00:00.000Z", status: BlueCatbirdChatDefs.DefsLeaveRequestStatus = .value_pending) throws -> BlueCatbirdChatGetConversationState.Output {
    let state = try MLSGroupMutationResult(ffiResult: FfiGroupMutationResult(conversation: FfiConversationView(
      canonicalStateJson: nil,
      groupId: String(repeating: "ab", count: 32),
      conversationId: cid,
      epoch: 4,
      members: [FfiMemberView(did: account, role: "member")],
      name: "Saved group",
      description: nil,
      avatarUrl: nil,
      createdAt: nil,
      updatedAt: nil)), userDID: account).conversation
    let request = BlueCatbirdChatDefs.LeaveRequestView(
      leaveRequestId: requestID, conversationId: requestCID ?? cid,
      requesterDid: try DID(didString: requester ?? account), requesterDeviceId: device,
      prior: state.coordinates, status: status,
      requestedAt: ATProtocolDate(date: try #require(MLSPendingLeaveHint.date("2030-01-01T00:00:00.000Z"))),
      expiresAt: ATProtocolDate(date: MLSPendingLeaveHint.date(expiry) ?? .distantPast))
    return BlueCatbirdChatGetConversationState.Output(state: state, pendingResetRequests: [], pendingLeaveRequests: [request], pendingLeafRecoveryRequests: [])
  }

  @Test func pendingHintSurvivesStoreReopenAndExpiryOnlyChangesWording() async throws {
    let suite = "mls-leave-hint-test-\(UUID().uuidString)"
    let defaults = try #require(UserDefaults(suiteName: suite))
    defer { defaults.removePersistentDomain(forName: suite) }
    let now = try #require(MLSPendingLeaveHint.date("2030-01-01T12:00:00.000Z"))
    let hint = try #require(try MLSPendingLeaveHint.validated(output: output(), userDID: account, conversationID: cid, now: now))
    let first = MLSPendingLeaveHintStore(defaults: UserDefaults(suiteName: suite))
    try await first.replace(hint, userDID: account, conversationID: cid)
    let reopened = MLSPendingLeaveHintStore(defaults: UserDefaults(suiteName: suite))
    let saved = try #require(await reopened.load(userDID: account, conversationID: cid))
    #expect(saved.request.leaveRequestId == requestID)
    #expect(saved.request.prior.epoch == 4)
    #expect(saved.presentation(now: now) == .pending)
    #expect(saved.presentation(now: now.addingTimeInterval(86_400)) == .checking)
    #expect(await reopened.load(userDID: "did:plc:otheruser", conversationID: cid) == nil)
    #expect(await reopened.load(userDID: account, conversationID: "550e8400-e29b-41d4-a716-446655440001") == nil)
  }

  @Test func invalidRefreshCannotReplacePreviouslyValidatedPendingHint() async throws {
    let suite = "mls-leave-hint-test-\(UUID().uuidString)"
    let defaults = try #require(UserDefaults(suiteName: suite))
    defer { defaults.removePersistentDomain(forName: suite) }
    let store = MLSPendingLeaveHintStore(defaults: UserDefaults(suiteName: suite))
    let hint = try #require(try MLSPendingLeaveHint.validated(output: output(), userDID: account, conversationID: cid, now: Date()))
    try await store.replace(hint, userDID: account, conversationID: cid)
    #expect(throws: (any Error).self) {
      try MLSPendingLeaveHint.validated(output: output(requestCID: "550e8400-e29b-41d4-a716-446655440001"), userDID: account, conversationID: cid, now: Date())
    }
    #expect(throws: (any Error).self) {
      try MLSPendingLeaveHint.validated(output: output(expiry: "not a timestamp"), userDID: account, conversationID: cid, now: Date())
    }
    #expect(await store.load(userDID: account, conversationID: cid) == hint)
  }

  @Test func authoritativeAbsenceAndTerminalRequestClearOnlyTheUIHint() async throws {
    let response = try output(status: .value_cancelled)
    #expect(try MLSPendingLeaveHint.validated(output: response, userDID: account, conversationID: cid, now: Date()) == nil)
    let absent = BlueCatbirdChatGetConversationState.Output(state: response.state, pendingResetRequests: [], pendingLeaveRequests: [], pendingLeafRecoveryRequests: [])
    #expect(try MLSPendingLeaveHint.validated(output: absent, userDID: account, conversationID: cid, now: Date()) == nil)
    #expect(try MLSPendingLeaveHint.validated(output: output(requester: "did:plc:anotheruser"), userDID: account, conversationID: cid, now: Date()) == nil)
  }

  @Test func accountLeaveRequiresTypedNativeReceiptExactAccountDeviceAndTerminalState() {
    let payload = MLSMessagePayload(messageType: .system, text: "membership.left")
    let marker = "membership-left:\(requestID):\(device)"
    func accepts(_ payload: MLSMessagePayload, _ id: String = "", _ sender: String? = nil, _ deviceID: String? = nil, _ terminal: String? = "device_removed") -> Bool {
      MLSSystemMessagePresentation.isVerifiedAccountLeave(payload: payload, messageID: id.isEmpty ? marker : id,
        senderDID: sender ?? account, currentUserDID: account, currentDeviceID: deviceID ?? device, terminalState: terminal)
    }
    #expect(accepts(payload))
    #expect(!accepts(payload, requestID))
    #expect(!accepts(payload, "membership-left:garbage:\(device)"))
    #expect(!accepts(payload, "membership-left:\(requestID.uppercased()):\(device)"))
    #expect(!accepts(payload, marker, "did:plc:anotheruser"))
    #expect(!accepts(payload, marker, nil, "ec247b70-d391-45db-a035-fa4658fce42b"))
    #expect(!accepts(payload, marker, nil, nil, nil))
    #expect(!accepts(MLSMessagePayload(messageType: .text, text: "membership.left")))
    #expect(!accepts(MLSMessagePayload(version: 2, messageType: .system, text: "membership.left")))
  }

  @Test func onlyStructuredCurrentSystemPayloadsReceiveSystemDisplayCopy() {
    let text = MLSMessagePayload(messageType: .text, text: "history_boundary.new_member")
    #expect(MLSSystemMessagePresentation.text(text) == "history_boundary.new_member")
    #expect(!MLSSystemMessagePresentation.isSystem(text))
    let system = MLSMessagePayload(messageType: .system, text: "history_boundary.new_member")
    #expect(MLSSystemMessagePresentation.text(system) == "You joined this conversation")
    let left = MLSMessagePayload(messageType: .system, text: "membership.left")
    #expect(MLSSystemMessagePresentation.text(left) == "Conversation membership changed")
    #expect(MLSSystemMessagePresentation.text(left, verifiedAccountLeave: true) == "You left this conversation")
    #expect(MLSSystemMessagePresentation.text(MLSMessagePayload(messageType: .system, text: "conversation.closed")) == "Conversation status changed")
    #expect(MLSSystemMessagePresentation.text(MLSMessagePayload(messageType: .text, text: "conversation.closed")) == "conversation.closed")
  }
}


extension MLSConversationLeavePresentationTests {
  @Test func onlySuccessfulMissingDeviceReadinessBecomesPendingAccess() {
    for state in ConversationRecoveryState.allCases {
      let expected = state == .needsRejoin || state == .groupMissing
      #expect(MLSConversationLifecycleError.isPendingDeviceAccess(
        MLSConversationReadyResult(recoveryState: state, epoch: 2, sendAllowed: false)) == expected)
      #expect(!MLSConversationLifecycleError.isPendingDeviceAccess(
        MLSConversationReadyResult(recoveryState: state, epoch: 2, sendAllowed: true)))
    }
  }
}
