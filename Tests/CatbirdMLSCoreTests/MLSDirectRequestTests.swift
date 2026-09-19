import CatbirdMLS
@testable import CatbirdMLSCore
import Petrel
import PetrelCatbird
import XCTest

final class MLSDirectRequestTests: XCTestCase {
  func testRuntimeRoutesDirectRequestLifecycleThroughNativeBridge() throws {
    let bridge = RecordingDirectRequestBridge()
    let runtime = MLSOrchestratorRuntime(
      userDID: "did:plc:alice",
      mode: .rustFull,
      bridge: bridge
    )
    let invitation = GroupInvitationReference(
      authorityDid: "did:plc:authority",
      conversationId: "group-conversation",
      invitationTransitionId: "transition-1",
      invitedByDid: "did:plc:alice",
      invitedByDeviceId: "device-a",
      recipientDid: "did:plc:bob"
    )
    let input = DirectRequestInput(
      draftId: "00000000-0000-4000-8000-000000000001",
      recipientDid: "did:plc:bob",
      text: "protected introduction",
      invitation: invitation
    )
    bridge.groupInvitation = invitation

    XCTAssertEqual(
      try runtime.startDirectRequest(input: input),
      .outcomeUnknown(requestId: "request-1")
    )
    XCTAssertEqual(
      try runtime.resumeDirectRequest(draftId: input.draftId),
      .retryable(code: "RETRY_LATER")
    )
    XCTAssertEqual(
      try runtime.cancelDirectRequest(draftId: input.draftId),
      .terminalNotPublished(requestId: "request-1", code: "CANCELLED")
    )
    XCTAssertEqual(
      try runtime.refreshRequestPreview(conversationId: "conversation-1"),
      bridge.preview
    )
    XCTAssertEqual(try runtime.listDirectRequestViews(), [bridge.preview])
    XCTAssertNil(try runtime.getDirectRequestDeliveryEnabled())
    try runtime.setDirectRequestDeliveryEnabled(enabled: true)
    XCTAssertEqual(
      try runtime.closeDirectRequest(conversationId: "conversation-1"),
      bridge.preview
    )
    XCTAssertEqual(
      try runtime.getGroupInvitationReference(
        conversationId: "group-conversation",
        recipientDid: "did:plc:bob"
      ),
      invitation
    )
    XCTAssertEqual(bridge.startedInput, input)
    XCTAssertEqual(bridge.resumedDraftId, input.draftId)
    XCTAssertEqual(bridge.cancelledDraftId, input.draftId)
    XCTAssertEqual(bridge.refreshedConversationId, "conversation-1")
    XCTAssertEqual(bridge.deliveryEnabled, true)
    XCTAssertEqual(bridge.closedConversationId, "conversation-1")
    XCTAssertEqual(bridge.invitationConversationId, "group-conversation")
    XCTAssertEqual(bridge.invitationRecipientDid, "did:plc:bob")
  }

  func testCanonicalInventoryOptsIntoVersionTwoAndRequiresRetainedScope() throws {
    let parameters = MLSAPIClient.canonicalConversationInventoryParameters(
      actorDeviceId: "device-a",
      limit: 50,
      cursor: nil
    )
    XCTAssertEqual(parameters.supportedProtocolVersions, [.value_1, .value_2])

    let scoped = BlueCatbirdChatGetConversations.Output(
      items: [],
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-1",
      hasMore: false,
      snapshotExpiresAt: ATProtocolDate(date: Date(timeIntervalSince1970: 2_000_000_000)),
      inventoryScope: .init(
        supportedProtocolVersions: [.value_1, .value_2],
        scopeDigest: Bytes(data: Data([0x01]))
      )
    )
    XCTAssertNoThrow(try MLSAPIClient.validateDirectRequestInventoryScope(scoped))

    let unscoped = BlueCatbirdChatGetConversations.Output(
      items: [],
      inventorySessionId: "session-1",
      snapshotEventCursor: "cursor-1",
      hasMore: false,
      snapshotExpiresAt: ATProtocolDate(date: Date(timeIntervalSince1970: 2_000_000_000))
    )
    XCTAssertThrowsError(try MLSAPIClient.validateDirectRequestInventoryScope(unscoped))
  }

  func testCanonicalEntryClassificationKeepsRequestEntriesOutOfOrdinaryMessages() throws {
    let request = BlueCatbirdChatDefs.DirectRequestEntry(
      entryId: "request-entry",
      conversationId: "conversation-1",
      seq: 4,
      signedRequest: .init(
        body: .unexpected(.object([:])),
        signature: Bytes(data: Data([0x01]))
      ),
      receivedAt: ATProtocolDate(date: Date(timeIntervalSince1970: 1))
    )
    let acceptance = BlueCatbirdChatDefs.DirectRequestAcceptanceEntry(
      entryId: "acceptance-entry",
      conversationId: "conversation-1",
      seq: 5,
      signedRequest: .init(
        body: .unexpected(.object([:])),
        signature: Bytes(data: Data([0x02]))
      ),
      receivedAt: ATProtocolDate(date: Date(timeIntervalSince1970: 2))
    )

    let page = MLSAPIClient.classifyCanonicalEntries(
      [
        .blueCatbirdChatDefsDirectRequestEntry(request),
        .blueCatbirdChatDefsDirectRequestAcceptanceEntry(acceptance),
      ],
      hasMore: false,
      nextAfterSeq: 6
    )

    XCTAssertTrue(page.applicationEntries.isEmpty)
    XCTAssertEqual(page.directRequestEntries.map(\.entryId), ["request-entry"])
    XCTAssertEqual(page.directRequestAcceptanceEntries.map(\.entryId), ["acceptance-entry"])
    XCTAssertNil(page.lastSeq)
    XCTAssertEqual(
      BlueCatbirdChatDefs.ConversationEntry.blueCatbirdChatDefsDirectRequestEntry(request).entryID,
      "request-entry"
    )
  }
}

private final class RecordingDirectRequestBridge: OrchestratorBridge {
  var startedInput: DirectRequestInput?
  var resumedDraftId: String?
  var cancelledDraftId: String?
  var refreshedConversationId: String?
  var deliveryEnabled: Bool?
  var closedConversationId: String?
  var invitationConversationId: String?
  var invitationRecipientDid: String?
  var groupInvitation: GroupInvitationReference?
  let preview = DirectRequestView(
    introduction: RequestIntroductionMetadata(
      senderDid: "did:plc:alice",
      senderDeviceId: "device-a",
      receivedAt: "2026-09-19T12:00:00.000Z",
      epoch: 1,
      sequenceNumber: 4
    ),
    conversationId: "conversation-1",
    firstMessageId: "message-1",
    consent: .incomingPending,
    crypto: .ready,
    preview: .ready(text: "protected introduction", invitation: nil),
    capabilities: RequestCapabilities(
      canPreview: true,
      canAccept: true,
      canClose: true,
      canSend: false,
      canEmitReceipts: false,
      canAdminister: false
    ),
    stateVersion: 4,
    generation: 2
  )

  init() {
    super.init(noPointer: .init())
  }

  required init(unsafeFromRawPointer pointer: UnsafeMutableRawPointer) {
    super.init(unsafeFromRawPointer: pointer)
  }

  override func startDirectRequest(input: DirectRequestInput) throws -> DirectRequestOutcome {
    startedInput = input
    return .outcomeUnknown(requestId: "request-1")
  }

  override func resumeDirectRequest(draftId: String) throws -> DirectRequestOutcome {
    resumedDraftId = draftId
    return .retryable(code: "RETRY_LATER")
  }

  override func cancelDirectRequest(draftId: String) throws -> DirectRequestOutcome {
    cancelledDraftId = draftId
    return .terminalNotPublished(requestId: "request-1", code: "CANCELLED")
  }

  override func refreshRequestPreview(conversationId: String) throws -> DirectRequestView {
    refreshedConversationId = conversationId
    return preview
  }

  override func listDirectRequestViews() throws -> [DirectRequestView] {
    [preview]
  }

  override func getDirectRequestDeliveryEnabled() throws -> Bool? {
    nil
  }

  override func setDirectRequestDeliveryEnabled(enabled: Bool) throws {
    deliveryEnabled = enabled
  }

  override func closeDirectRequest(conversationId: String) throws -> DirectRequestView {
    closedConversationId = conversationId
    return preview
  }

  override func getGroupInvitationReference(
    conversationId: String,
    recipientDid: String
  ) throws -> GroupInvitationReference? {
    invitationConversationId = conversationId
    invitationRecipientDid = recipientDid
    return groupInvitation
  }
}
