import CatbirdMLS
@testable import CatbirdMLSCore
import Foundation
import GRDB
import Petrel
import PetrelCatbird
import XCTest

final class MLSDirectRequestTests: XCTestCase {
  private let projectionConversationID = "550e8400-e29b-41d4-a716-446655440000"
  private let projectionMessageID = "650e8400-e29b-41d4-a716-446655440000"
  private var tempDirectory: URL!
  private var dbPool: DatabasePool!
  private var context: MlsContext!

  override func setUp() async throws {
    try await super.setUp()
    tempDirectory = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSDirectRequestTests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
    dbPool = try DatabasePool(path: tempDirectory.appendingPathComponent("messages.sqlite").path)
    try MLSGRDBManager.makeMigrator().migrate(dbPool)
    context = try MlsContext(
      storagePath: tempDirectory.appendingPathComponent("native.sqlite").path,
      encryptionKey: String(repeating: "ab", count: 32),
      keychain: InMemoryKeychainAccess()
    )
    try context.setContentRootKey(key: Data(repeating: 0x5A, count: 32))
  }

  override func tearDown() async throws {
    if let context {
      context.clearContentRootKey()
      try? context.flushAndPrepareClose()
    }
    context = nil
    if let dbPool {
      try? dbPool.close()
    }
    dbPool = nil
    if let tempDirectory {
      try? FileManager.default.removeItem(at: tempDirectory)
    }
    tempDirectory = nil
    try await super.tearDown()
  }

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

  func testAcceptedProjectionIsIdempotentAndFencesFirstMessageAndTerminalState() throws {
    let userDID = "did:plc:receiver"
    let adapter = try makeProjectionAdapter(userDID: userDID)
    let pending = requestView(consent: .incomingPending, stateVersion: 4)
    try adapter.persistDirectRequestView(pending)

    try dbPool.read { db in
      XCTAssertEqual(try MLSMessageModel.fetchCount(db), 0)
    }

    let accepted = requestView(
      consent: .accepted,
      stateVersion: 5,
      canSend: true
    )
    try adapter.persistDirectRequestView(accepted)
    try adapter.persistDirectRequestView(accepted)

    try dbPool.read { db in
      XCTAssertEqual(try MLSMessageModel.fetchCount(db), 1)
      let message = try XCTUnwrap(MLSMessageModel.fetchOne(db))
      XCTAssertEqual(message.messageID, projectionMessageID)
      XCTAssertEqual(message.conversationID, projectionConversationID)
      XCTAssertEqual(message.senderID, "did:plc:sender")
      XCTAssertEqual(message.decryptedPayload(context: context)?.text, "protected introduction")
      XCTAssertEqual(
        try String.fetchOne(
          db,
          sql: "SELECT history_projected_message_id FROM mls_direct_request_projection"
        ),
        projectionMessageID
      )
    }

    // A stale inventory echo cannot demote accepted consent.
    try adapter.persistDirectRequestView(pending)
    try dbPool.read { db in
      XCTAssertEqual(
        try String.fetchOne(db, sql: "SELECT consent FROM mls_direct_request_projection"),
        "accepted"
      )
    }

    let replacedMessage = requestView(
      consent: .accepted,
      stateVersion: 6,
      firstMessageID: "750e8400-e29b-41d4-a716-446655440000",
      canSend: true
    )
    XCTAssertThrowsError(try adapter.persistDirectRequestView(replacedMessage))
    try dbPool.read { db in
      XCTAssertEqual(try MLSMessageModel.fetchCount(db), 1)
      XCTAssertEqual(
        try String.fetchOne(db, sql: "SELECT first_message_id FROM mls_direct_request_projection"),
        projectionMessageID
      )
    }

    let closed = requestView(consent: .closed, stateVersion: 6)
    try adapter.persistDirectRequestView(closed)
    XCTAssertThrowsError(
      try adapter.persistDirectRequestView(
        requestView(consent: .accepted, stateVersion: 7, canSend: true)
      )
    )
    try dbPool.read { db in
      XCTAssertEqual(
        try String.fetchOne(db, sql: "SELECT consent FROM mls_direct_request_projection"),
        "closed"
      )
    }
  }

  func testFailedProjectionRollsBackAndCorrectedRetryMaterializesOnce() throws {
    let adapter = try makeProjectionAdapter(userDID: "did:plc:receiver")
    let invalid = requestView(
      consent: .accepted,
      stateVersion: 5,
      receivedAt: "not-a-canonical-timestamp",
      canSend: true
    )
    XCTAssertThrowsError(try adapter.persistDirectRequestView(invalid))
    try dbPool.read { db in
      XCTAssertEqual(try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM mls_direct_request_projection"), 0)
      XCTAssertEqual(try MLSMessageModel.fetchCount(db), 0)
    }

    let corrected = requestView(consent: .accepted, stateVersion: 5, canSend: true)
    try adapter.persistDirectRequestView(corrected)
    try adapter.persistDirectRequestView(corrected)
    try dbPool.read { db in
      XCTAssertEqual(try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM mls_direct_request_projection"), 1)
      XCTAssertEqual(try MLSMessageModel.fetchCount(db), 1)
    }
  }

  /// Rust's `materialize_accepted_request_introduction` and the host projection
  /// both write the accepted introduction; whichever lands first, the other
  /// must recognise the same message. `.001` does not survive a stored-`Date`
  /// round trip bit-exactly, which used to surface as `conflictingHistory`.
  func testRustMaterializedIntroductionSatisfiesHostProjection() throws {
    let userDID = "did:plc:receiver"
    let adapter = try makeProjectionAdapter(userDID: userDID)
    try adapter.persistDirectRequestView(
      requestView(consent: .incomingPending, stateVersion: 4, receivedAt: "2026-09-19T12:00:00.001Z"))
    try adapter.storeMessage(message: FfiMessage(
      id: projectionMessageID, conversationId: projectionConversationID,
      senderDid: "did:plc:sender", text: "protected introduction",
      timestamp: "2026-09-19T12:00:00.001+00:00", epoch: 2, sequenceNumber: 4,
      isOwn: false, deliveryStatus: nil, payloadJson: nil))
    try adapter.persistDirectRequestView(
      requestView(consent: .accepted, stateVersion: 5, receivedAt: "2026-09-19T12:00:00.001Z", canSend: true))
    try dbPool.read { db in
      XCTAssertEqual(try MLSMessageModel.fetchCount(db), 1)
    }
  }

  func testHostProjectedIntroductionAcceptsMillisecondInstantsThatDoNotRoundTrip() throws {
    let adapter = try makeProjectionAdapter(userDID: "did:plc:receiver")
    let accepted = requestView(
      consent: .accepted, stateVersion: 5, receivedAt: "2026-09-19T12:00:00.005Z", canSend: true)
    try adapter.persistDirectRequestView(accepted)
    try adapter.persistDirectRequestView(accepted)
    try dbPool.read { db in
      XCTAssertEqual(try MLSMessageModel.fetchCount(db), 1)
    }
  }

  func testMissingRequestProjectionDoesNotDefaultToLegacyOrdinaryMessaging() throws {
    let userDID = "did:plc:receiver"
    _ = try makeProjectionAdapter(userDID: userDID)
    try dbPool.read { db in
      XCTAssertEqual(
        try MLSDirectRequestProjection.classifyConversation(
          userDID: userDID,
          conversationID: projectionConversationID,
          in: db
        ),
        .unknown
      )
    }

    let adapter = MLSOrchestratorStorageAdapter(
      dbPool: dbPool,
      userDID: userDID,
      mlsContext: context
    )
    try adapter.persistDirectRequestView(requestView(consent: .incomingPending, stateVersion: 4))
    try dbPool.read { db in
      XCTAssertEqual(
        try MLSDirectRequestProjection.classifyConversation(
          userDID: userDID,
          conversationID: projectionConversationID,
          in: db
        ),
        .requestAwaitingProjection
      )
    }
  }

  private func makeProjectionAdapter(userDID: String) throws -> MLSOrchestratorStorageAdapter {
    let adapter = MLSOrchestratorStorageAdapter(
      dbPool: dbPool,
      userDID: userDID,
      mlsContext: context
    )
    try adapter.ensureConversationExists(
      userDid: userDID,
      conversationId: projectionConversationID,
      groupId: String(repeating: "01", count: 32)
    )
    return adapter
  }

  private func requestView(
    consent: RequestConsent,
    stateVersion: UInt64,
    firstMessageID: String? = nil,
    receivedAt: String = "2026-09-19T12:00:00.000Z",
    canSend: Bool = false
  ) -> DirectRequestView {
    DirectRequestView(
      introduction: RequestIntroductionMetadata(
        senderDid: "did:plc:sender",
        senderDeviceId: "550e8400-e29b-41d4-a716-446655440001",
        receivedAt: receivedAt,
        epoch: 2,
        sequenceNumber: 4
      ),
      conversationId: projectionConversationID,
      firstMessageId: firstMessageID ?? projectionMessageID,
      consent: consent,
      crypto: .ready,
      preview: .ready(text: "protected introduction", invitation: nil),
      capabilities: RequestCapabilities(
        canPreview: true,
        canAccept: consent == .incomingPending,
        canClose: consent != .closed,
        canSend: canSend,
        canEmitReceipts: canSend,
        canAdminister: canSend
      ),
      stateVersion: stateVersion,
      generation: 1
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
