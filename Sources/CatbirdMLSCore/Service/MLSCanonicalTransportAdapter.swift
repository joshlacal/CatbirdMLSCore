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

  /// Strongly typed action table for the generated durable union. Stream
  /// managers may adapt these actions to existing conversation, Welcome,
  /// reset, recovery, leave, access, and watermark handlers. Missing actions
  /// are failures, never successful no-ops.
  public struct MLSCanonicalDurableEventActions {
    public typealias ConversationChangedHandler =
      (BlueCatbirdChatDefs.ConversationChangedEvent) async throws -> Void
    public typealias ConversationClosedHandler =
      (BlueCatbirdChatDefs.ConversationClosedEvent) async throws -> Void
    public typealias MessageAvailableHandler =
      (
        BlueCatbirdChatDefs.MessageAvailableEvent,
        String,
        [BlueCatbirdMlsChatDefs.MessageView]
      ) async throws -> Void
    public typealias WelcomeAvailableHandler =
      (BlueCatbirdChatDefs.WelcomeAvailableEvent) async throws -> Void
    public typealias WelcomeDispositionHandler =
      (BlueCatbirdChatDefs.WelcomeDispositionEvent) async throws -> Void
    public typealias ResetRequestedHandler =
      (BlueCatbirdChatDefs.ResetRequestedEvent) async throws -> Void
    public typealias LeafRecoveryHandler =
      (BlueCatbirdChatDefs.LeafRecoveryEvent) async throws -> Void
    public typealias LeaveRequestHandler =
      (BlueCatbirdChatDefs.LeaveRequestEvent) async throws -> Void
    public typealias AccessEndedHandler =
      (BlueCatbirdChatDefs.AccessEndedEvent) async throws -> Void
    public typealias WatermarkHandler =
      (BlueCatbirdChatDefs.WatermarkEvent) async throws -> Void
    public typealias TypingHandler =
      (BlueCatbirdChatDefs.TypingEvent) async throws -> Void

    public var onConversationChanged: ConversationChangedHandler?
    public var onConversationClosed: ConversationClosedHandler?
    public var onMessageAvailable: MessageAvailableHandler?
    public var onWelcomeAvailable: WelcomeAvailableHandler?
    public var onWelcomeDisposition: WelcomeDispositionHandler?
    public var onResetRequested: ResetRequestedHandler?
    public var onLeafRecovery: LeafRecoveryHandler?
    public var onLeaveRequest: LeaveRequestHandler?
    public var onAccessEnded: AccessEndedHandler?
    public var onWatermark: WatermarkHandler?
    public var onTyping: TypingHandler?

    public init(
      onConversationChanged: ConversationChangedHandler? = nil,
      onConversationClosed: ConversationClosedHandler? = nil,
      onMessageAvailable: MessageAvailableHandler? = nil,
      onWelcomeAvailable: WelcomeAvailableHandler? = nil,
      onWelcomeDisposition: WelcomeDispositionHandler? = nil,
      onResetRequested: ResetRequestedHandler? = nil,
      onLeafRecovery: LeafRecoveryHandler? = nil,
      onLeaveRequest: LeaveRequestHandler? = nil,
      onAccessEnded: AccessEndedHandler? = nil,
      onWatermark: WatermarkHandler? = nil,
      onTyping: TypingHandler? = nil
    ) {
      self.onConversationChanged = onConversationChanged
      self.onConversationClosed = onConversationClosed
      self.onMessageAvailable = onMessageAvailable
      self.onWelcomeAvailable = onWelcomeAvailable
      self.onWelcomeDisposition = onWelcomeDisposition
      self.onResetRequested = onResetRequested
      self.onLeafRecovery = onLeafRecovery
      self.onLeaveRequest = onLeaveRequest
      self.onAccessEnded = onAccessEnded
      self.onWatermark = onWatermark
      self.onTyping = onTyping
    }

    /// A canonical subscription may start only when every generated event arm
    /// has a concrete throwing action. Typing is included in this check even
    /// though it does not advance the durable cursor, so no generated union
    /// arm can be silently dropped.
    internal var hasCompleteRequiredActions: Bool {
      onConversationChanged != nil &&
        onConversationClosed != nil &&
        onMessageAvailable != nil &&
        onWelcomeAvailable != nil &&
        onWelcomeDisposition != nil &&
        onResetRequested != nil &&
        onLeafRecovery != nil &&
        onLeaveRequest != nil &&
        onAccessEnded != nil &&
        onWatermark != nil &&
        onTyping != nil
    }

    /// Capability identity is Core-owned and compiled from the generated
    /// union revision plus action-table completeness. There is no caller
    /// supplied revision string that can authorize recovery.
    internal var capabilityIdentity: MLSCanonicalSubscriptionCapability? {
      guard hasCompleteRequiredActions else { return nil }
      return .current
    }

    internal func dispatch(_ event: MLSCanonicalDurableEvent) async throws {
      switch event {
      case let .typing(typing):
        guard let onTyping else { throw MLSCanonicalActionMissingError.typing }
        try await onTyping(typing)
      case let .messageAvailable(available, cursor, messages):
        guard let onMessageAvailable else {
          throw MLSCanonicalActionMissingError.messageAvailable
        }
        try await onMessageAvailable(available, cursor, messages)
      case let .conversationChanged(changed):
        guard let onConversationChanged else {
          throw MLSCanonicalActionMissingError.conversationChanged
        }
        try await onConversationChanged(changed)
      case let .conversationClosed(closed):
        guard let onConversationClosed else {
          throw MLSCanonicalActionMissingError.conversationClosed
        }
        try await onConversationClosed(closed)
      case let .welcomeAvailable(available):
        guard let onWelcomeAvailable else {
          throw MLSCanonicalActionMissingError.welcomeAvailable
        }
        try await onWelcomeAvailable(available)
      case let .welcomeDisposition(disposition):
        guard let onWelcomeDisposition else {
          throw MLSCanonicalActionMissingError.welcomeDisposition
        }
        try await onWelcomeDisposition(disposition)
      case let .resetRequested(reset):
        guard let onResetRequested else {
          throw MLSCanonicalActionMissingError.resetRequested
        }
        try await onResetRequested(reset)
      case let .leafRecovery(recovery):
        guard let onLeafRecovery else {
          throw MLSCanonicalActionMissingError.leafRecovery
        }
        try await onLeafRecovery(recovery)
      case let .leaveRequest(leave):
        guard let onLeaveRequest else {
          throw MLSCanonicalActionMissingError.leaveRequest
        }
        try await onLeaveRequest(leave)
      case let .accessEnded(accessEnded):
        guard let onAccessEnded else {
          throw MLSCanonicalActionMissingError.accessEnded
        }
        try await onAccessEnded(accessEnded)
      case let .watermark(watermark):
        guard let onWatermark else {
          throw MLSCanonicalActionMissingError.watermark
        }
        try await onWatermark(watermark)
      }
    }
  }

  internal enum MLSCanonicalActionMissingError: Error, LocalizedError, Equatable {
    case typing
    case messageAvailable
    case conversationChanged
    case conversationClosed
    case welcomeAvailable
    case welcomeDisposition
    case resetRequested
    case leafRecovery
    case leaveRequest
    case accessEnded
    case watermark

    internal var actionIdentifier: String {
      switch self {
      case .typing:
        return "typing"
      case .messageAvailable:
        return "messageAvailable"
      case .conversationChanged:
        return "conversationChanged"
      case .conversationClosed:
        return "conversationClosed"
      case .welcomeAvailable:
        return "welcomeAvailable"
      case .welcomeDisposition:
        return "welcomeDisposition"
      case .resetRequested:
        return "resetRequested"
      case .leafRecovery:
        return "leafRecovery"
      case .leaveRequest:
        return "leaveRequest"
      case .accessEnded:
        return "accessEnded"
      case .watermark:
        return "watermark"
      }
    }

    internal var errorDescription: String? {
      switch self {
      case .typing: return "No canonical typing action is installed"
      case .messageAvailable: return "No canonical messageAvailable action is installed"
      case .conversationChanged: return "No canonical conversationChanged action is installed"
      case .conversationClosed: return "No canonical conversationClosed action is installed"
      case .welcomeAvailable: return "No canonical welcomeAvailable action is installed"
      case .welcomeDisposition: return "No canonical welcomeDisposition action is installed"
      case .resetRequested: return "No canonical resetRequested action is installed"
      case .leafRecovery: return "No canonical leafRecovery action is installed"
      case .leaveRequest: return "No canonical leaveRequest action is installed"
      case .accessEnded: return "No canonical accessEnded action is installed"
      case .watermark: return "No canonical watermark action is installed"
      }
    }
  }

  internal enum MLSCanonicalTypingProjectionError: Error, LocalizedError, Equatable {
    case invalidIdentity

    internal var errorDescription: String? {
      "Canonical typing actor DID could not be projected to the legacy callback"
    }
  }

  internal enum MLSCanonicalCursorError: Error, LocalizedError, Equatable {
    case previousCursorMismatch(expected: String, actual: String)
    case cursorDidNotAdvance(String)

    internal var errorDescription: String? {
      switch self {
      case let .previousCursorMismatch(expected, actual):
        return "Canonical event previous cursor \(actual) does not match expected fence \(expected)"
      case let .cursorDidNotAdvance(cursor):
        return "Canonical event cursor did not advance: \(cursor)"
      }
    }
  }

  internal struct MLSCanonicalMessageAvailabilityError: Error, LocalizedError, Equatable {
    internal let conversationId: String
    internal let sequence: Int

    internal var errorDescription: String? {
      "messageAvailable seq \(sequence) for \(conversationId) was not fetched and projected"
    }
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

  /// Outcome of consuming one inner transport stream. Returning from this
  /// helper releases the stream iterator before a manager starts its next
  /// ticketed attempt, so a failed handler cannot leave a producer running
  /// while a later cursor is persisted.
  internal enum MLSCanonicalStreamLoopOutcome {
    case ended(eventCount: Int)
    case reconnect(Error, eventCount: Int)
    case stopped(eventCount: Int)
  }

  /// Decides whether a message belongs to the already-committed replay prefix
  /// of a stable subscription fence. The server ticket starts at the snapshot
  /// cursor on every reconnect, while the local cursor may already be farther
  /// along that same stream. Known envelopes in that prefix are skipped after
  /// validating their cursor chain; they must not be sent through `saveCursor`
  /// again, which would regress durable storage. Unknown envelopes are never
  /// skippable because a prior run could not have committed past them.
  internal enum MLSCanonicalReplayDecision {
    case handle(expectedPreviousCursor: String)
    case skip
    case reconnect(Error)
  }

  internal struct MLSCanonicalReplayGate {
    private(set) var scanCursor: String
    private(set) var targetCursor: String?

    internal init(snapshotCursor: String, savedCursor: String?) {
      self.scanCursor = snapshotCursor
      self.targetCursor = savedCursor.flatMap { $0 == snapshotCursor ? nil : $0 }
    }

    internal mutating func decide(
      _ message: BlueCatbirdChatSubscribeEvents.Message
    ) -> MLSCanonicalReplayDecision {
      guard targetCursor != nil else {
        return .handle(expectedPreviousCursor: scanCursor)
      }

      // Typing is deliberately unsequenced and can be delivered while the
      // durable replay prefix is being walked. It still requires its typed
      // action, but it does not move the durable scan cursor.
      if case .blueCatbirdChatDefsTypingEvent(_) = message {
        return .handle(expectedPreviousCursor: scanCursor)
      }

      guard case let .blueCatbirdChatDefsEventEnvelope(envelope) = message else {
        return .handle(expectedPreviousCursor: scanCursor)
      }

      // An unknown payload is the first uncommitted event by definition. Let
      // the canonical handler reject it and reconnect from the same cursor.
      if case .unexpected = envelope.payload {
        return .handle(expectedPreviousCursor: scanCursor)
      }

      guard envelope.previousCursor == scanCursor else {
        return .reconnect(
          MLSCanonicalCursorError.previousCursorMismatch(
            expected: scanCursor,
            actual: envelope.previousCursor
          )
        )
      }
      guard envelope.cursor != envelope.previousCursor else {
        return .reconnect(MLSCanonicalCursorError.cursorDidNotAdvance(envelope.cursor))
      }

      scanCursor = envelope.cursor
      if envelope.cursor == targetCursor {
        targetCursor = nil
      }
      return .skip
    }
  }

  internal static func consumeCanonicalStream(
    _ stream: AsyncThrowingStream<BlueCatbirdChatSubscribeEvents.Message, Error>,
    shouldStop: @escaping () async -> Bool,
    handle: @escaping (BlueCatbirdChatSubscribeEvents.Message) async
      -> MLSCanonicalStreamHandlingResult
  ) async throws -> MLSCanonicalStreamLoopOutcome {
    var eventCount = 0
    for try await message in stream {
      let stopRequested = await shouldStop()
      if Task.isCancelled || stopRequested {
        return .stopped(eventCount: eventCount)
      }
      eventCount += 1
      let result = await handle(message)
      if case let .reconnect(error) = result {
        return .reconnect(error, eventCount: eventCount)
      }
    }
    return .ended(eventCount: eventCount)
  }

  /// Persist a durable cursor before the manager updates its in-memory fence.
  /// Keeping this ordering in one Core seam makes cursor-store failures
  /// observable to the stream loop instead of turning them into background
  /// best-effort writes.
  internal static func persistCanonicalCursor(
    _ cursor: String,
    for conversationId: String,
    store: MLSEventCursorStore?
  ) async throws {
    guard let store else {
      throw MLSCanonicalSubscriptionFailureConfigurationError.missingStorage
    }
    try await MainActor.run {
      try store.updateCursor(for: conversationId, cursor: cursor)
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
    expectedPreviousCursor: String? = nil,
    loadEntries: @escaping (String, Int) async throws -> [BlueCatbirdChatDefs.ConversationEntry],
    onDurableEvent: @escaping (MLSCanonicalDurableEvent) async throws -> Void,
    saveCursor: @escaping (String) async throws -> Void
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

    if let expectedPreviousCursor,
       envelope.previousCursor != expectedPreviousCursor
    {
      return .reconnect(
        MLSCanonicalCursorError.previousCursorMismatch(
          expected: expectedPreviousCursor,
          actual: envelope.previousCursor
        )
      )
    }
    guard envelope.cursor != envelope.previousCursor else {
      return .reconnect(MLSCanonicalCursorError.cursorDidNotAdvance(envelope.cursor))
    }

    let event: MLSCanonicalDurableEvent
    switch envelope.payload {
    case let .blueCatbirdChatDefsMessageAvailableEvent(available):
      let conversationID = String(describing: available.conversationId)
      guard subscriptionKey == "__global__" || subscriptionKey == conversationID else {
        do {
          try await saveCursor(envelope.cursor)
        } catch {
          return .reconnect(error)
        }
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
        guard messages.contains(where: { $0.seq == available.seq }) else {
          return .reconnect(
            MLSCanonicalMessageAvailabilityError(
              conversationId: conversationID,
              sequence: available.seq
            )
          )
        }
        event = .messageAvailable(available, cursor: envelope.cursor, messages: messages)
      } catch {
        return .reconnect(error)
      }

    case let .blueCatbirdChatDefsConversationChangedEvent(changed):
      guard subscriptionKey == "__global__"
        || String(describing: changed.conversationId) == subscriptionKey
      else {
        do {
          try await saveCursor(envelope.cursor)
        } catch {
          return .reconnect(error)
        }
        return .handled
      }
      event = .conversationChanged(changed)

    case let .blueCatbirdChatDefsConversationClosedEvent(closed):
      guard subscriptionKey == "__global__"
        || String(describing: closed.conversationId) == subscriptionKey
      else {
        do {
          try await saveCursor(envelope.cursor)
        } catch {
          return .reconnect(error)
        }
        return .handled
      }
      event = .conversationClosed(closed)

    case let .blueCatbirdChatDefsWelcomeAvailableEvent(available):
      guard subscriptionKey == "__global__"
        || String(describing: available.conversationId) == subscriptionKey
      else {
        do {
          try await saveCursor(envelope.cursor)
        } catch {
          return .reconnect(error)
        }
        return .handled
      }
      event = .welcomeAvailable(available)

    case let .blueCatbirdChatDefsWelcomeDispositionEvent(disposition):
      event = .welcomeDisposition(disposition)

    case let .blueCatbirdChatDefsResetRequestedEvent(reset):
      guard subscriptionKey == "__global__"
        || String(describing: reset.conversationId) == subscriptionKey
      else {
        do {
          try await saveCursor(envelope.cursor)
        } catch {
          return .reconnect(error)
        }
        return .handled
      }
      event = .resetRequested(reset)

    case let .blueCatbirdChatDefsLeafRecoveryEvent(recovery):
      guard subscriptionKey == "__global__"
        || String(describing: recovery.conversationId) == subscriptionKey
      else {
        do {
          try await saveCursor(envelope.cursor)
        } catch {
          return .reconnect(error)
        }
        return .handled
      }
      event = .leafRecovery(recovery)

    case let .blueCatbirdChatDefsLeaveRequestEvent(leave):
      guard subscriptionKey == "__global__"
        || String(describing: leave.conversationId) == subscriptionKey
      else {
        do {
          try await saveCursor(envelope.cursor)
        } catch {
          return .reconnect(error)
        }
        return .handled
      }
      event = .leaveRequest(leave)

    case let .blueCatbirdChatDefsAccessEndedEvent(accessEnded):
      guard subscriptionKey == "__global__"
        || String(describing: accessEnded.conversationId) == subscriptionKey
      else {
        do {
          try await saveCursor(envelope.cursor)
        } catch {
          return .reconnect(error)
        }
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
    do {
      try await saveCursor(envelope.cursor)
    } catch {
      return .reconnect(error)
    }
    return .handled
  }

  /// Shared canonical stream-handler seam used by both WebSocket and SSE.
  /// This compatibility overload accepts a throwing message reconciliation
  /// action; canonical durable messages never use a fire-and-forget callback.
  internal static func handleCanonicalStreamMessage(
    _ message: BlueCatbirdChatSubscribeEvents.Message,
    subscriptionKey: String,
    loadEntries: @escaping (String, Int) async throws -> [BlueCatbirdChatDefs.ConversationEntry],
    onMessage: @escaping (BlueCatbirdMlsChatSubscribeEvents.MessageEvent) async throws -> Void,
    onError: @escaping (Error) async -> Void,
    saveCursor: @escaping (String) async throws -> Void
  ) async {
    let result = await handleCanonicalStreamMessage(
      message,
      subscriptionKey: subscriptionKey,
      loadEntries: loadEntries,
      onDurableEvent: { event in
        let actions = MLSCanonicalDurableEventActions(
          onMessageAvailable: { _, cursor, messages in
            for message in messages {
              try await onMessage(
                BlueCatbirdMlsChatSubscribeEvents.MessageEvent(
                  cursor: cursor,
                  message: message,
                  ephemeral: nil,
                  epoch: message.epoch
                )
              )
            }
          }
        )
        try await actions.dispatch(event)
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
internal typealias MLSCanonicalDurableEventActions =
  MLSCanonicalTransportAdapter.MLSCanonicalDurableEventActions
internal typealias MLSCanonicalActionMissingError =
  MLSCanonicalTransportAdapter.MLSCanonicalActionMissingError
internal typealias MLSCanonicalCursorError =
  MLSCanonicalTransportAdapter.MLSCanonicalCursorError
internal typealias MLSCanonicalMessageAvailabilityError =
  MLSCanonicalTransportAdapter.MLSCanonicalMessageAvailabilityError
internal typealias MLSCanonicalTypingProjectionError =
  MLSCanonicalTransportAdapter.MLSCanonicalTypingProjectionError
internal typealias MLSCanonicalReplayGate =
  MLSCanonicalTransportAdapter.MLSCanonicalReplayGate
