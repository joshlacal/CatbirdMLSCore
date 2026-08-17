import Foundation
import PetrelCatbird

/// The three retained audiences that make up a subscription inventory.
///
/// These are deliberately separate cursor domains. They share one retained
/// audience fence and one inventory session, but a page cursor from one domain
/// must never be sent to another domain.
internal enum MLSInventoryDomain: String, CaseIterable, Hashable, Sendable {
  case conversations
  case pendingWelcomes
  case leafRecovery
}

internal enum MLSInventorySessionError: Error, Equatable, LocalizedError {
  case sessionIncomplete
  case sessionMismatch
  case continuationChanged(MLSInventoryDomain)
  case missingContinuation(MLSInventoryDomain)
  case unexpectedContinuation(MLSInventoryDomain)
  case repeatedContinuation(MLSInventoryDomain, String)

  var errorDescription: String? {
    switch self {
    case .sessionIncomplete:
      return "The inventory session is incomplete"
    case .sessionMismatch:
      return "The inventory session or event cursor does not match"
    case let .continuationChanged(domain):
      return "The \(domain.rawValue) inventory continuation changed session state"
    case let .missingContinuation(domain):
      return "The \(domain.rawValue) inventory page omitted its required continuation cursor"
    case let .unexpectedContinuation(domain):
      return "The \(domain.rawValue) inventory page returned a continuation after its final page"
    case let .repeatedContinuation(domain, cursor):
      return "The \(domain.rawValue) inventory pagination repeated cursor \(cursor)"
    }
  }
}

/// Completion evidence retained by the client while it prepares a ticket.
/// A conversation page alone is intentionally not sufficient evidence.
internal struct MLSInventorySessionCompletion: Equatable, Sendable {
  internal let inventorySessionId: String
  internal let snapshotEventCursor: String
  internal let completedDomains: Set<MLSInventoryDomain>

  internal var isComplete: Bool {
    completedDomains == Set(MLSInventoryDomain.allCases)
  }

  internal static func requireTicketReady(
    inventorySessionId: String,
    eventCursor: String,
    completion: MLSInventorySessionCompletion?
  ) throws {
    guard let completion else {
      throw MLSInventorySessionError.sessionIncomplete
    }
    guard completion.inventorySessionId == inventorySessionId,
          completion.snapshotEventCursor == eventCursor
    else {
      throw MLSInventorySessionError.sessionMismatch
    }
    guard completion.isComplete else {
      throw MLSInventorySessionError.sessionIncomplete
    }
  }
}

/// The aggregate retained snapshot used by the WebSocket and SSE managers.
///
/// It is intentionally an internal type: callers outside Core receive the
/// generated endpoint DTOs, while the two stream managers consume one coherent
/// value that includes all three inventory domains.
internal struct MLSCanonicalInventorySnapshot {
  internal let inventorySessionId: String
  internal let snapshotEventCursor: String
  internal let snapshotExpiresAt: Date
  internal let conversationItems: [BlueCatbirdChatDefs.ConversationInventoryItem]
  internal let pendingWelcomeItems: [BlueCatbirdChatDefs.WelcomeView]
  internal let leafRecoveryItems: [BlueCatbirdChatDefs.LeafRecoveryInboxItem]

  internal var completion: MLSInventorySessionCompletion {
    MLSInventorySessionCompletion(
      inventorySessionId: inventorySessionId,
      snapshotEventCursor: snapshotEventCursor,
      completedDomains: Set(MLSInventoryDomain.allCases)
    )
  }
}

/// Fetches each retained inventory audience to completion and verifies the
/// cross-domain session fence on every response.
internal enum MLSInventorySessionAssembler {
  internal typealias ConversationFetcher = (String?) async throws
    -> BlueCatbirdChatGetConversations.Output
  internal typealias WelcomeFetcher = (String, String?) async throws
    -> BlueCatbirdChatGetPendingWelcomes.Output
  internal typealias RecoveryFetcher = (String, String?) async throws
    -> BlueCatbirdChatGetLeafRecoveryInbox.Output

  internal static func assemble(
    fetchConversations: @escaping ConversationFetcher,
    fetchPendingWelcomes: @escaping WelcomeFetcher,
    fetchLeafRecoveryInbox: @escaping RecoveryFetcher
  ) async throws -> MLSCanonicalInventorySnapshot {
    var conversationCursor: String?
    var conversationItems: [BlueCatbirdChatDefs.ConversationInventoryItem] = []
    var firstMetadata: InventoryPageMetadata?
    var seenConversationCursors: Set<String> = []

    while true {
      let page = try await fetchConversations(conversationCursor)
      let metadata = try pageMetadata(
        page.inventorySessionId,
        eventCursor: page.snapshotEventCursor,
        snapshotExpiresAt: page.snapshotExpiresAt.date,
        nextPageCursor: page.nextPageCursor,
        hasMore: page.hasMore,
        domain: .conversations
      )
      try validate(metadata, against: &firstMetadata, domain: .conversations)
      conversationItems.append(contentsOf: page.items)
      guard page.hasMore else { break }
      guard let nextCursor = page.nextPageCursor else {
        // pageMetadata already checks this, but keep the unwrap local so the
        // progress invariant remains explicit at the loop boundary.
        throw MLSInventorySessionError.missingContinuation(.conversations)
      }
      guard nextCursor != conversationCursor else {
        throw MLSInventorySessionError.repeatedContinuation(.conversations, nextCursor)
      }
      guard seenConversationCursors.insert(nextCursor).inserted else {
        throw MLSInventorySessionError.repeatedContinuation(.conversations, nextCursor)
      }
      conversationCursor = nextCursor
    }

    guard let firstMetadata else {
      // The first request is required to return a page. This is defensive in
      // case a future fetcher implementation short-circuits without a value.
      throw MLSInventorySessionError.sessionIncomplete
    }

    let sessionId = firstMetadata.sessionId

    var welcomeCursor: String?
    var pendingWelcomeItems: [BlueCatbirdChatDefs.WelcomeView] = []
    var seenWelcomeCursors: Set<String> = []
    repeat {
      let page = try await fetchPendingWelcomes(sessionId, welcomeCursor)
      let metadata = try pageMetadata(
        page.inventorySessionId,
        eventCursor: page.snapshotEventCursor,
        snapshotExpiresAt: page.snapshotExpiresAt.date,
        nextPageCursor: page.nextPageCursor,
        hasMore: page.hasMore,
        domain: .pendingWelcomes
      )
      try validate(metadata, matching: firstMetadata, domain: .pendingWelcomes)
      pendingWelcomeItems.append(contentsOf: page.items)
      if page.hasMore {
        guard let nextCursor = page.nextPageCursor else {
          throw MLSInventorySessionError.missingContinuation(.pendingWelcomes)
        }
        guard nextCursor != welcomeCursor else {
          throw MLSInventorySessionError.repeatedContinuation(.pendingWelcomes, nextCursor)
        }
        guard seenWelcomeCursors.insert(nextCursor).inserted else {
          throw MLSInventorySessionError.repeatedContinuation(.pendingWelcomes, nextCursor)
        }
        welcomeCursor = nextCursor
      } else {
        welcomeCursor = nil
      }
      if !page.hasMore { break }
    } while true

    var recoveryCursor: String?
    var leafRecoveryItems: [BlueCatbirdChatDefs.LeafRecoveryInboxItem] = []
    var seenRecoveryCursors: Set<String> = []
    repeat {
      let page = try await fetchLeafRecoveryInbox(sessionId, recoveryCursor)
      let metadata = try pageMetadata(
        page.inventorySessionId,
        eventCursor: page.snapshotEventCursor,
        snapshotExpiresAt: page.snapshotExpiresAt.date,
        nextPageCursor: page.nextPageCursor,
        hasMore: page.hasMore,
        domain: .leafRecovery
      )
      try validate(metadata, matching: firstMetadata, domain: .leafRecovery)
      leafRecoveryItems.append(contentsOf: page.items)
      if page.hasMore {
        guard let nextCursor = page.nextPageCursor else {
          throw MLSInventorySessionError.missingContinuation(.leafRecovery)
        }
        guard nextCursor != recoveryCursor else {
          throw MLSInventorySessionError.repeatedContinuation(.leafRecovery, nextCursor)
        }
        guard seenRecoveryCursors.insert(nextCursor).inserted else {
          throw MLSInventorySessionError.repeatedContinuation(.leafRecovery, nextCursor)
        }
        recoveryCursor = nextCursor
      } else {
        recoveryCursor = nil
      }
      if !page.hasMore { break }
    } while true

    return MLSCanonicalInventorySnapshot(
      inventorySessionId: firstMetadata.sessionId,
      snapshotEventCursor: firstMetadata.eventCursor,
      snapshotExpiresAt: firstMetadata.expiresAt,
      conversationItems: conversationItems,
      pendingWelcomeItems: pendingWelcomeItems,
      leafRecoveryItems: leafRecoveryItems
    )
  }

  private struct InventoryPageMetadata {
    let sessionId: String
    let eventCursor: String
    let expiresAt: Date
  }

  private static func pageMetadata(
    _ sessionId: String,
    eventCursor: String,
    snapshotExpiresAt: Date,
    nextPageCursor: String?,
    hasMore: Bool,
    domain: MLSInventoryDomain
  ) throws -> InventoryPageMetadata {
    if hasMore, nextPageCursor == nil {
      throw MLSInventorySessionError.missingContinuation(domain)
    }
    if !hasMore, nextPageCursor != nil {
      throw MLSInventorySessionError.unexpectedContinuation(domain)
    }
    return InventoryPageMetadata(
      sessionId: sessionId,
      eventCursor: eventCursor,
      expiresAt: snapshotExpiresAt
    )
  }

  private static func validate(
    _ metadata: InventoryPageMetadata,
    against first: inout InventoryPageMetadata?,
    domain: MLSInventoryDomain
  ) throws {
    guard let first else {
      first = metadata
      return
    }
    guard first.sessionId == metadata.sessionId,
          first.eventCursor == metadata.eventCursor,
          first.expiresAt == metadata.expiresAt
    else {
      throw MLSInventorySessionError.continuationChanged(domain)
    }
  }

  private static func validate(
    _ metadata: InventoryPageMetadata,
    matching first: InventoryPageMetadata,
    domain: MLSInventoryDomain
  ) throws {
    guard first.sessionId == metadata.sessionId,
          first.eventCursor == metadata.eventCursor,
          first.expiresAt == metadata.expiresAt
    else {
      throw MLSInventorySessionError.continuationChanged(domain)
    }
  }
}

/// Actions used to reconcile every item in the aggregate inventory before a
/// stream cursor is installed. The generated unions remain intact so callers
/// can route each variant to the existing conversation, Welcome, and recovery
/// managers without silently dropping tombstones or terminal recovery views.
internal struct MLSCanonicalInventoryActionSet {
  internal typealias ConversationStateHandler =
    (BlueCatbirdChatDefs.ConversationState) async throws -> Void
  internal typealias ConversationRemovalHandler =
    (BlueCatbirdChatDefs.ConversationRemovalTombstone) async throws -> Void
  internal typealias ConversationCloseHandler =
    (BlueCatbirdChatDefs.ConversationCloseTombstone) async throws -> Void
  internal typealias WelcomeHandler =
    (BlueCatbirdChatDefs.WelcomeView) async throws -> Void
  internal typealias LeafRecoveryHandler =
    (BlueCatbirdChatDefs.LeafRecoveryInboxItem) async throws -> Void

  internal var onConversationState: ConversationStateHandler?
  internal var onConversationRemoval: ConversationRemovalHandler?
  internal var onConversationClose: ConversationCloseHandler?
  internal var onPendingWelcome: WelcomeHandler?
  internal var onLeafRecovery: LeafRecoveryHandler?

  internal init(
    onConversationState: ConversationStateHandler? = nil,
    onConversationRemoval: ConversationRemovalHandler? = nil,
    onConversationClose: ConversationCloseHandler? = nil,
    onPendingWelcome: WelcomeHandler? = nil,
    onLeafRecovery: LeafRecoveryHandler? = nil
  ) {
    self.onConversationState = onConversationState
    self.onConversationRemoval = onConversationRemoval
    self.onConversationClose = onConversationClose
    self.onPendingWelcome = onPendingWelcome
    self.onLeafRecovery = onLeafRecovery
  }
}

internal enum MLSCanonicalInventoryActionMissingError: Error, Equatable, LocalizedError {
  case conversationState
  case conversationRemoval
  case conversationClose
  case pendingWelcome
  case leafRecovery
  case unsupportedLeafRecoveryItem
  case unsupportedConversationItem

  internal var actionIdentifier: String {
    switch self {
    case .conversationState:
      return "conversationState"
    case .conversationRemoval:
      return "conversationRemoval"
    case .conversationClose:
      return "conversationClose"
    case .pendingWelcome:
      return "pendingWelcome"
    case .leafRecovery:
      return "leafRecovery"
    case .unsupportedLeafRecoveryItem:
      return "unsupportedLeafRecoveryItem"
    case .unsupportedConversationItem:
      return "unsupportedConversationItem"
    }
  }

  internal var errorDescription: String? {
    switch self {
    case .conversationState:
      return "No canonical conversation-state reconciliation action is installed"
    case .conversationRemoval:
      return "No canonical conversation-removal reconciliation action is installed"
    case .conversationClose:
      return "No canonical conversation-close reconciliation action is installed"
    case .pendingWelcome:
      return "No canonical pending-Welcome reconciliation action is installed"
    case .leafRecovery:
      return "No canonical leaf-recovery reconciliation action is installed"
    case .unsupportedLeafRecoveryItem:
      return "Unsupported canonical leaf-recovery inventory item"
    case .unsupportedConversationItem:
      return "Unsupported canonical conversation inventory item"
    }
  }
}

/// Applies the aggregate in wire order. A caller must provide an action for
/// every item variant that appears; this function never filters an item out.
internal enum MLSCanonicalInventoryReconciler {
  internal static func reconcile(
    _ snapshot: MLSCanonicalInventorySnapshot,
    actions: MLSCanonicalInventoryActionSet
  ) async throws {
    for item in snapshot.conversationItems {
      switch item {
      case let .blueCatbirdChatDefsConversationInventoryState(state):
        guard let action = actions.onConversationState else {
          throw MLSCanonicalInventoryActionMissingError.conversationState
        }
        try await action(state.state)
      case let .blueCatbirdChatDefsConversationRemovalTombstone(tombstone):
        guard let action = actions.onConversationRemoval else {
          throw MLSCanonicalInventoryActionMissingError.conversationRemoval
        }
        try await action(tombstone)
      case let .blueCatbirdChatDefsConversationCloseTombstone(tombstone):
        guard let action = actions.onConversationClose else {
          throw MLSCanonicalInventoryActionMissingError.conversationClose
        }
        try await action(tombstone)
      case .unexpected:
        throw MLSCanonicalInventoryActionMissingError.unsupportedConversationItem
      }
    }

    for welcome in snapshot.pendingWelcomeItems {
      guard let action = actions.onPendingWelcome else {
        throw MLSCanonicalInventoryActionMissingError.pendingWelcome
      }
      try await action(welcome)
    }

    for recovery in snapshot.leafRecoveryItems {
      guard let action = actions.onLeafRecovery else {
        throw MLSCanonicalInventoryActionMissingError.leafRecovery
      }
      switch recovery {
      case .blueCatbirdChatDefsLeafRecoveryView,
           .blueCatbirdChatDefsRecoveryWorkPendingView,
           .blueCatbirdChatDefsRecoveryWorkCompletedByTransitionView,
           .blueCatbirdChatDefsRecoveryWorkSupersededByTransitionView,
           .blueCatbirdChatDefsRecoveryWorkSupersededByRevocationView:
        try await action(recovery)
      case .unexpected:
        throw MLSCanonicalInventoryActionMissingError.unsupportedLeafRecoveryItem
      }
    }
  }
}

/// The ticket fence is established once for a subscription attempt and is
/// deliberately retained across stream reconnects. A failed durable event must
/// replay against this same retained audience; fetching a fresh aggregate here
/// would move the ticket fence past the failed event.
internal struct MLSCanonicalSubscriptionFence: Equatable, Sendable {
  internal let inventorySessionId: String
  internal let snapshotEventCursor: String
  internal let snapshotExpiresAt: Date
}

/// A durable event that cannot be interpreted by the installed client is a
/// terminal subscription failure, not a transient reconnect condition. The
/// latch is kept by the manager across reconnects and fence expiry so a new
/// aggregate cannot silently move the ticket past the failed event.
internal enum MLSCanonicalSubscriptionTerminalFailure: Error, Equatable, Sendable {
  case unsupportedDurableEvent(typeIdentifier: String)
  case missingDurableAction(action: String)
  case missingInventoryAction(action: String)

  internal var errorDescription: String? {
    switch self {
    case let .unsupportedDurableEvent(typeIdentifier):
      return "Unsupported durable event remains blocked: \(typeIdentifier)"
    case let .missingDurableAction(action):
      return "Required durable action remains missing: \(action)"
    case let .missingInventoryAction(action):
      return "Required inventory action remains missing: \(action)"
    }
  }
}

/// The only transitions that may clear a terminal durable-event latch. A
/// network reconnect or snapshot expiry does not clear it; the caller must
/// install a supported client/action-table transition explicitly.
internal enum MLSCanonicalSubscriptionRecoveryTransition: Equatable, Sendable {
  case supportedClientRecovery
  case actionTableReplaced
}

internal struct MLSCanonicalSubscriptionFailureLatch: Equatable, Sendable {
  internal private(set) var terminalFailure: MLSCanonicalSubscriptionTerminalFailure?

  @discardableResult
  internal mutating func record(_ error: Error) -> Bool {
    let classified: MLSCanonicalSubscriptionTerminalFailure?
    if let unsupported = error as? MLSUnsupportedDurableEventError {
      classified = .unsupportedDurableEvent(typeIdentifier: unsupported.typeIdentifier)
    } else if let missing = error as? MLSCanonicalActionMissingError {
      classified = .missingDurableAction(action: missing.actionIdentifier)
    } else if let missing = error as? MLSCanonicalInventoryActionMissingError {
      classified = .missingInventoryAction(action: missing.actionIdentifier)
    } else {
      classified = nil
    }

    guard let classified else { return false }
    // Preserve the first failed fence. A later error must not replace the
    // event that is awaiting a supported client transition.
    if terminalFailure == nil {
      terminalFailure = classified
    }
    return true
  }

  internal mutating func clear(after transition: MLSCanonicalSubscriptionRecoveryTransition) {
    switch transition {
    case .supportedClientRecovery, .actionTableReplaced:
      terminalFailure = nil
    }
  }
}

internal enum MLSCanonicalSubscriptionCoordinatorError: Error, Equatable, LocalizedError {
  case blocked(MLSCanonicalSubscriptionTerminalFailure)

  internal var errorDescription: String? {
    switch self {
    case let .blocked(failure):
      return failure.errorDescription
    }
  }
}

internal enum MLSCanonicalSubscriptionCoordinator {
  internal static func prepare(
    fence: inout MLSCanonicalSubscriptionFence?,
    initialCursor: String?,
    terminalFailure: MLSCanonicalSubscriptionTerminalFailure? = nil,
    fetchInventory: @escaping () async throws -> MLSCanonicalInventorySnapshot,
    reconcile: @escaping (MLSCanonicalInventorySnapshot) async throws -> Void,
    installCompletion: @escaping (MLSCanonicalInventorySnapshot) -> Void,
    persistFence: @escaping (String) async throws -> Void
  ) async throws -> MLSCanonicalSubscriptionFence {
    if let currentFence = fence {
      if Date() < currentFence.snapshotExpiresAt {
        // A terminal event is replayed against the retained ticket fence
        // while it remains valid. This is the same-fence reconnect path; it
        // must not fetch or install a newer aggregate.
        return currentFence
      }
      if let terminalFailure {
        // Once the retained fence expires, a terminal failure blocks before
        // the expiry branch can discard it or fetch a newer aggregate.
        throw MLSCanonicalSubscriptionCoordinatorError.blocked(terminalFailure)
      }
      // An expired retained audience is explicit unsupported-state recovery:
      // it is the one case where a new aggregate is required. Event-handler
      // failures and unknown payloads never reach this branch while the
      // original fence is still valid.
      fence = nil
    }

    if let terminalFailure {
      // A failed inventory reconciliation has no installed fence. It still
      // cannot authorize a fresh aggregate until an explicit recovery
      // transition clears the latch.
      throw MLSCanonicalSubscriptionCoordinatorError.blocked(terminalFailure)
    }

    let snapshot = try await fetchInventory()
    // Concrete inventory actions must complete before either the cursor or the
    // ticket evidence is installed. A failed action leaves the coordinator
    // without a fence, so the caller can retry the same setup explicitly.
    try await reconcile(snapshot)
    if initialCursor != snapshot.snapshotEventCursor {
      try await persistFence(snapshot.snapshotEventCursor)
    }
    installCompletion(snapshot)

    let prepared = MLSCanonicalSubscriptionFence(
      inventorySessionId: snapshot.inventorySessionId,
      snapshotEventCursor: snapshot.snapshotEventCursor,
      snapshotExpiresAt: snapshot.snapshotExpiresAt
    )
    fence = prepared
    return prepared
  }
}
