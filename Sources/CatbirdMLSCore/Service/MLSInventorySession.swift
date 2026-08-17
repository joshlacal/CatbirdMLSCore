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
      conversationCursor = page.nextPageCursor
    }

    guard let firstMetadata else {
      // The first request is required to return a page. This is defensive in
      // case a future fetcher implementation short-circuits without a value.
      throw MLSInventorySessionError.sessionIncomplete
    }

    let sessionId = firstMetadata.sessionId

    var welcomeCursor: String?
    var pendingWelcomeItems: [BlueCatbirdChatDefs.WelcomeView] = []
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
        welcomeCursor = page.nextPageCursor
      } else {
        welcomeCursor = nil
      }
      if !page.hasMore { break }
    } while true

    var recoveryCursor: String?
    var leafRecoveryItems: [BlueCatbirdChatDefs.LeafRecoveryInboxItem] = []
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
        recoveryCursor = page.nextPageCursor
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
