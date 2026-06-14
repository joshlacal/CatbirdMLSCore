import XCTest
@testable import CatbirdMLSCore

final class MLSSyncCatchupPolicyTests: XCTestCase {
  func testIncrementalSyncCatchesUpExistingConversationMessages() {
    let localLastMessageAt = Date(timeIntervalSince1970: 100)
    let serverLastMessageAt = Date(timeIntervalSince1970: 101)

    XCTAssertTrue(
      MLSConversationManager.shouldCatchUpMessagesDuringSync(
        needsGroupInit: false,
        fullSync: false,
        serverLastMessageAt: serverLastMessageAt,
        localLastMessageAt: localLastMessageAt
      )
    )
  }

  func testIncrementalSyncSkipsExistingConversationWithoutNewMessages() {
    let lastMessageAt = Date(timeIntervalSince1970: 100)

    XCTAssertFalse(
      MLSConversationManager.shouldCatchUpMessagesDuringSync(
        needsGroupInit: false,
        fullSync: false,
        serverLastMessageAt: lastMessageAt,
        localLastMessageAt: lastMessageAt
      )
    )
  }

  func testIncrementalSyncCatchesUpWhenServerHasFirstMessage() {
    XCTAssertTrue(
      MLSConversationManager.shouldCatchUpMessagesDuringSync(
        needsGroupInit: false,
        fullSync: false,
        serverLastMessageAt: Date(timeIntervalSince1970: 100),
        localLastMessageAt: nil
      )
    )
  }

  func testIncrementalSyncCatchesUpWhenServerOmitsFreshnessSignal() {
    XCTAssertTrue(
      MLSConversationManager.shouldCatchUpMessagesDuringSync(
        needsGroupInit: false,
        fullSync: false,
        serverLastMessageAt: nil,
        localLastMessageAt: Date(timeIntervalSince1970: 100)
      )
    )
  }

  func testFullSyncStillCatchesUpMessages() {
    XCTAssertTrue(
      MLSConversationManager.shouldCatchUpMessagesDuringSync(
        needsGroupInit: false,
        fullSync: true,
        serverLastMessageAt: nil,
        localLastMessageAt: nil
      )
    )
  }

  func testNewGroupInitStillCatchesUpMessages() {
    XCTAssertTrue(
      MLSConversationManager.shouldCatchUpMessagesDuringSync(
        needsGroupInit: true,
        fullSync: false,
        serverLastMessageAt: nil,
        localLastMessageAt: nil
      )
    )
  }

  func testCatchupCursorUsesCachedMaxWhenSequenceStateMissing() {
    XCTAssertEqual(
      MLSConversationManager.resolvedCatchupSinceSequence(
        sequenceStateSeq: nil,
        cachedMaxSeq: 19
      ),
      19
    )
  }

  func testCatchupCursorUsesCachedMaxWhenSequenceStateIsZero() {
    XCTAssertEqual(
      MLSConversationManager.resolvedCatchupSinceSequence(
        sequenceStateSeq: 0,
        cachedMaxSeq: 34
      ),
      34
    )
  }

  func testCatchupCursorKeepsValidSequenceState() {
    XCTAssertEqual(
      MLSConversationManager.resolvedCatchupSinceSequence(
        sequenceStateSeq: 12,
        cachedMaxSeq: 34
      ),
      12
    )
  }

  func testCatchupCursorAllowsInitialFullFetchWhenNoCachedMessages() {
    XCTAssertNil(
      MLSConversationManager.resolvedCatchupSinceSequence(
        sequenceStateSeq: nil,
        cachedMaxSeq: nil
      )
    )
  }

  func testBootstrapDisambiguationTreatsHTTP404MissingWelcomeAsNoWelcome() {
    XCTAssertTrue(
      MLSConversationManager.isMissingWelcomeForBootstrapDisambiguation(
        MLSAPIError.httpError(statusCode: 404, message: "No welcome message available")
      )
    )

    XCTAssertFalse(
      MLSConversationManager.isMissingWelcomeForBootstrapDisambiguation(
        MLSAPIError.httpError(statusCode: 500, message: "server unavailable")
      )
    )
  }
}
