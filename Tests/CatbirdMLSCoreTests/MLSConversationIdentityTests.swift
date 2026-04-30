import XCTest
@testable import CatbirdMLSCore

final class MLSConversationIdentityTests: XCTestCase {
  func testProcessingGroupIdPrefersStoredGroupIdOverConversationId() {
    let conversationID = "4b2cdbaad35a9d1376213419413ba529"
    let groupID = "b947c701d4a849b986b5730b12540f87"

    XCTAssertEqual(
      MLSConversationIdentity.processingGroupIdHex(
        conversationID: conversationID,
        groupId: groupID
      ),
      groupID
    )
  }

  func testProcessingGroupIdFallsBackToConversationIdWhenStoredGroupIdMissing() {
    let conversationID = "4b2cdbaad35a9d1376213419413ba529"

    XCTAssertEqual(
      MLSConversationIdentity.processingGroupIdHex(
        conversationID: conversationID,
        groupId: nil
      ),
      conversationID
    )
  }

  func testMessagesUpdatedEventDescriptionIncludesConversationAndCount() {
    let event = MLSStateEvent.messagesUpdated(
      convoId: "4b2cdbaad35a9d1376213419413ba529",
      count: 3
    )

    XCTAssertEqual(
      event.description,
      "Messages updated in 4b2cdbaad35a9d1376213419413ba529: 3"
    )
  }
}
