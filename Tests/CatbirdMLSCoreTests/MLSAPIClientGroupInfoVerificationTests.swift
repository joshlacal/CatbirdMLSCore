import XCTest
import Petrel
@testable import CatbirdMLSCore

final class MLSAPIClientGroupInfoVerificationTests: XCTestCase {

  func testDispositionVerifiesBytesWhenEpochsMatch() {
    let disposition = MLSAPIClient.groupInfoVerificationDisposition(
      uploadedEpoch: 12,
      storedEpoch: 12
    )

    XCTAssertEqual(disposition, .verifyStoredBytes)
  }

  func testDispositionAcceptsConcurrentAdvanceWhenServerIsAhead() {
    let disposition = MLSAPIClient.groupInfoVerificationDisposition(
      uploadedEpoch: 12,
      storedEpoch: 13
    )

    XCTAssertEqual(disposition, .acceptConcurrentAdvance(serverEpoch: 13))
  }

  func testDispositionRetriesWhenVerificationReadsStaleEpoch() {
    let disposition = MLSAPIClient.groupInfoVerificationDisposition(
      uploadedEpoch: 12,
      storedEpoch: 11
    )

    XCTAssertEqual(disposition, .retryStaleRead(serverEpoch: 11))
  }

  func testConversationIdentityMatchesStableConversationIdAfterReset() {
    XCTAssertTrue(
      MLSConversationIdentity.matches(
        requestedId: "legacy-convo-id",
        conversationId: "legacy-convo-id",
        groupId: "post-reset-group-id"
      )
    )
  }

  func testConversationIdentityStillMatchesLegacyGroupId() {
    XCTAssertTrue(
      MLSConversationIdentity.matches(
        requestedId: "group-id",
        conversationId: "conversation-id",
        groupId: "group-id"
      )
    )
  }

  func testConversationIdentityRejectsUnrelatedIds() {
    XCTAssertFalse(
      MLSConversationIdentity.matches(
        requestedId: "other-id",
        conversationId: "conversation-id",
        groupId: "group-id"
      )
    )
  }
}

/// Tests for the pure `buildExternalCommitInput` helper that constructs the
/// `BlueCatbirdMlsChatCommitGroupChange.Input` payload sent over the wire by
/// `MLSAPIClient.processExternalCommit`.
///
/// Bug under fix: `processExternalCommit` (MLSAPIClient.swift:1732-1769) declares
/// `groupInfo: Data? = nil` but never forwards it into the `Input(...)` constructor
/// at lines 1744-1750. The parameter is silently dropped before reaching the wire,
/// which is the smoking gun behind the multi-device epoch-spiral pathology.
///
/// Task 4 (this file) is RED: asserts the helper's contract before it exists.
/// Task 5 will extract the helper and wire it into `processExternalCommit`.
final class MLSAPIClientBuildExternalCommitInputTests: XCTestCase {

  func testBuildExternalCommitInputForwardsGroupInfo() {
    let commit = Data(repeating: 0xAA, count: 64)
    let groupInfo = Data(repeating: 0xBB, count: 256)

    let input = MLSAPIClient.buildExternalCommitInput(
      convoId: "convo-x",
      externalCommit: commit,
      groupInfo: groupInfo,
      confirmationTag: nil,
      idempotencyKey: "test-idem-1"
    )

    XCTAssertEqual(input.action, "externalCommit")
    XCTAssertEqual(input.convoId, "convo-x")
    XCTAssertEqual(input.commit?.data, commit)
    XCTAssertEqual(
      input.groupInfo?.data, groupInfo,
      "groupInfo must be wrapped into Bytes and forwarded — the bug we're fixing")
    XCTAssertEqual(input.idempotencyKey, "test-idem-1")
    XCTAssertNil(input.confirmationTag)
  }

  func testBuildExternalCommitInputNilGroupInfoIsNil() {
    let input = MLSAPIClient.buildExternalCommitInput(
      convoId: "convo-y",
      externalCommit: Data(repeating: 0xCC, count: 64),
      groupInfo: nil,
      confirmationTag: nil,
      idempotencyKey: "test-idem-2"
    )

    XCTAssertNil(
      input.groupInfo,
      "nil groupInfo arg should result in nil Input.groupInfo (lexicon optional)")
  }

  func testBuildExternalCommitInputDecodesConfirmationTag() {
    // confirmationTag arrives as a base64-encoded String per the current public
    // API of processExternalCommit (MLSAPIClient.swift:1736). The helper must
    // decode it and forward raw Bytes through, matching the existing pattern at
    // MLSAPIClient.swift:913 (addMembers) and 1749 (processExternalCommit body).
    let rawTagBytes = Data([0xDE, 0xAD, 0xBE, 0xEF])
    let tagB64 = rawTagBytes.base64EncodedString()

    let input = MLSAPIClient.buildExternalCommitInput(
      convoId: "convo-z",
      externalCommit: Data(repeating: 0xEE, count: 64),
      groupInfo: nil,
      confirmationTag: tagB64,
      idempotencyKey: "test-idem-3"
    )

    XCTAssertEqual(
      input.confirmationTag?.data, rawTagBytes,
      "helper must base64-decode the String input and forward raw Bytes")
  }

  func testBuildExternalCommitInputNilConfirmationTagIsNil() {
    let input = MLSAPIClient.buildExternalCommitInput(
      convoId: "convo-w",
      externalCommit: Data(repeating: 0xFF, count: 64),
      groupInfo: nil,
      confirmationTag: nil,
      idempotencyKey: "test-idem-4"
    )

    XCTAssertNil(input.confirmationTag)
  }
}
