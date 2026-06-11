import XCTest
import Petrel
import PetrelCatbird
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

/// Tests for the pure `buildAddMembersInput` helper that constructs the
/// `BlueCatbirdMlsChatCommitGroupChange.Input` payload sent over the wire by
/// `MLSAPIClient.addMembers`.
///
/// Bug under fix: `addMembers` (MLSAPIClient.swift:887-931) does not accept a
/// `groupInfo: Data?` parameter at all and silently builds an `Input` without
/// it. Same shape as the `processExternalCommit` bug fixed in Task 5; the
/// lexicon `Input.groupInfo` field has been there all along, only
/// `updateGroupInfo` was forwarding it. This task adds the parameter and
/// routes construction through a static helper so the contract is testable.
final class MLSAPIClientBuildAddMembersInputTests: XCTestCase {

  func testBuildAddMembersInputForwardsGroupInfo() throws {
    let commit = Data(repeating: 0xAA, count: 64)
    let welcome = Data(repeating: 0xBB, count: 96)
    let groupInfo = Data(repeating: 0xCC, count: 256)
    let dids = [try DID(didString: "did:plc:alice")]

    let input = MLSAPIClient.buildAddMembersInput(
      convoId: "convo-x",
      didList: dids,
      commit: commit,
      welcomeMessage: welcome,
      groupInfo: groupInfo,
      keyPackageHashes: nil,
      confirmationTag: nil,
      idempotencyKey: "test-idem-1"
    )

    XCTAssertEqual(input.action, "addMembers")
    XCTAssertEqual(input.convoId, "convo-x")
    XCTAssertEqual(input.memberDids, dids)
    XCTAssertEqual(input.commit?.data, commit)
    XCTAssertEqual(input.welcome?.data, welcome)
    XCTAssertEqual(
      input.groupInfo?.data, groupInfo,
      "groupInfo must be wrapped into Bytes and forwarded — the bug we're fixing")
    XCTAssertEqual(input.idempotencyKey, "test-idem-1")
    XCTAssertNil(input.confirmationTag)
    XCTAssertNil(input.keyPackageHashes)
  }

  func testBuildAddMembersInputNilGroupInfoIsNil() throws {
    let input = MLSAPIClient.buildAddMembersInput(
      convoId: "convo-y",
      didList: [try DID(didString: "did:plc:bob")],
      commit: nil,
      welcomeMessage: nil,
      groupInfo: nil,
      keyPackageHashes: nil,
      confirmationTag: nil,
      idempotencyKey: "test-idem-2"
    )
    XCTAssertNil(input.groupInfo)
  }

  func testBuildAddMembersInputDecodesConfirmationTag() throws {
    let rawTagBytes = Data([0xDE, 0xAD, 0xBE, 0xEF])
    let tagB64 = rawTagBytes.base64EncodedString()

    let input = MLSAPIClient.buildAddMembersInput(
      convoId: "convo-z",
      didList: [try DID(didString: "did:plc:carol")],
      commit: nil,
      welcomeMessage: nil,
      groupInfo: nil,
      keyPackageHashes: nil,
      confirmationTag: tagB64,
      idempotencyKey: "test-idem-3"
    )

    XCTAssertEqual(
      input.confirmationTag?.data, rawTagBytes,
      "helper must base64-decode the String input and forward raw Bytes")
  }

  func testBuildAddMembersInputForwardsKeyPackageHashes() throws {
    // The plan emphasizes per-device routing via key_package_hashes (out of
    // scope for this task, but the helper must forward them — they're the
    // payload that future Plans B/C may need to thread through).
    let hashEntry = BlueCatbirdMlsChatCommitGroupChange.KeyPackageHashEntry(
      did: try DID(didString: "did:plc:carol"),
      hash: "abc123"
    )
    let input = MLSAPIClient.buildAddMembersInput(
      convoId: "convo-w",
      didList: [try DID(didString: "did:plc:carol")],
      commit: nil,
      welcomeMessage: nil,
      groupInfo: nil,
      keyPackageHashes: [hashEntry],
      confirmationTag: nil,
      idempotencyKey: "test-idem-4"
    )

    XCTAssertEqual(input.keyPackageHashes?.count, 1)
    XCTAssertEqual(input.keyPackageHashes?.first?.hash, "abc123")
  }
}

final class MLSAPIClientBuildRemoveMemberInputTests: XCTestCase {

  func testBuildRemoveMemberInputForwardsGroupInfo() throws {
    let commit = Data(repeating: 0xAA, count: 64)
    let groupInfo = Data(repeating: 0xCC, count: 256)
    let dids = [try DID(didString: "did:plc:alice")]

    let input = MLSAPIClient.buildRemoveMemberInput(
      convoId: "convo-x",
      didList: dids,
      commit: commit,
      groupInfo: groupInfo,
      confirmationTag: nil,
      idempotencyKey: "test-idem-1"
    )

    XCTAssertEqual(input.action, "removeMember")
    XCTAssertEqual(input.convoId, "convo-x")
    XCTAssertEqual(input.memberDids, dids)
    XCTAssertEqual(input.commit?.data, commit)
    XCTAssertEqual(
      input.groupInfo?.data, groupInfo,
      "groupInfo must be wrapped into Bytes and forwarded — the bug we're fixing")
    XCTAssertEqual(input.idempotencyKey, "test-idem-1")
    XCTAssertNil(input.confirmationTag)
  }

  func testBuildRemoveMemberInputNilGroupInfoIsNil() throws {
    let input = MLSAPIClient.buildRemoveMemberInput(
      convoId: "convo-y",
      didList: [try DID(didString: "did:plc:bob")],
      commit: nil,
      groupInfo: nil,
      confirmationTag: nil,
      idempotencyKey: "test-idem-2"
    )
    XCTAssertNil(input.groupInfo)
  }

  func testBuildRemoveMemberInputDecodesConfirmationTag() throws {
    let rawTagBytes = Data([0xDE, 0xAD, 0xBE, 0xEF])
    let tagB64 = rawTagBytes.base64EncodedString()

    let input = MLSAPIClient.buildRemoveMemberInput(
      convoId: "convo-z",
      didList: [try DID(didString: "did:plc:carol")],
      commit: nil,
      groupInfo: nil,
      confirmationTag: tagB64,
      idempotencyKey: "test-idem-3"
    )

    XCTAssertEqual(
      input.confirmationTag?.data, rawTagBytes,
      "helper must base64-decode the String input and forward raw Bytes")
  }
}
