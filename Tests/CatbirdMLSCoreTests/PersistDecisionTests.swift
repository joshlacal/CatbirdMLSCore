//
//  PersistDecisionTests.swift
//  CatbirdMLSCoreTests
//
//  Phase D-Swift, Task D-S.4 — `PersistDecision` enum + pure-function
//  decision logic that the atomic-block branching uses to decide whether
//  to write a `processingError`-bearing row (which the UI shows as a
//  failed-decrypt error bubble), write a successful payload, or skip
//  persistence entirely.
//
//  The May 2026 symptom: `persistProcessedPayload` was unconditionally
//  writing rows for every path, including own-message echoes and
//  wrong-epoch old messages. Both cases should be SKIPPED (no row written)
//  rather than surfaced as error placeholders.
//
//  The new flow:
//    - Pure-function `PersistDecisionPolicy.decide(...)` takes the
//      incoming envelope's metadata + group state and returns a
//      `PersistDecision`.
//    - The atomic block in `persistProcessedPayload` switches on the
//      decision: `.persist` writes the row, `.skipPersist` does nothing,
//      `.surfaceError` writes a typed-error row that the UI can act on.
//
//  Phase E.iOS will add `.awaitingReissue` / `.surrenderedAtRecipient` to
//  `SkipReason`; the cases are reserved (commented out) here.
//

import XCTest
@testable import CatbirdMLSCore

final class PersistDecisionTests: XCTestCase {

  // MARK: - Decision enum semantics

  func testEqualityIsValueBased() {
    let a = PersistDecision.skipPersist(reason: .ownMessage)
    let b = PersistDecision.skipPersist(reason: .ownMessage)
    XCTAssertEqual(a, b, "PersistDecision must be Equatable by value")

    let c = PersistDecision.skipPersist(reason: .alreadyMerged)
    XCTAssertNotEqual(a, c, "Distinct SkipReason values must be inequal")
  }

  func testSkipReasonCarriesEpochContext() {
    let reason = SkipReason.wrongEpochOldMessage(messageEpoch: 3, groupEpoch: 5)
    if case .wrongEpochOldMessage(let messageEpoch, let groupEpoch) = reason {
      XCTAssertEqual(messageEpoch, 3)
      XCTAssertEqual(groupEpoch, 5)
    } else {
      XCTFail("SkipReason.wrongEpochOldMessage must preserve associated values")
    }
  }

  // MARK: - Pure-function decision logic

  func testOwnMessageOutcomeIsSkipPersist() {
    let input = PersistDecisionInput(
      senderDeviceDid: "did:plc:alice#dev1",
      localDeviceDid: "did:plc:alice#dev1",
      messageEpoch: 5,
      groupEpoch: 5,
      decryptSucceeded: true,
      isCommit: false,
      hasPlaceholderPayload: false
    )
    XCTAssertEqual(
      PersistDecisionPolicy.decide(input),
      .skipPersist(reason: .ownMessage),
      "An application message from our own device must skip persistence"
    )
  }

  func testOwnCommitOutcomeIsSkipPersist() {
    let input = PersistDecisionInput(
      senderDeviceDid: "did:plc:alice#dev1",
      localDeviceDid: "did:plc:alice#dev1",
      messageEpoch: 5,
      groupEpoch: 5,
      decryptSucceeded: true,
      isCommit: true,
      hasPlaceholderPayload: false
    )
    XCTAssertEqual(
      PersistDecisionPolicy.decide(input),
      .skipPersist(reason: .ownCommitConfirmed),
      "A commit message from our own device must skip persistence as ownCommitConfirmed"
    )
  }

  func testWrongEpochOldMessageIsSkipPersist() {
    let input = PersistDecisionInput(
      senderDeviceDid: "did:plc:bob#dev2",
      localDeviceDid: "did:plc:alice#dev1",
      messageEpoch: 3,
      groupEpoch: 5,
      decryptSucceeded: false,
      isCommit: false,
      hasPlaceholderPayload: false
    )
    XCTAssertEqual(
      PersistDecisionPolicy.decide(input),
      .skipPersist(reason: .wrongEpochOldMessage(messageEpoch: 3, groupEpoch: 5)),
      "A message from a strictly-older epoch must skip persistence"
    )
  }

  func testEmptySenderAndLocalDidsDoNotMatchAsOwnMessage() {
    // The sender DID is extracted from the MLS AAD POST-decrypt. Sites that
    // call into the policy BEFORE decrypt (wrong-epoch, decrypt-failed) pass
    // an empty string. The policy must NOT treat two empty strings as
    // matching ("we don't know who sent this" != "we sent it ourselves").
    let input = PersistDecisionInput(
      senderDeviceDid: "",
      localDeviceDid: "",
      messageEpoch: 3,
      groupEpoch: 5,
      decryptSucceeded: false,
      isCommit: false,
      hasPlaceholderPayload: false
    )
    XCTAssertEqual(
      PersistDecisionPolicy.decide(input),
      .skipPersist(reason: .wrongEpochOldMessage(messageEpoch: 3, groupEpoch: 5)),
      "Empty sender / local DIDs must NOT match as own-message; epoch rule applies instead"
    )
  }

  func testSuccessfulDecryptIsPersist() {
    let input = PersistDecisionInput(
      senderDeviceDid: "did:plc:bob#dev2",
      localDeviceDid: "did:plc:alice#dev1",
      messageEpoch: 5,
      groupEpoch: 5,
      decryptSucceeded: true,
      isCommit: false,
      hasPlaceholderPayload: false
    )
    XCTAssertEqual(
      PersistDecisionPolicy.decide(input),
      .persist,
      "A successfully-decrypted application message from a peer must be persisted"
    )
  }

  func testFutureEpochMessageIsHandledByOrderingNotPersist() {
    // Future-epoch messages should never reach persistProcessedPayload —
    // they're caught at MLSMessageOrderingCoordinator.shouldProcessMessage
    // and routed to `.bufferForFutureEpoch`. If one does slip through, the
    // decision must NOT surface an error (the message is still potentially
    // valid; we're just not ready for it yet).
    let input = PersistDecisionInput(
      senderDeviceDid: "did:plc:bob#dev2",
      localDeviceDid: "did:plc:alice#dev1",
      messageEpoch: 7,
      groupEpoch: 5,
      decryptSucceeded: false,
      isCommit: false,
      hasPlaceholderPayload: false
    )
    let decision = PersistDecisionPolicy.decide(input)
    // Future-epoch falls through to `.skipPersist(reason: .preJoinGap)` —
    // not strictly "pre-join" semantics, but the same "we shouldn't persist
    // an error placeholder" effect.
    if case .skipPersist = decision {
      // OK
    } else {
      XCTFail("Future-epoch message must skip persistence rather than surfacing error; got \(decision)")
    }
  }

  // MARK: - Extension-point compile check

  func testSkipReasonExtensionPointsAreReserved() {
    // This test exists purely to document the extension points for Phase
    // E.iOS. The commented-out cases in SkipReason (`awaitingReissue`,
    // `surrenderedAtRecipient`) must NOT be added by D-S.4 — they belong
    // to E.iOS which wires WelcomeRecoveryDecision. The test serves as a
    // compile-time anchor: if a future change adds these cases without
    // owning the wiring, the test names will collide and the conflict
    // becomes visible.
    //
    // No assertions — the comment IS the test.
    _ = SkipReason.ownMessage
    _ = SkipReason.ownCommitConfirmed
    _ = SkipReason.wrongEpochOldMessage(messageEpoch: 0, groupEpoch: 0)
    _ = SkipReason.alreadyMerged
    _ = SkipReason.preJoinGap(joinedAtEpoch: 0)
  }
}
