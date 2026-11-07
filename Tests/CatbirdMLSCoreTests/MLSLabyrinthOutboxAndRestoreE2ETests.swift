import CatbirdMLS
@testable import CatbirdMLSCore
import Foundation
import XCTest

/// Labyrinth outbox, kick rescue, and recovery end to end over the production native archive
/// (`LabyrinthSession`) through `PetrelPlatformTransport`, against `NativeArchiveMockNetwork`.
/// Live sends carry DS-signed `I13` receipts, so every id is canonical: bare `did:plc`
/// accounts, UUIDv4 devices, conversations, and messages.
final class MLSLabyrinthOutboxAndRestoreE2ETests: XCTestCase {
  private struct Device {
    let config: CatbirdMlsClientConfig
    let keystore: KeychainPlatformKeyStore
    let transport: PetrelPlatformTransport
  }

  private let network = NativeArchiveMockNetwork.shared
  private let dsDid = NativeArchiveMockNetwork.dsDid
  private let batch = NativeArchiveMockNetwork.batch
  private let ownerBatch = NativeArchiveMockNetwork.ownerBatch
  private var roots: [URL] = []
  private var boundedSession: MLSBoundedURLSession!

  override func setUpWithError() throws {
    try super.setUpWithError()
    network.reset()
    boundedSession = MLSBoundedURLSession(protocolClasses: [NativeArchiveMockURLProtocol.self])
  }

  override func tearDownWithError() throws {
    boundedSession?.invalidateAndCancel()
    boundedSession = nil
    let fm = FileManager.default
    let trashRoot = fm.temporaryDirectory.appendingPathComponent("_trash-CatbirdLabyrinthE2E", isDirectory: true)
    try? fm.createDirectory(at: trashRoot, withIntermediateDirectories: true)
    for root in roots {
      try? fm.moveItem(at: root, to: trashRoot.appendingPathComponent(root.lastPathComponent))
    }
    roots = []
    try super.tearDownWithError()
  }

  func testAutomaticBackgroundOutboxDrainAndOfflineRestartResumeSurvival() async throws {
    let aliceDid = "did:plc:alicee2eoutboxaaaaaaaaaa"
    let conversationId = "6f1c2a3b-4d5e-4f60-8a71-b2c3d4e5f601"
    let onlineMessageId = "6f1c2a3b-4d5e-4f60-8a71-b2c3d4e5f611"
    let offlineMessageId = "6f1c2a3b-4d5e-4f60-8a71-b2c3d4e5f612"
    let convoSpaceURI = conversationSpace(conversationId)
    let alice = try device(did: aliceDid, deviceId: "a11ce001-0000-4000-8000-000000000001", label: "Dev1")

    // 1. Open Device 1, enroll, setup Labyrinth recovery code, create group, and send online message.
    let coordinator1 = try await open(alice)
    _ = try await coordinator1.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    _ = try await coordinator1.setupRecoveryCode()
    _ = try await coordinator1.createGroup(
      conversationId: conversationId,
      sequencerDid: dsDid,
      title: "Automatic Drain Group",
      description: nil
    )

    let sentOnline = try await coordinator1.sendMessage(
      conversationId: conversationId,
      messageId: onlineMessageId,
      text: "hello automatic background outbox drain"
    )
    XCTAssertEqual(sentOnline.messageId, onlineMessageId)

    // Without calling flushLabyrinthQueue() manually, background drain on ffiDispatchQueue must empty the queue!
    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await coordinator1.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }
    XCTAssertEqual(
      network.recordCount(space: convoSpaceURI, repo: aliceDid, collection: batch),
      1,
      "Automatic background drain after sendMessage must commit a batch to the conversation Space"
    )

    // 2. Make conversation Space temporarily offline (HTTP 503): sendMessage MUST still succeed and queue durably!
    network.setStatusOverride(503, forSpace: convoSpaceURI)

    let sentOffline = try await coordinator1.sendMessage(
      conversationId: conversationId,
      messageId: offlineMessageId,
      text: "queued while Space returns 503"
    )
    XCTAssertEqual(sentOffline.messageId, offlineMessageId)

    // The background best-effort drain hits 503 and retains the row without crashing/throwing.
    try await waitUntil(timeoutSeconds: 3.0) { network.overrideRejections(forSpace: convoSpaceURI) > 0 }
    let pendingBeforeRestart = try await coordinator1.listPendingLabyrinthQueue(conversationId: conversationId)
    XCTAssertEqual(pendingBeforeRestart.map(\.messageId), [offlineMessageId])
    XCTAssertNotNil(pendingBeforeRestart.first?.frameNonceB64)

    // 3. Simulate app termination + cold reopen while Space is STILL offline (503).
    await coordinator1.close()

    let coordinatorReopened = try await open(alice)
    let pendingAfterRestart = try await coordinatorReopened.listPendingLabyrinthQueue(conversationId: conversationId)
    XCTAssertEqual(
      pendingAfterRestart.map(\.messageId),
      [offlineMessageId],
      "Queued Labyrinth payload must survive coordinator close/reopen across restarts"
    )

    // 4. Background the app, restore Space availability (200), then resumeFromBackground():
    //    resumeFromBackground() must automatically drain the surviving offline row!
    _ = try await coordinatorReopened.prepareForBackground()
    let closedInBg = await MainActor.run { coordinatorReopened.isHandleClosed }
    XCTAssertTrue(closedInBg)

    network.setStatusOverride(nil, forSpace: convoSpaceURI)
    try await coordinatorReopened.resumeFromBackground()

    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await coordinatorReopened.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }
    XCTAssertEqual(
      network.recordCount(space: convoSpaceURI, repo: aliceDid, collection: batch),
      2,
      "resumeFromBackground must automatically drain queued outbox rows to the conversation Space"
    )

    // Explicit flush on empty queue is a clean no-op.
    let explicitZero = try await coordinatorReopened.flushLabyrinthQueue()
    XCTAssertEqual(explicitZero, 0)
    XCTAssertFalse(network.sawMainThreadRequest)

    await coordinatorReopened.close()
    XCTAssertEqual(network.contractViolations, [])
  }

  func testKick403FallbackRescueConversationAndFreshDeviceMailboxRestoreEndToEnd() async throws {
    let aliceDid = "did:plc:alicekickrestoreaaaaaaaa"
    let bobDid = "did:plc:bobpeerkickbbbbbbbbbbbbb"
    let aliceDevice1 = "a11ce002-0000-4000-8000-000000000001"
    let aliceDevice2 = "a11ce002-0000-4000-8000-000000000002"
    let conversationId = "7a2d3b4c-5e6f-4a70-8b81-c2d3e4f5a602"
    let bobMessageId = "7a2d3b4c-5e6f-4a70-8b81-c2d3e4f5a611"
    let aliceCachedMessageId = "7a2d3b4c-5e6f-4a70-8b81-c2d3e4f5a612"
    let alicePostKickMessageId = "7a2d3b4c-5e6f-4a70-8b81-c2d3e4f5a613"
    let convoSpaceURI = conversationSpace(conversationId)
    let personalMailboxSpaceURI = "at://\(aliceDid)/space/blue.catbird.labyrinth.mailbox/self"

    // -------------------------------------------------------------------------
    // Device 1 and peer Bob: enroll, live group invite/accept/Welcome, seed Bob + Alice history
    // through the DS (verified I13 receipts), get kicked (403), drain outbox fallback +
    // rescueConversationOnKick, then wipe Device 1!
    // -------------------------------------------------------------------------
    let bob = try await open(try device(did: bobDid, deviceId: "b0b00002-0000-4000-8000-000000000001", label: "Bob"))
    _ = try await bob.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    let device1 = try await open(try device(did: aliceDid, deviceId: aliceDevice1, label: "Dev1"))
    _ = try await device1.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    let recoveryCode = try await device1.setupRecoveryCode()

    _ = try await createGroup(
      conversationId,
      title: "Pre-Kick Conversation",
      owner: device1,
      peer: bob,
      peerDid: bobDid
    )

    _ = try await bob.sendMessage(
      conversationId: conversationId,
      messageId: bobMessageId,
      text: "Bob peer message before kick"
    )
    _ = try await device1.sync()
    _ = try await device1.sendMessage(
      conversationId: conversationId,
      messageId: aliceCachedMessageId,
      text: "Alice cached message before kick"
    )
    let seeded = try await device1.listMessages(conversationId: conversationId, limit: 20)
    XCTAssertEqual(Set(seeded.map(\.messageId)), [bobMessageId, aliceCachedMessageId])
    // Pre-kick history (Bob's witness copy and Alice's own) drains into the conversation Space.
    try await waitUntil(timeoutSeconds: 3.0) {
      try await device1.listPendingLabyrinthQueue(conversationId: conversationId).isEmpty
    }
    let batchesBeforeKick = network.recordCount(space: convoSpaceURI, repo: aliceDid, collection: batch)
    XCTAssertGreaterThanOrEqual(batchesBeforeKick, 1)

    // Kick Alice from the conversation Space (403 Forbidden on conversation Space; personal mailbox stays 200).
    network.setStatusOverride(403, forSpace: convoSpaceURI)

    _ = try await device1.sendMessage(
      conversationId: conversationId,
      messageId: alicePostKickMessageId,
      text: "Alice message queued right as 403 kick landed"
    )

    // Wait for automatic background drain (or explicit flushLabyrinthQueue) to fall back to personal mailbox Space!
    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await device1.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }
    let postFlushExtra = try await device1.flushLabyrinthQueue()
    XCTAssertEqual(postFlushExtra, 0)
    XCTAssertEqual(
      network.recordCount(space: convoSpaceURI, repo: aliceDid, collection: batch),
      batchesBeforeKick,
      "403 conversation Space must reject direct conversation batch writes"
    )
    XCTAssertGreaterThanOrEqual(
      network.recordCount(space: personalMailboxSpaceURI, repo: aliceDid, collection: ownerBatch),
      1,
      "403 kick during outbox drain must fall back to personal mailbox ownerBatch"
    )

    // Rescue the cached history (including Bob's peer message!) into the personal mailbox Space. The
    // post-kick message already fell back there, so the native rescue seals exactly the other two.
    let rescuedCount = try await device1.rescueConversationOnKick(conversationId: conversationId)
    XCTAssertEqual(rescuedCount, 2, "rescueConversationOnKick must owner-archive both pre-kick messages")

    // A second rescue finds every copy already in the owner archive and writes nothing.
    let rescuedSecondPass = try await device1.rescueConversationOnKick(conversationId: conversationId)
    XCTAssertEqual(rescuedSecondPass, 0)

    // Wipe Device 1 completely before restoring on Device 2.
    try await device1.signOutAndWipe()

    // -------------------------------------------------------------------------
    // Device 2: Completely fresh storageRoot + fresh KeychainBackend, zero shared local state.
    // Restore via recovery code (with a 1-character typo inside Reed-Solomon budget!) + owner mailbox!
    // -------------------------------------------------------------------------
    let recorder = await MainActor.run { EventRecorder() }
    let device2Config = try device(did: aliceDid, deviceId: aliceDevice2, label: "Dev2")
    let device2 = try await MainActor.run {
      let c = try MLSClientCoordinator(config: device2Config.config, keystore: device2Config.keystore, transport: device2Config.transport)
      c.onChangeCallback = { event in
        recorder.events.append(event)
      }
      return c
    }

    _ = try await device2.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)

    // Wrong recovery code MUST fail closed.
    do {
      _ = try await device2.restoreWithRecoveryCode("AAAA-BBBB-CCCC-DDDD-EEEE-FFFF")
      XCTFail("Bogus recovery code must fail closed")
    } catch {
      // Expected failure.
    }

    // Introduce 2 character typos (after the 2-character version prefix) into the 40-char recovery code.
    let typoRecoveryCode = Self.injectTwoCharacterTypos(into: recoveryCode)
    let backupStatus = try await device2.restoreWithRecoveryCode(typoRecoveryCode)
    XCTAssertTrue(backupStatus.backupEnabled)
    XCTAssertTrue(backupStatus.hasRecoveryCode)
    XCTAssertTrue(backupStatus.activeDeviceIds.contains(aliceDevice2))

    let restoredMessagesCount = try await device2.restoreConversationFromOwnerMailbox(
      conversationId: conversationId
    )
    XCTAssertEqual(restoredMessagesCount, 3)

    // Second restore pass must be idempotent (0 duplicate inserts).
    let secondRestoreCount = try await device2.restoreConversationFromOwnerMailbox(
      conversationId: conversationId
    )
    XCTAssertEqual(secondRestoreCount, 0)

    let conversations = try await device2.listConversations(limit: 10)
    XCTAssertEqual(conversations.map(\.conversationId), [conversationId])

    // listMessages returns newest-first or seq-ordered depending on query; sort by seq ascending to assert all 3.
    let restoredMessages = try await device2.listMessages(conversationId: conversationId, limit: 20)
      .sorted { $0.seq < $1.seq }
    XCTAssertEqual(restoredMessages.count, 3)

    XCTAssertEqual(restoredMessages[0].messageId, bobMessageId)
    XCTAssertEqual(restoredMessages[0].senderDid, bobDid)
    XCTAssertFalse(restoredMessages[0].isOwn, "Peer message restored from v5 attributed frame must have isOwn == false")
    XCTAssertTrue(restoredMessages[0].plaintextJson.contains("Bob peer message before kick"))

    XCTAssertEqual(restoredMessages[1].messageId, aliceCachedMessageId)
    XCTAssertEqual(restoredMessages[1].senderDid, aliceDid)
    XCTAssertTrue(restoredMessages[1].isOwn)
    XCTAssertTrue(restoredMessages[1].plaintextJson.contains("Alice cached message before kick"))

    XCTAssertEqual(restoredMessages[2].messageId, alicePostKickMessageId)
    XCTAssertEqual(restoredMessages[2].senderDid, aliceDid)
    XCTAssertTrue(restoredMessages[2].isOwn)
    XCTAssertTrue(restoredMessages[2].plaintextJson.contains("Alice message queued right as 403 kick landed"))

    // Give MainActor observer hop a moment to deliver PlatformChangeEvents.
    try await waitUntil(timeoutSeconds: 2.0) {
      await MainActor.run {
        recorder.events.contains(.messagesUpdated(conversationId: conversationId))
          && recorder.events.contains(.conversationsUpdated)
      }
    }

    XCTAssertFalse(network.sawMainThreadRequest)
    await device2.close()
    await bob.close()
    XCTAssertEqual(network.contractViolations, [])
  }

  func testColdStartEnrollDrainsOfflineQueueConcurrentBurstCoalescesAndClosedHandleRejects() async throws {
    let aliceDid = "did:plc:aliceedgecasesaaaaaaaaaa"
    let conversationId = "8b3e4c5d-6f70-4b81-9c92-d3e4f5a6b703"
    let coldStartMessageId = "8b3e4c5d-6f70-4b81-9c92-d3e4f5a6b710"
    let convoSpaceURI = conversationSpace(conversationId)
    let alice = try device(did: aliceDid, deviceId: "a11ce003-0000-4000-8000-000000000001", label: "Dev1")

    let coordinator1 = try await open(alice)
    _ = try await coordinator1.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    _ = try await coordinator1.createGroup(
      conversationId: conversationId,
      sequencerDid: dsDid,
      title: "Cold-Start & Burst Group",
      description: nil
    )

    // 1. Queue a message while Space returns 503, terminate coordinator, restore 200, cold-reopen + enrollDevice!
    network.setStatusOverride(503, forSpace: convoSpaceURI)
    _ = try await coordinator1.sendMessage(
      conversationId: conversationId,
      messageId: coldStartMessageId,
      text: "queued offline before cold restart"
    )
    try await waitUntil(timeoutSeconds: 3.0) { network.overrideRejections(forSpace: convoSpaceURI) > 0 }
    await coordinator1.close()

    network.setStatusOverride(nil, forSpace: convoSpaceURI)
    let coordinator2 = try await open(alice)
    let pendingOnColdOpen = try await coordinator2.listPendingLabyrinthQueue(conversationId: conversationId)
    XCTAssertEqual(pendingOnColdOpen.map(\.messageId), [coldStartMessageId])

    _ = try await coordinator2.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await coordinator2.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }

    // 2. Concurrent 12-message burst coalesces through OutboxDrainCoalescer and commits all 12 rows.
    try await withThrowingTaskGroup(of: Void.self) { group in
      for idx in 0..<12 {
        group.addTask {
          _ = try await coordinator2.sendMessage(
            conversationId: conversationId,
            messageId: String(format: "8b3e4c5d-6f70-4b81-9c92-d3e4f5a6b8%02d", idx),
            text: "burst payload \(idx)"
          )
        }
      }
      try await group.waitForAll()
    }
    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await coordinator2.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }
    XCTAssertEqual(
      network.archivedItemCount(space: convoSpaceURI, repo: aliceDid, collection: batch),
      13,
      "The cold-start row and all 12 burst rows must each be sealed into the conversation Space exactly once"
    )

    // 3. Empty/unknown conversation rescue & restore boundary cases return 0 cleanly.
    let unknownConversationId = "8b3e4c5d-6f70-4b81-9c92-d3e4f5a6b7ff"
    let emptyRescue = try await coordinator2.rescueConversationOnKick(conversationId: unknownConversationId)
    XCTAssertEqual(emptyRescue, 0)
    let emptyRestore = try await coordinator2.restoreConversationFromOwnerMailbox(conversationId: unknownConversationId)
    XCTAssertEqual(emptyRestore, 0)

    // 4. After prepareForBackground() (handle closed), outbox/rescue/restore fail closed with ContextClosed.
    _ = try await coordinator2.prepareForBackground()
    for closedOp in [
      { _ = try await coordinator2.listPendingLabyrinthQueue() },
      { _ = try await coordinator2.flushLabyrinthQueue() },
      { _ = try await coordinator2.rescueConversationOnKick(conversationId: conversationId) },
      { _ = try await coordinator2.restoreConversationFromOwnerMailbox(conversationId: conversationId) },
    ] {
      do {
        try await closedOp()
        XCTFail("Expected ContextClosed error when SQLite handle is closed")
      } catch CatbirdMLS.MlsError.ContextClosed {
        // Expected.
      }
    }

    await coordinator2.close()
    XCTAssertEqual(network.contractViolations, [])
  }

  func testColdLaunchCheckSpacesEligibilityAutomaticallyDrainsWithoutReEnroll() async throws {
    let aliceDid = "did:plc:alicecoldeligaaaaaaaaaaa"
    let conversationId = "9c4f5d6e-7081-4c92-8da3-e4f5a6b7c804"
    let messageId = "9c4f5d6e-7081-4c92-8da3-e4f5a6b7c811"
    let convoSpaceURI = conversationSpace(conversationId)
    let alice = try device(did: aliceDid, deviceId: "a11ce004-0000-4000-8000-000000000001", label: "Dev1")

    let coordinator1 = try await open(alice)
    _ = try await coordinator1.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    _ = try await coordinator1.createGroup(
      conversationId: conversationId,
      sequencerDid: dsDid,
      title: "Cold Eligibility Drain",
      description: nil
    )

    network.setStatusOverride(503, forSpace: convoSpaceURI)
    _ = try await coordinator1.sendMessage(
      conversationId: conversationId,
      messageId: messageId,
      text: "queued before app termination on already-enrolled device"
    )
    try await waitUntil(timeoutSeconds: 3.0) { network.overrideRejections(forSpace: convoSpaceURI) > 0 }
    await coordinator1.close()

    // Cold reopen of already-enrolled account: calling checkSpacesEligibility(grantedScopes:)
    // WITHOUT calling enrollDevice or flushLabyrinthQueue must automatically drain the outbox!
    network.setStatusOverride(nil, forSpace: convoSpaceURI)
    let coordinator2 = try await open(alice)
    let pendingOnOpen = try await coordinator2.listPendingLabyrinthQueue(conversationId: conversationId)
    XCTAssertEqual(pendingOnOpen.map(\.messageId), [messageId])

    let eligibility = try await coordinator2.checkSpacesEligibility(
      grantedScopes: MLSAppGroupStorage.requiredSpacesScopes
    )
    XCTAssertTrue(eligibility.eligible)

    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await coordinator2.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }
    XCTAssertEqual(
      network.recordCount(space: convoSpaceURI, repo: aliceDid, collection: batch),
      1,
      "Cold-launch checkSpacesEligibility must automatically drain queued outbox rows"
    )

    await coordinator2.close()
    XCTAssertEqual(network.contractViolations, [])
  }

  func testOutboxDrainCoalescerPreservesPostResumeWakeupUnderPreBackgroundRace() {
    let coalescer = OutboxDrainCoalescer()
    var handleClosed = false

    // 1. Pre-background drain pass starts.
    XCTAssertTrue(coalescer.requestDrain(isHandleClosed: { handleClosed }))
    XCTAssertTrue(coalescer.beginFirstIteration(isHandleClosed: { handleClosed }))

    // 2. App enters background while pass 1 is finishing, then immediately resumes and requests drain.
    coalescer.setSuspended(true)
    handleClosed = true
    handleClosed = false
    coalescer.setSuspended(false)
    XCTAssertFalse(
      coalescer.requestDrain(isHandleClosed: { handleClosed }),
      "In-flight drain pass should coalesce the post-resume wakeup via needsAnotherPass"
    )

    // 3. Pass 1 finishes AFTER resumeFromBackground reopened the handle: must NOT drop needsAnotherPass!
    XCTAssertTrue(
      coalescer.completeAndShouldRepeat(isHandleClosed: { handleClosed }),
      "Atomic completeAndShouldRepeat must observe reopened handle and run the coalesced post-resume pass"
    )
    XCTAssertFalse(
      coalescer.completeAndShouldRepeat(isHandleClosed: { handleClosed }),
      "After post-resume pass completes with no further requests, coalescer must go idle"
    )
  }

  func testMultiConversationHeadOfLineIsolationAndThreeDeviceMultiEpochRecoveryEndToEnd() async throws {
    let aliceDid = "did:plc:alicemultie2eaaaaaaaaaaa"
    let bobDid = "did:plc:bobmultie2ebbbbbbbbbbbbb"
    let ipadDeviceId = "a11ce005-0000-4000-8000-0000000000ad"
    let stalledConvo = "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d905"
    let healthyConvo = "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d906"
    let ep1 = "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d901"
    let ep2 = "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d902"
    let ep3 = "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d903"
    let ep4 = "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d904"
    let stalledSpaceURI = conversationSpace(stalledConvo)
    let healthySpaceURI = conversationSpace(healthyConvo)

    let device1 = try await open(try device(did: aliceDid, deviceId: "a11ce005-0000-4000-8000-000000000001", label: "Dev1"))
    _ = try await device1.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)

    // --- Part A: Per-conversation head-of-line 503 isolation ---
    _ = try await device1.createGroup(
      conversationId: stalledConvo,
      sequencerDid: dsDid,
      title: "Stalled Convo",
      description: nil
    )
    _ = try await device1.createGroup(
      conversationId: healthyConvo,
      sequencerDid: dsDid,
      title: "Healthy Convo",
      description: nil
    )
    network.setStatusOverride(503, forSpace: stalledSpaceURI)

    _ = try await device1.sendMessage(
      conversationId: stalledConvo,
      messageId: "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d915",
      text: "stalled behind 503"
    )
    _ = try await device1.sendMessage(
      conversationId: healthyConvo,
      messageId: "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d916",
      text: "must drain despite earlier stalled conversation"
    )

    try await waitUntil(timeoutSeconds: 3.0) {
      let healthyPending = try await device1.listPendingLabyrinthQueue(conversationId: healthyConvo)
      return healthyPending.isEmpty
    }
    XCTAssertEqual(
      network.recordCount(space: healthySpaceURI, repo: aliceDid, collection: batch),
      1
    )
    try await waitUntil(timeoutSeconds: 3.0) { network.overrideRejections(forSpace: stalledSpaceURI) > 0 }
    let stillStalled = try await device1.listPendingLabyrinthQueue(conversationId: stalledConvo)
    XCTAssertEqual(stillStalled.map(\.messageId), ["ad5e6f70-8192-4da3-9eb4-f5a6b7c8d915"])
    network.setStatusOverride(nil, forSpace: stalledSpaceURI)
    _ = try await device1.flushLabyrinthQueue()

    // --- Part B: Multi-epoch & 3-device recovery chain (native mailbox epochs start at genesis 0) ---
    // Epoch 0 rescue (before OTC & before setupRecoveryCode): Bob's live peer message, received by Device 1.
    let bob = try await open(try device(did: bobDid, deviceId: "b0b00005-0000-4000-8000-000000000001", label: "Bob"))
    _ = try await bob.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    _ = try await createGroup(ep1, title: "Epoch 1", owner: device1, peer: bob, peerDid: bobDid)
    _ = try await bob.sendMessage(
      conversationId: ep1,
      messageId: "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d911",
      text: "Bob message at epoch 1"
    )
    _ = try await device1.sync()
    try await waitUntil(timeoutSeconds: 3.0) {
      try await device1.listPendingLabyrinthQueue(conversationId: ep1).isEmpty
    }
    let r1 = try await device1.rescueConversationOnKick(conversationId: ep1)
    XCTAssertEqual(r1, 1)

    // Rotate 0 -> 1 via OTC pairing, rescue Epoch 1, setup recovery code at Epoch 1
    let otcOffer = try await device1.initiateOtcPairing()
    let otcDone = try await device1.completeOtcPairing(sixDigitCode: otcOffer.sixDigitCode)
    XCTAssertEqual(otcDone.newEpochSeq, 1)

    _ = try await device1.createGroup(
      conversationId: ep2,
      sequencerDid: dsDid,
      title: "Epoch 2",
      description: nil
    )
    _ = try await device1.sendMessage(
      conversationId: ep2,
      messageId: "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d912",
      text: "Alice message at epoch 2"
    )
    // The automatic drain and the kick rescue race for the outbox lock on the concurrent FFI
    // queue. Restore reads the still-readable conversation Space, so publish the sender copy
    // first, as for Epoch 1.
    try await waitUntil(timeoutSeconds: 3.0) {
      try await device1.listPendingLabyrinthQueue(conversationId: ep2).isEmpty
    }
    let r2 = try await device1.rescueConversationOnKick(conversationId: ep2)
    XCTAssertEqual(r2, 1)

    let recoveryCode = try await device1.setupRecoveryCode()

    // The old iPad enrolls into the mailbox with the recovery code (Gate 3 rotation 1 -> 2), then
    // Device 1 revokes its Labyrinth device key (2 -> 3), rescues Epoch 3, and wipes itself.
    let ipad = try await open(try device(did: aliceDid, deviceId: ipadDeviceId, label: "OldIPad"))
    _ = try await ipad.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    let ipadStatus = try await ipad.restoreWithRecoveryCode(recoveryCode)
    XCTAssertEqual(ipadStatus.epochSeq, 2)
    await ipad.close()
    let oldIPadKey = try XCTUnwrap(network.labyrinthDeviceKeyHex(account: aliceDid, chatDeviceId: ipadDeviceId))

    let revStatus = try await device1.revokeLabyrinthDevice(targetDeviceId: oldIPadKey)
    XCTAssertEqual(revStatus.epochSeq, 3)

    _ = try await device1.createGroup(
      conversationId: ep3,
      sequencerDid: dsDid,
      title: "Epoch 3",
      description: nil
    )
    _ = try await device1.sendMessage(
      conversationId: ep3,
      messageId: "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d913",
      text: "Alice message at epoch 3"
    )
    try await waitUntil(timeoutSeconds: 3.0) {
      try await device1.listPendingLabyrinthQueue(conversationId: ep3).isEmpty
    }
    let r3 = try await device1.rescueConversationOnKick(conversationId: ep3)
    XCTAssertEqual(r3, 1)
    try await device1.signOutAndWipe()

    // Device 2 restores (backward 1->0, forward 1->3, rotates 3->4), rescues Epoch 4, wipes Device 2
    let device2 = try await open(try device(did: aliceDid, deviceId: "a11ce005-0000-4000-8000-000000000002", label: "Dev2"))
    _ = try await device2.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    let st2 = try await device2.restoreWithRecoveryCode(recoveryCode)
    XCTAssertEqual(st2.epochSeq, 4)

    for cid in [ep1, ep2, ep3] {
      let count = try await device2.restoreConversationFromOwnerMailbox(conversationId: cid)
      XCTAssertEqual(count, 1, "Device 2 must restore \(cid)")
    }
    let ep1MsgsDev2 = try await device2.listMessages(conversationId: ep1, limit: 10)
    XCTAssertEqual(ep1MsgsDev2.first?.senderDid, bobDid)
    XCTAssertEqual(ep1MsgsDev2.first?.isOwn, false)

    _ = try await device2.createGroup(
      conversationId: ep4,
      sequencerDid: dsDid,
      title: "Epoch 4",
      description: nil
    )
    _ = try await device2.sendMessage(
      conversationId: ep4,
      messageId: "ad5e6f70-8192-4da3-9eb4-f5a6b7c8d914",
      text: "Device 2 message at epoch 4"
    )
    try await waitUntil(timeoutSeconds: 3.0) {
      try await device2.listPendingLabyrinthQueue(conversationId: ep4).isEmpty
    }
    let r4 = try await device2.rescueConversationOnKick(conversationId: ep4)
    XCTAssertEqual(r4, 1)
    try await device2.signOutAndWipe()

    // Device 3 restores with the SAME 40-char recoveryCode from Device 1 (rotates 4->5) and recovers all 4 epochs!
    let device3 = try await open(try device(did: aliceDid, deviceId: "a11ce005-0000-4000-8000-000000000003", label: "Dev3"))
    _ = try await device3.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    let st3 = try await device3.restoreWithRecoveryCode(recoveryCode)
    XCTAssertEqual(st3.epochSeq, 5)

    for cid in [ep1, ep2, ep3, ep4] {
      let count = try await device3.restoreConversationFromOwnerMailbox(conversationId: cid)
      XCTAssertEqual(count, 1, "Device 3 must restore \(cid) using original recovery code")
    }
    let ep4MsgsDev3 = try await device3.listMessages(conversationId: ep4, limit: 10)
    XCTAssertEqual(ep4MsgsDev3.first?.senderDid, aliceDid)
    XCTAssertTrue(ep4MsgsDev3.first?.plaintextJson.contains("Device 2 message at epoch 4") ?? false)

    await device3.close()
    await bob.close()
    XCTAssertEqual(network.contractViolations, [])
  }

  // MARK: - Helpers

  private func conversationSpace(_ conversationId: String) -> String {
    "at://\(dsDid)/space/blue.catbird.labyrinth.conversation/\(conversationId)-0"
  }

  /// One device: fresh storage root, fresh Keychain backend, the account's own PDS host.
  private func device(did: String, deviceId: String, label: String) throws -> Device {
    let root = FileManager.default.temporaryDirectory
      .appendingPathComponent("CatbirdLabyrinthE2E-\(label)-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
    roots.append(root)
    return Device(
      config: CatbirdMlsClientConfig(
        storageRoot: root.path,
        accountDid: did,
        deviceId: deviceId,
        homeDsDid: dsDid,
        homeDsEndpoint: NativeArchiveMockNetwork.dsEndpoint,
        pdsEndpoint: NativeArchiveMockNetwork.pdsEndpoint(for: did)
      ),
      keystore: KeychainPlatformKeyStore(
        service: "blue.catbird.mls.e2e.\(label).\(UUID().uuidString)",
        backend: RecordingKeychainBackend()
      ),
      transport: PetrelPlatformTransport(
        homeDsEndpoint: URL(string: NativeArchiveMockNetwork.dsEndpoint),
        boundedSession: boundedSession
      )
    )
  }

  private func open(_ device: Device) async throws -> MLSClientCoordinator {
    try await MainActor.run {
      try MLSClientCoordinator(config: device.config, keystore: device.keystore, transport: device.transport)
    }
  }

  /// Live group with one peer: invite, participant acceptance, leaf-recovery fulfillment, Welcome join.
  private func createGroup(
    _ conversationId: String,
    title: String,
    owner: MLSClientCoordinator,
    peer: MLSClientCoordinator,
    peerDid: String
  ) async throws -> ConversationRow {
    let row = try await owner.createGroup(
      conversationId: conversationId,
      sequencerDid: dsDid,
      memberDids: [peerDid],
      title: title,
      description: nil
    )
    _ = try await peer.sync()
    _ = try await owner.sync()
    _ = try await peer.sync()
    let joined = try await peer.listConversations(limit: 50).first { $0.conversationId == conversationId }
    XCTAssertEqual(joined?.groupId, row.groupId, "peer must join the same MLS group")
    return row
  }

  private func waitUntil(
    timeoutSeconds: TimeInterval,
    condition: () async throws -> Bool
  ) async throws {
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while Date() < deadline {
      if try await condition() {
        return
      }
      try await Task.sleep(nanoseconds: 15_000_000)
    }
    let finalCheck = try await condition()
    XCTAssertTrue(finalCheck, "Timed out after \(timeoutSeconds)s waiting for condition")
  }

  private static func injectTwoCharacterTypos(into code: String) -> String {
    var chars = Array(code)
    var alnumSeen = 0
    var substituted = 0
    for idx in chars.indices {
      if chars[idx].isLetter || chars[idx].isNumber {
        alnumSeen += 1
        if alnumSeen > 2 {
          chars[idx] = (chars[idx] == "A") ? "C" : "A"
          substituted += 1
          if substituted == 2 {
            break
          }
        }
      }
    }
    return String(chars)
  }
}

@MainActor
private final class EventRecorder {
  var events: [PlatformChangeEvent] = []
}
