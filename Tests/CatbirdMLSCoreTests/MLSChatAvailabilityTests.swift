import Foundation
import Petrel
import PetrelCatbird
import Testing
@testable import CatbirdMLSCore

struct MLSChatAvailabilityTests {
  private let descending = [
    "did:plc:yirep3fqwvhazp5kcimfo2ne",
    "did:plc:oq3qa6f332ergklpj2dvd3up",
    "did:plc:nnnnnnnnnnnnnnnnnnnnnnnn",
    "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm",
    "did:plc:llllllllllllllllllllllll",
    "did:plc:kkkkkkkkkkkkkkkkkkkkkkkk",
    "did:plc:jjjjjjjjjjjjjjjjjjjjjjjj",
  ]

  private func declaration(_ policy: String = "all", version: String = "1") throws -> BlueCatbirdChatDeclaration {
    BlueCatbirdChatDeclaration(
      allowIncoming: policy,
      deliveryService: try DID(didString: "did:web:chat.catbird.blue"),
      protocolVersion: version,
      createdAt: ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_000))
    )
  }

  @Test func descendingDuplicateDIDsAreSortedBeforeBatchingAndResultsKeepCallerOrder() async throws {
    let inputs = descending + [descending[0], descending[5]]
    let recorder = RequestRecorder()
    let declaration = try declaration()
    let present = Set([descending[0], descending[5]])
    let results = await MLSAPIClient.resolveChatAvailability(
      dids: try inputs.map { try DID(didString: $0) },
      fetchDeclaration: { did in
        await recorder.recordDeclaration(did)
        return declaration
      },
      fetchDevices: { dids in
        let keys = dids.map { $0.didString() }
        await recorder.recordDevices(keys)
        #expect((1...5).contains(keys.count))
        #expect(keys == Set(keys).sorted { $0.utf8.lexicographicallyPrecedes($1.utf8) })
        return present.intersection(keys)
      }
    )
    let requests = await recorder.deviceRequests.sorted { $0[0] < $1[0] }
    let sorted = descending.sorted { $0.utf8.lexicographicallyPrecedes($1.utf8) }
    #expect(requests == [Array(sorted.prefix(5)), Array(sorted.suffix(2))])
    #expect(await recorder.declarations.count == 7)
    #expect(results.map { $0.did.didString() } == inputs)
    #expect(results.map(\.availability) == inputs.map { present.contains($0) ? .available : .unavailable })
  }

  @Test func failedDeviceChunkIsUnknownAndSuccessfulEmptyChunkIsUnavailable() async throws {
    let declaration = try declaration()
    let sorted = descending.sorted()
    let failed = Set(sorted.prefix(5))
    let results = await MLSAPIClient.resolveChatAvailability(
      dids: try descending.map { try DID(didString: $0) },
      fetchDeclaration: { _ in declaration },
      fetchDevices: { dids in
        if failed.contains(dids[0].didString()) { throw URLError(.badServerResponse) }
        return []
      }
    )
    #expect(results.filter { failed.contains($0.did.didString()) }.allSatisfy { $0.availability == .unknown })
    #expect(results.filter { !failed.contains($0.did.didString()) }.allSatisfy { $0.availability == .unavailable })
    #expect(throws: MLSAPIClient.AvailabilityError.self) {
      try MLSAPIClient.confirmedOptInStatuses(results)
    }
  }

  @Test func declarationFailureIsUnknownButNoneAndAbsenceStayUnavailable() async throws {
    let none = try declaration("none")
    let dids = try descending.prefix(3).map { try DID(didString: $0) }
    let failedDID = dids[0].didString()
    let deniedDID = dids[1].didString()
    let results = await MLSAPIClient.resolveChatAvailability(
      dids: dids,
      fetchDeclaration: { did in
        if did == failedDID { throw URLError(.timedOut) }
        return did == deniedDID ? none : nil
      },
      fetchDevices: { _ in
        Issue.record("Unavailable and unknown declarations must not query devices")
        return []
      }
    )
    #expect(results.map(\.availability) == [.unknown, .unavailable, .unavailable])
  }

  @Test func deviceAuthenticationFailureDoesNotUseDeclarationOnlyFallback() async throws {
    let declaration = try declaration()
    let results = await MLSAPIClient.resolveChatAvailability(
      dids: [try DID(didString: descending[0])],
      fetchDeclaration: { _ in declaration },
      fetchDevices: { _ in throw MLSAPIError.noAuthentication }
    )
    #expect(results[0].availability == .unknown)
  }

  @Test func followingIsAnOptInCandidateButUnknownPolicyAndProtocolAreDenied() async throws {
    let following = try declaration("following")
    let unknown = try declaration("future-policy")
    let incompatible = try declaration(version: "2")
    let dids = try descending.prefix(3).map { try DID(didString: $0) }
    let declarations = Dictionary(uniqueKeysWithValues: zip(dids.map { $0.didString() }, [following, unknown, incompatible]))
    let results = await MLSAPIClient.resolveChatAvailability(
      dids: dids,
      fetchDeclaration: { declarations[$0] },
      fetchDevices: { devices in
        #expect(devices.map { $0.didString() } == [dids[0].didString()])
        return Set(devices.map { $0.didString() })
      }
    )
    #expect(results.map(\.availability) == [.available, .unavailable, .unavailable])
    #expect(!MLSDeclarationService.evaluateEligibility(declaration: following, targetFollowsViewer: false))
    #expect(MLSDeclarationService.evaluateEligibility(declaration: following, targetFollowsViewer: true))
  }

  @Test func unrequestedDeviceResponseIsUnknown() async throws {
    let declaration = try declaration()
    let unexpected = descending[1]
    let results = await MLSAPIClient.resolveChatAvailability(
      dids: [try DID(didString: descending[0])],
      fetchDeclaration: { _ in declaration },
      fetchDevices: { _ in [unexpected] }
    )
    #expect(results[0].availability == .unknown)
  }

  @Test func failedReadCanBeRetriedWithoutCachingAnOptOut() async throws {
    let declaration = try declaration()
    let recorder = RequestRecorder()
    let did = try DID(didString: descending[0])
    let fetch: @Sendable ([DID]) async throws -> Set<String> = { dids in
      let attempt = await recorder.recordAttempt()
      if attempt == 1 { throw URLError(.cannotConnectToHost) }
      return Set(dids.map { $0.didString() })
    }
    let failed = await MLSAPIClient.resolveChatAvailability(dids: [did], fetchDeclaration: { _ in declaration }, fetchDevices: fetch)
    let retried = await MLSAPIClient.resolveChatAvailability(dids: [did], fetchDeclaration: { _ in declaration }, fetchDevices: fetch)
    #expect(failed[0].availability == .unknown)
    #expect(retried[0].availability == .available)
    #expect(try MLSAPIClient.confirmedOptInStatuses(retried)[0].optedIn)
  }

  @Test func declarationReaderOnlyTreatsExplicitRecordNotFoundAsAbsence() throws {
    for status in [400, 404] {
      #expect(try MLSPublicPDSReader.declaration(from: Data(#"{"error":"RecordNotFound"}"#.utf8), statusCode: status) == nil)
      #expect(throws: (any Error).self) {
        try MLSPublicPDSReader.declaration(from: Data(#"{"error":"InvalidRequest"}"#.utf8), statusCode: status)
      }
      #expect(throws: (any Error).self) {
        try MLSPublicPDSReader.declaration(from: Data("upstream unavailable".utf8), statusCode: status)
      }
    }
  }

  @Test func malformedOrFailedDeclarationResponseRemainsRetryableFailure() throws {
    for body in ["not json", "{}", #"{"value":null}"#, #"{"value":{"allowIncoming":"none"}}"#] {
      #expect(throws: (any Error).self) {
        try MLSPublicPDSReader.declaration(from: Data(body.utf8), statusCode: 200)
      }
    }
    #expect(throws: (any Error).self) {
      try MLSPublicPDSReader.declaration(from: Data(#"{"error":"RecordNotFound"}"#.utf8), statusCode: 500)
    }
    let valid = Data(#"{"value":{"$type":"blue.catbird.chat.declaration","allowIncoming":"none","deliveryService":"did:web:chat.catbird.blue","protocolVersion":"1","createdAt":"2026-06-27T00:00:00.000Z"}}"#.utf8)
    #expect(try MLSPublicPDSReader.declaration(from: valid, statusCode: 200)?.allowIncoming == "none")
  }

  @Test func concurrentIdenticalAndOverlappingLookupsIssueEachUncachedDIDOnce() async throws {
    let cache = MLSChatAvailabilityCache(ttl: 60)
    let recorder = TestLookupRecorder()

    let alice = try DID(didString: "did:plc:alice")
    let bob = try DID(didString: "did:plc:bob")
    let charlie = try DID(didString: "did:plc:charlie")

    let gateBatch1Entered = TestGate()
    let gateAllowBatch1Finish = TestGate()

    let gateBatch2Entered = TestGate()
    let gateAllowBatch2Finish = TestGate()

    // Task 1: requests [alice, bob]
    async let task1 = cache.resolve(dids: [alice, bob]) { chunk in
      let keys = chunk.map { $0.didString() }
      await recorder.record(dids: keys)
      await gateBatch1Entered.open()
      await gateAllowBatch1Finish.pass()
      return [
        "did:plc:alice": .available,
        "did:plc:bob": .unavailable
      ]
    }

    // Wait until Task 1 has started its batch fetch and is in-flight
    await gateBatch1Entered.pass()

    // Task 2: requests [bob, charlie] overlapping with in-flight bob
    async let task2 = cache.resolve(dids: [bob, charlie]) { chunk in
      let keys = chunk.map { $0.didString() }
      await recorder.record(dids: keys)
      await gateBatch2Entered.open()
      await gateAllowBatch2Finish.pass()
      return [
        "did:plc:charlie": .available
      ]
    }

    // Wait until Task 2 has started its batch fetch for charlie
    await gateBatch2Entered.pass()

    // Task 3: duplicate request for [alice, bob] while both are in-flight
    async let task3 = cache.resolve(dids: [alice, bob]) { _ in
      Issue.record("Task 3 should not issue a new fetch because both DIDs are already in-flight")
      return [:]
    }

    // Now unblock the in-flight fetches
    await gateAllowBatch1Finish.open()
    await gateAllowBatch2Finish.open()

    let result1 = await task1
    let result2 = await task2
    let result3 = await task3

    // Verify results
    #expect(result1["did:plc:alice"] == .available)
    #expect(result1["did:plc:bob"] == .unavailable)

    #expect(result2["did:plc:bob"] == .unavailable)
    #expect(result2["did:plc:charlie"] == .available)

    #expect(result3["did:plc:alice"] == .available)
    #expect(result3["did:plc:bob"] == .unavailable)

    // Verify each DID was only fetched once
    #expect(await recorder.count(for: "did:plc:alice") == 1)
    #expect(await recorder.count(for: "did:plc:bob") == 1)
    #expect(await recorder.count(for: "did:plc:charlie") == 1)
  }

  @Test func freshQueryDoesNotWaitUnrelatedFlightBeforeStarting() async throws {
    let cache = MLSChatAvailabilityCache(ttl: 60)
    let alice = try DID(didString: "did:plc:alice")
    let bob = try DID(didString: "did:plc:bob")

    let gateAliceStarted = TestGate()
    let gateAliceFinish = TestGate()

    let gateBobStarted = TestGate()
    let gateBobFinish = TestGate()

    // Start long-running flight for alice
    async let taskAlice = cache.resolve(dids: [alice]) { _ in
      await gateAliceStarted.open()
      await gateAliceFinish.pass()
      return ["did:plc:alice": .available]
    }

    // Ensure alice is in flight
    await gateAliceStarted.pass()

    // Start query for [alice, bob] where alice is already in flight and bob is new
    async let taskCombined = cache.resolve(dids: [alice, bob]) { chunk in
      // Bob's fetch must start immediately, without waiting for Alice to finish
      let keys = chunk.map { $0.didString() }
      if keys.contains("did:plc:bob") {
        await gateBobStarted.open()
      }
      await gateBobFinish.pass()
      return ["did:plc:bob": .unavailable]
    }

    // Bob's fetch MUST start while Alice's fetch is still paused!
    await gateBobStarted.pass()

    // Now unblock both
    await gateBobFinish.open()
    await gateAliceFinish.open()

    let resAlice = await taskAlice
    let resCombined = await taskCombined

    #expect(resAlice["did:plc:alice"] == .available)
    #expect(resCombined["did:plc:alice"] == .available)
    #expect(resCombined["did:plc:bob"] == .unavailable)
  }

  @Test func invalidationDuringFlightPreventsLateCompletionFromRepopulatingCache() async throws {
    let cache = MLSChatAvailabilityCache(ttl: 60)
    let alice = try DID(didString: "did:plc:alice")

    let gateFlightStarted = TestGate()
    let gateAllowFlightFinish = TestGate()
    let recorder = TestLookupRecorder()

    // Start flight for alice
    async let firstLookup = cache.resolve(dids: [alice]) { chunk in
      await recorder.record(dids: chunk.map { $0.didString() })
      await gateFlightStarted.open()
      await gateAllowFlightFinish.pass()
      return ["did:plc:alice": .available]
    }

    // Wait until flight is actively running
    await gateFlightStarted.pass()

    // Invalidate alice while fetch is in-flight
    await cache.invalidate(did: "did:plc:alice")

    // Allow the original flight to complete
    await gateAllowFlightFinish.open()
    _ = await firstLookup

    // Late completion must NOT repopulate the cache
    let cachedAfterLateCompletion = await cache.get(did: "did:plc:alice")
    #expect(cachedAfterLateCompletion == nil)

    // Subsequent lookup must issue a fresh fetch rather than returning stale result
    let secondLookup = await cache.resolve(dids: [alice]) { chunk in
      await recorder.record(dids: chunk.map { $0.didString() })
      return ["did:plc:alice": .unavailable]
    }

    #expect(secondLookup["did:plc:alice"] == .unavailable)
    #expect(await recorder.count(for: "did:plc:alice") == 2)
    #expect(await cache.get(did: "did:plc:alice") == .unavailable)
  }

  @Test func clearDuringFlightPreventsLateCompletionFromRepopulatingCache() async throws {
    let cache = MLSChatAvailabilityCache(ttl: 60)
    let bob = try DID(didString: "did:plc:bob")

    let gateFlightStarted = TestGate()
    let gateAllowFlightFinish = TestGate()

    async let flight = cache.resolve(dids: [bob]) { _ in
      await gateFlightStarted.open()
      await gateAllowFlightFinish.pass()
      return ["did:plc:bob": .available]
    }

    await gateFlightStarted.pass()

    // Clear entire cache while fetch is in-flight
    await cache.clear()

    await gateAllowFlightFinish.open()
    _ = await flight

    #expect(await cache.get(did: "did:plc:bob") == nil)
  }

  @Test func unknownAvailabilityIsNeverCachedAndIsRetried() async throws {
    let cache = MLSChatAvailabilityCache(ttl: 60)
    let failing = try DID(didString: "did:plc:failing")
    let recorder = TestLookupRecorder()

    // First lookup: transient error returns .unknown
    let result1 = await cache.resolve(dids: [failing]) { chunk in
      await recorder.record(dids: chunk.map { $0.didString() })
      return ["did:plc:failing": .unknown]
    }

    #expect(result1["did:plc:failing"] == .unknown)
    #expect(await cache.get(did: "did:plc:failing") == nil)

    // Second lookup: must retry and fetch again
    let result2 = await cache.resolve(dids: [failing]) { chunk in
      await recorder.record(dids: chunk.map { $0.didString() })
      return ["did:plc:failing": .available]
    }

    #expect(result2["did:plc:failing"] == .available)
    #expect(await recorder.count(for: "did:plc:failing") == 2)
    #expect(await cache.get(did: "did:plc:failing") == .available)
  }

  @Test func boundedCacheEvictsOldestEntriesWhenCapacityExceeded() async throws {
    let cache = MLSChatAvailabilityCache(maxEntries: 2, ttl: 300)
    let did1 = try DID(didString: "did:plc:user1")
    let did2 = try DID(didString: "did:plc:user2")
    let did3 = try DID(didString: "did:plc:user3")

    _ = await cache.resolve(dids: [did1]) { _ in ["did:plc:user1": .available] }
    _ = await cache.resolve(dids: [did2]) { _ in ["did:plc:user2": .available] }

    #expect(await cache.get(did: "did:plc:user1") == .available)
    #expect(await cache.get(did: "did:plc:user2") == .available)

    // Adding a 3rd entry exceeds maxEntries: 2, so oldest (user1) must be evicted
    _ = await cache.resolve(dids: [did3]) { _ in ["did:plc:user3": .available] }

    #expect(await cache.get(did: "did:plc:user1") == nil)
    #expect(await cache.get(did: "did:plc:user2") == .available)
    #expect(await cache.get(did: "did:plc:user3") == .available)
  }
}

private actor RequestRecorder {
  var declarations: [String] = []
  var deviceRequests: [[String]] = []
  var attempts = 0
  func recordDeclaration(_ did: String) { declarations.append(did) }
  func recordDevices(_ dids: [String]) { deviceRequests.append(dids) }
  func recordAttempt() -> Int { attempts += 1; return attempts }
}

private actor TestGate {
  private var continuations: [CheckedContinuation<Void, Never>] = []
  private var isOpened = false

  func pass() async {
    if isOpened { return }
    await withCheckedContinuation { cont in
      continuations.append(cont)
    }
  }

  func open() {
    isOpened = true
    let toResume = continuations
    continuations.removeAll()
    for cont in toResume {
      cont.resume()
    }
  }
}

private actor TestLookupRecorder {
  var callCount: [String: Int] = [:]
  var batchCalls: [[String]] = []

  func record(dids: [String]) {
    batchCalls.append(dids)
    for did in dids {
      callCount[did, default: 0] += 1
    }
  }

  func count(for did: String) -> Int {
    callCount[did, default: 0]
  }
}
