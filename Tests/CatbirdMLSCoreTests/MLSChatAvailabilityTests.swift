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
}

private actor RequestRecorder {
  var declarations: [String] = []
  var deviceRequests: [[String]] = []
  var attempts = 0
  func recordDeclaration(_ did: String) { declarations.append(did) }
  func recordDevices(_ dids: [String]) { deviceRequests.append(dids) }
  func recordAttempt() -> Int { attempts += 1; return attempts }
}
