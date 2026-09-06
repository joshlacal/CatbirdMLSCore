import Foundation
import Petrel
import PetrelCatbird
import XCTest
@testable import CatbirdMLSCore

final class MLSInventoryRequestBackoffTests: XCTestCase {
  private typealias Scope = MLSInventoryRequestBackoff.Scope
  private let scope = Scope(service: "did:web:chat.catbird.blue", did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", deviceId: "22222222-2222-4222-8222-222222222222", endpoint: "blue.catbird.chat.getConversations")

  private func response(_ status: Int, _ retryAfter: String? = nil) -> (Data, HTTPURLResponse) {
    let headers = retryAfter.map { ["Retry-After": $0] } ?? [:]
    return (Data("{\"error\":\"RateLimited\"}".utf8), HTTPURLResponse(url: URL(string: "https://chat.catbird.blue/xrpc/blue.catbird.chat.getConversations")!, statusCode: status, httpVersion: "HTTP/1.1", headerFields: headers)!)
  }

  func testActual429SuppressesAnotherFreshAllocationWithoutHoldingWork() async throws {
    let gate = MLSInventoryRequestBackoff()
    let calls = Counter()
    let first = try await gate.perform(scope: scope, currentScope: { self.scope }) {
      await calls.increment()
      return self.response(429, "901")
    }
    XCTAssertEqual(first.1.statusCode, 429, "The original typed response remains intact")
    do {
      _ = try await gate.perform(scope: scope, currentScope: { self.scope }) {
        await calls.increment()
        return self.response(200)
      }
      XCTFail("Another allocation must fail promptly during the real Retry-After window")
    } catch let error as MLSAPIError {
      guard case let .rateLimited(delay) = error else { return XCTFail("Unexpected admission error") }
      XCTAssertGreaterThan(try XCTUnwrap(delay), 899)
    }
    let count = await calls.value
    XCTAssertEqual(count, 1)
  }

  func testRetainedReadsAndOtherAccountsBypassAllocationCooldown() async throws {
    let gate = MLSInventoryRequestBackoff()
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429, "901") }
    let other = Scope(service: scope.service, did: "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", deviceId: scope.deviceId, endpoint: scope.endpoint)
    let retained = try await gate.perform(scope: nil, currentScope: { throw ProbeError.identityWasRead }) { self.response(200) }
    let separate = try await gate.perform(scope: other, currentScope: { other }) { self.response(200) }
    XCTAssertEqual(retained.1.statusCode, 200)
    XCTAssertEqual(separate.1.statusCode, 200)
  }

  func testConversationCooldownDoesNotDenyOwnSnapshotReplacement() async throws {
    let gate = MLSInventoryRequestBackoff()
    let own = Scope(service: scope.service, did: scope.did, deviceId: scope.deviceId,
                    endpoint: "blue.catbird.chat.getOwnDevices")
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429, "901") }
    let replacement = try await gate.perform(scope: own, currentScope: { own }) { self.response(200) }
    XCTAssertEqual(replacement.1.statusCode, 200, "Shared-generation capacity does not prove own replacement is denied")
    _ = try await gate.perform(scope: own, currentScope: { own }) { self.response(429, "30") }
    do {
      _ = try await gate.perform(scope: own, currentScope: { own }) {
        XCTFail("An actual own-device denial must suppress another own allocation")
        return self.response(200)
      }
      XCTFail("Own-device cooldown must remain independent")
    } catch let error as MLSAPIError {
      guard case let .rateLimited(delay) = error else { return XCTFail("Unexpected admission error") }
      XCTAssertLessThanOrEqual(try XCTUnwrap(delay), 30, "The 901-second shared deadline must not leak into own admission")
    }
  }

  func testMissingOrInvalidHintUsesLocalPolicyOnlyForActual429() async throws {
    for header in [nil, "", "0", "-1", "+1", "1.5", "902", "999999999999999999999999999999999", "Sat, 05 Sep 2026 21:00:00 GMT"] as [String?] {
      let gate = MLSInventoryRequestBackoff()
      _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429, header) }
      do {
        _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(200) }
        XCTFail("A genuine429 without a usable header still needs local backoff")
      } catch MLSAPIError.rateLimited(let delay) {
        XCTAssertEqual(delay, 1, "Unsupported hints use client policy, not a fabricated server deadline")
      }
    }
    let gate = MLSInventoryRequestBackoff()
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(200, "901") }
    let admitted = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(200) }
    XCTAssertEqual(admitted.1.statusCode, 200)
  }

  func testCooldownUsesMonotonicTimeAndExpiresWithoutRefreshingIt() async throws {
    let clock = TestClock()
    let gate = MLSInventoryRequestBackoff(now: { clock.current })
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429, "12") }
    clock.advance(seconds: 11)
    do {
      _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(200) }
      XCTFail("Cooldown should still apply")
    } catch let error as MLSAPIError {
      guard case let .rateLimited(delay) = error else { return XCTFail("Wrong error") }
      XCTAssertEqual(delay, 1)
    }
    clock.advance(seconds: 1)
    let admitted = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(200) }
    XCTAssertEqual(admitted.1.statusCode, 200)
  }

  func testChangedAccountOrDeviceCannotIssueCapturedAllocation() async throws {
    let gate = MLSInventoryRequestBackoff()
    let calls = Counter()
    for changed in [
      Scope(service: scope.service, did: "different-account", deviceId: scope.deviceId, endpoint: scope.endpoint),
      Scope(service: scope.service, did: scope.did, deviceId: "different-device", endpoint: scope.endpoint)
    ] {
      do {
        _ = try await gate.perform(scope: scope, currentScope: { changed }) {
          await calls.increment()
          return self.response(200)
        }
        XCTFail("Identity must be rechecked before issuing the request")
      } catch {}
    }
    let count = await calls.value
    XCTAssertEqual(count, 0)
  }

  func testCancellationDoesNotIssueRequestOrCreateCooldown() async throws {
    let gate = MLSInventoryRequestBackoff()
    let calls = Counter()
    let capturedScope = scope
    let original = response(429, "901")
    let task = Task {
      withUnsafeCurrentTask { $0?.cancel() }
      return try await gate.perform(scope: capturedScope, currentScope: { capturedScope }) {
        await calls.increment()
        return original
      }
    }
    do { _ = try await task.value; XCTFail("Cancelled admission must throw") }
    catch is CancellationError {}
    let count = await calls.value
    XCTAssertEqual(count, 0)
    let admitted = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(200) }
    XCTAssertEqual(admitted.1.statusCode, 200)
  }

  func testOnlyUnscopedInitialInventoryRequestsParticipate() {
    let actor = URLQueryItem(name: "actorDeviceId", value: scope.deviceId)
    for endpoint in ["blue.catbird.chat.getConversations", "blue.catbird.chat.getOwnDevices"] {
      XCTAssertEqual(MLSInventoryRequestBackoff.allocationDevice(method: "GET", endpoint: endpoint, query: [actor]), scope.deviceId)
      for name in ["pageCursor", "inventorySessionId"] {
        XCTAssertNil(MLSInventoryRequestBackoff.allocationDevice(method: "GET", endpoint: endpoint, query: [actor, URLQueryItem(name: name, value: "retained")]))
      }
      XCTAssertNil(MLSInventoryRequestBackoff.allocationDevice(method: "POST", endpoint: endpoint, query: [actor]))
    }
    for endpoint in ["getConversations", "blue.catbird.chat.getEntries", "blue.catbird.chat.getConversationState", "blue.catbird.chat.getSubscriptionTicket", "blue.catbird.chat.subscribeEvents"] {
      XCTAssertNil(MLSInventoryRequestBackoff.allocationDevice(method: "GET", endpoint: endpoint, query: [actor]))
    }
  }

  func testBlockedAllocationDoesNotBlockRetainedReadOrDurableCursorAction() async throws {
    let gate = MLSInventoryRequestBackoff()
    let captured = scope
    let success = response(200)
    _ = try await gate.perform(scope: scope, currentScope: { captured }) { self.response(429, "901") }
    let blocked = Task {
      do {
        _ = try await gate.perform(scope: captured, currentScope: { captured }) { success }
        return false
      } catch let error as MLSAPIError {
        if case .rateLimited = error { return true }
        return false
      }
    }
    let saved = CursorProbe()
    let message = BlueCatbirdChatDefs.SubscriptionMessage.blueCatbirdChatDefsEventEnvelope(
      BlueCatbirdChatDefs.EventEnvelope(previousCursor: "retained-before", cursor: "retained-after",
        payload: .blueCatbirdChatDefsWatermarkEvent(BlueCatbirdChatDefs.WatermarkEvent(issuedAt: ATProtocolDate(date: Date()))),
        createdAt: ATProtocolDate(date: Date())))
    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      message, subscriptionKey: "__global__", expectedPreviousCursor: "retained-before",
      loadEntries: { _, _ in throw ProbeError.identityWasRead },
      onDurableEvent: { _ in
        let retained = try await gate.perform(scope: nil, currentScope: { throw ProbeError.identityWasRead }) { success }
        XCTAssertEqual(retained.1.statusCode, 200)
      },
      saveCursor: { await saved.save($0) })
    guard case .handled = result else { return XCTFail("Unrelated durable handling must remain independent") }
    let denied = try await blocked.value
    let cursor = await saved.value
    XCTAssertTrue(denied)
    XCTAssertEqual(cursor, "retained-after")
  }

  func testRecoveryHintNeedingFreshInventoryLeavesCursorForReplayDuringCooldown() async throws {
    let gate = MLSInventoryRequestBackoff()
    let captured = scope
    _ = try await gate.perform(scope: scope, currentScope: { captured }) { self.response(429, "901") }
    let saved = CursorProbe()
    let message = BlueCatbirdChatDefs.SubscriptionMessage.blueCatbirdChatDefsEventEnvelope(
      BlueCatbirdChatDefs.EventEnvelope(previousCursor: "retained-before", cursor: "blocked-after",
        payload: .blueCatbirdChatDefsConversationChangedEvent(BlueCatbirdChatDefs.ConversationChangedEvent(conversationId: "11111111-1111-4111-8111-111111111111")),
        createdAt: ATProtocolDate(date: Date())))
    let result = await MLSCanonicalTransportAdapter.handleCanonicalStreamMessage(
      message, subscriptionKey: "__global__", expectedPreviousCursor: "retained-before",
      loadEntries: { _, _ in [] },
      onDurableEvent: { _ in
        _ = try await gate.perform(scope: captured, currentScope: { captured }) { self.response(200) }
      }, saveCursor: { await saved.save($0) })
    guard case .reconnect = result else { return XCTFail("Incomplete recovery must preserve its durable barrier") }
    let cursor = await saved.value
    XCTAssertNil(cursor)
  }

  func testRawInitialDecoderPreservesGeneratedErrorAndContentTypeChecks() throws {
    let (data, rateLimited) = response(429, "901")
    XCTAssertThrowsError(try MLSAPIClient.decodeInitialConversationInventory(data: data, response: rateLimited)) { error in
      XCTAssertTrue(error is ATProtoError<BlueCatbirdChatGetConversations.Error>)
    }
    let (_, wrongType) = response(200)
    XCTAssertThrowsError(try MLSAPIClient.decodeInitialConversationInventory(data: Data("{}".utf8), response: wrongType)) { error in
      guard case NetworkError.invalidContentType = error else { return XCTFail("Success still requires JSON content type") }
    }
    let valid = Data("""
      {"items":[],"inventorySessionId":"retained-session","snapshotEventCursor":"opaque-cursor","hasMore":false,"snapshotExpiresAt":"2026-09-05T20:00:00.000Z"}
      """.utf8)
    let http = HTTPURLResponse(url: wrongType.url!, statusCode: 200, httpVersion: "HTTP/1.1", headerFields: ["Content-Type": "application/json"])!
    let decoded = try MLSAPIClient.decodeInitialConversationInventory(data: valid, response: http)
    XCTAssertEqual(decoded.inventorySessionId, "retained-session")
    XCTAssertEqual(decoded.snapshotEventCursor, "opaque-cursor")
  }

  private enum ProbeError: Error { case identityWasRead }
  private actor Counter { var value = 0; func increment() { value += 1 } }
  private actor CursorProbe { var value: String?; func save(_ cursor: String) { value = cursor } }
  private final class TestClock: @unchecked Sendable {
    private let lock = NSLock()
    private var instant = ContinuousClock().now
    var current: ContinuousClock.Instant { lock.withLock { instant } }
    func advance(seconds: Int) { lock.withLock { instant = instant.advanced(by: .seconds(seconds)) } }
  }
}
