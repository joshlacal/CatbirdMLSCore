import Foundation
import XCTest
@testable import CatbirdMLSCore

final class MLSInventoryFallbackBackoffTests: XCTestCase, @unchecked Sendable {
  private let scope = MLSInventoryRequestBackoff.Scope(service: "did:web:synthetic.invalid", did: "did:plc:synthetic", deviceId: "synthetic-device", endpoint: "blue.catbird.chat.getConversations")
  private func response(_ status: Int, _ header: String? = nil) -> (Data, HTTPURLResponse) {
    (Data("{\"error\":\"RateLimited\",\"message\":\"RateLimited\"}".utf8), HTTPURLResponse(url: URL(string: "https://synthetic.invalid")!, statusCode: status, httpVersion: "HTTP/1.1", headerFields: header.map { ["Retry-After": $0] } ?? [:])!)
  }
  private func assertDenied(_ gate: MLSInventoryRequestBackoff, seconds: Double, file: StaticString = #filePath, line: UInt = #line) async throws {
    do {
      _ = try await gate.perform(scope: scope, currentScope: { self.scope }) {
        XCTFail("Denied request reached transport", file: file, line: line)
        return self.response(200)
      }
      XCTFail("Missing local retry denial", file: file, line: line)
    } catch MLSAPIError.rateLimited(let delay) { XCTAssertEqual(delay, seconds, file: file, line: line) }
  }
  func testMissingHintEscalatesCapsAndLaterSuccessResetsLocalPolicy() async throws {
    let clock = Clock(); let gate = MLSInventoryRequestBackoff(now: { clock.now })
    for seconds in [1, 2, 4, 8, 16, 30, 30, 30] {
      let original = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }
      XCTAssertEqual(original.1.statusCode, 429)
      XCTAssertNil(original.1.value(forHTTPHeaderField: "Retry-After"), "Never fabricate a server header")
      try await assertDenied(gate, seconds: Double(seconds)); clock.advance(seconds)
    }
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(200) }
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }
    try await assertDenied(gate, seconds: 1)
  }
  func testValidServerHintOverridesFallbackAndIsNotShortened() async throws {
    let clock = Clock(); let gate = MLSInventoryRequestBackoff(now: { clock.now })
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }; clock.advance(1)
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429, "231") }
    try await assertDenied(gate, seconds: 231); clock.advance(230)
    try await assertDenied(gate, seconds: 1); clock.advance(1)
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }
    try await assertDenied(gate, seconds: 2)
  }
  func testConcurrentStartedWaveCountsAsOneFallbackStep() async throws {
    let clock = Clock(); let gate = MLSInventoryRequestBackoff(now: { clock.now }); let wave = Wave(count: 24)
    let tasks = (0..<24).map { _ in Task {
      try await gate.perform(scope: self.scope, currentScope: { self.scope }) {
        await wave.arrive(); return self.response(429)
      }
    } }
    await wave.waitForAll(); await wave.release()
    for task in tasks { let result = try await task.value; XCTAssertEqual(result.1.statusCode, 429) }
    try await assertDenied(gate, seconds: 1); clock.advance(1)
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }
    try await assertDenied(gate, seconds: 2)
  }
  func testOlderSuccessCannotClearNewerDenial() async throws {
    let clock = Clock(); let gate = MLSInventoryRequestBackoff(now: { clock.now }); let old = Wave(count: 1)
    let success = Task {
      try await gate.perform(scope: self.scope, currentScope: { self.scope }) {
        await old.arrive(); return self.response(200)
      }
    }
    await old.waitForAll()
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }
    await old.release(); _ = try await success.value
    try await assertDenied(gate, seconds: 1)
  }
  func testLateSameWaveDenialDoesNotLetAlreadyStartedSuccessClearIt() async throws {
    let clock = Clock(); let gate = MLSInventoryRequestBackoff(now: { clock.now }); let old = Wave(count: 1)
    let lateDenial = Task {
      try await gate.perform(scope: self.scope, currentScope: { self.scope }) {
        await old.arrive(); return self.response(429)
      }
    }
    await old.waitForAll()
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }; clock.advance(1)
    let later = Wave(count: 1)
    let success = Task {
      try await gate.perform(scope: self.scope, currentScope: { self.scope }) {
        await later.arrive(); return self.response(200)
      }
    }
    await later.waitForAll(); await old.release(); _ = try await lateDenial.value
    await later.release(); _ = try await success.value
    try await assertDenied(gate, seconds: 1); clock.advance(1)
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }
    try await assertDenied(gate, seconds: 2)
  }
  func testIdleHistoryRetiresAfterExistingMaximumHintHorizon() async throws {
    let clock = Clock(); let gate = MLSInventoryRequestBackoff(now: { clock.now })
    for seconds in [1,2,4] {
      _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }; clock.advance(seconds)
    }
    clock.advance(901)
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }
    try await assertDenied(gate, seconds: 1)
  }
  func testFallbackIsScopedAndRetainedRequestsStayIndependent() async throws {
    let clock = Clock(); let gate = MLSInventoryRequestBackoff(now: { clock.now })
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }
    let retained = try await gate.perform(scope: nil, currentScope: { XCTFail("Retained identity was consulted"); return nil }) { self.response(200) }
    XCTAssertEqual(retained.1.statusCode, 200)
    for other in [
      MLSInventoryRequestBackoff.Scope(service: "other-service", did: scope.did, deviceId: scope.deviceId, endpoint: scope.endpoint),
      MLSInventoryRequestBackoff.Scope(service: scope.service, did: "other-account", deviceId: scope.deviceId, endpoint: scope.endpoint),
      MLSInventoryRequestBackoff.Scope(service: scope.service, did: scope.did, deviceId: "other-device", endpoint: scope.endpoint),
      MLSInventoryRequestBackoff.Scope(service: scope.service, did: scope.did, deviceId: scope.deviceId, endpoint: "blue.catbird.chat.getOwnDevices")
    ] {
      let result = try await gate.perform(scope: other, currentScope: { other }) { self.response(200) }
      XCTAssertEqual(result.1.statusCode, 200)
    }
    try await assertDenied(gate, seconds: 1)
  }
  func testCancellationDuringIdentityRecheckNeverEntersTransport() async throws {
    let gate = MLSInventoryRequestBackoff(); let identity = Wave(count: 1)
    let request = Task {
      try await gate.perform(scope: self.scope, currentScope: {
        await identity.arrive(); return self.scope
      }) { XCTFail("Cancelled identity recheck entered transport"); return self.response(429) }
    }
    await identity.waitForAll(); request.cancel(); await identity.release()
    do { _ = try await request.value; XCTFail("Cancellation must propagate") }
    catch is CancellationError {}
    let result = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(200) }
    XCTAssertEqual(result.1.statusCode, 200, "Cancellation creates no retry deadline")
  }
  func testNonSuccessDoesNotResetFallbackOrInventNewDenial() async throws {
    let clock = Clock(); let gate = MLSInventoryRequestBackoff(now: { clock.now })
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }; clock.advance(1)
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(503, "901") }
    _ = try await gate.perform(scope: scope, currentScope: { self.scope }) { self.response(429) }
    try await assertDenied(gate, seconds: 2, file: #filePath, line: #line)
  }
  private final class Clock: @unchecked Sendable {
    private let lock = NSLock(); private var instant = ContinuousClock.now
    var now: ContinuousClock.Instant { lock.withLock { instant } }
    func advance(_ seconds: Int) { lock.withLock { instant = instant.advanced(by: .seconds(seconds)) } }
  }
  private actor Wave {
    let count: Int; var requests: [CheckedContinuation<Void, Never>] = []; var waiter: CheckedContinuation<Void, Never>?
    init(count: Int) { self.count = count }
    func arrive() async {
      await withCheckedContinuation { continuation in
        requests.append(continuation)
        if requests.count == count { waiter?.resume(); waiter = nil }
      }
    }
    func waitForAll() async {
      if requests.count == count { return }
      await withCheckedContinuation { waiter = $0 }
    }
    func release() { for request in requests { request.resume() }; requests.removeAll() }
  }
}
