//
//  MLSDiagnosticsTests.swift
//  CatbirdMLSCoreTests
//

import Foundation
import XCTest
@testable import CatbirdMLSCore

final class MLSDiagnosticsTests: XCTestCase {
  private var testDefaults: UserDefaults!
  private var suiteName: String!

  override func setUp() {
    super.setUp()
    suiteName = "MLSDiagnosticsTests.\(UUID().uuidString)"
    testDefaults = UserDefaults(suiteName: suiteName)!
    MLSSuspensionFlightRecorder.setDefaultsOverrideForTesting(testDefaults)
    MLSDiagnostics.reporter = nil
    MLSDiagnostics.clear()
  }

  override func tearDown() {
    MLSDiagnostics.clear()
    MLSDiagnostics.reporter = nil
    MLSSuspensionFlightRecorder.setDefaultsOverrideForTesting(nil)
    testDefaults.removePersistentDomain(forName: suiteName)
    testDefaults = nil
    super.tearDown()
  }

  func testRingBufferNewestFirstOrderAndCap() {
    // Record 210 items (cap is 200)
    for i in 0..<210 {
      MLSDiagnostics.record(
        .sendFailed,
        code: "Code_\(i)",
        conversation: "convo_\(i)",
        attempt: i
      )
    }

    let records = MLSDiagnostics.recent(limit: 250)
    // Buffer must be capped at 200
    XCTAssertEqual(records.count, 200)

    // First item must be the newest (i = 209)
    XCTAssertEqual(records.first?.code, "Code_209")
    XCTAssertEqual(records.first?.attempt, 209)

    // Last item must be the oldest surviving (i = 10, since 210 - 200 = 10)
    XCTAssertEqual(records.last?.code, "Code_10")
    XCTAssertEqual(records.last?.attempt, 10)

    // Sub-limit should also be honoured in newest-first order
    let sub = MLSDiagnostics.recent(limit: 5)
    XCTAssertEqual(sub.count, 5)
    XCTAssertEqual(sub.map(\.code), ["Code_209", "Code_208", "Code_207", "Code_206", "Code_205"])
  }

  func testReporterSinkReceivesRecordAndBufferOnlyMode() {
    // 1. Buffer-only mode (reporter is nil)
    XCTAssertNil(MLSDiagnostics.reporter)
    MLSDiagnostics.record(.streamPaused, code: "RateLimited", conversation: "convo-123")
    let bufferOnlyRecords = MLSDiagnostics.recent(limit: 10)
    XCTAssertEqual(bufferOnlyRecords.count, 1)
    XCTAssertEqual(bufferOnlyRecords.first?.code, "RateLimited")

    // 2. Reporter sink attached
    final class RecordBox: @unchecked Sendable {
      var records: [MLSDiagnosticRecord] = []
      private let lock = NSLock()
      func append(_ record: MLSDiagnosticRecord) {
        lock.lock()
        defer { lock.unlock() }
        records.append(record)
      }
      func get() -> [MLSDiagnosticRecord] {
        lock.lock()
        defer { lock.unlock() }
        return records
      }
    }

    let box = RecordBox()

    MLSDiagnostics.reporter = { record in
      box.append(record)
    }

    MLSDiagnostics.record(
      .sendRecovered,
      code: "SuccessAfterRetry",
      conversation: "convo-456",
      attempt: 2
    )

    let reportedRecords = box.get()
    XCTAssertEqual(reportedRecords.count, 1)
    guard let reported = reportedRecords.first else {
      return XCTFail("Expected a reported record")
    }
    XCTAssertEqual(reported.code, "SuccessAfterRetry")
    XCTAssertEqual(reported.conversationIDPrefix, "convo-456")
    XCTAssertEqual(reported.attempt, 2)

    // Ring buffer also has both records
    let allRecords = MLSDiagnostics.recent(limit: 10)
    XCTAssertEqual(allRecords.count, 2)
  }

  func testExportTextPrivacyAndRedaction() {
    let fullDID = "did:plc:abcdef1234567890abcdef"
    let bearerToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.supersecretpayload"
    let plaintext = "Top secret message content: do not leak"

    MLSDiagnostics.record(
      .sendFailed,
      code: "StaleCoordinates",
      conversation: "conversation-long-id-123456789",
      epoch: 42,
      generation: 7,
      stateVersion: 100,
      retryAfter: 15.0,
      attempt: 2,
      detail: [
        "userDID": fullDID,
        "token": bearerToken,
        "plaintext": plaintext,
        "message": "also should be dropped"
      ]
    )

    let text = MLSDiagnostics.exportText()

    // Must contain coordinates, codes, attempt, retryAfter
    XCTAssertTrue(text.contains("StaleCoordinates"))
    XCTAssertTrue(text.contains("epoch=42"))
    XCTAssertTrue(text.contains("gen=7"))
    XCTAssertTrue(text.contains("stateVersion=100"))
    XCTAssertTrue(text.contains("attempt=2"))
    XCTAssertTrue(text.contains("retryAfter=15.0s"))
    XCTAssertTrue(text.contains("convo=conversation-lon")) // truncated to 16 chars

    // MUST NOT contain full DID, token-like string, or plaintext
    XCTAssertFalse(text.contains(fullDID), "Export text must not contain full DID")
    XCTAssertFalse(text.contains(bearerToken), "Export text must not contain token")
    XCTAssertFalse(text.contains(plaintext), "Export text must not contain message plaintext")
    XCTAssertFalse(text.contains("also should be dropped"))
  }

  func testErrorCodeAndCoordinateExtraction() {
    // 1. 400 StaleCoordinates
    let staleError = OrchestratorBridgeError.ServerError(
      status: 400,
      body: #"{"error":"StaleCoordinates","stateVersion":0}"#
    )
    XCTAssertEqual(MLSDiagnostics.errorCode(from: staleError), "StaleCoordinates")
    let staleCoords = MLSDiagnostics.extractCoordinates(from: staleError)
    XCTAssertEqual(staleCoords.stateVersion, 0)

    // 2. 429 RateLimited with retryAfter
    let rateLimitError = OrchestratorBridgeError.ServerError(
      status: 429,
      body: #"{"error":"RateLimited","retryAfter":240}"#
    )
    XCTAssertEqual(MLSDiagnostics.errorCode(from: rateLimitError), "RateLimited")
    XCTAssertEqual(MLSDiagnostics.extractRetryAfter(from: rateLimitError), 240)

    // 3. RecoveryFailed with SecretReuse message
    let recoveryError = OrchestratorBridgeError.RecoveryFailed(
      message: "SecretReuse without durable envelope evidence for epoch 4"
    )
    XCTAssertEqual(MLSDiagnostics.errorCode(from: recoveryError), "RecoveryFailed")
    let recoveryDetail = MLSDiagnostics.extractDetail(from: recoveryError)
    XCTAssertEqual(recoveryDetail["subcode"], "SecretReuseWithoutDurableEnvelope")

    // 4. RecipientNotReady / DeviceAccessPending
    let pendingError = OrchestratorBridgeError.InvalidInput(
      message: "conversation_device_access_pending: waiting for peer acceptance"
    )
    XCTAssertEqual(MLSDiagnostics.errorCode(from: pendingError), "RecipientNotReady")

    // 5. 502 Upstream Timeout
    let upstreamError = OrchestratorBridgeError.ServerError(status: 502, body: "Bad Gateway")
    XCTAssertEqual(MLSDiagnostics.errorCode(from: upstreamError), "HTTP_502")

    // 6. Generation Mismatch
    let genMismatchError = OrchestratorBridgeError.Api(
      message: "MLS Coordination generation mismatch: expected 4 got 3"
    )
    XCTAssertEqual(MLSDiagnostics.errorCode(from: genMismatchError), "GenerationMismatch")
  }
}
