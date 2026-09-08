import XCTest

@testable import CatbirdMLSCore

/// A real diagnostics export read `code=UnknownError` seventeen times in
/// eighteen seconds: the trail named neither the failure nor anything a reader
/// could act on, and one looping conversation buried every other record.
final class MLSDiagnosticsIdentifiabilityTests: XCTestCase {
  private struct PipelineFailure: Error {}

  /// Sendable sink so the reporter closure can collect records under Swift's
  /// concurrency checking.
  private final class Sink: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [MLSDiagnosticRecord] = []
    func append(_ record: MLSDiagnosticRecord) {
      lock.lock(); defer { lock.unlock() }
      storage.append(record)
    }
    var records: [MLSDiagnosticRecord] {
      lock.lock(); defer { lock.unlock() }
      return storage
    }
  }

  override func setUp() {
    super.setUp()
    MLSDiagnostics.reporter = nil
    MLSDiagnostics.clear()
  }

  override func tearDown() {
    MLSDiagnostics.reporter = nil
    MLSDiagnostics.clear()
    super.tearDown()
  }

  func testUnmappedErrorReportsItsConcreteTypeRatherThanUnknown() {
    let code = MLSDiagnostics.errorCode(from: PipelineFailure())

    XCTAssertNotEqual(code, "UnknownError")
    XCTAssertTrue(code.contains("PipelineFailure"), "code should name the type, got \(code)")
  }

  func testRecordingFromAnErrorCarriesARedactedSummary() {
    let sink = Sink()
    MLSDiagnostics.reporter = { sink.append($0) }

    MLSDiagnostics.record(.conversationLoadFailed, error: PipelineFailure(), conversation: "5ad64d82-4b69-42e1")

    let records = sink.records
    XCTAssertEqual(records.count, 1)
    XCTAssertEqual(records.first?.detail["errorType"], "PipelineFailure")
    XCTAssertFalse((records.first?.detail["errorSummary"] ?? "").isEmpty)
  }

  func testSummaryRedactsTokenShapedAndOversizedValues() {
    struct LeakyFailure: Error, CustomStringConvertible {
      var description: String {
        "rejected token eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payloadpayloadpayload for did:plc:34x52srgxttjewbke5hguloh"
      }
    }

    let summary = MLSDiagnostics.errorSummary(from: LeakyFailure())

    XCTAssertFalse(summary.contains("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"))
    XCTAssertFalse(summary.contains("did:plc:34x52srgxttjewbke5hguloh"))
    XCTAssertTrue(summary.contains("rejected token"))
    XCTAssertLessThanOrEqual(summary.count, 180)
  }

  func testIdenticalFailureStormCollapsesToOneRecord() {
    let sink = Sink()
    MLSDiagnostics.reporter = { sink.append($0) }

    for _ in 0..<17 {
      MLSDiagnostics.record(
        .conversationLoadFailed, code: "GroupMissing", conversation: "5ad64d82-4b69-42e1"
      )
    }

    XCTAssertEqual(sink.records.count, 1)
    XCTAssertEqual(MLSDiagnostics.recent(limit: 50).count, 1)
  }

  func testDistinctFailuresAreNotSuppressed() {
    let sink = Sink()
    MLSDiagnostics.reporter = { sink.append($0) }

    MLSDiagnostics.record(.conversationLoadFailed, code: "GroupMissing", conversation: "5ad64d82")
    MLSDiagnostics.record(.conversationLoadFailed, code: "RateLimited", conversation: "5ad64d82")
    MLSDiagnostics.record(.conversationLoadFailed, code: "GroupMissing", conversation: "29154b98")
    MLSDiagnostics.record(.sendFailed, code: "GroupMissing", conversation: "5ad64d82")

    XCTAssertEqual(sink.records.count, 4)
  }
}
