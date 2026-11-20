import Testing
@testable import CatbirdMLSCore

/// A failing backup flush is reported, without repeating itself on every chat action.
@Suite("MLS Backup Flush Failure Log Tests")
struct MLSBackupFlushFailureLogTests {
  private struct Failure: Error, CustomStringConvertible {
    let description: String
  }

  @Test("The first failure is reported")
  func firstFailureIsReported() {
    let log = MLSBackupFlushFailureLog()
    #expect(log.failed(Failure(description: "xrpc 404")) == .failure("xrpc 404"))
  }

  @Test("The same failure on the next attempts is not repeated")
  func repeatedFailureIsQuiet() {
    let log = MLSBackupFlushFailureLog()
    _ = log.failed(Failure(description: "xrpc 404"))
    #expect(log.failed(Failure(description: "xrpc 404")) == nil)
    #expect(log.failed(Failure(description: "xrpc 404")) == nil)
  }

  @Test("A different failure is reported")
  func changedFailureIsReported() {
    let log = MLSBackupFlushFailureLog()
    _ = log.failed(Failure(description: "xrpc 404"))
    #expect(log.failed(Failure(description: "xrpc 403")) == .failure("xrpc 403"))
  }

  @Test("A success after a failure is reported once, and the failure can be reported again later")
  func recoveryIsReportedOnce() {
    let log = MLSBackupFlushFailureLog()
    #expect(log.succeeded() == nil)
    _ = log.failed(Failure(description: "xrpc 404"))
    #expect(log.succeeded() == .recovered)
    #expect(log.succeeded() == nil)
    #expect(log.failed(Failure(description: "xrpc 404")) == .failure("xrpc 404"))
  }
}
