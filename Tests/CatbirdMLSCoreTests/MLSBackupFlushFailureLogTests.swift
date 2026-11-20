import Foundation
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

  @Test("Each account's last outcome is kept for a status screen")
  func lastOutcomeIsKeptPerAccount() {
    let log = MLSBackupFlushFailureLog()
    let first = Date(timeIntervalSince1970: 10)
    let second = Date(timeIntervalSince1970: 20)
    #expect(log.lastOutcome(account: "did:plc:a") == nil)

    _ = log.failed(Failure(description: "xrpc 404"), account: "did:plc:a", at: first)
    #expect(log.lastOutcome(account: "did:plc:a") == MLSBackupFlushOutcome(failure: "xrpc 404", date: first))
    #expect(log.lastOutcome(account: "did:plc:b") == nil)

    // A repeated failure is not logged again, but its time still moves forward.
    #expect(log.failed(Failure(description: "xrpc 404"), account: "did:plc:a", at: second) == nil)
    #expect(log.lastOutcome(account: "did:plc:a")?.date == second)

    _ = log.succeeded(account: "did:plc:a", at: second)
    #expect(log.lastOutcome(account: "did:plc:a") == MLSBackupFlushOutcome(failure: nil, date: second))
  }

  @Test("One account's failure does not hide the same failure on another account")
  func failuresAreTrackedPerAccount() {
    let log = MLSBackupFlushFailureLog()
    #expect(log.failed(Failure(description: "xrpc 404"), account: "did:plc:a") == .failure("xrpc 404"))
    #expect(log.failed(Failure(description: "xrpc 404"), account: "did:plc:b") == .failure("xrpc 404"))
    #expect(log.succeeded(account: "did:plc:a") == .recovered)
    #expect(log.failed(Failure(description: "xrpc 404"), account: "did:plc:b") == nil)
  }
}
