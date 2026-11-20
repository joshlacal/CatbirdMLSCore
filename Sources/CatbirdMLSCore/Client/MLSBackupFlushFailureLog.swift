import Foundation
import OSLog

/// How an account's most recent encrypted-backup flush ended.
public struct MLSBackupFlushOutcome: Equatable, Sendable {
  /// The engine's error text when the flush failed; nil when it succeeded.
  public let failure: String?
  public let date: Date

  public init(failure: String?, date: Date) {
    self.failure = failure
    self.date = date
  }
}

/// Reports a failed encrypted-backup flush to the device log, and remembers how each
/// account's last flush ended so a status screen can say whether backup is working.
///
/// The flush runs in the background after almost every chat action, and its error used to
/// be discarded. Backup could fail on every attempt for weeks with nothing to show for it:
/// in November 2026 it had never once succeeded in production and no log said so.
///
/// The same failure repeats on every trigger, so a message is written when it first appears
/// and again only when it changes, and once more when a flush succeeds after failing.
final class MLSBackupFlushFailureLog: @unchecked Sendable {
  static let shared = MLSBackupFlushFailureLog()

  private let lock = NSLock()
  private var lastFailure: [String: String] = [:]
  private var outcomes: [String: MLSBackupFlushOutcome] = [:]
  private let logger = Logger(subsystem: "blue.catbird", category: "LabyrinthBackup")

  /// What a flush outcome should write to the log, if anything.
  enum Entry: Equatable {
    case failure(String)
    case recovered
  }

  /// Records a failed flush. Returns the entry to log, or nil when this failure repeats the
  /// account's last one.
  func failed(_ error: Error, account: String = "", at date: Date = Date()) -> Entry? {
    let message = String(describing: error)
    lock.lock()
    defer { lock.unlock() }
    outcomes[account] = MLSBackupFlushOutcome(failure: message, date: date)
    guard lastFailure[account] != message else { return nil }
    lastFailure[account] = message
    return .failure(message)
  }

  /// Records a successful flush. Returns `.recovered` when it follows a failure.
  func succeeded(account: String = "", at date: Date = Date()) -> Entry? {
    lock.lock()
    defer { lock.unlock() }
    outcomes[account] = MLSBackupFlushOutcome(failure: nil, date: date)
    guard lastFailure.removeValue(forKey: account) != nil else { return nil }
    return .recovered
  }

  /// How this account's most recent flush ended; nil when none has run in this process.
  func lastOutcome(account: String) -> MLSBackupFlushOutcome? {
    lock.lock()
    defer { lock.unlock() }
    return outcomes[account]
  }

  func write(_ entry: Entry?) {
    switch entry {
    case .failure(let message):
      logger.error("Encrypted backup flush failed: \(message, privacy: .public)")
    case .recovered:
      logger.notice("Encrypted backup flush succeeded after earlier failures")
    case nil:
      break
    }
  }
}
