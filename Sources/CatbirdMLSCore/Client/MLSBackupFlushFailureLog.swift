import Foundation
import OSLog

/// Reports a failed encrypted-backup flush to the device log.
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
  private var lastFailure: String?
  private let logger = Logger(subsystem: "blue.catbird", category: "LabyrinthBackup")

  /// What a flush outcome should write to the log, if anything.
  enum Entry: Equatable {
    case failure(String)
    case recovered
  }

  /// Records a failed flush. Returns the entry to log, or nil when this failure repeats the last.
  func failed(_ error: Error) -> Entry? {
    let message = String(describing: error)
    lock.lock()
    defer { lock.unlock() }
    guard lastFailure != message else { return nil }
    lastFailure = message
    return .failure(message)
  }

  /// Records a successful flush. Returns `.recovered` when it follows a failure.
  func succeeded() -> Entry? {
    lock.lock()
    defer { lock.unlock() }
    guard lastFailure != nil else { return nil }
    lastFailure = nil
    return .recovered
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
