import Foundation
import MLSFFI
import OSLog

/// Swift implementation of MLSLogger that bridges Rust FFI logs to OSLog
///
/// Usage:
/// ```swift
/// let context = MlsContext()
/// let logger = MLSLoggerImplementation()
/// context.setLogger(logger: logger)
/// ```
public class MLSLoggerImplementation: MlsLogger {
  private let logger = Logger(
    subsystem: "blue.catbird.mls",
    category: "MLSFFI"
  )

  public init() {}

  /// Receive log messages from Rust FFI and forward to OSLog
  public func log(level: String, message: String) {
    // Suppress repetitive non-critical logs
    if shouldSuppressLog(message) {
      return
    }

    switch level.lowercased() {
    case "debug":
      logger.debug("\(message, privacy: .public)")
    case "info":
      logger.info("\(message, privacy: .public)")
    case "warning":
      logger.warning("\(message, privacy: .public)")
    case "error":
      logger.error("\(message, privacy: .public)")
    default:
      logger.log("\(message, privacy: .public)")
    }
  }

  private func shouldSuppressLog(_ message: String) -> Bool {
    // Suppress verbose bundle storage logs
    if message.contains("stored/updated in provider storage") {
      return true
    }
    if message.contains("[MLS-CONTEXT]") && message.contains("Bundle") {
      return true
    }
    // Suppress duplicate key package warnings (these are expected and harmless)
    if message.contains("Duplicate key package detected") {
      return true
    }
    return false
  }
}
