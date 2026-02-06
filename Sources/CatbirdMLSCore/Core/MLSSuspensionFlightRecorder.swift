//
//  MLSSuspensionFlightRecorder.swift
//  CatbirdMLSCore
//
//  Flight recorder for debugging 0xdead10cc crashes (file lock held during suspension).
//
//  This records suspension-related events in a ring buffer stored in App Group UserDefaults.
//  On next launch after a crash, the flight recorder can be examined to diagnose what
//  happened in the moments before the crash.
//
//  Design:
//  - Ring buffer of 50 entries to limit storage size
//  - JSON-encoded for easy inspection
//  - Stored in App Group for cross-process visibility (NSE can also write)
//  - Thread-safe via DispatchQueue
//

import Foundation
import OSLog

/// Records suspension-related events for post-crash analysis.
/// Helps diagnose 0xdead10cc crashes by providing a trail of events leading up to the crash.
public final class MLSSuspensionFlightRecorder: @unchecked Sendable {
  public static let shared = MLSSuspensionFlightRecorder()

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "FlightRecorder")
  private let suiteName = "group.blue.catbird.shared"
  private let storageKey = "mls_suspension_flight_recorder"
  private let maxEntries = 50
  private let queue = DispatchQueue(label: "blue.catbird.mls.flightRecorder")

  /// Event types for the flight recorder
  public enum EventType: String, Codable {
    case scenePhaseChange = "scene_phase"
    case locksReleased = "locks_released"
    case flushStarted = "flush_started"
    case flushCompleted = "flush_completed"
    case flushTimeout = "flush_timeout"
    case flushFailed = "flush_failed"
    case checkpointStarted = "checkpoint_started"
    case checkpointCompleted = "checkpoint_completed"
    case checkpointSkipped = "checkpoint_skipped"
    case checkpointFailed = "checkpoint_failed"
    case nseYield = "nse_yield"
    case nseLockContention = "nse_lock_contention"
    case suspensionPrepare = "suspension_prepare"
    case resumeFromSuspension = "resume"
    case backgroundTaskExpired = "bg_task_expired"
    case serviceExtensionExpired = "service_ext_expired"
    case lockAcquired = "lock_acquired"
    case lockReleased = "lock_released"
    case coordinationTimeout = "coordination_timeout"
    case memoryWarning = "memory_warning"
  }

  /// A single flight recorder entry
  public struct Entry: Codable {
    public let timestamp: Date
    public let event: EventType
    public let details: String
    public let heldLocks: [String]
    public let process: String  // "app" or "nse"

    public init(
      timestamp: Date = Date(),
      event: EventType,
      details: String,
      heldLocks: [String] = [],
      process: String = "app"
    ) {
      self.timestamp = timestamp
      self.event = event
      self.details = details
      self.heldLocks = heldLocks
      self.process = process
    }
  }

  private var defaults: UserDefaults? {
    UserDefaults(suiteName: suiteName)
  }

  private init() {}

  /// Record an event to the flight recorder
  /// - Parameters:
  ///   - event: The type of event
  ///   - details: Additional context about the event
  ///   - heldLocks: List of lock identifiers currently held (if known)
  ///   - process: Which process is recording ("app" or "nse")
  public func record(
    _ event: EventType,
    details: String = "",
    heldLocks: [String] = [],
    process: String = "app"
  ) {
    queue.async { [weak self] in
      guard let self = self, let defaults = self.defaults else { return }

      let entry = Entry(
        event: event,
        details: details,
        heldLocks: heldLocks,
        process: process
      )

      // Load existing entries
      var entries = self.loadEntries()

      // Append new entry
      entries.append(entry)

      // Trim to max size (ring buffer)
      if entries.count > self.maxEntries {
        entries = Array(entries.suffix(self.maxEntries))
      }

      // Save back
      self.saveEntries(entries, to: defaults)

      // Also log for real-time debugging
      self.logger.info(
        "📼 [FlightRecorder] \(event.rawValue): \(details.isEmpty ? "-" : details, privacy: .public)"
      )
    }
  }

  /// Get all recorded entries (for diagnostics display)
  /// - Returns: Array of flight recorder entries, newest last
  public func getEntries() -> [Entry] {
    return queue.sync {
      loadEntries()
    }
  }

  /// Get entries as a formatted string for logging/display
  /// - Returns: Human-readable log of recent events
  public func getFormattedLog() -> String {
    let entries = getEntries()
    guard !entries.isEmpty else {
      return "No flight recorder entries"
    }

    let formatter = DateFormatter()
    formatter.dateFormat = "HH:mm:ss.SSS"

    var lines: [String] = ["=== MLS Suspension Flight Recorder ==="]
    for entry in entries {
      let time = formatter.string(from: entry.timestamp)
      let locks = entry.heldLocks.isEmpty ? "" : " locks=[\(entry.heldLocks.joined(separator: ","))]"
      lines.append("[\(time)] [\(entry.process)] \(entry.event.rawValue): \(entry.details)\(locks)")
    }
    lines.append("=== End Flight Recorder ===")

    return lines.joined(separator: "\n")
  }

  /// Clear all recorded entries (call after successful recovery/analysis)
  public func clear() {
    queue.async { [weak self] in
      guard let self = self, let defaults = self.defaults else { return }
      defaults.removeObject(forKey: self.storageKey)
      self.logger.info("📼 [FlightRecorder] Cleared all entries")
    }
  }

  /// Check if there are entries from a previous session (potential crash investigation)
  /// - Returns: Number of entries from before current process started
  public func hasPreviousSessionEntries() -> Bool {
    let entries = getEntries()
    guard !entries.isEmpty else { return false }

    // Check if oldest entry is from before our process started
    // Simple heuristic: if we have entries, assume they might be from previous session
    // A more sophisticated check would store process launch time
    return !entries.isEmpty
  }

  // MARK: - Private Helpers

  private func loadEntries() -> [Entry] {
    guard let defaults = defaults,
          let data = defaults.data(forKey: storageKey) else {
      return []
    }

    do {
      return try JSONDecoder().decode([Entry].self, from: data)
    } catch {
      logger.warning("📼 [FlightRecorder] Failed to decode entries: \(error.localizedDescription)")
      return []
    }
  }

  private func saveEntries(_ entries: [Entry], to defaults: UserDefaults) {
    do {
      let data = try JSONEncoder().encode(entries)
      defaults.set(data, forKey: storageKey)
    } catch {
      logger.warning("📼 [FlightRecorder] Failed to encode entries: \(error.localizedDescription)")
    }
  }
}
