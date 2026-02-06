//
//  MLSWelcomeGate.swift
//  CatbirdMLSCore
//
//  Cross-process Welcome/message ordering gate.
//

import Foundation
import OSLog

/// Coordinates ordering between Welcome processing and message decryption.
///
/// This is implemented using file markers in the App Group container so it works
/// across processes (main app + Notification Service Extension).
public actor MLSWelcomeGate {

  public static let shared = MLSWelcomeGate()

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "WelcomeGate")

  private let gateDirectory: URL

  private init() {
    let baseDirectory = MLSStoragePaths.baseContainerURL()
    gateDirectory = baseDirectory.appendingPathComponent("mls_welcome_gate", isDirectory: true)

    do {
      try FileManager.default.createDirectory(at: gateDirectory, withIntermediateDirectories: true)
    } catch {
      logger.error("❌ [WelcomeGate] Failed to create gate directory: \(error.localizedDescription)")
    }
  }

  /// Mark that Welcome processing has started for the given conversation.
  public func beginWelcomeProcessing(for conversationID: String, userDID: String) {
    let url = markerURL(conversationID: conversationID, userDID: userDID)

    // Ensure user directory exists
    try? FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    if !FileManager.default.fileExists(atPath: url.path) {
      FileManager.default.createFile(atPath: url.path, contents: Data(), attributes: nil)
      logger.info("⏳ [WelcomeGate] Begin Welcome: convo=\(conversationID.prefix(16))..., user=\(userDID.prefix(20), privacy: .private)")
    }
  }

  /// Mark that Welcome processing has completed for the given conversation.
  public func completeWelcomeProcessing(for conversationID: String, userDID: String) {
    let url = markerURL(conversationID: conversationID, userDID: userDID)
    do {
      try FileManager.default.removeItem(at: url)
    } catch {
      // It's ok if it didn't exist.
    }
    logger.info("✅ [WelcomeGate] Complete Welcome: convo=\(conversationID.prefix(16))..., user=\(userDID.prefix(20), privacy: .private)")
  }

  /// Cross-process check: is Welcome currently pending for this conversation?
  public func hasPendingWelcome(for conversationID: String, userDID: String) -> Bool {
    let url = markerURL(conversationID: conversationID, userDID: userDID)
    guard FileManager.default.fileExists(atPath: url.path) else { return false }

    // Safety: if we crashed mid-Welcome, don't block forever.
    if let attrs = try? FileManager.default.attributesOfItem(atPath: url.path),
       let modified = attrs[.modificationDate] as? Date,
       Date().timeIntervalSince(modified) > 30 {
      try? FileManager.default.removeItem(at: url)
      logger.warning("⚠️ [WelcomeGate] Stale Welcome marker removed: convo=\(conversationID.prefix(16))...")
      return false
    }

    return true
  }

  /// Wait until Welcome is no longer pending (or timeout).
  ///
  /// - Returns: true if Welcome completed (or wasn't pending), false if timed out/cancelled.
  public func waitForWelcomeIfPending(
    for conversationID: String,
    userDID: String,
    timeout: Duration = .seconds(3)
  ) async -> Bool {
    if !hasPendingWelcome(for: conversationID, userDID: userDID) {
      return true
    }

    let deadline = ContinuousClock.now.advanced(by: timeout)
    while ContinuousClock.now < deadline {
      if Task.isCancelled { return false }
      if !hasPendingWelcome(for: conversationID, userDID: userDID) {
        return true
      }
      try? await Task.sleep(for: .milliseconds(50))
    }

    return !hasPendingWelcome(for: conversationID, userDID: userDID)
  }

  // MARK: - Private

  private func markerURL(conversationID: String, userDID: String) -> URL {
    let userComponent = sanitize(userDID)
    let convoComponent = sanitize(conversationID)
    return gateDirectory
      .appendingPathComponent(userComponent, isDirectory: true)
      .appendingPathComponent("\(convoComponent).pending", isDirectory: false)
  }

  private func sanitize(_ value: String) -> String {
    let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "._-"))
    let replacement = UnicodeScalar("_")

    var output = String.UnicodeScalarView()
    output.reserveCapacity(value.unicodeScalars.count)

    for scalar in value.unicodeScalars {
      output.append(allowed.contains(scalar) ? scalar : replacement)
    }

    return String(output)
  }
}
