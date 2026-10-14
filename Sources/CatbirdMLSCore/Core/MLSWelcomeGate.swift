//
//  MLSWelcomeGate.swift
//  CatbirdMLSCore
//
//  Cross-process Welcome/message ordering gate.
//

import CryptoKit
import Foundation
import OSLog

/// Coordinates ordering between Welcome processing and message decryption.
///
/// This is implemented using file markers in the App Group container so it works
/// across processes (main app + Notification Service Extension).
public actor MLSWelcomeGate {

  public static let shared = MLSWelcomeGate()

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "WelcomeGate")

  private var unavailableUsers: Set<String> = []

  private init() {}

  /// Resolved on every call, never cached: the app and the NSE must agree on one
  /// directory, and a path frozen at first use diverges from the other process
  /// once the storage base changes (a test override, or a container that
  /// was not yet available when the gate was first touched).
  private func gateDirectory() throws -> URL {
    try MLSStoragePaths.welcomeGateDirectory()
  }

  /// Atomically claim Welcome processing ownership for the given conversation.
  /// Uses exclusive create (O_EXCL) so only one process wins; losers throw.
  public func beginWelcomeProcessing(for conversationID: String, userDID: String) throws {
    do {
      let url = try markerURL(conversationID: conversationID, userDID: userDID)
      try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
      let fd = open(url.path, O_CREAT | O_EXCL | O_WRONLY, 0o600)
      if fd >= 0 {
        close(fd)
        logger.info("⏳ [WelcomeGate] Begin Welcome: convo=\(conversationID.prefix(16))..., user=\(userDID.prefix(20), privacy: .private)")
        return
      }
      if errno == EEXIST {
        throw MLSStorageInitializationError.admissionDenied(
          details: "Welcome already being processed for \(conversationID)"
        )
      }
      throw MLSStorageInitializationError.admissionDenied(
        details: "Failed to create welcome marker for \(conversationID): \(errno)"
      )
    } catch let error as MLSStorageInitializationError {
      throw error
    } catch {
      throw MLSStorageInitializationError.admissionDenied(
        details: "Filesystem error beginning welcome for \(conversationID): \(error.localizedDescription)"
      )
    }
  }

  /// Mark that Welcome processing has completed for the given conversation.
  public func completeWelcomeProcessing(for conversationID: String, userDID: String) {
    do {
      let url = try markerURL(conversationID: conversationID, userDID: userDID)
      if try MLSStoragePaths.fileExistsStrict(at: url) {
        try FileManager.default.removeItem(at: url)
      }
      unavailableUsers.remove(userDID)
      logger.info("✅ [WelcomeGate] Complete Welcome: convo=\(conversationID.prefix(16))..., user=\(userDID.prefix(20), privacy: .private)")
    } catch {
      logger.critical("🚨 [WelcomeGate] Failed to remove welcome marker for \(conversationID.prefix(16)): \(error.localizedDescription)")
    }
  }
  /// Cross-process check: is Welcome currently pending for this conversation?
  public func hasPendingWelcome(for conversationID: String, userDID: String) -> Bool {
    if unavailableUsers.contains(userDID) {
      return true
    }

    do {
      let url = try markerURL(conversationID: conversationID, userDID: userDID)
      guard try MLSStoragePaths.fileExistsStrict(at: url) else { return false }
    } catch {
      logger.critical("🚨 [WelcomeGate] Failed strict existence check for \(conversationID.prefix(16)): \(error.localizedDescription), failing closed")
      return true
    }

    return true
  }

  /// Remove all pending Welcome markers for a user on explicit account reset.
  public func clearAll(for userDID: String) throws {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let userComponent = MLSStoragePaths.didHash(normalized)
    let userDir = try gateDirectory().appendingPathComponent(userComponent, isDirectory: true)
    if try MLSStoragePaths.fileExistsStrict(at: userDir) {
      try FileManager.default.removeItem(at: userDir)
    }
    unavailableUsers.remove(normalized)
    unavailableUsers.remove(userDID)
    logger.info("🗑️ [WelcomeGate] Cleared all welcome markers for user: \(normalized.prefix(20), privacy: .private)")
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

  func markerURL(conversationID: String, userDID: String) throws -> URL {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let userComponent = MLSStoragePaths.didHash(normalized)
    let convoDigest = SHA256.hash(data: Data(conversationID.utf8)).compactMap { String(format: "%02x", $0) }.joined()
    return try gateDirectory()
      .appendingPathComponent(userComponent, isDirectory: true)
      .appendingPathComponent("\(convoDigest).pending", isDirectory: false)
  }
}
