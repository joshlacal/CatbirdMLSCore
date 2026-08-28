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

  private let gateDirectory: URL
  private var unavailableUsers: Set<String> = []


  private init() {
    do {
      gateDirectory = try MLSStoragePaths.welcomeGateDirectory()
      try FileManager.default.createDirectory(at: gateDirectory, withIntermediateDirectories: true)
    } catch {
      fatalError("Required App Group container unavailable for MLSWelcomeGate: \(error.localizedDescription)")
    }
  }

  /// Atomically claim Welcome processing ownership for the given conversation.
  /// Uses exclusive create (O_EXCL) so only one process wins; losers throw.
  public func beginWelcomeProcessing(for conversationID: String, userDID: String) throws {
    let url = markerURL(conversationID: conversationID, userDID: userDID)

    do {
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
    let url = markerURL(conversationID: conversationID, userDID: userDID)
    do {
      if try MLSStoragePaths.fileExistsStrict(at: url) {
        try FileManager.default.removeItem(at: url)
      }
      unavailableUsers.remove(userDID)
      logger.info("✅ [WelcomeGate] Complete Welcome: convo=\(conversationID.prefix(16))..., user=\(userDID.prefix(20), privacy: .private)")
    } catch {
      logger.critical("🚨 [WelcomeGate] Failed to remove welcome marker at \(url.path): \(error.localizedDescription)")
    }
  }
  /// Cross-process check: is Welcome currently pending for this conversation?
  public func hasPendingWelcome(for conversationID: String, userDID: String) -> Bool {
    if unavailableUsers.contains(userDID) {
      return true
    }

    let url = markerURL(conversationID: conversationID, userDID: userDID)
    do {
      guard try MLSStoragePaths.fileExistsStrict(at: url) else { return false }
    } catch {
      logger.critical("🚨 [WelcomeGate] Failed strict existence check for \(url.path): \(error.localizedDescription), failing closed")
      return true
    }

    return true
  }

  /// Remove all pending Welcome markers for a user on explicit account reset.
  public func clearAll(for userDID: String) throws {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let userComponent = MLSStoragePaths.didHash(normalized)
    let userDir = gateDirectory.appendingPathComponent(userComponent, isDirectory: true)
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

  func markerURL(conversationID: String, userDID: String) -> URL {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let userComponent = MLSStoragePaths.didHash(normalized)
    let convoDigest = SHA256.hash(data: Data(conversationID.utf8)).compactMap { String(format: "%02x", $0) }.joined()
    return gateDirectory
      .appendingPathComponent(userComponent, isDirectory: true)
      .appendingPathComponent("\(convoDigest).pending", isDirectory: false)
  }
}
