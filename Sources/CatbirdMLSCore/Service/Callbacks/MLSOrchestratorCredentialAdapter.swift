//
//  MLSOrchestratorCredentialAdapter.swift
//  CatbirdMLSCore
//
//  Bridges the OrchestratorCredentialCallback protocol (called synchronously from Rust)
//  to the iOS Keychain via MLSKeychain and MLSKeychainManager.
//

import CatbirdMLS
import Foundation
import OSLog
import Security

// MARK: - MLSOrchestratorCredentialAdapter

/// Adapts iOS Keychain storage to the synchronous `OrchestratorCredentialCallback`
/// protocol expected by the Rust orchestrator via UniFFI.
///
/// Signing keys are stored in the Keychain using `MLSKeychain` (which uses
/// `kSecClassKey` with application tags). MLS DID and device UUID are stored
/// in the Keychain as generic passwords via `MLSKeychainManager`, scoped by
/// user DID to maintain per-user isolation.
///
/// All methods are synchronous and called from a Rust background thread.
public final class MLSOrchestratorCredentialAdapter: OrchestratorCredentialCallback, @unchecked Sendable {

  private let keychainManager: MLSKeychainManager
  private let logger = Logger(subsystem: "blue.catbird", category: "OrchestratorCredentialAdapter")

  /// Keychain key prefix for MLS DID storage (scoped by user DID).
  private static let mlsDidKeyPrefix = "mls.credential.mlsDid."

  /// Keychain key prefix for device UUID storage (scoped by user DID).
  private static let deviceUuidKeyPrefix = "mls.credential.deviceUuid."

  // MARK: - Initialization

  /// Create an adapter that stores credentials in the iOS Keychain.
  /// - Parameter keychainManager: The keychain manager instance to use. Defaults to `.shared`.
  public init(keychainManager: MLSKeychainManager = .shared) {
    self.keychainManager = keychainManager
  }

  // MARK: - Signing Keys

  public func storeSigningKey(userDid: String, keyData: Data) throws {
    logger.debug("Storing signing key for user: \(userDid.prefix(20))...")
    try MLSKeychain.storeSignatureKey(keyData, forIdentity: userDid)
    logger.info("Stored signing key for user: \(userDid.prefix(20))...")
  }

  public func getSigningKey(userDid: String) throws -> Data? {
    logger.debug("Retrieving signing key for user: \(userDid.prefix(20))...")
    do {
      let keyData = try MLSKeychain.retrieveSignatureKey(forIdentity: userDid)
      logger.debug("Retrieved signing key for user: \(userDid.prefix(20))...")
      return keyData
    } catch MLSKeychainError.retrieveFailed(let status) where status == errSecItemNotFound {
      // Key does not exist yet - this is expected for new users
      logger.debug("No signing key found for user: \(userDid.prefix(20))...")
      return nil
    }
  }

  public func deleteSigningKey(userDid: String) throws {
    logger.debug("Deleting signing key for user: \(userDid.prefix(20))...")
    try MLSKeychain.deleteSignatureKey(forIdentity: userDid)
    logger.info("Deleted signing key for user: \(userDid.prefix(20))...")
  }

  // MARK: - MLS DID

  public func storeMlsDid(userDid: String, mlsDid: String) throws {
    let key = Self.mlsDidKeyPrefix + userDid
    let data = Data(mlsDid.utf8)
    logger.debug("Storing MLS DID for user: \(userDid.prefix(20))...")
    try keychainManager.store(
      data,
      forKey: key,
      accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    )
    logger.info("Stored MLS DID for user: \(userDid.prefix(20))...")
  }

  public func getMlsDid(userDid: String) throws -> String? {
    let key = Self.mlsDidKeyPrefix + userDid
    logger.debug("Retrieving MLS DID for user: \(userDid.prefix(20))...")
    guard let data = try keychainManager.retrieve(forKey: key) else {
      return nil
    }
    return String(data: data, encoding: .utf8)
  }

  // MARK: - Device UUID

  public func storeDeviceUuid(userDid: String, uuid: String) throws {
    let key = Self.deviceUuidKeyPrefix + userDid
    let data = Data(uuid.utf8)
    logger.debug("Storing device UUID for user: \(userDid.prefix(20))...")
    try keychainManager.store(
      data,
      forKey: key,
      accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    )
    logger.info("Stored device UUID for user: \(userDid.prefix(20))...")
  }

  public func getDeviceUuid(userDid: String) throws -> String? {
    let key = Self.deviceUuidKeyPrefix + userDid
    logger.debug("Retrieving device UUID for user: \(userDid.prefix(20))...")
    guard let data = try keychainManager.retrieve(forKey: key) else {
      return nil
    }
    return String(data: data, encoding: .utf8)
  }

  // MARK: - Credential State

  public func hasCredentials(userDid: String) throws -> Bool {
    // A user "has credentials" if they have a signing key stored.
    // This is the minimum requirement for MLS operations.
    let signingKey = try getSigningKey(userDid: userDid)
    return signingKey != nil
  }

  public func clearAll(userDid: String) throws {
    logger.info("Clearing all credentials for user: \(userDid.prefix(20))...")

    // Delete signing key
    do {
      try deleteSigningKey(userDid: userDid)
    } catch {
      logger.warning("Failed to delete signing key during clearAll: \(error.localizedDescription)")
    }

    // Delete MLS DID
    let mlsDidKey = Self.mlsDidKeyPrefix + userDid
    do {
      try keychainManager.delete(forKey: mlsDidKey)
    } catch {
      logger.warning("Failed to delete MLS DID during clearAll: \(error.localizedDescription)")
    }

    // Delete device UUID
    let deviceUuidKey = Self.deviceUuidKeyPrefix + userDid
    do {
      try keychainManager.delete(forKey: deviceUuidKey)
    } catch {
      logger.warning("Failed to delete device UUID during clearAll: \(error.localizedDescription)")
    }

    logger.info("Cleared all credentials for user: \(userDid.prefix(20))...")
  }
}
