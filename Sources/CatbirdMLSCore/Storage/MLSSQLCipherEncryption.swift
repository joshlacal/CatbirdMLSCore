//
//  MLSSQLCipherEncryption.swift
//  Catbird
//
//  Production-ready encryption key management for SQLCipher database
//

import Foundation
import GRDB
import Security
import OSLog

/// Manages encryption keys for SQLCipher databases with secure Keychain storage
/// Actor provides thread-safe access to keychain operations
public actor MLSSQLCipherEncryption {

  // MARK: - Properties

  /// Shared singleton instance
  public static let shared = MLSSQLCipherEncryption()

  /// Key size for AES-256 (32 bytes)
  private let keySize: Int = 32

  /// Salt size for SQLCipher (16 bytes)
  private let saltSize: Int = 16

  /// Logger for debugging keychain operations
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "SQLCipherEncryption")

  // MARK: - Initialization

  private init() {}

  // MARK: - Public API

  /// Generate or retrieve encryption key for a specific user
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: 256-bit encryption key as Data
  /// - Throws: MLSSQLCipherError if key generation or retrieval fails
  func getOrCreateKey(for userDID: String) throws -> Data {
    let keyAccount = MLSStoragePaths.grdbKeyAccount(for: userDID)
    logger.debug("[SQLCipher] getOrCreateKey for user: \(userDID.prefix(24), privacy: .private)...")
    do {
      return try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: keyAccount, length: keySize)
    } catch {
      throw MLSSQLCipherError.keychainAccessFailed(operation: "getOrCreateKey", status: (error as? KeychainError).flatMap { err in
        if case .storeFailed(let status) = err { return status }
        if case .retrieveFailed(let status) = err { return status }
        return nil
      } ?? -1)
    }
  }

  /// Retrieve existing encryption key for a user
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: 256-bit encryption key as Data, or nil if not found
  /// - Throws: MLSSQLCipherError if retrieval fails
  func getKey(for userDID: String) throws -> Data? {
    let keyAccount = MLSStoragePaths.grdbKeyAccount(for: userDID)
    do {
      return try MLSKeychainManager.shared.retrieveKeyStrict(forKey: keyAccount, expectedLength: keySize)
    } catch {
      if let status = (error as? KeychainError).flatMap({ err -> OSStatus? in
        if case .retrieveFailed(let s) = err { return s }
        return nil
      }) {
        throw MLSSQLCipherError.keychainAccessFailed(operation: "getKey", status: status)
      }
      throw error
    }
  }

  /// Delete encryption key for a user (when deleting account)
  /// - Parameter userDID: User's decentralized identifier
  /// - Throws: MLSSQLCipherError if deletion fails
  func deleteKey(for userDID: String) throws {
    let keyAccount = MLSStoragePaths.grdbKeyAccount(for: userDID)
    do {
      try MLSKeychainManager.shared.deleteStrict(forKey: keyAccount)
      logger.debug("[SQLCipher] Key deleted for user: \(userDID.prefix(24), privacy: .private)...")
    } catch {
      if let status = (error as? KeychainError).flatMap({ err -> OSStatus? in
        if case .deleteFailed(let s) = err { return s }
        return nil
      }) {
        throw MLSSQLCipherError.keychainAccessFailed(operation: "deleteKey", status: status)
      }
      throw error
    }
  }

  /// Convert raw key data to hex string for SQLCipher PRAGMA
  /// - Parameter key: Raw key data
  /// - Returns: Hex string prefixed with "x'" for SQLCipher
  func keyToHexString(_ key: Data) -> String {
    let hexString = key.map { String(format: "%02x", $0) }.joined()
    return "x'\(hexString)'"
  }

  /// Convert raw key and salt data to combined hex string for SQLCipher PRAGMA key
  func keyToHexString(_ key: Data, salt: Data) -> String {
    let combined = key + salt
    let hexString = combined.map { String(format: "%02x", $0) }.joined()
    return "x'\(hexString)'"
  }

  // MARK: - Salt Management

  /// Generate or retrieve salt for a specific user
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: 16-byte salt as Data
  /// - Throws: MLSSQLCipherError if salt generation or retrieval fails
  func getOrCreateSalt(for userDID: String, dbPath: URL? = nil) throws -> Data {
    let saltAccount = MLSStoragePaths.grdbSaltAccount(for: userDID)
    logger.debug("[SQLCipher] getOrCreateSalt for user: \(userDID.prefix(24), privacy: .private)...")
    do {
      return try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: saltAccount, length: saltSize)
    } catch {
      if let status = (error as? KeychainError).flatMap({ err -> OSStatus? in
        if case .storeFailed(let s) = err { return s }
        if case .retrieveFailed(let s) = err { return s }
        return nil
      }) {
        throw MLSSQLCipherError.keychainAccessFailed(operation: "getOrCreateSalt", status: status)
      }
      throw error
    }
  }

  func getSalt(for userDID: String) throws -> Data? {
    let saltAccount = MLSStoragePaths.grdbSaltAccount(for: userDID)
    do {
      return try MLSKeychainManager.shared.retrieveKeyStrict(forKey: saltAccount, expectedLength: saltSize)
    } catch {
      if let status = (error as? KeychainError).flatMap({ err -> OSStatus? in
        if case .retrieveFailed(let s) = err { return s }
        return nil
      }) {
        throw MLSSQLCipherError.keychainAccessFailed(operation: "getSalt", status: status)
      }
      throw error
    }
  }

  func deleteSalt(for userDID: String) throws {
    let saltAccount = MLSStoragePaths.grdbSaltAccount(for: userDID)
    do {
      try MLSKeychainManager.shared.deleteStrict(forKey: saltAccount)
      logger.debug("[SQLCipher] Salt deleted for user: \(userDID.prefix(24), privacy: .private)...")
    } catch {
      if let status = (error as? KeychainError).flatMap({ err -> OSStatus? in
        if case .deleteFailed(let s) = err { return s }
        return nil
      }) {
        throw MLSSQLCipherError.keychainAccessFailed(operation: "deleteSalt", status: status)
      }
      throw error
    }
  }
}

// MARK: - Key Verification

extension MLSSQLCipherEncryption {
  /// Verify that a key can successfully decrypt a SQLCipher database.
  func verifyKey(
    _ key: Data,
    databaseURL: URL,
    salt: Data,
    testQuery: String = "SELECT count(*) FROM sqlite_master;"
  ) -> Bool {
    guard key.count == keySize else {
      logger.error("[SQLCipher] Key verification failed: invalid key size (\(key.count) bytes, expected \(self.keySize))")
      return false
    }

    guard salt.count == saltSize else {
      logger.error("[SQLCipher] Key verification failed: invalid salt size (\(salt.count) bytes, expected \(self.saltSize))")
      return false
    }

    guard FileManager.default.fileExists(atPath: databaseURL.path) else {
      logger.error("[SQLCipher] Key verification failed: database file does not exist at \(databaseURL.path)")
      return false
    }

    var config = Configuration()
    config.readonly = true
    config.allowsUnsafeTransactions = true

    config.prepareDatabase { db in
      try db.execute(sql: "PRAGMA cipher_memory_security = OFF;")
      let hexKey = key.map { String(format: "%02x", $0) }.joined()
      let hexSalt = salt.map { String(format: "%02x", $0) }.joined()
      try db.execute(sql: "PRAGMA key = \"x'\(hexKey)\(hexSalt)'\";")
      try db.execute(sql: "PRAGMA cipher_page_size = 4096;")
      try db.execute(sql: "PRAGMA kdf_iter = 256000;")
      try db.execute(sql: "PRAGMA cipher_hmac_algorithm = HMAC_SHA512;")
      try db.execute(sql: "PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA512;")
    }

    do {
      let dbQueue = try DatabaseQueue(path: databaseURL.path, configuration: config)
      defer { try? dbQueue.close() }

      let result: Bool = try dbQueue.read { db in
        let cipherVersion: String? = try String.fetchOne(db, sql: "PRAGMA cipher_version;")
        guard let cipherVersion, !cipherVersion.isEmpty else {
          return false
        }
        _ = try db.execute(sql: testQuery)
        return true
      }

      if result {
        logger.debug("[SQLCipher] Key verification succeeded for \(databaseURL.lastPathComponent)")
      }
      return result
    } catch {
      logger.warning("[SQLCipher] Key verification failed for \(databaseURL.lastPathComponent): \(error.localizedDescription)")
      return false
    }
  }
}
