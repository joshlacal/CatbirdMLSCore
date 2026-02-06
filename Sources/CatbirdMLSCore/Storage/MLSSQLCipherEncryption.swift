//
//  MLSSQLCipherEncryption.swift
//  Catbird
//
//  Production-ready encryption key management for SQLCipher database
//

import Foundation
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

  /// Keychain service identifier for SQLCipher database keys
  private let keychainService = "blue.catbird.mls.sqlcipher"

  /// Logger for debugging keychain operations
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "SQLCipherEncryption")

  /// Cached keychain access group (probing is expensive)
  private var _cachedAccessGroup: String??
  
  /// Shared keychain access group for App/Extension sharing
  /// Uses the same pattern as MLSKeychainManager
  private var keychainAccessGroup: String? {
    #if targetEnvironment(simulator)
    logger.debug("[SQLCipher] Running on simulator - using default keychain (no sharing)")
    return nil
    #else
    // Use cached value if available
    if let cached = _cachedAccessGroup {
      return cached
    }
    
    let resolved = MLSKeychainManager.resolvedAccessGroup(suffix: "blue.catbird.shared")
    _cachedAccessGroup = .some(resolved)
    
    if let resolved {
      logger.info("[SQLCipher] ✅ Keychain access group resolved: \(resolved)")
    } else {
      logger.error("[SQLCipher] ⚠️ Failed to resolve keychain access group - using default (may cause sharing issues)")
    }
    
    return resolved
    #endif
  }

  // MARK: - Initialization

  private init() {
    // Pre-resolve access group on init to cache it early
    #if !targetEnvironment(simulator)
    _ = keychainAccessGroup
    #endif
  }

  // MARK: - Public API

  /// Generate or retrieve encryption key for a specific user
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: 256-bit encryption key as Data
  /// - Throws: MLSSQLCipherError if key generation or retrieval fails
  func getOrCreateKey(for userDID: String) throws -> Data {
    let keychainKey = makeKeychainKey(for: userDID)
    
    logger.debug("[SQLCipher] getOrCreateKey for user: \(userDID.prefix(24))..., accessGroup=\(self.keychainAccessGroup ?? "default")")

    // Try to retrieve existing key from Keychain
    if let existingKey = try? retrieveKey(keychainKey: keychainKey) {
      guard existingKey.count == keySize else {
        logger.error("[SQLCipher] Key size mismatch: expected \(self.keySize) bytes, got \(existingKey.count)")
        throw MLSSQLCipherError.invalidEncryptionKey(reason: "Key size mismatch: expected \(keySize) bytes, got \(existingKey.count)")
      }
      logger.debug("[SQLCipher] Retrieved existing key for user")
      return existingKey
    }

    // Generate new key if none exists
    logger.info("[SQLCipher] No existing key found, generating new key for user: \(userDID.prefix(24))...")
    let newKey = try generateKey()
    try storeKey(newKey, keychainKey: keychainKey)
    return newKey
  }

  /// Retrieve existing encryption key for a user
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: 256-bit encryption key as Data, or nil if not found
  /// - Throws: MLSSQLCipherError if retrieval fails
  func getKey(for userDID: String) throws -> Data? {
    let keychainKey = makeKeychainKey(for: userDID)
    return try? retrieveKey(keychainKey: keychainKey)
  }

  /// Delete encryption key for a user (when deleting account)
  /// - Parameter userDID: User's decentralized identifier
  /// - Throws: MLSSQLCipherError if deletion fails
  func deleteKey(for userDID: String) throws {
    let keychainKey = makeKeychainKey(for: userDID)

    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: keychainService,
      kSecAttrAccount as String: keychainKey
    ]
    
    // Add shared access group for consistency
    if let accessGroup = keychainAccessGroup {
      query[kSecAttrAccessGroup as String] = accessGroup
    }

    let status = SecItemDelete(query as CFDictionary)

    guard status == errSecSuccess || status == errSecItemNotFound else {
      logger.error("[SQLCipher] Delete failed with status: \(status)")
      throw MLSSQLCipherError.keychainAccessFailed(operation: "delete", status: status)
    }
    
    logger.debug("[SQLCipher] Key deleted for user: \(userDID.prefix(24))...")
  }

  /// Rotate encryption key for a user (generates new key, returns both old and new)
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: Tuple of (oldKey, newKey) for re-encryption
  /// - Throws: MLSSQLCipherError if rotation fails
  func rotateKey(for userDID: String) throws -> (oldKey: Data, newKey: Data) {
    // Retrieve current key
    guard let oldKey = try getKey(for: userDID) else {
      throw MLSSQLCipherError.invalidEncryptionKey(reason: "No existing key found for rotation")
    }

    // Generate new key
    let newKey = try generateKey()

    // Store new key (overwrites old key)
    let keychainKey = makeKeychainKey(for: userDID)
    try storeKey(newKey, keychainKey: keychainKey, update: true)

    return (oldKey, newKey)
  }

  /// Convert raw key data to hex string for SQLCipher PRAGMA
  /// - Parameter key: Raw key data
  /// - Returns: Hex string prefixed with "x'" for SQLCipher
  func keyToHexString(_ key: Data) -> String {
    let hexString = key.map { String(format: "%02x", $0) }.joined()
    return "x'\(hexString)'"
  }

  // MARK: - Salt Management (for cipher_plaintext_header_size)

  /// Salt size for SQLCipher (16 bytes)
  private let saltSize: Int = 16

  /// Generate or retrieve salt for a specific user
  ///
  /// When using `cipher_plaintext_header_size = 32`, the salt must be provided explicitly
  /// via `PRAGMA cipher_salt` because it's no longer stored in the encrypted header.
  ///
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: 16-byte salt as Data
  /// - Throws: MLSSQLCipherError if salt generation or retrieval fails
  func getOrCreateSalt(for userDID: String) throws -> Data {
    let keychainKey = makeSaltKeychainKey(for: userDID)

    logger.debug("[SQLCipher] getOrCreateSalt for user: \(userDID.prefix(24))...")

    // Try to retrieve existing salt from Keychain
    if let existingSalt = try? retrieveSalt(keychainKey: keychainKey) {
      guard existingSalt.count == saltSize else {
        logger.error("[SQLCipher] Salt size mismatch: expected \(self.saltSize) bytes, got \(existingSalt.count)")
        throw MLSSQLCipherError.invalidEncryptionKey(reason: "Salt size mismatch: expected \(saltSize) bytes, got \(existingSalt.count)")
      }
      logger.debug("[SQLCipher] Retrieved existing salt for user")
      return existingSalt
    }

    // Generate new salt if none exists
    logger.info("[SQLCipher] No existing salt found, generating new salt for user: \(userDID.prefix(24))...")
    let newSalt = try generateSalt()
    try storeSalt(newSalt, keychainKey: keychainKey)
    return newSalt
  }

  /// Retrieve existing salt for a user
  /// - Parameter userDID: User's decentralized identifier
  /// - Returns: 16-byte salt as Data, or nil if not found
  func getSalt(for userDID: String) throws -> Data? {
    let keychainKey = makeSaltKeychainKey(for: userDID)
    return try? retrieveSalt(keychainKey: keychainKey)
  }

  /// Delete salt for a user (when deleting account)
  /// - Parameter userDID: User's decentralized identifier
  /// - Throws: MLSSQLCipherError if deletion fails
  func deleteSalt(for userDID: String) throws {
    let keychainKey = makeSaltKeychainKey(for: userDID)

    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: keychainService,
      kSecAttrAccount as String: keychainKey
    ]

    if let accessGroup = keychainAccessGroup {
      query[kSecAttrAccessGroup as String] = accessGroup
    }

    let status = SecItemDelete(query as CFDictionary)

    guard status == errSecSuccess || status == errSecItemNotFound else {
      logger.error("[SQLCipher] Salt delete failed with status: \(status)")
      throw MLSSQLCipherError.keychainAccessFailed(operation: "delete salt", status: status)
    }

    logger.debug("[SQLCipher] Salt deleted for user: \(userDID.prefix(24))...")
  }

  /// Generate cryptographically secure random salt
  private func generateSalt() throws -> Data {
    var saltData = Data(count: saltSize)

    let result = saltData.withUnsafeMutableBytes { bufferPointer in
      SecRandomCopyBytes(kSecRandomDefault, saltSize, bufferPointer.baseAddress!)
    }

    guard result == errSecSuccess else {
      throw MLSSQLCipherError.keyGenerationFailed
    }

    return saltData
  }

  /// Store salt in Keychain
  private func storeSalt(_ salt: Data, keychainKey: String) throws {
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: keychainService,
      kSecAttrAccount as String: keychainKey,
      kSecValueData as String: salt,
      kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
      kSecAttrSynchronizable as String: false
    ]

    if let accessGroup = keychainAccessGroup {
      query[kSecAttrAccessGroup as String] = accessGroup
    }

    // Try to add, handle duplicate
    let status = SecItemAdd(query as CFDictionary, nil)

    if status == errSecDuplicateItem {
      // Update existing
      var updateQuery: [String: Any] = [
        kSecClass as String: kSecClassGenericPassword,
        kSecAttrService as String: keychainService,
        kSecAttrAccount as String: keychainKey
      ]
      if let accessGroup = keychainAccessGroup {
        updateQuery[kSecAttrAccessGroup as String] = accessGroup
      }

      let updateStatus = SecItemUpdate(
        updateQuery as CFDictionary,
        [kSecValueData as String: salt] as CFDictionary
      )

      guard updateStatus == errSecSuccess else {
        logger.error("[SQLCipher] Salt update failed with status: \(updateStatus)")
        throw MLSSQLCipherError.keychainAccessFailed(operation: "store salt", status: updateStatus)
      }
      logger.debug("[SQLCipher] Salt updated successfully")
      return
    }

    guard status == errSecSuccess else {
      logger.error("[SQLCipher] Salt store failed with status: \(status)")
      throw MLSSQLCipherError.keychainAccessFailed(operation: "store salt", status: status)
    }

    logger.debug("[SQLCipher] Salt stored successfully")
  }

  /// Retrieve salt from Keychain
  private func retrieveSalt(keychainKey: String) throws -> Data {
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: keychainService,
      kSecAttrAccount as String: keychainKey,
      kSecReturnData as String: true,
      kSecMatchLimit as String: kSecMatchLimitOne
    ]

    if let accessGroup = keychainAccessGroup {
      query[kSecAttrAccessGroup as String] = accessGroup
    }

    var result: AnyObject?
    let status = SecItemCopyMatching(query as CFDictionary, &result)

    guard status == errSecSuccess else {
      throw MLSSQLCipherError.keychainAccessFailed(operation: "retrieve salt", status: status)
    }

    guard let saltData = result as? Data else {
      throw MLSSQLCipherError.invalidEncryptionKey(reason: "Retrieved salt is not in expected format")
    }

    return saltData
  }

  /// Generate Keychain account identifier for user's salt
  private func makeSaltKeychainKey(for userDID: String) -> String {
    "mls.sqlcipher.db.salt.\(userDID)"
  }

  // MARK: - Private Methods

  /// Generate cryptographically secure random key
  private func generateKey() throws -> Data {
    var keyData = Data(count: keySize)

    let result = keyData.withUnsafeMutableBytes { bufferPointer in
      SecRandomCopyBytes(kSecRandomDefault, keySize, bufferPointer.baseAddress!)
    }

    guard result == errSecSuccess else {
      throw MLSSQLCipherError.keyGenerationFailed
    }

    return keyData
  }

  /// Store key in Keychain with maximum security
  /// Uses shared access group when available for App/Extension sharing
  private func storeKey(_ key: Data, keychainKey: String, update: Bool = false) throws {
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: keychainService,
      kSecAttrAccount as String: keychainKey,
      kSecValueData as String: key,

      // Security attributes
      kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
      kSecAttrSynchronizable as String: false // NEVER sync to iCloud
    ]
    
    // Add shared access group for App/Extension keychain sharing
    if let accessGroup = keychainAccessGroup {
      query[kSecAttrAccessGroup as String] = accessGroup
      logger.debug("[SQLCipher] Storing key with accessGroup: \(accessGroup)")
    }

    if update {
      // Try to update existing key
      var updateQuery: [String: Any] = [
        kSecClass as String: kSecClassGenericPassword,
        kSecAttrService as String: keychainService,
        kSecAttrAccount as String: keychainKey
      ]
      
      if let accessGroup = keychainAccessGroup {
        updateQuery[kSecAttrAccessGroup as String] = accessGroup
      }

      let updateAttributes: [String: Any] = [
        kSecValueData as String: key
      ]

      let updateStatus = SecItemUpdate(updateQuery as CFDictionary, updateAttributes as CFDictionary)

      if updateStatus == errSecSuccess {
        logger.debug("[SQLCipher] Key updated successfully")
        return
      } else if updateStatus == errSecItemNotFound {
        // Fall through to insert new item
      } else {
        logger.error("[SQLCipher] Update failed with status: \(updateStatus)")
        throw MLSSQLCipherError.keychainAccessFailed(operation: "update", status: updateStatus)
      }
    }

    // Insert new key
    let status = SecItemAdd(query as CFDictionary, nil)

    if status == errSecDuplicateItem {
      var updateQuery: [String: Any] = [
        kSecClass as String: kSecClassGenericPassword,
        kSecAttrService as String: keychainService,
        kSecAttrAccount as String: keychainKey
      ]
      
      if let accessGroup = keychainAccessGroup {
        updateQuery[kSecAttrAccessGroup as String] = accessGroup
      }

      let updateAttributes: [String: Any] = [
        kSecValueData as String: key,
        kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
      ]

      let updateStatus = SecItemUpdate(updateQuery as CFDictionary, updateAttributes as CFDictionary)

      if updateStatus == errSecSuccess {
        logger.debug("[SQLCipher] Key updated (was duplicate)")
        return
      }

      // Fall back to delete+add in case attributes differ
      SecItemDelete(updateQuery as CFDictionary)
      let retryStatus = SecItemAdd(query as CFDictionary, nil)

      guard retryStatus == errSecSuccess else {
        logger.error("[SQLCipher] Store retry failed with status: \(retryStatus)")
        throw MLSSQLCipherError.keychainAccessFailed(operation: "store", status: retryStatus)
      }

      logger.debug("[SQLCipher] Key stored after delete+add")
      return
    }

    guard status == errSecSuccess else {
      logger.error("[SQLCipher] Store failed with status: \(status)")
      throw MLSSQLCipherError.keychainAccessFailed(operation: "store", status: status)
    }
    
    logger.debug("[SQLCipher] Key stored successfully")
  }

  /// Retrieve key from Keychain
  /// Uses shared access group when available for App/Extension sharing
  private func retrieveKey(keychainKey: String) throws -> Data {
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: keychainService,
      kSecAttrAccount as String: keychainKey,
      kSecReturnData as String: true,
      kSecMatchLimit as String: kSecMatchLimitOne
    ]
    
    // Add shared access group for App/Extension keychain sharing
    if let accessGroup = keychainAccessGroup {
      query[kSecAttrAccessGroup as String] = accessGroup
      logger.debug("[SQLCipher] Retrieving key with accessGroup: \(accessGroup)")
    }

    var result: AnyObject?
    let status = SecItemCopyMatching(query as CFDictionary, &result)

    guard status == errSecSuccess else {
      if status == errSecItemNotFound {
        logger.debug("[SQLCipher] Key not found in keychain (this may be expected for first-time setup)")
      } else {
        logger.error("[SQLCipher] Retrieve failed with status: \(status) - this may indicate keychain access issues in extension context")
      }
      throw MLSSQLCipherError.keychainAccessFailed(operation: "retrieve", status: status)
    }

    guard let keyData = result as? Data else {
      logger.error("[SQLCipher] Retrieved data is not in expected format")
      throw MLSSQLCipherError.invalidEncryptionKey(reason: "Retrieved data is not in expected format")
    }
    
    logger.debug("[SQLCipher] Key retrieved successfully (\(keyData.count) bytes)")

    return keyData
  }

  /// Generate Keychain account identifier for user
  private func makeKeychainKey(for userDID: String) -> String {
    "mls.sqlcipher.db.key.\(userDID)"
  }
}

// MARK: - Key Verification

extension MLSSQLCipherEncryption {
  /// Verify that a key can successfully decrypt a test database
  /// - Parameters:
  ///   - key: Encryption key to verify
  ///   - testQuery: Optional test query to execute (defaults to SELECT count(*) FROM sqlite_master)
  /// - Returns: True if key is valid and database can be accessed
  func verifyKey(_ key: Data, testQuery: String = "SELECT 1 FROM sqlite_master LIMIT 1;") -> Bool {
    // This will be implemented once we have the database connection
    // For now, just verify key size
    key.count == keySize
  }
}

// MARK: - Debug Helpers (Development Only)

#if DEBUG
extension MLSSQLCipherEncryption {
  /// List all stored keys for debugging (NEVER use in production)
  func listAllKeys() throws -> [String] {
    let query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: keychainService,
      kSecReturnAttributes as String: true,
      kSecMatchLimit as String: kSecMatchLimitAll
    ]

    var result: AnyObject?
    let status = SecItemCopyMatching(query as CFDictionary, &result)

    guard status == errSecSuccess else {
      if status == errSecItemNotFound {
        return []
      }
      throw MLSSQLCipherError.keychainAccessFailed(operation: "list", status: status)
    }

    guard let items = result as? [[String: Any]] else {
      return []
    }

    return items.compactMap { $0[kSecAttrAccount as String] as? String }
  }

  /// Delete all SQLCipher keys (for testing only)
  func deleteAllKeys() throws {
    let query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: keychainService
    ]

    let status = SecItemDelete(query as CFDictionary)

    guard status == errSecSuccess || status == errSecItemNotFound else {
      throw MLSSQLCipherError.keychainAccessFailed(operation: "delete all", status: status)
    }
  }
}
#endif
