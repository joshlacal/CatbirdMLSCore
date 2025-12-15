//
//  MLSKeychainManager.swift
//  Catbird
//
//  Secure Keychain storage for MLS cryptographic materials
//

import Foundation
import Security
import OSLog

/// Manages secure storage of MLS cryptographic materials in Keychain
public final class MLSKeychainManager: @unchecked Sendable {

  // MARK: - Singleton

  public static let shared = MLSKeychainManager()

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSKeychainManager")

  // MARK: - Configuration

  /// The Keychain Access Group to use for shared storage.
  /// Must be set to share keys between App and Extension.
  /// Format: "TeamID.com.example.group"
  public var accessGroup: String?

  public static func resolvedAccessGroup(suffix: String) -> String? {
    #if os(macOS) || targetEnvironment(macCatalyst)
    guard let task = SecTaskCreateFromSelf(nil) else { return nil }
    guard
      let value = SecTaskCopyValueForEntitlement(task, "keychain-access-groups" as CFString, nil),
      let groups = value as? [String]
    else { return nil }
    return groups.first(where: { $0.hasSuffix(suffix) })
    #else
    // iOS/tvOS/watchOS: SecTask* APIs are unavailable; infer the TeamID prefix by probing the default access group.
    let probeService = "blue.catbird.mls.accessGroupProbe"
    let probeAccount = UUID().uuidString
    let probeData = Data([0])

    let addQuery: [CFString: Any] = [
      kSecClass: kSecClassGenericPassword,
      kSecAttrService: probeService,
      kSecAttrAccount: probeAccount,
      kSecValueData: probeData,
      kSecAttrAccessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
    ]
    _ = SecItemAdd(addQuery as CFDictionary, nil)

    let matchQuery: [CFString: Any] = [
      kSecClass: kSecClassGenericPassword,
      kSecAttrService: probeService,
      kSecAttrAccount: probeAccount,
      kSecReturnAttributes: true,
      kSecMatchLimit: kSecMatchLimitOne,
    ]

    defer {
      let delQuery: [CFString: Any] = [
        kSecClass: kSecClassGenericPassword,
        kSecAttrService: probeService,
        kSecAttrAccount: probeAccount,
      ]
      _ = SecItemDelete(delQuery as CFDictionary)
    }

    var item: CFTypeRef?
    guard SecItemCopyMatching(matchQuery as CFDictionary, &item) == errSecSuccess,
      let attrs = item as? [CFString: Any],
      let accessGroup = attrs[kSecAttrAccessGroup] as? String,
      let dot = accessGroup.firstIndex(of: ".")
    else { return nil }

    let prefix = String(accessGroup[..<accessGroup.index(after: dot)])
    return prefix + suffix
    #endif
  }

  // MARK: - Keychain Keys

  private enum KeychainKey {
    case groupState(conversationID: String)
    case privateKey(conversationID: String, epoch: Int64)
    case signatureKey(conversationID: String)
    case encryptionKey(conversationID: String)
    case epochSecrets(conversationID: String, epoch: Int64)
    case hpkePrivateKey(keyPackageID: String)
    case currentEpoch(conversationID: String)
    case welcomeMessage(conversationID: String)

    var key: String {
      switch self {
      case .groupState(let id):
        return "mls.groupstate.\(id)"
      case .privateKey(let id, let epoch):
        return "mls.privatekey.\(id).epoch.\(epoch)"
      case .signatureKey(let id):
        return "mls.signaturekey.\(id)"
      case .encryptionKey(let id):
        return "mls.encryptionkey.\(id)"
      case .epochSecrets(let id, let epoch):
        return "mls.epochsecrets.\(id).epoch.\(epoch)"
      case .hpkePrivateKey(let id):
        return "mls.hpke.privatekey.\(id)"
      case .currentEpoch(let id):
        return "mls.currentepoch.\(id)"
      case .welcomeMessage(let id):
        return "mls.welcomemessage.\(id)"
      }
    }

    var accessGroup: String {
      "blue.catbird.mls.keychain"
    }
  }

  // MARK: - Initialization

  private init() {}

  // MARK: - Encryption Key Management (for encrypted storage)

  /// Store or retrieve the master encryption key for MLS state storage
  /// This key encrypts the MLS state blob stored in Core Data
  /// - Parameter userDID: User's DID identifier
  /// - Returns: 32-byte encryption key (AES-256)
  /// - Throws: KeychainError if generation or storage fails
  public func getOrCreateEncryptionKey(forUserDID userDID: String) throws -> Data {
    let key = "mls.encryption.key.\(userDID)"

    // Try to retrieve existing key
    if let existingKey = try retrieve(forKey: key) {
      logger.debug("Retrieved existing encryption key for user: \(userDID)")
      return existingKey
    }

    // Generate new 256-bit AES key
    let newKey = try generateSecureRandomKey(length: 32)
    try store(newKey, forKey: key, accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly)
    logger.info("🔑 Generated new encryption key for user: \(userDID)")

    return newKey
  }

  /// Delete encryption key for a user (when logging out or clearing data)
  /// - Parameter userDID: User's DID identifier
  /// - Throws: KeychainError if deletion fails
  public func deleteEncryptionKey(forUserDID userDID: String) throws {
    let key = "mls.encryption.key.\(userDID)"
    try delete(forKey: key)
    logger.info("Deleted encryption key for user: \(userDID)")
  }

  // MARK: - Group State Management

  /// Store encrypted group state for a conversation
  public func storeGroupState(_ data: Data, forConversationID conversationID: String) throws {
    let key = KeychainKey.groupState(conversationID: conversationID)
    try store(data, forKey: key.key, accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly)
    logger.info("Stored group state for conversation: \(conversationID)")
  }

  /// Retrieve encrypted group state for a conversation
  public func retrieveGroupState(forConversationID conversationID: String) throws -> Data? {
    let key = KeychainKey.groupState(conversationID: conversationID)
    return try retrieve(forKey: key.key)
  }

  /// Delete group state for a conversation
  public func deleteGroupState(forConversationID conversationID: String) throws {
    let key = KeychainKey.groupState(conversationID: conversationID)
    try delete(forKey: key.key)
    logger.info("Deleted group state for conversation: \(conversationID)")
  }

  // MARK: - Epoch Management

  /// Store current epoch for a conversation
  public func storeCurrentEpoch(_ epoch: Int, forConversationID conversationID: String) throws {
    let epochData = withUnsafeBytes(of: Int64(epoch)) { Data($0) }
    let key = KeychainKey.currentEpoch(conversationID: conversationID)
    try store(epochData, forKey: key.key, accessible: kSecAttrAccessibleAfterFirstUnlock)
    logger.debug("Stored current epoch \(epoch) for conversation: \(conversationID)")
  }

  /// Retrieve current epoch for a conversation
  public func retrieveCurrentEpoch(forConversationID conversationID: String) throws -> Int? {
    let key = KeychainKey.currentEpoch(conversationID: conversationID)
    guard let data = try retrieve(forKey: key.key) else {
      return nil
    }

    guard data.count == MemoryLayout<Int64>.size else {
      logger.error("Invalid epoch data size for conversation: \(conversationID)")
      return nil
    }

    let epoch = data.withUnsafeBytes { $0.loadUnaligned(as: Int64.self) }
    return Int(epoch)
  }

  /// Delete stored epoch for a conversation
  public func deleteCurrentEpoch(forConversationID conversationID: String) throws {
    let key = KeychainKey.currentEpoch(conversationID: conversationID)
    try delete(forKey: key.key)
    logger.debug("Deleted current epoch for conversation: \(conversationID)")
  }

  // MARK: - Welcome Message Management

  /// Store Welcome message for a conversation (single-use for security)
  /// - Parameters:
  ///   - welcomeData: Serialized Welcome message from server
  ///   - conversationID: Conversation identifier
  /// - Throws: KeychainError if storage fails
  public func storeWelcomeMessage(_ welcomeData: Data, forConversationID conversationID: String)
    throws
  {
    let key = KeychainKey.welcomeMessage(conversationID: conversationID)
    try store(
      welcomeData, forKey: key.key, accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly)
    logger.info(
      "Stored Welcome message for conversation: \(conversationID) (\(welcomeData.count) bytes)")
  }

  /// Retrieve Welcome message for a conversation
  /// - Parameter conversationID: Conversation identifier
  /// - Returns: Serialized Welcome message, or nil if not found
  /// - Throws: KeychainError if retrieval fails
  public func retrieveWelcomeMessage(forConversationID conversationID: String) throws -> Data? {
    let key = KeychainKey.welcomeMessage(conversationID: conversationID)
    let data = try retrieve(forKey: key.key)

    if let data = data {
      logger.info(
        "Retrieved Welcome message for conversation: \(conversationID) (\(data.count) bytes)")
    }

    return data
  }

  /// Delete Welcome message for a conversation (call after successful group join)
  /// - Parameter conversationID: Conversation identifier
  /// - Throws: KeychainError if deletion fails
  public func deleteWelcomeMessage(forConversationID conversationID: String) throws {
    let key = KeychainKey.welcomeMessage(conversationID: conversationID)
    try delete(forKey: key.key)
    logger.info("Deleted Welcome message for conversation: \(conversationID)")
  }

  // MARK: - Invite PSK Storage

  /// Store pre-shared key for an invite
  /// - Parameters:
  ///   - psk: Pre-shared key data
  ///   - inviteID: Invite identifier
  /// - Throws: KeychainError if storage fails
  public func storeInvitePSK(_ psk: Data, for inviteID: String) throws {
    let key = "mls.invite.psk.\(inviteID)"
    try store(psk, forKey: key, accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly)
    logger.info("Stored invite PSK for invite: \(inviteID)")
  }

  /// Retrieve pre-shared key for an invite
  /// - Parameter inviteID: Invite identifier
  /// - Returns: Pre-shared key data, or nil if not found
  public func retrieveInvitePSK(for inviteID: String) -> Data? {
    let key = "mls.invite.psk.\(inviteID)"
    return try? retrieve(forKey: key)
  }

  /// Delete pre-shared key for an invite
  /// - Parameter inviteID: Invite identifier
  /// - Throws: KeychainError if deletion fails
  public func deleteInvitePSK(for inviteID: String) throws {
    let key = "mls.invite.psk.\(inviteID)"
    try delete(forKey: key)
    logger.info("Deleted invite PSK for invite: \(inviteID)")
  }

  // MARK: - Rejoin PSK Storage

  /// Store pre-shared key for rejoining a conversation
  /// - Parameters:
  ///   - psk: Pre-shared key data
  ///   - conversationID: Conversation identifier
  ///   - userDID: User's DID identifier
  /// - Throws: KeychainError if storage fails
  public func storeRejoinPSK(_ psk: Data, for conversationID: String, userDID: String) throws {
    let key = "mls.rejoin.psk.\(conversationID).\(userDID)"
    try store(psk, forKey: key, accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly)
    logger.info("Stored rejoin PSK for conversation: \(conversationID), user: \(userDID)")
  }

  /// Retrieve pre-shared key for rejoining a conversation
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - userDID: User's DID identifier
  /// - Returns: Pre-shared key data, or nil if not found
  public func retrieveRejoinPSK(for conversationID: String, userDID: String) -> Data? {
    let key = "mls.rejoin.psk.\(conversationID).\(userDID)"
    return try? retrieve(forKey: key)
  }

  /// Delete pre-shared key for rejoining a conversation
  /// - Parameters:
  ///   - conversationID: Conversation identifier
  ///   - userDID: User's DID identifier
  /// - Throws: KeychainError if deletion fails
  public func deleteRejoinPSK(for conversationID: String, userDID: String) throws {
    let key = "mls.rejoin.psk.\(conversationID).\(userDID)"
    try delete(forKey: key)
    logger.info("Deleted rejoin PSK for conversation: \(conversationID), user: \(userDID)")
  }

  // MARK: - General Key Storage (for FFI Bridge)

  /// Retrieve a generic key by key ID (wrapper for FFI bridge)
  /// - Parameter keyId: Key identifier
  /// - Returns: Key data, or nil if not found
  /// - Throws: KeychainError if retrieval fails
  public func retrieveKey(for keyId: String) throws -> Data? {
    return try retrieve(forKey: keyId)
  }

  /// Store a generic key by key ID (wrapper for FFI bridge)
  /// - Parameters:
  ///   - keyData: Key data to store
  ///   - keyId: Key identifier
  /// - Throws: KeychainError if storage fails
  public func storeKey(_ keyData: Data, for keyId: String) throws {
    try store(keyData, forKey: keyId, accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly)
  }

  /// Delete a generic key by key ID (wrapper for FFI bridge)
  /// - Parameter keyId: Key identifier
  /// - Throws: KeychainError if deletion fails
  public func deleteKey(for keyId: String) throws {
    try delete(forKey: keyId)
  }

  // MARK: - Private Key Management

  /// Store private key for a specific epoch
  public func storePrivateKey(
    _ key: Data,
    forConversationID conversationID: String,
    epoch: Int64
  ) throws {
    let keychainKey = KeychainKey.privateKey(conversationID: conversationID, epoch: epoch)
    // Use kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly for Notification Service Extension access
    try store(
      key, forKey: keychainKey.key, accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly)
    logger.info("Stored private key for conversation: \(conversationID), epoch: \(epoch)")
  }

  /// Retrieve private key for a specific epoch
  public func retrievePrivateKey(
    forConversationID conversationID: String,
    epoch: Int64
  ) throws -> Data? {
    let key = KeychainKey.privateKey(conversationID: conversationID, epoch: epoch)
    return try retrieve(forKey: key.key)
  }

  /// Delete private key for a specific epoch
  public func deletePrivateKey(
    forConversationID conversationID: String,
    epoch: Int64
  ) throws {
    let key = KeychainKey.privateKey(conversationID: conversationID, epoch: epoch)
    try delete(forKey: key.key)
    logger.debug("Deleted private key for conversation: \(conversationID), epoch: \(epoch)")
  }

  /// Delete all private keys for epochs before the specified epoch
  public func deletePrivateKeys(
    forConversationID conversationID: String,
    beforeEpoch epoch: Int64
  ) throws {
    // Keychain doesn't support range queries, so we need to track epochs separately
    // For now, we'll delete keys one by one for known epochs
    for oldEpoch in 0..<epoch {
      try? deletePrivateKey(forConversationID: conversationID, epoch: oldEpoch)
    }
    logger.info("Deleted private keys before epoch \(epoch) for conversation: \(conversationID)")
  }

  // MARK: - Signature Key Management

  /// Store signature private key for a conversation
  public func storeSignatureKey(_ key: Data, forConversationID conversationID: String) throws {
    let keychainKey = KeychainKey.signatureKey(conversationID: conversationID)
    // Use kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly for Notification Service Extension access
    try store(
      key, forKey: keychainKey.key, accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly)
    logger.info("Stored signature key for conversation: \(conversationID)")
  }

  /// Retrieve signature private key for a conversation
  public func retrieveSignatureKey(forConversationID conversationID: String) throws -> Data? {
    let key = KeychainKey.signatureKey(conversationID: conversationID)
    return try retrieve(forKey: key.key)
  }

  /// Delete signature key for a conversation
  public func deleteSignatureKey(forConversationID conversationID: String) throws {
    let key = KeychainKey.signatureKey(conversationID: conversationID)
    try delete(forKey: key.key)
    logger.info("Deleted signature key for conversation: \(conversationID)")
  }

  // MARK: - Encryption Key Management

  /// Store encryption key for a conversation
  public func storeEncryptionKey(_ key: Data, forConversationID conversationID: String) throws {
    let keychainKey = KeychainKey.encryptionKey(conversationID: conversationID)
    // Use kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly for Notification Service Extension access
    try store(
      key, forKey: keychainKey.key, accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly)
    logger.info("Stored encryption key for conversation: \(conversationID)")
  }

  /// Retrieve encryption key for a conversation
  public func retrieveEncryptionKey(forConversationID conversationID: String) throws -> Data? {
    let key = KeychainKey.encryptionKey(conversationID: conversationID)
    return try retrieve(forKey: key.key)
  }

  /// Delete encryption key for a conversation
  public func deleteEncryptionKey(forConversationID conversationID: String) throws {
    let key = KeychainKey.encryptionKey(conversationID: conversationID)
    try delete(forKey: key.key)
    logger.info("Deleted encryption key for conversation: \(conversationID)")
  }

  // MARK: - Epoch Secrets Management

  /// Store epoch secrets (application secrets, exporter secrets, etc.)
  /// Also stores metadata for retention policy tracking
  public func storeEpochSecrets(
    _ secrets: Data,
    forConversationID conversationID: String,
    epoch: Int64
  ) throws {
    let key = KeychainKey.epochSecrets(conversationID: conversationID, epoch: epoch)
    try store(secrets, forKey: key.key, accessible: kSecAttrAccessibleWhenUnlockedThisDeviceOnly)
    logger.info("Stored epoch secrets for conversation: \(conversationID), epoch: \(epoch)")

    // Store metadata for retention tracking
    let metadata = EpochKeyMetadata(
      conversationID: conversationID,
      epoch: epoch,
      createdAt: Date()
    )
    try storeEpochMetadata(metadata, forConversationID: conversationID, epoch: epoch)
  }

  /// Retrieve epoch secrets
  public func retrieveEpochSecrets(
    forConversationID conversationID: String,
    epoch: Int64
  ) throws -> Data? {
    let key = KeychainKey.epochSecrets(conversationID: conversationID, epoch: epoch)
    return try retrieve(forKey: key.key)
  }

  /// Delete epoch secrets
  public func deleteEpochSecrets(
    forConversationID conversationID: String,
    epoch: Int64
  ) throws {
    let key = KeychainKey.epochSecrets(conversationID: conversationID, epoch: epoch)
    try delete(forKey: key.key)
    logger.debug("Deleted epoch secrets for conversation: \(conversationID), epoch: \(epoch)")
  }

  // MARK: - HPKE Key Management

  /// Store HPKE private key for a key package
  public func storeHPKEPrivateKey(_ key: Data, forKeyPackageID keyPackageID: String) throws {
    let keychainKey = KeychainKey.hpkePrivateKey(keyPackageID: keyPackageID)
    // Use kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly for Notification Service Extension access
    try store(
      key, forKey: keychainKey.key, accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly)
    logger.info("Stored HPKE private key for key package: \(keyPackageID)")
  }

  /// Retrieve HPKE private key for a key package
  public func retrieveHPKEPrivateKey(forKeyPackageID keyPackageID: String) throws -> Data? {
    let key = KeychainKey.hpkePrivateKey(keyPackageID: keyPackageID)
    return try retrieve(forKey: key.key)
  }

  /// Delete HPKE private key for a key package
  public func deleteHPKEPrivateKey(forKeyPackageID keyPackageID: String) throws {
    let key = KeychainKey.hpkePrivateKey(keyPackageID: keyPackageID)
    try delete(forKey: key.key)
    logger.info("Deleted HPKE private key for key package: \(keyPackageID)")
  }

  // MARK: - Batch Operations

  /// Delete all keys associated with a conversation
  public func deleteAllKeys(forConversationID conversationID: String) throws {
    try deleteGroupState(forConversationID: conversationID)
    try deleteSignatureKey(forConversationID: conversationID)
    try deleteEncryptionKey(forConversationID: conversationID)
    try deleteCurrentEpoch(forConversationID: conversationID)

    // Delete epoch-specific keys (attempt for reasonable range)
    for epoch in 0...1000 {
      try? deletePrivateKey(forConversationID: conversationID, epoch: Int64(epoch))
      try? deleteEpochSecrets(forConversationID: conversationID, epoch: Int64(epoch))
    }

    logger.info("Deleted all keys for conversation: \(conversationID)")
  }

  // MARK: - Core Keychain Operations

  public func store(
    _ data: Data,
    forKey key: String,
    accessible: CFString = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
  ) throws {
    // Delete existing item first
    try? delete(forKey: key)

    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrAccount as String: key,
      kSecAttrService as String: "blue.catbird.mls",
      kSecValueData as String: data,
      kSecAttrAccessible as String: accessible,
      kSecAttrSynchronizable as String: false,
    ]

    if let accessGroup = accessGroup {
      query[kSecAttrAccessGroup as String] = accessGroup
    }

    let status = SecItemAdd(query as CFDictionary, nil)

    guard status == errSecSuccess else {
      logger.error("Failed to store keychain item: \(key), status: \(status)")
      throw KeychainError.storeFailed(status)
    }
  }

  public func retrieve(forKey key: String) throws -> Data? {
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrAccount as String: key,
      kSecAttrService as String: "blue.catbird.mls",
      kSecReturnData as String: true,
      kSecMatchLimit as String: kSecMatchLimitOne,
    ]

    if let accessGroup = accessGroup {
      query[kSecAttrAccessGroup as String] = accessGroup
    }

    var result: AnyObject?
    let status = SecItemCopyMatching(query as CFDictionary, &result)

    if status == errSecItemNotFound {
      return nil
    }

    guard status == errSecSuccess else {
      logger.error("Failed to retrieve keychain item: \(key), status: \(status)")
      throw KeychainError.retrieveFailed(status)
    }

    return result as? Data
  }

  public func delete(forKey key: String) throws {
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrAccount as String: key,
      kSecAttrService as String: "blue.catbird.mls",
    ]

    if let accessGroup = accessGroup {
      query[kSecAttrAccessGroup as String] = accessGroup
    }

    let status = SecItemDelete(query as CFDictionary)

    guard status == errSecSuccess || status == errSecItemNotFound else {
      logger.error("Failed to delete keychain item: \(key), status: \(status)")
      throw KeychainError.deleteFailed(status)
    }
  }

  // MARK: - Epoch Metadata Management (for retention policy)

  /// Store epoch metadata for retention tracking
  internal func storeEpochMetadata(
    _ metadata: EpochKeyMetadata,
    forConversationID conversationID: String,
    epoch: Int64
  ) throws {
    let key = "mls.epoch.metadata.\(conversationID).epoch.\(epoch)"
    let data = try JSONEncoder().encode(metadata)
    try store(data, forKey: key, accessible: kSecAttrAccessibleAfterFirstUnlock)
    logger.debug("Stored epoch metadata for conversation: \(conversationID), epoch: \(epoch)")
  }

  /// Retrieve epoch metadata
  internal func retrieveEpochMetadata(
    forConversationID conversationID: String,
    epoch: Int64
  ) throws -> EpochKeyMetadata? {
    let key = "mls.epoch.metadata.\(conversationID).epoch.\(epoch)"
    guard let data = try retrieve(forKey: key) else {
      return nil
    }
    return try JSONDecoder().decode(EpochKeyMetadata.self, from: data)
  }

  /// Delete epoch metadata
  internal func deleteEpochMetadata(
    forConversationID conversationID: String,
    epoch: Int64
  ) throws {
    let key = "mls.epoch.metadata.\(conversationID).epoch.\(epoch)"
    try delete(forKey: key)
    logger.debug("Deleted epoch metadata for conversation: \(conversationID), epoch: \(epoch)")
  }

  // MARK: - Key Rotation Support

  /// Store archived key for recovery purposes
  public func archiveKey(
    _ key: Data,
    type: String,
    conversationID: String,
    epoch: Int64
  ) throws {
    let archiveKey = "mls.archive.\(type).\(conversationID).epoch.\(epoch)"
    try store(key, forKey: archiveKey, accessible: kSecAttrAccessibleAfterFirstUnlock)
    logger.info("Archived \(type) key for conversation: \(conversationID), epoch: \(epoch)")
  }

  /// Retrieve archived key
  public func retrieveArchivedKey(
    type: String,
    conversationID: String,
    epoch: Int64
  ) throws -> Data? {
    let archiveKey = "mls.archive.\(type).\(conversationID).epoch.\(epoch)"
    return try retrieve(forKey: archiveKey)
  }

  // MARK: - Security Utilities

  /// Generate a secure random key
  public func generateSecureRandomKey(length: Int = 32) throws -> Data {
    var bytes = [UInt8](repeating: 0, count: length)
    let status = SecRandomCopyBytes(kSecRandomDefault, length, &bytes)

    guard status == errSecSuccess else {
      throw KeychainError.randomGenerationFailed(status)
    }

    return Data(bytes)
  }

  /// Verify keychain accessibility
  public func verifyKeychainAccess() throws {
    let testKey = "mls.test.access"
    let testData = Data("test".utf8)

    try store(testData, forKey: testKey)

    guard let retrieved = try retrieve(forKey: testKey),
      retrieved == testData
    else {
      throw KeychainError.accessVerificationFailed
    }

    try delete(forKey: testKey)
    logger.info("Keychain access verified successfully")
  }
}

// MARK: - Errors

public enum KeychainError: LocalizedError {
  case storeFailed(OSStatus)
  case retrieveFailed(OSStatus)
  case deleteFailed(OSStatus)
  case randomGenerationFailed(OSStatus)
  case accessVerificationFailed

  public var errorDescription: String? {
    switch self {
    case .storeFailed(let status):
      return "Failed to store item in keychain: \(status)"
    case .retrieveFailed(let status):
      return "Failed to retrieve item from keychain: \(status)"
    case .deleteFailed(let status):
      return "Failed to delete item from keychain: \(status)"
    case .randomGenerationFailed(let status):
      return "Failed to generate random data: \(status)"
    case .accessVerificationFailed:
      return "Keychain access verification failed"
    }
  }
}
