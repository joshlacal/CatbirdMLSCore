//
//  MLSContentRootKey.swift
//  CatbirdMLSCore
//
//  Per-DID content-root key (field-level encryption) Keychain helper.
//

import Foundation
import Security

public enum MLSContentRootKeyError: Error {
  case keychainError(OSStatus)
  case invalidStoredKey
  case keyGenerationFailed
}

/// Per-DID content-root key used by the field-level encryption layer in
/// `MLSStorage`. Stored in a Keychain item distinct from the SQLCipher
/// database key so that compromise of the SQLCipher item does not directly
/// expose the content root.
public enum MLSContentRootKey {
  private static let service = "blue.catbird.mls.content"
  private static let accessGroupSuffix = "blue.catbird.shared"
  private static let keyLength = 32

  public static func loadOrCreate(for userDID: String) throws -> Data {
    if let existing = try load(for: userDID) {
      return existing
    }
    var bytes = [UInt8](repeating: 0, count: keyLength)
    let status = SecRandomCopyBytes(kSecRandomDefault, keyLength, &bytes)
    guard status == errSecSuccess else {
      throw MLSContentRootKeyError.keyGenerationFailed
    }
    let data = Data(bytes)
    try store(data, for: userDID)
    return data
  }

  public static func delete(for userDID: String) throws {
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: service,
      kSecAttrAccount as String: account(for: userDID),
    ]
    if let group = MLSKeychainManager.resolvedAccessGroup(suffix: accessGroupSuffix) {
      query[kSecAttrAccessGroup as String] = group
    }
    let status = SecItemDelete(query as CFDictionary)
    if status != errSecSuccess && status != errSecItemNotFound {
      throw MLSContentRootKeyError.keychainError(status)
    }
  }

  private static func load(for userDID: String) throws -> Data? {
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: service,
      kSecAttrAccount as String: account(for: userDID),
      kSecReturnData as String: true,
      kSecMatchLimit as String: kSecMatchLimitOne,
    ]
    if let group = MLSKeychainManager.resolvedAccessGroup(suffix: accessGroupSuffix) {
      query[kSecAttrAccessGroup as String] = group
    }
    var result: AnyObject?
    let status = SecItemCopyMatching(query as CFDictionary, &result)
    switch status {
    case errSecSuccess:
      guard let data = result as? Data, data.count == keyLength else {
        throw MLSContentRootKeyError.invalidStoredKey
      }
      return data
    case errSecItemNotFound:
      return nil
    default:
      throw MLSContentRootKeyError.keychainError(status)
    }
  }

  private static func store(_ data: Data, for userDID: String) throws {
    var attrs: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: service,
      kSecAttrAccount as String: account(for: userDID),
      kSecValueData as String: data,
      kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
      kSecAttrSynchronizable as String: false,
    ]
    if let group = MLSKeychainManager.resolvedAccessGroup(suffix: accessGroupSuffix) {
      attrs[kSecAttrAccessGroup as String] = group
    }
    let status = SecItemAdd(attrs as CFDictionary, nil)
    guard status == errSecSuccess else {
      throw MLSContentRootKeyError.keychainError(status)
    }
  }

  private static func account(for userDID: String) -> String {
    "mls.content.root.\(userDID)"
  }
}
