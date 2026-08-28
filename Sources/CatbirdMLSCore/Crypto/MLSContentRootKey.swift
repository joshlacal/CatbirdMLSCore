//
//  MLSContentRootKey.swift
//  CatbirdMLSCore
//
//  Per-DID content-root key (field-level encryption) Keychain helper.
//

import Foundation
import Security

public enum MLSContentRootKeyError: Error, LocalizedError {
  case keychainError(OSStatus)
  case invalidStoredKey
  case keyGenerationFailed

  public var errorDescription: String? {
    switch self {
    case .keychainError(let status):
      return "Keychain error for content root key: \(status)"
    case .invalidStoredKey:
      return "Invalid content root key in Keychain (wrong length or format)"
    case .keyGenerationFailed:
      return "Failed to generate random content root key"
    }
  }
}

/// Per-DID content-root key used by the field-level encryption layer in
/// `MLSStorage`. Stored in a Keychain item distinct from the SQLCipher
/// database key so that compromise of the SQLCipher item does not directly
/// expose the content root.
public enum MLSContentRootKey {
  private static let service = "blue.catbird.mls.content"
  private static let keyLength = 32

  public static func loadOrCreate(for userDID: String) throws -> Data {
    let acc = MLSStoragePaths.contentRootAccount(for: userDID)
    do {
      return try MLSKeychainManager.shared.getOrCreateImmutableKey(
        forKey: acc,
        service: service,
        length: keyLength
      )
    } catch {
      throw MLSContentRootKeyError.keyGenerationFailed
    }
  }

  static func loadStrict(for userDID: String) throws -> Data {
    let acc = MLSStoragePaths.contentRootAccount(for: userDID)
    guard let data = try MLSKeychainManager.shared.retrieveKeyStrict(
      forKey: acc,
      service: service,
      expectedLength: keyLength
    ) else {
      throw MLSContentRootKeyError.invalidStoredKey
    }
    return data
  }

  public static func delete(for userDID: String) throws {
    let acc = MLSStoragePaths.contentRootAccount(for: userDID)
    do {
      try MLSKeychainManager.shared.deleteStrict(forKey: acc, service: service)
    } catch {
      if let kcErr = error as? KeychainError, case .deleteFailed(let status) = kcErr {
        throw MLSContentRootKeyError.keychainError(status)
      }
      throw error
    }
  }

  private static func load(for userDID: String) throws -> Data? {
    let acc = MLSStoragePaths.contentRootAccount(for: userDID)
    return try MLSKeychainManager.shared.retrieveKeyStrict(
      forKey: acc,
      service: service,
      expectedLength: keyLength
    )
  }
}
