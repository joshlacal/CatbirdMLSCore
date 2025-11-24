//
//  MLSKeychainAccessBridge.swift
//  CatbirdMLSCore
//
//  Bridges Swift Keychain access to Rust FFI
//

import Foundation
import MLSFFI

/// Bridges iOS Keychain access for Rust FFI
final class MLSKeychainAccessBridge: KeychainAccess {
  func read(key: String) throws -> Data? {
    // Use MLSKeychainManager to retrieve key
    return try MLSKeychainManager.shared.retrieveKey(for: key)
  }

  func write(key: String, value: Data) throws {
    try MLSKeychainManager.shared.storeKey(value, for: key)
  }

  func delete(key: String) throws {
    try MLSKeychainManager.shared.deleteKey(for: key)
  }
}
