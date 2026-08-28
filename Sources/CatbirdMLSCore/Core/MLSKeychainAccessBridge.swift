//
//  MLSKeychainAccessBridge.swift
//  CatbirdMLSCore
//
//  Bridges Swift Keychain access to Rust FFI
//

import Foundation
import CatbirdMLS

/// Bridges iOS Keychain access for Rust FFI
final class MLSKeychainAccessBridge: KeychainAccess {
    private func scopedKey(_ key: String) -> String {
        if key.hasSuffix(MLSStoragePaths.cleanIdentifierSuffix) {
            return key
        }
        return "\(key)\(MLSStoragePaths.cleanIdentifierSuffix)"
    }

    func read(key: String) async throws -> Data? {
        return try MLSKeychainManager.shared.retrieveKey(for: scopedKey(key))
    }

    func write(key: String, value: Data) async throws {
        try MLSKeychainManager.shared.storeKey(value, for: scopedKey(key))
    }

    func delete(key: String) async throws {
        try MLSKeychainManager.shared.deleteKey(for: scopedKey(key))
    }
}
