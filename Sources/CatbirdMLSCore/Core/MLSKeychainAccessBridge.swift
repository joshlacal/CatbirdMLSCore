//
//  MLSKeychainAccessBridge.swift
//  CatbirdMLSCore
//
//  Bridges Swift Keychain access to Rust FFI
//

import Foundation
import CatbirdMLS
import Security

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
        let scoped = scopedKey(key)
        if let existing = try MLSKeychainManager.shared.retrieveKey(for: scoped) {
            if existing == value {
                return
            } else {
                throw MLSStorageInitializationError.keychainError(errSecDuplicateItem)
            }
        }

        var query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrAccount as String: scoped,
            kSecAttrService as String: MLSKeychainManager.shared.serviceName,
            kSecValueData as String: value,
        ]

        if !MLSKeychainManager.shared.skipDataProtection {
            query[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
            query[kSecAttrSynchronizable as String] = false
        }

        #if os(macOS) || targetEnvironment(macCatalyst)
        if !MLSKeychainManager.shared.skipDataProtection {
            query[kSecUseDataProtectionKeychain as String] = true
        }
        #endif

        if let accessGroup = MLSKeychainManager.shared.effectiveAccessGroup {
            query[kSecAttrAccessGroup as String] = accessGroup
        }

        let status = SecItemAdd(query as CFDictionary, nil)
        if status == errSecSuccess {
            return
        } else if status == errSecDuplicateItem {
            guard let winner = try MLSKeychainManager.shared.retrieveKey(for: scoped) else {
                throw KeychainError.retrieveFailed(errSecItemNotFound)
            }
            if winner == value {
                return
            } else {
                throw MLSStorageInitializationError.keychainError(errSecDuplicateItem)
            }
        } else {
            throw KeychainError.storeFailed(status)
        }
    }

    func delete(key: String) async throws {
        try MLSKeychainManager.shared.deleteKey(for: scopedKey(key))
    }
}
