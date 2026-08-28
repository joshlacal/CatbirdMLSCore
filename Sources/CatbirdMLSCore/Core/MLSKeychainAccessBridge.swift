//
//  MLSKeychainAccessBridge.swift
//  CatbirdMLSCore
//
//  Bridges Swift Keychain access to Rust FFI
//

import Foundation
import CatbirdMLS
import Security

/// Bridges iOS Keychain access for Rust FFI using per-DID scoped clean service
final class MLSKeychainAccessBridge: KeychainAccess {
    let userDID: String
    let service: String

    init(userDID: String = "") {
        let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        self.userDID = normalized
        if !normalized.isEmpty {
            self.service = MLSStoragePaths.hybridSignerService(for: normalized)
        } else {
            self.service = MLSKeychainManager.shared.serviceName
        }
    }

    private func scopedKey(_ key: String) -> String {
        MLSStoragePaths.hybridSignerSlot(key: key)
    }

    func read(key: String) async throws -> Data? {
        try MLSKeychainManager.shared.retrieveKeyStrict(forKey: scopedKey(key), service: service)
    }

    func write(key: String, value: Data) async throws {
        try MLSKeychainManager.shared.storeImmutableKeyStrict(
            value,
            forKey: scopedKey(key),
            service: service
        )
    }

    func delete(key: String) async throws {
        try MLSKeychainManager.shared.deleteStrict(forKey: scopedKey(key), service: service)
    }

    static func deleteServiceAll(for userDID: String) throws {
        let service = MLSStoragePaths.hybridSignerService(for: userDID)
        try MLSKeychainManager.shared.deleteAllStrict(forService: service)
    }
}
