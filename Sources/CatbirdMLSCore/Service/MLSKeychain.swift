//
//  MLSKeychain.swift
//  Catbird
//
//  Secure storage for MLS cryptographic keys using iOS Keychain.
//  Implements hardware-backed security with optional Secure Enclave support.
//

import Foundation
import Security
import OSLog
import Synchronization
import Petrel

/// Errors that can occur during Keychain operations
public enum MLSKeychainError: Error {
    case storeFailed(OSStatus)
    case retrieveFailed(OSStatus)
    case deleteFailed(OSStatus)
    case invalidData
    case secureEnclaveUnavailable
    
    public var localizedDescription: String {
        switch self {
        case .storeFailed(let status):
            return "Failed to store key in Keychain (status: \(status))"
        case .retrieveFailed(let status):
            return "Failed to retrieve key from Keychain (status: \(status))"
        case .deleteFailed(let status):
            return "Failed to delete key from Keychain (status: \(status))"
        case .invalidData:
            return "Invalid key data"
        case .secureEnclaveUnavailable:
            return "Secure Enclave not available on this device"
        }
    }
}

/// Secure storage manager for MLS signature keys
public class MLSKeychain {
    private static let logger = Logger(subsystem: Bundle.main.bundleIdentifier ?? "blue.catbird", category: "MLSKeychain")

    /// Service name used when storing signature keys as GenericPassword (daemon / non-sandboxed CLI mode).
    private static let signatureKeyService = "blue.catbird.mls.signature"

    /// Service name used when storing group keys as GenericPassword (daemon / non-sandboxed CLI mode).
    private static let groupKeyService = "blue.catbird.mls.groupkey"

    private static let secItemDeleteOverrideMutex = Mutex<(@Sendable ([String: Any]) -> OSStatus)?>(nil)

    internal static func setSecItemDeleteOverrideForTesting(
        _ override: (@Sendable ([String: Any]) -> OSStatus)?
    ) {
        secItemDeleteOverrideMutex.withLock { $0 = override }
    }

    private static func performSecItemDelete(_ query: [String: Any]) -> OSStatus {
        if let override = secItemDeleteOverrideMutex.withLock({ $0 }) {
            return override(query)
        }
        return KeychainSecItem.delete(query as CFDictionary)
    }

    // MARK: - Clean Generation (OpenMLS 0.9)

    static func storeSignatureKeyClean(
        _ key: Data,
        forIdentity identity: String
    ) throws {
        guard !key.isEmpty else {
            throw MLSKeychainError.invalidData
        }
        let tag = MLSStoragePaths.orchestratorSignerAccount(for: identity)
        _ = try MLSKeychainManager.shared.storeOrAdoptImmutableKey(
            key,
            forKey: tag,
            service: signatureKeyService
        )
    }

    static func retrieveSignatureKeyClean(forIdentity identity: String) throws -> Data? {
        let tag = MLSStoragePaths.orchestratorSignerAccount(for: identity)
        return try MLSKeychainManager.shared.retrieveKeyStrict(
            forKey: tag,
            service: signatureKeyService
        )
    }

    static func deleteSignatureKeyClean(forIdentity identity: String) throws {
        let tag = MLSStoragePaths.orchestratorSignerAccount(for: identity)
        try MLSKeychainManager.shared.deleteStrict(
            forKey: tag,
            service: signatureKeyService
        )
    }
    // MARK: - Legacy / General Keychain Storage
    
    
    /// Store a signature key in the Keychain
    /// - Parameters:
    ///   - key: The key data to store
    ///   - identity: The identity (DID) associated with this key
    ///   - useSecureEnclave: Attempt to use Secure Enclave (if available)
    /// - Throws: MLSKeychainError if storage fails
    public static func storeSignatureKey(
        _ key: Data,
        forIdentity identity: String,
        useSecureEnclave: Bool = false
    ) throws {
        guard !key.isEmpty else {
            throw MLSKeychainError.invalidData
        }

        _ = useSecureEnclave  // no longer used — see note below

        let tag = "blue.catbird.mls.sig.\(identity)"
        let skipDP = MLSKeychainManager.shared.skipDataProtection

        // Delete any existing item first — covers both the current
        // GenericPassword storage AND the legacy kSecClassKey storage.
        try? deleteSignatureKey(forIdentity: identity)

        // Store as an OPAQUE GenericPassword item on every platform.
        //
        // The blob is a serialized OpenMLS `SignatureKeyPair` (JSON, hundreds of
        // bytes) — NOT a raw 32-byte EC key. An earlier version stored it under
        // `kSecClassKey` with `kSecAttrKeyType = ECSECPrimeRandom` +
        // `kSecAttrKeySizeInBits = 256`, i.e. it told the keychain "this is a
        // 256-bit EC key." The keychain does not round-trip an arbitrary blob
        // under that key-class/attribute contract, so `get_signing_key` handed
        // the orchestrator unusable bytes, `import_identity_key` failed to
        // deserialize, `durable_signer_in_sync` stayed false, and the device
        // DESTRUCTIVELY re-registered on every launch/account-switch (fresh
        // device_uuid, remove_device deleting its published key packages, then
        // 20 republishes) — churning key packages and stranding new members
        // whose Welcomes referenced the deleted packages. GenericPassword
        // round-trips arbitrary `Data` reliably, so the durable signer survives
        // and the device registers ONCE. (Secure Enclave can only hold EC
        // private keys, never this composite blob, so `useSecureEnclave` was
        // never valid here and is ignored.)
        var query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: signatureKeyService,
            kSecAttrAccount as String: tag,
            kSecValueData as String: key,
        ]
        if !skipDP {
            query[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        }

        let status = KeychainSecItem.add(query as CFDictionary, nil)
        if status == errSecDuplicateItem {
            let updateQuery: [String: Any] = [
                kSecClass as String: kSecClassGenericPassword,
                kSecAttrService as String: signatureKeyService,
                kSecAttrAccount as String: tag,
            ]
            let updateAttrs: [String: Any] = [kSecValueData as String: key]
            let updateStatus = KeychainSecItem.update(updateQuery as CFDictionary, updateAttrs as CFDictionary)
            guard updateStatus == errSecSuccess else {
                throw MLSKeychainError.storeFailed(updateStatus)
            }
        } else if status != errSecSuccess {
            throw MLSKeychainError.storeFailed(status)
        }

        logger.info("Stored MLS signature key for identity: \(identity, privacy: .private)")
    }
    
    /// Retrieve a signature key from the Keychain
    /// - Parameter identity: The identity (DID) associated with the key
    /// - Returns: The key data
    /// - Throws: MLSKeychainError if retrieval fails
    public static func retrieveSignatureKey(forIdentity identity: String) throws -> Data {
        let tag = "blue.catbird.mls.sig.\(identity)"

        // Current storage: opaque GenericPassword.
        let gpQuery: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: signatureKeyService,
            kSecAttrAccount as String: tag,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var result: AnyObject?
        var status = KeychainSecItem.copyMatching(gpQuery as CFDictionary, &result)
        if status == errSecSuccess, let keyData = result as? Data {
            return keyData
        }

        // Migration fallback: adopt a durable signer written by the legacy
        // kSecClassKey path (present only on devices upgraded from the version
        // that mis-stored the blob as an EC key). The next store rewrites it as
        // a GenericPassword, so this path is hit at most once per identity.
        let legacyQuery: [String: Any] = [
            kSecClass as String: kSecClassKey,
            kSecAttrApplicationTag as String: tag,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        result = nil
        status = KeychainSecItem.copyMatching(legacyQuery as CFDictionary, &result)
        guard status == errSecSuccess, let keyData = result as? Data else {
            // Surface not-found so the caller (get_signing_key) can return nil.
            throw MLSKeychainError.retrieveFailed(status)
        }
        return keyData
    }
    
    /// Delete a signature key from the Keychain
    /// - Parameter identity: The identity (DID) associated with the key
    /// - Throws: MLSKeychainError if deletion fails
    public static func deleteSignatureKey(forIdentity identity: String) throws {
        let tag = "blue.catbird.mls.sig.\(identity)"

        // Current GenericPassword storage.
        let gpQuery: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: signatureKeyService,
            kSecAttrAccount as String: tag,
        ]
        let gpStatus = KeychainSecItem.delete(gpQuery as CFDictionary)
        guard gpStatus == errSecSuccess || gpStatus == errSecItemNotFound else {
            throw MLSKeychainError.deleteFailed(gpStatus)
        }

        // Legacy kSecClassKey storage (migration cleanup so a stale item can't
        // shadow the GenericPassword one on read).
        let legacyQuery: [String: Any] = [
            kSecClass as String: kSecClassKey,
            kSecAttrApplicationTag as String: tag,
        ]
        let legacyStatus = KeychainSecItem.delete(legacyQuery as CFDictionary)
        guard legacyStatus == errSecSuccess || legacyStatus == errSecItemNotFound else {
            throw MLSKeychainError.deleteFailed(legacyStatus)
        }
    }
    
    /// Delete all MLS keys from the Keychain (e.g., on logout)
    public static func deleteAllKeys() throws {
        let skipDP = MLSKeychainManager.shared.skipDataProtection
        let hasDeleteOverride = secItemDeleteOverrideMutex.withLock { $0 != nil }

        if let fake = MLSKeychainManager.activeFakeStorage {
            let sigFakeStatus = try fake.deleteAll(service: signatureKeyService)
            guard sigFakeStatus == errSecSuccess || sigFakeStatus == errSecItemNotFound else {
                throw MLSKeychainError.deleteFailed(sigFakeStatus)
            }
            let groupFakeStatus = try fake.deleteAll(service: groupKeyService)
            guard groupFakeStatus == errSecSuccess || groupFakeStatus == errSecItemNotFound else {
                throw MLSKeychainError.deleteFailed(groupFakeStatus)
            }
            if !hasDeleteOverride {
                logger.info("Deleted all MLS keys from fake Keychain storage")
                return
            }
        }

        for service in [signatureKeyService, groupKeyService] {
            let genericPasswordQuery: [String: Any] = [
                kSecClass as String: kSecClassGenericPassword,
                kSecAttrService as String: service,
            ]
            let status = performSecItemDelete(genericPasswordQuery)
            guard status == errSecSuccess || status == errSecItemNotFound else {
                throw MLSKeychainError.deleteFailed(status)
            }

            if !skipDP, let accessGroup = MLSKeychainManager.shared.effectiveAccessGroup {
                var dpQuery: [String: Any] = [
                    kSecClass as String: kSecClassGenericPassword,
                    kSecAttrService as String: service,
                    kSecAttrAccessGroup as String: accessGroup,
                    kSecAttrSynchronizable as String: kSecAttrSynchronizableAny,
                ]
                #if os(macOS) || targetEnvironment(macCatalyst)
                dpQuery[kSecUseDataProtectionKeychain as String] = true
                #endif

                let dpStatus = performSecItemDelete(dpQuery)
                guard dpStatus == errSecSuccess
                    || dpStatus == errSecItemNotFound
                    || dpStatus == errSecMissingEntitlement
                else {
                    throw MLSKeychainError.deleteFailed(dpStatus)
                }
            }
        }

        logger.info("Deleted all MLS keys from Keychain")
    }
    
    // MARK: - Secure Enclave Detection
    
    /// Check if Secure Enclave is available on this device
    /// - Returns: true if Secure Enclave is available
    public static func isSecureEnclaveAvailable() -> Bool {
        // Secure Enclave is available on iPhone 5s and later, iPad Air and later
        // Check by attempting to create a key with Secure Enclave flag
        
        let attributes: [String: Any] = [
            kSecAttrTokenID as String: kSecAttrTokenIDSecureEnclave,
            kSecAttrKeyType as String: kSecAttrKeyTypeECSECPrimeRandom,
            kSecAttrKeySizeInBits as String: 256,
            kSecPrivateKeyAttrs as String: [
                kSecAttrIsPermanent as String: false
            ]
        ]
        
        var error: Unmanaged<CFError>?
        guard let _ = SecKeyCreateRandomKey(attributes as CFDictionary, &error) else {
            return false
        }
        
        return true
    }
    
    // MARK: - Group Key Storage
    
    /// Store a group's encryption key (for backup/restore scenarios)
    /// Note: In production MLS, group keys are derived and not directly stored.
    /// This is for future backup/restore functionality only.
    public static func storeGroupKey(_ key: Data, forGroupId groupId: Data) throws {
        guard !key.isEmpty else {
            throw MLSKeychainError.invalidData
        }

        let groupIdHex = groupId.map { String(format: "%02x", $0) }.joined()
        let tag = "blue.catbird.mls.group.\(groupIdHex)"
        let skipDP = MLSKeychainManager.shared.skipDataProtection

        try? deleteGroupKey(forGroupId: groupId)

        if let fake = MLSKeychainManager.activeFakeStorage {
            _ = try? fake.delete(service: groupKeyService, account: tag)
            let status = try fake.add(service: groupKeyService, account: tag, data: key)
            guard status == errSecSuccess || status == errSecDuplicateItem else {
                throw MLSKeychainError.storeFailed(status)
            }
            return
        }

        var query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: groupKeyService,
            kSecAttrAccount as String: tag,
            kSecValueData as String: key,
        ]
        if !skipDP {
            query[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        }
        let status = KeychainSecItem.add(query as CFDictionary, nil)
        if status == errSecDuplicateItem {
            let updateQuery: [String: Any] = [
                kSecClass as String: kSecClassGenericPassword,
                kSecAttrService as String: groupKeyService,
                kSecAttrAccount as String: tag,
            ]
            let updateAttrs: [String: Any] = [kSecValueData as String: key]
            let updateStatus = KeychainSecItem.update(updateQuery as CFDictionary, updateAttrs as CFDictionary)
            guard updateStatus == errSecSuccess else {
                throw MLSKeychainError.storeFailed(updateStatus)
            }
        } else if status != errSecSuccess {
            throw MLSKeychainError.storeFailed(status)
        }
    }

    /// Retrieve a group's encryption key
    public static func retrieveGroupKey(forGroupId groupId: Data) throws -> Data {
        let groupIdHex = groupId.map { String(format: "%02x", $0) }.joined()
        let tag = "blue.catbird.mls.group.\(groupIdHex)"

        if let fake = MLSKeychainManager.activeFakeStorage {
            guard let data = try fake.get(service: groupKeyService, account: tag) else {
                throw MLSKeychainError.retrieveFailed(errSecItemNotFound)
            }
            return data
        }

        let gpQuery: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: groupKeyService,
            kSecAttrAccount as String: tag,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var result: AnyObject?
        var status = KeychainSecItem.copyMatching(gpQuery as CFDictionary, &result)
        if status == errSecSuccess, let keyData = result as? Data {
            return keyData
        }

        let legacyQuery: [String: Any] = [
            kSecClass as String: kSecClassKey,
            kSecAttrApplicationTag as String: tag,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        result = nil
        status = KeychainSecItem.copyMatching(legacyQuery as CFDictionary, &result)
        guard status == errSecSuccess, let keyData = result as? Data else {
            throw MLSKeychainError.retrieveFailed(status)
        }
        return keyData
    }

    /// Delete a group's encryption key
    public static func deleteGroupKey(forGroupId groupId: Data) throws {
        let groupIdHex = groupId.map { String(format: "%02x", $0) }.joined()
        let tag = "blue.catbird.mls.group.\(groupIdHex)"

        if let fake = MLSKeychainManager.activeFakeStorage {
            let status = try fake.delete(service: groupKeyService, account: tag)
            guard status == errSecSuccess || status == errSecItemNotFound else {
                throw MLSKeychainError.deleteFailed(status)
            }
            return
        }

        let gpQuery: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: groupKeyService,
            kSecAttrAccount as String: tag,
        ]
        let gpStatus = performSecItemDelete(gpQuery)
        guard gpStatus == errSecSuccess || gpStatus == errSecItemNotFound else {
            throw MLSKeychainError.deleteFailed(gpStatus)
        }

        let legacyQuery: [String: Any] = [
            kSecClass as String: kSecClassKey,
            kSecAttrApplicationTag as String: tag,
        ]
        let legacyStatus = performSecItemDelete(legacyQuery)
        guard legacyStatus == errSecSuccess || legacyStatus == errSecItemNotFound else {
            throw MLSKeychainError.deleteFailed(legacyStatus)
        }
    }
}

// MARK: - Testing Support

#if DEBUG
public extension MLSKeychain {
    /// Test Keychain operations (development only)
    public static func runTests() {
        logger.info("Testing MLSKeychain...")
        
        let testIdentity = "did:plc:test123"
        let testKey = Data(repeating: 0xAB, count: 32)
        
        do {
            // Test store
            try storeSignatureKey(testKey, forIdentity: testIdentity)
            logger.info("Store test passed")
            
            // Test retrieve
            let retrieved = try retrieveSignatureKey(forIdentity: testIdentity)
            assert(retrieved == testKey, "Retrieved key doesn't match")
            logger.info("Retrieve test passed")
            
            // Test delete
            try deleteSignatureKey(forIdentity: testIdentity)
            logger.info("Delete test passed")
            
            // Verify deletion
            do {
                _ = try retrieveSignatureKey(forIdentity: testIdentity)
                logger.error("Delete verification failed; key still exists")
            } catch {
                logger.info("Delete verification passed")
            }
            
            // Test Secure Enclave availability
            if isSecureEnclaveAvailable() {
                logger.info("Secure Enclave available")
            } else {
                logger.warning("Secure Enclave not available")
            }
            
            logger.info("All MLSKeychain tests passed")
            
        } catch {
            logger.error("Test failed: \(String(describing: error), privacy: .public)")
        }
    }
}
#endif
