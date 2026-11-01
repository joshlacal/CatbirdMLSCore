import CatbirdMLS
import Foundation
import Security

public protocol KeychainBackend: Sendable {
  func add(_ attributes: [String: Any]) -> OSStatus
  func copyMatching(_ query: [String: Any]) -> (OSStatus, AnyObject?)
  func update(_ query: [String: Any], _ attributesToUpdate: [String: Any]) -> OSStatus
  func delete(_ query: [String: Any]) -> OSStatus
}

public final class SystemKeychainBackend: KeychainBackend {
  public init() {}

  public func add(_ attributes: [String: Any]) -> OSStatus {
    SecItemAdd(attributes as CFDictionary, nil)
  }

  public func copyMatching(_ query: [String: Any]) -> (OSStatus, AnyObject?) {
    var result: AnyObject?
    let status = SecItemCopyMatching(query as CFDictionary, &result)
    return (status, result)
  }

  public func update(_ query: [String: Any], _ attributesToUpdate: [String: Any]) -> OSStatus {
    SecItemUpdate(query as CFDictionary, attributesToUpdate as CFDictionary)
  }

  public func delete(_ query: [String: Any]) -> OSStatus {
    SecItemDelete(query as CFDictionary)
  }
}

private struct CachedChatDpopEnvelope: Codable {
  let token: String
  let expiresAtMs: Int64
}

/// Thin Keychain-backed implementation of `PlatformKeyStore` & `PushKeystore` (`Spec §6.3`, `I8`, `A-7`, `A-8`, `I10`).
///
/// Security invariants:
/// - All items are created with `kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly` on initial `SecItemAdd` (`A-8`).
/// - All account keys are keyed strictly by `sha256(normalized_did)` (`I8`, `A-7`), never raw DIDs.
/// - `loadCachedChatDpopToken` returns `nil` when missing or expired, never attempting OAuth refresh (`I10`).
public final class KeychainPlatformKeyStore: PlatformKeyStore, PushKeystore, @unchecked Sendable {
  public let service: String
  public let accessGroup: String?
  private let backend: KeychainBackend
  private let lock = NSLock()
  private var knownSignatureKeyIdsByAccount: [String: Set<String>] = [:]

  private static let resolvedGroupLock = NSLock()
  nonisolated(unsafe) private static var cachedResolvedGroups: [String: String] = [:]

  /// Resolves the `{TeamID}.{suffix}` Keychain access group from entitlements (`macOS`) or Keychain probe (`iOS`).
  public static func resolvedAccessGroup(suffix: String = "blue.catbird.shared") -> String? {
    #if targetEnvironment(simulator)
      return nil
    #else
      resolvedGroupLock.lock()
      if let cached = cachedResolvedGroups[suffix] {
        resolvedGroupLock.unlock()
        return cached
      }
      resolvedGroupLock.unlock()

      let resolved: String?
      #if os(macOS) || targetEnvironment(macCatalyst)
        if let task = SecTaskCreateFromSelf(nil),
           let value = SecTaskCopyValueForEntitlement(task, "keychain-access-groups" as CFString, nil),
           let groups = value as? [String]
        {
          resolved = groups.first(where: { $0.hasSuffix(suffix) })
        } else {
          resolved = nil
        }
      #else
        let probeService = "blue.catbird.mls.accessGroupProbe"
        let probeAccount = UUID().uuidString
        let addQuery: [CFString: Any] = [
          kSecClass: kSecClassGenericPassword,
          kSecAttrService: probeService,
          kSecAttrAccount: probeAccount,
          kSecValueData: Data([0]),
          kSecAttrAccessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
        ]
        _ = SecItemAdd(addQuery as CFDictionary, nil)

        let matchQuery: [CFString: Any] = [
          kSecClass: kSecClassGenericPassword,
          kSecAttrService: probeService,
          kSecAttrAccount: probeAccount,
          kSecReturnAttributes: kCFBooleanTrue as Any,
          kSecMatchLimit: kSecMatchLimitOne,
        ]
        var item: AnyObject?
        let status = SecItemCopyMatching(matchQuery as CFDictionary, &item)
        _ = SecItemDelete([
          kSecClass: kSecClassGenericPassword,
          kSecAttrService: probeService,
          kSecAttrAccount: probeAccount,
        ] as CFDictionary)
        if status == errSecSuccess,
           let attrs = item as? [String: Any],
           let defaultGroup = attrs[kSecAttrAccessGroup as String] as? String,
           let teamPrefix = defaultGroup.split(separator: ".").first
        {
          resolved = "\(teamPrefix).\(suffix)"
        } else {
          resolved = nil
        }
      #endif

      if let resolved {
        resolvedGroupLock.lock()
        cachedResolvedGroups[suffix] = resolved
        resolvedGroupLock.unlock()
      }
      return resolved
    #endif
  }

  public init(
    service: String = "blue.catbird.mls.clean-slate",
    accessGroup: String? = KeychainPlatformKeyStore.resolvedAccessGroup(),
    backend: KeychainBackend = SystemKeychainBackend()
  ) {
    self.service = service
    self.accessGroup = accessGroup
    self.backend = backend
  }

  public func loadOrCreateDbKey(accountDidHash: String) throws -> Data {
    try Self.validateDidHash(accountDidHash)
    let account = Self.dbKeyAccount(for: accountDidHash)
    if let existing = try readData(account: account) {
      guard existing.count == 32 else {
        throw CatbirdMLS.MlsError.StorageError(
          message: "Corrupt DB key length (\(existing.count) bytes) in Keychain"
        )
      }
      return existing
    }

    var generated = Data(count: 32)
    let rngStatus = generated.withUnsafeMutableBytes { rawBuffer -> Int32 in
      guard let base = rawBuffer.baseAddress else { return errSecParam }
      return SecRandomCopyBytes(kSecRandomDefault, 32, base)
    }
    guard rngStatus == errSecSuccess else {
      throw CatbirdMLS.MlsError.StorageError(
        message: "SecRandomCopyBytes failed with status \(rngStatus)"
      )
    }

    var addAttrs = baseQuery(account: account)
    addAttrs[kSecValueData as String] = generated
    addAttrs[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    addAttrs[kSecAttrSynchronizable as String] = kCFBooleanFalse

    let status = backend.add(addAttrs)
    if status == errSecDuplicateItem {
      if let raced = try readData(account: account), raced.count == 32 {
        return raced
      }
    }
    guard status == errSecSuccess else {
      throw CatbirdMLS.MlsError.StorageError(
        message: "Keychain SecItemAdd(dbkey) failed with OSStatus \(status)"
      )
    }
    return generated
  }

  public func loadDbKey(accountDidHash: String) throws -> Data {
    try Self.validateDidHash(accountDidHash)
    let account = Self.dbKeyAccount(for: accountDidHash)
    guard let existing = try readData(account: account) else {
      throw CatbirdMLS.MlsError.StorageError(
        message: "Missing SQLCipher DB key for account hash \(accountDidHash.prefix(12))"
      )
    }
    guard existing.count == 32 else {
      throw CatbirdMLS.MlsError.StorageError(
        message: "Invalid SQLCipher DB key length (\(existing.count))"
      )
    }
    return existing
  }

  public func loadSignatureKeypair(keyId: String) throws -> Data? {
    guard !keyId.isEmpty else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Empty signature keyId")
    }
    return try readData(account: Self.sigKeyAccount(for: keyId))
  }

  public func storeSignatureKeypair(keyId: String, keypairBytes: Data) throws {
    guard !keyId.isEmpty, !keypairBytes.isEmpty else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Invalid signature keypair input")
    }
    let account = Self.sigKeyAccount(for: keyId)
    try upsertData(account: account, data: keypairBytes)
    for segment in keyId.split(separator: ":") {
      let candidate = String(segment).lowercased()
      if candidate.count == 64 && candidate.allSatisfy(\.isHexDigit) {
        lock.lock()
        knownSignatureKeyIdsByAccount[candidate, default: []].insert(keyId)
        lock.unlock()
      }
    }
  }

  public func deleteAccountKeys(accountDidHash: String) throws {
    try Self.validateDidHash(accountDidHash)
    let normalized = accountDidHash.lowercased()
    deleteAccountItem(account: Self.dbKeyAccount(for: normalized))
    deleteAccountItem(account: Self.chatDpopAccount(for: normalized))
    // Deterministic cleanup across cold app restarts where in-memory tracking is empty (`I8`).
    deleteAccountItem(account: Self.sigKeyAccount(for: "mls-signer:\(normalized)"))
    deleteAccountItem(account: Self.sigKeyAccount(for: "mls-push-gw-signer:\(normalized)"))
    deleteAccountItem(account: Self.sigKeyAccount(for: "\(normalized):sig"))

    lock.lock()
    let trackedSigKeys = knownSignatureKeyIdsByAccount.removeValue(forKey: normalized) ?? []
    lock.unlock()
    for sigKeyId in trackedSigKeys {
      deleteAccountItem(account: Self.sigKeyAccount(for: sigKeyId))
    }
  }

  public func loadCachedChatDpopToken(accountDidHash: String) throws -> String? {
    try Self.validateDidHash(accountDidHash)
    let account = Self.chatDpopAccount(for: accountDidHash)
    guard let raw = try readData(account: account),
          let decoded = try? JSONDecoder().decode(CachedChatDpopEnvelope.self, from: raw)
    else {
      return nil
    }
    let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
    guard decoded.expiresAtMs > nowMs, !decoded.token.isEmpty else {
      return nil
    }
    return decoded.token
  }

  public func storeCachedChatDpopToken(
    accountDidHash: String,
    token: String,
    expiresAtMs: Int64
  ) throws {
    try Self.validateDidHash(accountDidHash)
    let encoded = try JSONEncoder().encode(
      CachedChatDpopEnvelope(token: token, expiresAtMs: expiresAtMs)
    )
    try upsertData(account: Self.chatDpopAccount(for: accountDidHash), data: encoded)
  }

  // MARK: - Private Helpers

  private static func validateDidHash(_ hash: String) throws {
    guard hash.count == 64,
          hash.utf8.allSatisfy({
            ($0 >= 0x30 && $0 <= 0x39) || ($0 >= 0x61 && $0 <= 0x66)
          })
    else {
      throw CatbirdMLS.MlsError.InvalidInput(
        message: "Account key must be 64-char lowercase hex sha256(normalized_did)"
      )
    }
  }

  private static func dbKeyAccount(for didHash: String) -> String {
    "dbkey:\(didHash)"
  }

  private static func sigKeyAccount(for keyId: String) -> String {
    "sigkey:\(keyId)"
  }

  private static func chatDpopAccount(for didHash: String) -> String {
    "chatdpop:\(didHash)"
  }

  private func baseQuery(account: String) -> [String: Any] {
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: service,
      kSecAttrAccount as String: account,
    ]
    if let accessGroup, !accessGroup.isEmpty {
      query[kSecAttrAccessGroup as String] = accessGroup
    }
    return query
  }

  private func readData(account: String) throws -> Data? {
    var query = baseQuery(account: account)
    query[kSecReturnData as String] = kCFBooleanTrue
    query[kSecMatchLimit as String] = kSecMatchLimitOne
    let (status, result) = backend.copyMatching(query)
    if status == errSecItemNotFound {
      return nil
    }
    guard status == errSecSuccess else {
      throw CatbirdMLS.MlsError.StorageError(
        message: "Keychain SecItemCopyMatching(\(account.prefix(16))) failed: \(status)"
      )
    }
    return result as? Data
  }

  private func upsertData(account: String, data: Data) throws {
    let query = baseQuery(account: account)
    let updateStatus = backend.update(query, [kSecValueData as String: data])
    if updateStatus == errSecSuccess {
      return
    }
    if updateStatus != errSecItemNotFound {
      _ = backend.delete(query)
    }
    var addAttrs = query
    addAttrs[kSecValueData as String] = data
    addAttrs[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    addAttrs[kSecAttrSynchronizable as String] = kCFBooleanFalse
    let addStatus = backend.add(addAttrs)
    guard addStatus == errSecSuccess else {
      throw CatbirdMLS.MlsError.StorageError(
        message: "Keychain SecItemAdd(\(account.prefix(16))) failed: \(addStatus)"
      )
    }
  }

  private func deleteAccountItem(account: String) {
    let status = backend.delete(baseQuery(account: account))
    _ = status
  }
}
