import Foundation
import OSLog
import Petrel

internal actor MLSDeviceRecordService {
  private let logger = Logger(subsystem: "blue.catbird", category: "MLSDeviceRecordService")
  private let atProtoClient: ATProtoClient
  private let mlsClient: MLSClient

  private static let deviceCollection = "blue.catbird.mlsChat.device"
  private static let policyCollection = "blue.catbird.mlsChat.policy"

  private var deviceKeyCache: [String: (keys: Set<String>, fetchedAt: Date)] = [:]
  private let cacheTTL: TimeInterval = 5 * 60

  init(atProtoClient: ATProtoClient, mlsClient: MLSClient) {
    self.atProtoClient = atProtoClient
    self.mlsClient = mlsClient
  }

  // MARK: - Device Record Publishing

  func ensureDeviceRecordPublished(userDid: String) async throws {
    let normalized = userDid.lowercased()

    let sigMaterial = try await resolveDeviceSignatureKey(for: normalized)
    let did = try DID(didString: normalized)
    let existing = try await fetchDeviceRecordsFromPDS(did: did)
    let sigKeyB64 = sigMaterial.publicKey.base64EncodedString()

    let alreadyPublished = existing.contains { record in
      guard let device = decodeDeviceRecord(from: record.value) else { return false }
      return device.mlsSignaturePublicKey.data.base64EncodedString() == sigKeyB64
    }

    if alreadyPublished {
      logger.debug("Device record already published")
    } else {
      try await publishDeviceRecord(
        did: did,
        signaturePublicKey: sigMaterial.publicKey,
        algorithm: sigMaterial.algorithm
      )
    }

    // One-time cleanup of legacy declaration chain records
    let cleanupKey = "mls.device.legacy.cleanup.\(normalized)"
    if !UserDefaults.standard.bool(forKey: cleanupKey) {
      await cleanupLegacyDeclarationRecords(userDid: normalized)
      cleanupLegacyKeychainKeys(userDid: normalized)
      UserDefaults.standard.set(true, forKey: cleanupKey)
    }
  }

  /// Remove a device record by matching its public key.
  func removeDeviceRecord(userDid: String, signaturePublicKey: Data) async throws {
    let normalized = userDid.lowercased()
    let did = try DID(didString: normalized)
    let collection = try NSID(nsidString: Self.deviceCollection)
    let records = try await fetchDeviceRecordsFromPDS(did: did)
    let targetKeyB64 = signaturePublicKey.base64EncodedString()

    for record in records {
      guard let device = decodeDeviceRecord(from: record.value),
            device.mlsSignaturePublicKey.data.base64EncodedString() == targetKeyB64,
            let rkey = record.uri.recordKey
      else { continue }

      let input = ComAtprotoRepoDeleteRecord.Input(
        repo: .did(did),
        collection: collection,
        rkey: try RecordKey(keyString: rkey)
      )
      let (code, _) = try await atProtoClient.com.atproto.repo.deleteRecord(input: input)
      if (200...299).contains(code) {
        logger.info("Deleted device record matching public key")
      }
    }
  }

  /// Remove the current device's record.
  func removeCurrentDeviceRecord(userDid: String) async throws {
    let normalized = userDid.lowercased()
    let sigMaterial = try await resolveDeviceSignatureKey(for: normalized)
    try await removeDeviceRecord(userDid: normalized, signaturePublicKey: sigMaterial.publicKey)
  }

  // MARK: - Key Package Verification

  func verifyKeyPackageAuthorization(
    localAccountDid: String,
    targetDid: String,
    keyPackageData: Data
  ) async throws -> MLSDeviceVerificationDecision {
    let normalizedTarget = targetDid.lowercased()
    let sigMaterial = try await mlsClient.extractKeyPackageSignatureKey(keyPackageData: keyPackageData)
    let sigKeyB64 = sigMaterial.publicKey.base64EncodedString()
    let deviceKeys = try await fetchDeviceKeys(for: normalizedTarget)
    return Self.verifyKeyPackage(signatureKeyB64: sigKeyB64, againstDeviceKeys: deviceKeys)
  }

  nonisolated static func verifyKeyPackage(
    signatureKeyB64: String,
    againstDeviceKeys: Set<String>?
  ) -> MLSDeviceVerificationDecision {
    guard let deviceKeys = againstDeviceKeys else {
      return MLSDeviceVerificationDecision(
        allowed: true,
        warning: "No device records published for target user (TOFU)",
        failureReason: nil
      )
    }

    if deviceKeys.isEmpty {
      return MLSDeviceVerificationDecision(
        allowed: false,
        warning: nil,
        failureReason: "Target user has no authorized devices"
      )
    }

    if deviceKeys.contains(signatureKeyB64) {
      return MLSDeviceVerificationDecision(allowed: true, warning: nil, failureReason: nil)
    }

    return MLSDeviceVerificationDecision(
      allowed: false,
      warning: nil,
      failureReason: "Key package signature key not found in target's published device records"
    )
  }

  // MARK: - Chat Policy

  func publishChatPolicy(
    userDid: String,
    whoCanMessageMe: MLSWhoCanMessageMe?,
    allowFollowersBypass: Bool?,
    allowFollowingBypass: Bool?,
    autoExpireDays: Int?
  ) async throws {
    let normalized = userDid.lowercased()
    let did = try DID(didString: normalized)
    let collection = try NSID(nsidString: Self.policyCollection)
    let rkey = try RecordKey(keyString: "self")

    let record = BlueCatbirdMlsChatPolicy(
      whoCanMessageMe: whoCanMessageMe?.rawValue,
      allowFollowersBypass: allowFollowersBypass,
      allowFollowingBypass: allowFollowingBypass,
      autoExpireDays: autoExpireDays,
      createdAt: ATProtocolDate(date: Date())
    )

    let input = ComAtprotoRepoPutRecord.Input(
      repo: .did(did),
      collection: collection,
      rkey: rkey,
      validate: false,
      record: .knownType(record)
    )
    let (code, _) = try await atProtoClient.com.atproto.repo.putRecord(input: input)
    guard (200...299).contains(code) else {
      throw DeviceRecordError.networkFailure("Failed to publish chat policy (status: \(code))")
    }
  }

  func fetchChatPolicy(for targetDid: String) async throws -> MLSChatPolicy? {
    let normalized = targetDid.lowercased()
    let did = try DID(didString: normalized)
    let collection = try NSID(nsidString: Self.policyCollection)
    let rkey = try RecordKey(keyString: "self")

    let input = ComAtprotoRepoGetRecord.Parameters(
      repo: .did(did),
      collection: collection,
      rkey: rkey,
      cid: nil
    )
    let (code, data) = try await atProtoClient.com.atproto.repo.getRecord(input: input)
    guard (200...299).contains(code), let data else { return nil }
    guard let policy = decodePolicyRecord(from: data.value) else { return nil }

    return MLSChatPolicy(
      allowFollowersBypass: policy.allowFollowersBypass,
      allowFollowingBypass: policy.allowFollowingBypass,
      whoCanMessageMe: policy.whoCanMessageMe.flatMap { MLSWhoCanMessageMe(rawValue: $0) },
      autoExpireDays: policy.autoExpireDays
    )
  }

  // MARK: - Legacy Cleanup

  /// Clean up legacy declaration chain records from the repo.
  /// Called once after migration to device records.
  private func cleanupLegacyDeclarationRecords(userDid: String) async {
    let normalized = userDid.lowercased()
    guard let did = try? DID(didString: normalized) else { return }
    let legacyCollection = "blue.catbird.mlsChat.declaration"
    guard let collection = try? NSID(nsidString: legacyCollection) else { return }

    do {
      let input = ComAtprotoRepoListRecords.Parameters(
        repo: .did(did),
        collection: collection,
        limit: 100,
        cursor: nil,
        reverse: false
      )
      let (code, data) = try await atProtoClient.com.atproto.repo.listRecords(input: input)
      guard (200...299).contains(code), let data else { return }

      for record in data.records {
        guard let rkey = record.uri.recordKey else { continue }
        let deleteInput = ComAtprotoRepoDeleteRecord.Input(
          repo: .did(did),
          collection: collection,
          rkey: try RecordKey(keyString: rkey)
        )
        let (deleteCode, _) = try await atProtoClient.com.atproto.repo.deleteRecord(
          input: deleteInput
        )
        if (200...299).contains(deleteCode) {
          logger.debug("Cleaned up legacy declaration record: \(rkey)")
        }
      }
      logger.info("Legacy declaration record cleanup complete")
    } catch {
      logger.warning("Legacy declaration cleanup failed (non-fatal): \(error.localizedDescription)")
    }
  }

  /// Clean up legacy declaration Keychain keys.
  private nonisolated func cleanupLegacyKeychainKeys(userDid: String) {
    let normalized = userDid.lowercased()
    let legacyKeys: [(key: String, synchronizable: Bool)] = [
      ("mls.declaration.root.online.\(normalized)", true),
      ("mls.declaration.root.recovery.\(normalized)", false),
      ("mls.declaration.device.sigpub.\(normalized)", false),
      ("mls.declaration.device.sigalg.\(normalized)", false),
    ]
    for entry in legacyKeys {
      try? MLSKeychainManager.shared.delete(forKey: entry.key, synchronizable: entry.synchronizable)
    }
  }

  // MARK: - Internal Helpers

  private func resolveDeviceSignatureKey(
    for did: String
  ) async throws -> (publicKey: Data, algorithm: String) {
    let keyPackage = try await mlsClient.createKeyPackage(for: did)
    return try await mlsClient.extractKeyPackageSignatureKey(keyPackageData: keyPackage)
  }

  private func fetchDeviceKeys(for targetDid: String) async throws -> Set<String>? {
    if let cached = deviceKeyCache[targetDid],
       Date().timeIntervalSince(cached.fetchedAt) < cacheTTL {
      return cached.keys
    }

    let did = try DID(didString: targetDid)
    let records = try await fetchDeviceRecordsFromPDS(did: did)

    if records.isEmpty { return nil }

    var keys: Set<String> = []
    for record in records {
      guard let device = decodeDeviceRecord(from: record.value) else { continue }
      keys.insert(device.mlsSignaturePublicKey.data.base64EncodedString())
    }

    deviceKeyCache[targetDid] = (keys: keys, fetchedAt: Date())
    return keys
  }

  private func fetchDeviceRecordsFromPDS(
    did: DID
  ) async throws -> [ComAtprotoRepoListRecords.Record] {
    let collection = try NSID(nsidString: Self.deviceCollection)
    let input = ComAtprotoRepoListRecords.Parameters(
      repo: .did(did),
      collection: collection,
      limit: 50,
      cursor: nil,
      reverse: false
    )
    let (code, data) = try await atProtoClient.com.atproto.repo.listRecords(input: input)
    guard (200...299).contains(code), let data else { return [] }
    return data.records
  }

  private func publishDeviceRecord(
    did: DID,
    signaturePublicKey: Data,
    algorithm: String
  ) async throws {
    let collection = try NSID(nsidString: Self.deviceCollection)

    let record = BlueCatbirdMlsChatDevice(
      deviceId: UUID().uuidString,
      deviceName: nil,
      mlsSignaturePublicKey: Bytes(data: signaturePublicKey),
      algorithm: algorithm,
      platform: "ios",
      createdAt: ATProtocolDate(date: Date())
    )

    let input = ComAtprotoRepoCreateRecord.Input(
      repo: .did(did),
      collection: collection,
      rkey: nil,
      validate: false,
      record: .knownType(record),
      swapCommit: nil
    )

    let (code, _) = try await atProtoClient.com.atproto.repo.createRecord(input: input)
    guard (200...299).contains(code) else {
      throw DeviceRecordError.networkFailure("Failed to publish device record (status: \(code))")
    }
    logger.info("Published device record")
  }

  private func decodeDeviceRecord(from value: ATProtocolValueContainer) -> BlueCatbirdMlsChatDevice? {
    switch value {
    case .knownType(let typed):
      return typed as? BlueCatbirdMlsChatDevice
    case .unknownType(_, let nested):
      return decodeDeviceRecord(from: nested)
    default:
      return nil
    }
  }

  private func decodePolicyRecord(from value: ATProtocolValueContainer) -> BlueCatbirdMlsChatPolicy? {
    switch value {
    case .knownType(let typed):
      return typed as? BlueCatbirdMlsChatPolicy
    case .unknownType(_, let nested):
      return decodePolicyRecord(from: nested)
    default:
      return nil
    }
  }

  private enum DeviceRecordError: LocalizedError {
    case networkFailure(String)
    var errorDescription: String? {
      switch self {
      case .networkFailure(let reason): return reason
      }
    }
  }
}
