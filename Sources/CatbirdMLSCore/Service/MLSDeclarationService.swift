import CryptoKit
import Foundation
import GRDB
import OSLog
import Petrel

public enum MLSDeclarationRolloutMode: String, Codable, Sendable {
  case shadow
  case soft
  case full
}

public enum MLSWhoCanMessageMe: String, Codable, Sendable {
  case everyone
  case mutuals
  case following
  case nobody
}

public struct MLSChatPolicy: Sendable, Codable, Equatable {
  public var allowFollowersBypass: Bool?
  public var allowFollowingBypass: Bool?
  public var whoCanMessageMe: MLSWhoCanMessageMe?
  public var autoExpireDays: Int?

  public init(
    allowFollowersBypass: Bool? = nil,
    allowFollowingBypass: Bool? = nil,
    whoCanMessageMe: MLSWhoCanMessageMe? = nil,
    autoExpireDays: Int? = nil
  ) {
    self.allowFollowersBypass = allowFollowersBypass
    self.allowFollowingBypass = allowFollowingBypass
    self.whoCanMessageMe = whoCanMessageMe
    self.autoExpireDays = autoExpireDays
  }
}

public struct MLSDeclarationAuthorizationDecision: Sendable {
  public let allowed: Bool
  public let requiresUserFriction: Bool
  public let warning: String?
  public let failureReason: String?

  public init(
    allowed: Bool,
    requiresUserFriction: Bool = false,
    warning: String? = nil,
    failureReason: String? = nil
  ) {
    self.allowed = allowed
    self.requiresUserFriction = requiresUserFriction
    self.warning = warning
    self.failureReason = failureReason
  }
}

public struct MLSDeclarationSelfCheckResult: Sendable {
  public let securityFrozen: Bool
  public let reason: String?

  public init(securityFrozen: Bool, reason: String?) {
    self.securityFrozen = securityFrozen
    self.reason = reason
  }
}

public enum MLSDeclarationChainState: String, Codable, Sendable {
  case verified
  case missingChain
  case invalid
  case rollback
  case unavailable
}

internal actor MLSDeclarationService {
  private typealias EventUnion = BlueCatbirdMlsChatDeclaration.BlueCatbirdMlsChatDeclarationEventUnion
  private typealias ChainState = MLSDeclarationChainState

  struct ChainHead: Sendable, Codable, Equatable {
    let epoch: Int
    let seq: Int
    let cid: String
    let uri: String

    func isOlder(than other: ChainHead) -> Bool {
      if epoch != other.epoch {
        return epoch < other.epoch
      }
      if seq != other.seq {
        return seq < other.seq
      }
      // Same logical position but different CID is suspicious / rollback-ish.
      return cid != other.cid
    }
  }

  private struct VerificationSnapshot: Sendable {
    let state: ChainState
    let head: ChainHead?
    let verifiedAt: Date
    let authorizedDeviceKeys: Set<String>
    let onlineRootPublicKey: Data?
    let onlineRootAlg: String?
    let recoveryRootPublicKey: Data?
    let recoveryRootAlg: String?
    let hadRecovery: Bool
    let errorMessage: String?
    let chatPolicy: MLSChatPolicy?
  }

  private struct CachedSnapshot: Sendable {
    let localAccountDID: String
    let targetDID: String
    let state: ChainState
    let verifiedAt: Date
    let head: ChainHead?
    let authorizedDeviceKeys: Set<String>
    let onlineRootPublicKey: Data?
    let onlineRootAlg: String?
    let recoveryRootPublicKey: Data?
    let recoveryRootAlg: String?
    let hadRecovery: Bool
    let lastError: String?
    let chatPolicy: MLSChatPolicy?
  }

  private struct CanonicalSlot: Sendable {
    let epoch: Int
    let seq: Int
    let uri: ATProtocolURI
    let cid: CID
    let declaration: BlueCatbirdMlsChatDeclaration?
    let mismatchReason: String?
  }

  private struct ResolvedHeadContext: Sendable {
    let head: ChainHead?
    let repoCommitCID: CID
  }

  private enum DeclarationError: LocalizedError {
    case networkFailure(String)
    case invalidChain(String)
    case invalidRecord(String)
    case unsupportedSignatureAlgorithm(String)
    case missingCurrentDeviceKey

    var errorDescription: String? {
      switch self {
      case .networkFailure(let reason):
        return reason
      case .invalidChain(let reason):
        return reason
      case .invalidRecord(let reason):
        return reason
      case .unsupportedSignatureAlgorithm(let alg):
        return "Unsupported declaration signature algorithm: \(alg)"
      case .missingCurrentDeviceKey:
        return "Missing current device key material"
      }
    }
  }

  private struct CurrentDeviceSignatureMaterial: Sendable {
    let publicKey: Data
    let algorithm: String
  }

  private let logger = Logger(subsystem: "blue.catbird", category: "MLSDeclarationService")
  private let atProtoClient: ATProtoClient
  private let mlsClient: MLSClient
  private let database: MLSDatabase

  private var rolloutMode: MLSDeclarationRolloutMode

  private let securitySensitiveTTL: TimeInterval = 10 * 60
  private let passiveDecryptTTL: TimeInterval = 24 * 60 * 60
  private let hardTTL: TimeInterval = 7 * 24 * 60 * 60
  private let proactiveRootRotationInterval: TimeInterval = 90 * 24 * 60 * 60

  private static let declarationCollection = "blue.catbird.mlsChat.declaration"

  init(
    atProtoClient: ATProtoClient,
    mlsClient: MLSClient,
    database: MLSDatabase,
    rolloutMode: MLSDeclarationRolloutMode
  ) {
    self.atProtoClient = atProtoClient
    self.mlsClient = mlsClient
    self.database = database
    self.rolloutMode = rolloutMode
  }

  func setRolloutMode(_ mode: MLSDeclarationRolloutMode) {
    rolloutMode = mode
  }

  func purgeCache(localAccountDid: String) async {
    let normalizedLocal = localAccountDid.lowercased()
    do {
      try await database.write { db in
        try db.execute(
          sql: "DELETE FROM MLSDeclarationCache WHERE localAccountDID = ?",
          arguments: [normalizedLocal]
        )
      }
    } catch {
      logger.error("Failed to purge declaration cache for \(normalizedLocal, privacy: .private): \(error.localizedDescription)")
    }
  }

  func getSelfDeclarationPolicy(localAccountDID: String) async -> MLSChatPolicy? {
    let normalizedLocal = localAccountDID.lowercased()
    let cached = await readCache(localAccountDID: normalizedLocal, targetDID: normalizedLocal)
    return cached?.chatPolicy
  }

  func ensureSelfChainInitialized(localAccountDid: String) async throws {
    let normalizedLocal = localAccountDid.lowercased()

    var snapshot = try await verifyChain(localAccountDID: normalizedLocal, targetDID: normalizedLocal)
    if snapshot.state == .missingChain {
      logger.info("Declaration chain missing for self DID, publishing rootInit")
      _ = try await publishRootInit(localAccountDid: normalizedLocal, note: "catbird-initialize")
      snapshot = try await verifyChain(localAccountDID: normalizedLocal, targetDID: normalizedLocal)

      // Avoid a read-after-write race where the newly written seq=0 record has not
      // propagated to listRecords yet. If we proceed while still "missingChain",
      // deviceAdd can fail because publishing logic still sees seq=0.
      if snapshot.state == .missingChain {
        for attempt in 1...5 {
          try await Task.sleep(nanoseconds: UInt64(attempt) * 120_000_000)
          snapshot = try await verifyChain(localAccountDID: normalizedLocal, targetDID: normalizedLocal)
          if snapshot.state == .verified {
            break
          }
        }
      }

      if snapshot.state == .missingChain {
        let message = "Published rootInit but declaration head is not yet visible"
        if rolloutMode == .full {
          throw DeclarationError.invalidChain("\(message) (enforcement mode)")
        }
        logger.warning("\(message); deferring deviceAdd until next chain refresh")
        return
      }
    }

    // Self chain is invalid — epoch bump to start fresh
    if snapshot.state == .invalid {
      logger.warning(
        "⚠️ [Declaration] Self chain is invalid (\(snapshot.errorMessage ?? "unknown reason")), performing epoch bump"
      )
      _ = try await republishFreshChain(
        localAccountDid: normalizedLocal,
        reason: snapshot.errorMessage ?? "self-chain-invalid"
      )
      snapshot = try await verifyChain(localAccountDID: normalizedLocal, targetDID: normalizedLocal)

      if snapshot.state != .verified {
        logger.error("❌ [Declaration] Epoch bump did not produce a verified chain (state: \(snapshot.state.rawValue))")
        if rolloutMode == .full {
          throw DeclarationError.invalidChain("Epoch bump failed to produce healthy chain")
        }
        return
      }
    }

    guard snapshot.state == .verified || snapshot.state == .missingChain else {
      throw DeclarationError.invalidChain("Self declaration chain is not healthy: \(snapshot.state.rawValue)")
    }

    var canPublishLocalUpdates = true
    if snapshot.state == .verified {
      do {
        snapshot = try await reconcileSelfRootAuthority(localAccountDid: normalizedLocal, snapshot: snapshot)
      } catch {
        canPublishLocalUpdates = false
        logger.error("Failed to reconcile declaration root authority: \(error.localizedDescription)")
        if rolloutMode == .full {
          throw error
        }
      }
    }

    guard canPublishLocalUpdates else {
      return
    }

    let deviceMaterial: CurrentDeviceSignatureMaterial
    do {
      deviceMaterial = try await currentDeviceSignatureMaterial(for: normalizedLocal)
    } catch {
      logger.warning(
        "Failed to resolve declaration device signature material; attempting device registration retry: \(error.localizedDescription)"
      )
      _ = try await mlsClient.ensureDeviceRegistered(userDid: normalizedLocal)
      deviceMaterial = try await currentDeviceSignatureMaterial(for: normalizedLocal)
    }
    let deviceKeyB64 = deviceMaterial.publicKey.base64EncodedString()

    if !snapshot.authorizedDeviceKeys.contains(deviceKeyB64) {
      let deviceInfo = await mlsClient.getDeviceInfo(for: normalizedLocal)
      let deviceId = deviceInfo?.deviceId ?? UUID().uuidString.lowercased()
      _ = try await publishDeviceAdd(
        localAccountDid: normalizedLocal,
        deviceId: deviceId,
        deviceName: nil,
        deviceMlsSignaturePublicKey: deviceMaterial.publicKey,
        deviceAlgorithm: deviceMaterial.algorithm
      )
      _ = try await verifyChain(localAccountDID: normalizedLocal, targetDID: normalizedLocal)
    }
  }

  func verifySelfChainOnLaunch(localAccountDid: String) async -> MLSDeclarationSelfCheckResult {
    let normalizedLocal = localAccountDid.lowercased()

    do {
      let snapshot = try await verifyChain(localAccountDID: normalizedLocal, targetDID: normalizedLocal)

      if snapshot.state == .rollback {
        return MLSDeclarationSelfCheckResult(
          securityFrozen: true,
          reason: "Declaration rollback detected for current account"
        )
      }

      if snapshot.state == .verified {
        let deviceMaterial = try await currentDeviceSignatureMaterial(for: normalizedLocal)
        let deviceKeyB64 = deviceMaterial.publicKey.base64EncodedString()
        if !snapshot.authorizedDeviceKeys.contains(deviceKeyB64) {
          return MLSDeclarationSelfCheckResult(
            securityFrozen: true,
            reason: "Current device key is no longer authorized by declaration chain"
          )
        }
      }

      return MLSDeclarationSelfCheckResult(securityFrozen: false, reason: nil)
    } catch {
      logger.error("Self-chain verification failed: \(error.localizedDescription)")
      // In shadow/soft, don't freeze on verifier failures.
      if rolloutMode == .full {
        return MLSDeclarationSelfCheckResult(
          securityFrozen: true,
          reason: "Unable to verify self declaration chain in enforcement mode"
        )
      }
      return MLSDeclarationSelfCheckResult(securityFrozen: false, reason: nil)
    }
  }

  private func reconcileSelfRootAuthority(
    localAccountDid: String,
    snapshot: VerificationSnapshot
  ) async throws -> VerificationSnapshot {
    guard snapshot.state == .verified else {
      return snapshot
    }
    guard let chainOnlineRoot = snapshot.onlineRootPublicKey,
      let chainRecoveryRoot = snapshot.recoveryRootPublicKey
    else {
      return snapshot
    }

    let onlineRoot = try loadRootPrivateKey(for: localAccountDid, kind: .online)
    let recoveryRoot = try loadRootPrivateKey(for: localAccountDid, kind: .recovery)
    let onlineMatches = onlineRoot?.publicKey.rawRepresentation == chainOnlineRoot
    let recoveryMatches = recoveryRoot?.publicKey.rawRepresentation == chainRecoveryRoot

    if onlineMatches {
      if shouldPerformProactiveRootRotation(for: localAccountDid) {
        logger.info("Proactively rotating declaration online root")
        _ = try await publishRootRotate(localAccountDid: localAccountDid, rotationMode: "normal")
        return try await verifyChain(localAccountDID: localAccountDid, targetDID: localAccountDid)
      }
      return snapshot
    }

    if recoveryMatches {
      logger.warning("Recovering declaration online root due to local key mismatch/missing key")
      _ = try await publishRecoveryRotateOnlineRoot(
        localAccountDid: localAccountDid,
        recoveryReason: "online-root-mismatch-or-missing"
      )
      return try await verifyChain(localAccountDID: localAccountDid, targetDID: localAccountDid)
    }

    // Neither root key matches — this device has never seen the chain's keys.
    // This happens when a second device starts before iCloud Keychain has synced
    // the root keys from the first device. Epoch-bump to establish fresh authority
    // that this device controls, then re-add itself.
    logger.warning(
      "⚠️ [Declaration] No matching root keys for chain — new device or iCloud sync pending. Performing epoch bump."
    )
    _ = try await republishFreshChain(
      localAccountDid: localAccountDid,
      reason: "new-device-no-matching-root-keys"
    )
    return try await verifyChain(localAccountDID: localAccountDid, targetDID: localAccountDid)
  }

  func authorizeKeyPackage(
    localAccountDid: String,
    targetDid: String,
    keyPackageData: Data,
    securitySensitive: Bool
  ) async throws -> MLSDeclarationAuthorizationDecision {
    let normalizedLocal = localAccountDid.lowercased()
    let normalizedTarget = targetDid.lowercased()

    let signatureMaterial = try await mlsClient.extractKeyPackageSignatureKey(
      keyPackageData: keyPackageData
    )
    let signatureKeyB64 = signatureMaterial.publicKey.base64EncodedString()

    let cached = await readCache(localAccountDID: normalizedLocal, targetDID: normalizedTarget)
    let now = Date()

    let refreshTTL = securitySensitive ? securitySensitiveTTL : passiveDecryptTTL
    let cacheAge = cached.map { now.timeIntervalSince($0.verifiedAt) }
    let shouldRefresh: Bool = {
      guard let cached else { return true }
      return now.timeIntervalSince(cached.verifiedAt) > refreshTTL
    }()

    var snapshot: VerificationSnapshot?
    if shouldRefresh {
      do {
        snapshot = try await verifyChain(localAccountDID: normalizedLocal, targetDID: normalizedTarget)
      } catch {
        logger.error("Declaration refresh failed for \(normalizedTarget, privacy: .private): \(error.localizedDescription)")
      }
    }

    if snapshot == nil, let cached {
      let age = cacheAge ?? .infinity
      if age > hardTTL {
        return MLSDeclarationAuthorizationDecision(
          allowed: rolloutMode == .shadow,
          warning: rolloutMode == .shadow ? "Declaration cache expired (hard TTL) - shadow allow" : nil,
          failureReason: rolloutMode == .shadow ? nil : "Declaration cache expired and refresh unavailable"
        )
      }

      if securitySensitive && age > securitySensitiveTTL {
        return MLSDeclarationAuthorizationDecision(
          allowed: rolloutMode == .shadow,
          warning: rolloutMode == .shadow ? "Declaration stale beyond security TTL - shadow allow" : nil,
          failureReason: rolloutMode == .shadow ? nil : "Declaration stale beyond security-sensitive TTL"
        )
      }

      if cached.state == .verified {
        if cached.authorizedDeviceKeys.contains(signatureKeyB64) {
          return MLSDeclarationAuthorizationDecision(allowed: true)
        }
        return MLSDeclarationAuthorizationDecision(
          allowed: false,
          failureReason: "Key package signature key is not authorized by declaration chain"
        )
      }

      return decisionForNonVerifiedState(cached.state, securitySensitive: securitySensitive)
    }

    guard let snapshot else {
      // No cache, no fresh chain.
      return decisionForUnavailableWithoutCache(securitySensitive: securitySensitive)
    }

    switch snapshot.state {
    case .verified:
      if snapshot.authorizedDeviceKeys.contains(signatureKeyB64) {
        return MLSDeclarationAuthorizationDecision(allowed: true)
      }
      return MLSDeclarationAuthorizationDecision(
        allowed: false,
        failureReason: "Key package signature key is not authorized by declaration chain"
      )
    case .missingChain, .invalid, .rollback, .unavailable:
      let decision = decisionForNonVerifiedState(snapshot.state, securitySensitive: securitySensitive)
      logger.info(
        "🔐 [Declaration] authorizeKeyPackage for \(normalizedTarget.prefix(30), privacy: .private)...: state=\(snapshot.state.rawValue), allowed=\(decision.allowed), error=\(snapshot.errorMessage ?? "none")"
      )
      return decision
    }
  }

  // MARK: - Publishing API

  @discardableResult
  func publishRootInit(localAccountDid: String, note: String?) async throws -> ChainHead {
    let normalizedLocal = localAccountDid.lowercased()
    let onlineRoot = try loadOrCreateRootPrivateKey(for: normalizedLocal, kind: .online)
    let recoveryRoot = try loadOrCreateRootPrivateKey(for: normalizedLocal, kind: .recovery)

    return try await publishRecordWithRetry(localAccountDid: normalizedLocal) {
      epoch,
      seq,
      prev,
      createdAt
      in
      guard seq == 0 else {
        throw DeclarationError.invalidRecord("rootInit must be seq=0")
      }
      let event = EventUnion(
        BlueCatbirdMlsChatDeclaration.RootInit(
          onlineRootPublicKey: Bytes(data: onlineRoot.publicKey.rawRepresentation),
          onlineRootAlg: "p256",
          recoveryRootPublicKey: Bytes(data: recoveryRoot.publicKey.rawRepresentation),
          recoveryRootAlg: "p256",
          note: note
        ))

      var record = BlueCatbirdMlsChatDeclaration(
        did: try DID(didString: normalizedLocal),
        epoch: epoch,
        seq: seq,
        prev: prev,
        event: event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: Data()),
          sigAlg: "p256",
          coSig: nil,
          coSigAlg: nil,
          deviceProof: nil
        ),
        createdAt: createdAt
      )

      let payload = try self.canonicalSigningPayload(for: record)
      let signature = try self.signPayload(payload, with: onlineRoot)
      record = BlueCatbirdMlsChatDeclaration(
        did: record.did,
        epoch: record.epoch,
        seq: record.seq,
        prev: record.prev,
        event: record.event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: signature),
          sigAlg: "p256",
          coSig: nil,
          coSigAlg: nil,
          deviceProof: nil
        ),
        createdAt: record.createdAt
      )
      return record
    }
  }

  // MARK: - Epoch Bump (Start Fresh)

  /// Delete all declaration records for the current epoch and publish a fresh rootInit
  /// at epoch+1. Use when the self declaration chain is in an irrecoverably invalid state
  /// (e.g., corrupted records, key mismatch without recovery, leftover test data).
  @discardableResult
  func republishFreshChain(localAccountDid: String, reason: String) async throws -> ChainHead {
    let normalizedLocal = localAccountDid.lowercased()
    let did = try DID(didString: normalizedLocal)
    let collection = try NSID(nsidString: Self.declarationCollection)

    // Fetch all existing records to determine current epoch and delete them
    let rawRecords = try await fetchDeclarationRecords(for: did)
    let slots = extractCanonicalSlots(rawRecords, targetDID: normalizedLocal)
    let currentMaxEpoch = slots.map(\.epoch).max() ?? 0
    let newEpoch = currentMaxEpoch + 1

    logger.warning(
      "🔄 [Declaration] Epoch bump: deleting \(rawRecords.count) records, current epoch \(currentMaxEpoch) → new epoch \(newEpoch). Reason: \(reason)"
    )

    // Delete all existing declaration records
    for record in rawRecords {
      guard let rkey = record.uri.recordKey else { continue }
      let deleteInput = ComAtprotoRepoDeleteRecord.Input(
        repo: .did(did),
        collection: collection,
        rkey: try RecordKey(keyString: rkey)
      )
      let (code, _) = try await atProtoClient.com.atproto.repo.deleteRecord(input: deleteInput)
      if !(200...299).contains(code) {
        logger.warning("Failed to delete declaration record \(rkey) (status: \(code))")
      }
    }

    // Purge local cache
    await purgeCache(localAccountDid: normalizedLocal)

    // Generate fresh root keys
    let onlineRoot = try createFreshRootPrivateKey(for: normalizedLocal, kind: .online)
    let recoveryRoot = try createFreshRootPrivateKey(for: normalizedLocal, kind: .recovery)

    // Publish new rootInit at epoch+1, seq=0
    let createdAt = ATProtocolDate(date: Date())
    let event = EventUnion(
      BlueCatbirdMlsChatDeclaration.RootInit(
        onlineRootPublicKey: Bytes(data: onlineRoot.publicKey.rawRepresentation),
        onlineRootAlg: "p256",
        recoveryRootPublicKey: Bytes(data: recoveryRoot.publicKey.rawRepresentation),
        recoveryRootAlg: "p256",
        note: "epoch-bump: \(reason)"
      ))

    var record = BlueCatbirdMlsChatDeclaration(
      did: did,
      epoch: newEpoch,
      seq: 0,
      prev: nil,
      event: event,
      proofs: BlueCatbirdMlsChatDeclaration.Proofs(
        sig: Bytes(data: Data()),
        sigAlg: "p256",
        coSig: nil,
        coSigAlg: nil,
        deviceProof: nil
      ),
      createdAt: createdAt
    )

    let payload = try canonicalSigningPayload(for: record)
    let signature = try signPayload(payload, with: onlineRoot)
    record = BlueCatbirdMlsChatDeclaration(
      did: record.did,
      epoch: record.epoch,
      seq: record.seq,
      prev: record.prev,
      event: record.event,
      proofs: BlueCatbirdMlsChatDeclaration.Proofs(
        sig: Bytes(data: signature),
        sigAlg: "p256",
        coSig: nil,
        coSigAlg: nil,
        deviceProof: nil
      ),
      createdAt: record.createdAt
    )

    let rkey = try RecordKey(keyString: deterministicRkey(epoch: newEpoch, seq: 0))

    let latest = try await atProtoClient.com.atproto.sync.getLatestCommit(
      input: ComAtprotoSyncGetLatestCommit.Parameters(did: did)
    )
    guard (200...299).contains(latest.responseCode), let commit = latest.data else {
      throw DeclarationError.networkFailure("Failed to fetch latest repo commit for epoch bump")
    }

    let createInput = ComAtprotoRepoCreateRecord.Input(
      repo: .did(did),
      collection: collection,
      rkey: rkey,
      validate: false,
      record: .knownType(record),
      swapCommit: commit.cid
    )

    let (responseCode, responseData) = try await atProtoClient.com.atproto.repo.createRecord(input: createInput)
    guard (200...299).contains(responseCode), let responseData else {
      throw DeclarationError.networkFailure(
        "Failed to publish epoch-bump rootInit (status: \(responseCode))"
      )
    }

    logger.info("✅ [Declaration] Epoch bump complete: epoch \(newEpoch), seq 0")

    return ChainHead(
      epoch: newEpoch,
      seq: 0,
      cid: responseData.cid.string,
      uri: responseData.uri.uriString()
    )
  }

  @discardableResult
  func publishDeviceAdd(
    localAccountDid: String,
    deviceId: String,
    deviceName: String?,
    deviceMlsSignaturePublicKey: Data,
    deviceAlgorithm: String
  ) async throws -> ChainHead {
    let normalizedLocal = localAccountDid.lowercased()
    let onlineRoot = try loadOrCreateRootPrivateKey(for: normalizedLocal, kind: .online)

    return try await publishRecordWithRetry(localAccountDid: normalizedLocal) {
      epoch,
      seq,
      prev,
      createdAt
      in
      if seq == 0 {
        throw DeclarationError.invalidRecord("deviceAdd requires existing chain head")
      }
      let event = EventUnion(
        BlueCatbirdMlsChatDeclaration.DeviceAdd(
          deviceId: deviceId,
          deviceName: deviceName,
          deviceMlsSignaturePublicKey: Bytes(data: deviceMlsSignaturePublicKey)
        ))

      var record = BlueCatbirdMlsChatDeclaration(
        did: try DID(didString: normalizedLocal),
        epoch: epoch,
        seq: seq,
        prev: prev,
        event: event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: Data()),
          sigAlg: "p256",
          coSig: nil,
          coSigAlg: nil,
          deviceProof: nil
        ),
        createdAt: createdAt
      )

      let deviceProofPayload = try self.canonicalDeviceProofPayload(for: record)
      let deviceProofSignature = try await self.mlsClient.signDeclarationProof(
        for: normalizedLocal,
        payload: deviceProofPayload
      )

      let rootPayload = try self.canonicalSigningPayload(for: record)
      let rootSignature = try self.signPayload(rootPayload, with: onlineRoot)

      record = BlueCatbirdMlsChatDeclaration(
        did: record.did,
        epoch: record.epoch,
        seq: record.seq,
        prev: record.prev,
        event: record.event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: rootSignature),
          sigAlg: "p256",
          coSig: nil,
          coSigAlg: nil,
          deviceProof: BlueCatbirdMlsChatDeclaration.DeviceProof(
            proofSig: Bytes(data: deviceProofSignature),
            proofAlg: self.normalizeSignatureAlgorithm(deviceAlgorithm)
          )
        ),
        createdAt: record.createdAt
      )
      return record
    }
  }

  @discardableResult
  func publishDeviceRevoke(
    localAccountDid: String,
    deviceMlsSignaturePublicKey: Data?,
    deviceId: String?,
    reason: String?
  ) async throws -> ChainHead {
    let normalizedLocal = localAccountDid.lowercased()
    guard deviceMlsSignaturePublicKey != nil || deviceId != nil else {
      throw DeclarationError.invalidRecord("deviceRevoke requires key or deviceId")
    }
    let onlineRoot = try loadOrCreateRootPrivateKey(for: normalizedLocal, kind: .online)

    return try await publishRecordWithRetry(localAccountDid: normalizedLocal) {
      epoch,
      seq,
      prev,
      createdAt
      in
      let event = EventUnion(
        BlueCatbirdMlsChatDeclaration.DeviceRevoke(
          deviceMlsSignaturePublicKey: deviceMlsSignaturePublicKey.map { Bytes(data: $0) },
          deviceId: deviceId,
          reason: reason
        ))

      var record = BlueCatbirdMlsChatDeclaration(
        did: try DID(didString: normalizedLocal),
        epoch: epoch,
        seq: seq,
        prev: prev,
        event: event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: Data()),
          sigAlg: "p256",
          coSig: nil,
          coSigAlg: nil,
          deviceProof: nil
        ),
        createdAt: createdAt
      )

      let payload = try self.canonicalSigningPayload(for: record)
      let signature = try self.signPayload(payload, with: onlineRoot)
      record = BlueCatbirdMlsChatDeclaration(
        did: record.did,
        epoch: record.epoch,
        seq: record.seq,
        prev: record.prev,
        event: record.event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: signature),
          sigAlg: "p256",
          coSig: nil,
          coSigAlg: nil,
          deviceProof: nil
        ),
        createdAt: record.createdAt
      )

      return record
    }
  }

  @discardableResult
  func publishRootRotate(
    localAccountDid: String,
    rotationMode: String
  ) async throws -> ChainHead {
    let normalizedLocal = localAccountDid.lowercased()
    guard let oldOnlineRoot = try loadRootPrivateKey(for: normalizedLocal, kind: .online) else {
      throw DeclarationError.invalidChain("Cannot rotate online root without existing local online root key")
    }
    let newOnlineRoot = P256.Signing.PrivateKey()

    let head = try await publishRecordWithRetry(localAccountDid: normalizedLocal) {
      epoch,
      seq,
      prev,
      createdAt
      in
      let event = EventUnion(
        BlueCatbirdMlsChatDeclaration.RootRotate(
          newOnlineRootPublicKey: Bytes(data: newOnlineRoot.publicKey.rawRepresentation),
          newOnlineRootAlg: "p256",
          rotationMode: rotationMode
        ))

      var record = BlueCatbirdMlsChatDeclaration(
        did: try DID(didString: normalizedLocal),
        epoch: epoch,
        seq: seq,
        prev: prev,
        event: event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: Data()),
          sigAlg: "p256",
          coSig: Bytes(data: Data()),
          coSigAlg: "p256",
          deviceProof: nil
        ),
        createdAt: createdAt
      )

      let payload = try self.canonicalSigningPayload(for: record)
      let oldSignature = try self.signPayload(payload, with: oldOnlineRoot)
      let newSignature = try self.signPayload(payload, with: newOnlineRoot)

      record = BlueCatbirdMlsChatDeclaration(
        did: record.did,
        epoch: record.epoch,
        seq: record.seq,
        prev: record.prev,
        event: record.event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: oldSignature),
          sigAlg: "p256",
          coSig: Bytes(data: newSignature),
          coSigAlg: "p256",
          deviceProof: nil
        ),
        createdAt: record.createdAt
      )
      return record
    }

    try storeRootPrivateKey(newOnlineRoot, for: normalizedLocal, kind: .online)
    return head
  }

  @discardableResult
  func publishRecoveryRotateOnlineRoot(
    localAccountDid: String,
    recoveryReason: String?
  ) async throws -> ChainHead {
    let normalizedLocal = localAccountDid.lowercased()
    guard let recoveryRoot = try loadRootPrivateKey(for: normalizedLocal, kind: .recovery) else {
      throw DeclarationError.invalidChain("Cannot recover online root without local recovery root key")
    }
    let newOnlineRoot = P256.Signing.PrivateKey()

    let head = try await publishRecordWithRetry(localAccountDid: normalizedLocal) {
      epoch,
      seq,
      prev,
      createdAt
      in
      let event = EventUnion(
        BlueCatbirdMlsChatDeclaration.RecoveryRotateOnlineRoot(
          newOnlineRootPublicKey: Bytes(data: newOnlineRoot.publicKey.rawRepresentation),
          newOnlineRootAlg: "p256",
          recoveryReason: recoveryReason
        ))

      var record = BlueCatbirdMlsChatDeclaration(
        did: try DID(didString: normalizedLocal),
        epoch: epoch,
        seq: seq,
        prev: prev,
        event: event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: Data()),
          sigAlg: "p256",
          coSig: Bytes(data: Data()),
          coSigAlg: "p256",
          deviceProof: nil
        ),
        createdAt: createdAt
      )

      let payload = try self.canonicalSigningPayload(for: record)
      let recoverySignature = try self.signPayload(payload, with: recoveryRoot)
      let newSignature = try self.signPayload(payload, with: newOnlineRoot)

      record = BlueCatbirdMlsChatDeclaration(
        did: record.did,
        epoch: record.epoch,
        seq: record.seq,
        prev: record.prev,
        event: record.event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: recoverySignature),
          sigAlg: "p256",
          coSig: Bytes(data: newSignature),
          coSigAlg: "p256",
          deviceProof: nil
        ),
        createdAt: record.createdAt
      )
      return record
    }

    try storeRootPrivateKey(newOnlineRoot, for: normalizedLocal, kind: .online)
    return head
  }

  @discardableResult
  func publishChatPolicyUpdate(
    localAccountDid: String,
    allowFollowersBypass: Bool,
    allowFollowingBypass: Bool,
    whoCanMessageMe: MLSWhoCanMessageMe?,
    autoExpireDays: Int
  ) async throws -> ChainHead {
    let normalizedLocal = localAccountDid.lowercased()
    guard (1...30).contains(autoExpireDays) else {
      throw DeclarationError.invalidRecord("chatPolicyUpdate autoExpireDays must be 1...30")
    }
    let onlineRoot = try loadOrCreateRootPrivateKey(for: normalizedLocal, kind: .online)

    return try await publishRecordWithRetry(localAccountDid: normalizedLocal) {
      epoch,
      seq,
      prev,
      createdAt
      in
      if seq == 0 {
        throw DeclarationError.invalidRecord("chatPolicyUpdate requires existing declaration chain")
      }

      let event = EventUnion(
        BlueCatbirdMlsChatDeclaration.ChatPolicyUpdate(
          allowFollowersBypass: allowFollowersBypass,
          allowFollowingBypass: allowFollowingBypass,
          whoCanMessageMe: whoCanMessageMe?.rawValue,
          autoExpireDays: autoExpireDays
        ))

      var record = BlueCatbirdMlsChatDeclaration(
        did: try DID(didString: normalizedLocal),
        epoch: epoch,
        seq: seq,
        prev: prev,
        event: event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: Data()),
          sigAlg: "p256",
          coSig: nil,
          coSigAlg: nil,
          deviceProof: nil
        ),
        createdAt: createdAt
      )

      let payload = try self.canonicalSigningPayload(for: record)
      let signature = try self.signPayload(payload, with: onlineRoot)
      record = BlueCatbirdMlsChatDeclaration(
        did: record.did,
        epoch: record.epoch,
        seq: record.seq,
        prev: record.prev,
        event: record.event,
        proofs: BlueCatbirdMlsChatDeclaration.Proofs(
          sig: Bytes(data: signature),
          sigAlg: "p256",
          coSig: nil,
          coSigAlg: nil,
          deviceProof: nil
        ),
        createdAt: record.createdAt
      )
      return record
    }
  }

  // MARK: - Verification

  private func verifyChain(localAccountDID: String, targetDID: String) async throws -> VerificationSnapshot {
    do {
      let did = try DID(didString: targetDID)
      let rawRecords = try await fetchDeclarationRecords(for: did)
      var canonicalSlots = extractCanonicalSlots(rawRecords, targetDID: targetDID)

      // Read-after-write resilience for brand-new chains: some PDS paths can briefly
      // return stale listRecords results right after seq=0 createRecord succeeds.
      // Fall back to deterministic genesis getRecord before treating as missing.
      if canonicalSlots.isEmpty,
        let genesisSlot = try await fetchGenesisSlotFallback(did: did, targetDID: targetDID)
      {
        canonicalSlots = [genesisSlot]
      }

      if canonicalSlots.isEmpty {
        let snapshot = VerificationSnapshot(
          state: .missingChain,
          head: nil,
          verifiedAt: Date(),
          authorizedDeviceKeys: [],
          onlineRootPublicKey: nil,
          onlineRootAlg: nil,
          recoveryRootPublicKey: nil,
          recoveryRootAlg: nil,
          hadRecovery: false,
          errorMessage: nil,
          chatPolicy: nil
        )
        await writeCache(localAccountDID: localAccountDID, targetDID: targetDID, snapshot: snapshot)
        return snapshot
      }

      let verified = try verifyCanonicalSlots(canonicalSlots, expectedTargetDID: targetDID)
      let cached = await readCache(localAccountDID: localAccountDID, targetDID: targetDID)

      if let cachedHead = cached?.head {
        let newHead = verified.head
        if newHead.isOlder(than: cachedHead) {
          return VerificationSnapshot(
            state: .rollback,
            head: newHead,
            verifiedAt: Date(),
            authorizedDeviceKeys: cached?.authorizedDeviceKeys ?? [],
            onlineRootPublicKey: cached?.onlineRootPublicKey,
            onlineRootAlg: cached?.onlineRootAlg,
            recoveryRootPublicKey: cached?.recoveryRootPublicKey,
            recoveryRootAlg: cached?.recoveryRootAlg,
            hadRecovery: cached?.hadRecovery ?? false,
            errorMessage: "Fetched declaration head is older than pinned head",
            chatPolicy: cached?.chatPolicy
          )
        }
      }

      let snapshot = VerificationSnapshot(
        state: .verified,
        head: verified.head,
        verifiedAt: Date(),
        authorizedDeviceKeys: verified.authorizedKeys,
        onlineRootPublicKey: verified.onlineRootPublicKey,
        onlineRootAlg: verified.onlineRootAlg,
        recoveryRootPublicKey: verified.recoveryRootPublicKey,
        recoveryRootAlg: verified.recoveryRootAlg,
        hadRecovery: verified.hadRecovery,
        errorMessage: nil,
        chatPolicy: verified.chatPolicy
      )

      await writeCache(localAccountDID: localAccountDID, targetDID: targetDID, snapshot: snapshot)
      return snapshot
    } catch let error as DeclarationError {
      logger.error(
        "❌ Declaration chain verification failed for \(targetDID, privacy: .private): \(error.localizedDescription)"
      )
      let snapshot = VerificationSnapshot(
        state: .invalid,
        head: nil,
        verifiedAt: Date(),
        authorizedDeviceKeys: [],
        onlineRootPublicKey: nil,
        onlineRootAlg: nil,
        recoveryRootPublicKey: nil,
        recoveryRootAlg: nil,
        hadRecovery: false,
        errorMessage: error.localizedDescription,
        chatPolicy: nil
      )
      await writeCache(localAccountDID: localAccountDID, targetDID: targetDID, snapshot: snapshot)
      return snapshot
    } catch {
      let cached = await readCache(localAccountDID: localAccountDID, targetDID: targetDID)
      if let cached {
        return VerificationSnapshot(
          state: .unavailable,
          head: cached.head,
          verifiedAt: cached.verifiedAt,
          authorizedDeviceKeys: cached.authorizedDeviceKeys,
          onlineRootPublicKey: cached.onlineRootPublicKey,
          onlineRootAlg: cached.onlineRootAlg,
          recoveryRootPublicKey: cached.recoveryRootPublicKey,
          recoveryRootAlg: cached.recoveryRootAlg,
          hadRecovery: cached.hadRecovery,
          errorMessage: error.localizedDescription,
          chatPolicy: cached.chatPolicy
        )
      }
      return VerificationSnapshot(
        state: .unavailable,
        head: nil,
        verifiedAt: Date.distantPast,
        authorizedDeviceKeys: [],
        onlineRootPublicKey: nil,
        onlineRootAlg: nil,
        recoveryRootPublicKey: nil,
        recoveryRootAlg: nil,
        hadRecovery: false,
        errorMessage: error.localizedDescription,
        chatPolicy: nil
      )
    }
  }

  private struct VerifiedChainState {
    let head: ChainHead
    let authorizedKeys: Set<String>
    let onlineRootPublicKey: Data
    let onlineRootAlg: String
    let recoveryRootPublicKey: Data
    let recoveryRootAlg: String
    let hadRecovery: Bool
    let chatPolicy: MLSChatPolicy?
  }

  private func verifyCanonicalSlots(
    _ slots: [CanonicalSlot],
    expectedTargetDID: String
  ) throws -> VerifiedChainState {
    let latestEpoch = slots.map(\.epoch).max() ?? 0
    let latestSlots = slots
      .filter { $0.epoch == latestEpoch }
      .sorted { lhs, rhs in
        if lhs.seq != rhs.seq { return lhs.seq < rhs.seq }
        return lhs.uri.uriString() < rhs.uri.uriString()
      }

    var sequenceMap: [Int: CanonicalSlot] = [:]
    for slot in latestSlots {
      sequenceMap[slot.seq] = slot
    }

    guard let maxSeq = latestSlots.map(\.seq).max() else {
      throw DeclarationError.invalidChain("No records in latest declaration epoch")
    }

    guard let genesis = sequenceMap[0] else {
      throw DeclarationError.invalidChain("Declaration epoch missing seq=0 rootInit")
    }

    guard let genesisRecord = genesis.declaration else {
      throw DeclarationError.invalidRecord(genesis.mismatchReason ?? "Invalid seq=0 declaration record")
    }

    var onlineRootPublicKey: Data
    var onlineRootAlg: String
    var recoveryRootPublicKey: Data
    var recoveryRootAlg: String

    switch genesisRecord.event {
    case .blueCatbirdMlsChatDeclarationRootInit(let rootInit):
      onlineRootPublicKey = rootInit.onlineRootPublicKey.data
      onlineRootAlg = normalizeSignatureAlgorithm(rootInit.onlineRootAlg)
      recoveryRootPublicKey = rootInit.recoveryRootPublicKey.data
      recoveryRootAlg = normalizeSignatureAlgorithm(rootInit.recoveryRootAlg)
    default:
      throw DeclarationError.invalidChain("seq=0 declaration record is not rootInit")
    }

    guard genesisRecord.prev == nil else {
      throw DeclarationError.invalidChain("rootInit seq=0 must not contain prev")
    }

    let genesisPayload = try canonicalSigningPayload(for: genesisRecord)
    guard
      verifySignature(
        algorithm: normalizeSignatureAlgorithm(genesisRecord.proofs.sigAlg),
        publicKey: onlineRootPublicKey,
        signature: genesisRecord.proofs.sig.data,
        payload: genesisPayload
      )
    else {
      throw DeclarationError.invalidChain("rootInit signature validation failed")
    }

    var authorizedKeys: Set<String> = []
    var deviceKeyByDeviceID: [String: String] = [:]
    var hadRecovery = false
    var currentChatPolicy: MLSChatPolicy? = nil

    if genesisRecord.did.description.lowercased() != expectedTargetDID.lowercased() {
      throw DeclarationError.invalidChain("rootInit did does not match target DID")
    }

    if maxSeq >= 1 {
      for seq in 1...maxSeq {
        guard let slot = sequenceMap[seq] else {
          throw DeclarationError.invalidChain("Declaration epoch has sequence gap at seq=\(seq)")
        }

        guard let record = slot.declaration else {
          throw DeclarationError.invalidRecord(slot.mismatchReason ?? "Invalid declaration record at seq=\(seq)")
        }

        if record.did.description.lowercased() != expectedTargetDID.lowercased() {
          throw DeclarationError.invalidChain("Declaration record did mismatch at seq=\(seq)")
        }

        guard let prev = record.prev else {
          throw DeclarationError.invalidChain("Declaration record seq=\(seq) missing prev")
        }

        guard let prevSlot = sequenceMap[seq - 1] else {
          throw DeclarationError.invalidChain("Missing previous declaration slot at seq=\(seq - 1)")
        }

        if prev.cid != prevSlot.cid.string {
          throw DeclarationError.invalidChain("Declaration prev pointer mismatch at seq=\(seq)")
        }

        let payload = try canonicalSigningPayload(for: record)

        switch record.event {
        case .blueCatbirdMlsChatDeclarationDeviceAdd(let deviceAdd):
          guard
            verifySignature(
              algorithm: normalizeSignatureAlgorithm(record.proofs.sigAlg),
              publicKey: onlineRootPublicKey,
              signature: record.proofs.sig.data,
              payload: payload
            )
          else {
            throw DeclarationError.invalidChain("deviceAdd root signature invalid at seq=\(seq)")
          }

          guard let deviceProof = record.proofs.deviceProof else {
            throw DeclarationError.invalidChain("deviceAdd missing device proof at seq=\(seq)")
          }

          let devicePayload = try canonicalDeviceProofPayload(for: record)
          guard
            verifySignature(
              algorithm: normalizeSignatureAlgorithm(deviceProof.proofAlg),
              publicKey: deviceAdd.deviceMlsSignaturePublicKey.data,
              signature: deviceProof.proofSig.data,
              payload: devicePayload
            )
          else {
            throw DeclarationError.invalidChain("deviceAdd device proof invalid at seq=\(seq)")
          }

          let keyB64 = deviceAdd.deviceMlsSignaturePublicKey.data.base64EncodedString()
          authorizedKeys.insert(keyB64)
          deviceKeyByDeviceID[deviceAdd.deviceId] = keyB64

        case .blueCatbirdMlsChatDeclarationDeviceRevoke(let deviceRevoke):
          guard
            verifySignature(
              algorithm: normalizeSignatureAlgorithm(record.proofs.sigAlg),
              publicKey: onlineRootPublicKey,
              signature: record.proofs.sig.data,
              payload: payload
            )
          else {
            throw DeclarationError.invalidChain("deviceRevoke root signature invalid at seq=\(seq)")
          }

          if let key = deviceRevoke.deviceMlsSignaturePublicKey?.data {
            authorizedKeys.remove(key.base64EncodedString())
          } else if let revokedDeviceID = deviceRevoke.deviceId,
            let keyB64 = deviceKeyByDeviceID[revokedDeviceID]
          {
            authorizedKeys.remove(keyB64)
          } else {
            throw DeclarationError.invalidChain("deviceRevoke must contain device key or deviceId")
          }

        case .blueCatbirdMlsChatDeclarationChatPolicyUpdate(let chatPolicyUpdate):
          guard
            verifySignature(
              algorithm: normalizeSignatureAlgorithm(record.proofs.sigAlg),
              publicKey: onlineRootPublicKey,
              signature: record.proofs.sig.data,
              payload: payload
            )
          else {
            throw DeclarationError.invalidChain("chatPolicyUpdate root signature invalid at seq=\(seq)")
          }

          var newPolicy = currentChatPolicy ?? MLSChatPolicy()
          if let val = chatPolicyUpdate.allowFollowersBypass {
            newPolicy.allowFollowersBypass = val
          }
          if let val = chatPolicyUpdate.allowFollowingBypass {
            newPolicy.allowFollowingBypass = val
          }
          if let val = chatPolicyUpdate.whoCanMessageMe {
            newPolicy.whoCanMessageMe = MLSWhoCanMessageMe(rawValue: val)
          }
          if let val = chatPolicyUpdate.autoExpireDays {
            newPolicy.autoExpireDays = val
          }
          currentChatPolicy = newPolicy

        case .blueCatbirdMlsChatDeclarationRootRotate(let rootRotate):
          guard
            verifySignature(
              algorithm: normalizeSignatureAlgorithm(record.proofs.sigAlg),
              publicKey: onlineRootPublicKey,
              signature: record.proofs.sig.data,
              payload: payload
            )
          else {
            throw DeclarationError.invalidChain("rootRotate old-root signature invalid at seq=\(seq)")
          }

          guard let coSigAlg = record.proofs.coSigAlg, let coSig = record.proofs.coSig else {
            throw DeclarationError.invalidChain("rootRotate missing co-signature at seq=\(seq)")
          }

          let newOnlineRootKey = rootRotate.newOnlineRootPublicKey.data
          guard
            verifySignature(
              algorithm: normalizeSignatureAlgorithm(coSigAlg),
              publicKey: newOnlineRootKey,
              signature: coSig.data,
              payload: payload
            )
          else {
            throw DeclarationError.invalidChain("rootRotate new-root co-signature invalid at seq=\(seq)")
          }

          onlineRootPublicKey = newOnlineRootKey
          onlineRootAlg = normalizeSignatureAlgorithm(rootRotate.newOnlineRootAlg)

        case .blueCatbirdMlsChatDeclarationRecoveryRotateOnlineRoot(let recoveryRotate):
          guard
            verifySignature(
              algorithm: normalizeSignatureAlgorithm(record.proofs.sigAlg),
              publicKey: recoveryRootPublicKey,
              signature: record.proofs.sig.data,
              payload: payload
            )
          else {
            throw DeclarationError.invalidChain("recoveryRotateOnlineRoot recovery signature invalid at seq=\(seq)")
          }

          guard let coSigAlg = record.proofs.coSigAlg, let coSig = record.proofs.coSig else {
            throw DeclarationError.invalidChain("recoveryRotateOnlineRoot missing co-signature at seq=\(seq)")
          }

          let newOnlineRootKey = recoveryRotate.newOnlineRootPublicKey.data
          guard
            verifySignature(
              algorithm: normalizeSignatureAlgorithm(coSigAlg),
              publicKey: newOnlineRootKey,
              signature: coSig.data,
              payload: payload
            )
          else {
            throw DeclarationError.invalidChain("recoveryRotateOnlineRoot new-root co-signature invalid at seq=\(seq)")
          }

          onlineRootPublicKey = newOnlineRootKey
          onlineRootAlg = normalizeSignatureAlgorithm(recoveryRotate.newOnlineRootAlg)
          hadRecovery = true

        case .blueCatbirdMlsChatDeclarationRootInit:
          throw DeclarationError.invalidChain("rootInit can only appear at seq=0")

        case .unexpected:
          throw DeclarationError.invalidRecord("Unexpected declaration event variant at seq=\(seq)")
        }
      }
    }

    guard let headSlot = sequenceMap[maxSeq] else {
      throw DeclarationError.invalidChain("Unable to resolve verified declaration head")
    }

    let head = ChainHead(
      epoch: latestEpoch,
      seq: maxSeq,
      cid: headSlot.cid.string,
      uri: headSlot.uri.uriString()
    )

    return VerifiedChainState(
      head: head,
      authorizedKeys: authorizedKeys,
      onlineRootPublicKey: onlineRootPublicKey,
      onlineRootAlg: onlineRootAlg,
      recoveryRootPublicKey: recoveryRootPublicKey,
      recoveryRootAlg: recoveryRootAlg,
      hadRecovery: hadRecovery,
      chatPolicy: currentChatPolicy
    )
  }

  private func extractCanonicalSlots(
    _ records: [ComAtprotoRepoListRecords.Record],
    targetDID: String
  ) -> [CanonicalSlot] {
    var slots: [CanonicalSlot] = []
    var nonConformingCount = 0

    for record in records {
      guard let rkey = record.uri.recordKey else {
        continue
      }

      guard let parsed = parseDeterministicRkey(rkey) else {
        nonConformingCount += 1
        continue
      }

      let declaration = decodeDeclaration(from: record.value)
      if let declaration {
        let didMatches = declaration.did.description.lowercased() == targetDID.lowercased()
        let epochMatches = declaration.epoch == parsed.epoch
        let seqMatches = declaration.seq == parsed.seq

        if didMatches && epochMatches && seqMatches {
          slots.append(
            CanonicalSlot(
              epoch: parsed.epoch,
              seq: parsed.seq,
              uri: record.uri,
              cid: record.cid,
              declaration: declaration,
              mismatchReason: nil
            ))
        } else {
          slots.append(
            CanonicalSlot(
              epoch: parsed.epoch,
              seq: parsed.seq,
              uri: record.uri,
              cid: record.cid,
              declaration: declaration,
              mismatchReason: "Canonical rkey payload mismatch"
            ))
        }
      } else {
        slots.append(
          CanonicalSlot(
            epoch: parsed.epoch,
            seq: parsed.seq,
            uri: record.uri,
            cid: record.cid,
            declaration: nil,
            mismatchReason: "Record does not decode to blue.catbird.mlsChat.declaration"
          ))
      }
    }

    if nonConformingCount > 0 {
      switch rolloutMode {
      case .shadow:
        logger.info("Ignoring \(nonConformingCount) non-canonical declaration record(s) (shadow mode)")
      case .soft, .full:
        logger.error("Detected \(nonConformingCount) non-canonical declaration record(s) in collection")
      }
    }

    return slots
  }

  private func decodeDeclaration(from value: ATProtocolValueContainer) -> BlueCatbirdMlsChatDeclaration? {
    switch value {
    case .knownType(let typed):
      return typed as? BlueCatbirdMlsChatDeclaration
    case .unknownType(_, let nested):
      return decodeDeclaration(from: nested)
    default:
      return nil
    }
  }

  // MARK: - Publish Helpers

  private func publishRecordWithRetry(
    localAccountDid: String,
    build: @escaping (
      _ epoch: Int,
      _ seq: Int,
      _ prev: BlueCatbirdMlsChatDeclaration.PrevLink?,
      _ createdAt: ATProtocolDate
    ) async throws -> BlueCatbirdMlsChatDeclaration
  ) async throws -> ChainHead {
    struct PendingPublishRecord {
      let head: ChainHead?
      let epoch: Int
      let seq: Int
      let record: BlueCatbirdMlsChatDeclaration
      let rkey: RecordKey
    }

    let did = try DID(didString: localAccountDid)
    let collection = try NSID(nsidString: Self.declarationCollection)

    var lastFailureStatusCode: Int?
    var lastFailureRkey: String?
    var pendingRecord: PendingPublishRecord?

    for attempt in 1...5 {
      let context = try await resolveHeadContext(for: did)

      let epoch = context.head?.epoch ?? 0
      let seq = (context.head?.seq ?? -1) + 1
      let prevRef: BlueCatbirdMlsChatDeclaration.PrevLink? = context.head.flatMap { head in
        guard
          let _ = try? ATProtocolURI(uriString: head.uri),
          let cid = try? CID.parse(head.cid)
        else {
          return nil
        }
        return BlueCatbirdMlsChatDeclaration.PrevLink(cid: cid.string)
      }

      let record: BlueCatbirdMlsChatDeclaration
      let rkey: RecordKey

      if let pendingRecord,
        pendingRecord.head == context.head,
        pendingRecord.epoch == epoch,
        pendingRecord.seq == seq
      {
        // swapCommit-only retries must preserve payload bytes/signatures for idempotency.
        record = pendingRecord.record
        rkey = pendingRecord.rkey
      } else {
        let createdAt = ATProtocolDate(date: Date())
        record = try await build(epoch, seq, prevRef, createdAt)
        rkey = try RecordKey(keyString: deterministicRkey(epoch: epoch, seq: seq))
        pendingRecord = PendingPublishRecord(
          head: context.head,
          epoch: epoch,
          seq: seq,
          record: record,
          rkey: rkey
        )
      }

      let input = ComAtprotoRepoCreateRecord.Input(
        repo: .did(did),
        collection: collection,
        rkey: rkey,
        // Custom declaration lexicon may not be installed on every PDS.
        // Keep protocol-level validation client-side and disable server lex validation.
        validate: false,
        record: .knownType(record),
        swapCommit: context.repoCommitCID
      )

      let (responseCode, responseData) = try await atProtoClient.com.atproto.repo.createRecord(input: input)
      if (200...299).contains(responseCode), let responseData {
        return ChainHead(
          epoch: epoch,
          seq: seq,
          cid: responseData.cid.string,
          uri: responseData.uri.uriString()
        )
      }

      lastFailureStatusCode = responseCode
      lastFailureRkey = rkey.value
      logger.warning(
        "Declaration publish failed (attempt \(attempt)/5, status: \(responseCode), rkey: \(rkey.value, privacy: .private(mask: .hash)))"
      )

      // Deterministic rkey may already exist; treat equivalent payload as idempotent success.
      if let existing = try await fetchDeclarationRecord(did: did, rkey: rkey),
        let existingDeclaration = decodeDeclaration(from: existing.value),
        declarationsAreEqual(existingDeclaration, record)
      {
        logger.info(
          "Declaration publish detected idempotent existing record at rkey \(rkey.value, privacy: .private(mask: .hash))"
        )
        return ChainHead(
          epoch: epoch,
          seq: seq,
          cid: existing.cid?.string ?? context.head?.cid ?? "",
          uri: existing.uri.uriString()
        )
      }

      let refreshed = try await resolveHeadContext(for: did)
      if refreshed.head == context.head {
        // swapCommit likely stale due unrelated repo write; retry same slot.
        logger.info(
          "Declaration publish retrying same slot (head unchanged at epoch \(epoch), seq \(seq))"
        )
        try await Task.sleep(nanoseconds: UInt64.random(in: 40_000_000...140_000_000))
        continue
      }

      // Declaration head advanced, rebuild with next seq in next iteration.
      logger.info(
        "Declaration head advanced during publish (old seq: \(context.head?.seq ?? -1), new seq: \(refreshed.head?.seq ?? -1)); rebinding"
      )
      pendingRecord = nil
      try await Task.sleep(nanoseconds: UInt64.random(in: 40_000_000...140_000_000))

      if attempt == 5 {
        throw DeclarationError.networkFailure(
          "Failed to publish declaration record after retries (status: \(lastFailureStatusCode ?? -1), rkey: \(lastFailureRkey ?? "unknown"))"
        )
      }
    }

    throw DeclarationError.networkFailure(
      "Retry budget exhausted publishing declaration record (status: \(lastFailureStatusCode ?? -1), rkey: \(lastFailureRkey ?? "unknown"))"
    )
  }

  private func declarationsAreEqual(
    _ lhs: BlueCatbirdMlsChatDeclaration,
    _ rhs: BlueCatbirdMlsChatDeclaration
  ) -> Bool {
    lhs == rhs
  }

  private func resolveHeadContext(for did: DID) async throws -> ResolvedHeadContext {
    let localDid = did.description.lowercased()
    let snapshot = try await verifyChain(localAccountDID: localDid, targetDID: localDid)

    let latest = try await atProtoClient.com.atproto.sync.getLatestCommit(
      input: ComAtprotoSyncGetLatestCommit.Parameters(did: did)
    )

    guard (200...299).contains(latest.responseCode), let commit = latest.data else {
      throw DeclarationError.networkFailure("Failed to fetch latest repo commit for swapCommit")
    }

    return ResolvedHeadContext(head: snapshot.head, repoCommitCID: commit.cid)
  }

  private func fetchDeclarationRecord(
    did: DID,
    rkey: RecordKey
  ) async throws -> ComAtprotoRepoGetRecord.Output? {
    let input = ComAtprotoRepoGetRecord.Parameters(
      repo: .did(did),
      collection: try NSID(nsidString: Self.declarationCollection),
      rkey: rkey,
      cid: nil
    )
    let (code, data) = try await atProtoClient.com.atproto.repo.getRecord(input: input)
    guard (200...299).contains(code) else { return nil }
    return data
  }

  // MARK: - Network helpers

  private func fetchDeclarationRecords(for did: DID) async throws -> [ComAtprotoRepoListRecords.Record] {
    var cursor: String?
    var records: [ComAtprotoRepoListRecords.Record] = []
    let collection = try NSID(nsidString: Self.declarationCollection)

    repeat {
      let input = ComAtprotoRepoListRecords.Parameters(
        repo: .did(did),
        collection: collection,
        limit: 100,
        cursor: cursor,
        reverse: false
      )

      let (responseCode, data) = try await atProtoClient.com.atproto.repo.listRecords(input: input)
      guard (200...299).contains(responseCode), let data else {
        throw DeclarationError.networkFailure("listRecords failed for declaration collection")
      }

      records.append(contentsOf: data.records)
      cursor = data.cursor
    } while cursor != nil

    return records
  }

  // MARK: - Decisions

  nonisolated static func authorizationDecision(
    for state: MLSDeclarationChainState,
    rolloutMode: MLSDeclarationRolloutMode,
    securitySensitive: Bool
  ) -> MLSDeclarationAuthorizationDecision {
    switch state {
    case .missingChain:
      switch rolloutMode {
      case .shadow:
        return MLSDeclarationAuthorizationDecision(
          allowed: true,
          warning: "No declaration chain found (shadow mode)"
        )
      case .soft:
        return MLSDeclarationAuthorizationDecision(
          allowed: true,
          requiresUserFriction: securitySensitive,
          warning: "No declaration chain found (soft mode friction required)"
        )
      case .full:
        return MLSDeclarationAuthorizationDecision(
          allowed: false,
          failureReason: "Declaration chain required in enforcement mode"
        )
      }

    case .invalid:
      switch rolloutMode {
      case .shadow:
        return MLSDeclarationAuthorizationDecision(
          allowed: true,
          warning: "Declaration chain is invalid (shadow mode - allowing)"
        )
      case .soft, .full:
        return MLSDeclarationAuthorizationDecision(
          allowed: false,
          failureReason: "Declaration chain is invalid"
        )
      }

    case .rollback:
      switch rolloutMode {
      case .shadow:
        return MLSDeclarationAuthorizationDecision(
          allowed: true,
          warning: "Declaration rollback detected (shadow mode - allowing)"
        )
      case .soft, .full:
        return MLSDeclarationAuthorizationDecision(
          allowed: false,
          failureReason: "Declaration rollback detected"
        )
      }

    case .unavailable:
      switch rolloutMode {
      case .shadow:
        return MLSDeclarationAuthorizationDecision(
          allowed: true,
          warning: "Declaration service unavailable (shadow mode)"
        )
      case .soft, .full:
        return MLSDeclarationAuthorizationDecision(
          allowed: false,
          failureReason: "Declaration service unavailable"
        )
      }

    case .verified:
      return MLSDeclarationAuthorizationDecision(allowed: true)
    }
  }

  private func decisionForNonVerifiedState(
    _ state: ChainState,
    securitySensitive: Bool
  ) -> MLSDeclarationAuthorizationDecision {
    Self.authorizationDecision(for: state, rolloutMode: rolloutMode, securitySensitive: securitySensitive)
  }

  private func decisionForUnavailableWithoutCache(
    securitySensitive: Bool
  ) -> MLSDeclarationAuthorizationDecision {
    switch rolloutMode {
    case .shadow:
      return MLSDeclarationAuthorizationDecision(
        allowed: true,
        warning: "Declaration unavailable without cache (shadow mode)"
      )
    case .soft:
      return MLSDeclarationAuthorizationDecision(
        allowed: false,
        failureReason: securitySensitive
          ? "Cannot verify declaration for security-sensitive operation"
          : "Declaration unavailable"
      )
    case .full:
      return MLSDeclarationAuthorizationDecision(
        allowed: false,
        failureReason: "Cannot verify declaration chain"
      )
    }
  }

  // MARK: - Cache

  private func readCache(localAccountDID: String, targetDID: String) async -> CachedSnapshot? {
    do {
      return try await database.read { db in
        guard
          let row = try Row.fetchOne(
            db,
            sql: """
              SELECT localAccountDID, targetDID, chainState, verifiedAt,
                     headEpoch, headSeq, headCID, headURI,
                     onlineRootPublicKey, onlineRootAlg,
                     recoveryRootPublicKey, recoveryRootAlg,
                     authorizedDeviceKeysJSON, hadRecovery, lastError,
                     chatPolicyJSON
              FROM MLSDeclarationCache
              WHERE localAccountDID = ? AND targetDID = ?
            """,
            arguments: [localAccountDID, targetDID]
          )
        else {
          return nil
        }

        let stateRaw: String = row["chainState"]
        let state = ChainState(rawValue: stateRaw) ?? .unavailable
        let verifiedAtTimestamp: Double = row["verifiedAt"]

        let headEpoch: Int? = row["headEpoch"]
        let headSeq: Int? = row["headSeq"]
        let headCID: String? = row["headCID"]
        let headURI: String? = row["headURI"]
        let head: ChainHead?
        if let headEpoch, let headSeq, let headCID, let headURI {
          head = ChainHead(epoch: headEpoch, seq: headSeq, cid: headCID, uri: headURI)
        } else {
          head = nil
        }

        let keysJSON: String? = row["authorizedDeviceKeysJSON"]
        let keys: Set<String>
        if let keysJSON,
          let data = keysJSON.data(using: .utf8),
          let decoded = try? JSONDecoder().decode([String].self, from: data)
        {
          keys = Set(decoded)
        } else {
          keys = []
        }

        let chatPolicyJSON: String? = row["chatPolicyJSON"]
        let chatPolicy: MLSChatPolicy?
        if let chatPolicyJSON,
          let data = chatPolicyJSON.data(using: .utf8),
          let decoded = try? JSONDecoder().decode(MLSChatPolicy.self, from: data)
        {
          chatPolicy = decoded
        } else {
          chatPolicy = nil
        }

        let hadRecoveryInt: Int64 = row["hadRecovery"]

        return CachedSnapshot(
          localAccountDID: row["localAccountDID"],
          targetDID: row["targetDID"],
          state: state,
          verifiedAt: Date(timeIntervalSince1970: verifiedAtTimestamp),
          head: head,
          authorizedDeviceKeys: keys,
          onlineRootPublicKey: row["onlineRootPublicKey"],
          onlineRootAlg: row["onlineRootAlg"],
          recoveryRootPublicKey: row["recoveryRootPublicKey"],
          recoveryRootAlg: row["recoveryRootAlg"],
          hadRecovery: hadRecoveryInt == 1,
          lastError: row["lastError"],
          chatPolicy: chatPolicy
        )
      }
    } catch {
      logger.error("Failed to read declaration cache: \(error.localizedDescription)")
      return nil
    }
  }

  private func fetchGenesisSlotFallback(did: DID, targetDID: String) async throws -> CanonicalSlot? {
    let genesisRkey = try RecordKey(keyString: deterministicRkey(epoch: 0, seq: 0))
    guard let record = try await fetchDeclarationRecord(did: did, rkey: genesisRkey) else {
      return nil
    }
    guard let cid = record.cid else {
      return nil
    }
    guard let declaration = decodeDeclaration(from: record.value) else {
      return nil
    }

    let didMatches = declaration.did.description.lowercased() == targetDID.lowercased()
    let epochMatches = declaration.epoch == 0
    let seqMatches = declaration.seq == 0

    guard didMatches, epochMatches, seqMatches else {
      return CanonicalSlot(
        epoch: 0,
        seq: 0,
        uri: record.uri,
        cid: cid,
        declaration: declaration,
        mismatchReason: "Genesis fallback canonical mismatch"
      )
    }

    return CanonicalSlot(
      epoch: 0,
      seq: 0,
      uri: record.uri,
      cid: cid,
      declaration: declaration,
      mismatchReason: nil
    )
  }

  private func writeCache(
    localAccountDID: String,
    targetDID: String,
    snapshot: VerificationSnapshot
  ) async {
    let keysJSON: String
    do {
      let encoded = try JSONEncoder().encode(Array(snapshot.authorizedDeviceKeys).sorted())
      keysJSON = String(decoding: encoded, as: UTF8.self)
    } catch {
      keysJSON = "[]"
    }

    let chatPolicyJSON: String?
    if let chatPolicy = snapshot.chatPolicy,
      let encoded = try? JSONEncoder().encode(chatPolicy)
    {
      chatPolicyJSON = String(decoding: encoded, as: UTF8.self)
    } else {
      chatPolicyJSON = nil
    }

    do {
      try await database.write { db in
        try db.execute(
          sql: """
            INSERT INTO MLSDeclarationCache (
              localAccountDID, targetDID, chainState, verifiedAt,
              headEpoch, headSeq, headCID, headURI,
              onlineRootPublicKey, onlineRootAlg,
              recoveryRootPublicKey, recoveryRootAlg,
              authorizedDeviceKeysJSON, hadRecovery, lastError,
              chatPolicyJSON
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(localAccountDID, targetDID) DO UPDATE SET
              chainState = excluded.chainState,
              verifiedAt = excluded.verifiedAt,
              headEpoch = excluded.headEpoch,
              headSeq = excluded.headSeq,
              headCID = excluded.headCID,
              headURI = excluded.headURI,
              onlineRootPublicKey = excluded.onlineRootPublicKey,
              onlineRootAlg = excluded.onlineRootAlg,
              recoveryRootPublicKey = excluded.recoveryRootPublicKey,
              recoveryRootAlg = excluded.recoveryRootAlg,
              authorizedDeviceKeysJSON = excluded.authorizedDeviceKeysJSON,
              hadRecovery = excluded.hadRecovery,
              lastError = excluded.lastError,
              chatPolicyJSON = excluded.chatPolicyJSON
          """,
          arguments: [
            localAccountDID,
            targetDID,
            snapshot.state.rawValue,
            snapshot.verifiedAt.timeIntervalSince1970,
            snapshot.head?.epoch,
            snapshot.head?.seq,
            snapshot.head?.cid,
            snapshot.head?.uri,
            snapshot.onlineRootPublicKey,
            snapshot.onlineRootAlg,
            snapshot.recoveryRootPublicKey,
            snapshot.recoveryRootAlg,
            keysJSON,
            snapshot.hadRecovery ? 1 : 0,
            snapshot.errorMessage,
            chatPolicyJSON
          ]
        )
      }
    } catch {
      logger.error("Failed to write declaration cache: \(error.localizedDescription)")
    }
  }

  // MARK: - Root key persistence

  private enum RootKeyKind: String {
    case online
    case recovery
  }

  private func rootKeyKeychainKey(for did: String, kind: RootKeyKind) -> String {
    "mls.declaration.root.\(kind.rawValue).\(did.lowercased())"
  }

  private func rootRotationMetadataKey(for did: String) -> String {
    "mls.declaration.root.online.rotatedAt.\(did.lowercased())"
  }

  private func loadRootPrivateKey(
    for did: String,
    kind: RootKeyKind
  ) throws -> P256.Signing.PrivateKey? {
    let keyName = rootKeyKeychainKey(for: did, kind: kind)
    let synchronizable = kind == .online
    // Try synchronizable query first for online root (multi-device),
    // fall back to non-sync for migration from old device-local storage
    if let data = try MLSKeychainManager.shared.retrieve(forKey: keyName, synchronizable: synchronizable) {
      return try? P256.Signing.PrivateKey(rawRepresentation: data)
    }
    if synchronizable {
      // Migration: try loading legacy device-local key
      if let data = try MLSKeychainManager.shared.retrieve(forKey: keyName, synchronizable: false) {
        let key = try? P256.Signing.PrivateKey(rawRepresentation: data)
        if let key {
          // Re-store as synchronizable so it propagates to other devices
          try? storeRootPrivateKey(key, for: did, kind: kind)
          try? MLSKeychainManager.shared.delete(forKey: keyName, synchronizable: false)
        }
        return key
      }
    }
    return nil
  }

  private func loadOrCreateRootPrivateKey(
    for did: String,
    kind: RootKeyKind
  ) throws -> P256.Signing.PrivateKey {
    if let key = try loadRootPrivateKey(for: did, kind: kind) {
      return key
    }

    let key = P256.Signing.PrivateKey()
    try storeRootPrivateKey(key, for: did, kind: kind)
    return key
  }

  /// Force-create a new root key, replacing any existing one.
  /// Used during epoch bumps to start with a clean key pair.
  private func createFreshRootPrivateKey(
    for did: String,
    kind: RootKeyKind
  ) throws -> P256.Signing.PrivateKey {
    let key = P256.Signing.PrivateKey()
    try storeRootPrivateKey(key, for: did, kind: kind)
    return key
  }

  private func storeRootPrivateKey(
    _ key: P256.Signing.PrivateKey,
    for did: String,
    kind: RootKeyKind
  ) throws {
    let keyName = rootKeyKeychainKey(for: did, kind: kind)
    // Online root syncs via iCloud Keychain so all devices share the same
    // declaration authority. Recovery root stays device-local for security.
    let accessible: CFString = kind == .online
      ? kSecAttrAccessibleAfterFirstUnlock
      : kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    let synchronizable = kind == .online
    try MLSKeychainManager.shared.store(
      key.rawRepresentation,
      forKey: keyName,
      accessible: accessible,
      synchronizable: synchronizable
    )
    if kind == .online {
      markOnlineRootRotatedNow(for: did)
    }
  }

  private func markOnlineRootRotatedNow(for did: String) {
    UserDefaults.standard.set(Date().timeIntervalSince1970, forKey: rootRotationMetadataKey(for: did))
  }

  private func shouldPerformProactiveRootRotation(for did: String) -> Bool {
    let key = rootRotationMetadataKey(for: did)
    let lastRotated = UserDefaults.standard.double(forKey: key)
    guard lastRotated > 0 else {
      markOnlineRootRotatedNow(for: did)
      return false
    }
    let age = Date().timeIntervalSince1970 - lastRotated
    return age >= proactiveRootRotationInterval
  }

  // MARK: - Current device signature material

  private func deviceSignaturePublicKeyCacheKey(for did: String) -> String {
    "mls.declaration.device.sigpub.\(did.lowercased())"
  }

  private func deviceSignatureAlgorithmCacheKey(for did: String) -> String {
    "mls.declaration.device.sigalg.\(did.lowercased())"
  }

  private func currentDeviceSignatureMaterial(for did: String) async throws -> CurrentDeviceSignatureMaterial {
    let normalized = did.lowercased()
    let keyKey = deviceSignaturePublicKeyCacheKey(for: normalized)
    let algKey = deviceSignatureAlgorithmCacheKey(for: normalized)

    if let keyData = try MLSKeychainManager.shared.retrieve(forKey: keyKey),
      let algData = try MLSKeychainManager.shared.retrieve(forKey: algKey),
      let alg = String(data: algData, encoding: .utf8)
    {
      return CurrentDeviceSignatureMaterial(publicKey: keyData, algorithm: alg)
    }

    // Bootstrap from a freshly generated key package only when cache is absent.
    let keyPackage = try await mlsClient.createKeyPackage(for: normalized)
    let extracted = try await mlsClient.extractKeyPackageSignatureKey(keyPackageData: keyPackage)

    try MLSKeychainManager.shared.store(
      extracted.publicKey,
      forKey: keyKey,
      accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    )
    try MLSKeychainManager.shared.store(
      Data(extracted.algorithm.utf8),
      forKey: algKey,
      accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    )

    return CurrentDeviceSignatureMaterial(
      publicKey: extracted.publicKey,
      algorithm: extracted.algorithm
    )
  }

  // MARK: - Canonical payloads

  private func canonicalSigningPayload(for record: BlueCatbirdMlsChatDeclaration) throws -> Data {
    var map = OrderedCBORMap()
    map = map.adding(key: "$type", value: BlueCatbirdMlsChatDeclaration.typeIdentifier)
    map = map.adding(key: "did", value: try record.did.toCBORValue())
    map = map.adding(key: "epoch", value: record.epoch)
    map = map.adding(key: "seq", value: record.seq)
    if let prev = record.prev {
      map = map.adding(key: "prev", value: try prev.toCBORValue())
    }
    map = map.adding(key: "event", value: try record.event.toCBORValue())
    map = map.adding(key: "createdAt", value: try record.createdAt.toCBORValue())
    return try DAGCBOR.encodeValue(map)
  }

  private func canonicalDeviceProofPayload(for record: BlueCatbirdMlsChatDeclaration) throws -> Data {
    guard case .blueCatbirdMlsChatDeclarationDeviceAdd(let deviceAdd) = record.event else {
      throw DeclarationError.invalidRecord("Device proof payload requested for non-deviceAdd event")
    }
    guard let prevCID = record.prev?.cid else {
      throw DeclarationError.invalidRecord("deviceAdd requires prev.cid for proof payload")
    }

    var map = OrderedCBORMap()
    map = map.adding(key: "domainSep", value: "blue.catbird.mlsChat.declaration.deviceAdd.v1")
    map = map.adding(key: "did", value: record.did.description)
    map = map.adding(key: "epoch", value: record.epoch)
    map = map.adding(key: "seq", value: record.seq)
    map = map.adding(key: "prevCid", value: prevCID)
    map = map.adding(key: "deviceMlsSignaturePublicKey", value: deviceAdd.deviceMlsSignaturePublicKey.data)
    map = map.adding(key: "createdAt", value: record.createdAt.iso8601String)
    return try DAGCBOR.encodeValue(map)
  }

  private func signPayload(_ payload: Data, with key: P256.Signing.PrivateKey) throws -> Data {
    let signature = try key.signature(for: payload)
    return signature.rawRepresentation
  }

  // MARK: - Signature verification

  private func normalizeSignatureAlgorithm(_ algorithm: String) -> String {
    let lower = algorithm.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    if lower == "ecdsa-p256" || lower == "secp256r1" || lower == "p-256" {
      return "p256"
    }
    if lower == "ed-25519" {
      return "ed25519"
    }
    return lower
  }

  private func verifySignature(
    algorithm: String,
    publicKey: Data,
    signature: Data,
    payload: Data
  ) -> Bool {
    switch normalizeSignatureAlgorithm(algorithm) {
    case "p256":
      guard let publicKey = try? P256.Signing.PublicKey(rawRepresentation: publicKey),
        let signature = try? P256.Signing.ECDSASignature(rawRepresentation: signature)
      else {
        return false
      }
      return publicKey.isValidSignature(signature, for: payload)

    case "ed25519":
      guard let publicKey = try? Curve25519.Signing.PublicKey(rawRepresentation: publicKey) else {
        return false
      }
      return publicKey.isValidSignature(signature, for: payload)

    default:
      return false
    }
  }

  // MARK: - Deterministic rkey

  private func deterministicRkey(epoch: Int, seq: Int) -> String {
    Self.formatDeterministicRkey(epoch: epoch, seq: seq)
  }

  private func parseDeterministicRkey(_ rkey: String) -> (epoch: Int, seq: Int)? {
    Self.parseDeterministicRkey(rkey)
  }

  nonisolated static func formatDeterministicRkey(epoch: Int, seq: Int) -> String {
    String(format: "e%020lld-s%020lld", Int64(epoch), Int64(seq))
  }

  nonisolated static func parseDeterministicRkey(_ rkey: String) -> (epoch: Int, seq: Int)? {
    let parts = rkey.split(separator: "-", omittingEmptySubsequences: false)
    guard parts.count == 2,
      parts[0].count == 21,
      parts[1].count == 21,
      parts[0].first == "e",
      parts[1].first == "s"
    else {
      return nil
    }

    let epochString = String(parts[0].dropFirst())
    let seqString = String(parts[1].dropFirst())

    guard epochString.count == 20,
      seqString.count == 20,
      let epoch = Int(epochString),
      let seq = Int(seqString)
    else {
      return nil
    }

    return (epoch, seq)
  }
}
