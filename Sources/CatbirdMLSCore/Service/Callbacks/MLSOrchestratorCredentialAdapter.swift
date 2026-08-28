//
//  MLSOrchestratorCredentialAdapter.swift
//  CatbirdMLSCore
//
//  Bridges the OrchestratorCredentialCallback protocol (called synchronously from Rust)
//  to the iOS Keychain via MLSKeychain and MLSKeychainManager.
//

import CatbirdMLS
import CryptoKit
import Foundation
import OSLog
import Security

// MARK: - MLSOrchestratorCredentialAdapter

/// Adapts iOS Keychain storage to the synchronous `OrchestratorCredentialCallback`
/// protocol expected by the Rust orchestrator via UniFFI.
///
/// Signing keys are stored in the Keychain using `MLSKeychain` (which uses
/// `kSecClassKey` with application tags). MLS DID and device UUID are stored
/// in the Keychain as generic passwords via `MLSKeychainManager`, scoped by
/// user DID to maintain per-user isolation.
///
/// All methods are synchronous and called from a Rust background thread.
public final class MLSOrchestratorCredentialAdapter: OrchestratorCredentialCallback, @unchecked Sendable {

  /// Binding metadata is resolved by the authenticated host. It is never
  /// accepted from the request caller because the Rust signer binds the
  /// transcript to this exact device/session snapshot.
  public struct SigningBindingSnapshot: Equatable, Sendable {
    public let deviceId: String
    public let dpopJkt: String
    public let authGeneration: Int64?

    public init(deviceId: String, dpopJkt: String, authGeneration: Int64?) {
      self.deviceId = deviceId
      self.dpopJkt = dpopJkt
      self.authGeneration = authGeneration
    }
  }

  /// Atomic signing authority snapshot resolved per call. Any component
  /// mismatch between before and after signing invalidates the whole call.
  public struct SigningAuthoritySnapshot: Equatable, Sendable {
    public let actorDid: String
    public let deviceId: String
    public let dpopJkt: String
    public let authGeneration: Int64?
    public let signerHandle: String
    public let publicKey: Data
    public let signer: TranscriptSigner

    public init(
      actorDid: String,
      deviceId: String,
      dpopJkt: String,
      authGeneration: Int64?,
      signerHandle: String,
      publicKey: Data,
      signer: @escaping TranscriptSigner
    ) {
      self.actorDid = actorDid
      self.deviceId = deviceId
      self.dpopJkt = dpopJkt
      self.authGeneration = authGeneration
      self.signerHandle = signerHandle
      self.publicKey = publicKey
      self.signer = signer
    }

    public static func == (lhs: SigningAuthoritySnapshot, rhs: SigningAuthoritySnapshot) -> Bool {
      lhs.actorDid == rhs.actorDid &&
      lhs.deviceId == rhs.deviceId &&
      lhs.dpopJkt == rhs.dpopJkt &&
      lhs.authGeneration == rhs.authGeneration &&
      lhs.signerHandle == rhs.signerHandle &&
      lhs.publicKey == rhs.publicKey
    }
  }

  /// The signer runs inside platform-owned key custody. The callback returns
  /// only signature bytes; private key material is not part of this seam.
  public typealias TranscriptSigner = @Sendable (String, Data) throws -> Data
  public typealias SigningPublicKeyResolver = @Sendable (String) -> Data?
  public typealias SigningBindingResolver = @Sendable (String) -> SigningBindingSnapshot?
  public typealias SigningAuthorityResolver = @Sendable (String) -> SigningAuthoritySnapshot?

  private let keychainManager: MLSKeychainManager
  private let authorizedDeviceKeyResolver: (@Sendable (String) -> [Data]?)?
  private let signingAuthorityResolver: SigningAuthorityResolver?
  private let transcriptSigner: TranscriptSigner?
  private let signingPublicKeyResolver: SigningPublicKeyResolver?
  private let signingBindingResolver: SigningBindingResolver?
  private let logger = Logger(subsystem: "blue.catbird", category: "OrchestratorCredentialAdapter")

  /// Keychain key prefix for MLS DID storage (scoped by user DID).
  private static let mlsDidKeyPrefix = "mls.credential.mlsDid."

  /// Keychain key prefix for device UUID storage (scoped by user DID).
  private static let deviceUuidKeyPrefix = "mls.credential.deviceUuid."

  // MARK: - Initialization

  /// Create an adapter that stores credentials in the iOS Keychain.
  /// - Parameter keychainManager: The keychain manager instance to use. Defaults to `.shared`.
  public init(
    keychainManager: MLSKeychainManager = .shared,
    authorizedDeviceKeyResolver: (@Sendable (String) -> [Data]?)? = nil,
    signingAuthorityResolver: SigningAuthorityResolver? = nil,
    transcriptSigner: TranscriptSigner? = nil,
    signingPublicKeyResolver: SigningPublicKeyResolver? = nil,
    signingBindingResolver: SigningBindingResolver? = nil
  ) {
    self.keychainManager = keychainManager
    self.authorizedDeviceKeyResolver = authorizedDeviceKeyResolver
    self.signingAuthorityResolver = signingAuthorityResolver
    self.transcriptSigner = transcriptSigner
    self.signingPublicKeyResolver = signingPublicKeyResolver
    self.signingBindingResolver = signingBindingResolver
  }

  public convenience init(
    authorizedDeviceKeyResolver: @escaping @Sendable (String) -> [Data]?
  ) {
    self.init(
      keychainManager: .shared,
      authorizedDeviceKeyResolver: authorizedDeviceKeyResolver,
      transcriptSigner: nil,
      signingPublicKeyResolver: nil,
      signingBindingResolver: nil
    )
  }

  // MARK: - Signing Keys

  public func storeSigningKey(userDid: String, keyData: Data) throws {
    logger.debug("Storing signing key for user: \(userDid.prefix(20))...")
    try MLSKeychain.storeSignatureKey(keyData, forIdentity: userDid)
    logger.info("Stored signing key for user: \(userDid.prefix(20))...")
  }

  public func getSigningKey(userDid: String) throws -> Data? {
    logger.debug("Retrieving signing key for user: \(userDid.prefix(20))...")
    do {
      let keyData = try MLSKeychain.retrieveSignatureKey(forIdentity: userDid)
      logger.debug("Retrieved signing key for user: \(userDid.prefix(20))...")
      return keyData
    } catch MLSKeychainError.retrieveFailed(let status) where status == errSecItemNotFound {
      // Key does not exist yet - this is expected for new users
      logger.debug("No signing key found for user: \(userDid.prefix(20))...")
      return nil
    }
  }

  public func signCleanChatTranscript(
    userDid: String,
    transcript: Data,
    keyId: String
  ) throws -> CleanChatSigningAuthorityFfi? {
    guard !transcript.isEmpty else {
      logger.error("Refusing to sign an empty clean-chat transcript")
      return nil
    }

    if let signingAuthorityResolver {
      guard let authorityBefore = signingAuthorityResolver(userDid),
            authorityBefore.actorDid == userDid,
            !authorityBefore.deviceId.isEmpty,
            !authorityBefore.dpopJkt.isEmpty,
            !authorityBefore.publicKey.isEmpty
      else {
        logger.error("Clean-chat atomic signing authority is unavailable before signing for user: \(userDid.prefix(20))...")
        return nil
      }

      guard Self.keyIdentifier(forPublicKey: authorityBefore.publicKey) == keyId else {
        logger.error("Clean-chat signer key identifier did not match the requested authority")
        return nil
      }

      let signature = try authorityBefore.signer(userDid, transcript)

      guard let verificationKey = try? Curve25519.Signing.PublicKey(rawRepresentation: authorityBefore.publicKey),
            verificationKey.isValidSignature(signature, for: transcript)
      else {
        logger.error("Clean-chat signer returned a signature/public-key pair that failed verification")
        return nil
      }

      guard let authorityAfter = signingAuthorityResolver(userDid),
            authorityAfter == authorityBefore
      else {
        logger.error("Clean-chat atomic signing authority changed while signing")
        return nil
      }

      return CleanChatSigningAuthorityFfi(
        publicKey: authorityAfter.publicKey,
        signature: signature,
        deviceId: authorityAfter.deviceId,
        authGeneration: authorityAfter.authGeneration
      )
    }

    // Capture the authenticated authority before entering key custody. The
    // second snapshot below must remain equal so a logout, key rotation, or
    // account switch racing this synchronous callback fails closed rather than
    // returning a signature bound to a different session.
    guard let bindingBeforeSignature = signingBindingResolver?(userDid),
          !bindingBeforeSignature.deviceId.isEmpty,
          !bindingBeforeSignature.dpopJkt.isEmpty
    else {
      logger.error("Clean-chat signer binding is unavailable before signing for user: \(userDid.prefix(20))...")
      return nil
    }

    // A platform signer and its public-key resolver are separate non-exporting
    // handles. Resolve the key on both sides of signing so a concurrent key
    // rotation cannot pair an old signature with a new authority key.
    let publicKeyBeforeSignature = signingPublicKeyResolver?(userDid)
    if transcriptSigner != nil {
      guard let publicKeyBeforeSignature,
            !publicKeyBeforeSignature.isEmpty
      else {
        logger.error("Clean-chat signer public key is unavailable before signing")
        return nil
      }
    }

    // The normal path delegates to the Rust/OpenMLS context (or another
    // platform non-exporting signer). A raw CryptoKit key is accepted only as
    // a compatibility path for older keychain records; serialized OpenMLS
    // keypairs never cross this callback and therefore fail closed unless a
    // platform signer was injected.
    let signature: Data
    let publicKey: Data
    if let transcriptSigner {
      signature = try transcriptSigner(userDid, transcript)
      guard let publicKeyAfterSignature = signingPublicKeyResolver?(userDid),
            publicKeyAfterSignature == publicKeyBeforeSignature
      else {
        logger.error("Clean-chat signer authority changed while signing")
        return nil
      }
      publicKey = publicKeyAfterSignature
    } else {
      let keyData = try getSigningKey(userDid: userDid)
      guard let keyData,
            let privateKey = try? Curve25519.Signing.PrivateKey(rawRepresentation: keyData)
      else {
        logger.error("No non-exporting clean-chat signer is available for user: \(userDid.prefix(20))...")
        return nil
      }
      signature = try privateKey.signature(for: transcript)
      publicKey = privateKey.publicKey.rawRepresentation
    }

    guard Self.keyIdentifier(forPublicKey: publicKey) == keyId else {
      logger.error("Clean-chat signer key identifier did not match the requested authority")
      return nil
    }

    if transcriptSigner != nil {
      guard let verificationKey = try? Curve25519.Signing.PublicKey(rawRepresentation: publicKey),
            verificationKey.isValidSignature(signature, for: transcript)
      else {
        logger.error("Clean-chat signer returned a signature/public-key pair that failed verification")
        return nil
      }
    }

    // Binding is authoritative host state. Do not invent a device/JKT or
    // silently sign with stale metadata when the session changes while the
    // callback is executing.
    guard let bindingAfterSignature = signingBindingResolver?(userDid),
          bindingAfterSignature == bindingBeforeSignature,
          !bindingAfterSignature.deviceId.isEmpty,
          !bindingAfterSignature.dpopJkt.isEmpty
    else {
      logger.error("Clean-chat signer authority changed while signing")
      return nil
    }

    return CleanChatSigningAuthorityFfi(
      publicKey: publicKey,
      signature: signature,
      deviceId: bindingAfterSignature.deviceId,
      authGeneration: bindingAfterSignature.authGeneration
    )
  }

  /// Rust derives clean-chat key IDs from the Ed25519 public key using
  /// base64url without padding. Keep this helper local to the callback so the
  /// caller cannot provide an unrelated public-key/signature tuple.
  public static func keyIdentifier(forPublicKey publicKey: Data) -> String {
    Data(SHA256.hash(data: publicKey))
      .base64EncodedString()
      .replacingOccurrences(of: "+", with: "-")
      .replacingOccurrences(of: "/", with: "_")
      .trimmingCharacters(in: CharacterSet(charactersIn: "="))
  }

  public func deleteSigningKey(userDid: String) throws {
    logger.debug("Deleting signing key for user: \(userDid.prefix(20))...")
    try MLSKeychain.deleteSignatureKey(forIdentity: userDid)
    logger.info("Deleted signing key for user: \(userDid.prefix(20))...")
  }

  // MARK: - MLS DID

  public func storeMlsDid(userDid: String, mlsDid: String) throws {
    let normalized = userDid.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let key = "\(Self.mlsDidKeyPrefix)\(normalized).\(MLSStoragePaths.cleanSuffix)"
    let data = Data(mlsDid.utf8)
    logger.debug("Storing MLS DID for user: \(normalized.prefix(20))...")
    try keychainManager.store(
      data,
      forKey: key,
      accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    )
    logger.info("Stored MLS DID for user: \(normalized.prefix(20))...")
  }

  public func getMlsDid(userDid: String) throws -> String? {
    let normalized = userDid.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let key = "\(Self.mlsDidKeyPrefix)\(normalized).\(MLSStoragePaths.cleanSuffix)"
    logger.debug("Retrieving MLS DID for user: \(normalized.prefix(20))...")
    guard let data = try keychainManager.retrieve(forKey: key) else {
      return nil
    }
    return String(data: data, encoding: .utf8)
  }
  // MARK: - Device UUID

  public func storeDeviceUuid(userDid: String, uuid: String) throws {
    let normalized = userDid.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let key = "\(Self.deviceUuidKeyPrefix)\(normalized).\(MLSStoragePaths.cleanSuffix)"
    let data = Data(uuid.utf8)
    logger.debug("Storing device UUID for user: \(normalized.prefix(20))...")
    try keychainManager.store(
      data,
      forKey: key,
      accessible: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    )
    logger.info("Stored device UUID for user: \(normalized.prefix(20))...")
  }

  public func getDeviceUuid(userDid: String) throws -> String? {
    let normalized = userDid.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let key = "\(Self.deviceUuidKeyPrefix)\(normalized).\(MLSStoragePaths.cleanSuffix)"
    logger.debug("Retrieving device UUID for user: \(normalized.prefix(20))...")
    guard let data = try keychainManager.retrieve(forKey: key) else {
      return nil
    }
    return String(data: data, encoding: .utf8)
  }
  // MARK: - Credential State

  public func hasCredentials(userDid: String) throws -> Bool {
    // A user "has credentials" if they have a signing key stored.
    // This is the minimum requirement for MLS operations.
    let signingKey = try getSigningKey(userDid: userDid)
    return signingKey != nil
  }

  public func clearAll(userDid: String) throws {
    logger.info("Clearing all credentials for user: \(userDid.prefix(20))...")

    // Delete signing key
    do {
      try deleteSigningKey(userDid: userDid)
    } catch {
      logger.warning("Failed to delete signing key during clearAll: \(error.localizedDescription)")
    }

    // Delete MLS DID
    let normalized = userDid.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let mlsDidKey = "\(Self.mlsDidKeyPrefix)\(normalized).\(MLSStoragePaths.cleanSuffix)"
    do {
      try keychainManager.delete(forKey: mlsDidKey)
    } catch {
      logger.warning("Failed to delete MLS DID during clearAll: \(error.localizedDescription)")
    }

    // Delete device UUID
    let deviceUuidKey = "\(Self.deviceUuidKeyPrefix)\(normalized).\(MLSStoragePaths.cleanSuffix)"
    do {
      try keychainManager.delete(forKey: deviceUuidKey)
    } catch {
      logger.warning("Failed to delete device UUID during clearAll: \(error.localizedDescription)")
    }
    logger.info("Cleared all credentials for user: \(userDid.prefix(20))...")
  }

  // MARK: - Authorized Device Keys

  public func getAuthorizedDeviceKeys(userDid: String) throws -> [Data]? {
    logger.debug("Resolving authorized device keys for user: \(userDid.prefix(20))...")
    return authorizedDeviceKeyResolver?(userDid)
  }
}
