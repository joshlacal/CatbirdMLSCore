//
//  MLSCredentialBinding.swift
//  CatbirdMLSCore
//
//  N44e — iOS twin of the Rust orchestrator's fetch-time credential-binding
//  check (ADR-009 D3, stage-1 warn-and-allow), mirroring
//  `catbird-mls/src/orchestrator/credential_binding.rs`. iOS deliberately
//  bypasses the Rust orchestrator (it drives `MlsContext` directly with its
//  own Swift sync/recovery loop), so the verify-on-fetch check needs this
//  Swift twin.
//
//  ## Verification depth (honest scope)
//
//  The Rust FFI classifier now checks the DS-labeled DID against the serialized
//  KeyPackage's leaf credential identity and extracts the leaf signature key.
//  Full signing-key authorization still requires passing a DID-resolved set of
//  valid MLS signing keys into `classifyKeyPackageBinding`.
//

import Foundation
import CatbirdMLS

/// Pure helpers for binding MLS credentials / DS labels to ATProto DIDs.
/// Function-for-function twin of the Rust `credential_binding` module so the
/// two implementations cannot drift on root extraction or comparison rules.
public enum MLSCredentialBinding {

  public enum KeyPackageBindingStatus: Equatable, Sendable {
    case verified
    case identityMismatch
    case signingKeyMismatch
    case signingKeyUnavailable
    case unverifiable
  }

  public struct KeyPackageBindingClassification: Equatable, Sendable {
    public let status: KeyPackageBindingStatus
    public let identityMatches: Bool
    public let signingKeyMatches: Bool?
    public let expectedRootDID: String
    public let claimedIdentity: String?
    public let claimedRootDID: String?
    public let signaturePublicKey: Data?
    public let signatureAlgorithm: String?
    public let reason: String?
  }

  /// Outcome of an identity-claim consistency check (twin of the Rust
  /// `CredentialVerification` enum, structural subset).
  public enum Verification: Equatable, Sendable {
    /// The claimed identity's DID root matches the expected DID's root.
    case verified
    /// The roots differ — the identity does not belong to the expected DID.
    case didMismatch(expectedDID: String, claimedIdentity: String, claimedRootDID: String)
  }

  /// Extract the DID root from a credential identity (ADR-009 D1 part 1,
  /// fragment-aware): `did:plc:alice#device-1` → `did:plc:alice`; a bare DID
  /// is returned unchanged.
  public static func credentialRootDID(_ identity: String) -> String {
    identity.split(separator: "#", maxSplits: 1, omittingEmptySubsequences: false)
      .first.map(String.init) ?? identity
  }

  /// Structural identity-claim consistency check. Compares the DID root of
  /// `claimedIdentity` against the DID root of `expectedDID` (fragment-aware
  /// on both sides), ASCII-case-insensitively — matching the Rust twin's
  /// `eq_ignore_ascii_case` semantics.
  public static func checkIdentityClaim(
    expectedDID: String, claimedIdentity: String
  ) -> Verification {
    let claimedRoot = credentialRootDID(claimedIdentity)
    let expectedRoot = credentialRootDID(expectedDID)
    if !claimedRoot.isEmpty, claimedRoot.lowercased() == expectedRoot.lowercased() {
      return .verified
    }
    return .didMismatch(
      expectedDID: expectedDID,
      claimedIdentity: claimedIdentity,
      claimedRootDID: claimedRoot
    )
  }

  public static func classifyKeyPackageBinding(
    expectedDID: String,
    keyPackageData: Data,
    authorizedSignatureKeys: [Data]?
  ) -> KeyPackageBindingClassification {
    let ffi = mlsClassifyKeyPackageBinding(
      expectedDid: expectedDID,
      keyPackageBytes: keyPackageData,
      authorizedSignatureKeys: authorizedSignatureKeys
    )
    return KeyPackageBindingClassification(
      status: KeyPackageBindingStatus(ffi.status),
      identityMatches: ffi.identityMatches,
      signingKeyMatches: ffi.signingKeyMatches,
      expectedRootDID: ffi.expectedRootDid,
      claimedIdentity: ffi.claimedIdentity,
      claimedRootDID: ffi.claimedRootDid,
      signaturePublicKey: ffi.signaturePublicKey,
      signatureAlgorithm: ffi.signatureAlgorithm,
      reason: ffi.reason
    )
  }
}

private extension MLSCredentialBinding.KeyPackageBindingStatus {
  init(_ ffi: FfiKeyPackageBindingStatus) {
    switch ffi {
    case .verified:
      self = .verified
    case .identityMismatch:
      self = .identityMismatch
    case .signingKeyMismatch:
      self = .signingKeyMismatch
    case .signingKeyUnavailable:
      self = .signingKeyUnavailable
    case .unverifiable:
      self = .unverifiable
    }
  }
}
