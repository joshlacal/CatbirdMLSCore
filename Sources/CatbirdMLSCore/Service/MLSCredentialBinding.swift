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
//  The Rust check has three steps per fetched key package:
//    1. the DS-labeled DID is one of the DIDs the caller requested;
//    2. the package's leaf credential decodes structurally
//       (`extract_key_package_identity`: TLS-decode `KeyPackageIn`, read the
//       BasicCredential identity as UTF-8 — the identity is the bare DID or
//       `did:...#device-id`);
//    3. the credential's DID root equals the labeled DID.
//
//  From Swift, only step 1 is currently checkable: the UniFFI surface
//  (`Sources/CatbirdMLS/CatbirdMLS.swift`) exposes no decoder for a
//  standalone serialized KeyPackage's credential (`computeKeyPackageHash`
//  hashes opaquely; `debugGroupMembers` only inspects already-joined group
//  members), and there is no maintained Swift MLS TLS codec to decode the
//  bytes locally — hand-rolling one would be an unmaintained fork of the
//  wire format.
//
//  **FFI helper needed for steps 2–3**: export catbird-mls's
//  `orchestrator::credential_binding::extract_key_package_identity` through
//  UniFFI (e.g. a module-level `extractKeyPackageIdentity(keyPackageData:
//  Data) throws -> String`). Once exposed, plug it into the marked seam in
//  `MLSAPIClient.getKeyPackages` and compare via
//  `MLSCredentialBinding.checkIdentityClaim`.
//

import Foundation

/// Pure helpers for binding MLS credentials / DS labels to ATProto DIDs.
/// Function-for-function twin of the Rust `credential_binding` module so the
/// two implementations cannot drift on root extraction or comparison rules.
public enum MLSCredentialBinding {

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
}
