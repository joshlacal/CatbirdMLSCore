//
//  MLSExternalJoinAuthorizerBridge.swift
//  CatbirdMLSCore
//
//  Bridge for authorizing external join requests in MLS.
//

import Foundation
import MLSFFI
import OSLog

/// Bridge implementation for external join authorization
/// Allows the Swift application layer to validate external join requests
public final class MLSExternalJoinAuthorizerBridge: ExternalJoinAuthorizer {
    
    private let logger = Logger(subsystem: "blue.catbird.mls", category: "ExternalJoinAuthorizer")
    
    public init() {}
    
    public func authorizeExternalJoin(
        groupId: Data,
        requesterCredential: CredentialData,
        requesterSignatureKey: Data
    ) -> Bool {
        let groupIdHex = groupId.hexEncodedString()
        
        // Extract DID from credential
        guard let didString = String(data: requesterCredential.identity, encoding: .utf8) else {
            logger.error("❌ [AUTH] Failed to decode credential identity for group \(groupIdHex)")
            return false
        }
        
        logger.info("🔒 [AUTH] Authorizing external join for \(didString) in group \(groupIdHex)")
        
        // Policy:
        // 1. Must be a valid DID
        // 2. Must not be blocked (TODO: Check block list)
        // 3. For now, we allow all valid DIDs to join via external commit if they have the invite link
        //    In the future, this could check against a pending invite list or server-side policy
        
        if !didString.starts(with: "did:") {
            logger.warning("❌ [AUTH] Rejected invalid DID format: \(didString)")
            return false
        }
        
        // TODO: Add block list check here
        
        logger.info("✅ [AUTH] Authorized external join for \(didString)")
        return true
    }
}
