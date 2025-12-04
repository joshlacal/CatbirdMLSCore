//
//  MLSTreeHashPinModel.swift
//  CatbirdMLSCore
//
//  Tree hash pinning model for MLS state verification.
//  Used to detect state divergence between clients.
//

import Foundation
import GRDB

/// Pinned tree hash for an MLS group at a specific epoch
///
/// Tree hash pinning enables detection of group state divergence:
/// - If a client receives a tree hash that doesn't match the pinned value,
///   it indicates the group state has diverged (possibly due to server manipulation)
/// - Clients can detect and alert on potential security issues
/// - Provides audit trail for forensic analysis
public struct MLSTreeHashPinModel: Codable, Sendable, Hashable, Identifiable {
    /// Unique identifier for the pin
    public let pinID: String
    
    /// Conversation this pin belongs to
    public let conversationID: String
    
    /// Epoch number for this tree hash
    public let epoch: Int64
    
    /// The tree hash value at this epoch
    public let treeHash: Data
    
    /// When this hash was pinned
    public let pinnedAt: Date
    
    /// Source of this pin (e.g., "local_commit", "received_commit", "external_commit")
    public let source: String
    
    /// Whether this pin has been verified by another client
    public let verified: Bool
    
    public var id: String { pinID }
    
    // MARK: - Initialization
    
    public init(
        pinID: String = UUID().uuidString,
        conversationID: String,
        epoch: Int64,
        treeHash: Data,
        pinnedAt: Date = Date(),
        source: String = "local_commit",
        verified: Bool = false
    ) {
        self.pinID = pinID
        self.conversationID = conversationID
        self.epoch = epoch
        self.treeHash = treeHash
        self.pinnedAt = pinnedAt
        self.source = source
        self.verified = verified
    }
    
    // MARK: - Verification
    
    /// Check if a received tree hash matches this pinned value
    /// - Parameter receivedHash: The tree hash received from the server
    /// - Returns: True if hashes match, false if divergence detected
    public func matches(_ receivedHash: Data) -> Bool {
        treeHash == receivedHash
    }
    
    /// Create a new pin marked as verified
    public func asVerified() -> MLSTreeHashPinModel {
        MLSTreeHashPinModel(
            pinID: pinID,
            conversationID: conversationID,
            epoch: epoch,
            treeHash: treeHash,
            pinnedAt: pinnedAt,
            source: source,
            verified: true
        )
    }
    
    // MARK: - Hex Representation
    
    /// Tree hash as hex string for display/logging
    public var treeHashHex: String {
        treeHash.map { String(format: "%02x", $0) }.joined()
    }
    
    /// Truncated tree hash for logging (first 16 chars)
    public var treeHashShort: String {
        String(treeHashHex.prefix(16)) + "..."
    }
}

// MARK: - Tree Hash Divergence

/// Represents a detected tree hash divergence
public struct MLSTreeHashDivergence: Codable, Sendable {
    public let conversationID: String
    public let epoch: Int64
    public let expectedHash: Data
    public let receivedHash: Data
    public let detectedAt: Date
    public let source: String
    
    public init(
        conversationID: String,
        epoch: Int64,
        expectedHash: Data,
        receivedHash: Data,
        detectedAt: Date = Date(),
        source: String
    ) {
        self.conversationID = conversationID
        self.epoch = epoch
        self.expectedHash = expectedHash
        self.receivedHash = receivedHash
        self.detectedAt = detectedAt
        self.source = source
    }
    
    /// Human-readable description of the divergence
    public var description: String {
        let expected = expectedHash.prefix(8).map { String(format: "%02x", $0) }.joined()
        let received = receivedHash.prefix(8).map { String(format: "%02x", $0) }.joined()
        return "Tree hash divergence at epoch \(epoch): expected \(expected)..., got \(received)..."
    }
}

// MARK: - GRDB Conformance

extension MLSTreeHashPinModel: FetchableRecord, PersistableRecord {
    public static let databaseTableName = "MLSTreeHashPinModel"
    
    public enum Columns {
        public static let pinID = Column("pinID")
        public static let conversationID = Column("conversationID")
        public static let epoch = Column("epoch")
        public static let treeHash = Column("treeHash")
        public static let pinnedAt = Column("pinnedAt")
        public static let source = Column("source")
        public static let verified = Column("verified")
    }
}
