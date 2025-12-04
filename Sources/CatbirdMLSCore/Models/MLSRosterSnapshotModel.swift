//
//  MLSRosterSnapshotModel.swift
//  CatbirdMLSCore
//
//  Roster snapshot model for tracking group membership at each epoch.
//  Used for membership change detection and tree hash pinning.
//

import Foundation
import GRDB

/// Snapshot of group membership at a specific epoch
///
/// This model captures the complete roster state at an epoch transition,
/// enabling:
/// - Detection of membership changes (adds/removes)
/// - Tree hash pinning for state divergence detection
/// - Audit trail for group membership history
public struct MLSRosterSnapshotModel: Codable, Sendable, Hashable, Identifiable {
    /// Unique identifier for the snapshot
    public let snapshotID: String
    
    /// Conversation this snapshot belongs to
    public let conversationID: String
    
    /// Epoch number when this snapshot was taken
    public let epoch: Int64
    
    /// List of member DIDs in the group at this epoch
    public let memberDIDs: [String]
    
    /// Tree hash for this epoch (optional, used for state verification)
    public let treeHash: Data?
    
    /// Timestamp when this snapshot was created
    public let timestamp: Date
    
    /// Previous epoch's snapshot ID for chain verification
    public let previousSnapshotID: String?
    
    public var id: String { snapshotID }
    
    // MARK: - Initialization
    
    public init(
        snapshotID: String = UUID().uuidString,
        conversationID: String,
        epoch: Int64,
        memberDIDs: [String],
        treeHash: Data? = nil,
        timestamp: Date = Date(),
        previousSnapshotID: String? = nil
    ) {
        self.snapshotID = snapshotID
        self.conversationID = conversationID
        self.epoch = epoch
        self.memberDIDs = memberDIDs
        self.treeHash = treeHash
        self.timestamp = timestamp
        self.previousSnapshotID = previousSnapshotID
    }
    
    // MARK: - Membership Diff
    
    /// Calculate the difference between this snapshot and another
    /// - Parameter other: The snapshot to compare against
    /// - Returns: The membership diff, or nil if snapshots are from different conversations
    public func diff(from other: MLSRosterSnapshotModel) -> MLSMembershipDiff? {
        guard conversationID == other.conversationID else { return nil }
        
        let currentSet = Set(memberDIDs)
        let previousSet = Set(other.memberDIDs)
        
        let added = currentSet.subtracting(previousSet)
        let removed = previousSet.subtracting(currentSet)
        
        return MLSMembershipDiff(
            conversationID: conversationID,
            fromEpoch: other.epoch,
            toEpoch: epoch,
            addedMembers: Array(added),
            removedMembers: Array(removed),
            timestamp: timestamp
        )
    }
    
    /// Check if tree hash matches expected value
    /// - Parameter expectedHash: The expected tree hash
    /// - Returns: True if hashes match, false if mismatch or no hash available
    public func verifyTreeHash(_ expectedHash: Data) -> Bool {
        guard let hash = treeHash else { return false }
        return hash == expectedHash
    }
}

// MARK: - Membership Diff

/// Represents changes in group membership between two epochs
public struct MLSMembershipDiff: Codable, Sendable, Hashable {
    public let conversationID: String
    public let fromEpoch: Int64
    public let toEpoch: Int64
    public let addedMembers: [String]
    public let removedMembers: [String]
    public let timestamp: Date
    
    /// True if there are any membership changes
    public var hasChanges: Bool {
        !addedMembers.isEmpty || !removedMembers.isEmpty
    }
    
    /// Total number of changes
    public var changeCount: Int {
        addedMembers.count + removedMembers.count
    }
    
    /// Human-readable summary of changes
    public var summary: String {
        var parts: [String] = []
        
        if !addedMembers.isEmpty {
            let count = addedMembers.count
            parts.append("\(count) member\(count == 1 ? "" : "s") joined")
        }
        
        if !removedMembers.isEmpty {
            let count = removedMembers.count
            parts.append("\(count) member\(count == 1 ? "" : "s") left")
        }
        
        return parts.isEmpty ? "No membership changes" : parts.joined(separator: ", ")
    }
}

// MARK: - GRDB Conformance

extension MLSRosterSnapshotModel: FetchableRecord, PersistableRecord {
    public static let databaseTableName = "MLSRosterSnapshotModel"
    
    public enum Columns {
        public static let snapshotID = Column("snapshotID")
        public static let conversationID = Column("conversationID")
        public static let epoch = Column("epoch")
        public static let memberDIDs = Column("memberDIDs")
        public static let treeHash = Column("treeHash")
        public static let timestamp = Column("timestamp")
        public static let previousSnapshotID = Column("previousSnapshotID")
    }
    
    // Custom encoding for memberDIDs array
    public func encode(to container: inout PersistenceContainer) throws {
        container[Columns.snapshotID] = snapshotID
        container[Columns.conversationID] = conversationID
        container[Columns.epoch] = epoch
        container[Columns.memberDIDs] = try JSONEncoder().encode(memberDIDs)
        container[Columns.treeHash] = treeHash
        container[Columns.timestamp] = timestamp
        container[Columns.previousSnapshotID] = previousSnapshotID
    }
    
    public init(row: Row) throws {
        snapshotID = row[Columns.snapshotID]
        conversationID = row[Columns.conversationID]
        epoch = row[Columns.epoch]
        
        let memberDIDsData: Data = row[Columns.memberDIDs]
        memberDIDs = try JSONDecoder().decode([String].self, from: memberDIDsData)
        
        treeHash = row[Columns.treeHash]
        timestamp = row[Columns.timestamp]
        previousSnapshotID = row[Columns.previousSnapshotID]
    }
}
