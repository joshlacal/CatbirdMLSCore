//
//  MLSPolicyModel.swift
//  Catbird
//
//  MLS policy model for conversation security policies
//

import Foundation
import GRDB

/// MLS policy model for managing conversation security policies
///
/// Policies control how MLS groups operate, including:
/// - External commit restrictions
/// - Invite requirements for joining
/// - Rejoin policies and windows
/// - Admin protection rules
public struct MLSPolicyModel: Codable, Sendable, Hashable, Identifiable {
    public let policyID: String
    public let conversationID: String
    public let allowExternalCommits: Bool
    public let requireInviteForJoin: Bool
    public let allowRejoin: Bool
    public let rejoinWindowDays: Int?
    public let preventRemovingLastAdmin: Bool
    public let updatedAt: Date

    public var id: String { policyID }

    // MARK: - Initialization

    public init(
        policyID: String,
        conversationID: String,
        allowExternalCommits: Bool = false,
        requireInviteForJoin: Bool = true,
        allowRejoin: Bool = true,
        rejoinWindowDays: Int? = 30,
        preventRemovingLastAdmin: Bool = true,
        updatedAt: Date = Date()
    ) {
        self.policyID = policyID
        self.conversationID = conversationID
        self.allowExternalCommits = allowExternalCommits
        self.requireInviteForJoin = requireInviteForJoin
        self.allowRejoin = allowRejoin
        self.rejoinWindowDays = rejoinWindowDays
        self.preventRemovingLastAdmin = preventRemovingLastAdmin
        self.updatedAt = updatedAt
    }

    // MARK: - Presets

    /// Default secure policy (recommended)
    /// - External commits: disabled
    /// - Invite required: yes
    /// - Rejoin: allowed within 30 days
    /// - Last admin protection: enabled
    public static func defaultPolicy(conversationID: String) -> MLSPolicyModel {
        MLSPolicyModel(
            policyID: UUID().uuidString,
            conversationID: conversationID,
            allowExternalCommits: false,
            requireInviteForJoin: true,
            allowRejoin: true,
            rejoinWindowDays: 30,
            preventRemovingLastAdmin: true
        )
    }

    /// Strict security policy
    /// - External commits: disabled
    /// - Invite required: yes
    /// - Rejoin: disabled
    /// - Last admin protection: enabled
    public static func strictPolicy(conversationID: String) -> MLSPolicyModel {
        MLSPolicyModel(
            policyID: UUID().uuidString,
            conversationID: conversationID,
            allowExternalCommits: false,
            requireInviteForJoin: true,
            allowRejoin: false,
            rejoinWindowDays: nil,
            preventRemovingLastAdmin: true
        )
    }

    /// Open policy (less secure, for public groups)
    /// - External commits: enabled
    /// - Invite required: no
    /// - Rejoin: allowed indefinitely
    /// - Last admin protection: enabled
    public static func openPolicy(conversationID: String) -> MLSPolicyModel {
        MLSPolicyModel(
            policyID: UUID().uuidString,
            conversationID: conversationID,
            allowExternalCommits: true,
            requireInviteForJoin: false,
            allowRejoin: true,
            rejoinWindowDays: nil,
            preventRemovingLastAdmin: true
        )
    }

    // MARK: - Computed Properties

    /// Check if rejoin is allowed within the window for a given removal date
    /// - Parameter removedAt: Date when the member was removed
    /// - Returns: True if rejoin is allowed based on policy
    public func canRejoin(removedAt: Date) -> Bool {
        guard allowRejoin else { return false }

        // No window restriction means rejoin always allowed
        guard let windowDays = rejoinWindowDays else { return true }

        let daysSinceRemoval = Calendar.current.dateComponents(
            [.day],
            from: removedAt,
            to: Date()
        ).day ?? 0

        return daysSinceRemoval <= windowDays
    }

    /// Calculate the rejoin expiration date for a member removed at a given date
    /// - Parameter removedAt: Date when the member was removed
    /// - Returns: Expiration date for rejoin eligibility, or nil if no expiration
    public func rejoinExpirationDate(removedAt: Date) -> Date? {
        guard allowRejoin else { return removedAt }

        guard let windowDays = rejoinWindowDays else { return nil }

        return Calendar.current.date(byAdding: .day, value: windowDays, to: removedAt)
    }

    // MARK: - Update Methods

    /// Create copy with updated policy settings
    public func withUpdatedSettings(
        allowExternalCommits: Bool? = nil,
        requireInviteForJoin: Bool? = nil,
        allowRejoin: Bool? = nil,
        rejoinWindowDays: Int?? = nil,
        preventRemovingLastAdmin: Bool? = nil
    ) -> MLSPolicyModel {
        MLSPolicyModel(
            policyID: policyID,
            conversationID: conversationID,
            allowExternalCommits: allowExternalCommits ?? self.allowExternalCommits,
            requireInviteForJoin: requireInviteForJoin ?? self.requireInviteForJoin,
            allowRejoin: allowRejoin ?? self.allowRejoin,
            rejoinWindowDays: rejoinWindowDays ?? self.rejoinWindowDays,
            preventRemovingLastAdmin: preventRemovingLastAdmin ?? self.preventRemovingLastAdmin,
            updatedAt: Date()
        )
    }
}

// MARK: - GRDB Conformance
extension MLSPolicyModel: FetchableRecord, PersistableRecord {
    public static let databaseTableName = "MLSPolicyModel"

    public enum Columns {
        static let policyID = Column("policyID")
        static let conversationID = Column("conversationID")
        static let allowExternalCommits = Column("allowExternalCommits")
        static let requireInviteForJoin = Column("requireInviteForJoin")
        static let allowRejoin = Column("allowRejoin")
        static let rejoinWindowDays = Column("rejoinWindowDays")
        static let preventRemovingLastAdmin = Column("preventRemovingLastAdmin")
        static let updatedAt = Column("updatedAt")
    }
}
