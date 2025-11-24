//
//  MLSInviteModel.swift
//  Catbird
//
//  MLS invite model for secure group invitations
//

import Foundation
import GRDB

/// MLS invite model for managing secure group invitations
///
/// Invites allow users to join MLS groups securely. Each invite contains:
/// - A pre-shared key (PSK) stored separately in Keychain
/// - Optional expiration and usage limits
/// - Tracking of who created the invite and for which conversation
public struct MLSInviteModel: Codable, Sendable, Hashable, Identifiable {
    let inviteID: String
    let conversationID: String
    let createdBy: String
    let targetDID: String?
    let createdAt: Date
    let expiresAt: Date?
    let maxUses: Int?
    let usesCount: Int
    let revoked: Bool

    public var id: String { inviteID }

    // MARK: - Initialization

    public init(
        inviteID: String,
        conversationID: String,
        createdBy: String,
        targetDID: String? = nil,
        createdAt: Date = Date(),
        expiresAt: Date? = nil,
        maxUses: Int? = nil,
        usesCount: Int = 0,
        revoked: Bool = false
    ) {
        self.inviteID = inviteID
        self.conversationID = conversationID
        self.createdBy = createdBy
        self.targetDID = targetDID
        self.createdAt = createdAt
        self.expiresAt = expiresAt
        self.maxUses = maxUses
        self.usesCount = usesCount
        self.revoked = revoked
    }

    // MARK: - Computed Properties

    /// Check if the invite has expired
    public var isExpired: Bool {
        guard let expiryDate = expiresAt else { return false }
        return Date() > expiryDate
    }

    /// Check if the invite has reached its maximum uses
    public var isMaxedOut: Bool {
        guard let max = maxUses else { return false }
        return usesCount >= max
    }

    /// Check if the invite is valid (not expired, not maxed out, not revoked)
    public var isValid: Bool {
        return !isExpired && !isMaxedOut && !revoked
    }

    /// Retrieve the PSK from Keychain
    /// - Returns: Pre-shared key data, or nil if not found
    public var psk: Data? {
        return MLSKeychainManager.shared.retrieveInvitePSK(for: inviteID)
    }

    // MARK: - URL Generation

    /// Generate an invite URL for sharing
    ///
    /// Format: `catbird://invite?c={conversationID}&i={inviteID}&psk={base64url(psk)}`
    ///
    /// - Returns: Invite URL, or nil if PSK cannot be retrieved
    public func toInviteURL() -> URL? {
        guard let pskData = psk else { return nil }

        let pskBase64url = pskData.base64urlEncodedString()

        var components = URLComponents()
        components.scheme = "catbird"
        components.host = "invite"
        components.queryItems = [
            URLQueryItem(name: "c", value: conversationID),
            URLQueryItem(name: "i", value: inviteID),
            URLQueryItem(name: "psk", value: pskBase64url)
        ]

        return components.url
    }

    /// Parse an invite URL
    ///
    /// - Parameter url: Invite URL in format `catbird://invite?c={conversationID}&i={inviteID}&psk={base64url(psk)}`
    /// - Returns: Tuple of (conversationID, inviteID, psk), or nil if parsing fails
    public static func fromInviteURL(_ url: URL) -> (conversationID: String, inviteID: String, psk: Data)? {
        guard url.scheme == "catbird",
              url.host == "invite",
              let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
              let queryItems = components.queryItems else {
            return nil
        }

        var conversationID: String?
        var inviteID: String?
        var psk: Data?

        for item in queryItems {
            switch item.name {
            case "c":
                conversationID = item.value
            case "i":
                inviteID = item.value
            case "psk":
                if let value = item.value {
                    psk = Data(base64urlEncoded: value)
                }
            default:
                break
            }
        }

        guard let convID = conversationID,
              let invID = inviteID,
              let pskData = psk else {
            return nil
        }

        return (conversationID: convID, inviteID: invID, psk: pskData)
    }

    // MARK: - Update Methods

    /// Create copy with incremented use count
    func withIncrementedUses() -> MLSInviteModel {
        MLSInviteModel(
            inviteID: inviteID,
            conversationID: conversationID,
            createdBy: createdBy,
            targetDID: targetDID,
            createdAt: createdAt,
            expiresAt: expiresAt,
            maxUses: maxUses,
            usesCount: usesCount + 1,
            revoked: revoked
        )
    }

    /// Create copy marked as revoked
    func withRevoked() -> MLSInviteModel {
        MLSInviteModel(
            inviteID: inviteID,
            conversationID: conversationID,
            createdBy: createdBy,
            targetDID: targetDID,
            createdAt: createdAt,
            expiresAt: expiresAt,
            maxUses: maxUses,
            usesCount: usesCount,
            revoked: true
        )
    }
}

// MARK: - GRDB Conformance
extension MLSInviteModel: FetchableRecord, PersistableRecord {
    public static let databaseTableName = "MLSInviteModel"

    public enum Columns {
        static let inviteID = Column("inviteID")
        static let conversationID = Column("conversationID")
        static let createdBy = Column("createdBy")
        static let targetDID = Column("targetDID")
        static let createdAt = Column("createdAt")
        static let expiresAt = Column("expiresAt")
        static let maxUses = Column("maxUses")
        static let usesCount = Column("usesCount")
        static let revoked = Column("revoked")
    }
}
