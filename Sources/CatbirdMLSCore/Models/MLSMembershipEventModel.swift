//
//  MLSMembershipEventModel.swift
//  CatbirdMLSCore
//
//  MLS membership event audit log model
//

import Foundation
import GRDB

/// MLS membership event model for tracking member adds/removes
public struct MLSMembershipEventModel: Codable, Sendable, Hashable, Identifiable {
    public let id: String
    public let conversationID: String
    public let currentUserDID: String
    public let memberDID: String
    public let eventType: EventType
    public let timestamp: Date
    public let actorDID: String?
    public let epoch: Int64
    public let metadata: Data?

    public enum EventType: String, Codable, Sendable {
        case joined
        case left
        case removed
        case kicked
        case roleChanged
        case deviceAdded
    }

    public init(
        id: String = UUID().uuidString,
        conversationID: String,
        currentUserDID: String,
        memberDID: String,
        eventType: EventType,
        timestamp: Date = Date(),
        actorDID: String? = nil,
        epoch: Int64,
        metadata: Data? = nil
    ) {
        self.id = id
        self.conversationID = conversationID
        self.currentUserDID = currentUserDID
        self.memberDID = memberDID
        self.eventType = eventType
        self.timestamp = timestamp
        self.actorDID = actorDID
        self.epoch = epoch
        self.metadata = metadata
    }
}

// MARK: - GRDB Conformance

extension MLSMembershipEventModel: FetchableRecord, PersistableRecord {
    public static let databaseTableName = "MLSMembershipEventModel"

    public enum Columns {
        public static let id = Column("id")
        public static let conversationID = Column("conversationID")
        public static let currentUserDID = Column("currentUserDID")
        public static let memberDID = Column("memberDID")
        public static let eventType = Column("eventType")
        public static let timestamp = Column("timestamp")
        public static let actorDID = Column("actorDID")
        public static let epoch = Column("epoch")
        public static let metadata = Column("metadata")
    }

    enum CodingKeys: String, CodingKey {
        case id
        case conversationID
        case currentUserDID
        case memberDID
        case eventType
        case timestamp
        case actorDID
        case epoch
        case metadata
    }
}
