//
//  MLSReportModel.swift
//  CatbirdMLSCore
//
//  Local cache model for MLS moderation reports
//

import Foundation
import GRDB

/// Persisted representation of an MLS moderation report
public struct MLSReportModel: Codable, Sendable, Hashable, Identifiable {
  public let id: String
  public let conversationID: String
  public let reporterDID: String
  public let reportedDID: String
  public let reason: String
  public let details: String?
  public let status: String
  public let action: String?
  public let resolutionNotes: String?
  public let createdAt: Date
  public let resolvedAt: Date?

  public init(
    id: String,
    conversationID: String,
    reporterDID: String,
    reportedDID: String,
    reason: String,
    details: String? = nil,
    status: String = "pending",
    action: String? = nil,
    resolutionNotes: String? = nil,
    createdAt: Date = Date(),
    resolvedAt: Date? = nil
  ) {
    self.id = id
    self.conversationID = conversationID
    self.reporterDID = reporterDID
    self.reportedDID = reportedDID
    self.reason = reason
    self.details = details
    self.status = status
    self.action = action
    self.resolutionNotes = resolutionNotes
    self.createdAt = createdAt
    self.resolvedAt = resolvedAt
  }
}

// MARK: - GRDB

extension MLSReportModel: FetchableRecord, PersistableRecord {
  public static let databaseTableName = "MLSReportModel"

  public enum Columns {
    public static let id = Column("id")
    public static let conversationID = Column("convo_id")
    public static let reporterDID = Column("reporter_did")
    public static let reportedDID = Column("reported_did")
    public static let reason = Column("reason")
    public static let details = Column("details")
    public static let status = Column("status")
    public static let action = Column("action")
    public static let resolutionNotes = Column("resolution_notes")
    public static let createdAt = Column("created_at")
    public static let resolvedAt = Column("resolved_at")
  }

  enum CodingKeys: String, CodingKey {
    case id
    case conversationID = "convo_id"
    case reporterDID = "reporter_did"
    case reportedDID = "reported_did"
    case reason
    case details
    case status
    case action
    case resolutionNotes = "resolution_notes"
    case createdAt = "created_at"
    case resolvedAt = "resolved_at"
  }
}
