//
//  MLSDecryptionReceiptModel.swift
//  CatbirdMLSCore
//
//  Cross-process decryption deduplication ledger.
//
//  CRITICAL: This table prevents double-decryption between NSE and main app.
//  When a message arrives via both APNs and SSE, only one process should
//  decrypt it. The other process should wait for completion and use the
//  cached plaintext.
//
//  Created: 2026-01-03
//

import Foundation
import GRDB

// MARK: - Decryption Receipt State

/// State of a decryption operation in the cross-process ledger
public enum MLSDecryptionReceiptState: String, Codable, Sendable {
  /// Decryption is currently in progress
  case inProgress = "in_progress"
  /// Decryption completed successfully
  case done = "done"
  /// Decryption failed but may be retried
  case failedRetryable = "failed_retryable"
  /// Decryption failed permanently (corrupt ciphertext, expired keys, etc.)
  case failedPermanent = "failed_permanent"
}

// MARK: - Decryption Receipt Model

/// GRDB record for tracking decryption operations across processes.
///
/// This provides atomic check-and-claim semantics to prevent double-decrypt
/// between NSE and main app. Before attempting MLS decryption, both processes
/// check this ledger:
/// - If `in_progress`: Wait for the other process to complete
/// - If `done`: Skip decryption and use cached plaintext
/// - If no record: Claim the decryption and proceed
///
/// The primary key uses a ciphertext-derived key that is stable across
/// delivery channels (APNs and SSE).
public struct MLSDecryptionReceiptModel: Codable, FetchableRecord, PersistableRecord, Sendable {
  
  // MARK: - Primary Key
  
  /// Unique identifier for the ciphertext being decrypted.
  /// Format: "{groupIdHex}:{epoch}:{wireSequence}" or SHA256 hash prefix.
  public let ciphertextKey: String
  
  /// User DID for multi-user isolation
  public let currentUserDID: String
  
  // MARK: - Message Identification (for cache lookup)
  
  /// Application message ID (used to retrieve cached plaintext after completion)
  public let messageID: String
  
  /// MLS group ID (raw bytes)
  public let groupID: Data
  
  /// Conversation ID for association
  public let conversationID: String
  
  // MARK: - Processing State
  
  /// Current state of the decryption operation
  public var state: MLSDecryptionReceiptState
  
  // MARK: - MLS Coordinates (for diagnostics)
  
  /// MLS epoch number
  public let epoch: Int64?
  
  /// Server-assigned wire sequence number
  public let wireSequence: Int64?
  
  // MARK: - Process Tracking
  
  /// Which process claimed this decryption ("app" or "nse")
  public var processID: String
  
  /// When the decryption operation started
  public var startedAt: Date
  
  /// When the decryption operation completed (nil if in_progress)
  public var completedAt: Date?
  
  // MARK: - Error Tracking
  
  /// Error type if decryption failed
  public var errorType: String?
  
  /// Number of retry attempts
  public var retryCount: Int
  
  // MARK: - Cleanup
  
  /// Expiry time for automatic cleanup (typically 24 hours after creation)
  public let expiresAt: Date
  
  // MARK: - Table Definition
  
  public static let databaseTableName = "MLSDecryptionReceiptModel"
  
  public enum Columns: String, ColumnExpression {
    case ciphertextKey
    case currentUserDID
    case messageID
    case groupID
    case conversationID
    case state
    case epoch
    case wireSequence
    case processID
    case startedAt
    case completedAt
    case errorType
    case retryCount
    case expiresAt
  }
  
  // MARK: - Initialization
  
  public init(
    ciphertextKey: String,
    currentUserDID: String,
    messageID: String,
    groupID: Data,
    conversationID: String,
    state: MLSDecryptionReceiptState = .inProgress,
    epoch: Int64? = nil,
    wireSequence: Int64? = nil,
    processID: String,
    startedAt: Date = Date(),
    completedAt: Date? = nil,
    errorType: String? = nil,
    retryCount: Int = 0,
    expiresAt: Date? = nil
  ) {
    self.ciphertextKey = ciphertextKey
    self.currentUserDID = currentUserDID
    self.messageID = messageID
    self.groupID = groupID
    self.conversationID = conversationID
    self.state = state
    self.epoch = epoch
    self.wireSequence = wireSequence
    self.processID = processID
    self.startedAt = startedAt
    self.completedAt = completedAt
    self.errorType = errorType
    self.retryCount = retryCount
    // Default expiry: 24 hours from creation
    self.expiresAt = expiresAt ?? Date().addingTimeInterval(24 * 60 * 60)
  }
}

// MARK: - Schema Creation

extension MLSDecryptionReceiptModel {
  /// Create the database table
  public static func createTable(in db: Database) throws {
    try db.create(table: databaseTableName, ifNotExists: true) { t in
      // Primary key columns
      t.column(Columns.ciphertextKey.rawValue, .text).notNull()
      t.column(Columns.currentUserDID.rawValue, .text).notNull()
      
      // Message identification
      t.column(Columns.messageID.rawValue, .text).notNull()
      t.column(Columns.groupID.rawValue, .blob).notNull()
      t.column(Columns.conversationID.rawValue, .text).notNull()
      
      // Processing state
      t.column(Columns.state.rawValue, .text).notNull()
      
      // MLS coordinates
      t.column(Columns.epoch.rawValue, .integer)
      t.column(Columns.wireSequence.rawValue, .integer)
      
      // Process tracking
      t.column(Columns.processID.rawValue, .text).notNull()
      t.column(Columns.startedAt.rawValue, .datetime).notNull()
      t.column(Columns.completedAt.rawValue, .datetime)
      
      // Error tracking
      t.column(Columns.errorType.rawValue, .text)
      t.column(Columns.retryCount.rawValue, .integer).notNull().defaults(to: 0)
      
      // Cleanup
      t.column(Columns.expiresAt.rawValue, .datetime).notNull()
      
      // Composite primary key
      t.primaryKey([Columns.ciphertextKey.rawValue, Columns.currentUserDID.rawValue])
    }
    
    // Index for cleanup job (find expired receipts)
    try db.create(
      index: "idx_\(databaseTableName)_expires",
      on: databaseTableName,
      columns: [Columns.expiresAt.rawValue],
      ifNotExists: true
    )
    
    // Index for finding in-progress by group (for abort on account switch)
    try db.create(
      index: "idx_\(databaseTableName)_group_state",
      on: databaseTableName,
      columns: [
        Columns.currentUserDID.rawValue,
        Columns.groupID.rawValue,
        Columns.state.rawValue
      ],
      ifNotExists: true
    )
  }
}

// MARK: - Query Helpers

extension MLSDecryptionReceiptModel {
  /// Filter receipts for a specific user
  static func forUser(_ userDID: String) -> QueryInterfaceRequest<MLSDecryptionReceiptModel> {
    filter(Columns.currentUserDID == userDID)
  }
  
  /// Find expired receipts for cleanup
  static func expired(before date: Date = Date()) -> QueryInterfaceRequest<MLSDecryptionReceiptModel> {
    filter(Columns.expiresAt < date)
  }
  
  /// Find stale in-progress receipts (older than timeout, likely crashed process)
  static func staleInProgress(
    olderThan seconds: TimeInterval = 30,
    userDID: String? = nil
  ) -> QueryInterfaceRequest<MLSDecryptionReceiptModel> {
    let cutoff = Date().addingTimeInterval(-seconds)
    var request = filter(Columns.state == MLSDecryptionReceiptState.inProgress.rawValue)
      .filter(Columns.startedAt < cutoff)
    
    if let userDID = userDID {
      request = request.filter(Columns.currentUserDID == userDID)
    }
    
    return request
  }
}

// MARK: - Ciphertext Key Generation

extension MLSDecryptionReceiptModel {
  /// Generate a stable ciphertext key from message metadata.
  ///
  /// This key must be:
  /// 1. Stable across delivery channels (APNs and SSE deliver the same key)
  /// 2. Unique per ciphertext (no two different ciphertexts share a key)
  ///
  /// Format: "{groupIdHex}:{epoch}:{wireSequence}"
  ///
  /// - Parameters:
  ///   - groupID: MLS group ID
  ///   - epoch: MLS epoch number
  ///   - wireSequence: Server-assigned sequence number
  /// - Returns: Stable ciphertext key string
  public static func generateCiphertextKey(
    groupID: Data,
    epoch: Int64,
    wireSequence: Int64
  ) -> String {
    let groupIdHex = groupID.prefix(16).hexEncodedString()
    return "\(groupIdHex):\(epoch):\(wireSequence)"
  }
  
  /// Generate a ciphertext key from hash when sequence is unavailable.
  ///
  /// Fallback for cases where wire sequence isn't available (e.g., local sends
  /// before server echo).
  ///
  /// - Parameters:
  ///   - ciphertext: Raw ciphertext bytes
  /// - Returns: Hash-based ciphertext key
  public static func generateCiphertextKeyFromHash(ciphertext: Data) -> String {
    // Use first 32 chars of hex-encoded SHA256
    let hash = ciphertext.sha256()
    return hash.prefix(16).hexEncodedString()
  }
}
