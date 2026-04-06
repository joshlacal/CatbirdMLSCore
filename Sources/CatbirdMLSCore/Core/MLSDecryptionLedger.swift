//
//  MLSDecryptionLedger.swift
//  CatbirdMLSCore
//
//  Experimental cross-process decryption deduplication coordinator.
//
//  NOTE:
//  - This ledger is currently NOT wired into production decryption paths.
//  - Production routing and signaling are handled by MLSNotificationCoordinator.
//  - Keep this implementation for future experimentation only.
//
//  Decision Rules:
//  - If ledger has `done`: Skip decrypt, return cached plaintext
//  - If ledger has `in_progress` (< 30s old): Wait for completion
//  - If ledger has `in_progress` (> 30s old): Take over (stale process)
//  - If ledger has `failed_retryable` (< 3 retries): Retry
//  - If ledger has `failed_permanent`: Skip, return error
//  - If no record: Claim and proceed
//
//  Created: 2026-01-03
//

import Foundation
import GRDB
import OSLog

// MARK: - Decision Result

/// Result of checking the decryption ledger
public enum MLSDecryptionDecision: Sendable {
  /// Skip decryption - message already processed. Retrieve from cache.
  case skip(reason: SkipReason, messageID: String)
  
  /// Wait for another process to complete, then check cache
  case wait(startedAt: Date, processID: String)
  
  /// Proceed with decryption (new or retry)
  case proceed(isRetry: Bool, attemptNumber: Int)
  
  public enum SkipReason: String, Sendable {
    case alreadyDecrypted = "already_decrypted"
    case maxRetriesExceeded = "max_retries_exceeded"
    case permanentFailure = "permanent_failure"
  }
}

// MARK: - Error Classification

/// Classification of decryption errors for state transitions
public enum MLSDecryptionErrorClass: Sendable {
  /// Expected outcomes that indicate success (cache should have data)
  case expectedSuccess  // SecretReuseError, CannotDecryptOwnMessage
  
  /// Transient errors that may resolve on retry
  case transient        // NetworkError, DatabaseBusy, Timeout, StaleState
  
  /// Permanent errors that should not be retried
  case permanent        // InvalidCiphertext, UnknownGroup, KeyPackageExpired
}

// MARK: - Decryption Ledger

/// Coordinates cross-process decryption to prevent double-decrypt.
///
/// Experimental implementation only; production decryption paths currently do not consult
/// this ledger. The ledger uses SQLCipher with immediate transactions for atomicity.
///
/// Thread-safe through actor isolation.
@available(*, deprecated, message: "Experimental-only; not used in production decryption paths.")
public actor MLSDecryptionLedger {
  
  // MARK: - Singleton
  
  public static let shared = MLSDecryptionLedger()
  
  // MARK: - Properties
  
  private nonisolated let logger = Logger(subsystem: "blue.catbird.mls", category: "DecryptionLedger")
  
  /// Stale threshold: if in_progress older than this, assume process crashed
  private nonisolated static let staleThresholdSeconds: TimeInterval = 30.0
  
  /// Maximum retry count before marking permanent failure
  private nonisolated static let maxRetryCount = 3
  
  /// Polling interval when waiting for in-progress to complete
  private let pollIntervalBase: TimeInterval = 0.05  // 50ms
  private let pollIntervalMax: TimeInterval = 2.0    // 2s
  private let waitTimeoutTotal: TimeInterval = 5.0   // 5s max wait
  
  /// Process identifier for this process
  private nonisolated let processID: String
  
  // MARK: - Initialization
  
  private init() {
    // Determine process type from bundle
    if Bundle.main.bundleURL.pathExtension == "appex" {
      self.processID = "nse"
    } else {
      self.processID = "app"
    }
    logger.info("MLSDecryptionLedger initialized (process: \(self.processID))")
  }
  
  // MARK: - Public API
  
  /// Check the ledger and decide whether to proceed with decryption.
  ///
  /// This is the primary entry point. Call this BEFORE attempting MLS decryption.
  ///
  /// - Parameters:
  ///   - groupID: MLS group ID
  ///   - epoch: MLS epoch number
  ///   - wireSequence: Server-assigned sequence number
  ///   - messageID: Application message ID (for cache lookup)
  ///   - conversationID: Conversation ID
  ///   - userDID: Current user's DID
  ///   - database: Database for ledger operations
  /// - Returns: Decision on how to proceed
  public func shouldDecrypt(
    groupID: Data,
    epoch: Int64,
    wireSequence: Int64,
    messageID: String,
    conversationID: String,
    userDID: String,
    database: any DatabaseWriter
  ) async throws -> MLSDecryptionDecision {
    let ciphertextKey = MLSDecryptionReceiptModel.generateCiphertextKey(
      groupID: groupID,
      epoch: epoch,
      wireSequence: wireSequence
    )
    
    return try await shouldDecryptInternal(
      ciphertextKey: ciphertextKey,
      groupID: groupID,
      epoch: epoch,
      wireSequence: wireSequence,
      messageID: messageID,
      conversationID: conversationID,
      userDID: userDID,
      database: database
    )
  }
  
  /// Check the ledger using hash-based key (fallback when sequence unavailable).
  public func shouldDecryptWithHash(
    ciphertext: Data,
    groupID: Data,
    epoch: Int64?,
    messageID: String,
    conversationID: String,
    userDID: String,
    database: any DatabaseWriter
  ) async throws -> MLSDecryptionDecision {
    let ciphertextKey = MLSDecryptionReceiptModel.generateCiphertextKeyFromHash(ciphertext: ciphertext)
    
    return try await shouldDecryptInternal(
      ciphertextKey: ciphertextKey,
      groupID: groupID,
      epoch: epoch,
      wireSequence: nil,
      messageID: messageID,
      conversationID: conversationID,
      userDID: userDID,
      database: database
    )
  }
  
  /// Mark decryption as complete (success or failure).
  ///
  /// Call this after MLS decryption completes, with the result.
  ///
  /// - Parameters:
  ///   - groupID: MLS group ID
  ///   - epoch: MLS epoch number
  ///   - wireSequence: Server-assigned sequence number
  ///   - userDID: Current user's DID
  ///   - success: Whether decryption succeeded
  ///   - errorType: Error type string if failed
  ///   - database: Database for ledger operations
  public func markComplete(
    groupID: Data,
    epoch: Int64,
    wireSequence: Int64,
    userDID: String,
    success: Bool,
    errorType: String? = nil,
    database: any DatabaseWriter
  ) async throws {
    let ciphertextKey = MLSDecryptionReceiptModel.generateCiphertextKey(
      groupID: groupID,
      epoch: epoch,
      wireSequence: wireSequence
    )
    
    try await markCompleteInternal(
      ciphertextKey: ciphertextKey,
      userDID: userDID,
      success: success,
      errorType: errorType,
      database: database
    )
  }
  
  /// Classify an error for state transition purposes.
  public func classifyError(_ error: Error) -> MLSDecryptionErrorClass {
    let errorString = String(describing: error).lowercased()
    
    // Expected success (cache should have data)
    if errorString.contains("secretreuse") ||
       errorString.contains("cannot decrypt own message") ||
       errorString.contains("cannotdecryptownmessage") {
      return .expectedSuccess
    }
    
    // Transient (may resolve on retry)
    if errorString.contains("network") ||
       errorString.contains("timeout") ||
       errorString.contains("busy") ||
       errorString.contains("locked") ||
       errorString.contains("stale") ||
       errorString.contains("reload") {
      return .transient
    }
    
    // Everything else is permanent
    return .permanent
  }
  
  /// Clean up expired receipts
  public func cleanupExpiredReceipts(
    userDID: String,
    database: any DatabaseWriter
  ) async throws {
    let count = try await database.write { db -> Int in
      try MLSDecryptionReceiptModel
        .filter(MLSDecryptionReceiptModel.Columns.currentUserDID == userDID)
        .filter(MLSDecryptionReceiptModel.Columns.expiresAt < Date())
        .deleteAll(db)
    }
    
    if count > 0 {
      logger.info("🧹 Cleaned up \(count) expired decryption receipts")
    }
  }
  
  // MARK: - Internal Implementation
  
  private func shouldDecryptInternal(
    ciphertextKey: String,
    groupID: Data,
    epoch: Int64?,
    wireSequence: Int64?,
    messageID: String,
    conversationID: String,
    userDID: String,
    database: any DatabaseWriter
  ) async throws -> MLSDecryptionDecision {
    // Use IMMEDIATE transaction for atomic check-and-claim
    return try await database.write { [self] db -> MLSDecryptionDecision in
      // Step 1: Check existing receipt
      if let receipt = try MLSDecryptionReceiptModel
        .filter(MLSDecryptionReceiptModel.Columns.ciphertextKey == ciphertextKey)
        .filter(MLSDecryptionReceiptModel.Columns.currentUserDID == userDID)
        .fetchOne(db) {
          return try handleExistingReceipt(receipt, in: db)
      }
      
      // Step 2: No receipt exists - claim this ciphertext
      let newReceipt = MLSDecryptionReceiptModel(
        ciphertextKey: ciphertextKey,
        currentUserDID: userDID,
        messageID: messageID,
        groupID: groupID,
        conversationID: conversationID,
        state: .inProgress,
        epoch: epoch,
        wireSequence: wireSequence,
        processID: self.processID
      )
      
      try newReceipt.insert(db)
      
      logger.info("📋 [LEDGER] Claimed ciphertext \(ciphertextKey.prefix(24))... for \(self.processID)")
      
      return .proceed(isRetry: false, attemptNumber: 1)
    }
  }
  
  private nonisolated func handleExistingReceipt(
    _ receipt: MLSDecryptionReceiptModel,
    in db: Database
  ) throws -> MLSDecryptionDecision {
    switch receipt.state {
    case .done:
      // Already decrypted - skip and use cache
      logger.info("✅ [LEDGER] Ciphertext \(receipt.ciphertextKey.prefix(24))... already done")
      return .skip(reason: .alreadyDecrypted, messageID: receipt.messageID)
      
    case .inProgress:
      // Check if stale (process crashed)
      let age = Date().timeIntervalSince(receipt.startedAt)
      if age > Self.staleThresholdSeconds {
        // Take over stale in-progress
        logger.warning("⚠️ [LEDGER] Taking over stale in-progress (age: \(Int(age))s)")
        
        var updated = receipt
        updated.processID = processID
        updated.startedAt = Date()
        updated.retryCount += 1
        try updated.update(db)
        
        return .proceed(isRetry: true, attemptNumber: updated.retryCount + 1)
      } else {
        // Genuinely in-progress - caller should wait
        logger.info("⏳ [LEDGER] Ciphertext in-progress by \(receipt.processID) (age: \(Int(age))s)")
        return .wait(startedAt: receipt.startedAt, processID: receipt.processID)
      }
      
    case .failedRetryable:
      if receipt.retryCount >= Self.maxRetryCount {
        // Too many retries - mark permanent
        logger.warning("❌ [LEDGER] Max retries exceeded for \(receipt.ciphertextKey.prefix(24))...")
        
        var updated = receipt
        updated.state = .failedPermanent
        updated.completedAt = Date()
        try updated.update(db)
        
        return .skip(reason: .maxRetriesExceeded, messageID: receipt.messageID)
      } else {
        // Retry
        logger.info("🔄 [LEDGER] Retrying \(receipt.ciphertextKey.prefix(24))... (attempt \(receipt.retryCount + 1))")
        
        var updated = receipt
        updated.state = .inProgress
        updated.processID = processID
        updated.startedAt = Date()
        updated.retryCount += 1
        try updated.update(db)
        
        return .proceed(isRetry: true, attemptNumber: updated.retryCount + 1)
      }
      
    case .failedPermanent:
      // Do not attempt
      logger.warning("⛔️ [LEDGER] Permanent failure for \(receipt.ciphertextKey.prefix(24))...")
      return .skip(reason: .permanentFailure, messageID: receipt.messageID)
    }
  }
  
  private func markCompleteInternal(
    ciphertextKey: String,
    userDID: String,
    success: Bool,
    errorType: String?,
    database: any DatabaseWriter
  ) async throws {
    try await database.write { [self] db in
      guard var receipt = try MLSDecryptionReceiptModel
        .filter(MLSDecryptionReceiptModel.Columns.ciphertextKey == ciphertextKey)
        .filter(MLSDecryptionReceiptModel.Columns.currentUserDID == userDID)
        .fetchOne(db) else {
        logger.warning("⚠️ [LEDGER] markComplete called but no receipt found for \(ciphertextKey.prefix(24))...")
        return
      }
      
      if success {
        receipt.state = .done
        receipt.completedAt = Date()
        logger.info("✅ [LEDGER] Marked done: \(ciphertextKey.prefix(24))...")
      } else if let errorType = errorType {
        let errorClass = classifyErrorString(errorType)
        
        switch errorClass {
        case .expectedSuccess:
          // SecretReuseError, CannotDecryptOwnMessage - treat as done
          receipt.state = .done
          receipt.completedAt = Date()
          receipt.errorType = errorType
          logger.info("✅ [LEDGER] Marked done (expected): \(ciphertextKey.prefix(24))... (\(errorType))")
          
        case .transient:
          receipt.state = .failedRetryable
          receipt.completedAt = Date()
          receipt.errorType = errorType
          logger.info("♻️ [LEDGER] Marked retryable: \(ciphertextKey.prefix(24))... (\(errorType))")
          
        case .permanent:
          receipt.state = .failedPermanent
          receipt.completedAt = Date()
          receipt.errorType = errorType
          logger.warning("❌ [LEDGER] Marked permanent: \(ciphertextKey.prefix(24))... (\(errorType))")
        }
      } else {
        // Unknown error - assume transient
        receipt.state = .failedRetryable
        receipt.completedAt = Date()
        logger.info("♻️ [LEDGER] Marked retryable (unknown): \(ciphertextKey.prefix(24))...")
      }
      
      try receipt.update(db)
    }
  }
  
  nonisolated private func classifyErrorString(_ errorString: String) -> MLSDecryptionErrorClass {
    let lowercased = errorString.lowercased()
    
    if lowercased.contains("secretreuse") ||
       lowercased.contains("cannot decrypt own") ||
       lowercased.contains("cannotdecryptownmessage") {
      return .expectedSuccess
    }
    
    if lowercased.contains("network") ||
       lowercased.contains("timeout") ||
       lowercased.contains("busy") ||
       lowercased.contains("locked") ||
       lowercased.contains("stale") {
      return .transient
    }
    
    return .permanent
  }
  
  // MARK: - Wait/Poll Logic
  
  /// Wait for an in-progress decryption to complete.
  ///
  /// Use this when `shouldDecrypt` returns `.wait`.
  ///
  /// - Parameters:
  ///   - ciphertextKey: The ciphertext key to wait for
  ///   - userDID: Current user's DID
  ///   - database: Database for polling
  /// - Returns: Final decision after waiting (typically .skip if done, or .proceed if timed out)
  public func waitForCompletion(
    groupID: Data,
    epoch: Int64,
    wireSequence: Int64,
    userDID: String,
    database: any DatabaseWriter
  ) async throws -> MLSDecryptionDecision {
    let ciphertextKey = MLSDecryptionReceiptModel.generateCiphertextKey(
      groupID: groupID,
      epoch: epoch,
      wireSequence: wireSequence
    )
    
    let startTime = Date()
    var currentDelay = pollIntervalBase
    
    while Date().timeIntervalSince(startTime) < waitTimeoutTotal {
      // Check current state
      let receipt = try await database.read { db in
        try MLSDecryptionReceiptModel
          .filter(MLSDecryptionReceiptModel.Columns.ciphertextKey == ciphertextKey)
          .filter(MLSDecryptionReceiptModel.Columns.currentUserDID == userDID)
          .fetchOne(db)
      }
      
      guard let receipt = receipt else {
        // Receipt deleted? Unusual but proceed
        logger.warning("⚠️ [WAIT] Receipt disappeared during wait")
        return .proceed(isRetry: false, attemptNumber: 1)
      }
      
      switch receipt.state {
      case .done:
        logger.info("✅ [WAIT] Completed while waiting - using cache")
        return .skip(reason: .alreadyDecrypted, messageID: receipt.messageID)
        
      case .failedPermanent:
        logger.warning("❌ [WAIT] Failed permanently while waiting")
        return .skip(reason: .permanentFailure, messageID: receipt.messageID)
        
      case .failedRetryable:
        // Other process failed - we can try
        logger.info("🔄 [WAIT] Other process failed - taking over")
        return try await shouldDecrypt(
          groupID: groupID,
          epoch: epoch,
          wireSequence: wireSequence,
          messageID: receipt.messageID,
          conversationID: receipt.conversationID,
          userDID: userDID,
          database: database
        )
        
      case .inProgress:
        // Still in progress - continue waiting
        break
      }
      
      // Exponential backoff
      try await Task.sleep(nanoseconds: UInt64(currentDelay * 1_000_000_000))
      currentDelay = min(currentDelay * 2, pollIntervalMax)
    }
    
    // Timeout - take over
    logger.warning("⏰ [WAIT] Timeout waiting for \(ciphertextKey.prefix(24))... - taking over")
    
    // Re-run decision logic which will detect stale and take over
    return try await shouldDecrypt(
      groupID: groupID,
      epoch: epoch,
      wireSequence: wireSequence,
      messageID: "", // Will be fetched from existing receipt
      conversationID: "", // Will be fetched from existing receipt
      userDID: userDID,
      database: database
    )
  }
}

// MARK: - Data Extension for SHA256

import CryptoKit

extension Data {
  /// Compute SHA256 hash of this data
  func sha256() -> Data {
    let digest = SHA256.hash(data: self)
    return Data(digest)
  }
}
