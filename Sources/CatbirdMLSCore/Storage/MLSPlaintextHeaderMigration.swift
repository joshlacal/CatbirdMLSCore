//
//  MLSPlaintextHeaderMigration.swift
//  CatbirdMLSCore
//
//  Migration helper for SQLCipher plaintext header (0xdead10cc fix).
//
//  CRITICAL CONTEXT:
//  iOS 0xdead10cc termination code means the app held file locks during suspension.
//  SQLCipher normally encrypts the SQLite header, making iOS unable to identify
//  the file as a WAL-mode database. This prevents iOS from automatically
//  checkpointing before suspension.
//
//  The fix is `cipher_plaintext_header_size = 32` which leaves the SQLite header
//  unencrypted. However, this pragma only affects NEW databases - existing
//  databases created before this pragma was added still have encrypted headers.
//
//  This migration detects and recreates databases created before the fix.
//  MLS conversation history will be lost, but this is necessary to prevent
//  0xdead10cc crashes.
//

import Foundation
import OSLog

/// Handles migration of MLS databases to use SQLCipher plaintext headers.
/// This is a one-time migration required for the 0xdead10cc crash fix.
public enum MLSPlaintextHeaderMigration {

  private static let logger = Logger(
    subsystem: "blue.catbird.mls",
    category: "PlaintextHeaderMigration"
  )

  // MARK: - Migration Keys

  /// UserDefaults key prefix for tracking plaintext header migration
  /// Format: MLSPlaintextHeaderMigrationV1_{sanitized-userDID}
  private static let migrationKeyPrefix = "MLSPlaintextHeaderMigrationV1"

  /// UserDefaults key for Rust FFI database migration
  /// Format: MLSRustFFIMigrationV1_{base64-did}
  private static let rustFFIMigrationKeyPrefix = "MLSRustFFIMigrationV1"

  /// UserDefaults suite for shared App Group
  private static let appGroupSuite = "group.blue.catbird.shared"

  // MARK: - Migration Check

  /// Check if database needs migration and delete it if so.
  /// Call this BEFORE opening any MLS database.
  ///
  /// - Parameters:
  ///   - userDID: The user's decentralized identifier
  ///   - databaseType: Which database to check (rustFFI or swiftGRDB)
  /// - Returns: True if migration was performed (database was deleted)
  public static func ensurePlaintextHeaderMigration(
    for userDID: String,
    databaseType: DatabaseType
  ) -> Bool {
    let defaults = UserDefaults(suiteName: appGroupSuite)
    let migrationKey = migrationKey(for: userDID, type: databaseType)

    // Already migrated - verify the header is actually plaintext
    if defaults?.bool(forKey: migrationKey) == true {
      let dbPath = databasePath(for: userDID, type: databaseType)
      if FileManager.default.fileExists(atPath: dbPath) && !verifyPlaintextHeader(at: dbPath) {
        // Flag says migrated but header is still encrypted!
        // Clear flag and fall through to re-migration
        logger.warning("🔧 [PlaintextHeader] Migration flag set but header still encrypted - re-migrating")
        defaults?.removeObject(forKey: migrationKey)
        defaults?.synchronize()
        // Fall through to the migration logic below
      } else {
        return false  // Verified good
      }
    }

    let dbPath = databasePath(for: userDID, type: databaseType)
    let dbExists = FileManager.default.fileExists(atPath: dbPath)

    if dbExists {
      // Database exists but wasn't created with plaintext header
      // Delete it and let it be recreated with the new pragma
      logger.warning(
        """
        🔧 [0xdead10cc-MIGRATION] Recreating \(databaseType.rawValue) database for plaintext header
        User: \(userDID.prefix(20), privacy: .private)...
        Path: \(dbPath, privacy: .private)
        ⚠️ MLS conversation history will be lost - this is necessary to prevent 0xdead10cc crashes
        """
      )

      deleteDatabase(at: dbPath, type: databaseType)

      // Mark as migrated
      defaults?.set(true, forKey: migrationKey)
      defaults?.synchronize()

      logger.info(
        "✅ [0xdead10cc-MIGRATION] \(databaseType.rawValue) database deleted for \(userDID.prefix(20), privacy: .private)... - will be recreated with plaintext header"
      )

      return true
    } else {
      // Database doesn't exist yet - mark as migrated so new database is created with pragma
      defaults?.set(true, forKey: migrationKey)
      defaults?.synchronize()

      logger.debug(
        "📝 [0xdead10cc-MIGRATION] No existing \(databaseType.rawValue) database for \(userDID.prefix(20), privacy: .private)... - marking as migrated"
      )

      return false
    }
  }

  /// Database type for migration tracking
  public enum DatabaseType: String {
    /// Rust FFI MLS state database (`mls-state/{base64-did}.db`)
    case rustFFI = "RustFFI"
    /// Swift GRDB messages database (`MLS/mls_messages_{sanitized-did}.db`)
    case swiftGRDB = "SwiftGRDB"
  }

  // MARK: - Path Helpers

  /// Get the migration key for a user and database type
  private static func migrationKey(for userDID: String, type: DatabaseType) -> String {
    let sanitized: String
    switch type {
    case .rustFFI:
      // Match MLSClient.swift base64 encoding
      sanitized = userDID.data(using: .utf8)?
        .base64EncodedString()
        .replacingOccurrences(of: "/", with: "_")
        .replacingOccurrences(of: "+", with: "-")
        .replacingOccurrences(of: "=", with: "")
        .prefix(64)
        .description ?? "default"
    case .swiftGRDB:
      // Match MLSGRDBManager.swift sanitization
      sanitized = userDID
        .replacingOccurrences(of: ":", with: "-")
        .replacingOccurrences(of: "/", with: "-")
        .replacingOccurrences(of: "#", with: "-")
        .replacingOccurrences(of: "?", with: "-")
    }

    switch type {
    case .rustFFI:
      return "\(rustFFIMigrationKeyPrefix)_\(sanitized)"
    case .swiftGRDB:
      return "\(migrationKeyPrefix)_\(sanitized)"
    }
  }

  /// Get the database file path for a user and database type
  private static func databasePath(for userDID: String, type: DatabaseType) -> String {
    let baseContainer = MLSStoragePaths.baseContainerURL()

    switch type {
    case .rustFFI:
      // Match MLSClient.swift path: mls-state/{base64-did}.db
      let didHash = userDID.data(using: .utf8)?
        .base64EncodedString()
        .replacingOccurrences(of: "/", with: "_")
        .replacingOccurrences(of: "+", with: "-")
        .replacingOccurrences(of: "=", with: "")
        .prefix(64)
        .description ?? "default"

      let mlsStateDir = baseContainer.appendingPathComponent("mls-state", isDirectory: true)
      return mlsStateDir.appendingPathComponent("\(didHash).db").path

    case .swiftGRDB:
      // Match MLSGRDBManager.swift path: MLS/mls_messages_{sanitized-did}.db
      let sanitizedDID = userDID
        .replacingOccurrences(of: ":", with: "-")
        .replacingOccurrences(of: "/", with: "-")
        .replacingOccurrences(of: "#", with: "-")
        .replacingOccurrences(of: "?", with: "-")

      let mlsDir = baseContainer.appendingPathComponent("MLS", isDirectory: true)
      return mlsDir.appendingPathComponent("mls_messages_\(sanitizedDID).db").path
    }
  }

  /// Delete database and its associated WAL/SHM files
  private static func deleteDatabase(at path: String, type: DatabaseType) {
    let fileManager = FileManager.default

    // Main database file
    try? fileManager.removeItem(atPath: path)

    // WAL file
    let walPath = path + "-wal"
    try? fileManager.removeItem(atPath: walPath)

    // SHM file
    let shmPath = path + "-shm"
    try? fileManager.removeItem(atPath: shmPath)

    // For Rust FFI, also delete journal file if it exists
    if type == .rustFFI {
      let journalPath = path + "-journal"
      try? fileManager.removeItem(atPath: journalPath)
    }

    logger.debug("🗑️ Deleted database files at: \(path, privacy: .private)")
  }

  // MARK: - Header Verification

  /// Verify that a database file actually has a plaintext SQLite header.
  /// Reads the first 16 bytes and checks for the SQLite magic string "SQLite format 3\0".
  /// Returns true if the header is plaintext (correct), false if encrypted or unreadable.
  private static func verifyPlaintextHeader(at path: String) -> Bool {
    guard FileManager.default.fileExists(atPath: path) else {
      // File doesn't exist yet - will be created with correct header
      return true
    }

    guard let fileHandle = FileHandle(forReadingAtPath: path) else {
      logger.warning("⚠️ [PlaintextHeader] Cannot open file for verification: \(path, privacy: .private)")
      return false
    }
    defer { fileHandle.closeFile() }

    let headerData = fileHandle.readData(ofLength: 16)
    guard headerData.count >= 16 else {
      logger.warning("⚠️ [PlaintextHeader] File too small for header check: \(headerData.count) bytes")
      return false  // File too small, likely corrupt or empty
    }

    // SQLite magic bytes: "SQLite format 3\0"
    let sqliteMagic: [UInt8] = [
      0x53, 0x51, 0x4C, 0x69, 0x74, 0x65, 0x20, 0x66,
      0x6F, 0x72, 0x6D, 0x61, 0x74, 0x20, 0x33, 0x00
    ]

    let headerBytes = [UInt8](headerData)
    let isPlaintext = headerBytes.prefix(16) == sqliteMagic.prefix(16)

    if isPlaintext {
      logger.debug("✅ [PlaintextHeader] Verified SQLite magic header at: \(path, privacy: .private)")
    } else {
      let hexHeader = headerBytes.prefix(16).map { String(format: "%02x", $0) }.joined(separator: " ")
      logger.warning(
        """
        ❌ [PlaintextHeader] ENCRYPTED header detected despite migration flag!
        Path: \(path, privacy: .private)
        Header: \(hexHeader, privacy: .public)
        Expected: 53 51 4C 69 74 65 20 66 6F 72 6D 61 74 20 33 00
        Action: Will re-migrate (delete and recreate)
        """
      )
    }

    return isPlaintext
  }

  // MARK: - Migration Status

  /// Check if a user's database has been migrated
  /// - Parameters:
  ///   - userDID: The user's decentralized identifier
  ///   - databaseType: Which database to check
  /// - Returns: True if already migrated
  public static func isMigrated(for userDID: String, databaseType: DatabaseType) -> Bool {
    let defaults = UserDefaults(suiteName: appGroupSuite)
    let key = migrationKey(for: userDID, type: databaseType)
    return defaults?.bool(forKey: key) == true
  }

  /// Force re-migration for a user (for testing or recovery)
  /// - Parameters:
  ///   - userDID: The user's decentralized identifier
  ///   - databaseType: Which database to reset
  public static func resetMigrationStatus(for userDID: String, databaseType: DatabaseType) {
    let defaults = UserDefaults(suiteName: appGroupSuite)
    let key = migrationKey(for: userDID, type: databaseType)
    defaults?.removeObject(forKey: key)
    defaults?.synchronize()

    logger.info("🔄 Reset migration status for \(databaseType.rawValue): \(userDID.prefix(20), privacy: .private)...")
  }

  /// Reset all migration flags for a user (both databases)
  public static func resetAllMigrations(for userDID: String) {
    resetMigrationStatus(for: userDID, databaseType: .rustFFI)
    resetMigrationStatus(for: userDID, databaseType: .swiftGRDB)
  }
}
