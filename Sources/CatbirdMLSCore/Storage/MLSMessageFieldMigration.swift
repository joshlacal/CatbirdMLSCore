//
//  MLSMessageFieldMigration.swift
//  CatbirdMLSCore
//
//  One-shot per-DID migration for field-level encryption columns on
//  MLSMessageModel.
//

import Foundation
import GRDB

/// One-shot per-DID migration that introduces the field-level encryption
/// columns (`payloadEncrypted`, `entryHMAC`, `payloadKeyVersion`,
/// `isTombstone`, `deletedAt`) on `MLSMessageModel` and drops any
/// pre-migration message rows for the DID.
///
/// Follows the precedent of `MLSPlaintextHeaderMigration`: existing
/// conversation rows are preserved; only message rows are dropped.
/// Tracked per-DID in App Group `UserDefaults` under the key
/// `MLSMessageFieldMigrationV1_<sanitized-did>`.
public enum MLSMessageFieldMigration {
    private static let migrationKeyPrefix = "MLSMessageFieldMigrationV1_"
    private static let appGroupSuite = "group.blue.catbird.shared"

    public static func runIfNeeded(
        userDID: String,
        database: DatabaseWriter
    ) throws {
        let defaults = UserDefaults(suiteName: appGroupSuite)
            ?? UserDefaults.standard
        let sanitized = sanitize(userDID)
        let key = migrationKeyPrefix + sanitized
        if defaults.bool(forKey: key) {
            return
        }

        try database.write { db in
            try ensureColumns(in: db)
            try db.execute(
                sql: "DELETE FROM MLSMessageModel WHERE currentUserDID = ?",
                arguments: [userDID]
            )
        }

        defaults.set(true, forKey: key)
    }

    private static func ensureColumns(in db: Database) throws {
        let columns = try db.columns(in: "MLSMessageModel")
        let names = Set(columns.map { $0.name })
        if !names.contains("payloadEncrypted") {
            try db.execute(sql: "ALTER TABLE MLSMessageModel ADD COLUMN payloadEncrypted BLOB")
        }
        if !names.contains("entryHMAC") {
            try db.execute(sql: "ALTER TABLE MLSMessageModel ADD COLUMN entryHMAC BLOB")
        }
        if !names.contains("payloadKeyVersion") {
            try db.execute(sql: "ALTER TABLE MLSMessageModel ADD COLUMN payloadKeyVersion INTEGER")
        }
        if !names.contains("isTombstone") {
            try db.execute(sql: "ALTER TABLE MLSMessageModel ADD COLUMN isTombstone INTEGER NOT NULL DEFAULT 0")
        }
        if !names.contains("deletedAt") {
            try db.execute(sql: "ALTER TABLE MLSMessageModel ADD COLUMN deletedAt INTEGER")
        }
    }

    private static func sanitize(_ did: String) -> String {
        did.replacingOccurrences(of: ":", with: "_")
            .replacingOccurrences(of: "/", with: "_")
    }
}
