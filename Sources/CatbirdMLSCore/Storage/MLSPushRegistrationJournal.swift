import Foundation
import GRDB

enum MLSPushRegistrationJournal {
  enum State: String, Sendable {
    case pending
    case active
    case superseded
  }

  struct Entry: Equatable, Sendable {
    let currentUserDID: String
    let deviceID: String
    let provider: MLSPushRegistrationProvider
    let appID: String
    let registrationID: String
    let tokenGeneration: Int64
    let tokenSHA256: Data
    let enabled: Bool
    let keyID: String
    let authGeneration: Int64
    let publicKey: Data
    let signedAt: String
    let bodyJSON: Data
    let transcript: Data
    let signature: Data
    let requestJSON: Data
    let bindingDigest: Data?
    let state: State
  }

  static func createTable(in db: Database) throws {
    try db.execute(sql: """
      CREATE TABLE IF NOT EXISTS mls_push_registration_journal (
        current_user_did TEXT NOT NULL,
        device_id TEXT NOT NULL,
        provider TEXT NOT NULL CHECK(provider IN ('apns', 'fcm')),
        app_id TEXT NOT NULL,
        registration_id TEXT NOT NULL,
        token_generation INTEGER NOT NULL CHECK(token_generation > 0),
        token_sha256 BLOB NOT NULL CHECK(length(token_sha256) = 32),
        enabled INTEGER NOT NULL,
        key_id TEXT NOT NULL,
        auth_generation INTEGER NOT NULL CHECK(auth_generation > 0),
        public_key BLOB NOT NULL,
        signed_at TEXT NOT NULL,
        body_json BLOB NOT NULL,
        transcript BLOB NOT NULL,
        signature BLOB NOT NULL CHECK(length(signature) = 64),
        request_json BLOB NOT NULL,
        binding_digest BLOB,
        state TEXT NOT NULL CHECK(state IN ('pending', 'active', 'superseded')),
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(current_user_did, device_id, provider, app_id, token_generation),
        UNIQUE(current_user_did, device_id, provider, app_id, registration_id, token_generation)
      );
      CREATE INDEX IF NOT EXISTS idx_push_registration_latest
        ON mls_push_registration_journal(
          current_user_did, device_id, provider, app_id, token_generation DESC
        );
      """)
  }

  static func latest(
    userDID: String,
    deviceID: String,
    provider: MLSPushRegistrationProvider,
    appID: String,
    in db: Database
  ) throws -> Entry? {
    try createTable(in: db)
    guard let row = try Row.fetchOne(
      db,
      sql: """
        SELECT * FROM mls_push_registration_journal
        WHERE current_user_did = ? AND device_id = ? AND provider = ? AND app_id = ?
        ORDER BY token_generation DESC LIMIT 1
        """,
      arguments: [MLSStorageHelpers.normalizeDID(userDID), deviceID, provider.rawValue, appID]
    ) else { return nil }
    return try entry(from: row)
  }

  static func reserve(_ entry: Entry, in db: Database) throws -> Entry {
    try createTable(in: db)
    let normalizedDID = MLSStorageHelpers.normalizeDID(entry.currentUserDID)
    if let current = try latest(
      userDID: normalizedDID,
      deviceID: entry.deviceID,
      provider: entry.provider,
      appID: entry.appID,
      in: db
    ) {
      if current.tokenSHA256 == entry.tokenSHA256,
         current.enabled == entry.enabled,
         current.keyID == entry.keyID,
         current.authGeneration == entry.authGeneration,
         current.publicKey == entry.publicKey
      { return current }
      guard entry.tokenGeneration > current.tokenGeneration else {
        throw MLSPushRegistrationError.invalidField("tokenGeneration")
      }
      guard entry.registrationID == current.registrationID else {
        throw MLSPushRegistrationError.invalidField("registrationId")
      }
    } else {
      guard entry.tokenGeneration == 1 else {
        throw MLSPushRegistrationError.invalidField("initial tokenGeneration")
      }
    }

    try db.execute(sql: """
      INSERT INTO mls_push_registration_journal (
        current_user_did, device_id, provider, app_id, registration_id,
        token_generation, token_sha256, enabled, key_id, auth_generation,
        public_key, signed_at, body_json, transcript, signature, request_json,
        binding_digest, state, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'pending', ?)
      """, arguments: [
        normalizedDID, entry.deviceID, entry.provider.rawValue, entry.appID,
        entry.registrationID, entry.tokenGeneration, entry.tokenSHA256,
        entry.enabled, entry.keyID, entry.authGeneration, entry.publicKey,
        entry.signedAt, entry.bodyJSON, entry.transcript, entry.signature,
        entry.requestJSON, Date(),
      ])
    return entry
  }

  static func markActive(
    _ entry: Entry,
    bindingDigest: Data,
    in db: Database
  ) throws {
    guard bindingDigest.count == 32 else { throw MLSPushRegistrationError.invalidField("bindingDigest") }
    let normalizedDID = MLSStorageHelpers.normalizeDID(entry.currentUserDID)
    try db.execute(sql: """
      UPDATE mls_push_registration_journal
      SET state = 'superseded', updated_at = ?
      WHERE current_user_did = ? AND device_id = ? AND provider = ? AND app_id = ?
        AND token_generation < ? AND state != 'superseded'
      """, arguments: [
        Date(), normalizedDID, entry.deviceID, entry.provider.rawValue,
        entry.appID, entry.tokenGeneration,
      ])
    try db.execute(sql: """
      UPDATE mls_push_registration_journal
      SET state = 'active', binding_digest = ?, updated_at = ?
      WHERE current_user_did = ? AND device_id = ? AND provider = ? AND app_id = ?
        AND token_generation = ? AND state = 'pending'
        AND request_json = ? AND signature = ?
      """, arguments: [
        bindingDigest, Date(), normalizedDID, entry.deviceID,
        entry.provider.rawValue, entry.appID, entry.tokenGeneration,
        entry.requestJSON, entry.signature,
      ])
    guard db.changesCount == 1 else {
      throw MLSPushRegistrationError.invalidResponse("journal acknowledgment fence failed")
    }
  }

  private static func entry(from row: Row) throws -> Entry {
    guard let provider = MLSPushRegistrationProvider(rawValue: row["provider"]),
          let state = State(rawValue: row["state"])
    else { throw MLSPushRegistrationError.invalidResponse("corrupt push-registration journal") }
    return Entry(
      currentUserDID: row["current_user_did"],
      deviceID: row["device_id"],
      provider: provider,
      appID: row["app_id"],
      registrationID: row["registration_id"],
      tokenGeneration: row["token_generation"],
      tokenSHA256: row["token_sha256"],
      enabled: row["enabled"],
      keyID: row["key_id"],
      authGeneration: row["auth_generation"],
      publicKey: row["public_key"],
      signedAt: row["signed_at"],
      bodyJSON: row["body_json"],
      transcript: row["transcript"],
      signature: row["signature"],
      requestJSON: row["request_json"],
      bindingDigest: row["binding_digest"],
      state: state
    )
  }
}
