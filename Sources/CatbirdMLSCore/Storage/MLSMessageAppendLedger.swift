import CatbirdMLS
import CryptoKit
import Foundation
import GRDB

/// A new, explicitly versioned append chain. Legacy message rows are neither
/// imported nor reauthenticated. Only commitments are retained here: edits,
/// unsend and payload expiry must not leave a second decryptable body behind.
/// The mutable message projection and this ledger are written in one transaction.
enum MLSMessageAppendLedger {
  static let table = "MLSMessageAppendLedgerV1"
  private static let proofDomain = "catbird.message-append.v1:"
  private static let bodyDomain = "catbird.message-body.v1:"

  enum IntegrityError: Error {
    case immutableMessageConflict
    case invalidAppendChain
  }

  struct SealedPayload {
    let wire: Data
    let entryHMAC: Data
  }

  static func createSchema(in db: Database) throws {
    try db.execute(sql: """
      CREATE TABLE IF NOT EXISTS MLSMessageAppendLedgerV1 (
        currentUserDID TEXT NOT NULL,
        cryptoConversationID TEXT NOT NULL,
        messageID TEXT NOT NULL,
        appendOrdinal INTEGER NOT NULL CHECK (appendOrdinal > 0),
        payloadDigest BLOB NOT NULL CHECK (length(payloadDigest) = 32),
        bodyHMAC BLOB NOT NULL CHECK (length(bodyHMAC) = 32),
        entryHMAC BLOB NOT NULL CHECK (length(entryHMAC) = 32),
        proofHMAC BLOB NOT NULL CHECK (length(proofHMAC) = 32),
        PRIMARY KEY (currentUserDID, messageID),
        UNIQUE (currentUserDID, cryptoConversationID, appendOrdinal)
      )
      """)
  }

  // Avoid GRDB's trapping typed subscripts on persisted data. SQLite affinity
  // alone does not prevent text in an INTEGER/BLOB column after corruption.
  private static func ordinal(_ row: Row) throws -> Int64 {
    guard case .int64(let value) = try row.decode(DatabaseValue.self, forColumn: "appendOrdinal").storage,
      value > 0 else { throw IntegrityError.invalidAppendChain }
    return value
  }

  private static func bytes(_ row: Row, _ column: String) throws -> Data {
    guard case .blob(let value) = try row.decode(DatabaseValue.self, forColumn: column).storage,
      value.count == 32 else { throw IntegrityError.invalidAppendChain }
    return value
  }

  private static func identity(_ row: Row, _ column: String) throws -> String {
    guard case .string(let value) = try row.decode(DatabaseValue.self, forColumn: column).storage
    else { throw IntegrityError.invalidAppendChain }
    return value
  }

  private static func bodyData(payload: Data, senderID: String) throws -> Data {
    try JSONSerialization.data(withJSONObject: [
      "payload": JSONSerialization.jsonObject(with: payload),
      "senderID": MLSStorageHelpers.normalizeDID(senderID),
    ], options: [.sortedKeys, .fragmentsAllowed])
  }

  private static func proofData(ordinal: Int64, digest: Data, bodyHMAC: Data, entryHMAC: Data) throws -> Data {
    try JSONSerialization.data(withJSONObject: [
      "ordinal": ordinal,
      "payloadDigest": digest.base64EncodedString(),
      "bodyHMAC": bodyHMAC.base64EncodedString(),
      "entryHMAC": entryHMAC.base64EncodedString(),
    ], options: [.sortedKeys])
  }

  /// Verify only the new ledger, in its persisted local order. Sender MLS
  /// generation, server entry sequence and timestamps never select a tail.
  /// This does not enable a historical chain gate on message reads.
  static func verifiedEntries(context: MlsContext, in db: Database,
    userDID: String, cryptoConversationID: String) throws -> [Row]
  {
    let rows = try Row.fetchAll(db, sql: """
      SELECT * FROM MLSMessageAppendLedgerV1
      WHERE currentUserDID = ? AND cryptoConversationID = ? ORDER BY appendOrdinal
      """, arguments: [userDID, cryptoConversationID])
    var previous: Data?
    for (offset, row) in rows.enumerated() {
      let appendOrdinal = try ordinal(row)
      guard appendOrdinal == Int64(offset) + 1 else { throw IntegrityError.invalidAppendChain }
      let proof = try proofData(ordinal: appendOrdinal, digest: bytes(row, "payloadDigest"),
        bodyHMAC: bytes(row, "bodyHMAC"), entryHMAC: bytes(row, "entryHMAC"))
      guard try MLSFieldEncryption.verifyHMAC(context: context, conversationID: cryptoConversationID,
        previousHMAC: previous, messageID: proofDomain + identity(row, "messageID"),
        payloadWire: proof, expected: bytes(row, "proofHMAC"))
      else { throw IntegrityError.invalidAppendChain }
      previous = try bytes(row, "proofHMAC")
    }
    return rows
  }

  /// Nil means an immutable duplicate: preserve every original encrypted byte
  /// and the current projection (which may legitimately be edited or expired).
  /// Epoch, sequence and timestamp remain receipt metadata: optimistic sends
  /// may legitimately promote them without changing the original body.
  /// Error placeholders are replaceable until a successful payload is appended.
  static func prepare(context: MlsContext, in db: Database, userDID: String,
    cryptoConversationID: String, messageID: String, payload: Data,
    senderID: String, existing: MLSMessageModel?,
    recordAppend: Bool = true) throws -> SealedPayload?
  {
    let body = try bodyData(payload: payload, senderID: senderID)
    let rows = try verifiedEntries(context: context, in: db,
      userDID: userDID, cryptoConversationID: cryptoConversationID)
    if let original = try Row.fetchOne(db, sql: """
      SELECT * FROM MLSMessageAppendLedgerV1 WHERE currentUserDID = ? AND messageID = ?
      """, arguments: [userDID, messageID])
    {
      guard try identity(original, "cryptoConversationID") == cryptoConversationID,
        try MLSFieldEncryption.verifyHMAC(context: context, conversationID: cryptoConversationID,
          previousHMAC: nil, messageID: bodyDomain + messageID,
          payloadWire: body, expected: bytes(original, "bodyHMAC"))
      else { throw IntegrityError.immutableMessageConflict }
      if let existing, existing.isEdited == 0, existing.isTombstone == 0,
        !existing.payloadExpired
      {
        guard let wire = existing.payloadEncrypted,
          try Data(SHA256.hash(data: wire)) == bytes(original, "payloadDigest"),
          try existing.entryHMAC == bytes(original, "entryHMAC")
        else { throw IntegrityError.invalidAppendChain }
        let storedPayload = try MLSFieldEncryption.decrypt(context: context,
          conversationID: cryptoConversationID, wire: wire)
        guard try MLSFieldEncryption.verifyHMAC(context: context,
          conversationID: cryptoConversationID, previousHMAC: nil,
          messageID: bodyDomain + messageID,
          payloadWire: bodyData(payload: storedPayload, senderID: existing.senderID),
          expected: bytes(original, "bodyHMAC"))
        else { throw IntegrityError.invalidAppendChain }
      }
      return nil
    }

    if let existing, existing.processingError == nil {
      // Legacy edited/deleted/expired content cannot prove its original body.
      // Preserve it without creating a new proof or resurrecting its payload.
      if existing.isEdited == 1 || existing.isTombstone == 1 || existing.payloadExpired {
        return nil
      }
      let originalData: Data?
      if let wire = existing.payloadEncrypted {
        originalData = try MLSFieldEncryption.decrypt(context: context,
          conversationID: existing.cryptoConversationID ?? existing.conversationID, wire: wire)
      } else {
        originalData = existing.payloadJSON
      }
      if let originalData {
        guard try bodyData(payload: originalData, senderID: existing.senderID) == body
        else { throw IntegrityError.immutableMessageConflict }
        return nil
      }
    }

    let wire = try MLSFieldEncryption.encrypt(context: context,
      conversationID: cryptoConversationID, plaintext: payload)
    let entryHMAC = try MLSFieldEncryption.computeHMAC(context: context,
      conversationID: cryptoConversationID,
      previousHMAC: rows.last.map { try bytes($0, "entryHMAC") },
      messageID: messageID, payloadWire: wire)
    if recordAppend {
      let ordinal = Int64(rows.count) + 1
      let digest = Data(SHA256.hash(data: wire))
      let bodyHMAC = try MLSFieldEncryption.computeHMAC(context: context,
        conversationID: cryptoConversationID, previousHMAC: nil,
        messageID: bodyDomain + messageID, payloadWire: body)
      let proof = try proofData(ordinal: ordinal, digest: digest,
        bodyHMAC: bodyHMAC, entryHMAC: entryHMAC)
      let proofHMAC = try MLSFieldEncryption.computeHMAC(context: context,
        conversationID: cryptoConversationID,
        previousHMAC: rows.last.map { try bytes($0, "proofHMAC") },
        messageID: proofDomain + messageID, payloadWire: proof)
      try db.execute(sql: """
        INSERT INTO MLSMessageAppendLedgerV1
          (currentUserDID, cryptoConversationID, messageID, appendOrdinal,
           payloadDigest, bodyHMAC, entryHMAC, proofHMAC)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, arguments: [userDID, cryptoConversationID, messageID, ordinal,
          digest, bodyHMAC, entryHMAC, proofHMAC])
    }
    return SealedPayload(wire: wire, entryHMAC: entryHMAC)
  }
}
