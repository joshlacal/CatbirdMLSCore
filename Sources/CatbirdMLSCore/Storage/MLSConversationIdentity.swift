import Foundation
import GRDB

/// The storage identity rules used by callback paths.
///
/// A group id is an MLS identifier, not a conversation id.  During the period
/// where callbacks supplied the group id as the conversation id, the database
/// could contain one exact raw alias (`lowercase(hex(groupID))`) alongside the
/// real conversation UUID.  This file deliberately keeps lookup (read-only)
/// separate from healing (transactional) so reads cannot delete or rewrite
/// state as a side effect.
enum MLSConversationTableScope {
  case stable
  case group
  case disposable
}

struct MLSConversationScopedTable {
  let name: String
  let idColumn: String
  let userColumn: String?
}

extension MLSStorageHelpers {
  /// Inventory of every table in this package that can be encountered while
  /// adopting a conversation projection.  Stable rows move with the
  /// canonical conversation; group rows remain keyed by MLS group; disposable
  /// rows are account/global state and must never be guessed into a migration.
  /// The two crypto-bearing stable tables (members and epoch keys) and message
  /// reactions use dedicated mergers below; all other stable entries use the
  /// descriptor-driven collision/fail-closed path.
  static let conversationTableInventory: [(name: String, scope: MLSConversationTableScope)] = [
    ("MLSConversationModel", .stable),
    ("MLSMessageModel", .stable),
    ("MLSMemberModel", .stable),
    ("MLSEpochKeyModel", .stable),
    ("MLSMessageReactionModel", .stable),
    ("MLSStorageBlobModel", .stable),
    ("MLSAdminRosterModel", .stable),
    ("MLSMembershipEventModel", .stable),
    ("MLSRosterSnapshotModel", .stable),
    ("MLSTreeHashPinModel", .stable),
    ("MLSValidationAuditLog", .stable),
    ("MLSOrphanedReactionModel", .stable),
    ("mls_conversation_sequence_state", .stable),
    ("mls_pending_messages", .stable),
    ("MLSBootstrapPendingModel", .stable),
    ("MLSRecoveryAttemptStateModel", .stable),
    ("MLSOrphanedMutationModel", .stable),
    ("MLSDecryptionReceiptModel", .stable),
    ("MLSDeliveryAck", .stable),
    ("mls_conversation_read_frontier", .stable),
    ("mls_remote_read_cursor", .stable),
    ("MLSInviteModel", .stable),
    ("MLSPolicyModel", .stable),
    ("mls_orchestrator_pending_local_deletes", .stable),
    ("mls_orchestrator_quarantine", .stable),
    ("mls_orchestrator_pending_messages", .stable),
    ("mls_orchestrator_sequencer_receipts", .stable),
    ("mls_orchestrator_group_state", .group),
    ("MLSKeyPackageModel", .disposable),
    ("MLSConsumptionRecordModel", .disposable),
    ("MLSRecoveryGlobalStateModel", .disposable),
    ("mls_orchestrator_sync_cursors", .disposable),
    // MLSDeclarationCache was explicitly dropped by v20 and is listed here
    // only to make the historical inventory auditable.
    ("MLSDeclarationCache", .disposable),
  ]

  static var conversationScopedTables: [MLSConversationScopedTable] {
    // Stable-scoped core state.  MLSConversationModel is the parent and is
    // intentionally absent: callers insert the canonical parent first.
    [
      .init(name: "MLSMessageModel", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "MLSMessageReactionModel", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "MLSStorageBlobModel", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "MLSAdminRosterModel", idColumn: "convo_id", userColumn: nil),
      .init(name: "MLSMembershipEventModel", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "MLSRosterSnapshotModel", idColumn: "conversationID", userColumn: nil),
      .init(name: "MLSTreeHashPinModel", idColumn: "conversationID", userColumn: nil),
      .init(name: "MLSValidationAuditLog", idColumn: "conversationID", userColumn: nil),
      .init(name: "MLSOrphanedReactionModel", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "mls_conversation_sequence_state", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "mls_pending_messages", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "MLSBootstrapPendingModel", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "MLSRecoveryAttemptStateModel", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "MLSOrphanedMutationModel", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "MLSDecryptionReceiptModel", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "MLSDeliveryAck", idColumn: "conversationId", userColumn: "currentUserDID"),
      .init(name: "mls_conversation_read_frontier", idColumn: "conversationID", userColumn: "currentUserDID"),
      .init(name: "mls_remote_read_cursor", idColumn: "conversationID", userColumn: "currentUserDID"),
      // These models are not created by the current migrator, but older
      // installations may still have them.  If present they are migrated;
      // if their shape is not known, migration fails closed.
      .init(name: "MLSInviteModel", idColumn: "conversationID", userColumn: nil),
      .init(name: "MLSPolicyModel", idColumn: "conversationID", userColumn: nil),
      // Adapter-owned stable-scoped state.
      .init(name: "mls_orchestrator_pending_local_deletes", idColumn: "conversation_id", userColumn: "user_did"),
      .init(name: "mls_orchestrator_quarantine", idColumn: "conversation_id", userColumn: "user_did"),
      .init(name: "mls_orchestrator_pending_messages", idColumn: "conversation_id", userColumn: "user_did"),
      .init(name: "mls_orchestrator_sequencer_receipts", idColumn: "conversation_id", userColumn: "user_did"),
    ]
  }

  static func isCanonicalUUIDv4(_ value: String) -> Bool {
    guard value.count == 36,
          value == value.lowercased(),
          value.firstIndex(of: "-") == value.index(value.startIndex, offsetBy: 8),
          value.index(value.startIndex, offsetBy: 13) < value.endIndex,
          value.index(value.startIndex, offsetBy: 18) < value.endIndex,
          value.index(value.startIndex, offsetBy: 23) < value.endIndex
    else { return false }

    let chars = Array(value)
    guard chars[8] == "-", chars[13] == "-", chars[18] == "-", chars[23] == "-",
          chars[14] == "4",
          ["8", "9", "a", "b"].contains(chars[19]),
          UUID(uuidString: value)?.uuidString.lowercased() == value
    else { return false }
    return true
  }

  static func normalizedGroupHex(_ groupID: String) -> (data: Data, hex: String)? {
    guard let data = Data(hexEncoded: groupID) else { return nil }
    return (data, data.hexEncodedString())
  }

  static func fetchConversationRows(
    in db: Database,
    userDID: String,
    groupData: Data
  ) throws -> [MLSConversationModel] {
    try MLSConversationModel
      .filter(MLSConversationModel.Columns.groupID == groupData)
      .filter(MLSConversationModel.Columns.currentUserDID == userDID)
      .order(MLSConversationModel.Columns.conversationID)
      .fetchAll(db)
  }

  /// Returns the only canonical UUID row for a group, or throws when the
  /// group has more than one possible stable identity.  A non-raw, non-UUID
  /// row is never considered an alias: it is an ambiguity unless it is the
  /// sole legacy direct row and no healing is possible.
  static func canonicalCandidate(
    rows: [MLSConversationModel],
    rawGroupHex: String,
    requestedID: String,
    direct: MLSConversationModel?
  ) throws -> MLSConversationModel? {
    let nonRaw = rows.filter { $0.conversationID != rawGroupHex }
    let invalid = nonRaw.filter { !isCanonicalUUIDv4($0.conversationID) }
    if !invalid.isEmpty {
      let isSoleLegacyDirect = invalid.count == 1
        && nonRaw.count == 1
        && rows.count == 1
        && direct?.conversationID == invalid[0].conversationID
        && direct?.conversationID == requestedID
      if !isSoleLegacyDirect {
        throw MLSStorageError.invalidConversationID(invalid[0].conversationID)
      }
    }

    let candidates = nonRaw.filter { isCanonicalUUIDv4($0.conversationID) }
    guard candidates.count <= 1 else {
      throw MLSStorageError.ambiguousConversationID(rawGroupHex)
    }
    return candidates.first
  }

  static func makePlaceholder(
    conversationID: String,
    userDID: String,
    groupData: Data,
    isPlaceholder: Bool
  ) -> MLSConversationModel {
    MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: userDID,
      groupID: groupData,
      epoch: 0,
      joinMethod: .unknown,
      joinEpoch: 0,
      title: nil,
      avatarURL: nil,
      createdAt: Date(),
      updatedAt: Date(),
      lastMessageAt: nil,
      lastMembershipChangeAt: nil,
      unacknowledgedMemberChanges: 0,
      isActive: true,
      needsRejoin: false,
      rejoinRequestedAt: nil,
      lastRecoveryAttempt: nil,
      consecutiveFailures: 0,
      isPlaceholder: isPlaceholder
    )
  }

  /// Transactional, mutating resolver.  The only row ever retired by this
  /// method is the exact raw alias whose id is the normalized group hex.
  static func ensureConversationExistsStrictSync(
    in db: Database,
    userDID: String,
    conversationID: String,
    groupID: String,
    isPlaceholder: Bool
  ) throws -> String {
    let normalizedUserDID = normalizeDID(userDID)
    guard let normalizedGroup = normalizedGroupHex(groupID) else {
      throw MLSStorageError.invalidGroupID(groupID)
    }

    let direct = try MLSConversationModel
      .filter(MLSConversationModel.Columns.conversationID == conversationID)
      .filter(MLSConversationModel.Columns.currentUserDID == normalizedUserDID)
      .fetchOne(db)

    // A direct row with a different group is an idempotent legacy row.  It is
    // not evidence that any same-group row can be merged into it.
    if let direct, direct.groupID != normalizedGroup.data {
      return direct.conversationID
    }

    let rows = try fetchConversationRows(
      in: db,
      userDID: normalizedUserDID,
      groupData: normalizedGroup.data
    )
    let rawAlias = rows.first { $0.conversationID == normalizedGroup.hex }
    let canonical = try canonicalCandidate(
      rows: rows,
      rawGroupHex: normalizedGroup.hex,
      requestedID: conversationID,
      direct: direct
    )

    let requestedIsExactRaw = conversationID == normalizedGroup.hex
    if requestedIsExactRaw {
      if let canonical {
        if let rawAlias {
          try migrateConversationAliasChildren(
            in: db,
            alias: rawAlias,
            canonical: canonical,
            userDID: normalizedUserDID,
            rawGroupHex: normalizedGroup.hex
          )
        }
        return canonical.conversationID
      }
      if let direct { return direct.conversationID }
      let row = makePlaceholder(
        conversationID: normalizedGroup.hex,
        userDID: normalizedUserDID,
        groupData: normalizedGroup.data,
        isPlaceholder: isPlaceholder
      )
      try row.insert(db)
      return row.conversationID
    }

    if let direct {
      // A direct canonical row is safe only when it is the sole stable row or
      // the only canonical row alongside the exact raw alias.  The candidate
      // validation above rejects lookalikes and multiple stable rows.
      if isCanonicalUUIDv4(direct.conversationID) {
        if let canonical, canonical.conversationID != direct.conversationID {
          throw MLSStorageError.ambiguousConversationID(normalizedGroup.hex)
        }
        if let rawAlias, rawAlias.conversationID != direct.conversationID {
          try migrateConversationAliasChildren(
            in: db,
            alias: rawAlias,
            canonical: direct,
            userDID: normalizedUserDID,
            rawGroupHex: normalizedGroup.hex
          )
        }
      } else if rawAlias != nil || canonical != nil {
        throw MLSStorageError.invalidConversationID(direct.conversationID)
      }
      return direct.conversationID
    }

    if isCanonicalUUIDv4(conversationID) {
      if let canonical {
        if canonical.conversationID != conversationID {
          throw MLSStorageError.ambiguousConversationID(normalizedGroup.hex)
        }
        if let rawAlias {
          try migrateConversationAliasChildren(
            in: db,
            alias: rawAlias,
            canonical: canonical,
            userDID: normalizedUserDID,
            rawGroupHex: normalizedGroup.hex
          )
        }
        return canonical.conversationID
      }
      if let rawAlias {
        return try migrateSoleRawAlias(
          in: db,
          alias: rawAlias,
          to: conversationID,
          groupData: normalizedGroup.data,
          userDID: normalizedUserDID,
          rawGroupHex: normalizedGroup.hex
        )
      }
    } else if rawAlias != nil || canonical != nil {
      // A UUID-looking request is not enough; only canonical lowercase v4
      // UUIDs may replace a raw alias.  This closes uppercase, v1, compact,
      // and arbitrary lookalike identifiers.
      throw MLSStorageError.invalidConversationID(conversationID)
    }

    // With no same-group row, retain compatibility for pre-existing callers
    // that create a direct legacy id.  It cannot retire any other row.
    let row = makePlaceholder(
      conversationID: conversationID,
      userDID: normalizedUserDID,
      groupData: normalizedGroup.data,
      isPlaceholder: isPlaceholder
    )
    try row.insert(db)
    return row.conversationID
  }

  /// Read-only canonical resolver.  It never inserts, updates, or deletes.
  /// `groupID` should be supplied when the callback has it; when omitted, an
  /// exact lowercase hex conversation id is used as the group hint.
  public static func resolveCanonicalConversationIDSync(
    in db: Database,
    userDID: String,
    conversationID: String,
    groupID: String? = nil
  ) throws -> String? {
    let normalizedUserDID = normalizeDID(userDID)
    let direct = try MLSConversationModel
      .filter(MLSConversationModel.Columns.conversationID == conversationID)
      .filter(MLSConversationModel.Columns.currentUserDID == normalizedUserDID)
      .fetchOne(db)

    let groupHint: String?
    if let groupID {
      guard normalizedGroupHex(groupID) != nil else {
        throw MLSStorageError.invalidGroupID(groupID)
      }
      groupHint = groupID
    } else {
      groupHint = Data(hexEncoded: conversationID) == nil
        ? direct?.groupID.hexEncodedString()
        : conversationID
    }
    guard let groupHint, let normalizedGroup = normalizedGroupHex(groupHint) else {
      return direct?.conversationID
    }
    if let direct, direct.groupID != normalizedGroup.data {
      return direct.conversationID
    }

    let rows = try fetchConversationRows(
      in: db,
      userDID: normalizedUserDID,
      groupData: normalizedGroup.data
    )
    let canonical = try canonicalCandidate(
      rows: rows,
      rawGroupHex: normalizedGroup.hex,
      requestedID: conversationID,
      direct: direct
    )

    // A group callback may only use the exact normalized raw group id as an
    // alias.  Do not let a lookalike (or an arbitrary legacy id) borrow the
    // canonical row merely because the callback supplied the real group as a
    // hint.  The mutating resolver applies the same rule before migration;
    // keeping it here makes read-only adapter paths fail closed as well.
    let rawAlias = rows.first { $0.conversationID == normalizedGroup.hex }
    if conversationID != normalizedGroup.hex,
       !isCanonicalUUIDv4(conversationID),
       (rawAlias != nil || canonical != nil)
    {
      throw MLSStorageError.invalidConversationID(conversationID)
    }

    if conversationID == normalizedGroup.hex {
      return canonical?.conversationID ?? direct?.conversationID
    }
    if let direct {
      if isCanonicalUUIDv4(direct.conversationID),
         let canonical, canonical.conversationID != direct.conversationID
      {
        throw MLSStorageError.ambiguousConversationID(normalizedGroup.hex)
      }
      return direct.conversationID
    }
    if isCanonicalUUIDv4(conversationID),
       let canonical,
       canonical.conversationID != conversationID
    {
      // A caller cannot silently route a different stable UUID through the
      // one canonical row for this group.  The mutating path treats this as
      // ambiguity as well; keep read-only callback paths fail-closed.
      throw MLSStorageError.ambiguousConversationID(normalizedGroup.hex)
    }
    if isCanonicalUUIDv4(conversationID),
       rawAlias != nil,
       canonical == nil
    {
      // Adoption requires the transactional resolver to insert the canonical
      // parent before migrating children.  A read-only callback must not
      // create a stranded stable parent beside the raw alias.
      throw MLSStorageError.invalidConversationID(conversationID)
    }
    return canonical?.conversationID
  }

  static func migrateSoleRawAlias(
    in db: Database,
    alias: MLSConversationModel,
    to conversationID: String,
    groupData: Data,
    userDID: String,
    rawGroupHex: String
  ) throws -> String {
    guard alias.conversationID == rawGroupHex else {
      throw MLSStorageError.invalidConversationID(alias.conversationID)
    }

    // Insert the parent first.  Child rows with foreign keys must never be
    // moved while the destination parent is absent.
    let canonical = MLSConversationModel(
      conversationID: conversationID,
      currentUserDID: userDID,
      groupID: groupData,
      epoch: alias.epoch,
      joinMethod: alias.joinMethod,
      joinEpoch: alias.joinEpoch,
      title: alias.title,
      description: alias.description,
      avatarURL: alias.avatarURL,
      avatarImageData: alias.avatarImageData,
      createdAt: alias.createdAt,
      updatedAt: alias.updatedAt,
      lastMessageAt: alias.lastMessageAt,
      lastMembershipChangeAt: alias.lastMembershipChangeAt,
      unacknowledgedMemberChanges: alias.unacknowledgedMemberChanges,
      isActive: alias.isActive,
      needsRejoin: alias.needsRejoin,
      needsReset: alias.needsReset,
      isUnrecoverable: alias.isUnrecoverable,
      rejoinRequestedAt: alias.rejoinRequestedAt,
      lastRecoveryAttempt: alias.lastRecoveryAttempt,
      consecutiveFailures: alias.consecutiveFailures,
      isPlaceholder: alias.isPlaceholder,
      requestState: alias.requestState,
      mutedUntil: alias.mutedUntil,
      pendingNewGroupId: alias.pendingNewGroupId,
      pendingResetGeneration: alias.pendingResetGeneration
    )
    try canonical.insert(db)
    try migrateConversationAliasChildren(
      in: db,
      alias: alias,
      canonical: canonical,
      userDID: userDID,
      rawGroupHex: rawGroupHex
    )
    return canonical.conversationID
  }

  static func migrateConversationAliasChildren(
    in db: Database,
    alias: MLSConversationModel,
    canonical: MLSConversationModel,
    userDID: String,
    rawGroupHex: String
  ) throws {
    guard alias.conversationID == rawGroupHex,
          alias.conversationID != canonical.conversationID
    else { return }

    // Crypto-bearing tables get table-specific collision handling.  A
    // differing epoch secret is an error; identical secrets are deduplicated.
    try migrateEpochKeys(
      in: db,
      from: alias.conversationID,
      to: canonical.conversationID,
      userDID: userDID
    )
    try migrateMembers(
      in: db,
      from: alias.conversationID,
      to: canonical.conversationID,
      userDID: userDID
    )

    // Every stable-scoped table is migrated before the alias parent can be
    // deleted.  SQLite constraint failures intentionally abort the enclosing
    // transaction, leaving the alias and all state intact.
    // MLSMessageModel needs a table-specific move: its routing ID changes,
    // while encrypted payloads/HMACs retain the original authenticated
    // conversation identity in cryptoConversationID.  Treating it as a plain
    // foreign-key update would make every historical message undecryptable.
    try migrateMessages(
      in: db,
      from: alias.conversationID,
      to: canonical.conversationID,
      userDID: userDID
    )

    // Reactions are keyed for local idempotence by owner + message target +
    // actor + emoji, not by the historical conversation alias.  Merge those
    // keys deterministically before changing the routing id so a canonical
    // and alias copy cannot leave unordered duplicates behind.
    try migrateReactions(
      in: db,
      from: alias.conversationID,
      to: canonical.conversationID,
      userDID: userDID
    )

    for descriptor in conversationScopedTables
      where descriptor.name != "MLSMessageModel"
        && descriptor.name != "MLSMessageReactionModel"
    {
      try migrateTable(
        in: db,
        descriptor: descriptor,
        from: alias.conversationID,
        to: canonical.conversationID,
        userDID: userDID
      )
    }

    // Group state is keyed by group identity, so retain the row and update
    // only the exact raw alias mapping.  It is not a stable-scoped child.
    if try db.tableExists("mls_orchestrator_group_state") {
      let columns = try db.columns(in: "mls_orchestrator_group_state").map(\.name)
      guard columns.contains("group_id"), columns.contains("conversation_id") else {
        throw MLSStorageError.unsafeConversationAliasMigration(
          "mls_orchestrator_group_state has an unsupported schema"
        )
      }
      try db.execute(
        sql: """
          UPDATE mls_orchestrator_group_state
          SET conversation_id = ?
          WHERE group_id = ? AND conversation_id = ?
          """,
        arguments: [canonical.conversationID, rawGroupHex, alias.conversationID]
      )
    }

    // Do not rely on SQLite's ON DELETE CASCADE as a cleanup mechanism.  A
    // future migration or adapter-owned table may add a conversation FK (or a
    // plain conversation-id column) that is not yet safe to merge.  Inspect
    // every live table after the known moves and abort if any row still points
    // at the raw alias; the enclosing transaction then leaves both the alias
    // parent and all of its state intact.
    try validateNoResidualConversationReferences(
      in: db,
      aliasID: alias.conversationID,
      userDID: userDID
    )

    // The parent is deleted last.  With foreign keys enabled this is also a
    // final assertion that no declared child was accidentally left behind.
    try alias.delete(db)
  }

  private static func validateNoResidualConversationReferences(
    in db: Database,
    aliasID: String,
    userDID: String
  ) throws {
    let inventoryNames = Set(conversationTableInventory.map(\.name))
    let tables = try String.fetchAll(
      db,
      sql: """
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    )

    func quoted(_ identifier: String) -> String {
      "\"\(identifier.replacingOccurrences(of: "\"", with: "\"\""))\""
    }

    for table in tables where table != MLSConversationModel.databaseTableName {
      let columns = try db.columns(in: table).map(\.name)
      let conversationColumns = columns.filter {
        ["conversationID", "conversationId", "conversation_id", "convo_id"].contains($0)
      }
      for column in conversationColumns {
        let count = try Int.fetchOne(
          db,
          sql: "SELECT COUNT(*) FROM \(quoted(table)) WHERE \(quoted(column)) = ?",
          arguments: [aliasID]
        ) ?? 0
        guard count > 0 else { continue }

        let scope = inventoryNames.contains(table) ? "known" : "uninventoried"
        throw MLSStorageError.unsafeConversationAliasMigration(
          "\(scope) table \(table) still references alias \(aliasID)"
        )
      }

      // Also inspect FK columns whose name is not conversation-like.  This
      // catches future child tables before the parent delete can cascade them.
      let foreignKeys = try Row.fetchAll(
        db,
        sql: "PRAGMA foreign_key_list(\(quoted(table)))"
      )
      for foreignKey in foreignKeys {
        guard (foreignKey["table"] as String?) == MLSConversationModel.databaseTableName,
              let from = foreignKey["from"] as String?
        else { continue }
        let count = try Int.fetchOne(
          db,
          sql: "SELECT COUNT(*) FROM \(quoted(table)) WHERE \(quoted(from)) = ?",
          arguments: [aliasID]
        ) ?? 0
        guard count == 0 else {
          throw MLSStorageError.unsafeConversationAliasMigration(
            "\(table).\(from) still references alias \(aliasID)"
          )
        }
      }
    }
  }

  /// Move message routing rows while preserving the identity authenticated by
  /// the field-encryption AEAD/HMAC derivation.  A message primary-key
  /// collision is deliberately an error: silently choosing one ciphertext
  /// would strand or overwrite authenticated history, so the enclosing
  /// transaction rolls back and leaves the raw alias intact.
  static func migrateMessages(
    in db: Database,
    from oldID: String,
    to newID: String,
    userDID: String
  ) throws {
    guard try db.tableExists(MLSMessageModel.databaseTableName) else { return }
    let columns = try db.columns(in: MLSMessageModel.databaseTableName).map(\.name)
    guard columns.contains("conversationID"),
          columns.contains("cryptoConversationID"),
          columns.contains("currentUserDID")
    else {
      throw MLSStorageError.unsafeConversationAliasMigration(
        "MLSMessageModel lacks durable crypto binding columns"
      )
    }

    do {
      try db.execute(
        sql: """
          UPDATE MLSMessageModel
          SET cryptoConversationID = COALESCE(cryptoConversationID, conversationID),
              conversationID = ?
          WHERE conversationID = ? AND currentUserDID = ?
          """,
        arguments: [newID, oldID, userDID]
      )
    } catch {
      throw MLSStorageError.unsafeConversationAliasMigration(
        "MLSMessageModel: \(error.localizedDescription)"
      )
    }
  }

  static func migrateTable(
    in db: Database,
    descriptor: MLSConversationScopedTable,
    from oldID: String,
    to newID: String,
    userDID: String
  ) throws {
    guard try db.tableExists(descriptor.name) else { return }
    let columns = try db.columns(in: descriptor.name).map(\.name)
    guard columns.contains(descriptor.idColumn) else {
      throw MLSStorageError.unsafeConversationAliasMigration(
        "\(descriptor.name) is missing \(descriptor.idColumn)"
      )
    }
    if let userColumn = descriptor.userColumn, !columns.contains(userColumn) {
      throw MLSStorageError.unsafeConversationAliasMigration(
        "\(descriptor.name) is missing \(userColumn)"
      )
    }

    var sql = "UPDATE \(descriptor.name) SET \(descriptor.idColumn) = ? WHERE \(descriptor.idColumn) = ?"
    var arguments: StatementArguments = [newID, oldID]
    if let userColumn = descriptor.userColumn {
      sql += " AND \(userColumn) = ?"
      arguments += [userDID]
    }
    do {
      try db.execute(sql: sql, arguments: arguments)
    } catch {
      throw MLSStorageError.unsafeConversationAliasMigration(
        "\(descriptor.name): \(error.localizedDescription)"
      )
    }
  }

  static func migrateReactions(
    in db: Database,
    from oldID: String,
    to newID: String,
    userDID: String
  ) throws {
    guard try db.tableExists(MLSReactionModel.databaseTableName) else { return }
    let aliases = try MLSReactionModel
      .filter(MLSReactionModel.Columns.conversationID == oldID)
      .filter(MLSReactionModel.Columns.currentUserDID == userDID)
      .fetchAll(db)
    let canonicals = try MLSReactionModel
      .filter(MLSReactionModel.Columns.conversationID == newID)
      .filter(MLSReactionModel.Columns.currentUserDID == userDID)
      .fetchAll(db)

    func key(_ reaction: MLSReactionModel) -> String {
      [
        userDID,
        newID,
        reaction.messageID,
        normalizeDID(reaction.actorDID),
        reaction.emoji,
      ].joined(separator: "\u{1f}")
    }

    // Existing canonical rows can themselves contain duplicates (for example
    // after an interrupted pre-v35 import).  Pick the same deterministic
    // winner used for alias collisions and retire the losers before handling
    // raw-alias rows; `Dictionary(uniqueKeysWithValues:)` would otherwise
    // trap and bypass the enclosing transaction's typed failure path.
    func reactionPreference(_ lhs: MLSReactionModel, _ rhs: MLSReactionModel) -> Bool {
      if lhs.timestamp != rhs.timestamp { return lhs.timestamp > rhs.timestamp }
      return lhs.reactionID > rhs.reactionID
    }
    var canonicalByKey: [String: MLSReactionModel] = [:]
    for reaction in canonicals.sorted(by: reactionPreference) {
      let reactionKey = key(reaction)
      if canonicalByKey[reactionKey] != nil {
        try db.execute(
          sql: "DELETE FROM MLSMessageReactionModel WHERE reactionID = ?",
          arguments: [reaction.reactionID]
        )
      } else {
        canonicalByKey[reactionKey] = reaction
      }
    }
    for alias in aliases {
      let reactionKey = key(alias)
      if let current = canonicalByKey[reactionKey] {
        // Latest timestamp wins; a stable reaction id breaks ties so this is
        // reproducible across devices and migrations.
        let aliasWins = alias.timestamp > current.timestamp
          || (alias.timestamp == current.timestamp && alias.reactionID > current.reactionID)
        if aliasWins {
          try db.execute(
            sql: """
              UPDATE MLSMessageReactionModel
              SET action = ?, timestamp = ?
              WHERE reactionID = ?
              """,
            arguments: [alias.action, alias.timestamp, current.reactionID]
          )
          canonicalByKey[reactionKey] = MLSReactionModel(
            reactionID: current.reactionID,
            messageID: current.messageID,
            conversationID: newID,
            currentUserDID: userDID,
            actorDID: current.actorDID,
            emoji: current.emoji,
            action: alias.action,
            timestamp: alias.timestamp
          )
        }
        try db.execute(
          sql: "DELETE FROM MLSMessageReactionModel WHERE reactionID = ?",
          arguments: [alias.reactionID]
        )
      } else {
        try db.execute(
          sql: "UPDATE MLSMessageReactionModel SET conversationID = ? WHERE reactionID = ?",
          arguments: [newID, alias.reactionID]
        )
        canonicalByKey[reactionKey] = MLSReactionModel(
          reactionID: alias.reactionID,
          messageID: alias.messageID,
          conversationID: newID,
          currentUserDID: userDID,
          actorDID: normalizeDID(alias.actorDID),
          emoji: alias.emoji,
          action: alias.action,
          timestamp: alias.timestamp
        )
      }
    }
  }

  static func migrateMembers(
    in db: Database,
    from oldID: String,
    to newID: String,
    userDID: String
  ) throws {
    guard try db.tableExists(MLSMemberModel.databaseTableName) else { return }
    let aliases = try MLSMemberModel
      .filter(MLSMemberModel.Columns.conversationID == oldID)
      .filter(MLSMemberModel.Columns.currentUserDID == userDID)
      .fetchAll(db)
    guard !aliases.isEmpty else { return }

    let existingRows = try MLSMemberModel
      .filter(MLSMemberModel.Columns.conversationID == newID)
      .filter(MLSMemberModel.Columns.currentUserDID == userDID)
      .fetchAll(db)

    let aliasesByDID = Dictionary(grouping: aliases, by: { normalizeDID($0.did) })
    let canonicalByDID = Dictionary(grouping: existingRows, by: { normalizeDID($0.did) })

    for normalizedDID in aliasesByDID.keys.sorted() {
      let canonicalID = "\(newID)_\(normalizedDID)"
      let aliasRows = aliasesByDID[normalizedDID] ?? []
      let canonicalRows = canonicalByDID[normalizedDID] ?? []
      let allRows = canonicalRows + aliasRows
      guard let first = allRows.sorted(by: memberPreference).first else { continue }

      let mergedIsActive = allRows.contains(where: \.isActive)
      let mostRecent = allRows.sorted(by: memberPreference)
      let merged = MLSMemberModel(
        memberID: canonicalID,
        conversationID: newID,
        currentUserDID: userDID,
        did: normalizedDID,
        handle: mostRecent.compactMap(\.handle).first,
        displayName: mostRecent.compactMap(\.displayName).first,
        leafIndex: mostRecent.first(where: { $0.leafIndex >= 0 })?.leafIndex ?? first.leafIndex,
        credentialData: mostRecent.compactMap(\.credentialData).first,
        signaturePublicKey: mostRecent.compactMap(\.signaturePublicKey).first,
        addedAt: allRows.map(\.addedAt).min() ?? first.addedAt,
        updatedAt: allRows.map(\.updatedAt).max() ?? first.updatedAt,
        removedAt: mergedIsActive ? nil : mostRecent.compactMap(\.removedAt).first,
        removedBy: mergedIsActive ? nil : mostRecent.compactMap(\.removedBy).first,
        removalReason: mergedIsActive ? nil : mostRecent.compactMap(\.removalReason).first,
        isActive: mergedIsActive,
        role: allRows.reduce(.member) { preferredRole($0, $1.role) },
        capabilities: mostRecent.compactMap(\.capabilities).first,
        avatarURL: mostRecent.compactMap(\.avatarURL).first
      )

      // Prefer an already-canonical primary key. Otherwise promote the most
      // recently updated alias. All other rows for this normalized DID are
      // merged and removed inside the same transaction.
      let keeper = canonicalRows.first(where: { $0.memberID == canonicalID })
        ?? canonicalRows.sorted(by: memberPreference).first
        ?? aliasRows.sorted(by: memberPreference).first
      guard let keeper else { continue }

      if keeper.memberID != canonicalID {
        try db.execute(
          sql: "UPDATE MLSMemberModel SET memberID = ? WHERE memberID = ? AND currentUserDID = ?",
          arguments: [canonicalID, keeper.memberID, userDID]
        )
      }
      try merged.update(db)

      for row in allRows where row.memberID != canonicalID {
        try db.execute(
          sql: "DELETE FROM MLSMemberModel WHERE memberID = ? AND currentUserDID = ?",
          arguments: [row.memberID, userDID]
        )
      }
    }
  }

  private static func memberPreference(_ lhs: MLSMemberModel, _ rhs: MLSMemberModel) -> Bool {
    if lhs.updatedAt != rhs.updatedAt { return lhs.updatedAt > rhs.updatedAt }
    return lhs.memberID > rhs.memberID
  }

  static func preferredRole(
    _ lhs: MLSMemberModel.Role,
    _ rhs: MLSMemberModel.Role
  ) -> MLSMemberModel.Role {
    func rank(_ role: MLSMemberModel.Role) -> Int {
      switch role {
      case .member: return 0
      case .moderator: return 1
      case .admin: return 2
      }
    }
    return rank(rhs) > rank(lhs) ? rhs : lhs
  }

  static func migrateEpochKeys(
    in db: Database,
    from oldID: String,
    to newID: String,
    userDID: String
  ) throws {
    guard try db.tableExists(MLSEpochKeyModel.databaseTableName) else { return }
    let aliases = try MLSEpochKeyModel
      .filter(MLSEpochKeyModel.Columns.conversationID == oldID)
      .filter(MLSEpochKeyModel.Columns.currentUserDID == userDID)
      .fetchAll(db)
      .sorted {
        if $0.epoch != $1.epoch { return $0.epoch < $1.epoch }
        return $0.epochKeyID < $1.epochKeyID
      }
    let canonicals = try MLSEpochKeyModel
      .filter(MLSEpochKeyModel.Columns.conversationID == newID)
      .filter(MLSEpochKeyModel.Columns.currentUserDID == userDID)
      .fetchAll(db)
      .sorted {
        if $0.epoch != $1.epoch { return $0.epoch < $1.epoch }
        return $0.epochKeyID < $1.epochKeyID
      }

    // Preflight every epoch before changing a row.  This prevents a partial
    // crypto migration if an alias contains conflicting material.
    let grouped = Dictionary(grouping: aliases + canonicals, by: \.epoch)
    for (epoch, rows) in grouped {
      guard let first = rows.first else { continue }
      if rows.dropFirst().contains(where: { $0.keyMaterial != first.keyMaterial }) {
        throw MLSStorageError.epochSecretConflict(conversationID: newID, epoch: UInt64(max(0, epoch)))
      }
    }

    for alias in aliases {
      let matches = canonicals.filter { $0.epoch == alias.epoch }
      if let keeper = matches.first {
        if alias.isActive && !keeper.isActive {
          try db.execute(
            sql: "UPDATE MLSEpochKeyModel SET isActive = 1, deletedAt = NULL WHERE epochKeyID = ?",
            arguments: [keeper.epochKeyID]
          )
        }
        try db.execute(
          sql: "DELETE FROM MLSEpochKeyModel WHERE epochKeyID = ?",
          arguments: [alias.epochKeyID]
        )
      } else {
        try db.execute(
          sql: "UPDATE MLSEpochKeyModel SET conversationID = ? WHERE epochKeyID = ?",
          arguments: [newID, alias.epochKeyID]
        )
      }
    }

    // Remove only duplicate rows whose secret is byte-for-byte identical.
    // Differing rows were rejected in the preflight above. Query again after
    // alias updates so two identical alias rows cannot become unordered
    // duplicates when there was no canonical row before migration.
    let migratedRows = try MLSEpochKeyModel
      .filter(MLSEpochKeyModel.Columns.conversationID == newID)
      .filter(MLSEpochKeyModel.Columns.currentUserDID == userDID)
      .fetchAll(db)
      .sorted {
        if $0.epoch != $1.epoch { return $0.epoch < $1.epoch }
        return $0.epochKeyID < $1.epochKeyID
      }
    for (_, rows) in Dictionary(grouping: migratedRows, by: \.epoch) {
      for duplicate in rows.dropFirst() {
        try db.execute(
          sql: "DELETE FROM MLSEpochKeyModel WHERE epochKeyID = ?",
          arguments: [duplicate.epochKeyID]
        )
      }
    }
  }
}
