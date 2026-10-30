import CatbirdMLS
import CoreFoundation
import Foundation
import GRDB
import PetrelCatbird

/// Host-side transaction for Rust-authorized canonical conversation projection.
/// Rust establishes protocol authority; this layer rechecks account, identity,
/// incarnation, and lifecycle fences against the durable Apple store before it
/// changes presentation state.
enum MLSConversationProjectionCommit {
    enum CommitError: Error {
        case invalidRequest(String)
        case protectedStorage(String)
    }

    enum Mode: String, Codable {
        case live
        case replay
    }

    struct Lifecycle: Codable, Equatable {
        let generation: Int64?
        let stateVersion: Int64?
        let resetGeneration: Int32?
        let resetPendingTargetGroupId: String?

        var isUnknown: Bool {
            generation == nil && stateVersion == nil && resetGeneration == nil
                && resetPendingTargetGroupId == nil
        }
    }

    struct Alias: Codable, Equatable {
        let ownerUserDid: String
        let canonicalConversationId: String
        let canonicalGroupId: String
        let aliasConversationId: String
        let aliasGroupId: String
        let canonicalIncarnation: UInt64
        let aliasIncarnation: UInt64
        let canonicalLifecycle: Lifecycle
        let aliasLifecycle: Lifecycle
    }

    private struct RequestHeader: Codable {
        let ownerUserDid: String
        let mode: Mode
        let aliases: [Alias]
        let canonicalLifecycle: Lifecycle
    }

    struct Outcome: Codable, Equatable {
        let type: String
        let canonicalIds: [String]?
        let affectedRows: UInt32

        static func retired(_ rows: UInt32) -> Self {
            Self(type: "retired", canonicalIds: nil, affectedRows: rows)
        }

        static func alreadyAbsent(_ rows: UInt32) -> Self {
            Self(type: "alreadyAbsent", canonicalIds: nil, affectedRows: rows)
        }

        static var fenceMismatch: Self {
            Self(type: "fenceMismatch", canonicalIds: nil, affectedRows: 0)
        }

        static func ambiguous(_ ids: [String]) -> Self {
            Self(type: "ambiguous", canonicalIds: ids, affectedRows: 0)
        }
    }

    struct CanonicalView {
        let object: [String: Any]
        let bytes: Data
        let conversationID: String
        let groupHex: String
        let groupID: Data
        let epoch: UInt64
        let canonicalStateJSON: String?
        let canonicalState: BlueCatbirdChatDefs.ConversationState?
        let createdAt: Date?
        let updatedAt: Date?
    }

    static func createTables(in db: Database) throws {
        try db.execute(sql: """
        CREATE TABLE IF NOT EXISTS mls_conversation_projection_canonical (
          owner_did TEXT NOT NULL,
          conversation_id TEXT NOT NULL,
          group_id BLOB NOT NULL,
          incarnation INTEGER NOT NULL,
          payload_encrypted BLOB NOT NULL,
          updated_at DATETIME NOT NULL,
          PRIMARY KEY (owner_did, conversation_id)
        )
        """)
        try db.execute(sql: """
        CREATE TABLE IF NOT EXISTS mls_conversation_projection_alias_retirement (
          owner_did TEXT NOT NULL,
          alias_conversation_id TEXT NOT NULL,
          canonical_conversation_id TEXT NOT NULL,
          group_id BLOB NOT NULL,
          canonical_incarnation INTEGER NOT NULL,
          alias_incarnation INTEGER NOT NULL,
          payload_encrypted BLOB NOT NULL,
          created_at DATETIME NOT NULL,
          PRIMARY KEY (owner_did, alias_conversation_id)
        )
        """)
        try db.execute(sql: """
        CREATE INDEX IF NOT EXISTS idx_conversation_projection_alias_canonical
        ON mls_conversation_projection_alias_retirement(owner_did, canonical_conversation_id)
        """)
    }

    static func isRetiredAlias(ownerDID: String, conversationID: String, in db: Database) throws -> Bool {
        guard try db.tableExists("mls_conversation_projection_alias_retirement") else { return false }
        return try Bool.fetchOne(db, sql: """
        SELECT EXISTS(
          SELECT 1 FROM mls_conversation_projection_alias_retirement
          WHERE owner_did = ? AND alias_conversation_id = ?
        )
        """, arguments: [ownerDID, conversationID]) == true
    }

    static func canonicalIDForRetiredAlias(
        ownerDID: String,
        conversationID: String,
        in db: Database
    ) throws -> String? {
        guard try db.tableExists("mls_conversation_projection_alias_retirement") else { return nil }
        return try String.fetchOne(db, sql: """
        SELECT canonical_conversation_id
        FROM mls_conversation_projection_alias_retirement
        WHERE owner_did = ? AND alias_conversation_id = ?
        """, arguments: [ownerDID, conversationID])
    }

    static func commit(
        requestJSON: String,
        adapterUserDID: String,
        dbPool: DatabasePool,
        context: MlsContext
    ) throws -> String {
        guard requestJSON.utf8.count <= 1_048_576 else {
            throw CommitError.invalidRequest("Projection request exceeds limit")
        }
        let requestData = Data(requestJSON.utf8)
        let header = try JSONDecoder().decode(RequestHeader.self, from: requestData)
        let owner = MLSStorageHelpers.normalizeDID(header.ownerUserDid)
        guard !owner.isEmpty,
              owner == MLSStorageHelpers.normalizeDID(adapterUserDID),
              owner == header.ownerUserDid
        else { throw CommitError.invalidRequest("Projection owner mismatch") }

        guard let root = try JSONSerialization.jsonObject(with: requestData) as? [String: Any] else {
            throw CommitError.invalidRequest("Invalid projection JSON")
        }
        let canonicalObject = root["canonical"] as? [String: Any]
        guard (header.mode == .live) == (canonicalObject != nil) else {
            throw CommitError.invalidRequest("Projection mode mismatch")
        }

        let outcome = try dbPool.write { db -> Outcome in
            try createTables(in: db)
            let canonicalID = try canonicalObject.flatMap { try parseView($0, owner: owner).conversationID }
                ?? header.aliases.first?.canonicalConversationId
            guard let canonicalID,
                  MLSStorageHelpers.isCanonicalUUIDv4(canonicalID)
            else { throw CommitError.invalidRequest("Invalid canonical identity") }

            let previous = try readCanonical(
                owner: owner,
                conversationID: canonicalID,
                in: db,
                context: context
            )
            let supplied = try canonicalObject.map { try parseView($0, owner: owner) }
            guard let view = supplied ?? previous else {
                throw CommitError.invalidRequest("Replay requires a durable canonical projection")
            }
            guard view.conversationID == canonicalID else {
                throw CommitError.invalidRequest("Canonical projection identity mismatch")
            }

            if let state = view.canonicalState {
                let coordinates = state.coordinates
                guard coordinates.conversationId == canonicalID,
                      coordinates.groupId.data == view.groupID,
                      coordinates.epoch >= 0,
                      UInt64(coordinates.epoch) == view.epoch,
                      state.participants.contains(where: { $0.userDid.description == owner })
                else { throw CommitError.invalidRequest("Canonical state identity mismatch") }
                if header.mode == .live {
                    guard header.canonicalLifecycle.generation == Int64(coordinates.generation),
                          header.canonicalLifecycle.stateVersion == Int64(coordinates.stateVersion)
                    else { throw CommitError.invalidRequest("Canonical coordinate fence mismatch") }
                }
            } else if !header.canonicalLifecycle.isUnknown {
                throw CommitError.invalidRequest("Canonical lifecycle requires policy state")
            }

            if let oldState = previous?.canonicalState {
                guard let newState = view.canonicalState else {
                    throw CommitError.invalidRequest("Cannot discard retained canonical policy")
                }
                let old = oldState.coordinates
                let new = newState.coordinates
                if (old.generation, old.stateVersion) > (new.generation, new.stateVersion)
                    || ((old.generation, old.stateVersion) == (new.generation, new.stateVersion)
                        && oldState != newState)
                    || (old.generation == new.generation && old.groupId != new.groupId)
                    || (old.lifecycle != .value_active && new.lifecycle == .value_active)
                {
                    return .fenceMismatch
                }
            }

            let canonicalRow = try conversation(owner: owner, id: canonicalID, in: db)
            let oldGeneration = previous?.canonicalState?.coordinates.generation
            let newGeneration = view.canonicalState?.coordinates.generation
            let generationAdvanced: Bool
            if let oldGeneration, let newGeneration {
                generationAdvanced = oldGeneration < newGeneration
            } else {
                generationAdvanced = false
            }
            if let row = canonicalRow {
                let resetAuthorized = row.needsReset
                    && header.canonicalLifecycle.resetGeneration.map(Int64.init) == row.pendingResetGeneration
                    && header.canonicalLifecycle.resetPendingTargetGroupId == row.pendingNewGroupId
                    && row.pendingNewGroupId == view.groupHex
                if try (row.needsReset && !resetAuthorized)
                    || (row.groupID != view.groupID && !generationAdvanced && !resetAuthorized)
                    || terminalBlocksActive(owner: owner, conversationID: canonicalID,
                                            incoming: view.canonicalState, in: db)
                {
                    return .fenceMismatch
                }
                if header.mode == .replay,
                   row.groupID != view.groupID || row.epoch < 0 || UInt64(row.epoch) != view.epoch
                   || !lifecycleMatches(row: row, state: previous?.canonicalState,
                                        fence: header.canonicalLifecycle)
                {
                    return .fenceMismatch
                }
            } else if header.mode == .replay {
                return .fenceMismatch
            }

            let stableRows = try MLSConversationModel
                .filter(MLSConversationModel.Columns.currentUserDID == owner)
                .filter(MLSConversationModel.Columns.groupID == view.groupID)
                .filter(MLSConversationModel.Columns.conversationID != canonicalID)
                .fetchAll(db)
                .map(\.conversationID)
                .filter(MLSStorageHelpers.isCanonicalUUIDv4)
            if !stableRows.isEmpty {
                return .ambiguous((stableRows + [canonicalID]).sorted())
            }

            if header.mode == .replay || !header.aliases.isEmpty {
                guard let row = canonicalRow,
                      row.epoch >= 0,
                      UInt64(row.epoch) == view.epoch,
                      let native = try Row.fetchOne(db, sql: """
                      SELECT epoch FROM mls_orchestrator_group_state
                      WHERE group_id = ?
                      """, arguments: [view.groupHex]),
                      (native["epoch"] as Int64?) == Int64(view.epoch)
                else { return .fenceMismatch }
            }

            var retirements: [(Alias, Data)] = []
            var seen = Set<String>()
            for alias in header.aliases {
                guard alias.ownerUserDid == owner,
                      alias.canonicalConversationId == canonicalID,
                      alias.canonicalGroupId == view.groupHex,
                      alias.aliasGroupId == view.groupHex,
                      alias.aliasConversationId == view.groupHex,
                      alias.aliasConversationId != canonicalID,
                      alias.canonicalIncarnation == view.epoch,
                      alias.aliasIncarnation <= alias.canonicalIncarnation,
                      alias.canonicalLifecycle == header.canonicalLifecycle,
                      seen.insert(alias.aliasConversationId).inserted
                else { throw CommitError.invalidRequest("Invalid alias retirement identity") }

                if let encrypted = try Data.fetchOne(db, sql: """
                SELECT payload_encrypted FROM mls_conversation_projection_alias_retirement
                WHERE owner_did = ? AND alias_conversation_id = ?
                """, arguments: [owner, alias.aliasConversationId]) {
                    let old = try decrypt(Alias.self, encrypted: encrypted,
                                          scope: aliasScope(owner: owner, aliasID: alias.aliasConversationId), context: context)
                    guard old == alias else { return .fenceMismatch }
                    continue
                }

                guard let row = try conversation(owner: owner, id: alias.aliasConversationId, in: db),
                      row.groupID == view.groupID,
                      row.epoch >= 0,
                      UInt64(row.epoch) == alias.aliasIncarnation
                else { return .fenceMismatch }
                let aliasState = try MLSCanonicalPolicyProjection.storedState(
                    userDID: owner,
                    conversationID: alias.aliasConversationId,
                    in: db
                )
                guard lifecycleMatches(row: row, state: aliasState, fence: alias.aliasLifecycle) else {
                    return .fenceMismatch
                }
                try retirements.append((alias, encrypt(alias,
                                                       scope: aliasScope(owner: owner, aliasID: alias.aliasConversationId), context: context)))
            }

            if header.mode == .live {
                guard let supplied else { throw CommitError.invalidRequest("Missing live projection") }
                let now = Date()
                let candidate = MLSConversationModel(
                    conversationID: canonicalID,
                    currentUserDID: owner,
                    groupID: supplied.groupID,
                    epoch: Int64(supplied.epoch),
                    joinMethod: canonicalRow?.joinMethod ?? .unknown,
                    joinEpoch: canonicalRow?.joinEpoch ?? 0,
                    title: canonicalRow?.title,
                    description: canonicalRow?.description,
                    avatarURL: canonicalRow?.avatarURL,
                    avatarImageData: canonicalRow?.avatarImageData,
                    createdAt: canonicalRow?.createdAt ?? supplied.createdAt ?? now,
                    updatedAt: supplied.updatedAt ?? now,
                    lastMessageAt: canonicalRow?.lastMessageAt,
                    lastMembershipChangeAt: canonicalRow?.lastMembershipChangeAt,
                    unacknowledgedMemberChanges: canonicalRow?.unacknowledgedMemberChanges ?? 0,
                    isActive: true,
                    needsRejoin: canonicalRow?.needsRejoin ?? false,
                    needsReset: false,
                    isUnrecoverable: canonicalRow?.isUnrecoverable ?? false,
                    rejoinRequestedAt: canonicalRow?.rejoinRequestedAt,
                    lastRecoveryAttempt: canonicalRow?.lastRecoveryAttempt,
                    consecutiveFailures: canonicalRow?.consecutiveFailures ?? 0,
                    isPlaceholder: false,
                    requestState: canonicalRow?.requestState ?? .none,
                    mutedUntil: canonicalRow?.mutedUntil,
                    pendingNewGroupId: nil,
                    pendingResetGeneration: canonicalRow?.pendingResetGeneration
                )

                var final = candidate
                if let stateJSON = supplied.canonicalStateJSON,
                   let update = try MLSCanonicalPolicyProjection.prepare(stateJSON, existing: candidate, in: db)
                {
                    final = candidate.withRequestState(update.requestState)
                    try MLSCanonicalPolicyProjection.persist(update, userDID: owner, in: db)
                } else if supplied.canonicalStateJSON != nil {
                    return .fenceMismatch
                }
                try final.save(db)
                let encrypted = try MLSFieldEncryption.encrypt(
                    context: context,
                    conversationID: canonicalScope(owner: owner, conversationID: canonicalID),
                    plaintext: supplied.bytes
                )
                try db.execute(sql: """
                INSERT INTO mls_conversation_projection_canonical
                  (owner_did, conversation_id, group_id, incarnation, payload_encrypted, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(owner_did, conversation_id) DO UPDATE SET
                  group_id = excluded.group_id,
                  incarnation = excluded.incarnation,
                  payload_encrypted = excluded.payload_encrypted,
                  updated_at = excluded.updated_at
                """, arguments: [owner, canonicalID, supplied.groupID, supplied.epoch, encrypted, now])
            }

            for (alias, encrypted) in retirements {
                try db.execute(sql: """
                INSERT INTO mls_conversation_projection_alias_retirement
                  (owner_did, alias_conversation_id, canonical_conversation_id, group_id,
                   canonical_incarnation, alias_incarnation, payload_encrypted, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, arguments: [owner, alias.aliasConversationId, canonicalID, view.groupID,
                                 alias.canonicalIncarnation, alias.aliasIncarnation, encrypted, Date()])
            }

            let affected = UInt32(retirements.count) + (header.mode == .live ? 1 : 0)
            return retirements.isEmpty ? .alreadyAbsent(affected) : .retired(affected)
        }
        return try String(decoding: JSONEncoder().encode(outcome), as: UTF8.self)
    }

    private static func parseView(_ object: [String: Any], owner: String) throws -> CanonicalView {
        guard let conversationID = object["conversationId"] as? String,
              let groupHex = object["groupId"] as? String,
              groupHex == groupHex.lowercased(),
              let groupID = Data(hexEncoded: groupHex), groupID.count == 32,
              let epoch = unsignedInteger(object["epoch"])
        else { throw CommitError.invalidRequest("Invalid canonical view") }
        let bytes = try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
        var stateJSON: String?
        var state: BlueCatbirdChatDefs.ConversationState?
        if let stateObject = object["canonicalState"] as? [String: Any] {
            let data = try JSONSerialization.data(withJSONObject: stateObject, options: [.sortedKeys])
            let json = String(decoding: data, as: UTF8.self)
            state = try MLSCanonicalPolicyProjection.decode(
                json,
                conversationID: conversationID,
                groupID: groupID,
                maximumEpoch: Int64(epoch)
            )
            guard state?.participants.contains(where: { $0.userDid.description == owner }) == true else {
                throw CommitError.invalidRequest("Canonical projection excludes owner")
            }
            stateJSON = json
        }
        return CanonicalView(
            object: object,
            bytes: bytes,
            conversationID: conversationID,
            groupHex: groupHex,
            groupID: groupID,
            epoch: epoch,
            canonicalStateJSON: stateJSON,
            canonicalState: state,
            createdAt: date(object["createdAt"]),
            updatedAt: date(object["updatedAt"])
        )
    }

    private static func readCanonical(
        owner: String,
        conversationID: String,
        in db: Database,
        context: MlsContext
    ) throws -> CanonicalView? {
        guard let encrypted = try Data.fetchOne(db, sql: """
        SELECT payload_encrypted FROM mls_conversation_projection_canonical
        WHERE owner_did = ? AND conversation_id = ?
        """, arguments: [owner, conversationID]) else { return nil }
        let bytes = try MLSFieldEncryption.decrypt(
            context: context,
            conversationID: canonicalScope(owner: owner, conversationID: conversationID),
            wire: encrypted
        )
        guard let object = try JSONSerialization.jsonObject(with: bytes) as? [String: Any] else {
            throw CommitError.protectedStorage("Invalid protected canonical projection")
        }
        return try parseView(object, owner: owner)
    }

    private static func conversation(owner: String, id: String, in db: Database) throws
        -> MLSConversationModel?
    {
        try MLSConversationModel
            .filter(MLSConversationModel.Columns.currentUserDID == owner)
            .filter(MLSConversationModel.Columns.conversationID == id)
            .fetchOne(db)
    }

    private static func lifecycleMatches(
        row: MLSConversationModel,
        state: BlueCatbirdChatDefs.ConversationState?,
        fence: Lifecycle
    ) -> Bool {
        if let generation = fence.generation,
           Int64(state?.coordinates.generation ?? -1) != generation
        {
            return false
        }
        if let version = fence.stateVersion,
           Int64(state?.coordinates.stateVersion ?? -1) != version
        {
            return false
        }
        if let reset = fence.resetGeneration,
           row.pendingResetGeneration != Int64(reset)
        {
            return false
        }
        if let target = fence.resetPendingTargetGroupId,
           row.pendingNewGroupId != target
        {
            return false
        }
        if row.needsReset || row.pendingNewGroupId != nil {
            return fence.resetGeneration != nil && fence.resetPendingTargetGroupId != nil
        }
        return true
    }

    private static func terminalBlocksActive(
        owner: String,
        conversationID: String,
        incoming: BlueCatbirdChatDefs.ConversationState?,
        in db: Database
    ) throws -> Bool {
        guard try db.tableExists("mls_orchestrator_terminal_access"),
              let terminal = try String.fetchOne(db, sql: """
              SELECT state FROM mls_orchestrator_terminal_access
              WHERE user_did = ? AND conversation_id = ?
              """, arguments: [owner, conversationID])
        else { return false }
        return (terminal == "closed" || terminal == "device_removed")
            && (incoming == nil || incoming?.coordinates.lifecycle == .value_active)
    }

    private static func encrypt<T: Encodable>(_ value: T, scope: String, context: MlsContext) throws -> Data {
        try MLSFieldEncryption.encrypt(
            context: context,
            conversationID: scope,
            plaintext: JSONEncoder().encode(value)
        )
    }

    private static func decrypt<T: Decodable>(
        _ type: T.Type,
        encrypted: Data,
        scope: String,
        context: MlsContext
    ) throws -> T {
        let bytes = try MLSFieldEncryption.decrypt(context: context, conversationID: scope, wire: encrypted)
        return try JSONDecoder().decode(type, from: bytes)
    }

    private static func canonicalScope(owner: String, conversationID: String) -> String {
        "conversation-projection:\(owner):canonical:\(conversationID)"
    }

    private static func aliasScope(owner: String, aliasID: String) -> String {
        "conversation-projection:\(owner):alias:\(aliasID)"
    }

    private static func unsignedInteger(_ value: Any?) -> UInt64? {
        guard let number = value as? NSNumber,
              CFGetTypeID(number) != CFBooleanGetTypeID(),
              number.doubleValue.rounded() == number.doubleValue,
              number.doubleValue >= 0,
              number.doubleValue <= 9_007_199_254_740_991
        else { return nil }
        return number.uint64Value
    }

    private static func date(_ value: Any?) -> Date? {
        guard let text = value as? String else { return nil }
        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return fractional.date(from: text) ?? ISO8601DateFormatter().date(from: text)
    }
}
