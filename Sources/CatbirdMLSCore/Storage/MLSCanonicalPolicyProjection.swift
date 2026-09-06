import Foundation
import CoreFoundation
import GRDB
import Petrel
import PetrelCatbird

/// Durable display/consent projection of the canonical policy roster. This
/// table does not authorize MLS access or retire recovery/terminal evidence.
enum MLSCanonicalPolicyProjection {
  struct Update {
    let json: String
    let state: BlueCatbirdChatDefs.ConversationState
    let requestState: MLSRequestState
  }

  enum ValidationError: Error {
    case invalidSnapshot
  }

  static func createTable(in db: Database) throws {
    try db.execute(sql: """
      CREATE TABLE IF NOT EXISTS mls_orchestrator_canonical_policy (
        user_did TEXT NOT NULL,
        conversation_id TEXT NOT NULL,
        generation INTEGER NOT NULL,
        state_version INTEGER NOT NULL,
        canonical_state_json TEXT NOT NULL,
        terminal_invitation_authorized INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (user_did, conversation_id)
      )
      """)
  }

  static func decode(_ json: String, conversationID: String, groupID: Data, maximumEpoch: Int64? = nil) throws
    -> BlueCatbirdChatDefs.ConversationState
  {
    let data = Data(json.utf8)
    try validateReadRoutingExtensions(data)
    let state = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationState.self, from: data)
    let coordinates = state.coordinates
    let maximum = 9_007_199_254_740_991
    guard isCanonicalUUID(conversationID),
          coordinates.conversationId == conversationID,
          coordinates.groupId.data == groupID, groupID.count == 32,
          coordinates.epoch >= 0, coordinates.epoch <= maximum,
          maximumEpoch.map({ Int64(coordinates.epoch) <= $0 }) ?? true,
          (0...maximum).contains(coordinates.generation),
          (0...maximum).contains(coordinates.stateVersion),
          coordinates.groupContextHash.data.count == 32,
          coordinates.confirmationTag.data.count == 32,
          (0...maximum).contains(state.snapshotSeq),
          !state.participants.isEmpty else { throw ValidationError.invalidSnapshot }
    var previousDID: String?
    for participant in state.participants {
      let did = participant.userDid.description
      guard previousDID.map({ $0.utf8.lexicographicallyPrecedes(did.utf8) }) ?? true,
            participant.leafCount >= 0,
            participant.status == .value_pending || participant.status == .value_active else {
        throw ValidationError.invalidSnapshot
      }
      if participant.status == .value_pending {
        guard participant.leafCount == 0, participant.invitationProvenance != nil else {
          throw ValidationError.invalidSnapshot
        }
      }
      if let invitation = participant.invitationProvenance {
        guard isCanonicalUUID(invitation.invitationTransitionId),
              isCanonicalUUID(invitation.invitedByDeviceId) else { throw ValidationError.invalidSnapshot }
      }
      previousDID = did
    }
    return state
  }

  /// A nil legacy snapshot is handled by callers without invoking this method.
  /// A stale policy snapshot is ignored as a whole, before any roster writes.
  static func prepare(_ json: String, existing: MLSConversationModel, in db: Database) throws -> Update? {
    let state = try decode(json, conversationID: existing.conversationID,
      groupID: existing.groupID)
    guard existing.isActive, !existing.needsReset, existing.pendingNewGroupId == nil,
          state.coordinates.lifecycle == .value_active else { return nil }
    let terminal = try String.fetchOne(db,
      sql: "SELECT state FROM mls_orchestrator_terminal_access WHERE user_did = ? AND conversation_id = ?",
      arguments: [existing.currentUserDID, existing.conversationID])
    // The native display boundary admits a DeviceRemoved policy overlay only
    // after checking retained removal/exit evidence. This changes consent UI,
    // never the terminal crypto access row. Closed has no admission successor.
    guard terminal == nil || terminal == "device_removed" else { return nil }
    guard let participant = state.participants.first(where: { $0.userDid.description == existing.currentUserDID }) else {
      throw ValidationError.invalidSnapshot
    }
    if let previous = try storedState(userDID: existing.currentUserDID, conversationID: existing.conversationID, in: db) {
      let old = previous.coordinates
      let new = state.coordinates
      guard new.generation >= old.generation else { return nil }
      if new.generation == old.generation {
        guard new.groupId == old.groupId else { throw ValidationError.invalidSnapshot }
        guard new.stateVersion >= old.stateVersion else { return nil }
        guard new.epoch >= old.epoch else { return nil }
        // Directory revocation is a monotonic display overlay: it does not
        // advance conversation policy version, epoch, or application sequence.
        let hasStaleRevocation = previous.leaves.contains { oldLeaf in
          oldLeaf.deviceStatus == .value_revoked && state.leaves.contains {
            sameLeafIdentity($0, oldLeaf) && $0.deviceStatus != .value_revoked
          }
        }
        if new.stateVersion == old.stateVersion {
          // Routing hints are not policy authority. Swift API reads retain
          // them; the native display codec strips them after validation. Their
          // presence or value cannot change equal-version policy equivalence.
          // Same policy version cannot change consent or the signed coordinates.
          guard new == old, state.participants == previous.participants,
                hasOnlyDirectoryStatusChanges(from: previous.leaves, to: state.leaves),
                state.conversationKind == previous.conversationKind,
                state.cipherSuite == previous.cipherSuite, state.metadataSnapshot == previous.metadataSnapshot else {
            throw ValidationError.invalidSnapshot
          }
          guard state.snapshotSeq >= previous.snapshotSeq else { return nil }
        }
        // Ignore stale active directory echoes without throwing or undoing
        // consent. Native may next publish its retained-revoked merged page.
        guard !hasStaleRevocation else { return nil }
      } else if new.groupId == old.groupId {
        throw ValidationError.invalidSnapshot
      }
    }
    return Update(json: json, state: state,
      requestState: participant.status == .value_pending ? .pendingInbound : .none)
  }

  /// Match the native read-only routing domain before the generated Swift
  /// decoder can degrade malformed optional fields to nil. Keep the original
  /// JSON untouched; these hints never participate in policy equivalence.
  private static func validateReadRoutingExtensions(_ data: Data) throws {
    guard let fields = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
      throw ValidationError.invalidSnapshot
    }
    if let value = fields["sequencerDid"], !(value is NSNull) {
      guard let text = value as? String, (try? DID(didString: text)) != nil else {
        throw ValidationError.invalidSnapshot
      }
    }
    if let value = fields["sequencerTerm"], !(value is NSNull) {
      guard let number = value as? NSNumber,
            CFGetTypeID(number) != CFBooleanGetTypeID() else { throw ValidationError.invalidSnapshot }
      // JSONDecoder's Int accepts integral floating-point notation; native
      // serde Number.as_u64 does not. Foundation preserves that lexical class.
      let numberType = String(cString: number.objCType)
      guard numberType != "f", numberType != "d",
            (0...9_007_199_254_740_991).contains(number.int64Value) else {
        throw ValidationError.invalidSnapshot
      }
    }
  }

  private static func sameLeafIdentity(_ lhs: BlueCatbirdChatDefs.DeviceLeafView,
                                       _ rhs: BlueCatbirdChatDefs.DeviceLeafView) -> Bool {
    lhs.userDid == rhs.userDid && lhs.deviceId == rhs.deviceId && lhs.keyId == rhs.keyId
      && lhs.leafOrigin == rhs.leafOrigin && lhs.joinKeyPackageRef == rhs.joinKeyPackageRef
  }

  private static func hasOnlyDirectoryStatusChanges(from old: [BlueCatbirdChatDefs.DeviceLeafView],
                                             to new: [BlueCatbirdChatDefs.DeviceLeafView]) -> Bool {
    guard old.count == new.count else { return false }
    return zip(old, new).allSatisfy { prior, next in
      sameLeafIdentity(prior, next) && (prior.deviceStatus == next.deviceStatus
        || (prior.deviceStatus == .value_active && next.deviceStatus == .value_revoked)
        || (prior.deviceStatus == .value_revoked && next.deviceStatus == .value_active))
    }
  }

  static func persist(_ update: Update, userDID: String, terminalInvitationAuthorized: Bool = false, in db: Database) throws {
    try createTable(in: db)
    let coordinate = update.state.coordinates
    try db.execute(sql: """
      INSERT INTO mls_orchestrator_canonical_policy
        (user_did, conversation_id, generation, state_version, canonical_state_json, terminal_invitation_authorized)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(user_did, conversation_id) DO UPDATE SET
        generation = excluded.generation, state_version = excluded.state_version,
        canonical_state_json = excluded.canonical_state_json,
        terminal_invitation_authorized = excluded.terminal_invitation_authorized
      """, arguments: [userDID, coordinate.conversationId, coordinate.generation, coordinate.stateVersion,
        update.json, terminalInvitationAuthorized])
  }

  static func clearTerminalInvitation(userDID: String, conversationID: String, in db: Database) throws {
    guard try db.tableExists("mls_orchestrator_canonical_policy") else { return }
    try db.execute(sql: "UPDATE mls_orchestrator_canonical_policy SET terminal_invitation_authorized = 0 WHERE user_did = ? AND conversation_id = ? AND terminal_invitation_authorized != 0",
      arguments: [userDID, conversationID])
  }

  static func retireConsent(userDID: String, conversationID: String, in db: Database) throws {
    try db.execute(sql: "UPDATE MLSConversationModel SET requestState = 'none' WHERE currentUserDID = ? AND conversationID = ? AND requestState != 'none'",
      arguments: [userDID, conversationID])
    try clearTerminalInvitation(userDID: userDID, conversationID: conversationID, in: db)
  }

  static func hasPendingConsent(_ model: MLSConversationModel, in db: Database) throws -> Bool {
    guard model.isActive, model.requestState == .pendingInbound,
          !model.needsReset, model.pendingNewGroupId == nil else { return false }
    let terminal = try db.tableExists("mls_orchestrator_terminal_access")
      ? String.fetchOne(db, sql: "SELECT state FROM mls_orchestrator_terminal_access WHERE user_did = ? AND conversation_id = ?",
        arguments: [model.currentUserDID, model.conversationID]) : nil
    guard let terminal else { return true }
    guard terminal == "device_removed", try db.tableExists("mls_orchestrator_canonical_policy") else { return false }
    guard try Bool.fetchOne(db, sql: "SELECT terminal_invitation_authorized FROM mls_orchestrator_canonical_policy WHERE user_did = ? AND conversation_id = ?",
      arguments: [model.currentUserDID, model.conversationID]) == true,
      let policy = try storedState(userDID: model.currentUserDID, conversationID: model.conversationID, in: db),
      policy.coordinates.groupId.data == model.groupID,
      Int64(policy.coordinates.epoch) <= model.epoch else { return false }
    // A retained G0 invitation cannot reappear after a verified G1 reset
    // clears its live flags. Only a new native display projection can bind it.
    return policy.participants.first(where: { $0.userDid.description == model.currentUserDID })?.status == .value_pending
  }

  static func storedState(userDID: String, conversationID: String, in db: Database) throws
    -> BlueCatbirdChatDefs.ConversationState?
  {
    guard try db.tableExists("mls_orchestrator_canonical_policy"),
          let row = try Row.fetchOne(db, sql: """
            SELECT generation, state_version, canonical_state_json FROM mls_orchestrator_canonical_policy
            WHERE user_did = ? AND conversation_id = ?
            """, arguments: [userDID, conversationID]) else { return nil }
    let json: String = row["canonical_state_json"]
    let decoded = try JSONDecoder().decode(BlueCatbirdChatDefs.ConversationState.self, from: Data(json.utf8))
    let state = try decode(json, conversationID: conversationID,
      groupID: decoded.coordinates.groupId.data)
    guard state.coordinates.generation == (row["generation"] as Int),
          state.coordinates.stateVersion == (row["state_version"] as Int),
          state.participants.contains(where: { $0.userDid.description == userDID }) else {
      throw ValidationError.invalidSnapshot
    }
    return state
  }

  static func json(for model: MLSConversationModel, in db: Database) throws -> String? {
    guard let state = try storedState(userDID: model.currentUserDID, conversationID: model.conversationID, in: db),
          state.coordinates.groupId.data == model.groupID,
          Int64(state.coordinates.epoch) <= model.epoch else { return nil }
    return try String(decoding: JSONEncoder().encode(state), as: UTF8.self)
  }

  private static func isCanonicalUUID(_ value: String) -> Bool {
    let bytes = Array(value.utf8)
    guard bytes.count == 36, bytes[14] == 52, [56, 57, 97, 98].contains(bytes[19]),
          let uuid = UUID(uuidString: value) else { return false }
    return uuid.uuidString.lowercased() == value
  }
}

public extension MLSConversationModel {
  /// UI consent state with terminal/reset fences; does not grant MLS access.
  func hasPendingConsent(in database: Database) throws -> Bool {
    try MLSCanonicalPolicyProjection.hasPendingConsent(self, in: database)
  }
}
