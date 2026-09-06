import CryptoKit
import Foundation
import GRDB
import PetrelCatbird

/// Presentation only. Neither a pending hint nor a rendered receipt grants MLS access.
public enum MLSConversationLeavePresentation: Equatable, Sendable {
  case none
  case pending
  case checking
  case left
}

public enum MLSSystemMessagePresentation {
  public static func isSystem(_ payload: MLSMessagePayload) -> Bool {
    payload.version == 1 && payload.messageType == .system
  }

  public static func text(_ payload: MLSMessagePayload, verifiedAccountLeave: Bool = false) -> String {
    guard isSystem(payload) else { return payload.text ?? "" }
    switch payload.text {
    case "history_boundary.new_member": return "You joined this conversation"
    case "history_boundary.device_rejoined": return "Messages before this point aren't available on this device"
    case "membership.left": return verifiedAccountLeave ? "You left this conversation" : "Conversation membership changed"
    case "conversation.closed": return "Conversation status changed"
    default: return payload.text ?? ""
    }
  }

  public static func isVerifiedAccountLeave(
    payload: MLSMessagePayload,
    messageID: String,
    senderDID: String,
    currentUserDID: String,
    currentDeviceID: String,
    terminalState: String?
  ) -> Bool {
    guard isSystem(payload), payload.text == "membership.left",
          senderDID == currentUserDID,
          ["device_removed", "closed"].contains(terminalState ?? ""),
          canonicalUUID(currentDeviceID) else { return false }
    let components = messageID.split(separator: ":", omittingEmptySubsequences: false)
    guard components.count == 3,
          components[0] == "membership-left",
          canonicalUUID(String(components[1])),
          components[2] == currentDeviceID else { return false }
    return true
  }

  internal static func canonicalUUID(_ value: String) -> Bool {
    guard let uuid = UUID(uuidString: value), uuid.uuidString.lowercased() == value else { return false }
    let bytes = uuid.uuid
    return bytes.6 >> 4 == 4 && bytes.8 & 0xc0 == 0x80
  }
}

internal struct MLSPendingLeaveHint: Codable, Equatable, Sendable {
  let userDID: String
  let conversationID: String
  let request: BlueCatbirdChatDefs.LeaveRequestView
  let fetchedAt: Date

  var expiresAt: Date { request.expiresAt.date }

  func presentation(now: Date) -> MLSConversationLeavePresentation {
    guard expiresAt > now else { return .checking }
    return .pending
  }

  static func date(_ value: String) -> Date? {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return formatter.date(from: value) ?? ISO8601DateFormatter().date(from: value)
  }

  static func validated(
    output: BlueCatbirdChatGetConversationState.Output,
    userDID: String,
    conversationID: String,
    now: Date
  ) throws -> MLSPendingLeaveHint? {
    guard MLSSystemMessagePresentation.canonicalUUID(conversationID),
          output.state.coordinates.conversationId == conversationID else {
      throw MLSConversationError.operationFailed("Could not verify leave status.")
    }
    let own = output.pendingLeaveRequests.filter { $0.requesterDid.didString() == userDID }
    guard own.count <= 1 else { throw MLSConversationError.operationFailed("Could not verify leave status.") }
    guard let request = own.first else { return nil }
    guard request.conversationId == conversationID,
          request.prior.conversationId == conversationID,
          MLSSystemMessagePresentation.canonicalUUID(request.leaveRequestId),
          MLSSystemMessagePresentation.canonicalUUID(request.requesterDeviceId),
          request.prior.generation == output.state.coordinates.generation,
          request.prior.groupId == output.state.coordinates.groupId,
          request.prior.stateVersion > 0,
          request.prior.stateVersion <= output.state.coordinates.stateVersion,
          request.prior.epoch >= 0,
          request.requestedAt.date.timeIntervalSince1970.isFinite,
          request.expiresAt.date.timeIntervalSince1970.isFinite,
          request.requestedAt.date < request.expiresAt.date else {
      throw MLSConversationError.operationFailed("Could not verify leave status.")
    }
    guard request.status == .value_pending else { return nil }
    return MLSPendingLeaveHint(userDID: userDID, conversationID: conversationID, request: request, fetchedAt: now)
  }
}

/// A small account/conversation-scoped display hint in the clean storage namespace.
/// Server-read failures never erase it, and expiry never asserts that leaving succeeded.
internal actor MLSPendingLeaveHintStore {
  static let shared = MLSPendingLeaveHintStore(defaults: UserDefaults(suiteName: MLSStoragePaths.appGroupIdentifier))
  private let defaults: UserDefaults?

  init(defaults: UserDefaults?) { self.defaults = defaults }

  private func key(userDID: String, conversationID: String) -> String {
    let binding = "\(MLSStoragePaths.cleanSuffix)\0\(userDID)\0\(conversationID)"
    let digest = SHA256.hash(data: Data(binding.utf8)).map { String(format: "%02x", $0) }.joined()
    return "mls.leave-hint.\(MLSStoragePaths.cleanSuffix).\(digest)"
  }

  func load(userDID: String, conversationID: String) -> MLSPendingLeaveHint? {
    guard let data = defaults?.data(forKey: key(userDID: userDID, conversationID: conversationID)),
          let hint = try? JSONDecoder().decode(MLSPendingLeaveHint.self, from: data),
          hint.userDID == userDID, hint.conversationID == conversationID,
          hint.request.conversationId == conversationID,
          hint.request.requesterDid.didString() == userDID else { return nil }
    return hint
  }

  func replace(_ hint: MLSPendingLeaveHint?, userDID: String, conversationID: String) throws {
    let key = key(userDID: userDID, conversationID: conversationID)
    guard let defaults else { throw MLSConversationError.operationFailed("Could not save leave status.") }
    if let hint {
      guard hint.userDID == userDID, hint.conversationID == conversationID else {
        throw MLSConversationError.operationFailed("Could not save leave status.")
      }
      defaults.set(try JSONEncoder().encode(hint), forKey: key)
    } else {
      defaults.removeObject(forKey: key)
    }
  }
}

extension MLSConversationManager {
  /// Read the local native receipt. It is created only by verified account removal;
  /// canonical incoming ApplicationEntry IDs cannot use this reserved namespace.
  public func hasVerifiedAccountLeave(conversationID: String) async -> Bool {
    guard let userDid else { return false }
    do {
      let terminal = try await database.read { db in
        try String.fetchOne(db,
          sql: "SELECT t.state FROM mls_orchestrator_terminal_access t JOIN MLSConversationModel c ON c.currentUserDID = t.user_did AND c.conversationID = t.conversation_id AND c.groupID = t.group_id WHERE t.user_did = ? AND t.conversation_id = ?",
          arguments: [userDid, conversationID])
      }
      guard terminal == "device_removed" || terminal == "closed",
            let deviceID = try? MLSOrchestratorCredentialAdapter().getDeviceUuid(userDid: userDid),
            let context = try? await MLSCoreContext.shared.getContext(for: userDid) else { return false }
      return try await database.read { db in
        let terminal = try String.fetchOne(db,
          sql: "SELECT t.state FROM mls_orchestrator_terminal_access t JOIN MLSConversationModel c ON c.currentUserDID = t.user_did AND c.conversationID = t.conversation_id AND c.groupID = t.group_id WHERE t.user_did = ? AND t.conversation_id = ?",
          arguments: [userDid, conversationID])
        guard terminal == "device_removed" || terminal == "closed" else { return false }
        let models = try MLSMessageModel
          .filter(MLSMessageModel.Columns.currentUserDID == userDid)
          .filter(MLSMessageModel.Columns.conversationID == conversationID)
          .filter(MLSMessageModel.Columns.senderID == userDid)
          .filter(MLSMessageModel.Columns.messageID.like("membership-left:%"))
          .fetchAll(db)
        return models.contains { model in
          guard let payload = model.decryptedPayload(context: context) else { return false }
          return MLSSystemMessagePresentation.isVerifiedAccountLeave(
            payload: payload, messageID: model.messageID, senderDID: model.senderID,
            currentUserDID: userDid, currentDeviceID: deviceID, terminalState: terminal)
        }
      }
    } catch { return false }
  }

  public func conversationLeavePresentation(conversationID: String, refresh: Bool = false) async -> MLSConversationLeavePresentation {
    guard let userDid else { return .none }
    let generation = sessionGeneration
    let store = MLSPendingLeaveHintStore.shared
    if await hasVerifiedAccountLeave(conversationID: conversationID) {
      guard generation == sessionGeneration else { return .none }
      try? await store.replace(nil, userDID: userDid, conversationID: conversationID)
      return generation == sessionGeneration ? .left : .none
    }
    if refresh, protocolAuthorityMode == .rustFull {
      do {
        let output = try await apiClient.getCanonicalConversationState(conversationId: conversationID)
        try validateSessionGeneration(capturedGeneration: generation)
        let hint = try MLSPendingLeaveHint.validated(output: output, userDID: userDid, conversationID: conversationID, now: Date())
        try await store.replace(hint, userDID: userDid, conversationID: conversationID)
      } catch {
        logger.debug("Leave status refresh unavailable; preserving the saved display hint")
      }
    }
    let hint = await store.load(userDID: userDid, conversationID: conversationID)
    guard generation == sessionGeneration else { return .none }
    return hint?.presentation(now: Date()) ?? .none
  }
}
