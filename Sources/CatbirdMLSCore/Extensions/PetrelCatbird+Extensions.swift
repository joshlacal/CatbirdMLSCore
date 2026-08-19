//
//  PetrelCatbird+Extensions.swift
//  CatbirdMLSCore
//
//  Canonical clean-chat schema extensions for Apple Swift client layer
//

import Foundation
import Petrel
import PetrelCatbird

extension BlueCatbirdChatDefs.ApplicationEntry {
    public var id: String { entryId }
    public var convoId: String { conversationId }
    public var createdAt: BlueCatbirdChatDefs.CanonicalDatetime { receivedAt }
    public var parsedBody: BlueCatbirdChatDefs.ApplicationSendBody? {
        guard case let .blueCatbirdChatDefsApplicationSendBody(body) = signedRequest.body else {
            return nil
        }
        return body
    }
    public var epoch: Int {
        parsedBody?.prior.epoch ?? 0
    }
    public var senderDID: String {
        parsedBody?.actorDid.description ?? ""
    }
    public var ciphertext: Data {
        parsedBody?.applicationMessage.bytes.data ?? Data()
    }

    public init(
        convoId: String,
        id: String,
        senderDid: DID,
        senderDeviceDid: String = "",
        senderSeq: Int = 1,
        ciphertext: Bytes,
        epoch: Int = 0,
        seq: Int = 0,
        createdAt: BlueCatbirdChatDefs.CanonicalDatetime = ATProtocolDate(date: Date())
    ) {
        let priorContext = BlueCatbirdChatDefs.MlsAadPriorContext(
            conversationId: Bytes(data: Data(convoId.utf8)),
            generation: 1,
            stateVersion: 1,
            groupId: Bytes(data: Data()),
            epoch: epoch,
            groupContextHash: Bytes(data: Data()),
            confirmationTag: Bytes(data: Data()),
            lifecycle: "active"
        )
        let priorCoords = BlueCatbirdChatDefs.ConversationCoordinates(
            conversationId: convoId,
            generation: 1,
            stateVersion: 1,
            groupId: Bytes(data: Data()),
            epoch: epoch,
            groupContextHash: Bytes(data: Data()),
            confirmationTag: Bytes(data: Data()),
            lifecycle: .value_active
        )
        let aad = BlueCatbirdChatDefs.ApplicationAad(
            protocolVersion: .value_1,
            conversationId: Bytes(data: Data(convoId.utf8)),
            generation: 1,
            messageId: Bytes(data: Data(id.utf8)),
            prior: priorContext
        )
        let messagePayload = BlueCatbirdChatDefs.PrivateApplicationMessage(
            framing: "mls",
            contentType: "application/mls-message",
            bytes: ciphertext,
            sha256: Bytes(data: Data())
        )
        let body = BlueCatbirdChatDefs.ApplicationSendBody(
            signatureDomain: "applicationSend",
            messageId: id,
            actorDid: senderDid,
            actorDeviceId: senderDeviceDid,
            keyId: "k1",
            authGeneration: 1,
            prior: priorCoords,
            aad: aad,
            applicationMessage: messagePayload,
            blobBindings: [],
            signedAt: createdAt
        )
        let signedRequest = BlueCatbirdChatDefs.SignedApplicationSend(
            body: .blueCatbirdChatDefsApplicationSendBody(body),
            signature: Bytes(data: Data())
        )
        self.init(
            entryId: id,
            conversationId: convoId,
            seq: seq,
            signedRequest: signedRequest,
            receivedAt: createdAt
        )
    }
}

extension BlueCatbirdChatDefs.CommitEntry {
    public var id: String { entryId }
    public var convoId: String { conversationId }
    public var createdAt: BlueCatbirdChatDefs.CanonicalDatetime { receivedAt }
    public var parsedBody: BlueCatbirdChatDefs.CommitTransitionBody? {
        guard case let .blueCatbirdChatDefsCommitTransitionBody(body) = signedRequest.body else {
            return nil
        }
        return body
    }
    public var epoch: Int {
        parsedBody?.prior.epoch ?? 0
    }
    public var commitData: Data {
        parsedBody?.commit.bytes.data ?? Data()
    }
    public var ciphertext: Data {
        commitData
    }
}

extension BlueCatbirdChatDefs.KeyPackageArtifact {
    public var keyPackage: Data { bytes.data }
    public var keyPackageHash: String? {
        keyPackageRef.data.isEmpty ? nil : keyPackageRef.data.hexEncodedString()
    }
}

extension BlueCatbirdChatDefs.ParticipantView {
    public var did: DID { userDid }
}

extension BlueCatbirdChatDefs.ConversationState {
    public var id: String { coordinates.conversationId }
    public var convoId: String { coordinates.conversationId }
    public var resetGeneration: Int { coordinates.generation }
    public var members: [BlueCatbirdChatDefs.ParticipantView] { participants }
}

extension BlueCatbirdChatDefs.ConversationEntry {
    public var isApplication: Bool {
        if case .blueCatbirdChatDefsApplicationEntry = self { return true }
        return false
    }
    public var isCommit: Bool {
        if case .blueCatbirdChatDefsCommitEntry = self { return true }
        return false
    }
    public var applicationEntry: BlueCatbirdChatDefs.ApplicationEntry? {
        if case let .blueCatbirdChatDefsApplicationEntry(entry) = self { return entry }
        return nil
    }
    public var commitEntry: BlueCatbirdChatDefs.CommitEntry? {
        if case let .blueCatbirdChatDefsCommitEntry(entry) = self { return entry }
        return nil
    }
    public var entryID: String {
        switch self {
        case .blueCatbirdChatDefsApplicationEntry(let e): return e.entryId
        case .blueCatbirdChatDefsCommitEntry(let e): return e.entryId
        case .blueCatbirdChatDefsPolicyEntry(let e): return e.entryId
        case .blueCatbirdChatDefsMetadataEntry(let e): return e.entryId
        case .blueCatbirdChatDefsCreationEntry(let e): return e.entryId
        case .blueCatbirdChatDefsParticipantAcceptanceEntry(let e): return e.entryId
        case .blueCatbirdChatDefsConversationCloseEntry(let e): return e.entryId
        case .blueCatbirdChatDefsResetRequestEntry(let e): return e.entryId
        case .blueCatbirdChatDefsResetActivationEntry(let e): return e.entryId
        case .blueCatbirdChatDefsLeafRecoveryFulfillmentEntry(let e): return e.entryId
        case .blueCatbirdChatDefsLeaveRequestEntry(let e): return e.entryId
        case .blueCatbirdChatDefsZeroLeafLeaveEntry(let e): return e.entryId
        case .blueCatbirdChatDefsLeaveCancellationEntry(let e): return e.entryId
        case .blueCatbirdChatDefsLeaveCommitFulfillmentEntry(let e): return e.entryId
        case .unexpected: return ""
        }
    }
}
