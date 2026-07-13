import CatbirdMLS
@testable import CatbirdMLSCore
import Foundation
import Petrel
import PetrelCatbird
import Testing

@Suite("Orchestrator API receipt mapping")
struct MLSOrchestratorAPIAdapterReceiptTests {
  private func receipt(
    convoId: String = "convo-1",
    epoch: Int = 7,
    term: Int = 11,
    commitHash: Data = Data(repeating: 0xA5, count: 32),
    issuedAt: Int = 1_720_000_000,
    signature: Data = Data(repeating: 0x5A, count: 64)
  ) throws -> BlueCatbirdMlsChatCommitGroupChange.SequencerReceipt {
    BlueCatbirdMlsChatCommitGroupChange.SequencerReceipt(
      convoId: convoId,
      epoch: epoch,
      sequencerTerm: term,
      commitHash: Bytes(data: commitHash),
      sequencerDid: try DID(didString: "did:web:mlschat.catbird.blue"),
      issuedAt: issuedAt,
      signature: Bytes(data: signature)
    )
  }

  @Test("all signed receipt fields cross the API bridge losslessly")
  func completeReceiptMapsLosslessly() throws {
    let mapped = try #require(
      MLSOrchestratorAPIAdapter.sequencerReceipt(
        try receipt(),
        expectedConversationId: "convo-1"
      )
    )

    #expect(mapped.convoId == "convo-1")
    #expect(mapped.epoch == 7)
    #expect(mapped.sequencerTerm == 11)
    #expect(mapped.commitHash == Data(repeating: 0xA5, count: 32))
    #expect(mapped.sequencerDid == "did:web:mlschat.catbird.blue")
    #expect(mapped.issuedAt == 1_720_000_000)
    #expect(mapped.signature == Data(repeating: 0x5A, count: 64))
  }

  @Test("absent receipt remains compatible")
  func absentReceiptStaysNil() throws {
    #expect(
      try MLSOrchestratorAPIAdapter.sequencerReceipt(
        nil,
        expectedConversationId: "convo-1"
      ) == nil
    )
  }

  @Test(
    "malformed signed receipt fails closed",
    arguments: [
      (epoch: -1, term: 1, hashCount: 32, issuedAt: 1, signatureCount: 64),
      (epoch: 1, term: -1, hashCount: 32, issuedAt: 1, signatureCount: 64),
      (epoch: 1, term: 1, hashCount: 31, issuedAt: 1, signatureCount: 64),
      (epoch: 1, term: 1, hashCount: 32, issuedAt: -1, signatureCount: 64),
      (epoch: 1, term: 1, hashCount: 32, issuedAt: 1, signatureCount: 63),
    ]
  )
  func malformedReceiptFailsClosed(
    fields: (epoch: Int, term: Int, hashCount: Int, issuedAt: Int, signatureCount: Int)
  ) throws {
    let value = try receipt(
      epoch: fields.epoch,
      term: fields.term,
      commitHash: Data(repeating: 0, count: fields.hashCount),
      issuedAt: fields.issuedAt,
      signature: Data(repeating: 0, count: fields.signatureCount)
    )

    #expect(throws: MLSAPIError.self) {
      try MLSOrchestratorAPIAdapter.sequencerReceipt(
        value,
        expectedConversationId: "convo-1"
      )
    }
  }

  enum ReceiptOperation: CaseIterable {
    case addMembers
    case idempotentAddMembers
    case externalCommit
  }

  @Test(
    "operation receipt conversation mismatch fails closed",
    arguments: ReceiptOperation.allCases
  )
  func operationReceiptConversationMismatchFailsClosed(operation: ReceiptOperation) throws {
    let mismatched = try receipt(convoId: "attacker-controlled-conversation")

    #expect(throws: MLSAPIError.self) {
      switch operation {
      case .addMembers:
        try MLSOrchestratorAPIAdapter.addMembersReceipt(
          mismatched,
          expectedConversationId: "requested-conversation"
        )
      case .idempotentAddMembers:
        try MLSOrchestratorAPIAdapter.idempotentAddMembersReceipt(
          mismatched,
          expectedConversationId: "requested-conversation"
        )
      case .externalCommit:
        try MLSOrchestratorAPIAdapter.externalCommitReceipt(
          mismatched,
          expectedConversationId: "requested-conversation"
        )
      }
    }
  }
}
