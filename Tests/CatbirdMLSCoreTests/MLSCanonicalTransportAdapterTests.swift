import CatbirdMLS
import Foundation
import Petrel
import PetrelCatbird
import XCTest
@testable import CatbirdMLSCore

final class MLSCanonicalTransportAdapterTests: XCTestCase {
  func testOpaqueBlobResponsesRoundTripThroughRustTransport() throws {
    let ciphertext = Data([0x00, 0x01, 0xFE, 0xFF])

    XCTAssertEqual(
      try MLSCanonicalTransportAdapter.decodeBlob(ciphertext),
      ciphertext
    )
  }

  func testGeneratedInputIsEncodedBeforeRustPreparation() throws {
    // Use a generated Petrel DTO deliberately shaped for the wrong route. The
    // adapter must encode it and let Rust's generated DTO parser reject it;
    // this prevents a Swift-side hand-maintained request schema from creeping
    // into the transport seam.
    let input = BlueCatbirdChatGetBlob.Output(data: Data([0x01]))
    let auth = CleanChatAuthContextFfi(
      authorization: "Bearer test",
      dpopProof: "proof",
      dpopJkt: "jkt",
      deviceId: "00000000-0000-4000-8000-000000000001",
      authGeneration: 1
    )

    XCTAssertThrowsError(
      try MLSCanonicalTransportAdapter.prepare(
        auth: auth,
        operation: .getBlob,
        input: input
      )
    ) { error in
      XCTAssertFalse(
        String(describing: error).contains("JSONEncoder"),
        "the generated Petrel input should be encoded before Rust validation"
      )
    }
  }
  func testCanonicalReadAndTicketRoutesStayOnGeneratedChatNamespace() {
    XCTAssertEqual(
      MLSChatEndpointCatalog.route(forLegacyEndpoint: "blue.catbird.mlsChat.getConvos")?.canonical,
      "blue.catbird.chat.getConversations"
    )
    XCTAssertEqual(
      MLSChatEndpointCatalog.route(forLegacyEndpoint: "blue.catbird.mlsChat.getMessages")?.canonical,
      "blue.catbird.chat.getEntries"
    )
    XCTAssertEqual(
      MLSChatEndpointCatalog.route(forLegacyEndpoint: "blue.catbird.mlsChat.getSubscriptionTicket")?.canonical,
      "blue.catbird.chat.getSubscriptionTicket"
    )
    XCTAssertEqual(
      MLSChatEndpointCatalog.route(forLegacyEndpoint: "blue.catbird.mlsChat.subscribeEvents")?.canonical,
      "blue.catbird.chat.subscribeEvents"
    )
  }
  func testCanonicalReadRoundTripDecodesGeneratedInventory() throws {
    let output = BlueCatbirdChatGetConversations.Output(
      items: [],
      inventorySessionId: "inventory-session",
      snapshotEventCursor: "event-cursor",
      nextPageCursor: nil,
      hasMore: false,
      snapshotExpiresAt: ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_000))
    )
    let encoded = try JSONEncoder().encode(output)
    let decoded = try MLSCanonicalTransportAdapter.decode(
      encoded,
      operation: .getConversations,
      as: BlueCatbirdChatGetConversations.Output.self
    )
    XCTAssertEqual(decoded.inventorySessionId, output.inventorySessionId)
    XCTAssertEqual(decoded.snapshotEventCursor, output.snapshotEventCursor)
    XCTAssertEqual(decoded.items.count, 0)
  }

  func testTicketPreparationBindsCanonicalTicketOperationAndBody() throws {
    let input = BlueCatbirdChatGetSubscriptionTicket.Input(
      inventorySessionId: "inventory-session",
      eventCursor: "event-cursor"
    )
    let auth = CleanChatAuthContextFfi(
      authorization: "Bearer test",
      dpopProof: "proof",
      dpopJkt: String(repeating: "A", count: 43),
      deviceId: "00000000-0000-4000-8000-000000000001",
      authGeneration: 1
    )
    let prepared = try MLSCanonicalTransportAdapter.prepare(
      auth: auth,
      operation: .getSubscriptionTicket,
      input: input
    )
    XCTAssertEqual(prepared.operation, .getSubscriptionTicket)
    XCTAssertEqual(prepared.method, "POST")
    XCTAssertEqual(prepared.path, "/xrpc/blue.catbird.chat.getSubscriptionTicket")
    XCTAssertEqual(
      try JSONSerialization.jsonObject(with: prepared.body ?? Data()) as? NSDictionary,
      try JSONSerialization.jsonObject(with: JSONEncoder().encode(input)) as? NSDictionary
    )
  }

  func testCanonicalApplicationEntryProjectsMessageView() throws {
    let did = try DID(didString: "did:plc:abc123")
    let prior = BlueCatbirdChatDefs.MlsAadPriorContext(
      conversationId: Bytes(data: Data([0x01])),
      generation: 1,
      stateVersion: 1,
      groupId: Bytes(data: Data([0x02])),
      epoch: 7,
      groupContextHash: Bytes(data: Data([0x03])),
      confirmationTag: Bytes(data: Data([0x04])),
      lifecycle: "active"
    )
    let coordinates = BlueCatbirdChatDefs.ConversationCoordinates(
      conversationId: "convo-1",
      generation: 1,
      stateVersion: 1,
      groupId: Bytes(data: Data([0x02])),
      epoch: 7,
      groupContextHash: Bytes(data: Data([0x03])),
      confirmationTag: Bytes(data: Data([0x04])),
      lifecycle: .value_active
    )
    let message = BlueCatbirdChatDefs.PrivateApplicationMessage(
      framing: "mls",
      contentType: "application/octet-stream",
      bytes: Bytes(data: Data([0xAA, 0xBB])),
      sha256: Bytes(data: Data([0x05]))
    )
    let body = BlueCatbirdChatDefs.ApplicationSendBody(
      signatureDomain: "blue.catbird.chat.application",
      messageId: "message-1",
      actorDid: did,
      actorDeviceId: "device-1",
      keyId: "key-1",
      authGeneration: 1,
      prior: coordinates,
      aad: BlueCatbirdChatDefs.ApplicationAad(
        protocolVersion: .value_1,
        conversationId: Bytes(data: Data([0x01])),
        generation: 1,
        messageId: Bytes(data: Data([0x06])),
        prior: prior
      ),
      applicationMessage: message,
      blobBindings: [],
      signedAt: ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_000))
    )
    let entry = BlueCatbirdChatDefs.ConversationEntry.blueCatbirdChatDefsApplicationEntry(
      BlueCatbirdChatDefs.ApplicationEntry(
        entryId: "entry-1",
        conversationId: "convo-1",
        seq: 4,
        signedRequest: BlueCatbirdChatDefs.SignedApplicationSend(
          body: .blueCatbirdChatDefsApplicationSendBody(body),
          signature: Bytes(data: Data([0x07]))
        ),
        receivedAt: ATProtocolDate(date: Date(timeIntervalSince1970: 1_700_000_001))
      )
    )
    let projected = MLSCanonicalTransportAdapter.projectMessageView(from: entry)
    XCTAssertEqual(projected?.id, "entry-1")
    XCTAssertEqual(projected?.convoId, "convo-1")
    XCTAssertEqual(projected?.ciphertext, Bytes(data: Data([0xAA, 0xBB])))
    XCTAssertEqual(projected?.seq, 4)
    XCTAssertEqual(
      MLSCanonicalTransportAdapter.projectMessageView(from: entry, messageType: .value_commit)?.messageType,
      .value_commit
    )
  }
  func testUnexpectedCanonicalEntryDoesNotProjectAsCiphertext() {
    let entry = BlueCatbirdChatDefs.ConversationEntry.unexpected(.object([:]))
    XCTAssertNil(MLSCanonicalTransportAdapter.projectMessageView(from: entry))
  }

}
