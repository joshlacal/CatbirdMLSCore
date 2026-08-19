import XCTest
import PetrelCatbird
@testable import CatbirdMLSCore

final class MLSMessageViewProjectionTests: XCTestCase {
  func testKnownEncryptedPayloadsProjectAsApplicationMessages() throws {
    let payloads: [MLSMessagePayload] = [
      .text("hello", embed: nil),
      .reaction(messageId: "message-1", emoji: "👍", action: .add),
      .deliveryAck(messageId: "message-1"),
      .recoveryRequest(messageId: "message-1", epoch: 3, sequenceNumber: 4),
      .edit(targetMessageId: "message-1", newText: "edited"),
      .unsend(targetMessageId: "message-1"),
    ]

    for payload in payloads {
      XCTAssertEqual(
        try MLSMessageViewProjection.viewType(for: payload),
        "application",
        "\(payload.messageType.rawValue) must use the generated app discriminator"
      )
    }
  }

  func testUnknownPayloadFailsClosedInsteadOfInventingViewDiscriminator() {
    let payload = MLSMessagePayload(
      messageType: .unknown,
      text: nil,
      embed: nil,
      recoveredMessageId: nil
    )

    XCTAssertThrowsError(try MLSMessageViewProjection.viewType(for: payload)) { error in
      guard case MLSMessageViewProjectionError.unsupportedPayloadType(.unknown) = error else {
        return XCTFail("Unexpected projection error: \(error)")
      }
    }
  }

  func testOptionalViewDiscriminatorDefaultsToApplicationForLegacyMissingField() {
    XCTAssertEqual(MLSMessageViewProjection.rawType(nil), "application")
    XCTAssertEqual(MLSMessageViewProjection.rawType("commit"), "commit")
  }
}
