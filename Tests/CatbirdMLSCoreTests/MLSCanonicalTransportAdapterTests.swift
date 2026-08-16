import CatbirdMLS
import Foundation
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

}
