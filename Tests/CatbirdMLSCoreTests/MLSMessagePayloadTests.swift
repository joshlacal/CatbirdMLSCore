import Foundation
import Testing

@testable import CatbirdMLSCore

@Suite("MLSMessagePayload edit/unsend + tolerant decode")
struct MLSMessagePayloadTests {

  // MARK: - Tolerant decode of unrecognized messageType

  @Test("decoding an unrecognized messageType falls back to .unknown instead of throwing")
  func decode_unrecognizedMessageType_fallsBackToUnknown() throws {
    let json = """
      {"version":1,"messageType":"someFutureType"}
      """.data(using: .utf8)!

    let payload = try MLSMessagePayload.decodeFromJSON(json)

    #expect(payload.messageType == .unknown)
    #expect(payload.version == 1)
  }

  @Test("decoding an unrecognized messageType alongside unknown extra fields still succeeds")
  func decode_unrecognizedMessageType_withExtraFields_stillSucceeds() throws {
    let json = """
      {"version":1,"messageType":"pollVote","pollVote":{"optionId":"abc"}}
      """.data(using: .utf8)!

    // Should not throw — unknown top-level fields are simply ignored by Codable,
    // and the unrecognized messageType string falls back to .unknown.
    let payload = try MLSMessagePayload.decodeFromJSON(json)
    #expect(payload.messageType == .unknown)
  }

  @Test("recognized messageType values still decode to their real case, not .unknown")
  func decode_recognizedMessageType_doesNotFallBackToUnknown() throws {
    let json = """
      {"version":1,"messageType":"text","text":"hello"}
      """.data(using: .utf8)!

    let payload = try MLSMessagePayload.decodeFromJSON(json)

    #expect(payload.messageType == .text)
    #expect(payload.text == "hello")
  }

  // MARK: - Edit payload round-trip

  @Test("edit payload round-trips through JSON with camelCase keys")
  func edit_roundTripsWithCamelCaseKeys() throws {
    let payload = MLSMessagePayload.edit(targetMessageId: "01ARZ3NDEKTSV4RRFFQ69G5FAV", newText: "updated text")

    let data = try payload.encodeToJSON()
    let jsonString = String(data: data, encoding: .utf8)!

    // Wire format per spec: {"version":1,"messageType":"edit","edit":{"targetMessageId":"...","newText":"..."}}
    #expect(jsonString.contains("\"messageType\":\"edit\""))
    #expect(jsonString.contains("\"targetMessageId\":\"01ARZ3NDEKTSV4RRFFQ69G5FAV\""))
    #expect(jsonString.contains("\"newText\":\"updated text\""))

    let decoded = try MLSMessagePayload.decodeFromJSON(data)
    #expect(decoded.messageType == .edit)
    #expect(decoded.edit?.targetMessageId == "01ARZ3NDEKTSV4RRFFQ69G5FAV")
    #expect(decoded.edit?.newText == "updated text")
  }

  @Test("edit payload decodes from a hand-written wire-format JSON literal")
  func edit_decodesFromExplicitWireFormat() throws {
    let json = """
      {"version":1,"messageType":"edit","edit":{"targetMessageId":"01ARZ3NDEKTSV4RRFFQ69G5FAV","newText":"hi"}}
      """.data(using: .utf8)!

    let payload = try MLSMessagePayload.decodeFromJSON(json)

    #expect(payload.messageType == .edit)
    #expect(payload.edit?.targetMessageId == "01ARZ3NDEKTSV4RRFFQ69G5FAV")
    #expect(payload.edit?.newText == "hi")
  }

  // MARK: - Delete/unsend payload round-trip

  @Test("unsend payload round-trips through JSON with camelCase keys")
  func unsend_roundTripsWithCamelCaseKeys() throws {
    let payload = MLSMessagePayload.unsend(targetMessageId: "01ARZ3NDEKTSV4RRFFQ69G5FAV")

    let data = try payload.encodeToJSON()
    let jsonString = String(data: data, encoding: .utf8)!

    // Wire format per spec: {"version":1,"messageType":"delete","delete":{"targetMessageId":"..."}}
    #expect(jsonString.contains("\"messageType\":\"delete\""))
    #expect(jsonString.contains("\"targetMessageId\":\"01ARZ3NDEKTSV4RRFFQ69G5FAV\""))

    let decoded = try MLSMessagePayload.decodeFromJSON(data)
    #expect(decoded.messageType == .delete)
    #expect(decoded.delete?.targetMessageId == "01ARZ3NDEKTSV4RRFFQ69G5FAV")
  }

  @Test("delete payload decodes from a hand-written wire-format JSON literal")
  func delete_decodesFromExplicitWireFormat() throws {
    let json = """
      {"version":1,"messageType":"delete","delete":{"targetMessageId":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}}
      """.data(using: .utf8)!

    let payload = try MLSMessagePayload.decodeFromJSON(json)

    #expect(payload.messageType == .delete)
    #expect(payload.delete?.targetMessageId == "01ARZ3NDEKTSV4RRFFQ69G5FAV")
  }

  // MARK: - .unknown must never be encoded

  @Test("recognized message types never encode as the unknown sentinel raw value")
  func encode_neverProducesUnknownSentinel() throws {
    let payloads: [MLSMessagePayload] = [
      .text("hi"),
      .reaction(messageId: "m1", emoji: "🎉", action: .add),
      .edit(targetMessageId: "m1", newText: "edited"),
      .unsend(targetMessageId: "m1"),
    ]

    for payload in payloads {
      let data = try payload.encodeToJSON()
      let jsonString = String(data: data, encoding: .utf8)!
      #expect(!jsonString.contains("__unknown__"))
    }
  }
}
