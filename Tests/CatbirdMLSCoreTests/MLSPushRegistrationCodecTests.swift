@testable import CatbirdMLSCore
import CryptoKit
import Foundation
import Testing

struct MLSPushRegistrationCodecTests {
  @Test("Swift push registration transcript matches the public cross-language vector")
  func publicVector() throws {
    let fixture = try fixture()
    let bodyObject = try #require(fixture["body"] as? [String: Any])
    let publicKey = try Data(hex: #require(fixture["publicKeyHex"] as? String))
    let authority = MLSPushSigningAuthority(
      actorDID: try #require(bodyObject["actorDid"] as? String),
      deviceID: try #require(bodyObject["actorDeviceId"] as? String),
      keyID: try #require(bodyObject["keyId"] as? String),
      authGeneration: Int64(try #require(bodyObject["authGeneration"] as? Int)),
      publicKey: publicKey
    )
    let tokenBytes = try #require(bodyObject["tokenSha256"] as? [String: String])
    let providerName = try #require(bodyObject["provider"] as? String)
    let provider = try #require(MLSPushRegistrationProvider(rawValue: providerName))
    let encodedTokenDigest = try #require(tokenBytes["$bytes"])
    let tokenDigest = try #require(Data(base64Encoded: encodedTokenDigest))
    let body = MLSPushRegistrationCodec.Body(
      authority: authority,
      registrationID: try #require(bodyObject["registrationId"] as? String),
      provider: provider,
      appID: try #require(bodyObject["appId"] as? String),
      tokenGeneration: Int64(try #require(bodyObject["tokenGeneration"] as? Int)),
      tokenSHA256: tokenDigest,
      enabled: try #require(bodyObject["enabled"] as? Bool),
      signedAt: try #require(bodyObject["signedAt"] as? String)
    )

    let transcript = try MLSPushRegistrationCodec.transcript(for: body)
    #expect(transcript.hex == (fixture["transcriptHex"] as? String))
    #expect(Data(SHA256.hash(data: transcript)).hex == (fixture["digestHex"] as? String))

    let seed = Data(0 ... 31)
    let privateKey = try Curve25519.Signing.PrivateKey(rawRepresentation: seed)
    #expect(privateKey.publicKey.rawRepresentation == publicKey)
    let fixtureSignature = try Data(hex: #require(fixture["signatureHex"] as? String))
    #expect(privateKey.publicKey.isValidSignature(fixtureSignature, for: transcript))
    // CryptoKit may blind Ed25519 signing internally, so validity is the
    // portable assertion; the fixture's OpenSSL signature remains byte-exact.
    #expect(privateKey.publicKey.isValidSignature(try privateKey.signature(for: transcript), for: transcript))
    #expect(
      try MLSPushRegistrationCodec.tokenDigest(
        try #require(fixture["testOnlyToken"] as? String),
        enabled: true
      ) == body.tokenSha256.data
    )
  }

  @Test("Request uses standard DAG-JSON bytes objects")
  func requestWireBytes() throws {
    let fixture = try fixture()
    let bodyObject = try #require(fixture["body"] as? [String: Any])
    let tokenBytes = try #require(bodyObject["tokenSha256"] as? [String: String])
    let encodedTokenDigest = try #require(tokenBytes["$bytes"])
    let tokenDigest = try #require(Data(base64Encoded: encodedTokenDigest))
    let body = MLSPushRegistrationCodec.Body(
      authority: MLSPushSigningAuthority(
        actorDID: try #require(bodyObject["actorDid"] as? String),
        deviceID: try #require(bodyObject["actorDeviceId"] as? String),
        keyID: try #require(bodyObject["keyId"] as? String),
        authGeneration: Int64(try #require(bodyObject["authGeneration"] as? Int)),
        publicKey: try Data(hex: #require(fixture["publicKeyHex"] as? String))
      ),
      registrationID: try #require(bodyObject["registrationId"] as? String),
      provider: .fcm,
      appID: try #require(bodyObject["appId"] as? String),
      tokenGeneration: Int64(try #require(bodyObject["tokenGeneration"] as? Int)),
      tokenSHA256: tokenDigest,
      enabled: true,
      signedAt: try #require(bodyObject["signedAt"] as? String)
    )
    let signature = try Data(hex: #require(fixture["signatureHex"] as? String))
    let request = try MLSPushRegistrationCodec.requestData(
      body: body,
      signature: signature,
      token: try #require(fixture["testOnlyToken"] as? String)
    )
    let object = try #require(JSONSerialization.jsonObject(with: request) as? [String: Any])
    let registration = try #require(object["registration"] as? [String: Any])
    let signatureObject = try #require(registration["signature"] as? [String: String])
    let encodedBody = try #require(registration["body"] as? [String: Any])
    let encodedTokenHash = try #require(encodedBody["tokenSha256"] as? [String: String])
    #expect(signatureObject["$bytes"] == signature.base64EncodedString())
    #expect(encodedTokenHash["$bytes"] == body.tokenSha256.data.base64EncodedString())
  }

  @Test("Activation requires exact echoed body, digest, and active current device")
  func strictResponse() throws {
    let fixture = try fixture()
    let bodyObject = try #require(fixture["body"] as? [String: Any])
    let publicKey = try Data(hex: #require(fixture["publicKeyHex"] as? String))
    let authority = MLSPushSigningAuthority(
      actorDID: try #require(bodyObject["actorDid"] as? String),
      deviceID: try #require(bodyObject["actorDeviceId"] as? String),
      keyID: try #require(bodyObject["keyId"] as? String),
      authGeneration: Int64(try #require(bodyObject["authGeneration"] as? Int)),
      publicKey: publicKey
    )
    let bodyData = try JSONSerialization.data(withJSONObject: bodyObject)
    let body = try JSONDecoder().decode(MLSPushRegistrationCodec.Body.self, from: bodyData)
    let wireBody = try #require(
      JSONSerialization.jsonObject(with: JSONEncoder().encode(body)) as? [String: Any]
    )
    var response: [String: Any] = [
      "device": [
        "deviceId": authority.deviceID,
        "keyId": authority.keyID,
        "signaturePublicKey": ["$bytes": publicKey.base64EncodedString()],
        "authGeneration": authority.authGeneration,
        "status": "active",
      ],
      "pushBinding": [
        "body": wireBody,
        "bindingDigest": [
          "$bytes": Data(SHA256.hash(data: try MLSPushRegistrationCodec.transcript(for: body)))
            .base64EncodedString()
        ],
      ],
    ]
    let responseData = try JSONSerialization.data(withJSONObject: response)
    let receipt = try MLSPushRegistrationCodec.verifyResponse(
      responseData,
      expectedBody: body,
      expectedAuthority: authority
    )
    #expect(receipt.tokenGeneration == 3)

    var changedBody = wireBody
    changedBody["unexpected"] = true
    var binding = try #require(response["pushBinding"] as? [String: Any])
    binding["body"] = changedBody
    response["pushBinding"] = binding
    #expect(throws: MLSPushRegistrationError.self) {
      _ = try MLSPushRegistrationCodec.verifyResponse(
        JSONSerialization.data(withJSONObject: response),
        expectedBody: body,
        expectedAuthority: authority
      )
    }
  }

  /// The public cross-language vector, byte-identical to catbird-atproto's
  /// tests/fixtures/push-registration.json. It lives in this repository so the
  /// test does not depend on a parent workspace checkout.
  private func fixture() throws -> [String: Any] {
    let url = URL(fileURLWithPath: #filePath)
      .deletingLastPathComponent()
      .deletingLastPathComponent()
      .appendingPathComponent("Fixtures/push-registration-vector.json")
    return try #require(JSONSerialization.jsonObject(with: Data(contentsOf: url)) as? [String: Any])
  }
}

private extension Data {
  init(hex: String) throws {
    guard hex.count.isMultiple(of: 2) else { throw MLSPushRegistrationError.invalidField("hex") }
    var output = Data()
    var index = hex.startIndex
    while index < hex.endIndex {
      let next = hex.index(index, offsetBy: 2)
      guard let byte = UInt8(hex[index ..< next], radix: 16) else {
        throw MLSPushRegistrationError.invalidField("hex")
      }
      output.append(byte)
      index = next
    }
    self = output
  }

  var hex: String { map { String(format: "%02x", $0) }.joined() }
}
