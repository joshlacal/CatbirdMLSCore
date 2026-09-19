import CryptoKit
import Foundation

public enum MLSPushRegistrationProvider: String, Codable, Sendable {
  case apns
  case fcm
}

public struct MLSPushRegistrationReceipt: Equatable, Sendable {
  public let registrationID: String
  public let tokenGeneration: Int64
  public let bindingDigest: Data
  public let enabled: Bool

  public init(
    registrationID: String,
    tokenGeneration: Int64,
    bindingDigest: Data,
    enabled: Bool
  ) {
    self.registrationID = registrationID
    self.tokenGeneration = tokenGeneration
    self.bindingDigest = bindingDigest
    self.enabled = enabled
  }
}

enum MLSPushRegistrationError: Error, LocalizedError, Equatable {
  case invalidField(String)
  case signingAuthorityUnavailable
  case signingAuthorityChanged
  case invalidSignature
  case invalidResponse(String)
  case generationExhausted

  var errorDescription: String? {
    switch self {
    case let .invalidField(field): return "Invalid push-registration field: \(field)"
    case .signingAuthorityUnavailable: return "The current push-registration signing authority is unavailable"
    case .signingAuthorityChanged: return "The push-registration signing authority changed during registration"
    case .invalidSignature: return "The push-registration signature did not verify"
    case let .invalidResponse(detail): return "Invalid push-registration response: \(detail)"
    case .generationExhausted: return "The push-registration generation is exhausted"
    }
  }
}

struct MLSPushSigningAuthority: Equatable, Sendable {
  let actorDID: String
  let deviceID: String
  let keyID: String
  let authGeneration: Int64
  let publicKey: Data
}

enum MLSPushRegistrationCodec {
  static let protocolVersion = "2"
  static let signatureDomain = "CATBIRD-CHAT-PUSH-BINDING\0"
  static let authorityDID = "did:web:chat.catbird.blue"
  static let maxSafeInteger: Int64 = 9_007_199_254_740_991

  struct Body: Codable, Equatable, Sendable {
    let protocolVersion: String
    let signatureDomain: String
    let authorityDid: String
    let actorDid: String
    let actorDeviceId: String
    let keyId: String
    let authGeneration: Int64
    let registrationId: String
    let provider: MLSPushRegistrationProvider
    let appId: String
    let tokenGeneration: Int64
    let tokenSha256: Bytes
    let enabled: Bool
    let signedAt: String

    enum CodingKeys: String, CodingKey {
      case type = "$type"
      case protocolVersion, signatureDomain, authorityDid, actorDid, actorDeviceId
      case keyId, authGeneration, registrationId, provider, appId, tokenGeneration
      case tokenSha256, enabled, signedAt
    }

    init(
      authority: MLSPushSigningAuthority,
      registrationID: String,
      provider: MLSPushRegistrationProvider,
      appID: String,
      tokenGeneration: Int64,
      tokenSHA256: Data,
      enabled: Bool,
      signedAt: String
    ) {
      protocolVersion = MLSPushRegistrationCodec.protocolVersion
      signatureDomain = MLSPushRegistrationCodec.signatureDomain
      authorityDid = MLSPushRegistrationCodec.authorityDID
      actorDid = authority.actorDID
      actorDeviceId = authority.deviceID
      keyId = authority.keyID
      authGeneration = authority.authGeneration
      registrationId = registrationID
      self.provider = provider
      appId = appID
      self.tokenGeneration = tokenGeneration
      tokenSha256 = Bytes(tokenSHA256)
      self.enabled = enabled
      self.signedAt = signedAt
    }

    init(from decoder: Decoder) throws {
      let container = try decoder.container(keyedBy: CodingKeys.self)
      protocolVersion = try container.decode(String.self, forKey: .protocolVersion)
      signatureDomain = try container.decode(String.self, forKey: .signatureDomain)
      authorityDid = try container.decode(String.self, forKey: .authorityDid)
      actorDid = try container.decode(String.self, forKey: .actorDid)
      actorDeviceId = try container.decode(String.self, forKey: .actorDeviceId)
      keyId = try container.decode(String.self, forKey: .keyId)
      authGeneration = try container.decode(Int64.self, forKey: .authGeneration)
      registrationId = try container.decode(String.self, forKey: .registrationId)
      provider = try container.decode(MLSPushRegistrationProvider.self, forKey: .provider)
      appId = try container.decode(String.self, forKey: .appId)
      tokenGeneration = try container.decode(Int64.self, forKey: .tokenGeneration)
      tokenSha256 = try container.decode(Bytes.self, forKey: .tokenSha256)
      enabled = try container.decode(Bool.self, forKey: .enabled)
      signedAt = try container.decode(String.self, forKey: .signedAt)
    }

    func encode(to encoder: Encoder) throws {
      var container = encoder.container(keyedBy: CodingKeys.self)
      try container.encode("blue.catbird.chat.defs#pushRegistrationBody", forKey: .type)
      try container.encode(protocolVersion, forKey: .protocolVersion)
      try container.encode(signatureDomain, forKey: .signatureDomain)
      try container.encode(authorityDid, forKey: .authorityDid)
      try container.encode(actorDid, forKey: .actorDid)
      try container.encode(actorDeviceId, forKey: .actorDeviceId)
      try container.encode(keyId, forKey: .keyId)
      try container.encode(authGeneration, forKey: .authGeneration)
      try container.encode(registrationId, forKey: .registrationId)
      try container.encode(provider, forKey: .provider)
      try container.encode(appId, forKey: .appId)
      try container.encode(tokenGeneration, forKey: .tokenGeneration)
      try container.encode(tokenSha256, forKey: .tokenSha256)
      try container.encode(enabled, forKey: .enabled)
      try container.encode(signedAt, forKey: .signedAt)
    }
  }

  struct Bytes: Codable, Equatable, Sendable {
    let data: Data
    init(_ data: Data) { self.data = data }
    enum CodingKeys: String, CodingKey { case bytes = "$bytes" }
    init(from decoder: Decoder) throws {
      let container = try decoder.container(keyedBy: CodingKeys.self)
      let encoded = try container.decode(String.self, forKey: .bytes)
      guard let data = Data(base64Encoded: encoded) else {
        throw DecodingError.dataCorruptedError(forKey: .bytes, in: container, debugDescription: "Invalid base64")
      }
      self.data = data
    }
    func encode(to encoder: Encoder) throws {
      var container = encoder.container(keyedBy: CodingKeys.self)
      try container.encode(data.base64EncodedString(), forKey: .bytes)
    }
  }

  private struct SignedRegistration: Encodable {
    let body: Body
    let signature: Bytes
    enum CodingKeys: String, CodingKey { case type = "$type"; case body, signature }
    func encode(to encoder: Encoder) throws {
      var container = encoder.container(keyedBy: CodingKeys.self)
      try container.encode("blue.catbird.chat.defs#signedPushRegistration", forKey: .type)
      try container.encode(body, forKey: .body)
      try container.encode(signature, forKey: .signature)
    }
  }

  private struct Request: Encodable {
    let token: String?
    let registration: SignedRegistration
  }

  private struct Binding: Decodable {
    let body: Body
    let bindingDigest: Bytes
  }

  private struct Device: Decodable {
    let deviceId: String
    let keyId: String
    let signaturePublicKey: Bytes
    let authGeneration: Int64
    let status: String
  }

  private struct Response: Decodable {
    let device: Device
    let pushBinding: Binding
  }

  static func tokenDigest(_ token: String?, enabled: Bool) throws -> Data {
    if enabled {
      guard let token, !token.isEmpty else { throw MLSPushRegistrationError.invalidField("token") }
      return Data(SHA256.hash(data: Data(token.utf8)))
    }
    guard token == nil else { throw MLSPushRegistrationError.invalidField("disabled token") }
    return Data(SHA256.hash(data: Data()))
  }

  static func transcript(for body: Body) throws -> Data {
    try validate(body)
    var data = Data(signatureDomain.utf8)
    try appendString(body.protocolVersion, to: &data)
    try appendString(body.authorityDid, to: &data)
    try appendString(body.actorDid, to: &data)
    data.append(try uuidBytes(body.actorDeviceId, field: "actorDeviceId"))
    try appendString(body.keyId, to: &data)
    appendUInt64(UInt64(body.authGeneration), to: &data)
    data.append(try uuidBytes(body.registrationId, field: "registrationId"))
    try appendString(body.provider.rawValue, to: &data)
    try appendString(body.appId, to: &data)
    appendUInt64(UInt64(body.tokenGeneration), to: &data)
    data.append(body.tokenSha256.data)
    data.append(body.enabled ? 1 : 0)
    try appendString(body.signedAt, to: &data)
    return data
  }

  static func requestData(body: Body, signature: Data, token: String?) throws -> Data {
    try validate(body)
    guard signature.count == 64 else { throw MLSPushRegistrationError.invalidField("signature") }
    guard body.enabled == (token != nil) else { throw MLSPushRegistrationError.invalidField("token/enabled") }
    return try JSONEncoder().encode(
      Request(token: token, registration: SignedRegistration(body: body, signature: Bytes(signature)))
    )
  }

  static func verifyResponse(
    _ data: Data,
    expectedBody: Body,
    expectedAuthority: MLSPushSigningAuthority
  ) throws -> MLSPushRegistrationReceipt {
    guard let responseObject = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
          let bindingObject = responseObject["pushBinding"] as? [String: Any],
          let responseBodyObject = bindingObject["body"] as? [String: Any],
          let expectedBodyObject = try? JSONSerialization.jsonObject(
            with: JSONEncoder().encode(expectedBody)
          ) as? [String: Any],
          NSDictionary(dictionary: responseBodyObject).isEqual(to: expectedBodyObject)
    else { throw MLSPushRegistrationError.invalidResponse("binding body was not exact") }
    let response: Response
    do { response = try JSONDecoder().decode(Response.self, from: data) }
    catch { throw MLSPushRegistrationError.invalidResponse("strict decode failed") }
    guard response.pushBinding.body == expectedBody else {
      throw MLSPushRegistrationError.invalidResponse("binding body mismatch")
    }
    let expectedDigest = Data(SHA256.hash(data: try transcript(for: expectedBody)))
    guard response.pushBinding.bindingDigest.data == expectedDigest else {
      throw MLSPushRegistrationError.invalidResponse("binding digest mismatch")
    }
    guard response.device.deviceId == expectedAuthority.deviceID,
          response.device.keyId == expectedAuthority.keyID,
          response.device.signaturePublicKey.data == expectedAuthority.publicKey,
          response.device.authGeneration == expectedAuthority.authGeneration,
          response.device.status == "active"
    else { throw MLSPushRegistrationError.invalidResponse("device authority mismatch") }
    return MLSPushRegistrationReceipt(
      registrationID: expectedBody.registrationId,
      tokenGeneration: expectedBody.tokenGeneration,
      bindingDigest: expectedDigest,
      enabled: expectedBody.enabled
    )
  }

  static func canonicalTimestamp(_ date: Date) -> String {
    let milliseconds = Int64((date.timeIntervalSince1970 * 1_000).rounded(.down))
    let exactDate = Date(timeIntervalSince1970: Double(milliseconds) / 1_000)
    return makeTimestampFormatter().string(from: exactDate)
  }

  private static func makeTimestampFormatter() -> ISO8601DateFormatter {
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    return formatter
  }

  private static func validate(_ body: Body) throws {
    guard body.protocolVersion == protocolVersion else { throw MLSPushRegistrationError.invalidField("protocolVersion") }
    guard body.signatureDomain == signatureDomain else { throw MLSPushRegistrationError.invalidField("signatureDomain") }
    guard body.authorityDid == authorityDID else { throw MLSPushRegistrationError.invalidField("authorityDid") }
    guard (12 ... 261).contains(body.actorDid.utf8.count), !body.actorDid.contains("#"), !body.actorDid.contains("?") else {
      throw MLSPushRegistrationError.invalidField("actorDid")
    }
    guard body.keyId.utf8.count == 43,
          body.keyId.allSatisfy({ $0.isASCII && ($0.isLetter || $0.isNumber || $0 == "-" || $0 == "_") })
    else { throw MLSPushRegistrationError.invalidField("keyId") }
    guard (1 ... maxSafeInteger).contains(body.authGeneration) else { throw MLSPushRegistrationError.invalidField("authGeneration") }
    guard (1 ... maxSafeInteger).contains(body.tokenGeneration) else { throw MLSPushRegistrationError.invalidField("tokenGeneration") }
    guard body.tokenSha256.data.count == 32 else { throw MLSPushRegistrationError.invalidField("tokenSha256") }
    guard !body.appId.isEmpty, body.appId.utf8.count <= 128,
          body.appId.unicodeScalars.allSatisfy({ $0.isASCII && (0x21 ... 0x7e).contains($0.value) })
    else { throw MLSPushRegistrationError.invalidField("appId") }
    guard body.signedAt.utf8.count == 24,
          makeTimestampFormatter().date(from: body.signedAt).map({ canonicalTimestamp($0) == body.signedAt }) == true
    else { throw MLSPushRegistrationError.invalidField("signedAt") }
    _ = try uuidBytes(body.actorDeviceId, field: "actorDeviceId")
    _ = try uuidBytes(body.registrationId, field: "registrationId")
  }

  private static func appendString(_ value: String, to data: inout Data) throws {
    let bytes = Data(value.utf8)
    guard let length = UInt32(exactly: bytes.count) else { throw MLSPushRegistrationError.invalidField("string length") }
    data.append(contentsOf: withUnsafeBytes(of: length.bigEndian, Array.init))
    data.append(bytes)
  }

  private static func appendUInt64(_ value: UInt64, to data: inout Data) {
    data.append(contentsOf: withUnsafeBytes(of: value.bigEndian, Array.init))
  }

  private static func uuidBytes(_ value: String, field: String) throws -> Data {
    guard value == value.lowercased(), let uuid = UUID(uuidString: value) else {
      throw MLSPushRegistrationError.invalidField(field)
    }
    var raw = uuid.uuid
    return withUnsafeBytes(of: &raw) { Data($0) }
  }
}
