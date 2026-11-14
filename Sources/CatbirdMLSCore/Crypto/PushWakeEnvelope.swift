import CryptoKit
import Foundation

/// Sealed chat push wake envelope, version 2: the device half.
///
/// The gateway (`nest`) seals the wake fields into one fixed-size blob so the push
/// provider sees only `{ "v": 2, "kid", "sealed" }`. Contract and test vector:
/// `docs/security/2026-11-09-sealed-push-wake-envelope.md`. Gateway reference:
/// `nest/catbird/src/services/push/wake_envelope.rs`.
///
/// ```text
/// ikm    = ASCII bytes of pushKey, exactly as issued
/// prk    = HKDF-SHA256-Extract(salt = "catbird-push-wake-v2", ikm)
/// key    = HKDF-Expand(prk, "key", 32)
/// kid    = lowercase hex of HKDF-Expand(prk, "kid", 8)
/// plain  = u16be(len(header)) || header || u16be(len(ct)) || ct || zero padding to 2560
/// aad    = "catbird-push-wake-v2|" + kid + "|" + appId
/// sealed = base64url-nopad( nonce(12) || AES-256-GCM(key, nonce, plain, aad) )
/// ```
public enum PushWakeEnvelope {
  public static let version: Int64 = 2
  public static let plaintextLength = 2560
  public static let maxInlineCiphertext = 2200
  public static let nonceLength = 12
  public static let tagLength = 16
  public static let sealedLength = nonceLength + plaintextLength + tagLength

  private static let hkdfSalt = Data("catbird-push-wake-v2".utf8)
  private static let aadPrefix = "catbird-push-wake-v2|"

  public enum OpenError: Error, Equatable {
    case notVersion2
    case malformedEnvelope
    case kidMismatch
    case sealedNotBase64URL
    case sealedWrongLength
    case authenticationFailed
    case headerOverrun
    case headerInvalid
    case ciphertextOverrun
    case paddingNotZero
    case fetchRequiredMismatch
  }

  /// The key material one `pushKey` yields.
  public struct Keys: Sendable {
    public let key: SymmetricKey
    /// Provider-visible routing tag: 16 lowercase hex characters.
    public let kid: String

    /// Derive from the `pushKey` string exactly as the gateway issued it.
    public init(pushKey: String) {
      let prk = HKDF<SHA256>.extract(
        inputKeyMaterial: SymmetricKey(data: Data(pushKey.utf8)),
        salt: PushWakeEnvelope.hkdfSalt
      )
      key = HKDF<SHA256>.expand(pseudoRandomKey: prk, info: Data("key".utf8), outputByteCount: 32)
      let kidKey = HKDF<SHA256>.expand(pseudoRandomKey: prk, info: Data("kid".utf8), outputByteCount: 8)
      kid = kidKey.withUnsafeBytes { PushWakeEnvelope.hexLower($0) }
    }
  }

  /// The sealed header. Member names match the version 1 clear payload.
  public struct Header: Equatable, Sendable {
    public let wakeKind: String
    public let conversationId: String
    public let generation: Int64
    public let seq: Int64
    public let epoch: Int64
    public let fetchRequired: Bool

    public init(
      wakeKind: String,
      conversationId: String,
      generation: Int64,
      seq: Int64,
      epoch: Int64,
      fetchRequired: Bool
    ) {
      self.wakeKind = wakeKind
      self.conversationId = conversationId
      self.generation = generation
      self.seq = seq
      self.epoch = epoch
      self.fetchRequired = fetchRequired
    }
  }

  public struct Opened: Equatable, Sendable {
    public let header: Header
    /// Raw MLS ciphertext; nil exactly when `header.fetchRequired` is true.
    public let ciphertext: Data?
  }

  /// Open the provider-visible `catbird` object `{ "v": 2, "kid", "sealed" }`.
  public static func open(
    _ catbird: [String: Any],
    keys: Keys,
    appId: String
  ) throws -> Opened {
    guard version(of: catbird) == version else { throw OpenError.notVersion2 }
    guard catbird.count == 3,
          let kid = catbird["kid"] as? String,
          let sealed = catbird["sealed"] as? String
    else { throw OpenError.malformedEnvelope }
    guard kid == keys.kid else { throw OpenError.kidMismatch }
    guard let raw = decodeBase64URLNoPad(sealed) else { throw OpenError.sealedNotBase64URL }
    guard raw.count == sealedLength else { throw OpenError.sealedWrongLength }

    let plain: Data
    do {
      let box = try AES.GCM.SealedBox(
        nonce: AES.GCM.Nonce(data: raw.prefix(nonceLength)),
        ciphertext: raw.dropFirst(nonceLength).dropLast(tagLength),
        tag: raw.suffix(tagLength)
      )
      plain = try AES.GCM.open(box, using: keys.key, authenticating: aad(kid: kid, appId: appId))
    } catch {
      throw OpenError.authenticationFailed
    }
    return try decodePlaintext(plain)
  }

  /// The `v` member as an integer, or nil when absent or not an integer. An absent
  /// `v` means a version 1 clear payload.
  public static func version(of catbird: [String: Any]) -> Int64? {
    switch catbird["v"] {
    case let number as NSNumber where CFGetTypeID(number) != CFBooleanGetTypeID():
      let value = number.int64Value
      return number.doubleValue == Double(value) ? value : nil
    case let string as String:
      return Int64(string)
    default:
      return nil
    }
  }

  /// Seal a wake the way the gateway does. The gateway is the only production
  /// sealer; this exists so tests and local harnesses can build version 2 wakes and
  /// check the two implementations against the shared test vector.
  public static func seal(
    header: Data,
    ciphertext: Data?,
    keys: Keys,
    appId: String,
    nonce: Data? = nil
  ) throws -> [String: Any] {
    let ciphertext = ciphertext ?? Data()
    guard ciphertext.count <= maxInlineCiphertext,
          2 + header.count + 2 + ciphertext.count <= plaintextLength
    else { throw OpenError.ciphertextOverrun }
    var plain = Data(capacity: plaintextLength)
    plain.append(contentsOf: [UInt8(header.count >> 8), UInt8(header.count & 0xFF)])
    plain.append(header)
    plain.append(contentsOf: [UInt8(ciphertext.count >> 8), UInt8(ciphertext.count & 0xFF)])
    plain.append(ciphertext)
    plain.append(Data(count: plaintextLength - plain.count))

    let gcmNonce = try nonce.map { try AES.GCM.Nonce(data: $0) } ?? AES.GCM.Nonce()
    let box = try AES.GCM.seal(
      plain,
      using: keys.key,
      nonce: gcmNonce,
      authenticating: aad(kid: keys.kid, appId: appId)
    )
    var out = Data(gcmNonce)
    out.append(box.ciphertext)
    out.append(box.tag)
    return ["v": Int(version), "kid": keys.kid, "sealed": encodeBase64URLNoPad(out)]
  }

  // MARK: - Plaintext framing

  static func decodePlaintext(_ plain: Data) throws -> Opened {
    let bytes = [UInt8](plain)
    guard bytes.count == plaintextLength else { throw OpenError.sealedWrongLength }
    let headerLength = Int(bytes[0]) << 8 | Int(bytes[1])
    let headerEnd = 2 + headerLength
    guard headerEnd + 2 <= bytes.count else { throw OpenError.headerOverrun }
    let header = try decodeHeader(Data(bytes[2..<headerEnd]))
    let ciphertextLength = Int(bytes[headerEnd]) << 8 | Int(bytes[headerEnd + 1])
    let ciphertextStart = headerEnd + 2
    let ciphertextEnd = ciphertextStart + ciphertextLength
    guard ciphertextLength <= maxInlineCiphertext, ciphertextEnd <= bytes.count else {
      throw OpenError.ciphertextOverrun
    }
    guard bytes[ciphertextEnd...].allSatisfy({ $0 == 0 }) else { throw OpenError.paddingNotZero }
    guard header.fetchRequired == (ciphertextLength == 0) else {
      throw OpenError.fetchRequiredMismatch
    }
    return Opened(
      header: header,
      ciphertext: ciphertextLength > 0 ? Data(bytes[ciphertextStart..<ciphertextEnd]) : nil
    )
  }

  private static let headerMembers: Set<String> = [
    "wakeKind", "conversationId", "generation", "seq", "epoch", "fetchRequired",
  ]

  /// Exactly the six members, each with its exact JSON type: no unknown members,
  /// none missing, no booleans standing in for integers or the reverse.
  private static func decodeHeader(_ data: Data) throws -> Header {
    guard let object = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
          Set(object.keys) == headerMembers,
          let wakeKind = object["wakeKind"] as? String,
          let conversationId = object["conversationId"] as? String,
          let generation = jsonInteger(object["generation"]),
          let seq = jsonInteger(object["seq"]),
          let epoch = jsonInteger(object["epoch"]),
          let fetchRequired = jsonBool(object["fetchRequired"])
    else { throw OpenError.headerInvalid }
    return Header(
      wakeKind: wakeKind,
      conversationId: conversationId,
      generation: generation,
      seq: seq,
      epoch: epoch,
      fetchRequired: fetchRequired
    )
  }

  private static func jsonInteger(_ value: Any?) -> Int64? {
    guard let number = value as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID() else {
      return nil
    }
    let integer = number.int64Value
    return number.doubleValue == Double(integer) ? integer : nil
  }

  private static func jsonBool(_ value: Any?) -> Bool? {
    guard let number = value as? NSNumber, CFGetTypeID(number) == CFBooleanGetTypeID() else {
      return nil
    }
    return number.boolValue
  }

  // MARK: - Encoding helpers

  private static func aad(kid: String, appId: String) -> Data {
    Data("\(aadPrefix)\(kid)|\(appId)".utf8)
  }

  private static func hexLower(_ bytes: UnsafeRawBufferPointer) -> String {
    bytes.map { String(format: "%02x", $0) }.joined()
  }

  /// Strict base64url without padding: the standard alphabet's `+`, `/` and `=`
  /// are rejected, as the gateway's decoder rejects them.
  static func decodeBase64URLNoPad(_ string: String) -> Data? {
    guard string.utf8.allSatisfy({
      ($0 >= 0x41 && $0 <= 0x5A) || ($0 >= 0x61 && $0 <= 0x7A) || ($0 >= 0x30 && $0 <= 0x39)
        || $0 == 0x2D || $0 == 0x5F
    }), string.utf8.count % 4 != 1
    else { return nil }
    let standard = string
      .replacingOccurrences(of: "-", with: "+")
      .replacingOccurrences(of: "_", with: "/")
    return Data(base64Encoded: standard + String(repeating: "=", count: (4 - standard.utf8.count % 4) % 4))
  }

  static func encodeBase64URLNoPad(_ data: Data) -> String {
    data.base64EncodedString()
      .replacingOccurrences(of: "+", with: "-")
      .replacingOccurrences(of: "/", with: "_")
      .replacingOccurrences(of: "=", with: "")
  }
}
