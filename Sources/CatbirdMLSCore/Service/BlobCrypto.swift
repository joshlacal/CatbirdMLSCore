import CryptoKit
import Foundation

/// Handles per-blob AES-256-GCM encryption/decryption for image DMs.
/// Each blob gets a unique key and IV. Ciphertext integrity is checked
/// via the SHA-256 stored in the MLS message embed.
public enum BlobCrypto {
  public struct EncryptedBlob: Sendable {
    public let ciphertext: Data
    public let key: Data
    public let iv: Data
    public let sha256: String
  }

  /// Encrypt image data for blob upload.
  /// - Parameters:
  ///   - plaintext: Raw image bytes
  /// - Returns: Encrypted blob with key, IV, and ciphertext hash
  public static func encrypt(plaintext: Data) throws -> EncryptedBlob {
    let key = SymmetricKey(size: .bits256)
    let nonce = AES.GCM.Nonce()
    let sealedBox = try AES.GCM.seal(plaintext, using: key, nonce: nonce)

    // Store ciphertext + tag WITHOUT the nonce prefix.
    // The nonce is stored separately in the MLS message embed as `iv`.
    var ciphertextAndTag = Data(sealedBox.ciphertext)
    ciphertextAndTag.append(contentsOf: sealedBox.tag)

    let hash = SHA256.hash(data: ciphertextAndTag)
    let sha256Hex = hash.map { String(format: "%02x", $0) }.joined()

    return EncryptedBlob(
      ciphertext: ciphertextAndTag,
      key: key.withUnsafeBytes { Data($0) },
      iv: Data(nonce),
      sha256: sha256Hex
    )
  }

  /// Decrypt a downloaded blob.
  /// - Parameters:
  ///   - ciphertext: Encrypted blob bytes (ciphertext + tag, no nonce prefix)
  ///   - key: AES-256-GCM key (32 bytes)
  ///   - iv: Nonce (12 bytes)
  ///   - expectedSHA256: Hex-encoded SHA-256 of the ciphertext for integrity check
  /// - Returns: Decrypted image bytes
  public static func decrypt(
    ciphertext: Data,
    key: Data,
    iv: Data,
    expectedSHA256: String
  ) throws -> Data {
    // Verify ciphertext integrity before decryption
    let hash = SHA256.hash(data: ciphertext)
    let actualSHA256 = hash.map { String(format: "%02x", $0) }.joined()

    guard actualSHA256 == expectedSHA256 else {
      throw BlobCryptoError.integrityCheckFailed
    }

    let symmetricKey = SymmetricKey(data: key)
    let nonce = try AES.GCM.Nonce(data: iv)

    // ciphertext contains ciphertext bytes + 16-byte GCM tag (no nonce prefix)
    let tagSize = 16
    guard ciphertext.count > tagSize else {
      throw BlobCryptoError.decryptionFailed
    }
    let encryptedBytes = ciphertext.prefix(ciphertext.count - tagSize)
    let tag = ciphertext.suffix(tagSize)

    let sealedBox = try AES.GCM.SealedBox(
      nonce: nonce, ciphertext: encryptedBytes, tag: tag
    )
    let plaintext = try AES.GCM.open(sealedBox, using: symmetricKey)

    return plaintext
  }
}

public enum BlobCryptoError: Error, LocalizedError {
  case integrityCheckFailed
  case decryptionFailed

  public var errorDescription: String? {
    switch self {
    case .integrityCheckFailed:
      return "Image integrity check failed — the data may have been tampered with"
    case .decryptionFailed:
      return "Image could not be decrypted"
    }
  }
}
