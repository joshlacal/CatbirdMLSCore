//
//  MLSFieldEncryption.swift
//  CatbirdMLSCore
//
//  Swift facade over the Rust field-level encryption FFI exposed on
//  `MlsContext`.
//

import Foundation
import CatbirdMLS

public enum MLSFieldEncryptionError: Error {
  case rootKeyMissing
  case encryptFailed(String)
  case decryptFailed(String)
  case hmacFailed(String)
}

public enum MLSFieldEncryption {
  public static func encrypt(
    context: MlsContext,
    conversationID: String,
    plaintext: Data
  ) throws -> Data {
    do {
      return try context.encryptMessagePayload(
        conversationId: conversationID,
        plaintext: plaintext
      )
    } catch {
      throw MLSFieldEncryptionError.encryptFailed(String(describing: error))
    }
  }

  public static func decrypt(
    context: MlsContext,
    conversationID: String,
    wire: Data
  ) throws -> Data {
    do {
      return try context.decryptMessagePayload(
        conversationId: conversationID,
        wire: wire
      )
    } catch {
      throw MLSFieldEncryptionError.decryptFailed(String(describing: error))
    }
  }

  public static func computeHMAC(
    context: MlsContext,
    conversationID: String,
    previousHMAC: Data?,
    messageID: String,
    payloadWire: Data
  ) throws -> Data {
    do {
      return try context.computeMessageHmac(
        conversationId: conversationID,
        prevHmac: previousHMAC,
        messageId: messageID,
        payloadWire: payloadWire
      )
    } catch {
      throw MLSFieldEncryptionError.hmacFailed(String(describing: error))
    }
  }

  public static func verifyHMAC(
    context: MlsContext,
    conversationID: String,
    previousHMAC: Data?,
    messageID: String,
    payloadWire: Data,
    expected: Data
  ) throws -> Bool {
    do {
      return try context.verifyMessageHmac(
        conversationId: conversationID,
        prevHmac: previousHMAC,
        messageId: messageID,
        payloadWire: payloadWire,
        expected: expected
      )
    } catch {
      throw MLSFieldEncryptionError.hmacFailed(String(describing: error))
    }
  }
}
