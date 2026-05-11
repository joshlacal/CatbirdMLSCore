//
//  MLSFieldEncryptionTests.swift
//  CatbirdMLSCoreTests
//
//  Exercises `MLSFieldEncryption` (Swift facade over the Rust field-level
//  encryption FFI on `MlsContext`). Uses an ephemeral `MlsContext` with a
//  deterministic content root key so tests are hermetic — they never touch
//  the real iOS Keychain.
//

import Foundation
import XCTest
import GRDB
@testable import CatbirdMLSCore
@testable import CatbirdMLS

final class MLSFieldEncryptionTests: XCTestCase {
  private var ctx: MlsContext!
  private var tempStorageDir: URL!

  override func setUp() async throws {
    try await super.setUp()

    tempStorageDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSFieldEncryptionTests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(
      at: tempStorageDir, withIntermediateDirectories: true)
    let storagePath = tempStorageDir.appendingPathComponent("ctx.db").path
    // 32-byte hex-encoded SQLCipher key (matches the prod
    // MLSCoreContext.getEncryptionKey shape).
    let encryptionKey = String(repeating: "ab", count: 32)
    ctx = try MlsContext(
      storagePath: storagePath,
      encryptionKey: encryptionKey,
      keychain: InMemoryKeychainAccess()
    )
    // Deterministic 32-byte root key — same shape Rust expects from
    // MLSContentRootKey.loadOrCreate, but without keychain side-effects.
    try ctx.setContentRootKey(key: Data(repeating: 0x42, count: 32))
  }

  override func tearDown() async throws {
    if let ctx {
      ctx.clearContentRootKey()
      try? ctx.flushAndPrepareClose()
    }
    ctx = nil
    if let tempStorageDir {
      try? FileManager.default.removeItem(at: tempStorageDir)
    }
    tempStorageDir = nil
    try await super.tearDown()
  }

  // MARK: - Encrypt / Decrypt

  func testEncryptRoundTripThroughMlsContext() throws {
    let plaintext = Data("hello".utf8)
    let wire = try MLSFieldEncryption.encrypt(
      context: ctx,
      conversationID: "convo-1",
      plaintext: plaintext
    )
    XCTAssertFalse(wire.isEmpty)
    XCTAssertNotEqual(wire, plaintext, "wire must not be plaintext")

    let recovered = try MLSFieldEncryption.decrypt(
      context: ctx,
      conversationID: "convo-1",
      wire: wire
    )
    XCTAssertEqual(recovered, plaintext)
  }

  func testWrongConversationIDFailsAuth() throws {
    let plaintext = Data("hello".utf8)
    let wire = try MLSFieldEncryption.encrypt(
      context: ctx,
      conversationID: "convo-1",
      plaintext: plaintext
    )

    XCTAssertThrowsError(
      try MLSFieldEncryption.decrypt(
        context: ctx,
        conversationID: "convo-2",
        wire: wire
      ),
      "Decrypting with the wrong conversationID must fail (AEAD AAD is bound to it)"
    )
  }

  // MARK: - HMAC chain

  func testHmacChainRoundTrip() throws {
    let conversationID = "convo-chain"

    let wire1 = try MLSFieldEncryption.encrypt(
      context: ctx, conversationID: conversationID, plaintext: Data("m1".utf8))
    let h1 = try MLSFieldEncryption.computeHMAC(
      context: ctx,
      conversationID: conversationID,
      previousHMAC: nil,
      messageID: "m1",
      payloadWire: wire1
    )
    XCTAssertFalse(h1.isEmpty)

    let wire2 = try MLSFieldEncryption.encrypt(
      context: ctx, conversationID: conversationID, plaintext: Data("m2".utf8))
    let h2 = try MLSFieldEncryption.computeHMAC(
      context: ctx,
      conversationID: conversationID,
      previousHMAC: h1,
      messageID: "m2",
      payloadWire: wire2
    )
    XCTAssertFalse(h2.isEmpty)
    XCTAssertNotEqual(h1, h2, "chained HMACs differ")

    XCTAssertTrue(
      try MLSFieldEncryption.verifyHMAC(
        context: ctx,
        conversationID: conversationID,
        previousHMAC: nil,
        messageID: "m1",
        payloadWire: wire1,
        expected: h1
      ))
    XCTAssertTrue(
      try MLSFieldEncryption.verifyHMAC(
        context: ctx,
        conversationID: conversationID,
        previousHMAC: h1,
        messageID: "m2",
        payloadWire: wire2,
        expected: h2
      ))

    // Tampering with the chain (skip prev or swap order) must fail verify.
    let badVerify = try MLSFieldEncryption.verifyHMAC(
      context: ctx,
      conversationID: conversationID,
      previousHMAC: nil,
      messageID: "m2",
      payloadWire: wire2,
      expected: h2
    )
    XCTAssertFalse(badVerify, "verifying m2 with prev=nil must not pass")
  }
}
