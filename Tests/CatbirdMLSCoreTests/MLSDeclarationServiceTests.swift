//
//  MLSDeclarationServiceTests.swift
//  CatbirdMLSCoreTests
//
//  Created on 2026-08-24.
//

import Foundation
import Testing
import Petrel
import PetrelCatbird
@testable import CatbirdMLSCore

struct MLSDeclarationServiceTests {

  // MARK: - Eligibility Evaluation Tests

  @Test func declarationAbsentIsNotEligible() {
    let isEligible = MLSDeclarationService.evaluateEligibility(
      declaration: nil,
      targetFollowsViewer: false
    )
    #expect(!isEligible, "A user without a chat declaration record must not be eligible for secure chat")
  }

  @Test func declarationAbsentWithFollowedByIsNotEligible() {
    let isEligible = MLSDeclarationService.evaluateEligibility(
      declaration: nil,
      targetFollowsViewer: true
    )
    #expect(!isEligible, "Even if the user follows the viewer, absence of declaration means no secure chat")
  }

  @Test func declarationAllowIncomingNoneIsNotEligible() throws {
    let decl = BlueCatbirdChatDeclaration(
      allowIncoming: "none",
      deliveryService: try DID(didString: "did:web:chat.catbird.blue"),
      protocolVersion: "1",
      createdAt: ATProtocolDate(date: Date())
    )
    let isEligible = MLSDeclarationService.evaluateEligibility(
      declaration: decl,
      targetFollowsViewer: true
    )
    #expect(!isEligible, "allowIncoming: none must reject all incoming messages")
  }

  @Test func declarationAllowIncomingFollowingWithoutFollowedByIsNotEligible() throws {
    let decl = BlueCatbirdChatDeclaration(
      allowIncoming: "following",
      deliveryService: try DID(didString: "did:web:chat.catbird.blue"),
      protocolVersion: "1",
      createdAt: ATProtocolDate(date: Date())
    )
    let isEligible = MLSDeclarationService.evaluateEligibility(
      declaration: decl,
      targetFollowsViewer: false
    )
    #expect(!isEligible, "allowIncoming: following requires target to follow viewer")
  }

  @Test func declarationAllowIncomingFollowingWithFollowedByIsEligible() throws {
    let decl = BlueCatbirdChatDeclaration(
      allowIncoming: "following",
      deliveryService: try DID(didString: "did:web:chat.catbird.blue"),
      protocolVersion: "1",
      createdAt: ATProtocolDate(date: Date())
    )
    let isEligible = MLSDeclarationService.evaluateEligibility(
      declaration: decl,
      targetFollowsViewer: true
    )
    #expect(isEligible, "allowIncoming: following allows messages when target follows viewer")
  }

  @Test func declarationAllowIncomingAllIsEligible() throws {
    let decl = BlueCatbirdChatDeclaration(
      allowIncoming: "all",
      deliveryService: try DID(didString: "did:web:chat.catbird.blue"),
      protocolVersion: "1",
      createdAt: ATProtocolDate(date: Date())
    )
    let isEligible = MLSDeclarationService.evaluateEligibility(
      declaration: decl,
      targetFollowsViewer: false
    )
    #expect(isEligible, "allowIncoming: all allows messages from anyone")
  }

  @Test func declarationIncompatibleProtocolVersionIsNotEligible() throws {
    let decl = BlueCatbirdChatDeclaration(
      allowIncoming: "all",
      deliveryService: try DID(didString: "did:web:chat.catbird.blue"),
      protocolVersion: "2",
      createdAt: ATProtocolDate(date: Date())
    )
    let isEligible = MLSDeclarationService.evaluateEligibility(
      declaration: decl,
      targetFollowsViewer: true
    )
    #expect(!isEligible, "protocolVersion != 1 must be refused cleanly")
  }

  @Test func declarationUnknownAllowIncomingFailsClosed() throws {
    let decl = BlueCatbirdChatDeclaration(
      allowIncoming: "restricted_custom",
      deliveryService: try DID(didString: "did:web:chat.catbird.blue"),
      protocolVersion: "1",
      createdAt: ATProtocolDate(date: Date())
    )
    let isEligible = MLSDeclarationService.evaluateEligibility(
      declaration: decl,
      targetFollowsViewer: true
    )
    #expect(!isEligible, "Unknown allowIncoming policy value must fail closed")
  }

  // MARK: - Device Record Format Compatibility with declaration_client.rs

  @Test func deviceRecordMatchesDeclarationClientShape() throws {
    // Exact shape declaration_client.rs:294-304 expects:
    // {
    //   "$type": "blue.catbird.chat.device",
    //   "mlsSignaturePublicKey": { "$bytes": "AQID" },
    //   "algorithm": "ed25519",
    //   "createdAt": "2026-07-16T00:00:00.000Z"
    // }
    let rawKey = Data([0x01, 0x02, 0x03]) // Base64 is "AQID"
    let device = BlueCatbirdChatDevice(
      mlsSignaturePublicKey: Bytes(data: rawKey),
      algorithm: "ed25519",
      createdAt: ATProtocolDate(date: Date(timeIntervalSince1970: 1784160000))
    )

    let encoder = JSONEncoder()
    let data = try encoder.encode(device)
    let json = try #require(
      JSONSerialization.jsonObject(with: data) as? [String: Any]
    )

    #expect(json["$type"] as? String == "blue.catbird.chat.device")
    #expect(json["algorithm"] as? String == "ed25519")

    let keyObj = try #require(json["mlsSignaturePublicKey"] as? [String: Any])
    #expect(keyObj["$bytes"] as? String == "AQID")
  }

  // MARK: - Declaration Record Serialization Roundtrip

  @Test func declarationRecordSerializationRoundtrip() throws {
    let dsDid = try DID(didString: "did:web:chat.catbird.blue")
    let now = Date()
    let decl = BlueCatbirdChatDeclaration(
      allowIncoming: "following",
      deliveryService: dsDid,
      protocolVersion: "1",
      createdAt: ATProtocolDate(date: now)
    )

    let encoder = JSONEncoder()
    let data = try encoder.encode(decl)

    let decoder = JSONDecoder()
    let decoded = try decoder.decode(BlueCatbirdChatDeclaration.self, from: data)

    #expect(decoded.allowIncoming == "following")
    #expect(decoded.deliveryService.didString() == "did:web:chat.catbird.blue")
    #expect(decoded.protocolVersion == "1")
  }
}
