//
//  MLSDeclarationService.swift
//  CatbirdMLSCore
//
//  Created on 2026-08-24.
//

import Foundation
import OSLog
import Petrel
import PetrelCatbird

/// Service for managing Catbird secure chat declaration records (`blue.catbird.chat.declaration/self`).
/// Presence of a declaration record indicates the user has opted in to Catbird secure chat.
public actor MLSDeclarationService {
  private let logger = Logger(subsystem: "blue.catbird", category: "MLSDeclarationService")
  private let atProtoClient: ATProtoClient
  public static let declarationCollection = "blue.catbird.chat.declaration"
  public static let defaultDeliveryService = "did:web:chat.catbird.blue"

  private var declarationCache: [String: (declaration: BlueCatbirdChatDeclaration?, fetchedAt: Date)] = [:]
  private let cacheTTL: TimeInterval = 5 * 60

  public init(atProtoClient: ATProtoClient) {
    self.atProtoClient = atProtoClient
  }

  // MARK: - Publishing

  /// Publish or update the local user's chat declaration record on their PDS.
  public func publishDeclaration(
    userDid: String,
    allowIncoming: String = "all",
    deliveryService: String = defaultDeliveryService
  ) async throws {
    let normalized = userDid.lowercased()
    let did = try DID(didString: normalized)
    let collection = try NSID(nsidString: Self.declarationCollection)
    let rkey = try RecordKey(keyString: "self")
    let dsDid = try DID(didString: deliveryService)

    let record = BlueCatbirdChatDeclaration(
      allowIncoming: allowIncoming,
      deliveryService: dsDid,
      protocolVersion: "1",
      createdAt: ATProtocolDate(date: Date())
    )

    let input = ComAtprotoRepoPutRecord.Input(
      repo: .did(did),
      collection: collection,
      rkey: rkey,
      validate: false,
      record: .knownType(record)
    )

    let (code, _) = try await atProtoClient.com.atproto.repo.putRecord(input: input)
    guard (200...299).contains(code) else {
      throw DeclarationError.networkFailure("Failed to publish chat declaration (status: \(code))")
    }

    declarationCache[normalized] = (declaration: record, fetchedAt: Date())
    logger.info("Published chat declaration for \(normalized, privacy: .private)")
  }

  /// Delete the local user's chat declaration record from their PDS (e.g. on opt-out).
  public func deleteDeclaration(userDid: String) async throws {
    let normalized = userDid.lowercased()
    let did = try DID(didString: normalized)
    let collection = try NSID(nsidString: Self.declarationCollection)
    let rkey = try RecordKey(keyString: "self")

    let input = ComAtprotoRepoDeleteRecord.Input(
      repo: .did(did),
      collection: collection,
      rkey: rkey
    )

    let (code, _) = try await atProtoClient.com.atproto.repo.deleteRecord(input: input)
    declarationCache.removeValue(forKey: normalized)
    if (200...299).contains(code) {
      logger.info("Deleted chat declaration for \(normalized, privacy: .private)")
    }
  }

  // MARK: - Fetching

  /// Fetch the chat declaration record for a target DID (with 5-minute caching).
  public func fetchDeclaration(for targetDid: String) async throws -> BlueCatbirdChatDeclaration? {
    let normalized = targetDid.lowercased()
    if let cached = declarationCache[normalized],
       Date().timeIntervalSince(cached.fetchedAt) < cacheTTL {
      return cached.declaration
    }

    let declaration: BlueCatbirdChatDeclaration?
    do {
      declaration = try await MLSPublicPDSReader.fetchDeclaration(
        did: normalized,
        resolvePDS: { [atProtoClient] did in
          try await atProtoClient.resolveDIDToPDSURL(did: did)
        }
      )
    } catch {
      logger.warning(
        "Declaration fetch failed for \(normalized, privacy: .private): \(error.localizedDescription)"
      )
      declaration = nil
    }

    declarationCache[normalized] = (declaration: declaration, fetchedAt: Date())
    return declaration
  }

  /// Fetch chat declarations for a batch of DIDs.
  public func fetchDeclarations(for dids: [String]) async -> [String: BlueCatbirdChatDeclaration?] {
    await withTaskGroup(of: (String, BlueCatbirdChatDeclaration?).self) { group in
      for did in dids {
        group.addTask {
          let decl = try? await self.fetchDeclaration(for: did)
          return (did.lowercased(), decl)
        }
      }

      var results: [String: BlueCatbirdChatDeclaration?] = [:]
      for await (did, decl) in group {
        results[did] = decl
      }
      return results
    }
  }

  // MARK: - Eligibility Evaluation

  /// Check whether a target DID is eligible to receive Catbird secure chat messages.
  /// - Absence of declaration -> not eligible (false).
  /// - protocolVersion != "1" -> not eligible (false).
  /// - allowIncoming == "none" -> not eligible (false).
  /// - allowIncoming == "following" -> requires targetFollowsViewer == true.
  /// - allowIncoming == "all" -> eligible (true).
  public func checkEligibility(targetDid: String, targetFollowsViewer: Bool) async -> Bool {
    guard let declaration = try? await fetchDeclaration(for: targetDid) else {
      return false
    }
    return Self.evaluateEligibility(declaration: declaration, targetFollowsViewer: targetFollowsViewer)
  }

  /// Pure synchronous evaluation of chat eligibility from a declaration record.
  public nonisolated static func evaluateEligibility(
    declaration: BlueCatbirdChatDeclaration?,
    targetFollowsViewer: Bool
  ) -> Bool {
    guard let declaration = declaration else {
      // Absence of declaration means the user does not use Catbird secure chat.
      return false
    }

    guard declaration.protocolVersion == "1" else {
      // Incompatible protocol version
      return false
    }

    switch declaration.allowIncoming {
    case "all":
      return true
    case "none":
      return false
    case "following":
      return targetFollowsViewer
    default:
      // Unknown policy value -> fail closed
      return false
    }
  }

  public enum DeclarationError: LocalizedError {
    case networkFailure(String)
    public var errorDescription: String? {
      switch self {
      case .networkFailure(let reason): return reason
      }
    }
  }
}
