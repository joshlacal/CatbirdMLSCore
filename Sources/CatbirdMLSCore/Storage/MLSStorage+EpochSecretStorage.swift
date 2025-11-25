//
//  MLSStorage+EpochSecretStorage.swift
//  CatbirdMLSCore
//
//  Epoch secret storage bridge for MLS FFI
//

import Foundation
import MLSFFI
import OSLog

/// Epoch secret storage implementation for MLS FFI callback
///
/// Provides async storage for MLS epoch secrets with SQLCipher encryption.
/// With UniFFI async callback interfaces, the methods are natively async,
/// eliminating the need for semaphore-based sync/async bridging.
public final class MLSEpochSecretStorageBridge: EpochSecretStorage {

    private let storage = MLSStorage.shared
    private let logger = Logger(subsystem: "blue.catbird.mls", category: "EpochSecretStorage")
    private let userDID: String

    /// Initialize with explicit userDID parameter
    /// - Parameter userDID: User's decentralized identifier
    public init(userDID: String) {
        self.userDID = userDID
    }

    public func storeEpochSecret(conversationId: String, epoch: UInt64, secretData: Data) async -> Bool {
        logger.debug("[EPOCH-STORAGE] storeEpochSecret called: conversation=\(conversationId), epoch=\(epoch), \(secretData.count) bytes")

        do {
            let database = try await MLSGRDBManager.shared.getDatabaseQueue(for: userDID)

            // Ensure conversation exists BEFORE storing epoch secret
            try await storage.ensureConversationExists(
                userDID: userDID,
                conversationID: conversationId,
                groupID: conversationId,
                database: database
            )

            try await storage.saveEpochSecret(
                userDID: userDID,
                conversationID: conversationId,
                epoch: epoch,
                secretData: secretData,
                database: database
            )
            logger.info("[EPOCH-STORAGE] Stored epoch secret successfully")
            return true
        } catch {
            logger.error("[EPOCH-STORAGE] Failed to store epoch secret: \(error.localizedDescription)")
            return false
        }
    }

    public func getEpochSecret(conversationId: String, epoch: UInt64) async -> Data? {
        logger.debug("[EPOCH-STORAGE] getEpochSecret called: conversation=\(conversationId), epoch=\(epoch)")

        do {
            let database = try await MLSGRDBManager.shared.getDatabaseQueue(for: userDID)
            let data = try await storage.getEpochSecret(
                userDID: userDID,
                conversationID: conversationId,
                epoch: epoch,
                database: database
            )

            if let data = data {
                logger.info("[EPOCH-STORAGE] Retrieved epoch secret: \(data.count) bytes")
            } else {
                logger.debug("[EPOCH-STORAGE] No epoch secret found")
            }
            return data
        } catch {
            logger.error("[EPOCH-STORAGE] Failed to retrieve epoch secret: \(error.localizedDescription)")
            return nil
        }
    }

    public func deleteEpochSecret(conversationId: String, epoch: UInt64) async -> Bool {
        logger.debug("[EPOCH-STORAGE] deleteEpochSecret called: conversation=\(conversationId), epoch=\(epoch)")

        do {
            let database = try await MLSGRDBManager.shared.getDatabaseQueue(for: userDID)
            try await storage.deleteEpochSecret(
                userDID: userDID,
                conversationID: conversationId,
                epoch: epoch,
                database: database
            )
            logger.info("[EPOCH-STORAGE] Deleted epoch secret successfully")
            return true
        } catch {
            logger.error("[EPOCH-STORAGE] Failed to delete epoch secret: \(error.localizedDescription)")
            return false
        }
    }
}
