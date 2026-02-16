//
//  MLSStorage+EpochSecretStorage.swift
//  CatbirdMLSCore
//
//  Epoch secret storage bridge for MLS FFI.
//  Uses DatabasePool for better read concurrency.
//

import Foundation
import CatbirdMLS
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
    private let databaseManager: MLSGRDBManager

    /// Initialize with explicit userDID and database manager
    /// - Parameters:
    ///   - userDID: User's decentralized identifier
    ///   - databaseManager: Database manager instance (owned by session)
    public init(userDID: String, databaseManager: MLSGRDBManager) {
        self.userDID = userDID
        self.databaseManager = databaseManager
    }

    public func storeEpochSecret(conversationId: String, epoch: UInt64, secretData: Data) async -> Bool {
        logger.debug("[EPOCH-STORAGE] storeEpochSecret called: conversation=\(conversationId), epoch=\(epoch), \(secretData.count) bytes")

        do {
            // Use write(for:) to safely route to Pool or lightweight Queue
            try await databaseManager.write(for: userDID) { [storage, userDID] db in
                // Ensure conversation exists BEFORE storing epoch secret
                try storage.ensureConversationExistsSync(
                    userDID: userDID,
                    conversationID: conversationId,
                    groupID: conversationId,
                    db: db
                )

                try storage.saveEpochSecretSync(
                    userDID: userDID,
                    conversationID: conversationId,
                    epoch: epoch,
                    secretData: secretData,
                    db: db
                )
            }
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
            // Use read(for:) to safely route to Pool or lightweight Queue
            let data = try await databaseManager.read(for: userDID) { [storage, userDID] db in
                try storage.getEpochSecretSync(
                    userDID: userDID,
                    conversationID: conversationId,
                    epoch: epoch,
                    db: db
                )
            }

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
            // Use write(for:) to safely route to Pool or lightweight Queue
            try await databaseManager.write(for: userDID) { [storage, userDID] db in
                try storage.deleteEpochSecretSync(
                    userDID: userDID,
                    conversationID: conversationId,
                    epoch: epoch,
                    db: db
                )
            }
            logger.info("[EPOCH-STORAGE] Deleted epoch secret successfully")
            return true
        } catch {
            logger.error("[EPOCH-STORAGE] Failed to delete epoch secret: \(error.localizedDescription)")
            return false
        }
    }

    public func deleteEpochsBefore(conversationId: String, cutoffEpoch: UInt64) async -> UInt32 {
        logger.debug("[EPOCH-STORAGE] deleteEpochsBefore called: conversation=\(conversationId), cutoff=\(cutoffEpoch)")

        do {
            // Use write(for:) to safely route to Pool or lightweight Queue
            let deletedCount = try await databaseManager.write(for: userDID) { [storage, userDID] db in
                try storage.deleteEpochsBeforeSync(
                    userDID: userDID,
                    conversationID: conversationId,
                    cutoffEpoch: cutoffEpoch,
                    db: db
                )
            }
            logger.info("[EPOCH-STORAGE] Deleted \(deletedCount) old epoch secrets")
            return UInt32(deletedCount)
        } catch {
            logger.error("[EPOCH-STORAGE] Failed to delete old epoch secrets: \(error.localizedDescription)")
            return 0
        }
    }
}
