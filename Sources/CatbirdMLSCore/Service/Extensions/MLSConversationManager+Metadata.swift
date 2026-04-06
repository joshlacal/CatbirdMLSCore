import CryptoKit
import Foundation
import OSLog
import Petrel
import Synchronization

// MARK: - Metadata Types

/// JSON-serialized MetadataReference from the Rust FFI layer.
/// This lightweight reference lives in the MLS group's app_data_dictionary.
/// It points to an encrypted blob stored on the server.
public struct MetadataReferenceJSON: Codable, Sendable {
  /// Schema identifier (e.g., "blue.catbird/group-metadata/v1")
  public let schema: String
  /// Monotonic counter, incremented on each metadata update
  public let metadata_version: UInt64
  /// UUIDv4 opaque locator for the encrypted blob on the server
  public let blob_locator: String
  /// SHA-256 of the encrypted blob (for integrity verification)
  public let ciphertext_hash: [UInt8]

  public init(
    schema: String,
    metadata_version: UInt64,
    blob_locator: String,
    ciphertext_hash: [UInt8]
  ) {
    self.schema = schema
    self.metadata_version = metadata_version
    self.blob_locator = blob_locator
    self.ciphertext_hash = ciphertext_hash
  }
}

/// Plaintext group metadata payload (v1 schema).
/// This is what gets encrypted/decrypted in the metadata blob.
public struct GroupMetadataV1: Codable, Sendable {
  public let version: UInt32
  public let title: String
  public let description: String
  public let avatar_blob_locator: String?
  public let avatar_content_type: String?

  public init(
    version: UInt32 = 1,
    title: String,
    description: String = "",
    avatar_blob_locator: String? = nil,
    avatar_content_type: String? = nil
  ) {
    self.version = version
    self.title = title
    self.description = description
    self.avatar_blob_locator = avatar_blob_locator
    self.avatar_content_type = avatar_content_type
  }
}

// MARK: - MLSConversationManager Metadata Extension

extension MLSConversationManager {

  // MARK: - Failed Blob Tracking

  private static let failedBlobKeys = Mutex<Set<String>>([])
  private static let failedBlobAttempts = Mutex<[String: Int]>([:])

  private func blobCacheKey(locator: String?, groupId: String) -> String {
    "\(locator ?? "latest")|\(groupId)"
  }

  private func shouldSkipBlobFetch(locator: String?, groupId: String) -> Bool {
    let key = blobCacheKey(locator: locator, groupId: groupId)
    return Self.failedBlobKeys.withLock { $0.contains(key) }
  }

  private func recordBlobFetchFailure(locator: String?, groupId: String) {
    let key = blobCacheKey(locator: locator, groupId: groupId)
    let attempts = Self.failedBlobAttempts.withLock { attempts in
      let count = (attempts[key] ?? 0) + 1
      attempts[key] = count
      return count
    }
    if attempts >= 2 {
      Self.failedBlobKeys.withLock { keys in
        _ = keys.insert(key)
      }
      logger.warning("🚫 [Metadata] Blob \(key.prefix(40)) failed \(attempts) times — suppressing future fetches")
    }
  }

  // MARK: - Join-Time Metadata Bootstrap

  /// Bootstrap metadata immediately after a successful Welcome join or External Commit.
  /// This reads the current `MetadataReference` from the joined MLS group state and
  /// decrypts the current metadata blob without waiting for a later commit.
  internal func bootstrapMetadataAfterJoin(
    groupIdHex: String,
    joinSource: String
  ) async {
    guard let userDid = userDid else {
      logger.warning("⚠️ [Metadata] No user DID - skipping \(joinSource) bootstrap")
      return
    }

    guard let groupIdData = Data(hexEncoded: groupIdHex) else {
      logger.warning("⚠️ [Metadata] Invalid group ID - skipping \(joinSource) bootstrap")
      return
    }

    do {
      // Try to get the metadata key and reference from the FFI layer.
      // getCurrentMetadata derives the key from the group's current epoch exporter.
      let bootstrapInfo = try await mlsClient.getCurrentMetadata(
        for: userDid,
        groupId: groupIdData
      )

      let metadataKey: Data
      let epoch: UInt64

      if let info = bootstrapInfo {
        metadataKey = info.metadataKey
        epoch = info.epoch

        // If we have a full reference (with blob locator), use the normal commit path
        if let refJson = info.metadataReferenceJson {
          logger.info(
            "🚀 [Metadata] Bootstrapping metadata after \(joinSource) for \(groupIdHex.prefix(16))... at epoch \(epoch) (has reference)"
          )
          await processMetadataFromCommit(
            groupIdHex: groupIdHex,
            metadataKey: metadataKey,
            epoch: epoch,
            metadataReferenceData: refJson
          )
          return
        }

        // Legacy compatibility: older groups may still lack a MetadataReference.
        logger.info(
          "🚀 [Metadata] Bootstrapping metadata after \(joinSource) for \(groupIdHex.prefix(16))... at epoch \(epoch) (legacy fallback: no MetadataReference)"
        )
      } else {
        // getCurrentMetadata returned nil — the group may not be loaded yet.
        // Derive epoch directly as a fallback confirmation, then retry.
        logger.info(
          "📋 [Metadata] getCurrentMetadata returned nil after \(joinSource) for \(groupIdHex.prefix(16))... — trying epoch-based fallback"
        )

        epoch = try await mlsClient.getEpoch(for: userDid, groupId: groupIdData)

        // Retry getCurrentMetadata now that we've confirmed the group exists via getEpoch
        guard
          let retryInfo = try await mlsClient.getCurrentMetadata(
            for: userDid,
            groupId: groupIdData
          )
        else {
          logger.warning(
            "⚠️ [Metadata] Still no metadata key after retry for \(groupIdHex.prefix(16))... — skipping bootstrap"
          )
          return
        }
        metadataKey = retryInfo.metadataKey

        if let refJson = retryInfo.metadataReferenceJson {
          logger.info(
            "🚀 [Metadata] Got metadata key and reference on retry for \(groupIdHex.prefix(16))... at epoch \(epoch)"
          )
          await processMetadataFromCommit(
            groupIdHex: groupIdHex,
            metadataKey: metadataKey,
            epoch: epoch,
            metadataReferenceData: refJson
          )
          return
        }

        logger.info(
          "🚀 [Metadata] Got metadata key on retry for \(groupIdHex.prefix(16))... at epoch \(epoch) (legacy fallback: no MetadataReference)"
        )
      }

      // Legacy compatibility path for groups that still lack MetadataReference.
      let encryptedBlob: Data
      do {
        encryptedBlob = try await apiClient.getLatestGroupMetadataBlob(groupId: groupIdHex)
        logger.debug(
          "📥 [Metadata] Fetched latest blob for bootstrap: \(encryptedBlob.count) bytes"
        )
      } catch {
        logger.warning(
          "⚠️ [Metadata] No metadata blob available on server for \(groupIdHex.prefix(16))...: \(error.localizedDescription)"
        )
        return
      }

      // Decrypt the blob using metadataVersion 1 (the version used during initial encryption)
      let metadataVersion: UInt64 = 1
      let metadata = try decryptMetadataBlobSwift(
        key: metadataKey,
        groupId: groupIdHex,
        epoch: epoch,
        metadataVersion: metadataVersion,
        ciphertext: encryptedBlob
      )

      logger.info(
        "✅ [Metadata] Decrypted bootstrap metadata: title='\(metadata.title)', description='\(metadata.description.prefix(50))'"
      )

      // Write to the local conversation database and notify observers
      await updateConversationMetadata(
        groupIdHex: groupIdHex,
        metadata: metadata,
        metadataKey: metadataKey,
        epoch: epoch,
        metadataVersion: metadataVersion
      )
    } catch {
      logger.warning(
        "⚠️ [Metadata] Failed to bootstrap metadata after \(joinSource): \(error.localizedDescription)"
      )
    }
  }

  // MARK: - Group Creation Metadata Upload

  /// Upload the encrypted metadata blob generated during group creation.
  /// Called after `createGroup` returns a `GroupCreationResult` with metadata artifacts.
  ///
  /// - Parameters:
  ///   - groupIdHex: Hex-encoded group ID
  ///   - encryptedBlob: The encrypted metadata blob bytes from the Rust FFI
  ///   - blobLocator: The UUIDv4 blob locator for server storage
  ///   - metadataReferenceJSON: JSON-serialized MetadataReference from FFI
  internal func uploadMetadataBlobAfterCreation(
    groupIdHex: String,
    encryptedBlob: Data,
    blobLocator: String,
    metadataReferenceJSON: Data?
  ) async {
    logger.info(
      "📤 [Metadata] Uploading metadata blob for new group \(groupIdHex.prefix(16))... locator: \(blobLocator.prefix(8))..."
    )

    do {
      let (confirmedLocator, size) = try await apiClient.putGroupMetadataBlob(
        blobLocator: blobLocator,
        groupId: groupIdHex,
        encryptedBlob: encryptedBlob
      )
      logger.info(
        "✅ [Metadata] Blob uploaded for group creation - locator: \(confirmedLocator.prefix(8))..., size: \(size) bytes"
      )

      // Parse and cache the MetadataReference for future re-wraps
      if let refJSON = metadataReferenceJSON {
        do {
          let ref = try JSONDecoder().decode(MetadataReferenceJSON.self, from: refJSON)
          logger.debug(
            "📋 [Metadata] Cached metadata reference: version=\(ref.metadata_version), schema=\(ref.schema)"
          )
        } catch {
          logger.warning(
            "⚠️ [Metadata] Failed to parse metadata reference JSON: \(error.localizedDescription)"
          )
        }
      }
    } catch {
      // Non-fatal: Group creation succeeds even if blob upload fails.
      // Metadata will be temporarily unavailable until the next successful re-wrap.
      logger.warning(
        "⚠️ [Metadata] Failed to upload metadata blob for group creation: \(error.localizedDescription)"
      )
      logger.warning(
        "   Group creation continues - metadata will be unavailable until next epoch re-wrap"
      )
    }
  }

  // MARK: - Incoming Commit Metadata Processing

  /// Process metadata information from an incoming commit.
  /// Fetches the encrypted blob from the server, decrypts it using the commit's metadata key,
  /// and updates the local conversation metadata.
  ///
  /// - Parameters:
  ///   - groupIdHex: Hex-encoded group ID
  ///   - metadataKey: 32-byte ChaCha20-Poly1305 key from CommitMetadataInfo
  ///   - epoch: The post-commit epoch this key is bound to
  ///   - metadataReferenceData: Raw bytes of the MetadataReference from app_data_dictionary
  ///   - blobLocator: Direct blob locator fallback for legacy paths
  internal func processMetadataFromCommit(
    groupIdHex: String,
    metadataKey: Data,
    epoch: UInt64,
    metadataReferenceData: Data? = nil,
    blobLocator: String? = nil
  ) async {
    logger.info(
      "📥 [Metadata] Processing metadata from commit for group \(groupIdHex.prefix(16))... epoch: \(epoch)"
    )

    var metadataVersion: UInt64 = 1
    var locator: String?
    var shouldFetchLatestBlob = false
    var parsedReference: MetadataReferenceJSON?

    if let refData = metadataReferenceData {
      do {
        let ref = try JSONDecoder().decode(MetadataReferenceJSON.self, from: refData)
        parsedReference = ref
        locator = ref.blob_locator
        metadataVersion = ref.metadata_version
        logger.debug(
          "📋 [Metadata] Using MetadataReference: locator=\(ref.blob_locator.prefix(8))..., version=\(metadataVersion)"
        )
      } catch {
        logger.warning(
          "⚠️ [Metadata] Failed to parse MetadataReference: \(error.localizedDescription)"
        )
      }
    }

    if locator == nil, let directLocator = blobLocator {
      locator = directLocator
    }

    if locator == nil {
      shouldFetchLatestBlob = true
      logger.info(
        "📋 [Metadata] No metadata reference or locator in commit - using legacy latest-blob fallback for group \(groupIdHex.prefix(16))..."
      )
    }

    // Fetch the encrypted blob from the server
    let encryptedBlob: Data
    do {
      if shouldFetchLatestBlob {
        encryptedBlob = try await apiClient.getLatestGroupMetadataBlob(groupId: groupIdHex)
        logger.debug(
          "📥 [Metadata] Fetched latest encrypted blob: \(encryptedBlob.count) bytes"
        )
      } else if let locator {
        if shouldSkipBlobFetch(locator: locator, groupId: groupIdHex) {
          logger.debug("⏭️ [Metadata] Skipping known-failed blob fetch for \(groupIdHex.prefix(16))")
          return
        }
        encryptedBlob = try await apiClient.getGroupMetadataBlob(
          blobLocator: locator,
          groupId: groupIdHex
        )
        logger.debug(
          "📥 [Metadata] Fetched encrypted blob: \(encryptedBlob.count) bytes"
        )
      } else {
        logger.warning("⚠️ [Metadata] No way to fetch metadata blob - skipping")
        return
      }
    } catch {
      if !shouldFetchLatestBlob {
        logger.warning(
          "⚠️ [Metadata] Failed to fetch metadata blob by locator: \(error.localizedDescription)"
        )
        recordBlobFetchFailure(locator: locator, groupId: groupIdHex)
        logger.warning("   Falling back to latest-blob lookup for legacy compatibility")
        do {
          encryptedBlob = try await apiClient.getLatestGroupMetadataBlob(groupId: groupIdHex)
        } catch {
          logger.warning(
            "⚠️ [Metadata] Legacy fallback fetch also failed: \(error.localizedDescription)"
          )
          logger.warning("   Metadata unavailable for this epoch - will recover on next re-wrap")
          return
        }
      } else {
        logger.warning(
          "⚠️ [Metadata] Failed to fetch metadata blob: \(error.localizedDescription)"
        )
        logger.warning("   Metadata unavailable for this epoch - will recover on next re-wrap")
        return
      }
    }

    // Verify integrity (SHA-256 of ciphertext)
    if let ref = parsedReference {
      if ref.ciphertext_hash.isEmpty {
        logger.debug("📋 [Metadata] MetadataReference has no ciphertext hash yet; skipping integrity check")
      } else {
        let computedHash = Array(SHA256.hash(data: encryptedBlob))
        if computedHash != ref.ciphertext_hash {
          logger.error(
            "❌ [Metadata] Ciphertext hash mismatch! Server may have tampered with blob."
          )
          logger.error("   Expected: \(ref.ciphertext_hash.prefix(8))...")
          logger.error("   Computed: \(computedHash.prefix(8))...")
          return
        }
        logger.debug("✅ [Metadata] Ciphertext integrity verified (SHA-256)")
      }
    }

    // Decrypt the metadata blob via Rust FFI
    do {
      let metadata = try decryptMetadataBlobSwift(
        key: metadataKey,
        groupId: groupIdHex,
        epoch: epoch,
        metadataVersion: metadataVersion,
        ciphertext: encryptedBlob
      )

      logger.info(
        "✅ [Metadata] Decrypted metadata: title='\(metadata.title)', description='\(metadata.description.prefix(50))'"
      )

      // Update the local conversation metadata in the database (including avatar if present)
      await updateConversationMetadata(
        groupIdHex: groupIdHex,
        metadata: metadata,
        metadataKey: metadataKey,
        epoch: epoch,
        metadataVersion: metadataVersion
      )
    } catch {
      logger.warning(
        "⚠️ [Metadata] Failed to decrypt metadata blob: \(error.localizedDescription)"
      )
      logger.warning("   Metadata unavailable for epoch \(epoch) - will recover on next re-wrap")
    }
  }

  // MARK: - Sender-Side Metadata Re-wrap

  /// Re-encrypt and upload metadata for the new epoch after merging a pending commit.
  /// This is the "eager re-wrap" step that ensures metadata remains accessible
  /// after every epoch-advancing commit.
  ///
  /// - Parameters:
  ///   - groupIdHex: Hex-encoded group ID
  ///   - metadataKey: 32-byte key from MergePendingCommitResult.commitMetadata
  ///   - epoch: The new post-merge epoch
  internal func reWrapMetadataAfterMerge(
    groupIdHex: String,
    metadataKey: Data,
    epoch: UInt64
  ) async {
    logger.info(
      "🔄 [Metadata] Re-wrapping metadata for group \(groupIdHex.prefix(16))... at epoch \(epoch)"
    )

    // Read current plaintext metadata from local cache / database
    guard let userDid = userDid else {
      logger.warning("⚠️ [Metadata] No user DID - skipping re-wrap")
      return
    }

    // Read the current metadata version from the group's MetadataReference.
    // This is the source of truth — it was set during group creation (version=1)
    // and incremented on each content change.
    var metadataVersion: UInt64 = 1
    var metadataBlobLocator: String?
    if let groupIdData = Data(hexEncoded: groupIdHex),
      let currentInfo = try? await mlsClient.getCurrentMetadata(for: userDid, groupId: groupIdData),
      let refJson = currentInfo.metadataReferenceJson,
      let ref = try? JSONDecoder().decode(MetadataReferenceJSON.self, from: refJson)
    {
      metadataVersion = ref.metadata_version
      metadataBlobLocator = ref.blob_locator
      logger.debug(
        "📋 [Metadata] Re-wrap using metadata_version=\(metadataVersion) and locator=\(ref.blob_locator.prefix(8))... from MetadataReference"
      )
    } else {
      logger.debug(
        "📋 [Metadata] No MetadataReference found, using legacy fallback metadata_version=\(metadataVersion)"
      )
    }

    // Fetch current metadata from local storage (FFI layer for title/description)
    let currentMetadata: GroupMetadataV1?
    var localAvatarData: Data?
    do {
      if let groupIdData = Data(hexEncoded: groupIdHex),
        let payload = try await mlsClient.getGroupMetadata(for: userDid, groupId: groupIdData)
      {
        // Read locally cached avatar image data from the database
        localAvatarData = try await database.read { db in
          try Data.fetchOne(
            db,
            sql: "SELECT avatarImageData FROM MLSConversationModel WHERE conversationID = ? AND currentUserDID = ?",
            arguments: [groupIdHex, userDid]
          )
        }

        // Build metadata with avatar locator if we have avatar data to re-encrypt
        let avatarLocator = localAvatarData != nil ? UUID().uuidString.lowercased() : nil
        currentMetadata = GroupMetadataV1(
          title: payload.name ?? "",
          description: payload.description ?? "",
          avatar_blob_locator: avatarLocator
        )
      } else {
        currentMetadata = nil
      }
    } catch {
      logger.debug(
        "⚠️ [Metadata] Could not read current metadata for re-wrap: \(error.localizedDescription)"
      )
      currentMetadata = nil
    }

    guard let metadata = currentMetadata else {
      logger.debug("📋 [Metadata] No metadata to re-wrap for group \(groupIdHex.prefix(16))...")
      return
    }

    let uploadLocator = metadataBlobLocator ?? UUID().uuidString.lowercased()

    do {
      let encryptedBlob = try encryptMetadataBlobSwift(
        key: metadataKey,
        groupId: groupIdHex,
        epoch: epoch,
        metadataVersion: metadataVersion,
        metadata: metadata
      )

      // Upload the re-wrapped metadata blob
      let (confirmedLocator, size) = try await apiClient.putGroupMetadataBlob(
        blobLocator: uploadLocator,
        groupId: groupIdHex,
        encryptedBlob: encryptedBlob
      )
      logger.info(
        "✅ [Metadata] Re-wrapped metadata uploaded - locator: \(confirmedLocator.prefix(8))..., size: \(size) bytes, epoch: \(epoch)"
      )

      // Re-encrypt and upload avatar blob if present
      if let avatarBytes = localAvatarData,
        let avatarLocator = metadata.avatar_blob_locator
      {
        let encryptedAvatar = try encryptAvatarBlobSwift(
          key: metadataKey,
          groupId: groupIdHex,
          epoch: epoch,
          metadataVersion: metadataVersion,
          avatarBytes: avatarBytes
        )
        let (avatarConfirmed, avatarSize) = try await apiClient.putGroupMetadataBlob(
          blobLocator: avatarLocator,
          groupId: groupIdHex,
          encryptedBlob: encryptedAvatar
        )
        logger.info(
          "✅ [Metadata] Re-wrapped avatar uploaded - locator: \(avatarConfirmed.prefix(8))..., size: \(avatarSize) bytes"
        )
      }
    } catch {
      // Non-fatal: Commit already succeeded. Metadata is temporarily unavailable
      // until the next successful re-wrap at a future epoch.
      logger.warning(
        "⚠️ [Metadata] Failed to re-wrap metadata: \(error.localizedDescription)"
      )
      logger.warning("   Commit succeeded but metadata temporarily unavailable")
    }
  }

  // MARK: - Local Metadata Update

  /// Update the local conversation database with decrypted metadata content.
  /// If the metadata contains an avatar blob locator, fetches and decrypts the avatar.
  ///
  /// - Parameters:
  ///   - groupIdHex: Hex-encoded group ID (also the conversation ID)
  ///   - metadata: Decrypted GroupMetadataV1 payload
  ///   - metadataKey: Optional 32-byte key for decrypting the avatar blob
  ///   - epoch: Post-commit epoch the key is bound to
  ///   - metadataVersion: Version counter for AAD binding
  private func updateConversationMetadata(
    groupIdHex: String,
    metadata: GroupMetadataV1,
    metadataKey: Data? = nil,
    epoch: UInt64 = 0,
    metadataVersion: UInt64 = 1
  ) async {
    guard let userDid = userDid else { return }

    // Fetch and decrypt avatar if locator is present
    var avatarData: Data?
    if let avatarLocator = metadata.avatar_blob_locator,
      !avatarLocator.isEmpty,
      let key = metadataKey
    {
      avatarData = await fetchAndDecryptAvatar(
        groupIdHex: groupIdHex,
        blobLocator: avatarLocator,
        metadataKey: key,
        epoch: epoch,
        metadataVersion: metadataVersion
      )
    }

    do {
      try await database.write { db in
        try db.execute(
          sql: """
            UPDATE MLSConversationModel
            SET title = ?, avatarImageData = ?, updatedAt = ?, isPlaceholder = 0
            WHERE conversationID = ? AND currentUserDID = ?
            """,
          arguments: [
            metadata.title.isEmpty ? nil : metadata.title,
            avatarData,
            Date(),
            groupIdHex,
            userDid,
          ]
        )
      }
      logger.debug(
        "✅ [Metadata] Updated conversation metadata for \(groupIdHex.prefix(16))...: title='\(metadata.title)', hasAvatar=\(avatarData != nil)"
      )

      // Notify observers that conversation metadata changed.
      // We use syncCompleted as a general-purpose "data changed" event.
      // A dedicated .metadataUpdated event could be added later if needed.
      notifyObservers(.syncCompleted(1))
    } catch {
      logger.warning(
        "⚠️ [Metadata] Failed to update conversation metadata: \(error.localizedDescription)"
      )
    }
  }

  // MARK: - Avatar Blob Fetch & Decrypt

  /// Fetch an encrypted avatar blob from the server and decrypt it.
  ///
  /// - Parameters:
  ///   - groupIdHex: Hex-encoded group ID
  ///   - blobLocator: UUIDv4 locator for the avatar blob on the server
  ///   - metadataKey: 32-byte ChaCha20-Poly1305 key from the epoch exporter
  ///   - epoch: Post-commit epoch the key is bound to
  ///   - metadataVersion: Version counter for AAD binding
  /// - Returns: Decrypted avatar image bytes, or nil on failure
  private func fetchAndDecryptAvatar(
    groupIdHex: String,
    blobLocator: String,
    metadataKey: Data,
    epoch: UInt64,
    metadataVersion: UInt64
  ) async -> Data? {
    if shouldSkipBlobFetch(locator: blobLocator, groupId: groupIdHex) {
      logger.debug("⏭️ [Metadata] Skipping known-failed blob fetch for \(groupIdHex.prefix(16))")
      return nil
    }
    do {
      let encryptedAvatar = try await apiClient.getGroupMetadataBlob(
        blobLocator: blobLocator,
        groupId: groupIdHex
      )
      logger.debug(
        "📥 [Metadata] Fetched encrypted avatar blob: \(encryptedAvatar.count) bytes"
      )

      let avatarBytes = try decryptAvatarBlobSwift(
        key: metadataKey,
        groupId: groupIdHex,
        epoch: epoch,
        metadataVersion: metadataVersion,
        ciphertext: encryptedAvatar
      )
      logger.info(
        "✅ [Metadata] Decrypted avatar: \(avatarBytes.count) bytes"
      )
      return avatarBytes
    } catch {
      logger.warning(
        "⚠️ [Metadata] Failed to fetch/decrypt avatar blob: \(error.localizedDescription)"
      )
      recordBlobFetchFailure(locator: blobLocator, groupId: groupIdHex)
      return nil
    }
  }

  // MARK: - Metadata Blob Encryption / Decryption (Rust FFI)

  /// Decrypt an avatar blob via Rust FFI (ChaCha20-Poly1305 with domain-separated AAD).
  private func decryptAvatarBlobSwift(
    key: Data,
    groupId: String,
    epoch: UInt64,
    metadataVersion: UInt64,
    ciphertext: Data
  ) throws -> Data {
    try mlsDecryptAvatarBlob(
      key: key,
      groupIdHex: groupId,
      epoch: epoch,
      metadataVersion: metadataVersion,
      ciphertext: ciphertext
    )
  }

  /// Decrypt a metadata blob via Rust FFI (ChaCha20-Poly1305).
  private func decryptMetadataBlobSwift(
    key: Data,
    groupId: String,
    epoch: UInt64,
    metadataVersion: UInt64,
    ciphertext: Data
  ) throws -> GroupMetadataV1 {
    let json = try mlsDecryptMetadataBlob(
      key: key,
      groupIdHex: groupId,
      epoch: epoch,
      metadataVersion: metadataVersion,
      ciphertext: ciphertext
    )
    return try JSONDecoder().decode(GroupMetadataV1.self, from: json)
  }

  /// Encrypt a metadata payload via Rust FFI (ChaCha20-Poly1305).
  private func encryptMetadataBlobSwift(
    key: Data,
    groupId: String,
    epoch: UInt64,
    metadataVersion: UInt64,
    metadata: GroupMetadataV1
  ) throws -> Data {
    let metadataJson = try JSONEncoder().encode(metadata)
    return try mlsEncryptMetadataBlob(
      key: key,
      groupIdHex: groupId,
      epoch: epoch,
      metadataVersion: metadataVersion,
      metadataJson: metadataJson
    )
  }

  /// Encrypt raw avatar bytes via Rust FFI (ChaCha20-Poly1305, domain-separated AAD).
  private func encryptAvatarBlobSwift(
    key: Data,
    groupId: String,
    epoch: UInt64,
    metadataVersion: UInt64,
    avatarBytes: Data
  ) throws -> Data {
    try mlsEncryptAvatarBlob(
      key: key,
      groupIdHex: groupId,
      epoch: epoch,
      metadataVersion: metadataVersion,
      avatarBytes: avatarBytes
    )
  }
}
