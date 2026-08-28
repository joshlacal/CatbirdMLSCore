import Foundation
import OSLog

/// Lightweight epoch checkpoint for fast staleness detection without SQLite queries.
/// This enables sub-millisecond detection of NSE<>App epoch desync.
///
/// The checkpoint is written to shared container after every epoch-advancing operation
/// (message decrypt, commit processing) by whichever process (App or NSE) performed it.
///
/// This is in CatbirdMLSCore so both the main app and NSE can access it.
public actor MLSEpochCheckpoint {
  public static let shared = MLSEpochCheckpoint()

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSEpochCheckpoint")

  /// In-memory cache of last known epochs per group (key: normalizedDID/groupIdHex)
  private var epochCache: [String: EpochRecord] = [:]

  /// Per-DID serialized write tail task (bounded: one task per DID, chained so reset
  /// can drain all prior writes by awaiting the tail).
  private var writeTails: [String: Task<Void, Never>] = [:]

  /// Per-DID write suspension flag (set during reset to block new writes)
  private var suspendedDIDs: Set<String> = []

  /// File storage directory (shared container for cross-process access)
  private let checkpointDir: URL

  #if DEBUG
  private var testPreDirectoryCreationHook: (@Sendable () async -> Void)?

  public func setTestPreDirectoryCreationHook(_ hook: (@Sendable () async -> Void)?) {
    testPreDirectoryCreationHook = hook
  }

  func getTestPreDirectoryCreationHook() -> (@Sendable () async -> Void)? {
    testPreDirectoryCreationHook
  }
  #endif

  /// Epoch record with metadata
  public struct EpochRecord: Codable, Sendable {
    public let groupId: String  // Hex-encoded group ID
    public let epoch: UInt64
    public let lastModified: Date
    public let modifiedBy: ProcessIdentifier

    public enum ProcessIdentifier: String, Codable, Sendable {
      case mainApp = "app"
      case notificationServiceExtension = "nse"
    }

    public init(groupId: String, epoch: UInt64, lastModified: Date, modifiedBy: ProcessIdentifier) {
      self.groupId = groupId
      self.epoch = epoch
      self.lastModified = lastModified
      self.modifiedBy = modifiedBy
    }
  }

  /// Result of epoch sync check
  public struct SyncCheckResult: Sendable {
    public let wasStale: Bool
    public let diskEpoch: UInt64
    public let memoryEpoch: UInt64
    public let modifiedBy: EpochRecord.ProcessIdentifier?

    public init(wasStale: Bool, diskEpoch: UInt64, memoryEpoch: UInt64, modifiedBy: EpochRecord.ProcessIdentifier?) {
      self.wasStale = wasStale
      self.diskEpoch = diskEpoch
      self.memoryEpoch = memoryEpoch
      self.modifiedBy = modifiedBy
    }
  }

  // MARK: - Initialization

  private init() {
    do {
      checkpointDir = try MLSStoragePaths.checkpointsDirectory()
      try FileManager.default.createDirectory(at: checkpointDir, withIntermediateDirectories: true)
    } catch {
      fatalError("Required App Group container unavailable for MLSEpochCheckpoint: \(error.localizedDescription)")
    }

    logger.info("✅ [MLSEpochCheckpoint] Initialized at \(self.checkpointDir.path)")
  }

  // MARK: - Layout

  /// Per-DID checkpoint directory. Reset deletes this whole directory, so every
  /// reader and writer must derive file paths from it.
  private func userDirectory(_ normalizedDID: String) -> URL {
    checkpointDir.appendingPathComponent(normalizedDID, isDirectory: true)
  }

  private func checkpointURL(_ normalizedDID: String, _ groupIdHex: String) -> URL {
    userDirectory(normalizedDID).appendingPathComponent("\(groupIdHex).json")
  }

  // MARK: - Public API

  /// Record an epoch update for a group
  /// Call this after any operation that advances the epoch (decrypt, commit)
  public func recordEpoch(
    userDID: String,
    groupId: Data,
    epoch: UInt64,
    isNSE: Bool = false
  ) {
    let normalizedDID = MLSStoragePaths.normalizeDID(userDID)
    let groupIdHex = groupId.hexEncodedString()
    let cacheKey = "\(normalizedDID)/\(groupIdHex)"

    let expectedGen = (try? MLSCoordinationStore.shared.getState().coordinationGeneration) ?? 0

    // Block new writes while this DID is under reset
    if suspendedDIDs.contains(normalizedDID) || MLSStoragePaths.isResetActive(for: normalizedDID) {
      logger.warning("🛡️ [EPOCH] Write blocked for \(normalizedDID.prefix(20), privacy: .private) during reset")
      return
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // MONOTONIC GUARD (2024-12-24): Prevent Epoch Regression
    // ═══════════════════════════════════════════════════════════════════════════
    // Check strict monotonicity to prevent overwriting a newer epoch with an older one.
    // This happens if a slower process (e.g. NSE) finishes processing an old message
    // after the App has already processed a newer one.
    // ═══════════════════════════════════════════════════════════════════════════

    // Get current state (Memory)
    let currentMemoryEpoch = epochCache[cacheKey]?.epoch ?? 0
    let processLabel = isNSE ? "NSE" : "App"

    logger.debug("📝 [EPOCH-TRIO] Request from \(processLabel): New=\(epoch) | Memory=\(currentMemoryEpoch) | Group=\(groupIdHex.prefix(16))...")

    // Check Memory Monotonicity
    if epoch < currentMemoryEpoch {
      logger.warning("🛡️ [MONOTONIC-GUARD] REJECTED write: New \(epoch) < Memory \(currentMemoryEpoch). We are lagging behind.")
      return
    }

    // Create record
    let record = EpochRecord(
      groupId: groupIdHex,
      epoch: epoch,
      lastModified: Date(),
      modifiedBy: isNSE ? .notificationServiceExtension : .mainApp
    )

    // Update in-memory cache (we passed the check)
    epochCache[cacheKey] = record

    // Persist to disk (fire-and-forget for performance, but tracked for reset drain)
    let userDir = userDirectory(normalizedDID)
    let fileURL = checkpointURL(normalizedDID, groupIdHex)

    // Acquire shared reset-lifecycle lease synchronously BEFORE publishing/enqueueing the tail:
    let lifecycleLease: MLSLeaseToken
    do {
      lifecycleLease = try MLSStorageCoordinator.shared.acquireSharedResetLifecycleLeaseSync(for: normalizedDID)
    } catch {
      logger.warning("🛡️ [EPOCH] Failed to acquire shared reset-lifecycle lease for \(normalizedDID.prefix(20), privacy: .private): \(error.localizedDescription)")
      return
    }

    // Chain this write onto the per-DID serialized tail (bounded: one task per DID).
    let previousTail = writeTails[normalizedDID]
    let task = Task.detached(priority: .utility) { [logger, weak self] in
      defer { lifecycleLease.release() }
      _ = await previousTail?.value
      do {
        // Under the shared reset-lifecycle lease, revalidate reset-sentinel and coordination generation
        guard !MLSStoragePaths.isResetActive(for: normalizedDID) else {
          logger.warning("🛡️ [EPOCH-GUARD] Aborting epoch disk write - persistent reset sentinel is active for \(normalizedDID.prefix(20), privacy: .private)")
          return
        }
        guard let currentCoord = try? MLSCoordinationStore.shared.getState(),
              currentCoord.phase != .closed,
              currentCoord.coordinationGeneration == expectedGen else {
          logger.warning("🛡️ [EPOCH-GUARD] Aborting epoch disk write - coordination state changed/closed during reset")
          return
        }

        #if DEBUG
        if let selfInstance = self, let hook = await selfInstance.testPreDirectoryCreationHook {
          await hook()
        }
        #endif
        try FileManager.default.createDirectory(at: userDir, withIntermediateDirectories: true)
        var shouldWrite = true
        if FileManager.default.fileExists(atPath: fileURL.path) {
          let diskData = try Data(contentsOf: fileURL)
          let diskRecord = try JSONDecoder().decode(EpochRecord.self, from: diskData)
          if epoch < diskRecord.epoch {
            logger.warning("🛡️ [MONOTONIC-GUARD] REJECTED disk write: New \(epoch) < Disk \(diskRecord.epoch). Other process won.")
            shouldWrite = false
          } else {
            logger.debug("🔍 [EPOCH-TRIO] Disk Check: Disk=\(diskRecord.epoch) | New=\(epoch)")
          }
        }
        if shouldWrite {
          let data = try JSONEncoder().encode(record)
          try data.write(to: fileURL, options: .atomic)
          logger.info("📍 [EPOCH-COMMIT] Persisted epoch \(epoch) for \(groupIdHex.prefix(8))... (by: \(record.modifiedBy.rawValue))")
        }
      } catch {
        logger.error("❌ [MLSEpochCheckpoint] Failed to persist epoch: \(error.localizedDescription)")
      }
    }
    writeTails[normalizedDID] = task
  }

  /// Check if in-memory epoch is stale compared to disk
  /// Returns nil if no checkpoint exists (first time seeing this group)
  public func checkStaleness(
    userDID: String,
    groupId: Data,
    inMemoryEpoch: UInt64
  ) async -> SyncCheckResult? {
    let normalizedDID = MLSStoragePaths.normalizeDID(userDID)
    let groupIdHex = groupId.hexEncodedString()
    let cacheKey = "\(normalizedDID)/\(groupIdHex)"

    // Try in-memory cache first (fastest)
    if let cached = epochCache[cacheKey] {
      return SyncCheckResult(
        wasStale: cached.epoch > inMemoryEpoch,
        diskEpoch: cached.epoch,
        memoryEpoch: inMemoryEpoch,
        modifiedBy: cached.modifiedBy
      )
    }

    // Fall back to disk read
    let fileURL = checkpointURL(normalizedDID, groupIdHex)

    guard FileManager.default.fileExists(atPath: fileURL.path) else {
      // No checkpoint exists - this is first time seeing this group
      return nil
    }

    do {
      let data = try Data(contentsOf: fileURL)
      let record = try JSONDecoder().decode(EpochRecord.self, from: data)

      // Update in-memory cache
      epochCache[cacheKey] = record

      let wasStale = record.epoch > inMemoryEpoch
      if wasStale {
        logger.warning("🔄 [EPOCH FENCE] Stale context detected for group \(groupIdHex.prefix(16))...: disk=\(record.epoch), memory=\(inMemoryEpoch) (last modified by: \(record.modifiedBy.rawValue))")
      }

      return SyncCheckResult(
        wasStale: wasStale,
        diskEpoch: record.epoch,
        memoryEpoch: inMemoryEpoch,
        modifiedBy: record.modifiedBy
      )
    } catch {
      logger.error("❌ [MLSEpochCheckpoint] Failed to read checkpoint: \(error.localizedDescription)")
      return nil
    }
  }

  /// Get the last known epoch for a group (from cache or disk)
  public func getLastKnownEpoch(userDID: String, groupId: Data) async -> UInt64? {
    let normalizedDID = MLSStoragePaths.normalizeDID(userDID)
    let groupIdHex = groupId.hexEncodedString()
    let cacheKey = "\(normalizedDID)/\(groupIdHex)"

    // Check cache first
    if let cached = epochCache[cacheKey] {
      return cached.epoch
    }

    // Check disk
    let fileURL = checkpointURL(normalizedDID, groupIdHex)
    guard FileManager.default.fileExists(atPath: fileURL.path) else {
      return nil
    }

    do {
      let data = try Data(contentsOf: fileURL)
      let record = try JSONDecoder().decode(EpochRecord.self, from: data)
      epochCache[cacheKey] = record
      return record.epoch
    } catch {
      return nil
    }
  }

  /// Clear checkpoint for a group (call on group leave/delete)
  public func clearCheckpoint(userDID: String, groupId: Data) {
    let normalizedDID = MLSStoragePaths.normalizeDID(userDID)
    let groupIdHex = groupId.hexEncodedString()
    let cacheKey = "\(normalizedDID)/\(groupIdHex)"
    epochCache.removeValue(forKey: cacheKey)

    let fileURL = checkpointURL(normalizedDID, groupIdHex)
    try? FileManager.default.removeItem(at: fileURL)
    logger.debug("🗑️ [EPOCH] Cleared checkpoint for group \(groupIdHex.prefix(16))...")
  }

  /// Clear all checkpoints for a user (call on logout/account switch).
  /// Blocks new writes, drains pending write tasks, then deletes the per-DID directory.
  /// Throws on deletion failure so reset can abort and preserve authority.
  public func clearAllCheckpoints(userDID: String) async throws {
    let normalizedDID = MLSStoragePaths.normalizeDID(userDID)
    let prefix = "\(normalizedDID)/"

    // 1. Block new writes for this DID
    suspendedDIDs.insert(normalizedDID)

    // 2. Drain the serialized write tail for this DID (all prior writes complete)
    if let tail = writeTails.removeValue(forKey: normalizedDID) {
      _ = await tail.value
    }

    // 3. Evict cache entries for this DID
    epochCache = epochCache.filter { !$0.key.hasPrefix(prefix) }

    // 4. Delete the per-DID directory (throwing on failure)
    let userDir = userDirectory(normalizedDID)
    if try MLSStoragePaths.fileExistsStrict(at: userDir) {
      try FileManager.default.removeItem(at: userDir)
    }
    logger.info("🗑️ [EPOCH] Cleared all epoch checkpoints for \(normalizedDID.prefix(20), privacy: .private)")
  }

  /// Resume writes for a DID after a successful reset.
  public func resumeWrites(userDID: String) {
    let normalizedDID = MLSStoragePaths.normalizeDID(userDID)
    suspendedDIDs.remove(normalizedDID)
  }

  /// Reload cache from disk (call after app foregrounding)
  public func reloadCacheFromDisk() async {
    epochCache.removeAll()

    do {
      let userDirs = try FileManager.default.contentsOfDirectory(at: checkpointDir, includingPropertiesForKeys: nil)
      var reloadedCount = 0

      for userDir in userDirs where userDir.hasDirectoryPath {
        let normalizedDID = userDir.lastPathComponent
        let files = try FileManager.default.contentsOfDirectory(at: userDir, includingPropertiesForKeys: nil)
        for file in files where file.pathExtension == "json" {
          do {
            let data = try Data(contentsOf: file)
            let record = try JSONDecoder().decode(EpochRecord.self, from: data)
            epochCache["\(normalizedDID)/\(record.groupId)"] = record
            reloadedCount += 1
          } catch {
            // Skip corrupted files
            logger.warning("⚠️ [EPOCH] Skipping corrupted checkpoint: \(file.lastPathComponent)")
          }
        }
      }

      logger.info("🔄 [EPOCH] Reloaded \(reloadedCount) epoch checkpoint(s) from disk")
    } catch {
      logger.error("❌ [MLSEpochCheckpoint] Failed to reload cache: \(error.localizedDescription)")
    }
  }
}
