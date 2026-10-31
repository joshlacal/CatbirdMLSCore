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

  /// In-memory cache of last known epochs per group (key: didHash/groupIdHex)
  private var epochCache: [String: EpochRecord] = [:]

  /// Per-DID serialized write tail task (bounded: one task per DID, chained so reset
  /// can drain all prior writes by awaiting the tail).
  private var writeTails: [String: Task<Void, Never>] = [:]

  /// Per-DID write suspension flag (set during reset to block new writes)
  private var suspendedDIDs: Set<String> = []

  /// File storage directory (shared container for cross-process access)
  private let checkpointDir: URL
  private let explicitCheckpointDir: URL?

  private enum PathValidationError: LocalizedError {
    case containmentViolation(String)
    case invalidGroupIdentifier(String)

    var errorDescription: String? {
      switch self {
      case .containmentViolation(let path):
        return "Epoch checkpoint path escaped checkpoint root: \(path)"
      case .invalidGroupIdentifier(let groupId):
        return "Invalid epoch checkpoint group identifier: \(groupId)"
      }
    }
  }

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
      let resolved = try MLSStoragePaths.checkpointsDirectory()
      checkpointDir = resolved
      explicitCheckpointDir = nil
      try FileManager.default.createDirectory(at: resolved, withIntermediateDirectories: true)
    } catch {
      fatalError("Required App Group container unavailable for MLSEpochCheckpoint: \(error.localizedDescription)")
    }

    logger.info("✅ [MLSEpochCheckpoint] Initialized at \(self.checkpointDir.path)")
  }

  internal init(checkpointDirectory: URL) {
    checkpointDir = checkpointDirectory
    explicitCheckpointDir = checkpointDirectory
    try? FileManager.default.createDirectory(at: checkpointDirectory, withIntermediateDirectories: true)
  }

  // MARK: - Layout

  private func currentCheckpointRoot() -> URL {
    if let explicitCheckpointDir {
      return explicitCheckpointDir
    }
    return (try? MLSStoragePaths.checkpointsDirectory()) ?? checkpointDir
  }

  /// Strict validation matching `^[a-z0-9:._-]+$` with no path separators or `..` segments.
  internal static func isSafeLegacyDirectoryComponent(_ candidate: String) -> Bool {
    guard !candidate.isEmpty,
          candidate != ".",
          candidate != "..",
          !candidate.contains(".."),
          !candidate.contains("/"),
          !candidate.contains("\\")
    else {
      return false
    }
    for scalar in candidate.unicodeScalars {
      let v = scalar.value
      let isLowerAlpha = v >= 0x61 && v <= 0x7A
      let isDigit = v >= 0x30 && v <= 0x39
      let isAllowedPunct = scalar == ":" || scalar == "." || scalar == "_" || scalar == "-"
      if !(isLowerAlpha || isDigit || isAllowedPunct) {
        return false
      }
    }
    return true
  }

  /// Per-DID checkpoint directory keyed on `MLSStoragePaths.didHash(for:)` and containment-checked.
  internal func checkpointDirectory(for userDID: String) throws -> URL {
    let root = currentCheckpointRoot()
    let standardizedCheckpointRoot = root.standardizedFileURL
    let didHash = MLSStoragePaths.didHash(for: userDID)
    let userDir = root.appendingPathComponent(didHash, isDirectory: true)
    let standardizedUserDir = userDir.standardizedFileURL
    guard standardizedUserDir.path.hasPrefix(standardizedCheckpointRoot.path + "/") else {
      throw PathValidationError.containmentViolation(standardizedUserDir.path)
    }
    return userDir
  }

  private func checkpointURL(for userDID: String, groupIdHex: String) throws -> URL {
    guard !groupIdHex.isEmpty, groupIdHex.allSatisfy(\.isHexDigit) else {
      throw PathValidationError.invalidGroupIdentifier(groupIdHex)
    }
    let userDir = try checkpointDirectory(for: userDID)
    let standardizedUserDir = userDir.standardizedFileURL
    let fileURL = userDir.appendingPathComponent("\(groupIdHex).json")
    let standardizedFileURL = fileURL.standardizedFileURL
    guard standardizedFileURL.path.hasPrefix(standardizedUserDir.path + "/") else {
      throw PathValidationError.containmentViolation(standardizedFileURL.path)
    }
    return fileURL
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
    let didKey = MLSStoragePaths.didHash(for: userDID)
    let groupIdHex = groupId.hexEncodedString()
    let cacheKey = "\(didKey)/\(groupIdHex)"

    let expectedGen = (try? MLSCoordinationStore.shared.getState().coordinationGeneration) ?? 0

    // Block new writes while this DID is under reset
    if suspendedDIDs.contains(didKey) || MLSStoragePaths.isResetActive(for: normalizedDID) {
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

    // Resolve and validate paths strictly inside checkpoint root before enqueueing disk write
    let rootSnapshot = currentCheckpointRoot().standardizedFileURL
    let userDir: URL
    let fileURL: URL
    do {
      userDir = try checkpointDirectory(for: userDID)
      fileURL = try checkpointURL(for: userDID, groupIdHex: groupIdHex)
    } catch {
      logger.error("❌ [MLSEpochCheckpoint] Refusing epoch write due to invalid path: \(error.localizedDescription)")
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

    // Acquire shared reset-lifecycle lease synchronously BEFORE publishing/enqueueing the tail:
    let lifecycleLease: MLSLeaseToken
    do {
      lifecycleLease = try MLSStorageCoordinator.shared.acquireSharedResetLifecycleLeaseSync(for: normalizedDID)
    } catch {
      logger.warning("🛡️ [EPOCH] Failed to acquire shared reset-lifecycle lease for \(normalizedDID.prefix(20), privacy: .private): \(error.localizedDescription)")
      return
    }

    // Chain this write onto the per-DID serialized tail (bounded: one task per DID).
    let previousTail = writeTails[didKey]
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
        let standardizedUserDir = userDir.standardizedFileURL
        guard standardizedUserDir.path.hasPrefix(rootSnapshot.path + "/"),
              fileURL.standardizedFileURL.path.hasPrefix(standardizedUserDir.path + "/")
        else {
          throw PathValidationError.containmentViolation(standardizedUserDir.path)
        }
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
    writeTails[didKey] = task
  }

  /// Check if in-memory epoch is stale compared to disk
  /// Returns nil if no checkpoint exists (first time seeing this group)
  public func checkStaleness(
    userDID: String,
    groupId: Data,
    inMemoryEpoch: UInt64
  ) async -> SyncCheckResult? {
    let didKey = MLSStoragePaths.didHash(for: userDID)
    let groupIdHex = groupId.hexEncodedString()
    let cacheKey = "\(didKey)/\(groupIdHex)"

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
    guard let fileURL = try? checkpointURL(for: userDID, groupIdHex: groupIdHex) else {
      return nil
    }

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
    let didKey = MLSStoragePaths.didHash(for: userDID)
    let groupIdHex = groupId.hexEncodedString()
    let cacheKey = "\(didKey)/\(groupIdHex)"

    // Check cache first
    if let cached = epochCache[cacheKey] {
      return cached.epoch
    }

    // Check disk
    guard let fileURL = try? checkpointURL(for: userDID, groupIdHex: groupIdHex),
          FileManager.default.fileExists(atPath: fileURL.path)
    else {
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
    let didKey = MLSStoragePaths.didHash(for: userDID)
    let groupIdHex = groupId.hexEncodedString()
    let cacheKey = "\(didKey)/\(groupIdHex)"
    epochCache.removeValue(forKey: cacheKey)

    if let fileURL = try? checkpointURL(for: userDID, groupIdHex: groupIdHex) {
      let root = currentCheckpointRoot().standardizedFileURL
      if fileURL.standardizedFileURL.path.hasPrefix(root.path + "/") {
        try? FileManager.default.removeItem(at: fileURL)
      }
    }
    logger.debug("🗑️ [EPOCH] Cleared checkpoint for group \(groupIdHex.prefix(16))...")
  }

  /// Clear all checkpoints for a user (call on logout/account switch).
  /// Blocks new writes, drains pending write tasks, then deletes the per-DID directory.
  /// Throws on deletion failure so reset can abort and preserve authority.
  public func clearAllCheckpoints(userDID: String) async throws {
    let normalizedDID = MLSStoragePaths.normalizeDID(userDID)
    let didKey = MLSStoragePaths.didHash(for: userDID)
    let prefix = "\(didKey)/"

    // 1. Block new writes for this DID
    suspendedDIDs.insert(didKey)

    // 2. Drain the serialized write tail for this DID (all prior writes complete)
    if let tail = writeTails.removeValue(forKey: didKey) {
      _ = await tail.value
    }

    // 3. Evict cache entries for this DID
    epochCache = epochCache.filter { !$0.key.hasPrefix(prefix) }

    // 4. Delete the hashed per-DID directory (enforcing strict root containment)
    let root = currentCheckpointRoot()
    let standardizedCheckpointRoot = root.standardizedFileURL
    let userDir = try checkpointDirectory(for: userDID)
    let standardizedUserDir = userDir.standardizedFileURL
    guard standardizedUserDir.path.hasPrefix(standardizedCheckpointRoot.path + "/") else {
      throw PathValidationError.containmentViolation(standardizedUserDir.path)
    }
    if try MLSStoragePaths.fileExistsStrict(at: userDir) {
      try FileManager.default.removeItem(at: userDir)
    }

    // 5. Legacy unhashed directory cleanup: ONLY touch if normalizedDID matches strict
    //    `^[a-z0-9:._-]+$` (no `/`, `\`, or `..`) AND remains strictly inside checkpoint root.
    if normalizedDID != didKey && Self.isSafeLegacyDirectoryComponent(normalizedDID) {
      let legacyDir = root.appendingPathComponent(normalizedDID, isDirectory: true)
      let standardizedLegacyDir = legacyDir.standardizedFileURL
      if standardizedLegacyDir.path.hasPrefix(standardizedCheckpointRoot.path + "/"),
         try MLSStoragePaths.fileExistsStrict(at: legacyDir) {
        try FileManager.default.removeItem(at: legacyDir)
      }
    }

    logger.info("🗑️ [EPOCH] Cleared all epoch checkpoints for \(normalizedDID.prefix(20), privacy: .private)")
  }

  /// Resume writes for a DID after a successful reset.
  public func resumeWrites(userDID: String) {
    let didKey = MLSStoragePaths.didHash(for: userDID)
    suspendedDIDs.remove(didKey)
  }

  /// Reload cache from disk (call after app foregrounding)
  public func reloadCacheFromDisk() async {
    epochCache.removeAll()

    do {
      let root = currentCheckpointRoot()
      let standardizedCheckpointRoot = root.standardizedFileURL
      guard FileManager.default.fileExists(atPath: root.path) else {
        return
      }
      let userDirs = try FileManager.default.contentsOfDirectory(at: root, includingPropertiesForKeys: nil)
      var reloadedCount = 0

      for userDir in userDirs where userDir.hasDirectoryPath {
        let dirComponent = userDir.lastPathComponent
        guard Self.isSafeLegacyDirectoryComponent(dirComponent),
              userDir.standardizedFileURL.path.hasPrefix(standardizedCheckpointRoot.path + "/")
        else {
          continue
        }
        let didBucket: String
        if dirComponent.count == 64 && dirComponent.allSatisfy(\.isHexDigit) {
          didBucket = dirComponent.lowercased()
        } else {
          didBucket = MLSStoragePaths.didHash(for: dirComponent)
        }
        let files = try FileManager.default.contentsOfDirectory(at: userDir, includingPropertiesForKeys: nil)
        for file in files where file.pathExtension == "json" {
          do {
            let data = try Data(contentsOf: file)
            let record = try JSONDecoder().decode(EpochRecord.self, from: data)
            guard !record.groupId.isEmpty, record.groupId.allSatisfy(\.isHexDigit) else {
              continue
            }
            epochCache["\(didBucket)/\(record.groupId)"] = record
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
