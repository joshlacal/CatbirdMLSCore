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
  
  /// In-memory cache of last known epochs per group
  private var epochCache: [String: EpochRecord] = [:]
  
  /// File storage directory (shared container for cross-process access)
  private let checkpointDir: URL
  
  // MARK: - Types
  
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
    // Use shared storage base for cross-process access when available
    let baseDirectory = MLSStoragePaths.baseContainerURL()
    checkpointDir = baseDirectory.appendingPathComponent("epoch-checkpoints", isDirectory: true)
    
    // Create directory if needed
    do {
      try FileManager.default.createDirectory(at: checkpointDir, withIntermediateDirectories: true)
    } catch {
      logger.error("❌ [MLSEpochCheckpoint] Failed to create checkpoint directory: \(error.localizedDescription)")
    }
    
    logger.info("✅ [MLSEpochCheckpoint] Initialized at \(self.checkpointDir.path)")
  }
  
  // MARK: - Public API
  
  /// Record an epoch update for a group
  /// Call this after any operation that advances the epoch (decrypt, commit)
  /// Record an epoch update for a group
  /// Call this after any operation that advances the epoch (decrypt, commit)
  public func recordEpoch(
    groupId: Data,
    epoch: UInt64,
    isNSE: Bool = false
  ) {
    let groupIdHex = groupId.map { String(format: "%02hhx", $0) }.joined()
    
    // ═══════════════════════════════════════════════════════════════════════════
    // MONOTONIC GUARD (2024-12-24): Prevent Epoch Regression
    // ═══════════════════════════════════════════════════════════════════════════
    // Check strict monotonicity to prevent overwriting a newer epoch with an older one.
    // This happens if a slower process (e.g. NSE) finishes processing an old message
    // after the App has already processed a newer one.
    // ═══════════════════════════════════════════════════════════════════════════
    
    // Get current state (Memory)
    let currentMemoryEpoch = epochCache[groupIdHex]?.epoch ?? 0
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
    epochCache[groupIdHex] = record
    
    // Persist to disk (fire-and-forget for performance)
    let fileURL = checkpointDir.appendingPathComponent("\(groupIdHex).json")
    
    Task.detached(priority: .utility) { [logger] in
      do {
        // Double-check disk state before writing (Atomic Swap + Guard)
        // We read the disk file to ensure we don't overwrite a newer file written by another process
        var shouldWrite = true
        if let diskData = try? Data(contentsOf: fileURL),
           let diskRecord = try? JSONDecoder().decode(EpochRecord.self, from: diskData) {
          
          if epoch < diskRecord.epoch {
             logger.warning("🛡️ [MONOTONIC-GUARD] REJECTED disk write: New \(epoch) < Disk \(diskRecord.epoch). Other process won.")
             shouldWrite = false
          } else {
             logger.debug("🔍 [EPOCH-TRIO] Disk Check: Disk=\(diskRecord.epoch) | New=\(epoch)")
          }
        }
        
        if shouldWrite {
          let data = try JSONEncoder().encode(record)
          // Use atomic writing to prevent partial writes
          try data.write(to: fileURL, options: .atomic)
          logger.info("📍 [EPOCH-COMMIT] Persisted epoch \(epoch) for \(groupIdHex.prefix(8))... (by: \(record.modifiedBy.rawValue))")
        }
      } catch {
        logger.error("❌ [MLSEpochCheckpoint] Failed to persist epoch: \(error.localizedDescription)")
      }
    }
  }
  
  /// Check if in-memory epoch is stale compared to disk
  /// Returns nil if no checkpoint exists (first time seeing this group)
  public func checkStaleness(
    groupId: Data,
    inMemoryEpoch: UInt64
  ) async -> SyncCheckResult? {
    let groupIdHex = groupId.map { String(format: "%02hhx", $0) }.joined()
    
    // Try in-memory cache first (fastest)
    if let cached = epochCache[groupIdHex] {
      return SyncCheckResult(
        wasStale: cached.epoch > inMemoryEpoch,
        diskEpoch: cached.epoch,
        memoryEpoch: inMemoryEpoch,
        modifiedBy: cached.modifiedBy
      )
    }
    
    // Fall back to disk read
    let fileURL = checkpointDir.appendingPathComponent("\(groupIdHex).json")
    
    guard FileManager.default.fileExists(atPath: fileURL.path) else {
      // No checkpoint exists - this is first time seeing this group
      return nil
    }
    
    do {
      let data = try Data(contentsOf: fileURL)
      let record = try JSONDecoder().decode(EpochRecord.self, from: data)
      
      // Update in-memory cache
      epochCache[groupIdHex] = record
      
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
  public func getLastKnownEpoch(groupId: Data) async -> UInt64? {
    let groupIdHex = groupId.map { String(format: "%02hhx", $0) }.joined()
    
    // Check cache first
    if let cached = epochCache[groupIdHex] {
      return cached.epoch
    }
    
    // Check disk
    let fileURL = checkpointDir.appendingPathComponent("\(groupIdHex).json")
    guard FileManager.default.fileExists(atPath: fileURL.path) else {
      return nil
    }
    
    do {
      let data = try Data(contentsOf: fileURL)
      let record = try JSONDecoder().decode(EpochRecord.self, from: data)
      epochCache[groupIdHex] = record
      return record.epoch
    } catch {
      return nil
    }
  }
  
  /// Clear checkpoint for a group (call on group leave/delete)
  public func clearCheckpoint(groupId: Data) {
    let groupIdHex = groupId.map { String(format: "%02hhx", $0) }.joined()
    epochCache.removeValue(forKey: groupIdHex)
    
    let fileURL = checkpointDir.appendingPathComponent("\(groupIdHex).json")
    try? FileManager.default.removeItem(at: fileURL)
    logger.debug("🗑️ [EPOCH] Cleared checkpoint for group \(groupIdHex.prefix(16))...")
  }
  
  /// Clear all checkpoints for a user (call on logout/account switch)
  public func clearAllCheckpoints() {
    epochCache.removeAll()
    
    do {
      let files = try FileManager.default.contentsOfDirectory(at: checkpointDir, includingPropertiesForKeys: nil)
      for file in files where file.pathExtension == "json" {
        try? FileManager.default.removeItem(at: file)
      }
      logger.info("🗑️ [EPOCH] Cleared all epoch checkpoints")
    } catch {
      logger.error("❌ [MLSEpochCheckpoint] Failed to clear checkpoints: \(error.localizedDescription)")
    }
  }
  
  /// Reload cache from disk (call after app foregrounding)
  public func reloadCacheFromDisk() async {
    epochCache.removeAll()
    
    do {
      let files = try FileManager.default.contentsOfDirectory(at: checkpointDir, includingPropertiesForKeys: nil)
      var reloadedCount = 0
      
      for file in files where file.pathExtension == "json" {
        do {
          let data = try Data(contentsOf: file)
          let record = try JSONDecoder().decode(EpochRecord.self, from: data)
          epochCache[record.groupId] = record
          reloadedCount += 1
        } catch {
          // Skip corrupted files
          logger.warning("⚠️ [EPOCH] Skipping corrupted checkpoint: \(file.lastPathComponent)")
        }
      }
      
      logger.info("🔄 [EPOCH] Reloaded \(reloadedCount) epoch checkpoint(s) from disk")
    } catch {
      logger.error("❌ [MLSEpochCheckpoint] Failed to reload cache: \(error.localizedDescription)")
    }
  }
}
