
import CatbirdMLS
import Foundation
import OSLog
import Petrel

/// Manages MLS key packages: creation, persistence, and server synchronization
public actor MLSKeyPackageManager {
  private let logger = Logger(subsystem: "blue.catbird", category: "MLSKeyPackageManager")

  // MARK: - Dependencies

  private let splitClient: MLSClient
  private let apiClient: MLSAPIClient
  private var monitor: MLSKeyPackageMonitor?
  private var isActiveUserCheck: (@Sendable (String) async -> Bool)?

  // MARK: - State

  /// Last time key packages were refreshed
  private var lastKeyPackageRefresh: Date?

  /// Hashes that have been reported as exhausted/unavailable by the MLS service (keyed by DID)
  private var exhaustedKeyPackageHashes: [String: Set<String>] = [:]

  /// Cooldown timestamp for key package replenishment (triggered by 429)
  private var replenishmentCooldownUntil: Date?

  // MARK: - Configuration

  private let defaultCipherSuite: String = "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519"
  /// Spec §10: KEY_PACKAGE_CHECK_INTERVAL_SEC = 300
  private let keyPackageRefreshInterval: TimeInterval = 300  // 5 minutes

  // MARK: - Initialization

  public init(
    client: MLSClient,
    apiClient: MLSAPIClient,
    monitor: MLSKeyPackageMonitor?
  ) {
    self.splitClient = client
    self.apiClient = apiClient
    self.monitor = monitor
  }

  /// Update the monitor (e.g. after initialization)
  public func setMonitor(_ monitor: MLSKeyPackageMonitor) {
    self.monitor = monitor
  }

  /// Set the active-user check closure (called during init wiring)
  public func setActiveUserCheck(_ check: @escaping @Sendable (String) async -> Bool) {
    self.isActiveUserCheck = check
  }

  // MARK: - Public State Accessors

  /// Mark a key package hash as exhausted for a specific DID
  public func markKeyPackageExhausted(hash: String, for did: String) {
    var exhausted = exhaustedKeyPackageHashes[did, default: []]
    exhausted.insert(hash)
    exhaustedKeyPackageHashes[did] = exhausted
    logger.debug("MARKED EXHAUSTED: \(hash.prefix(8)) for \(did)")
  }

  /// Check if a key package hash is marked as exhausted
  public func isKeyPackageExhausted(hash: String, for did: String) -> Bool {
    return exhaustedKeyPackageHashes[did]?.contains(hash) ?? false
  }

  /// Clear exhausted key packages for a DID (e.g. after refresh)
  public func clearExhaustedKeyPackages(for did: String) {
    if exhaustedKeyPackageHashes[did] != nil {
      exhaustedKeyPackageHashes.removeValue(forKey: did)
      logger.debug("Cleared exhausted key packages for \(did)")
    }
  }

  /// Clear ALL exhausted key packages (e.g. on full reset)
  public func clearAllExhaustedKeyPackages() {
    exhaustedKeyPackageHashes.removeAll()
    logger.debug("Cleared ALL exhausted key packages")
  }

  /// Get count of exhausted packages for a DID
  public func getExhaustedCount(for did: String) -> Int {
    return exhaustedKeyPackageHashes[did]?.count ?? 0
  }

  /// Reset the last refresh timestamp (e.g. on logout/failure)
  public func resetLastRefresh() {
    lastKeyPackageRefresh = nil
  }
  
  /// Explicitly set the last refresh timestamp
  public func setLastRefresh(_ date: Date) {
    lastKeyPackageRefresh = date
  }

  /// Unreserve key packages when a transient server error occurs (e.g., 502 Bad Gateway)
  public func unreserveKeyPackages(_ packages: [KeyPackageWithHash]) {
    for package in packages {
      let didKey = package.did.description
      if var exhausted = exhaustedKeyPackageHashes[didKey] {
        exhausted.remove(package.hash)
        if exhausted.isEmpty {
          exhaustedKeyPackageHashes.removeValue(forKey: didKey)
        } else {
          exhaustedKeyPackageHashes[didKey] = exhausted
        }
        logger.debug("♻️ Unreserved key package hash for \(didKey): \(package.hash.prefix(16))...")
      }
    }
  }

  // MARK: - DID Validation

  /// Validates that a key package's credential identity matches the expected user DID.
  /// Extracts the identity from the TLS-serialized key package and compares the DID portion.
  /// - Parameters:
  ///   - keyPackageData: Raw TLS-serialized key package bytes
  ///   - expectedUserDid: The DID of the currently authenticated user
  /// - Returns: `true` if the credential identity's DID matches, `false` otherwise
  private func validateKeyPackageDID(_ keyPackageData: Data, expectedUserDid: String) -> Bool {
    do {
      let credentialIdentity = try mlsExtractKeyPackageIdentity(keyPackageBytes: keyPackageData)
      // Extract bare DID from credential identity (handles both "did:plc:x" and "did:plc:x#device")
      let embeddedDID: String
      if let hashIndex = credentialIdentity.firstIndex(of: "#") {
        embeddedDID = String(credentialIdentity[..<hashIndex])
      } else {
        embeddedDID = credentialIdentity
      }
      let normalizedExpected = expectedUserDid.trimmingCharacters(in: .whitespacesAndNewlines)
      if embeddedDID != normalizedExpected {
        logger.warning(
          "🚨 [DID CONTAMINATION] Key package credential DID mismatch: embedded=\(embeddedDID), expected=\(normalizedExpected)"
        )
        return false
      }
      return true
    } catch {
      logger.error("❌ Failed to extract key package identity: \(error.localizedDescription)")
      return false
    }
  }

  // MARK: - Public API

  /// Publish a new key package for the current user
  /// - Parameter userDid: The DID of the user
  /// - Parameter expiresAt: Optional expiration date (defaults to 30 days)
  /// - Returns: Published key package reference
  @discardableResult
  public func publishKeyPackage(for userDid: String, expiresAt: Date? = nil) async throws -> BlueCatbirdMlsChatDefs.KeyPackageRef {
    logger.info("Publishing key package for \(userDid)")

    // Create key package locally (uses mlsDid automatically)
    // CRITICAL FIX: MLSClient.createKeyPackage() returns raw TLS-serialized KeyPackage bytes
    let keyPackageData = try await splitClient.createKeyPackage(for: userDid)

    logger.debug(
      "📦 Key package created: \(keyPackageData.count) bytes (first 16: \(keyPackageData.prefix(16).map { String(format: "%02x", $0) }.joined(separator: " ")))"
    )

    // Validate credential identity matches the current user's DID before uploading
    guard validateKeyPackageDID(keyPackageData, expectedUserDid: userDid) else {
      logger.error(
        "🚨 [DID CONTAMINATION] Refusing to upload key package with wrong credential DID for user \(userDid.prefix(20))"
      )
      throw MLSConversationError.noAuthentication
    }

    // Note: MLSClient.createKeyPackage persists automatically via SqliteStorageProvider

    // Publish to server (returns empty response)
    do {
      // Server requires an explicit future expiration; default to 30 days if not provided
      let expiry = expiresAt ?? Date(timeIntervalSinceNow: 30 * 24 * 60 * 60)

      // Send raw TLS bytes directly to server
      try await apiClient.publishKeyPackage(
        keyPackage: keyPackageData,
        cipherSuite: defaultCipherSuite,
        expiresAt: ATProtocolDate(date: expiry)
      )

      // Create a local reference
      let didObj = try DID(didString: userDid)
      let keyPackageRef = BlueCatbirdMlsChatDefs.KeyPackageRef(
        did: didObj,
        keyPackage: keyPackageData.base64EncodedString(),
        keyPackageHash: nil,  // Server will compute and return this in getKeyPackages
        cipherSuite: defaultCipherSuite
      )

      logger.info("Successfully published key package for: \(userDid) (state already persisted)")
      return keyPackageRef

    } catch {
      logger.error("Failed to publish key package: \(error.localizedDescription)")
      throw MLSConversationError.serverError(error)
    }
  }

  /// Smart key package refresh using monitor (preferred method)
  public func smartRefreshKeyPackages(
    for userDid: String,
    isShuttingDown: Bool,
    maxGeneratedPackages: Int? = nil
  ) async throws {
    // Skip refresh for inactive users to prevent spam loops during account switches
    if let check = isActiveUserCheck, await !check(userDid) {
      logger.debug("⏭️ Skipping key package refresh - user \(userDid.prefix(20)) is not the active user")
      return
    }

    logger.debug("🔍 Checking if key package refresh is needed (smart monitoring)")
    
    if isShuttingDown {
      logger.info("⏸️ Skipping key package refresh - storage reset in progress")
      return
    }

    // Check cooldown
    if let cooldown = replenishmentCooldownUntil {
      if Date() < cooldown {
        let remaining = Int(cooldown.timeIntervalSinceNow)
        logger.warning("⏸️ Replenishment in cooldown for another \(remaining)s - skipping")
        return
      } else {
        // Cooldown expired
        replenishmentCooldownUntil = nil
      }
    }

    // Prefer device-scoped inventory over user-scoped totals.
    // In multi-device setups, user totals can look healthy while this device has 0 packages.
    do {
      let perDeviceStatus = try await splitClient.syncKeyPackageHashes(for: userDid)
      let minimumPerDeviceAvailable = 20

      if perDeviceStatus.remainingAvailable < minimumPerDeviceAvailable {
        let targetInventory = max(minimumPerDeviceAvailable + 10, 25)
        logger.warning(
          "⚠️ Device inventory low (\(perDeviceStatus.remainingAvailable)); replenishing toward \(targetInventory)"
        )
        try await uploadKeyPackageBatchSmart(
          for: userDid,
          count: targetInventory,
          maxGeneratedPackages: maxGeneratedPackages
        )
      } else {
        logger.info(
          "✅ Device key package inventory sufficient: \(perDeviceStatus.remainingAvailable) available"
        )
      }

      lastKeyPackageRefresh = Date()
      return
    } catch {
      logger.warning(
        "⚠️ Device-specific key package check failed (\(error.localizedDescription)); falling back to aggregate stats"
      )
    }

    // Check if this is first-time registration
    let isFirstTime: Bool
    var freshStats: BlueCatbirdMlsChatPublishKeyPackages.Output?

    do {
      let stats = try await apiClient.getKeyPackageStats()
      freshStats = stats
      isFirstTime = stats.stats.available == 0
      if isFirstTime {
        logger.info(
          "🆕 First-time registration detected (0 packages on server) - bypassing rate limit")
      }
    } catch {
      isFirstTime = false
      logger.warning(
        "⚠️ Failed to fetch key package stats during initial check: \(error.localizedDescription)")
    }

    // 🛡️ FIX: Minimum interval check
    if !isFirstTime {
      let minimumInterval: TimeInterval = 300  // 5 minutes
      if let lastRefresh = lastKeyPackageRefresh {
        let timeSinceLastRefresh = Date().timeIntervalSince(lastRefresh)
        if timeSinceLastRefresh < minimumInterval {
          logger.info(
            "⏱️ Too soon since last refresh (\(Int(timeSinceLastRefresh))s ago), skipping (minimum: \(Int(minimumInterval))s)"
          )
          return
        }
      }
    }

    guard let monitor = monitor else {
      logger.warning("⚠️ Monitor not initialized, using basic refresh")
      return try await refreshKeyPackagesBasic(for: userDid, maxGeneratedPackages: maxGeneratedPackages)
    }

    let cache = MLSKeyPackageCache.shared

    if freshStats == nil {
      var forceServerRefresh = false
      if let cachedCount = await cache.getCachedCount() {
        logger.debug("Using cached count: \(cachedCount)")
        let threshold = 20
        if cachedCount >= threshold {
          logger.info("✅ Cached inventory sufficient: \(cachedCount) >= \(threshold)")
          return
        }
        forceServerRefresh = true
      }

      if await !cache.shouldRefreshFromServer() && !forceServerRefresh {
        logger.debug("Skipping server refresh, cache is fresh")
        return
      }

      do {
        freshStats = try await apiClient.getKeyPackageStats()
      } catch {
        logger.error("❌ Failed to check key package stats: \(error.localizedDescription)")
        logger.info("ℹ️ Skipping key package upload - server unavailable or error occurred")
        throw error
      }
    }

    guard let stats = freshStats else { return }

    do {
      await cache.updateFromServer(count: stats.stats.available)

      let enhancedStats = EnhancedKeyPackageStats(
        available: stats.stats.available,
        threshold: 10,
        total: stats.stats.available,
        consumed: 0,
        consumedLast24h: nil,
        consumedLast7d: nil,
        averageDailyConsumption: nil,
        predictedDepletionDays: nil,
        needsReplenish: stats.stats.available < 10
      )

      logger.info(
        "📊 Key package inventory: available=\(enhancedStats.available), threshold=\(enhancedStats.threshold), dynamic=\(enhancedStats.dynamicThreshold)"
      )

      let recommendation = try await monitor.getReplenishmentRecommendation(stats: enhancedStats)

      if recommendation.shouldReplenish {
        logger.warning(
          "⚠️ Replenishment needed [\(recommendation.priority.rawValue)]: \(recommendation.reason)")
        try await uploadKeyPackageBatchSmart(
          for: userDid,
          count: recommendation.recommendedBatchSize,
          maxGeneratedPackages: maxGeneratedPackages
        )
        lastKeyPackageRefresh = Date()
      } else {
        logger.debug("✅ Key packages are sufficient: \(stats.stats.available) available")
        lastKeyPackageRefresh = Date()
      }
    } catch {
      logger.error("❌ Error during replenishment logic: \(error.localizedDescription)")
      throw error
    }
  }

  /// Basic refresh without smart monitoring
  public func refreshKeyPackagesBasic(
    for userDid: String,
    maxGeneratedPackages: Int? = nil
  ) async throws {
    logger.debug("Checking if key package refresh is needed (basic mode)")

    do {
      let stats = try await apiClient.getKeyPackageStats()
      let available = stats.stats.available
      let threshold = 10
      logger.info(
        "📊 Key package inventory: available=\(available), threshold=\(threshold)")

      if available < threshold {
        logger.warning(
          "⚠️ Key package count (\(available)) below threshold (\(threshold)) - replenishing..."
        )
        let neededCount = max(100 - available, 0)
        try await uploadKeyPackageBatchSmart(
          for: userDid,
          count: neededCount,
          maxGeneratedPackages: maxGeneratedPackages
        )
        lastKeyPackageRefresh = Date()
      } else {
        logger.debug("✅ Key packages are sufficient: \(available) available")
      }
    } catch {
      logger.error("❌ Failed to check key package stats: \(error.localizedDescription)")
      logger.info("ℹ️ Skipping key package upload - server unavailable or error occurred")
      throw error
    }
  }

  /// Refresh key packages based on time interval
  public func refreshKeyPackagesBasedOnInterval(
    for userDid: String,
    isShuttingDown: Bool,
    maxGeneratedPackages: Int? = nil
  ) async throws {
    logger.debug("Checking if key package refresh is needed based on interval")

    if let lastRefresh = lastKeyPackageRefresh {
      let timeSinceLastRefresh = Date().timeIntervalSince(lastRefresh)
      if timeSinceLastRefresh < keyPackageRefreshInterval {
        logger.debug(
          "Key packages were refreshed \(Int(timeSinceLastRefresh))s ago, skipping"
        )
        return
      }
    }

    logger.info("Refreshing key packages based on interval")
    try await smartRefreshKeyPackages(
      for: userDid,
      isShuttingDown: isShuttingDown,
      maxGeneratedPackages: maxGeneratedPackages
    )
    lastKeyPackageRefresh = Date()
  }

  /// Smart batch upload using batch API
  public func uploadKeyPackageBatchSmart(
    for userDid: String,
    count: Int = 25,  // Capped to stay under server body limit (~3.7 KB per package)
    maxGeneratedPackages: Int? = nil
  ) async throws {
    logger.info("🔄 Starting smart key package replenishment (requested count: \(count))...")

    // STEP 0: Ensure device is registered
    let normalizedUserDid = userDid.trimmingCharacters(in: .whitespacesAndNewlines)
    let mlsDid = try await splitClient.ensureDeviceRegistered(userDid: normalizedUserDid)
    logger.info("📱 Device registered (server metadata DID: \(mlsDid))")

    let deviceInfo = await splitClient.getDeviceInfo(for: normalizedUserDid)

    // 🔥 CRITICAL FIX: Get client identity (did#deviceUUID) for key package creation
    // The signer is registered with clientIdentity during device registration, so we MUST
    // use the same identity format here. Using bare DID causes signature key mismatch
    // when the recipient tries to verify messages signed by this device.
    guard let clientIdentity = await splitClient.getClientIdentity(for: normalizedUserDid) else {
      logger.error("❌ Device not registered - cannot determine client identity for key packages")
      throw MLSConversationError.noAuthentication
    }
    logger.debug("📦 Using client identity for key packages: \(clientIdentity)")

    // STEP 1: Check LOCAL key package count
    let localBundleCount = try await splitClient.ensureLocalBundlesAvailable(for: normalizedUserDid)
    let minimumLocalBundles: UInt64 = 10
    let localBundlesNeeded = localBundleCount < minimumLocalBundles
      ? Int(minimumLocalBundles - localBundleCount)
      : 0

    if localBundlesNeeded > 0 {
      logger.info("📦 Local storage needs \(localBundlesNeeded) bundles (have: \(localBundleCount))")
    }

    // STEP 2: Query DEVICE-SPECIFIC server inventory via syncKeyPackages
    // This is critical for multi-device support - user-level stats include packages from other devices
    var deviceServerAvailable: Int
    let serverThreshold = 20 // Minimum packages per device
    
    do {
      // syncKeyPackageHashes returns per-device stats
      let syncResult = try await splitClient.syncKeyPackageHashes(for: normalizedUserDid)
      deviceServerAvailable = syncResult.remainingAvailable
      logger.info("📊 [Device-specific] Server has \(deviceServerAvailable) key packages for THIS device")
    } catch {
      if applyBackoffIfNeeded(for: error, context: "device inventory sync") {
        logger.warning("⏸️ Deferring key package replenishment until cooldown expires")
        throw error
      }
      // If sync fails, fall back to user-level stats but log warning
      logger.warning("⚠️ Failed to get device-specific stats, falling back to user-level: \(error.localizedDescription)")
      let (userAvailable, _) = try await apiClient.queryKeyPackageInventory()
      // Be conservative - assume this device has none if we can't check
      deviceServerAvailable = 0
      logger.warning("⚠️ Assuming 0 key packages for this device (user total: \(userAvailable))")
    }
    
    // STEP 3: Calculate server upload need (per-device)
    // Use the requested count as the target inventory level, but at least the threshold+buffer
    let targetInventory = max(serverThreshold + 10, count)
    let serverUploadNeeded = max(0, targetInventory - deviceServerAvailable)

    // STEP 4: Determine total packages to generate
    let totalToGenerate = max(localBundlesNeeded, serverUploadNeeded)

    // Detect recovery mode: both local and server have 0 bundles for this device
    let useRecoveryMode = localBundleCount == 0 && deviceServerAvailable == 0
    if useRecoveryMode {
      logger.warning("🔑 Recovery mode detected: local=0, device server=0 - will bypass rate limits")
    }

    if totalToGenerate == 0 {
      logger.info("✅ Both local and device-server inventories are sufficient")
      return
    }

    // STEP 5: Cap at API batch limit
    let requestedGenerateCount = min(totalToGenerate, 25)  // Server body limit cap
    let generateCount: Int
    if let maxGeneratedPackages {
      generateCount = min(requestedGenerateCount, maxGeneratedPackages)
      if generateCount < requestedGenerateCount {
        logger.info(
          "📦 Applying generation cap: \(generateCount) of \(requestedGenerateCount) packages")
      }
    } else {
      generateCount = requestedGenerateCount
    }
    if generateCount == 0 {
      logger.info("⏸️ Generation capped at 0 - skipping key package batch")
      return
    }
    let willUploadToServer = serverUploadNeeded > 0

    logger.info("📦 Generating \(generateCount) key packages")

    // STEP 6: Generate key packages
    let expiry = Date(timeIntervalSinceNow: 30 * 24 * 60 * 60) // 30 days
    var packages: [MLSKeyPackageUploadData] = []
    
    for _ in 0..<generateCount {
      try Task.checkCancellation()
      // 🔥 CRITICAL FIX: Use clientIdentity (did#deviceUUID), NOT bare DID
      // This ensures the signer registered during device registration is reused,
      // preventing signature key mismatch when messages are verified by recipients
      let keyPackageBytes = try await splitClient.createKeyPackage(
        for: normalizedUserDid,
        identity: clientIdentity  // Fixed: was normalizedUserDid (bare DID)
      )
      let keyPackageBase64 = keyPackageBytes.base64EncodedString()

      let packageData = MLSKeyPackageUploadData(
        keyPackage: keyPackageBase64,
        cipherSuite: defaultCipherSuite,
        expires: expiry,
        idempotencyKey: UUID().uuidString.lowercased(),
        deviceId: deviceInfo?.deviceId,
        credentialDid: clientIdentity  // Fixed: was normalizedUserDid (bare DID)
      )

      packages.append(packageData)
    }

    logger.info("✅ Generated \(generateCount) key packages (Persisted via FFI)")

    // STEP 7: Validate credential identity matches current user DID
    // This prevents DID contamination when switching accounts on the same device
    let validatedPackages = packages.filter { pkg in
      guard let keyPackageData = Data(base64Encoded: pkg.keyPackage) else {
        logger.warning("⚠️ Skipping key package with invalid base64 encoding")
        return false
      }
      return validateKeyPackageDID(keyPackageData, expectedUserDid: normalizedUserDid)
    }
    let discardedCount = packages.count - validatedPackages.count
    if discardedCount > 0 {
      logger.error(
        "🚨 [DID CONTAMINATION] Discarded \(discardedCount) key packages with wrong credential DID"
      )
    }
    if validatedPackages.isEmpty && !packages.isEmpty {
      logger.error("🚨 [DID CONTAMINATION] ALL key packages had wrong credential DID — aborting upload")
      return
    }

    // STEP 8: Upload to server if needed
    try Task.checkCancellation()
    if willUploadToServer {
      logger.info("📤 Uploading \(validatedPackages.count) packages to server\(useRecoveryMode ? " (recovery mode)" : "")")
      
      do {
        let result = try await apiClient.publishKeyPackagesBatch(validatedPackages, recoveryMode: useRecoveryMode, deviceId: deviceInfo?.deviceId)
        logger.info("✅ Batch upload complete: \(result.succeeded) succeeded, \(result.failed) failed")

        if result.failed > 0 {
          logger.warning("⚠️ \(result.failed) key packages failed to upload")
          // Check errors for rate limits
          if let errors = result.errors {
             for error in errors {
                 // If generic network error suggests rate limit (though specific 429 should be caught below)
                 if error.error.contains("429") {
                     let cooldown: TimeInterval = 300 // 5 minutes default
                     self.replenishmentCooldownUntil = Date().addingTimeInterval(cooldown)
                     logger.warning("⚠️ Rate limit detected in batch results. Pausing replenishment for \(Int(cooldown))s")
                     break
                 }
             }
          }
        }

        if result.succeeded > 0 {
          await MLSKeyPackageCache.shared.updateAfterUpload(uploaded: result.succeeded)
          
          // CRITICAL FIX: Clear exhausted key package hashes
          exhaustedKeyPackageHashes.removeValue(forKey: normalizedUserDid)
          logger.info("🔄 Cleared exhausted key package cache for self")
        }
        
        // Monitor tracking
        if let monitor = monitor, result.succeeded > 0 {
            // Monitor logic if tracking uploads
        }
      } catch {
        _ = applyBackoffIfNeeded(for: error, context: "batch upload")
        throw error
      }
    } else {
      logger.info("⏭️ Skipping server upload - server inventory is sufficient")
    }
  }

  /// Apply cooldown for rate-limited or transient gateway failures.
  @discardableResult
  private func applyBackoffIfNeeded(for error: Error, context: String) -> Bool {
    if let apiError = error as? MLSAPIError {
      switch apiError {
      case .rateLimited(let retryAfter):
        handleRateLimit(retryAfter: retryAfter)
        return true
      case .httpError(let statusCode, _) where statusCode == 429:
        handleRateLimit(retryAfter: nil)
        return true
      case .httpError(let statusCode, _) where [502, 503, 504].contains(statusCode):
        handleTransientServerFailure(statusCode: statusCode, context: context)
        return true
      case .serverUnavailable:
        handleTransientServerFailure(statusCode: 503, context: context)
        return true
      default:
        break
      }
    }

    if error.localizedDescription.contains("429") {
      handleRateLimit(retryAfter: nil)
      return true
    }
    if error.localizedDescription.contains("502")
      || error.localizedDescription.contains("503")
      || error.localizedDescription.contains("504")
    {
      handleTransientServerFailure(statusCode: 502, context: context)
      return true
    }
    return false
  }

  private func handleTransientServerFailure(statusCode: Int, context: String) {
    let cooldown: TimeInterval = 30
    self.replenishmentCooldownUntil = Date().addingTimeInterval(cooldown)
    logger.warning(
      "⚠️ Transient server error (\(statusCode)) during \(context). Pausing replenishment for \(Int(cooldown))s"
    )
  }

  /// Handle cooldown for rate limited requests
  private func handleRateLimit(retryAfter: TimeInterval?) {
    let cooldownCallback = retryAfter ?? 300 // Default 5 minutes
    self.replenishmentCooldownUntil = Date().addingTimeInterval(cooldownCallback)
    logger.warning("⚠️ Rate limited. Pausing replenishment for \(Int(cooldownCallback))s")
  }
}
