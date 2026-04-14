import CryptoKit
import Foundation
import OSLog
import Petrel

#if os(iOS)
  import UIKit
#elseif os(macOS)
  import AppKit
#endif

/// Manages device registration and identity for MLS multi-device support
@available(iOS 18.0, macOS 13.0, *)
public actor MLSDeviceManager {

  // MARK: - Properties

  private static let deviceIdKey = "blue.catbird.mls.deviceId"
  private static let mlsDidKey = "blue.catbird.mls.mlsDid"
  private static let signatureKeyKey = "blue.catbird.mls.signatureKey"
  private static let deviceUUIDKey = "blue.catbird.mls.deviceUUID"
  private static let cipherSuiteVersionKey = "blue.catbird.mls.cipherSuiteVersion"
  /// Bump this when changing the default cipher suite to force re-registration
  private static let currentCipherSuiteVersion = 2  // v1 = classical, v2 = X-Wing PQ
  private static let keychainLogger = Logger(
    subsystem: "blue.catbird", category: "MLSDeviceManager.Keychain")

  private let apiClient: ATProtoClient
  private let mlsAPIClient: MLSAPIClient
  private let mlsClient: MLSClient
  private let logger = Logger(subsystem: "blue.catbird", category: "MLSDeviceManager")

  // MARK: - Per-User Device Info Storage

  /// Stores device info per user DID
  /// Key: normalized userDID, Value: device info for that user
  private var deviceInfoByUser: [String: UserDeviceInfo] = [:]

  /// Per-user device information
  public struct UserDeviceInfo: Codable {
    public let deviceId: String
    public let mlsDid: String
    public let deviceUUID: String?
  }

  private var signaturePrivateKey: Curve25519.Signing.PrivateKey?

  /// Tracks users currently undergoing device registration to prevent race conditions
  /// Actor isolation ensures thread-safe access to this set
  private var registrationInProgress: Set<String> = []

  /// Continuations waiting for registration to complete (keyed by normalized user DID)
  private var registrationWaiters: [String: [CheckedContinuation<Void, Error>]] = [:]

  // MARK: - Initialization

  public init(apiClient: ATProtoClient, mlsAPIClient: MLSAPIClient, mlsClient: MLSClient) {
    self.apiClient = apiClient
    self.mlsAPIClient = mlsAPIClient
    self.mlsClient = mlsClient

    // Load per-user device info from UserDefaults
    if let data = UserDefaults.standard.data(forKey: "blue.catbird.mls.deviceInfoByUser"),
      let decoded = try? JSONDecoder().decode([String: UserDeviceInfo].self, from: data)
    {
      self.deviceInfoByUser = decoded
      logger.debug("Loaded device info for \(decoded.count) users")
    }

    // Load signature key from keychain (still global, shared across users on same device)
    if let keyData = try? Self.loadSignatureKey() {
      self.signaturePrivateKey = try? Curve25519.Signing.PrivateKey(rawRepresentation: keyData)
    }
  }

  /// Save per-user device info to UserDefaults
  private func saveDeviceInfoStorage() {
    if let data = try? JSONEncoder().encode(deviceInfoByUser) {
      UserDefaults.standard.set(data, forKey: "blue.catbird.mls.deviceInfoByUser")
    }
  }

  /// Notify all waiters that registration completed (or failed)
  private func notifyRegistrationWaiters(for userDid: String, error: Error? = nil) {
    guard let waiters = registrationWaiters.removeValue(forKey: userDid) else { return }

    for continuation in waiters {
      if let error = error {
        continuation.resume(throwing: error)
      } else {
        continuation.resume()
      }
    }
  }

  // MARK: - Device Registration

  /// Registers the device with the MLS service if not already registered
  /// - Parameter userDid: The user's DID for MLS context initialization
  /// - Returns: The MLS DID for this device
  public func ensureDeviceRegistered(userDid: String) async throws -> String {
    // Normalize userDID for consistent storage lookup
    let normalizedUserDid = userDid.trimmingCharacters(in: .whitespacesAndNewlines)

    // CRITICAL FIX: Prevent concurrent registration race condition
    // If another call is already registering this user, wait for completion using async/await
    if registrationInProgress.contains(normalizedUserDid) {
      logger.info(
        "⏸️ [RACE CONDITION PREVENTION] Registration already in progress for \(normalizedUserDid.prefix(20)) - waiting..."
      )
      logger.debug("   This prevents multiple devices from being created for the same user")
      logger.debug("   Current registration locks: \(self.registrationInProgress.count)")

      // Use withCheckedThrowingContinuation for efficient async waiting
      try await withCheckedThrowingContinuation {
        (continuation: CheckedContinuation<Void, Error>) in
        // Add ourselves to the waiters list
        if registrationWaiters[normalizedUserDid] == nil {
          registrationWaiters[normalizedUserDid] = []
        }
        registrationWaiters[normalizedUserDid]?.append(continuation)
      }

      logger.info("✅ Concurrent registration completed - re-checking device info")
      // Fall through to re-check if registration succeeded
    }

    // Check if cipher suite has been upgraded — force re-registration if so
    let storedVersion = UserDefaults.standard.integer(forKey: Self.cipherSuiteVersionKey)
    if storedVersion < Self.currentCipherSuiteVersion && deviceInfoByUser[normalizedUserDid] != nil {
      logger.info(
        "🔄 [CIPHER SUITE UPGRADE] v\(storedVersion) → v\(Self.currentCipherSuiteVersion) — forcing re-registration with new cipher suite"
      )
      deviceInfoByUser.removeValue(forKey: normalizedUserDid)
      saveDeviceInfoStorage()
      await MLSKeyPackageCache.shared.invalidate()
      // Fall through to full registration below
    }

    // Check if this user already has device registration
    if let existingInfo = deviceInfoByUser[normalizedUserDid] {
      logger.info(
        "✅ [DEVICE ALREADY REGISTERED] Found local device info for user \(normalizedUserDid.prefix(20))"
      )
      logger.debug("   Device ID: \(existingInfo.deviceId)")
      logger.debug("   MLS DID: \(existingInfo.mlsDid)")
      logger.debug("   UUID: \(existingInfo.deviceUUID ?? "N/A")")

      // ✅ CRITICAL FIX: Verify key packages for THIS device (not just user-level totals).
      // User-level stats can be non-zero because another device uploaded packages.
      // Multi-device joins require this specific device to maintain inventory.
      do {
        let syncResult = try await mlsClient.syncKeyPackageHashes(for: normalizedUserDid)
        if syncResult.remainingAvailable > 0 {
          logger.info(
            "✅ [KEY PACKAGE VERIFICATION] Device inventory healthy (\(syncResult.remainingAvailable) available)"
          )
          logger.debug("   Skipping device registration - using existing device")
          return existingInfo.mlsDid
        }

        logger.warning(
          "⚠️ [DEVICE CHECK] This device has 0 server key packages after sync - attempting replenish"
        )
        do {
          let replenish = try await mlsClient.monitorAndReplenishBundles(for: normalizedUserDid)
          if replenish.available > 0 || replenish.uploaded > 0 {
            logger.info(
              "✅ [DEVICE CHECK] Replenished key packages (available: \(replenish.available), uploaded: \(replenish.uploaded))"
            )
            return existingInfo.mlsDid
          }
        } catch {
          logger.warning(
            "⚠️ [DEVICE CHECK] Replenishment failed for existing device: \(error.localizedDescription)"
          )
        }

        // Guard: skip destructive nuke-and-pave for inactive users to prevent
        // spam loops during account switching.
        let isActive = await MLSGRDBManager.shared.isActiveUser(normalizedUserDid)
        if !isActive {
          logger.warning(
            "⚠️ [INACTIVE USER] Skipping nuke-and-pave for inactive user \(normalizedUserDid.prefix(20)) — returning existing device"
          )
          return existingInfo.mlsDid
        }

        logger.error(
          "⚠️ INCONSISTENT STATE: Device registered locally but still has ZERO device key packages"
        )
        logger.error(
          "   Clearing local registration to trigger clean re-registration"
        )

        // Clear local device info to trigger full re-registration
        deviceInfoByUser.removeValue(forKey: normalizedUserDid)
        saveDeviceInfoStorage()

        logger.info("🔄 Cleared local device info - proceeding with full re-registration")

        // Invalidate stale packages for this device before re-registering.
        logger.info("🔄 [FRESH INSTALL] Syncing with server to invalidate stale key packages...")
        do {
          let staleSync = try await mlsAPIClient.syncKeyPackages(
            localHashes: [],  // Empty = all server packages for this device are orphaned
            deviceId: existingInfo.deviceId
          )
          logger.info(
            "✅ [FRESH INSTALL] Invalidated \(staleSync.deletedCount) stale key packages"
          )
        } catch {
          logger.warning(
            "⚠️ [FRESH INSTALL] Failed to sync stale packages: \(error.localizedDescription)"
          )
        }

        // Fall through to full registration (both Phase 1 and Phase 2)
      } catch {
        logger.warning("⚠️ Failed to verify key packages on server: \(error.localizedDescription)")
        logger.warning("   Assuming registration is valid and continuing...")
        return existingInfo.mlsDid
      }

    }

    // Guard: don't start fresh registration for inactive users
    let isActiveForRegistration = await MLSGRDBManager.shared.isActiveUser(normalizedUserDid)
    if !isActiveForRegistration {
      logger.warning(
        "⚠️ [INACTIVE USER] Skipping fresh device registration for inactive user \(normalizedUserDid.prefix(20))"
      )
      throw MLSError.operationFailed
    }

    // Mark registration as in progress to block concurrent calls
    registrationInProgress.insert(normalizedUserDid)
    logger.info(
      "🔐 Starting device registration for user: \(normalizedUserDid.prefix(20)) (marked as in-progress)"
    )

    // Track whether we need to notify waiters on exit
    var registrationError: Error?

    // Ensure we clean up the lock and notify waiters on ALL exit paths
    defer {
      registrationInProgress.remove(normalizedUserDid)
      notifyRegistrationWaiters(for: normalizedUserDid, error: registrationError)
      logger.debug("🔓 Cleared registration lock for \(normalizedUserDid.prefix(20))")
    }

    // Get device UUID (persists across app reinstalls via OS identifier)
    let deviceUUID: String
    if let existingInfo = deviceInfoByUser[normalizedUserDid],
      let existingUUID = existingInfo.deviceUUID
    {
      // Reuse UUID from previous registration for this user
      deviceUUID = existingUUID
      logger.info("Using cached device UUID for user: \(deviceUUID)")
    } else {
      // Use identifierForVendor (iOS) or generate and store locally (macOS)
      #if os(iOS)
        if let idfv = UIDevice.current.identifierForVendor?.uuidString {
          deviceUUID = idfv
          logger.info("Using IDFV as device UUID: \(deviceUUID) (persists across reinstalls)")
        } else {
          // Fallback if IDFV unavailable (rare - usually only in simulator during development)
          deviceUUID =
            try Self.loadDeviceUUID()
            ?? {
              let newUUID = UUID().uuidString
              try? Self.saveDeviceUUID(newUUID)
              return newUUID
            }()
          logger.info("Generated fallback device UUID: \(deviceUUID) (stored locally)")
        }
      #elseif os(macOS)
        // macOS doesn't have IDFV, so generate and store locally
        deviceUUID =
          try Self.loadDeviceUUID()
          ?? {
            let newUUID = UUID().uuidString
            try? Self.saveDeviceUUID(newUUID)
            return newUUID
          }()
        logger.info("Using device UUID: \(deviceUUID) (stored locally)")
      #endif
    }

    // Get device name
    let deviceName = getDeviceName()

    // Generate Ed25519 signature keypair
    let signatureKey: Curve25519.Signing.PrivateKey
    if let existingKey = signaturePrivateKey {
      signatureKey = existingKey
      logger.info("Using existing signature keypair")
    } else {
      signatureKey = Curve25519.Signing.PrivateKey()
      self.signaturePrivateKey = signatureKey
      try Self.saveSignatureKey(signatureKey.rawRepresentation)
      logger.info("Generated new Ed25519 signature keypair")
    }

    // Get signature public key
    let signaturePublicKey = signatureKey.publicKey.rawRepresentation

    // ═══════════════════════════════════════════════════════════════════════════
    // MLS CREDENTIAL IDENTITY: Use bare DID format
    // ═══════════════════════════════════════════════════════════════════════════
    //
    // The MLS server enforces "bare DID only" policy for credential identities.
    // Key packages with DID#deviceUUID format are rejected by server validation.
    //
    // Multi-device tracking is handled via:
    // - device_id column in server's key_packages table (server-side tracking)
    // - deviceUUID parameter in registerDevice call (client-side tracking)
    //
    // The credential identity in key packages MUST be the bare user DID.
    //
    // ═══════════════════════════════════════════════════════════════════════════
    let mlsCredentialIdentity = normalizedUserDid  // Bare DID - server rejects DID#deviceUUID
    logger.info("🔐 Creating key packages with MLS credential identity...")
    logger.info("   User DID: \(normalizedUserDid)")
    logger.info("   Device UUID: \(deviceUUID) (for server-side tracking only)")
    logger.info("   MLS credential identity (bare DID): \(mlsCredentialIdentity)")

    let keyPackageCount = 25  // Capped to stay under server 2–4 MB body limit (~3.7 KB each)
    var keyPackageItems: [BlueCatbirdMlsChatRegisterDevice.KeyPackageItem] = []

    // Use new batch API for atomic creation under single lock
    let keyPackages = try await mlsClient.batchCreateKeyPackages(
      for: normalizedUserDid,  // Use normalized DID for context lookup
      identity: mlsCredentialIdentity,  // Full DID#deviceUUID for multi-device MLS
      count: keyPackageCount
    )

    // Convert to API format
    let expirationDate = Date().addingTimeInterval(90 * 24 * 60 * 60)

    for packageData in keyPackages {
      // Validate credential identity matches the current user's DID before including
      do {
        let credentialIdentity = try mlsExtractKeyPackageIdentity(keyPackageBytes: packageData)
        let embeddedDID = Self.extractUserDID(from: credentialIdentity)
        if embeddedDID != normalizedUserDid {
          logger.error(
            "🚨 [DID CONTAMINATION] Skipping key package: credential DID \(embeddedDID) != expected \(normalizedUserDid)"
          )
          continue
        }
      } catch {
        logger.error("❌ Failed to extract key package identity: \(error.localizedDescription)")
        continue
      }

      let keyPackageItem = BlueCatbirdMlsChatRegisterDevice.KeyPackageItem(
        keyPackage: Bytes(data: packageData),
        cipherSuite: "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
        expires: ATProtocolDate(date: expirationDate)
      )
      keyPackageItems.append(keyPackageItem)
    }

    if keyPackageItems.isEmpty {
      logger.error("🚨 [DID CONTAMINATION] ALL key packages had wrong credential DID — aborting registration")
      registrationError = MLSError.operationFailed
      throw MLSError.operationFailed
    }
    if keyPackageItems.count < keyPackages.count {
      logger.warning(
        "🚨 [DID CONTAMINATION] Discarded \(keyPackages.count - keyPackageItems.count) key packages with wrong credential DID"
      )
    }

    logger.info("✅ Created \(keyPackageItems.count) key packages in batch")
    logger.info("✅ MLS state auto-flushed to SQLite via batch transaction")

    // Register device with key packages included
    logger.info(
      "📡 Registering device with server (including \(keyPackageItems.count) key packages)...")

    let input = BlueCatbirdMlsChatRegisterDevice.Input(
      deviceName: deviceName,
      deviceUUID: deviceUUID,  // Persistent UUID for re-registration detection
      keyPackages: keyPackageItems,  // Include all created key packages
      signaturePublicKey: Bytes(data: signaturePublicKey)
    )

    let maxRetries = 3
    var lastError: Error?

    for attempt in 1...maxRetries {
      do {
        logger.info("📡 Attempting server registration (attempt \(attempt)/\(maxRetries))...")

        let (responseCode, output) = try await apiClient.blue.catbird.mlschat.registerDevice(
          input: input)

        guard responseCode == 200, let output = output else {
          let errorMsg = "HTTP \(responseCode)"
          logger.error("❌ Registration failed: \(errorMsg)")
          if let output = output {
            logger.error("   Response: \(String(describing: output))")
          }

          if attempt < maxRetries {
            let delay = Double(attempt * 2)  // Exponential backoff: 2s, 4s, 6s
            logger.info("⏳ Retrying in \(delay)s...")
            try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
            continue
          } else {
            throw MLSError.deviceRegistrationFailed
          }
        }

        // ✅ Registration Success! Device registered with key packages
        let userDeviceInfo = UserDeviceInfo(
          deviceId: output.deviceId,
          mlsDid: output.mlsDid,
          deviceUUID: deviceUUID
        )

        // Save device info now that registration succeeded
        deviceInfoByUser[normalizedUserDid] = userDeviceInfo
        saveDeviceInfoStorage()
        UserDefaults.standard.set(Self.currentCipherSuiteVersion, forKey: Self.cipherSuiteVersionKey)

        logger.info(
          "✅ Device registration complete - Device registered for user \(normalizedUserDid.prefix(20)):"
        )
        logger.info("   deviceId: \(output.deviceId)")
        logger.info("   mlsDid: \(output.mlsDid) (metadata only)")
        logger.info("   keyPackages: \(keyPackageItems.count) uploaded")
        logger.info("   autoJoinedConvos: \(output.autoJoinedConvos.count)")
        logger.info("   welcomeMessages: \(output.welcomeMessages?.count ?? 0)")

        // ✅ CRITICAL: Call optIn to record that user has opted into MLS chat
        // This is required for other users to see this user as "available" for group invites
        do {
          let (optedIn, optedInAt) = try await mlsAPIClient.optIn(deviceId: output.deviceId)
          logger.info("✅ Opt-in recorded: optedIn=\(optedIn), optedInAt=\(optedInAt)")
        } catch {
          logger.error("❌ Failed to record opt-in status: \(error.localizedDescription)")
          logger.error("   User may appear as 'unavailable' when others try to add them to groups")
          // Don't fail registration - device is registered, just opt-in status is missing
        }

        // Successfully registered - process welcome messages and return
        if let welcomeMessages = output.welcomeMessages, !welcomeMessages.isEmpty {
          logger.info(
            "Processing \(welcomeMessages.count) welcome messages for auto-joined conversations...")
          // Note: Welcome messages from device registration are auto-join invitations
          // from existing conversations. These should be processed to join the groups,
          // but MLSDeviceManager doesn't have access to MLSConversationManager.
          //
          // Options for implementation:
          // 1. Return welcome messages to caller (MLSConversationManager.ensureKeyPackagesAvailable)
          // 2. Emit a notification that MLSConversationManager observes
          // 3. Store pending welcomes in GRDB for later processing
          //
          // For now, MLSConversationManager.syncWithServer() handles fetching
          // conversations and processing welcomes via separate API calls.
        }

        return output.mlsDid  // Exit function on success
      } catch {
        lastError = error
        logger.error("❌ Registration attempt \(attempt) failed: \(error.localizedDescription)")

        // ✅ CRITICAL FIX: Rollback MLS state on registration failure
        // Clean up key package bundles created before registration attempt
        // Device info was NOT saved (only saved on success), so retry is clean
        do {
          logger.warning("🔄 Rolling back MLS state (clearing key package bundles)...")
          try await mlsClient.clearStorage(for: userDid)
          logger.info("✅ MLS state cleared - ready for clean retry")
        } catch {
          logger.error("⚠️ Failed to clear MLS state during rollback: \(error.localizedDescription)")
          // Continue anyway - retry might still succeed or final failure will clear state
        }

        if attempt < maxRetries {
          let delay = Double(attempt * 2)
          logger.info("⏳ Retrying in \(delay)s...")
          try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
        }
      }
    }

    // All retries exhausted - clear signature key for fresh start next time
    logger.error("❌ CRITICAL: Device registration failed after \(maxRetries) attempts")
    logger.error("   Key packages were created but registration with server failed")
    logger.error(
      "   This WILL cause NoMatchingKeyPackage errors when others try to invite this user")
    logger.error("   ACTION REQUIRED: Check server connectivity and re-register device")

    // Clear signature key to allow fresh registration attempt
    self.signaturePrivateKey = nil
    try? Self.deleteSignatureKey()
    logger.info("🔄 Cleared signature key for clean retry on next attempt")

    // Set the error so waiters are notified of failure
    registrationError = lastError ?? MLSError.deviceRegistrationFailed

    if let error = lastError {
      throw error
    } else {
      throw MLSError.deviceRegistrationFailed
    }
  }

  /// Delete device from server (removes all associated key packages)
  /// This is the CRITICAL first step for recovering from key package desync
  /// - Parameter userDid: The user's DID
  public func deleteDevice(for userDid: String) async throws -> Int {
    let normalizedUserDid = userDid.trimmingCharacters(in: .whitespacesAndNewlines)
    guard let deviceInfo = deviceInfoByUser[normalizedUserDid] else {
      logger.warning(
        "No deviceId to delete for user \(normalizedUserDid.prefix(20)) - device may not be registered"
      )
      return 0
    }

    logger.info(
      "🗑️ Deleting device \(deviceInfo.deviceId) for user \(normalizedUserDid.prefix(20))...")

    let input = BlueCatbirdMlsChatDeleteDevice.Input(
      deviceId: deviceInfo.deviceId
    )
    let (responseCode, output) = try await apiClient.blue.catbird.mlschat.deleteDevice(input: input)

    guard responseCode == 200, let output = output else {
      logger.error("❌ Failed to delete device: HTTP \(responseCode)")
      throw MLSError.operationFailed
    }

    logger.info("✅ Device deleted successfully")
    logger.info("   - Deleted: \(output.deleted), keyPackagesDeleted: \(output.keyPackagesDeleted)")

    return 0
  }

  /// Forces a re-registration of the device (useful for testing or recovery)
  /// ⚠️ WARNING: This clears server-side bundles FIRST to prevent desync!
  public func reregisterDevice(userDid: String) async throws -> String {
    let normalizedUserDid = userDid.trimmingCharacters(in: .whitespacesAndNewlines)
    logger.info("🔄 Starting full device re-registration for user \(normalizedUserDid.prefix(20))")

    // CRITICAL FIX: Delete device from server FIRST to clear corrupted key packages
    if deviceInfoByUser[normalizedUserDid] != nil {
      do {
        let deletedCount = try await deleteDevice(for: normalizedUserDid)
        logger.info("✅ Cleared \(deletedCount) key packages from server before re-registration")
      } catch {
        logger.error("⚠️ Failed to delete device from server: \(error.localizedDescription)")
        logger.error("   Continuing with re-registration anyway (may cause bundle count mismatch)")
      }
    }

    // Clear stored credentials for this user
    deviceInfoByUser.removeValue(forKey: normalizedUserDid)
    saveDeviceInfoStorage()

    // Invalidate key package cache
    await MLSKeyPackageCache.shared.invalidate()

    // Clear local MLS storage to force fresh start
    logger.info("🗑️ Clearing local MLS storage...")
    try await mlsClient.clearStorage(for: userDid)
    logger.info("✅ Local MLS storage cleared")

    // Re-register with fresh bundles
    logger.info("📝 Re-registering device with fresh key package bundles...")
    return try await ensureDeviceRegistered(userDid: userDid)
  }

  /// Recover from key package desync corruption
  /// This is the main recovery method to call when logs show STORAGE CORRUPTION
  ///
  /// Steps:
  /// 1. Delete device and all server-side key packages
  /// 2. Clear local MLS storage
  /// 3. Re-register with fresh bundles
  /// 4. Note: User must be re-invited to existing conversations
  public func recoverFromKeyPackageDesync(userDid: String) async throws {
    logger.error("🚨 RECOVERING FROM KEY PACKAGE DESYNC")
    logger.error("   This will:")
    logger.error("   1. Delete all server-side key packages")
    logger.error("   2. Clear local MLS storage")
    logger.error("   3. Re-register with fresh bundles")
    logger.error("   4. INVALIDATE existing conversations - user must be re-invited")

    let mlsDid = try await reregisterDevice(userDid: userDid)

    logger.info("✅ Recovery complete!")
    logger.info("   New MLS DID: \(mlsDid)")
    logger.warning("⚠️ IMPORTANT: User must be re-invited to existing conversations")
    logger.warning("   Old Welcome messages cannot be decrypted with new key packages")
  }

  // MARK: - Device Info

  /// Gets a human-readable device name
  private func getDeviceName() -> String {
    #if os(iOS)
      return UIDevice.current.name
    #elseif os(macOS)
      return Host.current().localizedName ?? "Mac"
    #endif
  }

  /// Gets device info to include with key package uploads
  /// Get device info for a specific user
  /// - Parameter userDid: The user's DID
  /// - Returns: Device info tuple or nil if not registered
  public func getDeviceInfo(for userDid: String) -> (
    deviceId: String, mlsDid: String, deviceUUID: String?
  )? {
    let normalizedUserDid = userDid.trimmingCharacters(in: .whitespacesAndNewlines)
    guard let info = deviceInfoByUser[normalizedUserDid] else {
      return nil
    }
    return (info.deviceId, info.mlsDid, info.deviceUUID)
  }

  /// Check if a device is registered locally for the given user
  public func isDeviceRegistered(for userDid: String) -> Bool {
    let normalizedUserDid = userDid.trimmingCharacters(in: .whitespacesAndNewlines)
    return deviceInfoByUser[normalizedUserDid] != nil
  }

  // MARK: - Client Identity Helpers

  /// Constructs an MLS client identity from user DID and device UUID.
  /// Format: `did:plc:xxx#deviceUUID`
  ///
  /// In MLS, each leaf node (client) needs a unique credential identity.
  /// Using `did#deviceId` allows multiple devices per user while maintaining
  /// unique identities in the MLS tree.
  ///
  /// - Parameters:
  ///   - userDid: The user's DID (e.g., `did:plc:abc123`)
  ///   - deviceUUID: The device's unique identifier
  /// - Returns: Client identity string in format `did#deviceUUID`
  public static func makeClientIdentity(userDid: String, deviceUUID: String) -> String {
    "\(userDid)#\(deviceUUID)"
  }

  /// Extracts the user DID from an MLS client identity.
  /// Handles both legacy (bare DID) and new (`did#deviceId`) formats.
  ///
  /// - Parameter clientIdentity: The MLS credential identity string
  /// - Returns: The user's DID portion
  public static func extractUserDID(from clientIdentity: String) -> String {
    if let hashIndex = clientIdentity.firstIndex(of: "#") {
      return String(clientIdentity[..<hashIndex])
    }
    // Legacy format: bare DID without device suffix
    return clientIdentity
  }

  /// Gets the MLS client identity for a user on this device.
  /// Returns the bare DID format that matches key package credentials.
  /// The server enforces "bare DID only" policy for MLS credential identities.
  ///
  /// - Parameter userDid: The user's DID
  /// - Returns: Bare DID (matches key package credential) if device is registered, nil otherwise
  public func getClientIdentity(for userDid: String) -> String? {
    let normalizedUserDid = userDid.trimmingCharacters(in: .whitespacesAndNewlines)
    guard deviceInfoByUser[normalizedUserDid] != nil else {
      return nil
    }
    // Return bare DID - must match key package credential for signature verification
    // Server rejects DID#deviceUUID format in MLS credentials
    return normalizedUserDid
  }

  // MARK: - Keychain Storage

  /// Save signature private key to keychain
  private static func saveSignatureKey(_ keyData: Data) throws {
    // SECURITY: Use ThisDeviceOnly to prevent signature key from syncing via iCloud Keychain.
    // If a device is stolen and user gets a new phone, we want a fresh signature key,
    // not the compromised one from the stolen device.
    let skipDP = MLSKeychainManager.shared.skipDataProtection
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrAccount as String: signatureKeyKey,
      kSecAttrService as String: "blue.catbird.mls",
      kSecValueData as String: keyData,
    ]
    if !skipDP {
      query[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    }

    // Delete existing key first
    SecItemDelete(query as CFDictionary)

    let status = SecItemAdd(query as CFDictionary, nil)
    if status == errSecDuplicateItem {
      // Update stale item in-place; fall back to delete+add if needed
      let updateQuery: [String: Any] = [
        kSecClass as String: kSecClassGenericPassword,
        kSecAttrAccount as String: signatureKeyKey,
        kSecAttrService as String: "blue.catbird.mls",
      ]

      var updateAttributes: [String: Any] = [
        kSecValueData as String: keyData,
      ]
      if !skipDP {
        updateAttributes[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
      }

      let updateStatus = SecItemUpdate(
        updateQuery as CFDictionary, updateAttributes as CFDictionary)

      if updateStatus == errSecSuccess {
        keychainLogger.info("Handled duplicate MLS signature key by updating existing item")
        return
      }

      keychainLogger.error(
        "Failed to update duplicate MLS signature key (status: \(updateStatus)) - retrying with delete/add"
      )
      SecItemDelete(updateQuery as CFDictionary)
      let retryStatus = SecItemAdd(query as CFDictionary, nil)
      guard retryStatus == errSecSuccess else {
        keychainLogger.error(
          "Failed to rewrite MLS signature key after duplicate (status: \(retryStatus))")
        throw MLSError.operationFailed
      }

      keychainLogger.info("Rewrote MLS signature key after clearing duplicate")
      return
    }

    guard status == errSecSuccess else {
      keychainLogger.error("Failed to store MLS signature key (status: \(status))")
      throw MLSError.operationFailed
    }
  }

  /// Load signature private key from keychain
  private static func loadSignatureKey() throws -> Data {
    let query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrAccount as String: signatureKeyKey,
      kSecAttrService as String: "blue.catbird.mls",
      kSecReturnData as String: true,
    ]

    var result: AnyObject?
    let status = SecItemCopyMatching(query as CFDictionary, &result)

    guard status == errSecSuccess, let keyData = result as? Data else {
      throw MLSError.operationFailed
    }

    return keyData
  }

  /// Delete signature private key from keychain
  private static func deleteSignatureKey() throws {
    let query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrAccount as String: signatureKeyKey,
      kSecAttrService as String: "blue.catbird.mls",
    ]

    let status = SecItemDelete(query as CFDictionary)
    // Ignore errSecItemNotFound - key might not exist
    guard status == errSecSuccess || status == errSecItemNotFound else {
      throw MLSError.operationFailed
    }
  }

  /// Save device UUID to local Keychain (fallback for macOS or when IDFV unavailable)
  private static func saveDeviceUUID(_ uuid: String) throws {
    let data = uuid.data(using: .utf8)!
    let skipDP = MLSKeychainManager.shared.skipDataProtection
    var query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrAccount as String: deviceUUIDKey,
      kSecAttrService as String: "blue.catbird.mls",
      kSecValueData as String: data,
        // NOT synchronized - device-local storage only
    ]
    if !skipDP {
      query[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlock
    }

    // Delete existing UUID first
    SecItemDelete(query as CFDictionary)

    let status = SecItemAdd(query as CFDictionary, nil)
    guard status == errSecSuccess else {
      throw MLSError.operationFailed
    }
  }

  /// Load device UUID from local Keychain (fallback for macOS or when IDFV unavailable)
  private static func loadDeviceUUID() throws -> String? {
    let query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrAccount as String: deviceUUIDKey,
      kSecAttrService as String: "blue.catbird.mls",
      kSecReturnData as String: true,
    ]

    var result: AnyObject?
    let status = SecItemCopyMatching(query as CFDictionary, &result)

    guard status == errSecSuccess, let data = result as? Data,
      let uuid = String(data: data, encoding: .utf8)
    else {
      return nil  // Return nil instead of throwing for missing UUID
    }

    return uuid
  }

  /// Delete device UUID from local Keychain
  private static func deleteDeviceUUID() throws {
    let query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrAccount as String: deviceUUIDKey,
      kSecAttrService as String: "blue.catbird.mls",
    ]

    let status = SecItemDelete(query as CFDictionary)
    // Ignore errSecItemNotFound - UUID might not exist
    guard status == errSecSuccess || status == errSecItemNotFound else {
      throw MLSError.operationFailed
    }
  }
}

// MARK: - Error Extension

extension MLSError {
  public static var deviceRegistrationFailed: MLSError {
    .operationFailed
  }
}
