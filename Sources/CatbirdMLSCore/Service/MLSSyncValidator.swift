import Foundation
import OSLog
import Petrel

/// Actor responsible for periodic validation of MLS device state and sync status
/// Runs validation checks every 6 hours and triggers recovery actions when safe
@available(iOS 18.0, macOS 13.0, *)
public final class MLSSyncValidator {
    private let logger = Logger(subsystem: "blue.catbird", category: "SyncValidator")
    private let client: ATProtoClient

    /// Validation interval (6 hours)
    private let validationInterval: TimeInterval = 6 * 60 * 60

    /// Last validation timestamp
    private var lastValidation: Date?

    /// Timer for periodic validation
    private var validationTimer: Timer?

    /// Whether automatic recovery is enabled
    public var autoRecoveryEnabled: Bool = true

    public init(client: ATProtoClient) {
        self.client = client
    }

    /// Start periodic sync validation
    public func startPeriodicValidation() {
        logger.info("Starting periodic sync validation (interval: 6 hours)")

        // Perform initial validation after a short delay
        Task {
            try? await Task.sleep(nanoseconds: 30_000_000_000) // 30 seconds
            await performValidation()
        }

        // Schedule periodic validation
        validationTimer = Timer.scheduledTimer(
            withTimeInterval: validationInterval,
            repeats: true
        ) { [weak self] _ in
            Task { @MainActor in
                await self?.performValidation()
            }
        }
    }

    /// Stop periodic sync validation
    public func stopPeriodicValidation() {
        logger.info("Stopping periodic sync validation")
        validationTimer?.invalidate()
        validationTimer = nil
    }

    /// Perform a sync validation check
    public func performValidation() async {
        logger.info("Starting sync validation check")
        lastValidation = Date()

        do {
            // Use listDevices to check device validity
            let input = BlueCatbirdMlsChatListDevices.Parameters()
            let (responseCode, output) = try await client.blue.catbird.mlschat.listDevices(input: input)

            guard responseCode == 200, let output = output else {
                logger.error("Validation failed with HTTP \(responseCode)")
                return
            }

            // Derive validation from device list
            let devices = output.devices
            let hasDevices = !devices.isEmpty
            let totalKeyPackages = devices.reduce(0) { $0 + $1.keyPackageCount }
            let lowPackageDevices = devices.filter { $0.keyPackageCount < 20 }

            if hasDevices && lowPackageDevices.isEmpty {
                logger.info("✅ Device state is valid")
            } else if !hasDevices {
                logger.warning("⚠️ Device state validation failed")
                logger.warning("Issues: No devices registered")
            } else {
                logger.warning("⚠️ Device state validation: low key packages detected")
                for device in lowPackageDevices {
                    logger.warning("   Device \(device.deviceId): \(device.keyPackageCount) key packages")
                }
            }

            // Log detailed stats
            logger.info("Device count: \(devices.count), total key packages: \(totalKeyPackages)")

            // Trigger automatic recovery if enabled and safe
            if (!hasDevices || !lowPackageDevices.isEmpty) && autoRecoveryEnabled {
                await performRecoveryActions(devices: output)
            }

        } catch {
            logger.error("Sync validation failed: \(error.localizedDescription)")
        }
    }

    /// Perform automatic recovery actions based on validation results
    private func performRecoveryActions(devices: BlueCatbirdMlsChatListDevices.Output) async {
        logger.info("Performing automatic recovery actions")

        // 1. Replenish key packages if below threshold
        let lowPackageDevices = devices.devices.filter { $0.keyPackageCount < 20 }
        if !lowPackageDevices.isEmpty {
            logger.info("🔑 Key package inventory low - need replenishment")
            for device in lowPackageDevices {
                logger.info("   Device \(device.deviceId): \(device.keyPackageCount) available, target: 20")
            }
            // Note: Key package replenishment is handled by MLSKeyPackageMonitor
            // We just log the recommendation here
        }

        // 2. Log device health recommendations
        if devices.devices.isEmpty {
            logger.warning("⚠️ No devices registered - device registration required")
        }
    }

    /// Get time until next validation
    public var timeUntilNextValidation: TimeInterval? {
        guard let lastValidation = lastValidation else {
            return nil
        }
        let nextValidation = lastValidation.addingTimeInterval(validationInterval)
        return nextValidation.timeIntervalSinceNow
    }

    /// Get time since last validation
    public var timeSinceLastValidation: TimeInterval? {
        guard let lastValidation = lastValidation else {
            return nil
        }
        return Date().timeIntervalSince(lastValidation)
    }
}
