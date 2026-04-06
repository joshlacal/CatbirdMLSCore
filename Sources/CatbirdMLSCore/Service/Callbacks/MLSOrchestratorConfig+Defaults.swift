import CatbirdMLS
import Foundation

extension FfiOrchestratorConfig {
  /// Default orchestrator configuration matching Rust defaults.
  static let `default` = FfiOrchestratorConfig(
    maxDevices: 10,
    targetKeyPackageCount: 50,
    keyPackageReplenishThreshold: 10,
    syncCooldownSeconds: 5,
    maxConsecutiveSyncFailures: 5,
    syncPauseDurationSeconds: 60,
    rejoinCooldownSeconds: 60,
    maxRejoinAttempts: 3
  )
}
