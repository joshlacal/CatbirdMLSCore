import Foundation

public extension OrchestratorBridgeProtocol {
  func runDeferredRecovery(reason: String) throws -> FfiDeferredRecoveryReport {
    throw OrchestratorBridgeError.InvalidInput(
      message: "runDeferredRecovery(reason:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }
}
