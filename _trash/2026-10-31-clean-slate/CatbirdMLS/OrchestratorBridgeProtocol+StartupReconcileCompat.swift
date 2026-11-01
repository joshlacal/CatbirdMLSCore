import Foundation

public extension OrchestratorBridgeProtocol {
  func startupReconcile() throws -> FfiStartupReconcileReport {
    throw OrchestratorBridgeError.InvalidInput(
      message: "startupReconcile() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }
}
