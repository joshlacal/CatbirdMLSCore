import Foundation

public extension OrchestratorBridgeProtocol {
  func debugWipeLocalGroupForRecovery(convoId: String) throws -> FfiDebugWipeLocalGroupResult {
    throw OrchestratorBridgeError.InvalidInput(
      message: "debugWipeLocalGroupForRecovery(convoId:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }
}
