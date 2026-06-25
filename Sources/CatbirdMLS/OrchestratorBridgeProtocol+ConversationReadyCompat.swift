import Foundation

public extension OrchestratorBridgeProtocol {
  func ensureConversationReady(convoId: String) throws -> FfiConversationReadyResult {
    throw OrchestratorBridgeError.InvalidInput(
      message: "ensureConversationReady(convoId:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }
}
