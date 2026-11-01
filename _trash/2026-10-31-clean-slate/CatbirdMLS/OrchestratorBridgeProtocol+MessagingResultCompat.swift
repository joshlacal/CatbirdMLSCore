import Foundation

public extension OrchestratorBridgeProtocol {
  func sendPayloadResultJson(
    conversationId: String,
    payloadJson: String
  ) throws -> FfiSendResult {
    throw OrchestratorBridgeError.InvalidInput(
      message: "sendPayloadResultJson(conversationId:payloadJson:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func processIncomingMessage(
    envelope: FfiIncomingEnvelope,
    serverEpoch: UInt64?
  ) throws -> FfiMessageProcessingResult {
    throw OrchestratorBridgeError.InvalidInput(
      message: "processIncomingMessage(envelope:serverEpoch:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func processServerEvent(eventJson: String) throws -> [FfiEngineEvent] {
    throw OrchestratorBridgeError.InvalidInput(
      message: "processServerEvent(eventJson:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }
}
