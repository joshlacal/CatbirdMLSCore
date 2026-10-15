import Foundation

public extension OrchestratorBridgeProtocol {
  func labyrinthArchiveStatus() throws -> FfiLabyrinthArchiveStatus {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthArchiveStatus() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthClearArchiveReports() throws {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthClearArchiveReports() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthCreateRecoveryCode() throws -> String {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthCreateRecoveryCode() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthDeleteAccount() throws -> FfiLabyrinthDeletion {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthDeleteAccount() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthEnableBackup() throws -> String {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthEnableBackup() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthFlushArchive() throws -> UInt64 {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthFlushArchive() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthImportHistory(conversationId: String) throws -> FfiLabyrinthImport {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthImportHistory(conversationId:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthListThreads() throws -> FfiLabyrinthThreads {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthListThreads() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthLoadHistoryPage(
    conversationId: String,
    fromMs: UInt64?,
    untilMs: UInt64?
  ) throws -> FfiLabyrinthHistory {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthLoadHistoryPage(conversationId:fromMs:untilMs:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthProposeRepin(did: String) throws -> FfiLabyrinthRepinProposal {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthProposeRepin(did:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthRepinPeer(did: String, newHeadHex: String) throws {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthRepinPeer(did:newHeadHex:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthRestoreWithRecoveryCode(code: String) throws -> String {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthRestoreWithRecoveryCode(code:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthRetryQuarantined() throws -> UInt64 {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthRetryQuarantined() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthRetryRemoteDeletion() throws -> Bool {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthRetryRemoteDeletion() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthRevokeDevice(deviceKeyHex: String) throws -> UInt64 {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthRevokeDevice(deviceKeyHex:) is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }

  func labyrinthSignOut() throws -> Bool {
    throw OrchestratorBridgeError.InvalidInput(
      message: "labyrinthSignOut() is unavailable on this OrchestratorBridgeProtocol conformer"
    )
  }
}
