import Foundation
import CatbirdMLS

public struct MLSPeerBadErrorClassification: Equatable, Sendable {
  public let peerBad: Bool
  public let quarantineTriggerEligible: Bool
  public let wrongEpoch: Bool
  public let reason: String
}

public enum MLSErrorClassifier {
  public static func classifyPeerBad(
    _ error: MlsError,
    localEpoch: UInt64?,
    messageEpoch: UInt64
  ) -> MLSPeerBadErrorClassification {
    let mapped = map(error)
    let ffi = mlsClassifyPeerBadError(
      errorKind: mapped.kind,
      errorMessage: mapped.message,
      localEpoch: localEpoch,
      messageEpoch: messageEpoch
    )
    return MLSPeerBadErrorClassification(
      peerBad: ffi.peerBad,
      quarantineTriggerEligible: ffi.quarantineTriggerEligible,
      wrongEpoch: ffi.wrongEpoch,
      reason: ffi.reason
    )
  }

  private static func map(_ error: MlsError) -> (kind: FfiMlsErrorKind, message: String) {
    switch error {
    case .InvalidCommit(let message):
      return (.invalidCommit, message)
    case .WireFormatPolicyViolation(let message):
      return (.wireFormatPolicyViolation, message)
    case .InvalidProposalRef(let message):
      return (.invalidProposalRef, message)
    case .TlsCodec(let message):
      return (.tlsCodec, message)
    case .CommitProcessingFailed(let message):
      return (.commitProcessingFailed, message)
    case .OpenMls(let message):
      return (.openMls, message)
    case .DecryptionFailed(let message):
      return (.decryptionFailed, message)
    default:
      return (.other, error.localizedDescription)
    }
  }
}
