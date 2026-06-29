import Foundation

/// Selects which layer is authoritative for high-level MLS protocol decisions.
public enum MLSProtocolAuthorityMode: String, Codable, CaseIterable, Sendable {
  /// Existing Swift-owned orchestration remains authoritative.
  case swiftLegacy

  /// Rust receives mirrored decisions for diagnostics, but Swift still decides.
  case rustShadow

  /// Rust orchestrator owns protocol decisions; Swift owns lifecycle/storage access.
  case rustAuthoritative

  /// Rust orchestrator owns both protocol decisions and protocol mutations.
  case rustFull

  public static let defaultMode: MLSProtocolAuthorityMode = .rustFull

  public init?(rawRuntimeValue: String) {
    switch rawRuntimeValue {
    case "fullRust":
      self = .rustFull
    default:
      self.init(rawValue: rawRuntimeValue)
    }
  }

  public var mirrorsRustDecisions: Bool {
    self == .rustShadow || self == .rustAuthoritative || self == .rustFull
  }

  public var usesRustForDecisions: Bool {
    self == .rustAuthoritative || self == .rustFull
  }

  public var requiresRustOnlyProtocolMutations: Bool {
    self == .rustFull
  }
}
