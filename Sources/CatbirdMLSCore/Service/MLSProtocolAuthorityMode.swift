import Foundation

/// Selects which layer is authoritative for high-level MLS protocol decisions.
public enum MLSProtocolAuthorityMode: String, Codable, CaseIterable, Sendable {
  /// Existing Swift-owned orchestration remains authoritative.
  case swiftLegacy

  /// Rust receives mirrored decisions for diagnostics, but Swift still decides.
  case rustShadow

  /// Rust orchestrator owns protocol decisions; Swift owns lifecycle/storage access.
  case rustAuthoritative

  public static let defaultMode: MLSProtocolAuthorityMode = .swiftLegacy

  public var mirrorsRustDecisions: Bool {
    self == .rustShadow || self == .rustAuthoritative
  }

  public var usesRustForDecisions: Bool {
    self == .rustAuthoritative
  }
}
