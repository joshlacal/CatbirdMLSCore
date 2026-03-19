// MLSDeviceRecordTypes.swift

import Foundation

public enum MLSWhoCanMessageMe: String, Codable, Sendable {
  case everyone
  case mutuals
  case following
  case nobody
}

public struct MLSChatPolicy: Sendable, Codable, Equatable {
  public var allowFollowersBypass: Bool?
  public var allowFollowingBypass: Bool?
  public var whoCanMessageMe: MLSWhoCanMessageMe?
  public var autoExpireDays: Int?

  public init(
    allowFollowersBypass: Bool? = nil,
    allowFollowingBypass: Bool? = nil,
    whoCanMessageMe: MLSWhoCanMessageMe? = nil,
    autoExpireDays: Int? = nil
  ) {
    self.allowFollowersBypass = allowFollowersBypass
    self.allowFollowingBypass = allowFollowingBypass
    self.whoCanMessageMe = whoCanMessageMe
    self.autoExpireDays = autoExpireDays
  }
}

public struct MLSDeviceVerificationDecision: Sendable {
  public let allowed: Bool
  public let warning: String?
  public let failureReason: String?
}
