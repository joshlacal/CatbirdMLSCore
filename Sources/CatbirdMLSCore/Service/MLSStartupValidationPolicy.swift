import Foundation

enum MLSStartupValidationPolicy {
  enum Decision: Equatable {
    case noCorruption
    case markCorruptedGroupsForRejoin
    case deferDestructiveRecovery
  }

  static let massCorruptionThreshold = 5
  static let automaticRecoverySuppressionInterval: TimeInterval = 10 * 60

  static func decision(
    totalConversations: Int,
    validatedCount: Int,
    corruptedCount: Int
  ) -> Decision {
    guard corruptedCount > 0 else {
      return .noCorruption
    }

    let majorityFailed = corruptedCount * 2 >= max(totalConversations, 1)
    let failuresDominateSuccesses = corruptedCount > validatedCount
    if corruptedCount >= massCorruptionThreshold && majorityFailed && failuresDominateSuccesses {
      return .deferDestructiveRecovery
    }

    return .markCorruptedGroupsForRejoin
  }
}
