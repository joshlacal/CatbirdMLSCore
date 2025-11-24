//
//  ConsumptionRecord.swift
//  CatbirdMLSCore
//
//  Created by Claude Code
//

import Foundation

/// Records key package consumption events for tracking and prediction
public struct ConsumptionRecord: Codable, Hashable, Sendable {
  /// When the consumption occurred
  public let timestamp: Date

  /// Number of packages consumed in this event
  public let packagesConsumed: Int

  /// The operation that caused consumption
  public let operation: ConsumptionOperation

  /// Optional context (e.g., conversation ID, number of members added)
  public let context: String?

  public init(
    timestamp: Date = Date(),
    packagesConsumed: Int,
    operation: ConsumptionOperation,
    context: String? = nil
  ) {
    self.timestamp = timestamp
    self.packagesConsumed = packagesConsumed
    self.operation = operation
    self.context = context
  }
}

/// Types of operations that consume key packages
public enum ConsumptionOperation: String, Codable, Sendable {
  case createConversation = "create_conversation"
  case addMembers = "add_members"
  case joinGroup = "join_group"
  case external = "external"  // Consumed by another client/device
  case unknown = "unknown"
}

public extension ConsumptionRecord {
  /// Check if this record is within the specified time window
  func isWithin(days: Int) -> Bool {
    let cutoff = Date().addingTimeInterval(-TimeInterval(days * 24 * 60 * 60))
    return timestamp >= cutoff
  }

  /// Check if this record is within the specified time window
  func isWithin(hours: Int) -> Bool {
    let cutoff = Date().addingTimeInterval(-TimeInterval(hours * 60 * 60))
    return timestamp >= cutoff
  }
}

/// Statistics derived from consumption records
public struct ConsumptionStatistics: Sendable {
  /// Average packages consumed per day
  public let averageDailyConsumption: Double

  /// Total packages consumed in the tracked period
  public let totalConsumed: Int

  /// Number of days in the tracked period
  public let trackedDays: Int

  /// Consumption in last 24 hours
  public let last24Hours: Int

  /// Consumption in last 7 days
  public let last7Days: Int

  /// Peak daily consumption in tracked period
  public let peakDailyConsumption: Int

  /// Predicted days until depletion (based on current inventory)
  public let predictedDepletionDays: Double?

  public init(
    averageDailyConsumption: Double,
    totalConsumed: Int,
    trackedDays: Int,
    last24Hours: Int,
    last7Days: Int,
    peakDailyConsumption: Int,
    currentInventory: Int? = nil
  ) {
    self.averageDailyConsumption = averageDailyConsumption
    self.totalConsumed = totalConsumed
    self.trackedDays = trackedDays
    self.last24Hours = last24Hours
    self.last7Days = last7Days
    self.peakDailyConsumption = peakDailyConsumption

    // Calculate predicted depletion
    if let inventory = currentInventory, averageDailyConsumption > 0 {
      self.predictedDepletionDays = Double(inventory) / averageDailyConsumption
    } else {
      self.predictedDepletionDays = nil
    }
  }

  /// Empty statistics (no consumption data)
  public static var empty: ConsumptionStatistics {
    ConsumptionStatistics(
      averageDailyConsumption: 0,
      totalConsumed: 0,
      trackedDays: 0,
      last24Hours: 0,
      last7Days: 0,
      peakDailyConsumption: 0
    )
  }
}
