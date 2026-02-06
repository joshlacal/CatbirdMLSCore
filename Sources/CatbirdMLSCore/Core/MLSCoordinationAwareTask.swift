//
//  MLSCoordinationAwareTask.swift
//  CatbirdMLSCore
//

import Foundation

/// Small helper for making long-running async work fail-fast when account switching begins.
public enum MLSCoordinationAwareTask {

  public struct GenerationStaleError: Error, LocalizedError, Sendable {
    public let expected: Int
    public let current: Int

    public var errorDescription: String? {
      "MLS coordination generation changed (expected \(expected), current \(current))"
    }

    public init(expected: Int, current: Int) {
      self.expected = expected
      self.current = current
    }
  }

  public static func captureGeneration() -> Int {
    MLSCoordinationStore.shared.currentGeneration
  }

  public static func validateGeneration(_ expected: Int) throws {
    let current = MLSCoordinationStore.shared.currentGeneration
    if expected != current {
      throw GenerationStaleError(expected: expected, current: current)
    }
  }
}
