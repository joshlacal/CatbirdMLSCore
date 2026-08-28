import Foundation
import os.log

/// Manages cross-process coordination state for the Stop-The-World protocol.
/// Persisted in a shared JSON file in the App Group container.
public final class MLSCoordinationStore {
  
  public static let shared = MLSCoordinationStore()
  
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSCoordination")
  
  private let fileName = "coordination_state.\(MLSStoragePaths.cleanSuffix).json"
  public var currentGeneration: Int {
    getState().coordinationGeneration
  }
  
  public enum Phase: String, Codable {
    case active
    case switching
    case closed
  }
  
  public struct State: Codable {
    public var coordinationGeneration: Int
    public var activeUserDID: String?
    public var phase: Phase
    public var updatedAt: Date
    
    public static var initial: State {
      State(coordinationGeneration: 1, activeUserDID: nil, phase: .active, updatedAt: Date())
    }
  }
  
  private let queue = DispatchQueue(label: "blue.catbird.mls.coordination", qos: .userInitiated)
  
  private var fileURL: URL {
    if let dir = try? MLSStoragePaths.coordinationDirectory() {
      return dir.appendingPathComponent(fileName)
    }
    let fallbackDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("blue.catbird.mls.coordination", isDirectory: true)
    return fallbackDir.appendingPathComponent(fileName)
  }

  private init() {
    ensureStateExists()
  }

  private func ensureStateExists() {
    let url = fileURL
    var statBuf = stat()
    if lstat(url.path, &statBuf) == 0 {
      return
    }
    do {
      let dir = url.deletingLastPathComponent()
      try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
      let data = try JSONEncoder().encode(State.initial)
      try data.write(to: url, options: .withoutOverwriting)
    } catch let error as CocoaError where error.code == .fileWriteFileExists {
      // Created by peer concurrently; fine
    } catch {
      logger.error("❌ Failed to create initial coordination state: \(error.localizedDescription)")
    }
  }

  /// Strict fetch that differentiates absent file from corrupt/unreadable JSON
  func fetchState() throws -> State {
    let url = fileURL
    var statBuf = stat()
    if lstat(url.path, &statBuf) != 0 {
      if errno == ENOENT {
        return State.initial
      }
      throw MLSStorageInitializationError.unreadableState(
        details: "Filesystem error accessing coordination state: errno \(errno)"
      )
    }
    do {
      let data = try Data(contentsOf: url)
      let state = try JSONDecoder().decode(State.self, from: data)
      return state
    } catch {
      logger.critical("🚨 [COORD] Coordination state file exists but is corrupt: \(error.localizedDescription)")
      throw MLSStorageInitializationError.unreadableState(
        details: "Corrupt coordination state JSON: \(error.localizedDescription)"
      )
    }
  }

  /// Get current coordination state
  public func getState() -> State {
    do {
      return try fetchState()
    } catch {
      logger.critical("⚠️ [COORD] Failed to fetch coordination state: \(error.localizedDescription)")
      return State.initial
    }
  }
  
  /// Increment the coordination generation
  /// Called when starting a "Stop-The-World" event like account switching
  public func incrementGeneration(for userDID: String?) {
    queue.sync {
      do {
        var state = try fetchState()
        state.coordinationGeneration += 1
        state.activeUserDID = userDID
        state.updatedAt = Date()
        try saveStrict(state)
        logger.info("🔢 [COORD] Generation incremented to \(state.coordinationGeneration) for user: \(userDID?.prefix(16) ?? "nil", privacy: .private)")
      } catch {
        logger.critical("🚨 [COORD] Refusing to increment generation from unreadable/unwritable state: \(error.localizedDescription)")
      }
    }
  }

  func incrementGenerationStrict(for userDID: String?) throws {
    try queue.sync {
      var state = try fetchState()
      state.coordinationGeneration += 1
      state.activeUserDID = userDID
      state.updatedAt = Date()
      try saveStrict(state)
      logger.info("🔢 [COORD] Generation incremented strictly to \(state.coordinationGeneration) for user: \(userDID?.prefix(16) ?? "nil", privacy: .private)")
    }
  }
  
  /// Update the coordination phase
  public func updatePhase(_ phase: Phase) {
    queue.sync {
      do {
        var state = try fetchState()
        state.phase = phase
        state.updatedAt = Date()
        try saveStrict(state)
        logger.info("📡 [COORD] Phase updated to \(phase.rawValue)")
      } catch {
        logger.critical("🚨 [COORD] Refusing to update phase from unreadable/unwritable state: \(error.localizedDescription)")
      }
    }
  }
  
  /// Validate that the provided generation still matches the current state.
  /// Throws an error if the generation has changed, indicating the task should cancel.
  public func validateGeneration(_ expectedGen: Int) throws {
    let currentGen: Int
    do {
      currentGen = try fetchState().coordinationGeneration
    } catch {
      logger.critical("🚫 [COORD] Generation validation failed due to unreadable state: \(error.localizedDescription)")
      throw MLSCoordinationError.generationMismatch(expected: expectedGen, current: -1)
    }
    if expectedGen != currentGen {
      logger.warning("🚫 [COORD] Generation mismatch: expected \(expectedGen), current \(currentGen). Task must cancel.")
      throw MLSCoordinationError.generationMismatch(expected: expectedGen, current: currentGen)
    }
  }

  /// Reset state for specific user on account removal
  func deleteState(for userDID: String) {
    queue.sync {
      let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
      do {
        var state = try fetchState()
        if state.activeUserDID?.lowercased() == normalized {
          state.activeUserDID = nil
          state.phase = .active
          state.updatedAt = Date()
          try saveStrict(state)
        }
      } catch {
        logger.critical("🚨 [COORD] Refusing to delete state from unreadable/unwritable state: \(error.localizedDescription)")
      }
    }
  }

  func deleteStateStrict(for userDID: String) throws {
    try queue.sync {
      let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
      var state = try fetchState()
      if state.activeUserDID?.lowercased() == normalized {
        state.activeUserDID = nil
        state.phase = .active
        state.updatedAt = Date()
        try saveStrict(state)
      }
    }
  }

  func saveStrict(_ state: State) throws {
    let url = fileURL
    let dir = url.deletingLastPathComponent()
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    let data = try JSONEncoder().encode(state)
    try data.write(to: url, options: .atomic)
  }

  private func save(_ state: State) {
    do {
      try saveStrict(state)
    } catch {
      logger.critical("❌ Failed to save coordination state: \(error.localizedDescription)")
    }
  }
}

public enum MLSCoordinationError: Error, LocalizedError {
  case generationMismatch(expected: Int, current: Int)
  
  public var errorDescription: String? {
    switch self {
    case .generationMismatch(let expected, let current):
      return "MLS Coordination generation mismatch (expected \(expected), current \(current))"
    }
  }
}
