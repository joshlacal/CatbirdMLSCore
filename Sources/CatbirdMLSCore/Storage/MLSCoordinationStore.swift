import Foundation
import Synchronization
import os.log

/// Manages cross-process coordination state for the Stop-The-World protocol.
/// Persisted in a shared JSON file in the App Group container.
public final class MLSCoordinationStore {
  
  public static let shared = MLSCoordinationStore()
  
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSCoordination")
  private let activeUserProvider = Mutex<(@Sendable () -> String?)?>(nil)

  /// Provide an in-memory active user authority (e.g. from AppStateManager in the main app).
  public func setActiveUserProvider(_ provider: (@Sendable () -> String?)?) {
    activeUserProvider.withLock { $0 = provider }
  }

  
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
  
  private func fileURL() throws -> URL {
    let dir = try MLSStoragePaths.coordinationDirectory()
    return dir.appendingPathComponent(fileName)
  }

  private init() {
    ensureStateExists()
  }

  private func ensureStateExists() {
    do {
      let url = try fileURL()
      var statBuf = stat()
      if lstat(url.path, &statBuf) == 0 {
        return
      }
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
    let url = try fileURL()
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
      var state = try fetchState()
      if let provider = activeUserProvider.withLock({ $0 }), let liveActive = provider() {
        state.activeUserDID = liveActive
      }
      return state
    } catch {
      let fallbackActive = activeUserProvider.withLock({ $0 })?()
      logger.critical("⚠️ [COORD] Failed to fetch coordination state (corrupt or unavailable): \(error.localizedDescription)")
      return State(coordinationGeneration: -1, activeUserDID: fallbackActive, phase: .closed, updatedAt: Date())
    }
  }

  /// Set or update the active user DID and persist to the App Group container.
  /// If the active user has changed from the current state, this increments the coordination generation
  /// to invalidate in-flight work from the previous account.
  public func setActiveUserDID(_ userDID: String?) {
    queue.sync {
      do {
        var state = try fetchState()
        let normalized = userDID?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let currentNormalized = state.activeUserDID?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        if normalized != currentNormalized {
          state.coordinationGeneration += 1
          logger.info("🔢 [COORD] Generation incremented to \(state.coordinationGeneration) on active user change (\(currentNormalized ?? "nil", privacy: .private) -> \(normalized ?? "nil", privacy: .private))")
        }
        state.activeUserDID = userDID
        state.phase = .active
        state.updatedAt = Date()
        try saveStrict(state)
        logger.info("👤 [COORD] Active user set to \(userDID?.prefix(16) ?? "nil", privacy: .private)")
      } catch {
        logger.critical("🚨 [COORD] Refusing to set active user from unreadable/unwritable state: \(error.localizedDescription)")
      }
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
    if expectedGen != currentGen || currentGen < 0 {
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
    let url = try fileURL()
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
