import Foundation
import os.log

/// Manages cross-process coordination state for the Stop-The-World protocol.
/// Persisted in a shared JSON file in the App Group container.
public final class MLSCoordinationStore {
  
  public static let shared = MLSCoordinationStore()
  
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "MLSCoordination")
  
  private let fileName = "coordination_state.json"
  
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
  
  private var containerURL: URL {
    MLSStoragePaths.baseContainerURL()
  }
  
  private var fileURL: URL {
    containerURL.appendingPathComponent(fileName)
  }
  
  private init() {
    ensureStateExists()
  }
  
  private func ensureStateExists() {
    guard !FileManager.default.fileExists(atPath: fileURL.path) else { return }
    save(State.initial)
  }
  
  /// Get current coordination state
  public func getState() -> State {
    guard let data = try? Data(contentsOf: fileURL),
          let state = try? JSONDecoder().decode(State.self, from: data) else {
      return State.initial
    }
    return state
  }
  
  /// Increment the coordination generation
  /// Called when starting a "Stop-The-World" event like account switching
  public func incrementGeneration(for userDID: String?) {
    queue.sync {
      var state = getState()
      state.coordinationGeneration += 1
      state.activeUserDID = userDID
      state.updatedAt = Date()
      save(state)
      logger.info("🔢 [COORD] Generation incremented to \(state.coordinationGeneration) for user: \(userDID?.prefix(16) ?? "nil")")
    }
  }
  
  /// Update the coordination phase
  public func updatePhase(_ phase: Phase) {
    queue.sync {
      var state = getState()
      state.phase = phase
      state.updatedAt = Date()
      save(state)
      logger.info("📡 [COORD] Phase updated to \(phase.rawValue)")
    }
  }
  
  /// Validate that the provided generation still matches the current state.
  /// Throws an error if the generation has changed, indicating the task should cancel.
  public func validateGeneration(_ expectedGen: Int) throws {
    let currentGen = getState().coordinationGeneration
    if expectedGen != currentGen {
      logger.warning("🚫 [COORD] Generation mismatch: expected \(expectedGen), current \(currentGen). Task must cancel.")
      throw MLSCoordinationError.generationMismatch(expected: expectedGen, current: currentGen)
    }
  }
  
  private func save(_ state: State) {
    do {
      let data = try JSONEncoder().encode(state)
      try data.write(to: fileURL, options: .atomic)
    } catch {
      logger.error("❌ Failed to save coordination state: \(error.localizedDescription)")
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
