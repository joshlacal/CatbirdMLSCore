import CryptoKit
import Foundation
import OSLog
import Security

/// Errors encountered during MLS storage initialization, validation, and coordination.
public enum MLSStorageInitializationError: Error, LocalizedError, Sendable, Equatable {
  case mixedState(details: String)
  case incompleteAttempt(details: String)
  case unreadableState(details: String)
  case validationFailed(details: String)
  case admissionDenied(details: String)
  case appGroupUnavailable(String)
  case keychainError(OSStatus)
  case invalidMarker(details: String)

  public var errorDescription: String? {
    switch self {
    case .mixedState(let details):
      return "MLS storage in inconsistent mixed state: \(details)"
    case .incompleteAttempt(let details):
      return "MLS storage creation attempt incomplete: \(details)"
    case .unreadableState(let details):
      return "MLS storage unreadable or corrupt: \(details)"
    case .validationFailed(let details):
      return "MLS storage validation failed: \(details)"
    case .admissionDenied(let details):
      return "MLS storage admission denied: \(details)"
    case .appGroupUnavailable(let group):
      return "Required App Group container unavailable: \(group)"
    case .keychainError(let status):
      return "Keychain operation failed with OSStatus: \(status)"
    case .invalidMarker(let details):
      return "Invalid MLS storage marker: \(details)"
    }
  }
}

/// Shared storage path resolver for clean MLS components (OpenMLS 0.9 generation).
public enum MLSStoragePaths {
  public static let generationToken = "mls-state-clean-v2-openmls-v09"
  public static let cleanSuffix = "clean-v2-openmls-v09"
  public static let cleanIdentifierSuffix = ".clean-v2-openmls-v09"

  public static let appGroupIdentifier = "group.blue.catbird.shared"
  private static let lock = NSLock()
  private static var overrideURL: URL?

  /// Override the base container used for MLS storage (process-local, testing).
  public static func setBaseDirectoryOverride(_ url: URL?) {
    lock.lock()
    overrideURL = url
    lock.unlock()
  }

  /// Resolve the base container for MLS storage.
  public static func baseContainerURL() -> URL {
    lock.lock()
    let override = overrideURL
    lock.unlock()

    if let override {
      return override
    }

    if let shared = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: appGroupIdentifier
    ) {
      return shared
    }

    return FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
  }

  /// Resolve the base container strictly requiring the App Group container or test override.
  /// Throws when unavailable; never falls back to process-local Application Support or .standard defaults.
  public static func requiredCleanContainerURL() throws -> URL {
    lock.lock()
    let override = overrideURL
    lock.unlock()

    if let override {
      return override
    }

    if let shared = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: appGroupIdentifier
    ) {
      return shared
    }

    throw MLSStorageInitializationError.appGroupUnavailable(appGroupIdentifier)
  }

  // MARK: - Generation-Scoped Directories

  public static func rustDatabaseDirectory() -> URL {
    baseContainerURL().appendingPathComponent("mls-state-\(cleanSuffix)", isDirectory: true)
  }

  public static func grdbDatabaseDirectory() -> URL {
    baseContainerURL().appendingPathComponent("MLS-\(cleanSuffix)", isDirectory: true)
  }

  public static func checkpointsDirectory() -> URL {
    baseContainerURL().appendingPathComponent("epoch-checkpoints-\(cleanSuffix)", isDirectory: true)
  }

  public static func welcomeGateDirectory() -> URL {
    baseContainerURL().appendingPathComponent("mls_welcome_gate-\(cleanSuffix)", isDirectory: true)
  }

  public static func coordinationDirectory() -> URL {
    baseContainerURL().appendingPathComponent("mls-coordination-\(cleanSuffix)", isDirectory: true)
  }
}

/// Supported database kinds in clean OpenMLS 0.9 architecture.
public enum MLSDatabaseKind: String, Codable, Sendable, CaseIterable {
  case rustState
  case swiftGRDB
}

/// Lifecycle state for an initialization attempt.
public enum MLSInitializationState: String, Codable, Sendable {
  case creating
  case complete
}

/// Generation-scoped persisted record binding attempt UUID, database kind, DID, path hash, and state.
public struct MLSInitializationRecord: Codable, Sendable, Equatable {
  public let generationToken: String
  public let attemptUUID: String
  public let userDID: String
  public let databaseKind: String
  public let databasePathHash: String
  public let state: MLSInitializationState
  public let createdAt: TimeInterval
  public let completedAt: TimeInterval?

  public init(
    generationToken: String,
    attemptUUID: String,
    userDID: String,
    databaseKind: String,
    databasePathHash: String,
    state: MLSInitializationState,
    createdAt: TimeInterval = Date().timeIntervalSince1970,
    completedAt: TimeInterval? = nil
  ) {
    self.generationToken = generationToken
    self.attemptUUID = attemptUUID
    self.userDID = userDID
    self.databaseKind = databaseKind
    self.databasePathHash = databasePathHash
    self.state = state
    self.createdAt = createdAt
    self.completedAt = completedAt
  }
}

/// Result of evaluating storage state against the all-or-none state machine.
public enum MLSStorageStateEvaluation: Sendable, Equatable {
  case allAbsent
  case complete(MLSInitializationRecord)
  case incompleteAttempt(MLSInitializationRecord)
  case mixedState(details: String)
  case unreadableState(details: String)
}

/// Token representing an active lease or lock.
public final class MLSLeaseToken: @unchecked Sendable {
  private let fileDescriptor: Int32
  private let path: String
  private var isReleased = false
  private let lock = NSLock()

  init(fileDescriptor: Int32, path: String) {
    self.fileDescriptor = fileDescriptor
    self.path = path
  }

  public func release() {
    lock.lock()
    defer { lock.unlock() }
    guard !isReleased else { return }
    isReleased = true
    if fileDescriptor >= 0 {
      flock(fileDescriptor, LOCK_UN)
      close(fileDescriptor)
    }
  }

  deinit {
    release()
  }
}

/// Coordinates cross-process first-open admission, CAS markers, immutable keys, and fail-closed state.
public final class MLSStorageCoordinator: @unchecked Sendable {
  public static let shared = MLSStorageCoordinator()

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "StorageCoordinator")
  private let internalLock = NSLock()

  private init() {}

  // MARK: - Paths & Identifiers

  public func databaseURL(for kind: MLSDatabaseKind, userDID: String) -> URL {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    switch kind {
    case .rustState:
      let didHash = normalizedDID.data(using: .utf8)?.base64EncodedString()
        .replacingOccurrences(of: "/", with: "_")
        .replacingOccurrences(of: "+", with: "-")
        .replacingOccurrences(of: "=", with: "")
        .prefix(64).description ?? "default"
      return MLSStoragePaths.rustDatabaseDirectory().appendingPathComponent("\(didHash).db")

    case .swiftGRDB:
      let sanitized = normalizedDID
        .replacingOccurrences(of: ":", with: "-")
        .replacingOccurrences(of: "/", with: "-")
        .replacingOccurrences(of: "#", with: "-")
        .replacingOccurrences(of: "?", with: "-")
      return MLSStoragePaths.grdbDatabaseDirectory().appendingPathComponent("mls_messages_\(sanitized).db")
    }
  }

  public func databasePathHash(for kind: MLSDatabaseKind, userDID: String) -> String {
    let url = databaseURL(for: kind, userDID: userDID)
    let standardPath = url.standardizedFileURL.path
    let digest = SHA256.hash(data: Data(standardPath.utf8))
    return digest.compactMap { String(format: "%02x", $0) }.joined()
  }

  public func markerURL(for kind: MLSDatabaseKind, userDID: String) -> URL {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let didHash = databasePathHash(for: kind, userDID: normalizedDID)
    let markersDir = MLSStoragePaths.coordinationDirectory().appendingPathComponent("markers", isDirectory: true)
    return markersDir.appendingPathComponent("marker_\(kind.rawValue)_\(didHash.prefix(16)).json")
  }

  public func leaseURL(for kind: MLSDatabaseKind, userDID: String, type: String) -> URL {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let didHash = databasePathHash(for: kind, userDID: normalizedDID)
    let leasesDir = MLSStoragePaths.coordinationDirectory().appendingPathComponent("leases", isDirectory: true)
    return leasesDir.appendingPathComponent("\(type)_\(kind.rawValue)_\(didHash.prefix(16)).lease")
  }

  public func quarantineDirectoryURL(for kind: MLSDatabaseKind) -> URL {
    switch kind {
    case .rustState:
      return MLSStoragePaths.rustDatabaseDirectory().appendingPathComponent("Quarantine", isDirectory: true)
    case .swiftGRDB:
      return MLSStoragePaths.grdbDatabaseDirectory().appendingPathComponent("Quarantine", isDirectory: true)
    }
  }

  // MARK: - State Evaluation

  public func evaluateState(for kind: MLSDatabaseKind, userDID: String) throws -> MLSStorageStateEvaluation {
    _ = try MLSStoragePaths.requiredCleanContainerURL()
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()

    let dbURL = databaseURL(for: kind, userDID: normalizedDID)
    let dbExists = FileManager.default.fileExists(atPath: dbURL.path)

    let keysExist = try hasRequiredKeys(for: kind, userDID: normalizedDID)
    let marker = try readMarker(for: kind, userDID: normalizedDID)

    if !dbExists && !keysExist && marker == nil {
      return .allAbsent
    }

    if dbExists && keysExist, let marker, marker.state == .complete {
      // Validate marker binding
      let expectedHash = databasePathHash(for: kind, userDID: normalizedDID)
      guard marker.generationToken == MLSStoragePaths.generationToken,
            marker.userDID == normalizedDID,
            marker.databaseKind == kind.rawValue,
            marker.databasePathHash == expectedHash
      else {
        return .mixedState(details: "Marker binding mismatch for complete database")
      }
      return .complete(marker)
    }

    if let marker, marker.state == .creating {
      return .incompleteAttempt(marker)
    }

    // Any other state is a mixed state
    var mixedDetails: [String] = []
    if dbExists { mixedDetails.append("database present") } else { mixedDetails.append("database absent") }
    if keysExist { mixedDetails.append("key present") } else { mixedDetails.append("key absent") }
    if let marker { mixedDetails.append("marker=\(marker.state.rawValue)") } else { mixedDetails.append("marker absent") }

    return .mixedState(details: mixedDetails.joined(separator: ", "))
  }

  public func hasRequiredKeys(for kind: MLSDatabaseKind, userDID: String) throws -> Bool {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    switch kind {
    case .rustState:
      let keyAccount = "mls.encryption.key.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      return try MLSKeychainManager.shared.retrieveKeyStrict(forKey: keyAccount) != nil

    case .swiftGRDB:
      let keyAccount = "mls.sqlcipher.db.key.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      let saltAccount = "mls.sqlcipher.db.salt.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      let hasKey = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: keyAccount) != nil
      let hasSalt = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: saltAccount) != nil
      return hasKey && hasSalt
    }
  }

  // MARK: - Marker I/O

  public func readMarker(for kind: MLSDatabaseKind, userDID: String) throws -> MLSInitializationRecord? {
    let url = markerURL(for: kind, userDID: userDID)
    guard FileManager.default.fileExists(atPath: url.path) else { return nil }

    do {
      let data = try Data(contentsOf: url)
      let record = try JSONDecoder().decode(MLSInitializationRecord.self, from: data)
      return record
    } catch {
      throw MLSStorageInitializationError.invalidMarker(details: "Failed to decode marker: \(error.localizedDescription)")
    }
  }

  public func writeMarkerDirectlyForTesting(_ record: MLSInitializationRecord) throws {
    guard let kind = MLSDatabaseKind(rawValue: record.databaseKind) else { return }
    let url = markerURL(for: kind, userDID: record.userDID)
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
    let data = try JSONEncoder().encode(record)
    try data.write(to: url, options: .atomic)
  }

  // MARK: - Leases & Mutexes

  public func acquireAdmissionLease(for kind: MLSDatabaseKind, userDID: String) throws -> MLSLeaseToken {
    let url = leaseURL(for: kind, userDID: userDID, type: "admission")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open admission lease file: \(errno)")
    }

    if flock(fd, LOCK_SH) != 0 {
      close(fd)
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to acquire shared admission lease: \(errno)")
    }

    return MLSLeaseToken(fileDescriptor: fd, path: url.path)
  }

  public func acquireExclusiveResetLease(for kind: MLSDatabaseKind, userDID: String) throws -> MLSLeaseToken {
    let url = leaseURL(for: kind, userDID: userDID, type: "admission")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open admission lease file for reset: \(errno)")
    }

    if flock(fd, LOCK_EX) != 0 {
      close(fd)
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to acquire exclusive reset lease: \(errno)")
    }

    return MLSLeaseToken(fileDescriptor: fd, path: url.path)
  }

  public func acquireMutationMutex(for kind: MLSDatabaseKind, userDID: String) throws -> MLSLeaseToken {
    let url = leaseURL(for: kind, userDID: userDID, type: "mutation")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open mutation lock file: \(errno)")
    }

    if flock(fd, LOCK_EX) != 0 {
      close(fd)
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to acquire mutation lock: \(errno)")
    }

    return MLSLeaseToken(fileDescriptor: fd, path: url.path)
  }

  // MARK: - Coordinated Creation & Completion

  public func beginCreation(for kind: MLSDatabaseKind, userDID: String) throws -> (MLSInitializationRecord, MLSLeaseToken) {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let admissionLease = try acquireAdmissionLease(for: kind, userDID: normalizedDID)

    // Re-check state under admission lease
    let status = try evaluateState(for: kind, userDID: normalizedDID)
    guard case .allAbsent = status else {
      admissionLease.release()
      switch status {
      case .complete(let record):
        return (record, try acquireAdmissionLease(for: kind, userDID: normalizedDID))
      case .incompleteAttempt(let record):
        throw MLSStorageInitializationError.incompleteAttempt(details: "Restarted during creating attempt \(record.attemptUUID)")
      case .mixedState(let details):
        throw MLSStorageInitializationError.mixedState(details: details)
      case .unreadableState(let details):
        throw MLSStorageInitializationError.unreadableState(details: details)
      case .allAbsent:
        break
      }
    }

    let attemptUUID = UUID().uuidString
    let pathHash = databasePathHash(for: kind, userDID: normalizedDID)
    let record = MLSInitializationRecord(
      generationToken: MLSStoragePaths.generationToken,
      attemptUUID: attemptUUID,
      userDID: normalizedDID,
      databaseKind: kind.rawValue,
      databasePathHash: pathHash,
      state: .creating
    )

    let url = markerURL(for: kind, userDID: normalizedDID)
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
    let data = try JSONEncoder().encode(record)

    // CAS write
    let tempURL = url.deletingLastPathComponent().appendingPathComponent("tmp_\(attemptUUID).json")
    try data.write(to: tempURL, options: .atomic)
    _ = try? FileManager.default.removeItem(at: url)
    try FileManager.default.moveItem(at: tempURL, to: url)

    logger.info("📝 [StorageCoordinator] Published creating marker for \(kind.rawValue): \(normalizedDID.prefix(20), privacy: .private)")
    return (record, admissionLease)
  }

  public func completeCreation(for kind: MLSDatabaseKind, userDID: String, attemptUUID: String) throws {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let pathHash = databasePathHash(for: kind, userDID: normalizedDID)

    let completedRecord = MLSInitializationRecord(
      generationToken: MLSStoragePaths.generationToken,
      attemptUUID: attemptUUID,
      userDID: normalizedDID,
      databaseKind: kind.rawValue,
      databasePathHash: pathHash,
      state: .complete,
      completedAt: Date().timeIntervalSince1970
    )

    let url = markerURL(for: kind, userDID: normalizedDID)
    let data = try JSONEncoder().encode(completedRecord)
    let tempURL = url.deletingLastPathComponent().appendingPathComponent("tmp_complete_\(attemptUUID).json")
    try data.write(to: tempURL, options: .atomic)
    _ = try? FileManager.default.removeItem(at: url)
    try FileManager.default.moveItem(at: tempURL, to: url)

    logger.info("✅ [StorageCoordinator] Published complete marker for \(kind.rawValue): \(normalizedDID.prefix(20), privacy: .private)")
  }
}
