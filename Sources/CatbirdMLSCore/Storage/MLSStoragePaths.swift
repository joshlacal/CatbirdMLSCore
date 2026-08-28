import CryptoKit
import Foundation
import OSLog
import Security

/// Errors encountered during MLS storage initialization, validation, and coordination.
enum MLSStorageInitializationError: Error, LocalizedError, Sendable, Equatable {
  case mixedState(details: String)
  case incompleteAttempt(details: String)
  case unreadableState(details: String)
  case validationFailed(details: String)
  case admissionDenied(details: String)
  case appGroupUnavailable(String)
  case keychainError(OSStatus)
  case invalidMarker(details: String)

  var errorDescription: String? {
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
  static let generationToken = "mls-state-clean-v2-openmls-v09"
  static let cleanSuffix = "clean-v2-openmls-v09"
  static let cleanIdentifierSuffix = ".clean-v2-openmls-v09"

  static let appGroupIdentifier = "group.blue.catbird.shared"
  private static let lock = NSLock()
  private static var overrideURL: URL?

  /// Override the base container used for MLS storage (process-local, testing).
  public static func setBaseDirectoryOverride(_ url: URL?) {
    lock.lock()
    overrideURL = url
    lock.unlock()
  }

  /// Resolve the base container for MLS storage.
  /// Retained for untouched legacy/test compatibility; clean runtime paths use requiredCleanContainerURL().
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
  static func requiredCleanContainerURL() throws -> URL {
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

  static func rustDatabaseDirectory() throws -> URL {
    try requiredCleanContainerURL().appendingPathComponent("mls-state-\(cleanSuffix)", isDirectory: true)
  }

  static func grdbDatabaseDirectory() throws -> URL {
    try requiredCleanContainerURL().appendingPathComponent("MLS-\(cleanSuffix)", isDirectory: true)
  }

  static func checkpointsDirectory() throws -> URL {
    try requiredCleanContainerURL().appendingPathComponent("epoch-checkpoints-\(cleanSuffix)", isDirectory: true)
  }

  static func welcomeGateDirectory() throws -> URL {
    try requiredCleanContainerURL().appendingPathComponent("mls_welcome_gate-\(cleanSuffix)", isDirectory: true)
  }

  static func coordinationDirectory() throws -> URL {
    try requiredCleanContainerURL().appendingPathComponent("mls-coordination-\(cleanSuffix)", isDirectory: true)
  }
}

/// Supported database kinds in clean OpenMLS 0.9 architecture.
enum MLSDatabaseKind: String, Codable, Sendable, CaseIterable {
  case rustState
  case swiftGRDB
}

/// Lifecycle state for an initialization attempt.
enum MLSInitializationState: String, Codable, Sendable {
  case creating
  case complete
}

/// Generation-scoped persisted record binding attempt UUID, database kind, DID, path hash, and state.
struct MLSInitializationRecord: Codable, Sendable, Equatable {
  let generationToken: String
  let attemptUUID: String
  let userDID: String
  let databaseKind: String
  let databasePathHash: String
  let state: MLSInitializationState
  let createdAt: TimeInterval
  let completedAt: TimeInterval?

  init(
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
enum MLSStorageStateEvaluation: Sendable, Equatable {
  case allAbsent
  case complete(MLSInitializationRecord)
  case incompleteAttempt(MLSInitializationRecord)
  case mixedState(details: String)
  case unreadableState(details: String)
}

/// Token representing an active lease or lock.
final class MLSLeaseToken: @unchecked Sendable {
  private let fileDescriptor: Int32
  private let path: String
  private var isReleased = false
  private let lock = NSLock()

  init(fileDescriptor: Int32, path: String) {
    self.fileDescriptor = fileDescriptor
    self.path = path
  }

  func release() {
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
final class MLSStorageCoordinator: @unchecked Sendable {
  static let shared = MLSStorageCoordinator()

  private let logger = Logger(subsystem: "blue.catbird.mls", category: "StorageCoordinator")
  private let localLock = NSLock()
  private var resourceLocks: [String: NSLock] = [:]

  /// Seam for test barriers
  var testBarrierHook: (@Sendable () -> Void)?

  private init() {}

  private func resourceLock(for kind: MLSDatabaseKind, userDID: String) -> NSLock {
    localLock.lock()
    defer { localLock.unlock() }
    let key = "\(kind.rawValue):\(userDID)"
    if let existing = resourceLocks[key] {
      return existing
    }
    let newLock = NSLock()
    resourceLocks[key] = newLock
    return newLock
  }

  // MARK: - Paths & Identifiers

  func databaseURL(for kind: MLSDatabaseKind, userDID: String) throws -> URL {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    switch kind {
    case .rustState:
      let didHash = normalizedDID.data(using: .utf8)?.base64EncodedString()
        .replacingOccurrences(of: "/", with: "_")
        .replacingOccurrences(of: "+", with: "-")
        .replacingOccurrences(of: "=", with: "")
        .prefix(64).description ?? "default"
      return try MLSStoragePaths.rustDatabaseDirectory().appendingPathComponent("\(didHash).db")

    case .swiftGRDB:
      let sanitized = normalizedDID
        .replacingOccurrences(of: ":", with: "-")
        .replacingOccurrences(of: "/", with: "-")
        .replacingOccurrences(of: "#", with: "-")
        .replacingOccurrences(of: "?", with: "-")
      return try MLSStoragePaths.grdbDatabaseDirectory().appendingPathComponent("mls_messages_\(sanitized).db")
    }
  }

  func databasePathHash(for kind: MLSDatabaseKind, userDID: String) throws -> String {
    let url = try databaseURL(for: kind, userDID: userDID)
    let standardPath = url.standardizedFileURL.path
    let digest = SHA256.hash(data: Data(standardPath.utf8))
    return digest.compactMap { String(format: "%02x", $0) }.joined()
  }

  func markerURL(for kind: MLSDatabaseKind, userDID: String) throws -> URL {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let didHash = try databasePathHash(for: kind, userDID: normalizedDID)
    let markersDir = try MLSStoragePaths.coordinationDirectory().appendingPathComponent("markers", isDirectory: true)
    return markersDir.appendingPathComponent("marker_\(kind.rawValue)_\(didHash.prefix(16)).json")
  }

  func leaseURL(for kind: MLSDatabaseKind, userDID: String, type: String) throws -> URL {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let didHash = try databasePathHash(for: kind, userDID: normalizedDID)
    let leasesDir = try MLSStoragePaths.coordinationDirectory().appendingPathComponent("leases", isDirectory: true)
    return leasesDir.appendingPathComponent("\(type)_\(kind.rawValue)_\(didHash.prefix(16)).lease")
  }

  func quarantineDirectoryURL(for kind: MLSDatabaseKind) throws -> URL {
    switch kind {
    case .rustState:
      return try MLSStoragePaths.rustDatabaseDirectory().appendingPathComponent("Quarantine", isDirectory: true)
    case .swiftGRDB:
      return try MLSStoragePaths.grdbDatabaseDirectory().appendingPathComponent("Quarantine", isDirectory: true)
    }
  }

  // MARK: - State Evaluation (under Admission Lease)

  func evaluateState(for kind: MLSDatabaseKind, userDID: String) throws -> MLSStorageStateEvaluation {
    _ = try MLSStoragePaths.requiredCleanContainerURL()
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()

    let lease = try acquireAdmissionLease(for: kind, userDID: normalizedDID)
    defer { lease.release() }

    return try evaluateStateUnderLease(for: kind, userDID: normalizedDID)
  }

  private func evaluateStateUnderLease(for kind: MLSDatabaseKind, userDID: String) throws -> MLSStorageStateEvaluation {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let dbURL = try databaseURL(for: kind, userDID: normalizedDID)
    let dbExists = FileManager.default.fileExists(atPath: dbURL.path)

    let keyState = try checkRequiredKeys(for: kind, userDID: normalizedDID)
    let marker = try readMarker(for: kind, userDID: normalizedDID)

    if !dbExists && keyState.allAbsent && marker == nil {
      return .allAbsent
    }

    if dbExists && keyState.allPresent, let marker, marker.state == .complete {
      // Validate marker binding
      let expectedHash = try databasePathHash(for: kind, userDID: normalizedDID)
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
    if kind == .swiftGRDB {
      if keyState.hasKey { mixedDetails.append("key present") } else { mixedDetails.append("key absent") }
      if keyState.hasSalt { mixedDetails.append("salt present") } else { mixedDetails.append("salt absent") }
    } else {
      if keyState.hasKey { mixedDetails.append("key present") } else { mixedDetails.append("key absent") }
    }
    if let marker { mixedDetails.append("marker=\(marker.state.rawValue)") } else { mixedDetails.append("marker absent") }

    return .mixedState(details: mixedDetails.joined(separator: ", "))
  }

  private struct KeyPresence {
    let hasKey: Bool
    let hasSalt: Bool
    var allPresent: Bool { hasKey && hasSalt }
    var allAbsent: Bool { !hasKey && !hasSalt }
  }

  private func checkRequiredKeys(for kind: MLSDatabaseKind, userDID: String) throws -> KeyPresence {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    switch kind {
    case .rustState:
      let keyAccount = "mls.encryption.key.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      let key = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: keyAccount)
      let exists = (key != nil)
      return KeyPresence(hasKey: exists, hasSalt: exists)

    case .swiftGRDB:
      let keyAccount = "mls.sqlcipher.db.key.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      let saltAccount = "mls.sqlcipher.db.salt.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      let hasKey = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: keyAccount) != nil
      let hasSalt = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: saltAccount) != nil
      return KeyPresence(hasKey: hasKey, hasSalt: hasSalt)
    }
  }

  // MARK: - Marker I/O

  func readMarker(for kind: MLSDatabaseKind, userDID: String) throws -> MLSInitializationRecord? {
    let url = try markerURL(for: kind, userDID: userDID)
    guard FileManager.default.fileExists(atPath: url.path) else { return nil }

    do {
      let data = try Data(contentsOf: url)
      let record = try JSONDecoder().decode(MLSInitializationRecord.self, from: data)
      return record
    } catch {
      throw MLSStorageInitializationError.unreadableState(details: "Failed to decode marker: \(error.localizedDescription)")
    }
  }

  func writeMarkerDirectlyForTesting(_ record: MLSInitializationRecord) throws {
    guard let kind = MLSDatabaseKind(rawValue: record.databaseKind) else { return }
    let url = try markerURL(for: kind, userDID: record.userDID)
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
    let data = try JSONEncoder().encode(record)
    try data.write(to: url, options: .atomic)
  }

  // MARK: - Leases & Mutexes

  func acquireAdmissionLease(for kind: MLSDatabaseKind, userDID: String) throws -> MLSLeaseToken {
    let url = try leaseURL(for: kind, userDID: userDID, type: "admission")
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

  func acquireExclusiveResetLease(for kind: MLSDatabaseKind, userDID: String) throws -> MLSLeaseToken {
    let url = try leaseURL(for: kind, userDID: userDID, type: "admission")
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

  func acquireMutationMutex(for kind: MLSDatabaseKind, userDID: String) throws -> MLSLeaseToken {
    let url = try leaseURL(for: kind, userDID: userDID, type: "mutation")
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

  // MARK: - Coordinated Open & Creation

  /// Authoritative single entry point for coordinating database open / creation according to
  /// the fail-closed OpenMLS 0.9 cross-process storage protocol.
  func coordinateOpen<T>(
    for kind: MLSDatabaseKind,
    userDID: String,
    createOrOpen: (_ attemptUUID: String, _ isFirstCreation: Bool) async throws -> T
  ) async throws -> T {
    _ = try MLSStoragePaths.requiredCleanContainerURL()
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()

    // 1. Process-local lock serialization
    let rLock = resourceLock(for: kind, userDID: normalizedDID)
    rLock.lock()
    defer { rLock.unlock() }

    // 2. Shared admission lease (retained across entire open/validation)
    let admissionLease = try acquireAdmissionLease(for: kind, userDID: normalizedDID)
    defer { admissionLease.release() }

    // 3. Pre-CAS evaluation under admission lease
    let preState = try evaluateStateUnderLease(for: kind, userDID: normalizedDID)

    switch preState {
    case .complete(let record):
      // Existing valid complete state: open and validate under the retained admission lease
      return try await createOrOpen(record.attemptUUID, false)

    case .incompleteAttempt(let record):
      // Restarted during creating state, or late arrival observing in-progress creation -> fail closed
      throw MLSStorageInitializationError.incompleteAttempt(
        details: "Restarted during creating attempt \(record.attemptUUID)"
      )

    case .mixedState(let details):
      throw MLSStorageInitializationError.mixedState(details: details)

    case .unreadableState(let details):
      throw MLSStorageInitializationError.unreadableState(details: details)

    case .allAbsent:
      break
    }

    // 4. Participant observed total absence: attempt atomic no-overwrite marker publish
    let expectedHash = try databasePathHash(for: kind, userDID: normalizedDID)
    let candidateAttemptUUID = UUID().uuidString
    let candidateRecord = MLSInitializationRecord(
      generationToken: MLSStoragePaths.generationToken,
      attemptUUID: candidateAttemptUUID,
      userDID: normalizedDID,
      databaseKind: kind.rawValue,
      databasePathHash: expectedHash,
      state: .creating
    )
    let markerURL = try self.markerURL(for: kind, userDID: normalizedDID)
    try FileManager.default.createDirectory(at: markerURL.deletingLastPathComponent(), withIntermediateDirectories: true)

    let candidateData = try JSONEncoder().encode(candidateRecord)
    let winningAttemptUUID: String

    do {
      // Atomic create-if-absent (fails if file already exists)
      try candidateData.write(to: markerURL, options: .withoutOverwriting)
      winningAttemptUUID = candidateAttemptUUID
      logger.info("📝 [StorageCoordinator] Published creating marker for \(kind.rawValue): \(normalizedDID.prefix(20), privacy: .private) (attempt \(winningAttemptUUID))")
    } catch let error as CocoaError where error.code == .fileWriteFileExists {
      // Lost CAS race to a concurrent total-absence entrant. Read and validate winning marker
      guard let winningData = try? Data(contentsOf: markerURL),
            let winningRecord = try? JSONDecoder().decode(MLSInitializationRecord.self, from: winningData)
      else {
        throw MLSStorageInitializationError.unreadableState(details: "Failed to read winning creating marker")
      }
      guard winningRecord.generationToken == MLSStoragePaths.generationToken,
            winningRecord.userDID == normalizedDID,
            winningRecord.databaseKind == kind.rawValue,
            winningRecord.databasePathHash == expectedHash,
            winningRecord.state == .creating || winningRecord.state == .complete
      else {
        throw MLSStorageInitializationError.invalidMarker(details: "Winning marker validation mismatch")
      }
      winningAttemptUUID = winningRecord.attemptUUID
      logger.info("🤝 [StorageCoordinator] Admitted entrant joined winning creating attempt \(winningAttemptUUID)")
    }

    // 5. Test barrier hook for deterministic concurrency testing
    testBarrierHook?()

    // 6. Keychain Add-only first-winner creation
    switch kind {
    case .rustState:
      let keyAccount = "mls.encryption.key.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      _ = try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: keyAccount, length: 32)

    case .swiftGRDB:
      let keyAccount = "mls.sqlcipher.db.key.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      let saltAccount = "mls.sqlcipher.db.salt.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      _ = try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: keyAccount, length: 32)
      _ = try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: saltAccount, length: 16)
    }

    // 7. Acquire mutation mutex while still holding admission lease
    let mutationMutex = try acquireMutationMutex(for: kind, userDID: normalizedDID)
    defer { mutationMutex.release() }

    // 8. Re-read/revalidate under mutation mutex
    guard let currentMarkerData = try? Data(contentsOf: markerURL),
          let currentMarker = try? JSONDecoder().decode(MLSInitializationRecord.self, from: currentMarkerData)
    else {
      throw MLSStorageInitializationError.unreadableState(details: "Marker missing or unreadable under mutation mutex")
    }

    guard currentMarker.generationToken == MLSStoragePaths.generationToken,
          currentMarker.userDID == normalizedDID,
          currentMarker.databaseKind == kind.rawValue,
          currentMarker.databasePathHash == expectedHash,
          currentMarker.attemptUUID == winningAttemptUUID
    else {
      throw MLSStorageInitializationError.invalidMarker(
        details: "Marker mismatch under mutation mutex (expected attempt \(winningAttemptUUID), found \(currentMarker.attemptUUID))"
      )
    }

    if currentMarker.state == .complete {
      // Peer on same winning attempt already completed creation
      return try await createOrOpen(winningAttemptUUID, false)
    } else if currentMarker.state == .creating {
      // First participant creates and validates DB
      let result = try await createOrOpen(winningAttemptUUID, true)

      // 9. Atomically complete marker under mutation mutex
      guard let recheckData = try? Data(contentsOf: markerURL),
            let recheckMarker = try? JSONDecoder().decode(MLSInitializationRecord.self, from: recheckData),
            recheckMarker.state == .creating,
            recheckMarker.attemptUUID == winningAttemptUUID
      else {
        throw MLSStorageInitializationError.invalidMarker(details: "Marker state changed before completion")
      }

      let completeRecord = MLSInitializationRecord(
        generationToken: MLSStoragePaths.generationToken,
        attemptUUID: winningAttemptUUID,
        userDID: normalizedDID,
        databaseKind: kind.rawValue,
        databasePathHash: expectedHash,
        state: .complete,
        createdAt: currentMarker.createdAt,
        completedAt: Date().timeIntervalSince1970
      )
      let completeData = try JSONEncoder().encode(completeRecord)
      let tempURL = markerURL.deletingLastPathComponent().appendingPathComponent("tmp_complete_\(winningAttemptUUID).json")
      try completeData.write(to: tempURL, options: .atomic)
      _ = try? FileManager.default.removeItem(at: markerURL)
      try FileManager.default.moveItem(at: tempURL, to: markerURL)

      logger.info("✅ [StorageCoordinator] Completed creation for \(kind.rawValue): \(normalizedDID.prefix(20), privacy: .private)")
      return result
    } else {
      throw MLSStorageInitializationError.invalidMarker(details: "Unknown marker state: \(currentMarker.state)")
    }
  }

  // MARK: - Coordinated Reset

  /// Authoritative single entry point for resetting a clean-generation resource set.
  /// Takes exclusive admission lease, then mutation mutex.
  /// Removes only new-generation DB/sidecars/quarantine and new-generation keys.
  /// Marker is removed ONLY after all deletions succeed.
  func coordinateReset(
    for kind: MLSDatabaseKind,
    userDID: String,
    customDelete: (() async throws -> Void)? = nil
  ) async throws {
    _ = try MLSStoragePaths.requiredCleanContainerURL()
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()

    let rLock = resourceLock(for: kind, userDID: normalizedDID)
    rLock.lock()
    defer { rLock.unlock() }

    let resetLease = try acquireExclusiveResetLease(for: kind, userDID: normalizedDID)
    defer { resetLease.release() }

    let mutationMutex = try acquireMutationMutex(for: kind, userDID: normalizedDID)
    defer { mutationMutex.release() }

    // 1. Delete clean database files and sidecars
    let dbURL = try databaseURL(for: kind, userDID: normalizedDID)
    let sidecars = [
      dbURL,
      URL(fileURLWithPath: dbURL.path + "-wal"),
      URL(fileURLWithPath: dbURL.path + "-shm"),
      URL(fileURLWithPath: dbURL.path + "-journal"),
      dbURL.appendingPathExtension("wal"),
      dbURL.appendingPathExtension("shm"),
      dbURL.appendingPathExtension("journal")
    ]
    for fileURL in sidecars {
      if FileManager.default.fileExists(atPath: fileURL.path) {
        try FileManager.default.removeItem(at: fileURL)
      }
    }

    // 2. Delete clean quarantine entries
    let quarantineDir = try quarantineDirectoryURL(for: kind)
    if let entries = try? FileManager.default.contentsOfDirectory(atPath: quarantineDir.path) {
      let filterTag: String
      switch kind {
      case .rustState:
        let didHash = normalizedDID.data(using: .utf8)?.base64EncodedString()
          .replacingOccurrences(of: "/", with: "_")
          .replacingOccurrences(of: "+", with: "-")
          .replacingOccurrences(of: "=", with: "")
          .prefix(64).description ?? "default"
        filterTag = String(didHash.prefix(16))
      case .swiftGRDB:
        filterTag = normalizedDID.replacingOccurrences(of: ":", with: "-")
      }
      for entry in entries where entry.contains(filterTag) {
        let entryURL = quarantineDir.appendingPathComponent(entry)
        try? FileManager.default.removeItem(at: entryURL)
      }
    }

    // 3. Delete clean Keychain keys
    switch kind {
    case .rustState:
      let keyAccount = "mls.encryption.key.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      try MLSKeychainManager.shared.delete(forKey: keyAccount)

    case .swiftGRDB:
      let keyAccount = "mls.sqlcipher.db.key.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      let saltAccount = "mls.sqlcipher.db.salt.\(normalizedDID).\(MLSStoragePaths.cleanSuffix)"
      try MLSKeychainManager.shared.delete(forKey: keyAccount)
      try MLSKeychainManager.shared.delete(forKey: saltAccount)
    }

    // 4. Custom deletion callback if provided
    if let customDelete {
      try await customDelete()
    }

    // 5. Remove marker file ONLY after all other deletions succeed
    let markerURL = try self.markerURL(for: kind, userDID: normalizedDID)
    if FileManager.default.fileExists(atPath: markerURL.path) {
      try FileManager.default.removeItem(at: markerURL)
    }

    logger.info("🗑️ [StorageCoordinator] Coordinated reset finished for \(kind.rawValue): \(normalizedDID.prefix(20), privacy: .private)")
  }
}
