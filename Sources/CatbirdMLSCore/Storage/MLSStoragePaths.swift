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
  static let generationToken = "mls-state-clean-v2-openmls-v09-r2"
  static let cleanSuffix = "clean-v2-openmls-v09-r2"
  static let cleanIdentifierSuffix = ".clean-v2-openmls-v09-r2"
  @inlinable
  public static func normalizeDID(_ did: String) -> String {
    did.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
  }

  static func sanitize(_ did: String) -> String {
    let normalized = did.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return normalized
      .replacingOccurrences(of: ":", with: "-")
      .replacingOccurrences(of: "/", with: "-")
      .replacingOccurrences(of: "#", with: "-")
      .replacingOccurrences(of: "?", with: "-")
  }

  public static func didHash(_ did: String) -> String {
    SHA256.hash(data: Data(normalizeDID(did).utf8)).map { String(format: "%02x", $0) }.joined()
  }

  static func quarantineTag(for userDID: String) -> String {
    didHash(userDID)
  }

  static func fileExistsStrict(at url: URL) throws -> Bool {
    var statBuf = stat()
    let result = lstat(url.path, &statBuf)
    if result == 0 {
      return true
    }
    if errno == ENOENT {
      return false
    }
    throw MLSStorageInitializationError.unreadableState(
      details: "Filesystem error accessing \(url.path): errno \(errno)"
    )
  }

  // MARK: - Centralized Account & Service Identifiers

  static func rustMEKAccount(for userDID: String) -> String {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return "mls.encryption.key.\(normalized).\(cleanSuffix)"
  }

  static func grdbKeyAccount(for userDID: String) -> String {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return "mls.sqlcipher.db.key.\(normalized).\(cleanSuffix)"
  }

  static func grdbSaltAccount(for userDID: String) -> String {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return "mls.sqlcipher.db.salt.\(normalized).\(cleanSuffix)"
  }

  static func contentRootAccount(for userDID: String) -> String {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return "mls.content.root.\(normalized).\(cleanSuffix)"
  }

  static func identityBackupAccount(for userDID: String) -> String {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return "mls.identity.backup.\(normalized).\(cleanSuffix)"
  }

  static func mlsDidAccount(for userDID: String) -> String {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return "mls.credential.mlsDid.\(normalized).\(cleanSuffix)"
  }

  static func deviceUuidAccount(for userDID: String) -> String {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return "mls.credential.deviceUuid.\(normalized).\(cleanSuffix)"
  }

  static func hybridSignerService(for userDID: String) -> String {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return "blue.catbird.mls.hybrid.\(normalized).\(cleanSuffix)"
  }

  static func hybridSignerSlot(key: String) -> String {
    if key.hasSuffix(cleanIdentifierSuffix) {
      return key
    }
    return "\(key)\(cleanIdentifierSuffix)"
  }

  static func orchestratorSignerIdentity(for userDID: String) -> String {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    if normalized.hasSuffix(cleanIdentifierSuffix) {
      return normalized
    }
    return "\(normalized).\(cleanSuffix)"
  }

  static func orchestratorSignerAccount(for userDID: String) -> String {
    let identity = orchestratorSignerIdentity(for: userDID)
    return "blue.catbird.mls.sig.\(identity)"
  }
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
  // MARK: - NSE Database Owner Mapping Contract

  public static func databaseOwnerMappingKey(forNormalizedDIDHash hash: String) -> String {
    "blue.catbird.mls.database-owner.\(hash.lowercased()).\(cleanSuffix)"
  }

  public static func databaseOwnerMappingKey(for userDID: String) -> String {
    let digest = SHA256.hash(data: Data(userDID.utf8)).compactMap { String(format: "%02x", $0) }.joined()
    return databaseOwnerMappingKey(forNormalizedDIDHash: digest)
  }

  /// Store or adopt canonical DID mapping for a database path hash in App Group defaults.
  static func publishDatabaseOwnerMapping(for userDID: String) throws {
    let key = databaseOwnerMappingKey(for: userDID)

    guard let defaults = UserDefaults(suiteName: appGroupIdentifier) else {
      throw MLSStorageInitializationError.appGroupUnavailable(appGroupIdentifier)
    }

    if let existing = defaults.string(forKey: key) {
      if existing == userDID {
        return
      }
      throw MLSStorageInitializationError.validationFailed(
        details: "Conflicting database owner mapping for \(userDID): expected \(userDID), found \(existing)"
      )
    } else if defaults.object(forKey: key) != nil {
      throw MLSStorageInitializationError.unreadableState(
        details: "Corrupt non-string database owner mapping for key: \(key)"
      )
    }

    defaults.set(userDID, forKey: key)
    defaults.synchronize()
  }

  /// Read-only validation of existing canonical DID mapping for a database path hash in App Group defaults.
  static func validateDatabaseOwnerMapping(for userDID: String) throws {
    let key = databaseOwnerMappingKey(for: userDID)

    guard let defaults = UserDefaults(suiteName: appGroupIdentifier) else {
      throw MLSStorageInitializationError.appGroupUnavailable(appGroupIdentifier)
    }

    guard let obj = defaults.object(forKey: key) else {
      throw MLSStorageInitializationError.validationFailed(
        details: "Missing database owner mapping for \(userDID)"
      )
    }

    guard let existing = obj as? String else {
      throw MLSStorageInitializationError.unreadableState(
        details: "Corrupt non-string database owner mapping for key: \(key)"
      )
    }

    guard existing == userDID else {
      throw MLSStorageInitializationError.validationFailed(
        details: "Conflicting database owner mapping for \(userDID): expected \(userDID), found \(existing)"
      )
    }
  }

  /// Resolve canonical DID from exact database path hash without domain enumeration.
  public static func resolveDatabaseOwnerDID(
    forNormalizedDIDHash hash: String,
    defaults: UserDefaults? = UserDefaults(suiteName: "group.blue.catbird.shared")
  ) throws -> String? {
    let key = databaseOwnerMappingKey(forNormalizedDIDHash: hash)
    guard let defaults else {
      throw MLSStorageInitializationError.appGroupUnavailable("group.blue.catbird.shared")
    }
    guard let obj = defaults.object(forKey: key) else {
      return nil
    }
    guard let canonicalDID = obj as? String else {
      throw MLSStorageInitializationError.unreadableState(
        details: "Corrupt non-string database owner mapping for hash: \(hash)"
      )
    }
    let computedDigest = SHA256.hash(data: Data(canonicalDID.utf8)).compactMap { String(format: "%02x", $0) }.joined()
    guard computedDigest == hash.lowercased() else {
      throw MLSStorageInitializationError.validationFailed(
        details: "Database owner mapping verification failed: hash mismatch for \(canonicalDID)"
      )
    }
    return canonicalDID
  }

  /// Remove exact database owner mapping on account reset.
  /// Removes BOTH the original (possibly mixed-case) DID key and the normalized key,
  /// since coordinateOpen publishes both. A delayed push must not resolve/recreate.
  static func removeDatabaseOwnerMapping(for userDID: String) throws {
    let normalized = normalizeDID(userDID)
    guard let defaults = UserDefaults(suiteName: appGroupIdentifier) else {
      throw MLSStorageInitializationError.appGroupUnavailable(appGroupIdentifier)
    }
    defaults.removeObject(forKey: databaseOwnerMappingKey(for: userDID))
    defaults.removeObject(forKey: databaseOwnerMappingKey(for: normalized))
    defaults.synchronize()
  }

  // MARK: - Cross-Kind Reset Sentinel

  /// Persistent cross-kind reset sentinel key (per normalized DID). Set by clearStorage
  /// before any Swift/Rust deletion and cleared only after Swift+Rust+checkpoint completion.
  private static func resetSentinelKey(for userDID: String) -> String {
    "mls.reset-sentinel.\(normalizeDID(userDID)).\(cleanSuffix)"
  }

  /// Set the cross-kind reset sentinel for a DID (start of clearStorage).
  static func setResetSentinel(for userDID: String) {
    guard let defaults = UserDefaults(suiteName: appGroupIdentifier) else { return }
    defaults.set(Date().timeIntervalSince1970, forKey: resetSentinelKey(for: userDID))
    defaults.synchronize()
  }

  /// Clear the cross-kind reset sentinel for a DID (only after Swift+Rust+checkpoint completion).
  static func clearResetSentinel(for userDID: String) {
    guard let defaults = UserDefaults(suiteName: appGroupIdentifier) else { return }
    defaults.removeObject(forKey: resetSentinelKey(for: userDID))
    defaults.synchronize()
  }

  /// Check whether a cross-kind reset is active for a DID.
  public static func isResetActive(for userDID: String) -> Bool {
    guard let defaults = UserDefaults(suiteName: appGroupIdentifier) else { return false }
    return defaults.object(forKey: resetSentinelKey(for: userDID)) != nil
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
  private struct AdmissionLeaseHolder: @unchecked Sendable {
    let token: MLSLeaseToken
    var refCount: Int
  }

  private let activeLeasesLock = NSLock()
  private var activeAdmissionLeases: [String: AdmissionLeaseHolder] = [:]

  private func activeLeaseKey(for kind: MLSDatabaseKind, userDID: String) -> String {
    "\(kind.rawValue)_\(MLSStoragePaths.normalizeDID(userDID))"
  }

  func retainAdmissionLease(_ lease: MLSLeaseToken, for kind: MLSDatabaseKind, userDID: String) {
    activeLeasesLock.lock()
    defer { activeLeasesLock.unlock() }
    let key = activeLeaseKey(for: kind, userDID: userDID)
    if var holder = activeAdmissionLeases[key] {
      holder.refCount += 1
      activeAdmissionLeases[key] = holder
      lease.release()
    } else {
      activeAdmissionLeases[key] = AdmissionLeaseHolder(token: lease, refCount: 1)
    }
  }

  func releaseAdmissionLease(for kind: MLSDatabaseKind, userDID: String) {
    activeLeasesLock.lock()
    defer { activeLeasesLock.unlock() }
    let key = activeLeaseKey(for: kind, userDID: userDID)
    guard var holder = activeAdmissionLeases[key] else { return }
    holder.refCount -= 1
    if holder.refCount <= 0 {
      activeAdmissionLeases.removeValue(forKey: key)
      holder.token.release()
    } else {
      activeAdmissionLeases[key] = holder
    }
  }

  func releaseAllAdmissionLeases(for userDID: String) {
    activeLeasesLock.lock()
    defer { activeLeasesLock.unlock() }
    for kind in MLSDatabaseKind.allCases {
      let key = activeLeaseKey(for: kind, userDID: userDID)
      if let holder = activeAdmissionLeases.removeValue(forKey: key) {
        holder.token.release()
      }
    }
  }

  func hasActiveAdmissionLease(for kind: MLSDatabaseKind, userDID: String) -> Bool {
    activeLeasesLock.lock()
    defer { activeLeasesLock.unlock() }
    let key = activeLeaseKey(for: kind, userDID: userDID)
    return activeAdmissionLeases[key] != nil
  }

  /// Seam for test barriers
  #if DEBUG
  private let testLock = NSLock()
  private var _testBarrierHook: (@Sendable () -> Void)?
  private var _testPrePublicationHook: (@Sendable () -> Void)?

  var testBarrierHook: (@Sendable () -> Void)? {
    get {
      testLock.lock()
      defer { testLock.unlock() }
      return _testBarrierHook
    }
    set {
      testLock.lock()
      defer { testLock.unlock() }
      _testBarrierHook = newValue
    }
  }

  var testPrePublicationHook: (@Sendable () -> Void)? {
    get {
      testLock.lock()
      defer { testLock.unlock() }
      return _testPrePublicationHook
    }
    set {
      testLock.lock()
      defer { testLock.unlock() }
      _testPrePublicationHook = newValue
    }
  }
  #endif

  private init() {}
  // MARK: - Paths & Identifiers

  func databaseURL(for kind: MLSDatabaseKind, userDID: String) throws -> URL {
    let digest = MLSStoragePaths.didHash(userDID)
    switch kind {
    case .rustState:
      return try MLSStoragePaths.rustDatabaseDirectory().appendingPathComponent("\(digest).db")

    case .swiftGRDB:
      return try MLSStoragePaths.grdbDatabaseDirectory().appendingPathComponent("mls_messages_\(digest).db")
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
    return markersDir.appendingPathComponent("marker_\(kind.rawValue)_\(didHash).json")
  }

  func leaseURL(for kind: MLSDatabaseKind, userDID: String, type: String) throws -> URL {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let didHash = try databasePathHash(for: kind, userDID: normalizedDID)
    let leasesDir = try MLSStoragePaths.coordinationDirectory().appendingPathComponent("leases", isDirectory: true)
    return leasesDir.appendingPathComponent("\(type)_\(kind.rawValue)_\(didHash).lease")
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
    let url = try leaseURL(for: kind, userDID: normalizedDID, type: "admission")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open admission lease file: \(errno)")
    }
    defer {
      flock(fd, LOCK_UN)
      close(fd)
    }

    if flock(fd, LOCK_SH) != 0 {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to acquire shared admission lease: \(errno)")
    }

    return try evaluateStateUnderLease(for: kind, userDID: normalizedDID)
  }

  private func evaluateStateUnderLease(for kind: MLSDatabaseKind, userDID: String) throws -> MLSStorageStateEvaluation {
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let dbURL = try databaseURL(for: kind, userDID: normalizedDID)
    let dbExists = try MLSStoragePaths.fileExistsStrict(at: dbURL)

    // Strict orphan sidecar detection: sidecar present without main DB is mixed state
    let sidecarExtensions = ["-wal", "-shm", "-journal", ".wal", ".shm", ".journal"]
    let hasSidecars = try sidecarExtensions.contains { ext in
      try MLSStoragePaths.fileExistsStrict(at: URL(fileURLWithPath: dbURL.path + ext))
    }
    if !dbExists && hasSidecars {
      return .mixedState(details: "Orphan database sidecars present without main database")
    }

    let keyState = try checkRequiredKeys(for: kind, userDID: normalizedDID)
    let marker = try readMarker(for: kind, userDID: normalizedDID)

    // Check owner mapping in App Group defaults
    let ownerKey = MLSStoragePaths.databaseOwnerMappingKey(for: userDID)
    guard let defaults = UserDefaults(suiteName: MLSStoragePaths.appGroupIdentifier) else {
      throw MLSStorageInitializationError.appGroupUnavailable(MLSStoragePaths.appGroupIdentifier)
    }
    let ownerObj = defaults.object(forKey: ownerKey)
    if let ownerObj {
      guard let existing = ownerObj as? String else {
        throw MLSStorageInitializationError.unreadableState(
          details: "Corrupt non-string database owner mapping for key: \(ownerKey)"
        )
      }
      guard existing == userDID else {
        return .mixedState(details: "Conflicting database owner mapping for \(userDID): found \(existing)")
      }
    }

    if !dbExists && keyState.allAbsent && marker == nil {
      if ownerObj != nil {
        let anyDBExists = MLSDatabaseKind.allCases.contains { otherKind in
          guard let url = try? databaseURL(for: otherKind, userDID: normalizedDID) else { return false }
          return (try? MLSStoragePaths.fileExistsStrict(at: url)) == true
        }
        let anyMarkerExists = MLSDatabaseKind.allCases.contains {
          (try? readMarker(for: $0, userDID: normalizedDID)) != nil
        }
        if !anyDBExists && !anyMarkerExists {
          return .mixedState(details: "Orphan database owner mapping present while database and keys are absent")
        }
      }
      if kind == .rustState {
        let contentRoot = try MLSKeychainManager.shared.retrieveKeyStrict(
          forKey: MLSStoragePaths.contentRootAccount(for: normalizedDID),
          service: "blue.catbird.mls.content",
          expectedLength: 32
        )
        let identityBackup = try MLSKeychainManager.shared.retrieveKeyStrict(
          forKey: MLSStoragePaths.identityBackupAccount(for: normalizedDID)
        )
        let mlsDid = try MLSKeychainManager.shared.retrieveKeyStrict(
          forKey: MLSStoragePaths.mlsDidAccount(for: normalizedDID)
        )
        let deviceUuid = try MLSKeychainManager.shared.retrieveKeyStrict(
          forKey: MLSStoragePaths.deviceUuidAccount(for: normalizedDID)
        )
        let orchestratorSigner = try MLSKeychainManager.shared.retrieveKeyStrict(
          forKey: MLSStoragePaths.orchestratorSignerAccount(for: normalizedDID),
          service: "blue.catbird.mls.signature"
        )
        let hasHybridKeys = try MLSKeychainManager.shared.hasAnyKeyStrict(
          forService: MLSStoragePaths.hybridSignerService(for: normalizedDID)
        )
        if contentRoot != nil || identityBackup != nil || mlsDid != nil || deviceUuid != nil || orchestratorSigner != nil || hasHybridKeys {
          return .mixedState(details: "Optional clean slots present while Rust DB, MEK, and content root are absent")
        }
      }
      return .allAbsent
    }

    if dbExists && keyState.allPresent, let marker, marker.state == .complete {
      guard ownerObj != nil else {
        return .mixedState(details: "Database and marker complete but database owner mapping is missing")
      }
      // Validate marker binding
      let expectedHash = try databasePathHash(for: kind, userDID: normalizedDID)
      guard marker.generationToken == MLSStoragePaths.generationToken,
            marker.userDID == normalizedDID,
            marker.databaseKind == kind.rawValue,
            marker.databasePathHash == expectedHash
      else {
        return .mixedState(details: "Marker binding mismatch for complete database")
      }

      if kind == .rustState {
        let mlsDid = try MLSKeychainManager.shared.retrieveKeyStrict(
          forKey: MLSStoragePaths.mlsDidAccount(for: normalizedDID)
        )
        let deviceUuid = try MLSKeychainManager.shared.retrieveKeyStrict(
          forKey: MLSStoragePaths.deviceUuidAccount(for: normalizedDID)
        )
        let orchestratorSigner = try MLSKeychainManager.shared.retrieveKeyStrict(
          forKey: MLSStoragePaths.orchestratorSignerAccount(for: normalizedDID),
          service: "blue.catbird.mls.signature"
        )
        let presentCount = (mlsDid != nil ? 1 : 0) + (deviceUuid != nil ? 1 : 0) + (orchestratorSigner != nil ? 1 : 0)
        if presentCount != 0 && presentCount != 3 {
          return .mixedState(
            details: "Partial clean credential slots present on complete reopen (mlsDid=\(mlsDid != nil), deviceUuid=\(deviceUuid != nil), signer=\(orchestratorSigner != nil))"
          )
        }
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
      if keyState.hasSalt { mixedDetails.append("contentRoot present") } else { mixedDetails.append("contentRoot absent") }
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
      let keyAccount = MLSStoragePaths.rustMEKAccount(for: normalizedDID)
      let contentRootAccount = MLSStoragePaths.contentRootAccount(for: normalizedDID)
      let hasKey = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: keyAccount, expectedLength: 32) != nil
      let hasContentRoot = try MLSKeychainManager.shared.retrieveKeyStrict(
        forKey: contentRootAccount,
        service: "blue.catbird.mls.content",
        expectedLength: 32
      ) != nil
      return KeyPresence(hasKey: hasKey, hasSalt: hasContentRoot)

    case .swiftGRDB:
      let keyAccount = MLSStoragePaths.grdbKeyAccount(for: normalizedDID)
      let saltAccount = MLSStoragePaths.grdbSaltAccount(for: normalizedDID)
      let hasKey = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: keyAccount, expectedLength: 32) != nil
      let hasSalt = try MLSKeychainManager.shared.retrieveKeyStrict(forKey: saltAccount, expectedLength: 16) != nil
      return KeyPresence(hasKey: hasKey, hasSalt: hasSalt)
    }
  }

  // MARK: - Marker I/O

  func readMarker(for kind: MLSDatabaseKind, userDID: String) throws -> MLSInitializationRecord? {
    let url = try markerURL(for: kind, userDID: userDID)
    guard try MLSStoragePaths.fileExistsStrict(at: url) else { return nil }

    do {
      let data = try Data(contentsOf: url)
      let record = try JSONDecoder().decode(MLSInitializationRecord.self, from: data)
      return record
    } catch {
      throw MLSStorageInitializationError.unreadableState(details: "Failed to decode marker: \(error.localizedDescription)")
    }
  }

  #if DEBUG
  func writeMarkerDirectlyForTesting(_ record: MLSInitializationRecord) throws {
    guard let kind = MLSDatabaseKind(rawValue: record.databaseKind) else {
      throw MLSStorageInitializationError.validationFailed(details: "Invalid database kind: \(record.databaseKind)")
    }
    let url = try markerURL(for: kind, userDID: record.userDID)
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
    let data = try JSONEncoder().encode(record)
    try data.write(to: url, options: .atomic)
  }
  #endif

  // MARK: - Leases & Mutexes

  private func acquireFlockAsync(fd: Int32, operation: Int32, timeoutSeconds: Double = 15.0) async throws {
    let startTime = ContinuousClock.now
    let deadline = startTime.advanced(by: .seconds(timeoutSeconds))
    let nbOp = operation | LOCK_NB

    while true {
      try Task.checkCancellation()

      if flock(fd, nbOp) == 0 {
        return
      }

      let err = errno
      if err != EWOULDBLOCK && err != EAGAIN && err != EINTR {
        throw MLSStorageInitializationError.admissionDenied(details: "flock failed with errno: \(err)")
      }

      if ContinuousClock.now >= deadline {
        throw MLSStorageInitializationError.admissionDenied(
          details: "Timed out waiting for lock (operation \(operation)) after \(timeoutSeconds)s"
        )
      }

      try await Task.sleep(for: .milliseconds(25))
    }
  }

  func acquireAdmissionLease(for kind: MLSDatabaseKind, userDID: String) async throws -> MLSLeaseToken {
    let url = try leaseURL(for: kind, userDID: userDID, type: "admission")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open admission lease file: \(errno)")
    }
    do {
      try await acquireFlockAsync(fd: fd, operation: LOCK_SH, timeoutSeconds: 15.0)
    } catch {
      close(fd)
      throw error
    }

    return MLSLeaseToken(fileDescriptor: fd, path: url.path)
  }

  func acquireExclusiveResetLease(for kind: MLSDatabaseKind, userDID: String) async throws -> MLSLeaseToken {
    let url = try leaseURL(for: kind, userDID: userDID, type: "admission")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open admission lease file for reset: \(errno)")
    }

    do {
      try await acquireFlockAsync(fd: fd, operation: LOCK_EX, timeoutSeconds: 15.0)
    } catch {
      close(fd)
      throw error
    }
    return MLSLeaseToken(fileDescriptor: fd, path: url.path)
  }

  /// Acquire an exclusive per-DID reset-lifecycle mutex.
  /// Held continuously from the start of clearStorage (before sentinel, drain, or admission locks)
  /// through full completion, preventing overlapping resets without forcibly releasing live handles.
  func acquireResetLifecycleMutex(for userDID: String) async throws -> MLSLeaseToken {
    let url = try leaseURL(for: .swiftGRDB, userDID: userDID, type: "reset-lifecycle")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open reset-lifecycle mutex file: \(errno)")
    }

    do {
      try await acquireFlockAsync(fd: fd, operation: LOCK_EX, timeoutSeconds: 15.0)
    } catch {
      close(fd)
      throw error
    }

    return MLSLeaseToken(fileDescriptor: fd, path: url.path)
  }

  /// Acquire a shared (read) per-DID reset-lifecycle lease.
  /// Held by epoch checkpoint writers through directory creation and atomic file write.
  /// Conflicts with the exclusive reset-lifecycle mutex held by clearStorage (Phase 0).
  func acquireSharedResetLifecycleLease(for userDID: String) async throws -> MLSLeaseToken {
    let url = try leaseURL(for: .swiftGRDB, userDID: userDID, type: "reset-lifecycle")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open reset-lifecycle lease file: \(errno)")
    }

    do {
      try await acquireFlockAsync(fd: fd, operation: LOCK_SH, timeoutSeconds: 15.0)
    } catch {
      close(fd)
      throw error
    }

    return MLSLeaseToken(fileDescriptor: fd, path: url.path)
  }

  /// Synchronous variant of acquireSharedResetLifecycleLease for non-async callers.
  func acquireSharedResetLifecycleLeaseSync(for userDID: String, timeoutSeconds: Double = 5.0) throws -> MLSLeaseToken {
    let url = try leaseURL(for: .swiftGRDB, userDID: userDID, type: "reset-lifecycle")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open reset-lifecycle lease file: \(errno)")
    }

    let startTime = ContinuousClock.now
    let deadline = startTime.advanced(by: .seconds(timeoutSeconds))
    while true {
      if flock(fd, LOCK_SH | LOCK_NB) == 0 {
        return MLSLeaseToken(fileDescriptor: fd, path: url.path)
      }
      let err = errno
      if err != EWOULDBLOCK && err != EAGAIN && err != EINTR {
        close(fd)
        throw MLSStorageInitializationError.admissionDenied(details: "flock shared reset-lifecycle failed with errno: \(err)")
      }
      if ContinuousClock.now >= deadline {
        close(fd)
        throw MLSStorageInitializationError.admissionDenied(details: "Timed out acquiring shared reset-lifecycle lease")
      }
      usleep(25000)
    }
  }

  /// Acquire cross-kind exclusive reset leases for both Swift GRDB and Rust state for a DID.
  /// Acquires exclusive admission locks on both database kinds simultaneously.
  func acquireCrossKindResetLease(for userDID: String) async throws -> [MLSDatabaseKind: MLSLeaseToken] {
    let normalized = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()

    let swiftLease = try await acquireExclusiveResetLease(for: .swiftGRDB, userDID: normalized)
    do {
      let rustLease = try await acquireExclusiveResetLease(for: .rustState, userDID: normalized)
      return [.swiftGRDB: swiftLease, .rustState: rustLease]
    } catch {
      swiftLease.release()
      throw error
    }
  }
  func acquireMutationMutex(for kind: MLSDatabaseKind, userDID: String) async throws -> MLSLeaseToken {
    let url = try leaseURL(for: kind, userDID: userDID, type: "mutation")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open mutation lock file: \(errno)")
    }

    do {
      try await acquireFlockAsync(fd: fd, operation: LOCK_EX, timeoutSeconds: 15.0)
    } catch {
      close(fd)
      throw error
    }

    return MLSLeaseToken(fileDescriptor: fd, path: url.path)
  }

  func acquireMutationMutexSync(for kind: MLSDatabaseKind, userDID: String, timeoutSeconds: Double = 15.0) throws -> MLSLeaseToken {
    let url = try leaseURL(for: kind, userDID: userDID, type: "mutation")
    try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)

    let fd = open(url.path, O_CREAT | O_RDWR, 0o666)
    guard fd >= 0 else {
      throw MLSStorageInitializationError.admissionDenied(details: "Failed to open mutation lock file: \(errno)")
    }

    let startTime = ContinuousClock.now
    let deadline = startTime.advanced(by: .seconds(timeoutSeconds))
    while true {
      if flock(fd, LOCK_EX | LOCK_NB) == 0 {
        return MLSLeaseToken(fileDescriptor: fd, path: url.path)
      }
      let err = errno
      if err != EWOULDBLOCK && err != EAGAIN && err != EINTR {
        close(fd)
        throw MLSStorageInitializationError.admissionDenied(details: "flock mutation failed with errno: \(err)")
      }
      if ContinuousClock.now >= deadline {
        close(fd)
        throw MLSStorageInitializationError.admissionDenied(details: "Timed out waiting for mutation lock")
      }
      usleep(25000)
    }
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

    // 1. Shared admission lease (retained across entire open/validation/completion/failure)
    let admissionLease = try await acquireAdmissionLease(for: kind, userDID: normalizedDID)
    var shouldRetainLease = false
    defer {
      if !shouldRetainLease {
        admissionLease.release()
      }
    }

    // 1b. Cross-kind reset sentinel: if a clearStorage is in progress (Swift or Rust
    //     phase), reject before any filesystem/key access. The exclusive reset owns and
    //     clears this sentinel only after Swift+Rust+checkpoint completion.
    if MLSStoragePaths.isResetActive(for: normalizedDID) {
      throw MLSStorageInitializationError.admissionDenied(
        details: "Cross-kind storage reset in progress for \(normalizedDID)"
      )
    }

    // 2. Pre-CAS evaluation under admission lease
    let preState = try evaluateStateUnderLease(for: kind, userDID: normalizedDID)

    switch preState {
    case .complete(let record):
      // Read-only exact-validate existing owner mapping BEFORE opening/validating database
      try MLSStoragePaths.validateDatabaseOwnerMapping(for: userDID)
      let result = try await createOrOpen(record.attemptUUID, false)
      shouldRetainLease = true
      retainAdmissionLease(admissionLease, for: kind, userDID: normalizedDID)
      return result

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

    // 3. Participant observed total absence: attempt atomic no-overwrite marker publish via link
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
    let markerDir = markerURL.deletingLastPathComponent()
    try FileManager.default.createDirectory(at: markerDir, withIntermediateDirectories: true)

    #if DEBUG
    testPrePublicationHook?()
    #endif

    let candidateData = try JSONEncoder().encode(candidateRecord)
    let pathHash = try databasePathHash(for: kind, userDID: normalizedDID)
    let tempPrefix = "tmp_\(kind.rawValue)_\(pathHash)_"
    let tempMarkerURL = markerDir.appendingPathComponent("\(tempPrefix)\(candidateAttemptUUID)_\(ProcessInfo.processInfo.processIdentifier).json")
    try candidateData.write(to: tempMarkerURL, options: .atomic)
    defer { try? FileManager.default.removeItem(at: tempMarkerURL) }

    let winningAttemptUUID: String
    let linkResult = link(tempMarkerURL.path, markerURL.path)
    if linkResult == 0 {
      winningAttemptUUID = candidateAttemptUUID
      logger.info("📝 [StorageCoordinator] Published creating marker for \(kind.rawValue): \(normalizedDID.prefix(20), privacy: .private) (attempt \(winningAttemptUUID))")
    } else if errno == EEXIST {
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
    } else {
      throw MLSStorageInitializationError.unreadableState(details: "Failed to create marker via link (errno: \(errno))")
    }

    #if DEBUG
    testBarrierHook?()
    #endif

    // 5. Keychain Add-only first-winner creation
    switch kind {
    case .rustState:
      let keyAccount = MLSStoragePaths.rustMEKAccount(for: normalizedDID)
      _ = try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: keyAccount, length: 32)
      let contentRootAccount = MLSStoragePaths.contentRootAccount(for: normalizedDID)
      _ = try MLSKeychainManager.shared.getOrCreateImmutableKey(
        forKey: contentRootAccount,
        service: "blue.catbird.mls.content",
        length: 32
      )

    case .swiftGRDB:
      let keyAccount = MLSStoragePaths.grdbKeyAccount(for: normalizedDID)
      let saltAccount = MLSStoragePaths.grdbSaltAccount(for: normalizedDID)
      _ = try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: keyAccount, length: 32)
      _ = try MLSKeychainManager.shared.getOrCreateImmutableKey(forKey: saltAccount, length: 16)
    }

    // 6. Acquire mutation mutex while still holding admission lease
    let mutationMutex = try await acquireMutationMutex(for: kind, userDID: normalizedDID)
    defer { mutationMutex.release() }

    // 7. Re-read/revalidate under mutation mutex
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

    let dbURL = try databaseURL(for: kind, userDID: normalizedDID)
    let dbExists = try MLSStoragePaths.fileExistsStrict(at: dbURL)
    let sidecarExtensions = ["-wal", "-shm", "-journal", ".wal", ".shm", ".journal"]
    let hasSidecars = try sidecarExtensions.contains { ext in
      try MLSStoragePaths.fileExistsStrict(at: URL(fileURLWithPath: dbURL.path + ext))
    }

    if currentMarker.state == .complete {
      // Peer on same winning attempt already completed creation
      guard dbExists else {
        throw MLSStorageInitializationError.mixedState(details: "Complete marker found but database file missing")
      }
      try MLSStoragePaths.validateDatabaseOwnerMapping(for: userDID)
      let result = try await createOrOpen(winningAttemptUUID, false)
      shouldRetainLease = true
      retainAdmissionLease(admissionLease, for: kind, userDID: normalizedDID)
      return result
    }

    guard currentMarker.state == .creating else {
      throw MLSStorageInitializationError.invalidMarker(details: "Unknown marker state: \(currentMarker.state)")
    }

    // Database and sidecars must still be absent before first creation
    if dbExists || hasSidecars {
      throw MLSStorageInitializationError.mixedState(details: "Partial database or sidecars already exist before creation completed")
    }
    // First participant creates and validates DB
    let result = try await createOrOpen(winningAttemptUUID, true)

    // 8. Strictly prove DB exists and required keys remain valid before completing marker
    guard try MLSStoragePaths.fileExistsStrict(at: dbURL) else {
      throw MLSStorageInitializationError.validationFailed(details: "Database file was not created on disk")
    }
    let keyPresence = try checkRequiredKeys(for: kind, userDID: normalizedDID)
    guard keyPresence.allPresent else {
      throw MLSStorageInitializationError.validationFailed(details: "Required keys missing after database creation")
    }

    try MLSStoragePaths.publishDatabaseOwnerMapping(for: normalizedDID)
    try MLSStoragePaths.publishDatabaseOwnerMapping(for: userDID)
    // 10. Atomically complete marker under mutation mutex
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
    try completeData.write(to: markerURL, options: .atomic)

    logger.info("✅ [StorageCoordinator] Completed creation for \(kind.rawValue): \(normalizedDID.prefix(20), privacy: .private)")
    shouldRetainLease = true
    retainAdmissionLease(admissionLease, for: kind, userDID: normalizedDID)
    return result
  }

  // MARK: - Coordinated Reset

  /// Authoritative single entry point for resetting a clean-generation resource set.
  /// Takes exclusive admission lease, then mutation mutex.
  /// Removes only new-generation DB/sidecars/quarantine and new-generation keys.
  /// Marker is removed ONLY after all deletions succeed.
  func coordinateReset(
    for kind: MLSDatabaseKind,
    userDID: String,
    originalUserDID: String? = nil,
    heldResetLease: MLSLeaseToken? = nil,
    customDelete: (() async throws -> Void)? = nil
  ) async throws {
    _ = try MLSStoragePaths.requiredCleanContainerURL()
    let normalizedDID = userDID.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let origDID = originalUserDID ?? userDID

    let resetLease: MLSLeaseToken?
    if heldResetLease == nil {
      resetLease = try await acquireExclusiveResetLease(for: kind, userDID: normalizedDID)
    } else {
      resetLease = nil
    }
    defer { resetLease?.release() }

    let mutationMutex = try await acquireMutationMutex(for: kind, userDID: normalizedDID)
    defer { mutationMutex.release() }

    // Ownership check before deletion: if marker exists, verify it matches normalizedDID
    let markerURL = try self.markerURL(for: kind, userDID: normalizedDID)
    let expectedHash = try databasePathHash(for: kind, userDID: normalizedDID)
    if let marker = try readMarker(for: kind, userDID: normalizedDID) {
      guard marker.userDID == normalizedDID && marker.databasePathHash == expectedHash else {
        throw MLSStorageInitializationError.validationFailed(details: "Marker binding mismatch: refusing to delete unowned database URL")
      }
    }
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
      if try MLSStoragePaths.fileExistsStrict(at: fileURL) {
        try FileManager.default.removeItem(at: fileURL)
      }
    }

    // 2. Delete clean quarantine entries matching exact DID quarantine tag prefix
    let quarantineDir = try quarantineDirectoryURL(for: kind)
    if try MLSStoragePaths.fileExistsStrict(at: quarantineDir) {
      let entries = try FileManager.default.contentsOfDirectory(atPath: quarantineDir.path)
      let filterTag = MLSStoragePaths.quarantineTag(for: normalizedDID)
      let targetPrefix = "quarantine_\(kind.rawValue)_\(filterTag)_"
      for entry in entries where entry.hasPrefix(targetPrefix) || entry == filterTag || entry.hasPrefix("\(filterTag)_") || entry.hasSuffix("_\(filterTag)") {
        let entryURL = quarantineDir.appendingPathComponent(entry)
        try FileManager.default.removeItem(at: entryURL)
      }
    }

    // 3. Delete clean marker CAS temp files matching exact pathHash
    let markerDir = markerURL.deletingLastPathComponent()
    let tempPrefix = "tmp_\(kind.rawValue)_\(expectedHash)_"
    if try MLSStoragePaths.fileExistsStrict(at: markerDir) {
      let markerEntries = try FileManager.default.contentsOfDirectory(atPath: markerDir.path)
      for entry in markerEntries where entry.hasPrefix(tempPrefix) {
        let tempURL = markerDir.appendingPathComponent(entry)
        try FileManager.default.removeItem(at: tempURL)
      }
    }

    // 4. Delete clean Keychain keys
    switch kind {
    case .rustState:
      let keyAccount = MLSStoragePaths.rustMEKAccount(for: normalizedDID)
      try MLSKeychainManager.shared.deleteStrict(forKey: keyAccount)
      let contentRootAccount = MLSStoragePaths.contentRootAccount(for: normalizedDID)
      try MLSKeychainManager.shared.deleteStrict(forKey: contentRootAccount, service: "blue.catbird.mls.content")
      let identityBackup = MLSStoragePaths.identityBackupAccount(for: normalizedDID)
      try MLSKeychainManager.shared.deleteStrict(forKey: identityBackup)
      let mlsDidAccount = MLSStoragePaths.mlsDidAccount(for: normalizedDID)
      try MLSKeychainManager.shared.deleteStrict(forKey: mlsDidAccount)
      let deviceUuidAccount = MLSStoragePaths.deviceUuidAccount(for: normalizedDID)
      try MLSKeychainManager.shared.deleteStrict(forKey: deviceUuidAccount)
      let orchestratorSigner = MLSStoragePaths.orchestratorSignerAccount(for: normalizedDID)
      try MLSKeychainManager.shared.deleteStrict(forKey: orchestratorSigner, service: "blue.catbird.mls.signature")
      let hybridService = MLSStoragePaths.hybridSignerService(for: normalizedDID)
      try MLSKeychainManager.shared.deleteAllStrict(forService: hybridService)

    case .swiftGRDB:
      let keyAccount = MLSStoragePaths.grdbKeyAccount(for: normalizedDID)
      let saltAccount = MLSStoragePaths.grdbSaltAccount(for: normalizedDID)
      try MLSKeychainManager.shared.deleteStrict(forKey: keyAccount)
      try MLSKeychainManager.shared.deleteStrict(forKey: saltAccount)
    }

    // 5. Custom deletion callback if provided
    if let customDelete {
      try await customDelete()
    }
    // 6. Remove NSE database owner mapping before removing marker (both original and normalized)
    try MLSStoragePaths.removeDatabaseOwnerMapping(for: origDID)
    try MLSStoragePaths.removeDatabaseOwnerMapping(for: userDID)

    // 7. Clean handshake store entries for this user
    try MLSAppGroupHandshakeStore.shared.clearAll(for: normalizedDID)

    // 8. Remove clean welcome gate markers for this user
    try await MLSWelcomeGate.shared.clearAll(for: normalizedDID)
    try await MLSEpochCheckpoint.shared.clearAllCheckpoints(userDID: normalizedDID)
    // 10. Remove marker file ONLY after all other deletions succeed
    if try MLSStoragePaths.fileExistsStrict(at: markerURL) {
      try FileManager.default.removeItem(at: markerURL)
    }

    // 11. Only after the reset reaches a clean authority/marker state, resume writes.
    //     If any step above threw, we remain suspended and the reset fails closed.
    await MLSEpochCheckpoint.shared.resumeWrites(userDID: normalizedDID)
  }
}
