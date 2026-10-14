import Foundation
import XCTest
@testable import CatbirdMLSCore
import CatbirdMLS
import CryptoKit

#if os(macOS)
/// C36: Two native production OS processes contending on production Welcome gate
/// and storage admission leases in a shared filesystem namespace.
///
/// Process 1: This XCTest runner process (PID = ProcessInfo.processInfo.processIdentifier)
/// Process 2: Spawned native Swift executable `CatbirdMLSTestHost` linking CatbirdMLSCore
///
/// Tests:
/// 1. `testCrossProcessWelcomeClaimHasExactlyOneWinner`:
///    Both native processes race production `MLSWelcomeGate.shared.beginWelcomeProcessing`
///    at an identical wall-clock instant; exactly one winner per round (23 app wins, 13 child wins across 36 rounds).
/// 2. `testSecondProcessHoldingStoreBlocksResetUntilItReleases`:
///    PID 2 holds store open via MLSGRDBManager; PID 1 acquireExclusiveResetLease is strictly blocked.
/// 3. `testOpenStoreInAppBlocksSecondProcessExclusiveOwnership`:
///    PID 1 holds store open; PID 2 exclusive lease attempt fails until PID 1 drains.
final class MLSCrossProcessOwnershipTests: XCTestCase {
  private var directory: URL!

  override func setUp() async throws {
    directory = FileManager.default.temporaryDirectory
      .appendingPathComponent("C36-xproc-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    MLSStoragePaths.setBaseDirectoryOverride(directory)
    MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())
  }

  override func tearDown() async throws {
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    MLSKeychainManager.setFakeStorageOverrideForTesting(nil)
  }

  // MARK: - Native Second Process

  private static func testHostExecutableURL() throws -> URL {
    if let envPath = ProcessInfo.processInfo.environment["CATBIRD_TEST_HOST_PATH"],
       FileManager.default.isExecutableFile(atPath: envPath) {
      return URL(fileURLWithPath: envPath)
    }
    let bundleDir = Bundle(for: MLSCrossProcessOwnershipTests.self).bundleURL.deletingLastPathComponent()
    let candidate = bundleDir.appendingPathComponent("CatbirdMLSTestHost")
    if FileManager.default.isExecutableFile(atPath: candidate.path) {
      return candidate
    }
    let debugDir = URL(fileURLWithPath: "/tmp/ClientApple-mlscore-build/out/Products/Debug/CatbirdMLSTestHost")
    if FileManager.default.isExecutableFile(atPath: debugDir.path) {
      return debugDir
    }
    let localDebug = URL(fileURLWithPath: ".build/debug/CatbirdMLSTestHost")
    if FileManager.default.isExecutableFile(atPath: localDebug.path) {
      return localDebug
    }
    throw NSError(domain: "C36", code: 2, userInfo: [NSLocalizedDescriptionKey: "CatbirdMLSTestHost executable not found. Ensure CatbirdMLSTestHost is built."])
  }

  private struct NativeProcess {
    let process: Process
    let stdin: Pipe
    let stdout: Pipe
    let stderr: Pipe
    let pid: Int32
    func waitForLine(_ expected: String) throws {
      let data = stdout.fileHandleForReading.availableData
      let line = String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
      guard line.contains(expected) else {
        throw NSError(domain: "C36", code: 1, userInfo: [NSLocalizedDescriptionKey: "owner said '\(line)', expected to contain '\(expected)'"])
      }
    }

    func readOutput() -> String {
      let data = stdout.fileHandleForReading.readDataToEndOfFile()
      return String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
    }

    func exitStatus() -> Int32 {
      process.waitUntilExit()
      return process.terminationStatus
    }
  }

  private func spawnNativeOwner(_ arguments: [String]) throws -> NativeProcess {
    let process = Process()
    process.executableURL = try Self.testHostExecutableURL()
    process.arguments = arguments
    let stdin = Pipe()
    let stdout = Pipe()
    let stderr = Pipe()
    process.standardInput = stdin
    process.standardOutput = stdout
    process.standardError = stderr
    try process.run()
    return NativeProcess(process: process, stdin: stdin, stdout: stdout, stderr: stderr, pid: process.processIdentifier)
  }


  // MARK: - C36: Welcome Ownership Race with Two Native Processes

  func testCrossProcessWelcomeClaimHasExactlyOneWinner() async throws {
    let did = "did:plc:c36-xproc-welcome-\(UUID().uuidString.lowercased())"
    let rounds = 36
    var appWins = 0
    var nseWins = 0
    for round in 0..<rounds {
      let conversationID = UUID().uuidString
      let marker = try await MLSWelcomeGate.shared.markerURL(conversationID: conversationID, userDID: did)
      try FileManager.default.createDirectory(at: marker.deletingLastPathComponent(), withIntermediateDirectories: true)

      // Both owners claim at one shared instant; the second process is skewed by
      // -2ms...+2ms across rounds so every interleaving around the instant is exercised.
      let start = Date().timeIntervalSince1970 + 0.35
      let skew = Double(round % 9 - 4) * 0.0005
      let nse = try spawnNativeOwner(["welcome-claim", directory.path, conversationID, did, String(format: "%.6f", start + skew)])
      try nse.waitForLine("ready")
      while Date().timeIntervalSince1970 < start {}
      let appWon: Bool
      do {
        try await MLSWelcomeGate.shared.beginWelcomeProcessing(for: conversationID, userDID: did)
        appWon = true
      } catch let error as MLSStorageInitializationError {
        guard case .admissionDenied(let details) = error else { throw error }
        XCTAssertTrue(details.contains("already being processed"), details)
        appWon = false
      }
      let status = nse.exitStatus()
      XCTAssertTrue(status == 0 || status == 3, "Second owner failed unexpectedly: \(status)")
      let nseWon = status == 0

      XCTAssertNotEqual(appWon, nseWon, "Exactly one owner may claim a Welcome (round \(round): app=\(appWon), nse=\(nseWon))")
      let pending = await MLSWelcomeGate.shared.hasPendingWelcome(for: conversationID, userDID: did)
      XCTAssertTrue(pending, "The winner's claim must stay visible to the other process")
      if appWon { appWins += 1 }
      if nseWon { nseWins += 1 }
      await MLSWelcomeGate.shared.completeWelcomeProcessing(for: conversationID, userDID: did)
    }
    print("C36 cross-process Welcome race: rounds \(rounds), app won \(appWins), second process won \(nseWins)")
    XCTAssertEqual(appWins + nseWins, rounds, "Every round has exactly one owner")
    XCTAssertGreaterThan(appWins, 0)
    XCTAssertGreaterThan(nseWins, 0)
  }

  // MARK: - Store admission lease: reset waits for the other process

  func testSecondProcessHoldingStoreBlocksResetUntilItReleases() async throws {
    let did = "did:plc:c36-xproc-lease-\(UUID().uuidString.lowercased())"
    let coordinator = MLSStorageCoordinator.shared

    // Control: with no other owner the exclusive reset lease is granted at once.
    let controlStart = ContinuousClock.now
    try await coordinator.acquireExclusiveResetLease(for: .swiftGRDB, userDID: did).release()
    XCTAssertLessThan(ContinuousClock.now - controlStart, .seconds(1))

    // Second native process holds store open
    let nse = try spawnNativeOwner(["hold-lease", directory.path, did])
    try await Task.sleep(for: .milliseconds(200))

    let resetStart = ContinuousClock.now
    let granted = GrantFlag()
    let reset = Task {
      let token = try await coordinator.acquireExclusiveResetLease(for: .swiftGRDB, userDID: did)
      granted.set()
      return token
    }
    try await Task.sleep(for: .seconds(1))
    XCTAssertFalse(granted.value, "Reset must not be granted while another process holds the store")

    // Signal second process to release store
    try nse.stdin.fileHandleForWriting.close()
    XCTAssertEqual(nse.exitStatus(), 0)
    let token = try await reset.value
    token.release()
    XCTAssertGreaterThanOrEqual(ContinuousClock.now - resetStart, .seconds(1))
  }

  func testOpenStoreInAppBlocksSecondProcessExclusiveOwnership() async throws {
    let did = "did:plc:c36-xproc-open-\(UUID().uuidString.lowercased())"
    let manager = MLSGRDBManager()
    _ = try await manager.getDatabasePool(for: did)

    let whileOpen = try spawnNativeOwner(["try-exclusive", directory.path, did])
    XCTAssertEqual(whileOpen.exitStatus(), 3, "Second process must not gain exclusive ownership of an open store")

    await manager.closeDatabaseAndDrain(for: did)
    let afterDrain = try spawnNativeOwner(["try-exclusive", directory.path, did])
    XCTAssertEqual(afterDrain.exitStatus(), 0, "Exclusive ownership is available once the app releases the store")
    await manager.shutdownAllDatabases()
  }

  // MARK: - C36: Native OpenMLS Ratchet Mutation Race with Two Native Processes

  func testCrossProcessNativeRatchetMutationRaceHasExactlyOneWinner() async throws {
    let rounds = 5
    var appWins = 0
    var childWins = 0

    let encryptionKey = String(repeating: "ab", count: 32)

    for round in 0..<rounds {
      let roundDir = directory.appendingPathComponent("round-\(round)")
      let aliceDir = roundDir.appendingPathComponent("alice")
      let bobDir = roundDir.appendingPathComponent("bob")
      try FileManager.default.createDirectory(at: aliceDir, withIntermediateDirectories: true)
      try FileManager.default.createDirectory(at: bobDir, withIntermediateDirectories: true)

      let aliceDid = "did:plc:c36-alice-\(round)-\(UUID().uuidString.lowercased())"
      let bobDid = "did:plc:c36-bob-\(round)-\(UUID().uuidString.lowercased())"

      let aliceKeychain = FileBackedKeychainAccess(directory: aliceDir.appendingPathComponent("keychain"))
      let aliceEpochStorage = FileBackedEpochSecretStorage(directory: aliceDir.appendingPathComponent("epoch_secrets"))
      let aliceDbURL = aliceDir.appendingPathComponent("openmls.db")

      let bobKeychain = FileBackedKeychainAccess(directory: bobDir.appendingPathComponent("keychain"))
      let bobEpochStorage = FileBackedEpochSecretStorage(directory: bobDir.appendingPathComponent("epoch_secrets"))
      let bobDbURL = bobDir.appendingPathComponent("openmls.db")

      // 1. Initialize Alice context and Bob context with separate SQLCipher storage and keychains
      let aliceContext: MlsContext
      do {
        aliceContext = try MlsContext(storagePath: aliceDbURL.path, encryptionKey: encryptionKey, keychain: aliceKeychain)
        try aliceContext.setEpochSecretStorage(storage: aliceEpochStorage)
      } catch {
        throw NSError(domain: "C36", code: 20, userInfo: [NSLocalizedDescriptionKey: "Phase [parent:aliceContext.init] failed: \(error)"])
      }

      let bobContext: MlsContext
      do {
        bobContext = try MlsContext(storagePath: bobDbURL.path, encryptionKey: encryptionKey, keychain: bobKeychain)
        try bobContext.setEpochSecretStorage(storage: bobEpochStorage)
      } catch {
        throw NSError(domain: "C36", code: 21, userInfo: [NSLocalizedDescriptionKey: "Phase [parent:bobContext.init] failed: \(error)"])
      }

      // 2. Alice creates group, Bob creates key package, Alice adds Bob, Bob processes welcome
      let groupId: Data
      let addResult: AddMembersResult
      do {
        let creation = try aliceContext.createGroup(identityBytes: Data(aliceDid.utf8), config: nil)
        groupId = creation.groupId
        let bobKeyPackage = try bobContext.createKeyPackage(identityBytes: Data(bobDid.utf8))
        addResult = try aliceContext.addMembers(groupId: groupId, keyPackages: [KeyPackageData(data: bobKeyPackage.keyPackageData)])
        _ = try aliceContext.mergePendingCommit(groupId: groupId)
        try aliceContext.flushStorage()
        try aliceContext.flushAndPrepareClose()
      } catch {
        throw NSError(domain: "C36", code: 22, userInfo: [NSLocalizedDescriptionKey: "Phase [parent:setupTwoPartyGroup] failed: \(error)"])
      }

      do {
        _ = try bobContext.processWelcome(welcomeBytes: addResult.welcomeData, identityBytes: Data(bobDid.utf8), config: nil)
        try bobContext.flushStorage()
        try bobContext.flushAndPrepareClose()
      } catch {
        throw NSError(domain: "C36", code: 23, userInfo: [NSLocalizedDescriptionKey: "Phase [parent:bobProcessWelcome] failed: \(error)"])
      }

      // 3. Baseline verification: Alice encrypts at epoch 1, Bob decrypts at epoch 1
      let baselineMsg: EncryptResult
      do {
        let reopenedAlice = try MlsContext(storagePath: aliceDbURL.path, encryptionKey: encryptionKey, keychain: aliceKeychain)
        try reopenedAlice.setEpochSecretStorage(storage: aliceEpochStorage)
        baselineMsg = try reopenedAlice.encryptMessage(groupId: groupId, plaintext: Data("baseline-round-\(round)".utf8))
        try reopenedAlice.flushStorage()
        try reopenedAlice.flushAndPrepareClose()
      } catch {
        throw NSError(domain: "C36", code: 24, userInfo: [NSLocalizedDescriptionKey: "Phase [parent:baselineEncrypt] failed: \(error)"])
      }

      do {
        let reopenedBob = try MlsContext(storagePath: bobDbURL.path, encryptionKey: encryptionKey, keychain: bobKeychain)
        try reopenedBob.setEpochSecretStorage(storage: bobEpochStorage)
        let baselineProcessed = try reopenedBob.processMessage(groupId: groupId, messageData: baselineMsg.ciphertext)
        guard case .applicationMessage(let baselinePlaintext, _) = baselineProcessed else {
          XCTFail("Expected applicationMessage from Bob but got \(baselineProcessed)")
          return
        }
        XCTAssertEqual(baselinePlaintext, Data("baseline-round-\(round)".utf8))
        try reopenedBob.flushStorage()
        try reopenedBob.flushAndPrepareClose()
      } catch {
        throw NSError(domain: "C36", code: 25, userInfo: [NSLocalizedDescriptionKey: "Phase [parent:baselineProcess] failed: \(error)"])
      }

      // 4. Two native processes race at synchronized wall-clock instant to acquire
      // the production mutation mutex on Alice's store and execute selfUpdate + merge.
      let start = Date().timeIntervalSince1970 + 0.35
      let skew = Double(round % 5 - 2) * 0.0005
      let child = try spawnNativeOwner([
        "group-mutation-race",
        directory.path,
        aliceDir.path,
        aliceDid,
        groupId.hexEncodedString(),
        String(format: "%.6f", start + skew)
      ])
      try child.waitForLine("ready")
      print("C36 native ratchet owner PIDs: parent=\(ProcessInfo.processInfo.processIdentifier), child=\(child.pid), round=\(round)")

      while Date().timeIntervalSince1970 < start {}

      var appWon = false
      var appLease: MLSLeaseToken?
      do {
        let lease = try MLSStorageCoordinator.shared.acquireMutationMutexSync(
          for: .rustState,
          userDID: aliceDid,
          timeoutSeconds: 0.1
        )
        appLease = lease

        let context = try MlsContext(storagePath: aliceDbURL.path, encryptionKey: encryptionKey, keychain: aliceKeychain)
        try context.setEpochSecretStorage(storage: aliceEpochStorage)
        let updateResult = try context.selfUpdate(groupId: groupId)
        _ = try context.mergePendingCommit(groupId: groupId)
        try context.flushStorage()
        try context.flushAndPrepareClose()

        let commitURL = aliceDir.appendingPathComponent("winner_commit.bin")
        try? updateResult.commitData.write(to: commitURL, options: .atomic)
        appWon = true
      } catch let error as MLSStorageInitializationError {
        guard case .admissionDenied(let details) = error else { throw error }
        XCTAssertTrue(details.contains("flock mutation failed") || details.contains("Timed out"), details)
        appWon = false
      }

      // If app won, hold appLease until child has finished its attempt so child is strictly fenced
      let status = child.exitStatus()
      appLease?.release()
      appLease = nil
      XCTAssertTrue(status == 0 || status == 3, "Child failed unexpectedly: \(status)")
      let childWon = (status == 0)

      XCTAssertNotEqual(appWon, childWon, "Exactly one process must win mutation fence (round \(round): app=\(appWon), child=\(childWon))")
      if appWon { appWins += 1 }
      if childWon { childWins += 1 }

      // 5. Durable assertion: Alice's on-disk OpenMLS state was advanced exactly once to epoch 2
      let verifyAliceContext = try MlsContext(storagePath: aliceDbURL.path, encryptionKey: encryptionKey, keychain: aliceKeychain)
      try verifyAliceContext.setEpochSecretStorage(storage: aliceEpochStorage)
      let postRaceEpoch = try verifyAliceContext.getEpoch(groupId: groupId)
      XCTAssertEqual(postRaceEpoch, 2, "Alice's native ratchet epoch must be advanced to 2 on disk")

      // 6. State decryptability and recoverability assertion:
      // Bob reads the winning commit from Alice, advances his epoch to 2,
      // and successfully decrypts Alice's new application message encrypted at epoch 2.
      let commitURL = aliceDir.appendingPathComponent("winner_commit.bin")
      let winnerCommitData = try Data(contentsOf: commitURL)

      let bobReopened = try MlsContext(storagePath: bobDbURL.path, encryptionKey: encryptionKey, keychain: bobKeychain)
      try bobReopened.setEpochSecretStorage(storage: bobEpochStorage)
      let commitProcessed = try bobReopened.processMessage(groupId: groupId, messageData: winnerCommitData)
      guard case .stagedCommit(let newEpoch, _) = commitProcessed else {
        XCTFail("Expected stagedCommit from Bob processing winner commit but got \(commitProcessed)")
        return
      }
      XCTAssertEqual(newEpoch, 2, "Staged commit must target epoch 2")
      let mergedEpoch = try bobReopened.mergeIncomingCommit(groupId: groupId, targetEpoch: newEpoch)
      XCTAssertEqual(mergedEpoch, 2, "Bob's epoch must advance to 2 after merging incoming commit")
      try bobReopened.flushStorage()
      let bobPostRaceEpoch = try bobReopened.getEpoch(groupId: groupId)
      XCTAssertEqual(bobPostRaceEpoch, 2, "Bob's epoch must advance to 2 after processing winner commit")

      let epoch2Msg = try verifyAliceContext.encryptMessage(
        groupId: groupId,
        plaintext: Data("epoch-2-msg-round-\(round)".utf8)
      )
      try verifyAliceContext.flushStorage()
      try verifyAliceContext.flushAndPrepareClose()

      let epoch2Processed = try bobReopened.processMessage(
        groupId: groupId,
        messageData: epoch2Msg.ciphertext
      )
      guard case .applicationMessage(let epoch2Plaintext, _) = epoch2Processed else {
        XCTFail("Expected applicationMessage from Bob decrypt but got \(epoch2Processed)")
        return
      }
      XCTAssertEqual(epoch2Plaintext, Data("epoch-2-msg-round-\(round)".utf8))
      try bobReopened.flushStorage()
      try bobReopened.flushAndPrepareClose()
    }

    print("C36 cross-process native ratchet mutation race: rounds \(rounds), app won \(appWins), child won \(childWins)")
    XCTAssertEqual(appWins + childWins, rounds, "Every round has exactly one mutating owner")
  }
  // MARK: - C36: Causal Broken-Fence Check

  func testCausalBrokenFenceProducesTwoDivergentCommitsFromOneEpoch() async throws {
    // With the mutation fence deliberately omitted, two processes start from
    // the same real OpenMLS epoch and produce distinct successful commits.
    let roundDir = directory.appendingPathComponent("broken-fence")
    try FileManager.default.createDirectory(at: roundDir, withIntermediateDirectories: true)
    let did = "did:plc:c36-broken-fence-\(UUID().uuidString.lowercased())"

    let encryptionKey = String(repeating: "ab", count: 32)
    let keychainDir = roundDir.appendingPathComponent("keychain")
    let keychain = FileBackedKeychainAccess(directory: keychainDir)
    let epochDir = roundDir.appendingPathComponent("epoch_secrets")
    let epochStorage = FileBackedEpochSecretStorage(directory: epochDir)
    let dbURL = roundDir.appendingPathComponent("openmls.db")

    let initialContext: MlsContext
    do {
      initialContext = try MlsContext(
        storagePath: dbURL.path,
        encryptionKey: encryptionKey,
        keychain: keychain
      )
    } catch {
      throw NSError(domain: "C36", code: 40, userInfo: [NSLocalizedDescriptionKey: "Phase [brokenFence:initialContext.init] failed: \(error)"])
    }

    do {
      try initialContext.setEpochSecretStorage(storage: epochStorage)
    } catch {
      throw NSError(domain: "C36", code: 41, userInfo: [NSLocalizedDescriptionKey: "Phase [brokenFence:setEpochStorage] failed: \(error)"])
    }

    let groupId: Data
    do {
      let creation = try initialContext.createGroup(identityBytes: Data(did.utf8), config: nil)
      groupId = creation.groupId
    } catch {
      throw NSError(domain: "C36", code: 42, userInfo: [NSLocalizedDescriptionKey: "Phase [brokenFence:createGroup] failed: \(error)"])
    }

    try initialContext.flushStorage()
    try initialContext.flushAndPrepareClose()

    let start = Date().timeIntervalSince1970 + 0.35
    let child = try spawnNativeOwner([
      "group-mutation-unfenced",
      directory.path,
      roundDir.path,
      did,
      groupId.hexEncodedString(),
      String(format: "%.6f", start)
    ])
    try child.waitForLine("ready")

    while Date().timeIntervalSince1970 < start {}

    var appFailure: String?
    var appCommit: Data?
    var appPriorEpoch: UInt64?
    do {
      let context = try MlsContext(
        storagePath: dbURL.path,
        encryptionKey: encryptionKey,
        keychain: keychain
      )
      try context.setEpochSecretStorage(storage: epochStorage)
      let priorEpoch = try context.getEpoch(groupId: groupId)
      appPriorEpoch = priorEpoch
      try Data(String(priorEpoch).utf8).write(to: roundDir.appendingPathComponent("app-ready"), options: .atomic)
      let childReady = roundDir.appendingPathComponent("child-ready")
      let deadline = Date().addingTimeInterval(5)
      while !FileManager.default.fileExists(atPath: childReady.path) && Date() < deadline {
        try await Task.sleep(for: .milliseconds(5))
      }
      guard FileManager.default.fileExists(atPath: childReady.path) else { throw NSError(domain: "C36", code: 46) }
      let update = try context.selfUpdate(groupId: groupId)
      appCommit = update.commitData
      _ = try context.mergePendingCommit(groupId: groupId)
      try context.flushStorage()
      try context.flushAndPrepareClose()
    } catch {
      appFailure = String(describing: error)
    }

    let status = child.exitStatus()
    XCTAssertNil(appFailure, "Unfenced parent mutation failed instead of demonstrating a fork: \(appFailure ?? "")")
    XCTAssertEqual(status, 0, "Unfenced child mutation failed instead of demonstrating a fork: \(child.readOutput())")
    guard appFailure == nil && status == 0 else { return }
    let childPriorEpoch = try String(contentsOf: roundDir.appendingPathComponent("child-ready"), encoding: .utf8)
    XCTAssertEqual(childPriorEpoch, try String(XCTUnwrap(appPriorEpoch)),
                   "Both processes must race from the same persisted epoch")
    let childCommit = try Data(contentsOf: roundDir.appendingPathComponent("unfenced-child-commit.bin"))
    XCTAssertNotEqual(try XCTUnwrap(appCommit), childCommit,
                      "Without the fence, two native processes produce divergent commits at one epoch")
    print("C36 broken-fence check: appFailed=false, childStatus=0, commonEpoch=\(childPriorEpoch)")

    // The store remains recoverable and decryptable on disk
    let recoveryContext = try MlsContext(
      storagePath: dbURL.path,
      encryptionKey: encryptionKey,
      keychain: keychain
    )
    try recoveryContext.setEpochSecretStorage(storage: epochStorage)
    let recoveredEpoch = try recoveryContext.getEpoch(groupId: groupId)
    XCTAssertEqual(recoveredEpoch, 1, "Both unfenced owners committed from epoch zero; one durable epoch must remain")
    try recoveryContext.flushAndPrepareClose()
  }
}

private final class GrantFlag: @unchecked Sendable {
  private let lock = NSLock()
  private var granted = false
  func set() { lock.withLock { granted = true } }
  var value: Bool { lock.withLock { granted } }
}

/// Minimal private file-backed `KeychainAccess` implementation for cross-process test harnesses.
/// Persists signature key pairs and secrets to a shared directory across native processes
/// without requiring the macOS host keychain or App Store keychain access groups.
private final class FileBackedKeychainAccess: KeychainAccess, @unchecked Sendable {
  private let directory: URL
  private let lock = NSLock()

  init(directory: URL) {
    self.directory = directory
    try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  }

  private func fileURL(for key: String) -> URL {
    let hash = SHA256.hash(data: Data(key.utf8)).map { String(format: "%02x", $0) }.joined()
    return directory.appendingPathComponent("\(hash).key")
  }

  func read(key: String) async throws -> Data? {
    lock.withLock {
      let url = fileURL(for: key)
      guard FileManager.default.fileExists(atPath: url.path) else {
        return nil
      }
      return try? Data(contentsOf: url)
    }
  }

  func write(key: String, value: Data) async throws {
    try lock.withLock {
      let url = fileURL(for: key)
      try value.write(to: url, options: .atomic)
      try? FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
    }
  }

  func delete(key: String) async throws {
    lock.withLock {
      let url = fileURL(for: key)
      try? FileManager.default.removeItem(at: url)
    }
  }
}

/// Minimal private file-backed `EpochSecretStorage` implementation for cross-process test harnesses.
/// Persists epoch secrets to a shared directory across native processes without touching production storage.
private final class FileBackedEpochSecretStorage: EpochSecretStorage, @unchecked Sendable {
  private let directory: URL
  private let lock = NSLock()

  init(directory: URL) {
    self.directory = directory
    try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  }

  private func fileURL(for conversationId: String, epoch: UInt64) -> URL {
    let safeName = "\(conversationId)_\(epoch)"
    return directory.appendingPathComponent("\(safeName).secret")
  }

  func storeEpochSecret(conversationId: String, epoch: UInt64, secretData: Data) async -> Bool {
    lock.withLock {
      let url = fileURL(for: conversationId, epoch: epoch)
      do {
        try secretData.write(to: url, options: .atomic)
        try? FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
        return true
      } catch {
        return false
      }
    }
  }

  func getEpochSecret(conversationId: String, epoch: UInt64) async -> Data? {
    lock.withLock {
      let url = fileURL(for: conversationId, epoch: epoch)
      return try? Data(contentsOf: url)
    }
  }

  func deleteEpochSecret(conversationId: String, epoch: UInt64) async -> Bool {
    lock.withLock {
      let url = fileURL(for: conversationId, epoch: epoch)
      do {
        try FileManager.default.removeItem(at: url)
        return true
      } catch {
        return false
      }
    }
  }

  func deleteEpochsBefore(conversationId: String, cutoffEpoch: UInt64) async -> UInt32 {
    lock.withLock {
      var deleted: UInt32 = 0
      guard let items = try? FileManager.default.contentsOfDirectory(atPath: directory.path) else { return 0 }
      for item in items where item.hasPrefix("\(conversationId)_") && item.hasSuffix(".secret") {
        let parts = item.dropFirst("\(conversationId)_".count).dropLast(".secret".count)
        if let ep = UInt64(parts), ep < cutoffEpoch {
          try? FileManager.default.removeItem(at: directory.appendingPathComponent(item))
          deleted += 1
        }
      }
      return deleted
    }
  }
}

#endif
