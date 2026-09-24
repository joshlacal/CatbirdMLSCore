import Foundation
import XCTest
@testable import CatbirdMLSCore

#if os(macOS)
/// C36: two real owners (this test process = app, a spawned OS process = NSE) contend
/// for the same account's crypto-state fences on disk:
///   - the O_EXCL Welcome marker that decides which process may import a Welcome, and
///   - the BSD flock admission lease that keeps a reset from running under an open store.
/// Each fence test has a negative control showing the assertion fails when the fence is absent.
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

  // MARK: - Second-process owner

  /// The second owner. Modes:
  ///   excl  <marker> <t>   spin until wall time <t>, then O_CREAT|O_EXCL the marker (exit 0 won, 3 lost)
  ///   naive <marker> <t>   same, without O_EXCL (negative control: always "wins")
  ///   hold-sh <lease>      take LOCK_SH, print "held", keep it until stdin closes
  ///   try-ex <lease>       LOCK_EX|LOCK_NB once (exit 0 acquired, 3 blocked)
  private static let ownerScript = """
    import fcntl, os, sys, time
    mode = sys.argv[1]
    if mode in ("excl", "naive"):
        marker, start = sys.argv[2], float(sys.argv[3])
        print("ready", flush=True)
        if start - time.time() > 10: sys.exit(5)
        while time.time() < start:
            pass
        flags = os.O_CREAT | os.O_WRONLY | (os.O_EXCL if mode == "excl" else 0)
        try:
            os.close(os.open(marker, flags, 0o600))
        except FileExistsError:
            sys.exit(3)
        sys.exit(0)
    lease = sys.argv[2]
    fd = os.open(lease, os.O_RDWR | os.O_CREAT, 0o666)
    if mode == "hold-sh":
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            sys.exit(4)
        print("held", flush=True)
        sys.stdin.read()
        sys.exit(0)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(3)
    sys.exit(0)
    """

  private struct Owner {
    let process: Process
    let stdin: Pipe
    let stdout: Pipe

    func waitForLine(_ expected: String) throws {
      let data = stdout.fileHandleForReading.availableData
      let line = String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
      guard line == expected else {
        throw NSError(domain: "C36", code: 1, userInfo: [NSLocalizedDescriptionKey: "owner said '\(line)', expected '\(expected)'"])
      }
    }

    func exitStatus() -> Int32 {
      process.waitUntilExit()
      return process.terminationStatus
    }
  }

  private func spawnOwner(_ arguments: [String]) throws -> Owner {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/python3")
    process.arguments = ["-c", Self.ownerScript] + arguments
    let stdin = Pipe()
    let stdout = Pipe()
    process.standardInput = stdin
    process.standardOutput = stdout
    try process.run()
    return Owner(process: process, stdin: stdin, stdout: stdout)
  }

  // MARK: - Welcome ownership: exactly one process imports

  func testCrossProcessWelcomeClaimHasExactlyOneWinner() async throws {
    let did = "did:plc:c36-xproc-welcome-\(UUID().uuidString.lowercased())"
    let rounds = 36
    var appWins = 0
    var nseWins = 0
    for round in 0..<rounds {
      let conversationID = UUID().uuidString
      let marker = await MLSWelcomeGate.shared.markerURL(conversationID: conversationID, userDID: did)
      try FileManager.default.createDirectory(at: marker.deletingLastPathComponent(), withIntermediateDirectories: true)
      // Both owners claim at one shared instant; the second process is skewed by
      // -1ms...+1ms across rounds so every interleaving around the instant is exercised.
      let start = Date().timeIntervalSince1970 + 0.4
      let skew = Double(round % 9 - 4) * 0.00025
      let nse = try spawnOwner(["excl", marker.path, String(format: "%.6f", start + skew)])
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
    // Both owners winning some rounds shows the claims genuinely overlapped rather than ran in sequence.
    XCTAssertGreaterThan(appWins, 0)
    XCTAssertGreaterThan(nseWins, 0)
  }

  /// Negative control: without O_EXCL both owners "win" the same race, so the
  /// single-winner assertion above would fail if the gate lost its exclusive create.
  func testNegativeControlNonExclusiveClaimLetsBothOwnersWin() throws {
    let marker = directory.appendingPathComponent("naive-\(UUID().uuidString).pending")
    let start = Date().timeIntervalSince1970 + 0.4
    let nse = try spawnOwner(["naive", marker.path, String(format: "%.6f", start)])
    try nse.waitForLine("ready")
    while Date().timeIntervalSince1970 < start {}
    let fd = open(marker.path, O_CREAT | O_WRONLY, 0o600)
    let appWon = fd >= 0
    if fd >= 0 { close(fd) }
    let nseWon = nse.exitStatus() == 0
    XCTAssertTrue(appWon && nseWon, "Without O_EXCL the race produces two owners")
  }

  // MARK: - Store admission lease: reset waits for the other process

  func testSecondProcessHoldingStoreBlocksResetUntilItReleases() async throws {
    let did = "did:plc:c36-xproc-lease-\(UUID().uuidString.lowercased())"
    let coordinator = MLSStorageCoordinator.shared
    let lease = try coordinator.leaseURL(for: .swiftGRDB, userDID: did, type: "admission")
    try FileManager.default.createDirectory(at: lease.deletingLastPathComponent(), withIntermediateDirectories: true)

    // Control: with no other owner the exclusive reset lease is granted at once.
    let controlStart = ContinuousClock.now
    try await coordinator.acquireExclusiveResetLease(for: .swiftGRDB, userDID: did).release()
    XCTAssertLessThan(ContinuousClock.now - controlStart, .seconds(1))

    // The NSE process now holds the store open (shared admission lease).
    let nse = try spawnOwner(["hold-sh", lease.path])
    try nse.waitForLine("held")

    let resetStart = ContinuousClock.now
    let granted = GrantFlag()
    let reset = Task {
      let token = try await coordinator.acquireExclusiveResetLease(for: .swiftGRDB, userDID: did)
      granted.set()
      return token
    }
    try await Task.sleep(for: .seconds(1))
    XCTAssertFalse(granted.value, "Reset must not be granted while another process holds the store")

    // NSE closes the store; the reset proceeds.
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
    let lease = try MLSStorageCoordinator.shared.leaseURL(for: .swiftGRDB, userDID: did, type: "admission")

    let whileOpen = try spawnOwner(["try-ex", lease.path])
    XCTAssertEqual(whileOpen.exitStatus(), 3, "Second process must not gain exclusive ownership of an open store")

    await manager.closeDatabaseAndDrain(for: did)
    let afterDrain = try spawnOwner(["try-ex", lease.path])
    XCTAssertEqual(afterDrain.exitStatus(), 0, "Exclusive ownership is available once the app releases the store")
    await manager.shutdownAllDatabases()
  }
}

private final class GrantFlag: @unchecked Sendable {
  private let lock = NSLock()
  private var granted = false
  func set() { lock.withLock { granted = true } }
  var value: Bool { lock.withLock { granted } }
}
#endif
