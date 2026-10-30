import Foundation
@testable import CatbirdMLSCore
import CatbirdMLS
import GRDB
import CryptoKit

@main
struct CatbirdMLSTestHost {
  static func main() async {
    let args = CommandLine.arguments
    guard args.count >= 2 else {
      fputs("Usage: CatbirdMLSTestHost <command> [args...]\n", stderr)
      exit(1)
    }

    let command = args[1]
    let pid = ProcessInfo.processInfo.processIdentifier

    switch command {
    case "welcome-claim":
      // welcome-claim <baseDir> <convoID> <did> <targetTimeEpochSec>
      guard args.count >= 6 else {
        fputs("Usage: CatbirdMLSTestHost welcome-claim <baseDir> <convoID> <did> <targetTimeEpochSec>\n", stderr)
        exit(1)
      }
      let baseDir = args[2]
      let convoID = args[3]
      let did = args[4]
      guard let targetTime = Double(args[5]) else {
        fputs("Invalid targetTime\n", stderr)
        exit(1)
      }

      MLSStoragePaths.setBaseDirectoryOverride(URL(fileURLWithPath: baseDir))
      MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())
      print("ready")
      fflush(stdout)

      // Spin until synchronized target wall-clock time
      while Date().timeIntervalSince1970 < targetTime {}

      do {
        try await MLSWelcomeGate.shared.beginWelcomeProcessing(for: convoID, userDID: did)
        print("WELCOME won")
        fflush(stdout)
        exit(0)
      } catch let error as MLSStorageInitializationError {
        if case .admissionDenied(let details) = error {
          print("WELCOME denied \(details)")
          fflush(stdout)
          exit(3)
        }
        fputs("WELCOME error: \(error)\n", stderr)
        exit(1)
      } catch {
        fputs("WELCOME error: \(error)\n", stderr)
        exit(1)
      }
    case "hold-lease":
      // hold-lease <baseDir> <did>
      guard args.count >= 4 else {
        fputs("Usage: CatbirdMLSTestHost hold-lease <baseDir> <did>\n", stderr)
        exit(1)
      }
      let baseDir = args[2]
      let did = args[3]

      MLSStoragePaths.setBaseDirectoryOverride(URL(fileURLWithPath: baseDir))
      MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())
      print("PID \(pid)")
      fflush(stdout)

      do {
        let manager = MLSGRDBManager()
        _ = try await manager.getDatabasePool(for: did)
        print("HELD")
        fflush(stdout)

        _ = readLine()

        await manager.closeDatabaseAndDrain(for: did)
        exit(0)
      } catch {
        fputs("Error in hold-lease: \(error)\n", stderr)
        exit(1)
      }

    case "try-exclusive":
      // try-exclusive <baseDir> <did>
      guard args.count >= 4 else {
        fputs("Usage: CatbirdMLSTestHost try-exclusive <baseDir> <did>\n", stderr)
        exit(1)
      }
      let baseDir = args[2]
      let did = args[3]

      MLSStoragePaths.setBaseDirectoryOverride(URL(fileURLWithPath: baseDir))
      MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())
      print("PID \(pid)")
      fflush(stdout)

      do {
        let lease = try await MLSStorageCoordinator.shared.acquireExclusiveResetLease(for: .swiftGRDB, userDID: did)
        lease.release()
        print("EXCLUSIVE acquired")
        fflush(stdout)
        exit(0)
      } catch {
        print("EXCLUSIVE denied")
        fflush(stdout)
        exit(3)
      }

    case "group-mutation-race":
      // group-mutation-race <coordRootDir> <aliceDir> <did> <groupIdHex> <targetTimeEpochSec>
      guard args.count >= 7 else {
        fputs("Usage: CatbirdMLSTestHost group-mutation-race <coordRootDir> <aliceDir> <did> <groupIdHex> <targetTimeEpochSec>\n", stderr)
        exit(1)
      }
      let coordRootDir = args[2]
      let aliceDir = args[3]
      let did = args[4]
      let groupIdHex = args[5]
      guard let targetTime = Double(args[6]) else {
        fputs("Invalid targetTime\n", stderr)
        exit(1)
      }

      MLSStoragePaths.setBaseDirectoryOverride(URL(fileURLWithPath: coordRootDir))
      MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())

      let aliceDirURL = URL(fileURLWithPath: aliceDir)
      let keychainDir = aliceDirURL.appendingPathComponent("keychain")
      let keychain = FileBackedKeychainAccess(directory: keychainDir)
      let epochDir = aliceDirURL.appendingPathComponent("epoch_secrets")
      let epochStorage = FileBackedEpochSecretStorage(directory: epochDir)
      let dbURL = aliceDirURL.appendingPathComponent("openmls.db")
      let encryptionKey = String(repeating: "ab", count: 32)

      guard let groupId = Data(hexEncoded: groupIdHex) else {
        fputs("Invalid groupIdHex\n", stderr)
        exit(1)
      }

      print("ready")
      fflush(stdout)

      // Spin until synchronized target wall-clock time
      while Date().timeIntervalSince1970 < targetTime {}

      do {
        let lease = try MLSStorageCoordinator.shared.acquireMutationMutexSync(for: .rustState, userDID: did, timeoutSeconds: 0.1)
        let context: MlsContext
        do {
          context = try MlsContext(storagePath: dbURL.path, encryptionKey: encryptionKey, keychain: keychain)
        } catch {
          lease.release()
          fputs("PHASE [child:MlsContext.init] error: \(error)\n", stderr)
          exit(1)
        }
        do {
          try context.setEpochSecretStorage(storage: epochStorage)
        } catch {
          lease.release()
          fputs("PHASE [child:setEpochSecretStorage] error: \(error)\n", stderr)
          exit(1)
        }
        let updateResult: AddMembersResult
        do {
          updateResult = try context.selfUpdate(groupId: groupId)
        } catch {
          lease.release()
          fputs("PHASE [child:selfUpdate] error: \(error)\n", stderr)
          exit(1)
        }
        do {
          _ = try context.mergePendingCommit(groupId: groupId)
        } catch {
          lease.release()
          fputs("PHASE [child:mergePendingCommit] error: \(error)\n", stderr)
          exit(1)
        }
        do {
          try context.flushStorage()
          try context.flushAndPrepareClose()
        } catch {
          lease.release()
          fputs("PHASE [child:flushStorage] error: \(error)\n", stderr)
          exit(1)
        }
        let commitURL = aliceDirURL.appendingPathComponent("winner_commit.bin")
        try? updateResult.commitData.write(to: commitURL, options: .atomic)
        print("MUTATION won")
        fflush(stdout)
        // Hold mutation lease for 300ms so any competing process with 100ms timeout is strictly fenced
        usleep(300_000)
        lease.release()
        exit(0)
      } catch let error as MLSStorageInitializationError {
        if case .admissionDenied(let details) = error {
          print("MUTATION fenced \(details)")
          fflush(stdout)
          exit(3)
        }
        fputs("MUTATION error: \(error)\n", stderr)
        exit(1)
      } catch {
        fputs("MUTATION error: \(error)\n", stderr)
        exit(1)
      }

    case "group-mutation-unfenced":
      // group-mutation-unfenced <coordRootDir> <baseDir> <did> <groupIdHex> <targetTimeEpochSec>
      guard args.count >= 7 else {
        fputs("Usage: CatbirdMLSTestHost group-mutation-unfenced <coordRootDir> <baseDir> <did> <groupIdHex> <targetTimeEpochSec>\n", stderr)
        exit(1)
      }
      let coordRootDir = args[2]
      let baseDir = args[3]
      let _ = args[4] // did is part of argument protocol
      let groupIdHex = args[5]
      guard let targetTime = Double(args[6]) else {
        fputs("Invalid targetTime\n", stderr)
        exit(1)
      }

      MLSStoragePaths.setBaseDirectoryOverride(URL(fileURLWithPath: coordRootDir))
      MLSKeychainManager.setFakeStorageOverrideForTesting(MLSKeychainFakeStorage())

      let baseDirURL = URL(fileURLWithPath: baseDir)
      let keychainDir = baseDirURL.appendingPathComponent("keychain")
      let keychain = FileBackedKeychainAccess(directory: keychainDir)
      let epochDir = baseDirURL.appendingPathComponent("epoch_secrets")
      let epochStorage = FileBackedEpochSecretStorage(directory: epochDir)
      let dbURL = baseDirURL.appendingPathComponent("openmls.db")
      let encryptionKey = String(repeating: "ab", count: 32)

      guard let groupId = Data(hexEncoded: groupIdHex) else {
        fputs("Invalid groupIdHex\n", stderr)
        exit(1)
      }
      print("ready")
      fflush(stdout)

      // Spin until synchronized target wall-clock time
      while Date().timeIntervalSince1970 < targetTime {}

      // Intentional broken fence: mutate OpenMLS state without acquiring mutation mutex
      do {
        let context = try MlsContext(storagePath: dbURL.path, encryptionKey: encryptionKey, keychain: keychain)
        try context.setEpochSecretStorage(storage: epochStorage)
        let priorEpoch = try context.getEpoch(groupId: groupId)
        try Data(String(priorEpoch).utf8).write(to: baseDirURL.appendingPathComponent("child-ready"), options: .atomic)
        let appReady = baseDirURL.appendingPathComponent("app-ready")
        let deadline = Date().addingTimeInterval(5)
        while !FileManager.default.fileExists(atPath: appReady.path) && Date() < deadline {
          try await Task.sleep(for: .milliseconds(5))
        }
        guard FileManager.default.fileExists(atPath: appReady.path) else { throw NSError(domain: "C36", code: 45) }
        let update = try context.selfUpdate(groupId: groupId)
        _ = try context.mergePendingCommit(groupId: groupId)
        try context.flushStorage()
        try context.flushAndPrepareClose()
        try update.commitData.write(to: baseDirURL.appendingPathComponent("unfenced-child-commit.bin"), options: .atomic)
        print("UNFENCED won")
        fflush(stdout)
        exit(0)
      } catch {
        print("UNFENCED collision \(error)")
        fflush(stdout)
        exit(4)
      }

    default:
      fputs("Unknown command: \(command)\n", stderr)
      exit(1)
    }
  }
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

