//
//  TestSupport+InMemoryKeychain.swift
//  CatbirdMLSCoreTests
//
//  Lightweight in-memory `KeychainAccess` stub so tests can construct
//  ephemeral `MlsContext` instances without touching the real iOS Keychain.
//

import Foundation
import Synchronization
@testable import CatbirdMLS

final class InMemoryKeychainAccess: KeychainAccess, @unchecked Sendable {
  private let store = Mutex<[String: Data]>([:])

  func read(key: String) async throws -> Data? {
    store.withLock { $0[key] }
  }

  func write(key: String, value: Data) async throws {
    store.withLock { $0[key] = value }
  }

  func delete(key: String) async throws {
    store.withLock { $0.removeValue(forKey: key) }
  }
}
