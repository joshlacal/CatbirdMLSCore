//
//  MLSDeviceUUIDCache.swift
//  CatbirdMLSCore
//

import Foundation

/// In-memory cache for resolved device UUIDs scoped strictly by user DID and coordination generation.
/// Thread-safe via NSLock, avoiding synchronous keychain IPC on repeated resolutions.
public final class MLSDeviceUUIDCache: @unchecked Sendable {
  public static let shared = MLSDeviceUUIDCache()

  public struct Entry: Equatable, Sendable {
    public let deviceUuid: String
    public let generation: Int

    public init(deviceUuid: String, generation: Int) {
      self.deviceUuid = deviceUuid
      self.generation = generation
    }
  }

  private let lock = NSLock()
  private var entries: [String: Entry] = [:]

  public init() {}

  /// Retrieves the cached device UUID for `userDid` if the stored generation matches `generation`.
  /// If the generation has changed, the stale entry is purged and nil is returned.
  public func get(for userDid: String, generation: Int) -> String? {
    lock.lock()
    defer { lock.unlock() }
    guard let entry = entries[userDid] else { return nil }
    if entry.generation == generation {
      return entry.deviceUuid
    }
    entries.removeValue(forKey: userDid)
    return nil
  }

  /// Sets the cached device UUID for `userDid` associated with `generation`.
  public func set(_ deviceUuid: String, for userDid: String, generation: Int) {
    lock.lock()
    defer { lock.unlock() }
    entries[userDid] = Entry(deviceUuid: deviceUuid, generation: generation)
  }

  /// Invalidates the cache for a specific user DID, or all accounts if `userDid` is nil.
  public func invalidate(userDid: String? = nil) {
    lock.lock()
    defer { lock.unlock() }
    if let userDid = userDid {
      entries.removeValue(forKey: userDid)
    } else {
      entries.removeAll()
    }
  }

  /// Returns current cached entry for introspection / testing.
  public func entry(for userDid: String) -> Entry? {
    lock.lock()
    defer { lock.unlock() }
    return entries[userDid]
  }

  /// Clear everything (for testing).
  public func resetForTesting() {
    lock.lock()
    defer { lock.unlock() }
    entries.removeAll()
  }
}
