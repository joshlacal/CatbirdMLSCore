//
//  MLSAuthorizedDeviceKeyStore.swift
//  CatbirdMLSCore
//
//  Synchronously readable mirror of the authorized MLS device signing keys
//  already resolved from ATProto `blue.catbird.mlsChat.device` records.
//

import Foundation
import os

/// Holds the authorized MLS device signing keys that `MLSDeviceRecordService`
/// has already resolved, in a form that can be read without `await`.
///
/// The Rust orchestrator's ADR-009 device-key check reaches Swift through a
/// synchronous UniFFI callback, so it can neither await `MLSDeviceRecordService`
/// (an actor) nor perform network I/O. This store is the seam: the record
/// service writes every resolution into it, and the credential adapter reads it
/// on the callback thread.
///
/// Staleness policy: entries are served exactly as last resolved. This type
/// never fetches, refreshes, or expires anything — the record service's own
/// fetch path is the only writer, and the Rust orchestrator applies its own
/// `DEVICE_KEY_CACHE_TTL` on top of whatever we answer. A DID that has not been
/// resolved yet reads as `nil` (unknown), which is deliberately distinct from a
/// DID that resolved to zero authorized keys (`[]`).
final class MLSAuthorizedDeviceKeyStore: Sendable {

  private let entries = OSAllocatedUnfairLock(initialState: [String: [Data]]())

  init() {}

  /// Record the authorized device signing keys resolved for `did`.
  ///
  /// An empty `keys` array means "resolved, zero authorized devices" and is
  /// stored as such; callers must not use it to represent a failed lookup.
  func store(keys: [Data], for did: String) {
    let key = Self.normalize(did)
    entries.withLock { $0[key] = keys }
  }

  /// The authorized device signing keys known for `did`, or `nil` when this DID
  /// has never been resolved.
  func keys(for did: String) -> [Data]? {
    let key = Self.normalize(did)
    return entries.withLock { $0[key] }
  }

  /// Drop any resolution held for `did` so the next lookup reports "unknown"
  /// rather than a set we know to be out of date.
  func invalidate(_ did: String) {
    let key = Self.normalize(did)
    _ = entries.withLock { $0.removeValue(forKey: key) }
  }

  func removeAll() {
    entries.withLock { $0.removeAll() }
  }

  /// Rust looks up by credential *root* DID, so a `did:...#device-id` fragment
  /// must collapse onto the same entry as the bare DID.
  private static func normalize(_ did: String) -> String {
    let root = did.split(separator: "#", maxSplits: 1).first.map(String.init) ?? did
    return root.lowercased()
  }
}
