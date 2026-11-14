import CatbirdMLS
import Foundation
import Petrel

// MARK: - Public surface

/// Every Circle failure, typed for the UI. Mapped 1:1 from the Rust
/// `CircleFfiError`; any other error a Circle call throws is `.unavailable`.
/// The associated string is short diagnostic text from the Rust layer (an
/// error code or fixed message, never key or payload bytes), safe to log.
/// Branch on the case, not on the text.
public enum CircleCryptoError: Error, Sendable, Equatable {
  /// Authentic-looking input that does not open with this key: AEAD tag,
  /// signature, or wrap failure.
  case notOpenable(String)
  /// The sender/author or its epoch chain failed verification (sender is not
  /// the owner, owner is not the Space authority, directory fork/rollback).
  case untrusted(String)
  /// The record, request, or payload is not well formed.
  case malformed(String)
  /// I/O, directory reachability, session, or lock state; may succeed later.
  case unavailable(String)

  public var reason: String {
    switch self {
    case let .notOpenable(reason), let .untrusted(reason),
         let .malformed(reason), let .unavailable(reason):
      return reason
    }
  }

  init(_ error: CircleFfiError) {
    switch error {
    case let .NotOpenable(reason): self = .notOpenable(reason)
    case let .Untrusted(reason): self = .untrusted(reason)
    case let .Malformed(reason): self = .malformed(reason)
    case let .Unavailable(reason): self = .unavailable(reason)
    }
  }

  /// Maps any error thrown by a Circle FFI call.
  static func from(_ error: any Error) -> CircleCryptoError {
    switch error {
    case let error as CircleCryptoError: return error
    case let error as CircleFfiError: return CircleCryptoError(error)
    default: return .unavailable(String(describing: type(of: error)))
    }
  }
}

extension CircleCryptoError: LocalizedError {
  public var errorDescription: String? {
    switch self {
    case let .notOpenable(reason): return "Circle content could not be opened (\(reason))"
    case let .untrusted(reason): return "Circle sender could not be verified (\(reason))"
    case let .malformed(reason): return "Circle record is malformed (\(reason))"
    case let .unavailable(reason): return "Circle operation is unavailable (\(reason))"
    }
  }
}

/// An opened Circle key and the identity its wrap authenticated (the FFI's
/// `CircleKeyOpenResult`; renamed so it never collides with that type in a
/// file importing both modules). `circleID`
/// and `keyVersion` come from the verified AAD, never from the record's
/// unauthenticated fields, so callers should key their caches by these values.
/// `circleKey` is raw key material: never log it.
public struct OpenedCircleKey: Sendable, Equatable {
  public let circleKey: Data
  public let circleID: String
  public let keyVersion: UInt64

  public init(circleKey: Data, circleID: String, keyVersion: UInt64) {
    self.circleKey = circleKey
    self.circleID = circleID
    self.keyVersion = keyVersion
  }

  init(_ result: CatbirdMLS.CircleKeyOpenResult) {
    self.init(circleKey: result.circleKey, circleID: result.circleId, keyVersion: result.keyVersion)
  }
}

extension OpenedCircleKey: CustomStringConvertible, CustomDebugStringConvertible {
  public var description: String {
    "OpenedCircleKey(circleID: \(circleID), keyVersion: \(keyVersion), circleKey: <redacted>)"
  }

  public var debugDescription: String { description }
}

/// The result of wrapping a Circle key to its members.
///
/// `excluded` lists members the key was NOT wrapped to, each with the reason the
/// Rust layer gave. An exclusion can be transient (for example a directory or
/// network fault while resolving the member's epoch chain), so callers should
/// keep excluded members on the pending re-wrap list and retry later rather than
/// treating them as permanently removed.
public struct CircleKeyWrapOutcome: Sendable {
  public let recordJSON: String
  public let rkey: String
  public let excluded: [(did: String, reason: String)]

  public init(recordJSON: String, rkey: String, excluded: [(did: String, reason: String)]) {
    self.recordJSON = recordJSON
    self.rkey = rkey
    self.excluded = excluded
  }

  init(_ result: CircleKeyWrapResult) {
    self.init(
      recordJSON: result.recordJson,
      rkey: result.rkey,
      excluded: zip(result.excludedDids, result.excludedReasons).map { (did: $0, reason: $1) }
    )
  }
}

/// End-to-end encrypted Circle operations. Every key argument and return value
/// is raw key material: never log it or put it in a `description`.
///
/// Every throwing entry point throws `CircleCryptoError`.
///
/// Raw-bytes contract: every `recordJSON` argument is the record's canonical
/// JSON exactly as fetched from the PDS / AppView, passed through untouched.
/// Never re-encode it through another codec (for example Petrel's typed
/// models): that injects nested `$type` fields and changes the bytes, which
/// breaks the AAD and signature checks.
public protocol CircleCrypto: Sendable {
  /// A fresh random 32-byte Circle key.
  func generateCircleKey() async throws -> Data
  /// Wrap `circleKey` to every member (and to this account, which the Rust
  /// layer adds itself). `createdAt` is part of the AAD.
  func wrapCircleKey(
    circleID: String,
    keyVersion: UInt64,
    spaceURI: String,
    reason: String,
    memberDIDs: [String],
    circleKey: Data,
    createdAt: Date
  ) async throws -> CircleKeyWrapOutcome
  /// Open this account's wrap in a `blue.catbird.circle.key` record (raw
  /// fetched bytes). `nil` when the record carries no wrap for this account's
  /// current or previous epoch. `ownerDID` is the Circle owner
  /// (`CircleSummary.owner`); it must be the DID authority of `spaceURI` and
  /// the record's sender, or the open fails with `.untrusted`.
  /// The result's `circleID` / `keyVersion` are the authenticated values.
  func openCircleKey(recordJSON: String, spaceURI: String, ownerDID: String) async throws -> OpenedCircleKey?
  /// Seal a Circle post; returns the `blue.catbird.circle.post` record JSON.
  func sealPost(
    circleID: String,
    keyVersion: UInt64,
    circleKey: Data,
    postURI: String,
    authorDID: String,
    createdAt: Date,
    payloadJSON: String,
    replyJSON: String?,
    attachmentsJSON: [String]
  ) async throws -> String
  /// Open a sealed Circle post (raw fetched record bytes); returns the payload JSON.
  func openPost(
    recordJSON: String,
    postURI: String,
    authorDID: String,
    circleKey: Data,
    authorEpochJSON: String
  ) async throws -> String
  /// The per-post key disclosed to moderation: `HKDF(circleKey, circleID, postURI)`.
  /// Throws `CircleCryptoError.malformed` if `circleKey` is not 32 bytes.
  func postKey(circleKey: Data, circleID: String, postURI: String) throws -> Data
}

// MARK: - FFI implementation

/// `CircleCrypto` over the UniFFI `CatbirdMlsClient` Circle methods.
///
/// The FFI methods are synchronous. Wrap, open-key, seal and open-post open the
/// account's Labyrinth session (database, and for wrap/open possibly the
/// network), so they run on a private concurrent dispatch queue and are awaited
/// through a continuation: they never block the main actor or a Swift
/// concurrency cooperative thread. Key generation and per-post key derivation
/// are short, pure CPU work and run inline. FFI errors are mapped to
/// `CircleCryptoError` (`CircleFfiError` 1:1, anything else `.unavailable`).
public final class FFICircleCrypto: CircleCrypto, @unchecked Sendable {
  // @unchecked: the only stored state is the UniFFI client handle, whose Rust
  // object is `Send + Sync`; the Swift wrapper holds no mutable state.
  private let client: CatbirdMlsClientProtocol

  private static let ffiQueue = DispatchQueue(
    label: "blue.catbird.mls.circles.ffi",
    qos: .userInitiated,
    attributes: .concurrent
  )

  public init(client: CatbirdMlsClientProtocol) {
    self.client = client
  }

  public func generateCircleKey() async throws -> Data {
    client.circleKeyGenerate()
  }

  public func wrapCircleKey(
    circleID: String,
    keyVersion: UInt64,
    spaceURI: String,
    reason: String,
    memberDIDs: [String],
    circleKey: Data,
    createdAt: Date
  ) async throws -> CircleKeyWrapOutcome {
    let request = CircleKeyWrapRequest(
      circleId: circleID,
      keyVersion: keyVersion,
      spaceUri: spaceURI,
      reason: reason,
      memberDids: memberDIDs,
      createdAt: CircleCryptoFormat.createdAt(createdAt)
    )
    let client = self.client
    let result = try await Self.offMain {
      try client.circleKeyWrap(req: request, circleKey: circleKey)
    }
    return CircleKeyWrapOutcome(result)
  }

  public func openCircleKey(
    recordJSON: String,
    spaceURI: String,
    ownerDID: String
  ) async throws -> OpenedCircleKey? {
    let client = self.client
    let result = try await Self.offMain {
      try client.circleKeyOpen(recordJson: recordJSON, spaceUri: spaceURI, ownerDid: ownerDID)
    }
    return result.map(OpenedCircleKey.init)
  }

  public func sealPost(
    circleID: String,
    keyVersion: UInt64,
    circleKey: Data,
    postURI: String,
    authorDID: String,
    createdAt: Date,
    payloadJSON: String,
    replyJSON: String?,
    attachmentsJSON: [String]
  ) async throws -> String {
    let request = CirclePostSealRequest(
      circleId: circleID,
      keyVersion: keyVersion,
      circleKey: circleKey,
      postUri: postURI,
      authorDid: authorDID,
      createdAt: CircleCryptoFormat.createdAt(createdAt),
      payloadJson: payloadJSON,
      replyJson: replyJSON,
      attachmentsJson: attachmentsJSON
    )
    let client = self.client
    return try await Self.offMain {
      try client.circlePostSeal(req: request)
    }
  }

  public func openPost(
    recordJSON: String,
    postURI: String,
    authorDID: String,
    circleKey: Data,
    authorEpochJSON: String
  ) async throws -> String {
    let request = CirclePostOpenRequest(
      recordJson: recordJSON,
      postUri: postURI,
      authorDid: authorDID,
      circleKey: circleKey,
      authorEpochJson: authorEpochJSON
    )
    let client = self.client
    return try await Self.offMain {
      try client.circlePostOpen(req: request)
    }
  }

  public func postKey(circleKey: Data, circleID: String, postURI: String) throws -> Data {
    try Self.mapped {
      try client.circlePostKey(circleKey: circleKey, circleId: circleID, postUri: postURI)
    }
  }

  private static func mapped<T>(_ operation: () throws -> T) throws -> T {
    do {
      return try operation()
    } catch {
      throw CircleCryptoError.from(error)
    }
  }

  private static func offMain<T>(
    _ operation: @escaping @Sendable () throws -> T
  ) async throws -> T {
    try await withCheckedThrowingContinuation { continuation in
      ffiQueue.async {
        continuation.resume(with: Result { try mapped(operation) })
      }
    }
  }
}

// MARK: - Formatting helpers

enum CircleCryptoFormat {
  /// RFC 3339 UTC with exactly millisecond precision, identical to Petrel's
  /// `ATProtocolDate` encoding (the string is bound into the AAD, so callers that
  /// echo it into records must use the same formatter).
  static func createdAt(_ date: Date) -> String {
    ATProtocolDate(date: date).iso8601String
  }
}
