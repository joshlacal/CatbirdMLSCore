import Foundation
#if DEBUG
@_spi(Fixtures) import Petrel
#else
import Petrel
#endif

/// Why an account cannot take part in encrypted chat yet. Each case is a fact about the
/// account's own published state, read successfully.
public enum MLSChatIncapableReason: String, Error, Sendable, Equatable, CaseIterable {
  /// The DID document has no usable `#atproto_pds` service.
  case invalidDIDDocument
  /// The DID document has no `#atproto_kem` Multikey verification method.
  case missingKEMKey
  /// The `#atproto_kem` verification method carries no public key.
  case emptyKEMKey
  /// The repository has no `blue.catbird.chat.declaration` record.
  case missingDeclaration
  /// The declaration record exists but is not a supported declaration.
  case invalidDeclaration
  /// The complete device listing holds no `blue.catbird.chat.device` record.
  case noDevices
}

/// Why a capability lookup could not reach an answer. Never cached.
public enum MLSChatCapabilityLookupFailure: String, Sendable, Equatable, CaseIterable {
  /// A request failed or timed out.
  case network
  /// The device listing could not be read to its end.
  case incompleteDeviceListing
  /// A response could not be read.
  case invalidResponse
}

/// Whether an account can be in an encrypted (MLS) chat.
public enum MLSChatCapability: Sendable, Equatable {
  case capable
  case incapable(MLSChatIncapableReason)
  case unknown(MLSChatCapabilityLookupFailure)

  public var isCapable: Bool {
    if case .capable = self { return true }
    return false
  }
}

/// The state of an account's `blue.catbird.chat.declaration/self` record.
public enum MLSChatDeclarationState: Sendable, Equatable {
  case valid
  case missing
  case invalid
}

/// Reads the public records a capability decision is made from. Throwing means the read did
/// not complete; returning means the account's published state was read.
public protocol MLSChatCapabilitySource: Sendable {
  /// The account's own DID document, freshly fetched.
  func didDocument(for did: String) async throws -> DIDDocument
  /// The account's chat declaration, read from `pds`.
  func declarationState(for did: String, pds: URL) async throws -> MLSChatDeclarationState
  /// The number of device records in the account's complete device listing on `pds`.
  /// Throws when the listing cannot be read to its end.
  func deviceRecordCount(for did: String, pds: URL) async throws -> Int
}

/// The error a source throws when a device listing could not be read completely.
public struct MLSChatIncompleteDeviceListing: Error, Sendable, Equatable {
  public init() {}
}

/// Decides whether an account can be in an encrypted chat:
/// a valid DID document with a non-empty `#atproto_kem` Multikey verification method, a valid
/// `blue.catbird.chat.declaration`, and at least one `blue.catbird.chat.device` record in a
/// complete listing.
///
/// Answers are cached per DID for `ttl`; a lookup that fails is never cached, so the next
/// request reads again. An "incapable" answer is therefore at most `ttl` old.
public actor MLSChatCapabilityResolver {
  public static let defaultTTL: TimeInterval = 120

  public static let shared = MLSChatCapabilityResolver(source: MLSPublicChatCapabilitySource())

  private struct Entry {
    let capability: MLSChatCapability
    let storedAt: Date
  }

  private let source: any MLSChatCapabilitySource
  private let ttl: TimeInterval
  private let now: @Sendable () -> Date
  private var cache: [String: Entry] = [:]

  public init(
    source: any MLSChatCapabilitySource,
    ttl: TimeInterval = MLSChatCapabilityResolver.defaultTTL,
    now: @escaping @Sendable () -> Date = { Date() }
  ) {
    self.source = source
    self.ttl = ttl
    self.now = now
  }

  /// The capability of `did`. `forceRefresh` reads again even if a fresh answer is cached.
  public func capability(for did: String, forceRefresh: Bool = false) async -> MLSChatCapability {
    let key = did.trimmingCharacters(in: .whitespacesAndNewlines)
    if !forceRefresh, let entry = cache[key], now().timeIntervalSince(entry.storedAt) < ttl {
      return entry.capability
    }
    let result = await lookUp(did: key)
    switch result {
    case .capable, .incapable:
      cache[key] = Entry(capability: result, storedAt: now())
    case .unknown:
      cache.removeValue(forKey: key)
    }
    return result
  }

  /// Capabilities for several accounts, read a few at a time.
  public func capabilities(
    for dids: [String], forceRefresh: Bool = false
  ) async -> [String: MLSChatCapability] {
    var results: [String: MLSChatCapability] = [:]
    let unique = Array(Set(dids))
    var index = 0
    while index < unique.count {
      let batch = unique[index..<min(index + 6, unique.count)]
      index += batch.count
      await withTaskGroup(of: (String, MLSChatCapability).self) { group in
        for did in batch {
          group.addTask { (did, await self.capability(for: did, forceRefresh: forceRefresh)) }
        }
        for await (did, capability) in group {
          results[did] = capability
        }
      }
    }
    return results
  }

  /// Drops the cached answer for `did`.
  public func invalidate(did: String) {
    cache.removeValue(forKey: did.trimmingCharacters(in: .whitespacesAndNewlines))
  }

  /// Drops every cached answer, for example after an account switch.
  public func invalidateAll() {
    cache.removeAll()
  }

  private func lookUp(did: String) async -> MLSChatCapability {
    guard !did.isEmpty, !did.contains("#"), (try? DID(didString: did)) != nil else {
      return .incapable(.invalidDIDDocument)
    }
    let document: DIDDocument
    do {
      document = try await source.didDocument(for: did)
    } catch {
      return .unknown(Self.failure(for: error))
    }
    let pds: URL
    switch Self.evaluate(document: document, did: did) {
    case .failure(let reason): return .incapable(reason)
    case .success(let endpoint): pds = endpoint
    }
    do {
      switch try await source.declarationState(for: did, pds: pds) {
      case .valid: break
      case .missing: return .incapable(.missingDeclaration)
      case .invalid: return .incapable(.invalidDeclaration)
      }
    } catch {
      return .unknown(Self.failure(for: error))
    }
    do {
      let count = try await source.deviceRecordCount(for: did, pds: pds)
      return count > 0 ? .capable : .incapable(.noDevices)
    } catch is MLSChatIncompleteDeviceListing {
      return .unknown(.incompleteDeviceListing)
    } catch {
      return .unknown(Self.failure(for: error))
    }
  }

  /// Checks the parts of a DID document encrypted chat needs, returning its PDS endpoint.
  public static func evaluate(
    document: DIDDocument, did: String
  ) -> Result<URL, MLSChatIncapableReason> {
    guard document.id == did,
          let pds = try? MLSPublicPDSReader.currentPDS(in: document, did: did)
    else { return .failure(.invalidDIDDocument) }
    let kemMethods = document.verificationMethod.filter {
      ($0.id == "#atproto_kem" || $0.id == "\(did)#atproto_kem")
        && $0.type == "Multikey"
        && $0.controller == did
    }
    guard let kem = kemMethods.first else { return .failure(.missingKEMKey) }
    guard !kem.publicKeyMultibase.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
      return .failure(.emptyKEMKey)
    }
    return .success(pds)
  }

  static func failure(for error: Error) -> MLSChatCapabilityLookupFailure {
    if error is MLSChatIncompleteDeviceListing { return .incompleteDeviceListing }
    if error is URLError || error is CancellationError { return .network }
    if let bounded = error as? MLSBoundedURLSession.BoundedFetchError {
      if case .timedOut = bounded { return .network }
      return .invalidResponse
    }
    if let reader = error as? MLSPublicPDSReader.ReaderError, case .httpStatus = reader {
      return .network
    }
    if error is SpaceHostResolutionError { return .network }
    return .invalidResponse
  }
}

/// Reads capability inputs from the account's own DID document and PDS, unauthenticated.
public struct MLSPublicChatCapabilitySource: MLSChatCapabilitySource {
  public init() {}

  public func didDocument(for did: String) async throws -> DIDDocument {
    try await MLSPublicPDSReader.fetchValidatedDIDDocument(did: did)
  }

  public func declarationState(for did: String, pds: URL) async throws -> MLSChatDeclarationState {
    do {
      let declaration = try await MLSPublicPDSReader.fetchDeclaration(
        did: did, resolvePDS: { _ in pds })
      return declaration == nil ? .missing : .valid
    } catch MLSPublicPDSReader.ReaderError.invalidResponse {
      return .invalid
    } catch is DecodingError {
      return .invalid
    }
  }

  public func deviceRecordCount(for did: String, pds: URL) async throws -> Int {
    do {
      return try await MLSPublicPDSReader.fetchAuthorizedDeviceRecords(
        did: did, resolvePDS: { _ in pds }
      ).count
    } catch MLSPublicPDSReader.ReaderError.invalidResponse {
      throw MLSChatIncompleteDeviceListing()
    }
  }
}
