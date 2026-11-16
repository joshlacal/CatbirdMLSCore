import Foundation
import Petrel
import Testing
@testable import CatbirdMLSCore

/// Encrypted chat capability: DID document `#atproto_kem` Multikey, chat declaration and at
/// least one device record from a complete listing. Only a completed read can say "incapable";
/// a failed read is "unknown" and is never cached.
struct MLSChatCapabilityResolverTests {
  static let did = "did:plc:capabilitypeer0000000000"
  static let pds = "https://pds.example.com"

  final class FakeSource: MLSChatCapabilitySource, @unchecked Sendable {
    private let lock = NSLock()
    var document: Result<DIDDocument, Error>
    var declaration: Result<MLSChatDeclarationState, Error> = .success(.valid)
    var devices: Result<Int, Error> = .success(1)
    private(set) var documentReads = 0

    init(document: Result<DIDDocument, Error>) { self.document = document }

    func set(_ change: (FakeSource) -> Void) {
      lock.withLock { change(self) }
    }

    func didDocument(for did: String) async throws -> DIDDocument {
      try lock.withLock {
        documentReads += 1
        return try document.get()
      }
    }

    func declarationState(for did: String, pds: URL) async throws -> MLSChatDeclarationState {
      #expect(pds.absoluteString == MLSChatCapabilityResolverTests.pds)
      return try lock.withLock { try declaration.get() }
    }

    func deviceRecordCount(for did: String, pds: URL) async throws -> Int {
      try lock.withLock { try devices.get() }
    }
  }

  final class Clock: @unchecked Sendable {
    private let lock = NSLock()
    private var value = Date(timeIntervalSince1970: 1_800_000_000)
    var now: Date { lock.withLock { value } }
    func advance(_ seconds: TimeInterval) { lock.withLock { value += seconds } }
  }

  static func document(kem: String?, kemType: String = "Multikey", pds: String? = pds) -> DIDDocument {
    var methods = [
      VerificationMethod(id: "\(did)#atproto", type: "Multikey", controller: did,
                         publicKeyMultibase: "zQ3shXjHeiBuRCKmM36cuYnm7YEMzhGnCmCyW92sRJ9pribSF"),
    ]
    if let kem {
      methods.append(VerificationMethod(
        id: "\(did)#atproto_kem", type: kemType, controller: did, publicKeyMultibase: kem))
    }
    let services = pds.map {
      [Service(id: "#atproto_pds", type: "AtprotoPersonalDataServer", serviceEndpoint: $0)]
    } ?? []
    return DIDDocument(id: did, verificationMethod: methods, service: services)
  }

  static let kemKey = "zDnaerDaTF5BXEavCrfRZEk316dpbLsfPDZ3WJ5hRTPFU2169"

  func resolve(_ source: FakeSource, clock: Clock = Clock()) async -> MLSChatCapability {
    let resolver = MLSChatCapabilityResolver(source: source, ttl: 60, now: { clock.now })
    return await resolver.capability(for: Self.did)
  }

  // MARK: - Matrix

  @Test func capableWhenKEMDeclarationAndDevicesArePresent() async {
    let source = FakeSource(document: .success(Self.document(kem: Self.kemKey)))
    #expect(await resolve(source) == .capable)
  }

  @Test func missingKEMMethodIsIncapable() async {
    let source = FakeSource(document: .success(Self.document(kem: nil)))
    #expect(await resolve(source) == .incapable(.missingKEMKey))
  }

  @Test func kemMethodOfAnotherTypeIsMissing() async {
    let source = FakeSource(document: .success(
      Self.document(kem: Self.kemKey, kemType: "EcdsaSecp256r1VerificationKey2019")))
    #expect(await resolve(source) == .incapable(.missingKEMKey))
  }

  @Test func emptyKEMKeyIsIncapable() async {
    let source = FakeSource(document: .success(Self.document(kem: "")))
    #expect(await resolve(source) == .incapable(.emptyKEMKey))
  }

  @Test func documentWithoutPDSIsIncapable() async {
    let source = FakeSource(document: .success(Self.document(kem: Self.kemKey, pds: nil)))
    #expect(await resolve(source) == .incapable(.invalidDIDDocument))
  }

  @Test func missingDeclarationIsIncapable() async {
    let source = FakeSource(document: .success(Self.document(kem: Self.kemKey)))
    source.set { $0.declaration = .success(.missing) }
    #expect(await resolve(source) == .incapable(.missingDeclaration))
  }

  @Test func invalidDeclarationIsIncapable() async {
    let source = FakeSource(document: .success(Self.document(kem: Self.kemKey)))
    source.set { $0.declaration = .success(.invalid) }
    #expect(await resolve(source) == .incapable(.invalidDeclaration))
  }

  @Test func zeroDevicesIsIncapable() async {
    let source = FakeSource(document: .success(Self.document(kem: Self.kemKey)))
    source.set { $0.devices = .success(0) }
    #expect(await resolve(source) == .incapable(.noDevices))
  }

  @Test func incompleteDeviceListingIsUnknown() async {
    let source = FakeSource(document: .success(Self.document(kem: Self.kemKey)))
    source.set { $0.devices = .failure(MLSChatIncompleteDeviceListing()) }
    #expect(await resolve(source) == .unknown(.incompleteDeviceListing))
  }

  @Test func networkErrorReadingTheDocumentIsUnknown() async {
    let source = FakeSource(document: .failure(URLError(.notConnectedToInternet)))
    #expect(await resolve(source) == .unknown(.network))
  }

  @Test func networkErrorReadingTheDeclarationIsUnknown() async {
    let source = FakeSource(document: .success(Self.document(kem: Self.kemKey)))
    source.set { $0.declaration = .failure(URLError(.timedOut)) }
    #expect(await resolve(source) == .unknown(.network))
  }

  @Test func serverErrorReadingDevicesIsUnknown() async {
    let source = FakeSource(document: .success(Self.document(kem: Self.kemKey)))
    source.set { $0.devices = .failure(MLSPublicPDSReader.ReaderError.httpStatus(502)) }
    #expect(await resolve(source) == .unknown(.network))
  }

  @Test func malformedDIDIsIncapableWithoutAnyRead() async {
    let source = FakeSource(document: .success(Self.document(kem: Self.kemKey)))
    let resolver = MLSChatCapabilityResolver(source: source)
    #expect(await resolver.capability(for: "not-a-did") == .incapable(.invalidDIDDocument))
    #expect(source.documentReads == 0)
  }

  // MARK: - Cache

  @Test func incapableAnswerExpiresAfterTheTTL() async {
    let clock = Clock()
    let source = FakeSource(document: .success(Self.document(kem: nil)))
    let resolver = MLSChatCapabilityResolver(source: source, ttl: 60, now: { clock.now })
    #expect(await resolver.capability(for: Self.did) == .incapable(.missingKEMKey))

    // The account gains its KEM key; within the TTL the cached answer stands.
    source.set { $0.document = .success(Self.document(kem: Self.kemKey)) }
    clock.advance(59)
    #expect(await resolver.capability(for: Self.did) == .incapable(.missingKEMKey))
    #expect(source.documentReads == 1)

    // Past the TTL the account is read again and is capable.
    clock.advance(2)
    #expect(await resolver.capability(for: Self.did) == .capable)
    #expect(source.documentReads == 2)
  }

  @Test func forceRefreshReadsAgainWithinTheTTL() async {
    let clock = Clock()
    let source = FakeSource(document: .success(Self.document(kem: nil)))
    let resolver = MLSChatCapabilityResolver(source: source, ttl: 60, now: { clock.now })
    _ = await resolver.capability(for: Self.did)
    source.set { $0.document = .success(Self.document(kem: Self.kemKey)) }
    #expect(await resolver.capability(for: Self.did, forceRefresh: true) == .capable)
  }

  @Test func unknownAnswerIsNeverCached() async {
    let clock = Clock()
    let source = FakeSource(document: .failure(URLError(.timedOut)))
    let resolver = MLSChatCapabilityResolver(source: source, ttl: 60, now: { clock.now })
    #expect(await resolver.capability(for: Self.did) == .unknown(.network))
    source.set { $0.document = .success(Self.document(kem: Self.kemKey)) }
    #expect(await resolver.capability(for: Self.did) == .capable)
    #expect(source.documentReads == 2)
  }

  @Test func invalidateDropsTheCachedAnswer() async {
    let source = FakeSource(document: .success(Self.document(kem: nil)))
    let resolver = MLSChatCapabilityResolver(source: source)
    _ = await resolver.capability(for: Self.did)
    source.set { $0.document = .success(Self.document(kem: Self.kemKey)) }
    await resolver.invalidate(did: Self.did)
    #expect(await resolver.capability(for: Self.did) == .capable)
  }

  @Test func defaultTTLIsShort() {
    #expect(MLSChatCapabilityResolver.defaultTTL <= 300)
  }
}
