import Foundation
import Petrel
import PetrelCatbird

public enum MLSDeviceAuthAuthenticationMode: Equatable, Sendable {
  case gateway
  case direct
}

/// A non-secret continuity snapshot for one authenticated client.
///
/// The monotonic generation identifies the exact in-memory authentication
/// boundary. It never contains a bearer token, DPoP proof, JKT, challenge,
/// signature, or key material. The server remains authoritative for checking
/// that begin and complete use the same DPoP key.
public struct MLSDeviceAuthClientSnapshot: Equatable, Sendable {
  public let did: String?
  public let authenticationMode: MLSDeviceAuthAuthenticationMode
  public let authenticationGeneration: UInt64
  /// Exact MLS proxy/service authority used for this request context.
  /// Nil is reserved for non-production test or injected transports.
  public let mlsServiceDID: String?

  public init(
    did: String?,
    authenticationMode: MLSDeviceAuthAuthenticationMode,
    authenticationGeneration: UInt64,
    mlsServiceDID: String? = nil
  ) {
    self.did = did
    self.authenticationMode = authenticationMode
    self.authenticationGeneration = authenticationGeneration
    self.mlsServiceDID = mlsServiceDID
  }
}

public struct MLSDeviceAuthBindingChallenge: Equatable, Sendable {
  public let challengeID: String
  public let challenge: Data
  public let expiresAt: Date
  public let bindingVersion: Int

  public init(
    challengeID: String,
    challenge: Data,
    expiresAt: Date,
    bindingVersion: Int
  ) {
    self.challengeID = challengeID
    self.challenge = challenge
    self.expiresAt = expiresAt
    self.bindingVersion = bindingVersion
  }
}

public struct MLSDeviceAuthBindingCompletion: Equatable, Sendable {
  public let deviceID: String
  public let boundAt: Date
  public let bindingVersion: Int

  public init(deviceID: String, boundAt: Date, bindingVersion: Int) {
    self.deviceID = deviceID
    self.boundAt = boundAt
    self.bindingVersion = bindingVersion
  }
}

/// Volatile, non-authoritative evidence that this exact client completed the
/// server binding flow. This value is deliberately never persisted.
public struct MLSDeviceAuthBindingStatus: Equatable, Sendable {
  public let deviceID: String
  public let bindingVersion: Int
  public let boundAt: Date

  public init(deviceID: String, bindingVersion: Int, boundAt: Date) {
    self.deviceID = deviceID
    self.bindingVersion = bindingVersion
    self.boundAt = boundAt
  }
}

internal struct MLSDeviceAuthBindingValidatedReceipt: Equatable, Sendable {
  let status: MLSDeviceAuthBindingStatus
  let clientSnapshot: MLSDeviceAuthClientSnapshot
}

public enum MLSDeviceAuthBindingError: Error, Equatable, Sendable {
  case directAuthenticationUnsupported
  case sessionChanged
  case malformedResponse
  case invalidChallenge
  case challengeExpired
  case unsupportedVersion
  case signingFailed
  case invalidSignature
  case bindingMismatch
  case concurrentEnrollment
  case transportFailure
}

extension MLSDeviceAuthBindingError: LocalizedError {
  public var errorDescription: String? {
    switch self {
    case .directAuthenticationUnsupported:
      return "MLS device authentication requires a configured gateway session"
    case .sessionChanged:
      return "The authenticated MLS session changed during device binding"
    case .malformedResponse:
      return "The MLS device binding response was malformed"
    case .invalidChallenge:
      return "The MLS device binding challenge was invalid"
    case .challengeExpired:
      return "The MLS device binding challenge expired"
    case .unsupportedVersion:
      return "The MLS device binding version is unsupported"
    case .signingFailed:
      return "The MLS device binding challenge could not be signed"
    case .invalidSignature:
      return "The MLS device binding signature was invalid"
    case .bindingMismatch:
      return "The MLS device binding completion did not match the requested device"
    case .concurrentEnrollment:
      return "Another MLS device binding attempt is already active"
    case .transportFailure:
      return "The MLS device binding request failed"
    }
  }
}

public protocol MLSDeviceAuthBindingAPI: Actor {
  func snapshot() async -> MLSDeviceAuthClientSnapshot
  /// Executes `operation` synchronously only if `expected` is still exact.
  /// Conformers must serialize authentication mutation through the callback;
  /// a separate snapshot check is not sufficient.
  func commitIfSnapshotMatches(
    _ expected: MLSDeviceAuthClientSnapshot,
    operation: @Sendable () -> Bool
  ) async -> Bool
  func begin(deviceID: String) async throws -> MLSDeviceAuthBindingChallenge
  /// Performs begin only when the captured authentication snapshot still owns the request.
  /// The default fails closed so legacy conformers cannot silently send an unscoped mutation.
  func begin(
    deviceID: String,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingChallenge
  func complete(
    challengeID: String,
    signature: Data
  ) async throws -> MLSDeviceAuthBindingCompletion?
  /// Performs completion only when the same captured authentication snapshot still owns the
  /// request. The default fails closed so legacy conformers cannot send an unscoped mutation.
  func complete(
    challengeID: String,
    signature: Data,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingCompletion?
}

extension MLSDeviceAuthBindingAPI {
  public func begin(
    deviceID: String,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingChallenge {
    throw MLSDeviceAuthBindingError.sessionChanged
  }

  public func complete(
    challengeID: String,
    signature: Data,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingCompletion? {
    throw MLSDeviceAuthBindingError.sessionChanged
  }
}

public typealias MLSDeviceAuthChallengeSigner = @Sendable (Data) async throws -> Data

/// Serializes proof-of-possession enrollment for one exact authenticated
/// client and one server-minted device id.
public actor MLSDeviceAuthBindingService {
  private static let bindingVersion = 1
  private static let maximumChallengeBytes = 512
  private static let maximumChallengeIDBytes = 128
  private static let maximumDeviceIDBytes = 64

  private let expectedDID: String
  private let deviceID: String
  private let api: any MLSDeviceAuthBindingAPI
  private let signer: MLSDeviceAuthChallengeSigner
  private let now: @Sendable () -> Date

  private var invalidationGeneration: UInt64 = 0
  private var enrollmentInFlight = false
  private var bindingStatus: MLSDeviceAuthBindingStatus?
  private var boundSnapshot: MLSDeviceAuthClientSnapshot?

  public init(
    expectedDID: String,
    deviceID: String,
    api: any MLSDeviceAuthBindingAPI,
    signer: @escaping MLSDeviceAuthChallengeSigner,
    now: @escaping @Sendable () -> Date = { Date() }
  ) {
    self.expectedDID = expectedDID.trimmingCharacters(in: .whitespacesAndNewlines)
    self.deviceID = deviceID
    self.api = api
    self.signer = signer
    self.now = now
  }

  public func status() async -> MLSDeviceAuthBindingStatus? {
    await validatedReceipt()?.status
  }

  internal func validatedReceipt() async -> MLSDeviceAuthBindingValidatedReceipt? {
    guard let bindingStatus, let boundSnapshot else { return nil }
    let generation = invalidationGeneration
    let currentSnapshot = await api.snapshot()
    guard generation == invalidationGeneration,
      self.bindingStatus == bindingStatus,
      self.boundSnapshot == boundSnapshot
    else {
      return nil
    }
    guard currentSnapshot == boundSnapshot else {
      self.bindingStatus = nil
      self.boundSnapshot = nil
      return nil
    }
    return MLSDeviceAuthBindingValidatedReceipt(
      status: bindingStatus,
      clientSnapshot: boundSnapshot
    )
  }

  internal func commitIfSnapshotMatches(
    _ expected: MLSDeviceAuthClientSnapshot,
    operation: @Sendable () -> Bool
  ) async -> Bool {
    await api.commitIfSnapshotMatches(expected, operation: operation)
  }

  /// Clears all local binding evidence and logically cancels any suspended
  /// enrollment. A request already accepted by the server may finish remotely,
  /// but it can never restore local status after invalidation.
  public func invalidate() {
    invalidationGeneration &+= 1
    bindingStatus = nil
    boundSnapshot = nil
  }

  @discardableResult
  public func bind() async throws -> MLSDeviceAuthBindingStatus {
    guard !enrollmentInFlight else {
      throw MLSDeviceAuthBindingError.concurrentEnrollment
    }
    invalidationGeneration &+= 1
    enrollmentInFlight = true
    bindingStatus = nil
    boundSnapshot = nil
    defer { enrollmentInFlight = false }

    let generation = invalidationGeneration
    let initialSnapshot = await api.snapshot()
    try checkActive(generation: generation)
    try validate(snapshot: initialSnapshot)

    let challenge: MLSDeviceAuthBindingChallenge
    do {
      challenge = try await api.begin(deviceID: deviceID, matching: initialSnapshot)
    } catch is CancellationError {
      throw CancellationError()
    } catch let error as MLSDeviceAuthBindingError {
      throw error
    } catch {
      throw MLSDeviceAuthBindingError.transportFailure
    }

    try checkActive(generation: generation)
    try validate(challenge: challenge)

    let signature: Data
    do {
      signature = try await signer(challenge.challenge)
    } catch is CancellationError {
      throw CancellationError()
    } catch {
      throw MLSDeviceAuthBindingError.signingFailed
    }

    try checkActive(generation: generation)
    guard signature.count == 64 else {
      throw MLSDeviceAuthBindingError.invalidSignature
    }
    guard challenge.expiresAt > now() else {
      throw MLSDeviceAuthBindingError.challengeExpired
    }

    let preCompleteSnapshot = await api.snapshot()
    try checkActive(generation: generation)
    guard preCompleteSnapshot == initialSnapshot else {
      throw MLSDeviceAuthBindingError.sessionChanged
    }

    let completion: MLSDeviceAuthBindingCompletion?
    do {
      completion = try await api.complete(
        challengeID: challenge.challengeID,
        signature: signature,
        matching: initialSnapshot
      )
    } catch is CancellationError {
      throw CancellationError()
    } catch let error as MLSDeviceAuthBindingError {
      throw error
    } catch {
      throw MLSDeviceAuthBindingError.transportFailure
    }

    try checkActive(generation: generation)
    guard let completion else {
      throw MLSDeviceAuthBindingError.malformedResponse
    }
    guard completion.deviceID == deviceID,
      completion.bindingVersion == Self.bindingVersion
    else {
      throw MLSDeviceAuthBindingError.bindingMismatch
    }

    let finalSnapshot = await api.snapshot()
    try checkActive(generation: generation)
    guard finalSnapshot == initialSnapshot else {
      throw MLSDeviceAuthBindingError.sessionChanged
    }

    let status = MLSDeviceAuthBindingStatus(
      deviceID: completion.deviceID,
      bindingVersion: completion.bindingVersion,
      boundAt: completion.boundAt
    )
    bindingStatus = status
    boundSnapshot = finalSnapshot
    return status
  }

  private func checkActive(generation: UInt64) throws {
    try Task.checkCancellation()
    guard generation == invalidationGeneration else {
      throw CancellationError()
    }
  }

  private func validate(snapshot: MLSDeviceAuthClientSnapshot) throws {
    guard snapshot.authenticationMode == .gateway else {
      throw MLSDeviceAuthBindingError.directAuthenticationUnsupported
    }
    guard !expectedDID.isEmpty,
      !expectedDID.contains("#"),
      snapshot.did == expectedDID,
      !deviceID.isEmpty,
      deviceID.utf8.count <= Self.maximumDeviceIDBytes
    else {
      if deviceID.isEmpty || deviceID.utf8.count > Self.maximumDeviceIDBytes {
        throw MLSDeviceAuthBindingError.malformedResponse
      }
      throw MLSDeviceAuthBindingError.sessionChanged
    }
  }

  private func validate(challenge: MLSDeviceAuthBindingChallenge) throws {
    guard !challenge.challengeID.isEmpty,
      challenge.challengeID.utf8.count <= Self.maximumChallengeIDBytes
    else {
      throw MLSDeviceAuthBindingError.malformedResponse
    }
    guard !challenge.challenge.isEmpty,
      challenge.challenge.count <= Self.maximumChallengeBytes
    else {
      throw MLSDeviceAuthBindingError.invalidChallenge
    }
    guard challenge.bindingVersion == Self.bindingVersion else {
      throw MLSDeviceAuthBindingError.unsupportedVersion
    }
    guard challenge.expiresAt > now() else {
      throw MLSDeviceAuthBindingError.challengeExpired
    }
  }
}

internal typealias MLSDeviceAuthExactBeginRequest =
  @Sendable (
    ATProtoClient,
    AuthContinuitySnapshot,
    BlueCatbirdMlsChatBeginDeviceAuthBinding.Input
  ) async throws -> AuthContinuityTransactionResult<
    (
      responseCode: Int,
      data: BlueCatbirdMlsChatBeginDeviceAuthBinding.Output?
    )
  >

internal typealias MLSDeviceAuthExactCompleteRequest =
  @Sendable (
    ATProtoClient,
    AuthContinuitySnapshot,
    BlueCatbirdMlsChatCompleteDeviceAuthBinding.Input
  ) async throws -> AuthContinuityTransactionResult<
    (
      responseCode: Int,
      data: BlueCatbirdMlsChatCompleteDeviceAuthBinding.Output?
    )
  >

internal actor MLSDeviceAuthPetrelAPI: MLSDeviceAuthBindingAPI {
  private let apiClient: MLSAPIClient
  private let client: ATProtoClient
  private let exactBeginRequest: MLSDeviceAuthExactBeginRequest
  private let exactCompleteRequest: MLSDeviceAuthExactCompleteRequest

  init(
    apiClient: MLSAPIClient,
    exactBeginRequest: @escaping MLSDeviceAuthExactBeginRequest = { client, expected, input in
      try await client.performGeneratedRequestWithExactAuthContinuity(matching: expected) {
        try await client.blue.catbird.mlsChat.beginDeviceAuthBinding(input: input)
      }
    },
    exactCompleteRequest: @escaping MLSDeviceAuthExactCompleteRequest = { client, expected, input in
      try await client.performGeneratedRequestWithExactAuthContinuity(matching: expected) {
        try await client.blue.catbird.mlsChat.completeDeviceAuthBinding(input: input)
      }
    }
  ) {
    self.apiClient = apiClient
    self.client = apiClient.client
    self.exactBeginRequest = exactBeginRequest
    self.exactCompleteRequest = exactCompleteRequest
  }

  internal func switchEnvironmentForTesting(_ environment: MLSEnvironment) async {
    await apiClient.switchEnvironment(environment)
  }

  func snapshot() async -> MLSDeviceAuthClientSnapshot {
    let continuity = await client.authContinuitySnapshot()
    let mode: MLSDeviceAuthAuthenticationMode =
      continuity.mode == .gateway ? .gateway : .direct
    return MLSDeviceAuthClientSnapshot(
      did: continuity.did,
      authenticationMode: mode,
      authenticationGeneration: continuity.generation,
      mlsServiceDID: apiClient.mlsServiceDID
    )
  }

  func commitIfSnapshotMatches(
    _ expected: MLSDeviceAuthClientSnapshot,
    operation: @Sendable () -> Bool
  ) async -> Bool {
    guard expected.authenticationMode == .gateway,
      expected.mlsServiceDID == apiClient.mlsServiceDID
    else { return false }
    let expectedContinuity = AuthContinuitySnapshot(
      did: expected.did,
      mode: .gateway,
      generation: expected.authenticationGeneration
    )
    switch await client.performWithExactAuthContinuity(
      matching: expectedContinuity,
      operation
    ) {
    case .performed(let result):
      return result
    case .continuityChanged:
      return false
    }
  }

  func begin(deviceID: String) async throws -> MLSDeviceAuthBindingChallenge {
    let input = BlueCatbirdMlsChatBeginDeviceAuthBinding.Input(deviceId: deviceID)
    let (responseCode, output) = try await client.blue.catbird.mlsChat.beginDeviceAuthBinding(
      input: input)
    guard responseCode == 200 else {
      throw MLSDeviceAuthBindingError.transportFailure
    }
    guard let output else {
      throw MLSDeviceAuthBindingError.malformedResponse
    }
    return MLSDeviceAuthBindingChallenge(
      challengeID: output.challengeId,
      challenge: output.challenge.data,
      expiresAt: output.expiresAt.date,
      bindingVersion: output.bindingVersion
    )
  }

  func begin(
    deviceID: String,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingChallenge {
    try Task.checkCancellation()
    guard expected.authenticationMode == .gateway,
      expected.mlsServiceDID == apiClient.mlsServiceDID
    else {
      throw MLSDeviceAuthBindingError.sessionChanged
    }
    let expectedContinuity = AuthContinuitySnapshot(
      did: expected.did,
      mode: .gateway,
      generation: expected.authenticationGeneration
    )
    let input = BlueCatbirdMlsChatBeginDeviceAuthBinding.Input(deviceId: deviceID)
    let transaction:
      AuthContinuityTransactionResult<
        (
          responseCode: Int,
          data: BlueCatbirdMlsChatBeginDeviceAuthBinding.Output?
        )
      >
    do {
      transaction = try await exactBeginRequest(client, expectedContinuity, input)
      try Task.checkCancellation()
    } catch {
      try Task.checkCancellation()
      throw error
    }
    let response:
      (
        responseCode: Int,
        data: BlueCatbirdMlsChatBeginDeviceAuthBinding.Output?
      )
    switch transaction {
    case .performed(let performed):
      response = performed
    case .continuityChanged:
      throw MLSDeviceAuthBindingError.sessionChanged
    }
    guard response.responseCode == 200 else {
      throw MLSDeviceAuthBindingError.transportFailure
    }
    guard let output = response.data else {
      throw MLSDeviceAuthBindingError.malformedResponse
    }
    return MLSDeviceAuthBindingChallenge(
      challengeID: output.challengeId,
      challenge: output.challenge.data,
      expiresAt: output.expiresAt.date,
      bindingVersion: output.bindingVersion
    )
  }

  func complete(
    challengeID: String,
    signature: Data
  ) async throws -> MLSDeviceAuthBindingCompletion? {
    let input = BlueCatbirdMlsChatCompleteDeviceAuthBinding.Input(
      challengeId: challengeID,
      signature: Bytes(data: signature)
    )
    let (responseCode, output) = try await client.blue.catbird.mlsChat.completeDeviceAuthBinding(
      input: input)
    guard responseCode == 200 else {
      throw MLSDeviceAuthBindingError.transportFailure
    }
    return output.map {
      MLSDeviceAuthBindingCompletion(
        deviceID: $0.deviceId,
        boundAt: $0.boundAt.date,
        bindingVersion: $0.bindingVersion
      )
    }
  }

  func complete(
    challengeID: String,
    signature: Data,
    matching expected: MLSDeviceAuthClientSnapshot
  ) async throws -> MLSDeviceAuthBindingCompletion? {
    try Task.checkCancellation()
    guard expected.authenticationMode == .gateway,
      expected.mlsServiceDID == apiClient.mlsServiceDID
    else {
      throw MLSDeviceAuthBindingError.sessionChanged
    }
    let expectedContinuity = AuthContinuitySnapshot(
      did: expected.did,
      mode: .gateway,
      generation: expected.authenticationGeneration
    )
    let input = BlueCatbirdMlsChatCompleteDeviceAuthBinding.Input(
      challengeId: challengeID,
      signature: Bytes(data: signature)
    )
    let transaction:
      AuthContinuityTransactionResult<
        (
          responseCode: Int,
          data: BlueCatbirdMlsChatCompleteDeviceAuthBinding.Output?
        )
      >
    do {
      transaction = try await exactCompleteRequest(client, expectedContinuity, input)
      try Task.checkCancellation()
    } catch {
      try Task.checkCancellation()
      throw error
    }
    let response:
      (
        responseCode: Int,
        data: BlueCatbirdMlsChatCompleteDeviceAuthBinding.Output?
      )
    switch transaction {
    case .performed(let performed):
      response = performed
    case .continuityChanged:
      throw MLSDeviceAuthBindingError.sessionChanged
    }
    guard response.responseCode == 200 else {
      throw MLSDeviceAuthBindingError.transportFailure
    }
    return response.data.map {
      MLSDeviceAuthBindingCompletion(
        deviceID: $0.deviceId,
        boundAt: $0.boundAt.date,
        bindingVersion: $0.bindingVersion
      )
    }
  }
}
