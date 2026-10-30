import CryptoKit
import Foundation

extension MLSConversationManager {
  /// Register an APNs or FCM token with the current device's authenticated MLS
  /// signing authority. The exact signed request is journaled before transport.
  public func registerSignedPushToken(
    _ token: String,
    provider: MLSPushRegistrationProvider,
    appID: String
  ) async throws -> MLSPushRegistrationReceipt {
    try await updateSignedPushRegistration(
      token: token,
      enabled: true,
      provider: provider,
      appID: appID
    )
  }

  /// Disable the current signed push binding. A disable is a new monotonic
  /// generation whose transcript contains SHA-256(empty) and omits the token.
  public func disableSignedPushRegistration(
    provider: MLSPushRegistrationProvider,
    appID: String
  ) async throws -> MLSPushRegistrationReceipt {
    try await updateSignedPushRegistration(
      token: nil,
      enabled: false,
      provider: provider,
      appID: appID
    )
  }

  private func updateSignedPushRegistration(
    token: String?,
    enabled: Bool,
    provider: MLSPushRegistrationProvider,
    appID: String
  ) async throws -> MLSPushRegistrationReceipt {
    guard let userDid, !userDid.isEmpty else { throw MLSAPIError.noAuthentication }
    let capturedGeneration = sessionGeneration
    let authority = try await withRustAuthoritativeRuntime(
      operation: "currentPushSigningAuthority"
    ) { runtime in
      try runtime.currentPushSigningAuthority()
    }
    guard MLSStorageHelpers.normalizeDID(authority.actorDID) == MLSStorageHelpers.normalizeDID(userDid) else {
      throw MLSPushRegistrationError.signingAuthorityChanged
    }
    let tokenDigest = try MLSPushRegistrationCodec.tokenDigest(token, enabled: enabled)
    let entry = try await preparedPushRegistrationEntry(
      userDID: userDid,
      token: token,
      tokenDigest: tokenDigest,
      enabled: enabled,
      provider: provider,
      appID: appID,
      authority: authority
    )

    if entry.state == .active, let bindingDigest = entry.bindingDigest {
      try await requireCurrentPushSigningAuthority(authority)
      try validateSessionGeneration(capturedGeneration: capturedGeneration)
      return MLSPushRegistrationReceipt(
        registrationID: entry.registrationID,
        tokenGeneration: entry.tokenGeneration,
        bindingDigest: bindingDigest,
        enabled: entry.enabled
      )
    }

    let body = try JSONDecoder().decode(MLSPushRegistrationCodec.Body.self, from: entry.bodyJSON)
    let expectedAuthority = MLSPushSigningAuthority(
      actorDID: entry.currentUserDID,
      deviceID: entry.deviceID,
      keyID: entry.keyID,
      authGeneration: entry.authGeneration,
      publicKey: entry.publicKey
    )
    guard try MLSPushRegistrationCodec.transcript(for: body) == entry.transcript,
          let verificationKey = try? Curve25519.Signing.PublicKey(rawRepresentation: entry.publicKey),
          verificationKey.isValidSignature(entry.signature, for: entry.transcript)
    else { throw MLSPushRegistrationError.invalidSignature }
    try await requireCurrentPushSigningAuthority(expectedAuthority)
    try validateSessionGeneration(capturedGeneration: capturedGeneration)

    let (responseData, response) = try await apiClient.submitPreparedRequest(
      method: "POST",
      nsid: "blue.catbird.chat.updatePushToken",
      body: entry.requestJSON,
      query: nil
    )
    guard (200 ... 299).contains(response.statusCode) else {
      let detail = String(data: responseData, encoding: .utf8) ?? "registration failed"
      throw MLSAPIError.httpError(statusCode: response.statusCode, message: detail)
    }
    let receipt = try MLSPushRegistrationCodec.verifyResponse(
      responseData,
      expectedBody: body,
      expectedAuthority: expectedAuthority
    )
    try await requireCurrentPushSigningAuthority(expectedAuthority)
    try validateSessionGeneration(capturedGeneration: capturedGeneration)
    try await databaseManager.write(for: userDid) { db in
      try MLSPushRegistrationJournal.markActive(
        entry,
        bindingDigest: receipt.bindingDigest,
        in: db
      )
    }
    return receipt
  }

  private func preparedPushRegistrationEntry(
    userDID: String,
    token: String?,
    tokenDigest: Data,
    enabled: Bool,
    provider: MLSPushRegistrationProvider,
    appID: String,
    authority: MLSPushSigningAuthority
  ) async throws -> MLSPushRegistrationJournal.Entry {
    let normalizedDID = MLSStorageHelpers.normalizeDID(userDID)
    for _ in 0 ..< 3 {
      let latest = try await databaseManager.read(for: normalizedDID) { db in
        try MLSPushRegistrationJournal.latest(
          userDID: normalizedDID,
          deviceID: authority.deviceID,
          provider: provider,
          appID: appID,
          in: db
        )
      }
      if let latest,
         latest.tokenSHA256 == tokenDigest,
         latest.enabled == enabled,
         latest.keyID == authority.keyID,
         latest.authGeneration == authority.authGeneration,
         latest.publicKey == authority.publicKey,
         MLSPushRegistrationJournal.canReuse(latest)
      { return latest }

      let generation: Int64
      let registrationID: String
      if let latest {
        guard latest.tokenGeneration < MLSPushRegistrationCodec.maxSafeInteger else {
          throw MLSPushRegistrationError.generationExhausted
        }
        generation = latest.tokenGeneration + 1
        registrationID = latest.registrationID
      } else {
        generation = 1
        registrationID = UUID().uuidString.lowercased()
      }
      let body = MLSPushRegistrationCodec.Body(
        authority: authority,
        registrationID: registrationID,
        provider: provider,
        appID: appID,
        tokenGeneration: generation,
        tokenSHA256: tokenDigest,
        enabled: enabled,
        signedAt: MLSPushRegistrationCodec.canonicalTimestamp(Date())
      )
      let transcript = try MLSPushRegistrationCodec.transcript(for: body)
      let signature = try await withRustAuthoritativeRuntime(
        operation: "signPushRegistration"
      ) { runtime in
        try runtime.signPushRegistrationTranscript(
          transcript,
          expectedAuthority: authority
        )
      }
      let bodyJSON = try JSONEncoder().encode(body)
      let requestJSON = try MLSPushRegistrationCodec.requestData(
        body: body,
        signature: signature,
        token: token
      )
      let candidate = MLSPushRegistrationJournal.Entry(
        currentUserDID: normalizedDID,
        deviceID: authority.deviceID,
        provider: provider,
        appID: appID,
        registrationID: registrationID,
        tokenGeneration: generation,
        tokenSHA256: tokenDigest,
        enabled: enabled,
        keyID: authority.keyID,
        authGeneration: authority.authGeneration,
        publicKey: authority.publicKey,
        signedAt: body.signedAt,
        bodyJSON: bodyJSON,
        transcript: transcript,
        signature: signature,
        requestJSON: requestJSON,
        bindingDigest: nil,
        state: .pending
      )
      do {
        return try await databaseManager.write(for: normalizedDID) { db in
          try MLSPushRegistrationJournal.reserve(candidate, in: db)
        }
      } catch let error as MLSPushRegistrationError where error == .invalidField("tokenGeneration") {
        continue
      }
    }
    throw MLSPushRegistrationError.invalidResponse("concurrent registration did not converge")
  }

  private func requireCurrentPushSigningAuthority(
    _ authority: MLSPushSigningAuthority
  ) async throws {
    try await withRustAuthoritativeRuntime(
      operation: "verifyPushSigningAuthority"
    ) { runtime in
      try runtime.requireCurrentPushSigningAuthority(authority)
    }
  }
}
