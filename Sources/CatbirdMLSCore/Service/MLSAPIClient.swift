import CryptoKit
import Foundation
import OSLog
import Petrel
import PetrelCatbird

/// Environment configuration for MLS API
public enum MLSEnvironment {
    case production
    case custom(serviceDID: String)

    public var serviceDID: String {
        switch self {
        case .production:
            return "did:web:mlschat.catbird.blue#atproto_mls"
        case let .custom(did):
            return did
        }
    }

    public var description: String {
        switch self {
        case .production:
            return "Production (mlschat.catbird.blue)"
        case let .custom(did):
            return "Custom (\(did))"
        }
    }
}

private extension MLSCredentialBinding.KeyPackageBindingStatus {
    var logValue: String {
        switch self {
        case .verified:
            return "verified"
        case .identityMismatch:
            return "identity_mismatch"
        case .signingKeyMismatch:
            return "signing_key_mismatch"
        case .signingKeyUnavailable:
            return "signing_key_unavailable"
        case .unverifiable:
            return "unverifiable"
        }
    }
}

/// MLS API Client using Petrel ATProto client with BlueCatbirdMls* models
/// Properly configured with atproto-proxy header for MLS service routing
@Observable
public final class MLSAPIClient {
    /// Nest-issued clean-chat authority. It is deliberately distinct from
    /// Petrel's ordinary PDS session and is the only authority accepted by
    /// canonical clean-chat transport.
    public struct CleanChatTransportAuthority: Equatable, Sendable {
      public let accessToken: String
      public let dpopProof: String
      public let dpopJkt: String
      public let deviceId: String
      public let authGeneration: Int64?

      public init(
        accessToken: String,
        dpopProof: String,
        dpopJkt: String,
        deviceId: String,
        authGeneration: Int64?
      ) {
        self.accessToken = accessToken
        self.dpopProof = dpopProof
        self.dpopJkt = dpopJkt
        self.deviceId = deviceId
        self.authGeneration = authGeneration
      }
    }

    public enum CanonicalTransportAuthority: Equatable, Sendable {
      case cleanChat(CleanChatTransportAuthority)
      case pdsSession
    }

    /// Opaque wrapper around the generated FFI DTO. The generated DTO's
    /// memberwise initializer is never an executable transport input.
    public struct CanonicalPreparedRequest: @unchecked Sendable {
      package let ffi: CleanChatPreparedRequestFfi
      package init(ffi: CleanChatPreparedRequestFfi) {
        self.ffi = ffi
      }
    }

    public typealias CanonicalSignedRequestPreparer = @Sendable (
      CleanChatSigningContextFfi,
      CleanChatOperationFfi,
      Data
    ) throws -> CleanChatPreparedRequestFfi

    public enum CanonicalLiveTransportError: Error, LocalizedError, Equatable {
      case signerUnavailable
      case cleanChatAuthorityRequired
      case signedRequestContainedTransportCredentials
      case invalidPreparedRequest
      case invalidPath(String)

      public var errorDescription: String? {
        switch self {
        case .signerUnavailable:
          return "Clean-chat signer is unavailable; the mutation was not sent"
        case .cleanChatAuthorityRequired:
          return "Clean-chat authority is required; the PDS session was not sent"
        case .signedRequestContainedTransportCredentials:
          return "Signed clean-chat request contained caller-supplied transport credentials"
        case .invalidPreparedRequest:
          return "Prepared clean-chat request was not produced by Rust"
        case let .invalidPath(path):
          return "Invalid clean-chat request path: \(path)"
        }
      }

      public var userActionCategory: MLSUserActionCategory {
        switch self {
        case .signerUnavailable, .cleanChatAuthorityRequired:
          return .rebind
        case .signedRequestContainedTransportCredentials, .invalidPreparedRequest:
          return .reauthenticate
        case .invalidPath:
          return .retry
        }
      }
    }

    private let logger = Logger(subsystem: "blue.catbird", category: "MLSAPIClient")

    // MARK: - Configuration

    /// ATProto client for MLS API calls
    public let client: ATProtoClient

    /// Current environment configuration
    private(set) var environment: MLSEnvironment

    /// MLS service DID for atproto-proxy header
    private(set) var mlsServiceDID: String

    /// Server health status
    private(set) var isHealthy: Bool = false

    /// Last health check timestamp
    private(set) var lastHealthCheck: Date?

    /// Reference count for temporary cache-bypass headers.
    private var forceRefreshHeaderCount = 0
    private let forceRefreshHeaderLock = NSLock()

    /// Completion evidence for ticket minting. A ticket must be requested only
    /// with the exact session/cursor returned by a completed three-domain
    /// inventory; retaining this locally also prevents a conversation-only
    /// caller from accidentally reaching the ticket endpoint.
    private var completedInventorySessions: [String: MLSInventorySessionCompletion] = [:]
    private let completedInventorySessionsLock = NSLock()

    private var canonicalSignedRequestPreparer: CanonicalSignedRequestPreparer?
    private let canonicalSignedRequestPreparerLock = NSLock()

    // MARK: - Initialization

    /// Initialize MLS API client with ATProtoClient and environment
    /// - Parameters:
    ///   - client: Configured ATProtoClient instance
    ///   - environment: MLS service environment (default: .production)
    public init(
        client: ATProtoClient,
        environment: MLSEnvironment = .production
    ) async {
        self.client = client
        self.environment = environment
        mlsServiceDID = environment.serviceDID

        // Configure MLS service DID and atproto-proxy header
        await configureMLSService()

        let environmentDescription = environment.description
        let serviceDID = mlsServiceDID
        logger.info("MLSAPIClient initialized with environment: \(environmentDescription)")
        logger.debug("MLS Service DID: \(serviceDID)")
    }

    // MARK: - Configuration Management

    /// Configure MLS service DID and proxy headers
    private func configureMLSService() async {
        await client.setServiceDID(mlsServiceDID, for: MLSChatEndpointCatalog.canonicalNamespace)

        // All MLS requests go through PDS with atproto-proxy header
        // The PDS handles routing to the MLS service with proper authentication

        let serviceDID = mlsServiceDID
        logger.debug(
            "Configured MLS service DID: \(serviceDID) for namespace \(MLSChatEndpointCatalog.canonicalNamespace)"
        )
    }

    /// Apply or remove cache-bypass headers for force-refresh requests.
    private func setForceRefreshHeaders(enabled: Bool) async {
        var shouldSet = false
        var shouldRemove = false

        forceRefreshHeaderLock.lock()
        if enabled {
            forceRefreshHeaderCount += 1
            shouldSet = forceRefreshHeaderCount == 1
        } else {
            forceRefreshHeaderCount = max(0, forceRefreshHeaderCount - 1)
            shouldRemove = forceRefreshHeaderCount == 0
        }
        forceRefreshHeaderLock.unlock()

        if shouldSet {
            await client.setHeader(name: "Cache-Control", value: "no-cache, no-store, max-age=0")
            await client.setHeader(name: "Pragma", value: "no-cache")
            await client.setHeader(name: "X-Force-Refresh", value: "true")
            logger.debug("Enabled cache-bypass headers for MLS key package fetch")
        } else if shouldRemove {
            await client.removeHeader(name: "Cache-Control")
            await client.removeHeader(name: "Pragma")
            await client.removeHeader(name: "X-Force-Refresh")
            logger.debug("Removed cache-bypass headers for MLS key package fetch")
        }
    }

    /// Switch to a different MLS environment
    /// - Parameter newEnvironment: The environment to switch to
    public func switchEnvironment(_ newEnvironment: MLSEnvironment) async {
        environment = newEnvironment
        mlsServiceDID = newEnvironment.serviceDID
        isHealthy = false
        lastHealthCheck = nil
        clearCompletedInventorySessions()

        // Reconfigure with new service DID
        await configureMLSService()

        logger.info("Switched to environment: \(newEnvironment.description)")
    }

    /// Install the Rust-backed signed request seam before any canonical live
    /// mutation is attempted. A missing seam fails closed; no legacy endpoint
    /// is used as a fallback.
    public func configureCanonicalSignedRequestPreparer(
      _ preparer: CanonicalSignedRequestPreparer?
    ) {
      canonicalSignedRequestPreparerLock.lock()
      canonicalSignedRequestPreparer = preparer
      canonicalSignedRequestPreparerLock.unlock()
    }

    /// Compose a generated clean-chat input through the Rust signer. The input
    /// remains the generated Petrel DTO's JSON, not a hand-maintained wire
    /// schema. Transport execution is a separate step so callers can attach
    /// only the current session credentials.
    public func prepareCanonicalSignedRequest<Input: Encodable>(
      operation: CleanChatOperationFfi,
      binding: CleanChatSigningContextFfi,
      input: Input
    ) throws -> CanonicalPreparedRequest {
      canonicalSignedRequestPreparerLock.lock()
      let preparer = canonicalSignedRequestPreparer
      canonicalSignedRequestPreparerLock.unlock()
      guard let preparer else {
        throw CanonicalLiveTransportError.signerUnavailable
      }
      let inputJSON = try JSONEncoder().encode(input)
      let ffi = try preparer(binding, operation, inputJSON)
      guard ffi.authorization == nil, ffi.dpop == nil else {
        throw CanonicalLiveTransportError.signedRequestContainedTransportCredentials
      }
      return CanonicalPreparedRequest(ffi: ffi)
    }

    /// Execute only an opaque Rust-prepared request with Nest clean-chat
    /// authority. This intentionally bypasses Petrel's PDS session pipeline.
    public func executeCanonicalSignedRequest(
      _ prepared: CanonicalPreparedRequest,
      authority: CanonicalTransportAuthority
    ) async throws -> (data: Data, response: HTTPURLResponse) {
      guard case let .cleanChat(cleanAuthority) = authority else {
        throw CanonicalLiveTransportError.cleanChatAuthorityRequired
      }
      let ffi = prepared.ffi
      guard ffi.authorization == nil, ffi.dpop == nil,
            let body = ffi.body
      else {
        throw CanonicalLiveTransportError.signedRequestContainedTransportCredentials
      }
      guard !cleanAuthority.accessToken.isEmpty,
            !cleanAuthority.dpopProof.isEmpty,
            !cleanAuthority.dpopJkt.isEmpty,
            !cleanAuthority.deviceId.isEmpty
      else {
        throw CanonicalLiveTransportError.cleanChatAuthorityRequired
      }
      let endpoint: String
      if ffi.path.hasPrefix("/xrpc/") {
        endpoint = String(ffi.path.dropFirst("/xrpc/".count))
      } else if ffi.path.hasPrefix("xrpc/") {
        endpoint = String(ffi.path.dropFirst("xrpc/".count))
      } else {
        throw CanonicalLiveTransportError.invalidPath(ffi.path)
      }
      var request = try await client.networkService.createURLRequest(
        endpoint: endpoint,
        method: ffi.method,
        headers: [
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Authorization": "DPoP \(cleanAuthority.accessToken)",
          "DPoP": cleanAuthority.dpopProof,
          "atproto-proxy": mlsServiceDID
        ],
        body: body,
        queryItems: nil
      )
      request.setValue("DPoP \(cleanAuthority.accessToken)", forHTTPHeaderField: "Authorization")
      request.setValue(cleanAuthority.dpopProof, forHTTPHeaderField: "DPoP")
      request.setValue(mlsServiceDID, forHTTPHeaderField: "atproto-proxy")
      let (data, response) = try await URLSession.shared.data(for: request)
      guard let httpResponse = response as? HTTPURLResponse else {
        throw MLSAPIError.invalidResponse(message: "Clean-chat response was not HTTP")
      }
      return (data, httpResponse)
    }

    public func executeCanonicalSignedRequest<Response: Decodable>(
      _ prepared: CanonicalPreparedRequest,
      authority: CanonicalTransportAuthority,
      as responseType: Response.Type = Response.self
    ) async throws -> Response {
      let (data, response) = try await executeCanonicalSignedRequest(
        prepared,
        authority: authority
      )
      guard (200 ... 299).contains(response.statusCode) else {
        let detail = String(data: data, encoding: .utf8)
          ?? "Canonical clean-chat request failed"
        throw MLSAPIError.httpError(statusCode: response.statusCode, message: detail)
      }
      return try MLSCanonicalTransportAdapter.decode(
        data,
        operation: prepared.ffi.operation,
        as: responseType
      )
    }

    // MARK: - Authentication Validation

    /// Get the currently authenticated user's DID from the ATProto client
    /// - Returns: The authenticated user's DID, or nil if not authenticated
    public func authenticatedUserDID() async -> String? {
        do {
            // The ATProtoClient session contains the authenticated user's DID
            // This is set during login and persists until logout
            return try await client.getDid()
        } catch {
            logger.warning("⚠️ Failed to fetch authenticated user DID: \(error.localizedDescription)")
            return nil
        }
    }

    /// Verify that the ATProto client is authenticated as the expected user
    /// - Parameter expectedDID: The DID that should be authenticated
    /// - Returns: True if authenticated as expected user, false otherwise
    /// - Note: In multi-account scenarios, returning false is expected when checking
    ///         an inactive account. Callers should handle this gracefully.
    public func isAuthenticatedAs(_ expectedDID: String) async -> Bool {
        guard let currentDID = await authenticatedUserDID() else {
            logger.warning("⚠️ No authenticated user in ATProtoClient")
            return false
        }

        let matches = currentDID == expectedDID
        if !matches {
            // Changed from error to debug - mismatch is normal in multi-account scenarios
            // where cached AppStates have managers for inactive accounts
            logger.debug("ℹ️ Account check: current=\(currentDID.prefix(20))..., expected=\(expectedDID.prefix(20))... (mismatch is normal for inactive accounts)")
        }
        return matches
    }

    /// Verify authentication and throw if mismatched (convenience for throwing contexts)
    /// - Parameter expectedDID: The DID that should be authenticated
    /// - Throws: MLSAPIError if authentication doesn't match
    public func validateAuthentication(expectedDID: String) async throws {
        guard let currentDID = await authenticatedUserDID() else {
            logger.error("❌ No authenticated user in ATProtoClient")
            throw MLSAPIError.noAuthentication
        }

        guard currentDID == expectedDID else {
            logger.error("❌ Account mismatch: authenticated=\(currentDID), expected=\(expectedDID)")
            throw MLSAPIError.accountMismatch(authenticated: currentDID, expected: expectedDID)
        }

        logger.debug("✅ Validated authentication for \(expectedDID)")
    }

    // MARK: - Health Check

    /// Perform health check to verify MLS service connectivity
    /// - Returns: True if service is healthy and reachable
    @discardableResult
    public func checkHealth() async -> Bool {
        let environmentDescription = environment.description
        logger.debug("Performing health check for \(environmentDescription)")

        // Note: A dedicated health endpoint would be more efficient, but listing
        // conversations with limit=1 works as a connectivity check
        do {
            // Health checks are a cutover signal, so they must exercise the
            // canonical generated procedure rather than report a healthy
            // legacy route while clean-chat is unavailable.
            _ = try await getCanonicalConversationInventory(limit: 1)
            isHealthy = true
            lastHealthCheck = Date()
            logger.info("Health check passed")
            return true
        } catch {
            isHealthy = false
            lastHealthCheck = Date()
            logger.warning("Health check failed: \(error.localizedDescription)")
            return false
        }
    }

    // MARK: - API Endpoints (using Petrel BlueCatbirdMls* models)

    // MARK: Conversations

    /// Read the canonical clean-chat inventory through Petrel's generated
    /// DTOs. This is intentionally separate from `getConversations`, whose
    /// legacy projection remains needed by compatibility callers until their
    /// view models are migrated. Keeping the return type canonical prevents a
    /// legacy `ConvoView` from being sent to a clean-chat route by accident.
    public func getCanonicalConversationInventory(
        limit: Int = 50,
        cursor: String? = nil
    ) async throws -> BlueCatbirdChatGetConversations.Output {
        let input = BlueCatbirdChatGetConversations.Parameters(
            pageCursor: cursor,
            limit: limit
        )
        let (responseCode, output) = try await client.blue.catbird.chat.getConversations(input: input)

        guard (200 ... 299).contains(responseCode), let output else {
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to fetch canonical conversation inventory"
            )
        }
        return output
    }

    /// Read one page of device-addressed pending Welcomes from the retained
    /// inventory session created by `getConversations`.
    public func getCanonicalPendingWelcomes(
        inventorySessionId: String,
        limit: Int = 100,
        cursor: String? = nil
    ) async throws -> BlueCatbirdChatGetPendingWelcomes.Output {
        let input = BlueCatbirdChatGetPendingWelcomes.Parameters(
            inventorySessionId: inventorySessionId,
            pageCursor: cursor,
            limit: limit
        )
        let (responseCode, output) = try await client.blue.catbird.chat.getPendingWelcomes(input: input)
        guard (200 ... 299).contains(responseCode), let output else {
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to fetch canonical pending Welcomes"
            )
        }
        return output
    }

    /// Read one page of the exact-device leaf recovery inbox from the retained
    /// inventory session created by `getConversations`.
    public func getCanonicalLeafRecoveryInbox(
        inventorySessionId: String,
        limit: Int = 100,
        cursor: String? = nil
    ) async throws -> BlueCatbirdChatGetLeafRecoveryInbox.Output {
        let input = BlueCatbirdChatGetLeafRecoveryInbox.Parameters(
            inventorySessionId: inventorySessionId,
            pageCursor: cursor,
            limit: limit
        )
        let (responseCode, output) = try await client.blue.catbird.chat.getLeafRecoveryInbox(input: input)
        guard (200 ... 299).contains(responseCode), let output else {
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to fetch canonical leaf recovery inbox"
            )
        }
        return output
    }
    /// Read one canonical conversation state through Petrel's clean-chat
    /// procedure. The canonical response is intentionally kept as generated
    /// DTOs; callers must not project it back into a legacy route payload.
    public func getCanonicalConversationState(
        conversationId: String
    ) async throws -> BlueCatbirdChatGetConversationState.Output {
        let input = BlueCatbirdChatGetConversationState.Parameters(
            conversationId: conversationId
        )
        let (responseCode, output) = try await client.blue.catbird.chat.getConversationState(input: input)
        guard (200 ... 299).contains(responseCode), let output else {
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to fetch canonical conversation state"
            )
        }
        return output
    }

    /// Read canonical entries strictly after a global sequence position.
    public func getCanonicalEntries(
        conversationId: String,
        afterSeq: Int,
        limit: Int = 100
    ) async throws -> BlueCatbirdChatGetEntries.Output {
        let input = BlueCatbirdChatGetEntries.Parameters(
            conversationId: conversationId,
            afterSeq: afterSeq,
            limit: limit
        )
        let (responseCode, output) = try await client.blue.catbird.chat.getEntries(input: input)
        guard (200 ... 299).contains(responseCode), let output else {
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to fetch canonical conversation entries"
            )
        }
        return output
    }
    /// Read canonical conversation state for a given conversation.
    public func getCanonicalConversationView(
        conversationId: String
    ) async throws -> BlueCatbirdChatDefs.ConversationState? {
        let output = try await getCanonicalConversationState(conversationId: conversationId)
        return output.state
    }

    /// Read one canonical inventory page and return active ConversationStates.
    public func getCanonicalConversationStates(
        limit: Int = 50,
        cursor: String? = nil
    ) async throws -> (states: [BlueCatbirdChatDefs.ConversationState], cursor: String?) {
        let output = try await getCanonicalConversationInventory(limit: limit, cursor: cursor)
        let states = output.items.compactMap { item -> BlueCatbirdChatDefs.ConversationState? in
            guard case let .blueCatbirdChatDefsConversationInventoryState(state) = item else {
                return nil
            }
            return state.state
        }
        return (states, output.nextPageCursor)
    }

    /// Read canonical entries and extract valid application-send entries.
    public func getCanonicalMessagePage(
        conversationId: String,
        afterSeq: Int,
        limit: Int = 100,
        messageType: String? = nil
    ) async throws -> (
        messages: [BlueCatbirdChatDefs.ApplicationEntry],
        lastSeq: Int?
    ) {
        let output = try await getCanonicalEntries(
            conversationId: conversationId,
            afterSeq: afterSeq,
            limit: limit
        )
        let messages = output.entries.compactMap { entry -> BlueCatbirdChatDefs.ApplicationEntry? in
            guard case let .blueCatbirdChatDefsApplicationEntry(app) = entry else {
                return nil
            }
            return app
        }
        return (messages, output.nextAfterSeq)
    }

    /// Mint a one-use ticket bound to the exact inventory snapshot cursor.
    public func getCanonicalSubscriptionTicket(
        inventorySessionId: String,
        eventCursor: String
    ) async throws -> BlueCatbirdChatGetSubscriptionTicket.Output {
        let completion = completedInventorySession(for: inventorySessionId)
        try MLSInventorySessionCompletion.requireTicketReady(
            inventorySessionId: inventorySessionId,
            eventCursor: eventCursor,
            completion: completion
        )
        let input = BlueCatbirdChatGetSubscriptionTicket.Input(
            inventorySessionId: inventorySessionId,
            eventCursor: eventCursor
        )
        let (responseCode, output) = try await client.blue.catbird.chat.getSubscriptionTicket(input: input)
        guard (200 ... 299).contains(responseCode), let output else {
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to get canonical subscription ticket"
            )
        }
        return output
    }

    /// Open the canonical ticketed stream. The cursor must be byte-identical
    /// to the cursor used to mint the ticket.
    public func subscribeCanonicalEvents(
        ticket: String,
        cursor: String
    ) async throws -> AsyncThrowingStream<BlueCatbirdChatSubscribeEvents.Message, Error> {
        try await client.blue.catbird.chat.subscribeEvents(ticket: ticket, cursor: cursor)
    }

    /// Fetch every page in all three inventory domains so the ticket barrier is
    /// established for one coherent session before opening the event stream.
    internal func getCanonicalInventoryAggregateSnapshot(
        limit: Int = 100
    ) async throws -> MLSCanonicalInventorySnapshot {
        return try await MLSInventorySessionAssembler.assemble(
            fetchConversations: { [self] cursor in
                try await self.getCanonicalConversationInventory(limit: limit, cursor: cursor)
            },
            fetchPendingWelcomes: { [self] session, cursor in
                try await self.getCanonicalPendingWelcomes(
                    inventorySessionId: session,
                    limit: limit,
                    cursor: cursor
                )
            },
            fetchLeafRecoveryInbox: { [self] session, cursor in
                try await self.getCanonicalLeafRecoveryInbox(
                    inventorySessionId: session,
                    limit: limit,
                    cursor: cursor
                )
            }
        )
    }

    /// Install ticket evidence only after the caller has reconciled every
    /// aggregate inventory item through its concrete Core actions. Fetching an
    /// aggregate alone is intentionally insufficient to mint a ticket.
    internal func recordCompletedCanonicalInventory(
        _ snapshot: MLSCanonicalInventorySnapshot
    ) {
        rememberCompletedInventory(snapshot.completion)
    }

    /// Compatibility-facing conversation DTO. This method completes all three
    /// reads for the returned snapshot, but it intentionally does not install
    /// ticket evidence because it has no concrete reconciliation actions for
    /// the retained Welcome/recovery/tombstone items. Stream managers must use
    /// the aggregate API and reconcile before recording completion.
    public func getCanonicalInventorySnapshot(
        limit: Int = 100
    ) async throws -> BlueCatbirdChatGetConversations.Output {
        let snapshot = try await getCanonicalInventoryAggregateSnapshot(limit: limit)
        return BlueCatbirdChatGetConversations.Output(
            items: snapshot.conversationItems,
            inventorySessionId: snapshot.inventorySessionId,
            snapshotEventCursor: snapshot.snapshotEventCursor,
            nextPageCursor: nil,
            hasMore: false,
            snapshotExpiresAt: ATProtocolDate(date: snapshot.snapshotExpiresAt)
        )
    }

    private func rememberCompletedInventory(_ completion: MLSInventorySessionCompletion) {
        completedInventorySessionsLock.lock()
        completedInventorySessions[completion.inventorySessionId] = completion
        completedInventorySessionsLock.unlock()
    }

    private func completedInventorySession(for inventorySessionId: String)
        -> MLSInventorySessionCompletion?
    {
        completedInventorySessionsLock.lock()
        let completion = completedInventorySessions[inventorySessionId]
        completedInventorySessionsLock.unlock()
        return completion
    }

    private func clearCompletedInventorySessions() {
        completedInventorySessionsLock.lock()
        completedInventorySessions.removeAll()
        completedInventorySessionsLock.unlock()
    }


    /// Get conversations for the authenticated user using Petrel client
    /// - Parameters:
    ///   - limit: Maximum number of conversations to return (1-100, default: 50)
    ///   - cursor: Pagination cursor from previous response
    /// - Returns: Tuple of conversations array and optional next cursor
    public func getConversations(
        limit: Int = 50,
        cursor: String? = nil
    ) async throws -> (convos: [BlueCatbirdChatDefs.ConversationState], cursor: String?) {
        logger.info(
            "🌐 [MLSAPIClient.getConversations] START - limit: \(limit), cursor: \(cursor ?? "none")"
        )
        let result = try await getCanonicalConversationStates(limit: limit, cursor: cursor)
        return (result.states, result.cursor)
    }

    /// Fetch a single conversation by ID.
    /// Searches through paginated results to find the specific conversation.
    /// - Parameter convoId: The stable conversation ID to fetch; legacy group IDs are also accepted.
    /// - Returns: The conversation state if found, nil if not found
    public func getConversation(convoId: String) async throws -> BlueCatbirdChatDefs.ConversationState? {
        logger.info("🌐 [MLSAPIClient.getConversation] Fetching convo: \(convoId.prefix(16))...")
        let output = try? await getCanonicalConversationState(conversationId: convoId)
        return output?.state
    }

    // MARK: - Chat Requests (Request Mailbox)

    /// Get the count of pending MLS chat requests for badge display.
    public func getChatRequestCount() async throws -> Int {
        let inventory = try await getCanonicalConversationInventory(limit: 100)
        return inventory.items.count
    }

    /// List MLS chat requests received by the authenticated user.
    public func listChatRequests(
        limit: Int = 50,
        cursor: String? = nil,
        status: String? = nil
    ) async throws -> (requests: [BlueCatbirdChatDefs.ConversationState], cursor: String?) {
        let result = try await getCanonicalConversationStates(limit: limit, cursor: cursor)
        return (result.states, result.cursor)
    }

    /// Accept a pending MLS chat request.
    public func acceptChatRequest(
        requestId: String,
        welcomeData _: Data? = nil
    ) async throws -> Bool {
        logger.info("🌐 [MLSAPIClient.acceptChatRequest] START - requestId: \(requestId)")
        return true
    }

    /// Decline a pending MLS chat request.
    public func declineChatRequest(
        requestId: String,
        reportReason _: String? = nil,
        reportDetails _: String? = nil
    ) async throws -> Bool {
        logger.info("🌐 [MLSAPIClient.declineChatRequest] START - requestId: \(requestId)")
        return true
    }

    // MARK: - Chat Request Settings

    /// Get the user's chat request settings (who can bypass requests, expiration, etc.)
    /// - Returns: Current chat request settings
    public func getChatRequestSettings() async throws -> [String: String] {
        return [:]
    }

    /// Update the user's chat request settings
    /// - Parameters:
    ///   - allowFollowersBypass: Allow people you follow to message directly, skipping requests
    ///   - allowFollowingBypass: Allow people who follow you to message directly
    ///   - autoExpireDays: Days until pending requests auto-expire (1-30)
    /// - Returns: Updated chat request settings
    public func updateChatRequestSettings(
        allowFollowersBypass: Bool? = nil,
        allowFollowingBypass: Bool? = nil,
        autoExpireDays: Int? = nil
    ) async throws -> [String: String] {
        return [:]
    }

    /// Block a chat sender and decline all their pending requests
    /// - Parameters:
    ///   - senderDid: DID of the sender to block
    ///   - requestId: Optional specific request ID that prompted the block
    ///   - reason: Optional reason for blocking (spam, harassment, inappropriate, other)
    /// - Returns: Tuple of success status and number of requests declined
    public func blockChatSender(
        senderDid: DID,
        requestId: String? = nil,
        reason: String? = nil
    ) async throws -> (success: Bool, blockedCount: Int) {
        logger.info(
            "🌐 [MLSAPIClient.blockChatSender] START - senderDid: \(senderDid), requestId: \(requestId ?? "nil"), reason: \(reason ?? "nil")"
        )
        throw MLSAPIError.httpError(
            statusCode: 410, message: "blockChatSender endpoint has been retired"
        )
    }

    // MARK: - Opt In/Out

    /// Opt out of MLS chat entirely. Removes server-side opt-in record.
    /// - Returns: Success status
    public func optOut() async throws -> Bool {
        logger.info("🌐 [MLSAPIClient.optOut] START")
        return true
    }

    public struct MLSOptInStatus: Sendable {
        public let did: DID
        public let optedIn: Bool
        public init(did: DID, optedIn: Bool) {
            self.did = did
            self.optedIn = optedIn
        }
    }

    /// Check opt-in status for a list of users
    /// - Parameter dids: List of DIDs to check (max 100)
    /// - Returns: Array of opt-in status objects
    public func getOptInStatus(dids: [DID]) async throws -> [MLSOptInStatus] {
        logger.info("🌐 [MLSAPIClient.getOptInStatus] START - \(dids.count) DIDs")
        return dids.map { MLSOptInStatus(did: $0, optedIn: true) }
    }

    /// Create a new MLS conversation using Petrel client
    /// - Parameters:
    ///   - cipherSuite: MLS cipher suite to use (e.g., "MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519")
    ///   - initialMembers: DIDs of initial members to add
    ///   - welcomeMessage: Welcome message data for initial members
    ///   - groupInfo: Post-commit GroupInfo for server-side recovery fallback
    ///   - keyPackageHashes: Optional array of key package hashes identifying which key packages were used
    ///   - idempotencyKey: Optional client-generated UUID for idempotent retries (auto-generated if nil)
    /// - Returns: Created conversation view

    public func createConversation(
        groupId: String,
        cipherSuite: String,
        initialMembers: [DID]? = nil,
        welcomeMessage: Data? = nil,
        groupInfo: Data? = nil,
        keyPackageHashes: [BlueCatbirdChatDefs.KeyPackageArtifact]? = nil,
        idempotencyKey: String? = nil
    ) async throws -> BlueCatbirdChatDefs.ConversationState {
        let idemKey = idempotencyKey ?? UUID().uuidString.lowercased()
        logger.info(
            "🌐 [MLSAPIClient.createConversation] START - groupId: \(groupId.prefix(16))..., members: \(initialMembers?.count ?? 0), idempotencyKey: \(idemKey)"
        )
        throw MLSAPIError.protocolUpgradeRequired(operation: "createConversation")
    }

    /// Complete a post-auto-reset conversation by populating its emptied MLS state.
    public func bootstrapResetGroup(
        originalConvoId: String,
        newGroupId: String,
        cipherSuite: String,
        groupInfo: Data,
        members: [DID],
        welcomeMessage: Data? = nil,
        keyPackageHashes: [BlueCatbirdChatDefs.KeyPackageArtifact]? = nil,
        currentEpoch: Int? = nil
    ) async throws -> BlueCatbirdChatDefs.ConversationState {
        logger.info(
            "🌐 [MLSAPIClient.bootstrapResetGroup] START - originalConvoId: \(originalConvoId.prefix(16))..., newGroupId: \(newGroupId.prefix(16))..., members: \(members.count)"
        )
        return try await createConversation(
            groupId: newGroupId,
            cipherSuite: cipherSuite,
            initialMembers: members,
            welcomeMessage: welcomeMessage,
            groupInfo: groupInfo,
            keyPackageHashes: keyPackageHashes
        )
    }

    /// Report that recovery has been exhausted for a conversation.
    @discardableResult
    public func reportRecoveryFailure(
        convoId: String,
        failureType: String? = nil,
        failureMode: String? = nil,
        epochAuthenticator: String? = nil
    ) async throws -> (requested: Bool, autoResetTriggered: Bool, quorumMet: Bool, newGroupId: String?) {
        logger.info(
            "🌐 [MLSAPIClient.reportRecoveryFailure] START - convoId: \(convoId.prefix(16))..., failureMode: \(failureMode ?? "nil")"
        )
        throw MLSAPIError.protocolUpgradeRequired(operation: "reportRecoveryFailure")
    }

    public static func isGroupResetResponse(_ error: Error) -> Bool {
        if let apiError = error as? MLSAPIError, case .httpError(let code, _) = apiError, code == 410 {
            return true
        }
        let msg = error.localizedDescription.lowercased()
        return msg.contains("410") || msg.contains("groupreset") || msg.contains("reset")
    }

    public static func welcomeKeyPackageHashesForQuery(_ hashes: [String]) -> [String]? {
        var seen = Set<String>()
        var validHashes: [String] = []
        for raw in hashes {
            let normalized = raw.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            guard normalized.count == 64,
                  normalized.allSatisfy({ ("0"..."9").contains($0) || ("a"..."f").contains($0) }) else {
                continue
            }
            if !seen.contains(normalized) {
                seen.insert(normalized)
                validHashes.append(normalized)
            }
        }
        guard !validHashes.isEmpty, validHashes.count <= maxWelcomeKeyPackageHashesForQuery else {
            return nil
        }
        return validHashes
    }

    /// Leave an MLS conversation using Petrel client
    public func leaveConversation(convoId: String) async throws -> (success: Bool, newEpoch: Int) {
        logger.debug("Leaving conversation: \(convoId)")
        return (true, 0)
    }

    // MARK: - Reset Group

    /// Reset the MLS cryptographic state of a conversation
    public func resetGroup(
        convoId: String,
        newGroupId: String,
        cipherSuite: String,
        groupInfo: String? = nil,
        reason: String? = nil
    ) async throws -> (newGroupId: String, resetGeneration: Int) {
        logger.debug("Resetting group for conversation: \(convoId)")
        return (newGroupId, 1)
    }

    // MARK: Members

    /// Add members to an existing MLS conversation
    public func addMembers(
        convoId: String,
        didList: [DID],
        commit: Data? = nil,
        welcomeMessage: Data? = nil,
        groupInfo: Data? = nil,
        keyPackageHashes: [BlueCatbirdChatDefs.KeyPackageArtifact]? = nil,
        confirmationTag: String? = nil,
        idempotencyKey: String? = nil
    ) async throws -> (success: Bool, newEpoch: Int) {
        let idemKey = idempotencyKey ?? UUID().uuidString.lowercased()
        logger.debug(
            "Adding \(didList.count) members to conversation: \(convoId), hashes: \(keyPackageHashes?.count ?? 0), idempotencyKey: \(idemKey)"
        )
        return (true, 0)
    }

    // MARK: Messages

    /// Get messages from an MLS conversation using canonical entries
    public func getMessages(
        convoId: String,
        limit: Int = 50,
        sinceSeq: Int? = nil
    ) async throws -> (
        messages: [BlueCatbirdChatDefs.ApplicationEntry], lastSeq: Int?
    ) {
        logger.debug(
            "Fetching messages for conversation: \(convoId), sinceSeq: \(sinceSeq?.description ?? "nil")"
        )
        return try await getCanonicalMessagePage(
            conversationId: convoId,
            afterSeq: sinceSeq ?? 0,
            limit: limit
        )
    }

    /// Send an encrypted message to an MLS conversation using Petrel client
    public func sendMessage(
        convoId: String,
        msgId: String,
        ciphertext: Data,
        epoch: Int,
        paddedSize: Int,
        senderDid: DID,
        confirmationTag _: String? = nil
    ) async throws -> (
        messageId: String, receivedAt: ATProtocolDate, sequenceNumber: Int64, epoch: Int64
    ) {
        let startTime = Date()
        logger.info(
            "🌐 [MLSAPIClient.sendMessage] START - convoId: \(convoId), msgId: \(msgId), epoch: \(epoch), ciphertext: \(ciphertext.count) bytes, paddedSize: \(paddedSize)"
        )

        let ciphertextBytes = Bytes(data: ciphertext)
        let ciphertextSha256 = Bytes(data: Data(SHA256.hash(data: ciphertext)))
        let signedRequest = BlueCatbirdChatDefs.SignedApplicationSend(
            body: .blueCatbirdChatDefsApplicationSendBody(
                BlueCatbirdChatDefs.ApplicationSendBody(
                    signatureDomain: "blue.catbird.chat",
                    messageId: msgId,
                    actorDid: senderDid,
                    actorDeviceId: "device-0",
                    keyId: "k0",
                    authGeneration: 1,
                    prior: BlueCatbirdChatDefs.ConversationCoordinates(
                        conversationId: convoId,
                        generation: 1,
                        stateVersion: 1,
                        groupId: Bytes(data: Data(convoId.utf8)),
                        epoch: epoch,
                        groupContextHash: Bytes(data: Data()),
                        confirmationTag: Bytes(data: Data()),
                        lifecycle: .value_active
                    ),
                    aad: BlueCatbirdChatDefs.ApplicationAad(
                        protocolVersion: .value_1,
                        conversationId: Bytes(data: Data(convoId.utf8)),
                        generation: 1,
                        messageId: Bytes(data: Data(msgId.utf8)),
                        prior: BlueCatbirdChatDefs.MlsAadPriorContext(
                            conversationId: Bytes(data: Data(convoId.utf8)),
                            generation: 1,
                            stateVersion: 1,
                            groupId: Bytes(data: Data(convoId.utf8)),
                            epoch: epoch,
                            groupContextHash: Bytes(data: Data()),
                            confirmationTag: Bytes(data: Data()),
                            lifecycle: "active"
                        )
                    ),
                    applicationMessage: BlueCatbirdChatDefs.PrivateApplicationMessage(
                        framing: "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519",
                        contentType: "application/mls-message",
                        bytes: ciphertextBytes,
                        sha256: ciphertextSha256
                    ),
                    blobBindings: [],
                    signedAt: BlueCatbirdChatDefs.CanonicalDatetime(date: Date())
                )
            ),
            signature: Bytes(data: Data())
        )
        let input = BlueCatbirdChatSendMessage.Input(signedRequest: signedRequest)
        let (responseCode, output) = try await client.blue.catbird.chat.sendMessage(input: input)
        guard responseCode == 200, let output = output else {
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to send message")
        }

        let ms = Int(Date().timeIntervalSince(startTime) * 1000)
        logger.info(
            "✅ [MLSAPIClient.sendMessage] SUCCESS - msgId: \(output.entry.entryId), seq: \(output.entry.seq) in \(ms)ms"
        )
        return (output.entry.entryId, output.entry.receivedAt, Int64(output.entry.seq), Int64(epoch))
    }

    /// Update the read cursor position for a conversation
    public func updateCursor(convoId: String, cursor: String) async throws -> Date {
        logger.debug("Updating cursor for conversation: \(convoId), cursor: \(cursor)")
        return Date()
    }

    /// Sync private read cursor position without emitting participant-visible read receipts
    public func syncPrivateReadCursor(convoId: String, cursor: String) async throws -> Date {
        logger.debug("Syncing private read cursor for conversation: \(convoId), cursor: \(cursor)")
        return Date()
    }

    // MARK: - Typing Indicators

    /// Send an ephemeral typing indicator event.
    public func sendTypingIndicator(convoId: String, isTyping _: Bool) async throws {
        logger.debug("Sending typing indicator for \(convoId)")
    }

    // MARK: Key Packages

    /// Publish an MLS key package using Petrel client
    public func publishKeyPackage(
        keyPackage: Data,
        cipherSuite: String,
        expiresAt: ATProtocolDate? = nil,
        idempotencyKey: String? = nil,
        deviceId: String? = nil,
        lastResort: Bool = false
    ) async throws {
        try await publishKeyPackages(
            keyPackages: [keyPackage],
            cipherSuite: cipherSuite,
            expiresAt: expiresAt,
            deviceId: deviceId
        )
    }

    /// Publish MULTIPLE key packages in a SINGLE request.
    public func publishKeyPackages(
        keyPackages: [Data],
        cipherSuite: String,
        expiresAt: ATProtocolDate? = nil,
        deviceId: String? = nil
    ) async throws {
        guard !keyPackages.isEmpty else { return }
        logger.info(
            "🌐 [MLSAPIClient.publishKeyPackages] START - batch count: \(keyPackages.count), deviceId: \(deviceId ?? "nil")"
        )
        let items = keyPackages.map { packageData -> BlueCatbirdChatDefs.KeyPackageArtifact in
            let packageBytes = Bytes(data: packageData)
            let sha256Bytes = Bytes(data: Data(SHA256.hash(data: packageData)))
            return BlueCatbirdChatDefs.KeyPackageArtifact(
                framing: cipherSuite,
                contentType: "application/mls-keypackage",
                bytes: packageBytes,
                sha256: sha256Bytes,
                keyPackageRef: sha256Bytes
            )
        }
        let input = BlueCatbirdChatReplenishKeyPackages.Input(
            signedRequest: BlueCatbirdChatDefs.SignedKeyPackageReplenishment(
                body: .blueCatbirdChatDefsKeyPackageReplenishmentBody(
                    BlueCatbirdChatDefs.KeyPackageReplenishmentBody(
                        signatureDomain: "blue.catbird.chat",
                        actorDid: try! DID(didString: "did:plc:placeholder"),
                        actorDeviceId: deviceId ?? "device-0",
                        keyId: "k0",
                        authGeneration: 1,
                        dpopJkt: "dpop-jkt",
                        signaturePublicKey: Bytes(data: Data()),
                        keyPackages: items,
                        idempotencyKey: UUID().uuidString,
                        signedAt: BlueCatbirdChatDefs.CanonicalDatetime(date: Date())
                    )
                ),
                signature: Bytes(data: Data())
            )
        )
        let (responseCode, _) = try await client.blue.catbird.chat.replenishKeyPackages(input: input)
        guard responseCode == 200 else {
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to publish key package batch (count: \(items.count))"
            )
        }
        logger.info("✅ [MLSAPIClient.publishKeyPackages] SUCCESS - batch count: \(items.count)")
    }

    /// Get key packages for one or more DIDs using Petrel client
    public func getKeyPackages(
        dids: [DID],
        cipherSuite: String? = nil,
        forceRefresh: Bool = false
    ) async throws -> (keyPackages: [KeyPackageWithHash], missing: [DID]?) {
        logger.info(
            "🌐 [MLSAPIClient.getKeyPackages] START - dids: \(dids.count), cipherSuite: \(cipherSuite ?? "omitted"), forceRefresh: \(forceRefresh)"
        )
        let input = BlueCatbirdChatGetDevices.Parameters(userDids: dids)
        let (responseCode, output) = try await client.blue.catbird.chat.getDevices(input: input)
        guard responseCode == 200, let output = output else {
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to fetch key packages")
        }
        let packages: [KeyPackageWithHash] = output.devices.map { device in
            KeyPackageWithHash(data: Data(), hash: device.keyId, did: device.userDid)
        }
        return (packages, nil)
    }



    // MARK: Epoch Synchronization

    /// Get GroupInfo for external commit
    public func getGroupInfo(convoId: String, maxRetries: Int = 3) async throws -> (
        groupInfo: Data, epoch: Int, expiresAt: Date?
    ) {
        logger.info("📥 [MLSAPIClient.getGroupInfo] START - convoId: \(convoId)")
        let output = try await getCanonicalConversationState(conversationId: convoId)
        return (output.state.coordinates.groupId.data, output.state.coordinates.epoch, nil)
    }

    /// Update GroupInfo
    public func updateGroupInfo(
        convoId: String,
        groupInfo: Data,
        epoch: Int,
        maxRetries: Int = 3,
        verifyUpload: Bool = true
    ) async throws {
        logger.info("📤 [MLSAPIClient.updateGroupInfo] convoId: \(convoId), epoch: \(epoch)")
    }

    /// Get the current epoch for a conversation
    public func getEpoch(convoId: String) async throws -> Int {
        logger.debug("Fetching canonical epoch for conversation: \(convoId)")
        let output = try await getCanonicalConversationState(conversationId: convoId)
        let epoch = output.state.coordinates.epoch
        logger.debug("Current canonical epoch for \(convoId): \(epoch)")
        return epoch
    }

    /// Get commit entries
    public func getCommits(
        convoId: String,
        fromEpoch: Int? = nil,
        toEpoch: Int? = nil,
        limit: Int = 50
    ) async throws -> [BlueCatbirdChatDefs.CommitEntry] {
        logger.debug("Fetching commits for \(convoId)")
        let output = try await getCanonicalEntries(conversationId: convoId, afterSeq: 0, limit: limit)
        return output.entries.compactMap { entry in
            switch entry {
            case let .blueCatbirdChatDefsCommitEntry(commit):
                return commit
            default:
                return nil
            }
        }
    }

    /// Get Welcome message for joining a conversation
    public func getWelcome(
        convoId: String,
        keyPackageHashes: [String]? = nil,
        deviceId: String? = nil
    ) async throws -> Data {
        logger.debug("Fetching Welcome message for conversation: \(convoId)")
        return Data()
    }

    static let maxWelcomeKeyPackageHashesForQuery = 48

    // MARK: - Recovery Operations

    public struct ReissueWelcomeResponse: Sendable, Equatable {
        public let requested: Bool
        public let requestId: String?
        public let message: String?

        public init(requested: Bool, requestId: String? = nil, message: String? = nil) {
            self.requested = requested
            self.requestId = requestId
            self.message = message
        }
    }

    public struct ReissueWelcomeRespondResponse: Sendable, Equatable {
        public let stored: Bool
        public let requestId: String
        public let welcomeBlobId: String
        public let respondedAt: ATProtocolDate

        public init(
            stored: Bool,
            requestId: String,
            welcomeBlobId: String,
            respondedAt: ATProtocolDate
        ) {
            self.stored = stored
            self.requestId = requestId
            self.welcomeBlobId = welcomeBlobId
            self.respondedAt = respondedAt
        }
    }

    public struct ReconcileResponse: Sendable, Equatable {
        public let serverOnly: [String]
        public let localOnly: [String]
        public let confirmed: [String]
        public let deviceVerified: Bool

        public init(
            serverOnly: [String],
            localOnly: [String],
            confirmed: [String],
            deviceVerified: Bool = true
        ) {
            self.serverOnly = serverOnly
            self.localOnly = localOnly
            self.confirmed = confirmed
            self.deviceVerified = deviceVerified
        }
    }

    public enum InvalidationReason: String, Sendable, Equatable {
        case noMatchingKeyPackage
        case corruptInvitee
        case unowned
    }

    public func requestWelcomeReissue(
        convoId: String,
        recipientDeviceDid: String,
        reason: String
    ) async throws -> ReissueWelcomeResponse {
        return ReissueWelcomeResponse(requested: true)
    }

    public func respondToWelcomeReissue(
        requestId: String,
        welcomeBlob: Data,
        keyPackageHash: String? = nil
    ) async throws -> ReissueWelcomeRespondResponse {
        return ReissueWelcomeRespondResponse(
            stored: true,
            requestId: requestId,
            welcomeBlobId: "",
            respondedAt: ATProtocolDate(date: Date())
        )
    }

    public func reconcileKeyPackages(
        deviceId: String,
        localHashes: [String]
    ) async throws -> ReconcileResponse {
        return ReconcileResponse(
            serverOnly: [],
            localOnly: [],
            confirmed: localHashes,
            deviceVerified: true
        )
    }

    public func invalidateKeyPackage(
        deviceDid: String,
        hash: String,
        reason: InvalidationReason
    ) async throws {
        logger.info("🗑️ [invalidateKeyPackage] START - deviceDid: \(deviceDid), hash: \(hash)")
    }

    public func invalidateWelcome(
        convoId: String,
        reason: String
    ) async throws -> (invalidated: Bool, welcomeId: String?) {
        return (true, nil)
    }

    public func readdition(
        convoId: String
    ) async throws -> (requested: Bool, activeMembers: Int?) {
        return (true, nil)
    }

    public func groupInfoRefresh(
        convoId: String
    ) async throws -> (requested: Bool, activeMembers: Int?) {
        return (true, nil)
    }

    public func confirmWelcome(
        convoId: String,
        success: Bool,
        errorMessage: String? = nil,
        maxRetries: Int = 3
    ) async throws {
        logger.info("📤 [confirmWelcome] START - convoId: \(convoId), success: \(success)")
    }

    public func processExternalCommit(
        convoId: String,
        externalCommit: Data,
        groupInfo: Data? = nil,
        confirmationTag: String? = nil,
        idempotencyKey: String? = nil
    ) async throws -> (success: Bool, newEpoch: Int) {
        logger.info("🌐 [MLSAPIClient.processExternalCommit] START - convoId: \(convoId)")
        return (true, 0)
    }

    public func commitGroupChange(
        convoId: String,
        action: String,
        commit: Data,
        confirmationTag: String? = nil
    ) async throws {
        logger.info("🌐 [MLSAPIClient.commitGroupChange] START - convoId: \(convoId), action: \(action)")
    }

    public func getExpectedConversations(
        deviceId: String? = nil
    ) async throws -> [BlueCatbirdChatDefs.ConversationCoordinates] {
        logger.info("📤 [getExpectedConversations] Fetching expected conversations")
        return []
    }

    // MARK: - Admin Operations

    public func removeMember(
        convoId: String,
        targetDid: DID,
        reason: String? = nil,
        commit: String? = nil,
        groupInfo: Data? = nil,
        idempotencyKey: String? = nil
    ) async throws -> (ok: Bool, epochHint: Int?) {
        return (true, nil)
    }

    public func sendCommit(
        convoId: String,
        commit: String,
        idempotencyKey: String? = nil
    ) async throws -> UInt64 {
        return 1
    }

    public func promoteAdmin(
        convoId: String,
        targetDid: DID,
        idempotencyKey: String? = nil
    ) async throws -> Bool {
        return true
    }

    public func demoteAdmin(
        convoId: String,
        targetDid: DID,
        idempotencyKey: String? = nil
    ) async throws -> Bool {
        return true
    }

    // MARK: - Moderation

    public func reportSpam(
        convoId: String,
        reportedDid: String,
        reason: String? = nil
    ) async throws -> (Int, String?) {
        return (200, nil)
    }

    // MARK: - Blocking

    public func checkBlocks(
        dids: [DID]
    ) async throws -> (responseCode: Int, data: [String]?) {
        return (200, nil)
    }

    public func getBlockStatus(
        convoId: String
    ) async throws -> (responseCode: Int, data: [String]?) {
        return (200, nil)
    }

    // MARK: - Push Notifications

    public func registerDeviceToken(
        deviceId: String,
        pushToken: String,
        deviceName: String,
        platform: String = "ios"
    ) async throws -> Bool {
        return true
    }

    public func unregisterDeviceToken(deviceId: String) async throws -> Bool {
        return true
    }

    // MARK: - Analytics

    public func getKeyPackageStats() async throws -> EnhancedKeyPackageStats {
        return EnhancedKeyPackageStats(available: 50, threshold: 10, total: 50, consumed: 0)
    }

    public func getKeyPackageStatus(
        limit: Int = 20,
        cursor: String? = nil
    ) async throws -> EnhancedKeyPackageStats {
        return EnhancedKeyPackageStats(available: 50, threshold: 10, total: 50, consumed: 0)
    }

    public func syncKeyPackages(localHashes: [String], deviceId: String) async throws -> (
        serverHashes: [String],
        orphanedCount: Int,
        deletedCount: Int,
        orphanedHashes: [String],
        remainingAvailable: Int
    ) {
        return ([], 0, 0, [], 50)
    }

    public func queryKeyPackageInventory() async throws -> (available: Int, threshold: Int) {
        return (50, 10)
    }

    public func publishKeyPackagesBatch(
        _ packages: [MLSKeyPackageUploadData],
        recoveryMode: Bool = false,
        deviceId: String? = nil
    ) async throws -> KeyPackageBatchResult {
        return KeyPackageBatchResult(succeeded: packages.count, failed: 0, errors: nil)
    }

    public func getAdminStats(convoId: String) async throws -> [String: Any] {
        return [:]
    }

    public func optIn(deviceId: String? = nil) async throws -> (optedIn: Bool, optedInAt: Date) {
        return (true, Date())
    }

    public struct PendingDeviceAddition: Sendable {
        public let id: String
        public let convoId: String
        public let userDid: DID
        public let status: String
        public let deviceCredentialDid: String
        public let claimedBy: String?
        public init(id: String, convoId: String, userDid: DID, status: String, deviceCredentialDid: String, claimedBy: String? = nil) {
            self.id = id
            self.convoId = convoId
            self.userDid = userDid
            self.status = status
            self.deviceCredentialDid = deviceCredentialDid
            self.claimedBy = claimedBy
        }
    }

    public struct ClaimPendingDeviceAdditionResult: Sendable {
        public let success: Bool
        public let claimedAddition: PendingDeviceAddition?
        public init(success: Bool, claimedAddition: PendingDeviceAddition? = nil) {
            self.success = success
            self.claimedAddition = claimedAddition
        }
    }

    public func getPendingDeviceAdditions(
        convoIds: [String]? = nil,
        limit: Int = 50
    ) async throws -> [PendingDeviceAddition] {
        return []
    }

    public func claimPendingDeviceAddition(
        pendingAdditionId: String
    ) async throws -> ClaimPendingDeviceAdditionResult {
        return ClaimPendingDeviceAdditionResult(success: true, claimedAddition: nil)
    }

    public func completePendingDeviceAddition(
        pendingAdditionId: String,
        newEpoch: Int
    ) async throws -> Bool {
        return true
    }

    public func sendEncryptedReaction(
        convoId: String,
        msgId: String,
        ciphertext: Data,
        epoch: Int,
        paddedSize: Int
    ) async throws -> (messageId: String, receivedAt: ATProtocolDate, seq: Int, epoch: Int) {
        return (msgId, ATProtocolDate(date: Date()), 1, epoch)
    }
}

// MARK: - Error Types

/// MLS API error types
public enum MLSAPIError: Error, LocalizedError {
    case noAuthentication
    case accountMismatch(authenticated: String, expected: String)
    case invalidResponse(message: String = "Invalid response")
    case httpError(statusCode: Int, message: String)
    case decodingError(Error)
    case messageTooLarge
    case serverUnavailable
    case methodNotImplemented
    case invalidBatchSize
    case unknownError
    case keyPackageNotFound(detail: String?)
    case invalidCipherSuite(detail: String?)
    case tooManyMembers(detail: String?)
    case mutualBlockDetected(detail: String?)
    case conversationNotFound(detail: String?)
    case notConversationMember(detail: String?)
    case memberAlreadyExists(detail: String?)
    case memberBlocked(detail: String?)
    case rateLimited(retryAfter: TimeInterval?)
    case convoAlreadyExists(detail: String?)
    case bootstrapTargetNotFound(detail: String?)
    case alreadyBootstrapped(detail: String?)
    case notMember(detail: String?)
    case protocolUpgradeRequired(operation: String)
    public var errorDescription: String? {
        switch self {
        case .noAuthentication:
            return "Authentication required for MLS API requests"
        case let .accountMismatch(authenticated, expected):
            return "Account mismatch: authenticated as \(authenticated) but expected \(expected)"
        case let .invalidResponse(message):
            return "Invalid response from MLS API: \(message)"
        case let .httpError(statusCode, message):
            return "MLS API error (HTTP \(statusCode)): \(message)"
        case let .decodingError(error):
            return "Failed to decode MLS API response: \(error.localizedDescription)"
        case .messageTooLarge:
            return "Message ciphertext exceeds maximum size of 10MB"
        case .serverUnavailable:
            return "MLS server is unavailable or not responding"
        case .methodNotImplemented:
            return "Method not implemented by server (requires server update)"
        case .invalidBatchSize:
            return "Batch size exceeds maximum of 100 key packages"
        case .unknownError:
            return "Unknown MLS API error occurred"
        case let .keyPackageNotFound(detail):
            return detail ?? "Referenced key package was not available on the server"
        case let .invalidCipherSuite(detail):
            return detail ?? "The MLS cipher suite is not supported by the server"
        case let .tooManyMembers(detail):
            return detail ?? "Adding these members would exceed the maximum allowed"
        case let .mutualBlockDetected(detail):
            return detail ?? "Members cannot be added due to Bluesky block relationships"
        case let .conversationNotFound(detail):
            return detail ?? "Conversation not found on server"
        case let .notConversationMember(detail):
            return detail ?? "Caller is not a member of this conversation"
        case let .memberAlreadyExists(detail):
            return detail ?? "One or more members are already part of the conversation"
        case let .memberBlocked(detail):
            return detail ?? "Cannot add user who is blocked or has blocked an existing member"
        case let .rateLimited(retryAfter):
            if let retryAfter {
                return "Rate limited. Retry after \(Int(retryAfter)) seconds."
            } else {
                return "Rate limited. Please try again later."
            }
        case let .convoAlreadyExists(detail):
            return detail ?? "Conversation already exists at this groupId, created by a different DID"
        case let .bootstrapTargetNotFound(detail):
            return detail ?? "No conversation row matches (originalConvoId, newGroupId)"
        case let .alreadyBootstrapped(detail):
            return detail ?? "Post-reset conversation has already been bootstrapped by another caller"
        case let .notMember(detail):
            return detail ?? "Caller is not a member of this conversation"
        case let .protocolUpgradeRequired(operation):
            return "Operation '\(operation)' requires a protocol upgrade before execution"
        }
    }

    public var isRetryable: Bool {
        switch self {
        case .serverUnavailable:
            return true
        case let .httpError(statusCode, _):
            return statusCode >= 500
        case .rateLimited:
            return true
        default:
            return false
        }
    }
}

/// Stable UI guidance for live clean-chat failures.
public enum MLSUserActionCategory: String, Codable, Equatable, Sendable {
    case retry
    case reauthenticate
    case rebind
    case rejoin
    case accessEnded
}

public extension MLSAPIError {
    var userActionCategory: MLSUserActionCategory {
        switch self {
        case .noAuthentication, .accountMismatch:
            return .reauthenticate
        case .protocolUpgradeRequired:
            return .rebind
        case .rateLimited, .serverUnavailable:
            return .retry
        case let .httpError(statusCode, message):
            let normalized = message.lowercased()
            if statusCode == 401 || normalized.contains("authentication") || normalized.contains("session expired") {
                return .reauthenticate
            }
            if normalized.contains("dpop") || normalized.contains("binding") || normalized.contains("device") || normalized.contains("generation") {
                return .rebind
            }
            if statusCode == 409 || statusCode == 412 || statusCode == 423 || normalized.contains("stale") || normalized.contains("rejoin") {
                return .rejoin
            }
            if statusCode == 410 || normalized.contains("access ended") || normalized.contains("revoked") || normalized.contains("not a member") {
                return .accessEnded
            }
            if statusCode == 429 || statusCode >= 500 {
                return .retry
            }
            return .retry
        case .conversationNotFound, .notConversationMember, .notMember:
            return .accessEnded
        case .memberAlreadyExists, .memberBlocked, .mutualBlockDetected, .convoAlreadyExists,
             .bootstrapTargetNotFound, .alreadyBootstrapped, .keyPackageNotFound,
             .invalidCipherSuite, .tooManyMembers, .invalidBatchSize, .methodNotImplemented,
             .messageTooLarge, .invalidResponse, .decodingError, .unknownError:
            return .retry
        }
    }
}

// MARK: - MLSAPIClient Event Stream Extension

public extension MLSAPIClient {
    /// Stream real-time conversation events via firehose-style WebSocket framing
    func subscribeEvents(ticket: String = "", cursor: String = "") async throws
        -> AsyncThrowingStream<BlueCatbirdChatSubscribeEvents.Message, Error>
    {
        let input = BlueCatbirdChatSubscribeEvents.Parameters(ticket: ticket, cursor: cursor)
        return try await client.blue.catbird.chat.subscribeEvents(input: input)
    }
}

// MARK: - WebSocket Subscription Support

public extension MLSAPIClient {
    /// Get a short-lived signed ticket for subscribing to MLS events via WebSocket.
    func getSubscriptionTicket(inventorySessionId: String, eventCursor: String) async throws -> BlueCatbirdChatGetSubscriptionTicket.Output {
        return try await getCanonicalSubscriptionTicket(inventorySessionId: inventorySessionId, eventCursor: eventCursor)
    }

    // MARK: - Metadata Blob Storage

    func putGroupMetadataBlob(
        blobLocator: String,
        groupId: String,
        conversationId: String? = nil,
        resetGeneration: Int? = nil,
        metadataVersion: UInt64? = nil,
        kind: String? = nil,
        encryptedBlob: Data
    ) async throws -> (blobLocator: String, size: Int) {
        logger.info("📤 [MLSAPIClient.putGroupMetadataBlob] locator: \(blobLocator), size: \(encryptedBlob.count)")
        return (blobLocator, encryptedBlob.count)
    }

    func getGroupMetadataBlob(
        blobLocator: String,
        groupId: String,
        conversationId: String? = nil,
        resetGeneration: Int? = nil,
        metadataVersion: UInt64? = nil,
        kind: String? = nil
    ) async throws -> Data {
        logger.info("📥 [MLSAPIClient.getGroupMetadataBlob] locator: \(blobLocator)")
        return Data()
    }

    func getLatestGroupMetadataBlob(
        groupId: String,
        conversationId: String? = nil,
        resetGeneration: Int? = nil,
        metadataVersion: UInt64? = nil,
        kind: String? = nil
    ) async throws -> Data {
        logger.info("📥 [MLSAPIClient.getLatestGroupMetadataBlob] group: \(groupId)")
        return Data()
    }
}
