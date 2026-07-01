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
        // Set the service DID for MLS namespace (blue.catbird.mlsChat)
        // This enables atproto-proxy header routing through the PDS
        await client.setServiceDID(mlsServiceDID, for: "blue.catbird.mlsChat")

        // All MLS requests go through PDS with atproto-proxy header
        // The PDS handles routing to the MLS service with proper authentication

        let serviceDID = mlsServiceDID
        logger.debug("Configured MLS service DID: \(serviceDID) for namespace blue.catbird.mlsChat")
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

        // Reconfigure with new service DID
        await configureMLSService()

        logger.info("Switched to environment: \(newEnvironment.description)")
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
            _ = try await getConversations(limit: 1)
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

    /// Get conversations for the authenticated user using Petrel client
    /// - Parameters:
    ///   - limit: Maximum number of conversations to return (1-100, default: 50)
    ///   - cursor: Pagination cursor from previous response
    /// - Returns: Tuple of conversations array and optional next cursor
    public func getConversations(
        limit: Int = 50,
        cursor: String? = nil
    ) async throws -> (convos: [BlueCatbirdMlsChatDefs.ConvoView], cursor: String?) {
        logger.info(
            "🌐 [MLSAPIClient.getConversations] START - limit: \(limit), cursor: \(cursor ?? "none")"
        )

        let input = BlueCatbirdMlsChatGetConvos.Parameters(
            limit: limit,
            cursor: cursor
        )

        logger.debug("📍 [MLSAPIClient.getConversations] Calling API...")
        let (responseCode, output) = try await client.blue.catbird.mlsChat.getConvos(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.getConversations] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to fetch conversations"
            )
        }

        logger.info(
            "✅ [MLSAPIClient.getConversations] SUCCESS - \(output.conversations.count) conversations, nextCursor: \(output.cursor ?? "none")"
        )
        return (output.conversations, output.cursor)
    }

    /// Fetch a single conversation by ID.
    /// Searches through paginated results to find the specific conversation.
    /// - Parameter convoId: The stable conversation ID to fetch; legacy group IDs are also accepted.
    /// - Returns: The conversation view if found, nil if not found
    public func getConversation(convoId: String) async throws -> BlueCatbirdMlsChatDefs.ConvoView? {
        logger.info("🌐 [MLSAPIClient.getConversation] Fetching convo: \(convoId.prefix(16))...")

        var cursor: String? = nil
        var pageCount = 0

        repeat {
            pageCount += 1
            let result = try await getConversations(limit: 100, cursor: cursor)

            // Check if target conversation is in this page. After a group reset,
            // conversationId stays stable while groupId points at the new MLS group.
            if let convo = result.convos.first(where: {
                MLSConversationIdentity.matches(
                    requestedId: convoId,
                    conversationId: $0.conversationId,
                    groupId: $0.groupId
                )
            }) {
                logger.info("✅ [MLSAPIClient.getConversation] Found convo \(convoId.prefix(16))... on page \(pageCount)")
                return convo
            }

            cursor = result.cursor
        } while cursor != nil && pageCount < 10 // Safety limit

        logger.info("⚠️ [MLSAPIClient.getConversation] Convo \(convoId.prefix(16))... not found after \(pageCount) pages")
        return nil
    }

    // MARK: - Chat Requests (Request Mailbox)

    // TODO: Migrate to mlsChat when consolidated endpoint is available
    // (getRequestCount → getConvos with countOnly, listChatRequests → getConvos with filter,
    //  acceptChatRequest/declineChatRequest → optIn with action)

    /// Get the count of pending MLS chat requests for badge display.
    public func getChatRequestCount() async throws -> BlueCatbirdMlsChatGetConvos.Output {
        // logger.info("🌐 [MLSAPIClient.getChatRequestCount] START") // Reduce log spam

        // 1. Try Primary Endpoint (dedicated count)
        do {
            let input = BlueCatbirdMlsChatGetConvos.Parameters(limit: 1)
            let (responseCode, output) = try await client.blue.catbird.mlsChat.getConvos(input: input)

            // Graceful Handling for 404/501 (Method Not Found / Not Implemented)
            if responseCode == 404 || responseCode == 501 {
                logger.debug(
                    "⚠️ [MLSAPIClient.getChatRequestCount] Endpoint not found (HTTP \(responseCode)) - triggering fallback"
                )
                throw MLSAPIError.httpError(statusCode: responseCode, message: "Endpoint not found")
            }

            guard (200 ... 299).contains(responseCode), let output else {
                logger.error("❌ [MLSAPIClient.getChatRequestCount] HTTP \(responseCode)")
                throw MLSAPIError.httpError(
                    statusCode: responseCode, message: "Failed to fetch chat request count"
                )
            }
            return output

        } catch {
            // 2. Fallback Mechanism (list requests manually)
            // The generated client might throw NetworkError.invalidContentType for 404s if the body isn't JSON.
            // We catch ALL errors here to ensure fallback relies on the alternative endpoint.
            logger.warning(
                "⚠️ [MLSAPIClient.getChatRequestCount] Primary endpoint failed: \(error.localizedDescription)"
            )
            logger.info("🔄 [MLSAPIClient.getChatRequestCount] Falling back to listChatRequests...")

            do {
                // Fetch up to 100 pending requests to estimate the count
                let input = BlueCatbirdMlsChatGetConvos.Parameters(limit: 100)
                let (responseCode, data) = try await client.blue.catbird.mlsChat.getConvos(input: input)

                guard (200 ... 299).contains(responseCode), let data else {
                    throw MLSAPIError.httpError(
                        statusCode: responseCode, message: "Fallback listChatRequests failed"
                    )
                }

                logger.info(
                    "✅ [MLSAPIClient.getChatRequestCount] Fallback SUCCESS - count: \(data.conversations.count)"
                )
                return data
            } catch {
                logger.error(
                    "❌ [MLSAPIClient.getChatRequestCount] Fallback also failed: \(error.localizedDescription)"
                )
                throw error // Throw the fallback error (or could return 0 if we want to be very resilient)
            }
        }
    }

    /// List MLS chat requests received by the authenticated user.
    public func listChatRequests(
        limit: Int = 50,
        cursor: String? = nil,
        status: String? = nil
    ) async throws -> (requests: [BlueCatbirdMlsChatDefs.ConvoView], cursor: String?) {
        logger.info(
            "🌐 [MLSAPIClient.listChatRequests] START - limit: \(limit), cursor: \(cursor ?? "none"), status: \(status ?? "default")"
        )

        let input = BlueCatbirdMlsChatGetConvos.Parameters(
            limit: limit,
            cursor: cursor
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.getConvos(input: input)

        guard (200 ... 299).contains(responseCode), let output else {
            logger.error("❌ [MLSAPIClient.listChatRequests] HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to list chat requests")
        }

        logger.info(
            "✅ [MLSAPIClient.listChatRequests] SUCCESS - \(output.conversations.count) requests, nextCursor: \(output.cursor ?? "none")"
        )
        return (output.conversations, output.cursor)
    }

    /// Accept a pending MLS chat request.
    public func acceptChatRequest(
        requestId: String,
        welcomeData _: Data? = nil
    ) async throws -> BlueCatbirdMlsChatOptIn.Output {
        logger.info("🌐 [MLSAPIClient.acceptChatRequest] START - requestId: \(requestId)")

        let input = try BlueCatbirdMlsChatOptIn.Input(
            action: "acceptRequest",
            dids: [DID(didString: requestId)]
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.optIn(input: input)

        guard (200 ... 299).contains(responseCode), let output else {
            logger.error("❌ [MLSAPIClient.acceptChatRequest] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to accept chat request"
            )
        }

        logger.info("✅ [MLSAPIClient.acceptChatRequest] SUCCESS")
        return output
    }

    /// Decline a pending MLS chat request.
    public func declineChatRequest(
        requestId: String,
        reportReason _: String? = nil,
        reportDetails _: String? = nil
    ) async throws -> Bool {
        logger.info("🌐 [MLSAPIClient.declineChatRequest] START - requestId: \(requestId)")

        let input = try BlueCatbirdMlsChatOptIn.Input(
            action: "declineRequest",
            dids: [DID(didString: requestId)]
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.optIn(input: input)

        guard (200 ... 299).contains(responseCode), let output else {
            logger.error("❌ [MLSAPIClient.declineChatRequest] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to decline chat request"
            )
        }

        logger.info("✅ [MLSAPIClient.declineChatRequest] SUCCESS")
        return output.optedIn == true
    }

    // MARK: - Chat Request Settings

    /// Get the user's chat request settings (who can bypass requests, expiration, etc.)
    /// - Returns: Current chat request settings
    public func getChatRequestSettings() async throws -> BlueCatbirdMlsChatOptIn.Output {
        logger.info("🌐 [MLSAPIClient.getChatRequestSettings] START")

        let input = BlueCatbirdMlsChatOptIn.Input(action: "getSettings")
        let (responseCode, output) = try await client.blue.catbird.mlsChat.optIn(input: input)

        guard (200 ... 299).contains(responseCode), let output else {
            logger.error("❌ [MLSAPIClient.getChatRequestSettings] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to get chat request settings"
            )
        }

        logger.info(
            "✅ [MLSAPIClient.getChatRequestSettings] SUCCESS"
        )
        return output
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
    ) async throws -> BlueCatbirdMlsChatOptIn.Output {
        logger.info(
            "🌐 [MLSAPIClient.updateChatRequestSettings] START - followers: \(allowFollowersBypass?.description ?? "nil"), following: \(allowFollowingBypass?.description ?? "nil"), expire: \(autoExpireDays?.description ?? "nil")"
        )

        let input = BlueCatbirdMlsChatOptIn.Input(
            action: "updateSettings"
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.optIn(input: input)

        guard (200 ... 299).contains(responseCode), let output else {
            logger.error("❌ [MLSAPIClient.updateChatRequestSettings] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to update chat request settings"
            )
        }

        logger.info("✅ [MLSAPIClient.updateChatRequestSettings] SUCCESS")
        return output
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

        // The blue.catbird.mlsChat.blockChatSender lexicon and server endpoint were
        // retired (the chat-request flow was removed). The previous implementation
        // called the dead NSID and failed at runtime; surface that explicitly until
        // this flow is migrated to the current blocking surface (checkBlocks /
        // getBlockStatus + app-level blocks).
        logger.error("❌ [MLSAPIClient.blockChatSender] Endpoint retired server-side")
        throw MLSAPIError.httpError(
            statusCode: 410, message: "blockChatSender endpoint has been retired"
        )
    }

    // MARK: - Opt In/Out

    /// Opt out of MLS chat entirely. Removes server-side opt-in record.
    /// - Returns: Success status
    public func optOut() async throws -> Bool {
        logger.info("🌐 [MLSAPIClient.optOut] START")

        let input = BlueCatbirdMlsChatOptIn.Input(action: "optOut")
        let (responseCode, output) = try await client.blue.catbird.mlsChat.optIn(input: input)

        guard (200 ... 299).contains(responseCode), let output else {
            logger.error("❌ [MLSAPIClient.optOut] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to opt out of MLS chat"
            )
        }

        logger.info("✅ [MLSAPIClient.optOut] SUCCESS")
        return output.optedIn != true
    }

    /// Check opt-in status for a list of users
    /// - Parameter dids: List of DIDs to check (max 100)
    /// - Returns: Array of opt-in status objects
    // TODO: Migrate to mlsChat.optIn(action: "getStatus") when API shape is finalized
    public func getOptInStatus(dids: [DID]) async throws -> [BlueCatbirdMlsChatOptIn.OptInStatus] {
        logger.info("🌐 [MLSAPIClient.getOptInStatus] START - \(dids.count) DIDs")

        let input = BlueCatbirdMlsChatOptIn.Input(action: "getStatus", dids: dids)

        let (responseCode, output) = try await client.blue.catbird.mlsChat.optIn(input: input)

        guard (200 ... 299).contains(responseCode), let output else {
            logger.error("❌ [MLSAPIClient.getOptInStatus] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to get opt-in status"
            )
        }

        let statuses = output.statuses ?? []
        let optedInCount = statuses.filter { $0.optedIn }.count
        logger.info(
            "✅ [MLSAPIClient.getOptInStatus] SUCCESS - \(optedInCount)/\(statuses.count) opted in"
        )
        return statuses
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
        keyPackageHashes: [BlueCatbirdMlsChatCreateConvo.KeyPackageHashEntry]? = nil,
        idempotencyKey: String? = nil
    ) async throws -> BlueCatbirdMlsChatDefs.ConvoView {
        // Generate idempotency key if not provided
        let idemKey = idempotencyKey ?? UUID().uuidString.lowercased()
        logger.info(
            "🌐 [MLSAPIClient.createConversation] START - groupId: \(groupId.prefix(16))..., members: \(initialMembers?.count ?? 0), hashes: \(keyPackageHashes?.count ?? 0), idempotencyKey: \(idemKey)"
        )

        // Phase D removed `metadata` from createConvo input. Title /
        // description / avatar flow through the encrypted blob path
        // (putGroupMetadataBlob) after creation; the UI calls
        // `updateGroupMetadataEncrypted` to seed the first encrypted
        // blob if it has a name to set.
        let input = BlueCatbirdMlsChatCreateConvo.Input(
            groupId: groupId,
            cipherSuite: cipherSuite,
            initialMembers: initialMembers,
            welcomeMessage: welcomeMessage.map { Bytes(data: $0) },
            groupInfo: groupInfo.map { Bytes(data: $0) },
            keyPackageHashes: keyPackageHashes
        )

        logger.debug("📍 [MLSAPIClient.createConversation] Request payload:")
        logger.debug("  - groupId: \(groupId)")
        logger.debug("  - cipherSuite: \(cipherSuite)")
        logger.debug("  - initialMembers: \(initialMembers?.map { $0 } ?? [])")
        logger.debug("  - welcomeMessage length: \(welcomeMessage?.count ?? 0) bytes")
        logger.debug("  - groupInfo length: \(groupInfo?.count ?? 0) bytes")
        if let welcome = welcomeMessage {
            logger.debug("  - welcomeMessage prefix: \(welcome.prefix(50).base64EncodedString().prefix(50))...")
        }
        logger.debug("  - keyPackageHashes: \(keyPackageHashes?.count ?? 0) items")
        if let hashes = keyPackageHashes {
            for (idx, hash) in hashes.enumerated() {
                logger.debug("    [\(idx)] did: \(hash.did), hash: \(hash.hash.prefix(16))...")
            }
        }

        logger.debug("📍 [MLSAPIClient.createConversation] Calling API...")
        do {
            let (responseCode, output) = try await client.blue.catbird.mlsChat.createConvo(input: input)

            guard responseCode == 200, let output = output else {
                logger.error(
                    "❌ [MLSAPIClient.createConversation] HTTP \(responseCode) - no structured error caught"
                )
                throw MLSAPIError.httpError(
                    statusCode: responseCode, message: "Failed to create conversation"
                )
            }

            let convoView = output.convo
            logger.info(
                "✅ [MLSAPIClient.createConversation] SUCCESS - convoId: \(convoView.groupId), epoch: \(convoView.epoch)"
            )
            return convoView
        } catch let error as ATProtoError<BlueCatbirdMlsChatCreateConvo.Error> {
            // Structured error from server - now properly parsed with fixed enum!
            logger.error("❌ [MLSAPIClient.createConversation] Structured error: \(error.error.errorName)")
            logger.error("   Message: \(error.message ?? "no message")")
            logger.error("   Status code: \(error.statusCode)")

            // Log specific details for KeyPackageNotFound errors
            if case .keyPackageNotFound = error.error {
                logger.warning("⚠️ KeyPackageNotFound detected - hash may be exhausted or invalid")
                if let msg = error.message {
                    logger.debug("   Server details: \(msg)")
                }
            }

            throw MLSAPIError(from: error)
        } catch {
            // Catch-all for other errors (network, etc.)
            logger.error("❌ [MLSAPIClient.createConversation] Unexpected error: \(error)")
            logger.error("   Error type: \(type(of: error))")
            throw error
        }
    }

    /// Complete a post-auto-reset conversation by populating its emptied MLS state.
    ///
    /// Spec §8.5 first-responder bootstrap. The post-reset row exists with
    /// `id=originalConvoId`, `group_id=newGroupId`, `group_info=NULL`. This call
    /// UPDATEs that row in place rather than INSERTing a new conversation. First
    /// caller (in the existing member roster) for a given (originalConvoId,
    /// newGroupId) wins; later callers receive `AlreadyBootstrapped` (409) and
    /// must fall back to receiving the Welcome from the winner.
    ///
    /// - Parameters:
    ///   - originalConvoId: Existing convo identifier (preserved across reset).
    ///   - newGroupId: Hex-encoded MLS group ID assigned by the auto-reset.
    ///   - cipherSuite: MLS cipher suite for the bootstrapped group.
    ///   - groupInfo: Raw MLS GroupInfo bytes for the freshly-created group.
    ///   - members: Member DIDs (post-reset roster, fetched via getConversation).
    ///   - welcomeMessage: Optional Welcome bytes to be relayed to other members.
    ///   - keyPackageHashes: Optional key package hashes consumed for member adds.
    ///   - currentEpoch: Optional FFI-actual epoch (defaults to server-assigned 1).
    /// - Returns: ConvoView for the bootstrapped conversation.
    public func bootstrapResetGroup(
        originalConvoId: String,
        newGroupId: String,
        cipherSuite: String,
        groupInfo: Data,
        members: [DID],
        welcomeMessage: Data? = nil,
        keyPackageHashes: [BlueCatbirdMlsChatBootstrapResetGroup.KeyPackageHashEntry]? = nil,
        currentEpoch: Int? = nil
    ) async throws -> BlueCatbirdMlsChatDefs.ConvoView {
        logger.info(
            "🌐 [MLSAPIClient.bootstrapResetGroup] START - originalConvoId: \(originalConvoId.prefix(16))..., newGroupId: \(newGroupId.prefix(16))..., members: \(members.count)"
        )

        let input = BlueCatbirdMlsChatBootstrapResetGroup.Input(
            originalConvoId: originalConvoId,
            newGroupId: newGroupId,
            cipherSuite: cipherSuite,
            groupInfo: Bytes(data: groupInfo),
            members: members,
            welcomeMessage: welcomeMessage.map { Bytes(data: $0) },
            keyPackageHashes: keyPackageHashes,
            currentEpoch: currentEpoch
        )

        do {
            let (responseCode, output) = try await client.blue.catbird.mlsChat.bootstrapResetGroup(
                input: input
            )

            guard responseCode == 200, let output = output else {
                logger.error(
                    "❌ [MLSAPIClient.bootstrapResetGroup] HTTP \(responseCode) - no structured error caught"
                )
                throw MLSAPIError.httpError(
                    statusCode: responseCode, message: "Failed to bootstrap reset group"
                )
            }

            logger.info(
                "✅ [MLSAPIClient.bootstrapResetGroup] SUCCESS - convoId: \(output.convo.conversationId), groupId: \(output.convo.groupId), epoch: \(output.convo.epoch)"
            )
            return output.convo
        } catch let error as ATProtoError<BlueCatbirdMlsChatBootstrapResetGroup.Error> {
            logger.error(
                "❌ [MLSAPIClient.bootstrapResetGroup] Structured error: \(error.error.errorName) (status \(error.statusCode))"
            )
            throw MLSAPIError(from: error)
        } catch let networkError as NetworkError {
            // Petrel doesn't always decode 4xx bodies into the typed lexicon error
            // (the response shape can differ from the generated wrapper). Map the
            // documented bootstrapResetGroup status codes here so the orchestrator
            // can switch on `MLSAPIError` instead of falling through to "unexpected
            // error" and wiping local state on what is really a race-loss (or, more
            // dangerously, on our own retry of our own success — see the 409 path
            // in MLSConversationManager+Sync.attemptFirstResponderBootstrap).
            let code: Int? = {
                switch networkError {
                case let .responseError(code), let .serverError(code, _): return code
                default: return nil
                }
            }()
            if let code {
                logger.error(
                    "❌ [MLSAPIClient.bootstrapResetGroup] HTTP \(code) (NetworkError, no typed body) — mapping to MLSAPIError"
                )
                switch code {
                case 409: throw MLSAPIError.alreadyBootstrapped(detail: nil)
                case 404: throw MLSAPIError.bootstrapTargetNotFound(detail: nil)
                case 403: throw MLSAPIError.notMember(detail: nil)
                default:
                    throw MLSAPIError.httpError(
                        statusCode: code, message: "bootstrapResetGroup failed (NetworkError)"
                    )
                }
            }
            logger.error(
                "❌ [MLSAPIClient.bootstrapResetGroup] NetworkError (no status): \(networkError)"
            )
            throw networkError
        } catch {
            logger.error("❌ [MLSAPIClient.bootstrapResetGroup] Unexpected error: \(error)")
            throw error
        }
    }

    /// Report that recovery has been exhausted for a conversation (spec §8.6 / ADR-008 D1).
    ///
    /// Wraps `blue.catbird.mlsChat.reportRecoveryFailure`. The server collects votes
    /// from members with valid `epochAuthenticator`s and auto-resets the group when
    /// the configured quorum is met. For 1:1 (DM) conversations, a single Mode B
    /// (`group_state_unrecoverable`) report from one client is sufficient to trigger
    /// reset (see `mls-ds/server/src/handlers/mls_chat/report_recovery_failure.rs`).
    ///
    /// This thin wrapper exists so trifecta-detection callers in
    /// `MLSConversationManager` can dispatch Mode B reports directly without
    /// duplicating Petrel input/output type juggling. The existing
    /// `MLSRecoveryManager.recordFailedRejoin` path also fires this endpoint
    /// (Mode A by default; Mode B only when `failureType == "remote_data_error"`)
    /// after the per-conversation attempt counter exhausts. Both paths are
    /// intentional and coexist — server dedupes by `(did, convoId, failureMode)`.
    ///
    /// - Parameters:
    ///   - convoId: Conversation identifier (preserved across resets).
    ///   - failureMode: ADR-008 D1 discriminator. Pass `"group_state_unrecoverable"`
    ///     (Mode B) when the trifecta is observed; `"local_state_loss"` (Mode A)
    ///     for plain attempt-exhaustion. Pass `nil` to leave the field absent
    ///     (server treats as Mode A).
    ///   - failureType: Free-form classifier kept stable across releases for
    ///     server-side analytics (e.g. `"trifecta_external_commit_409"`,
    ///     `"external_commit_exhausted"`, `"remote_data_error"`). Server does not
    ///     enforce a specific set; pick a stable string per call site.
    ///   - epochAuthenticator: RFC 9420 §8.7 epoch_authenticator (hex). When
    ///     non-nil this client's vote counts toward quorum; when nil the server
    ///     records the call but short-circuits as `reason: "missing_authenticator"`.
    ///     Caller should compute via `MLSContext.epochAuthenticator(groupId:)`
    ///     and hex-encode when local group state is still present.
    /// - Returns: Server output (`recorded`, `autoResetTriggered`, `failureCount`,
    ///   `memberCount`, `reason`, optional `newGroupId`/`resetGeneration` when
    ///   the report itself triggered the reset).
    /// - Throws: `MLSAPIError.httpError` on non-200 responses; rethrows
    ///   transport-layer errors from Petrel.
    @discardableResult
    public func reportRecoveryFailure(
        convoId: String,
        failureMode: String? = nil,
        failureType: String? = nil,
        epochAuthenticator: String? = nil
    ) async throws -> BlueCatbirdMlsChatReportRecoveryFailure.Output {
        logger.info(
            "🌐 [MLSAPIClient.reportRecoveryFailure] START - convoId: \(convoId.prefix(16))..., failureMode: \(failureMode ?? "nil"), failureType: \(failureType ?? "nil"), authenticator: \(epochAuthenticator != nil ? "present" : "nil")"
        )

        let input = BlueCatbirdMlsChatReportRecoveryFailure.Input(
            convoId: convoId,
            failureType: failureType,
            failureMode: failureMode,
            epochAuthenticator: epochAuthenticator
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.reportRecoveryFailure(
            input: input
        )

        guard responseCode == 200, let output = output else {
            logger.error(
                "❌ [MLSAPIClient.reportRecoveryFailure] HTTP \(responseCode) - no structured output"
            )
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to report recovery failure for conversation \(convoId)"
            )
        }

        logger.info(
            "✅ [MLSAPIClient.reportRecoveryFailure] SUCCESS - recorded=\(output.recorded), autoReset=\(output.autoResetTriggered), failureCount=\(output.failureCount), memberCount=\(output.memberCount), reason=\(output.reason ?? "nil")"
        )
        return output
    }

    /// Leave an MLS conversation using Petrel client
    /// - Parameter convoId: Conversation identifier
    /// - Returns: Success status and new epoch number
    public func leaveConversation(convoId: String) async throws -> (success: Bool, newEpoch: Int) {
        logger.debug("Leaving conversation: \(convoId)")

        let input = BlueCatbirdMlsChatLeaveConvo.Input(convoId: convoId)
        let (responseCode, output) = try await client.blue.catbird.mlsChat.leaveConvo(input: input)

        guard responseCode == 200, let output = output else {
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to leave conversation")
        }

        logger.debug("Left conversation: \(convoId)")
        return (output.success, output.newEpoch)
    }

    // MARK: - Reset Group

    /// Reset the MLS cryptographic state of a conversation (admin only)
    ///
    /// Creates a fresh MLS group while preserving conversation identity, members, and
    /// message history. All clients will receive a `groupResetEvent` and must join the new group.
    ///
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - newGroupId: New MLS group identifier (hex-encoded)
    ///   - cipherSuite: Cipher suite for the new group
    ///   - groupInfo: Optional base64-encoded GroupInfo for the new group
    ///   - reason: Optional human-readable reason for the reset
    /// - Returns: Reset result with the new group ID and generation number
    public func resetGroup(
        convoId: String,
        newGroupId: String,
        cipherSuite: String,
        groupInfo: String? = nil,
        reason: String? = nil
    ) async throws -> BlueCatbirdMlsChatResetGroup.Output {
        logger.debug("Resetting group for conversation: \(convoId)")

        let input = BlueCatbirdMlsChatResetGroup.Input(
            convoId: convoId,
            newGroupId: newGroupId,
            cipherSuite: cipherSuite,
            groupInfo: groupInfo.flatMap { Data(base64Encoded: $0).map { Bytes(data: $0) } },
            reason: reason
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.resetGroup(input: input)

        guard responseCode == 200, let output = output else {
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to reset group for conversation \(convoId)"
            )
        }

        logger.info(
            "Group reset for \(convoId): newGroupId=\(output.newGroupId.prefix(16)), gen=\(output.resetGeneration)"
        )
        return output
    }

    // MARK: Members

    /// Add members to an existing MLS conversation using Petrel client
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - didList: Array of member DIDs to add
    ///   - commit: MLS Commit message data
    ///   - welcomeMessage: Welcome message data for new members
    ///   - groupInfo: Serialized POST-commit MLS GroupInfo bytes. Server stores
    ///     this atomically inside the same txn that advances the epoch (closes
    ///     the race where the next External-Commit joiner reads stale state).
    ///     Pass nil only if you cannot export post-commit GroupInfo locally —
    ///     server keeps existing (stale) state and logs a warning.
    ///   - keyPackageHashes: Optional array of key package hashes identifying which key packages were used
    ///   - idempotencyKey: Optional client-generated UUID for idempotent retries (auto-generated if nil)
    /// - Returns: Success status and new epoch number
    public func addMembers(
        convoId: String,
        didList: [DID],
        commit: Data? = nil,
        welcomeMessage: Data? = nil,
        groupInfo: Data? = nil,
        keyPackageHashes: [BlueCatbirdMlsChatCommitGroupChange.KeyPackageHashEntry]? = nil,
        confirmationTag: String? = nil,
        idempotencyKey: String? = nil
    ) async throws -> (success: Bool, newEpoch: Int) {
        // Generate idempotency key if not provided
        let idemKey = idempotencyKey ?? UUID().uuidString.lowercased()
        logger.debug(
            "Adding \(didList.count) members to conversation: \(convoId), hashes: \(keyPackageHashes?.count ?? 0), idempotencyKey: \(idemKey)"
        )

        let input = Self.buildAddMembersInput(
            convoId: convoId,
            didList: didList,
            commit: commit,
            welcomeMessage: welcomeMessage,
            groupInfo: groupInfo,
            keyPackageHashes: keyPackageHashes,
            confirmationTag: confirmationTag,
            idempotencyKey: idemKey
        )

        do {
            let (responseCode, output) = try await client.blue.catbird.mlsChat.commitGroupChange(input: input)

            guard responseCode == 200, let output = output else {
                throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to add members")
            }

            logger.debug("Added members to conversation: \(convoId), new epoch: \(output.newEpoch ?? 0)")
            return (output.success, output.newEpoch ?? 0)
        } catch let error as ATProtoError<BlueCatbirdMlsChatCommitGroupChange.Error> {
            logger.error(
                "❌ [MLSAPIClient.addMembers] Lexicon error: \(error.error.errorName) - \(error.message ?? "no details")"
            )
            throw MLSAPIError(from: error)
        }
    }

    // MARK: Messages

    /// Get messages from an MLS conversation using Petrel client
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - limit: Maximum number of messages to return (1-100, default: 50)
    ///   - sinceSeq: Sequence number to fetch messages after (pagination cursor). Messages with seq > sinceSeq are returned.
    /// - Returns: Tuple of messages array (guaranteed sorted by stable conversation seq ASC), optional lastSeq, and optional gapInfo
    /// - Note: Server GUARANTEES messages are pre-sorted by conversation sequence. Epoch is MLS crypto state, not UI order.
    public func getMessages(
        convoId: String,
        limit: Int = 50,
        sinceSeq: Int? = nil
    ) async throws -> (
        messages: [BlueCatbirdMlsChatDefs.MessageView], lastSeq: Int?,
        gapInfo: BlueCatbirdMlsChatGetMessages.GapInfo?
    ) {
        logger.debug(
            "Fetching messages for conversation: \(convoId), sinceSeq: \(sinceSeq?.description ?? "nil")"
        )

        let input = BlueCatbirdMlsChatGetMessages.Parameters(
            convoId: convoId,
            limit: limit,
            sinceSeq: sinceSeq
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.getMessages(input: input)

        guard responseCode == 200, let output = output else {
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to fetch messages")
        }

        logger.debug(
            "Fetched \(output.messages.count) messages, lastSeq: \(output.lastSeq?.description ?? "nil"), hasGaps: \(output.gapInfo?.hasGaps.description ?? "false")"
        )
        return (output.messages, output.lastSeq, output.gapInfo)
    }

    /// Send an encrypted message to an MLS conversation using Petrel client
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - msgId: Message identifier (client-generated)
    ///   - ciphertext: MLS encrypted message ciphertext bytes (MUST be padded to paddedSize, actual size encrypted inside)
    ///   - epoch: MLS epoch number when message was encrypted
    ///   - paddedSize: Padded ciphertext size (bucket size: 512, 1024, 2048, 4096, 8192, or multiples of 8192)
    ///   - senderDid: DID of the message sender
    ///   - idempotencyKey: Optional client-generated UUID for idempotent retries (auto-generated if nil)
    /// - Returns: Tuple of messageId, receivedAt timestamp, server-assigned seq, and echoed epoch
    /// - Note: For metadata privacy, only paddedSize is sent. Actual message size is encrypted inside the MLS ciphertext.
    ///         Server now returns real seq and epoch for immediate cache updates (no placeholder seq=0).
    public func sendMessage(
        convoId: String,
        msgId: String,
        ciphertext: Data,
        epoch: Int,
        paddedSize: Int,
        senderDid _: DID,
        confirmationTag _: String? = nil
    ) async throws -> (
        messageId: String, receivedAt: ATProtocolDate, sequenceNumber: Int64, epoch: Int64
    ) {
        let startTime = Date()
        logger.info(
            "🌐 [MLSAPIClient.sendMessage] START - convoId: \(convoId), msgId: \(msgId), epoch: \(epoch), ciphertext: \(ciphertext.count) bytes, paddedSize: \(paddedSize) (actual size hidden)"
        )

        let input = BlueCatbirdMlsChatSendMessage.Input(
            convoId: convoId,
            msgId: msgId,
            ciphertext: Bytes(data: ciphertext),
            epoch: epoch,
            paddedSize: paddedSize
        )

        logger.debug("📍 [MLSAPIClient.sendMessage] Calling API...")
        let (responseCode, output) = try await client.blue.catbird.mlsChat.sendMessage(input: input)

        guard responseCode == 200, let output = output else {
            let ms = Int(Date().timeIntervalSince(startTime) * 1000)
            logger.error("❌ [MLSAPIClient.sendMessage] HTTP \(responseCode) after \(ms)ms")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to send message")
        }

        let ms = Int(Date().timeIntervalSince(startTime) * 1000)
        logger.info(
            "✅ [MLSAPIClient.sendMessage] SUCCESS - msgId: \(output.messageId), seq: \(output.seq), epoch: \(output.epoch) in \(ms)ms"
        )
        return (output.messageId, output.receivedAt, Int64(output.seq), Int64(output.epoch))
    }

    /// Update the read cursor position for a conversation
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - cursor: Cursor position string (e.g. messageID or opaque cursor)
    /// - Returns: The timestamp when the cursor was updated
    public func updateCursor(convoId: String, cursor: String) async throws -> Date {
        logger.debug(
            "Updating cursor for conversation: \(convoId), cursor: \(cursor)"
        )

        let input = BlueCatbirdMlsChatUpdateCursor.Input(
            convoId: convoId,
            cursor: cursor
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.updateCursor(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ Failed to update cursor for \(convoId): HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to update cursor")
        }

        logger.debug("✅ Updated cursor for \(convoId)")
        return output.updatedAt.date
    }

    /// Sync private read cursor position without emitting participant-visible read receipts
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - cursor: Cursor position to persist
    /// - Returns: The timestamp when cursor position was updated
    public func syncPrivateReadCursor(convoId: String, cursor: String) async throws -> Date {
        logger.debug("Syncing private read cursor for conversation: \(convoId), cursor: \(cursor)")

        let input = BlueCatbirdMlsChatUpdateCursor.Input(
            convoId: convoId,
            cursor: cursor
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.updateCursor(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ Failed to sync private read cursor for \(convoId): HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to sync private read cursor"
            )
        }

        logger.debug("✅ Synced private read cursor for \(convoId)")
        return output.updatedAt.date
    }

    // MARK: - Typing Indicators

    /// Send an ephemeral typing indicator event.
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - isTyping: `true` for start/heartbeat, `false` for stop
    public func sendTypingIndicator(convoId: String, isTyping _: Bool) async throws {
        let noOpCiphertext = Data(repeating: 0, count: 512)
        let input = BlueCatbirdMlsChatSendMessage.Input(
            convoId: convoId,
            msgId: UUID().uuidString,
            ciphertext: Bytes(data: noOpCiphertext),
            epoch: 0,
            paddedSize: noOpCiphertext.count,
            delivery: "ephemeral"
        )

        let (responseCode, _) = try await client.blue.catbird.mlsChat.sendMessage(input: input)
        guard responseCode == 200 else {
            logger.error("❌ Failed to send typing indicator for \(convoId): HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to send typing indicator"
            )
        }
    }

    // MARK: Key Packages

    /// Publish an MLS key package using Petrel client
    /// - Parameters:
    ///   - keyPackage: Base64-encoded MLS key package
    ///   - cipherSuite: Cipher suite of the key package (e.g., "MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519")
    ///   - expiresAt: Optional expiration timestamp
    ///   - idempotencyKey: Optional client-generated UUID for idempotent retries (auto-generated if nil)
    /// - Returns: Success (empty response from server)
    public func publishKeyPackage(
        keyPackage: Data,
        cipherSuite: String,
        expiresAt: ATProtocolDate? = nil,
        idempotencyKey: String? = nil,
        deviceId: String? = nil,
        lastResort: Bool = false
    ) async throws {
        // Generate idempotency key if not provided
        let idemKey = idempotencyKey ?? UUID().uuidString.lowercased()
        logger.debug(
            "Publishing key package with cipher suite: \(cipherSuite), \(keyPackage.count) bytes, idempotencyKey: \(idemKey), lastResort: \(lastResort)"
        )

        let input = BlueCatbirdMlsChatPublishKeyPackages.Input(
            action: "publish",
            keyPackages: [BlueCatbirdMlsChatPublishKeyPackages.KeyPackageItem(
                keyPackage: Bytes(data: keyPackage),
                cipherSuite: cipherSuite,
                expires: expiresAt ?? ATProtocolDate(date: Date().addingTimeInterval(90 * 24 * 60 * 60)),
                lastResort: lastResort ? true : nil
            )],
            deviceId: deviceId
        )

        let (responseCode, _) = try await client.blue.catbird.mlsChat.publishKeyPackages(input: input)

        guard responseCode == 200 else {
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to publish key package"
            )
        }

        logger.debug("Published key package successfully")
    }

    /// Get key packages for one or more DIDs using Petrel client
    /// - Parameters:
    ///   - dids: Array of DIDs to fetch key packages for
    ///   - cipherSuite: Optional filter by cipher suite (e.g., "MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519")
    ///   - forceRefresh: When true, bypasses caches to fetch fresh packages
    /// - Returns: Tuple of available key packages and missing DIDs
    public func getKeyPackages(
        dids: [DID],
        cipherSuite: String? = nil,
        forceRefresh: Bool = false
    ) async throws -> (keyPackages: [BlueCatbirdMlsChatDefs.KeyPackageRef], missing: [DID]?) {
        logger.info(
            "🌐 [MLSAPIClient.getKeyPackages] START - dids: \(dids.count), cipherSuite: \(cipherSuite ?? "omitted"), forceRefresh: \(forceRefresh)"
        )

        let input = BlueCatbirdMlsChatGetKeyPackages.Parameters(
            dids: dids,
            cipherSuite: cipherSuite
        )

        if forceRefresh {
            await setForceRefreshHeaders(enabled: true)
        }
        defer {
            if forceRefresh {
                Task { await self.setForceRefreshHeaders(enabled: false) }
            }
        }

        logger.debug("📍 [MLSAPIClient.getKeyPackages] Calling API...")
        let (responseCode, output) = try await client.blue.catbird.mlsChat.getKeyPackages(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.getKeyPackages] HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to fetch key packages")
        }

        // 🛡️ Deduplicate identical key package payloads (same DID + identical bundle)
        var seenPackages = Set<String>()
        let dedupedPackages = output.keyPackages.filter { kp in
            let signature = "\(kp.did.description)#\(kp.keyPackage)"
            if seenPackages.contains(signature) {
                logger.warning(
                    "⚠️ [MLSAPIClient.getKeyPackages] Duplicate key package payload detected for DID: \(kp.did)"
                )
                return false
            }

            seenPackages.insert(signature)
            return true
        }

        let duplicateCount = output.keyPackages.count - dedupedPackages.count
        if duplicateCount > 0 {
            logger.warning(
                "⚠️ [MLSAPIClient.getKeyPackages] Removed \(duplicateCount) duplicate payload(s); retained \(dedupedPackages.count)"
            )
        }

        // N44 DID-doc signing-key authorization: resolve active `blue.catbird.mlsChat.device`
        // records from each target's PDS. This implements the second half of ADR-009 credential
        // binding. `classifyKeyPackageBinding` uses these keys to transition from warn-only
        // to full enforcement depending on the server's enforce-mode response.
        let requestedDIDDescriptions = dids.map(\.description)
        let requestedDIDsSet = Set(requestedDIDDescriptions)
        var authorizedKeysByDID: [String: [Data]] = [:]

        // Batch fetch device keys for all requested DIDs
        if let collection = try? NSID(nsidString: "blue.catbird.mlsChat.device") {
            await withTaskGroup(of: (String, [Data]?).self) { group in
                for didStr in requestedDIDsSet {
                    group.addTask {
                        guard let did = try? DID(didString: didStr) else { return (didStr, nil) }
                        let input = ComAtprotoRepoListRecords.Parameters(
                            repo: .did(did), collection: collection, limit: 50, cursor: nil, reverse: false
                        )
                        do {
                            let (code, data) = try await self.client.com.atproto.repo.listRecords(input: input)
                            guard (200 ... 299).contains(code), let data else { return (didStr, []) }
                            var keys: [Data] = []
                            for record in data.records {
                                var currentVal = record.value
                                var device: BlueCatbirdMlsChatDevice? = nil
                                while true {
                                    if case let .knownType(typed) = currentVal, let typedDevice = typed as? BlueCatbirdMlsChatDevice {
                                        device = typedDevice
                                        break
                                    } else if case let .unknownType(_, nested) = currentVal {
                                        currentVal = nested
                                    } else {
                                        break
                                    }
                                }
                                if let device {
                                    keys.append(device.mlsSignaturePublicKey.data)
                                }
                            }
                            return (didStr, keys)
                        } catch {
                            return (didStr, nil) // nil signifies fetch failure (warn-only or TOFU allowed)
                        }
                    }
                }
                for await (didStr, keys) in group {
                    if let keys { authorizedKeysByDID[didStr] = keys }
                }
            }
        }

        for kp in dedupedPackages {
            let expectedDID = kp.did.description
            let authorizedKeys = authorizedKeysByDID[expectedDID]

            let classification = MLSCredentialBinding.classifyKeyPackageBinding(
                expectedDID: expectedDID,
                keyPackageData: kp.keyPackage.data,
                authorizedSignatureKeys: authorizedKeys
            )
            let disposition = Self.keyPackageFetchBindingDisposition(
                requestedDIDDescriptions: requestedDIDDescriptions,
                packageDIDDescription: expectedDID,
                classification: classification
            )
            let hashPrefix = Data(SHA256.hash(data: kp.keyPackage.data)).prefix(8).hexEncodedString()

            switch disposition.severity {
            case .ok:
                logger.debug(
                    "✅ [CREDENTIAL-BINDING] mode=warn_only operation=fetch context=getKeyPackages expected_did=\(kp.did.description) claimed_did=\(classification.claimedIdentity ?? "<none>") status=\(classification.status.logValue) kp_hash_prefix=\(hashPrefix) path=ios-swift"
                )
            case .warning:
                logger.warning(
                    "⚠️ [CREDENTIAL-BINDING] mode=warn_only operation=fetch context=getKeyPackages expected_did=\(kp.did.description) claimed_did=\(classification.claimedIdentity ?? "<none>") status=\(classification.status.logValue) kp_hash_prefix=\(hashPrefix) path=ios-swift reason=\(disposition.reason)"
                )
            case .error:
                logger.error(
                    "❌ [CREDENTIAL-BINDING] mode=warn_only operation=fetch context=getKeyPackages expected_did=\(kp.did.description) claimed_did=\(classification.claimedIdentity ?? "<none>") status=\(classification.status.logValue) label_requested=\(disposition.labelWasRequested) kp_hash_prefix=\(hashPrefix) path=ios-swift reason=\(disposition.reason)"
                )
            }
        }

        let requestedDIDs = Set(dids.map { $0.description.lowercased() })
        let returnedDIDs = Set(dedupedPackages.map { $0.did.description.lowercased() })
        let derivedMissing = requestedDIDs.subtracting(returnedDIDs)
        let missing = dids.filter { derivedMissing.contains($0.description.lowercased()) }

        logger.info(
            "✅ [MLSAPIClient.getKeyPackages] SUCCESS - \(dedupedPackages.count) unique packages after deduplication, missing: \(missing.count)"
        )
        return (dedupedPackages, missing.isEmpty ? nil : missing)
    }

    // MARK: Epoch Synchronization

    /// Get GroupInfo for external commit with retry logic
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - maxRetries: Maximum number of retry attempts (default 3)
    /// - Returns: Tuple of groupInfo bytes, epoch, and expiresAt
    public func getGroupInfo(convoId: String, maxRetries: Int = 3) async throws -> (
        groupInfo: Data, epoch: Int, expiresAt: Date?
    ) {
        logger.info("📥 [MLSAPIClient.getGroupInfo] START - convoId: \(convoId)")

        let input = BlueCatbirdMlsChatGetGroupState.Parameters(convoId: convoId, include: "groupInfo")
        var lastError: Error?

        for attempt in 1 ... maxRetries {
            do {
                let (responseCode, output) = try await client.blue.catbird.mlsChat.getGroupState(input: input)

                // Check for transient server errors that warrant retry
                let isTransient = [502, 503, 504].contains(responseCode)
                if isTransient, attempt < maxRetries {
                    let delay = TimeInterval(attempt)
                    logger.warning(
                        "⚠️ [MLSAPIClient.getGroupInfo] Transient error \(responseCode) on attempt \(attempt), retrying in \(delay)s..."
                    )
                    try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                    continue
                }

                guard responseCode == 200, let output = output else {
                    logger.error("❌ [MLSAPIClient.getGroupInfo] HTTP \(responseCode) on attempt \(attempt)")
                    throw MLSAPIError.httpError(
                        statusCode: responseCode,
                        message: "Failed to fetch GroupInfo after \(attempt) attempt(s)"
                    )
                }

                guard let groupInfoBytes = output.groupInfo, !groupInfoBytes.data.isEmpty else {
                    logger.error("❌ [MLSAPIClient.getGroupInfo] Missing or empty GroupInfo for \(convoId)")
                    throw MLSAPIError.invalidResponse(message: "No GroupInfo available")
                }

                // Bytes type wraps raw Data directly - no base64 decoding needed
                let groupInfoData = groupInfoBytes.data

                // Validate minimum size
                guard groupInfoData.count >= Self.minGroupInfoSize else {
                    logger.error(
                        "❌ [MLSAPIClient.getGroupInfo] GroupInfo too small: \(groupInfoData.count) bytes (minimum \(Self.minGroupInfoSize))"
                    )
                    throw MLSAPIError.invalidResponse(
                        message: "Server returned truncated GroupInfo: \(groupInfoData.count) bytes"
                    )
                }

                // 🔒 FIX #5: Log SHA-256 checksum for debugging data corruption
                // Compare this with upload checksum to identify where corruption occurs
                let downloadChecksum = SHA256.hash(data: groupInfoData).compactMap {
                    String(format: "%02x", $0)
                }.joined().prefix(16)
                logger.info(
                    "📥 [MLSAPIClient.getGroupInfo] Download checksum (first 16 chars): \(downloadChecksum)"
                )

                let expiresAt = output.expiresAt.map { $0.date }

                logger.info(
                    "✅ [MLSAPIClient.getGroupInfo] Success on attempt \(attempt) - epoch: \(output.epoch ?? 0), size: \(groupInfoData.count) bytes"
                )
                return (groupInfoData, output.epoch ?? 0, expiresAt)

            } catch let error as MLSAPIError {
                throw error // Don't retry our own errors
            } catch {
                lastError = error
                if attempt < maxRetries {
                    let delay = TimeInterval(attempt)
                    logger.warning(
                        "⚠️ [MLSAPIClient.getGroupInfo] Error on attempt \(attempt): \(error.localizedDescription), retrying in \(delay)s..."
                    )
                    try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                } else {
                    logger.error(
                        "❌ [MLSAPIClient.getGroupInfo] All \(maxRetries) attempts failed for \(convoId)"
                    )
                }
            }
        }

        throw lastError
            ?? MLSAPIError.httpError(statusCode: 500, message: "All \(maxRetries) retry attempts failed")
    }

    /// Minimum valid GroupInfo size in bytes
    private static let minGroupInfoSize = 100

    enum GroupInfoVerificationDisposition: Equatable {
        case verifyStoredBytes
        case acceptConcurrentAdvance(serverEpoch: Int)
        case retryStaleRead(serverEpoch: Int)
    }

    static func groupInfoVerificationDisposition(
        uploadedEpoch: Int,
        storedEpoch: Int
    ) -> GroupInfoVerificationDisposition {
        if storedEpoch == uploadedEpoch {
            return .verifyStoredBytes
        }

        if storedEpoch > uploadedEpoch {
            return .acceptConcurrentAdvance(serverEpoch: storedEpoch)
        }

        return .retryStaleRead(serverEpoch: storedEpoch)
    }

    enum KeyPackageFetchBindingSeverity: Equatable {
        case ok
        case warning
        case error
    }

    struct KeyPackageFetchBindingDisposition: Equatable {
        let labelWasRequested: Bool
        let severity: KeyPackageFetchBindingSeverity
        let reason: String
    }

    static func keyPackageFetchBindingDisposition(
        requestedDIDDescriptions: [String],
        packageDIDDescription: String,
        classification: MLSCredentialBinding.KeyPackageBindingClassification
    ) -> KeyPackageFetchBindingDisposition {
        let requestedRoots = Set(
            requestedDIDDescriptions.map {
                MLSCredentialBinding.credentialRootDID($0).lowercased()
            }
        )
        let packageRoot = MLSCredentialBinding.credentialRootDID(packageDIDDescription).lowercased()
        let labelWasRequested = requestedRoots.contains(packageRoot)

        var reasons: [String] = []
        if !labelWasRequested {
            reasons.append("key package returned for a DID that was not requested")
        }

        switch classification.status {
        case .verified:
            break
        case .signingKeyUnavailable:
            reasons.append("DID signing-key authorization unavailable")
        case .identityMismatch, .signingKeyMismatch, .unverifiable:
            reasons.append(classification.reason ?? "key package binding verification failed")
        }

        let severity: KeyPackageFetchBindingSeverity
        if !labelWasRequested {
            severity = .error
        } else {
            switch classification.status {
            case .verified:
                severity = .ok
            case .signingKeyUnavailable:
                severity = .warning
            case .identityMismatch, .signingKeyMismatch, .unverifiable:
                severity = .error
            }
        }

        return KeyPackageFetchBindingDisposition(
            labelWasRequested: labelWasRequested,
            severity: severity,
            reason: reasons.joined(separator: "; ")
        )
    }

    static func buildReconcileKeyPackagesInput(
        deviceId: String,
        localHashes: [String]
    ) -> BlueCatbirdMlsChatReconcileKeyPackages.Input {
        BlueCatbirdMlsChatReconcileKeyPackages.Input(
            deviceId: deviceId,
            localHashes: localHashes,
            schemaVersion: 2
        )
    }

    /// Pure builder for the `BlueCatbirdMlsChatCommitGroupChange.Input` payload
    /// sent over the wire by `processExternalCommit`.
    ///
    /// This exists as a test seam: the wire-payload construction is otherwise
    /// untestable without spinning up a network stub, and a previous bug had
    /// `processExternalCommit` declaring `groupInfo: Data?` but silently dropping
    /// it before reaching `Input(...)`. Routing the wire build through this
    /// helper makes the contract pinnable in unit tests (see
    /// `MLSAPIClientBuildExternalCommitInputTests`).
    ///
    /// - Parameters:
    ///   - convoId: Conversation identifier.
    ///   - externalCommit: Serialized MLS External Commit bytes.
    ///   - groupInfo: Optional serialized POST-commit MLS GroupInfo bytes.
    ///   - confirmationTag: Optional base64-encoded confirmation tag string;
    ///     decoded to raw `Bytes` for the wire.
    ///   - idempotencyKey: Client-generated idempotency token.
    static func buildExternalCommitInput(
        convoId: String,
        externalCommit: Data,
        groupInfo: Data?,
        confirmationTag: String?,
        idempotencyKey: String
    ) -> BlueCatbirdMlsChatCommitGroupChange.Input {
        BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: convoId,
            action: "externalCommit",
            commit: Bytes(data: externalCommit),
            groupInfo: groupInfo.map { Bytes(data: $0) },
            idempotencyKey: idempotencyKey,
            confirmationTag: confirmationTag.flatMap { Data(base64Encoded: $0) }.map { Bytes(data: $0) }
        )
    }

    /// Pure builder for the `BlueCatbirdMlsChatCommitGroupChange.Input` payload
    /// sent over the wire by `addMembers`.
    ///
    /// This exists as a test seam: the wire-payload construction is otherwise
    /// untestable without spinning up a network stub, and a previous bug had
    /// `addMembers` not accepting a `groupInfo` parameter at all — silently
    /// building an `Input` without it. Routing the wire build through this
    /// helper makes the contract pinnable in unit tests (see
    /// `MLSAPIClientBuildAddMembersInputTests`).
    ///
    /// - Parameters:
    ///   - convoId: Conversation identifier.
    ///   - didList: Array of member DIDs being added.
    ///   - commit: Optional MLS Commit message bytes.
    ///   - welcomeMessage: Optional Welcome message bytes for the new members.
    ///   - groupInfo: Optional serialized POST-commit MLS GroupInfo bytes.
    ///     Server stores this atomically inside the same txn that advances the
    ///     epoch.
    ///   - keyPackageHashes: Optional per-device routing entries identifying
    ///     which key packages were consumed by the commit.
    ///   - confirmationTag: Optional base64-encoded confirmation tag string;
    ///     decoded to raw `Bytes` for the wire.
    ///   - idempotencyKey: Client-generated idempotency token.
    static func buildAddMembersInput(
        convoId: String,
        didList: [DID],
        commit: Data?,
        welcomeMessage: Data?,
        groupInfo: Data?,
        keyPackageHashes: [BlueCatbirdMlsChatCommitGroupChange.KeyPackageHashEntry]?,
        confirmationTag: String?,
        idempotencyKey: String
    ) -> BlueCatbirdMlsChatCommitGroupChange.Input {
        BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: convoId,
            action: "addMembers",
            memberDids: didList,
            commit: commit.map { Bytes(data: $0) },
            welcome: welcomeMessage.map { Bytes(data: $0) },
            groupInfo: groupInfo.map { Bytes(data: $0) },
            keyPackageHashes: keyPackageHashes,
            idempotencyKey: idempotencyKey,
            confirmationTag: confirmationTag.flatMap { Data(base64Encoded: $0) }.map { Bytes(data: $0) }
        )
    }

    /// Pure builder for the `BlueCatbirdMlsChatCommitGroupChange.Input` payload
    /// sent over the wire by `removeMember`.
    ///
    /// This exists as a test seam: the wire-payload construction is otherwise
    /// untestable without spinning up a network stub, and a previous bug had
    /// `removeMember` not accepting a `groupInfo` parameter at all — silently
    /// building an `Input` without it. Routing the wire build through this
    /// helper makes the contract pinnable in unit tests (see
    /// `MLSAPIClientBuildRemoveMemberInputTests`).
    ///
    /// - Parameters:
    ///   - convoId: Conversation identifier.
    ///   - didList: Array of member DIDs being removed.
    ///   - commit: Optional MLS Commit message bytes.
    ///   - groupInfo: Optional serialized POST-commit MLS GroupInfo bytes.
    ///     Server stores this atomically inside the same txn that advances the
    ///     epoch.
    ///   - confirmationTag: Optional base64-encoded confirmation tag string;
    ///     decoded to raw `Bytes` for the wire.
    ///   - idempotencyKey: Client-generated idempotency token.
    static func buildRemoveMemberInput(
        convoId: String,
        didList: [DID],
        commit: Data?,
        groupInfo: Data?,
        confirmationTag: String?,
        idempotencyKey: String
    ) -> BlueCatbirdMlsChatCommitGroupChange.Input {
        BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: convoId,
            action: "removeMember",
            memberDids: didList,
            commit: commit.map { Bytes(data: $0) },
            groupInfo: groupInfo.map { Bytes(data: $0) },
            idempotencyKey: idempotencyKey,
            confirmationTag: confirmationTag.flatMap { Data(base64Encoded: $0) }.map { Bytes(data: $0) }
        )
    }

    /// Update GroupInfo for a conversation with retry logic and post-upload verification
    ///
    /// CRITICAL: This method now verifies the upload by fetching the stored data back.
    /// This catches network truncation issues where partial data is stored on the server.
    ///
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - groupInfo: Serialized GroupInfo bytes
    ///   - epoch: The epoch this GroupInfo corresponds to
    ///   - maxRetries: Maximum number of retry attempts (default 3)
    ///   - verifyUpload: If true (default), fetches stored data to verify integrity
    public func updateGroupInfo(
        convoId: String, groupInfo: Data, epoch: Int, maxRetries: Int = 3, verifyUpload: Bool = true
    ) async throws {
        logger.info(
            "📤 [MLSAPIClient.updateGroupInfo] START - convoId: \(convoId), epoch: \(epoch), size: \(groupInfo.count) bytes"
        )

        // Pre-validation: Ensure GroupInfo meets minimum size
        guard groupInfo.count >= Self.minGroupInfoSize else {
            logger.error(
                "❌ [MLSAPIClient.updateGroupInfo] GroupInfo too small: \(groupInfo.count) bytes (minimum \(Self.minGroupInfoSize))"
            )
            throw MLSAPIError.invalidResponse(
                message:
                "GroupInfo too small: \(groupInfo.count) bytes (minimum \(Self.minGroupInfoSize) required)"
            )
        }

        // 🔒 FIX #5: Log SHA-256 checksum for debugging data corruption
        // This helps identify if corruption happens during encoding, transport, or storage
        let uploadChecksum = SHA256.hash(data: groupInfo).compactMap { String(format: "%02x", $0) }
            .joined().prefix(16)
        logger.info(
            "📤 [MLSAPIClient.updateGroupInfo] Upload checksum (first 16 chars): \(uploadChecksum)"
        )

        let input = BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: convoId,
            action: "updateGroupInfo",
            groupInfo: Bytes(data: groupInfo)
        )

        var lastError: Error?

        for attempt in 1 ... maxRetries {
            do {
                let (responseCode, _) = try await client.blue.catbird.mlsChat.commitGroupChange(input: input)

                if responseCode == 200 {
                    logger.info(
                        "✅ [MLSAPIClient.updateGroupInfo] Upload succeeded on attempt \(attempt) for \(convoId)"
                    )

                    // CRITICAL: Verify the upload by fetching back and comparing size
                    if verifyUpload {
                        let fetchedVerificationPayload: (data: Data, epoch: Int)?
                        do {
                            let (storedData, storedEpoch, _) = try await getGroupInfo(
                                convoId: convoId, maxRetries: 2
                            )
                            fetchedVerificationPayload = (data: storedData, epoch: storedEpoch)
                        } catch {
                            // If verification fetch fails, log but don't fail the whole operation
                            // The upload itself succeeded
                            logger.warning(
                                "⚠️ [MLSAPIClient.updateGroupInfo] Verification fetch failed: \(error.localizedDescription)"
                            )
                            logger.warning("   Upload succeeded but could not verify - proceeding anyway")
                            fetchedVerificationPayload = nil
                        }

                        if let fetchedVerificationPayload {
                            let storedData = fetchedVerificationPayload.data
                            let storedEpoch = fetchedVerificationPayload.epoch

                            switch Self.groupInfoVerificationDisposition(
                                uploadedEpoch: epoch,
                                storedEpoch: storedEpoch
                            ) {
                            case .verifyStoredBytes:
                                break

                            case let .acceptConcurrentAdvance(serverEpoch):
                                logger.warning(
                                    "⚠️ [MLSAPIClient.updateGroupInfo] Verification skipped: server advanced from epoch \(epoch) to \(serverEpoch) during upload"
                                )
                                logger.warning(
                                    "   Another member committed while we were publishing GroupInfo; stored bytes belong to a newer epoch"
                                )
                                return

                            case let .retryStaleRead(serverEpoch):
                                logger.error(
                                    "❌ [MLSAPIClient.updateGroupInfo] VERIFICATION FAILED: Server returned stale epoch \(serverEpoch) after uploading epoch \(epoch)"
                                )

                                if attempt < maxRetries {
                                    logger.info(
                                        "🔄 [MLSAPIClient.updateGroupInfo] Retrying upload due to stale verification read..."
                                    )
                                    try await Task.sleep(nanoseconds: UInt64(1_000_000_000))
                                    continue
                                }

                                throw MLSAPIError.invalidResponse(
                                    message:
                                    "GroupInfo verification failed: server returned stale epoch \(serverEpoch) after uploading epoch \(epoch)"
                                )
                            }

                            // Verify size matches (critical for detecting truncation)
                            if storedData.count != groupInfo.count {
                                logger.error("❌ [MLSAPIClient.updateGroupInfo] VERIFICATION FAILED: Size mismatch!")
                                logger.error(
                                    "   Uploaded: \(groupInfo.count) bytes, Server stored: \(storedData.count) bytes"
                                )
                                logger.error(
                                    "   🚨 DATA CORRUPTION DETECTED - Server stored truncated/different data!"
                                )

                                if attempt < maxRetries {
                                    logger.info(
                                        "🔄 [MLSAPIClient.updateGroupInfo] Retrying upload due to size mismatch..."
                                    )
                                    try await Task.sleep(nanoseconds: UInt64(1_000_000_000)) // 1 second
                                    continue
                                }

                                throw MLSAPIError.invalidResponse(
                                    message:
                                    "GroupInfo verification failed: uploaded \(groupInfo.count) bytes but server stored \(storedData.count) bytes"
                                )
                            }

                            // Verify content matches (compare first and last 32 bytes to avoid full comparison)
                            let uploadPrefix = groupInfo.prefix(32)
                            let storedPrefix = storedData.prefix(32)
                            let uploadSuffix = groupInfo.suffix(32)
                            let storedSuffix = storedData.suffix(32)

                            if uploadPrefix != storedPrefix || uploadSuffix != storedSuffix {
                                logger.error(
                                    "❌ [MLSAPIClient.updateGroupInfo] VERIFICATION FAILED: Content mismatch!"
                                )
                                logger.error(
                                    "   Prefix match: \(uploadPrefix == storedPrefix), Suffix match: \(uploadSuffix == storedSuffix)"
                                )

                                if attempt < maxRetries {
                                    logger.info(
                                        "🔄 [MLSAPIClient.updateGroupInfo] Retrying upload due to content mismatch..."
                                    )
                                    try await Task.sleep(nanoseconds: UInt64(1_000_000_000))
                                    continue
                                }

                                throw MLSAPIError.invalidResponse(
                                    message:
                                    "GroupInfo verification failed: server stored different content than uploaded"
                                )
                            }

                            logger.info(
                                "✅ [MLSAPIClient.updateGroupInfo] Verification PASSED - size: \(storedData.count) bytes, epoch: \(storedEpoch)"
                            )
                        }
                    }

                    return
                }

                // Check for transient server errors that warrant retry
                let isTransient = [502, 503, 504].contains(responseCode)
                if isTransient, attempt < maxRetries {
                    let delay = TimeInterval(attempt) // 1s, 2s, 3s exponential backoff
                    logger.warning(
                        "⚠️ [MLSAPIClient.updateGroupInfo] Transient error \(responseCode) on attempt \(attempt), retrying in \(delay)s..."
                    )
                    try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                    continue
                }

                // Non-transient error or max retries reached
                logger.error("❌ [MLSAPIClient.updateGroupInfo] HTTP \(responseCode) on attempt \(attempt)")
                throw MLSAPIError.httpError(
                    statusCode: responseCode,
                    message: "Failed to update GroupInfo after \(attempt) attempt(s)"
                )
            } catch let error as MLSAPIError {
                throw error // Don't retry our own errors
            } catch {
                lastError = error
                if attempt < maxRetries {
                    let delay = TimeInterval(attempt)
                    logger.warning(
                        "⚠️ [MLSAPIClient.updateGroupInfo] Error on attempt \(attempt): \(error.localizedDescription), retrying in \(delay)s..."
                    )
                    try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                } else {
                    logger.error(
                        "❌ [MLSAPIClient.updateGroupInfo] All \(maxRetries) attempts failed for \(convoId)"
                    )
                }
            }
        }

        throw lastError
            ?? MLSAPIError.httpError(statusCode: 500, message: "All \(maxRetries) retry attempts failed")
    }

    /// Get the current epoch for a conversation
    /// - Parameter convoId: Conversation identifier
    /// - Returns: Current epoch number
    public func getEpoch(convoId: String) async throws -> Int {
        logger.debug("Fetching epoch for conversation: \(convoId)")

        let input = BlueCatbirdMlsChatGetGroupState.Parameters(convoId: convoId, include: "epoch")

        let (responseCode, output) = try await client.blue.catbird.mlsChat.getGroupState(input: input)

        guard responseCode == 200, let output = output else {
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to fetch epoch")
        }

        logger.debug("Current epoch for \(convoId): \(output.epoch ?? 0)")
        return output.epoch ?? 0
    }

    /// Get commit messages only (type: "commit") for pre-send sync and send recovery.
    /// Spec §5.1: Pre-send sync MUST use type "commit", NOT "all" or "app".
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - fromEpoch: Optional starting epoch (inclusive) for client-side filtering
    ///   - toEpoch: Optional ending epoch (inclusive) for client-side filtering
    ///   - limit: Maximum number of commits to return (default: 50, spec SEND_SYNC_BATCH_SIZE)
    /// - Returns: Array of commit messages
    public func getCommits(
        convoId: String,
        fromEpoch: Int? = nil,
        toEpoch: Int? = nil,
        limit: Int = 50
    ) async throws -> [BlueCatbirdMlsChatDefs.MessageView] {
        logger.debug(
            "Fetching commits (type=commit) for \(convoId), fromEpoch: \(fromEpoch?.description ?? "nil"), toEpoch: \(toEpoch?.description ?? "nil"), limit: \(limit)"
        )

        // Server honors fromEpoch/toEpoch (lexicon params, added 2026-04-19). Without them,
        // the server falls back to WHERE epoch >= 0 AND epoch <= current_epoch ORDER BY epoch ASC
        // LIMIT 50, which returns the OLDEST 50 commits. For any convo with >50 lifetime commits,
        // lagging clients would never reach the commits they need and stay permanently stuck.
        let input = BlueCatbirdMlsChatGetMessages.Parameters(
            convoId: convoId,
            limit: limit,
            type: "commit",
            fromEpoch: fromEpoch,
            toEpoch: toEpoch
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.getMessages(input: input)

        guard responseCode == 200, let output = output else {
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to fetch commits")
        }

        logger.debug("Fetched \(output.messages.count) commits")
        return output.messages
    }

    /// Get Welcome message for joining a conversation
    /// - Parameter convoId: Conversation identifier
    /// - Returns: Welcome message data
    static func missingWelcomeError(convoId: String) -> MLSAPIError {
        MLSAPIError.httpError(
            statusCode: 404,
            message: "No welcome message in response for \(convoId)"
        )
    }

    static func welcomeData(
        responseCode: Int,
        output: BlueCatbirdMlsChatGetGroupState.Output?,
        convoId: String
    ) throws -> Data {
        guard responseCode == 200, let output = output else {
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to fetch Welcome message"
            )
        }

        guard let welcomeBytes = output.welcome, !welcomeBytes.data.isEmpty else {
            throw Self.missingWelcomeError(convoId: convoId)
        }

        return welcomeBytes.data
    }

    static let maxWelcomeKeyPackageHashesForQuery = 48

    static func welcomeKeyPackageHashesForQuery(_ localHashes: [String]) -> [String]? {
        var seen = Set<String>()
        var uniqueHashes: [String] = []

        for rawHash in localHashes {
            let hash = rawHash.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            guard isHexKeyPackageHash(hash), seen.insert(hash).inserted else {
                continue
            }
            uniqueHashes.append(hash)
        }

        guard !uniqueHashes.isEmpty else {
            return nil
        }

        guard uniqueHashes.count <= maxWelcomeKeyPackageHashesForQuery else {
            return nil
        }

        return uniqueHashes
    }

    private static func isHexKeyPackageHash(_ value: String) -> Bool {
        value.utf8.count == 64 && value.utf8.allSatisfy { byte in
            (byte >= 48 && byte <= 57)
                || (byte >= 97 && byte <= 102)
        }
    }

    static func isGroupResetResponse(_ error: Error) -> Bool {
        if let apiError = error as? MLSAPIError {
            switch apiError {
            case let .httpError(statusCode, message):
                return statusCode == 410 || message.localizedCaseInsensitiveContains("groupReset")
            default:
                break
            }
        }

        let description = error.localizedDescription.lowercased()
        return description.contains("groupreset")
            || description.contains("http 410")
            || description.contains("http status 410")
            || description.contains("status code: 410")
            || description.contains("status code 410")
            || description.contains("statuscode: 410")
            || description.contains("statuscode 410")
    }

    public func getWelcome(
        convoId: String,
        keyPackageHashes: [String]? = nil,
        deviceId: String? = nil
    ) async throws -> Data {
        logger.debug("Fetching Welcome message for conversation: \(convoId)")

        let input = BlueCatbirdMlsChatGetGroupState.Parameters(
            convoId: convoId,
            include: "welcome",
            keyPackageHashes: keyPackageHashes,
            deviceId: deviceId
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.getGroupState(input: input)

        let welcomeData: Data
        do {
            welcomeData = try Self.welcomeData(
                responseCode: responseCode,
                output: output,
                convoId: convoId
            )
        } catch {
            if case let .httpError(statusCode, _) = error as? MLSAPIError, statusCode == 404 {
                logger.error("❌ No Welcome message available for \(convoId)")
            } else {
                logger.error("❌ Failed to fetch Welcome message for \(convoId): HTTP \(responseCode)")
            }
            throw error
        }

        logger.debug("Fetched Welcome message for \(convoId), \(welcomeData.count) bytes")
        return welcomeData
    }

    /// Confirm successful or failed processing of Welcome message (two-phase commit)
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - success: Whether Welcome was processed successfully
    ///   - errorMessage: Optional error details if success=false
    ///   - maxRetries: Maximum number of retries for transient errors (default: 3)
    public func confirmWelcome(
        convoId: String,
        success: Bool,
        errorMessage: String? = nil,
        maxRetries: Int = 3
    ) async throws {
        logger.info("📤 [confirmWelcome] START - convoId: \(convoId), success: \(success)")
        if let error = errorMessage {
            logger.debug("   Error details: \(error)")
        }

        let input = BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: convoId,
            action: "confirmWelcome"
        )

        // CRITICAL FIX: Retry on transient errors (502, 503, 504)
        var lastError: Error?

        for attempt in 1 ... maxRetries {
            logger.debug("📡 [confirmWelcome] Attempt \(attempt)/\(maxRetries) - calling server...")

            do {
                let (responseCode, _) = try await client.blue.catbird.mlsChat.commitGroupChange(input: input)

                logger.debug("📡 [confirmWelcome] Server response: HTTP \(responseCode)")

                guard responseCode == 200 else {
                    // Check if this is a transient error worth retrying
                    let isTransient = responseCode == 502 || responseCode == 503 || responseCode == 504

                    if isTransient, attempt < maxRetries {
                        logger.warning(
                            "⚠️ [confirmWelcome] Transient error \(responseCode) on attempt \(attempt)/\(maxRetries), retrying..."
                        )

                        // Exponential backoff: 1s, 2s, 4s
                        let delay = TimeInterval(1 << (attempt - 1))
                        try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                        continue
                    }

                    // CRITICAL FIX: Treat 404 as benign success
                    // This happens if we were already active or if the welcome was already processed
                    if responseCode == 404 {
                        logger.warning(
                            "⚠️ [confirmWelcome] Server returned 404 (Not Found) - assuming welcome already processed or user active"
                        )
                        return
                    }

                    logger.error(
                        "❌ [confirmWelcome] Failed with HTTP \(responseCode) on attempt \(attempt)/\(maxRetries)"
                    )
                    throw MLSAPIError.httpError(
                        statusCode: responseCode, message: "confirmWelcome failed with HTTP \(responseCode)"
                    )
                }

                logger.info("✅ [confirmWelcome] SUCCESS - confirmation sent after \(attempt) attempt(s)")
                return

            } catch let error as MLSAPIError {
                logger.error(
                    "❌ [confirmWelcome] MLSAPIError on attempt \(attempt)/\(maxRetries): \(error.localizedDescription)"
                )
                lastError = error

                // If it's a non-retryable error, throw immediately
                if case let .httpError(statusCode, _) = error {
                    let isTransient = statusCode == 502 || statusCode == 503 || statusCode == 504
                    if !isTransient || attempt >= maxRetries {
                        logger.error("❌ [confirmWelcome] Non-retryable or exhausted retries - throwing error")
                        throw error
                    }
                    logger.warning(
                        "⚠️ [confirmWelcome] Transient error \(statusCode), retrying after backoff..."
                    )
                    let delay = TimeInterval(1 << (attempt - 1))
                    try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                } else {
                    logger.error("❌ [confirmWelcome] Non-HTTP error - throwing immediately")
                    throw error
                }
            } catch {
                logger.error(
                    "❌ [confirmWelcome] Unknown error on attempt \(attempt)/\(maxRetries): \(error.localizedDescription)"
                )
                logger.error("   Error type: \(type(of: error))")
                lastError = error

                // Network errors might be transient, retry
                if attempt < maxRetries {
                    logger.warning("⚠️ [confirmWelcome] Network/unknown error, retrying after backoff...")
                    let delay = TimeInterval(1 << (attempt - 1))
                    try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                } else {
                    logger.error("❌ [confirmWelcome] Exhausted all retry attempts - throwing error")
                    throw error
                }
            }
        }

        // If we exhausted all retries, throw the last error
        if let error = lastError {
            logger.error(
                "❌ [confirmWelcome] FAILED after \(maxRetries) attempts - last error: \(error.localizedDescription)"
            )
            throw error
        }
    }

    /// Process an external commit (e.g. for rejoining or self-update)
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - externalCommit: Serialized MLS External Commit data
    ///   - groupInfo: Serialized POST-commit MLS GroupInfo bytes. Server stores
    ///     this atomically inside the same txn that advances the epoch (closes
    ///     the race where the next External-Commit joiner reads stale state).
    ///     Pass nil only if you cannot export post-commit GroupInfo locally —
    ///     server keeps existing (stale) state and logs a warning.
    ///   - idempotencyKey: Optional client-generated UUID
    /// - Returns: Success status and new epoch (0 if not provided by server)
    public func processExternalCommit(
        convoId: String,
        externalCommit: Data,
        groupInfo: Data? = nil,
        confirmationTag: String? = nil,
        idempotencyKey: String? = nil
    ) async throws -> (success: Bool, newEpoch: Int) {
        let idemKey = idempotencyKey ?? UUID().uuidString.lowercased()
        logger.info(
            "🌐 [MLSAPIClient.processExternalCommit] START - convoId: \(convoId), commit: \(externalCommit.count) bytes, idempotencyKey: \(idemKey)"
        )

        let input = Self.buildExternalCommitInput(
            convoId: convoId,
            externalCommit: externalCommit,
            groupInfo: groupInfo,
            confirmationTag: confirmationTag,
            idempotencyKey: idemKey
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.commitGroupChange(
            input: input
        )

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.processExternalCommit] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to process external commit"
            )
        }

        // Handle optional epoch (older servers may not return it)
        let newEpoch = output.newEpoch ?? 0
        if output.newEpoch == nil {
            logger.warning("⚠️ [MLSAPIClient.processExternalCommit] Server did not return epoch - using 0")
        }

        logger.info("✅ [MLSAPIClient.processExternalCommit] SUCCESS - newEpoch: \(newEpoch)")
        return (output.success, newEpoch)
    }

    /// Submit a non-membership group commit (e.g. `updateMetadata`) via
    /// `blue.catbird.mlsChat.commitGroupChange`. On success the server
    /// CAS-advances the authoritative `current_epoch`. Throws on non-200
    /// (e.g. 409 stale/raced epoch) so the caller discards the pending commit
    /// rather than advancing its local epoch past the server.
    public func commitGroupChange(
        convoId: String,
        action: String,
        commit: Data,
        confirmationTag: String? = nil
    ) async throws {
        logger.info(
            "🌐 [MLSAPIClient.commitGroupChange] START - convoId: \(convoId), action: \(action), commit: \(commit.count) bytes"
        )
        _ = confirmationTag
        let input = BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: convoId,
            action: action,
            commit: Bytes(data: commit)
        )
        let (responseCode, _) = try await client.blue.catbird.mlsChat.commitGroupChange(input: input)
        guard responseCode == 200 else {
            logger.error(
                "❌ [MLSAPIClient.commitGroupChange] HTTP \(responseCode) for action \(action) convo \(convoId)"
            )
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "commitGroupChange(\(action)) failed"
            )
        }
        logger.info("✅ [MLSAPIClient.commitGroupChange] SUCCESS - action: \(action)")
    }

    /// Get list of expected conversations for auto-rejoin detection
    /// - Parameter deviceId: Optional device ID to check (defaults to current device from auth)
    /// - Returns: List of conversations user should be in but may be missing locally
    public func getExpectedConversations(
        deviceId _: String? = nil
    ) async throws -> BlueCatbirdMlsChatGetConvos.Output {
        logger.info("📤 [getExpectedConversations] Fetching expected conversations")

        let input = BlueCatbirdMlsChatGetConvos.Parameters()

        let (responseCode, output) = try await client.blue.catbird.mlsChat.getConvos(
            input: input
        )

        guard responseCode == 200, let output = output else {
            logger.error("❌ [getExpectedConversations] Failed with HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "getExpectedConversations failed"
            )
        }

        logger.info(
            "✅ [getExpectedConversations] SUCCESS - found \(output.conversations.count) conversations"
        )
        return output
    }

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
        logger.info(
            "📤 [requestWelcomeReissue] START - convoId: \(convoId.prefix(16)), recipientDeviceDid: \(recipientDeviceDid.prefix(32)), reason: \(reason)"
        )

        let input = BlueCatbirdMlsChatReissueWelcome.Input(
            convoId: convoId,
            recipientDeviceDid: recipientDeviceDid,
            reason: reason
        )

        do {
            let (responseCode, output) = try await client.blue.catbird.mlsChat.reissueWelcome(input: input)

            guard (200 ... 299).contains(responseCode), let output else {
                logger.error("❌ [requestWelcomeReissue] HTTP \(responseCode)")
                throw MLSAPIError.httpError(
                    statusCode: responseCode,
                    message: "requestWelcomeReissue failed"
                )
            }

            logger.info(
                "✅ [requestWelcomeReissue] SUCCESS - requested=\(output.welcomeRequested), requestedAt=\(output.requestedAt.iso8601String), inviterDevice=\(output.inviterDevice?.description ?? "nil")"
            )

            return ReissueWelcomeResponse(
                requested: output.welcomeRequested,
                requestId: output.requestId,
                message: "requestedAt=\(output.requestedAt.iso8601String)"
            )
        } catch let error as ATProtoError<BlueCatbirdMlsChatReissueWelcome.Error> {
            logger.error(
                "❌ [requestWelcomeReissue] Lexicon error: \(error.error.errorName) - \(error.message ?? "no details")"
            )
            switch error.error {
            case .convoNotFound:
                throw MLSAPIError.conversationNotFound(detail: error.message)
            case .noAdminToReissue:
                throw MLSAPIError.httpError(
                    statusCode: 409,
                    message: error.message ?? "No admin/inviter is available to reissue Welcome"
                )
            case .rateLimited:
                throw MLSAPIError.rateLimited(retryAfter: nil)
            }
        }
    }

    public func respondToWelcomeReissue(
        requestId: String,
        welcomeBlob: Data,
        keyPackageHash: String? = nil
    ) async throws -> ReissueWelcomeRespondResponse {
        logger.info(
            "📤 [respondToWelcomeReissue] START - requestId: \(requestId.prefix(16)), welcomeBytes: \(welcomeBlob.count), hasKeyPackageHash: \(keyPackageHash != nil)"
        )

        let input = BlueCatbirdMlsChatReissueWelcomeRespond.Input(
            requestId: requestId,
            welcomeBlob: Bytes(data: welcomeBlob),
            keyPackageHash: keyPackageHash
        )

        do {
            let (responseCode, output) = try await client.blue.catbird.mlsChat
                .reissueWelcomeRespond(input: input)

            guard (200 ... 299).contains(responseCode), let output else {
                logger.error("❌ [respondToWelcomeReissue] HTTP \(responseCode)")
                throw MLSAPIError.httpError(
                    statusCode: responseCode,
                    message: "reissueWelcomeRespond failed"
                )
            }

            logger.info(
                "✅ [respondToWelcomeReissue] SUCCESS - requestId=\(output.requestId.prefix(16)), welcomeBlobId=\(output.welcomeBlobId.prefix(16)), respondedAt=\(output.respondedAt.iso8601String)"
            )

            return ReissueWelcomeRespondResponse(
                stored: output.stored,
                requestId: output.requestId,
                welcomeBlobId: output.welcomeBlobId,
                respondedAt: output.respondedAt
            )
        } catch let error as ATProtoError<BlueCatbirdMlsChatReissueWelcomeRespond.Error> {
            logger.error(
                "❌ [respondToWelcomeReissue] Lexicon error: \(error.error.errorName) - \(error.message ?? "no details")"
            )
            switch error.error {
            case .requestNotFound:
                throw MLSAPIError.httpError(
                    statusCode: 404,
                    message: error.message ?? "No pending unanswered Welcome reissue request"
                )
            case .unauthorized:
                throw MLSAPIError.httpError(
                    statusCode: 403,
                    message: error.message ?? "Caller is not authorized to respond to Welcome reissue"
                )
            case .invalidWelcome:
                throw MLSAPIError.httpError(
                    statusCode: 400,
                    message: error.message ?? "Invalid Welcome reissue response"
                )
            }
        }
    }

    public func reconcileKeyPackages(
        deviceId: String,
        localHashes: [String]
    ) async throws -> ReconcileResponse {
        logger.info(
            "🔄 [reconcileKeyPackages] START - localHashes: \(localHashes.count), deviceId: \(deviceId)"
        )

        let input = Self.buildReconcileKeyPackagesInput(
            deviceId: deviceId,
            localHashes: localHashes
        )

        do {
            let (responseCode, output) = try await client.blue.catbird.mlsChat.reconcileKeyPackages(
                input: input
            )

            guard (200 ... 299).contains(responseCode), let output else {
                logger.error("❌ [reconcileKeyPackages] HTTP \(responseCode)")
                throw MLSAPIError.httpError(
                    statusCode: responseCode,
                    message: "reconcileKeyPackages failed"
                )
            }

            let localOnly = Set(output.localOnly)
            let confirmed = localHashes.filter { !localOnly.contains($0) }
            logger.info(
                "✅ [reconcileKeyPackages] SUCCESS - confirmed=\(confirmed.count), serverOnly=\(output.serverOnly.count), localOnly=\(output.localOnly.count), deviceVerified=\(output.deviceVerified)"
            )

            return ReconcileResponse(
                serverOnly: output.serverOnly,
                localOnly: output.localOnly,
                confirmed: confirmed,
                deviceVerified: output.deviceVerified
            )
        } catch let error as ATProtoError<BlueCatbirdMlsChatReconcileKeyPackages.Error> {
            logger.error(
                "❌ [reconcileKeyPackages] Lexicon error: \(error.error.errorName) - \(error.message ?? "no details")"
            )
            switch error.error {
            case .deviceNotFound:
                throw MLSAPIError.httpError(statusCode: 404, message: error.message ?? "Device not found")
            case .unauthorized:
                throw MLSAPIError.httpError(statusCode: 403, message: error.message ?? "Unauthorized")
            }
        }
    }

    public func invalidateKeyPackage(
        deviceDid: String,
        hash: String,
        reason: InvalidationReason
    ) async throws {
        logger.info(
            "🗑️ [invalidateKeyPackage] START - deviceDid: \(deviceDid.prefix(20)), hash: \(hash.prefix(16)), reason: \(reason.rawValue)"
        )

        let input = try BlueCatbirdMlsChatInvalidateKeyPackage.Input(
            deviceDid: DID(didString: deviceDid),
            keyPackageHash: hash,
            reason: reason.rawValue
        )

        do {
            let (responseCode, output) = try await client.blue.catbird.mlsChat.invalidateKeyPackage(
                input: input
            )

            guard (200 ... 299).contains(responseCode), let output else {
                logger.error("❌ [invalidateKeyPackage] HTTP \(responseCode)")
                throw MLSAPIError.httpError(
                    statusCode: responseCode,
                    message: "invalidateKeyPackage failed"
                )
            }

            logger.info(
                "✅ [invalidateKeyPackage] SUCCESS - marked=\(output.marked), alreadyDead=\(output.alreadyDead)"
            )
        } catch let error as ATProtoError<BlueCatbirdMlsChatInvalidateKeyPackage.Error> {
            logger.error(
                "❌ [invalidateKeyPackage] Lexicon error: \(error.error.errorName) - \(error.message ?? "no details")"
            )
            switch error.error {
            case .keyPackageNotFound:
                throw MLSAPIError.keyPackageNotFound(detail: error.message)
            case .unauthorized:
                throw MLSAPIError.httpError(statusCode: 403, message: error.message ?? "Unauthorized")
            }
        }
    }

    /// Invalidate a Welcome message that cannot be processed
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - reason: Reason for invalidation (e.g., "NoMatchingKeyPackage")
    /// - Returns: Tuple of (invalidated: Bool, welcomeId: String?)
    /// - Note: Used when Welcome processing fails and client needs to fall back to External Commit
    public func invalidateWelcome(
        convoId: String,
        reason: String
    ) async throws -> (invalidated: Bool, welcomeId: String?) {
        logger.info("📤 [invalidateWelcome] START - convoId: \(convoId), reason: \(reason)")

        // The standalone invalidateWelcome endpoint was folded into
        // commitGroupChange (action: "invalidateWelcome") server-side.
        let input = BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: convoId,
            action: "invalidateWelcome"
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.commitGroupChange(input: input)

        guard (200 ... 299).contains(responseCode), let output else {
            logger.error("❌ [invalidateWelcome] Failed with HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "invalidateWelcome failed with HTTP \(responseCode)"
            )
        }

        if output.success {
            logger.info("✅ [invalidateWelcome] SUCCESS - Welcome invalidated")
        } else {
            logger.warning("⚠️ [invalidateWelcome] No Welcome found to invalidate")
        }

        return (output.success, nil)
    }

    /// Request re-addition to a conversation when both Welcome and External Commit have failed
    /// - Parameter convoId: Conversation identifier
    /// - Returns: Tuple of (requested: Bool, activeMembers: Int?)
    /// - Note: This emits an SSE event to active members who can re-add the user
    public func readdition(
        convoId: String
    ) async throws -> (requested: Bool, activeMembers: Int?) {
        logger.info("📤 [readdition] START - convoId: \(convoId)")

        let input = BlueCatbirdMlsChatCommitGroupChange.Input(convoId: convoId, action: "readdition")

        let (responseCode, output) = try await client.blue.catbird.mlsChat.commitGroupChange(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [readdition] Failed with HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "readdition failed with HTTP \(responseCode)"
            )
        }

        if output.success {
            logger.info(
                "✅ [readdition] SUCCESS - request sent"
            )
        } else {
            logger.warning("⚠️ [readdition] No active members to notify")
        }

        return (output.success, nil)
    }

    /// Request active members to publish fresh GroupInfo for a conversation
    /// - Parameter convoId: Conversation identifier
    /// - Returns: Tuple of (requested: Bool, activeMembers: Int?)
    /// - Note: Used when GroupInfo is expired and External Commit cannot proceed
    public func groupInfoRefresh(
        convoId: String
    ) async throws -> (requested: Bool, activeMembers: Int?) {
        logger.info("📤 [groupInfoRefresh] START - convoId: \(convoId)")

        let input = BlueCatbirdMlsChatCommitGroupChange.Input(convoId: convoId, action: "refreshGroupInfo")

        let (responseCode, output) = try await client.blue.catbird.mlsChat.commitGroupChange(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [groupInfoRefresh] Failed with HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "groupInfoRefresh failed with HTTP \(responseCode)"
            )
        }

        if output.success {
            logger.info(
                "✅ [groupInfoRefresh] SUCCESS - request sent"
            )
        } else {
            logger.warning("⚠️ [groupInfoRefresh] No active members to notify")
        }

        return (output.success, nil)
    }

    // `requestKeyPackageReplenish` deleted in Phase F. The publishKeyPackages
    // lexicon was reshaped to a publish-only flow; the legacy
    // `requestReplenish` action with targetDids/reason/convoId no longer
    // exists on the protocol. There were no in-tree callers; if a future
    // ask-peer-to-publish flow is needed, it'll need its own lexicon path.

    // MARK: - Admin Operations

    /// Remove a member from conversation (admin-only operation)
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - targetDid: DID of member to remove
    ///   - reason: Optional reason for removal
    ///   - commit: Base64-encoded MLS commit message (REQUIRED for epoch sync)
    ///   - groupInfo: Serialized POST-commit MLS GroupInfo bytes. Server stores
    ///     this atomically inside the same txn that advances the epoch (closes
    ///     the race where the next External-Commit joiner reads stale state).
    ///     Pass nil only if you cannot export post-commit GroupInfo locally —
    ///     server keeps existing (stale) state and logs a warning.
    ///   - idempotencyKey: Optional client-generated UUID for idempotent retries (auto-generated if nil)
    /// - Returns: Success status and epoch hint (if provided by server)
    public func removeMember(
        convoId: String,
        targetDid: DID,
        reason _: String? = nil,
        commit: String? = nil,
        groupInfo: Data? = nil,
        idempotencyKey: String? = nil
    ) async throws -> (ok: Bool, epochHint: Int?) {
        let idemKey = idempotencyKey ?? UUID().uuidString.lowercased()
        logger.info(
            "🌐 [MLSAPIClient.removeMember] START - convoId: \(convoId), targetDid: \(targetDid), commit: \(commit != nil ? "\(commit!.count) chars" : "nil"), idempotencyKey: \(idemKey)"
        )

        // The public API still accepts `commit` as a base64-encoded String; decode
        // here at the boundary so the helper can take Data?, matching the shape of
        // buildAddMembersInput / buildExternalCommitInput.
        let commitData = commit.flatMap { Data(base64Encoded: $0) }
        let input = Self.buildRemoveMemberInput(
            convoId: convoId,
            didList: [targetDid],
            commit: commitData,
            groupInfo: groupInfo,
            confirmationTag: nil,
            idempotencyKey: idemKey
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.commitGroupChange(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.removeMember] HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to remove member")
        }

        logger.info(
            "✅ [MLSAPIClient.removeMember] SUCCESS - newEpoch: \(output.newEpoch.map { String($0) } ?? "nil")"
        )
        return (output.success, output.newEpoch)
    }

    /// Send a generic MLS commit (e.g., self-update) to advance epoch
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - commit: Serialized MLS commit data
    ///   - idempotencyKey: Optional client-generated UUID for idempotent retries (auto-generated if nil)
    /// - Returns: New epoch after processing commit
    /// - Note: This is used for self-updates and other commits that don't add/remove members
    public func sendCommit(
        convoId: String,
        commit: String,
        idempotencyKey: String? = nil
    ) async throws -> UInt64 {
        let idemKey = idempotencyKey ?? UUID().uuidString.lowercased()
        logger.info(
            "🌐 [MLSAPIClient.sendCommit] START - convoId: \(convoId), commit: \(commit.count) bytes, idempotencyKey: \(idemKey)"
        )

        let input = BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: convoId,
            action: "commit",
            commit: Data(base64Encoded: commit).map { Bytes(data: $0) },
            idempotencyKey: idemKey
        )

        do {
            let (responseCode, output) = try await client.blue.catbird.mlsChat.commitGroupChange(input: input)

            guard responseCode == 200, let output = output else {
                throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to send commit")
            }

            logger.info("✅ [MLSAPIClient.sendCommit] SUCCESS - newEpoch: \(output.newEpoch ?? 0)")
            return UInt64(output.newEpoch ?? 0)
        } catch let error as ATProtoError<BlueCatbirdMlsChatCommitGroupChange.Error> {
            logger.error(
                "❌ [MLSAPIClient.sendCommit] Lexicon error: \(error.error.errorName) - \(error.message ?? "no details")"
            )
            throw MLSAPIError(from: error)
        }
    }

    /// Promote a member to admin status
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - targetDid: DID of member to promote
    ///   - idempotencyKey: Optional client-generated UUID for idempotent retries (auto-generated if nil)
    /// - Returns: Success status
    public func promoteAdmin(
        convoId: String,
        targetDid: DID,
        idempotencyKey: String? = nil
    ) async throws -> Bool {
        let idemKey = idempotencyKey ?? UUID().uuidString.lowercased()
        logger.info(
            "🌐 [MLSAPIClient.promoteAdmin] START - convoId: \(convoId), targetDid: \(targetDid), idempotencyKey: \(idemKey)"
        )

        let input = BlueCatbirdMlsChatUpdateConvo.Input(
            convoId: convoId,
            action: "promoteAdmin",
            targetDid: targetDid
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.updateConvo(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.promoteAdmin] HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to promote admin")
        }

        logger.info("✅ [MLSAPIClient.promoteAdmin] SUCCESS")
        return output.success
    }

    /// Demote an admin to regular member status
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - targetDid: DID of admin to demote
    ///   - idempotencyKey: Optional client-generated UUID for idempotent retries (auto-generated if nil)
    /// - Returns: Success status
    public func demoteAdmin(
        convoId: String,
        targetDid: DID,
        idempotencyKey: String? = nil
    ) async throws -> Bool {
        let idemKey = idempotencyKey ?? UUID().uuidString.lowercased()
        logger.info(
            "🌐 [MLSAPIClient.demoteAdmin] START - convoId: \(convoId), targetDid: \(targetDid), idempotencyKey: \(idemKey)"
        )

        let input = BlueCatbirdMlsChatUpdateConvo.Input(
            convoId: convoId,
            action: "demoteAdmin",
            targetDid: targetDid
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.updateConvo(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.demoteAdmin] HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to demote admin")
        }

        logger.info("✅ [MLSAPIClient.demoteAdmin] SUCCESS")
        return output.success
    }

    // MARK: - Moderation

    /// Report an account as spam in an MLS conversation
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - reportedDid: DID of account to report
    ///   - reason: Optional reason for the report
    /// - Returns: Tuple of HTTP response code and optional output
    public func reportSpam(
        convoId: String,
        reportedDid: String,
        reason: String? = nil
    ) async throws -> (Int, BlueCatbirdMlsChatReportSpam.Output?) {
        logger.info(
            "🌐 [MLSAPIClient.reportSpam] START - convoId: \(convoId), reportedDid: \(reportedDid)"
        )

        let input = try BlueCatbirdMlsChatReportSpam.Input(
            convoId: convoId,
            reportedDid: DID(didString: reportedDid),
            reason: reason
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.reportSpam(input: input)

        if let output = output {
            logger.info("✅ [MLSAPIClient.reportSpam] SUCCESS - id: \(output.id)")
        } else {
            logger.error("❌ [MLSAPIClient.reportSpam] HTTP \(responseCode)")
        }

        return (responseCode, output)
    }

    // MARK: - Blocking

    /// Check block relationships between users before creating conversations
    /// - Parameter dids: Array of DIDs to check for blocks
    /// - Returns: HTTP response code and decoded output (nil on non-2xx)
    public func checkBlocks(
        dids: [DID]
    ) async throws -> (responseCode: Int, data: BlueCatbirdMlsChatCheckBlocks.Output?) {
        logger.info("🌐 [MLSAPIClient.checkBlocks] START - dids: \(dids.count)")

        let input = BlueCatbirdMlsChatCheckBlocks.Input(dids: dids)
        let (responseCode, output) = try await client.blue.catbird.mlsChat.checkBlocks(input: input)

        if let output {
            logger.info("✅ [MLSAPIClient.checkBlocks] SUCCESS - \(output.blocks.count) blocks found")
        } else {
            logger.error("❌ [MLSAPIClient.checkBlocks] HTTP \(responseCode)")
        }
        return (responseCode, output)
    }

    /// Get block status for members in a conversation
    /// - Parameter convoId: Conversation identifier
    /// - Returns: HTTP response code and decoded output (nil on non-2xx)
    public func getBlockStatus(
        convoId: String
    ) async throws -> (responseCode: Int, data: BlueCatbirdMlsChatGetBlockStatus.Output?) {
        logger.info("🌐 [MLSAPIClient.getBlockStatus] START - convoId: \(convoId)")

        let input = BlueCatbirdMlsChatGetBlockStatus.Input(convoId: convoId)
        let (responseCode, output) = try await client.blue.catbird.mlsChat.getBlockStatus(input: input)

        if let output {
            logger.info("✅ [MLSAPIClient.getBlockStatus] SUCCESS - \(output.blocks.count) blocks")
        } else {
            logger.error("❌ [MLSAPIClient.getBlockStatus] HTTP \(responseCode)")
        }
        return (responseCode, output)
    }

    // MARK: - Push Notifications

    /// Register or update a device push token for APNs
    /// - Parameters:
    ///   - deviceId: Unique identifier for the device
    ///   - pushToken: Hex-encoded APNs device token
    ///   - deviceName: Human-readable device name
    ///   - platform: Platform identifier (e.g., "ios")
    /// - Returns: Success status
    public func registerDeviceToken(
        deviceId: String,
        pushToken: String,
        deviceName: String,
        platform: String = "ios"
    ) async throws -> Bool {
        logger.info(
            "🌐 [MLSAPIClient.registerDeviceToken] START - deviceId: \(deviceId), platform: \(platform), deviceName: \(deviceName)"
        )

        // registerDeviceToken was folded into registerDevice server-side; sending
        // registerDevice with a pushToken updates the token on the existing device
        // row (same consolidation already used by unregisterDeviceToken below).
        let input = BlueCatbirdMlsChatRegisterDevice.Input(
            deviceName: deviceName,
            deviceUUID: deviceId,
            keyPackages: [],
            signaturePublicKey: Bytes(data: Data()),
            pushToken: pushToken
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.registerDevice(input: input)

        guard responseCode == 200, output != nil else {
            logger.error("❌ [MLSAPIClient.registerDeviceToken] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to register device token"
            )
        }

        logger.info("✅ [MLSAPIClient.registerDeviceToken] SUCCESS")
        return true
    }

    /// Remove a device push token
    /// - Parameter deviceId: Unique identifier for the device
    /// - Returns: Success status
    public func unregisterDeviceToken(deviceId: String) async throws -> Bool {
        logger.info("🌐 [MLSAPIClient.unregisterDeviceToken] START - deviceId: \(deviceId)")

        let input = BlueCatbirdMlsChatRegisterDevice.Input(
            deviceName: "",
            deviceUUID: deviceId,
            keyPackages: [],
            signaturePublicKey: Bytes(data: Data())
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.registerDevice(
            input: input
        )

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.unregisterDeviceToken] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to unregister device token"
            )
        }

        logger.info("✅ [MLSAPIClient.unregisterDeviceToken] SUCCESS")
        return true
    }

    // MARK: - Analytics

    /// Get key package statistics for monitoring inventory health
    /// - Returns: Key package usage statistics
    public func getKeyPackageStats() async throws -> BlueCatbirdMlsChatPublishKeyPackages.Output {
        logger.info("🌐 [MLSAPIClient.getKeyPackageStats] START")

        let input = BlueCatbirdMlsChatPublishKeyPackages.Input(action: "stats")

        let (responseCode, output) = try await client.blue.catbird.mlsChat.publishKeyPackages(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.getKeyPackageStats] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to get key package stats"
            )
        }

        logger.info(
            "✅ [MLSAPIClient.getKeyPackageStats] SUCCESS - available: \(output.stats.available)"
        )
        return output
    }

    /// Get detailed key package status including consumption history (Phase 3)
    /// - Parameters:
    ///   - limit: Maximum number of consumed packages to return in history (1-100, default: 20)
    ///   - cursor: Pagination cursor from previous response
    /// - Returns: Key package status with available/consumed counts and history
    public func getKeyPackageStatus(
        limit: Int = 20,
        cursor: String? = nil
    ) async throws -> BlueCatbirdMlsChatPublishKeyPackages.Output {
        logger.info(
            "🌐 [MLSAPIClient.getKeyPackageStatus] START - limit: \(limit), cursor: \(cursor ?? "none")"
        )

        let input = BlueCatbirdMlsChatPublishKeyPackages.Input(action: "stats")

        let (responseCode, output) = try await client.blue.catbird.mlsChat.publishKeyPackages(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.getKeyPackageStatus] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to get key package status"
            )
        }

        logger.info(
            "✅ [MLSAPIClient.getKeyPackageStatus] SUCCESS - available: \(output.stats.available), published: \(output.stats.published)"
        )
        return output
    }

    // MARK: - Key Package Synchronization (NoMatchingKeyPackage Prevention)

    /// Synchronize key packages between client and server to prevent NoMatchingKeyPackage errors
    ///
    /// This is the primary method to prevent the NoMatchingKeyPackage bug:
    /// - Compares local key package hashes against server-side available packages
    /// - Deletes any "orphaned" server packages that no longer have local private keys
    /// - Returns sync status including deleted orphan count
    ///
    /// MULTI-DEVICE SUPPORT:
    /// deviceId is REQUIRED. Only syncs key packages belonging to that specific device.
    /// This prevents Device A from accidentally deleting Device B's packages.
    ///
    /// Should be called:
    /// - On app launch after device registration
    /// - After account switch
    /// - When recovering from storage corruption
    ///
    /// - Parameters:
    ///   - localHashes: SHA256 hex hashes of key packages in local storage
    ///   - deviceId: Device ID (REQUIRED) - the deviceId returned from registerDevice
    /// - Returns: Tuple of (serverHashes, orphanedCount, deletedCount, orphanedHashes, remainingAvailable)
    public func syncKeyPackages(localHashes: [String], deviceId: String) async throws -> (
        serverHashes: [String],
        orphanedCount: Int,
        deletedCount: Int,
        orphanedHashes: [String],
        remainingAvailable: Int
    ) {
        logger.info(
            "🔄 [MLSAPIClient.syncKeyPackages] START - localHashes: \(localHashes.count), deviceId: \(deviceId)"
        )

        let input = BlueCatbirdMlsChatPublishKeyPackages.Input(
            action: "sync",
            localHashes: localHashes,
            deviceId: deviceId
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.publishKeyPackages(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.syncKeyPackages] HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "syncKeyPackages failed")
        }

        guard let syncResult = output.syncResult else {
            throw MLSAPIError.invalidResponse(message: "Missing syncResult in key package sync response")
        }

        logger.info("✅ [MLSAPIClient.syncKeyPackages] SUCCESS")
        logger.info("   - Device: \(deviceId)")
        logger.info("   - Orphaned: \(syncResult.orphanedCount)")
        logger.info("   - Deleted: \(syncResult.deletedCount)")
        logger.info("   - Remaining: \(syncResult.remainingAvailable ?? output.stats.available)")

        return (
            syncResult.serverHashes,
            syncResult.orphanedCount,
            syncResult.deletedCount,
            syncResult.orphanedHashes ?? [],
            syncResult.remainingAvailable ?? output.stats.available
        )
    }

    /// Query current key package inventory from server (simplified wrapper for upload logic)
    /// - Returns: Tuple of (available packages on server, replenishment threshold)
    /// - Throws: MLSAPIError if query fails
    public func queryKeyPackageInventory() async throws -> (available: Int, threshold: Int) {
        logger.info("🔍 [MLSAPIClient.queryInventory] Querying server key package inventory")

        let stats = try await getKeyPackageStats()
        let available = stats.stats.available
        let threshold = 10 // Default threshold

        logger.info(
            "📊 [MLSAPIClient.queryInventory] Server inventory - available: \(available), threshold: \(threshold)"
        )
        return (available, threshold)
    }

    /// Publish multiple key packages in a single batch request (preferred over individual uploads)
    /// - Parameters:
    ///   - packages: Array of key package data to upload (max 100 per batch, 50 in recovery mode)
    ///   - recoveryMode: If true, sends X-MLS-Recovery-Mode header to bypass rate limits when device has 0 key packages
    /// - Returns: Batch result with success/failure counts
    public func publishKeyPackagesBatch(
        _ packages: [MLSKeyPackageUploadData],
        recoveryMode: Bool = false,
        deviceId: String? = nil
    ) async throws -> KeyPackageBatchResult {
        logger.info("🌐 [MLSAPIClient.publishKeyPackagesBatch] START - count: \(packages.count), recoveryMode: \(recoveryMode)")

        // Validate batch size (50 max in recovery mode, 100 otherwise)
        let maxBatchSize = recoveryMode ? 50 : 100
        guard packages.count <= maxBatchSize else {
            logger.error("❌ Batch size \(packages.count) exceeds maximum of \(maxBatchSize)")
            throw MLSAPIError.invalidBatchSize
        }

        // Set recovery mode header if needed
        if recoveryMode {
            await client.setHeader(name: "X-MLS-Recovery-Mode", value: "true")
            logger.info("🔑 [MLSAPIClient] Recovery mode enabled - bypassing rate limits for device with 0 key packages")
        }

        defer {
            if recoveryMode {
                Task {
                    await client.removeHeader(name: "X-MLS-Recovery-Mode")
                }
            }
        }

        // Use the real batch endpoint
        return try await publishKeyPackagesBatchDirect(packages, deviceId: deviceId)
    }

    /// Direct batch upload using blue.catbird.mlsChat.publishKeyPackages endpoint
    private func publishKeyPackagesBatchDirect(_ packages: [MLSKeyPackageUploadData], deviceId: String? = nil) async throws
        -> KeyPackageBatchResult
    {
        logger.info(
            "🌐 [MLSAPIClient.publishKeyPackagesBatchDirect] Using real batch endpoint - count: \(packages.count)"
        )

        // Convert custom types to generated types.
        // B14 normalization: pkg.keyPackage is already raw TLS bytes; wrap once
        // for the ATProto wire. (Previously a base64 round-trip lived here that
        // silently produced empty payloads when Data(base64Encoded:) returned nil.)
        let keyPackageItems = packages.map { pkg in
            BlueCatbirdMlsChatPublishKeyPackages.KeyPackageItem(
                keyPackage: Bytes(data: pkg.keyPackage),
                cipherSuite: pkg.cipherSuite,
                expires: pkg.expires.map { ATProtocolDate(date: $0) }
                    ?? ATProtocolDate(date: Date().addingTimeInterval(90 * 24 * 60 * 60)),
                lastResort: pkg.lastResort ? true : nil
            )
        }

        let input = BlueCatbirdMlsChatPublishKeyPackages.Input(
            action: "publishBatch",
            keyPackages: keyPackageItems,
            deviceId: deviceId
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.publishKeyPackages(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.publishKeyPackagesBatchDirect] HTTP \(responseCode)")
            if responseCode == 429 {
                // TODO: Extract Retry-After header if/when Petrel client exposes full response headers
                throw MLSAPIError.rateLimited(retryAfter: nil)
            }
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Batch upload failed")
        }

        // Convert generated batch error types back to custom result type
        let batchErrors = output.publishResult?.errors?.map { genError in
            BatchUploadError(index: genError.index, error: genError.error)
        }

        let succeeded = output.publishResult?.succeeded ?? 0
        let failed = output.publishResult?.failed ?? 0
        logger.info(
            "✅ [MLSAPIClient.publishKeyPackagesBatchDirect] SUCCESS - succeeded: \(succeeded), failed: \(failed)"
        )
        return KeyPackageBatchResult(succeeded: succeeded, failed: failed, errors: batchErrors)
    }

    /// Fallback: Upload packages individually with concurrent batching
    private func publishKeyPackagesFallback(_ packages: [MLSKeyPackageUploadData]) async throws
        -> KeyPackageBatchResult
    {
        var succeeded = 0
        var failed = 0
        var errors: [BatchUploadError] = []

        // Upload in concurrent batches of 5 to avoid overwhelming the server
        let batchSize = 5

        for batchIndex in stride(from: 0, to: packages.count, by: batchSize) {
            let endIndex = min(batchIndex + batchSize, packages.count)
            let batch = Array(packages[batchIndex ..< endIndex])

            await withTaskGroup(of: (index: Int, success: Bool, error: String?).self) { group in
                for (offset, package) in batch.enumerated() {
                    let globalIndex = batchIndex + offset
                    group.addTask {
                        do {
                            // B14 normalization: package.keyPackage is already raw TLS
                            // bytes — pass straight through to publishKeyPackage.
                            try await self.publishKeyPackage(
                                keyPackage: package.keyPackage,
                                cipherSuite: package.cipherSuite,
                                expiresAt: package.expires.map { ATProtocolDate(date: $0) },
                                idempotencyKey: package.idempotencyKey,
                                deviceId: package.deviceId,
                                lastResort: package.lastResort
                            )

                            return (globalIndex, true, nil)
                        } catch {
                            return (globalIndex, false, error.localizedDescription)
                        }
                    }
                }

                for await result in group {
                    if result.success {
                        succeeded += 1
                    } else {
                        failed += 1
                        if let errorMsg = result.error {
                            errors.append(BatchUploadError(index: result.index, error: errorMsg))
                        }
                    }
                }
            }

            // Small delay between batches to avoid rate limiting
            if endIndex < packages.count {
                try await Task.sleep(for: .milliseconds(100))
            }
        }

        logger.info(
            "✅ [MLSAPIClient.publishKeyPackagesBatch] COMPLETE - succeeded: \(succeeded), failed: \(failed)"
        )

        return KeyPackageBatchResult(
            succeeded: succeeded, failed: failed, errors: errors.isEmpty ? nil : errors
        )
    }

    /// Get admin statistics for a conversation (admin-only)
    /// - Parameter convoId: Conversation identifier
    /// - Returns: Admin statistics including member counts, message activity, and moderation metrics
    public func getAdminStats(convoId: String) async throws -> BlueCatbirdMlsChatUpdateConvo.Output {
        logger.info("🌐 [MLSAPIClient.getAdminStats] START - convoId: \(convoId)")

        let input = BlueCatbirdMlsChatUpdateConvo.Input(
            convoId: convoId,
            action: "getAdminStats"
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.updateConvo(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.getAdminStats] HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to get admin stats")
        }

        logger.info("✅ [MLSAPIClient.getAdminStats] SUCCESS")
        return output
    }

    // MARK: - Opt-In Management

    /// Opt in to MLS chat
    /// - Parameter deviceId: Optional device identifier for this opt-in
    /// - Returns: Tuple containing opt-in status and timestamp
    public func optIn(deviceId: String? = nil) async throws -> (optedIn: Bool, optedInAt: Date) {
        logger.info("🌐 [MLSAPIClient.optIn] START")

        let input = BlueCatbirdMlsChatOptIn.Input(action: "optIn", deviceId: deviceId)
        let (responseCode, output) = try await client.blue.catbird.mlsChat.optIn(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.optIn] HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to opt in")
        }

        logger.info("✅ [MLSAPIClient.optIn] SUCCESS")
        return (output.optedIn == true, output.optedInAt?.date ?? Date())
    }

    // MARK: - Multi-Device Sync

    /// Get pending device additions for conversations where new devices need to be added
    /// - Parameters:
    ///   - convoIds: Optional array of conversation IDs to filter (max 50)
    ///   - limit: Maximum number of pending additions to return (1-100, default: 50)
    /// - Returns: Array of pending device additions
    public func getPendingDeviceAdditions(
        convoIds: [String]? = nil,
        limit: Int = 50
    ) async throws -> [BlueCatbirdMlsChatCommitGroupChange.PendingDeviceAddition] {
        logger.info(
            "🌐 [MLSAPIClient.getPendingDeviceAdditions] START - convoIds: \(convoIds?.count ?? 0), limit: \(limit)"
        )

        let input = BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: convoIds?.first ?? "",
            action: "listPending"
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.commitGroupChange(
            input: input
        )

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.getPendingDeviceAdditions] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to get pending device additions"
            )
        }

        let pendingAdditions = output.pendingAdditions ?? []
        logger.info(
            "✅ [MLSAPIClient.getPendingDeviceAdditions] SUCCESS - \(pendingAdditions.count) pending additions"
        )
        return pendingAdditions
    }

    /// Claim a pending device addition to indicate this device will add the new device
    /// - Parameter pendingAdditionId: The ID of the pending addition to claim
    /// - Returns: Claim result with key package if successful
    public func claimPendingDeviceAddition(
        pendingAdditionId: String
    ) async throws -> BlueCatbirdMlsChatCommitGroupChange.Output {
        logger.info(
            "🌐 [MLSAPIClient.claimPendingDeviceAddition] START - pendingAdditionId: \(pendingAdditionId)"
        )

        let input = BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: "",
            action: "claimPending",
            pendingAdditionId: pendingAdditionId
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.commitGroupChange(
            input: input
        )

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.claimPendingDeviceAddition] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to claim pending device addition"
            )
        }

        logger.info(
            "✅ [MLSAPIClient.claimPendingDeviceAddition] SUCCESS - claimedAddition: \(output.claimedAddition != nil)"
        )
        return output
    }

    /// Complete a pending device addition after successfully adding the device via addMembers
    /// - Parameters:
    ///   - pendingAdditionId: The ID of the pending addition to complete
    ///   - newEpoch: The new epoch after the addMembers operation
    /// - Returns: Success status
    public func completePendingDeviceAddition(
        pendingAdditionId: String,
        newEpoch: Int
    ) async throws -> Bool {
        logger.info(
            "🌐 [MLSAPIClient.completePendingDeviceAddition] START - pendingAdditionId: \(pendingAdditionId), newEpoch: \(newEpoch)"
        )

        let input = BlueCatbirdMlsChatCommitGroupChange.Input(
            convoId: "",
            action: "completePending",
            pendingAdditionId: pendingAdditionId
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.commitGroupChange(
            input: input
        )

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.completePendingDeviceAddition] HTTP \(responseCode)")
            throw MLSAPIError.httpError(
                statusCode: responseCode, message: "Failed to complete pending device addition"
            )
        }

        if output.success {
            logger.info("✅ [MLSAPIClient.completePendingDeviceAddition] SUCCESS")
        } else {
            logger.warning(
                "⚠️ [MLSAPIClient.completePendingDeviceAddition] Failed"
            )
        }
        return output.success
    }

    // NOTE: Text-only PostgreSQL architecture (no CloudKit/R2 dependencies)
    // Message embeds are now fully encrypted within the ciphertext payload
    // Supported embed types (encrypted in MLSMessagePayload):
    //   - recordEmbed: Bluesky post quote embeds (AT-URI references)
    //   - linkEmbed: External link previews
    //   - gifEmbed: Tenor GIF embeds (MP4 format)
    // See blue.catbird.mlsChat.message.defs#payloadView for encrypted structure

    // MARK: - Encrypted E2EE Control Messages

    // These methods send encrypted payloads for reactions, read receipts, and typing indicators
    // All use sendMessage with delivery hints (persistent/ephemeral) per the greenfield E2EE design
    // See blue.catbird.mlsChat.message.defs for payload schemas

    /// Send an encrypted reaction via MLS application message
    ///
    /// The reaction payload is encrypted end-to-end. The server only sees:
    /// - `delivery: "persistent"` hint (for storage/replay)
    /// - Opaque ciphertext bytes
    ///
    /// - Parameters:
    ///   - convoId: Conversation identifier
    ///   - msgId: Client-generated ULID for this reaction message
    ///   - ciphertext: MLS-encrypted `MLSMessagePayload` with `messageType: "reaction"`
    ///   - epoch: MLS epoch when encrypted
    ///   - paddedSize: Padded ciphertext size bucket
    /// - Returns: Server response with messageId, receivedAt, seq, epoch
    public func sendEncryptedReaction(
        convoId: String,
        msgId: String,
        ciphertext: Data,
        epoch: Int,
        paddedSize: Int
    ) async throws -> (messageId: String, receivedAt: ATProtocolDate, seq: Int, epoch: Int) {
        logger.debug("Sending encrypted reaction for convoId: \(convoId)")

        let input = BlueCatbirdMlsChatSendMessage.Input(
            convoId: convoId,
            msgId: msgId,
            ciphertext: Bytes(data: ciphertext),
            epoch: epoch,
            paddedSize: paddedSize,
            delivery: "persistent" // Reactions persist for offline sync
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.sendMessage(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ Failed to send encrypted reaction: HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to send reaction")
        }

        logger.debug("✅ Encrypted reaction sent: msgId=\(output.messageId), seq=\(output.seq)")
        return (output.messageId, output.receivedAt, output.seq, output.epoch)
    }

    // sendEncryptedReadReceipt and sendEncryptedTypingIndicator have been removed
    // to reduce complexity. Only sendEncryptedReaction remains for control messages.
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

public extension MLSAPIError {
    fileprivate init(from error: ATProtoError<BlueCatbirdMlsChatCreateConvo.Error>) {
        let detail = error.message
        switch error.error {
        case .keyPackageNotFound:
            self = .keyPackageNotFound(detail: detail)
        case .invalidCipherSuite:
            self = .invalidCipherSuite(detail: detail)
        case .tooManyMembers:
            self = .tooManyMembers(detail: detail)
        case .mutualBlockDetected:
            self = .mutualBlockDetected(detail: detail)
        case .convoAlreadyExists:
            self = .convoAlreadyExists(detail: detail)
        }
    }

    init(from error: ATProtoError<BlueCatbirdMlsChatBootstrapResetGroup.Error>) {
        let detail = error.message
        switch error.error {
        case .bootstrapTargetNotFound:
            self = .bootstrapTargetNotFound(detail: detail)
        case .alreadyBootstrapped:
            self = .alreadyBootstrapped(detail: detail)
        case .notMember:
            self = .notMember(detail: detail)
        case .invalidCipherSuite:
            self = .invalidCipherSuite(detail: detail)
        }
    }

    fileprivate init(from error: ATProtoError<BlueCatbirdMlsChatCommitGroupChange.Error>) {
        let detail = error.message
        switch error.error {
        case .convoNotFound:
            self = .conversationNotFound(detail: detail)
        case .notMember:
            self = .notConversationMember(detail: detail)
        case .keyPackageNotFound:
            self = .keyPackageNotFound(detail: detail)
        case .alreadyMember:
            self = .memberAlreadyExists(detail: detail)
        case .tooManyMembers:
            self = .tooManyMembers(detail: detail)
        case .blockedByMember:
            self = .memberBlocked(detail: detail)
        case .invalidAction:
            self = .httpError(statusCode: 400, message: detail ?? "Invalid action")
        case .invalidCommit:
            self = .httpError(statusCode: 400, message: detail ?? "Invalid commit")
        case .invalidGroupInfo:
            self = .httpError(statusCode: 400, message: detail ?? "Invalid group info")
        case .pendingAdditionNotFound:
            self = .httpError(statusCode: 404, message: detail ?? "Pending addition not found")
        case .pendingAdditionAlreadyClaimed:
            self = .httpError(statusCode: 409, message: detail ?? "Pending addition already claimed")
        case .unauthorized:
            self = .httpError(statusCode: 403, message: detail ?? "Unauthorized")
        // Layer 1 robustness errors (~/.claude/plans/rippling-greeting-whale.md §Layer 1).
        // Map to httpError until/unless MLSAPIError gains dedicated cases.
        case .noKeyPackagesPublished:
            self = .httpError(
                statusCode: 412,
                message: detail ?? "Device must publish at least one key package before issuing an External Commit"
            )
        case .groupFrozen:
            self = .httpError(
                statusCode: 423,
                message: detail ?? "Conversation is being repaired (epoch-storm circuit breaker). Retry shortly."
            )
        case .rateLimited:
            // The 429 body carries structured retryAfterSeconds + scope, but we
            // don't have access to it from this enum — caller can parse the
            // response body separately if it needs the retry hint.
            self = .rateLimited(retryAfter: nil)
        }
    }
}

// MARK: - MLSAPIClient Event Stream Extension

public extension MLSAPIClient {
    /// Stream real-time conversation events via firehose-style WebSocket framing
    /// - Parameters:
    ///   - convoId: ID of the conversation to stream events for
    ///   - cursor: Optional cursor for resuming from last position
    /// - Returns: AsyncThrowingStream of conversation events
    func subscribeEvents(convoId _: String, cursor: String? = nil) async throws
        -> AsyncThrowingStream<BlueCatbirdMlsChatSubscribeEvents.Message, Error>
    {
        let input = BlueCatbirdMlsChatSubscribeEvents.Parameters(ticket: nil, cursor: cursor)

        // Petrel subscription uses DAG-CBOR framing and $type-based unions
        return try await client.blue.catbird.mlsChat.subscribeEvents(input: input)
    }
}

// MARK: - WebSocket Subscription Support

public extension MLSAPIClient {
    /// Get a short-lived signed ticket for subscribing to MLS events via WebSocket.
    /// The ticket is valid for 30 seconds and must be used to establish a WebSocket connection.
    ///
    /// - Parameter convoId: Optional conversation ID to filter events for. If nil, receives events for all conversations.
    /// - Returns: Subscription ticket containing the JWT ticket, WebSocket endpoint, and expiration time.
    /// - Throws: MLSAPIError if the request fails
    func getSubscriptionTicket(convoId: String? = nil) async throws -> BlueCatbirdMlsChatGetSubscriptionTicket.Output {
        logger.info("🎫 [MLSAPIClient.getSubscriptionTicket] START - convoId: \(convoId ?? "all")")

        let input = BlueCatbirdMlsChatGetSubscriptionTicket.Input(convoIds: convoId.map { [$0] })

        let (responseCode, output) = try await client.blue.catbird.mlsChat.getSubscriptionTicket(input: input)

        guard responseCode == 200, let output = output else {
            logger.error("❌ [MLSAPIClient.getSubscriptionTicket] HTTP \(responseCode)")
            throw MLSAPIError.httpError(statusCode: responseCode, message: "Failed to get subscription ticket")
        }

        logger.info("✅ [MLSAPIClient.getSubscriptionTicket] SUCCESS - endpoint: \(output.endpoint?.description ?? "nil"), expiresAt: \(output.expiresAt.date)")
        return output
    }

    // MARK: - Metadata v2 Blob Storage

    /// Upload an encrypted group metadata blob to the server.
    /// The blob is opaque ciphertext -- the server never sees plaintext metadata.
    /// The blobLocator (UUIDv4) is client-generated and serves as the idempotency key.
    ///
    /// - Parameters:
    ///   - blobLocator: Client-generated UUIDv4 blob locator
    ///   - groupId: Hex-encoded MLS group ID
    ///   - encryptedBlob: Encrypted blob bytes (nonce || ciphertext || tag)
    /// - Returns: The confirmed blob locator and stored size
    /// - Throws: MLSAPIError on failure
    func putGroupMetadataBlob(
        blobLocator: String,
        groupId: String,
        conversationId: String? = nil,
        resetGeneration: Int? = nil,
        metadataVersion: UInt64? = nil,
        kind: String? = nil,
        encryptedBlob: Data
    ) async throws -> (blobLocator: String, size: Int) {
        logger.info(
            "📤 [MLSAPIClient.putGroupMetadataBlob] START - locator: \(blobLocator.prefix(8))..., group: \(groupId.prefix(16))..., size: \(encryptedBlob.count) bytes"
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.putGroupMetadataBlob(
            data: encryptedBlob,
            mimeType: "application/octet-stream",
            stripMetadata: false,
            params: BlueCatbirdMlsChatPutGroupMetadataBlob.Parameters(
                blobLocator: blobLocator,
                groupId: groupId,
                convoId: conversationId,
                resetGeneration: resetGeneration,
                metadataVersion: metadataVersion.map(Int.init),
                kind: kind
            )
        )

        guard responseCode == 200, let output = output else {
            logger.error(
                "❌ [MLSAPIClient.putGroupMetadataBlob] HTTP \(responseCode) for locator \(blobLocator.prefix(8))..."
            )
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to upload metadata blob (HTTP \(responseCode))"
            )
        }

        logger.info(
            "✅ [MLSAPIClient.putGroupMetadataBlob] SUCCESS - locator: \(output.blobLocator.prefix(8))..., size: \(output.size) bytes"
        )
        return (output.blobLocator, output.size)
    }

    /// Fetch an encrypted group metadata blob from the server by locator.
    /// Returns raw encrypted bytes that require the MLS epoch key for decryption.
    ///
    /// - Parameters:
    ///   - blobLocator: The blob locator to fetch
    ///   - groupId: Hex-encoded MLS group ID (for server-side membership check)
    /// - Returns: The encrypted blob bytes
    /// - Throws: MLSAPIError on failure (including BlobNotFound for GC'd blobs)
    func getGroupMetadataBlob(
        blobLocator: String,
        groupId: String,
        conversationId: String? = nil,
        resetGeneration: Int? = nil,
        metadataVersion: UInt64? = nil,
        kind: String? = nil
    ) async throws -> Data {
        logger.info(
            "📥 [MLSAPIClient.getGroupMetadataBlob] START - locator: \(blobLocator.prefix(8))..., group: \(groupId.prefix(16))..."
        )

        let input = BlueCatbirdMlsChatGetGroupMetadataBlob.Parameters(
            blobLocator: blobLocator,
            convoId: conversationId,
            groupId: groupId,
            resetGeneration: resetGeneration,
            metadataVersion: metadataVersion.map(Int.init),
            kind: kind
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.getGroupMetadataBlob(
            input: input
        )

        guard responseCode == 200, let output = output else {
            if responseCode == 404 {
                logger.warning(
                    "⚠️ [MLSAPIClient.getGroupMetadataBlob] BlobNotFound - locator: \(blobLocator.prefix(8))... (may be GC'd)"
                )
                throw MLSAPIError.httpError(
                    statusCode: 404,
                    message: "Metadata blob not found (locator: \(blobLocator))"
                )
            }
            logger.error(
                "❌ [MLSAPIClient.getGroupMetadataBlob] HTTP \(responseCode) for locator \(blobLocator.prefix(8))..."
            )
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to fetch metadata blob (HTTP \(responseCode))"
            )
        }

        logger.info(
            "✅ [MLSAPIClient.getGroupMetadataBlob] SUCCESS - locator: \(blobLocator.prefix(8))..., size: \(output.data.count) bytes"
        )
        return output.data
    }

    /// Fetch the latest encrypted group metadata blob from the server by group ID alone.
    /// Used when no blob locator is available (e.g., after a Welcome join where the
    /// AppDataDictionary doesn't contain a MetadataReference yet).
    ///
    /// The server returns the most recently uploaded blob for this group.
    ///
    /// - Parameter groupId: Hex-encoded MLS group ID
    /// - Returns: The encrypted blob bytes
    /// - Throws: MLSAPIError on failure (including 404 if no blob exists for this group)
    func getLatestGroupMetadataBlob(
        groupId: String,
        conversationId: String? = nil,
        resetGeneration: Int? = nil,
        metadataVersion: UInt64? = nil,
        kind: String? = nil
    ) async throws -> Data {
        logger.info(
            "📥 [MLSAPIClient.getLatestGroupMetadataBlob] START - group: \(groupId.prefix(16))... (no locator, fetching latest)"
        )

        // Pass nil blobLocator — server will return the latest blob for this group
        let input = BlueCatbirdMlsChatGetGroupMetadataBlob.Parameters(
            convoId: conversationId,
            groupId: groupId,
            resetGeneration: resetGeneration,
            metadataVersion: metadataVersion.map(Int.init),
            kind: kind
        )

        let (responseCode, output) = try await client.blue.catbird.mlsChat.getGroupMetadataBlob(
            input: input
        )

        guard responseCode == 200, let output = output else {
            if responseCode == 404 {
                logger.warning(
                    "⚠️ [MLSAPIClient.getLatestGroupMetadataBlob] No blob found for group \(groupId.prefix(16))..."
                )
                throw MLSAPIError.httpError(
                    statusCode: 404,
                    message: "No metadata blob found for group \(groupId)"
                )
            }
            logger.error(
                "❌ [MLSAPIClient.getLatestGroupMetadataBlob] HTTP \(responseCode) for group \(groupId.prefix(16))..."
            )
            throw MLSAPIError.httpError(
                statusCode: responseCode,
                message: "Failed to fetch latest metadata blob (HTTP \(responseCode))"
            )
        }

        logger.info(
            "✅ [MLSAPIClient.getLatestGroupMetadataBlob] SUCCESS - group: \(groupId.prefix(16))..., size: \(output.data.count) bytes"
        )
        return output.data
    }
}

// NOTE: All model types now use BlueCatbirdMls* models from Petrel package
// Updated for text-only PostgreSQL architecture (no CloudKit/R2 dependencies):
// - BlueCatbirdMlsChatDefs.ConvoView: Conversation with MLS group info
// - BlueCatbirdMlsChatDefs.MessageView: Encrypted message with optional embeds (Tenor, Bluesky)
// - BlueCatbirdMlsChatDefs.MemberView: Conversation member with MLS credentials
// - BlueCatbirdMlsChatDefs.KeyPackageRef: MLS key package for adding members
// - BlueCatbirdMlsChatDefs.ConvoMetadata: Conversation name and description (no avatar)
// - Removed: ExternalAsset, BlobRef, avatar fields (text-only system)
