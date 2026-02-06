//
//  MLSError.swift
//  Catbird
//
//  MLS-specific error types
//

import Foundation

/// Errors specific to MLS operations
public enum MLSError: LocalizedError {
    case conversationNotFound
    case noCurrentUser
    case operationFailed
    case welcomeProcessingTimeout(message: String)
    case configurationError
    case ratchetStateDesync(message: String)
    case ignoredOldEpochMessage
    /// Message was already decrypted (by NSE or duplicate processing) - safe to skip
    /// This is NOT an error condition - the message content should be in the cache
    case secretReuseSkipped(messageID: String)
    case invalidContent(String)
    case invalidCredential(String)
    /// GroupInfo has expired and cannot be used for External Commit
    case staleGroupInfo(convoId: String, message: String)
    /// GroupInfo is invalid (too small, wrong format, or encoding issues)
    case invalidGroupInfo(convoId: String, message: String)
    /// Member is already in the MLS group (duplicate add attempt)
    case memberAlreadyInGroup(member: String)
    /// In-memory MLS state is stale compared to on-disk state (NSE advanced ratchet)
    /// This indicates a race was detected and recovered; the operation should be retried
    case staleStateDetected(memoryEpoch: UInt64, diskEpoch: UInt64)
    /// State reload is in progress after NSE notification
    /// Operations should wait for reload to complete before proceeding
    case stateReloadInProgress
    /// Account switch detected during MLS operation - abort to prevent state corruption
    /// The operation should not be retried; the user context is invalid
    case accountSwitchInterrupted(epochBefore: UInt64, epochAfter: UInt64)
    /// Attempted to decrypt a message we sent ourselves
    /// MLS encrypts for recipients only - senders cannot decrypt their own messages
    /// The caller should use the pre-cached payload from the send operation
    case cannotDecryptOwnMessage
    /// Context creation blocked because app is transitioning to background (0xdead10cc prevention)
    /// This is a protective measure - MLS operations should not start during suspension
    case contextCreationBlocked(reason: String)

    public var errorDescription: String? {
        switch self {
        case .conversationNotFound:
            return "Conversation not found"
        case .noCurrentUser:
            return "No current user authenticated"
        case .operationFailed:
            return "The operation failed."
        case .welcomeProcessingTimeout(message: let message):
            // Defensive: Copy string to ensure it's valid and retained
            let safeCopy = String(describing: message)
            return "Welcome message processing timed out: \(safeCopy)"
        case .configurationError:
            return "MLS client not properly configured"
        case .ratchetStateDesync(message: let message):
            // Defensive: Copy string to ensure it's valid and retained
            let safeCopy = String(describing: message)
            return "MLS cryptographic state out of sync: \(safeCopy)"
        case .ignoredOldEpochMessage:
            return "Message from old epoch ignored"
        case .secretReuseSkipped(let messageID):
            // This is expected when NSE already decrypted the message
            // The caller should check the database cache for the plaintext
            return "Message \(messageID) already decrypted (SecretReuseError) - check cache"
        case .invalidContent(let message):
            // Defensive: Copy string to ensure it's valid and retained
            let safeCopy = String(describing: message)
            return "Invalid message content: \(safeCopy)"
        case .invalidCredential(let message):
            // Defensive: Copy string to ensure it's valid and retained
            let safeCopy = String(describing: message)
            return "Invalid credential: \(safeCopy)"
        case .staleGroupInfo(let convoId, let message):
            // Defensive: Copy strings to ensure they're valid and retained
            let safeConvoId = String(describing: convoId)
            let safeMessage = String(describing: message)
            return "Stale GroupInfo for \(safeConvoId): \(safeMessage)"
        case .invalidGroupInfo(let convoId, let message):
            // Defensive: Copy strings to ensure they're valid and retained
            let safeConvoId = String(describing: convoId)
            let safeMessage = String(describing: message)
            return "Invalid GroupInfo for \(safeConvoId): \(safeMessage)"
        case .memberAlreadyInGroup(let member):
            let safeMember = String(describing: member)
            return "Member \(safeMember) is already in the MLS group"
        case .staleStateDetected(let memoryEpoch, let diskEpoch):
            return "In-memory MLS state is stale (memory epoch \(memoryEpoch), disk epoch \(diskEpoch)). NSE likely advanced the ratchet."
        case .stateReloadInProgress:
            return "MLS state reload in progress - retry after completion"
        case .accountSwitchInterrupted(let epochBefore, let epochAfter):
            return
                "Account switch detected during operation (epoch \(epochBefore) → \(epochAfter)) - aborting to prevent state corruption"
        case .cannotDecryptOwnMessage:
            return "Cannot decrypt own message - MLS encrypts for recipients only. Use cached payload from send operation."
        case .contextCreationBlocked(let reason):
            return "MLS context creation blocked: \(reason)"
        }
    }
}
