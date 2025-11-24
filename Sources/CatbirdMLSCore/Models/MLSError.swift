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
    case invalidContent(String)
    case invalidCredential(String)

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
        case .invalidContent(let message):
            // Defensive: Copy string to ensure it's valid and retained
            let safeCopy = String(describing: message)
            return "Invalid message content: \(safeCopy)"
        case .invalidCredential(let message):
            // Defensive: Copy string to ensure it's valid and retained
            let safeCopy = String(describing: message)
            return "Invalid credential: \(safeCopy)"
        }
    }
}
