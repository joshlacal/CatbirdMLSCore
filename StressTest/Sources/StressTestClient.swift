//
//  StressTestClient.swift
//  MLSStressTest
//
//  Simulates a single MLS client for stress testing
//

import CatbirdMLSCore
import Foundation

/// Represents a single MLS client identity for stress testing
public actor StressTestClient {
  
  // MARK: - Properties
  
  /// Unique identifier for this client
  public let clientId: String
  
  /// The MLS context for this client
  private let context: MLSCoreContext
  
  /// User DID for this client
  public let userDID: String
  
  /// Groups this client is a member of
  private var joinedGroups: Set<String> = []
  
  /// Metrics collection
  private var metrics: ClientMetrics = ClientMetrics()
  
  // MARK: - Metrics
  
  public struct ClientMetrics: Sendable {
    public var messagesEncrypted: Int = 0
    public var messagesDecrypted: Int = 0
    public var groupsCreated: Int = 0
    public var groupsJoined: Int = 0
    public var encryptErrors: Int = 0
    public var decryptErrors: Int = 0
    public var sqliteBusyCount: Int = 0
    public var secretReuseErrors: Int = 0
    public var totalEncryptLatencyMs: Double = 0
    public var totalDecryptLatencyMs: Double = 0
    
    public var avgEncryptLatencyMs: Double {
      messagesEncrypted > 0 ? totalEncryptLatencyMs / Double(messagesEncrypted) : 0
    }
    
    public var avgDecryptLatencyMs: Double {
      messagesDecrypted > 0 ? totalDecryptLatencyMs / Double(messagesDecrypted) : 0
    }
  }
  
  // MARK: - Initialization
  
  /// Create a new stress test client with isolated storage
  /// - Parameters:
  ///   - clientId: Unique identifier for this client
  ///   - sharedStorageDirectory: Optional shared storage for multi-client same-DB testing
  public init(clientId: String, sharedStorageDirectory: URL? = nil) {
    self.clientId = clientId
    self.userDID = "did:plc:stress-test-\(clientId)"
    
    // Create isolated or shared configuration
    if let sharedDir = sharedStorageDirectory {
      let config = MLSCoreContext.Configuration(
        storageDirectory: sharedDir,
        keychainAccessGroup: nil,
        disableDarwinNotifications: true,
        loggerSubsystem: "blue.catbird.mls.stress.\(clientId)"
      )
      self.context = MLSCoreContext(configuration: config)
    } else {
      let config = MLSCoreContext.Configuration.stressTest(isolationId: clientId)
      self.context = MLSCoreContext(configuration: config)
    }
    
    print("📦 [Client \(clientId)] Initialized with userDID: \(userDID)")
  }
  
  // MARK: - Group Operations
  
  /// Create a new MLS group
  /// - Returns: Group ID as Data
  public func createGroup() async throws -> Data {
    let start = ContinuousClock.now
    
    do {
      let mlsContext = try await context.getContext(for: userDID)
      let result = try mlsContext.createGroup(identityBytes: userDID.data(using: .utf8)!, config: nil)
      
      let groupIdHex = result.groupId.hexEncodedString()
      joinedGroups.insert(groupIdHex)
      metrics.groupsCreated += 1
      
      let elapsed = start.duration(to: ContinuousClock.now)
      print("✅ [Client \(clientId)] Created group \(groupIdHex.prefix(16))... in \(elapsed)")
      
      return result.groupId
    } catch {
      print("❌ [Client \(clientId)] Failed to create group: \(error)")
      throw error
    }
  }
  
  /// Process a Welcome message to join a group
  /// - Parameter welcome: Welcome message data
  /// - Returns: Group ID
  public func joinGroup(welcome: Data) async throws -> Data {
    let start = ContinuousClock.now
    
    do {
      let mlsContext = try await context.getContext(for: userDID)
      let result = try mlsContext.processWelcome(
        welcomeBytes: welcome,
        identityBytes: userDID.data(using: .utf8)!,
        config: nil
      )
      
      let groupIdHex = result.groupId.hexEncodedString()
      joinedGroups.insert(groupIdHex)
      metrics.groupsJoined += 1
      
      let elapsed = start.duration(to: ContinuousClock.now)
      print("✅ [Client \(clientId)] Joined group \(groupIdHex.prefix(16))... in \(elapsed)")
      
      return result.groupId
    } catch {
      print("❌ [Client \(clientId)] Failed to join group: \(error)")
      throw error
    }
  }
  
  // MARK: - Messaging
  
  /// Encrypt and send a message to a group
  /// - Parameters:
  ///   - text: Message text
  ///   - groupId: Target group ID
  /// - Returns: Ciphertext data
  public func sendMessage(_ text: String, to groupId: Data) async throws -> Data {
    let start = ContinuousClock.now
    
    do {
      let mlsContext = try await context.getContext(for: userDID)
      guard let plaintext = text.data(using: .utf8) else {
        throw StressTestError.invalidMessage
      }
      
      let result = try mlsContext.encryptMessage(groupId: groupId, plaintext: plaintext)
      
      let elapsed = start.duration(to: ContinuousClock.now)
      let latencyMs = Double(elapsed.components.attoseconds) / 1_000_000_000_000_000
      
      metrics.messagesEncrypted += 1
      metrics.totalEncryptLatencyMs += latencyMs
      
      return result.ciphertext
    } catch {
      metrics.encryptErrors += 1
      classifyError(error)
      print("❌ [Client \(clientId)] Encrypt failed: \(error)")
      throw error
    }
  }
  
  /// Decrypt a received message
  /// - Parameters:
  ///   - ciphertext: Encrypted message data
  ///   - groupId: Source group ID
  /// - Returns: Decrypted plaintext string
  public func receiveMessage(_ ciphertext: Data, in groupId: Data) async throws -> String {
    let start = ContinuousClock.now
    
    do {
      let mlsContext = try await context.getContext(for: userDID)
      let result = try mlsContext.decryptMessage(groupId: groupId, ciphertext: ciphertext)
      
      let elapsed = start.duration(to: ContinuousClock.now)
      let latencyMs = Double(elapsed.components.attoseconds) / 1_000_000_000_000_000
      
      metrics.messagesDecrypted += 1
      metrics.totalDecryptLatencyMs += latencyMs
      
      guard let plaintext = String(data: result.plaintext, encoding: .utf8) else {
        throw StressTestError.invalidMessage
      }
      
      return plaintext
    } catch {
      metrics.decryptErrors += 1
      classifyError(error)
      print("❌ [Client \(clientId)] Decrypt failed: \(error)")
      throw error
    }
  }
  
  // MARK: - Metrics
  
  /// Get current metrics for this client
  public func getMetrics() -> ClientMetrics {
    return metrics
  }
  
  /// Reset metrics
  public func resetMetrics() {
    metrics = ClientMetrics()
  }
  
  // MARK: - Error Classification
  
  private func classifyError(_ error: Error) {
    let errorString = String(describing: error)
    
    if errorString.contains("SQLITE_BUSY") || errorString.contains("database is locked") {
      metrics.sqliteBusyCount += 1
    }
    
    if errorString.contains("SecretReuseError") || errorString.contains("secret reuse") {
      metrics.secretReuseErrors += 1
    }
  }
}

// MARK: - Errors

public enum StressTestError: Error, LocalizedError {
  case invalidMessage
  case groupNotFound
  case clientNotInitialized
  
  public var errorDescription: String? {
    switch self {
    case .invalidMessage: return "Invalid message encoding"
    case .groupNotFound: return "Group not found"
    case .clientNotInitialized: return "Client not initialized"
    }
  }
}

// MARK: - Data Extension

extension Data {
  func hexEncodedString() -> String {
    map { String(format: "%02x", $0) }.joined()
  }
}
