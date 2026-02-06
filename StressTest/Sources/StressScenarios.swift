//
//  StressScenarios.swift
//  MLSStressTest
//
//  Pre-built stress test scenarios for MLS testing
//

import CatbirdMLSCore
import Foundation

/// Collection of stress test scenarios
public struct StressScenarios {
  
  // MARK: - Two-User Rapid Messaging
  
  /// Test rapid message exchange between two clients (baseline)
  public static func twoUserRapidMessaging(messageCount: Int = 100) async throws -> ScenarioResult {
    print("\n" + String(repeating: "=", count: 60))
    print("🧪 SCENARIO: Two-User Rapid Messaging (\(messageCount) messages each)")
    print(String(repeating: "=", count: 60))
    
    let startTime = ContinuousClock.now
    
    // Create two clients with isolated storage
    let alice = StressTestClient(clientId: "alice")
    let bob = StressTestClient(clientId: "bob")
    
    // Alice creates group
    let groupId = try await alice.createGroup()
    
    // Alice adds Bob (simplified - in real scenario this involves key packages)
    // For now, we'll test with two clients encrypting/decrypting to same group
    // TODO: Implement full Welcome flow when key package APIs are exposed
    
    // Alice sends messages, Bob would decrypt
    print("\n📨 Alice sending \(messageCount) messages...")
    for i in 1...messageCount {
      let message = "Message \(i) from Alice at \(Date())"
      let ciphertext = try await alice.sendMessage(message, to: groupId)
      
      if i % 20 == 0 {
        print("   ... sent \(i)/\(messageCount)")
      }
    }
    
    let elapsed = startTime.duration(to: ContinuousClock.now)
    let aliceMetrics = await alice.getMetrics()
    
    print("\n📊 Results:")
    print("   Messages encrypted: \(aliceMetrics.messagesEncrypted)")
    print("   Encrypt errors: \(aliceMetrics.encryptErrors)")
    print("   SQLITE_BUSY: \(aliceMetrics.sqliteBusyCount)")
    print("   SecretReuseError: \(aliceMetrics.secretReuseErrors)")
    print("   Avg encrypt latency: \(String(format: "%.2f", aliceMetrics.avgEncryptLatencyMs))ms")
    print("   Total time: \(elapsed)")
    
    return ScenarioResult(
      name: "Two-User Rapid Messaging",
      clients: 2,
      messagesProcessed: aliceMetrics.messagesEncrypted,
      errors: aliceMetrics.encryptErrors,
      sqliteBusyCount: aliceMetrics.sqliteBusyCount,
      secretReuseErrors: aliceMetrics.secretReuseErrors,
      duration: elapsed
    )
  }
  
  // MARK: - Multi-Client Swarm
  
  /// Test many clients sending to same group concurrently
  public static func multiClientSwarm(clientCount: Int = 10, messagesPerClient: Int = 20) async throws -> ScenarioResult {
    print("\n" + String(repeating: "=", count: 60))
    print("🧪 SCENARIO: Multi-Client Swarm (\(clientCount) clients, \(messagesPerClient) msgs each)")
    print(String(repeating: "=", count: 60))
    
    let startTime = ContinuousClock.now
    
    // Create clients
    var clients: [StressTestClient] = []
    for i in 1...clientCount {
      let client = StressTestClient(clientId: "client-\(i)")
      clients.append(client)
    }
    
    // First client creates group
    let groupId = try await clients[0].createGroup()
    
    // All clients send messages concurrently
    print("\n📨 \(clientCount) clients sending \(messagesPerClient) messages each concurrently...")
    
    await withTaskGroup(of: Void.self) { group in
      for (index, client) in clients.enumerated() {
        group.addTask {
          for j in 1...messagesPerClient {
            let message = "Client-\(index + 1) message \(j)"
            do {
              _ = try await client.sendMessage(message, to: groupId)
            } catch {
              // Errors are tracked in metrics
            }
          }
        }
      }
    }
    
    let elapsed = startTime.duration(to: ContinuousClock.now)
    
    // Aggregate metrics
    var totalMessages = 0
    var totalErrors = 0
    var totalBusy = 0
    var totalSecretReuse = 0
    
    for client in clients {
      let m = await client.getMetrics()
      totalMessages += m.messagesEncrypted
      totalErrors += m.encryptErrors
      totalBusy += m.sqliteBusyCount
      totalSecretReuse += m.secretReuseErrors
    }
    
    print("\n📊 Results:")
    print("   Total messages encrypted: \(totalMessages) / \(clientCount * messagesPerClient)")
    print("   Total errors: \(totalErrors)")
    print("   SQLITE_BUSY: \(totalBusy)")
    print("   SecretReuseError: \(totalSecretReuse)")
    print("   Total time: \(elapsed)")
    print("   Throughput: \(String(format: "%.1f", Double(totalMessages) / Double(elapsed.components.seconds))) msgs/sec")
    
    return ScenarioResult(
      name: "Multi-Client Swarm",
      clients: clientCount,
      messagesProcessed: totalMessages,
      errors: totalErrors,
      sqliteBusyCount: totalBusy,
      secretReuseErrors: totalSecretReuse,
      duration: elapsed
    )
  }
  
  // MARK: - Shared Database Stress (App + NSE Simulation)
  
  /// Test multiple clients hitting the same database (simulates App + NSE)
  public static func sharedDatabaseStress(clientCount: Int = 4, messagesPerClient: Int = 50) async throws -> ScenarioResult {
    print("\n" + String(repeating: "=", count: 60))
    print("🧪 SCENARIO: Shared Database Stress (\(clientCount) processes, same DB)")
    print(String(repeating: "=", count: 60))
    
    let startTime = ContinuousClock.now
    
    // Create shared storage directory
    let sharedDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("mls-stress-test")
      .appendingPathComponent("shared-db-\(UUID().uuidString)")
    
    try FileManager.default.createDirectory(at: sharedDir, withIntermediateDirectories: true)
    print("📁 Shared storage: \(sharedDir.path)")
    
    // Create clients pointing to SAME storage (simulates App + NSE)
    var clients: [StressTestClient] = []
    for i in 1...clientCount {
      let client = StressTestClient(clientId: "process-\(i)", sharedStorageDirectory: sharedDir)
      clients.append(client)
    }
    
    // First client creates group
    let groupId = try await clients[0].createGroup()
    
    // All clients send messages concurrently to same DB
    print("\n📨 \(clientCount) 'processes' sending \(messagesPerClient) messages each to SHARED DB...")
    print("   (This simulates App + NSE concurrent access)")
    
    await withTaskGroup(of: Void.self) { group in
      for (index, client) in clients.enumerated() {
        group.addTask {
          for j in 1...messagesPerClient {
            let message = "Process-\(index + 1) message \(j)"
            do {
              _ = try await client.sendMessage(message, to: groupId)
            } catch {
              // Errors are tracked in metrics
            }
            
            // Small random delay to simulate real timing
            try? await Task.sleep(nanoseconds: UInt64.random(in: 1_000_000...5_000_000))
          }
        }
      }
    }
    
    let elapsed = startTime.duration(to: ContinuousClock.now)
    
    // Aggregate metrics
    var totalMessages = 0
    var totalErrors = 0
    var totalBusy = 0
    var totalSecretReuse = 0
    
    for client in clients {
      let m = await client.getMetrics()
      totalMessages += m.messagesEncrypted
      totalErrors += m.encryptErrors
      totalBusy += m.sqliteBusyCount
      totalSecretReuse += m.secretReuseErrors
    }
    
    // Cleanup
    try? FileManager.default.removeItem(at: sharedDir)
    
    print("\n📊 Results:")
    print("   Total messages encrypted: \(totalMessages) / \(clientCount * messagesPerClient)")
    print("   Total errors: \(totalErrors)")
    print("   SQLITE_BUSY (🎯 key metric): \(totalBusy)")
    print("   SecretReuseError: \(totalSecretReuse)")
    print("   Total time: \(elapsed)")
    
    if totalBusy > 0 {
      print("\n⚠️  SQLITE_BUSY detected! This indicates WAL contention.")
    }
    if totalSecretReuse > 0 {
      print("\n🚨 SecretReuseError detected! This indicates ratchet corruption.")
    }
    
    return ScenarioResult(
      name: "Shared Database Stress",
      clients: clientCount,
      messagesProcessed: totalMessages,
      errors: totalErrors,
      sqliteBusyCount: totalBusy,
      secretReuseErrors: totalSecretReuse,
      duration: elapsed
    )
  }
}

// MARK: - Scenario Result

public struct ScenarioResult: Sendable {
  public let name: String
  public let clients: Int
  public let messagesProcessed: Int
  public let errors: Int
  public let sqliteBusyCount: Int
  public let secretReuseErrors: Int
  public let duration: Duration
  
  public var passed: Bool {
    errors == 0 && secretReuseErrors == 0
  }
  
  public func printSummary() {
    let status = passed ? "✅ PASSED" : "❌ FAILED"
    print("\(status): \(name)")
    print("   Clients: \(clients), Messages: \(messagesProcessed), Errors: \(errors)")
    if sqliteBusyCount > 0 {
      print("   ⚠️  SQLITE_BUSY: \(sqliteBusyCount)")
    }
    if secretReuseErrors > 0 {
      print("   🚨 SecretReuseError: \(secretReuseErrors)")
    }
  }
}
