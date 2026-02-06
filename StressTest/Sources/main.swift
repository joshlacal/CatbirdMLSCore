//
//  main.swift
//  MLSStressTest
//
//  CLI entry point for MLS stress testing
//

import Foundation

// Swift 6 compatible async entry point
let semaphore = DispatchSemaphore(value: 0)

Task {
  await MLSStressTestCLI.run()
  semaphore.signal()
}

semaphore.wait()

enum MLSStressTestCLI {
  
  static func run() async {
    print("""
    
    ╔══════════════════════════════════════════════════════════════╗
    ║               MLS STRESS TEST HARNESS                        ║
    ║                                                              ║
    ║  Hit your M4 Max with 250 tons of MLS operations             ║
    ╚══════════════════════════════════════════════════════════════╝
    
    """)
    
    // Parse command line arguments
    let args = CommandLine.arguments
    let clientCount = parseArg(args, flag: "--clients", default: 10)
    let messageCount = parseArg(args, flag: "--messages", default: 50)
    let scenario = parseStringArg(args, flag: "--scenario", default: "all")
    
    print("Configuration:")
    print("  Clients: \(clientCount)")
    print("  Messages per client: \(messageCount)")
    print("  Scenario: \(scenario)")
    print("")
    
    var results: [ScenarioResult] = []
    
    do {
      switch scenario {
      case "two-user":
        let result = try await StressScenarios.twoUserRapidMessaging(messageCount: messageCount)
        results.append(result)
        
      case "swarm":
        let result = try await StressScenarios.multiClientSwarm(
          clientCount: clientCount,
          messagesPerClient: messageCount
        )
        results.append(result)
        
      case "shared-db":
        let result = try await StressScenarios.sharedDatabaseStress(
          clientCount: min(clientCount, 8),  // Cap at 8 for shared DB
          messagesPerClient: messageCount
        )
        results.append(result)
        
      case "all":
        // Run all scenarios
        let r1 = try await StressScenarios.twoUserRapidMessaging(messageCount: messageCount)
        results.append(r1)
        
        let r2 = try await StressScenarios.multiClientSwarm(
          clientCount: clientCount,
          messagesPerClient: messageCount
        )
        results.append(r2)
        
        let r3 = try await StressScenarios.sharedDatabaseStress(
          clientCount: min(clientCount, 8),
          messagesPerClient: messageCount
        )
        results.append(r3)
        
      default:
        print("Unknown scenario: \(scenario)")
        print("Available: two-user, swarm, shared-db, all")
        return
      }
      
    } catch {
      print("\n❌ Fatal error: \(error)")
      return
    }
    
    // Print summary
    print("\n" + String(repeating: "=", count: 60))
    print("📊 FINAL SUMMARY")
    print(String(repeating: "=", count: 60))
    
    for result in results {
      result.printSummary()
    }
    
    let allPassed = results.allSatisfy { $0.passed }
    let totalBusy = results.reduce(0) { $0 + $1.sqliteBusyCount }
    let totalSecretReuse = results.reduce(0) { $0 + $1.secretReuseErrors }
    
    print("")
    if allPassed {
      print("🎉 ALL SCENARIOS PASSED")
    } else {
      print("💔 SOME SCENARIOS FAILED")
    }
    
    if totalBusy > 0 {
      print("\n⚠️  Total SQLITE_BUSY occurrences: \(totalBusy)")
      print("   This indicates WAL checkpoint contention - consider:")
      print("   - Using ephemeral database pools for inactive users")
      print("   - Moving serialization to Rust layer")
    }
    
    if totalSecretReuse > 0 {
      print("\n🚨 Total SecretReuseError occurrences: \(totalSecretReuse)")
      print("   This indicates MLS ratchet corruption - investigate:")
      print("   - Per-group decryption serialization")
      print("   - Cross-process coordination")
    }
    
    print("\n✅ Stress test complete.")
  }
  
  // MARK: - Argument Parsing
  
  static func parseArg(_ args: [String], flag: String, default defaultValue: Int) -> Int {
    guard let index = args.firstIndex(of: flag),
          index + 1 < args.count,
          let value = Int(args[index + 1]) else {
      return defaultValue
    }
    return value
  }
  
  static func parseStringArg(_ args: [String], flag: String, default defaultValue: String) -> String {
    guard let index = args.firstIndex(of: flag),
          index + 1 < args.count else {
      return defaultValue
    }
    return args[index + 1]
  }
}
