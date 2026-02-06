
import XCTest
@testable import CatbirdMLSCore
import OSLog

final class MLSDatabaseGateTests: XCTestCase {
  
  override func setUp() async throws {
    // No specific setup needed as we use unique DIDs
  }
  
  func testGateLifecycle() async throws {
    let testDID = "did:plc:lifecycle_\(UUID().uuidString)"
    let gate = MLSDatabaseGate.shared
    
    // 1. Initially closed
    let initialState = await gate.gateState(for: testDID)
    XCTAssertEqual(initialState, .closed)
    
    // 2. Acquire should fail
    do {
      _ = try await gate.acquireConnection(for: testDID)
      XCTFail("Should not acquire connection when closed")
    } catch {
        // Expected
    }
    
    // 3. Open gate
    await gate.openGate(for: testDID)
    let openState = await gate.gateState(for: testDID)
    XCTAssertEqual(openState, .open)
    
    // 4. Acquire connection
    let token = try await gate.acquireConnection(for: testDID)
    XCTAssertEqual(token.userDID, testDID)
    
    let count = await gate.connectionCount(for: testDID)
    XCTAssertEqual(count, 1)
    
    // 5. Release connection
    await gate.releaseConnection(token)
    let countAfterRelease = await gate.connectionCount(for: testDID)
    XCTAssertEqual(countAfterRelease, 0)
  }
  
  func testDrainPreventsShutdown() async throws {
    let testDID = "did:plc:drain_\(UUID().uuidString)"
    let gate = MLSDatabaseGate.shared
    await gate.openGate(for: testDID)
    
    // 1. Acquire a connection (simulating a long running task)
    let token = try await gate.acquireConnection(for: testDID)
    
    // 2. Start shutdown in background
    let shutdownExpectation = expectation(description: "Shutdown complete")
    _ = Task {
      try await gate.closeGateAndDrain(for: testDID, timeout: .seconds(2))
      shutdownExpectation.fulfill()
    }
    
    // 3. Verify state is .closing immediately
    try await Task.sleep(nanoseconds: 50_000_000) // 50ms
    let closingState = await gate.gateState(for: testDID)
    XCTAssertEqual(closingState, .closing)
    
    // 4. Verify new connections are rejected while draining
    do {
      _ = try await gate.acquireConnection(for: testDID)
      XCTFail("Should reject new connections while closing")
    } catch MLSGateError.gateClosing {
      // Expected
    } catch {
      XCTFail("Unexpected error: \(error)")
    }
    
    // 5. Release the connection to allow drain to finish
    await gate.releaseConnection(token)
    
    // 6. Wait for shutdown to complete
    await fulfillment(of: [shutdownExpectation], timeout: 1.0)
    
    let finalState = await gate.gateState(for: testDID)
    XCTAssertEqual(finalState, .closed)
  }
  
  func testDrainTimeout() async throws {
    let testDID = "did:plc:timeout_\(UUID().uuidString)"
    let gate = MLSDatabaseGate.shared
    await gate.openGate(for: testDID)
    
    // 1. Acquire connection and HOLD it
    _ = try await gate.acquireConnection(for: testDID)
    
    // 2. Attempt close with short timeout
    do {
      try await gate.closeGateAndDrain(for: testDID, timeout: .milliseconds(200))
      XCTFail("Should have timed out")
    } catch MLSGateError.drainTimeout {
      // Expected
    } catch {
      XCTFail("Unexpected error: \(error)")
    }
  }
  
  func testWithConnectionHelper() async throws {
    let testDID = "did:plc:helper_\(UUID().uuidString)"
    let gate = MLSDatabaseGate.shared
    await gate.openGate(for: testDID)
    
    let result = try await gate.withConnection(for: testDID) {
      // Verify usage inside block
      let count = await gate.connectionCount(for: testDID)
      XCTAssertEqual(count, 1)
      return "success"
    }
    
    XCTAssertEqual(result, "success")
    let finalCount = await gate.connectionCount(for: testDID)
    XCTAssertEqual(finalCount, 0)
  }
}
