
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
    XCTAssertEqual(token.userDID, testDID.lowercased())
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

  func testExactTokenAtomicReleaseAndIdempotency() async throws {
    let testDID = "did:plc:exact_token_\(UUID().uuidString.lowercased())"
    let gate = MLSDatabaseGate.shared
    await gate.openGate(for: testDID)

    let token1 = try await gate.acquireConnection(for: testDID)
    let token2 = try await gate.acquireConnection(for: testDID)

    var count = await gate.connectionCount(for: testDID)
    XCTAssertEqual(count, 2)
    var valid1 = await gate.isTokenValid(token1)
    var valid2 = await gate.isTokenValid(token2)
    XCTAssertTrue(valid1)
    XCTAssertTrue(valid2)

    // Releasing token1 decrements count to 1
    await gate.releaseConnection(token1)
    count = await gate.connectionCount(for: testDID)
    XCTAssertEqual(count, 1)
    valid1 = await gate.isTokenValid(token1)
    valid2 = await gate.isTokenValid(token2)
    XCTAssertFalse(valid1)
    XCTAssertTrue(valid2)

    // Duplicate release of token1 is a no-op (idempotent, does not decrement count)
    await gate.releaseConnection(token1)
    count = await gate.connectionCount(for: testDID)
    XCTAssertEqual(count, 1)
    valid2 = await gate.isTokenValid(token2)
    XCTAssertTrue(valid2)

    // Releasing token2 decrements count to 0
    await gate.releaseConnection(token2)
    count = await gate.connectionCount(for: testDID)
    XCTAssertEqual(count, 0)
    valid2 = await gate.isTokenValid(token2)
    XCTAssertFalse(valid2)

    // Stale/fake token release does not decrement count below 0
    let fakeToken = MLSConnectionToken(userDID: testDID, generation: 999)
    await gate.releaseConnection(fakeToken)
    count = await gate.connectionCount(for: testDID)
    XCTAssertEqual(count, 0)
  }

  func testCaseVariantDIDSharesHandle() async throws {
    let lower = "did:plc:case_variant_\(UUID().uuidString.lowercased())"
    let upper = lower.uppercased()
    let gate = MLSDatabaseGate.shared

    await gate.openGate(for: upper)
    let isLowerOpen = await gate.isOpen(for: lower)
    let isUpperOpen = await gate.isOpen(for: upper)
    XCTAssertTrue(isLowerOpen)
    XCTAssertTrue(isUpperOpen)

    let token = try await gate.acquireConnection(for: upper)
    var lowerCount = await gate.connectionCount(for: lower)
    var upperCount = await gate.connectionCount(for: upper)
    XCTAssertEqual(lowerCount, 1)
    XCTAssertEqual(upperCount, 1)

    await gate.releaseConnection(token)
    lowerCount = await gate.connectionCount(for: lower)
    upperCount = await gate.connectionCount(for: upper)
    XCTAssertEqual(lowerCount, 0)
    XCTAssertEqual(upperCount, 0)
  }
}
