import CatbirdMLS
import XCTest
@testable import CatbirdMLSCore

final class MLSConversationRateLimitTests: XCTestCase {
  private func mapped(_ error: Error) -> MLSConversationLifecycleError? {
    MLSConversationLifecycleError.presenting(error, operation: .open) as? MLSConversationLifecycleError
  }

  func testMapsObservedBridgeRateLimit() {
    let result = mapped(OrchestratorBridgeError.ServerError(status: 429, body: "Rate limited. Retry after 240 seconds."))
    guard case .rateLimited(let hint) = result else { return XCTFail("Expected rate limit") }
    XCTAssertEqual(hint, 240)
    XCTAssertEqual(result?.retryAfter, 240)
    XCTAssertTrue(result?.errorDescription?.contains("240 seconds") == true)
  }

  func testMapsCanonicalJSONAndTypedErrors() {
    XCTAssertEqual(mapped(OrchestratorBridgeError.ServerError(status: 429, body: #"{"error":"RateLimitExceeded","retryAfter":164}"#))?.retryAfter, 164)
    XCTAssertEqual(mapped(MLSAPIError.rateLimited(retryAfter: 42))?.retryAfter, 42)
    XCTAssertEqual(mapped(MLSError.rateLimited(retryAfterSeconds: 60))?.retryAfter, 60)
  }

  func testRejectsInvalidHintsWithoutExposingBody() {
    for body in ["private server detail", "Rate limited. Retry after -4 seconds.", "Rate limited. Retry after inf seconds.", "prefix Rate limited. Retry after 240 seconds.", #"{"retryAfter":true}"#, #"{"retryAfter":1000000000000}"#] {
      let result = mapped(OrchestratorBridgeError.ServerError(status: 429, body: body))
      guard case .rateLimited(let hint) = result else { return XCTFail("Expected rate limit") }
      XCTAssertNil(hint)
      XCTAssertFalse(result?.errorDescription?.contains(body) == true)
    }
    for hint in [-1.0, 0, .nan, .infinity] {
      XCTAssertNil(mapped(MLSAPIError.rateLimited(retryAfter: hint))?.retryAfter)
    }
  }

  func testHintHorizonMatchesInventoryBackoff() {
    XCTAssertEqual(mapped(MLSAPIError.rateLimited(retryAfter: 901))?.retryAfter, 901)
    XCTAssertNil(mapped(MLSAPIError.rateLimited(retryAfter: 902))?.retryAfter)
    XCTAssertNil(mapped(MLSAPIError.rateLimited(retryAfter: 0.5))?.retryAfter)
    XCTAssertEqual(mapped(MLSAPIError.rateLimited(retryAfter: 1.5))?.retryAfter, 1.5)
    XCTAssertTrue(mapped(MLSAPIError.rateLimited(retryAfter: 1.5))?.errorDescription?.contains("2 seconds") == true)
  }

  func testDoesNotMapNon429FromBodyText() {
    let result = mapped(OrchestratorBridgeError.ServerError(status: 400, body: "Rate limited. Retry after 240 seconds."))
    if case .rateLimited = result { XCTFail("Only 429 identifies throttling") }
  }
}
