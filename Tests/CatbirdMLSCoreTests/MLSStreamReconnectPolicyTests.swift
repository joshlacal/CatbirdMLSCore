import XCTest
@testable import CatbirdMLSCore

final class MLSStreamReconnectPolicyTests: XCTestCase {
    func testHonorsServerRetryAfterBeyondLocalBackoffCap() {
        XCTAssertEqual(MLSStreamReconnectPolicy.delay(attempt: 11, error: MLSAPIError.rateLimited(retryAfter: 164)), 164)
    }

    func testUsesLargerOfBackoffAndServerHint() {
        XCTAssertEqual(MLSStreamReconnectPolicy.delay(attempt: 6, error: MLSAPIError.rateLimited(retryAfter: 2)), 30)
        XCTAssertEqual(MLSStreamReconnectPolicy.delay(attempt: 2, error: MLSError.rateLimited(retryAfterSeconds: 60)), 60)
    }

    func testInvalidAndAbsentHintsUseOrdinaryBackoff() {
        for hint: TimeInterval? in [nil, -1, 0, .nan, .infinity, -.infinity] {
            XCTAssertEqual(MLSStreamReconnectPolicy.delay(attempt: 11, error: MLSAPIError.rateLimited(retryAfter: hint)), 30)
        }
        XCTAssertEqual(MLSStreamReconnectPolicy.delay(attempt: 1, error: nil), 1)
        XCTAssertEqual(MLSStreamReconnectPolicy.delay(attempt: Int.max, error: nil), 30)
    }

    func testHugeFiniteHintCanSafelyConvertToNanoseconds() {
        let delay = MLSStreamReconnectPolicy.delay(attempt: 1, error: MLSAPIError.rateLimited(retryAfter: .greatestFiniteMagnitude))
        XCTAssertTrue(delay.isFinite)
        XCTAssertGreaterThan(delay, 164)
        XCTAssertLessThan(delay * 1_000_000_000, Double(UInt64.max))
        XCTAssertGreaterThan(UInt64(delay * 1_000_000_000), 0)
    }
}
