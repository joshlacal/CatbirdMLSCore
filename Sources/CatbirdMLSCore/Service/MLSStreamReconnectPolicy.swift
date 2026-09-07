import Foundation

/// Server throttling is a minimum wait, independent of the normal 30-second
/// exponential-backoff ceiling. Keep the result safe for Task.sleep conversion.
enum MLSStreamReconnectPolicy {
    static func delay(attempt: Int, error: Error?) -> TimeInterval {
        let backoff = min(pow(2, Double(min(max(attempt, 1) - 1, 5))), 30)
        guard let error,
              let hint = KeyPackagePublishCoordinator.rateLimitRetryAfter(for: error),
              let hint, hint.isFinite, hint > 0 else {
            return backoff
        }
        // Leave rounding headroom when converting seconds to UInt64 nanoseconds.
        let maximumSleep = (Double(UInt64.max) / 1_000_000_000).nextDown
        return max(backoff, min(hint, maximumSleep))
    }
}
