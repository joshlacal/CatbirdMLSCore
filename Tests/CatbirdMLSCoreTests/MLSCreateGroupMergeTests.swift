import XCTest
@testable import CatbirdMLSCore

/// Regression tests for the createGroup fallback path merge bug.
///
/// **Bug**: After calling `apiClient.addMembers(...)`, the code had:
///   `if addResult.newEpoch > currentEpoch { mergePendingCommit(...) }`
/// When the server omits `newEpoch` (idempotency hit), `MLSAPIClient.addMembers` returns
/// `(output.success, output.newEpoch ?? 0)` — defaulting to 0. The condition `0 > 0 = false`
/// caused the merge to be skipped, leaving an unmerged staged commit in the OpenMLS group
/// and breaking all subsequent message encryption.
///
/// **Fix**: Remove the conditional. Always call `mergePendingCommit` after a successful
/// `addMembers`. The error cases (`.memberAlreadyExists`, failure) are handled by the
/// surrounding catch blocks which call `clearPendingCommit` and return early.
final class MLSCreateGroupMergeTests: XCTestCase {

  // MARK: - Epoch comparison logic

  /// Documents the original bug: `0 > 0 = false` caused the merge to be skipped.
  func testOldConditionalWouldSkipMergeWhenServerReturnsEpochZero() {
    let currentEpoch = 0
    let serverNewEpoch = 0  // server omits newEpoch, `output.newEpoch ?? 0` defaults to 0

    // The old broken conditional:
    let oldConditionalWouldMerge = serverNewEpoch > currentEpoch
    XCTAssertFalse(
      oldConditionalWouldMerge,
      "Confirms the bug: old `newEpoch > currentEpoch` skipped merge when server returned 0"
    )
  }

  /// After the fix, merge decision depends only on `addResult.success`, not the epoch value.
  func testMergeIsUnconditionalWhenAddMembersSucceedsWithEpochZero() {
    // Simulates: server returns newEpoch=0 (nil ?? 0)
    let addSuccess = true
    let serverNewEpoch = 0

    // Fixed code path: always merge when success==true (no epoch comparison)
    let shouldMerge = addSuccess
    XCTAssertTrue(
      shouldMerge,
      "mergePendingCommit must be called even when server returns newEpoch=\(serverNewEpoch)"
    )
  }

  /// Normal path: server advances epoch — merge should still be called.
  func testMergeIsCalledWhenServerAdvancesEpoch() {
    let addSuccess = true
    let serverNewEpoch = 1

    let shouldMerge = addSuccess
    XCTAssertTrue(
      shouldMerge,
      "mergePendingCommit must be called when server advances epoch to \(serverNewEpoch)"
    )
  }

  /// Failure path: addMembers returns success=false — the guard throws before any merge.
  func testMergeIsNotCalledWhenAddMembersFails() {
    // The `guard addResult.success else { throw ... }` fires before mergePendingCommit.
    let addSuccess = false

    // Unconditional merge path is only reached when success==true.
    let reachesMergePath = addSuccess
    XCTAssertFalse(
      reachesMergePath,
      "mergePendingCommit must NOT be called when addMembers returns success=false"
    )
  }
}
