import XCTest
@testable import CatbirdMLSCore

final class MLSStorageSuspensionTests: XCTestCase {
  private let manager = MLSGRDBManager.shared

  func testSuspendedReadFailsFast() async throws {
    let userDID = "did:plc:storage-suspension-read-\(UUID().uuidString)"
    await manager.suspendDatabaseAccessForTesting(
      for: userDID,
      duration: 10,
      reason: "simulated corruption",
      operation: "unit-test"
    )

    do {
      _ = try await manager.read(for: userDID) { _ in
        XCTFail("Suspended reads should fail before opening the database")
        return 1
      }
      XCTFail("Expected storageUnavailable for suspended read")
    } catch let error as MLSSQLCipherError {
      guard case .storageUnavailable(let reason) = error else {
        XCTFail("Unexpected MLS SQLCipher error: \(error)")
        return
      }
      XCTAssertTrue(reason.contains("simulated corruption"))
    }

    await manager.clearDatabaseAccessSuspensionForTesting(for: userDID)
  }

  func testSuspendedDatabasePoolFailsFast() async throws {
    let userDID = "did:plc:storage-suspension-pool-\(UUID().uuidString)"
    await manager.suspendDatabaseAccessForTesting(
      for: userDID,
      duration: 10,
      reason: "simulated oom",
      operation: "unit-test"
    )

    do {
      _ = try await manager.getDatabasePool(for: userDID)
      XCTFail("Expected getDatabasePool to fail while suspended")
    } catch let error as MLSSQLCipherError {
      guard case .storageUnavailable(let reason) = error else {
        XCTFail("Unexpected MLS SQLCipher error: \(error)")
        return
      }
      XCTAssertTrue(reason.contains("simulated oom"))
    }

    await manager.clearDatabaseAccessSuspensionForTesting(for: userDID)
  }

  func testSuspensionExpiresAndClearsDescription() async throws {
    let userDID = "did:plc:storage-suspension-expiry-\(UUID().uuidString)"
    await manager.suspendDatabaseAccessForTesting(
      for: userDID,
      duration: 0.05,
      reason: "short lived",
      operation: "unit-test"
    )

    let isInitiallySuspended = await manager.isDatabaseAccessSuspended(for: userDID)
    XCTAssertTrue(isInitiallySuspended)
    let descriptionBefore = await manager.databaseAccessSuspensionDescription(for: userDID)
    XCTAssertNotNil(descriptionBefore)
    XCTAssertTrue(descriptionBefore?.contains("short lived") == true)

    try await Task.sleep(nanoseconds: 150_000_000)

    let isSuspendedAfterExpiry = await manager.isDatabaseAccessSuspended(for: userDID)
    XCTAssertFalse(isSuspendedAfterExpiry)
    let descriptionAfter = await manager.databaseAccessSuspensionDescription(for: userDID)
    XCTAssertNil(descriptionAfter)
  }

  func testForegroundResumePreparationBlocksDatabaseOpenUntilCleared() async throws {
    let userDID = "did:plc:foreground-resume-\(UUID().uuidString)"
    await manager.beginForegroundResumePreparationForTesting(for: userDID)
    let manager = self.manager

    let start = Date()
    let openTask = Task { try await manager.getDatabasePool(for: userDID) }

    try await Task.sleep(nanoseconds: 150_000_000)
    await manager.endForegroundResumePreparationForTesting(for: userDID)

    _ = try await openTask.value
    let elapsed = Date().timeIntervalSince(start)
    XCTAssertGreaterThanOrEqual(elapsed, 0.10)

    try await manager.deleteDatabase(for: userDID)
  }

  func testReadReusesOpenPoolWhenActiveTrackingWasCleared() async throws {
    let userDID = "did:plc:smart-route-read-\(UUID().uuidString)"
    await manager.setActiveUser(userDID)
    _ = try await manager.getDatabasePool(for: userDID)
    await manager.setActiveUser(nil)

    let isInitiallyActive = await manager.isActiveUser(userDID)
    XCTAssertFalse(isInitiallyActive)

    let value = try await manager.read(for: userDID) { db in
      try db.execute(sql: "SELECT 1;")
      return 1
    }

    let isActiveAfterRead = await manager.isActiveUser(userDID)
    let openCountAfterRead = await manager.openDatabaseCount
    XCTAssertEqual(value, 1)
    XCTAssertTrue(isActiveAfterRead)
    XCTAssertEqual(openCountAfterRead, 1)

    try await manager.deleteDatabase(for: userDID)
  }

  func testWriteReusesOpenPoolWhenActiveTrackingWasCleared() async throws {
    let userDID = "did:plc:smart-route-write-\(UUID().uuidString)"
    await manager.setActiveUser(userDID)
    _ = try await manager.getDatabasePool(for: userDID)
    await manager.setActiveUser(nil)

    let isInitiallyActive = await manager.isActiveUser(userDID)
    XCTAssertFalse(isInitiallyActive)

    let value = try await manager.write(for: userDID) { db in
      try db.execute(sql: "CREATE TABLE IF NOT EXISTS smart_route_probe (id INTEGER PRIMARY KEY);")
      return 1
    }

    let isActiveAfterWrite = await manager.isActiveUser(userDID)
    let openCountAfterWrite = await manager.openDatabaseCount
    XCTAssertEqual(value, 1)
    XCTAssertTrue(isActiveAfterWrite)
    XCTAssertEqual(openCountAfterWrite, 1)

    try await manager.deleteDatabase(for: userDID)
  }
}
