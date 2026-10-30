import Foundation
import Synchronization
import XCTest

@testable import CatbirdMLSCore

final class MLSCoordinationProviderTests: XCTestCase {
  override func setUpWithError() throws {
    try super.setUpWithError()
    let directory = FileManager.default.temporaryDirectory
      .appendingPathComponent("MLSCoordinationProviderTests-\(UUID().uuidString)")
    MLSStoragePaths.setBaseDirectoryOverride(directory)
    MLSCoordinationStore.shared.setActiveUserProvider(nil)
    try MLSCoordinationStore.shared.saveStrict(.init(
      coordinationGeneration: 7, activeUserDID: "did:plc:disk",
      phase: .active, updatedAt: Date()
    ))
  }

  override func tearDownWithError() throws {
    MLSCoordinationStore.shared.setActiveUserProvider(nil)
    MLSStoragePaths.setBaseDirectoryOverride(nil)
    try super.tearDownWithError()
  }

  func testRepeatedReadsOnSmallStackPreserveLiveAccountAndGeneration() {
    MLSCoordinationStore.shared.setActiveUserProvider { "did:plc:live" }
    let completed = DispatchSemaphore(value: 0)
    let valid = Mutex(true)
    let worker = Thread {
      defer { completed.signal() }
      // The original optional-closure mutex exhausted a 512 KB stack after
      // roughly 5,000 reads by accumulating reabstraction wrappers.
      for _ in 0..<12_000 {
        let state = MLSCoordinationStore.shared.getState()
        if state.activeUserDID != "did:plc:live" || state.coordinationGeneration != 7
          || MLSCoordinationStore.shared.currentGeneration != 7 {
          valid.withLock { $0 = false }
          return
        }
      }
    }
    worker.stackSize = 512 * 1024
    worker.start()
    XCTAssertEqual(completed.wait(timeout: .now() + 30), .success)
    XCTAssertTrue(valid.withLock { $0 })
  }

  func testProviderReplacementAndNilPreserveDiskFallback() {
    let liveDID = Mutex<String?>("did:plc:first")
    MLSCoordinationStore.shared.setActiveUserProvider { liveDID.withLock { $0 } }
    XCTAssertEqual(MLSCoordinationStore.shared.getState().activeUserDID, "did:plc:first")
    liveDID.withLock { $0 = "did:plc:second" }
    XCTAssertEqual(MLSCoordinationStore.shared.getState().activeUserDID, "did:plc:second")
    liveDID.withLock { $0 = nil }
    XCTAssertEqual(MLSCoordinationStore.shared.getState().activeUserDID, "did:plc:disk")

    MLSCoordinationStore.shared.setActiveUserProvider { "did:plc:replacement" }
    XCTAssertEqual(MLSCoordinationStore.shared.getState().activeUserDID, "did:plc:replacement")
    MLSCoordinationStore.shared.setActiveUserProvider(nil)
    XCTAssertEqual(MLSCoordinationStore.shared.getState().activeUserDID, "did:plc:disk")
    XCTAssertEqual(MLSCoordinationStore.shared.currentGeneration, 7)
  }

  func testProviderCanReplaceItselfOutsideLock() {
    MLSCoordinationStore.shared.setActiveUserProvider {
      MLSCoordinationStore.shared.setActiveUserProvider { "did:plc:replacement" }
      return "did:plc:original"
    }
    XCTAssertEqual(MLSCoordinationStore.shared.getState().activeUserDID, "did:plc:original")
    XCTAssertEqual(MLSCoordinationStore.shared.getState().activeUserDID, "did:plc:replacement")
  }

  func testUnreadableStateStillUsesLiveProviderAndFailsClosed() throws {
    let url = try MLSStoragePaths.coordinationDirectory()
      .appendingPathComponent("coordination_state.\(MLSStoragePaths.cleanSuffix).json")
    try Data("invalid JSON".utf8).write(to: url)
    MLSCoordinationStore.shared.setActiveUserProvider { "did:plc:live" }
    let state = MLSCoordinationStore.shared.getState()
    XCTAssertEqual(state.activeUserDID, "did:plc:live")
    XCTAssertEqual(state.coordinationGeneration, -1)
    XCTAssertEqual(state.phase, .closed)
    MLSCoordinationStore.shared.setActiveUserProvider(nil)
    XCTAssertNil(MLSCoordinationStore.shared.getState().activeUserDID)
  }
}
