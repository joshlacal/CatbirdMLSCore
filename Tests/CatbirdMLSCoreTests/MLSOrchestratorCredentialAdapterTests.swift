import Foundation
import XCTest

@testable import CatbirdMLSCore

final class MLSOrchestratorCredentialAdapterTests: XCTestCase {
  func testAuthorizedDeviceKeysReturnsNilWhenNoResolverIsConfigured() throws {
    let adapter = MLSOrchestratorCredentialAdapter()

    XCTAssertNil(try adapter.getAuthorizedDeviceKeys(userDid: "did:plc:alice"))
  }

  func testAuthorizedDeviceKeysUsesSynchronousResolver() throws {
    let authorizedKey = Data([0x01, 0x02, 0x03])
    let adapter = MLSOrchestratorCredentialAdapter { userDid in
      userDid == "did:plc:alice" ? [authorizedKey] : []
    }

    XCTAssertEqual(
      try adapter.getAuthorizedDeviceKeys(userDid: "did:plc:alice"),
      [authorizedKey]
    )
    XCTAssertEqual(
      try adapter.getAuthorizedDeviceKeys(userDid: "did:plc:bob"),
      []
    )
  }

  // MARK: - Device-record-backed resolver (production wiring)

  /// Mirrors how `MLSConversationManager.buildOrchestratorRuntime` wires the
  /// record service's store into the runtime.
  private func makeStoreBackedAdapter(
    _ store: MLSAuthorizedDeviceKeyStore
  ) -> MLSOrchestratorCredentialAdapter {
    MLSOrchestratorCredentialAdapter { did in store.keys(for: did) }
  }

  func testStoreBackedResolverReturnsResolvedDeviceKeys() throws {
    let store = MLSAuthorizedDeviceKeyStore()
    let alice = Data([0xA1, 0xA2, 0xA3])
    let aliceSecondDevice = Data([0xB1, 0xB2, 0xB3])
    store.store(keys: [alice, aliceSecondDevice], for: "did:plc:alice")

    XCTAssertEqual(
      try makeStoreBackedAdapter(store).getAuthorizedDeviceKeys(userDid: "did:plc:alice"),
      [alice, aliceSecondDevice]
    )
  }

  func testStoreBackedResolverThrowsForUnresolvedDID() throws {
    let store = MLSAuthorizedDeviceKeyStore()
    store.store(keys: [Data([0xA1])], for: "did:plc:alice")

    XCTAssertThrowsError(
      try makeStoreBackedAdapter(store).getAuthorizedDeviceKeys(userDid: "did:plc:bob")
    ) { error in
      guard case MLSAuthorizedDeviceKeyResolutionError.unresolved(let did) = error else {
        return XCTFail("Expected .unresolved, got \(error)")
      }
      XCTAssertEqual(did, "did:plc:bob")
    }
  }

  /// A repo that publishes no device records is a definitive answer ("zero
  /// authorized devices"), not an unresolved lookup.
  func testStoreBackedResolverReturnsEmptyForDIDWithNoDeviceRecords() throws {
    let store = MLSAuthorizedDeviceKeyStore()
    store.store(keys: [], for: "did:plc:carol")

    XCTAssertEqual(
      try makeStoreBackedAdapter(store).getAuthorizedDeviceKeys(userDid: "did:plc:carol"),
      []
    )
  }

  /// Rust looks up by credential root DID; a device fragment must hit the same
  /// entry, and lookups must not be case-sensitive.
  func testStoreNormalizesFragmentAndCase() throws {
    let store = MLSAuthorizedDeviceKeyStore()
    let key = Data([0xC1])
    store.store(keys: [key], for: "did:plc:Alice#device-abc")

    XCTAssertEqual(store.keys(for: "did:plc:alice"), [key])
    XCTAssertEqual(store.keys(for: "did:plc:alice#device-xyz"), [key])
  }

  func testStoreInvalidationRestoresUnresolvedState() throws {
    let store = MLSAuthorizedDeviceKeyStore()
    store.store(keys: [Data([0xD1])], for: "did:plc:dave")
    XCTAssertNotNil(store.keys(for: "did:plc:dave"))

    store.invalidate("did:plc:dave")

    XCTAssertNil(store.keys(for: "did:plc:dave"))
    XCTAssertThrowsError(
      try makeStoreBackedAdapter(store).getAuthorizedDeviceKeys(userDid: "did:plc:dave")
    )
  }
}
