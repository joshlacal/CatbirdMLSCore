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
}
