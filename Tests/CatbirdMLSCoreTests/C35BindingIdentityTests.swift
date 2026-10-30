//
//  C35BindingIdentityTests.swift
//  CatbirdMLSCoreTests
//
//  C35 Clause 1 — Real UniFFI binding & native contract tests.
//  Proves that CatbirdMLSCore executes through the real generated UniFFI
//  bindings, verifies native build info pragma contract, and validates
//  consumer-visible native rejection across the FFI boundary.
//

import Foundation
#if canImport(XCTest)
import XCTest

@preconcurrency import CatbirdMLS
@testable import CatbirdMLSCore

final class C35BindingIdentityTests: XCTestCase {

  /// Exercise real shipping UniFFI generated export functions through the linked binding,
  /// verifying the native SQLCipher pragma parameters and required storage capabilities version.
  func testUniFFINativeBuildInfoAndCapabilitiesContract() throws {
    let buildInfo = getFfiBuildInfo()
    XCTAssertTrue(
      buildInfo.contains("SQLCipher: plaintext_header=32, kdf_iter=256000, page_size=4096"),
      "getFfiBuildInfo must verify native SQLCipher pragma contract, got: \(buildInfo)"
    )

    let capVersion = securityStorageCapabilitiesVersion()
    XCTAssertEqual(
      capVersion,
      3,
      "securityStorageCapabilitiesVersion returned from native library must equal 3"
    )
  }

  /// Consumer-visible native validation rejection test: exercises real FFI boundary validation.
  func testNativeContractValidationRejection() throws {
    // 1. Valid conversation identifier passes native parsing without error
    let validConversationId = "018f3f6a-7b2c-4d91-8a5e-0f123456789a"
    XCTAssertNoThrow(
      try chatV2ValidateIdentifier(kind: .conversationId, value: validConversationId)
    )

    // 2. Consumer-visible rejection: invalid identifier must throw native ChatV2ValidationError
    let invalidConversationId = "not-a-valid-uuid-format"
    XCTAssertThrowsError(
      try chatV2ValidateIdentifier(kind: .conversationId, value: invalidConversationId),
      "chatV2ValidateIdentifier must fail closed when passed an invalid conversation ID"
    ) { error in
      XCTAssertTrue(
        error is ChatV2ValidationError,
        "Thrown error must be native ChatV2ValidationError, got: \(error)"
      )
    }
  }
}
#endif
