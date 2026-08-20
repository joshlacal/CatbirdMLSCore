import XCTest
import Petrel
import PetrelCatbird
@testable import CatbirdMLSCore

private struct MLSAPIClientTestError: LocalizedError {
  let errorDescription: String?
}

final class MLSAPIClientGroupInfoVerificationTests: XCTestCase {

  func testWelcomeHashQueryPreservesSmallUniqueHashList() {
    let hashes = [
      String(repeating: "a", count: 64),
      String(repeating: "b", count: 64),
      String(repeating: "a", count: 64),
    ]

    let queryHashes = MLSAPIClient.welcomeKeyPackageHashesForQuery(hashes)

    XCTAssertEqual(queryHashes, [hashes[0], hashes[1]])
  }

  func testWelcomeHashQueryOmitsOversizedLocalManifest() {
    let hashes = (0 ... MLSAPIClient.maxWelcomeKeyPackageHashesForQuery).map {
      String(format: "%064x", $0)
    }

    XCTAssertNil(MLSAPIClient.welcomeKeyPackageHashesForQuery(hashes))
  }

  func testGroupResetDetectionAcceptsTypedHTTP410() {
    let error = MLSAPIError.httpError(
      statusCode: 410,
      message: "Failed to fetch GroupInfo after 1 attempt(s)"
    )

    XCTAssertTrue(MLSAPIClient.isGroupResetResponse(error))
  }

  func testGroupResetDetectionAcceptsPetrelStatusDescription() {
    let error = MLSAPIClientTestError(
      errorDescription: "Received an error response from the server (Status Code: 410)."
    )

    XCTAssertTrue(MLSAPIClient.isGroupResetResponse(error))
  }

  func testCreateConversationFailsClosedWithProtocolUpgradeRequired() async {
    let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let client = await MLSAPIClient(client: atProtoClient)
    do {
      _ = try await client.createConversation(
        groupId: "018f3f6a7b2c4d918a5e0f123456789a",
        cipherSuite: "MLS_256_XWING_CHACHA20POLY1305_SHA256_Ed25519"
      )
      XCTFail("createConversation must fail closed with protocolUpgradeRequired")
    } catch let error as MLSAPIError {
      guard case let .protocolUpgradeRequired(op) = error else {
        XCTFail("Expected .protocolUpgradeRequired, got \(error)")
        return
      }
      XCTAssertEqual(op, "createConversation")
      XCTAssertEqual(error.userActionCategory, .rebind)
      XCTAssertTrue(error.localizedDescription.contains("protocol upgrade"))
    } catch {
      XCTFail("Expected MLSAPIError, got \(error)")
    }
  }

  func testReportRecoveryFailureFailsClosedWithProtocolUpgradeRequired() async {
    let atProtoClient = await ATProtoClient(baseURL: URL(string: "https://example.com")!)
    let client = await MLSAPIClient(client: atProtoClient)
    do {
      _ = try await client.reportRecoveryFailure(convoId: "convo-123")
      XCTFail("reportRecoveryFailure must fail closed with protocolUpgradeRequired")
    } catch let error as MLSAPIError {
      guard case let .protocolUpgradeRequired(op) = error else {
        XCTFail("Expected .protocolUpgradeRequired, got \(error)")
        return
      }
      XCTAssertEqual(op, "reportRecoveryFailure")
      XCTAssertEqual(error.userActionCategory, .rebind)
      XCTAssertTrue(error.localizedDescription.contains("protocol upgrade"))
    } catch {
      XCTFail("Expected MLSAPIError, got \(error)")
    }
  }
}

final class MLSClientHTTPStatusExtractionTests: XCTestCase {

  private struct PetrelStyleStatusError: LocalizedError {
    let errorDescription: String? =
      "Received an error response from the server (Status Code: 409)."
  }

  func testExtractsStatusCodeFromPetrelStyleDescription() {
    XCTAssertEqual(MLSClient.httpStatusCode(from: PetrelStyleStatusError()), 409)
  }

  func testExtractsStatusCodeFromMLSAPIError() {
    XCTAssertEqual(
      MLSClient.httpStatusCode(
        from: MLSAPIError.httpError(statusCode: 429, message: "rate limited")),
      429
    )
  }
}
