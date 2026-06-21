@testable import CatbirdMLSCore
import XCTest

final class MLSAPIClientWelcomeQueryTests: XCTestCase {
    func testWelcomeKeyPackageHashesForQueryOmitsHashesWhenSetWouldCreateLargeGetRequest() {
        let hashes = (0..<128).map { index in
            String(format: "%064x", index)
        }

        let queryHashes = MLSAPIClient.welcomeKeyPackageHashesForQuery(hashes)

        XCTAssertNil(queryHashes)
    }

    func testWelcomeKeyPackageHashesForQueryKeepsBoundedUsableSet() {
        let hashes = (0..<MLSAPIClient.maxWelcomeKeyPackageHashesForQuery).map { index in
            String(format: "%064x", index)
        }

        let queryHashes = MLSAPIClient.welcomeKeyPackageHashesForQuery(hashes)

        XCTAssertEqual(queryHashes, hashes)
    }

    func testWelcomeKeyPackageHashesForQueryNormalizesAndDropsInvalidDuplicates() {
        let valid = String(repeating: "a", count: 64)
        let duplicate = String(repeating: "A", count: 64)

        let queryHashes = MLSAPIClient.welcomeKeyPackageHashesForQuery([
            " \(valid)\n",
            duplicate,
            "not-a-hash",
            String(repeating: "g", count: 64),
            String(repeating: "b", count: 63),
        ])

        XCTAssertEqual(queryHashes, [valid])
    }

    func testWelcomeKeyPackageHashesForQueryReturnsNilWhenNoUsableHashesExist() {
        XCTAssertNil(MLSAPIClient.welcomeKeyPackageHashesForQuery(["", "abc", String(repeating: "z", count: 64)]))
    }
}
