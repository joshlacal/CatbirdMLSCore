import Testing
@testable import CatbirdMLSCore

@Suite("MLS keychain fixture service mapping")
struct MLSKeychainFixtureServiceTests {
  @Test("Fixture mapping is idempotent so strict store paths re-read the item they wrote")
  func mappingIsIdempotent() {
    let once = MLSKeychainManager.serviceName("blue.catbird.mls", fixtureLaunch: true)
    #expect(once == "blue.catbird.fixture.blue.catbird.mls")
    #expect(MLSKeychainManager.serviceName(once, fixtureLaunch: true) == once)
  }

  @Test("Non-fixture launches keep the production service")
  func productionUnchanged() {
    #expect(MLSKeychainManager.serviceName("blue.catbird.mls", fixtureLaunch: false) == "blue.catbird.mls")
  }
}
