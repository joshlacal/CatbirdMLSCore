import Testing
@testable import CatbirdMLSCore

struct MLSDeviceRecordServiceTests {
  @Test func keyPackageMatchesPublishedDeviceKey() {
    let decision = MLSDeviceRecordService.verifyKeyPackage(
      signatureKeyB64: "AAAA",
      againstDeviceKeys: Set(["AAAA", "BBBB"])
    )
    #expect(decision.allowed)
    #expect(decision.failureReason == nil)
  }

  @Test func keyPackageRejectsUnknownKey() {
    let decision = MLSDeviceRecordService.verifyKeyPackage(
      signatureKeyB64: "CCCC",
      againstDeviceKeys: Set(["AAAA", "BBBB"])
    )
    #expect(!decision.allowed)
    #expect(decision.failureReason != nil)
  }

  @Test func emptyDeviceKeysRejectsAll() {
    let decision = MLSDeviceRecordService.verifyKeyPackage(
      signatureKeyB64: "AAAA",
      againstDeviceKeys: Set<String>()
    )
    #expect(!decision.allowed)
  }

  @Test func nilDeviceKeysAllowsWithWarning() {
    // nil = no records published (TOFU)
    let decision = MLSDeviceRecordService.verifyKeyPackage(
      signatureKeyB64: "AAAA",
      againstDeviceKeys: nil
    )
    #expect(decision.allowed)
    #expect(decision.warning != nil)
  }
}
