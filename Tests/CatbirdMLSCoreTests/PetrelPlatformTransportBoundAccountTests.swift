import Foundation
import Petrel
import Testing
@testable import CatbirdMLSCore

/// A transport opened for one account must not send that account's requests under another
/// account's session after the app switches accounts.
@Suite("Petrel Platform Transport Bound Account Tests")
struct PetrelPlatformTransportBoundAccountTests {
  @Test("The bound account may send")
  func boundAccountPasses() throws {
    try PetrelPlatformTransport.requireBoundAccount(active: "did:plc:alice", bound: "did:plc:alice")
  }

  @Test("DID comparison ignores case")
  func comparisonIgnoresCase() throws {
    try PetrelPlatformTransport.requireBoundAccount(active: "did:web:Example.com", bound: "did:web:example.com")
  }

  @Test("Another active account is refused as unauthenticated")
  func otherAccountIsRefused() {
    #expect(throws: AuthError.self) {
      try PetrelPlatformTransport.requireBoundAccount(active: "did:plc:bob", bound: "did:plc:alice")
    }
    do {
      try PetrelPlatformTransport.requireBoundAccount(active: "did:plc:bob", bound: "did:plc:alice")
    } catch {
      // The engine sees a retryable 401, never a response from the wrong account.
      #expect(PetrelPlatformTransport.httpFailure(error).status == 401)
    }
  }

  @Test("An unbound transport checks nothing")
  func unboundTransportPasses() throws {
    try PetrelPlatformTransport.requireBoundAccount(active: "did:plc:bob", bound: nil)
  }
}
