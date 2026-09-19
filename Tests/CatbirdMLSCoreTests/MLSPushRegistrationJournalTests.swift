@testable import CatbirdMLSCore
import Foundation
import GRDB
import Testing

struct MLSPushRegistrationJournalTests {
  @Test("Unknown attempts retain exact bytes and a changed binding advances generation")
  func unknownThenChanged() throws {
    let queue = try DatabaseQueue()
    let first = entry(generation: 1, tokenByte: 1, requestByte: 11)
    let second = entry(generation: 2, tokenByte: 2, requestByte: 22)
    try queue.write { db in
      try MLSPushRegistrationJournal.createTable(in: db)
      #expect(try MLSPushRegistrationJournal.reserve(first, in: db) == first)
      #expect(try MLSPushRegistrationJournal.reserve(second, in: db) == second)
      let retained: Int = try Int.fetchOne(
        db,
        sql: "SELECT COUNT(*) FROM mls_push_registration_journal"
      ) ?? 0
      #expect(retained == 2)
      let oldBytes: Data? = try Data.fetchOne(
        db,
        sql: "SELECT request_json FROM mls_push_registration_journal WHERE token_generation = 1"
      )
      #expect(oldBytes == first.requestJSON)

      try MLSPushRegistrationJournal.markActive(
        second,
        bindingDigest: Data(repeating: 7, count: 32),
        in: db
      )
      let states = try Row.fetchAll(
        db,
        sql: "SELECT token_generation, state FROM mls_push_registration_journal ORDER BY token_generation"
      )
      #expect(states.count == 2)
      #expect((states[0]["state"] as String) == "superseded")
      #expect((states[1]["state"] as String) == "active")
    }
  }

  @Test("Exact pending retry reuses the original signed request")
  func exactPendingRetry() throws {
    let queue = try DatabaseQueue()
    let first = entry(generation: 1, tokenByte: 1, requestByte: 11)
    let independentlyPrepared = entry(generation: 2, tokenByte: 1, requestByte: 99)
    try queue.write { db in
      _ = try MLSPushRegistrationJournal.reserve(first, in: db)
      let reused = try MLSPushRegistrationJournal.reserve(independentlyPrepared, in: db)
      #expect(reused.tokenGeneration == 1)
      #expect(reused.requestJSON == first.requestJSON)
      let count: Int = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM mls_push_registration_journal") ?? 0
      #expect(count == 1)
    }
  }

  private func entry(
    generation: Int64,
    tokenByte: UInt8,
    requestByte: UInt8
  ) -> MLSPushRegistrationJournal.Entry {
    MLSPushRegistrationJournal.Entry(
      currentUserDID: "did:plc:abcdefghijklmnopqrstuvwx",
      deviceID: "00112233-4455-4677-8899-aabbccddeeff",
      provider: .apns,
      appID: "blue.catbird",
      registrationID: "11223344-5566-4788-99aa-bbccddeeff00",
      tokenGeneration: generation,
      tokenSHA256: Data(repeating: tokenByte, count: 32),
      enabled: true,
      keyID: "Vkdap1RjR0wChd9dvyvKtz2mUTWIOem3dIGy6rEHcIw",
      authGeneration: 7,
      publicKey: Data(repeating: 3, count: 32),
      signedAt: "2026-09-19T12:34:56.789Z",
      bodyJSON: Data(repeating: requestByte, count: 2),
      transcript: Data(repeating: requestByte, count: 3),
      signature: Data(repeating: requestByte, count: 64),
      requestJSON: Data(repeating: requestByte, count: 5),
      bindingDigest: nil,
      state: .pending
    )
  }
}
