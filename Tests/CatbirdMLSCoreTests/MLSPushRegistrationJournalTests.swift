@testable import CatbirdMLSCore
import Foundation
import GRDB
import Testing

struct MLSPushRegistrationJournalTests {
  private let instant = Date(timeIntervalSince1970: 1_789_821_296)
  @Test("Unknown attempts retain exact bytes and a changed binding advances generation")
  func unknownThenChanged() throws {
    let queue = try DatabaseQueue()
    let first = entry(generation: 1, tokenByte: 1, requestByte: 11)
    let second = entry(generation: 2, tokenByte: 2, requestByte: 22)
    try queue.write { db in
      try MLSPushRegistrationJournal.createTable(in: db)
      #expect(try MLSPushRegistrationJournal.reserve(first, in: db, now: instant) == first)
      #expect(try MLSPushRegistrationJournal.reserve(second, in: db, now: instant) == second)
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
      _ = try MLSPushRegistrationJournal.reserve(first, in: db, now: instant)
      let reused = try MLSPushRegistrationJournal.reserve(independentlyPrepared, in: db, now: instant)
      #expect(reused.tokenGeneration == 1)
      #expect(reused.requestJSON == first.requestJSON)
      let count: Int = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM mls_push_registration_journal") ?? 0
      #expect(count == 1)
    }
  }

  @Test("An aged unknown binding advances generation without erasing its custody")
  func agedUnknownThenFreshAcknowledgment() throws {
    let queue = try DatabaseQueue()
    let old = entry(generation: 1, tokenByte: 1, requestByte: 11, signedAt: instant.addingTimeInterval(-301))
    let fresh = entry(generation: 2, tokenByte: 1, requestByte: 22)
    let concurrent = entry(generation: 3, tokenByte: 1, requestByte: 33)
    try queue.write { db in
      _ = try MLSPushRegistrationJournal.reserve(old, in: db, now: instant)
      #expect(try MLSPushRegistrationJournal.reserve(fresh, in: db, now: instant) == fresh)
      #expect(try MLSPushRegistrationJournal.reserve(concurrent, in: db, now: instant) == fresh)
      let rows = try Row.fetchAll(db, sql: "SELECT request_json, state FROM mls_push_registration_journal ORDER BY token_generation")
      #expect(rows.count == 2)
      #expect((rows[0]["request_json"] as Data) == old.requestJSON)
      #expect((rows[0]["state"] as String) == "pending")
      try MLSPushRegistrationJournal.markActive(fresh, bindingDigest: Data(repeating: 7, count: 32), in: db)
    }
    #expect(throws: MLSPushRegistrationError.self) {
      try queue.write { db in
        try MLSPushRegistrationJournal.markActive(old, bindingDigest: Data(repeating: 8, count: 32), in: db)
      }
    }
    try queue.read { db in
      let rows = try Row.fetchAll(db, sql: "SELECT state FROM mls_push_registration_journal ORDER BY token_generation")
      #expect((rows[0]["state"] as String) == "superseded")
      #expect((rows[1]["state"] as String) == "active")
    }
  }

  @Test("The retry window is bounded in both clock directions and active receipts do not age out")
  func retryWindowAndActiveBinding() throws {
    let first = entry(generation: 1, tokenByte: 1, requestByte: 11)
    #expect(MLSPushRegistrationJournal.canReuse(first, now: instant.addingTimeInterval(240)))
    #expect(!MLSPushRegistrationJournal.canReuse(first, now: instant.addingTimeInterval(241)))
    #expect(!MLSPushRegistrationJournal.canReuse(first, now: instant.addingTimeInterval(-241)))
    let queue = try DatabaseQueue()
    try queue.write { db in
      _ = try MLSPushRegistrationJournal.reserve(first, in: db, now: instant)
      try MLSPushRegistrationJournal.markActive(first, bindingDigest: Data(repeating: 7, count: 32), in: db)
      let stored = try MLSPushRegistrationJournal.latest(
        userDID: first.currentUserDID,
        deviceID: first.deviceID,
        provider: first.provider,
        appID: first.appID,
        in: db
      )
      let active = try #require(stored)
      #expect(MLSPushRegistrationJournal.canReuse(active, now: instant.addingTimeInterval(86_400)))
      let candidate = entry(generation: 2, tokenByte: 1, requestByte: 22, signedAt: instant.addingTimeInterval(86_400))
      #expect(try MLSPushRegistrationJournal.reserve(candidate, in: db, now: instant.addingTimeInterval(86_400)) == active)
    }
  }

  private func entry(
    generation: Int64,
    tokenByte: UInt8,
    requestByte: UInt8,
    signedAt: Date? = nil
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
      signedAt: MLSPushRegistrationCodec.canonicalTimestamp(signedAt ?? instant),
      bodyJSON: Data(repeating: requestByte, count: 2),
      transcript: Data(repeating: requestByte, count: 3),
      signature: Data(repeating: requestByte, count: 64),
      requestJSON: Data(repeating: requestByte, count: 5),
      bindingDigest: nil,
      state: .pending
    )
  }
}
