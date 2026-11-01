import CatbirdMLS
@testable import CatbirdMLSCore
import CryptoKit
import Foundation
import XCTest

private final class MockLabyrinthSpaceServer: @unchecked Sendable {
  struct StoredRecord {
    let uri: String
    let collection: String
    let rkey: String
    let cid: String
    let valueJSON: [String: Any]
  }

  static let shared = MockLabyrinthSpaceServer()

  private let lock = NSLock()
  /// Keyed by `"\(space)|\(repo)"` -> `[uri: StoredRecord]`.
  private var repos: [String: [String: StoredRecord]] = [:]
  /// Optional per-space HTTP status override (e.g. `503` offline, `403` kicked).
  private var spaceStatusOverrides: [String: Int] = [:]
  private var tidCounter: UInt64 = 0
  private(set) var sawMainThreadRequest = false

  func reset() {
    lock.lock()
    repos.removeAll()
    spaceStatusOverrides.removeAll()
    tidCounter = 0
    sawMainThreadRequest = false
    lock.unlock()
  }

  func setStatusOverride(_ status: Int?, forSpace space: String) {
    lock.lock()
    if let status {
      spaceStatusOverrides[space] = status
    } else {
      spaceStatusOverrides.removeValue(forKey: space)
    }
    lock.unlock()
  }

  func recordCount(space: String, repo: String, collection: String? = nil) -> Int {
    lock.lock()
    defer { lock.unlock() }
    guard let bucket = repos["\(space)|\(repo)"] else {
      return 0
    }
    return bucket.values.filter { rec in
      collection == nil || rec.collection == collection
    }.count
  }

  func handle(url: URL, method: String, body: Data) -> (Int, Data) {
    let onMain = Thread.isMainThread
    lock.lock()
    if onMain {
      sawMainThreadRequest = true
    }
    lock.unlock()

    let path = url.path
    guard let xrpcRange = path.range(of: "/xrpc/") else {
      return (404, Data(#"{"error":"NotFound"}"#.utf8))
    }
    let nsid = String(path[xrpcRange.upperBound...])
    let queryItems = URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems ?? []
    func param(_ name: String) -> String? {
      queryItems.first(where: { $0.name == name })?.value
    }

    switch nsid {
    case "com.atproto.space.getSpace":
      if queryItems.isEmpty {
        return (
          400,
          Data(#"{"error":"InvalidRequest","message":"Missing space URI"}"#.utf8)
        )
      }
      let space = param("space") ?? ""
      if let forced = statusOverride(for: space) {
        return errorReply(status: forced, error: forced == 403 ? "Forbidden" : "SpaceUnavailable", message: space)
      }
      return jsonReply(200, ["uri": space])

    case "com.atproto.space.getRecord":
      guard let space = param("space"),
            let repo = param("repo"),
            let collection = param("collection"),
            let rkey = param("rkey")
      else {
        return errorReply(status: 400, error: "InvalidRequest", message: "missing query params")
      }
      if let forced = statusOverride(for: space) {
        return errorReply(status: forced, error: forced == 403 ? "Forbidden" : "SpaceUnavailable", message: space)
      }
      let uri = "\(space)/\(repo)/\(collection)/\(rkey)"
      lock.lock()
      let record = repos["\(space)|\(repo)"]?[uri]
      lock.unlock()
      guard let record else {
        return errorReply(status: 400, error: "RecordNotFound", message: uri)
      }
      return jsonReply(200, [
        "uri": record.uri,
        "cid": record.cid,
        "value": record.valueJSON,
      ])

    case "com.atproto.space.listRecords":
      guard let space = param("space"),
            let repo = param("repo")
      else {
        return errorReply(status: 400, error: "InvalidRequest", message: "missing space or repo")
      }
      if let forced = statusOverride(for: space) {
        return errorReply(status: forced, error: forced == 403 ? "Forbidden" : "SpaceUnavailable", message: space)
      }
      let limit = min(max(Int(param("limit") ?? "50") ?? 50, 1), 100)
      let reverse = param("reverse") == "true"
      let excludeValues = param("excludeValues") == "true"
      let collectionFilter = param("collection")
      let cursor = param("cursor")

      lock.lock()
      let allRecords = Array((repos["\(space)|\(repo)"] ?? [:]).values)
      lock.unlock()

      let filtered = allRecords.filter { rec in
        collectionFilter == nil || rec.collection == collectionFilter
      }
      let sorted = filtered.sorted { lhs, rhs in
        reverse ? (lhs.uri < rhs.uri) : (lhs.uri > rhs.uri)
      }
      let afterCursor = sorted.filter { rec in
        guard let cursor else { return true }
        return reverse ? (rec.uri > cursor) : (rec.uri < cursor)
      }
      let page = Array(afterCursor.prefix(limit))
      let recordsPayload: [[String: Any]] = page.map { rec in
        var entry: [String: Any] = [
          "collection": rec.collection,
          "rkey": rec.rkey,
          "cid": rec.cid,
        ]
        if !excludeValues {
          entry["value"] = rec.valueJSON
        }
        return entry
      }
      var response: [String: Any] = ["records": recordsPayload]
      if page.count == limit, let lastURI = page.last?.uri {
        response["cursor"] = lastURI
      }
      return jsonReply(200, response)

    case "com.atproto.space.listRepos":
      guard let space = param("space") else {
        return errorReply(status: 400, error: "InvalidRequest", message: "missing space")
      }
      if let forced = statusOverride(for: space) {
        return errorReply(status: forced, error: forced == 403 ? "Forbidden" : "SpaceUnavailable", message: space)
      }
      lock.lock()
      let dids = repos.compactMap { key, bucket -> [String: Any]? in
        guard !bucket.isEmpty else { return nil }
        let parts = key.split(separator: "|", maxSplits: 1).map(String.init)
        guard parts.count == 2, parts[0] == space else { return nil }
        return ["did": parts[1]]
      }
      lock.unlock()
      return jsonReply(200, ["repos": dids])

    case "com.atproto.space.applyWrites":
      guard method == "POST",
            let payload = (try? JSONSerialization.jsonObject(with: body)) as? [String: Any],
            let space = payload["space"] as? String,
            let repo = payload["repo"] as? String,
            let writes = payload["writes"] as? [[String: Any]]
      else {
        return errorReply(status: 400, error: "InvalidRequest", message: "invalid applyWrites body")
      }
      if let forced = statusOverride(for: space) {
        return errorReply(status: forced, error: forced == 403 ? "Forbidden" : "SpaceUnavailable", message: space)
      }

      lock.lock()
      defer { lock.unlock() }
      let bucketKey = "\(space)|\(repo)"
      var working = repos[bucketKey] ?? [:]
      var results: [[String: Any]] = []

      for write in writes {
        let kind = write["$type"] as? String ?? ""
        guard let collection = write["collection"] as? String else {
          return errorReply(status: 400, error: "InvalidRequest", message: "missing collection")
        }
        let rkey: String
        if let explicitRkey = write["rkey"] as? String, !explicitRkey.isEmpty {
          rkey = explicitRkey
        } else if kind.hasSuffix("#create") {
          rkey = nextTIDLocked()
        } else {
          return errorReply(status: 400, error: "InvalidRequest", message: "missing rkey")
        }
        let uri = "\(space)/\(repo)/\(collection)/\(rkey)"

        switch kind {
        case "com.atproto.space.applyWrites#create":
          if working[uri] != nil {
            return errorReply(status: 400, error: "RecordAlreadyExists", message: uri)
          }
          let val = (write["value"] as? [String: Any]) ?? [:]
          let cid = computeCID(for: val)
          working[uri] = StoredRecord(uri: uri, collection: collection, rkey: rkey, cid: cid, valueJSON: val)
          results.append([
            "$type": "com.atproto.space.applyWrites#createResult",
            "uri": uri,
            "cid": cid,
          ])

        case "com.atproto.space.applyWrites#update":
          guard working[uri] != nil else {
            return errorReply(status: 400, error: "RecordNotFound", message: uri)
          }
          let val = (write["value"] as? [String: Any]) ?? [:]
          let cid = computeCID(for: val)
          working[uri] = StoredRecord(uri: uri, collection: collection, rkey: rkey, cid: cid, valueJSON: val)
          results.append([
            "$type": "com.atproto.space.applyWrites#updateResult",
            "uri": uri,
            "cid": cid,
          ])

        case "com.atproto.space.applyWrites#delete":
          working.removeValue(forKey: uri)
          results.append(["$type": "com.atproto.space.applyWrites#deleteResult"])

        default:
          return errorReply(status: 400, error: "InvalidRequest", message: "unknown write \(kind)")
        }
      }

      repos[bucketKey] = working
      return jsonReply(200, ["results": results])

    case "com.atproto.space.deleteRecord":
      guard method == "POST",
            let payload = (try? JSONSerialization.jsonObject(with: body)) as? [String: Any],
            let space = payload["space"] as? String,
            let repo = payload["repo"] as? String,
            let collection = payload["collection"] as? String,
            let rkey = payload["rkey"] as? String
      else {
        return errorReply(status: 400, error: "InvalidRequest", message: "invalid deleteRecord body")
      }
      if let forced = statusOverride(for: space) {
        return errorReply(status: forced, error: forced == 403 ? "Forbidden" : "SpaceUnavailable", message: space)
      }
      let uri = "\(space)/\(repo)/\(collection)/\(rkey)"
      lock.lock()
      repos["\(space)|\(repo)"]?.removeValue(forKey: uri)
      lock.unlock()
      return jsonReply(200, [:])

    default:
      // Standard repo/chat XRPC endpoints during enrollDevice/createGroup/sendMessage.
      return jsonReply(200, [
        "uri": "at://did:plc:mock/record/self",
        "cid": "bafyreibasecid00000000000000000000000000000000",
        "ok": true,
      ])
    }
  }

  private func statusOverride(for space: String) -> Int? {
    lock.lock()
    defer { lock.unlock() }
    return spaceStatusOverrides[space]
  }

  private func nextTIDLocked() -> String {
    let alphabet = Array("234567abcdefghijklmnopqrstuvwxyz")
    tidCounter += 1
    let v = (UInt64(1) << 62) | tidCounter
    return String((0..<13).reversed().map { idx in
      alphabet[Int((v >> (5 * idx)) & 31)]
    })
  }

  private func computeCID(for dict: [String: Any]) -> String {
    let data = (try? JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys])) ?? Data()
    let digest = SHA256.hash(data: data)
    let hex = digest.map { String(format: "%02x", $0) }.joined()
    return "bafyrei" + String(hex.prefix(40))
  }

  private func jsonReply(_ status: Int, _ obj: [String: Any]) -> (Int, Data) {
    let data = (try? JSONSerialization.data(withJSONObject: obj)) ?? Data("{}".utf8)
    return (status, data)
  }

  private func errorReply(status: Int, error: String, message: String) -> (Int, Data) {
    jsonReply(status, ["error": error, "message": message])
  }
}

private final class MockLabyrinthSpaceURLProtocol: URLProtocol, @unchecked Sendable {
  private static let queue = DispatchQueue(
    label: "blue.catbird.mls.test.labyrinth-space-urlprotocol",
    qos: .userInitiated,
    attributes: .concurrent
  )

  override class func canInit(with request: URLRequest) -> Bool {
    true
  }

  override class func canonicalRequest(for request: URLRequest) -> URLRequest {
    request
  }

  override func startLoading() {
    guard let url = request.url else {
      client?.urlProtocol(self, didFailWithError: URLError(.badURL))
      return
    }
    let method = request.httpMethod ?? "GET"
    let body = Self.extractBody(from: request)

    Self.queue.async { [weak self] in
      guard let self else { return }
      let (status, responseBody) = MockLabyrinthSpaceServer.shared.handle(
        url: url,
        method: method,
        body: body
      )
      let response = HTTPURLResponse(
        url: url,
        statusCode: status,
        httpVersion: "HTTP/1.1",
        headerFields: ["Content-Type": "application/json"]
      )!
      self.client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
      self.client?.urlProtocol(self, didLoad: responseBody)
      self.client?.urlProtocolDidFinishLoading(self)
    }
  }

  override func stopLoading() {}

  private static func extractBody(from request: URLRequest) -> Data {
    if let body = request.httpBody {
      return body
    }
    guard let stream = request.httpBodyStream else {
      return Data()
    }
    stream.open()
    defer { stream.close() }
    var out = Data()
    var buffer = [UInt8](repeating: 0, count: 16 * 1024)
    while stream.hasBytesAvailable {
      let readCount = stream.read(&buffer, maxLength: buffer.count)
      if readCount <= 0 {
        break
      }
      out.append(buffer, count: readCount)
    }
    return out
  }
}

final class MLSLabyrinthOutboxAndRestoreE2ETests: XCTestCase {
  private var device1Root: URL!
  private var device2Root: URL!
  private var boundedSession: MLSBoundedURLSession!

  override func setUpWithError() throws {
    try super.setUpWithError()
    MockLabyrinthSpaceServer.shared.reset()
    let fm = FileManager.default
    device1Root = fm.temporaryDirectory
      .appendingPathComponent("CatbirdLabyrinthE2E-Dev1-\(UUID().uuidString)", isDirectory: true)
    device2Root = fm.temporaryDirectory
      .appendingPathComponent("CatbirdLabyrinthE2E-Dev2-\(UUID().uuidString)", isDirectory: true)
    try fm.createDirectory(at: device1Root, withIntermediateDirectories: true)
    try fm.createDirectory(at: device2Root, withIntermediateDirectories: true)
    boundedSession = MLSBoundedURLSession(protocolClasses: [MockLabyrinthSpaceURLProtocol.self])
  }

  override func tearDownWithError() throws {
    boundedSession?.invalidateAndCancel()
    boundedSession = nil
    let fm = FileManager.default
    let trashRoot = fm.temporaryDirectory
      .appendingPathComponent("_trash-CatbirdLabyrinthE2E", isDirectory: true)
    try? fm.createDirectory(at: trashRoot, withIntermediateDirectories: true)
    for root in [device1Root, device2Root].compactMap({ $0 }) {
      try? fm.moveItem(at: root, to: trashRoot.appendingPathComponent(root.lastPathComponent))
    }
    device1Root = nil
    device2Root = nil
    try super.tearDownWithError()
  }

  func testAutomaticBackgroundOutboxDrainAndOfflineRestartResumeSurvival() async throws {
    let aliceDid = "did:plc:alicee2eoutbox0000000000"
    let dsDid = "did:web:chat.catbird.blue"
    let conversationId = "convo-auto-drain-e2e"
    let convoSpaceURI = "at://\(dsDid)/space/blue.catbird.labyrinth.conversation/\(conversationId)-0"

    let backend1 = RecordingKeychainBackend()
    let keychainService1 = "blue.catbird.mls.e2e.dev1.\(UUID().uuidString)"
    let keystore1 = KeychainPlatformKeyStore(service: keychainService1, backend: backend1)
    let transport1 = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      boundedSession: boundedSession
    )
    let config1 = CatbirdMlsClientConfig(
      storageRoot: device1Root.path,
      accountDid: aliceDid,
      deviceId: "dev-iphone-1",
      homeDsDid: dsDid,
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://swan.catbird.blue"
    )

    // 1. Open Device 1, enroll, setup Labyrinth recovery code, create group, and send online message.
    let coordinator1 = try await MainActor.run {
      try MLSClientCoordinator(config: config1, keystore: keystore1, transport: transport1)
    }
    _ = try await coordinator1.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    _ = try await coordinator1.setupRecoveryCode()
    _ = try await coordinator1.createGroup(
      conversationId: conversationId,
      sequencerDid: dsDid,
      title: "Automatic Drain Group",
      description: nil
    )

    let sentOnline = try await coordinator1.sendMessage(
      conversationId: conversationId,
      messageId: "msg-online-1",
      text: "hello automatic background outbox drain"
    )
    XCTAssertEqual(sentOnline.messageId, "msg-online-1")

    // Without calling flushLabyrinthQueue() manually, background drain on ffiDispatchQueue must empty the queue!
    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await coordinator1.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }
    XCTAssertEqual(
      MockLabyrinthSpaceServer.shared.recordCount(
        space: convoSpaceURI,
        repo: aliceDid,
        collection: "blue.catbird.labyrinth.batch"
      ),
      1,
      "Automatic background drain after sendMessage must commit a batch to the conversation Space"
    )

    // 2. Make conversation Space temporarily offline (HTTP 503): sendMessage MUST still succeed and queue durably!
    MockLabyrinthSpaceServer.shared.setStatusOverride(503, forSpace: convoSpaceURI)

    let sentOffline = try await coordinator1.sendMessage(
      conversationId: conversationId,
      messageId: "msg-offline-2",
      text: "queued while Space returns 503"
    )
    XCTAssertEqual(sentOffline.messageId, "msg-offline-2")

    // Give background best-effort drain 60ms to hit 503 and retain the row without crashing/throwing.
    try await Task.sleep(nanoseconds: 60_000_000)
    let pendingBeforeRestart = try await coordinator1.listPendingLabyrinthQueue(conversationId: conversationId)
    XCTAssertEqual(pendingBeforeRestart.map(\.messageId), ["msg-offline-2"])
    XCTAssertNotNil(pendingBeforeRestart.first?.frameNonceB64)

    // 3. Simulate app termination + cold reopen while Space is STILL offline (503).
    await coordinator1.close()

    let coordinatorReopened = try await MainActor.run {
      try MLSClientCoordinator(config: config1, keystore: keystore1, transport: transport1)
    }
    let pendingAfterRestart = try await coordinatorReopened.listPendingLabyrinthQueue(conversationId: conversationId)
    XCTAssertEqual(
      pendingAfterRestart.map(\.messageId),
      ["msg-offline-2"],
      "Queued Labyrinth payload must survive coordinator close/reopen across restarts"
    )

    // 4. Background the app, restore Space availability (200), then resumeFromBackground():
    //    resumeFromBackground() must automatically drain the surviving offline row!
    _ = try await coordinatorReopened.prepareForBackground()
    let closedInBg = await MainActor.run { coordinatorReopened.isHandleClosed }
    XCTAssertTrue(closedInBg)

    MockLabyrinthSpaceServer.shared.setStatusOverride(nil, forSpace: convoSpaceURI)
    try await coordinatorReopened.resumeFromBackground()

    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await coordinatorReopened.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }
    XCTAssertEqual(
      MockLabyrinthSpaceServer.shared.recordCount(
        space: convoSpaceURI,
        repo: aliceDid,
        collection: "blue.catbird.labyrinth.batch"
      ),
      2,
      "resumeFromBackground must automatically drain queued outbox rows to the conversation Space"
    )

    // Explicit flush on empty queue is a clean no-op.
    let explicitZero = try await coordinatorReopened.flushLabyrinthQueue()
    XCTAssertEqual(explicitZero, 0)
    XCTAssertFalse(MockLabyrinthSpaceServer.shared.sawMainThreadRequest)

    await coordinatorReopened.close()
  }

  func testKick403FallbackRescueConversationAndFreshDeviceMailboxRestoreEndToEnd() async throws {
    let aliceDid = "did:plc:alicekickrestore00000000"
    let bobDid = "did:plc:bobpeerkick0000000000000"
    let dsDid = "did:web:chat.catbird.blue"
    let conversationId = "convo-kick-restore-e2e"
    let convoSpaceURI = "at://\(dsDid)/space/blue.catbird.labyrinth.conversation/\(conversationId)-0"
    let personalMailboxSpaceURI = "at://\(aliceDid)/space/blue.catbird.labyrinth.mailbox/self"

    let backend1 = RecordingKeychainBackend()
    let keystore1 = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.e2e.kick.dev1.\(UUID().uuidString)",
      backend: backend1
    )
    let transport1 = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      boundedSession: boundedSession
    )
    let config1 = CatbirdMlsClientConfig(
      storageRoot: device1Root.path,
      accountDid: aliceDid,
      deviceId: "dev-alice-1",
      homeDsDid: dsDid,
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://swan.catbird.blue"
    )

    // -------------------------------------------------------------------------
    // Device 1: Enroll, setup recovery code, seed Bob + Alice history, get kicked (403),
    // drain outbox fallback + rescueConversationOnKick, then wipe Device 1!
    // -------------------------------------------------------------------------
    let device1 = try await MainActor.run {
      try MLSClientCoordinator(config: config1, keystore: keystore1, transport: transport1)
    }
    _ = try await device1.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    let recoveryCode = try await device1.setupRecoveryCode()

    let convoRow = try await device1.createGroup(
      conversationId: conversationId,
      sequencerDid: dsDid,
      title: "Pre-Kick Conversation",
      description: nil
    )

    let seededHistory: [MessageRow] = [
      MessageRow(
        messageId: "msg-bob-1",
        conversationId: conversationId,
        groupId: convoRow.groupId,
        epoch: 0,
        seq: 1,
        senderDid: bobDid,
        senderDeviceId: "dev-bob-1",
        plaintextJson: #"{"text":"Bob peer message before kick"}"#,
        createdAtMs: 1_700_200_000_100,
        isOwn: false
      ),
      MessageRow(
        messageId: "msg-alice-cached-2",
        conversationId: conversationId,
        groupId: convoRow.groupId,
        epoch: 0,
        seq: 2,
        senderDid: aliceDid,
        senderDeviceId: "dev-alice-1",
        plaintextJson: #"{"text":"Alice cached message before kick"}"#,
        createdAtMs: 1_700_200_000_200,
        isOwn: true
      ),
    ]
    let importedSeed = try await device1.importConversationHistory(
      conversationId: conversationId,
      messages: seededHistory
    )
    XCTAssertEqual(importedSeed, 2)

    // Kick Alice from the conversation Space (403 Forbidden on conversation Space; personal mailbox stays 200).
    MockLabyrinthSpaceServer.shared.setStatusOverride(403, forSpace: convoSpaceURI)

    _ = try await device1.sendMessage(
      conversationId: conversationId,
      messageId: "msg-alice-postkick-3",
      text: "Alice message queued right as 403 kick landed"
    )

    // Wait for automatic background drain (or explicit flushLabyrinthQueue) to fall back to personal mailbox Space!
    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await device1.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }
    let postFlushExtra = try await device1.flushLabyrinthQueue()
    XCTAssertEqual(postFlushExtra, 0)
    XCTAssertEqual(
      MockLabyrinthSpaceServer.shared.recordCount(
        space: convoSpaceURI,
        repo: aliceDid,
        collection: "blue.catbird.labyrinth.batch"
      ),
      0,
      "403 conversation Space must reject direct conversation batch writes"
    )
    XCTAssertGreaterThanOrEqual(
      MockLabyrinthSpaceServer.shared.recordCount(
        space: personalMailboxSpaceURI,
        repo: aliceDid,
        collection: "blue.catbird.labyrinth.ownerBatch"
      ),
      1,
      "403 kick during outbox drain must fall back to personal mailbox ownerBatch"
    )

    // Rescue entire cached conversation history (including Bob's peer message!) into personal mailbox Space.
    let rescuedCount = try await device1.rescueConversationOnKick(conversationId: conversationId)
    XCTAssertEqual(rescuedCount, 3, "rescueConversationOnKick must archive all 3 cached messages")

    // Idempotent second rescue call must tolerate deterministic rkey collisions (`RecordAlreadyExists`).
    let rescuedSecondPass = try await device1.rescueConversationOnKick(conversationId: conversationId)
    XCTAssertEqual(rescuedSecondPass, 3)

    // Wipe Device 1 completely before restoring on Device 2.
    try await device1.signOutAndWipe()

    // -------------------------------------------------------------------------
    // Device 2: Completely fresh storageRoot + fresh KeychainBackend, zero shared local state.
    // Restore via recovery code (with a 1-character typo inside Reed-Solomon budget!) + owner mailbox!
    // -------------------------------------------------------------------------
    let backend2 = RecordingKeychainBackend()
    let keystore2 = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.e2e.kick.dev2.\(UUID().uuidString)",
      backend: backend2
    )
    let transport2 = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      boundedSession: boundedSession
    )
    let config2 = CatbirdMlsClientConfig(
      storageRoot: device2Root.path,
      accountDid: aliceDid,
      deviceId: "dev-alice-2-fresh",
      homeDsDid: dsDid,
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://swan.catbird.blue"
    )

    let recorder = await MainActor.run { EventRecorder() }
    let device2 = try await MainActor.run {
      let c = try MLSClientCoordinator(config: config2, keystore: keystore2, transport: transport2)
      c.onChangeCallback = { event in
        recorder.events.append(event)
      }
      return c
    }

    _ = try await device2.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)

    // Wrong recovery code MUST fail closed.
    do {
      _ = try await device2.restoreWithRecoveryCode("AAAA-BBBB-CCCC-DDDD-EEEE-FFFF")
      XCTFail("Bogus recovery code must fail closed")
    } catch {
      // Expected failure.
    }

    // Introduce 2 character typos (after the 2-character version prefix) into the 40-char recovery code.
    let typoRecoveryCode = Self.injectTwoCharacterTypos(into: recoveryCode)
    let backupStatus = try await device2.restoreWithRecoveryCode(typoRecoveryCode)
    XCTAssertTrue(backupStatus.backupEnabled)
    XCTAssertTrue(backupStatus.hasRecoveryCode)
    XCTAssertTrue(backupStatus.activeDeviceIds.contains("dev-alice-2-fresh"))

    let restoredMessagesCount = try await device2.restoreConversationFromOwnerMailbox(
      conversationId: conversationId
    )
    XCTAssertEqual(restoredMessagesCount, 3)

    // Second restore pass must be idempotent (0 duplicate inserts).
    let secondRestoreCount = try await device2.restoreConversationFromOwnerMailbox(
      conversationId: conversationId
    )
    XCTAssertEqual(secondRestoreCount, 0)

    let conversations = try await device2.listConversations(limit: 10)
    XCTAssertEqual(conversations.map(\.conversationId), [conversationId])

    // listMessages returns newest-first or seq-ordered depending on query; sort by seq ascending to assert all 3.
    let restoredMessages = try await device2.listMessages(conversationId: conversationId, limit: 20)
      .sorted { $0.seq < $1.seq }
    XCTAssertEqual(restoredMessages.count, 3)

    XCTAssertEqual(restoredMessages[0].messageId, "msg-bob-1")
    XCTAssertEqual(restoredMessages[0].senderDid, bobDid)
    XCTAssertFalse(restoredMessages[0].isOwn, "Peer message restored from v5 attributed frame must have isOwn == false")
    XCTAssertTrue(restoredMessages[0].plaintextJson.contains("Bob peer message before kick"))

    XCTAssertEqual(restoredMessages[1].messageId, "msg-alice-cached-2")
    XCTAssertEqual(restoredMessages[1].senderDid, aliceDid)
    XCTAssertTrue(restoredMessages[1].isOwn)
    XCTAssertTrue(restoredMessages[1].plaintextJson.contains("Alice cached message before kick"))

    XCTAssertEqual(restoredMessages[2].messageId, "msg-alice-postkick-3")
    XCTAssertEqual(restoredMessages[2].senderDid, aliceDid)
    XCTAssertTrue(restoredMessages[2].isOwn)
    XCTAssertTrue(restoredMessages[2].plaintextJson.contains("Alice message queued right as 403 kick landed"))

    // Give MainActor observer hop a moment to deliver PlatformChangeEvents.
    try await waitUntil(timeoutSeconds: 2.0) {
      await MainActor.run {
        recorder.events.contains(.messagesUpdated(conversationId: conversationId))
          && recorder.events.contains(.conversationsUpdated)
      }
    }

    XCTAssertFalse(MockLabyrinthSpaceServer.shared.sawMainThreadRequest)
    await device2.close()
  }

  func testColdStartEnrollDrainsOfflineQueueConcurrentBurstCoalescesAndClosedHandleRejects() async throws {
    let aliceDid = "did:plc:aliceedgecases0000000000"
    let dsDid = "did:web:chat.catbird.blue"
    let conversationId = "convo-edge-coldstart"
    let convoSpaceURI = "at://\(dsDid)/space/blue.catbird.labyrinth.conversation/\(conversationId)-0"

    let backend = RecordingKeychainBackend()
    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.e2e.edge.\(UUID().uuidString)",
      backend: backend
    )
    let transport = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      boundedSession: boundedSession
    )
    let config = CatbirdMlsClientConfig(
      storageRoot: device1Root.path,
      accountDid: aliceDid,
      deviceId: "dev-edge-1",
      homeDsDid: dsDid,
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://swan.catbird.blue"
    )

    let coordinator1 = try await MainActor.run {
      try MLSClientCoordinator(config: config, keystore: keystore, transport: transport)
    }
    _ = try await coordinator1.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    _ = try await coordinator1.createGroup(
      conversationId: conversationId,
      sequencerDid: dsDid,
      title: "Cold-Start & Burst Group",
      description: nil
    )

    // 1. Queue a message while Space returns 503, terminate coordinator, restore 200, cold-reopen + enrollDevice!
    MockLabyrinthSpaceServer.shared.setStatusOverride(503, forSpace: convoSpaceURI)
    _ = try await coordinator1.sendMessage(
      conversationId: conversationId,
      messageId: "msg-coldstart-1",
      text: "queued offline before cold restart"
    )
    try await Task.sleep(nanoseconds: 50_000_000)
    await coordinator1.close()

    MockLabyrinthSpaceServer.shared.setStatusOverride(nil, forSpace: convoSpaceURI)
    let coordinator2 = try await MainActor.run {
      try MLSClientCoordinator(config: config, keystore: keystore, transport: transport)
    }
    let pendingOnColdOpen = try await coordinator2.listPendingLabyrinthQueue(conversationId: conversationId)
    XCTAssertEqual(pendingOnColdOpen.map(\.messageId), ["msg-coldstart-1"])

    _ = try await coordinator2.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await coordinator2.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }

    // 2. Concurrent 12-message burst coalesces through OutboxDrainCoalescer and commits all 12 rows.
    try await withThrowingTaskGroup(of: Void.self) { group in
      for idx in 0..<12 {
        group.addTask {
          _ = try await coordinator2.sendMessage(
            conversationId: conversationId,
            messageId: "msg-burst-\(idx)",
            text: "burst payload \(idx)"
          )
        }
      }
      try await group.waitForAll()
    }
    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await coordinator2.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }
    XCTAssertEqual(
      MockLabyrinthSpaceServer.shared.recordCount(
        space: convoSpaceURI,
        repo: aliceDid,
        collection: "blue.catbird.labyrinth.batch"
      ),
      13
    )

    // 3. Empty/unknown conversation rescue & restore boundary cases return 0 cleanly.
    let emptyRescue = try await coordinator2.rescueConversationOnKick(conversationId: "nonexistent-convo")
    XCTAssertEqual(emptyRescue, 0)
    let emptyRestore = try await coordinator2.restoreConversationFromOwnerMailbox(conversationId: "nonexistent-convo")
    XCTAssertEqual(emptyRestore, 0)

    // 4. After prepareForBackground() (handle closed), outbox/rescue/restore fail closed with ContextClosed.
    _ = try await coordinator2.prepareForBackground()
    for closedOp in [
      { _ = try await coordinator2.listPendingLabyrinthQueue() },
      { _ = try await coordinator2.flushLabyrinthQueue() },
      { _ = try await coordinator2.rescueConversationOnKick(conversationId: conversationId) },
      { _ = try await coordinator2.restoreConversationFromOwnerMailbox(conversationId: conversationId) },
    ] {
      do {
        try await closedOp()
        XCTFail("Expected ContextClosed error when SQLite handle is closed")
      } catch CatbirdMLS.MlsError.ContextClosed {
        // Expected.
      }
    }

    await coordinator2.close()
  }

  func testColdLaunchCheckSpacesEligibilityAutomaticallyDrainsWithoutReEnroll() async throws {
    let aliceDid = "did:plc:alicecoldelig00000000000"
    let dsDid = "did:web:chat.catbird.blue"
    let conversationId = "convo-cold-eligibility-drain"
    let convoSpaceURI = "at://\(dsDid)/space/blue.catbird.labyrinth.conversation/\(conversationId)-0"

    let backend = RecordingKeychainBackend()
    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.e2e.coldelig.\(UUID().uuidString)",
      backend: backend
    )
    let transport = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      boundedSession: boundedSession
    )
    let config = CatbirdMlsClientConfig(
      storageRoot: device1Root.path,
      accountDid: aliceDid,
      deviceId: "dev-cold-elig-1",
      homeDsDid: dsDid,
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://swan.catbird.blue"
    )

    let coordinator1 = try await MainActor.run {
      try MLSClientCoordinator(config: config, keystore: keystore, transport: transport)
    }
    _ = try await coordinator1.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    _ = try await coordinator1.createGroup(
      conversationId: conversationId,
      sequencerDid: dsDid,
      title: "Cold Eligibility Drain",
      description: nil
    )

    MockLabyrinthSpaceServer.shared.setStatusOverride(503, forSpace: convoSpaceURI)
    _ = try await coordinator1.sendMessage(
      conversationId: conversationId,
      messageId: "msg-cold-elig-1",
      text: "queued before app termination on already-enrolled device"
    )
    try await Task.sleep(nanoseconds: 50_000_000)
    await coordinator1.close()

    // Cold reopen of already-enrolled account: calling checkSpacesEligibility(grantedScopes:)
    // WITHOUT calling enrollDevice or flushLabyrinthQueue must automatically drain the outbox!
    MockLabyrinthSpaceServer.shared.setStatusOverride(nil, forSpace: convoSpaceURI)
    let coordinator2 = try await MainActor.run {
      try MLSClientCoordinator(config: config, keystore: keystore, transport: transport)
    }
    let pendingOnOpen = try await coordinator2.listPendingLabyrinthQueue(conversationId: conversationId)
    XCTAssertEqual(pendingOnOpen.map(\.messageId), ["msg-cold-elig-1"])

    let eligibility = try await coordinator2.checkSpacesEligibility(
      grantedScopes: MLSAppGroupStorage.requiredSpacesScopes
    )
    XCTAssertTrue(eligibility.eligible)

    try await waitUntil(timeoutSeconds: 3.0) {
      let pending = try await coordinator2.listPendingLabyrinthQueue(conversationId: conversationId)
      return pending.isEmpty
    }
    XCTAssertEqual(
      MockLabyrinthSpaceServer.shared.recordCount(
        space: convoSpaceURI,
        repo: aliceDid,
        collection: "blue.catbird.labyrinth.batch"
      ),
      1,
      "Cold-launch checkSpacesEligibility must automatically drain queued outbox rows"
    )

    await coordinator2.close()
  }

  func testOutboxDrainCoalescerPreservesPostResumeWakeupUnderPreBackgroundRace() {
    let coalescer = OutboxDrainCoalescer()
    var handleClosed = false

    // 1. Pre-background drain pass starts.
    XCTAssertTrue(coalescer.requestDrain(isHandleClosed: { handleClosed }))
    XCTAssertTrue(coalescer.beginFirstIteration(isHandleClosed: { handleClosed }))

    // 2. App enters background while pass 1 is finishing, then immediately resumes and requests drain.
    coalescer.setSuspended(true)
    handleClosed = true
    handleClosed = false
    coalescer.setSuspended(false)
    XCTAssertFalse(
      coalescer.requestDrain(isHandleClosed: { handleClosed }),
      "In-flight drain pass should coalesce the post-resume wakeup via needsAnotherPass"
    )

    // 3. Pass 1 finishes AFTER resumeFromBackground reopened the handle: must NOT drop needsAnotherPass!
    XCTAssertTrue(
      coalescer.completeAndShouldRepeat(isHandleClosed: { handleClosed }),
      "Atomic completeAndShouldRepeat must observe reopened handle and run the coalesced post-resume pass"
    )
    XCTAssertFalse(
      coalescer.completeAndShouldRepeat(isHandleClosed: { handleClosed }),
      "After post-resume pass completes with no further requests, coalescer must go idle"
    )
  }

  func testMultiConversationHeadOfLineIsolationAndThreeDeviceMultiEpochRecoveryEndToEnd() async throws {
    let aliceDid = "did:plc:alicemultie2e00000000000"
    let bobDid = "did:plc:bobmultie2e0000000000000"
    let dsDid = "did:web:chat.catbird.blue"
    let stalledConvo = "convo-hol-stalled-e2e"
    let healthyConvo = "convo-hol-healthy-e2e"
    let stalledSpaceURI = "at://\(dsDid)/space/blue.catbird.labyrinth.conversation/\(stalledConvo)-0"
    let healthySpaceURI = "at://\(dsDid)/space/blue.catbird.labyrinth.conversation/\(healthyConvo)-0"

    let fm = FileManager.default
    let device3Root = fm.temporaryDirectory
      .appendingPathComponent("CatbirdLabyrinthE2E-Dev3-\(UUID().uuidString)", isDirectory: true)
    try fm.createDirectory(at: device3Root, withIntermediateDirectories: true)
    defer {
      let trashRoot = fm.temporaryDirectory
        .appendingPathComponent("_trash-CatbirdLabyrinthE2E", isDirectory: true)
      try? fm.createDirectory(at: trashRoot, withIntermediateDirectories: true)
      try? fm.moveItem(at: device3Root, to: trashRoot.appendingPathComponent(device3Root.lastPathComponent))
    }

    let keystore1 = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.e2e.multi.dev1.\(UUID().uuidString)",
      backend: RecordingKeychainBackend()
    )
    let transport1 = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      boundedSession: boundedSession
    )
    let config1 = CatbirdMlsClientConfig(
      storageRoot: device1Root.path,
      accountDid: aliceDid,
      deviceId: "dev-multi-1",
      homeDsDid: dsDid,
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://swan.catbird.blue"
    )

    let device1 = try await MainActor.run {
      try MLSClientCoordinator(config: config1, keystore: keystore1, transport: transport1)
    }
    _ = try await device1.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)

    // --- Part A: Per-conversation head-of-line 503 isolation ---
    _ = try await device1.createGroup(
      conversationId: stalledConvo,
      sequencerDid: dsDid,
      title: "Stalled Convo",
      description: nil
    )
    _ = try await device1.createGroup(
      conversationId: healthyConvo,
      sequencerDid: dsDid,
      title: "Healthy Convo",
      description: nil
    )
    MockLabyrinthSpaceServer.shared.setStatusOverride(503, forSpace: stalledSpaceURI)

    _ = try await device1.sendMessage(
      conversationId: stalledConvo,
      messageId: "msg-stalled-1",
      text: "stalled behind 503"
    )
    _ = try await device1.sendMessage(
      conversationId: healthyConvo,
      messageId: "msg-healthy-1",
      text: "must drain despite earlier stalled conversation"
    )

    try await waitUntil(timeoutSeconds: 3.0) {
      let healthyPending = try await device1.listPendingLabyrinthQueue(conversationId: healthyConvo)
      return healthyPending.isEmpty
    }
    XCTAssertEqual(
      MockLabyrinthSpaceServer.shared.recordCount(
        space: healthySpaceURI,
        repo: aliceDid,
        collection: "blue.catbird.labyrinth.batch"
      ),
      1
    )
    let stillStalled = try await device1.listPendingLabyrinthQueue(conversationId: stalledConvo)
    XCTAssertEqual(stillStalled.map(\.messageId), ["msg-stalled-1"])
    MockLabyrinthSpaceServer.shared.setStatusOverride(nil, forSpace: stalledSpaceURI)
    _ = try await device1.flushLabyrinthQueue()

    // --- Part B: Multi-epoch & 3-device recovery chain ---
    // Epoch 1 rescue (before OTC & before setupRecoveryCode)
    let ep1Row = try await device1.createGroup(
      conversationId: "convo-swift-ep1",
      sequencerDid: dsDid,
      title: "Epoch 1",
      description: nil
    )
    _ = try await device1.importConversationHistory(
      conversationId: "convo-swift-ep1",
      messages: [
        MessageRow(
          messageId: "ep1-bob",
          conversationId: "convo-swift-ep1",
          groupId: ep1Row.groupId,
          epoch: 0,
          seq: 1,
          senderDid: bobDid,
          senderDeviceId: "dev-bob",
          plaintextJson: #"{"text":"Bob message at epoch 1"}"#,
          createdAtMs: 1_700_400_000_001,
          isOwn: false
        )
      ]
    )
    let r1 = try await device1.rescueConversationOnKick(conversationId: "convo-swift-ep1")
    XCTAssertEqual(r1, 1)

    // Rotate 1 -> 2 via OTC pairing, rescue Epoch 2, setup recovery code at Epoch 2
    let otcOffer = try await device1.initiateOtcPairing()
    let otcDone = try await device1.completeOtcPairing(sixDigitCode: otcOffer.sixDigitCode)
    XCTAssertEqual(otcDone.newEpochSeq, 2)

    _ = try await device1.createGroup(
      conversationId: "convo-swift-ep2",
      sequencerDid: dsDid,
      title: "Epoch 2",
      description: nil
    )
    _ = try await device1.sendMessage(
      conversationId: "convo-swift-ep2",
      messageId: "ep2-alice",
      text: "Alice message at epoch 2"
    )
    let r2 = try await device1.rescueConversationOnKick(conversationId: "convo-swift-ep2")
    XCTAssertEqual(r2, 1)

    let recoveryCode = try await device1.setupRecoveryCode()

    // Rotate 2 -> 3 via device revocation after setupRecoveryCode, rescue Epoch 3, wipe Device 1
    let revStatus = try await device1.revokeLabyrinthDevice(targetDeviceId: "old-ipad")
    XCTAssertEqual(revStatus.epochSeq, 3)

    _ = try await device1.createGroup(
      conversationId: "convo-swift-ep3",
      sequencerDid: dsDid,
      title: "Epoch 3",
      description: nil
    )
    _ = try await device1.sendMessage(
      conversationId: "convo-swift-ep3",
      messageId: "ep3-alice",
      text: "Alice message at epoch 3"
    )
    let r3 = try await device1.rescueConversationOnKick(conversationId: "convo-swift-ep3")
    XCTAssertEqual(r3, 1)
    try await device1.signOutAndWipe()

    // Device 2 restores (backward 2->1->0, forward 2->3, rotates 3->4), rescues Epoch 4, wipes Device 2
    let config2 = CatbirdMlsClientConfig(
      storageRoot: device2Root.path,
      accountDid: aliceDid,
      deviceId: "dev-multi-2",
      homeDsDid: dsDid,
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://swan.catbird.blue"
    )
    let keystore2 = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.e2e.multi.dev2.\(UUID().uuidString)",
      backend: RecordingKeychainBackend()
    )
    let transport2 = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      boundedSession: boundedSession
    )
    let device2 = try await MainActor.run {
      try MLSClientCoordinator(config: config2, keystore: keystore2, transport: transport2)
    }
    _ = try await device2.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    let st2 = try await device2.restoreWithRecoveryCode(recoveryCode)
    XCTAssertEqual(st2.epochSeq, 4)

    for cid in ["convo-swift-ep1", "convo-swift-ep2", "convo-swift-ep3"] {
      let count = try await device2.restoreConversationFromOwnerMailbox(conversationId: cid)
      XCTAssertEqual(count, 1, "Device 2 must restore \(cid)")
    }
    let ep1MsgsDev2 = try await device2.listMessages(conversationId: "convo-swift-ep1", limit: 10)
    XCTAssertEqual(ep1MsgsDev2.first?.senderDid, bobDid)
    XCTAssertEqual(ep1MsgsDev2.first?.isOwn, false)

    _ = try await device2.createGroup(
      conversationId: "convo-swift-ep4",
      sequencerDid: dsDid,
      title: "Epoch 4",
      description: nil
    )
    _ = try await device2.sendMessage(
      conversationId: "convo-swift-ep4",
      messageId: "ep4-dev2",
      text: "Device 2 message at epoch 4"
    )
    let r4 = try await device2.rescueConversationOnKick(conversationId: "convo-swift-ep4")
    XCTAssertEqual(r4, 1)
    try await device2.signOutAndWipe()

    // Device 3 restores with the SAME 40-char recoveryCode from Device 1 (rotates 4->5) and recovers all 4 epochs!
    let config3 = CatbirdMlsClientConfig(
      storageRoot: device3Root.path,
      accountDid: aliceDid,
      deviceId: "dev-multi-3",
      homeDsDid: dsDid,
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://swan.catbird.blue"
    )
    let keystore3 = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.e2e.multi.dev3.\(UUID().uuidString)",
      backend: RecordingKeychainBackend()
    )
    let transport3 = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "https://chat.catbird.blue"),
      boundedSession: boundedSession
    )
    let device3 = try await MainActor.run {
      try MLSClientCoordinator(config: config3, keystore: keystore3, transport: transport3)
    }
    _ = try await device3.enrollDevice(grantedScopes: MLSAppGroupStorage.requiredSpacesScopes)
    let st3 = try await device3.restoreWithRecoveryCode(recoveryCode)
    XCTAssertEqual(st3.epochSeq, 5)

    for cid in ["convo-swift-ep1", "convo-swift-ep2", "convo-swift-ep3", "convo-swift-ep4"] {
      let count = try await device3.restoreConversationFromOwnerMailbox(conversationId: cid)
      XCTAssertEqual(count, 1, "Device 3 must restore \(cid) using original recovery code")
    }
    let ep4MsgsDev3 = try await device3.listMessages(conversationId: "convo-swift-ep4", limit: 10)
    XCTAssertEqual(ep4MsgsDev3.first?.senderDid, aliceDid)
    XCTAssertTrue(ep4MsgsDev3.first?.plaintextJson.contains("Device 2 message at epoch 4") ?? false)

    await device3.close()
  }

  private func waitUntil(
    timeoutSeconds: TimeInterval,
    condition: () async throws -> Bool
  ) async throws {
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while Date() < deadline {
      if try await condition() {
        return
      }
      try await Task.sleep(nanoseconds: 15_000_000)
    }
    let finalCheck = try await condition()
    XCTAssertTrue(finalCheck, "Timed out after \(timeoutSeconds)s waiting for condition")
  }

  private static func injectTwoCharacterTypos(into code: String) -> String {
    var chars = Array(code)
    var alnumSeen = 0
    var substituted = 0
    for idx in chars.indices {
      if chars[idx].isLetter || chars[idx].isNumber {
        alnumSeen += 1
        if alnumSeen > 2 {
          chars[idx] = (chars[idx] == "A") ? "C" : "A"
          substituted += 1
          if substituted == 2 {
            break
          }
        }
      }
    }
    return String(chars)
  }
}

@MainActor
private final class EventRecorder {
  var events: [PlatformChangeEvent] = []
}
