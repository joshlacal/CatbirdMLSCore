import CatbirdMLS
@testable import CatbirdMLSCore
import Foundation
import XCTest

private final class SlowMockURLProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var latencyMs: Int = 25
  private static let callbackQueue = DispatchQueue(
    label: "blue.catbird.mls.test.urlprotocol",
    qos: .userInitiated,
    attributes: .concurrent
  )
  private static let statsLock = NSLock()
  nonisolated(unsafe) private(set) static var handledCount: Int = 0
  nonisolated(unsafe) private(set) static var cancelledCount: Int = 0
  nonisolated(unsafe) private(set) static var sawMainThreadStart: Bool = false

  private let cancelLock = NSLock()
  private var isCancelled = false

  static func resetStats(latencyMs: Int = 25) {
    statsLock.lock()
    Self.latencyMs = latencyMs
    Self.handledCount = 0
    Self.cancelledCount = 0
    Self.sawMainThreadStart = false
    statsLock.unlock()
  }

  override class func canInit(with request: URLRequest) -> Bool {
    true
  }

  override class func canonicalRequest(for request: URLRequest) -> URLRequest {
    request
  }

  override func startLoading() {
    let onMain = Thread.isMainThread
    Self.statsLock.lock()
    Self.handledCount += 1
    if onMain {
      Self.sawMainThreadStart = true
    }
    let delayMs = Self.latencyMs
    Self.statsLock.unlock()

    guard let url = request.url else {
      client?.urlProtocol(self, didFailWithError: URLError(.badURL))
      return
    }

    let path = url.path
    let delaySeconds = path.contains("hang") ? 5.0 : Double(delayMs) / 1000.0

    Self.callbackQueue.asyncAfter(deadline: .now() + delaySeconds) { [weak self] in
      guard let self else { return }
      self.cancelLock.lock()
      let alreadyCancelled = self.isCancelled
      self.cancelLock.unlock()
      guard !alreadyCancelled else { return }

      if path.contains("redirect") {
        let redirectURL = URL(string: "https://evil.example.com/steal")!
        let redirectResp = HTTPURLResponse(
          url: url,
          statusCode: 302,
          httpVersion: "HTTP/1.1",
          headerFields: ["Location": redirectURL.absoluteString]
        )!
        var nextReq = URLRequest(url: redirectURL)
        nextReq.httpMethod = "GET"
        self.client?.urlProtocol(self, wasRedirectedTo: nextReq, redirectResponse: redirectResp)
        self.client?.urlProtocol(self, didReceive: redirectResp, cacheStoragePolicy: .notAllowed)
        self.client?.urlProtocolDidFinishLoading(self)
        return
      }

      if path.contains("oversized") {
        let resp = HTTPURLResponse(
          url: url,
          statusCode: 200,
          httpVersion: "HTTP/1.1",
          headerFields: ["Content-Type": "application/octet-stream"]
        )!
        self.client?.urlProtocol(self, didReceive: resp, cacheStoragePolicy: .notAllowed)
        self.client?.urlProtocol(self, didLoad: Data(repeating: 0xAA, count: 4096))
        self.client?.urlProtocolDidFinishLoading(self)
        return
      }

      let body: Data
      if url.host == "plc.directory" {
        let did = String(path.dropFirst())
        let doc = """
        {
          "id": "\(did)",
          "alsoKnownAs": ["at://alice.test"],
          "verificationMethod": [],
          "service": [
            {
              "id": "#atproto_pds",
              "type": "AtprotoPersonalDataServer",
              "serviceEndpoint": "https://93.184.216.34"
            }
          ]
        }
        """
        body = Data(doc.utf8)
      } else if path == "/xrpc/com.atproto.repo.listRecords" {
        let components = URLComponents(url: url, resolvingAgainstBaseURL: false)
        let repo = components?.queryItems?.first(where: { $0.name == "repo" })?.value ?? "did:plc:peer00000000000000000000"
        let keyB64 = Data(repeating: 0x11, count: 32).base64EncodedString()
        let json = """
        {
          "records": [
            {
              "uri": "at://\(repo)/blue.catbird.chat.device/dev1",
              "cid": "bafyreiabc",
              "value": {
                "$type": "blue.catbird.chat.device",
                "mlsSignaturePublicKey": { "$bytes": "\(keyB64)" },
                "algorithm": "ed25519",
                "createdAt": "2026-10-31T00:00:00.000Z"
              }
            }
          ]
        }
        """
        body = Data(json.utf8)
      } else if path == "/xrpc/blue.catbird.chat.getEntries" {
        let json = """
        {
          "entries": [
            {
              "entryId": "e-1",
              "seq": 1,
              "epoch": 0,
              "entryKind": "application",
              "senderDid": "did:plc:peer00000000000000000000",
              "mlsBytes": "\(Data([1, 2, 3, 4]).base64EncodedString())"
            }
          ]
        }
        """
        body = Data(json.utf8)
      } else {
        body = Data(#"{"ok":true}"#.utf8)
      }

      let resp = HTTPURLResponse(
        url: url,
        statusCode: 200,
        httpVersion: "HTTP/1.1",
        headerFields: ["Content-Type": "application/json"]
      )!
      self.client?.urlProtocol(self, didReceive: resp, cacheStoragePolicy: .notAllowed)
      self.client?.urlProtocol(self, didLoad: body)
      self.client?.urlProtocolDidFinishLoading(self)
    }
  }

  override func stopLoading() {
    cancelLock.lock()
    let firstCancel = !isCancelled
    isCancelled = true
    cancelLock.unlock()
    if firstCancel {
      Self.statsLock.lock()
      Self.cancelledCount += 1
      Self.statsLock.unlock()
    }
  }
}

final class MLSConcurrencyFaultLineTests: XCTestCase {
  private var tempRoot: URL!

  override func setUpWithError() throws {
    try super.setUpWithError()
    SlowMockURLProtocol.resetStats(latencyMs: 25)
    tempRoot = FileManager.default.temporaryDirectory
      .appendingPathComponent("CatbirdMLSConcurrency-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)
  }

  override func tearDownWithError() throws {
    if let tempRoot {
      let trashRoot = FileManager.default.temporaryDirectory
        .appendingPathComponent("_trash-CatbirdMLSConcurrency", isDirectory: true)
      try? FileManager.default.createDirectory(at: trashRoot, withIntermediateDirectories: true)
      try? FileManager.default.moveItem(
        at: tempRoot,
        to: trashRoot.appendingPathComponent(tempRoot.lastPathComponent)
      )
    }
    tempRoot = nil
    try super.tearDownWithError()
  }

  /// Regression test for Fault Line 4:
  /// Drives 64 simultaneous synchronous `PetrelPlatformTransport` FFI callbacks (`executeHttp`,
  /// `fetchMissingEntries`, and `getAuthorizedDeviceKeys`) across a concurrent GCD FFI queue
  /// while simultaneously launching 32 Swift `async` `MLSPushExtensionProcessor.processAsync`
  /// tasks (`96 >> activeProcessorCount` concurrent operations).
  /// Under the old `Task.detached` + `DispatchSemaphore.wait` transport bridge and cooperative
  /// FFI dispatch, saturating FFI + async tasks starved Swift's cooperative pool and timed out.
  func testFaultLine4CooperativePoolSaturationDoesNotDeadlockSyncTransport() async throws {
    let boundedSession = MLSBoundedURLSession(protocolClasses: [SlowMockURLProtocol.self])
    defer { boundedSession.invalidateAndCancel() }

    let transport = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "http://127.0.0.1:9999"),
      boundedSession: boundedSession,
      allowLoopbackForTesting: true
    )

    let backend = RecordingKeychainBackend()
    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.fl4.nse.\(UUID().uuidString)",
      backend: backend
    )
    let accountDidHash = try MLSAppGroupStorage.accountDidHash(for: "did:plc:fl4nseuser00000000000000")
    let storageRootURL = tempRoot!
    _ = try MLSAppGroupStorage.prepareStorageRoot(storageRootURL)

    let ffiCallerQueue = DispatchQueue(
      label: "blue.catbird.mls.test.ffi-callers",
      qos: .userInitiated,
      attributes: .concurrent
    )

    let started = ContinuousClock.now
    let syncConcurrency = 64
    let asyncPushConcurrency = 32

    try await withThrowingTaskGroup(of: Int.self) { group in
      for idx in 0..<syncConcurrency {
        group.addTask {
          try await withCheckedThrowingContinuation { continuation in
            ffiCallerQueue.async {
              do {
                switch idx % 3 {
                case 0:
                  let resp = try transport.executeHttp(
                    request: PlatformHttpRequest(
                      method: "GET",
                      url: "http://127.0.0.1:9999/xrpc/blue.catbird.chat.getConversationState?conversationId=conv-\(idx)",
                      headers: [PlatformHttpHeader(name: "Accept", value: "application/json")],
                      body: nil
                    )
                  )
                  XCTAssertEqual(resp.status, 200)
                  continuation.resume(returning: Int(resp.status))
                case 1:
                  let bundle = try transport.fetchMissingEntries(
                    conversationId: "conv-\(idx)",
                    afterSeq: 0,
                    chatDpopToken: "cached-dpop-token-\(idx)"
                  )
                  XCTAssertEqual(bundle.entries.count, 1)
                  continuation.resume(returning: bundle.entries.count)
                default:
                  let keys = try transport.getAuthorizedDeviceKeys(
                    did: "did:plc:peer00000000000000000000",
                    chatDpopToken: nil
                  )
                  XCTAssertEqual(keys.count, 1)
                  XCTAssertEqual(keys.first?.deviceId, "dev1")
                  continuation.resume(returning: keys.count)
                }
              } catch {
                continuation.resume(throwing: error)
              }
            }
          }
        }
      }

      for pushIdx in 0..<asyncPushConcurrency {
        group.addTask {
          let preview = await MLSPushExtensionProcessor.processAsync(
            storageRoot: storageRootURL,
            accountDidHash: accountDidHash,
            keystore: keystore,
            transport: transport,
            wake: PushWakePayload(
              kind: "message",
              conversationId: "conv-push-\(pushIdx)",
              generation: 1,
              entrySeq: Int64(pushIdx + 1),
              messageId: "msg-push-\(pushIdx)",
              recipientAccountHash: accountDidHash,
              fetchRequired: true,
              senderDid: "did:plc:peer00000000000000000000",
              inlineCiphertext: nil
            ),
            postDarwinNotification: false
          )
          XCTAssertEqual(preview.messageId, "msg-push-\(pushIdx)")
          return 1
        }
      }

      var completed = 0
      for try await _ in group {
        completed += 1
      }
      XCTAssertEqual(completed, syncConcurrency + asyncPushConcurrency)
    }

    let elapsed = started.duration(to: ContinuousClock.now)
    XCTAssertLessThan(
      elapsed,
      .seconds(5),
      "96 concurrent FFI transport + async push operations must finish rapidly without cooperative pool starvation"
    )
    XCTAssertFalse(SlowMockURLProtocol.sawMainThreadStart)
  }

  /// Verifies that `MLSClientCoordinator` dispatches blocking UniFFI calls onto its dedicated
  /// concurrent GCD queue (`ffiDispatchQueue`), keeping `@MainActor` responsive and avoiding
  /// SQLCipher <-> HTTP lock inversion (`I6`) across 48 concurrent coordinator operations.
  func testFaultLine4CoordinatorConcurrentOperationsKeepMainActorResponsiveAndAvoidLockInversion() async throws {
    let boundedSession = MLSBoundedURLSession(protocolClasses: [SlowMockURLProtocol.self])
    defer { boundedSession.invalidateAndCancel() }

    let recordedOnMain = NSLock()
    nonisolated(unsafe) var sawMainInTransport = false

    let transport = PetrelPlatformTransport(
      homeDsEndpoint: URL(string: "http://127.0.0.1:9999"),
      boundedSession: boundedSession,
      allowLoopbackForTesting: true,
      httpOverride: { req in
        if Thread.isMainThread {
          recordedOnMain.lock()
          sawMainInTransport = true
          recordedOnMain.unlock()
        }
        Thread.sleep(forTimeInterval: 0.015)
        return PlatformHttpResponse(
          status: 200,
          headers: [PlatformHttpHeader(name: "content-type", value: "application/json")],
          body: Data("{}".utf8)
        )
      }
    )

    let backend = RecordingKeychainBackend()
    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.fl4.\(UUID().uuidString)",
      backend: backend
    )
    let accountDid = "did:plc:fl4coordinatoruser000000"
    let storageRootPath = tempRoot.path

    let coordinator = try await MainActor.run {
      try MLSClientCoordinator(
        config: CatbirdMlsClientConfig(
          storageRoot: storageRootPath,
          accountDid: accountDid,
          deviceId: "dev-fl4",
          homeDsDid: "did:web:chat.catbird.blue",
          homeDsEndpoint: "http://127.0.0.1:9999",
          pdsEndpoint: "http://127.0.0.1:9999"
        ),
        keystore: keystore,
        transport: transport
      )
    }

    let groupRow = try await coordinator.createGroup(
      conversationId: "conv-fl4-stress",
      sequencerDid: "did:web:chat.catbird.blue",
      title: "FL4 Stress",
      description: nil
    )
    XCTAssertEqual(groupRow.conversationId, "conv-fl4-stress")

    let heartbeatTask = Task { @MainActor () -> Duration in
      var worstHop: Duration = .zero
      for _ in 0..<8 {
        let t0 = ContinuousClock.now
        try? await Task.sleep(nanoseconds: 10_000_000)
        let hop = t0.duration(to: ContinuousClock.now)
        if hop > worstHop {
          worstHop = hop
        }
      }
      return worstHop
    }

    try await withThrowingTaskGroup(of: Void.self) { group in
      for idx in 0..<48 {
        group.addTask {
          switch idx % 3 {
          case 0:
            _ = try await coordinator.sendMessage(
              conversationId: "conv-fl4-stress",
              messageId: "msg-fl4-\(idx)",
              text: "stress message \(idx)"
            )
          case 1:
            _ = try await coordinator.listMessages(
              conversationId: "conv-fl4-stress",
              beforeSeq: nil,
              limit: 50
            )
          default:
            _ = try await coordinator.listConversations(beforeUpdatedMs: nil, limit: 20)
          }
        }
      }
      try await group.waitForAll()
    }

    let worstMainHop = await heartbeatTask.value
    XCTAssertLessThan(worstMainHop, .seconds(1), "MainActor must remain unblocked during concurrent FFI load")
    XCTAssertFalse(sawMainInTransport, "Transport callbacks must never execute on MainActor/main thread")

    let finalMessages = try await coordinator.listMessages(
      conversationId: "conv-fl4-stress",
      beforeSeq: nil,
      limit: 100
    )
    XCTAssertEqual(finalMessages.count, 16)

    await coordinator.close()
  }

  /// Edge cases for `MLSBoundedURLSession.performSync`: byte-limit rejection, timeout cancellation,
  /// and `3xx` redirect blocking.
  func testFaultLine4PerformSyncEnforcesByteLimitTimeoutAndRedirectBlocking() async throws {
    let boundedSession = MLSBoundedURLSession(protocolClasses: [SlowMockURLProtocol.self])
    defer { boundedSession.invalidateAndCancel() }

    try await Task.detached {
      // 1. Oversized body (> 1024 bytes) throws `.responseTooLarge(limit: 1024)`.
      let oversizedReq = URLRequest(url: URL(string: "https://chat.catbird.blue/oversized")!)
      XCTAssertThrowsError(
        try boundedSession.performSync(for: oversizedReq, maxBytes: 1024, timeoutSeconds: 2.0)
      ) { error in
        XCTAssertEqual(
          error as? MLSBoundedURLSession.BoundedFetchError,
          .responseTooLarge(limit: 1024)
        )
      }

      // 2. Hung endpoint throws `.timedOut` and cancels the underlying URLSessionDataTask.
      let beforeCancels = SlowMockURLProtocol.cancelledCount
      let hangReq = URLRequest(url: URL(string: "https://chat.catbird.blue/hang")!)
      XCTAssertThrowsError(
        try boundedSession.performSync(for: hangReq, maxBytes: 4096, timeoutSeconds: 0.12)
      ) { error in
        guard case .timedOut = error as? MLSBoundedURLSession.BoundedFetchError else {
          XCTFail("Expected BoundedFetchError.timedOut, got \(error)")
          return
        }
      }
      try await Task.sleep(nanoseconds: 50_000_000)
      XCTAssertGreaterThan(SlowMockURLProtocol.cancelledCount, beforeCancels)

      // 3. `302` redirect is blocked rather than followed to `evil.example.com`.
      let redirectReq = URLRequest(url: URL(string: "https://chat.catbird.blue/redirect")!)
      let (_, http) = try boundedSession.performSync(
        for: redirectReq,
        maxBytes: 4096,
        timeoutSeconds: 2.0
      )
      XCTAssertEqual(http.statusCode, 302)
      XCTAssertEqual(http.url?.host, "chat.catbird.blue")
    }.value
  }
}
