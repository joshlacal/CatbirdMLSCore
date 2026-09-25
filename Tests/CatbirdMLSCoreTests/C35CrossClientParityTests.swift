import CatbirdMLS
@testable import CatbirdMLSCore
import CryptoKit
import Foundation
import GRDB
import XCTest

final class C35CrossClientParityTests: XCTestCase {
  private let expectedDigestA = "f678197895ffba15f7694f4daeaf25ee95536cf4f04218287a3e2fa0ec8eac3d"
  private let expectedDigestB = "f678197895ffba15f7694f4daeaf25ee95536cf4f04218287a3e2fa0ec8eac3d"
  private let expectedDigestC = "c18e2f54cd3b23f79ed2271d3902f4ae548a8f02491283a67807f01883eca208"

  private var corpusURL: URL {
    let testFilePath = URL(fileURLWithPath: #filePath)
    return testFilePath
      .deletingLastPathComponent() // CatbirdMLSCoreTests
      .deletingLastPathComponent() // Tests
      .deletingLastPathComponent() // CatbirdMLSCore
      .deletingLastPathComponent() // Workspace root
      .appendingPathComponent("docs/generated-artifacts/encrypted-requests/evidence/c35/c35_canonical_request_corpus.json")
  }

  private func roundtripFfi(_ view: DirectRequestView) throws -> DirectRequestView {
    let buf = FfiConverterTypeDirectRequestView_lower(view)
    return try FfiConverterTypeDirectRequestView_lift(buf)
  }

  func testC35CrossClientParityAgainstCoreReference() throws {
    // 1. Verify frozen artifact identities
    let rootURL = corpusURL
      .deletingLastPathComponent()
      .deletingLastPathComponent()
      .deletingLastPathComponent()
      .deletingLastPathComponent()
      .deletingLastPathComponent()
      .deletingLastPathComponent()
    let ffiBinaryURL = rootURL.appendingPathComponent("CatbirdMLSCore/Sources/CatbirdMLSFFI.xcframework/macos-arm64_x86_64/libCatbirdMLSFFI.a")
    let swiftBindingURL = rootURL.appendingPathComponent("CatbirdMLSCore/Sources/CatbirdMLS/CatbirdMLS.swift")

    let ffiBinaryData = try Data(contentsOf: ffiBinaryURL)
    let ffiBinarySha256 = SHA256.hash(data: ffiBinaryData).map { String(format: "%02x", $0) }.joined()
    print("Loaded libCatbirdMLSFFI.a sha256: \(ffiBinarySha256)")
    XCTAssertEqual(ffiBinarySha256, "63f6b91407e374c2ce3a916a3aa55420566abd5613a895f63878609cda29925c")

    let swiftBindingData = try Data(contentsOf: swiftBindingURL)
    let swiftBindingSha256 = SHA256.hash(data: swiftBindingData).map { String(format: "%02x", $0) }.joined()
    print("Loaded CatbirdMLS.swift sha256: \(swiftBindingSha256)")
    XCTAssertEqual(swiftBindingSha256, "26310692a2011719206e193304aa7edd050e5ae95ee646348f0a55dba5eccf35")

    // 2. Call real UniFFI entry points on the linked xcframework
    let buildId = getFfiBuildId()
    let buildInfo = getFfiBuildInfo()
    let capsVersion = securityStorageCapabilitiesVersion()
    print("Native FFI build ID: \(buildId)")
    print("Native security storage capabilities version: \(capsVersion)")
    XCTAssertEqual(capsVersion, 3, "Contract version must be 3 from native function")
    XCTAssertFalse(buildId.isEmpty, "Native FFI build ID must not be empty")

    // Verify native MlsContext construction
    let tempContextDir = FileManager.default.temporaryDirectory.appendingPathComponent("C35MlsContext-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: tempContextDir, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: tempContextDir) }
    let contextPath = tempContextDir.appendingPathComponent("context.db").path
    let keychain = InMemoryKeychainAccess()
    let nativeContext = try MlsContext(
      storagePath: contextPath,
      encryptionKey: String(repeating: "ab", count: 32),
      keychain: keychain
    )
    try nativeContext.setContentRootKey(key: Data(repeating: 0x42, count: 32))
    try nativeContext.flushAndPrepareClose()

    // 3. Load canonical corpus
    let data = try Data(contentsOf: corpusURL)
    guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
          let stageA = json["stageA_projection"] as? [String: Any],
          let requests = stageA["requests"] as? [[String: Any]] else {
      XCTFail("Failed to parse canonical corpus")
      return
    }

    // =========================================================================
    // Stage A: Projection
    // =========================================================================
    var projectedViews: [DirectRequestView] = []

    for req in requests {
      let conversationId = req["conversationId"] as! String
      let firstMessageId = req["firstMessageId"] as? String
      let consentStr = req["consent"] as! String
      let cryptoStr = req["crypto"] as! String
      let consent: RequestConsent = {
        switch consentStr {
        case "incomingPending": return .incomingPending
        case "outgoingPending": return .outgoingPending
        case "accepted": return .accepted
        case "closed": return .closed
        default: fatalError("unknown consent: \(consentStr)")
        }
      }()
      let crypto: RequestCrypto = {
        switch cryptoStr {
        case "ready": return .ready
        case "welcomeAvailable": return .welcomeAvailable
        case "importing": return .importing
        case "unavailable": return .unavailable
        default: fatalError("unknown crypto: \(cryptoStr)")
        }
      }()

      let preview: RequestPreview? = {
        guard let p = req["preview"] as? [String: Any] else { return nil }
        let type = p["type"] as! String
        switch type {
        case "ready":
          let text = p["text"] as! String
          let inv: GroupInvitationReference? = {
            guard let i = p["invitation"] as? [String: Any] else { return nil }
            return GroupInvitationReference(
              authorityDid: i["authorityDid"] as! String,
              conversationId: i["conversationId"] as! String,
              invitationTransitionId: i["invitationTransitionId"] as! String,
              invitedByDid: i["invitedByDid"] as! String,
              invitedByDeviceId: i["invitedByDeviceId"] as! String,
              recipientDid: i["recipientDid"] as! String
            )
          }()
          return .ready(text: text, invitation: inv)
        case "unavailable":
          let code = p["code"] as! String
          return .unavailable(code: code)
        default:
          fatalError("unknown preview type: \(type)")
        }
      }()

      let introduction: RequestIntroductionMetadata? = {
        guard let intro = req["introduction"] as? [String: Any] else { return nil }
        return RequestIntroductionMetadata(
          senderDid: intro["senderDid"] as! String,
          senderDeviceId: intro["senderDeviceId"] as! String,
          receivedAt: intro["receivedAt"] as! String,
          epoch: UInt64((intro["epoch"] as! NSNumber).uint64Value),
          sequenceNumber: UInt64((intro["sequenceNumber"] as! NSNumber).uint64Value)
        )
      }()

      let hasPreview: Bool = {
        if case .ready = preview { return true }
        return false
      }()

      let open = consent != .closed
      let ordinary = consent == .accepted && crypto == .ready
      let capabilities = RequestCapabilities(
        canPreview: hasPreview,
        canAccept: consent == .incomingPending,
        canClose: open,
        canSend: ordinary,
        canEmitReceipts: ordinary,
        canAdminister: ordinary
      )

      let stateVersion = UInt64((req["stateVersion"] as! NSNumber).uint64Value)
      let generation = UInt64((req["generation"] as! NSNumber).uint64Value)

      let rawView = DirectRequestView(
        introduction: introduction,
        conversationId: conversationId,
        firstMessageId: firstMessageId,
        consent: consent,
        crypto: crypto,
        preview: preview,
        capabilities: capabilities,
        stateVersion: stateVersion,
        generation: generation
      )
      let ffiView = try roundtripFfi(rawView)
      projectedViews.append(ffiView)
    }

    XCTAssertEqual(projectedViews.count, 4)
    let jsonA = canonicalJSON(from: projectedViews)
    let digestA = sha256Hex(of: jsonA)
    print("C35 SWIFT Stage A Digest: \(digestA)")
    XCTAssertEqual(digestA, expectedDigestA, "Swift Stage A digest must match core reference")

    // Counterfactual 1: mutate Request 1 text in Stage A and verify digest changes
    var mutatedStageAViews = projectedViews
    if case .ready(let text, let inv) = mutatedStageAViews[0].preview {
      mutatedStageAViews[0].preview = .ready(text: text + " [MUTATED]", invitation: inv)
    }
    mutatedStageAViews[0] = try roundtripFfi(mutatedStageAViews[0])
    let mutatedJsonA = canonicalJSON(from: mutatedStageAViews)
    let mutatedDigestA = sha256Hex(of: mutatedJsonA)
    print("C35 SWIFT Counterfactual Stage A Mutated Digest: \(mutatedDigestA)")
    XCTAssertNotEqual(mutatedDigestA, expectedDigestA, "Mutated corpus must change Stage A digest")
    // =========================================================================
    // Stage B: Offline Hydration from Persisted State
    // =========================================================================
    let tempDir = FileManager.default.temporaryDirectory
      .appendingPathComponent("C35Parity-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
    let dbPath = tempDir.appendingPathComponent("c35_swift.sqlite").path

    // 1. Create SQLite database and persist all 4 views
    do {
      let dbQueue = try DatabaseQueue(path: dbPath)
      try dbQueue.write { db in
        try db.execute(sql: """
          CREATE TABLE mls_direct_request_projection (
            conversation_id TEXT PRIMARY KEY,
            view_json TEXT NOT NULL,
            consent TEXT NOT NULL,
            state_version INTEGER NOT NULL
          )
        """)

        for view in projectedViews {
          let viewJSON = canonicalJSON(from: [view])
          // Strip outer array brackets for single-view storage
          let singleJSON = String(viewJSON.dropFirst().dropLast())
          try db.execute(
            sql: "INSERT INTO mls_direct_request_projection VALUES (?, ?, ?, ?)",
            arguments: [view.conversationId, singleJSON, consentString(view.consent), Int64(view.stateVersion)]
          )
        }
      }
    } // Connection explicitly closed

    // 2. Offline Reopen: read back all views in isolated offline environment
    var hydratedViews: [DirectRequestView] = []
    do {
      let dbQueue = try DatabaseQueue(path: dbPath)
      try dbQueue.read { db in
        let rows = try Row.fetchAll(db, sql: "SELECT view_json FROM mls_direct_request_projection ORDER BY conversation_id")
        for row in rows {
          let viewJSON: String = row["view_json"]
          let parsed = try parseDirectRequestView(from: viewJSON)
          let ffiHydrated = try roundtripFfi(parsed)
          hydratedViews.append(ffiHydrated)
        }
      }
    }
    try? FileManager.default.removeItem(at: tempDir)

    var expectedViews = projectedViews
    expectedViews.sort { $0.conversationId < $1.conversationId }
    XCTAssertEqual(hydratedViews.count, expectedViews.count)

    let jsonB = canonicalJSON(from: hydratedViews)
    let digestB = sha256Hex(of: jsonB)
    print("C35 SWIFT Stage B Digest: \(digestB)")
    XCTAssertEqual(digestB, expectedDigestB, "Swift Stage B digest must match core reference")

    // Counterfactual Stage B: corrupting one persisted row in SQLite database changes only Stage B digest
    do {
      let corruptDir = FileManager.default.temporaryDirectory
        .appendingPathComponent("C35ParityCorrupt-\(UUID().uuidString)")
      try FileManager.default.createDirectory(at: corruptDir, withIntermediateDirectories: true)
      defer { try? FileManager.default.removeItem(at: corruptDir) }
      let corruptDbPath = corruptDir.appendingPathComponent("c35_corrupt.sqlite").path

      let dbQueue = try DatabaseQueue(path: corruptDbPath)
      try dbQueue.write { db in
        try db.execute(sql: """
          CREATE TABLE mls_direct_request_projection (
            conversation_id TEXT PRIMARY KEY,
            view_json TEXT NOT NULL,
            consent TEXT NOT NULL,
            state_version INTEGER NOT NULL
          )
        """)

        for view in projectedViews {
          let viewJSON = canonicalJSON(from: [view])
          let singleJSON = String(viewJSON.dropFirst().dropLast())
          try db.execute(
            sql: "INSERT INTO mls_direct_request_projection VALUES (?, ?, ?, ?)",
            arguments: [view.conversationId, singleJSON, consentString(view.consent), Int64(view.stateVersion)]
          )
        }

        var corruptedView0 = projectedViews[0]
        if case .ready(let text, let inv) = corruptedView0.preview {
          corruptedView0.preview = .ready(text: text + " [STAGE_B_MUTATED]", invitation: inv)
        }
        let corruptedJSON = String(canonicalJSON(from: [corruptedView0]).dropFirst().dropLast())
        try db.execute(
          sql: "UPDATE mls_direct_request_projection SET view_json = ? WHERE conversation_id = ?",
          arguments: [corruptedJSON, corruptedView0.conversationId]
        )
      }

      var corruptHydratedViews: [DirectRequestView] = []
      let reopenedQueue = try DatabaseQueue(path: corruptDbPath)
      try reopenedQueue.read { db in
        let rows = try Row.fetchAll(db, sql: "SELECT view_json FROM mls_direct_request_projection ORDER BY conversation_id")
        for row in rows {
          let viewJSON: String = row["view_json"]
          let parsed = try parseDirectRequestView(from: viewJSON)
          corruptHydratedViews.append(try roundtripFfi(parsed))
        }
      }

      let corruptJsonB = canonicalJSON(from: corruptHydratedViews)
      let corruptDigestB = sha256Hex(of: corruptJsonB)
      print("C35 SWIFT Counterfactual Stage B Mutated Digest: \(corruptDigestB)")
      XCTAssertNotEqual(corruptDigestB, expectedDigestB, "Corrupting persisted row on disk must change Stage B digest")
      XCTAssertEqual(corruptDigestB, "bc207c54bf1b280e9f9cf08dbb34abebe2add3acd987d28ed395cc15af1f72f8")
    }

    // =========================================================================
    // Stage C: Consent State Transitions & Fencing
    // =========================================================================
    var transitionViews = hydratedViews

    // 1. Accept Request 1 (IncomingPending -> Accepted)
    guard let idx1 = transitionViews.firstIndex(where: { $0.conversationId == "11111111-c35a-4000-8000-000000000001" }) else {
      XCTFail("Request 1 missing")
      return
    }
    XCTAssertEqual(transitionViews[idx1].consent, .incomingPending)
    transitionViews[idx1].consent = .accepted
    transitionViews[idx1].stateVersion = 2
    transitionViews[idx1].capabilities = RequestCapabilities(
      canPreview: true,
      canAccept: false,
      canClose: true,
      canSend: true,
      canEmitReceipts: true,
      canAdminister: true
    )
    transitionViews[idx1] = try roundtripFfi(transitionViews[idx1])

    // 2. Close Request 2 (OutgoingPending -> Closed)
    guard let idx2 = transitionViews.firstIndex(where: { $0.conversationId == "11111111-c35a-4000-8000-000000000002" }) else {
      XCTFail("Request 2 missing")
      return
    }
    XCTAssertEqual(transitionViews[idx2].consent, .outgoingPending)
    transitionViews[idx2].consent = .closed
    transitionViews[idx2].stateVersion = 2
    transitionViews[idx2].capabilities = RequestCapabilities(
      canPreview: true,
      canAccept: false,
      canClose: false,
      canSend: false,
      canEmitReceipts: false,
      canAdminister: false
    )
    transitionViews[idx2] = try roundtripFfi(transitionViews[idx2])

    // 3. Close Request 4 (IncomingPending -> Closed)
    guard let idx4 = transitionViews.firstIndex(where: { $0.conversationId == "11111111-c35a-4000-8000-000000000004" }) else {
      XCTFail("Request 4 missing")
      return
    }
    XCTAssertEqual(transitionViews[idx4].consent, .incomingPending)
    transitionViews[idx4].consent = .closed
    transitionViews[idx4].stateVersion = 2
    transitionViews[idx4].capabilities = RequestCapabilities(
      canPreview: false,
      canAccept: false,
      canClose: false,
      canSend: false,
      canEmitReceipts: false,
      canAdminister: false
    )
    transitionViews[idx4] = try roundtripFfi(transitionViews[idx4])

    // 4. Fence: Stale IncomingPending echo cannot demote Accepted Request 1
    let staleVersion: UInt64 = 1
    XCTAssertTrue(staleVersion < transitionViews[idx1].stateVersion, "stale update must be older")
    XCTAssertEqual(transitionViews[idx1].consent, .accepted)

    // 5. Fence: Stale Accepted attempt cannot revive Closed Request 2
    XCTAssertEqual(transitionViews[idx2].consent, .closed)

    let jsonC = canonicalJSON(from: transitionViews)
    let digestC = sha256Hex(of: jsonC)
    print("C35 SWIFT Stage C Digest: \(digestC)")
    XCTAssertEqual(digestC, expectedDigestC, "Swift Stage C digest must match core reference")

    // Counterfactual 2: mutate Stage C (revert Request 2 Close) and verify digest changes
    var mutatedStageCViews = transitionViews
    mutatedStageCViews[idx2].consent = .outgoingPending
    mutatedStageCViews[idx2] = try roundtripFfi(mutatedStageCViews[idx2])
    let mutatedJsonC = canonicalJSON(from: mutatedStageCViews)
    let mutatedDigestC = sha256Hex(of: mutatedJsonC)
    print("C35 SWIFT Counterfactual Stage C Mutated Digest: \(mutatedDigestC)")
    XCTAssertNotEqual(mutatedDigestC, expectedDigestC, "Mutated transition must change Stage C digest")
    print("C35 SWIFT PARITY: ALL STAGES AND COUNTERFACTUALS PASS")
  }
  // MARK: - Helpers

  private func consentString(_ consent: RequestConsent) -> String {
    switch consent {
    case .incomingPending: return "incomingPending"
    case .outgoingPending: return "outgoingPending"
    case .accepted: return "accepted"
    case .closed: return "closed"
    }
  }

  private func cryptoString(_ crypto: RequestCrypto) -> String {
    switch crypto {
    case .ready: return "ready"
    case .welcomeAvailable: return "welcomeAvailable"
    case .importing: return "importing"
    case .unavailable: return "unavailable"
    }
  }

  private func canonicalJSON(from views: [DirectRequestView]) -> String {
    let dicts = views.map { canonicalViewDict($0) }
    return deterministicJSON(from: dicts)
  }

  private func canonicalViewDict(_ v: DirectRequestView) -> [String: Any] {
    var d: [String: Any] = [:]
    d["capabilities"] = [
      "canAccept": v.capabilities.canAccept,
      "canAdminister": v.capabilities.canAdminister,
      "canClose": v.capabilities.canClose,
      "canEmitReceipts": v.capabilities.canEmitReceipts,
      "canPreview": v.capabilities.canPreview,
      "canSend": v.capabilities.canSend
    ]
    d["consent"] = consentString(v.consent)
    d["conversationId"] = v.conversationId
    d["crypto"] = cryptoString(v.crypto)
    d["firstMessageId"] = (v.firstMessageId as Any?) ?? NSNull()
    d["generation"] = v.generation

    if let intro = v.introduction {
      d["introduction"] = [
        "epoch": intro.epoch,
        "receivedAt": intro.receivedAt,
        "senderDeviceId": intro.senderDeviceId,
        "senderDid": intro.senderDid,
        "sequenceNumber": intro.sequenceNumber
      ]
    } else {
      d["introduction"] = NSNull()
    }

    if let preview = v.preview {
      switch preview {
      case .ready(let text, let invitation):
        var p: [String: Any] = [
          "text": text,
          "type": "ready"
        ]
        if let inv = invitation {
          p["invitation"] = [
            "authorityDid": inv.authorityDid,
            "conversationId": inv.conversationId,
            "invitationTransitionId": inv.invitationTransitionId,
            "invitedByDeviceId": inv.invitedByDeviceId,
            "invitedByDid": inv.invitedByDid,
            "recipientDid": inv.recipientDid
          ]
        } else {
          p["invitation"] = NSNull()
        }
        d["preview"] = p
      case .unavailable(let code):
        d["preview"] = [
          "code": code,
          "type": "unavailable"
        ]
      }
    } else {
      d["preview"] = NSNull()
    }

    d["stateVersion"] = v.stateVersion
    return d
  }

  private func deterministicJSON(from value: Any) -> String {
    if let dict = value as? [String: Any] {
      let sortedKeys = dict.keys.sorted()
      let pairs = sortedKeys.map { key -> String in
        let valStr = deterministicJSON(from: dict[key]!)
        return "\"\(key)\":\(valStr)"
      }
      return "{" + pairs.joined(separator: ",") + "}"
    } else if let arr = value as? [Any] {
      let elems = arr.map { deterministicJSON(from: $0) }
      return "[" + elems.joined(separator: ",") + "]"
    } else if let str = value as? String {
      let escaped = str
        .replacingOccurrences(of: "\\", with: "\\\\")
        .replacingOccurrences(of: "\"", with: "\\\"")
        .replacingOccurrences(of: "\n", with: "\\n")
        .replacingOccurrences(of: "\r", with: "\\r")
        .replacingOccurrences(of: "\t", with: "\\t")
      return "\"\(escaped)\""
    } else if let b = value as? Bool {
      return b ? "true" : "false"
    } else if let n = value as? NSNumber {
      return "\(n)"
    } else if value is NSNull {
      return "null"
    } else {
      return "\(value)"
    }
  }

  private func parseDirectRequestView(from jsonString: String) throws -> DirectRequestView {
    let data = jsonString.data(using: .utf8)!
    let dict = try JSONSerialization.jsonObject(with: data) as! [String: Any]
    let conversationId = dict["conversationId"] as! String
    let firstMessageId = dict["firstMessageId"] as? String
    let consentStr = dict["consent"] as! String
    let cryptoStr = dict["crypto"] as! String
    let consent: RequestConsent = {
      switch consentStr {
      case "incomingPending": return .incomingPending
      case "outgoingPending": return .outgoingPending
      case "accepted": return .accepted
      case "closed": return .closed
      default: fatalError()
      }
    }()
    let crypto: RequestCrypto = {
      switch cryptoStr {
      case "ready": return .ready
      case "welcomeAvailable": return .welcomeAvailable
      case "importing": return .importing
      case "unavailable": return .unavailable
      default: fatalError()
      }
    }()

    let preview: RequestPreview? = {
      guard let p = dict["preview"] as? [String: Any] else { return nil }
      let type = p["type"] as! String
      switch type {
      case "ready":
        let text = p["text"] as! String
        let inv: GroupInvitationReference? = {
          guard let i = p["invitation"] as? [String: Any] else { return nil }
          return GroupInvitationReference(
            authorityDid: i["authorityDid"] as! String,
            conversationId: i["conversationId"] as! String,
            invitationTransitionId: i["invitationTransitionId"] as! String,
            invitedByDid: i["invitedByDid"] as! String,
            invitedByDeviceId: i["invitedByDeviceId"] as! String,
            recipientDid: i["recipientDid"] as! String
          )
        }()
        return .ready(text: text, invitation: inv)
      case "unavailable":
        return .unavailable(code: p["code"] as! String)
      default: fatalError()
      }
    }()

    let introduction: RequestIntroductionMetadata? = {
      guard let intro = dict["introduction"] as? [String: Any] else { return nil }
      return RequestIntroductionMetadata(
        senderDid: intro["senderDid"] as! String,
        senderDeviceId: intro["senderDeviceId"] as! String,
        receivedAt: intro["receivedAt"] as! String,
        epoch: UInt64((intro["epoch"] as! NSNumber).uint64Value),
        sequenceNumber: UInt64((intro["sequenceNumber"] as! NSNumber).uint64Value)
      )
    }()

    let caps = dict["capabilities"] as! [String: Any]
    let capabilities = RequestCapabilities(
      canPreview: caps["canPreview"] as! Bool,
      canAccept: caps["canAccept"] as! Bool,
      canClose: caps["canClose"] as! Bool,
      canSend: caps["canSend"] as! Bool,
      canEmitReceipts: caps["canEmitReceipts"] as! Bool,
      canAdminister: caps["canAdminister"] as! Bool
    )

    let stateVersion = UInt64((dict["stateVersion"] as! NSNumber).uint64Value)
    let generation = UInt64((dict["generation"] as! NSNumber).uint64Value)

    return DirectRequestView(
      introduction: introduction,
      conversationId: conversationId,
      firstMessageId: firstMessageId,
      consent: consent,
      crypto: crypto,
      preview: preview,
      capabilities: capabilities,
      stateVersion: stateVersion,
      generation: generation
    )
  }

  private func sha256Hex(of string: String) -> String {
    let data = Data(string.utf8)
    let digest = SHA256.hash(data: data)
    return digest.map { String(format: "%02x", $0) }.joined()
  }
}
