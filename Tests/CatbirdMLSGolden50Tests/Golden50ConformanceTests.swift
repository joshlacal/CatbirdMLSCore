import CatbirdMLS
@testable import CatbirdMLSCore
import CryptoKit
import Foundation
import Security
import XCTest

private final class GoldenInMemoryKeychainBackend: KeychainBackend, @unchecked Sendable {
  private let lock = NSLock()
  private var items: [String: [String: Any]] = [:]

  private func key(for dict: [String: Any]) -> String {
    let service = dict[kSecAttrService as String] as? String ?? ""
    let account = dict[kSecAttrAccount as String] as? String ?? ""
    return "\(service)|\(account)"
  }

  func add(_ attributes: [String: Any]) -> OSStatus {
    lock.lock()
    defer { lock.unlock() }
    let k = key(for: attributes)
    if items[k] != nil { return errSecDuplicateItem }
    items[k] = attributes
    return errSecSuccess
  }

  func copyMatching(_ query: [String: Any]) -> (OSStatus, AnyObject?) {
    lock.lock()
    defer { lock.unlock() }
    let k = key(for: query)
    guard let stored = items[k], let data = stored[kSecValueData as String] as? Data else {
      return (errSecItemNotFound, nil)
    }
    return (errSecSuccess, data as NSData)
  }

  func update(_ query: [String: Any], _ attributesToUpdate: [String: Any]) -> OSStatus {
    lock.lock()
    defer { lock.unlock() }
    let k = key(for: query)
    guard var stored = items[k] else { return errSecItemNotFound }
    for (attr, value) in attributesToUpdate {
      stored[attr] = value
    }
    items[k] = stored
    return errSecSuccess
  }

  func delete(_ query: [String: Any]) -> OSStatus {
    lock.lock()
    defer { lock.unlock() }
    if query[kSecAttrAccount as String] is String {
      return items.removeValue(forKey: key(for: query)) != nil ? errSecSuccess : errSecItemNotFound
    }
    let service = query[kSecAttrService as String] as? String ?? ""
    let matching = items.keys.filter { $0.hasPrefix("\(service)|") }
    guard !matching.isEmpty else { return errSecItemNotFound }
    for k in matching { items.removeValue(forKey: k) }
    return errSecSuccess
  }
}

private final class GoldenNoopTransport: PlatformTransport, @unchecked Sendable {
  func executeHttp(request: PlatformHttpRequest) throws -> PlatformHttpResponse {
    PlatformHttpResponse(status: 200, headers: [], body: Data("{}".utf8))
  }
  func connectRealtimeStream(homeDsEndpoint: String, cursorSeq: Int64?) throws {}
  func disconnectRealtimeStream() throws {}
}

/// `T7-GOLDEN-50` Swift / `CatbirdMLSCore` + `CatbirdMLSFFI` Conformance Consumer (`C5`).
///
/// Loads all 50 deterministic multi-platform golden transcripts from `catbird-sim/fixtures/golden_50/`,
/// verifies `MANIFEST.sha256` SHA-256 digests byte-for-byte, validates all 10 Wave 2 protocol categories
/// (`5` transcripts each = `50` total), runs `CatbirdMLSCore`'s real outbound HTTP/NSID/SSRF/own-PDS
/// validators (`MLSOutboundHTTPValidator`) and `MLSAppGroupStorage` / `CatbirdMLSFFI` identity hashing
/// over every client wire frame and DID across all 50 transcripts, and exercises live SQLCipher +
/// `MLSClientCoordinator` open/prepareForBackground/signOutAndWipe against canonical transcript actors.
final class Golden50ConformanceTests: XCTestCase {
  static let expectedTranscriptCount = 50
  static let expectedSchemaVersion = "catbird.golden-transcript.v1"

  enum GoldenCategory: String, CaseIterable, Codable, Sendable {
    case direct1To1 = "direct1_to1"
    case groupChat = "group_chat"
    case multidevice1To4Leaves = "multidevice1_to4_leaves"
    case epochRatchets = "epoch_ratchets"
    case federatedDsRelayReset = "federated_ds_relay_reset"
    case y7PruningSpaceBackfill = "y7_pruning_space_backfill"
    case d2DeviceApproval = "d2_device_approval"
    case d3Deletion = "d3_deletion"
    case d4PqOtcReindex = "d4_pq_otc_reindex"
    case y8ReadState = "y8_read_state"
  }

  struct GoldenCanonicalStep: Decodable, Sendable {
    let stepIndex: UInt32
    let actorLabel: String
    let operation: String

    enum CodingKeys: String, CodingKey {
      case stepIndex = "step_index"
      case actorLabel = "actor_label"
      case operation
    }
  }

  struct GoldenWireFrame: Decodable, Sendable {
    let seq: UInt64
    let deviceId: String
    let method: String
    let url: String
    let status: UInt16
    let requestBodyBlake3: String
    let responseBodyBlake3: String
    let space: String?
    let error: String?

    enum CodingKeys: String, CodingKey {
      case seq
      case deviceId = "device_id"
      case method
      case url
      case status
      case requestBodyBlake3 = "request_body_blake3"
      case responseBodyBlake3 = "response_body_blake3"
      case space
      case error
    }
  }

  struct GoldenMessageDigest: Decodable, Sendable {
    let messageId: String
    let seq: Int64
    let epoch: Int64
    let senderDid: String
    let plaintext: String

    enum CodingKeys: String, CodingKey {
      case messageId = "message_id"
      case seq
      case epoch
      case senderDid = "sender_did"
      case plaintext
    }
  }

  struct GoldenConversationDigest: Decodable, Sendable {
    let conversationId: String
    let isDirect: Bool
    let epoch: UInt64
    let treeHashHex: String
    let roster: [String]
    let activeLeafCount: Int
    let messages: [GoldenMessageDigest]
    let readThroughSeq: UInt64
    let prunedApplicationRows: Int

    enum CodingKeys: String, CodingKey {
      case conversationId = "conversation_id"
      case isDirect = "is_direct"
      case epoch
      case treeHashHex = "tree_hash_hex"
      case roster
      case activeLeafCount = "active_leaf_count"
      case messages
      case readThroughSeq = "read_through_seq"
      case prunedApplicationRows = "pruned_application_rows"
    }
  }

  struct GoldenDeviceDigest: Decodable, Sendable {
    let label: String
    let did: String
    let deviceId: String
    let homeDsDid: String
    let handleOpen: Bool
    let pendingApproval: Bool
    let conversationEpochs: [String: UInt64]
    let readWatermarks: [String: UInt64]

    enum CodingKeys: String, CodingKey {
      case label
      case did
      case deviceId = "device_id"
      case homeDsDid = "home_ds_did"
      case handleOpen = "handle_open"
      case pendingApproval = "pending_approval"
      case conversationEpochs = "conversation_epochs"
      case readWatermarks = "read_watermarks"
    }
  }

  struct GoldenTranscript: Decodable, Sendable {
    let schemaVersion: String
    let index: UInt8
    let slug: String
    let category: GoldenCategory
    let federatedTwoDs: Bool
    let seed: UInt64
    let description: String
    let canonicalSteps: [GoldenCanonicalStep]
    let wireFrames: [GoldenWireFrame]
    let wireDigestBlake3: String
    let conversations: [GoldenConversationDigest]
    let devices: [GoldenDeviceDigest]
    let postStateBlake3: String

    enum CodingKeys: String, CodingKey {
      case schemaVersion = "schema_version"
      case index
      case slug
      case category
      case federatedTwoDs = "federated_two_ds"
      case seed
      case description
      case canonicalSteps = "canonical_steps"
      case wireFrames = "wire_frames"
      case wireDigestBlake3 = "wire_digest_blake3"
      case conversations
      case devices
      case postStateBlake3 = "post_state_blake3"
    }
  }

  private func resolveGolden50Directory() throws -> URL {
    let packageRoot = URL(fileURLWithPath: #filePath)
      .deletingLastPathComponent() // CatbirdMLSGolden50Tests
      .deletingLastPathComponent() // Tests
      .deletingLastPathComponent() // CatbirdMLSCore
    let siblingRoot = packageRoot.deletingLastPathComponent()

    var candidates: [URL] = []
    if let explicit = ProcessInfo.processInfo.environment["CATBIRD_GOLDEN_50_DIR"], !explicit.isEmpty {
      candidates.append(URL(fileURLWithPath: explicit, isDirectory: true))
    }
    candidates.append(siblingRoot.appendingPathComponent("catbird-sim/fixtures/golden_50", isDirectory: true))

    for candidate in candidates {
      let standardized = candidate.standardizedFileURL
      let manifest = standardized.appendingPathComponent("MANIFEST.sha256")
      if FileManager.default.fileExists(atPath: manifest.path) {
        return standardized
      }
    }
    throw XCTSkip("golden_50 fixture directory not found in workspace; set CATBIRD_GOLDEN_50_DIR")
  }

  private static func sha256Hex(_ data: Data) -> String {
    SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
  }

  private static func isHex64(_ value: String) -> Bool {
    value.utf8.count == 64 && value.utf8.allSatisfy {
      ($0 >= 0x30 && $0 <= 0x39) || ($0 >= 0x61 && $0 <= 0x66)
    }
  }

  private static func isCanonicalLowercaseUUID(_ value: String) -> Bool {
    guard UUID(uuidString: value) != nil else { return false }
    return value == value.lowercased() && value.count == 36
  }

  func testGolden50ManifestAndAllTenProtocolCategoriesThroughCatbirdMLSCore() async throws {
    let goldenDir = try resolveGolden50Directory()
    let manifestURL = goldenDir.appendingPathComponent("MANIFEST.sha256")
    let manifestText = try String(contentsOf: manifestURL, encoding: .utf8)
    let manifestLines = manifestText
      .split(whereSeparator: \.isNewline)
      .map { String($0).trimmingCharacters(in: .whitespaces) }
      .filter { !$0.isEmpty }

    XCTAssertEqual(
      manifestLines.count,
      Self.expectedTranscriptCount,
      "MANIFEST.sha256 must contain exactly 50 golden transcript entries"
    )

    let decoder = JSONDecoder()
    var transcripts: [GoldenTranscript] = []
    transcripts.reserveCapacity(Self.expectedTranscriptCount)

    var countsByCategory: [GoldenCategory: Int] = [:]
    var totalValidatedClientXrpcFrames = 0
    var uniqueActorDids: Set<String> = []
    let ownPdsOrigins = [try XCTUnwrap(URL(string: "https://pds.catbird.blue"))]

    for (zeroBasedIdx, line) in manifestLines.enumerated() {
      let expectedIndex = UInt8(zeroBasedIdx + 1)
      let parts = line.split(separator: " ", maxSplits: 1, omittingEmptySubsequences: true)
      XCTAssertEqual(parts.count, 2, "Malformed MANIFEST.sha256 entry at line \(expectedIndex)")
      let expectedSha256 = String(parts[0])
      let filename = String(parts[1]).trimmingCharacters(in: .whitespaces)
      XCTAssertTrue(Self.isHex64(expectedSha256), "Invalid SHA-256 hex in MANIFEST.sha256 for \(filename)")
      XCTAssertTrue(
        filename.hasPrefix(String(format: "transcript_%02d_", expectedIndex)) && filename.hasSuffix(".json"),
        "Unexpected transcript filename ordering: \(filename)"
      )

      let fileURL = goldenDir.appendingPathComponent(filename)
      let rawBytes = try Data(contentsOf: fileURL)
      XCTAssertEqual(
        Self.sha256Hex(rawBytes),
        expectedSha256,
        "SHA-256 mismatch against MANIFEST.sha256 for \(filename)"
      )

      let transcript = try decoder.decode(GoldenTranscript.self, from: rawBytes)
      XCTAssertEqual(transcript.schemaVersion, Self.expectedSchemaVersion, filename)
      XCTAssertEqual(transcript.index, expectedIndex, filename)
      XCTAssertFalse(transcript.slug.isEmpty, filename)
      XCTAssertFalse(transcript.description.isEmpty, filename)
      XCTAssertFalse(transcript.canonicalSteps.isEmpty, filename)
      XCTAssertFalse(transcript.wireFrames.isEmpty, filename)
      XCTAssertFalse(transcript.devices.isEmpty, filename)
      XCTAssertTrue(Self.isHex64(transcript.wireDigestBlake3), filename)
      XCTAssertTrue(Self.isHex64(transcript.postStateBlake3), filename)

      countsByCategory[transcript.category, default: 0] += 1

      // FC-08: Zero automated external_commit / force_rejoin operations in any golden transcript.
      for step in transcript.canonicalSteps {
        let lowerOp = step.operation.lowercased()
        XCTAssertFalse(
          lowerOp.contains("external_commit") || lowerOp.contains("force_rejoin"),
          "FC-08 violation in \(filename) step \(step.stepIndex): \(step.operation)"
        )
      }

      // Exercise CatbirdMLSCore + FFI canonical DID normalization & hashing on every device DID.
      var leavesByDid: [String: Int] = [:]
      for device in transcript.devices {
        let canonicalDid = try MLSAppGroupStorage.canonicalizeAccountDid(device.did)
        XCTAssertEqual(canonicalDid, device.did, "Transcript DID must already be canonical in \(filename)")
        let mixedCaseDid = device.did.uppercased()
        XCTAssertEqual(
          try MLSAppGroupStorage.accountDidHash(for: device.did),
          try MLSAppGroupStorage.accountDidHash(for: "  \(mixedCaseDid)#atproto  "),
          "FFI accountDidHash must be case/fragment canonical for \(device.did)"
        )
        XCTAssertTrue(
          Self.isCanonicalLowercaseUUID(device.deviceId),
          "Device ID must be canonical lowercase UUID in \(filename): \(device.deviceId)"
        )
        uniqueActorDids.insert(canonicalDid)
        if device.handleOpen && !device.pendingApproval {
          leavesByDid[canonicalDid, default: 0] += 1
        }
      }

      // Multi-device per-DID leaf cap (<= 4 active approved leaves per DID).
      for (did, activeCount) in leavesByDid {
        XCTAssertLessThanOrEqual(
          activeCount,
          4,
          "Active approved leaf count exceeded 4 for \(did) in \(filename)"
        )
      }

      // Conversation digest invariants + MLSAppGroupStorage / UUID / tree-hash checks.
      for convo in transcript.conversations {
        XCTAssertTrue(
          Self.isCanonicalLowercaseUUID(convo.conversationId),
          "Conversation ID must be canonical lowercase UUID in \(filename): \(convo.conversationId)"
        )
        if convo.activeLeafCount == 0 {
          XCTAssertTrue(
            convo.treeHashHex.isEmpty,
            "Closed/tombstoned conversation with 0 active leaves must purge ratchet tree_hash_hex in \(filename)"
          )
        } else {
          XCTAssertTrue(
            Self.isHex64(convo.treeHashHex),
            "Conversation tree_hash_hex must be 64-char lowercase hex in \(filename)"
          )
        }
        for memberDid in convo.roster {
          XCTAssertEqual(try MLSAppGroupStorage.canonicalizeAccountDid(memberDid), memberDid)
        }
        for msg in convo.messages {
          XCTAssertTrue(Self.isCanonicalLowercaseUUID(msg.messageId), filename)
          XCTAssertEqual(try MLSAppGroupStorage.canonicalizeAccountDid(msg.senderDid), msg.senderDid)
        }
      }

      // Exercise CatbirdMLSCore's real MLSOutboundHTTPValidator over every client XRPC wire frame.
      let knownClientDeviceLabels = Set(transcript.devices.map(\.label))
      for frame in transcript.wireFrames {
        let frameURL = try XCTUnwrap(URL(string: frame.url), "Malformed URL in \(filename): \(frame.url)")
        var originComponents = URLComponents()
        originComponents.scheme = frameURL.scheme
        originComponents.host = frameURL.host
        originComponents.port = frameURL.port
        let originURL = try XCTUnwrap(originComponents.url)
        try MLSOutboundHTTPValidator.validatePublicHTTPSServiceEndpoint(originURL)

        guard knownClientDeviceLabels.contains(frame.deviceId),
              let xrpcRange = frameURL.path.range(of: "/xrpc/")
        else {
          continue
        }
        let nsid = String(frameURL.path[xrpcRange.upperBound...])
        if nsid.hasPrefix("blue.catbird.mlsDS.") {
          continue
        }
        if nsid == MLSOutboundHTTPValidator.deleteRecordNSID {
          // Golden wire frames store BLAKE3 digests rather than raw request bodies; verify the
          // validator's mailboxKeys swapRecord contract with a synthetic D-3 body for this actor.
          let actorDid = transcript.devices.first(where: { $0.label == frame.deviceId })?.did
            ?? "did:plc:alicesimuseraaaaaaaaaaaa"
          let syntheticBody = try JSONSerialization.data(withJSONObject: [
            "repo": actorDid,
            "collection": "blue.catbird.labyrinth.mailboxKeys",
            "rkey": "self",
            "swapRecord": "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          ])
          XCTAssertEqual(
            try MLSOutboundHTTPValidator.mailboxKeysDeletionRepo(method: frame.method, body: syntheticBody),
            actorDid
          )
        } else {
          XCTAssertNoThrow(
            try MLSOutboundHTTPValidator.validatePreparedRequestNSID(
              nsid,
              method: frame.method,
              requestURL: frameURL,
              ownPDSOrigins: ownPdsOrigins
            ),
            "CatbirdMLSCore MLSOutboundHTTPValidator rejected client wire frame \(frame.method) \(nsid) in \(filename)"
          )
        }
        totalValidatedClientXrpcFrames += 1
      }

      transcripts.append(transcript)
    }

    XCTAssertGreaterThan(
      totalValidatedClientXrpcFrames,
      1_000,
      "Expected >1,000 client XRPC wire frames across all 50 transcripts to pass MLSOutboundHTTPValidator"
    )
    XCTAssertEqual(countsByCategory.count, GoldenCategory.allCases.count)
    for category in GoldenCategory.allCases {
      XCTAssertEqual(
        countsByCategory[category],
        5,
        "Category \(category.rawValue) must contain exactly 5 golden transcripts"
      )
    }

    // Category-specific protocol assertions across the 50 transcripts:
    // 1. Transcript 05: Space-backed attachment upload (`com.atproto.space.uploadBlob`) + MLSMessagePayload round-trip.
    let t05 = transcripts[4]
    XCTAssertEqual(t05.slug, "direct_1to1_attachment_space_locator")
    XCTAssertTrue(
      t05.wireFrames.contains { $0.url.contains("/xrpc/com.atproto.space.uploadBlob") && $0.status == 200 },
      "Transcript 05 must exercise Space-scoped com.atproto.space.uploadBlob"
    )
    let attMessage = try XCTUnwrap(t05.conversations.first?.messages.last)
    XCTAssertTrue(attMessage.plaintext.hasPrefix("dm05-att:bafkrei"))
    let payload = MLSMessagePayload.text(attMessage.plaintext)
    let encodedPayload = try payload.encodeToJSON()
    let decodedPayload = try MLSMessagePayload.decodeFromJSON(encodedPayload)
    XCTAssertEqual(decodedPayload.messageType, .text)
    XCTAssertEqual(decodedPayload.text, attMessage.plaintext)

    // 2. Multi-device transcripts 11..15: verify 4-leaf fanout in #14 and clean retirement shrink in #15.
    let t14 = transcripts[13]
    XCTAssertEqual(t14.category, .multidevice1To4Leaves)
    XCTAssertGreaterThanOrEqual(t14.conversations.first?.activeLeafCount ?? 0, 4)
    let t15 = transcripts[14]
    XCTAssertTrue(t15.canonicalSteps.contains { $0.operation == "retire_current_device" })

    // 3. Federated DS transcripts 21..25: two-DS flag + cross-DS relay frames (`ds-b.catbird.blue`).
    for federated in transcripts[20..<25] {
      XCTAssertTrue(federated.federatedTwoDs, federated.slug)
      XCTAssertTrue(
        federated.wireFrames.contains { $0.url.contains("https://ds-b.catbird.blue/") },
        "Federated transcript \(federated.slug) must traverse second DS"
      )
    }

    // 4. Y-7 pruning transcripts 26..30: application payload pruning / dormant Space backfill.
    let prunedCounts = transcripts[25..<30].flatMap(\.conversations).map(\.prunedApplicationRows)
    XCTAssertTrue(prunedCounts.contains { $0 > 0 }, "Y-7 transcripts must record pruned application rows")

    // 5. D-2 device approval transcripts 31..35: approve, self-promote, revoke, and tampered CARv1 refusal.
    let d2Ops = Set(transcripts[30..<35].flatMap(\.canonicalSteps).map(\.operation))
    XCTAssertTrue(d2Ops.contains("approve_device"))
    XCTAssertTrue(d2Ops.contains("revoke_device"))
    XCTAssertTrue(d2Ops.contains("approve_device_tampered_carv1_refused"))

    // 6. D-3 deletion transcripts 36..40: close_conversation, delete_account_data, retire_current_device.
    let d3Ops = Set(transcripts[35..<40].flatMap(\.canonicalSteps).map(\.operation))
    XCTAssertTrue(d3Ops.contains("close_conversation"))
    XCTAssertTrue(d3Ops.contains("delete_account_data"))
    XCTAssertTrue(d3Ops.contains("retire_current_device"))

    // 7. D-4 PQ OTC / Passkey PRF / verify_and_reindex_history transcripts 41..45.
    let d4Ops = Set(transcripts[40..<45].flatMap(\.canonicalSteps).map(\.operation))
    XCTAssertTrue(d4Ops.contains("complete_otc_pairing"))
    XCTAssertTrue(d4Ops.contains("verify_and_reindex_history"))
    XCTAssertTrue(d4Ops.contains("register_passkey_prf"))
    XCTAssertTrue(d4Ops.contains("restore_with_passkey_prf"))

    // 8. Y-8 readState transcripts 46..50: sibling read_watermarks converge monotonically.
    for y8 in transcripts[45..<50] {
      let openDevicesByDid = Dictionary(
        grouping: y8.devices.filter { $0.handleOpen && !$0.pendingApproval },
        by: \.did
      )
      for (_, siblings) in openDevicesByDid where siblings.count >= 2 {
        let firstWatermarks = siblings[0].readWatermarks
        for sibling in siblings.dropFirst() {
          XCTAssertEqual(
            sibling.readWatermarks,
            firstWatermarks,
            "Y-8 sibling read_watermarks diverged in \(y8.slug)"
          )
        }
      }
    }

    // 9. Live CatbirdMLSFFI + MLSClientCoordinator SQLCipher smoke using transcript_01's canonical actor.
    let primaryDevice = try XCTUnwrap(transcripts[0].devices.first)
    let tempRoot = FileManager.default.temporaryDirectory
      .appendingPathComponent("Golden50SwiftFFI-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)
    defer {
      let trashRoot = FileManager.default.temporaryDirectory
        .appendingPathComponent("_trash-Golden50SwiftFFI", isDirectory: true)
      try? FileManager.default.createDirectory(at: trashRoot, withIntermediateDirectories: true)
      try? FileManager.default.moveItem(
        at: tempRoot,
        to: trashRoot.appendingPathComponent(tempRoot.lastPathComponent)
      )
    }

    let keystore = KeychainPlatformKeyStore(
      service: "blue.catbird.mls.golden50.\(UUID().uuidString)",
      backend: GoldenInMemoryKeychainBackend()
    )
    let config = CatbirdMlsClientConfig(
      storageRoot: tempRoot.path,
      accountDid: primaryDevice.did,
      deviceId: primaryDevice.deviceId,
      homeDsDid: primaryDevice.homeDsDid,
      homeDsEndpoint: "https://chat.catbird.blue",
      pdsEndpoint: "https://pds.catbird.blue"
    )
    let coordinator = try await MLSClientCoordinator.open(
      config: config,
      keystore: keystore,
      transport: GoldenNoopTransport()
    )
    let dbURL = try MLSAppGroupStorage.databaseURL(storageRoot: tempRoot, accountDid: primaryDevice.did)
    XCTAssertTrue(FileManager.default.fileExists(atPath: dbURL.path))
    let initialConversations = try await coordinator.listConversations()
    XCTAssertTrue(initialConversations.isEmpty)
    _ = try await coordinator.prepareForBackground()
    try await coordinator.resumeFromBackground()
    try await coordinator.signOutAndWipe()
    XCTAssertFalse(FileManager.default.fileExists(atPath: dbURL.path))
    XCTAssertGreaterThanOrEqual(uniqueActorDids.count, 3)
  }
}
