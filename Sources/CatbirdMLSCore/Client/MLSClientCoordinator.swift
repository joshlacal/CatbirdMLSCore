@preconcurrency @_exported import CatbirdMLS
import CoreFoundation
import Foundation
import Observation

extension CatbirdMlsClient: @unchecked Sendable {}
extension ConversationRow: @unchecked Sendable {}
extension MessageRow: @unchecked Sendable {}
extension RequestRow: @unchecked Sendable {}
extension GroupMetadataRow: @unchecked Sendable {}
extension SpaceAvatarPointer: @unchecked Sendable {}
extension SpacesEligibilityInput: @unchecked Sendable {}
extension SpacesEligibilityStatus: @unchecked Sendable {}
extension SpacesUnavailableReason: @unchecked Sendable {}
extension LabyrinthBackupStatus: @unchecked Sendable {}
extension GeneratedRecoveryCode: @unchecked Sendable {}
extension LabyrinthRevocationOutcome: @unchecked Sendable {}
extension OtcPairingOffer: @unchecked Sendable {}
extension OtcPairingCompletion: @unchecked Sendable {}
extension PreCutoverWipeReport: @unchecked Sendable {}
extension BackgroundCheckpointReport: @unchecked Sendable {}
extension NotificationPreview: @unchecked Sendable {}
extension PushWakePayload: @unchecked Sendable {}
extension PushFetchBundle: @unchecked Sendable {}
extension FetchedPushEntry: @unchecked Sendable {}
extension FetchedMetadataSnapshot: @unchecked Sendable {}
extension AuthorizedPeerDeviceKey: @unchecked Sendable {}
extension PlatformHttpHeader: @unchecked Sendable {}
extension PlatformHttpRequest: @unchecked Sendable {}
extension PlatformHttpResponse: @unchecked Sendable {}
extension CatbirdMlsClientConfig: @unchecked Sendable {}
extension RequestCapabilities: @unchecked Sendable {}
extension EnrollmentReceipt: @unchecked Sendable {}
extension MemberRole: @unchecked Sendable {}
extension PlatformChangeEvent: @unchecked Sendable {}
extension QueuedLabyrinthArchivePayload: @unchecked Sendable {}
extension SignedPushGatewayRegistration: @unchecked Sendable {}
extension CircleKeyOpenResult: @unchecked Sendable {}
extension CircleKeyWrapRequest: @unchecked Sendable {}
extension CircleKeyWrapResult: @unchecked Sendable {}
extension CircleOwnerStateDto: @unchecked Sendable {}
extension CirclePostOpenRequest: @unchecked Sendable {}
extension CirclePostSealRequest: @unchecked Sendable {}
extension DeviceRevocationReport: @unchecked Sendable {}
extension LeaveConversationOutcome: @unchecked Sendable {}
extension MailboxReadStateRecord: @unchecked Sendable {}
extension OtcInitiatedSession: @unchecked Sendable {}
extension PeerRepinProposalInfo: @unchecked Sendable {}
extension PendingGroupCreationStatus: @unchecked Sendable {}
extension PendingGroupCreationState: @unchecked Sendable {}
extension RegisteredDeviceInfo: @unchecked Sendable {}
extension UploadedConversationAttachment: @unchecked Sendable {}
extension VoicePrepareResult: @unchecked Sendable {}

public enum MLSAppGroupStorage {
  public static let appGroupIdentifier = "group.blue.catbird.shared"
  public static let nseDidCommitDarwinNotification = "blue.catbird.mls.nseDidCommit"
  public static let requiredSpacesScopes: [String] = [
    "atproto",
    "blob:*/*",
    "repo:blue.catbird.labyrinth.mailboxKeys",
    "repo:blue.catbird.chat.device?action=create",
    "repo:blue.catbird.chat.deviceRevocation?action=create",
    "space:blue.catbird.labyrinth.mailbox?action=create&action=delete&action=read&action=update&collection=blue.catbird.labyrinth.attachmentManifest&collection=blue.catbird.labyrinth.circleOwnerState&collection=blue.catbird.labyrinth.device&collection=blue.catbird.labyrinth.epoch&collection=blue.catbird.labyrinth.epochMember&collection=blue.catbird.labyrinth.otcSession&collection=blue.catbird.labyrinth.ownerBatch&collection=blue.catbird.labyrinth.readState&collection=blue.catbird.labyrinth.virtualDevice&manage=create&manage=delete",
    "space:blue.catbird.labyrinth.conversation?action=create&action=delete&action=read&action=update&authority=*&collection=blue.catbird.labyrinth.attachmentManifest&collection=blue.catbird.labyrinth.authorRedaction&collection=blue.catbird.labyrinth.batch&collection=blue.catbird.labyrinth.mek",
  ]

  /// Canonicalize an ATProto DID (`did:plc:...` or `did:web:...`) before hashing (`I8`).
  public static func canonicalizeAccountDid(_ rawDid: String) throws -> String {
    let trimmed = rawDid.trimmingCharacters(in: .whitespacesAndNewlines)
    let bare = trimmed.split(separator: "#", maxSplits: 1, omittingEmptySubsequences: false)
      .first
      .map { String($0).trimmingCharacters(in: .whitespacesAndNewlines) } ?? ""
    let parts = bare.split(separator: ":", maxSplits: 2, omittingEmptySubsequences: false)
    guard parts.count == 3 else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Invalid ATProto account DID: \(rawDid)")
    }
    let scheme = parts[0].lowercased()
    let method = parts[1].lowercased()
    let rawSpecific = String(parts[2])
    guard scheme == "did", !method.isEmpty, !rawSpecific.isEmpty else {
      throw CatbirdMLS.MlsError.InvalidInput(message: "Invalid ATProto account DID: \(rawDid)")
    }
    let specific: String
    if method == "web" {
      specific = rawSpecific.lowercased()
    } else if method == "plc" {
      specific = rawSpecific.lowercased()
    } else {
      specific = rawSpecific
    }
    return "\(scheme):\(method):\(specific)"
  }

  public static func accountDidHash(for accountDid: String) throws -> String {
    let canonical = try canonicalizeAccountDid(accountDid)
    return try accountDidHashHex(accountDid: canonical)
  }

  public static func defaultStorageRoot() throws -> URL {
    if let container = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: appGroupIdentifier
    ) {
      return container
    }
    let appSupport = try FileManager.default.url(
      for: .applicationSupportDirectory,
      in: .userDomainMask,
      appropriateFor: nil,
      create: true
    )
    return appSupport.appendingPathComponent("CatbirdSharedFallback", isDirectory: true)
  }

  public static func mlsDirectoryURL(storageRoot: URL) -> URL {
    if storageRoot.lastPathComponent == "catbird-mls" {
      return storageRoot
    }
    return storageRoot.appendingPathComponent("catbird-mls", isDirectory: true)
  }

  public static func databaseURL(storageRoot: URL, accountDid: String) throws -> URL {
    let hash = try accountDidHash(for: accountDid)
    return mlsDirectoryURL(storageRoot: storageRoot).appendingPathComponent("\(hash).db")
  }

  /// Wipe pre-cutover `.sqlite`, `.lease`, and `accounts.json` artifacts and enforce `A-7`/`I8` attributes.
  @discardableResult
  public static func prepareStorageRoot(_ storageRoot: URL) throws -> PreCutoverWipeReport {
    try FileManager.default.createDirectory(at: storageRoot, withIntermediateDirectories: true)
    let mlsDir = mlsDirectoryURL(storageRoot: storageRoot)
    try FileManager.default.createDirectory(at: mlsDir, withIntermediateDirectories: true)
    let report = try wipePreCutoverLocalArtifacts(storageRoot: storageRoot.path)
    try applyProtectionAttributes(toDirectoryAndContents: mlsDir)
    return report
  }

  public static func applyProtectionAttributes(toDirectoryAndContents mlsDir: URL) throws {
    try setExcludedFromBackupAndCompleteUntilFirstAuth(at: mlsDir)
    guard let entries = try? FileManager.default.contentsOfDirectory(
      at: mlsDir,
      includingPropertiesForKeys: nil
    ) else {
      return
    }
    for entry in entries {
      try? setExcludedFromBackupAndCompleteUntilFirstAuth(at: entry)
    }
  }

  private static func setExcludedFromBackupAndCompleteUntilFirstAuth(at url: URL) throws {
    var mutableURL = url
    var values = URLResourceValues()
    values.isExcludedFromBackup = true
    try mutableURL.setResourceValues(values)
    #if os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
    try? FileManager.default.setAttributes(
      [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
      ofItemAtPath: url.path
    )
    #endif
  }
}

public enum MLSPushExtensionProcessor {
  public static let genericFallbackBody = "New encrypted message"

  private static let pushDispatchQueue = DispatchQueue(
    label: "blue.catbird.mls.nse.ffi",
    qos: .userInitiated,
    attributes: .concurrent
  )

  /// Decodes a canonical `nest` APNs `{"aps": ..., "catbird": {...}}` wake envelope
  /// (as well as legacy `"payload"` or flat `userInfo` dictionaries) into a `PushWakePayload`.
  ///
  /// A `catbird` object with `v == 2` is the sealed envelope
  /// (`docs/security/2026-11-09-sealed-push-wake-envelope.md`): its `kid` is looked up in
  /// `wakeKeys`, the blob is opened, and `recipientAccountHash` is computed locally from the
  /// account the `kid` maps to. An unknown `kid`, a failed open or an unsupported `v` returns
  /// nil, so the caller delivers the notification unchanged. An object without `v` is the
  /// version 1 clear payload.
  public static func decodePushWakePayload(
    from userInfo: [AnyHashable: Any],
    wakeKeys: @autoclosure () -> any PushWakeKeyLookup = KeychainPlatformKeyStore()
  ) -> PushWakePayload? {
    let dict: [String: Any]
    if let nested = unwrapDictionary(userInfo["catbird"]) {
      dict = nested
    } else if let nested = unwrapDictionary(userInfo["payload"]) {
      dict = nested
    } else {
      dict = Dictionary(uniqueKeysWithValues: userInfo.compactMap { k, v in
        (k as? String).map { ($0, v) }
      })
    }

    if dict["v"] != nil {
      switch PushWakeEnvelope.version(of: dict) {
      case PushWakeEnvelope.version:
        return decodeSealedPushWake(dict, wakeKeys: wakeKeys())
      case 1:
        break
      default:
        return nil
      }
    }

    guard let rawConvo = (dict["conversationId"] as? String)
            ?? (dict["conversation_id"] as? String)
            ?? (dict["convo_id"] as? String),
          let rawHash = (dict["recipientAccountHash"] as? String)
            ?? (dict["recipient_account_hash"] as? String)
            ?? (dict["recipient_account"] as? String),
          !rawConvo.isEmpty,
          !rawHash.isEmpty
    else { return nil }

    let kind = (dict["wakeKind"] as? String)
      ?? (dict["wake_kind"] as? String)
      ?? (dict["kind"] as? String)
      ?? (dict["type"] as? String)
      ?? "application"
    let generation = parseFlexibleInt64(dict["generation"]) ?? 0
    let entrySeq = parseFlexibleInt64(dict["entrySeq"])
      ?? parseFlexibleInt64(dict["entry_seq"])
      ?? parseFlexibleInt64(dict["seq"])
      ?? 0
    let messageId = (dict["messageId"] as? String)
      ?? (dict["message_id"] as? String)
      ?? "msg-\(entrySeq)"
    let senderDid = (dict["senderDid"] as? String) ?? (dict["sender_did"] as? String)
    let rawCiphertext = (dict["ciphertext"] as? String)
      ?? (dict["inlineCiphertext"] as? String)
      ?? (dict["inline_ciphertext"] as? String)
    let inlineCiphertext = rawCiphertext.flatMap(decodeFlexibleBase64)
    let fetchRequired: Bool = if let b = dict["fetchRequired"] as? Bool {
      b
    } else if let b = dict["fetch_required"] as? Bool {
      b
    } else if let s = (dict["fetchRequired"] as? String) ?? (dict["fetch_required"] as? String),
              let parsed = Bool(s.trimmingCharacters(in: .whitespacesAndNewlines).lowercased())
    {
      parsed
    } else {
      inlineCiphertext == nil
    }

    return PushWakePayload(
      kind: kind,
      conversationId: rawConvo,
      generation: generation,
      entrySeq: entrySeq,
      messageId: messageId,
      recipientAccountHash: rawHash.trimmingCharacters(in: .whitespacesAndNewlines).lowercased(),
      fetchRequired: fetchRequired,
      senderDid: senderDid,
      inlineCiphertext: inlineCiphertext
    )
  }

  private static func decodeSealedPushWake(
    _ catbird: [String: Any],
    wakeKeys: any PushWakeKeyLookup
  ) -> PushWakePayload? {
    guard let kid = catbird["kid"] as? String,
          let record = wakeKeys.pushWakeKey(forKid: kid),
          let opened = try? PushWakeEnvelope.open(
            catbird,
            keys: PushWakeEnvelope.Keys(pushKey: record.pushKey),
            appId: record.appId
          ),
          !opened.header.conversationId.isEmpty,
          let recipientAccountHash = try? MLSAppGroupStorage.accountDidHash(for: record.accountDid)
    else { return nil }

    return PushWakePayload(
      kind: opened.header.wakeKind,
      conversationId: opened.header.conversationId,
      generation: opened.header.generation,
      entrySeq: opened.header.seq,
      messageId: "msg-\(opened.header.seq)",
      recipientAccountHash: recipientAccountHash,
      fetchRequired: opened.header.fetchRequired,
      senderDid: nil,
      inlineCiphertext: opened.ciphertext
    )
  }

  private static func unwrapDictionary(_ value: Any?) -> [String: Any]? {
    if let typed = value as? [String: Any] {
      return typed
    }
    if let anyHashable = value as? [AnyHashable: Any] {
      return Dictionary(uniqueKeysWithValues: anyHashable.compactMap { k, v in
        (k as? String).map { ($0, v) }
      })
    }
    if let jsonStr = value as? String,
       let data = jsonStr.data(using: .utf8),
       let parsed = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
    {
      return parsed
    }
    return nil
  }

  private static func parseFlexibleInt64(_ value: Any?) -> Int64? {
    if let i64 = value as? Int64 { return i64 }
    if let i = value as? Int { return Int64(i) }
    if let num = value as? NSNumber { return num.int64Value }
    if let str = value as? String { return Int64(str.trimmingCharacters(in: .whitespacesAndNewlines)) }
    return nil
  }

  private static func decodeFlexibleBase64(_ raw: String) -> Data? {
    let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else { return nil }
    if let direct = Data(base64Encoded: trimmed) {
      return direct
    }
    let standard = trimmed
      .replacingOccurrences(of: "-", with: "+")
      .replacingOccurrences(of: "_", with: "/")
    let padded = standard + String(repeating: "=", count: (4 - standard.utf8.count % 4) % 4)
    return Data(base64Encoded: padded)
  }

  /// Asynchronous entry point for Swift Concurrency callers inside the Notification Service
  /// Extension (`Fault Line 4`, `I10`): hops off Swift's cooperative thread pool onto a dedicated
  /// GCD queue before invoking synchronous Rust FFI + `PushFetchTransport`.
  public static func processAsync(
    storageRoot: URL,
    accountDidHash: String,
    keystore: PushKeystore,
    transport: PushFetchTransport?,
    wake: PushWakePayload,
    postDarwinNotification: Bool = true
  ) async -> NotificationPreview {
    await withCheckedContinuation { continuation in
      pushDispatchQueue.async {
        let preview = process(
          storageRoot: storageRoot,
          accountDidHash: accountDidHash,
          keystore: keystore,
          transport: transport,
          wake: wake,
          postDarwinNotification: postDarwinNotification
        )
        continuation.resume(returning: preview)
      }
    }
  }

  static func shouldPostDidCommit(
    for preview: NotificationPreview,
    requested: Bool
  ) -> Bool {
    requested && preview.receiptVerified && !preview.fallbackGeneric && !preview.fromCache
  }

  public static func process(
    storageRoot: URL,
    accountDidHash: String,
    keystore: PushKeystore,
    transport: PushFetchTransport?,
    wake: PushWakePayload,
    postDarwinNotification: Bool = true
  ) -> NotificationPreview {
    do {
      let mlsDir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRoot)
      let safeTransport = transport.map { UniFFINonThrowingPushFetchTransportProxy(inner: $0) }
      var preview = try processPushNotification(
        storageRoot: storageRoot.path,
        accountDidHash: accountDidHash,
        keystore: keystore,
        transport: safeTransport,
        wake: wake
      )
      if let safeTransport,
         safeTransport.didFailFetch,
         preview.fallbackGeneric,
         !wake.fetchRequired,
         wake.inlineCiphertext != nil
      {
        preview = try processPushNotification(
          storageRoot: storageRoot.path,
          accountDidHash: accountDidHash,
          keystore: keystore,
          transport: nil,
          wake: wake
        )
      }
      try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: mlsDir)
      if shouldPostDidCommit(for: preview, requested: postDarwinNotification) {
        postNseDidCommitDarwinNotification(name: preview.darwinNotificationName)
      }
      return preview
    } catch {
      return NotificationPreview(
        messageId: wake.messageId,
        conversationId: wake.conversationId,
        senderDid: wake.senderDid ?? "",
        senderDisplayName: nil,
        groupTitle: nil,
        body: genericFallbackBody,
        epoch: 0,
        seq: wake.entrySeq,
        fromCache: false,
        fallbackGeneric: true,
        darwinNotificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification,
        receiptVerified: false,
        isOwnSiblingSender: false
      )
    }
  }

  public static func postNseDidCommitDarwinNotification(
    name: String = MLSAppGroupStorage.nseDidCommitDarwinNotification
  ) {
    let center = CFNotificationCenterGetDarwinNotifyCenter()
    CFNotificationCenterPostNotification(
      center,
      CFNotificationName(name as CFString),
      nil,
      nil,
      true
    )
  }
}

private final class UniFFINonThrowingPushFetchTransportProxy: PushFetchTransport, @unchecked Sendable {
  private let inner: PushFetchTransport
  private let lock = NSLock()
  private var failedFetch = false

  var didFailFetch: Bool {
    lock.withLock { failedFetch }
  }

  init(inner: PushFetchTransport) {
    self.inner = inner
  }

  func fetchMissingEntries(
    conversationId: String,
    afterSeq: Int64,
    chatDpopToken: String
  ) throws -> PushFetchBundle {
    do {
      return try inner.fetchMissingEntries(
        conversationId: conversationId,
        afterSeq: afterSeq,
        chatDpopToken: chatDpopToken
      )
    } catch {
      lock.withLock { failedFetch = true }
      return PushFetchBundle(entries: [], metadataSnapshot: nil, authorizedDeviceKeys: [])
    }
  }

  func getAuthorizedDeviceKeys(
    did: String,
    chatDpopToken: String?
  ) throws -> [AuthorizedPeerDeviceKey] {
    do {
      return try inner.getAuthorizedDeviceKeys(did: did, chatDpopToken: chatDpopToken)
    } catch {
      return []
    }
  }
}

/// Shields UniFFI `0.28.3` callback traits whose Rust error type is `#[uniffi(flat_error)]` (`MLSError`),
/// because UniFFI `0.28.3`'s generated `try_lift` for `flat_error` panics (`"Can't lift flat errors"`)
/// whenever a foreign callback throws `CALL_ERROR` or `CALL_UNEXPECTED_ERROR` (`P0-7`).
private final class UniFFINonThrowingPlatformTransportProxy: PlatformTransport, @unchecked Sendable {
  private let inner: PlatformTransport

  init(inner: PlatformTransport) {
    self.inner = inner
  }

  func executeHttp(request: PlatformHttpRequest) throws -> PlatformHttpResponse {
    do {
      return try inner.executeHttp(request: request)
    } catch {
      return PetrelPlatformTransport.httpFailure(error)
    }
  }

  func connectRealtimeStream(homeDsEndpoint: String, cursorSeq: Int64?) throws {
    try? inner.connectRealtimeStream(homeDsEndpoint: homeDsEndpoint, cursorSeq: cursorSeq)
  }

  func disconnectRealtimeStream() throws {
    try? inner.disconnectRealtimeStream()
  }
}

private final class CoordinatorObserverBridge: PlatformChangeObserver, @unchecked Sendable {
  private let forward: @Sendable (PlatformChangeEvent) -> Void

  init(forward: @escaping @Sendable (PlatformChangeEvent) -> Void) {
    self.forward = forward
  }

  func onChange(event: PlatformChangeEvent) {
    forward(event)
  }
}

private final class DarwinNotificationToken: @unchecked Sendable {
  private let callback: @Sendable () -> Void
  private var isRegistered = true

  init(notificationName: String, callback: @escaping @Sendable () -> Void) {
    self.callback = callback
    let center = CFNotificationCenterGetDarwinNotifyCenter()
    let observerPtr = Unmanaged.passUnretained(self).toOpaque()
    CFNotificationCenterAddObserver(
      center,
      observerPtr,
      { _, observer, _, _, _ in
        guard let observer else { return }
        let token = Unmanaged<DarwinNotificationToken>.fromOpaque(observer).takeUnretainedValue()
        token.callback()
      },
      notificationName as CFString,
      nil,
      .deliverImmediately
    )
  }

  func unregister(notificationName: String) {
    guard isRegistered else { return }
    isRegistered = false
    let center = CFNotificationCenterGetDarwinNotifyCenter()
    CFNotificationCenterRemoveObserver(
      center,
      Unmanaged.passUnretained(self).toOpaque(),
      CFNotificationName(notificationName as CFString),
      nil
    )
  }

  deinit {
    unregister(notificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification)
  }
}

/// `@MainActor` `@Observable` coordinator wrapping the single Rust `CatbirdMlsClient` (`Spec §4`, `§6.3`, `I8`, `I9`, `I11`, `I15`).
@MainActor
@Observable
public final class MLSClientCoordinator {
  public let accountDid: String
  public let accountDidHash: String
  public let storageRootURL: URL
  public let preCutoverWipeReport: PreCutoverWipeReport

  public private(set) var darwinWakeupCount: Int = 0
  public private(set) var changeRevision: UInt64 = 0
  public private(set) var lastChangeEvent: PlatformChangeEvent?
  public private(set) var isClosedForSession: Bool = false

  @ObservationIgnored private let client: CatbirdMlsClient
  @ObservationIgnored private let observerBridge: CoordinatorObserverBridge
  @ObservationIgnored private let outboxDrainCoalescer = OutboxDrainCoalescer()
  @ObservationIgnored private var darwinToken: DarwinNotificationToken?
  @ObservationIgnored public var onChangeCallback: (@MainActor (PlatformChangeEvent) -> Void)?

  public var isHandleClosed: Bool {
    isClosedForSession || client.isHandleClosed()
  }

  /// Encrypted Circle operations over this account's Rust client.
  public func circleCrypto() -> any CircleCrypto {
    FFICircleCrypto(client: client)
  }

  /// Opens a `CatbirdMlsClient` and prepares App Group storage off `@MainActor` on `ffiDispatchQueue`,
  /// hopping to `@MainActor` only to initialize the `@Observable` wrapper and Darwin notification observer (`P0-7`).
  nonisolated public static func open(
    config: CatbirdMlsClientConfig,
    keystore: PlatformKeyStore,
    transport: PlatformTransport
  ) async throws -> MLSClientCoordinator {
    let canonicalDid = try MLSAppGroupStorage.canonicalizeAccountDid(config.accountDid)
    let storageRootURL = URL(fileURLWithPath: config.storageRoot, isDirectory: true)
    var normalizedConfig = config
    normalizedConfig.accountDid = canonicalDid

    let deferredBox = DeferredCoordinatorBox()
    let bridge = CoordinatorObserverBridge { event in
      Task { @MainActor in
        deferredBox.coordinator?.recordPlatformChange(event)
      }
    }

    let safeTransport = UniFFINonThrowingPlatformTransportProxy(inner: transport)
    let (openedClient, combinedRemoved): (CatbirdMlsClient, [String]) = try await withCheckedThrowingContinuation { continuation in
      Self.ffiDispatchQueue.async {
        continuation.resume(with: Result {
          let preWipe = try MLSAppGroupStorage.prepareStorageRoot(storageRootURL)
          let client = try CatbirdMlsClient.open(
            config: normalizedConfig,
            keystore: keystore,
            transport: safeTransport,
            observer: bridge
          )
          let mlsDir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
          try MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: mlsDir)
          let sdkWipe = client.preCutoverWipeReport()
          let merged = Array(Set(preWipe.removedFiles + sdkWipe.removedFiles)).sorted()
          return (client, merged)
        })
      }
    }

    return await MainActor.run {
      let coordinator = MLSClientCoordinator(
        openedClient: openedClient,
        storageRootURL: storageRootURL,
        preCutoverWipeReport: PreCutoverWipeReport(removedFiles: combinedRemoved),
        observerBridge: bridge
      )
      deferredBox.setCoordinator(coordinator)
      return coordinator
    }
  }

  private init(
    openedClient: CatbirdMlsClient,
    storageRootURL: URL,
    preCutoverWipeReport: PreCutoverWipeReport,
    observerBridge: CoordinatorObserverBridge
  ) {
    self.accountDid = openedClient.accountDid()
    self.accountDidHash = openedClient.accountDidHash()
    self.storageRootURL = storageRootURL
    self.preCutoverWipeReport = preCutoverWipeReport
    self.client = openedClient
    self.observerBridge = observerBridge

    let box = WeakCoordinatorBox(coordinator: self)
    self.darwinToken = DarwinNotificationToken(
      notificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification
    ) {
      Task { @MainActor in
        box.coordinator?.handleDarwinNotification()
      }
    }
  }

  public convenience init(
    config: CatbirdMlsClientConfig,
    keystore: PlatformKeyStore,
    transport: PlatformTransport
  ) throws {
    let canonicalDid = try MLSAppGroupStorage.canonicalizeAccountDid(config.accountDid)
    let storageRootURL = URL(fileURLWithPath: config.storageRoot, isDirectory: true)
    let preWipe = try MLSAppGroupStorage.prepareStorageRoot(storageRootURL)

    var normalizedConfig = config
    normalizedConfig.accountDid = canonicalDid

    let deferredBox = DeferredCoordinatorBox()
    let bridge = CoordinatorObserverBridge { event in
      Task { @MainActor in
        deferredBox.coordinator?.recordPlatformChange(event)
      }
    }

    let openedClient = try CatbirdMlsClient.open(
      config: normalizedConfig,
      keystore: keystore,
      transport: UniFFINonThrowingPlatformTransportProxy(inner: transport),
      observer: bridge
    )
    let mlsDir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    try MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: mlsDir)

    let sdkWipe = openedClient.preCutoverWipeReport()
    let combinedRemoved = Array(Set(preWipe.removedFiles + sdkWipe.removedFiles)).sorted()

    self.init(
      openedClient: openedClient,
      storageRootURL: storageRootURL,
      preCutoverWipeReport: PreCutoverWipeReport(removedFiles: combinedRemoved),
      observerBridge: bridge
    )
    deferredBox.setCoordinator(self)
  }

  nonisolated public static func evaluateEligibility(
    accountDid: String = "did:plc:check",
    pdsEndpoint: String = "https://pds.catbird.blue",
    pdsSupportsSpaces: Bool,
    grantedScopes: [String]
  ) -> SpacesEligibilityStatus {
    evaluateSpacesEligibility(
      input: SpacesEligibilityInput(
        accountDid: accountDid,
        pdsEndpoint: pdsEndpoint,
        pdsSupportsSpaces: pdsSupportsSpaces,
        grantedOauthScopes: grantedScopes
      )
    )
  }

  // MARK: - Core Conversation & Message Operations

  public func checkSpacesEligibility(grantedScopes: [String]) async throws -> SpacesEligibilityStatus {
    let c = client
    let status = try await runOffMain {
      try c.checkSpacesEligibility(grantedOauthScopes: grantedScopes)
    }
    if status.eligible {
      scheduleBackgroundOutboxDrain()
    }
    return status
  }

  public func enrollDevice(grantedScopes: [String]) async throws -> EnrollmentReceipt {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    let receipt = try await runOffMain {
      let receipt = try c.enrollDevice(grantedOauthScopes: grantedScopes)
      try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
      return receipt
    }
    scheduleBackgroundOutboxDrain()
    return receipt
  }

  public func createGroup(
    conversationId: String,
    sequencerDid: String,
    memberDids: [String] = [],
    title: String?,
    description: String?
  ) async throws -> ConversationRow {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    let row = try await runOffMain {
      let row = try c.createGroup(
        conversationId: conversationId,
        sequencerDid: sequencerDid,
        memberDids: memberDids,
        title: title,
        description: description
      )
      try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
      return row
    }
    scheduleBackgroundOutboxDrain()
    return row
  }

  public func pendingGroupCreationStatus(
    conversationId: String
  ) async throws -> PendingGroupCreationStatus? {
    let c = client
    return try await runOffMain {
      try c.pendingGroupCreationStatus(conversationId: conversationId)
    }
  }

  @discardableResult
  public func cancelPendingGroupCreation(conversationId: String) async throws -> Bool {
    let c = client
    return try await runOffMain {
      try c.cancelPendingGroupCreation(conversationId: conversationId)
    }
  }

  public func retryPendingGroupCreation(conversationId: String) async throws -> ConversationRow? {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    let row = try await runOffMain {
      let row = try c.retryPendingGroupCreation(conversationId: conversationId)
      if row != nil {
        try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
      }
      return row
    }
    if row != nil {
      scheduleBackgroundOutboxDrain()
    }
    return row
  }

  public func addGroupMembers(
    conversationId: String,
    memberDids: [String]
  ) async throws -> ConversationRow {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    return try await runOffMain {
      let row = try c.addGroupMembers(
        conversationId: conversationId,
        memberDids: memberDids
      )
      try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
      return row
    }
  }

  public func listMembers(conversationId: String) async throws -> [String] {
    let c = client
    return try await runOffMain {
      try c.listMembers(conversationId: conversationId)
    }
  }

  @discardableResult
  public func replenishKeyPackages(count: UInt32 = 10) async throws -> UInt32 {
    let c = client
    return try await runOffMain {
      try c.replenishKeyPackages(count: count)
    }
  }

  /// One `sync` pass, then its post-sync work: file protection and the outbox drain. A
  /// `SyncIncomplete` still synced every conversation it does not name, so that work runs before
  /// the error is rethrown.
  @discardableResult
  public func sync() async throws -> UInt32 {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    let synced: UInt32
    do {
      synced = try await runOffMain {
        let count = try c.sync()
        if count > 0 {
          try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
        }
        return count
      }
    } catch CatbirdMLS.MlsError.SyncIncomplete(let message) {
      try? await runOffMain { try MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir) }
      scheduleBackgroundOutboxDrain()
      throw CatbirdMLS.MlsError.SyncIncomplete(message: message)
    }
    if synced > 0 {
      scheduleBackgroundOutboxDrain()
    }
    return synced
  }

  public func listConversations(
    beforeUpdatedMs: Int64? = nil,
    limit: UInt32 = 50
  ) async throws -> [ConversationRow] {
    let c = client
    return try await runOffMain {
      try c.listConversations(beforeUpdatedMs: beforeUpdatedMs, limit: limit)
    }
  }

  public func listMessages(
    conversationId: String,
    beforeSeq: Int64? = nil,
    limit: UInt32 = 100
  ) async throws -> [MessageRow] {
    let c = client
    return try await runOffMain {
      try c.listMessages(conversationId: conversationId, beforeSeq: beforeSeq, limit: limit)
    }
  }

  public func listRequests(consentFilter: String? = nil) async throws -> [RequestRow] {
    let c = client
    return try await runOffMain {
      try c.listRequests(consentFilter: consentFilter)
    }
  }

  public func sendMessage(
    conversationId: String,
    messageId: String = UUID().uuidString.lowercased(),
    text: String
  ) async throws -> MessageRow {
    let c = client
    let row = try await runOffMain {
      try c.sendMessage(conversationId: conversationId, messageId: messageId, text: text)
    }
    scheduleBackgroundOutboxDrain()
    return row
  }

  @discardableResult
  public func unsendMessage(conversationId: String, messageId: String) async throws -> UInt32 {
    let c = client
    return try await runOffMain {
      try c.unsendMessage(conversationId: conversationId, messageId: messageId)
    }
  }

  public func acceptDirectRequest(requestId: String) async throws {
    let c = client
    try await runOffMain {
      try c.acceptDirectRequest(requestId: requestId)
    }
    scheduleBackgroundOutboxDrain()
  }

  public func declineDirectRequest(requestId: String) async throws {
    let c = client
    try await runOffMain {
      try c.declineDirectRequest(requestId: requestId)
    }
  }

  public func acknowledgeEntriesUpTo(conversationId: String, upToSeq: Int64) async throws {
    let c = client
    try await runOffMain {
      try c.acknowledgeEntriesUpTo(conversationId: conversationId, upToSeq: upToSeq)
    }
  }

  public func buildSignedPushGatewayRegistration(
    gatewayUrl: String,
    appId: String,
    pushKey: String,
    enabled: Bool
  ) async throws -> SignedPushGatewayRegistration {
    let c = client
    return try await runOffMain {
      try c.buildSignedPushGatewayRegistration(
        gatewayUrl: gatewayUrl,
        appId: appId,
        pushKey: pushKey,
        enabled: enabled
      )
    }
  }

  public func getGroupMetadata(conversationId: String) async throws -> GroupMetadataRow? {
    let c = client
    return try await runOffMain {
      try c.getGroupMetadata(conversationId: conversationId)
    }
  }

  public func historyBackupEnabled(conversationId: String) async throws -> Bool {
    let c = client
    return try await runOffMain { try c.historyBackupEnabled(conversationId: conversationId) }
  }

  public func setHistoryBackupEnabled(conversationId: String, enabled: Bool) async throws {
    let c = client
    try await runOffMain {
      _ = try c.setHistoryBackupEnabled(conversationId: conversationId, enabled: enabled)
    }
  }

  public func updateGroupMetadata(
    conversationId: String,
    title: String,
    description: String,
    spaceAvatar: SpaceAvatarPointer? = nil
  ) async throws -> GroupMetadataRow {
    let c = client
    return try await runOffMain {
      try c.updateGroupMetadata(
        conversationId: conversationId,
        title: title,
        description: description,
        spaceAvatar: spaceAvatar
      )
    }
  }

  public func removeGroupMember(
    actorRole: MemberRole,
    conversationId: String,
    targetDid: String
  ) async throws {
    let c = client
    try await runOffMain {
      try c.removeGroupMember(
        actorRole: actorRole,
        conversationId: conversationId,
        targetDid: targetDid
      )
    }
  }

  // MARK: - Labyrinth 1.1 Backup, Recovery, & PQ Pairing (D4, I16)

  public func labyrinthStatus() async throws -> LabyrinthBackupStatus {
    let c = client
    return try await runOffMain {
      try c.labyrinthStatus()
    }
  }

  public func acknowledgeRecoveryAttention() async throws -> LabyrinthBackupStatus {
    let c = client
    return try await runOffMain {
      try c.acknowledgeRecoveryAttention()
    }
  }

  public func acknowledgeRecoveryGeneration(expectedGeneration: UInt64) async throws -> LabyrinthBackupStatus {
    let c = client
    return try await runOffMain {
      try c.acknowledgeRecoveryGeneration(expectedGeneration: expectedGeneration)
    }
  }

  public func setupRecoveryCodeWithReceipt() async throws -> GeneratedRecoveryCode {
    let c = client
    let receipt = try await runOffMain { try c.setupRecoveryCodeWithReceipt() }
    scheduleBackgroundOutboxDrain()
    return receipt
  }

  public func setupRecoveryCode() async throws -> String {
    let c = client
    let code = try await runOffMain {
      try c.setupRecoveryCode()
    }
    scheduleBackgroundOutboxDrain()
    return code
  }

  public func restoreWithRecoveryCode(_ code: String) async throws -> LabyrinthBackupStatus {
    let c = client
    let status = try await runOffMain {
      try c.restoreWithRecoveryCode(recoveryCodeInput: code)
    }
    scheduleBackgroundOutboxDrain()
    return status
  }

  public func registerPasskeyPrf(credentialId: Data, prfOutput32: Data) async throws -> LabyrinthBackupStatus {
    let c = client
    let status = try await runOffMain {
      try c.registerPasskeyPrf(credentialId: credentialId, prfOutput32: prfOutput32)
    }
    scheduleBackgroundOutboxDrain()
    return status
  }

  public func initiateOtcPairing() async throws -> OtcPairingOffer {
    let c = client
    return try await runOffMain {
      try c.initiateOtcPairing()
    }
  }

  public func completeOtcPairing(sixDigitCode: String) async throws -> OtcPairingCompletion {
    let c = client
    let completion = try await runOffMain {
      try c.completeOtcPairing(sixDigitCode: sixDigitCode)
    }
    scheduleBackgroundOutboxDrain()
    return completion
  }

  public func listRecoveryMethods() async throws -> [LabyrinthRecoveryMethodInfo] {
    let c = client
    return try await runOffMain {
      try c.listRecoveryMethods()
    }
  }

  public func removeRecoveryMethod(
    vdeviceId: Data
  ) async throws -> LabyrinthRevocationOutcome {
    let c = client
    let outcome = try await runOffMain {
      try c.removeRecoveryMethod(vdeviceId: vdeviceId)
    }
    scheduleBackgroundOutboxDrain()
    return outcome
  }

  public func revokeLabyrinthDevice(
    targetDeviceId: String
  ) async throws -> LabyrinthRevocationOutcome {
    let c = client
    return try await runOffMain {
      try c.revokeLabyrinthDevice(targetDeviceId: targetDeviceId)
    }
  }

  public func importConversationHistory(
    conversationId: String,
    messages: [MessageRow]
  ) async throws -> UInt32 {
    let c = client
    return try await runOffMain {
      try c.importConversationHistory(conversationId: conversationId, messages: messages)
    }
  }

  public func listPendingLabyrinthQueue(
    conversationId: String? = nil,
    limit: UInt32 = 100
  ) async throws -> [QueuedLabyrinthArchivePayload] {
    let c = client
    return try await runOffMain {
      try c.listPendingLabyrinthQueue(conversationId: conversationId, limit: limit)
    }
  }

  @discardableResult
  public func flushLabyrinthQueue() async throws -> UInt32 {
    let c = client
    return try await runOffMain {
      try c.flushLabyrinthQueue()
    }
  }

  @discardableResult
  public func rescueConversationOnKick(conversationId: String) async throws -> UInt32 {
    let c = client
    return try await runOffMain {
      try c.rescueConversationOnKick(conversationId: conversationId)
    }
  }

  @discardableResult
  public func restoreConversationFromOwnerMailbox(conversationId: String) async throws -> UInt32 {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    return try await runOffMain {
      let restored = try c.restoreConversationFromOwnerMailbox(conversationId: conversationId)
      if restored > 0 {
        try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
      }
      return restored
    }
  }

  // MARK: - Wave 2 Group Leave, Space Attachments, Multi-Device, OTC, ReadState & Circles

  @discardableResult
  public func leaveConversation(conversationId: String) async throws -> LeaveConversationOutcome {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    let outcome = try await runOffMain {
      let result = try c.leaveConversation(conversationId: conversationId)
      try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
      return result
    }
    scheduleBackgroundOutboxDrain()
    return outcome
  }

  @discardableResult
  public func fulfillPendingGroupLeaves() async throws -> UInt32 {
    let c = client
    return try await runOffMain {
      try c.fulfillPendingGroupLeaves()
    }
  }

  public func closeConversation(conversationId: String) async throws {
    let c = client
    try await runOffMain {
      try c.closeConversation(conversationId: conversationId)
    }
  }

  public func uploadConversationAttachment(
    conversationId: String,
    plaintext: Data,
    mimeType: String
  ) async throws -> UploadedConversationAttachment {
    let c = client
    let uploaded = try await runOffMain {
      try c.uploadConversationAttachment(
        conversationId: conversationId,
        plaintext: plaintext,
        mimeType: mimeType
      )
    }
    scheduleBackgroundOutboxDrain()
    return uploaded
  }

  public func restoreWithPasskeyPrf(
    credentialId: Data,
    prfOutput32: Data
  ) async throws -> LabyrinthBackupStatus {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    let status = try await runOffMain {
      let restored = try c.restoreWithPasskeyPrf(credentialId: credentialId, prfOutput32: prfOutput32)
      try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
      return restored
    }
    scheduleBackgroundOutboxDrain()
    return status
  }

  public func retireCurrentDevice() async throws {
    let c = client
    try await runOffMain {
      try c.retireCurrentDevice()
    }
  }

  public func isDevicePendingApproval() async throws -> Bool {
    let c = client
    return try await runOffMain {
      try c.isDevicePendingApproval()
    }
  }

  public func approveDevice(targetDeviceId: String) async throws {
    let c = client
    try await runOffMain {
      try c.approveDevice(targetDeviceId: targetDeviceId)
    }
    scheduleBackgroundOutboxDrain()
  }

  public func approveDeviceWithKey(
    targetDeviceId: String,
    targetSignaturePublicKey: Data,
    targetAuthGeneration: Int64
  ) async throws {
    let c = client
    try await runOffMain {
      try c.approveDeviceWithKey(
        targetDeviceId: targetDeviceId,
        targetSignaturePublicKey: targetSignaturePublicKey,
        targetAuthGeneration: targetAuthGeneration
      )
    }
    scheduleBackgroundOutboxDrain()
  }

  public func listDevices() async throws -> [RegisteredDeviceInfo] {
    let c = client
    return try await runOffMain {
      try c.listDevices()
    }
  }

  @discardableResult
  public func revokeDevice(targetDeviceId: String) async throws -> DeviceRevocationReport {
    let c = client
    let report = try await runOffMain {
      try c.revokeDevice(targetDeviceId: targetDeviceId)
    }
    scheduleBackgroundOutboxDrain()
    return report
  }

  public func deleteAccountData() async throws {
    outboxDrainCoalescer.setSuspended(true)
    darwinToken?.unregister(notificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification)
    darwinToken = nil
    let c = client
    try await runOffMain {
      try c.deleteAccountData()
    }
    isClosedForSession = true
  }

  /// Requires completion on every participating delivery service before deleting a hosted account.
  /// The native journal and signing keys stay available if the provider requires another email code.
  public func deleteAccountDataForHostedAccount() async throws {
    let c = client
    try await runOffMain {
      try c.deleteAccountDataForHostedAccount()
    }
  }

  public func startOtcPairing(sixDigitCode: String) async throws -> OtcInitiatedSession {
    let c = client
    return try await runOffMain {
      try c.startOtcPairing(sixDigitCode: sixDigitCode)
    }
  }

  public func respondOtcPairing(sessionIdHex: String, sixDigitCode: String?) async throws {
    let c = client
    try await runOffMain {
      try c.respondOtcPairing(sessionIdHex: sessionIdHex, sixDigitCode: sixDigitCode)
    }
    scheduleBackgroundOutboxDrain()
  }

  @discardableResult
  public func verifyAndReindexHistory(conversationId: String) async throws -> UInt32 {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    return try await runOffMain {
      let count = try c.verifyAndReindexHistory(conversationId: conversationId)
      if count > 0 {
        try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
      }
      return count
    }
  }

  public func proposePeerRepin(peerDid: String) async throws -> PeerRepinProposalInfo {
    let c = client
    return try await runOffMain {
      try c.proposePeerRepin(peerDid: peerDid)
    }
  }

  public func repinPeer(peerDid: String, confirmedHead: Data) async throws {
    let c = client
    try await runOffMain {
      try c.repinPeer(peerDid: peerDid, confirmedHead: confirmedHead)
    }
  }

  public func getReadState(conversationId: String) async throws -> MailboxReadStateRecord? {
    let c = client
    return try await runOffMain {
      try c.getReadState(conversationId: conversationId)
    }
  }

  @discardableResult
  public func advanceReadState(
    conversationId: String,
    candidateSeq: UInt64
  ) async throws -> MailboxReadStateRecord {
    let c = client
    return try await runOffMain {
      try c.advanceReadState(conversationId: conversationId, candidateSeq: candidateSeq)
    }
  }

  public func listReadStates() async throws -> [MailboxReadStateRecord] {
    let c = client
    return try await runOffMain {
      try c.listReadStates()
    }
  }

  public func getCircleOwnerState(circleId: String) async throws -> CircleOwnerStateDto? {
    let c = client
    return try await runOffMain {
      try c.getCircleOwnerState(circleId: circleId)
    }
  }

  @discardableResult
  public func putCircleOwnerState(
    state: CircleOwnerStateDto,
    swapRecordCid: String? = nil
  ) async throws -> String {
    let c = client
    return try await runOffMain {
      try c.putCircleOwnerState(state: state, swapRecordCid: swapRecordCid)
    }
  }

  @discardableResult
  public func mergeCircleOwnerState(update: CircleOwnerStateDto) async throws -> CircleOwnerStateDto {
    let c = client
    return try await runOffMain {
      try c.mergeCircleOwnerState(update: update)
    }
  }

  @discardableResult
  public func clearCircleAclTombstone(
    circleId: String,
    memberDid: String
  ) async throws -> CircleOwnerStateDto? {
    let c = client
    return try await runOffMain {
      try c.clearCircleAclTombstone(circleId: circleId, memberDid: memberDid)
    }
  }

  public func listCircleOwnerStates() async throws -> [CircleOwnerStateDto] {
    let c = client
    return try await runOffMain {
      try c.listCircleOwnerStates()
    }
  }

  // MARK: - Voice DSP & Cross-Platform Opus Codec (`FC-09`)

  nonisolated public static func prepareVoiceMessage(
    pcmPath: String,
    sampleRate: UInt32
  ) async throws -> VoicePrepareResult {
    try await withCheckedThrowingContinuation { continuation in
      ffiDispatchQueue.async {
        continuation.resume(with: Result {
          try ffiPrepareVoiceMessage(pcmPath: pcmPath, sampleRate: sampleRate)
        })
      }
    }
  }

  nonisolated public static func decodeOpusToPcm(opusData: Data) async throws -> Data {
    try await withCheckedThrowingContinuation { continuation in
      ffiDispatchQueue.async {
        continuation.resume(with: Result {
          try ffiDecodeOpusToPcm(opusData: opusData)
        })
      }
    }
  }

  /// Decodes Opus-in-OGG (`audio/ogg; codecs=opus`) into an in-memory 48 kHz, 16-bit LE mono
  /// RIFF/WAVE container ready for `AVAudioPlayer(data:)` (`FC-09`).
  nonisolated public static func decodeOpusToWav(opusData: Data) async throws -> Data {
    let pcm = try await decodeOpusToPcm(opusData: opusData)
    return wrapPcm16MonoAsWav(pcmData: pcm, sampleRate: 48_000)
  }

  nonisolated internal static func wrapPcm16MonoAsWav(pcmData: Data, sampleRate: UInt32) -> Data {
    let dataSize = UInt32(clamping: pcmData.count)
    let riffChunkSize = 36 &+ dataSize
    let channels: UInt16 = 1
    let bitsPerSample: UInt16 = 16
    let blockAlign: UInt16 = channels * (bitsPerSample / 8)
    let byteRate: UInt32 = sampleRate * UInt32(blockAlign)

    var wav = Data(capacity: 44 + pcmData.count)
    wav.append(contentsOf: "RIFF".utf8)
    withUnsafeBytes(of: riffChunkSize.littleEndian) { wav.append(contentsOf: $0) }
    wav.append(contentsOf: "WAVE".utf8)
    wav.append(contentsOf: "fmt ".utf8)
    withUnsafeBytes(of: UInt32(16).littleEndian) { wav.append(contentsOf: $0) }
    withUnsafeBytes(of: UInt16(1).littleEndian) { wav.append(contentsOf: $0) } // PCM = 1
    withUnsafeBytes(of: channels.littleEndian) { wav.append(contentsOf: $0) }
    withUnsafeBytes(of: sampleRate.littleEndian) { wav.append(contentsOf: $0) }
    withUnsafeBytes(of: byteRate.littleEndian) { wav.append(contentsOf: $0) }
    withUnsafeBytes(of: blockAlign.littleEndian) { wav.append(contentsOf: $0) }
    withUnsafeBytes(of: bitsPerSample.littleEndian) { wav.append(contentsOf: $0) }
    wav.append(contentsOf: "data".utf8)
    withUnsafeBytes(of: dataSize.littleEndian) { wav.append(contentsOf: $0) }
    wav.append(pcmData)
    return wav
  }

  // MARK: - Lifecycle (I9, I11, Sign-Out)

  @discardableResult
  public func prepareForBackground() async throws -> BackgroundCheckpointReport {
    outboxDrainCoalescer.setSuspended(true)
    let c = client
    return try await runOffMain {
      try c.prepareForBackground()
    }
  }

  public func resumeFromBackground() async throws {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    try await runOffMain {
      try c.resumeFromBackground()
      try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
    }
    isClosedForSession = false
    outboxDrainCoalescer.setSuspended(false)
    scheduleBackgroundOutboxDrain()
  }

  public func signOutAndWipe() async throws {
    outboxDrainCoalescer.setSuspended(true)
    darwinToken?.unregister(notificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification)
    darwinToken = nil
    let c = client
    try await runOffMain {
      try c.signOutAndWipe()
    }
    isClosedForSession = true
  }

  public func close() async {
    outboxDrainCoalescer.setSuspended(true)
    darwinToken?.unregister(notificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification)
    darwinToken = nil
    let c = client
    _ = try? await runOffMain {
      try c.prepareForBackground()
    }
    isClosedForSession = true
  }

  // MARK: - Internal Observers & Background Outbox Drain

  nonisolated private static let ffiDispatchQueue = DispatchQueue(
    label: "blue.catbird.mls.coordinator.ffi",
    qos: .userInitiated,
    attributes: .concurrent
  )

  /// How this account's most recent background backup flush ended; nil when none has run
  /// since the app started.
  public var lastBackupFlushOutcome: MLSBackupFlushOutcome? {
    MLSBackupFlushFailureLog.shared.lastOutcome(account: accountDid)
  }

  private func scheduleBackgroundOutboxDrain() {
    guard !isHandleClosed else { return }
    let coalescer = outboxDrainCoalescer
    let c = client
    let account = accountDid
    guard coalescer.requestDrain(isHandleClosed: { c.isHandleClosed() }) else { return }
    Self.ffiDispatchQueue.async {
      guard coalescer.beginFirstIteration(isHandleClosed: { c.isHandleClosed() }) else {
        return
      }
      repeat {
        if !c.isHandleClosed() {
          // A failed flush is retried on the next trigger, but it is not silent.
          let failures = MLSBackupFlushFailureLog.shared
          do {
            _ = try c.flushLabyrinthQueue()
            failures.write(failures.succeeded(account: account))
          } catch {
            failures.write(failures.failed(error, account: account))
          }
        }
      } while coalescer.completeAndShouldRepeat(isHandleClosed: { c.isHandleClosed() })
    }
  }

  fileprivate func recordPlatformChange(_ event: PlatformChangeEvent) {
    changeRevision &+= 1
    lastChangeEvent = event
    onChangeCallback?(event)
  }

  fileprivate func handleDarwinNotification() {
    darwinWakeupCount += 1
    guard !isHandleClosed else { return }
    let c = client
    Self.ffiDispatchQueue.async {
      try? c.handleDarwinPushWakeup()
    }
  }

  private func runOffMain<T: Sendable>(
    _ operation: @escaping @Sendable () throws -> T
  ) async throws -> T {
    try await withCheckedThrowingContinuation { continuation in
      Self.ffiDispatchQueue.async {
        continuation.resume(with: Result { try operation() })
      }
    }
  }
}

final class OutboxDrainCoalescer: @unchecked Sendable {
  private let lock = NSLock()
  private var isDraining = false
  private var needsAnotherPass = false
  private var isSuspended = false

  func setSuspended(_ suspended: Bool) {
    lock.lock()
    isSuspended = suspended
    if suspended {
      needsAnotherPass = false
    }
    lock.unlock()
  }

  func requestDrain(isHandleClosed: () -> Bool) -> Bool {
    lock.lock()
    defer { lock.unlock() }
    guard !isSuspended, !isHandleClosed() else {
      return false
    }
    if isDraining {
      needsAnotherPass = true
      return false
    }
    isDraining = true
    needsAnotherPass = false
    return true
  }

  func beginFirstIteration(isHandleClosed: () -> Bool) -> Bool {
    lock.lock()
    defer { lock.unlock() }
    if isSuspended || isHandleClosed() {
      isDraining = false
      needsAnotherPass = false
      return false
    }
    needsAnotherPass = false
    return true
  }

  func completeAndShouldRepeat(isHandleClosed: () -> Bool) -> Bool {
    lock.lock()
    defer { lock.unlock() }
    if isSuspended || isHandleClosed() || !needsAnotherPass {
      isDraining = false
      needsAnotherPass = false
      return false
    }
    needsAnotherPass = false
    return true
  }
}

private final class WeakCoordinatorBox: @unchecked Sendable {
  @MainActor weak var coordinator: MLSClientCoordinator?

  @MainActor
  init(coordinator: MLSClientCoordinator) {
    self.coordinator = coordinator
  }
}

private final class DeferredCoordinatorBox: @unchecked Sendable {
  private let lock = NSLock()
  @MainActor private weak var storedCoordinator: MLSClientCoordinator?

  @MainActor
  var coordinator: MLSClientCoordinator? {
    lock.lock()
    defer { lock.unlock() }
    return storedCoordinator
  }

  @MainActor
  func setCoordinator(_ coordinator: MLSClientCoordinator) {
    lock.lock()
    storedCoordinator = coordinator
    lock.unlock()
  }
}

extension CatbirdMLS.MlsError {
  /// The conversations a `SyncIncomplete` names: each stopped at a step that did not verify or
  /// apply while every other conversation synced. `nil` for any other error. Only canonical
  /// conversation IDs are returned; the failure text itself is never parsed.
  /// ponytail: parses the flat FFI message (`sync incomplete (N applied): conversation <id>: <error>; …`);
  /// a structured `failures` field on the UniFFI error would replace the parse.
  public var syncIncompleteConversationIDs: Set<String>? {
    guard case .SyncIncomplete(let message) = self else { return nil }
    let id = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
    guard let pattern = try? Regex("(?:^sync incomplete \\(\\d+ applied\\): |; )conversation (\(id)): ") else {
      return []
    }
    return Set(message.matches(of: pattern).compactMap { $0.output[1].substring.map(String.init) })
  }
}
