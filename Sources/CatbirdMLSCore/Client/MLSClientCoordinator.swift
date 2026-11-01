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

public enum MLSAppGroupStorage {
  public static let appGroupIdentifier = "group.blue.catbird.shared"
  public static let nseDidCommitDarwinNotification = "blue.catbird.mls.nseDidCommit"
  public static let requiredSpacesScopes: [String] = [
    "atproto",
    "blob:*/*",
    "repo:blue.catbird.labyrinth.mailboxKeys",
    "repo:blue.catbird.mlsChat.device",
    "space:blue.catbird.labyrinth.mailbox?manage=create&manage=delete",
    "space:blue.catbird.labyrinth.conversation?authority=*",
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
      let preview = try processPushNotification(
        storageRoot: storageRoot.path,
        accountDidHash: accountDidHash,
        keystore: keystore,
        transport: transport,
        wake: wake
      )
      try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: mlsDir)
      if !preview.fallbackGeneric && !preview.fromCache && postDarwinNotification {
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
        darwinNotificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification
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
  @ObservationIgnored private var darwinToken: DarwinNotificationToken?
  @ObservationIgnored public var onChangeCallback: (@MainActor (PlatformChangeEvent) -> Void)?

  public var isHandleClosed: Bool {
    isClosedForSession || client.isHandleClosed()
  }

  public init(
    config: CatbirdMlsClientConfig,
    keystore: PlatformKeyStore,
    transport: PlatformTransport
  ) throws {
    let canonicalDid = try MLSAppGroupStorage.canonicalizeAccountDid(config.accountDid)
    let storageRootURL = URL(fileURLWithPath: config.storageRoot, isDirectory: true)
    let preWipe = try MLSAppGroupStorage.prepareStorageRoot(storageRootURL)

    var normalizedConfig = config
    normalizedConfig.accountDid = canonicalDid

    var weakSelfBox: WeakCoordinatorBox?
    let bridge = CoordinatorObserverBridge { event in
      Task { @MainActor in
        weakSelfBox?.coordinator?.recordPlatformChange(event)
      }
    }

    let openedClient = try CatbirdMlsClient.open(
      config: normalizedConfig,
      keystore: keystore,
      transport: transport,
      observer: bridge
    )
    let mlsDir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    try MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: mlsDir)

    let sdkWipe = openedClient.preCutoverWipeReport()
    let combinedRemoved = Array(Set(preWipe.removedFiles + sdkWipe.removedFiles)).sorted()

    self.accountDid = openedClient.accountDid()
    self.accountDidHash = openedClient.accountDidHash()
    self.storageRootURL = storageRootURL
    self.preCutoverWipeReport = PreCutoverWipeReport(removedFiles: combinedRemoved)
    self.client = openedClient
    self.observerBridge = bridge

    let box = WeakCoordinatorBox(coordinator: self)
    weakSelfBox = box

    self.darwinToken = DarwinNotificationToken(
      notificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification
    ) {
      Task { @MainActor in
        box.coordinator?.handleDarwinNotification()
      }
    }
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
    return try await runOffMain {
      try c.checkSpacesEligibility(grantedOauthScopes: grantedScopes)
    }
  }

  public func enrollDevice(grantedScopes: [String]) async throws -> EnrollmentReceipt {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    return try await runOffMain {
      let receipt = try c.enrollDevice(grantedOauthScopes: grantedScopes)
      try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
      return receipt
    }
  }

  public func createGroup(
    conversationId: String,
    sequencerDid: String,
    title: String?,
    description: String?
  ) async throws -> ConversationRow {
    let c = client
    let dir = MLSAppGroupStorage.mlsDirectoryURL(storageRoot: storageRootURL)
    return try await runOffMain {
      let row = try c.createGroup(
        conversationId: conversationId,
        sequencerDid: sequencerDid,
        title: title,
        description: description
      )
      try? MLSAppGroupStorage.applyProtectionAttributes(toDirectoryAndContents: dir)
      return row
    }
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
    return try await runOffMain {
      try c.sendMessage(conversationId: conversationId, messageId: messageId, text: text)
    }
  }

  public func acceptDirectRequest(requestId: String) async throws {
    let c = client
    try await runOffMain {
      try c.acceptDirectRequest(requestId: requestId)
    }
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

  public func getGroupMetadata(conversationId: String) async throws -> GroupMetadataRow? {
    let c = client
    return try await runOffMain {
      try c.getGroupMetadata(conversationId: conversationId)
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

  public func setupRecoveryCode() async throws -> String {
    let c = client
    return try await runOffMain {
      try c.setupRecoveryCode()
    }
  }

  public func restoreWithRecoveryCode(_ code: String) async throws -> LabyrinthBackupStatus {
    let c = client
    return try await runOffMain {
      try c.restoreWithRecoveryCode(recoveryCodeInput: code)
    }
  }

  public func registerPasskeyPrf(credentialId: Data, prfOutput32: Data) async throws -> LabyrinthBackupStatus {
    let c = client
    return try await runOffMain {
      try c.registerPasskeyPrf(credentialId: credentialId, prfOutput32: prfOutput32)
    }
  }

  public func initiateOtcPairing() async throws -> OtcPairingOffer {
    let c = client
    return try await runOffMain {
      try c.initiateOtcPairing()
    }
  }

  public func completeOtcPairing(sixDigitCode: String) async throws -> OtcPairingCompletion {
    let c = client
    return try await runOffMain {
      try c.completeOtcPairing(sixDigitCode: sixDigitCode)
    }
  }

  public func revokeLabyrinthDevice(targetDeviceId: String) async throws -> LabyrinthBackupStatus {
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

  // MARK: - Lifecycle (I9, I11, Sign-Out)

  @discardableResult
  public func prepareForBackground() async throws -> BackgroundCheckpointReport {
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
  }

  public func signOutAndWipe() async throws {
    darwinToken?.unregister(notificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification)
    darwinToken = nil
    let c = client
    try await runOffMain {
      try c.signOutAndWipe()
    }
    isClosedForSession = true
  }

  public func close() async {
    darwinToken?.unregister(notificationName: MLSAppGroupStorage.nseDidCommitDarwinNotification)
    darwinToken = nil
    let c = client
    _ = try? await runOffMain {
      try c.prepareForBackground()
    }
    isClosedForSession = true
  }

  // MARK: - Internal Observers

  nonisolated private static let ffiDispatchQueue = DispatchQueue(
    label: "blue.catbird.mls.coordinator.ffi",
    qos: .userInitiated,
    attributes: .concurrent
  )

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

private final class WeakCoordinatorBox: @unchecked Sendable {
  @MainActor weak var coordinator: MLSClientCoordinator?

  @MainActor
  init(coordinator: MLSClientCoordinator) {
    self.coordinator = coordinator
  }
}
