//
//  MLSStateChangeNotifier.swift
//  CatbirdMLSCore
//
//  Inter-process notification system for MLS state synchronization between
//  the main app and Notification Service Extension (NSE).
//
//  ═══════════════════════════════════════════════════════════════════════════
//  CRITICAL: RATCHET STATE DESYNC FIX (2024-12)
//  ═══════════════════════════════════════════════════════════════════════════
//
//  Problem: When the app is in the FOREGROUND and NSE decrypts a message
//  concurrently, the app's in-memory MLS state becomes stale.
//
//  Root Cause:
//  1. Forward Secrecy: Each decryption key is deleted after use
//  2. NSE runs as a SEPARATE PROCESS and advances the MLS ratchet on disk
//  3. Main app holds stale in-memory state (old epoch, old keys)
//  4. App tries to decrypt → uses old state → SecretReuseError or DecryptionFailed
//
//  Existing Fix (insufficient):
//  - handleScenePhaseChange reloads state when app transitions background → active
//  - BUT: This doesn't trigger when app is ALREADY in foreground
//
//  New Fix (this file):
//  - NSE posts a Darwin Notification after decrypting a message
//  - Main app listens for this notification and reloads MLS state from disk
//  - Works even when app is already in foreground
//
//  Darwin Notifications are the ONLY reliable IPC mechanism for App Extensions:
//  - Cross-process (app ↔ NSE run as separate processes)
//  - No data payload (just a signal) - sufficient for our "reload state" use case
//  - Works even when app is suspended/backgrounded
//
//  ═══════════════════════════════════════════════════════════════════════════
//  PHASE 5: Tokenized Darwin Handshake (2024-12)
//  ═══════════════════════════════════════════════════════════════════════════
//
//  Enhanced coordination protocol:
//  1. NSE writes `willClose(userDID, token)` to App Group + posts Darwin `nseWillClose` doorbell
//  2. Main app receives doorbell, releases readers, writes `ack(userDID, token)` + posts `appAcknowledged` doorbell
//  3. NSE waits for matching ack(token) (bounded timeout/backoff), proceeds with TRUNCATE checkpoint
//  4. NSE posts existing `stateChanged` after close
//
//  This ensures the main app releases its database readers before NSE
//  performs the TRUNCATE checkpoint, preventing WAL/SHM locking conflicts.
//
//  ═══════════════════════════════════════════════════════════════════════════

import Foundation
import OSLog

/// Notification name for MLS state changes (Darwin Notification)
/// This must match EXACTLY between the main app and NSE
public let kMLSStateChangedNotification = "blue.catbird.mls.stateChanged" as CFString

/// Notification name for NSE will close (Darwin Notification - Phase 5)
/// Posted by NSE before it starts the checkpoint/close sequence
public let kMLSNSEWillCloseNotification = "blue.catbird.mls.nseWillClose" as CFString

/// Notification name for app acknowledged (Darwin Notification - Phase 5)
/// Posted by main app after it releases database readers
public let kMLSAppAcknowledgedNotification = "blue.catbird.mls.appAcknowledged" as CFString

/// Manager for cross-process MLS state change notifications using Darwin Notifications.
///
/// Usage from NSE (after decrypting a message):
/// ```swift
/// MLSStateChangeNotifier.postStateChanged()
/// ```
///
/// Usage from Main App (during initialization):
/// ```swift
/// MLSStateChangeNotifier.shared.startObserving { userInfo in
///     await appState.reloadMLSStateFromDisk()
/// }
/// ```
public final class MLSStateChangeNotifier: @unchecked Sendable {
  
  // MARK: - Singleton
  
  public static let shared = MLSStateChangeNotifier()
  
  // MARK: - Properties
  
  private let logger = Logger(subsystem: "blue.catbird.mls", category: "StateChangeNotifier")
  
  /// Callback to invoke when state change notification is received
  private var onStateChanged: (() -> Void)?
  
  /// Async handler invoked when the NSE signals it will close (tokenized handshake).
  /// Returns true if the app successfully released readers for the request.
  private var onNSEWillCloseRequest: (@MainActor (MLSNSEWillCloseRequest) async -> Bool)?
  
  /// Whether we're currently observing for notifications
  private var isObserving: Bool = false
  
  /// Whether we're observing for NSE will close notifications (Phase 5)
  private var isObservingNSEWillClose: Bool = false
  
  /// Serial queue for thread-safe observer registration
  private let queue = DispatchQueue(label: "blue.catbird.mls.stateChangeNotifier")
  
  // MARK: - Debouncing (Fix for rapid NSE notifications)
  
  /// Debounce interval to coalesce rapid notifications (100ms)
  private let debounceInterval: TimeInterval = 0.1
  
  /// Pending debounced task for notification handling
  private var pendingDebounceTask: Task<Void, Never>?
  
  /// Lock for thread-safe debounce task management
  private let debounceLock = NSLock()
  
  // MARK: - Tokenized Handshake Debouncing

  private let handshakeDebounceInterval: TimeInterval = 0.05
  private var pendingHandshakeTask: Task<Void, Never>?
  private let handshakeLock = NSLock()
  
  // MARK: - Initialization
  
  private init() {
    logger.debug("MLSStateChangeNotifier initialized")
  }
  
  deinit {
    stopObserving()
  }
  
  // MARK: - Posting Notifications (called from NSE)
  
  /// Post a Darwin Notification that MLS state has changed on disk.
  ///
  /// Call this from the Notification Service Extension AFTER:
  /// 1. Successfully decrypting a message
  /// 2. Closing/draining the database connection
  ///
  /// The main app will receive this notification and reload its MLS state from disk,
  /// even if the app is already in the foreground.
  ///
  /// - Note: This is a static method so it can be called without keeping a reference
  ///   to the notifier instance (important for NSE which has limited lifecycle).
  public static func postStateChanged() {
    let logger = Logger(subsystem: "blue.catbird.mls", category: "StateChangeNotifier")
    
    // Get the Darwin notification center
    let center = CFNotificationCenterGetDarwinNotifyCenter()
    
    // Post the notification
    // - object: nil (Darwin notifications don't support object)
    // - userInfo: nil (Darwin notifications don't support userInfo)
    // - deliverImmediately: true (we want this delivered ASAP)
    CFNotificationCenterPostNotification(
      center,
      CFNotificationName(kMLSStateChangedNotification),
      nil,
      nil,
      true
    )
    
    logger.info("📢 [MLS State] Posted Darwin notification: stateChanged")
    logger.debug("   Main app should reload MLS state from disk")
  }
  
  // MARK: - Phase 5: Handshake Notifications
  
  /// Tokenized handshake: the NSE is about to close and checkpoint the database.
  ///
  /// Call this from the Notification Service Extension BEFORE starting the checkpoint/close sequence.
  /// The main app should release its database readers and acknowledge the *token* via the App Group store.
  ///
  /// - Note: This is a static method so it can be called without keeping a reference
  ///   to the notifier instance (important for NSE which has limited lifecycle).
  @discardableResult
  public static func postNSEWillClose(userDID: String) -> UInt64 {
    let logger = Logger(subsystem: "blue.catbird.mls", category: "StateChangeNotifier")

    let request = MLSAppGroupHandshakeStore.shared.issueWillCloseRequest(for: userDID)
    
    let center = CFNotificationCenterGetDarwinNotifyCenter()
    
    CFNotificationCenterPostNotification(
      center,
      CFNotificationName(kMLSNSEWillCloseNotification),
      nil,
      nil,
      true
    )
    
    logger.info("📢 [Handshake] NSE posting nseWillClose (token=\(request.token, privacy: .public))")
    return request.token
  }
  
  /// Post a Darwin Notification that the app has acknowledged the NSE will close.
  ///
  /// This is a doorbell only; correctness comes from `MLSAppGroupHandshakeStore`.
  ///
  /// - Note: This is a static method so it can be called without keeping a reference
  ///   to the notifier instance.
  public static func postAppAcknowledged() {
    let logger = Logger(subsystem: "blue.catbird.mls", category: "StateChangeNotifier")
    
    let center = CFNotificationCenterGetDarwinNotifyCenter()
    
    CFNotificationCenterPostNotification(
      center,
      CFNotificationName(kMLSAppAcknowledgedNotification),
      nil,
      nil,
      true
    )
    
    logger.info("📤 [Handshake] App posting appAcknowledged")
  }

  /// Acknowledge a specific `nseWillClose` token for a user and ring the Darwin doorbell.
  public static func acknowledgeNSEWillClose(userDID: String, token: UInt64) {
    MLSAppGroupHandshakeStore.shared.acknowledge(userDID: userDID, token: token)
    postAppAcknowledged()
  }
  
  /// Wait for the app to acknowledge a specific `nseWillClose` token.
  ///
  /// Call this from the NSE AFTER posting `nseWillClose` and BEFORE starting checkpoint.
  public static func waitForAppAcknowledgment(
    userDID: String,
    token: UInt64,
    timeout: Duration
  ) async -> Bool {
    await MLSAppGroupHandshakeStore.shared.waitForAcknowledgment(
      userDID: userDID,
      token: token,
      timeout: timeout
    )
  }
  
  // MARK: - Observing Notifications (called from Main App)
  
  /// Start observing for MLS state change notifications from NSE.
  ///
  /// Call this during app initialization (e.g., in AppState.initialize or CatbirdApp.init).
  ///
  /// When a notification is received, the callback will be invoked on the main thread.
  /// Use this to trigger `reloadMLSStateFromDisk()` on your MLS conversation manager.
  ///
  /// - Parameter callback: Closure to invoke when state change notification is received.
  ///   This will be called on the main queue.
  public func startObserving(onStateChanged callback: @escaping () -> Void) {
    queue.sync {
      guard !isObserving else {
        logger.debug("Already observing for state change notifications")
        return
      }
      
      self.onStateChanged = callback
      
      // Get the Darwin notification center
      let center = CFNotificationCenterGetDarwinNotifyCenter()
      
      // Register as observer
      // - observer: Pointer to self (bridged)
      // - callBack: C function that will be invoked
      // - name: The notification name
      // - object: nil (not used for Darwin notifications)
      // - suspensionBehavior: .deliverImmediately - we want to know ASAP
      CFNotificationCenterAddObserver(
        center,
        Unmanaged.passUnretained(self).toOpaque(),
        { center, observer, name, object, userInfo in
          // This callback runs on the notification center's thread
          // We need to get back to our instance and invoke the callback
          guard let observer = observer else { return }
          let notifier = Unmanaged<MLSStateChangeNotifier>.fromOpaque(observer).takeUnretainedValue()
          notifier.handleNotificationReceived()
        },
        kMLSStateChangedNotification,
        nil,
        .deliverImmediately
      )
      
      isObserving = true
      logger.info("🔔 [MLS State] Started observing for state change notifications from NSE")
    }
  }
  
  /// Start observing for NSE will close notifications (tokenized handshake).
  ///
  /// Call this during app initialization to enable the handshake protocol.
  /// When the NSE is about to close and checkpoint the database, the handler
  /// is invoked with the persisted request token. If the handler returns `true`,
  /// the notifier records an acknowledgment for the same token and rings the Darwin doorbell.
  public func startObservingNSEWillClose(
    onNSEWillCloseRequest handler: @escaping @MainActor (MLSNSEWillCloseRequest) async -> Bool
  ) {
    queue.sync {
      guard !isObservingNSEWillClose else {
        logger.debug("Already observing for nseWillClose notifications")
        return
      }
      
      self.onNSEWillCloseRequest = handler
      
      let center = CFNotificationCenterGetDarwinNotifyCenter()
      
      CFNotificationCenterAddObserver(
        center,
        Unmanaged.passUnretained(self).toOpaque(),
        { center, observer, name, object, userInfo in
          guard let observer = observer else { return }
          let notifier = Unmanaged<MLSStateChangeNotifier>.fromOpaque(observer).takeUnretainedValue()
          notifier.handleNSEWillCloseReceived()
        },
        kMLSNSEWillCloseNotification,
        nil,
        .deliverImmediately
      )
      
      isObservingNSEWillClose = true
      logger.info("🔔 [Handshake] Started observing for nseWillClose notifications")
    }
  }
  
  /// Stop observing for MLS state change notifications.
  ///
  /// Call this during cleanup/shutdown.
  public func stopObserving() {
    queue.sync {
      let center = CFNotificationCenterGetDarwinNotifyCenter()
      
      if isObserving {
        CFNotificationCenterRemoveObserver(
          center,
          Unmanaged.passUnretained(self).toOpaque(),
          CFNotificationName(kMLSStateChangedNotification),
          nil
        )
        
        // Cancel any pending debounce task
        debounceLock.lock()
        pendingDebounceTask?.cancel()
        pendingDebounceTask = nil
        debounceLock.unlock()
        
        onStateChanged = nil
        isObserving = false
        logger.info("🔕 [MLS State] Stopped observing for state change notifications")
      }
      
      if isObservingNSEWillClose {
        CFNotificationCenterRemoveObserver(
          center,
          Unmanaged.passUnretained(self).toOpaque(),
          CFNotificationName(kMLSNSEWillCloseNotification),
          nil
        )
        
        handshakeLock.lock()
        pendingHandshakeTask?.cancel()
        pendingHandshakeTask = nil
        handshakeLock.unlock()

        onNSEWillCloseRequest = nil
        isObservingNSEWillClose = false
        logger.info("🔕 [Handshake] Stopped observing for nseWillClose notifications")
      }
    }
  }
  
  // MARK: - Private
  
  /// Handle the Darwin notification callback with debouncing
  /// If NSE decrypts multiple messages rapidly, we coalesce the reload requests
  private func handleNotificationReceived() {
    logger.info("📥 [MLS State] Received Darwin notification: stateChanged from NSE")
    logger.info("   NSE advanced the MLS ratchet - main app must reload from disk")
    
    // Debounce rapid notifications to avoid spamming reloads
    debounceLock.lock()
    
    // Cancel any pending debounce task
    pendingDebounceTask?.cancel()
    
    // Create a new debounced task
    pendingDebounceTask = Task { [weak self] in
      guard let self = self else { return }
      
      // Wait for debounce interval
      do {
        try await Task.sleep(nanoseconds: UInt64(self.debounceInterval * 1_000_000_000))
      } catch {
        // Task was cancelled (new notification arrived) - exit silently
        return
      }
      
      // If we got here, the debounce period elapsed - invoke the callback
      guard !Task.isCancelled else { return }
      
      await MainActor.run { [weak self] in
        guard let self = self else { return }
        
        if let callback = self.onStateChanged {
          self.logger.info("🔄 [MLS State] Debounce elapsed - invoking state reload")
          callback()
        } else {
          self.logger.warning("   No callback registered - state reload may not occur!")
        }
      }
    }
    
    debounceLock.unlock()
    logger.debug("   Debounce timer reset (will fire in \(self.debounceInterval)s if no more notifications)")
  }
  
  /// Handle the nseWillClose doorbell (tokenized handshake).
  private func handleNSEWillCloseReceived() {
    logger.info("📥 [Handshake] App received nseWillClose doorbell")

    handshakeLock.lock()
    pendingHandshakeTask?.cancel()

    pendingHandshakeTask = Task { @MainActor [weak self] in
      guard let self else { return }

      do {
        try await Task.sleep(nanoseconds: UInt64(self.handshakeDebounceInterval * 1_000_000_000))
      } catch {
        return
      }
      await self.processPendingWillCloseRequests()
    }

    handshakeLock.unlock()
  }

  @MainActor
  private func processPendingWillCloseRequests() async {
    guard let handler = onNSEWillCloseRequest else {
      logger.warning("   No nseWillClose handler registered!")
      return
    }

    let requests = MLSAppGroupHandshakeStore.shared.allRequests()
    guard !requests.isEmpty else {
      logger.debug("   No handshake requests found in App Group store")
      return
    }

    // Coalesce per-user to the latest token (monotonic, so higher token supersedes).
    var latestByUser: [String: MLSNSEWillCloseRequest] = [:]
    for request in requests {
      if let existing = latestByUser[request.userDID], existing.token >= request.token {
        continue
      }
      latestByUser[request.userDID] = request
    }

    for request in latestByUser.values {
      if MLSAppGroupHandshakeStore.shared.isAcknowledged(userDID: request.userDID, token: request.token) {
        continue
      }

      logger.info("📥 [Handshake] Handling willClose token=\(request.token, privacy: .public) for \(request.userDID.prefix(20), privacy: .private)")

      let released = await handler(request)
      if released {
        MLSStateChangeNotifier.acknowledgeNSEWillClose(userDID: request.userDID, token: request.token)
      } else {
        logger.warning("🚫 [Handshake] Did not release readers in time; not acknowledging token=\(request.token, privacy: .public)")
      }
    }
  }
}

// MARK: - Convenience Extension for Main App Integration

extension MLSStateChangeNotifier {
  
  /// Configure state change observation with an async reload handler.
  ///
  /// This is a convenience method for async/await based apps.
  ///
  /// Usage:
  /// ```swift
  /// MLSStateChangeNotifier.shared.observeWithAsyncHandler {
  ///     await appState.reloadMLSStateFromDisk()
  /// }
  /// ```
  ///
  /// - Parameter handler: Async closure to invoke when state change is detected.
  public func observeWithAsyncHandler(_ handler: @escaping @MainActor () async -> Void) {
    startObserving {
      Task { @MainActor in
        await handler()
      }
    }
  }
  
  /// Configure nseWillClose observation with an async handler (tokenized handshake).
  ///
  /// This is a convenience method for async/await based apps.
  /// If the handler returns `true`, the notifier records an acknowledgment for the token and
  /// rings the Darwin `appAcknowledged` doorbell.
  ///
  /// - Parameter handler: Async closure to invoke for each pending request.
  public func observeNSEWillCloseWithAsyncHandler(
    _ handler: @escaping @MainActor (MLSNSEWillCloseRequest) async -> Bool
  ) {
    startObservingNSEWillClose(onNSEWillCloseRequest: handler)
  }
}
