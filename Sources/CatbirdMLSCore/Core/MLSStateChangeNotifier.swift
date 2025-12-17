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
//  PHASE 5: Darwin Notification Handshake (2024-12)
//  ═══════════════════════════════════════════════════════════════════════════
//
//  Enhanced coordination protocol:
//  1. NSE posts `nseWillClose` before starting checkpoint
//  2. Main app receives, releases readers, posts `appAcknowledged`
//  3. NSE waits for ack (1.5s timeout), proceeds with TRUNCATE checkpoint
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
  
  /// Callback to invoke when NSE will close notification is received (Phase 5)
  private var onNSEWillClose: (() -> Void)?
  
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
  
  // MARK: - Phase 5: Acknowledgment Tracking
  
  /// Flag set when we receive an acknowledgment from the app
  private var receivedAppAcknowledgment: Bool = false
  
  /// Lock for thread-safe acknowledgment tracking
  private let ackLock = NSLock()
  
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
  
  /// Post a Darwin Notification that NSE is about to close and checkpoint the database.
  ///
  /// Call this from the Notification Service Extension BEFORE starting the checkpoint/close sequence.
  /// The main app should release its database readers upon receiving this notification.
  ///
  /// - Note: This is a static method so it can be called without keeping a reference
  ///   to the notifier instance (important for NSE which has limited lifecycle).
  public static func postNSEWillClose() {
    let logger = Logger(subsystem: "blue.catbird.mls", category: "StateChangeNotifier")
    
    let center = CFNotificationCenterGetDarwinNotifyCenter()
    
    CFNotificationCenterPostNotification(
      center,
      CFNotificationName(kMLSNSEWillCloseNotification),
      nil,
      nil,
      true
    )
    
    logger.info("📢 [Handshake] NSE posting nseWillClose")
  }
  
  /// Post a Darwin Notification that the app has acknowledged the NSE will close.
  ///
  /// Call this from the main app AFTER releasing database readers in response to nseWillClose.
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
  
  /// Wait for the app to acknowledge the NSE will close notification.
  ///
  /// Call this from the NSE AFTER posting nseWillClose and BEFORE starting checkpoint.
  /// This blocks (with polling) until either:
  /// 1. The app posts appAcknowledged
  /// 2. The timeout expires
  ///
  /// - Parameter timeout: Maximum time to wait for acknowledgment (seconds)
  /// - Returns: true if acknowledgment was received, false if timed out
  public static func waitForAppAcknowledgment(timeout: TimeInterval) -> Bool {
    let logger = Logger(subsystem: "blue.catbird.mls", category: "StateChangeNotifier")
    
    logger.info("⏳ [Handshake] NSE waiting for app acknowledgment (timeout: \(timeout)s)")
    
    // Reset acknowledgment flag
    shared.ackLock.lock()
    shared.receivedAppAcknowledgment = false
    shared.ackLock.unlock()
    
    // Start observing for acknowledgment
    let center = CFNotificationCenterGetDarwinNotifyCenter()
    
    CFNotificationCenterAddObserver(
      center,
      Unmanaged.passUnretained(shared).toOpaque(),
      { center, observer, name, object, userInfo in
        guard let observer = observer else { return }
        let notifier = Unmanaged<MLSStateChangeNotifier>.fromOpaque(observer).takeUnretainedValue()
        notifier.handleAppAcknowledgmentReceived()
      },
      kMLSAppAcknowledgedNotification,
      nil,
      .deliverImmediately
    )
    
    // Poll for acknowledgment with timeout
    let startTime = Date()
    let pollInterval: TimeInterval = 0.05  // 50ms
    
    while Date().timeIntervalSince(startTime) < timeout {
      shared.ackLock.lock()
      let received = shared.receivedAppAcknowledgment
      shared.ackLock.unlock()
      
      if received {
        // Remove observer
        CFNotificationCenterRemoveObserver(
          center,
          Unmanaged.passUnretained(shared).toOpaque(),
          CFNotificationName(kMLSAppAcknowledgedNotification),
          nil
        )
        
        let duration = Date().timeIntervalSince(startTime)
        let durationStr = String(format: "%.2f", duration)
        logger.info("✅ [Handshake] NSE received acknowledgment in \(durationStr)s")
        return true
      }
      
      Thread.sleep(forTimeInterval: pollInterval)
    }
    
    // Timeout - remove observer and report failure
    CFNotificationCenterRemoveObserver(
      center,
      Unmanaged.passUnretained(shared).toOpaque(),
      CFNotificationName(kMLSAppAcknowledgedNotification),
      nil
    )
    
    logger.warning("⏱️ [Handshake] App acknowledgment timeout - acknowledgment not received")
    return false
  }
  
  /// Handle receiving the app acknowledgment notification
  private func handleAppAcknowledgmentReceived() {
    ackLock.lock()
    receivedAppAcknowledgment = true
    ackLock.unlock()
    
    logger.info("📥 [Handshake] Received app acknowledgment")
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
  
  /// Start observing for NSE will close notifications (Phase 5).
  ///
  /// Call this during app initialization to enable the handshake protocol.
  /// When the NSE is about to close and checkpoint the database, the callback
  /// will be invoked so the app can release its database readers.
  ///
  /// - Parameter callback: Closure to invoke when nseWillClose is received.
  ///   This will be called on the main queue. After releasing readers,
  ///   call `MLSStateChangeNotifier.postAppAcknowledged()` to signal the NSE.
  public func startObservingNSEWillClose(onNSEWillClose callback: @escaping () -> Void) {
    queue.sync {
      guard !isObservingNSEWillClose else {
        logger.debug("Already observing for nseWillClose notifications")
        return
      }
      
      self.onNSEWillClose = callback
      
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
        
        onNSEWillClose = nil
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
  
  /// Handle the nseWillClose notification (Phase 5)
  private func handleNSEWillCloseReceived() {
    logger.info("📥 [Handshake] App received nseWillClose, releasing readers")
    
    // Invoke callback on main queue
    DispatchQueue.main.async { [weak self] in
      guard let self = self else { return }
      
      if let callback = self.onNSEWillClose {
        callback()
      } else {
        self.logger.warning("   No nseWillClose callback registered!")
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
  
  /// Configure nseWillClose observation with an async handler (Phase 5).
  ///
  /// This is a convenience method for async/await based apps.
  /// After releasing readers, the handler should call `MLSStateChangeNotifier.postAppAcknowledged()`.
  ///
  /// Usage:
  /// ```swift
  /// MLSStateChangeNotifier.shared.observeNSEWillCloseWithAsyncHandler {
  ///     await appState.releaseMLSDatabaseReaders()
  ///     MLSStateChangeNotifier.postAppAcknowledged()
  /// }
  /// ```
  ///
  /// - Parameter handler: Async closure to invoke when nseWillClose is detected.
  public func observeNSEWillCloseWithAsyncHandler(_ handler: @escaping @MainActor () async -> Void) {
    startObservingNSEWillClose {
      Task { @MainActor in
        await handler()
      }
    }
  }
}
