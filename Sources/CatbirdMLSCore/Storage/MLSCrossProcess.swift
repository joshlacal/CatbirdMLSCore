//
//  MLSCrossProcess.swift
//  CatbirdMLSCore
//
//  Created on Feb 3, 2026.
//
//  Cross-process notification coordination using Darwin notifications.
//  Replaces advisory file locks to avoid 0xdead10cc crashes.
//
//  Signal pattern: Post notification after writes, other processes listen
//  and invalidate caches. No locks held = nothing for iOS to kill us for.
//

import Foundation

/// Cross-process notification coordination using Darwin notifications.
/// Replaces advisory file locks to avoid 0xdead10cc crashes.
///
/// Signal pattern: Post notification after writes, other processes listen
/// and invalidate caches. No locks held = nothing for iOS to kill us for.
public final class MLSCrossProcess: @unchecked Sendable {

    // MARK: - Singleton

    public static let shared = MLSCrossProcess()

    // MARK: - Properties

    private var observerTokens: [Int32] = []
    private let isMainApp: Bool

    /// Darwin notification names - separate channels for app and extensions
    private static let appChangedName = "blue.catbird.mls.app.changed" as CFString
    private static let nseChangedName = "blue.catbird.mls.nse.changed" as CFString

    /// Callback invoked when another process notifies of changes
    private var onChange: (@Sendable () -> Void)?

    /// Whether we're currently listening for notifications
    private var isListening = false

    // MARK: - Initialization

    private init() {
        // Detect if we're main app or extension
        // Main app bundle path ends with .app
        // Extensions end with .appex
        self.isMainApp = Bundle.main.bundlePath.hasSuffix(".app")
    }

    // MARK: - Public API

    /// Start listening for changes from other processes.
    ///
    /// When another process (main app or extension) calls `notifyChanged()`,
    /// this callback will be invoked on the main queue.
    ///
    /// - Parameter onChange: Callback invoked when another process notifies of changes.
    ///                       Use this to invalidate caches, reload data, etc.
    public func startListening(onChange: @escaping @Sendable () -> Void) {
        guard !isListening else { return }

        self.onChange = onChange
        self.isListening = true

        let center = CFNotificationCenterGetDarwinNotifyCenter()

        // Listen to OTHER process's notifications
        // Main app listens to NSE changes, extensions listen to app changes
        let nameToObserve = isMainApp ? Self.nseChangedName : Self.appChangedName

        CFNotificationCenterAddObserver(
            center,
            Unmanaged.passUnretained(self).toOpaque(),
            { _, observer, _, _, _ in
                guard let observer = observer else { return }
                let crossProcess = Unmanaged<MLSCrossProcess>.fromOpaque(observer).takeUnretainedValue()
                crossProcess.handleNotification()
            },
            nameToObserve,
            nil,
            .deliverImmediately
        )
    }

    /// Notify other processes that we made changes.
    ///
    /// Call this after any write operation that other processes should know about:
    /// - New messages received/decrypted
    /// - Group state changes
    /// - Key updates
    ///
    /// This is fire-and-forget - no locks held, nothing for iOS to terminate us for.
    public func notifyChanged() {
        let center = CFNotificationCenterGetDarwinNotifyCenter()

        // Post OUR notification for others to observe
        // Main app posts app changes, extensions post NSE changes
        let nameToPost = isMainApp ? Self.appChangedName : Self.nseChangedName

        CFNotificationCenterPostNotification(
            center,
            CFNotificationName(nameToPost),
            nil,
            nil,
            true  // deliverImmediately
        )
    }

    /// Stop listening for notifications from other processes.
    ///
    /// Call this on deinit or when shutting down. Safe to call multiple times.
    public func stopListening() {
        guard isListening else { return }

        let center = CFNotificationCenterGetDarwinNotifyCenter()
        CFNotificationCenterRemoveEveryObserver(center, Unmanaged.passUnretained(self).toOpaque())
        onChange = nil
        isListening = false
    }

    // MARK: - Internal

    private func handleNotification() {
        // Dispatch to main queue for safety
        // This ensures UI updates and most coordination happens on main thread
        DispatchQueue.main.async { [weak self] in
            self?.onChange?()
        }
    }

    deinit {
        stopListening()
    }
}

// MARK: - Convenience Extensions

extension MLSCrossProcess {

    /// Whether this process is the main app (vs an extension like NSE)
    public var isRunningAsMainApp: Bool {
        isMainApp
    }

    /// Human-readable description of which process type we are
    public var processTypeDescription: String {
        isMainApp ? "Main App" : "Extension"
    }
}
