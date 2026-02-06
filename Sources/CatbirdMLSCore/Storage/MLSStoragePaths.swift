import Foundation

/// Shared storage path resolver for MLS components.
public enum MLSStoragePaths {
  private static let appGroupIdentifier = "group.blue.catbird.shared"
  private static let lock = NSLock()
  private static var overrideURL: URL?

  /// Override the base container used for MLS storage (process-local).
  public static func setBaseDirectoryOverride(_ url: URL?) {
    lock.lock()
    overrideURL = url
    lock.unlock()
  }

  /// Resolve the base container for MLS storage.
  public static func baseContainerURL() -> URL {
    lock.lock()
    let override = overrideURL
    lock.unlock()

    if let override {
      return override
    }

    if let shared = FileManager.default.containerURL(
      forSecurityApplicationGroupIdentifier: appGroupIdentifier
    ) {
      return shared
    }

    return FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
  }
}
