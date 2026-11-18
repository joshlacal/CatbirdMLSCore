import Darwin
import Foundation
#if DEBUG
@_spi(Fixtures) import Petrel
#else
import Petrel
#endif

/// Shared SSRF, NSID, query-encoding, redirect-blocking, and streaming response size
/// defenses for outbound HTTP requests in `CatbirdMLSCore`.
enum MLSOutboundHTTPValidator {
  enum ValidationError: Error, LocalizedError, Equatable {
    case nonHTTPS
    case missingHost
    case credentialsNotAllowed
    case fragmentNotAllowed
    case disallowedPort(Int)
    case disallowedHost(String)
    case malformedEndpoint
    case disallowedNSID(String)
    case malformedQuery

    var errorDescription: String? {
      switch self {
      case .nonHTTPS:
        return "Service endpoint must use https"
      case .missingHost:
        return "Service endpoint is missing a host"
      case .credentialsNotAllowed:
        return "Service endpoint must not contain userinfo credentials"
      case .fragmentNotAllowed:
        return "Service endpoint must not contain a URL fragment"
      case .disallowedPort(let port):
        return "Service endpoint uses disallowed port \(port)"
      case .disallowedHost(let host):
        return "Service endpoint host '\(host)' is not a permitted public address"
      case .malformedEndpoint:
        return "Service endpoint URL is malformed"
      case .disallowedNSID(let nsid):
        return "Prepared request NSID '\(nsid)' is not permitted"
      case .malformedQuery:
        return "Query string contains invalid RFC 3986 characters"
      }
    }
  }

  static let allowedPreparedRequestNamespacePrefixes: [String] = [
    "blue.catbird.chat.",
    "blue.catbird.mlsChat.",
    "blue.catbird.mls.",
    "blue.catbird.labyrinth.",
    "blue.catbird.push.",
    "com.atproto.space.",
  ]

  /// Exact `com.atproto.*` procedures/queries the native core sends: ATProto Space attachment
  /// blob staging (`atproto#5187`) and public `blue.catbird.labyrinth.mailboxKeys` CAS/discovery.
  /// Destructive or unrelated repo endpoints (`applyWrites`, etc.) remain blocked; `deleteRecord`
  /// passes only in the shape `mailboxKeysDeletionRepo(method:body:)` accepts.
  static let allowedPreparedRequestExactNSIDs: Set<String> = [
    "com.atproto.repo.uploadBlob",
    "com.atproto.repo.createRecord",
    "com.atproto.repo.putRecord",
    "com.atproto.repo.getRecord",
    "com.atproto.repo.listRecords",
    "com.atproto.sync.getRecord",
    "com.atproto.sync.getBlocks",
    "com.atproto.sync.getLatestCommit",
    "com.atproto.sync.getRepo",
  ]

  /// NSIDs permitted only as a `GET` to the signed-in account's OWN PDS origin
  /// (the origin the transport's `ATProtoClient` or configured `ownPdsEndpoint` is bound to).
  /// Circle owner/member DID resolution and native archive Space reads ask the user's PDS first
  /// (`com.atproto.identity.resolveDid`); the Rust layer falls back to plc.directory / did:web on
  /// its own path. The same NSID to any other host, or with any other method, is rejected so the
  /// prepared-request channel cannot be used to probe arbitrary hosts.
  static let ownPDSOnlyExactNSIDs: Set<String> = [
    "com.atproto.identity.resolveDid",
  ]

  /// Validates a prepared request's NSID against its route. Own-PDS-only NSIDs
  /// pass only as a `GET` whose URL is same-origin with `ownPDSOrigin`; every
  /// other NSID goes through `validatePreparedRequestNSID(_:)` unchanged.
  static func validatePreparedRequestNSID(
    _ nsid: String,
    method: String,
    requestURL: URL,
    ownPDSOrigin: URL?
  ) throws {
    try validatePreparedRequestNSID(
      nsid,
      method: method,
      requestURL: requestURL,
      ownPDSOrigins: ownPDSOrigin.map { [$0] } ?? []
    )
  }

  static func validatePreparedRequestNSID(
    _ nsid: String,
    method: String,
    requestURL: URL,
    ownPDSOrigins: [URL]
  ) throws {
    guard ownPDSOnlyExactNSIDs.contains(nsid) else {
      try validatePreparedRequestNSID(nsid)
      return
    }
    guard method.uppercased() == "GET",
          ownPDSOrigins.contains(where: { isSameOrigin(requestURL, $0) })
    else {
      throw ValidationError.disallowedNSID(nsid)
    }
    try validatePreparedRequestNSID(nsid, permittingOwnPDSOnly: true)
  }

  /// Scheme, host and effective port equality (default ports filled in).
  static func isSameOrigin(_ lhs: URL, _ rhs: URL) -> Bool {
    guard let lScheme = lhs.scheme?.lowercased(),
          let rScheme = rhs.scheme?.lowercased(),
          lScheme == rScheme,
          let lHost = lhs.host?.lowercased(),
          let rHost = rhs.host?.lowercased(),
          lHost == rHost
    else {
      return false
    }
    let defaultPort: Int? = (lScheme == "https") ? 443 : (lScheme == "http" ? 80 : nil)
    return (lhs.port ?? defaultPort) == (rhs.port ?? defaultPort)
  }

  static let deleteRecordNSID = "com.atproto.repo.deleteRecord"

  /// The only permitted `com.atproto.repo.deleteRecord`: native Labyrinth account deletion
  /// (`delete_all_mailbox_keys`) removing one public `blue.catbird.labyrinth.mailboxKeys` record
  /// (`self` or a 12-digit per-epoch copy) under `swapRecord` CAS. Returns the target repo DID,
  /// which the caller must bind to the signed-in session before sending.
  static func mailboxKeysDeletionRepo(method: String, body: Data?) throws -> String {
    guard method == "POST",
          let body,
          let object = (try? JSONSerialization.jsonObject(with: body)) as? [String: Any],
          Set(object.keys) == ["repo", "collection", "rkey", "swapRecord"],
          object["collection"] as? String == "blue.catbird.labyrinth.mailboxKeys",
          let rkey = object["rkey"] as? String,
          rkey == "self" || (rkey.utf8.count == 12 && rkey.utf8.allSatisfy { $0 >= 0x30 && $0 <= 0x39 }),
          let swapRecord = object["swapRecord"] as? String, !swapRecord.isEmpty,
          let repo = object["repo"] as? String, repo.hasPrefix("did:")
    else {
      throw ValidationError.disallowedNSID(deleteRecordNSID)
    }
    return repo
  }

  /// Validates a DID-derived `#catbird_labyrinth` or `#atproto_pds` service endpoint against SSRF,
  /// scheme downgrade, non-default ports, fragments, userinfo, and local/private IP ranges.
  static func validatePublicHTTPSServiceEndpoint(_ url: URL) throws {
    #if DEBUG && canImport(Network) && canImport(Security)
    if let fixture = DebugFixtureTransport.current {
      guard fixture.permits(url) else { throw ValidationError.disallowedHost(url.host ?? "") }
      return
    }
    #endif

    let rawString = url.absoluteString
    if rawString.utf8.contains(UInt8(ascii: "#")) {
      throw ValidationError.fragmentNotAllowed
    }
    if rawString.utf8.contains(where: { $0 <= 0x20 || $0 >= 0x7F || $0 == UInt8(ascii: "\\") }) {
      throw ValidationError.malformedEndpoint
    }

    guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
      throw ValidationError.malformedEndpoint
    }
    guard components.scheme?.lowercased() == "https" else {
      throw ValidationError.nonHTTPS
    }
    guard components.user == nil,
          components.password == nil,
          components.percentEncodedUser == nil,
          components.percentEncodedPassword == nil
    else {
      throw ValidationError.credentialsNotAllowed
    }
    guard components.fragment == nil, components.percentEncodedFragment == nil else {
      throw ValidationError.fragmentNotAllowed
    }
    if let port = components.port, port != 443 {
      throw ValidationError.disallowedPort(port)
    }

    let lowerEncodedPath = components.percentEncodedPath.lowercased()
    if lowerEncodedPath.contains("%2e")
      || lowerEncodedPath.contains("%2f")
      || lowerEncodedPath.contains("%5c")
      || lowerEncodedPath.contains("%00")
      || lowerEncodedPath.contains("%23")
    {
      throw ValidationError.malformedEndpoint
    }
    let pathSegments = components.path.split(separator: "/", omittingEmptySubsequences: true)
    if pathSegments.contains(where: { $0 == "." || $0 == ".." }) {
      throw ValidationError.malformedEndpoint
    }

    guard let rawHost = components.host, !rawHost.isEmpty else {
      throw ValidationError.missingHost
    }

    var host = rawHost.lowercased()
    if host.hasPrefix("[") && host.hasSuffix("]") {
      host = String(host.dropFirst().dropLast())
    }
    guard !host.isEmpty,
          !host.utf8.contains(UInt8(ascii: "%")),
          !host.utf8.contains(where: { $0 <= 0x20 || $0 >= 0x7F })
    else {
      throw ValidationError.disallowedHost(rawHost)
    }

    var addr6 = in6_addr()
    if host.withCString({ inet_pton(AF_INET6, $0, &addr6) }) == 1 {
      let bytes = withUnsafeBytes(of: addr6) { Array($0) }
      guard isPublicUnicastIPv6(bytes) else {
        throw ValidationError.disallowedHost(rawHost)
      }
      return
    }
    if host.utf8.contains(UInt8(ascii: ":")) {
      throw ValidationError.disallowedHost(rawHost)
    }

    if host.hasPrefix(".") || host.contains("..") {
      throw ValidationError.disallowedHost(rawHost)
    }
    let normalizedHost = host.hasSuffix(".") ? String(host.dropLast()) : host
    guard !normalizedHost.isEmpty else {
      throw ValidationError.disallowedHost(rawHost)
    }

    if normalizedHost == "localhost"
      || normalizedHost.hasSuffix(".localhost")
      || normalizedHost == "local"
      || normalizedHost.hasSuffix(".local")
      || normalizedHost == "internal"
      || normalizedHost.hasSuffix(".internal")
      || normalizedHost == "localdomain"
      || normalizedHost.hasSuffix(".localdomain")
      || normalizedHost == "home.arpa"
      || normalizedHost.hasSuffix(".home.arpa")
    {
      throw ValidationError.disallowedHost(rawHost)
    }

    var addr4 = in_addr()
    if normalizedHost.withCString({ inet_pton(AF_INET, $0, &addr4) }) == 1 {
      let octets = normalizedHost.split(separator: ".", omittingEmptySubsequences: false)
      guard octets.count == 4,
            octets.allSatisfy({ !$0.isEmpty && ($0.count == 1 || !$0.hasPrefix("0")) })
      else {
        throw ValidationError.disallowedHost(rawHost)
      }
      let hostOrder = UInt32(bigEndian: addr4.s_addr)
      guard isPublicUnicastIPv4(hostOrder) else {
        throw ValidationError.disallowedHost(rawHost)
      }
      return
    }

    let labels = normalizedHost.split(separator: ".", omittingEmptySubsequences: false)
    if let tld = labels.last,
       tld.allSatisfy({ $0.isASCII && $0.isNumber }) || tld.hasPrefix("0x")
    {
      // Blocks shorthand/decimal/octal/hex IPv4 literals (e.g., 127.1, 2130706433, 0x7f000001).
      throw ValidationError.disallowedHost(rawHost)
    }

    for label in labels {
      guard !label.isEmpty,
            label.count <= 63,
            !label.hasPrefix("-"),
            !label.hasSuffix("-"),
            label.utf8.allSatisfy({
              ($0 >= 0x61 && $0 <= 0x7A) || ($0 >= 0x30 && $0 <= 0x39) || $0 == 0x2D
            })
      else {
        throw ValidationError.disallowedHost(rawHost)
      }
    }
  }

  static func isValidPublicHTTPSServiceEndpoint(_ url: URL) -> Bool {
    (try? validatePublicHTTPSServiceEndpoint(url)) != nil
  }

  static func isPublicUnicastIPv4(_ hostOrderAddr: UInt32) -> Bool {
    let b0 = UInt8((hostOrderAddr >> 24) & 0xFF)
    let b1 = UInt8((hostOrderAddr >> 16) & 0xFF)
    let b2 = UInt8((hostOrderAddr >> 8) & 0xFF)

    if b0 == 0 { return false } // 0.0.0.0/8 unspecified / "this network"
    if b0 == 10 { return false } // 10.0.0.0/8 RFC1918
    if b0 == 100 && (b1 & 0xC0) == 64 { return false } // 100.64.0.0/10 CGNAT
    if b0 == 127 { return false } // 127.0.0.0/8 loopback
    if b0 == 169 && b1 == 254 { return false } // 169.254.0.0/16 link-local / metadata
    if b0 == 172 && (b1 & 0xF0) == 16 { return false } // 172.16.0.0/12 RFC1918
    if b0 == 192 && b1 == 0 && b2 == 0 { return false } // 192.0.0.0/24 IETF protocol assignments
    if b0 == 192 && b1 == 168 { return false } // 192.168.0.0/16 RFC1918
    if b0 == 198 && (b1 == 18 || b1 == 19) { return false } // 198.18.0.0/15 benchmark
    if b0 >= 224 { return false } // 224.0.0.0/4 multicast + 240.0.0.0/4 reserved/broadcast
    return true
  }

  static func isPublicUnicastIPv6(_ bytes: [UInt8]) -> Bool {
    guard bytes.count == 16 else { return false }
    // :: unspecified
    if bytes.allSatisfy({ $0 == 0 }) { return false }
    // ::1 loopback
    if bytes.dropLast().allSatisfy({ $0 == 0 }) && bytes[15] == 1 { return false }
    // ::ffff:0:0/96 IPv4-mapped IPv6
    if bytes[0..<10].allSatisfy({ $0 == 0 }) && bytes[10] == 0xFF && bytes[11] == 0xFF {
      return false
    }
    // ::/96 IPv4-compatible IPv6
    if bytes[0..<12].allSatisfy({ $0 == 0 }) { return false }
    // ::ffff:0:0:0/96 SIIT IPv4-translated
    if bytes[0..<8].allSatisfy({ $0 == 0 })
      && bytes[8] == 0xFF && bytes[9] == 0xFF && bytes[10] == 0 && bytes[11] == 0
    {
      return false
    }
    // 64:ff9b::/96 NAT64 well-known prefix
    if bytes[0] == 0x00 && bytes[1] == 0x64 && bytes[2] == 0xFF && bytes[3] == 0x9B
      && bytes[4..<12].allSatisfy({ $0 == 0 })
    {
      return false
    }
    // fc00::/7 Unique Local Address (ULA)
    if (bytes[0] & 0xFE) == 0xFC { return false }
    // fe80::/10 link-local
    if bytes[0] == 0xFE && (bytes[1] & 0xC0) == 0x80 { return false }
    // fec0::/10 deprecated site-local
    if bytes[0] == 0xFE && (bytes[1] & 0xC0) == 0xC0 { return false }
    // ff00::/8 multicast
    if bytes[0] == 0xFF { return false }
    return true
  }

  /// Validates strict ATProto NSID syntax AND restricts to Catbird MLS/Labyrinth namespaces.
  /// Own-PDS-only NSIDs are rejected here; they are admitted only through the
  /// route-aware overload, which checks method and origin first.
  static func validatePreparedRequestNSID(_ nsid: String) throws {
    try validatePreparedRequestNSID(nsid, permittingOwnPDSOnly: false)
  }

  private static func validatePreparedRequestNSID(_ nsid: String, permittingOwnPDSOnly: Bool) throws {
    guard !nsid.isEmpty,
          nsid.count <= 317,
          !nsid.contains(".."),
          !nsid.utf8.contains(where: {
            $0 <= 0x20 || $0 >= 0x7F
              || $0 == UInt8(ascii: "/")
              || $0 == UInt8(ascii: "\\")
              || $0 == UInt8(ascii: "%")
              || $0 == UInt8(ascii: "#")
              || $0 == UInt8(ascii: "?")
          }),
          allowedPreparedRequestNamespacePrefixes.contains(where: { nsid.hasPrefix($0) })
            || allowedPreparedRequestExactNSIDs.contains(nsid)
            || (permittingOwnPDSOnly && ownPDSOnlyExactNSIDs.contains(nsid))
    else {
      throw ValidationError.disallowedNSID(nsid)
    }

    let segments = nsid.split(separator: ".", omittingEmptySubsequences: false)
    guard segments.count >= 4, let nameSegment = segments.last, !nameSegment.isEmpty else {
      throw ValidationError.disallowedNSID(nsid)
    }

    for authority in segments.dropLast() {
      guard !authority.isEmpty,
            authority.count <= 63,
            !authority.hasPrefix("-"),
            !authority.hasSuffix("-"),
            authority.utf8.allSatisfy({
              ($0 >= 0x41 && $0 <= 0x5A)
                || ($0 >= 0x61 && $0 <= 0x7A)
                || ($0 >= 0x30 && $0 <= 0x39)
                || $0 == 0x2D
            })
      else {
        throw ValidationError.disallowedNSID(nsid)
      }
    }

    guard let firstNameByte = nameSegment.utf8.first,
          (firstNameByte >= 0x41 && firstNameByte <= 0x5A)
            || (firstNameByte >= 0x61 && firstNameByte <= 0x7A),
          nameSegment.count <= 63,
          nameSegment.utf8.allSatisfy({
            ($0 >= 0x41 && $0 <= 0x5A)
              || ($0 >= 0x61 && $0 <= 0x7A)
              || ($0 >= 0x30 && $0 <= 0x39)
          }),
          (try? NSID(nsidString: nsid)) != nil
    else {
      throw ValidationError.disallowedNSID(nsid)
    }
  }

  /// Safely parses an RFC 3986 percent-encoded query string into `[URLQueryItem]` without
  /// triggering Foundation's fatal trap on invalid characters (such as `#` or malformed `%`).
  static func parsePercentEncodedQueryItems(_ rawQuery: String) throws -> [URLQueryItem] {
    let bytes = Array(rawQuery.utf8)
    var index = 0
    while index < bytes.count {
      let byte = bytes[index]
      if byte == UInt8(ascii: "%") {
        guard index + 2 < bytes.count,
              isASCIIHexDigit(bytes[index + 1]),
              isASCIIHexDigit(bytes[index + 2])
        else {
          throw ValidationError.malformedQuery
        }
        index += 3
        continue
      }
      guard isRFC3986QueryByte(byte) else {
        throw ValidationError.malformedQuery
      }
      index += 1
    }

    var components = URLComponents()
    components.percentEncodedQuery = rawQuery
    return components.queryItems ?? []
  }

  private static func isASCIIHexDigit(_ byte: UInt8) -> Bool {
    (byte >= 0x30 && byte <= 0x39)
      || (byte >= 0x41 && byte <= 0x46)
      || (byte >= 0x61 && byte <= 0x66)
  }

  private static func isRFC3986QueryByte(_ byte: UInt8) -> Bool {
    // unreserved: ALPHA / DIGIT / "-" / "." / "_" / "~"
    if (byte >= 0x41 && byte <= 0x5A)
      || (byte >= 0x61 && byte <= 0x7A)
      || (byte >= 0x30 && byte <= 0x39)
      || byte == 0x2D || byte == 0x2E || byte == 0x5F || byte == 0x7E
    {
      return true
    }
    // sub-delims: "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="
    switch byte {
    case 0x21, 0x24, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x3B, 0x3D:
      return true
    // pchar / query extras: ":" / "@" / "/" / "?"
    case 0x3A, 0x40, 0x2F, 0x3F:
      return true
    default:
      return false
    }
  }
}

/// Ephemeral `URLSession` wrapper that refuses all `3xx` redirects (preventing credential/DPoP
/// forwarding across redirects) and enforces a streaming `maxBytes` cap before buffering.
final class MLSBoundedURLSession: @unchecked Sendable {
  static let labyrinthMaxBytes = 5 * 1024 * 1024
  static let pdsRecordMaxBytes = 1 * 1024 * 1024

  enum BoundedFetchError: Error, LocalizedError, Equatable {
    case responseTooLarge(limit: Int)
    case nonHTTPResponse
    case timedOut(seconds: TimeInterval)

    var errorDescription: String? {
      switch self {
      case .responseTooLarge(let limit):
        return "HTTP response exceeded maximum size limit of \(limit) bytes"
      case .nonHTTPResponse:
        return "URL response was not an HTTPURLResponse"
      case .timedOut(let seconds):
        return "HTTP request timed out after \(seconds) seconds"
      }
    }
  }

  static let shared = MLSBoundedURLSession()

  private let sessionDelegate: Delegate
  private let delegateQueue: OperationQueue
  let underlyingSession: URLSession

  convenience init(protocolClasses: [AnyClass]? = nil) {
    let configuration = URLSessionConfiguration.ephemeral
    if let protocolClasses { configuration.protocolClasses = protocolClasses }
    self.init(configuration: configuration, trustDelegate: nil)
  }

  private init(configuration: URLSessionConfiguration, trustDelegate: (any URLSessionDelegate)?) {
    let delegate = Delegate(trustDelegate: trustDelegate)
    let underlyingQueue = DispatchQueue(
      label: "blue.catbird.mls.bounded-url-session",
      qos: .userInitiated,
      attributes: .concurrent
    )
    let queue = OperationQueue()
    queue.name = "blue.catbird.mls.bounded-url-session"
    queue.qualityOfService = .userInitiated
    queue.underlyingQueue = underlyingQueue

    configuration.httpCookieStorage = nil
    configuration.urlCredentialStorage = nil
    configuration.httpShouldSetCookies = false
    configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
    self.sessionDelegate = delegate
    self.delegateQueue = queue
    self.underlyingSession = URLSession(
      configuration: configuration,
      delegate: delegate,
      delegateQueue: queue
    )
  }

  #if DEBUG && canImport(Network) && canImport(Security)
  // Petrel installs exactly one immutable fixture per process. Its session supplies the
  // no-failover CONNECT configuration and pinned CA, while our delegate preserves streaming limits.
  private static let fixtureSessionLock = NSLock()
  nonisolated(unsafe) private static var fixtureSession: MLSBoundedURLSession?
  #endif

  private func session(for request: URLRequest) throws -> MLSBoundedURLSession {
    #if DEBUG && canImport(Network) && canImport(Security)
    if let fixture = DebugFixtureTransport.current {
      guard let url = request.url, fixture.permits(url) else { throw URLError(.unsupportedURL) }
      return try Self.fixtureSessionLock.withLock {
        if let session = Self.fixtureSession { return session }
        let pinned = try fixture.makeSession()
        defer { pinned.finishTasksAndInvalidate() }
        guard let trustDelegate = pinned.delegate else { throw URLError(.secureConnectionFailed) }
        let session = MLSBoundedURLSession(configuration: pinned.configuration, trustDelegate: trustDelegate)
        Self.fixtureSession = session
        return session
      }
    }
    #endif
    return self
  }

  func invalidateAndCancel() {
    underlyingSession.invalidateAndCancel()
  }

  func data(for request: URLRequest, maxBytes: Int) async throws -> (Data, HTTPURLResponse) {
    precondition(maxBytes > 0, "maxBytes must be positive")
    let session = try session(for: request)
    let task = session.underlyingSession.dataTask(with: request)
    return try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        session.sessionDelegate.register(
          taskIdentifier: task.taskIdentifier,
          maxBytes: maxBytes,
          target: .async(continuation)
        )
        task.resume()
      }
    } onCancel: {
      task.cancel()
    }
  }

  /// Synchronously executes a bounded, redirect-free HTTP request directly via `URLSession`'s
  /// dedicated `OperationQueue` (`delegateQueue`), without spawning any `Task` or `Task.detached`
  /// onto Swift's bounded cooperative thread pool (`Fault Line 4`).
  func performSync(
    for request: URLRequest,
    maxBytes: Int,
    timeoutSeconds: TimeInterval
  ) throws -> (Data, HTTPURLResponse) {
    precondition(!Thread.isMainThread, "MLSBoundedURLSession.performSync must never block the main thread")
    precondition(maxBytes > 0, "maxBytes must be positive")
    precondition(timeoutSeconds > 0, "timeoutSeconds must be positive")

    let waiter = SyncWaiter()
    let session = try session(for: request)
    let task = session.underlyingSession.dataTask(with: request)
    session.sessionDelegate.register(
      taskIdentifier: task.taskIdentifier,
      maxBytes: maxBytes,
      target: .sync(waiter)
    )
    task.resume()

    guard let outcome = waiter.wait(timeoutSeconds: timeoutSeconds) else {
      session.sessionDelegate.remove(taskIdentifier: task.taskIdentifier)
      task.cancel()
      throw BoundedFetchError.timedOut(seconds: timeoutSeconds)
    }
    return try outcome.get()
  }

  /// Performs a bounded, redirect-free fetch using `MLSBoundedURLSession.shared` by default,
  /// while supporting caller-injected custom `URLSession` instances (such as test fixtures)
  /// with task-level redirect blocking and byte-limit enforcement.
  static func fetchData(
    for request: URLRequest,
    maxBytes: Int,
    session: URLSession
  ) async throws -> (Data, HTTPURLResponse) {
    if session === shared.underlyingSession {
      return try await shared.data(for: request, maxBytes: maxBytes)
    }
    let (data, response) = try await session.data(
      for: request,
      delegate: NoRedirectTaskDelegate.shared
    )
    guard let http = response as? HTTPURLResponse else {
      throw BoundedFetchError.nonHTTPResponse
    }
    guard data.count <= maxBytes else {
      throw BoundedFetchError.responseTooLarge(limit: maxBytes)
    }
    return (data, http)
  }

  private final class NoRedirectTaskDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    static let shared = NoRedirectTaskDelegate()

    func urlSession(
      _ session: URLSession,
      task: URLSessionTask,
      willPerformHTTPRedirection response: HTTPURLResponse,
      newRequest request: URLRequest,
      completionHandler: @escaping (URLRequest?) -> Void
    ) {
      completionHandler(nil)
    }
  }

  fileprivate final class SyncWaiter: @unchecked Sendable {
    private let lock = NSLock()
    private let semaphore = DispatchSemaphore(value: 0)
    private var completed = false
    private var result: Result<(Data, HTTPURLResponse), Error>?

    func finish(_ outcome: Result<(Data, HTTPURLResponse), Error>) {
      lock.lock()
      guard !completed else {
        lock.unlock()
        return
      }
      completed = true
      result = outcome
      lock.unlock()
      semaphore.signal()
    }

    func wait(timeoutSeconds: TimeInterval) -> Result<(Data, HTTPURLResponse), Error>? {
      let nanos = Int64(max(1.0, timeoutSeconds * 1_000_000_000.0))
      if semaphore.wait(timeout: .now() + .nanoseconds(Int(clamping: nanos))) == .timedOut {
        lock.lock()
        if completed, let result {
          lock.unlock()
          return result
        }
        completed = true
        lock.unlock()
        return nil
      }
      lock.lock()
      defer { lock.unlock() }
      return result
    }
  }

  private final class Delegate: NSObject, URLSessionDataDelegate, @unchecked Sendable {
    private let trustDelegate: (any URLSessionDelegate)?

    init(trustDelegate: (any URLSessionDelegate)?) {
      self.trustDelegate = trustDelegate
    }

    func urlSession(
      _ session: URLSession,
      didReceive challenge: URLAuthenticationChallenge,
      completionHandler: @escaping @Sendable (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
      if let delegate = trustDelegate, let handle = delegate.urlSession(_:didReceive:completionHandler:) {
        handle(session, challenge, completionHandler)
      } else {
        completionHandler(.performDefaultHandling, nil)
      }
    }

    enum CompletionTarget {
      case async(CheckedContinuation<(Data, HTTPURLResponse), Error>)
      case sync(SyncWaiter)

      func succeed(_ value: (Data, HTTPURLResponse)) {
        switch self {
        case let .async(continuation):
          continuation.resume(returning: value)
        case let .sync(waiter):
          waiter.finish(.success(value))
        }
      }

      func fail(_ error: Error) {
        switch self {
        case let .async(continuation):
          continuation.resume(throwing: error)
        case let .sync(waiter):
          waiter.finish(.failure(error))
        }
      }
    }

    private struct TaskState {
      let maxBytes: Int
      let target: CompletionTarget
      var response: URLResponse?
      var buffer: Data = Data()
      var exceededLimit: Int?
    }

    private let lock = NSLock()
    private var tasks: [Int: TaskState] = [:]

    func register(
      taskIdentifier: Int,
      maxBytes: Int,
      target: CompletionTarget
    ) {
      lock.lock()
      tasks[taskIdentifier] = TaskState(maxBytes: maxBytes, target: target)
      lock.unlock()
    }

    func remove(taskIdentifier: Int) {
      lock.lock()
      _ = tasks.removeValue(forKey: taskIdentifier)
      lock.unlock()
    }

    func urlSession(
      _ session: URLSession,
      task: URLSessionTask,
      willPerformHTTPRedirection response: HTTPURLResponse,
      newRequest request: URLRequest,
      completionHandler: @escaping (URLRequest?) -> Void
    ) {
      lock.lock()
      if var state = tasks[task.taskIdentifier] {
        state.response = response
        tasks[task.taskIdentifier] = state
      }
      lock.unlock()
      completionHandler(nil)
    }

    func urlSession(
      _ session: URLSession,
      dataTask: URLSessionDataTask,
      didReceive response: URLResponse,
      completionHandler: @escaping (URLSession.ResponseDisposition) -> Void
    ) {
      var shouldCancel = false
      lock.lock()
      if var state = tasks[dataTask.taskIdentifier] {
        state.response = response
        if response.expectedContentLength > Int64(state.maxBytes) {
          state.exceededLimit = state.maxBytes
          shouldCancel = true
        }
        tasks[dataTask.taskIdentifier] = state
      }
      lock.unlock()

      if shouldCancel {
        completionHandler(.cancel)
        dataTask.cancel()
      } else {
        completionHandler(.allow)
      }
    }

    func urlSession(
      _ session: URLSession,
      dataTask: URLSessionDataTask,
      didReceive data: Data
    ) {
      var shouldCancel = false
      lock.lock()
      if var state = tasks[dataTask.taskIdentifier] {
        if state.buffer.count + data.count > state.maxBytes {
          state.exceededLimit = state.maxBytes
          shouldCancel = true
        } else {
          state.buffer.append(data)
        }
        tasks[dataTask.taskIdentifier] = state
      }
      lock.unlock()

      if shouldCancel {
        dataTask.cancel()
      }
    }

    func urlSession(
      _ session: URLSession,
      task: URLSessionTask,
      didCompleteWithError error: Error?
    ) {
      lock.lock()
      let state = tasks.removeValue(forKey: task.taskIdentifier)
      lock.unlock()

      guard let state else { return }
      if let limit = state.exceededLimit {
        state.target.fail(BoundedFetchError.responseTooLarge(limit: limit))
        return
      }
      if let error {
        state.target.fail(error)
        return
      }
      guard let http = (state.response ?? task.response) as? HTTPURLResponse else {
        state.target.fail(BoundedFetchError.nonHTTPResponse)
        return
      }
      state.target.succeed((state.buffer, http))
    }
  }
}
