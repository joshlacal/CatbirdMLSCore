import Foundation
import Petrel

/// Thread-safe in-memory cache and request coalescer for MLS chat availability.
///
/// Design guarantees:
/// 1. Bounded capacity and freshness: definitive `.available` and `.unavailable` states are
///    cached up to `maxEntries` (LRU eviction) with a configurable TTL (default 300s).
/// 2. Never caches transient errors: `.unknown` availability is NEVER cached so network
///    glitches and timeouts remain immediately retryable.
/// 3. Request coalescing: concurrent lookups for the same DID share a single in-flight task
///    to eliminate duplicate network calls.
/// 4. Fresh queries run concurrently: newly requested DIDs are launched immediately without
///    waiting for existing in-flight tasks of unrelated DIDs to finish.
/// 5. Generation-safe invalidation: `invalidate(did:)` and `clear()` bump generations so late
///    completions of in-flight requests never repopulate stale cache entries.
public actor MLSChatAvailabilityCache {
    private struct Entry: Sendable {
        let availability: MLSAPIClient.MLSChatAvailability
        let expiresAt: Date
    }

    private var cache: [String: Entry] = [:]
    private var accessOrder: [String] = [] // LRU: oldest at index 0, newest at end
    private var inFlight: [String: Task<MLSAPIClient.MLSChatAvailability, Never>] = [:]
    private var didGenerations: [String: UInt64] = [:]
    private var globalGeneration: UInt64 = 0

    public let maxEntries: Int
    public let ttl: TimeInterval

    public init(maxEntries: Int = 1000, ttl: TimeInterval = 300) {
        self.maxEntries = maxEntries
        self.ttl = ttl
    }

    /// Retrieve cached availability for a DID if present and unexpired.
    public func get(did: String) -> MLSAPIClient.MLSChatAvailability? {
        guard let entry = cache[did] else { return nil }
        if entry.expiresAt > Date() {
            touch(did)
            return entry.availability
        } else {
            removeEntry(did: did)
            return nil
        }
    }

    /// Invalidate cache and in-flight tracking for a specific DID.
    /// Increments generation so ongoing in-flight fetches cannot repopulate the cache.
    public func invalidate(did: String) {
        removeEntry(did: did)
        inFlight.removeValue(forKey: did)
        didGenerations[did, default: 0] &+= 1
    }

    /// Invalidate all cached entries and in-flight tracking.
    /// Increments global generation so ongoing in-flight fetches cannot repopulate the cache.
    public func clear() {
        cache.removeAll()
        accessOrder.removeAll()
        inFlight.removeAll()
        globalGeneration &+= 1
    }

    /// Atomically check cache, reuse existing in-flight tasks, and launch a coalesced batch task
    /// for newly requested DIDs before awaiting any results.
    ///
    /// Preserves caller batching for uncached DIDs and runs new queries without waiting for unrelated flights.
    public func resolve(
        dids: [DID],
        fetch: @escaping @Sendable ([DID]) async -> [String: MLSAPIClient.MLSChatAvailability]
    ) async -> [String: MLSAPIClient.MLSChatAvailability] {
        guard !dids.isEmpty else { return [:] }

        // Deduplicate input while preserving first-seen order
        var uniqueDIDs: [DID] = []
        var seen: Set<String> = []
        for did in dids {
            let key = did.didString()
            if seen.insert(key).inserted {
                uniqueDIDs.append(did)
            }
        }

        let now = Date()
        var resolved: [String: MLSAPIClient.MLSChatAvailability] = [:]
        var existingFlightTasks: [String: Task<MLSAPIClient.MLSChatAvailability, Never>] = [:]
        var toFetchDIDs: [DID] = []

        // Synchronous actor phase: check cache and existing in-flight tasks
        for did in uniqueDIDs {
            let key = did.didString()
            if let entry = cache[key] {
                if entry.expiresAt > now {
                    resolved[key] = entry.availability
                    touch(key)
                    continue
                } else {
                    removeEntry(did: key)
                }
            }

            if let task = inFlight[key] {
                existingFlightTasks[key] = task
            } else {
                toFetchDIDs.append(did)
            }
        }

        // Launch new batch fetch immediately BEFORE awaiting existing flights
        var newFlightTasks: [String: Task<MLSAPIClient.MLSChatAvailability, Never>] = [:]
        if !toFetchDIDs.isEmpty {
            let fetchChunk = toFetchDIDs
            let startGlobal = globalGeneration
            var startDidGens: [String: UInt64] = [:]
            for d in fetchChunk {
                let k = d.didString()
                startDidGens[k] = didGenerations[k, default: 0]
            }

            let batchTask = Task<[String: MLSAPIClient.MLSChatAvailability], Never> { [weak self] in
                let resultMap = await fetch(fetchChunk)
                if let self {
                    await self.commitBatchResult(
                        fetchChunk: fetchChunk,
                        resultMap: resultMap,
                        startGlobal: startGlobal,
                        startDidGens: startDidGens
                    )
                }
                return resultMap
            }

            for did in fetchChunk {
                let key = did.didString()
                let didTask = Task<MLSAPIClient.MLSChatAvailability, Never> {
                    let map = await batchTask.value
                    return map[key] ?? .unknown
                }
                inFlight[key] = didTask
                newFlightTasks[key] = didTask
            }
        }

        // Await new flights and existing flights concurrently
        if !newFlightTasks.isEmpty || !existingFlightTasks.isEmpty {
            await withTaskGroup(of: (String, MLSAPIClient.MLSChatAvailability).self) { group in
                for (key, task) in newFlightTasks {
                    group.addTask {
                        (key, await task.value)
                    }
                }
                for (key, task) in existingFlightTasks {
                    group.addTask {
                        (key, await task.value)
                    }
                }
                for await (key, availability) in group {
                    resolved[key] = availability
                }
            }
        }

        return resolved
    }

    // MARK: - Private Helpers

    private func commitBatchResult(
        fetchChunk: [DID],
        resultMap: [String: MLSAPIClient.MLSChatAvailability],
        startGlobal: UInt64,
        startDidGens: [String: UInt64]
    ) {
        for did in fetchChunk {
            let key = did.didString()

            // Generation safety: discard if invalidated during flight
            guard globalGeneration == startGlobal else { continue }
            guard didGenerations[key, default: 0] == startDidGens[key] else { continue }

            inFlight.removeValue(forKey: key)

            let availability = resultMap[key] ?? .unknown
            // Invariant: never cache .unknown
            if availability != .unknown {
                setEntry(did: key, availability: availability)
            }
        }
    }

    private func setEntry(did: String, availability: MLSAPIClient.MLSChatAvailability) {
        guard availability != .unknown else { return }
        cache[did] = Entry(availability: availability, expiresAt: Date().addingTimeInterval(ttl))
        touch(did)
        evictIfNeeded()
    }

    private func touch(_ did: String) {
        if let idx = accessOrder.firstIndex(of: did) {
            accessOrder.remove(at: idx)
        }
        accessOrder.append(did)
    }

    private func removeEntry(did: String) {
        cache.removeValue(forKey: did)
        if let idx = accessOrder.firstIndex(of: did) {
            accessOrder.remove(at: idx)
        }
    }

    private func evictIfNeeded() {
        guard cache.count > maxEntries else { return }

        // First, evict expired entries
        let now = Date()
        var unexpiredOrder: [String] = []
        for key in accessOrder {
            if let entry = cache[key], entry.expiresAt > now {
                unexpiredOrder.append(key)
            } else {
                cache.removeValue(forKey: key)
            }
        }
        accessOrder = unexpiredOrder

        // If still over capacity, evict oldest entries (front of accessOrder)
        while cache.count > maxEntries, !accessOrder.isEmpty {
            let oldestKey = accessOrder.removeFirst()
            cache.removeValue(forKey: oldestKey)
        }
    }
}
