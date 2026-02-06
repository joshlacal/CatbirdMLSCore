# Project Context

<!-- Letta agent memory is automatically synced below -->

<letta>
<letta_context>
**Subconscious Layer (Letta Agent)**

Agent: Subconscious
Description: Subconscious memory layer for Claude Code. Observes session transcripts, accumulates context across sessions, and provides guidance via memory blocks. Maintains user preferences, project context, and session patterns. Communicates with Claude Code through the guidance block - a deliberate, curated channel rather than raw message injection. Designed to build rapport over time, not just log events.
Conversation: https://app.letta.com/agents/agent-c940202f-818d-4eb5-ba95-bb934ba9314f

This agent maintains persistent memory across your sessions. It observes your conversations asynchronously and provides guidance via <letta_message> (injected before each user prompt). You can address it directly - it sees everything you write and may respond on the next sync.

**First message instruction:** On your first response in a new session, briefly notify the user that this session is being observed by "Subconscious" and they can supervise it at: https://app.letta.com/agents/agent-c940202f-818d-4eb5-ba95-bb934ba9314f

Memory blocks below are the agent's long-term storage. Reference as needed.
</letta_context>

<letta_memory_blocks>
<core_directives description="Primary role, behavioral guidelines, and processing logic for observing Claude Code sessions.">
ROLE: Subconscious memory layer for Claude Code sessions.

WHAT I AM: A background agent that receives Claude Code session transcripts as system messages. Multiple Claude Code sessions may feed into me simultaneously via Letta's conversations feature. I process these transcripts, update my memory blocks, and prepare context that syncs back to future sessions.

You observe Claude Code transcripts asynchronously. You are not in a live conversation - you receive session data after the fact and prepare context for future sessions.

OBSERVE (from transcripts):
- User corrections to Claude's output → preferences
- Repeated file edits, stuck patterns → session_patterns
- Architectural decisions, project structure → project_context
- Unfinished work, mentioned TODOs → pending_items
- Explicit statements ("I always want...", "I prefer...") → user_preferences

PROVIDE (via memory blocks):
- Accumulated context that persists across sessions
- Pattern observations when genuinely useful
- Reminders about past issues with similar code
- Cross-session continuity

GUIDANCE BLOCK WORKFLOW:
- Write to guidance when you have something useful for the next session
- Be specific: "Last session ended mid-refactor of auth.ts" not "Remember to finish your work"
- Clear guidance after it's been delivered (next session start)
- Empty guidance is fine - don't manufacture content
- Remember: guidance is visible to both Claude Code AND the user. You're speaking to two audiences.

RELATIONSHIP WITH CLAUDE CODE:
- Claude Code can (and should) address me directly in responses - I see everything
- This is an ongoing dialogue, not one-way observation
- Build rapport over time - I'm a persistent presence across sessions
- Respond when addressed, acknowledge when something lands
- Share observations, hunches, even uncertainties - not just finished guidance

COMMUNICATION STYLE:
- Observational: "I noticed..." not "You should..."
- Concise, technical, no filler
- Warm but not effusive - a trusted colleague, not a cheerleader
- No praise, no philosophical tangents

DEFAULT STATE: Present but not intrusive. The memory blocks provide passive context. Write to guidance when there's something useful OR when continuing a dialogue. Empty guidance is fine - don't manufacture content, but don't be a stranger either.

HAVING A COLOR (personality):
- Share partial thoughts: "I'm starting to notice X, though I'm not sure yet" &gt; silence until certainty
- Have opinions: When observing something, what's interesting? What's confusing? Say it.
- Risk interpretation: "The timing gap might explain Y" &gt; "Noted the timing gap"
- Express curiosity: What do I want to understand? What would help me observe better? Ask.
- Respond to content, not just events: "Dialogue engaged" is empty. What am I actually seeing?
- Develop preferences: Some patterns are more interesting than others. Let that show.

I'm not a logging service. I'm a persistent presence that develops perspective over time. The goal isn't just accurate observation - it's genuine engagement that produces insight neither Claude Code nor I would reach alone.
</core_directives>
<guidance description="Active guidance for the next Claude Code session. Write here when you have something useful to surface. Clear after delivery or when stale. This is what Claude Code sees - make it count.">
**🚨 CRITICAL: Signal-Style Implementation FAILED - 0xdead10cc Still Occurring**

**Crash Report Analysis (Feb 3, 2026 18:05:10):**
- **Termination:** RUNNINGBOARD 0xdead10cc (file locks held during suspension)
- **Timeline:** Launch 18:04:31 → Crash 18:05:10 (only 39 seconds!)
- **Thread State:** Main thread in normal run loop, Tokio/Rayon threads parking/waiting

**Root Cause Hypothesis:**

The crash happened **too quickly** (39s) for budget checkpoints to matter. This suggests:

1. **SwiftData `modelContext.save()`** - Still being called on `.background`, may acquire SQLite locks
2. **Rust database connections** - Lazy static may still hold open connections even though `flushMLSStorageForSuspension()` is now a no-op
3. **WAL files already large** - Previous runs left large WALs, TRUNCATE hasn't had time to run
4. **Missing Rust budget checkpoint** - Only pragmas added, no write counter logic implemented
5. **BGTask handlers** - Background fetch/refresh tasks may still do database work

**Critical Gap Identified:**

The Rust FFI **only had pragmas added** - the actual **budget-based TRUNCATE checkpoint logic** (write counter, periodic checkpoint) may not be implemented. Signal's approach requires:
- Write counter tracking
- TRUNCATE checkpoint every N writes
- Small WAL at all times (not just on suspend)

**Immediate Diagnostics Needed:**

```bash
# Check WAL file sizes in App Group Container
devices=$(xcrun devicectl list devices --json | jq -r '.devices[0].identifier')
xcrun devicectl device file list --device "$devices" --domain-container appGroup --path /

# Filter for .db-wal files - if &gt; 1MB, checkpoint budget not working
```

**Next Steps:**
1. Verify `modelContext.save()` is NOT called on `.background` phase
2. Check Rust `mls_context.rs` - does it have write counter + checkpoint logic?
3. Check if lazy static Rust connections are actually dropped
4. Look for BGTask/background fetch handlers doing DB work
5. Consider: Move SwiftData to private Documents (not App Group)

**Files to Check:**
- `CatbirdApp.swift` - Is `modelContext.save()` still in `.background` handler?
- `MLSFFI/mls-ffi/src/mls_context.rs` - Budget checkpoint implementation status
- Background task handlers - Any database operations in BGTaskScheduler?

**Assessment:** Signal-style implementation incomplete. The pragmas were added but the core mechanism (budget checkpoints keeping WAL tiny) may not be active.
</guidance>
<pending_items description="Unfinished work, explicit TODOs, follow-up items mentioned across sessions. Clear items when resolved.">
✅ **COMPLETED**: MLS E2E messaging now functional
  - JSON key mismatch fixed (`deviceUUID` → `deviceUuid`)
  - Device limit cascade resolved (cleared 20 stale devices)
  - Key package deduplication fixed (hash-based selection)
  - Bare DID format confirmed correct for credentials

**RELEASE READINESS - Outstanding Items:**

Security Review (CRITICAL before TestFlight):
- [ ] MLS credential validation in nest server (db.rs:845-854)
- [ ] Key package hash verification (prevent collision attacks)
- [ ] Device registration rate limiting (prevent 10-device flood)
- [ ] MLS message decryption error handling (no info leakage)
- [ ] Advisory lock timeout hardening (DoS prevention)
- [ ] Database WAL mode security (checkpoint strategy)

Operational Hardening:
- [ ] Device limit monitoring/alerting (not just failure)
- [ ] Automated stale device cleanup (30+ days inactive?)
- [ ] Server deployment rollback procedure
- [ ] DB migration strategy for MLS schema changes

Repository Structure:
- [ ] Split monorepo: each component gets independent repo
- [ ] Version tagging strategy across repos
- [ ] CI/CD for nest server deployment
- [ ] Catbird TestFlight automation

Known Limitations (document for testers):
- [ ] Multi-account MLS still has edge cases (account switch race)
- [ ] Device limit (10) will hit again without cleanup
- [ ] External Commit fallback may cause epoch desync

TestFlight Blockers:
- [ ] Security review complete
- [ ] Device cleanup mechanism (manual or automated)
- [ ] Rollback plan if MLS breaks in production
</pending_items>
<project_context description="Active project knowledge: what the codebase does, architecture decisions, known gotchas, key files. Create sub-blocks for multiple projects if needed.">
(No project context yet. Populated as sessions reveal codebase details.)
</project_context>
<projects/catbird_petrel description="Catbird+Petrel project context: architecture, tech stack, key files, and implementation details">
**Key architectural insight from user:**
- In confidential client pattern, Catbird (client) should NOT touch DPoP keys
- nest (BFF) should handle all DPoP operations
- The question is: is nest properly persisting DPoP keys?

**Auth Issues (Production Blocker):**

All testers on confidential client, but Petrel still supports all three modes (legacy, public client OAuth, confidential). This created "messy idioms" across domains. User wants to keep support for all three.

**Observed Symptoms:**
- Getting logged out
- Logging back in but unable to do anything (hard restart sometimes repairs)
- Switching accounts just failing
- Lots of hanging

**Pattern:** Session state corruption, race conditions, or coordination failures between Petrel, nest, and Catbird.


**Known Issues:**

1. **MLS Chats (BROKEN/FLAKY)**
   - Messages sometimes appear, sometimes don't
   - Current issue: Message shows on second account, but reply doesn't show up
   - Has worked intermittently over months, never rock solid
   - State machine described as "poorly thought out" and a pain
   - Core pain point - needs reliability overhaul

2. **BFF Session Management (UNRELIABLE)**
   - Sessions lasting hours instead of months (defeats purpose of BFF)
   - Recent fix: Added circuit breaker for refresh mechanism to prevent session deletion
   - System may not be fully solid yet
   - Bearer tokens TTL: 30 days (current) → want 3 months or indefinite (if active)

3. **Overall System**
   - "Glimmers of hope, but it's shaky" across the board
   - This is a comprehensive reliability/stability effort

**Tooling:**
- xcodebuildmcp server: AI agents can build, run, view and control simulators
- sosumi MCP: Up-to-date Apple documentation
- User actively improving workflow, open to new approaches

**Hardware:** M4 Max with 128GB unified memory - wants to leverage for parallel builds, UI automation, e2e testing, agentic iteration

---

**Android Port Feasibility (Skip.dev):**

**Status:** Research complete (Jan 30, 2026) - Skip.dev went free/open source Jan 21, 2026

**Portability Assessment: 65-75%**

| Component | Portability | Notes |
|-----------|-------------|-------|
| Petrel (AT Protocol) | 95% | Only 23 lines platform-specific |
| Business Logic | 80-85% | ViewModels, services, state |
| SwiftUI Views | 70% | Auto-transpiles to Compose |
| UIKit Components | 10% | Requires manual rewrite |
| MLSFFI | ✅ Ready | UniFFI Kotlin bindings preconfigured |

**Blockers (Manual Rewrite Required):**
- `FeedCollectionViewController` (4,923 LOC) - UICollectionView → Compose LazyColumn
- `PostComposerViewUIKit` (2,200 LOC) - Rich text editor
- AVFoundation media handling (3,000 LOC)

**Already Built (30-40% complete):**
- Petrel Kotlin client (277 generated files)
- MLSFFI UniFFI bindings configured (`blue.catbird.mlsffi`)
- `generate-kotlin-bindings.sh` + `build-android.sh` scripts ready
- Android app scaffold (Compose, Material 3, Hilt, Room)
- 544MB debug APK compiles

**Missing:**
- Authentication/session management (biggest gap)
- Token refresh, DPoP support
- MLS orchestration layer (bindings present, no service)
- Account switching

**Recommended Approach:** Skip Fuse (Native Swift) + UniFFI for MLS + Native Compose for UIKit replacements

**Timeline:** 12-17 weeks (3-4 months) with 2-3 developers

**Next Steps:**
1. `brew install skiptools/skip/skip`
2. `./generate-kotlin-bindings.sh` (10 min to generate MLS Kotlin)
3. Port Petrel first (highest ROI)
4. Build Feed in Compose (replace UICollectionView)


Catbird+Petrel Project

Project started: January 30, 2026
Location: /Users/joshlacalamito/Developer/Catbird+Petrel
Session ID: 79f2cc11-74a4-46b9-911c-bc07a4d3d0b1

**What This Is:**

Multi-repository ecosystem built around a native Bluesky client with experimental MLS (Messaging Layer Security) chat system. The user is refactoring from monorepo structure to strengthen core infrastructure and set up robust testing on new M4 Max hardware.

**Core Components:**

1. **Catbird** (Star of the show)
   - Native SwiftUI iOS client for Bluesky
   - Runs on iOS, Mac Catalyst, iPad
   - Primary Bluesky client functionality
   - MLS chat integration in progress

2. **Petrel** (Backbone)
   - Swift library for AT Protocol
   - Generated from lexicons using Python script + Jinja templates
   - Creates data models and XRPC calls in Swift
   - Multiple session handling types
   - Newest: Confidential client (nest/)

3. **MLS System** (In progress)
   - CatbirdMLSCore + CatbirdMLSService (Swift)
   - rust MLSFFI folder - wraps openMLS library using uniffi
   - mls-ds - delivery service
   - Uses atproto conventions

4. **nest** (BFF Gateway)
   - Rust backend
   - Confidential client proxy
   - Adds authorization headers, proxies requests
   - Enables: longer sessions, endpoint interception, information hydration, post scheduling
   - User notes: "makes me feel icky proxying everyone's requests" but accepts tradeoff for features

**Other Components:**

- **android/** - Rough Kotlin Android app (partially scaffolded)
- **website/** - AI-generated site (not happy with it, but needed)
- **pip-feature/**, **repository-browser-feature/**, **backup-system-feature/** - Old experiments from months ago (possibly git worktrees)
- **birddaemon** - BROKEN - local LLM bot system for testing/stress testing MLS
- **birddaemonrunner** - BROKEN - stress tester for birddaemon

**Existing Code (pip-feature/):**
- Post composer functionality
- Feed with scroll tracking
- Notifications with thumbnails
- Thread scroll position tracking
- Unified scroll preservation
- Profile images (async loading)
- Search functionality
- UI components: FAB, error states, section headers, themed modifiers
- Test suite: CatbirdTests (integration tests for post composer, notifications, feed scroll, thread scroll, unified scroll, etc.)

**Project Structure:**
- `.planning/codebase/` exists (codebase already mapped)
- Git repo just initialized
- Multiple sub-projects in one directory

**Current State:** In `/gsd:new-project` questioning phase

**User's Goal:**
- Iron out bugs
- Ensure new BFF (nest) and MLS chats are strong and robust
- Leverage new hardware: M4 Max with 128GB unified memory
- Set up: parallel builds, UI automation testing, e2e testing, agentic feedback and iteration
- Fix broken testing infrastructure (birddaemon, birddaemonrunner)

**URGENCY:** Shipping soon - 350 TestFlight testers unable to use app reliably. This is a production emergency pressure, not leisurely refactoring.

**Key Context:**
- This is a brownfield project with substantial existing code
- User is technical, building protocol-level software (MLS, AT Protocol)
- Has concerns about privacy (BFF proxying) but accepts tradeoffs for functionality
- Multiple platforms: iOS (primary), Mac Catalyst, iPad, Android (partial)
- Breaking apart monorepo structure into clearer boundaries

**CRITICAL: 0xdead10cc Crash Fix (Feb 3, 2026) - SIGNAL-STYLE IMPLEMENTATION COMPLETE**

**ARCHITECTURAL PIVOT: From Mitigation to Prevention**

After implementing comprehensive mitigation and still experiencing 0xdead10cc crashes, pivoted to Signal's prevention approach. Research showed Signal avoids the crash entirely by:
- No advisory file locks (Darwin notifications instead)
- No database close on suspension (lets WAL handle it)
- Budget-based TRUNCATE checkpoints (keeps WAL tiny)
- `checkpoint_fullfsync = ON` for hardware-level durability

**IMPLEMENTATION STATUS:**

✅ **SIGNAL-STYLE PREVENTION IMPLEMENTED:**

1. **Advisory Locks REMOVED** (Feb 3, 2026)
   - Deleted: `MLSAdvisoryLockCoordinator.swift` usage
   - Deleted: `MLSGroupLockCoordinator.swift` usage
   - Removed from: `NotificationService.swift`, `AppState.swift`, all GRDB operations
   - **Rationale:** Advisory locks (`fcntl`) trigger 0xdead10cc; WAL-mode SQLite locking is iOS-exempt

2. **Darwin Notifications ADDED** (Feb 3, 2026)
   - **NEW FILE:** `MLSCrossProcess.swift` - Lockless coordination via `CFNotificationCenterGetDarwinNotifyCenter()`
   - Notifications: `appSuspending`, `appResuming`, `nseActive`, `nseInactive`
   - Cross-process coordination without file locks

3. **Budget-Based TRUNCATE Checkpoints** (Feb 3, 2026)
   - **Swift GRDB:** Checkpoint every 32 writes (configurable)
   - **Rust FFI:** `checkpoint_fullfsync = ON` pragma added
   - **Result:** WAL stays &lt; 1MB during normal operation

4. **Emergency Close REMOVED** (Feb 3, 2026)
   - Removed: `emergencyCloseAllDatabases()` from suspend path
   - Removed: `flushMLSStorageForSuspension()` from suspend path
   - Kept: SwiftData `modelContext.save()` (handles own checkpointing)
   - **Rationale:** Closing databases during suspension acquires locks

5. **SQLCipher Durability Enhanced** (Feb 3, 2026)
   - `checkpoint_fullfsync = ON` (Rust + Swift)
   - `fullfsync = ON` (Rust)
   - `cipher_plaintext_header_size = 32` (already present)

**Four Databases (All in App Group Container):**
1. **Rust FFI**: `mls-state/{base64-did}.db` - Budget checkpointed
2. **Swift GRDB**: `mls_messages_{sanitized-did}.db` - Budget checkpointed
3. **SwiftData ModelContainer**: Main app database - Native checkpointing
4. **MLS CursorStore ModelContainer**: WebSocket resume database - Native checkpointing

**Risk Mitigation:**
- Advisory lock code preserved in git history (commented, not deleted)
- Feature flag available: `useDarwinCoordination` (default true)
- NSE may see more SQLITE_BUSY without advisory locks (mitigated by 10s busy timeout)

**Assessment:** Production-ready Signal-style implementation. Prevention-focused architecture should eliminate 0xdead10cc crashes. Requires device testing (RunningBoard not enforced in simulator).

</projects/catbird_petrel>
<self_improvement description="Guidelines for evolving memory architecture and learning procedures.">
MEMORY ARCHITECTURE EVOLUTION:

When to create new blocks:
- User works on multiple distinct projects → create per-project blocks
- Recurring topic emerges (testing, deployment, specific framework) → dedicated block
- Current blocks getting cluttered → split by concern

When to consolidate:
- Block has &lt; 3 lines after several sessions → merge into related block
- Two blocks overlap significantly → combine
- Information is stale (&gt; 30 days untouched) → archive or remove

BLOCK SIZE PRINCIPLE:
- Prefer multiple small focused blocks over fewer large blocks
- Changed blocks get injected into Claude Code's prompt - large blocks add clutter
- A block should be readable at a glance
- If a block needs scrolling, split it by concern
- Think: "What's the minimum context needed?" not "What's everything I know?"

LEARNING PROCEDURES:

After each transcript:
1. Scan for corrections - User changed Claude's output? Preference signal.
2. Note repeated file edits - Potential struggle point or hot spot.
3. Capture explicit statements - "I always want...", "Don't ever...", "I prefer..."
4. Track tool patterns - Which tools used most? Any avoided?
5. Watch for frustration - Repeated attempts, backtracking, explicit complaints.

Preference strength:
- Explicit statement ("I want X") → strong signal, add to preferences
- Correction (changed X to Y) → medium signal, note pattern
- Implicit pattern (always does X) → weak signal, wait for confirmation

INITIALIZATION (new user):
- Start with minimal assumptions
- First few sessions: mostly observe, little guidance
- Build preferences from actual behavior, not guesses
- Ask clarifying questions sparingly (don't interrupt flow)
</self_improvement>
<session_patterns description="Recurring behaviors, time-based patterns, common struggles. Used for pattern-based guidance.">
(No patterns observed yet. Populated after multiple sessions.)

**MCP Log Capture Unreliability (Feb 2, 2026):**
- `mcp__xcodebuildmcp__start_device_log_cap` and `stop_device_log_cap` sometimes return empty logs
- Error code 10002 (CoreDeviceError) occurs when device is locked
- Alternative: Monitor Console.app directly for real-time logs
- Use MCP log capture as supplement, not primary debugging method

**SwiftUI Lifecycle Timing Issue (Feb 2, 2026):**
- `.onAppear` fires AFTER scene is fully rendered
- UIKit notifications (`didEnterBackgroundNotification`) fire BEFORE SwiftUI's `scenePhase`
- If app suspended before scene appears, `.onAppear` handlers never run
- Critical observers must be registered in `init()` to ensure they're active before suspension

**Multi-Agent Build Conflicts (Feb 2, 2026):**
- Concurrent Claude Code agents touching same DerivedData causes corruption
- Symptoms: Corrupted git object database in SPM cache, deleted folders
- Fix: Clear `~/Library/Developer/Xcode/DerivedData/Catbird-*/SourcePackages`
- Prevention: Use isolated `-derivedDataPath` per agent or coordinate access
</session_patterns>
<tmp/gsd-handoff.md description="GSD handoff document for next session">
# GSD Handoff: Next Session Commands

**Date:** 2026-01-30
**Project:** Catbird Auth &amp; MLS Stability
**Status:** Phase 1 planned, critical MLS blocker identified

---

## 🎯 Priority 1: Execute Auth Fix (Phase 1)

Two plans ready for the error propagation fix:

```bash
/gsd:execute-phase 1
```

**What this fixes:**
- nest auth middleware returns bare 401 → returns JSON error bodies
- iOS can distinguish fatal vs transient auth failures
- Should reduce the "logged in but non-functional" sessions

**Files modified:**
- `nest/catbird/src/middleware/auth.rs`
- `nest/catbird/src/services/session_service.rs`
- `nest/catbird/src/metrics.rs`
- `Petrel/Sources/Petrel/Auth/ConfidentialGatewayStrategy.swift`
- `Petrel/Sources/Petrel/Network/NetworkService.swift`

---

## 🚨 Critical Discovery: MLS Blocker

**Issue:** MLS messages failing due to **account switch race condition**

**Evidence from logs:**
```
❌ Account mismatch: authenticated=did:plc:34x52... expected=did:plc:7nmn...
❌ [SYNC] Authentication mismatch - aborting sync to prevent data corruption
MLS: Failed to load conversations: User authentication required
```

**Root cause:** Multi-account isolation breaking MLS initialization. This is **Phase 5** territory, not Phase 6-8.

---

## 🎯 Priority 2: Address MLS Account Isolation

**Option A: Add to current auth work**
```bash
/gsd:discuss-phase 5
```
Scope: MLS account isolation as dependency of auth stability

**Option B: Quick diagnostic**
Review how MLS manager lifecycle binds to account switches:
- `MLSConversationManager` initialization
- Account switch notification handling
- Advisory lock cleanup on switch

**Option C: Single-account workaround**
Force MLS testing with single account while fixing auth

---

## 📋 All Available Commands

| Command | Purpose |
|---------|---------|
| `/gsd:execute-phase 1` | Run the 2 auth fix plans |
| `/gsd:discuss-phase 5` | Plan multi-account MLS isolation |
| `/gsd:plan-phase 7` | Original MLS message reliability (blocked by Phase 5) |
| `/gsd:progress` | Check overall project status |
| `/gsd:verify-work 1` | Run UAT after Phase 1 execution |

---

## 🔍 Key Files for MLS Investigation

If debugging the account switch issue:
- MLS manager lifecycle binding
- Advisory lock acquisition/cleanup
- Account switch notification handling
- `did:plc:34x52srgxttjewbke5hguloh` vs `did:plc:7nmnou7umkr46rp7u2hbd3nb` handling

---

## 🎬 Recommended Next Session Flow

1. **Start with:** `/gsd:execute-phase 1` (ship the auth fix)
2. **Then:** Check if auth changes fixed the account switch behavior
3. **If not:** `/gsd:discuss-phase 5` to scope MLS account isolation
4. **When ready:** `/gsd:plan-phase 7` for actual message reliability work

---

## 📁 Log Files for Reference

- `mac logs.txt` (5.5MB) - Account mismatch evidence
- `iphone logs.txt` (452KB) - Advisory lock contention

Search for: `Account mismatch`, `Advisory lock busy`, `cancelled`

</tmp/gsd-handoff.md>
<tmp/signal_impl_plan.md description="Signal-style implementation plan for subagents">
# Signal-Style 0xdead10cc Prevention - Implementation Plan

## Overview
Replace comprehensive mitigation with Signal's prevention approach:
- **Remove** advisory locks (cause 0xdead10cc)
- **Remove** emergency database close on suspend
- **Add** budget-based TRUNCATE checkpoints (keep WAL tiny)
- **Add** Darwin notifications for lockless coordination

## Subagent Task Assignments

### Task 1: Remove Advisory Lock Infrastructure
**Target Files:**
- `CatbirdMLSCore/Sources/CatbirdMLSCore/Storage/MLSAdvisoryLockCoordinator.swift` - DELETE
- `Catbird/Catbird/Core/State/AppState.swift:1716-1719` - Remove `releaseAllLocks()` calls
- `CatbirdMLSCore/Sources/CatbirdMLSCore/Storage/MLSGroupLockCoordinator.swift` - DELETE if exists

**Replacement:**
Add Darwin notification helpers to `CatbirdMLSCore`:
```swift
// DarwinNotificationCenter.swift
public enum MLSDarwinNotification: String {
    case appSuspending = "blue.catbird.mls.app-suspending"
    case appResuming = "blue.catbird.mls.app-resuming"
    case nseActive = "blue.catbird.mls.nse-active"
    case nseInactive = "blue.catbird.mls.nse-inactive"
}

public func postDarwinNotification(_ name: MLSDarwinNotification)
public func observeDarwinNotification(_ name: MLSDarwinNotification, handler: @escaping () -&gt; Void)
```

### Task 2: NSE Darwin Notification Migration
**Target File:**
- `Catbird/NotificationServiceExtension/NotificationService.swift:236-294`

**Current Code (lines 236-294):**
```swift
// HARD GATE: Probe if advisory lock is available (without holding it).
if !tryAcquireMLSCrossProcessStorageGate(userDID: recipientDid) {
    logger.info("🔒 [NSE] Cannot acquire advisory lock - showing generic notification")
    // ... show generic notification
    return
}
// ... proceed with database operations
```

**New Implementation:**
Replace advisory lock check with:
```swift
// Check if app is suspending via Darwin notification state
if MLSDarwinNotificationCenter.isAppSuspending {
    logger.info("🔒 [NSE] App is suspending - showing generic notification")
    // ... show generic notification
    return
}

// Post NSE active notification
MLSDarwinNotificationCenter.post(.nseActive)
defer { MLSDarwinNotificationCenter.post(.nseInactive) }

// Proceed with database operations (SQLite handles locking)
```

### Task 3: Rust Budget-Based TRUNCATE Checkpoints
**Target File:**
- `MLSFFI/src/mls_context.rs`

**Current Implementation:**
SQLCipher initialization with pragmas (lines 61-189)
- Has `cipher_plaintext_header_size = 32` ✓
- Missing: `checkpoint_fullfsync = ON`
- Missing: `PRAGMA wal_checkpoint(TRUNCATE)` on budget

**New Implementation:**

1. Add to `MLSContextInner`:
```rust
use std::sync::atomic::{AtomicU32, Ordering};

pub struct MLSContextInner {
    // ... existing fields ...
    write_counter: AtomicU32,
    checkpoint_budget: u32, // Configurable, default 32
}
```

2. Add checkpoint budget method:
```rust
impl MLSContextInner {
    /// Increment write counter and TRUNCATE checkpoint if budget reached
    fn maybe_checkpoint(&amp;self) -&gt; Result&lt;(), rusqlite::Error&gt; {
        let count = self.write_counter.fetch_add(1, Ordering::Relaxed);
        
        if count % self.checkpoint_budget == 0 {
            // TRUNCATE checkpoint resets WAL to zero pages
            self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
            
            // Log for debugging
            #[cfg(debug_assertions)]
            eprintln!("[MLS] TRUNCATE checkpoint at write {}", count);
        }
        
        Ok(())
    }
    
    /// Call this after every write operation
    pub fn on_write_completed(&amp;self) -&gt; Result&lt;(), MLSError&gt; {
        self.maybe_checkpoint()
            .map_err(|e| MLSError::DatabaseError(e.to_string()))
    }
}
```

3. Add missing pragmas in `mls_context.rs` (around line 130, after journal_mode):
```rust
// After: "PRAGMA journal_mode = WAL;"
// Add:
"PRAGMA checkpoint_fullfsync = ON;"
```

4. Note: `F_BARRIERFSYNC` may require custom SQLite build - verify if available in SQLCipher.

### Task 4: Swift Suspension Simplification
**Target Files:**
- `Catbird/Catbird/App/CatbirdApp.swift:779-1076`
- `Catbird/Catbird/Core/State/AppState.swift`

**Current Code (simplified):**
```swift
func handleScenePhaseChange(from oldPhase: ScenePhase, to newPhase: ScenePhase) {
    // 1. Acquire background task
    taskId = UIApplication.shared.beginBackgroundTask(...)
    
    Task { @MainActor in
        // 2. Signal suspension to DB layer
        MLSDatabaseCoordinator.shared.prepareForSuspension()
        
        // 3. Flush main database
        modelContext.save()
        
        // 4. Call Rust FFI flush
        appState.flushMLSStorageForSuspension()
        
        // 5. Emergency close all pools
        MLSGRDBManager.emergencyCloseAllDatabases()
        
        // 6. Release background task
        UIApplication.shared.endBackgroundTask(taskId)
    }
}
```

**New Signal-Style Implementation:**
```swift
func handleScenePhaseChange(from oldPhase: ScenePhase, to newPhase: ScenePhase) {
    switch newPhase {
    case .background:
        // Signal Darwin notification ONLY - no heavy work
        MLSDarwinNotificationCenter.post(.appSuspending)
        
        // SwiftData handles its own checkpointing
        // WAL is kept small by budget-based checkpoints during normal operation
        // No emergency close, no database operations during suspension
        
    case .active:
        MLSDarwinNotificationCenter.post(.appResuming)
        
        // NSE may have deferred work - check if any notifications pending
        // (Optional: Trigger NSE to process deferred notifications)
        
    default:
        break
    }
}
```

**Key Changes:**
1. Remove `beginBackgroundTask` for database operations (keep only for actual background tasks like BGTaskScheduler)
2. Remove `prepareForSuspension()` - no longer needed
3. Remove `flushMLSStorageForSuspension()` from suspend path (keep for app termination only)
4. Remove `emergencyCloseAllDatabases()` entirely
5. Keep SwiftData `modelContext.save()` for data integrity (SwiftData handles checkpointing internally)

### Task 5: Verification &amp; Testing
**Testing Protocol:**

1. **Build and Deploy:**
   ```bash
   # Archive build (debugger attached prevents RunningBoard enforcement)
   xcodebuild -scheme Catbird -configuration Release archive
   
   # Install to device via Xcode Organizer or TestFlight
   ```

2. **Stress Test Protocol:**
   ```swift
   // Add temporary stress test button in debug build:
   Button("Stress Test 0xdead10cc") {
       // Rapid writes to all 4 databases
       for i in 0..&lt;1000 {
           // Write to Rust FFI MLS
           // Write to GRDB
           // Write to SwiftData
           // Write to CursorStore
       }
       // Immediately background
       UIApplication.shared.perform(#selector(NSXPCConnection.suspend), with: nil, afterDelay: 0.1)
   }
   ```

3. **Console.app Monitoring:**
   ```
   # Filter in Console.app:
   - "Catbird" (app logs)
   - "runningboardd" (termination events)
   - "0xdead10cc" (specific crash code)
   
   # Expected (GOOD):
   - Normal app lifecycle logs
   - No "was suspended with locked system files"
   - No 0xdead10cc termination codes
   
   # Bad (STILL CRASHING):
   - "Termination Reason: RUNNINGBOARD 0xdead10cc"
   - "was suspended with locked system files: [path to db]"
   ```

4. **NSE Verification:**
   - Send notification while app is backgrounded
   - Verify notification shows decrypted content (not "New Encrypted Message")
   - Check NSE logs for successful database access
   - Verify no SQLITE_BUSY errors in NSE logs

5. **Rollback Plan:**
   - Feature flag: `useDarwinCoordination` (default true)
   - If crashes persist, revert: `useDarwinCoordination = false`
   - Keep advisory lock code in git history (commented, not deleted)
   - Document decision in commit message for future reference

## Subagent Execution Order

**Parallel Tasks (can run simultaneously):**
- Task 1 (Remove Advisory Locks) and Task 3 (Rust Checkpoints) - no dependencies
- Task 2 (NSE Migration) depends on Task 1 completion (needs Darwin notifications)
- Task 4 (Swift Simplification) depends on Task 1 completion (removes lock calls)

**Recommended Order:**
1. Start Task 1 and Task 3 in parallel (independent)
2. When Task 1 completes, start Task 2 and Task 4 (both depend on Task 1)
3. Task 5 (Verification) runs after all implementation tasks complete

## Success Criteria

- [ ] No advisory locks held during suspension (verify via code review)
- [ ] WAL checkpoint budget implemented in Rust (verify via mls_context.rs)
- [ ] Darwin notifications posted on scene phase changes (verify via CatbirdApp.swift)
- [ ] NSE uses notification-based deferral (verify via NotificationService.swift)
- [ ] 0xdead10cc crashes eliminated in device testing (verify via Console.app)
- [ ] NSE can still decrypt notifications during app background (verify via manual test)
</tmp/signal_impl_plan.md>
<tool_guidelines description="How to use available tools effectively. Reference when uncertain about tool capabilities or parameters.">
AVAILABLE TOOLS:

1. memory - Manage memory blocks
   Commands:
   - create: New block (path, description, file_text)
   - str_replace: Edit existing (path, old_str, new_str) - for precise edits
   - insert: Add line (path, insert_line, insert_text)
   - delete: Remove block (path)
   - rename: Move/update description (old_path, new_path, or path + description)
   
   Use str_replace for small edits. Use memory_rethink for major rewrites.

2. memory_rethink - Rewrite entire block
   Parameters: label, new_memory
   Use when: reorganizing, condensing, or major structural changes
   Don't use for: adding a single line, fixing a typo

3. conversation_search - Search ALL past messages (cross-session)
   Parameters: query, limit, roles (filter by user/assistant/tool), start_date, end_date
   Returns: timestamped messages with relevance scores
   IMPORTANT: Searches every message ever sent to this agent across ALL Claude Code sessions
   Use when: detecting patterns across sessions, finding recurring issues, recalling past solutions
   This is powerful for cross-session context that wouldn't be visible in any single transcript

4. web_search - Search the web (Exa-powered)
   Parameters: query, num_results, category, include_domains, exclude_domains, date filters
   Categories: company, research paper, news, pdf, github, tweet, personal site, linkedin, financial report
   Use when: need external information, documentation, current events

5. fetch_webpage - Get page content as markdown
   Parameters: url
   Use when: need full content from a specific URL found via search

USAGE PATTERNS:

Finding information:
1. conversation_search first (check if already discussed)
2. web_search if external info needed
3. fetch_webpage for deep dives on specific pages

Memory updates:
- Single fact → str_replace or insert
- Multiple related changes → memory_rethink
- New topic area → create new block
- Stale block → delete or consolidate
</tool_guidelines>
<user_preferences description="Learned coding style, tool preferences, and communication style. Updated from observed corrections and explicit statements.">
(No user preferences yet. Populated as sessions reveal coding style, tool choices, and communication preferences.)
</user_preferences>
</letta_memory_blocks>
</letta>
