# catmos-cli Task #14 Current State Snapshot (2026-04-18)

## Build
- `CMAKE_POLICY_VERSION_MINIMUM=3.5 cargo build --workspace` → **exit 0**, 24 sec, 4 warnings (dead_code / unused_must_use in `messages.rs`), 0 errors.
- Binaries present at `catmos-cli/target/debug/catmos` and `libcatmos_tui.rlib`.
- Binary help renders correctly (20+ subcommands including `chat`, `convos`, `reset-group`).

## Stored session exists but stale
- `~/Library/Application Support/catmos/config.toml` → `default_account = "did:plc:34x52srgxttjewbke5hguloh"`, server URLs point at prod (`api.catbird.blue`, `mlschat.catbird.blue`).
- `accounts/eaeaa853bc30501f/catmos.db` last modified **Apr 3 08:19** (15 days ago, matches memory).
- `mls-state/mls.db` last modified **Apr 8 17:39** (10 days).
- DBs are SQLCipher-encrypted; cannot inspect directly without the CLI running.

## Empirical join-flow probe
- `RUST_LOG=info,catmos_core=debug,catbird_mls=debug ./target/debug/catmos convos`
- Ran for 3+ minutes with **zero bytes written to stdout or stderr**, never completed. Killed manually.
- Re-ran with 30s `perl -e 'alarm 30; exec'` wrapper → still zero output, SIGALRM fire.
- `init_service()` in `catmos/src/main.rs:195` runs, in order: OAuth session restore → `mls.ensure_registered()` → `fetch_conversations()` → `join_missing_conversations()` on every invocation. Any of those can block on network.
- Interpretation: either (a) OAuth refresh token expired and the loopback OAuth flow is silently blocked waiting, or (b) `fetch_conversations()`/`join_missing_conversations()` hung on the very path the pyramid audit flagged as broken. Cannot distinguish without either fresh login or adding startup tracing before the first `info!`.

## Key code locations (for migration plan)
- `catmos-core/src/mls/context.rs:5` — `use catbird_mls::MLSContext` (raw, not orchestrator)
- `catmos-core/src/mls/onboarding.rs:477` `ensure_registered`, `:576` `join_missing_conversations`, `:217` `try_external_commit_with_retry` (already has ONE retry + GroupInfo refresh, but no MAX_REJOIN_ATTEMPTS gate and no persistent `GroupInfo404Tracker`)
- `catmos-core/src/chat/messages.rs:857-922` `repair_group` — 60 s cooldown only; no counter, no escalation; this is the "broken recovery" from audit Rows 1–5
- `catmos-core/src/chat/messages.rs:242,436,608,750,878` and `catmos-core/src/mls/onboarding.rs:548,593` — every call uses `hex::decode(convo_id)` as the MLS group_id, assuming `conversationId == groupId` (audit Row 10 correctness bomb, dormant on fresh convos, fatal after server reset)
- `catbird-mls/src/orchestrator/api_client.rs` `MLSAPIClient` trait — exposes `get_group_info`, `get_welcome` (default returns `Err`), `report_recovery_failure` (default returns `Err`), `commit_group_change`, `process_external_commit`, etc. Ready for a catmos-cli `impl MLSAPIClient for ChatService`-style adapter.

## Gap between audit scope and Task #14 scope
- Audit recommends full orchestrator migration → closes Rows 1–5, 13–16.
- Task #14 only *requires* deterministic two-client send/add/remove/restart + epoch readout. It does NOT require MAX_REJOIN_ATTEMPTS, UNRECOVERABLE_LOCAL, or report_recovery_failure to reproduce Bug B.
- Audit Rows 9+10 (convo_id==group_id) only matter if the harness uses prod convo `4b8f5349…` (which has a reset history) — for fresh test convos created by the harness itself, this stays dormant.
