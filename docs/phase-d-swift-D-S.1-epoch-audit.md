# Phase D-Swift D-S.1 — Local epoch source-of-truth audit

**Disposition:** No code change required. The bug the plan targeted does
not exist at the cited site.

## Plan's hypothesis

The D-S.0 → D-S.1 task plan said:

> *Symptom from May 2026 log:* `EPOCH-RECOVERY: Processed commit for
> epoch=2, FFI now at 1` immediately after `MLSClient.processCommit Success
> - newEpoch: 2`. **Swift's cached `localEpoch` is being read by recovery
> code while the FFI authoritative epoch has already advanced.**

The proposed fix was to delete every Swift-side `localEpoch` cache or
gate it with a refresh-on-merge assertion.

## Audit findings

Every Swift-side `localEpoch` read in
`CatbirdMLSCore/Sources/CatbirdMLSCore/Service/` is one of:

1. **A local variable freshly assigned from `mlsClient.getEpoch(...)`** —
   FFI-direct read, scope-local to the function. Examples:
   - `MLSConversationManager+Messaging.swift:323` (preSendSync)
   - `MLSConversationManager+Messaging.swift:483` (sendMessage post-lock)
   - `MLSConversationManager+Messaging.swift:1289` (EPOCH-RECOVERY trigger)
   - `MLSConversationManager+Messaging.swift:1553` (EPOCH-RECOVERY loop post-merge)
   - `MLSConversationManager+DeliveryAcks.swift:108, 230, 365` (delivery ack epoch)
   - `MLSConversationManager+Sync.swift:796` (sync routine)
2. **A parameter into `MLSMessageValidator`** — the validator's input;
   never cached past the function call.
3. **A throwaway `var localEpoch: UInt64 = 0` initializer that is
   immediately reassigned from FFI** — e.g.
   `MLSConversationManager+Messaging.swift:1761` (debug log)
   and `MLSClient.swift:1153` (epoch warning in External Commit).

**There is no stored property anywhere in CatbirdMLSCore that caches
`localEpoch` between calls.** The closest is
`MLSConversationModel.epoch` (a `Int64` on the GRDB row), which IS
cached, but it's used as the SERVER-VIEW epoch for UI display — not as
input to the decrypt path. The decrypt path consistently consults
`mlsClient.getEpoch(...)`.

## What the May 2026 symptom most likely was

`fetchAndProcessMissingCommits` at
`MLSConversationManager+Messaging.swift:1480-1600` reads `currentEpoch`
via FFI immediately after a successful `processCommit`. If the FFI
returned `1` after processing a commit the server said was "for epoch 2",
the cause is one of:

- **Duplicate commit application** — the commit had already been merged
  via PRE-SEND-SYNC, NSE, or an earlier EPOCH-RECOVERY round. The
  second `processCommit` returns a no-op (the FFI is idempotent for
  already-merged commits), and `getEpoch` correctly reports the current
  state (still epoch 1 if the commit advanced from epoch 0 to 1 in the
  prior application — meaning the "for epoch 2" was actually "to be
  applied at epoch 1, advancing to epoch 2", and the prior application
  had also been at the same target).

This is **D-S.2's territory**: the merged-commit tracker landed in
`Sources/CatbirdMLSCore/Service/MergedCommitTracker.swift` and is wired
into `processCommit(groupId:commitData:)` at function entry. Once a
commit hash is recorded as merged this session, subsequent
`processCommit` calls for the same hash short-circuit before invoking
the FFI. After D-S.2 the May 2026 symptom should not recur from this
path, because the second attempt won't reach the FFI at all.

## Conclusion

- D-S.1 produces **no code changes**.
- The audit confirms the "single source of truth" invariant the plan
  wanted is **already in place**: every Swift-side decrypt-path epoch
  read goes through `mlsClient.getEpoch(...)` (FFI direct).
- The May 2026 reproduction symptom is plausibly addressed by D-S.2's
  in-process commit dedup. Verification requires running the E2E
  scenario (D-S.5b TODO) against a build with D-S.2 + D-S.3 + D-S.4.
- If after D-S.5b validation the symptom recurs, the controller may
  revisit whether the FFI itself has an epoch-reporting bug that needs
  a Rust-side fix.

Audited at commit `<HEAD>` (refer to `git log` for the audit
checkpoint).

