# Labyrinth Space availability

Labyrinth provides encrypted message-history backup through the Rust MLS orchestrator. The Apple adapter now supports owner OAuth for a personal mailbox and sender-owned conversation records, and Petrel-verified foreign conversation reads. Actual storage placement is selected by the delivery service and pinned in encrypted local state. A missing adapter or a failed required host never silently moves existing history to another backend.

The governing workspace plan is [Spaces release readiness](../../../../Catbird+Petrel/docs/research/2026-10-23-spaces-release-readiness.md). The source and validation report is maintained at [the MLS/Labyrinth report](../../mls-labyrinth-report.md) in the isolated release workspace.

| Platform | Source capability in this integration | Qualification boundary |
| --- | --- | --- |
| Catbird iOS, iPadOS and macOS using CatbirdMLSCore | Owner OAuth mailbox/batch operations, foreign getRecord verification, exact generation routing, owner cleanup | Fresh native FFI, app install and real two-account/two-device history remain separate required checks. |
| Android | Default Rust/UniFFI Space callback remains unavailable; no adapter implementation was found in this checkout | Conversation-Space history is unsupported and must not be offered as available. |
| catmos desktop and catmos-cli | Default native gateway reports no Space adapter; no implementation was found in these checkouts | Conversation-Space history is unsupported. A legacy DS placement is a separate capability and is not qualified here. |
| Browser / WASM | Native Labyrinth module is excluded for `wasm32` | No browser Labyrinth parity is claimed. |

The Apple callback advertises implementation capability, then Rust probes actual host support. Only an explicit unsupported-host response can choose a first personal-mailbox fallback, and only when no existing Space mailbox/head would be forked. Once a conversation's placement is Space, unsupported platforms receive an explicit error rather than empty history.

The default public v1.5.21 native binary predates this integration. Builds using that default report Space capability unavailable and reject the Space callback. Only `CATBIRD_MLS_LOCAL_INTEGRATION=1` plus the exact `CATBIRD_MLS_FFI_PATH` enables it; the local framework must carry the protocol marker written after the complete rebuild and source-fingerprint check. The final artifact manifest separately records actual library hashes. Publishing a new binary and changing the public default remain release gates requiring approval.

The DS conversation-Space issuer is disabled by default. Enabling it requires a compatible hosted authority account, immutable generation provisioning, authenticated membership policy, and completed deletion notification delivery. Source tests and a healthy service are insufficient evidence of these operational prerequisites.

Account deletion journals remote intent before local erasure. If required Space inventory or owner cleanup cannot be performed, it remains pending; this conservative behavior also applies to an unqualified older PDS. Local sign-out does not claim remote deletion.
