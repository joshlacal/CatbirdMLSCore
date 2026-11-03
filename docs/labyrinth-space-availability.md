# Labyrinth Space availability

Labyrinth provides encrypted message-history backup through the Rust MLS client. On native platforms it runs inside `CatbirdMlsClient` as the verified `LabyrinthSession` archive, and the client sends every Space and PDS request through the platform's `PlatformTransport`. There is no separate Space callback for a platform to implement or advertise. Actual storage placement is selected by the delivery service and pinned in encrypted local state. A failed required host never silently moves existing history to another backend.

The original release plan is [Spaces release readiness](../../../../Catbird+Petrel/docs/research/2026-10-23-spaces-release-readiness.md), and its source and validation report is [the 2026-10-23 MLS/Labyrinth report](../../../../.workspaces/spaces-release-20261023/mls-labyrinth-report.md). Both predate the clean-slate branch; this page describes the branch.

## Eligibility

A client offers Spaces-backed history only when the account's PDS supports Spaces and the session holds every required scope (`checkSpacesEligibility`; on Apple `MLSAppGroupStorage.requiredSpacesScopes`). That includes the two explicit Labyrinth Space scopes listed in [CLAUDE.md](../CLAUDE.md#spaces-eligibility-and-oauth-scopes). A session granted only the retired `space:blue.catbird.labyrinth.mailbox?manage=create&manage=delete` and `space:blue.catbird.labyrinth.conversation?authority=*` forms is reported as missing both Space scopes and must re-authorize before backup or restore runs. Bluesky DMs stay available either way.

| Platform | Source capability on this branch | Qualification boundary |
| --- | --- | --- |
| Catbird iOS, iPadOS and macOS (CatbirdMLSCore) | Native archive through `PetrelPlatformTransport`: owner mailbox, conversation batches, outbox flush, kick rescue, mailbox restore | A framework rebuilt from the final sources, app install, and real two-account/two-device history remain separate required checks. |
| Android | Same native archive through `AndroidPlatformTransport`; Spaces gate and Labyrinth backup/restore in `MlsSdkManager` | Instrumented tests use an offline Spaces stub; real-device history remains a separate required check. |
| catmos desktop, catmos-cli | Desktop wraps `CatbirdMlsClient` with `TauriTransport`; catmos-cli supplies `CliPlatformTransport` | Conversation-Space history is not qualified here. |
| Browser / WASM | The native archive client is not compiled for `wasm32`; `WasmMlsClient` has its own `labyrinthStatus`, `flushLabyrinthQueue` and recovery-code restore | No browser parity with the native archive is claimed. |

## Native artifacts

The public `v1.5.21` native binary predates this integration and does not match the generated interface on this branch. Apple builds need a local framework rebuilt with `Scripts/rebuild-ffi.sh`, which writes the protocol marker only after a complete build and source-fingerprint check; `DevelopmentIntegration/spaces-apple-artifact-manifest.json` records the actual library hashes and source commits. Current local builds come from provisional Task 4 R1 sources and must be rebuilt after further source changes. Publishing a new binary and changing the public default remain release gates requiring approval.

The DS conversation-Space issuer is disabled by default. Enabling it requires a compatible hosted authority account, immutable generation provisioning, authenticated membership policy, and completed deletion notification delivery. Source tests and a healthy service are insufficient evidence of these operational prerequisites.

Account deletion journals remote intent before local erasure. If required Space inventory or owner cleanup cannot be performed, it remains pending; this conservative behavior also applies to an unqualified older PDS. Local sign-out does not claim remote deletion.
