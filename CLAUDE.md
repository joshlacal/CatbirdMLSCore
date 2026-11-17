# CLAUDE.md

Guidance for Claude Code when working in CatbirdMLSCore.

## Overview

CatbirdMLSCore is the thin Swift host layer over the Rust `catbird-mls` SDK (UniFFI `CatbirdMlsClient`), shared by the Catbird app and its Notification Service Extension (NSE). Rust owns MLS/OpenMLS state, the SQLCipher database, sync, the Labyrinth archive and push decryption. This package supplies only what needs Apple APIs: Keychain, App Group paths and file protection, the Petrel-backed HTTP transport, NSE entry points and Darwin notifications. There is no GRDB, no Swift MLS state and no Swift-side database schema.

Branch status (2026-11-08): bookmark `clean-slate-2026-10-31` (`ad63968c` here) remains the last sealed state. The unsealed Task 4 R4 artifact candidate rebuilds the generated interface and all-target Apple XCFramework from exact `catbird-mls@085e8a7435c2b1c61189040e626b680f7572559f` (R4 signed Space denial), `catbird-atproto@49c6fdb00671ffe96e58e3b5ebec7d5e6532f51f`, `labyrinth@f771b9c26558ce6a39ed87c657dc8bd6e331d94a`, `Petrel@f381cb844d33d76ad7ca948698480d381712304b` and `PetrelCatbird@e782e46ca8489c881517186fe0bf1a9c35c99634`. The verified schema-2 manifest identity is `595a2c0aaa3969a2744e1bb69fb76f091742c186a1384cd6ead7b03644faf304`. It awaits independent R4 review, so this candidate is neither sealed nor release-qualified. Rebuild after any source change (see [FFI workflow](#ffi-workflow)).

## Commands

```bash
# Build/test against the exact sibling checkouts (verifies graph + framework manifest, writes provenance)
DevelopmentIntegration/run-local.sh swift test --scratch-path .build-local-integration
DevelopmentIntegration/run-local.sh swift test --scratch-path .build-local-integration --filter AppGroupAndKeychainInvariantsTests

# Plain SwiftPM also uses the siblings and a marker-verified local framework (no verification or provenance)
swift build
swift test

# Tooling tests
python3 -m unittest discover -s Scripts/tests -v
python3 -m unittest discover -s DevelopmentIntegration/tests -v   # two test_local_dependencies failures are recorded as pre-existing in the Task 4 report
```

In the shared workspace, run heavy builds through the `ctl` slot, e.g. `CTL_AGENT=<agent> CMAKE_POLICY_VERSION_MINIMUM=3.5 ~/.cargo/bin/ctl run --slot xcode -- <cmd>`.

Never boot or touch the `iPhone 18 Pro Max` simulator (`51D953A5-461C-42C0-A947-C13930EE1BE2`); use `iPhone 17 Pro` or a scratch simulator.

## Module structure

- `Sources/CatbirdMLS/CatbirdMLS.swift`: the package's single generated UniFFI interface. Never edit by hand.
- `Sources/CatbirdMLSFFI.xcframework`: locally built native framework (gitignored); its `labyrinth-space-protocol.json` marker gates selection.
- `Sources/CatbirdMLSCore/Client/MLSClientCoordinator.swift`: `MLSAppGroupStorage` (paths, DID hashing, file protection, required scopes), `MLSPushExtensionProcessor` (NSE entry point), and the `@MainActor @Observable` `MLSClientCoordinator` wrapping one `CatbirdMlsClient`.
- `Sources/CatbirdMLSCore/KeyStore/KeychainPlatformKeyStore.swift`: `PlatformKeyStore` + `PushKeystore`.
- `Sources/CatbirdMLSCore/Transport/PetrelPlatformTransport.swift`: `PlatformTransport` + `PushFetchTransport`. Sends `blue.catbird.chat.*` straight to the home mls-ds with a `getServiceAuth` token (never through Nest), keeps `Atproto-Space-Mac`/`Atproto-Space-Credential` headers verbatim, and blocks redirects.
- `Sources/CatbirdMLSCore/Service/`: `MLSOutboundHTTPValidator`, `MLSPublicPDSReader`, `BlobCrypto`.
- `Sources/CatbirdMLSCore/Models/`, `Extensions/`: payload and presentation types, hex helpers. `Core/`, `Storage/`, `Crypto/`, `Logging/`, `Sync/` and `Tracking/` are empty.

## Shared app and NSE database

- **One file per account.** `MLSAppGroupStorage.databaseURL` is `<container of group.blue.catbird.shared>/catbird-mls/<accountDidHash>.db`, where the hash is of the canonicalized DID (`I8`). Raw DIDs never appear in paths or Keychain keys. `prepareStorageRoot` wipes pre-cutover `.sqlite`, `.lease` and `accounts.json` files and applies excluded-from-backup and complete-until-first-auth protection.
- **Both processes write it.** The app (`MLSClientCoordinator`) and the NSE (`MLSPushExtensionProcessor.process`/`processAsync`, which call Rust `processPushNotification`) open the same file. Rust's `MlsAccountDb::write_tx` checks `PRAGMA data_version` inside `BEGIN IMMEDIATE` and reloads cached groups when the other process committed (`I7`).
- **Format gate.** Opening a non-empty database without the current storage-format row throws `MlsError.LocalStateFormatUnsupported(message:)` before coordinator state or transport work; the message names the found and required versions. Round 4 has no local migration, so the app must wipe MLS storage and re-enrol.
- **Wake-ups.** After a real decrypt (not a fallback or cache hit) the NSE posts the Darwin notification `blue.catbird.mls.nseDidCommit`. The coordinator counts it (`darwinWakeupCount`) and calls `handleDarwinPushWakeup()`; resulting change events bump `changeRevision` and call `onChangeCallback` (`I11`).
- **No OAuth in the NSE.** The NSE uses only the cached chat DPoP token (`loadCachedChatDpopToken` returns `nil` when missing or expired) and falls back to the generic body `"New encrypted message"` within 2 s (`I10`). `decodePushWakePayload` reads Nest's APNs envelope (`aps` + `catbird`, including `wakeKind` and `fetchRequired`). Inline ciphertext is capped at 2,200 bytes (`I12`).
- **Backgrounding.** `prepareForBackground()` suspends admissions, runs `PRAGMA wal_checkpoint(PASSIVE)`, closes the connection and returns a `BackgroundCheckpointReport` (`I9`, avoids `0xdead10cc`). `resumeFromBackground()` reopens; `close()` and `signOutAndWipe()` also unregister the Darwin observer.
- **Creation recovery.** `pendingGroupCreationStatus`, `retryPendingGroupCreation`, and `cancelPendingGroupCreation` are the public coordinator controls for a retained group creation. Like the other Rust wrappers, all three execute through `runOffMain`; retry schedules the outbox only when it adopts a conversation.
- **Keychain.** `KeychainPlatformKeyStore` (service `blue.catbird.mls.clean-slate`) creates items with `kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly`, keyed by `sha256(normalized_did)`, in the shared access group resolved at runtime.
- **Device IDs.** Device IDs must be canonical UUIDv4. The verified native `LabyrinthSession` archive fails closed and rejects anything else (`archive requires UUIDv4 device_id`). The Space-only offline archive harness in `catbird-mls` exists only with `test`, `test-utils` or `sim` and is opened explicitly (`open_offline_archive_for_test`); it is never selected by device ID. Tests here use fixed UUIDv4 IDs.

## Spaces eligibility and OAuth scopes

`MLSAppGroupStorage.requiredSpacesScopes`:

- `atproto`
- `blob:*/*`
- `repo:blue.catbird.labyrinth.mailboxKeys`
- `repo:blue.catbird.chat.device`
- `space:blue.catbird.labyrinth.mailbox?action=create&action=delete&action=read&action=update&collection=blue.catbird.labyrinth.attachmentManifest&collection=blue.catbird.labyrinth.device&collection=blue.catbird.labyrinth.epoch&collection=blue.catbird.labyrinth.epochMember&collection=blue.catbird.labyrinth.otcSession&collection=blue.catbird.labyrinth.ownerBatch&collection=blue.catbird.labyrinth.virtualDevice&manage=create&manage=delete`
- `space:blue.catbird.labyrinth.conversation?action=create&action=delete&action=read&action=update&authority=*&collection=blue.catbird.labyrinth.attachmentManifest&collection=blue.catbird.labyrinth.batch&collection=blue.catbird.labyrinth.mek`

`MLSClientCoordinator.evaluateEligibility(...)` and `checkSpacesEligibility(grantedScopes:)` report any gap as `.missingOAuthScopes(missingScopes:)`. A session granted only the retired `space:blue.catbird.labyrinth.mailbox?manage=create&manage=delete` and `space:blue.catbird.labyrinth.conversation?authority=*` forms is ineligible (both Space scopes missing) and must re-authorize; for Nest sessions that is the `/auth/upgrade` flow. The same strings are pinned in Rust (`catbird_mls::REQUIRED_LABYRINTH_OAUTH_SCOPES`, used by `checkSpacesEligibility`), Nest, Android and catmos; change them everywhere together.

## Outbound request rules

`MLSOutboundHTTPValidator` allows these XRPC namespaces: `blue.catbird.chat.`, `blue.catbird.mlsChat.`, `blue.catbird.mls.`, `blue.catbird.labyrinth.`, `blue.catbird.push.` and `com.atproto.space.`. It also allows exactly `com.atproto.identity.resolveDid`, `com.atproto.repo.uploadBlob`, `putRecord`, `getRecord` and `listRecords`. `com.atproto.repo.deleteRecord` passes only as the Labyrinth `mailboxKeys` purge (`POST`, body keys exactly `repo`, `collection` = `blue.catbird.labyrinth.mailboxKeys`, `rkey` `self` or 12 digits, non-empty `swapRecord`), and only through the signed-in account's own PDS session for that `repo`. DID-derived endpoints must be public HTTPS on the default port; loopback is allowed only for tests.

## FFI workflow

1. Change Rust in `../catbird-mls`, `../catbird-atproto` or `../labyrinth`, then commit or checkpoint it; the receipt records published commits, not JJ working copies.
2. `CMAKE_POLICY_VERSION_MINIMUM=3.5 Scripts/rebuild-ffi.sh` builds all seven Apple targets with `CATBIRD_MLS_FEATURES=labyrinth` (it refuses a build without `labyrinth`). It preserves the previous framework and interface under `_trash/ffi-rebuild/<UTC>/`, installs the new `Sources/CatbirdMLSFFI.xcframework` and `Sources/CatbirdMLS/CatbirdMLS.swift`, and writes the protocol marker only after proving `catbird-mls`, `catbird-atproto` and the complete Labyrinth tree stayed unchanged. `CATBIRD_MLS_NATIVE_DIR`, `CATBIRD_ATPROTO_NATIVE_DIR` and `CATBIRD_LABYRINTH_NATIVE_DIR` select other checkouts.
3. Regenerate and verify `DevelopmentIntegration/spaces-apple-artifact-manifest.json` (commands in [DevelopmentIntegration/README.md](DevelopmentIntegration/README.md)).
4. Run the tests through `DevelopmentIntegration/run-local.sh`.

`Package.swift` selects the FFI in this order: `CATBIRD_MLS_FFI_PATH` (must name a marker-verified local or simulator framework, otherwise evaluation stops), then a marker-verified `Sources/CatbirdMLSFFI.xcframework`, then a marker-verified `Sources/CatbirdMLSFFISimulator.xcframework`, then the public `v1.5.21` release. `CATBIRD_MLS_LOCAL_INTEGRATION=0` skips the sibling packages and the automatic local-framework selection. The `v1.5.21` binary does not match the checked-in interface (UniFFI checksums include callback metadata), so publishing requires a new immutable release built with this interface. Publishing is a release gate that needs Josh's approval.

Frameworks from `rebuild-ffi.sh` are built without `catbird-mls`'s `sim` feature: `buggify!` compiles to `false`, planted bugs never activate, and SQLite runs on its default VFS. The deterministic seams fall back to the system clock and OS entropy.

## Deterministic coverage (catbird-sim)

Cross-process and crash cases that XCTest cannot reach run in `../catbird-sim` (the Hydrostatic Pressure Tank). Its full-stack harness runs two real delivery services (`ds-a`, did:web; `ds-b`, did:plc) with strict-home clients and real S2S routers and queues. An emulated account PDS serves personal Spaces and `mailboxKeys`; conversation Spaces stay on the real mls-ds Space host. An NSE twin (`alice_nse`) processes pushes on `alice_app`'s exact `.db` file. `catbird-crash-vfs`, a registered `sqlite3_vfs` beneath SQLCipher, cuts power mid-write, so reopening runs SQLite's WAL recovery over a physically torn encrypted `-wal`.

From `../catbird-sim` (full-stack runs need loopback PostgreSQL via `SIM_POSTGRES_BASE_URL`, default `postgresql://127.0.0.1:5432/postgres`):

```bash
cargo run --offline --release --bin catbird-sim -- replay --seed <N> --full-stack [--steps <M>] [--shrink] [--traces]
cargo run --offline --release --bin catbird-sim -- batch --start <N> --count <C> --full-stack [--steps <M>]
cargo run --offline --release --bin catbird-sim -- planted --bug <p1|p2|p3|p4|p5|p6|p8|p9|p10|p11|p12|p13|p15> [--max-seeds <N>]
```

`replay` prints `seed=… full_stack=… steps=… http_calls=… trace_blake3=… failures=…` and exits 1 on an oracle failure; `--shrink` prints the shrunk English failure story, and `--traces` writes HTTP traces and snapshots to stderr. Full-stack replay is deterministic: the Phase 2 gates `full_stack_seeds_replay_identically_in_process` and `full_stack_replay_is_identical_across_processes` require an identical `trace_blake3`. A different hash for the same seed on the same binary is itself a failure.

## Tests

`Tests/CatbirdMLSCoreTests/`: `AppGroupAndKeychainInvariantsTests` (paths, file protection, scopes, outbound rules), `PushExtensionIntegrationTests` (`I10`/`I11`/`I12`, Nest envelope decoding), `MLSLabyrinthOutboxAndRestoreE2ETests` (outbox drain, restart, kick rescue, mailbox restore), `MLSConcurrencyFaultLineTests`, `MLSMessagePayloadTests`, `MLSPublicPDSReaderTests`. `NativeArchiveMockNetwork` is the in-process home mls-ds plus per-account PDS/Space hosts serving the verified `LabyrinthSession` contract; it refuses any request that breaks the contract and records it in `contractViolations`.

## Security notes

- Never commit secrets, tokens or keys. SQLCipher and device keys stay in the Keychain.
- Never log ciphertext, keys or raw DIDs.
- Red holds: no production deploys, production DB migrations, or pushes/merges to `main` without Josh's explicit approval.
