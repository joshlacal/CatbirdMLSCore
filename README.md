# CatbirdMLSCore

Thin Swift package over the Rust `catbird-mls` SDK, shared by the Catbird app and its Notification Service Extension (NSE). Rust owns MLS state, SQLCipher storage, sync and Labyrinth backup. This package supplies the Apple side: Keychain storage, App Group paths, the Petrel-backed network transport, NSE push processing and Darwin wake-ups. There is no GRDB layer.

> **Branch status (2026-11-08).** Bookmark `clean-slate-2026-10-31` (`ad63968c` in this repo) remains the last sealed state. The unsealed Task 4 R4 artifact candidate rebuilds the generated interface and all-target Apple XCFramework from exact `catbird-mls@085e8a7435c2b1c61189040e626b680f7572559f` (R4 signed Space denial), `catbird-atproto@49c6fdb00671ffe96e58e3b5ebec7d5e6532f51f`, `labyrinth@f771b9c26558ce6a39ed87c657dc8bd6e331d94a`, `Petrel@f381cb844d33d76ad7ca948698480d381712304b` and `PetrelCatbird@e782e46ca8489c881517186fe0bf1a9c35c99634`. `DevelopmentIntegration/spaces-apple-artifact-manifest.json` is the verified schema-2 receipt (identity `595a2c0aaa3969a2744e1bb69fb76f091742c186a1384cd6ead7b03644faf304`). This candidate awaits independent R4 review and is neither sealed nor release-qualified; rebuild after any source change.

## Documentation

API reference (DocC): <https://joshlacal.github.io/CatbirdMLSCore/>. Build it locally with `Scripts/build-docs-site.sh`.

## Layout

```text
Sources/CatbirdMLS/CatbirdMLS.swift        UniFFI interface written by Scripts/rebuild-ffi.sh (never hand-edit)
Sources/CatbirdMLSFFI.xcframework          local native build (gitignored) + labyrinth-space-protocol.json marker
Sources/CatbirdMLSCore/
  Client/MLSClientCoordinator.swift        MLSAppGroupStorage, MLSPushExtensionProcessor, MLSClientCoordinator
  KeyStore/KeychainPlatformKeyStore.swift  PlatformKeyStore + PushKeystore
  Transport/PetrelPlatformTransport.swift  PlatformTransport + PushFetchTransport
  Service/                                 MLSOutboundHTTPValidator, MLSPublicPDSReader, BlobCrypto
  Models/, Extensions/                     payload/presentation types, hex helpers
DevelopmentIntegration/                    sibling-integration wrapper and Space artifact manifests
Scripts/                                   rebuild-ffi.sh, apple-artifact-manifest.py, release tooling
Tests/CatbirdMLSCoreTests/                 XCTest suites; NativeArchiveMockNetwork mocks DS, PDS and Space hosts
```

## Shared app and NSE database

The app and the NSE open the same per-account SQLCipher file, `<App Group container>/catbird-mls/<accountDidHash>.db` in `group.blue.catbird.shared`. Rust reconciles `PRAGMA data_version` inside every write transaction, so each process reloads state the other committed. The NSE posts `blue.catbird.mls.nseDidCommit` after it commits, and `MLSClientCoordinator` reacts to it. The NSE never refreshes OAuth; without a cached token it shows a generic body. Details and invariants: [CLAUDE.md](CLAUDE.md#shared-app-and-nse-database).

## Spaces scopes

Spaces-backed chat needs the OAuth scopes in `MLSAppGroupStorage.requiredSpacesScopes`, including the two explicit Labyrinth Space scopes (`action=` and `collection=` spelled out). A session granted only the retired `space:blue.catbird.labyrinth.mailbox?manage=create&manage=delete` and `space:blue.catbird.labyrinth.conversation?authority=*` forms is reported as `.missingOAuthScopes` and must re-authorize. Exact strings: [CLAUDE.md](CLAUDE.md#spaces-eligibility-and-oauth-scopes).

## Building

`Package.swift` uses the sibling `Petrel` and `PetrelCatbird` checkouts when they exist and selects `Sources/CatbirdMLSFFI.xcframework` when its protocol marker is valid. Set `CATBIRD_MLS_LOCAL_INTEGRATION=0` to force the pinned remote revisions and the public `v1.5.21` binary instead. That binary does **not** match the checked-in `Sources/CatbirdMLS/CatbirdMLS.swift` interface, so on this branch a working build needs a locally rebuilt framework.

```sh
# Rebuild from sibling catbird-mls, catbird-atproto and Labyrinth sources (all Apple targets)
CMAKE_POLICY_VERSION_MINIMUM=3.5 Scripts/rebuild-ffi.sh

# Check the build receipt against the current sources and framework
python3 Scripts/apple-artifact-manifest.py verify DevelopmentIntegration/spaces-apple-artifact-manifest.json

# Build and test against the exact siblings (verifies the dependency graph and writes provenance)
DevelopmentIntegration/run-local.sh swift test --scratch-path .build-local-integration
```

Receipt generation, the simulator-only framework and provenance rules: [DevelopmentIntegration/README.md](DevelopmentIntegration/README.md). Publishing a new release binary is a release gate that needs Josh's approval.

Never boot or touch the `iPhone 18 Pro Max` simulator (`51D953A5-461C-42C0-A947-C13930EE1BE2`); use `iPhone 17 Pro` or a scratch simulator.

## License

MIT
