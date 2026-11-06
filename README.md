# CatbirdMLSCore

Thin Swift package over the Rust `catbird-mls` SDK, shared by the Catbird app and its Notification Service Extension (NSE). Rust owns MLS state, SQLCipher storage, sync and Labyrinth backup. This package supplies the Apple side: Keychain storage, App Group paths, the Petrel-backed network transport, NSE push processing and Darwin wake-ups. There is no GRDB layer.

> **Branch status (2026-11-06).** Bookmark `clean-slate-2026-10-31` (`ad63968c` in this repo) remains the last sealed state. The unsealed Task 4 R3 artifact candidate rebuilds the generated interface and all-target Apple XCFramework from exact `catbird-mls@f609cd3413eff8dc291f25e607290627c16c4a0e`, `catbird-atproto@971fd048b4ab2128b8d5b2c35f1be7885abfa1ce`, `labyrinth@276c0972a05eb96f87509e8916e0e51ac46ac154`, `Petrel@f381cb844d33d76ad7ca948698480d381712304b` and `PetrelCatbird@1193ac5fdcff9a4c283704d649cdc3a6663fb8bf`. `DevelopmentIntegration/spaces-apple-artifact-manifest.json` is the verified schema-2 receipt (identity `2fbbcc35640db361524e5c7fa27d0fa37832e34e904e1222ddaa4edcf8324abc`). This candidate awaits independent R3 review and is neither sealed nor release-qualified; rebuild after any source change.

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
# Rebuild the framework and generated interface from ../catbird-mls (all Apple targets, labyrinth feature)
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
