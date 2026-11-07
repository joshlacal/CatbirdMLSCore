# Exact Apple sibling integration

This recipe runs the Core package against the exact `Petrel`, `PetrelCatbird` and native framework of the same isolated workspace, and proves it. `Package.swift` already prefers sibling `Petrel`/`PetrelCatbird` checkouts and a marker-verified `Sources/CatbirdMLSFFI.xcframework` whenever they exist (`CATBIRD_MLS_LOCAL_INTEGRATION=0` forces the published revisions and release binary). Only this wrapper also verifies the dependency graph and the artifact manifest and records provenance.

> **Status (2026-11-07).** `spaces-apple-artifact-manifest.json` is the verified schema-2 receipt for the unsealed Task 4 R3 all-target Apple candidate built from exact `catbird-mls@bb43b95853d6e95d5181323f326a7e81dd194540`, `catbird-atproto@838df09a4fa46ba651280c01e74b66b989ca7a74`, `labyrinth@276c0972a05eb96f87509e8916e0e51ac46ac154`, `Petrel@f381cb844d33d76ad7ca948698480d381712304b` and `PetrelCatbird@e4227af68226a61a4cae7478496d1a7b930f28f6`; its identity is `edcf85182b682bb0e4af775c50c19a4fa72cf5728c1d1e0bc518cfc783c8d5c8`. It awaits independent R3 review and is not sealed or release-qualified. `spaces-simulator-artifact-manifest.json` remains stale and no simulator framework is present; rebuild that separate path before using it.

Run from CatbirdMLSCore:

```sh
DevelopmentIntegration/run-local.sh swift test --scratch-path .build-local-integration
```

The wrapper sets `CATBIRD_MLS_LOCAL_INTEGRATION=1` and uses the exact root path dependencies to resolve PetrelCatbird's transitive Petrel identity to the same sibling. It validates that graph before running `swift build`, `swift test`, `swift package`, or `swift run`, and aborts if the graph contains a remote or wrong-path Petrel identity.

Each invocation writes a provenance record under `.build/LocalIntegration/provenance/`. The record includes the selected-input manifest hash, current and baseline JJ identities, exact sibling paths, generated Swift hash, and FFI identity. JJ-only isolated workspaces are supported; a Git HEAD is recorded only when the working copy exposes one.

The current graph is selected by `spaces-selected-inputs.json`. If `Sources/CatbirdMLSFFI.xcframework` exists, the wrapper requires its fresh-build protocol marker and verifies `spaces-apple-artifact-manifest.json` before selecting the framework. That manifest binds the exact native sources, SDK/schema revisions, generated interface and all library hashes. Otherwise the wrapper records the pinned `v1.5.21` release URL and checksum. That release has Space transport disabled and does not match the checked-in `Sources/CatbirdMLS/CatbirdMLS.swift` interface, so it is not a working configuration on this branch. A plain `swift build` also selects a marker-verified local framework, but without the manifest check or provenance record.

`prepare-local.py` is retained as a compatibility entry point and now delegates to the same verifier. Its previous fixed remote reconstruction inputs and overlays remain retained as historical evidence but are not selected. In particular, `petrel-sdk-manifest.json` (overlay baseline `0e68066f` and Petrel `dd2ad04`) and its historical overlay files are obsolete reconstruction receipts, not active dependency pins; they are preserved unchanged for provenance. The active source selector is `spaces-selected-inputs.json`, and active artifact receipts are `spaces-simulator-artifact-manifest.json` and `spaces-apple-artifact-manifest.json`.

Both Space artifact manifests (schema 2) record every path relative to the manifest's own directory and identify each source checkout by its published commit: the single parent of an empty JJ working copy, or a clean Git `HEAD`. JJ working-copy commits are never recorded, because they exist only in one workspace. `verify` therefore passes in any checkout of the same commits that keeps the sibling layout (`CatbirdMLSCore` beside `catbird-mls`, `catbird-atproto`, `Petrel` and `PetrelCatbird`) and holds the same frameworks, and it fails if a recorded checkout has uncommitted changes. `spaces-selected-inputs.json` likewise names each sibling relative to the directory that holds `CatbirdMLSCore`. Generate a manifest from a layout whose four source checkouts are clean, for example:

```sh
python3 Scripts/apple-artifact-manifest.py generate --qualification reviewed-source \
  --core . --native ../catbird-mls --atproto ../catbird-atproto \
  --petrel ../Petrel --petrel-catbird ../PetrelCatbird \
  --selected-inputs DevelopmentIntegration/spaces-selected-inputs.json \
  --output DevelopmentIntegration/spaces-apple-artifact-manifest.json
```

Check an existing receipt against the current checkouts and framework (prints the manifest identity, fails on any drift):

```sh
python3 Scripts/apple-artifact-manifest.py verify DevelopmentIntegration/spaces-apple-artifact-manifest.json
```

`Scripts/rebuild-ffi.sh` preserves the prior framework, generated Swift binding, and any stale legacy binding under `_trash/ffi-rebuild/<UTC timestamp>/` before installing new outputs. It requires the Labyrinth feature, builds all seven Apple targets, rejects source drift during the build, and writes the protocol marker only after the complete build. The artifact manifest is then generated and verified separately. These source and build receipts do not establish live OAuth, account or device behavior.

For early iOS simulator verification, `CATBIRD_MLS_APPLE_SCOPE=ios-arm64-simulator-only Scripts/rebuild-ffi.sh` creates the separate `Sources/CatbirdMLSFFISimulator.xcframework`. Its marker and manifest declare only the arm64 iOS simulator, and the manifest tool rejects claiming that matrix as a full release. Generate `spaces-simulator-artifact-manifest.json` with `Scripts/apple-artifact-manifest.py generate --framework Sources/CatbirdMLSFFISimulator.xcframework` and the same exact source arguments as the full manifest. Select it explicitly through `CATBIRD_MLS_FFI_PATH`; the local wrapper preserves that selection. It cannot be used for a macOS test host, physical iOS device, archive, or release qualification. The default full build remains required. Never boot or touch the `iPhone 18 Pro Max` simulator (`51D953A5-461C-42C0-A947-C13930EE1BE2`); use `iPhone 17 Pro` or a scratch simulator.

`Sources/CatbirdMLS/CatbirdMLS.swift` is the package's single generated interface. `rebuild-ffi.sh` replaces it together with the local framework, and both Space artifact manifests record its hash. UniFFI includes callback metadata in its checksums, so matching function signatures alone does not make interfaces interchangeable: this interface matches only the framework built with it, not the older public `v1.5.21` binary. Publishing a future binary release requires a new immutable artifact URL/checksum built together with this interface.
