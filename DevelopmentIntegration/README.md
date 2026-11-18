# Exact Apple sibling integration

This recipe runs the Core package against the exact `Petrel`, `PetrelCatbird`, `catbird-mls`, `catbird-atproto`, Labyrinth and native framework inputs of the same isolated workspace, and proves it. `Package.swift` already prefers sibling `Petrel`/`PetrelCatbird` checkouts and a locally verified `Sources/CatbirdMLSFFI.xcframework`. SwiftPM checks the source selection, generated bindings and all recorded framework file hashes. The wrapper also checks the dependency graph and records provenance. With `CATBIRD_MLS_LOCAL_INTEGRATION=0`, SwiftPM selects a published binary only if its immutable receipt matches the generated bindings.

> **Artifact gate (2026-11-16).** An Apple artifact receipt is accepted only when it explicitly records `catbird-mls`, `catbird-atproto`, the complete Labyrinth tree (including `labyrinth-protocol`), `Petrel` and `PetrelCatbird`. Legacy receipts that omit Labyrinth are rejected and must be regenerated from the final source tuple; they receive no compatibility exception. `qualification` remains authoritative, and an `unreviewed-candidate` receipt is not release qualification.

Run from CatbirdMLSCore:

```sh
DevelopmentIntegration/run-local.sh swift test --scratch-path .build-local-integration
```

The wrapper sets `CATBIRD_MLS_LOCAL_INTEGRATION=1` and uses the exact root path dependencies to resolve PetrelCatbird's transitive Petrel identity to the same sibling. It validates that graph before running `swift build`, `swift test`, `swift package`, or `swift run`, and aborts if the graph contains a remote or wrong-path Petrel identity.

Each invocation writes a provenance record under `.build/LocalIntegration/provenance/`. The record includes the selected-input manifest hash, current and baseline JJ identities, exact sibling paths, generated Swift hash, and FFI identity. JJ-only isolated workspaces are supported; a Git HEAD is recorded only when the working copy exposes one.

The current graph is selected by `spaces-selected-inputs.json`. If `Sources/CatbirdMLSFFI.xcframework` exists, the wrapper requires its fresh-build protocol marker and verifies `spaces-apple-artifact-manifest.json` before selecting the framework. That manifest binds the exact `catbird-mls`, `catbird-atproto` and Labyrinth sources, SDK/schema revisions, generated interface and all library hashes. The published v1.5.21 binary has different generated bindings. A checkout without matching local artifacts fails package resolution before attempting to fetch that release.

`prepare-local.py` is retained as a compatibility entry point and now delegates to the same verifier. Its previous fixed remote reconstruction inputs and overlays remain retained as historical evidence but are not selected. In particular, `petrel-sdk-manifest.json` (overlay baseline `0e68066f` and Petrel `dd2ad04`) and its historical overlay files are obsolete reconstruction receipts, not active dependency pins; they are preserved unchanged for provenance. The active source selector is `spaces-selected-inputs.json`, and the simulator rebuild writes `.build/qualified-simulator-ffi.json` and the full framework uses `DevelopmentIntegration/spaces-apple-artifact-manifest.json`.

Both Space artifact manifests (schema 2) record every path relative to the manifest's own directory and identify each source checkout by its published commit: the single parent of an empty JJ working copy, or a clean Git `HEAD`. JJ working-copy commits are never recorded, because they exist only in one workspace. `verify` therefore passes in any checkout of the same commits that keeps the sibling layout (`CatbirdMLSCore` beside `catbird-mls`, `catbird-atproto`, `labyrinth`, `Petrel` and `PetrelCatbird`) and holds the same frameworks, and it fails if a recorded checkout has uncommitted changes. `spaces-selected-inputs.json` likewise names each sibling relative to the directory that holds `CatbirdMLSCore`. Generate a manifest from a layout whose five source checkouts are clean, for example:

```sh
python3 Scripts/apple-artifact-manifest.py generate --qualification reviewed-source \
  --core . --native ../catbird-mls --atproto ../catbird-atproto --labyrinth ../labyrinth \
  --petrel ../Petrel --petrel-catbird ../PetrelCatbird \
  --selected-inputs DevelopmentIntegration/spaces-selected-inputs.json \
  --output DevelopmentIntegration/spaces-apple-artifact-manifest.json
```

Check an existing receipt against the current checkouts and framework (prints the manifest identity, fails on any drift):

```sh
python3 Scripts/apple-artifact-manifest.py verify DevelopmentIntegration/spaces-apple-artifact-manifest.json
```

### Golden50 candidate gate

Golden50 qualification never consumes a checked-in simulator fixture directory. It exports into an ignored temporary directory from the candidate tuple, replays the export through that tuple's real `catbird-sim` client and `mls-ds`, then runs the Swift consumer against the locally rebuilt full seven-target candidate framework. The script verifies the simulator's path dependencies, requires the framework marker to match `catbird-mls` + `catbird-atproto` + Labyrinth, and refuses to stamp the export if simulator, the complete `mls-ds` workspace, native, atproto or Labyrinth source changes during replay.

```sh
CATBIRD_MLS_CANDIDATE_ROOT=/path/to/candidate-tuple \
CATBIRD_MLS_FFI_PATH="$PWD/Sources/CatbirdMLSFFI.xcframework" \
SIM_POSTGRES_BASE_URL=postgresql://127.0.0.1:5432/postgres \
Scripts/qualify-golden-50.sh
```

`Scripts/rebuild-ffi.sh` preserves the prior framework, generated Swift binding, and any stale legacy binding under `_trash/ffi-rebuild/<UTC timestamp>/` before installing new outputs. It requires the Labyrinth feature, builds all seven Apple targets, rejects drift in `catbird-mls`, `catbird-atproto` or the complete Labyrinth tree during the build, and writes the protocol marker only after the complete build. `CATBIRD_LABYRINTH_NATIVE_DIR` selects a non-sibling Labyrinth checkout alongside the existing native checkout overrides. The artifact manifest is then generated and verified separately. These source and build receipts do not establish live OAuth, account or device behavior.

For early iOS simulator verification, `CATBIRD_MLS_APPLE_SCOPE=ios-arm64-simulator-only Scripts/rebuild-ffi.sh` creates the separate `Sources/CatbirdMLSFFISimulator.xcframework`. Its marker and manifest declare only the arm64 iOS simulator, and the manifest tool rejects claiming that matrix as a full release. The rebuild generates the simulator receipt under `.build/qualified-simulator-ffi.json`. Select it explicitly through `CATBIRD_MLS_FFI_PATH`; the local wrapper preserves that selection. It cannot be used for a macOS test host, physical iOS device, archive, or release qualification. The default full build remains required. Never boot or touch the `iPhone 18 Pro Max` simulator (`51D953A5-461C-42C0-A947-C13930EE1BE2`); use `iPhone 17 Pro` or a scratch simulator.

`Sources/CatbirdMLS/CatbirdMLS.swift` is the package's single generated interface. `rebuild-ffi.sh` replaces it together with the local framework, and both Space artifact manifests record its hash. UniFFI includes callback metadata in its checksums, so matching function signatures alone does not make interfaces interchangeable: this interface matches only the framework built with it, not the older public `v1.5.21` binary. Publishing a future binary release requires a new immutable artifact URL/checksum built together with this interface.
