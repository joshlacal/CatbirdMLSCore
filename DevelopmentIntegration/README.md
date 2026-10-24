# Exact Apple sibling integration

This recipe opts the standalone Core package into the exact `Petrel` and `PetrelCatbird` siblings in the same isolated workspace. Normal package evaluation remains pinned to the published revisions and release FFI artifact.

Run from CatbirdMLSCore:

```sh
DevelopmentIntegration/run-local.sh swift test --scratch-path .build-local-integration
```

The wrapper sets `CATBIRD_MLS_LOCAL_INTEGRATION=1` and uses the exact root path dependencies to resolve PetrelCatbird's transitive Petrel identity to the same sibling. It validates that graph before running `swift build`, `swift test`, `swift package`, or `swift run`, and aborts if the graph contains a remote or wrong-path Petrel identity.

Each invocation writes a provenance record under `.build/LocalIntegration/provenance/`. The record includes the selected-input manifest hash, current and baseline JJ identities, exact sibling paths, generated Swift hash, and FFI identity. JJ-only isolated workspaces are supported; a Git HEAD is recorded only when the working copy exposes one.

The current graph is selected by `spaces-selected-inputs.json`. If `Sources/CatbirdMLSFFI.xcframework` exists, the wrapper requires its fresh-build protocol marker and verifies `spaces-apple-artifact-manifest.json` before selecting the framework. That manifest binds the exact native sources, SDK/schema revisions, generated interface and all library hashes. Otherwise the wrapper records the pinned `v1.5.21` release URL and checksum, whose Space transport capability is disabled. Merely leaving an ignored framework in `Sources` does not change a normal build.

`prepare-local.py` is retained as a compatibility entry point and now delegates to the same verifier. Its previous fixed remote reconstruction inputs and overlays remain retained as historical evidence but are not selected.

`Scripts/rebuild-ffi.sh` preserves the prior framework, generated Swift binding, and any stale legacy binding under `_trash/ffi-rebuild/<UTC timestamp>/` before installing new outputs. It requires the Labyrinth feature, builds all seven Apple targets, rejects source drift during the build, and writes the protocol marker only after the complete build. The artifact manifest is then generated and verified separately. These source and build receipts do not establish live OAuth, account or device behavior.

For early iOS simulator verification, `CATBIRD_MLS_APPLE_SCOPE=ios-arm64-simulator-only Scripts/rebuild-ffi.sh` creates the separate `Sources/CatbirdMLSFFISimulator.xcframework`. Its marker and manifest declare only the arm64 iOS simulator, and the manifest tool rejects claiming that matrix as a full release. Generate `spaces-simulator-artifact-manifest.json` with `Scripts/apple-artifact-manifest.py generate --framework <absolute simulator framework path>` and the same exact source arguments as the full manifest. Select it explicitly through `CATBIRD_MLS_FFI_PATH`; the local wrapper preserves that selection. It cannot be used for a macOS test host, physical iOS device, archive, or release qualification. The default full build remains required.
