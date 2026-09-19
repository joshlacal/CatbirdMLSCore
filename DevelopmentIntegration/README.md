# Exact Apple sibling integration

This recipe opts the standalone Core package into the exact `Petrel` and `PetrelCatbird` siblings in the same isolated workspace. Normal package evaluation remains pinned to the published revisions and release FFI artifact.

Run from CatbirdMLSCore:

```sh
DevelopmentIntegration/run-local.sh swift test --scratch-path .build-local-integration
```

The wrapper sets `CATBIRD_MLS_LOCAL_INTEGRATION=1`, creates a command-scoped SwiftPM mirror so PetrelCatbird's transitive remote edge resolves to the same exact Petrel sibling, and injects that configuration into `swift build`, `swift test`, `swift package`, or `swift run`. It aborts if the resolved graph contains a remote or wrong-path Petrel identity.

Each invocation writes a provenance record under `.build/LocalIntegration/provenance/`. The record includes the selected-input manifest hash, current and baseline JJ identities, exact sibling paths, generated Swift hash, and FFI identity. JJ-only isolated workspaces are supported; a Git HEAD is recorded only when the working copy exposes one.

If `Sources/CatbirdMLSFFI.xcframework` exists, the wrapper opts into that exact local framework and records its complete file hash map. Otherwise it records the pinned `v1.5.18` release URL and checksum. Merely leaving an ignored framework in `Sources` does not change a normal build.

`prepare-local.py` is retained as a compatibility entry point and now delegates to the same verifier. Its previous fixed remote reconstruction inputs and overlays remain retained as historical evidence but are not selected.

`Scripts/rebuild-ffi.sh` preserves the prior framework, generated Swift binding, and any stale legacy binding under `_trash/ffi-rebuild/<UTC timestamp>/` before installing new outputs. Final FFI generation is deferred until the native request API is frozen; this prerequisite does not generate or modify bindings.
