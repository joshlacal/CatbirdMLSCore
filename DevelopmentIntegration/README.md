# Unreleased MLS local integration

This recipe builds the current handwritten sources with their matching generated native ABI and a narrowly updated SDK. It does not publish a release or change the primary app project's published package references.

Run from CatbirdMLSCore:

```sh
DevelopmentIntegration/run-local.sh swift test --scratch-path .build-local-integration
```

The wrapper reconstructs and verifies the SDK, then sets `CATBIRD_MLS_LOCAL_INTEGRATION=1` for the supplied build command. Without this explicit selection, the unreleased package manifest stops with a diagnostic. Do not select an arbitrary sibling checkout.

`prepare-local.py` fetches exactly PetrelCatbird revision `0e68066f0024d05d820be64c0e999771f829d3aa`, materializes its Package.swift, Sources and Tests, and applies precisely two generated overlays:

- `BlueCatbirdChatGetConversationState.swift`: optional `pendingLeafRecoveryRequests` and current generator output.
- `BlueCatbirdChatSubmitTransition.swift`: `SignedOperationExpired` error case.

Every resulting file must match `petrel-sdk-manifest.json`. The 141-file SDK manifest digest is `c0e6f3bb032b0e10efbbaed5542c3ac660b1eea0f0148adc0cfbf408afdb40c8`. The fixed destination is `.build/LocalIntegration/PetrelCatbird-c0e6f3bb032b0e10`; it is generated local state and stays ignored. Re-running verifies existing contents and refuses changes or extra files. Published Petrel remains pinned at `dd2ad04bafa1176d45e18be13267349f2d5ec33a`.

The native framework is `Sources/CatbirdMLSFFI.xcframework`, paired with legitimate generated `Sources/CatbirdMLS/CatbirdMLS.swift`. Its final shared-native source digest and per-artifact hashes must be recorded after the reviewed V3 build. Never pair these bindings with the earlier released framework.

The Apple integration owner prepares an isolated development app project from a preserved source snapshot, changing only its Core and PetrelCatbird package references to the canonical Core and this fixed SDK. The canonical app project keeps its published references. Run that development project's Xcode command through this wrapper so package-manifest evaluation receives the explicit local selection.

Before canonical integration, archive the exact existing manifest, generated bindings and old native artifact fallback. Record before/after hashes and verify the final local package, native slices and app build together. The local integration remains unpublished; release pins are unchanged until a separately coordinated release.
