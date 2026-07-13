# CatbirdMLSCore

Shared Swift package for MLS (Messaging Layer Security) functionality in Catbird.

## Overview

CatbirdMLSCore provides the core MLS implementation shared between:
- Catbird main app
- Notification Service Extension
- Other app extensions

## Architecture

### Notification Coordination Boundary

Catbird notification decryption is split across two layers:

- `catbird-mls` (Rust): MLS lifecycle, cryptography, orchestration, storage-facing data plane.
- `CatbirdMLSCore` (Swift host layer): Apple process coordination and OS IPC for app ↔ NSE.

`CatbirdMLSCore` owns Darwin doorbells, tokenized App Group handshake, and monotonic
state signaling (`MLSNotificationCoordinator`, `MLSStateMutationNotifier`).
`catbird-mls` intentionally remains platform-neutral so Android and WASM builds do not
depend on Apple-only APIs.

```text
CatbirdMLSCore/
├── Sources/CatbirdMLSCore/
│   ├── Core/               # Core MLS context and managers
│   ├── Extensions/         # Swift extensions
│   ├── FFI/                # Rust FFI bindings (auto-generated)
│   ├── Models/             # Data models
│   └── Storage/            # SQLCipher persistence & GRDB
├── Sources/
│   └── CatbirdMLSFFI.xcframework # Binary Rust FFI framework
├── Package.swift           # Swift Package Manager manifest
└── Scripts/
    └── rebuild-ffi.sh      # Rebuild Rust FFI and bindings
```

## Dependencies

- **GRDB.swift** (7.0+) - SQLite database toolkit
- **CatbirdMLSFFI** - Rust-based MLS protocol implementation (UniFFI)

## Getting Started

### 1. Download Pre-built XCFramework (Recommended)

Download the pinned pre-built `CatbirdMLSFFI.xcframework` from [GitHub Releases](https://github.com/joshlacal/CatbirdMLSCore/releases):

```bash
# Easy way: use the download script
./Scripts/download-ffi.sh

# To consume another immutable release explicitly:
FFI_RELEASE_TAG=ffi-w1-690d567b ./Scripts/download-ffi.sh
```

The downloader verifies the release checksum and the artifact's complete
internal manifest before replacing an installed framework. Do not extract a
release archive directly into `Sources/`; a failed or malformed extraction must
not destroy the last known-good framework.

Now you can build the package:

```bash
swift build
```

### 2. Building the FFI Layer from Source (Alternative)

If you need to build from source, the MLS FFI is built from Rust code in the [catbird-mls repository](https://github.com/joshlacal/catbird-mls).

#### Prerequisites

```bash
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Add iOS targets
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
```

#### Rebuild FFI from source

When you need to update the Rust FFI layer:

```bash
# From CatbirdMLSCore directory
./Scripts/rebuild-ffi.sh
```

This script will:
1. Use the sibling `catbird-mls` repository
2. Build the Rust FFI code for all iOS targets
3. Create `CatbirdMLSFFI.xcframework`
4. Generate Swift bindings
5. Copy everything to CatbirdMLSCore/Sources/

#### Manual FFI Build

Alternatively, build manually:

```bash
# Clone catbird-mls as a sibling if you haven't already
git clone https://github.com/joshlacal/catbird-mls.git ../catbird-mls

# From CatbirdMLSCore, build and copy the framework plus generated bindings
./Scripts/rebuild-ffi.sh
```

## Building CatbirdMLSCore

```bash
# Build the package
swift build

# Build for specific platform
swift build -c release

# Run tests (when implemented)
swift test
```

## Integration with Catbird

### In Package.swift

```swift
dependencies: [
    .package(path: "../CatbirdMLSCore")
]

targets: [
    .target(
        name: "YourTarget",
        dependencies: [
            "CatbirdMLSCore"
        ]
    )
]
```

### In Xcode Project

1. Add CatbirdMLSCore as a local Swift package
2. Link CatbirdMLSCore to your target
3. Import in Swift: `import CatbirdMLSCore`

## SQLCipher Storage

CatbirdMLSCore uses SQLCipher for encrypted database storage:

- **MLSSQLCipherEncryption** - Keychain-based encryption key management
- **MLSGRDBManager** - GRDB database manager with SQLCipher
- **MLSStorage** - High-level storage interface

Keys are stored securely in the iOS Keychain per user DID.

## Common Issues

### "Cannot find 'RustBuffer' in scope"

This means the FFI bindings are out of sync. Rebuild:

```bash
./Scripts/rebuild-ffi.sh
```

### "Binary artifact does not contain a binary"

The `CatbirdMLSFFI.xcframework` is missing. Download it from releases:

1. Run `./Scripts/download-ffi.sh` to fetch the repository's immutable default release
2. Confirm it extracted to `Sources/CatbirdMLSFFI.xcframework`
3. Or build from source: `./Scripts/rebuild-ffi.sh`

### Xcode indexing fails

1. Clean build folder: `swift package clean`
2. Rebuild FFI: `./Scripts/rebuild-ffi.sh`
3. Restart Xcode
4. Reset package caches: `File > Packages > Reset Package Caches`

## Development Workflow

1. **Make changes to Rust FFI** → Edit files in `../catbird-mls/`
2. **Rebuild FFI** → Run `./Scripts/rebuild-ffi.sh`
3. **Update Swift code** → Edit CatbirdMLSCore sources
4. **Build package** → `swift build`
5. **Test in Catbird** → Build and run Catbird app

## SQLCipher Files

SQLCipher-related files should live in CatbirdMLSCore **only**:

- ✅ `CatbirdMLSCore/Sources/CatbirdMLSCore/Storage/MLSSQLCipherEncryption.swift`
- ✅ `CatbirdMLSCore/Sources/CatbirdMLSCore/Models/MLSSQLCipherError.swift`
- ❌ ~~`Catbird/Catbird/Services/MLS/SQLCipher/`~~ (remove duplicates)

## License

MIT
