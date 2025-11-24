# CatbirdMLSCore

Shared Swift package for MLS (Messaging Layer Security) functionality in Catbird.

## Overview

CatbirdMLSCore provides the core MLS implementation shared between:
- Catbird main app
- Notification Service Extension
- Other app extensions

## Architecture

```
CatbirdMLSCore/
├── Sources/CatbirdMLSCore/
│   ├── Core/               # Core MLS context and managers
│   ├── Extensions/         # Swift extensions
│   ├── FFI/                # Rust FFI bindings (auto-generated)
│   ├── Models/             # Data models
│   └── Storage/            # SQLCipher persistence & GRDB
├── Sources/
│   └── MLSFFICore.xcframework   # Binary Rust FFI framework
├── Package.swift           # Swift Package Manager manifest
└── Scripts/
    └── rebuild-ffi.sh      # Rebuild Rust FFI and bindings
```

## Dependencies

- **GRDB.swift** (7.0+) - SQLite database toolkit
- **MLSFFICore** - Rust-based MLS protocol implementation (UniFFI)

## Getting Started

### 1. Download Pre-built XCFramework (Recommended)

Download the latest pre-built `MLSFFICore.xcframework` from [GitHub Releases](https://github.com/joshlacal/CatbirdMLSCore/releases):

```bash
# Easy way: use the download script
./Scripts/download-ffi.sh

# Or manually:
curl -LO https://github.com/joshlacal/CatbirdMLSCore/releases/latest/download/MLSFFICore.xcframework.zip
curl -LO https://github.com/joshlacal/CatbirdMLSCore/releases/latest/download/MLSFFICore.xcframework.zip.sha256
shasum -a 256 -c MLSFFICore.xcframework.zip.sha256
unzip MLSFFICore.xcframework.zip -d Sources/
rm MLSFFICore.xcframework.zip MLSFFICore.xcframework.zip.sha256
```

Now you can build the package:

```bash
swift build
```

### 2. Building the FFI Layer from Source (Alternative)

If you need to build from source, the MLS FFI is built from Rust code in the [mls-ffi repository](https://github.com/joshlacal/mls-ffi).

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
1. Clone/use the mls-ffi repository
2. Build the Rust FFI code for all iOS targets
3. Create the MLSFFICore.xcframework
4. Generate Swift bindings
5. Copy everything to CatbirdMLSCore/Sources/

#### Manual FFI Build

Alternatively, build manually:

```bash
# Clone mls-ffi if you haven't already
git clone https://github.com/joshlacal/mls-ffi.git ../MLSFFI/mls-ffi

cd ../MLSFFI/mls-ffi
./create-xcframework.sh

# Copy outputs back to CatbirdMLSCore
cp -R MLSFFICore.xcframework ../../CatbirdMLSCore/Sources/
cp build/bindings/MLSFFI.swift ../../CatbirdMLSCore/Sources/MLSFFI/
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

The MLSFFICore.xcframework is missing. Download it from releases:

1. Download from [GitHub Releases](https://github.com/joshlacal/CatbirdMLSCore/releases/latest)
2. Extract to `Sources/MLSFFICore.xcframework`
3. Or build from source: `./Scripts/rebuild-ffi.sh`

### Xcode indexing fails

1. Clean build folder: `swift package clean`
2. Rebuild FFI: `./Scripts/rebuild-ffi.sh`
3. Restart Xcode
4. Reset package caches: `File > Packages > Reset Package Caches`

## Development Workflow

1. **Make changes to Rust FFI** → Edit files in `MLSFFI/mls-ffi/src/`
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

Proprietary - Catbird App
