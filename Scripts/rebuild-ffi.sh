#!/usr/bin/env bash
set -euo pipefail

echo "🔨 Rebuilding MLS FFI for CatbirdMLSCore"
echo "=========================================="

ensure_ios_sdk() {
    if xcrun --sdk iphoneos --show-sdk-path >/dev/null 2>&1; then
        return
    fi

    # Current developer dir (often CommandLineTools) lacks the iOS SDK.
    # Fall back to any installed Xcode — release first, then beta.
    for candidate in /Applications/Xcode.app /Applications/Xcode-beta.app; do
        if [ -d "$candidate/Contents/Developer" ]; then
            echo "ℹ️  iphoneos SDK not found with current developer directory; using $candidate"
            export DEVELOPER_DIR="$candidate/Contents/Developer"
            if xcrun --sdk iphoneos --show-sdk-path >/dev/null 2>&1; then
                return
            fi
        fi
    done

    if ! xcrun --sdk iphoneos --show-sdk-path >/dev/null 2>&1; then
        echo "❌ iOS SDK (iphoneos) cannot be located."
        echo "   Fix permanently by selecting your Xcode:"
        echo "   sudo xcode-select -s /Applications/Xcode-beta.app/Contents/Developer"
        exit 1
    fi
}

ensure_ios_sdk

# Navigate to catbird-mls build directory
cd "$(dirname "$0")/../../catbird-mls"

# Run the XCFramework build script
echo "📦 Step 1: Building XCFramework..."
./create-xcframework.sh

if [ ! -d "CatbirdMLSFFI.xcframework" ]; then
    echo "❌ XCFramework build produced no CatbirdMLSFFI.xcframework"
    exit 1
fi
if [ ! -f "build/bindings/CatbirdMLS.swift" ]; then
    echo "❌ XCFramework build produced no Swift bindings"
    exit 1
fi

# Reject generator drift before either output can replace the reviewed pair.
if [ "${FFI_VERIFY_COMMITTED_BINDINGS:-0}" = "1" ]; then
    cmp build/bindings/CatbirdMLS.swift ../CatbirdMLSCore/Sources/CatbirdMLS/CatbirdMLS.swift
fi

backup_timestamp="${FFI_BACKUP_TIMESTAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
case "$backup_timestamp" in
    *[!0-9TZ]*)
        echo "❌ FFI_BACKUP_TIMESTAMP contains unsafe characters: $backup_timestamp"
        exit 1
        ;;
esac
backup_root="../CatbirdMLSCore/_trash/ffi-rebuild/$backup_timestamp"
if [ -e "$backup_root" ]; then
    echo "❌ FFI backup destination already exists: $backup_root"
    exit 1
fi
mkdir -p "$backup_root"

preserve_if_present() {
    source_path="$1"
    destination_name="$2"
    if [ -e "$source_path" ]; then
        mv "$source_path" "$backup_root/$destination_name"
        echo "🗃️  Preserved $source_path at $backup_root/$destination_name"
    fi
}

echo ""
echo "🗃️  Step 2: Preserving prior Core artifacts..."
preserve_if_present ../CatbirdMLSCore/Sources/CatbirdMLSFFI.xcframework CatbirdMLSFFI.xcframework
preserve_if_present ../CatbirdMLSCore/Sources/CatbirdMLS/CatbirdMLS.swift CatbirdMLS.swift
preserve_if_present ../CatbirdMLSCore/Sources/CatbirdMLS/MLSFFI.swift MLSFFI.swift

echo ""
echo "📋 Step 3: Copying XCFramework to CatbirdMLSCore..."
cp -R CatbirdMLSFFI.xcframework ../CatbirdMLSCore/Sources/
echo "✅ XCFramework copied"

echo ""
echo "📄 Step 4: Updating Swift bindings..."
cp build/bindings/CatbirdMLS.swift ../CatbirdMLSCore/Sources/CatbirdMLS/
echo "✅ Swift bindings updated"

echo ""
echo "✅ FFI rebuild complete!"
echo ""
echo "📍 Next steps:"
echo "   1. Open CatbirdMLSCore in Xcode or build with: swift build"
echo "   2. The package should now index and build correctly"
echo ""
