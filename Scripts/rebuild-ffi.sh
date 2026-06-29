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

echo ""
echo "📋 Step 2: Copying XCFramework to CatbirdMLSCore..."
rm -rf ../CatbirdMLSCore/Sources/CatbirdMLSFFI.xcframework
cp -R CatbirdMLSFFI.xcframework ../CatbirdMLSCore/Sources/
echo "✅ XCFramework copied"

echo ""
echo "📄 Step 3: Updating Swift bindings..."
if [ -f "build/bindings/CatbirdMLS.swift" ]; then
    # Remove any stale bindings (e.g. old MLSFFI.swift from pre-consolidation)
    rm -f ../CatbirdMLSCore/Sources/CatbirdMLS/MLSFFI.swift
    cp build/bindings/CatbirdMLS.swift ../CatbirdMLSCore/Sources/CatbirdMLS/
    echo "✅ Swift bindings updated"
else
    echo "⚠️  Warning: build/bindings/CatbirdMLS.swift not found"
    echo "   The XCFramework was built but Swift bindings weren't generated"
    exit 1
fi

echo ""
echo "✅ FFI rebuild complete!"
echo ""
echo "📍 Next steps:"
echo "   1. Open CatbirdMLSCore in Xcode or build with: swift build"
echo "   2. The package should now index and build correctly"
echo ""
