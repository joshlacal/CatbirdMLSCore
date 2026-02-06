#!/usr/bin/env bash
set -euo pipefail

echo "🔨 Rebuilding MLS FFI for CatbirdMLSCore"
echo "=========================================="

ensure_ios_sdk() {
    if xcrun --sdk iphoneos --show-sdk-path >/dev/null 2>&1; then
        return
    fi

    if [ -d "/Applications/Xcode.app/Contents/Developer" ]; then
        echo "ℹ️  iphoneos SDK not found with current developer directory; using /Applications/Xcode.app"
        export DEVELOPER_DIR="/Applications/Xcode.app/Contents/Developer"
    fi

    if ! xcrun --sdk iphoneos --show-sdk-path >/dev/null 2>&1; then
        echo "❌ iOS SDK (iphoneos) cannot be located."
        echo "   Fix by installing Xcode and selecting it:"
        echo "   sudo xcode-select -s /Applications/Xcode.app/Contents/Developer"
        exit 1
    fi
}

ensure_ios_sdk

# Navigate to MLSFFI build directory
cd "$(dirname "$0")/../../MLSFFI/mls-ffi"

# Run the XCFramework build script
echo "📦 Step 1: Building XCFramework..."
./create-xcframework.sh

echo ""
echo "📋 Step 2: Copying XCFramework to CatbirdMLSCore..."
rm -rf ../../CatbirdMLSCore/Sources/MLSFFICore.xcframework
cp -R MLSFFICore.xcframework ../../CatbirdMLSCore/Sources/

echo ""
echo "📄 Step 3: Updating Swift bindings..."
if [ -f "build/bindings/MLSFFI.swift" ]; then
    cp build/bindings/MLSFFI.swift ../../CatbirdMLSCore/Sources/MLSFFI/
    echo "✅ Swift bindings updated"
else
    echo "⚠️  Warning: build/bindings/MLSFFI.swift not found"
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
