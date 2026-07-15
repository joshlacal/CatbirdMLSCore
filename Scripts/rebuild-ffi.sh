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

# create-xcframework.sh materializes each native slice twice: first under
# build/libs, then inside the completed XCFramework. On release runners, keeping
# both copies while making a third package copy can exhaust the disk. Verify the
# durable outputs first, then release only the reproducible staging copies. Keep
# Cargo target outputs intact so the workflow can still save its build cache.
for framework_library in \
    CatbirdMLSFFI.xcframework/ios-arm64/libCatbirdMLSFFI.a \
    CatbirdMLSFFI.xcframework/ios-arm64_x86_64-simulator/libCatbirdMLSFFI.a \
    CatbirdMLSFFI.xcframework/ios-arm64_x86_64-maccatalyst/libCatbirdMLSFFI.a \
    CatbirdMLSFFI.xcframework/macos-arm64_x86_64/libCatbirdMLSFFI.a; do
    if [[ ! -s "$framework_library" ]]; then
        echo "❌ Completed XCFramework is missing ${framework_library}"
        exit 1
    fi
done
if [[ ! -s build/bindings/CatbirdMLS.swift ]]; then
    echo "❌ Generated Swift binding is missing"
    exit 1
fi
rm -rf build/libs build/frameworks
echo "🧹 Released verified XCFramework staging copies"

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
