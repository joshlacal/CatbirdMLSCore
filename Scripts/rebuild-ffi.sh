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

verify_xcframework() {
    local framework_root="$1"
    local phase="$2"
    local unexpected_symlink

    if [[ ! -d "$framework_root" || -L "$framework_root" ]]; then
        echo "❌ ${phase} XCFramework must be a real directory"
        exit 1
    fi
    unexpected_symlink="$(find "$framework_root" -type l -print -quit)"
    if [[ -n "$unexpected_symlink" ]]; then
        echo "❌ ${phase} XCFramework contains a symlink: ${unexpected_symlink}"
        exit 1
    fi
    if [[ ! -s "${framework_root}/Info.plist" ]]; then
        echo "❌ ${phase} XCFramework is missing Info.plist"
        exit 1
    fi
    for framework_slice in \
        ios-arm64 \
        ios-arm64_x86_64-simulator \
        ios-arm64_x86_64-maccatalyst \
        macos-arm64_x86_64; do
        for required_component in \
            libCatbirdMLSFFI.a \
            Headers/CatbirdMLSFFI.h \
            Headers/module.modulemap; do
            if [[ ! -s "${framework_root}/${framework_slice}/${required_component}" ]]; then
                echo "❌ ${phase} XCFramework is missing ${framework_slice}/${required_component}"
                exit 1
            fi
        done
    done
}

# create-xcframework.sh materializes each native slice twice: first under
# build/libs, then inside the completed XCFramework. On release runners, keeping
# both copies while making a third package copy can exhaust the disk. Verify the
# durable outputs first, then release only the reproducible staging copies. Keep
# Cargo target outputs intact so the workflow can still save its build cache.
verify_xcframework CatbirdMLSFFI.xcframework "Completed"
if [[ ! -s build/bindings/CatbirdMLS.swift ]]; then
    echo "❌ Generated Swift binding is missing"
    exit 1
fi
rm -rf build/libs build/frameworks
echo "🧹 Released verified XCFramework staging copies"

echo ""
echo "📋 Step 2: Transferring XCFramework to CatbirdMLSCore..."
# The completed framework is already the durable build output. Copying it while
# Cargo target outputs are retained for the workflow cache needs another several
# gigabytes and can exhaust a hosted runner. Fail closed unless source and
# destination share a filesystem, then rename the verified framework into the
# package without a copy fallback. An existing destination is disposable only
# at this exact generated-artifact path.
python3 - <<'PY'
import os
from pathlib import Path
import shutil
import stat

def require_real_directory(path: Path, label: str) -> None:
    path_stat = path.lstat()
    if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISDIR(path_stat.st_mode):
        raise SystemExit(f"{label} must be a real directory: {path}")


logical_rust_root = Path(os.environ["PWD"])
require_real_directory(logical_rust_root, "Rust source path")
rust_root = Path.cwd()
if logical_rust_root.resolve(strict=True) != rust_root or rust_root.name != "catbird-mls":
    raise SystemExit("Rust source path is not the expected sibling checkout")
require_real_directory(rust_root.parent, "Workspace root")

source = rust_root / "CatbirdMLSFFI.xcframework"
core_root = rust_root.parent / "CatbirdMLSCore"
destination_parent = core_root / "Sources"
destination = destination_parent / "CatbirdMLSFFI.xcframework"

require_real_directory(core_root, "Core checkout path")
require_real_directory(destination_parent, "XCFramework destination path")

source_stat = source.lstat()
if stat.S_ISLNK(source_stat.st_mode) or not stat.S_ISDIR(source_stat.st_mode):
    raise SystemExit("completed XCFramework source must be a real directory")

parent_stat = destination_parent.lstat()
if stat.S_ISLNK(parent_stat.st_mode) or not stat.S_ISDIR(parent_stat.st_mode):
    raise SystemExit("XCFramework destination parent must be a real directory")
if source_stat.st_dev != parent_stat.st_dev:
    raise SystemExit("XCFramework source and destination must share a filesystem")

if os.path.lexists(destination):
    destination_stat = destination.lstat()
    if stat.S_ISLNK(destination_stat.st_mode) or not stat.S_ISDIR(destination_stat.st_mode):
        raise SystemExit("existing XCFramework destination is not disposable")
    shutil.rmtree(destination)

os.replace(source, destination)
if os.path.lexists(source):
    raise SystemExit("XCFramework source still exists after transfer")
PY

verify_xcframework \
    ../CatbirdMLSCore/Sources/CatbirdMLSFFI.xcframework \
    "Transferred"
if [[ -e CatbirdMLSFFI.xcframework ]]; then
    echo "❌ XCFramework source still exists after transfer"
    exit 1
fi
echo "✅ XCFramework transferred"

echo ""
echo "📄 Step 3: Updating Swift bindings..."
if [ -f "build/bindings/CatbirdMLS.swift" ]; then
    # Remove any stale bindings (e.g. old MLSFFI.swift from pre-consolidation)
    rm -f ../CatbirdMLSCore/Sources/CatbirdMLS/MLSFFI.swift
    cp build/bindings/CatbirdMLS.swift ../CatbirdMLSCore/Sources/CatbirdMLS/
    if [[ ! -s ../CatbirdMLSCore/Sources/CatbirdMLS/CatbirdMLS.swift ]]; then
        echo "❌ Generated Swift binding was not installed"
        exit 1
    fi
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
