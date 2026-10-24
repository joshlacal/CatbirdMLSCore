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

export CATBIRD_MLS_APPLE_SCOPE="${CATBIRD_MLS_APPLE_SCOPE:-apple-all-targets}"
case "$CATBIRD_MLS_APPLE_SCOPE" in
    apple-all-targets) framework_name="CatbirdMLSFFI.xcframework" ;;
    ios-arm64-simulator-only) framework_name="CatbirdMLSFFISimulator.xcframework" ;;
    *) echo "Unsupported Apple artifact scope: $CATBIRD_MLS_APPLE_SCOPE" >&2; exit 1 ;;
esac

# This wrapper emits a Space-capable artifact marker. It must not certify an
# artifact built with the feature disabled by an inherited environment value.
export CATBIRD_MLS_FEATURES="${CATBIRD_MLS_FEATURES-labyrinth}"
if ! python3 - "$CATBIRD_MLS_FEATURES" <<'PY'
import re
import sys
sys.exit(0 if "labyrinth" in re.split(r"[\s,]+", sys.argv[1]) else 1)
PY
then
    echo "❌ The verified Space rebuild requires CATBIRD_MLS_FEATURES to include labyrinth."
    exit 1
fi

core_scripts_dir="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
core_dir="$(CDPATH= cd -- "$core_scripts_dir/.." && pwd)"
artifact_manifest_tool="$core_scripts_dir/apple-artifact-manifest.py"
# The public v1.5.21 artifact keeps its original generated interface.
# Updated callback metadata has new UniFFI checksums even when signatures match.
space_bindings_dir="$core_dir/Sources/CatbirdMLSSpaces"
if [ ! -f "$artifact_manifest_tool" ]; then
    echo "❌ Apple artifact manifest tool is absent: $artifact_manifest_tool"
    exit 1
fi

# An immutable JJ build workspace may be selected while the shared native
# feature workspace continues evolving. The default remains the exact sibling.
native_dir="${CATBIRD_MLS_NATIVE_DIR:-$core_dir/../catbird-mls}"
if [ ! -d "$native_dir" ] || [ ! -f "$native_dir/create-xcframework.sh" ]; then
    echo "❌ CATBIRD_MLS_NATIVE_DIR is not a catbird-mls source tree: $native_dir"
    exit 1
fi
native_dir="$(CDPATH= cd -- "$native_dir" && pwd)"
atproto_dir="${CATBIRD_ATPROTO_NATIVE_DIR:-$native_dir/../catbird-atproto}"
if [ ! -d "$atproto_dir" ] || [ ! -f "$atproto_dir/Cargo.toml" ]; then
    echo "❌ CATBIRD_ATPROTO_NATIVE_DIR is not a catbird-atproto source tree: $atproto_dir"
    exit 1
fi
atproto_dir="$(CDPATH= cd -- "$atproto_dir" && pwd)"
cd "$native_dir"

backup_timestamp="${FFI_BACKUP_TIMESTAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
case "$backup_timestamp" in
    *[!0-9TZ]*)
        echo "❌ FFI_BACKUP_TIMESTAMP contains unsafe characters: $backup_timestamp"
        exit 1
        ;;
esac
backup_root="$core_dir/_trash/ffi-rebuild/$backup_timestamp"
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

echo "🗃️  Preserving prior native generator outputs..."
preserve_if_present "$framework_name" "native-$framework_name"
preserve_if_present build/frameworks native-build-frameworks
preserve_if_present build/bindings native-build-bindings
preserve_if_present build/libs native-build-libs

# Capture the full generator-relevant Rust input before the long multi-target
# build. A mutable JJ working copy can otherwise change while the generator is
# running and leave artifacts whose recorded revision does not describe them.
source_fingerprint_before="$(python3 "$artifact_manifest_tool" source-fingerprint --native "$PWD" --dependency "$atproto_dir")"

# Run the XCFramework build script
echo "📦 Step 1: Building XCFramework..."
./create-xcframework.sh

source_fingerprint_after="$(python3 "$artifact_manifest_tool" source-fingerprint --native "$PWD" --dependency "$atproto_dir")"
if [ "$source_fingerprint_before" != "$source_fingerprint_after" ]; then
    echo "❌ Native source or local path dependency changed during FFI generation; refusing to install mixed-input artifacts."
    echo "   Before: $source_fingerprint_before"
    echo "   After:  $source_fingerprint_after"
    exit 1
fi

if [ ! -d "$framework_name" ]; then
    echo "❌ XCFramework build produced no $framework_name"
    exit 1
fi
if [ ! -f "build/bindings/CatbirdMLS.swift" ]; then
    echo "❌ XCFramework build produced no Swift bindings"
    exit 1
fi

# Reject generator drift before either output can replace the reviewed pair.
if [ "${FFI_VERIFY_COMMITTED_BINDINGS:-0}" = "1" ]; then
    cmp build/bindings/CatbirdMLS.swift "$space_bindings_dir/CatbirdMLS.swift"
fi

echo ""
echo "🗃️  Step 2: Preserving prior Core artifacts..."
preserve_if_present "$core_dir/Sources/$framework_name" "$framework_name"
preserve_if_present "$space_bindings_dir/CatbirdMLS.swift" CatbirdMLSSpaces.swift
preserve_if_present "$space_bindings_dir/MLSFFI.swift" MLSFFISpaces.swift

echo ""
echo "📋 Step 3: Copying XCFramework to CatbirdMLSCore..."
cp -R "$framework_name" "$core_dir/Sources/"
# Written only after the complete native build and source-stability check.
# This permits the explicit local package selector to expose Space callbacks;
# ordinary builds using the older public artifact remain unavailable.
python3 - "$core_dir/Sources/$framework_name/labyrinth-space-protocol.json" "$source_fingerprint_after" "$CATBIRD_MLS_FEATURES" "$CATBIRD_MLS_APPLE_SCOPE" <<'PY'
import json
from pathlib import Path
import re
import sys
Path(sys.argv[1]).write_text(json.dumps({
    "protocolVersion": 20261023,
    "sourceFingerprint": sys.argv[2],
    "features": sorted(set(filter(None, re.split(r"[\s,]+", sys.argv[3])))),
    "artifactScope": sys.argv[4],
}, sort_keys=True) + "\n")
PY
echo "✅ XCFramework copied"

echo ""
echo "📄 Step 4: Updating Swift bindings..."
mkdir -p "$space_bindings_dir"
cp build/bindings/CatbirdMLS.swift "$space_bindings_dir/"
echo "✅ Swift bindings updated"

echo ""
echo "✅ FFI rebuild complete!"
echo ""
echo "📍 Next steps:"
echo "   1. Open CatbirdMLSCore in Xcode or build with: swift build"
echo "   2. The package should now index and build correctly"
echo ""
