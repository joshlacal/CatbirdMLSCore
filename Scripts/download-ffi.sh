#!/usr/bin/env bash
set -euo pipefail

# Downloads the prebuilt Rust FFI binary target (Sources/CatbirdMLSFFI.xcframework)
# from GitHub Releases. The xcframework is gitignored (≈4 GB unzipped); CI and
# fresh clones fetch it with this script. To rebuild from Rust sources instead,
# use Scripts/rebuild-ffi.sh.
#
# Env:
#   FFI_RELEASE_TAG  release tag to pin (default: latest release)

ASSET="CatbirdMLSFFI.xcframework.zip"
REPO="joshlacal/CatbirdMLSCore"

if [[ -n "${FFI_RELEASE_TAG:-}" ]]; then
    BASE_URL="https://github.com/${REPO}/releases/download/${FFI_RELEASE_TAG}"
else
    BASE_URL="https://github.com/${REPO}/releases/latest/download"
fi

echo "📦 Downloading ${ASSET} from ${BASE_URL}"

cd "$(dirname "$0")/.."

curl -fL --retry 3 -O "${BASE_URL}/${ASSET}"

# Checksum must come from the same release as the zip. Releases built before the
# build-ffi workflow started uploading it have no .sha256 asset.
if curl -fL --retry 3 -O "${BASE_URL}/${ASSET}.sha256" 2>/dev/null; then
    echo "🔐 Verifying checksum..."
    shasum -a 256 -c "${ASSET}.sha256"
else
    echo "⚠️  No ${ASSET}.sha256 published for this release; skipping verification"
fi

echo "📂 Extracting to Sources/CatbirdMLSFFI.xcframework..."
rm -rf Sources/CatbirdMLSFFI.xcframework
# Zip was created with `ditto -c -k --keepParent`, so the archive root is
# CatbirdMLSFFI.xcframework/ itself.
unzip -q "${ASSET}" -d Sources/

rm -f "${ASSET}" "${ASSET}.sha256"

[[ -d Sources/CatbirdMLSFFI.xcframework ]] || { echo "❌ extraction produced no Sources/CatbirdMLSFFI.xcframework"; exit 1; }
echo "✅ Sources/CatbirdMLSFFI.xcframework ready"
