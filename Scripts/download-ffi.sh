#!/usr/bin/env bash
set -euo pipefail

echo "📦 Downloading MLSFFICore.xcframework from GitHub Releases"
echo "==========================================================="

# Get the latest release URL
LATEST_RELEASE_URL="https://github.com/joshlacal/CatbirdMLSCore/releases/latest/download"

echo "📥 Downloading XCFramework..."
curl -LO "${LATEST_RELEASE_URL}/MLSFFICore.xcframework.zip"
curl -LO "${LATEST_RELEASE_URL}/MLSFFICore.xcframework.zip.sha256"

echo ""
echo "🔐 Verifying checksum..."
if shasum -a 256 -c MLSFFICore.xcframework.zip.sha256; then
    echo "✅ Checksum verified"
else
    echo "❌ Checksum verification failed!"
    exit 1
fi

echo ""
echo "📂 Extracting to Sources/..."
# Remove old XCFramework if it exists
rm -rf Sources/MLSFFICore.xcframework
unzip -q MLSFFICore.xcframework.zip -d Sources/

echo ""
echo "🧹 Cleaning up..."
rm MLSFFICore.xcframework.zip MLSFFICore.xcframework.zip.sha256

echo ""
echo "✅ MLSFFICore.xcframework downloaded successfully!"
echo ""
echo "📍 Next steps:"
echo "   1. Build the package: swift build"
echo "   2. Or open in Xcode to start developing"
echo ""
