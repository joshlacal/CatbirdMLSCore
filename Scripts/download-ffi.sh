#!/usr/bin/env bash
set -euo pipefail

echo "📦 Downloading CatbirdMLSCore.xcframework from GitHub Releases"
echo "==========================================================="

# Get the latest release URL
LATEST_RELEASE_URL="https://github.com/joshlacal/CatbirdMLSCore/releases/latest/download"

echo "📥 Downloading XCFramework..."
curl -LO "${LATEST_RELEASE_URL}/CatbirdMLSCore.xcframework.zip"
curl -LO "${LATEST_RELEASE_URL}/CatbirdMLSCore.xcframework.zip.sha256"

echo ""
echo "🔐 Verifying checksum..."
if shasum -a 256 -c CatbirdMLSCore.xcframework.zip.sha256; then
    echo "✅ Checksum verified"
else
    echo "❌ Checksum verification failed!"
    exit 1
fi

echo ""
echo "📂 Extracting to Sources/..."
# Remove old XCFramework if it exists
rm -rf Sources/CatbirdMLSCore.xcframework
unzip -q CatbirdMLSCore.xcframework.zip -d Sources/

echo ""
echo "🧹 Cleaning up..."
rm CatbirdMLSCore.xcframework.zip CatbirdMLSCore.xcframework.zip.sha256

echo ""
echo "✅ CatbirdMLSCore.xcframework downloaded successfully!"
echo ""
echo "📍 Next steps:"
echo "   1. Build the package: swift build"
echo "   2. Or open in Xcode to start developing"
echo ""
