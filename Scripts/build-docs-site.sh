#!/usr/bin/env bash
# Builds the static DocC site for CatbirdMLSCore into _site/ (served at
# https://joshlacal.github.io/CatbirdMLSCore/). CI and local runs use this same script.
set -euo pipefail
cd "$(dirname "$0")/.."

REPO=CatbirdMLSCore
SCHEME=CatbirdMLSCore
MODULE=CatbirdMLSCore
DESTINATION='generic/platform=macOS'
export CATBIRD_MLS_LOCAL_INTEGRATION=0  # remote Petrel deps + released FFI binary, as in CI
rm -rf _site .build/docs
xcodebuild docbuild \
  -scheme "$SCHEME" \
  -destination "$DESTINATION" \
  -derivedDataPath .build/docs \
  -skipPackagePluginValidation \
  -quiet

archive=$(find .build/docs -type d -name "$MODULE.doccarchive" | head -n 1)
if [ -z "$archive" ]; then
  echo "error: $MODULE.doccarchive not found under .build/docs" >&2
  exit 1
fi

xcrun docc process-archive transform-for-static-hosting "$archive" \
  --output-path _site \
  --hosting-base-path "$REPO"

slug=$(printf '%s' "$MODULE" | tr '[:upper:]' '[:lower:]')
test -f "_site/documentation/$slug/index.html"
cat > _site/index.html <<HTML
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>$REPO documentation</title>
<link rel="canonical" href="documentation/$slug/">
<meta http-equiv="refresh" content="0; url=documentation/$slug/">
</head>
<body>
<p><a href="documentation/$slug/">$REPO documentation</a></p>
</body>
</html>
HTML
touch _site/.nojekyll
echo "site ready: _site/documentation/$slug/"
