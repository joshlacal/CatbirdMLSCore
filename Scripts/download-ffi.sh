#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
# Check binding and version before making a release request or replacing an installed artifact.
selection=$(python3 - <<'PY'
import hashlib, json, os, re
from pathlib import Path
inputs = json.loads(Path("Scripts/ffi-release-inputs.json").read_text())
binding = hashlib.sha256(Path("Sources/CatbirdMLS/CatbirdMLS.swift").read_bytes()).hexdigest()
if binding != inputs.get("generated_swift_sha256"):
    raise SystemExit("No published CatbirdMLSFFI release matches these bindings. Use the verified local rebuild.")
tag = inputs["release_tag"]
digest = inputs["archive_sha256"]
if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag) or not re.fullmatch(r"[0-9a-f]{64}", digest):
    raise SystemExit("Invalid published FFI selection")
if os.environ.get("FFI_RELEASE_TAG", tag) != tag:
    raise SystemExit("The requested tag differs from this package's pinned release")
print(tag, digest)
PY
)
read -r release_tag archive_sha <<< "$selection"
mkdir -p .build
stage=$(mktemp -d .build/ffi-download.XXXXXXXX)
asset=CatbirdMLSFFI.xcframework.zip
curl -fL --retry 3 "https://github.com/joshlacal/CatbirdMLSCore/releases/download/$release_tag/$asset" -o "$stage/$asset"
(cd "$stage" && echo "$archive_sha  $asset" | shasum -a 256 -c -)
ditto -x -k "$stage/$asset" "$stage"
test -f "$stage/CatbirdMLSFFI.xcframework/Info.plist"
installed=Sources/CatbirdMLSFFI.xcframework
if [[ -d "$installed" ]]; then
  mv "$installed" "$stage/previous.xcframework"
fi
if ! mv "$stage/CatbirdMLSFFI.xcframework" "$installed"; then
  [[ ! -d "$stage/previous.xcframework" ]] || mv "$stage/previous.xcframework" "$installed"
  exit 1
fi
echo "Installed verified $release_tag artifact. Download and any previous copy are retained in $stage."
