#!/bin/sh
set -eu

scripts_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
core_dir=$(CDPATH= cd -- "$scripts_dir/.." && pwd -P)

: "${CATBIRD_MLS_CANDIDATE_ROOT:?Set CATBIRD_MLS_CANDIDATE_ROOT to the candidate tuple root}"
: "${CATBIRD_MLS_FFI_PATH:?Set CATBIRD_MLS_FFI_PATH to the candidate-built local xcframework}"
: "${SIM_POSTGRES_BASE_URL:?Set SIM_POSTGRES_BASE_URL to a disposable loopback PostgreSQL instance}"

candidate_root=$(CDPATH= cd -- "$CATBIRD_MLS_CANDIDATE_ROOT" && pwd -P)
sim_dir=$(CDPATH= cd -- "$candidate_root/catbird-sim" && pwd -P)
native_dir=$(CDPATH= cd -- "$candidate_root/catbird-mls" && pwd -P)
atproto_dir=$(CDPATH= cd -- "$candidate_root/catbird-atproto" && pwd -P)
labyrinth_dir=$(CDPATH= cd -- "$candidate_root/labyrinth" && pwd -P)
server_root=$(CDPATH= cd -- "$candidate_root/mls-ds" && pwd -P)
server_dir="$server_root/server"
ffi_path=$(CDPATH= cd -- "$CATBIRD_MLS_FFI_PATH" && pwd -P)

case "$ffi_path" in
  "$core_dir/Sources/CatbirdMLSFFI.xcframework") ;;
  *)
    echo "CATBIRD_MLS_FFI_PATH must select the full local framework at $core_dir/Sources/CatbirdMLSFFI.xcframework" >&2
    exit 2
    ;;
esac

for required in \
  "$sim_dir/Cargo.toml" \
  "$sim_dir/src/bin/catbird-sim.rs" \
  "$native_dir/Cargo.toml" \
  "$atproto_dir/Cargo.toml" \
  "$labyrinth_dir/Cargo.toml" \
  "$labyrinth_dir/labyrinth-protocol/Cargo.toml" \
  "$server_dir/Cargo.toml" \
  "$server_root/Cargo.toml" \
  "$ffi_path/labyrinth-space-protocol.json" \
  "$core_dir/Sources/CatbirdMLS/CatbirdMLS.swift"
do
  if [ ! -f "$required" ]; then
    echo "Required candidate input is missing: $required" >&2
    exit 2
  fi
done

expected_fingerprint=$(
  python3 "$scripts_dir/apple-artifact-manifest.py" source-fingerprint \
    --native "$native_dir" \
    --dependency "$atproto_dir" \
    --dependency "$labyrinth_dir"
)
python3 - \
  "$ffi_path/labyrinth-space-protocol.json" \
  "$expected_fingerprint" \
  "$core_dir/Sources/CatbirdMLS/CatbirdMLS.swift" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

marker_path = Path(sys.argv[1])
expected_fingerprint = sys.argv[2]
binding_path = Path(sys.argv[3])
marker = json.loads(marker_path.read_text())
if marker.get("protocolVersion") != 20261023:
    raise SystemExit("Candidate FFI has the wrong clean-slate protocol version")
if marker.get("sourceFingerprint") != expected_fingerprint:
    raise SystemExit("Candidate FFI source fingerprint does not match the candidate native source bundle")
if "labyrinth" not in marker.get("features", []):
    raise SystemExit("Candidate FFI was not built with the labyrinth feature")
if marker.get("artifactScope") != "apple-all-targets":
    raise SystemExit("Candidate FFI is not the full seven-target Apple artifact")
binding_hash = hashlib.sha256(binding_path.read_bytes()).hexdigest()
if marker.get("generatedSwiftSHA256") != binding_hash:
    raise SystemExit("Candidate FFI and generated Swift binding are not paired")
PY

qualification_root="$core_dir/.build/Golden50Qualification"
mkdir -p "$qualification_root"
work_root=$(mktemp -d "$qualification_root/run.XXXXXX")
cleanup() {
  rm -rf "$work_root"
}
trap cleanup EXIT HUP INT TERM

export CARGO_TARGET_DIR="${CATBIRD_GOLDEN_50_CARGO_TARGET_DIR:-$qualification_root/cargo-target}"
metadata_path="$work_root/cargo-metadata.json"
cargo metadata \
  --offline \
  --locked \
  --no-deps \
  --format-version 1 \
  --manifest-path "$sim_dir/Cargo.toml" > "$metadata_path"
python3 - "$metadata_path" "$candidate_root" <<'PY'
import json
import sys
from pathlib import Path

metadata = json.loads(Path(sys.argv[1]).read_text())
root = Path(sys.argv[2]).resolve()
packages = [package for package in metadata["packages"] if package["name"] == "catbird-sim"]
if len(packages) != 1:
    raise SystemExit("Cargo metadata did not resolve exactly one candidate catbird-sim package")
dependencies = {dependency["name"]: dependency.get("path") for dependency in packages[0]["dependencies"]}
expected = {
    "catbird_mls": root / "catbird-mls",
    "catbird-server": root / "mls-ds" / "server",
    "atproto-spaces": root / "catbird-atproto" / "atproto-spaces",
    "labyrinth": root / "labyrinth",
}
for name, path in expected.items():
    actual = dependencies.get(name)
    if actual is None or Path(actual).resolve() != path.resolve():
        raise SystemExit(f"candidate catbird-sim dependency {name} resolved to {actual!r}, expected {path}")
PY

replay_fingerprint_before=$(
  python3 "$scripts_dir/apple-artifact-manifest.py" source-fingerprint \
    --native "$sim_dir" \
    --dependency "$native_dir" \
    --dependency "$server_root" \
    --dependency "$atproto_dir" \
    --dependency "$labyrinth_dir"
)

fixture_dir="$work_root/fixtures"
cargo run \
  --offline \
  --locked \
  --release \
  --manifest-path "$sim_dir/Cargo.toml" \
  --bin catbird-sim \
  -- export-golden-50 --out "$fixture_dir"
cargo run \
  --offline \
  --locked \
  --release \
  --manifest-path "$sim_dir/Cargo.toml" \
  --bin catbird-sim \
  -- verify-golden-50 --dir "$fixture_dir"
replay_fingerprint_after=$(
  python3 "$scripts_dir/apple-artifact-manifest.py" source-fingerprint \
    --native "$sim_dir" \
    --dependency "$native_dir" \
    --dependency "$server_root" \
    --dependency "$atproto_dir" \
    --dependency "$labyrinth_dir"
)
if [ "$replay_fingerprint_after" != "$replay_fingerprint_before" ]; then
  echo "Candidate simulator or native dependency source changed during Golden50 replay" >&2
  exit 1
fi
printf '%s\n' 'catbird.golden50.replay-verified.v1' > "$fixture_dir/.candidate-replay-verified"

export CATBIRD_GOLDEN_50_DIR="$fixture_dir"
export CATBIRD_MLS_FFI_PATH="$ffi_path"
"$core_dir/DevelopmentIntegration/run-local.sh" \
  swift test \
  --scratch-path "$qualification_root/swift" \
  --filter CatbirdMLSGolden50Tests
