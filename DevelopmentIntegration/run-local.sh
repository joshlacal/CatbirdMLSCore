#!/bin/sh
set -eu
integration_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "$#" -eq 0 ]; then
    printf '%s\n' 'Usage: DevelopmentIntegration/run-local.sh swift test [options]' >&2
    exit 2
fi

case "$1 ${2:-}" in
    "swift build"|"swift test"|"swift package"|"swift run") ;;
    *)
        printf '%s\n' 'Local integration accepts swift build, test, package, or run commands' >&2
        exit 2
        ;;
esac

core_dir=$(CDPATH= cd -- "$integration_dir/.." && pwd)
config_dir="$core_dir/.build/LocalIntegration/swiftpm-config"
provenance_dir="$core_dir/.build/LocalIntegration/provenance"
mkdir -p "$provenance_dir"
provenance="$provenance_dir/apple-dependencies-$(date -u +%Y%m%dT%H%M%SZ)-$$.json"

export CATBIRD_MLS_LOCAL_INTEGRATION=1
if [ -d "$core_dir/Sources/CatbirdMLSFFI.xcframework" ]; then
    export CATBIRD_MLS_FFI_PATH="$core_dir/Sources/CatbirdMLSFFI.xcframework"
else
    unset CATBIRD_MLS_FFI_PATH
fi

python3 "$integration_dir/verify-local-dependencies.py" \
    --config-dir "$config_dir" \
    --output "$provenance" >&2
printf 'Apple dependency provenance: %s\n' "$provenance" >&2

swift_subcommand=$2
shift 2
exec swift "$swift_subcommand" --config-path "$config_dir" "$@"
