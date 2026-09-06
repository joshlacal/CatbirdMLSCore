#!/bin/sh
set -eu
integration_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
python3 "$integration_dir/prepare-local.py" >&2
if [ "$#" -eq 0 ]; then
    printf '%s\n' 'Usage: DevelopmentIntegration/run-local.sh swift test [options]' >&2
    exit 2
fi
export CATBIRD_MLS_LOCAL_INTEGRATION=1
exec "$@"
