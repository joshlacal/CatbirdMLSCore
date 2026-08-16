#!/bin/bash
# Regression check: fail if Petrel appears under BOTH a remote URL identity and a
# local path identity in the CatbirdMLSCore dependency graph.
#
# Background: CatbirdMLSCore used to depend on Petrel via `.package(path: "../Petrel")`
# (local identity) while PetrelCatbird depends on Petrel via
# `.package(url: "https://github.com/joshlacal/Petrel.git", ...)` (remote identity).
# SwiftPM then resolved Petrel under two identities, producing the
# "Conflicting identity for petrel" warning. This script fails if that situation
# returns.
#
# Detection: `swift package show-dependencies` emits the warning
#   warning: 'petrelcatbird': Conflicting identity for petrel: dependency
#   'github.com/joshlacal/petrel' and dependency '/users/.../petrel' both point
#   to the same package identity 'petrel'.
# when both identities are present (the graph itself shows only the resolved
# identity). We also fail if the graph shows both a remote URL identity and a
# local path identity for petrel.
#
# Usage: scripts/check-petrel-identity.sh   (run from the CatbirdMLSCore package root)

set -euo pipefail

cd "$(dirname "$0")/.."

if command -v swift >/dev/null 2>&1; then
    deps="$(swift package show-dependencies 2>&1 || true)"
else
    echo "error: swift not found on PATH" >&2
    exit 1
fi

# Primary signal: the duplicate-identity warning emitted by SwiftPM.
conflict_warning="$(printf '%s\n' "$deps" | grep -i 'conflicting identity for petrel' || true)"

# Secondary signal: the graph itself shows both identities.
# Remote identity:   petrel<https://github.com/joshlacal/Petrel.git@...>
# Local identity:    petrel</Users/.../Petrel@unspecified>
remote_petrel="$(printf '%s\n' "$deps" | grep -i 'petrel<https://github.com/joshlacal/Petrel.git' || true)"
local_petrel="$(printf '%s\n' "$deps" | grep -i 'petrel</' || true)"

if [[ -n "$conflict_warning" ]]; then
    echo "FAIL: SwiftPM reports a conflicting identity for petrel:" >&2
    printf '%s\n' "$conflict_warning" >&2
    exit 1
fi

if [[ -n "$remote_petrel" && -n "$local_petrel" ]]; then
    echo "FAIL: Petrel resolved under BOTH remote and local identities:" >&2
    printf '%s\n' "$remote_petrel" >&2
    printf '%s\n' "$local_petrel" >&2
    exit 1
fi

echo "PASS: Petrel has a single identity in the dependency graph"
