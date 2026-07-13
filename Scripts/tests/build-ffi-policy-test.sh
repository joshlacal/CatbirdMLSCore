#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
readonly REPO_ROOT
readonly WORKFLOW="${REPO_ROOT}/.github/workflows/build-ffi.yml"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_contains() {
    local needle="$1"
    grep -Fq -- "$needle" "$WORKFLOW" || fail "workflow is missing: $needle"
}

assert_absent() {
    local needle="$1"
    if grep -Fq -- "$needle" "$WORKFLOW"; then
        fail "workflow still contains forbidden mutable-release behavior: $needle"
    fi
}

assert_contains "default: ffi-w1-690d567b"
assert_contains "default: 690d567b6d892064ae315f8776c3537006d88f93"
assert_contains "default: 8b2f92e28097c0788492f2f82328f4ab5b032953"
assert_contains "default: 6ae52433268312c716ea96526762b4e38a5967ec"
assert_contains "default: 22781c730383dd5faa690afe1e002f97876e8ef0"
assert_contains "name: Reject existing release tag or assets"
assert_contains "\"repos/\${GITHUB_REPOSITORY}/releases?per_page=100\""
assert_contains "--paginate"
assert_contains "name: Generate complete internal artifact manifest"
assert_contains "name: Reject generated binding drift"
assert_contains "git diff --exit-code -- Sources/CatbirdMLS/CatbirdMLS.swift"
assert_contains "COPYFILE_DISABLE=1 ditto"
assert_contains "--norsrc --noextattr --noqtn --noacl"
assert_contains "name: Verify archived artifact coverage"
assert_contains "\"\${verification_root}/source-files.txt\""
assert_contains "\"\${verification_root}/archive-files.txt\""
assert_contains "name: Write source, toolchain, and checksum provenance"
assert_contains "ffi-provenance.txt"
assert_contains "internal_manifest_sha256="
assert_contains "actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5 # v4.3.1"
assert_contains "actions/cache@0057852bfaa89a56745cba8c7296529d2fc39830 # v4.3.0"
assert_contains "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02 # v4.6.2"
assert_contains "group: ffi-release-\${{ inputs.release_tag }}"
assert_contains "\"repos/\${GITHUB_REPOSITORY}/git/refs\""
assert_contains "--verify-tag"
assert_contains "--latest=false"
assert_contains "gh release create"
assert_absent "--clobber"
assert_absent "gh release upload"
assert_absent "gh release edit"
assert_absent "releases/latest/download"

extract_step_script() {
    local step_name="$1"
    local output="$2"
    awk -v expected="      - name: ${step_name}" '
        $0 == expected { in_step = 1; next }
        in_step && /^      - name:/ { exit }
        in_step && /^        run: \|/ { in_run = 1; next }
        in_run { sub(/^          /, ""); print }
    ' "$WORKFLOW" > "$output"
    [[ -s "$output" ]] || fail "could not extract workflow step: $step_name"
}

assert_preflight_result() {
    local expected_result="$1"
    local release_tags="${2-}"
    local fixture_root
    fixture_root="$(mktemp -d)"
    local mock_bin="${fixture_root}/bin"
    local step_script="${fixture_root}/preflight.sh"
    mkdir -p "$mock_bin"
    extract_step_script "Reject existing release tag or assets" "$step_script"

    cat > "${mock_bin}/gh" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "$args" == *"/git/ref/tags/"* ]]; then
    printf 'HTTP/2.0 404 Not Found\n'
    exit 1
fi
if [[ "$args" == *"/releases?per_page=100"* ]]; then
    if [[ "${MOCK_RELEASE_API_FAIL:-0}" == "1" ]]; then
        exit 2
    fi
    printf '%s' "${MOCK_RELEASE_TAGS:-}"
    exit 0
fi
exit 99
MOCK
    chmod +x "${mock_bin}/gh"

    set +e
    PATH="${mock_bin}:$PATH" \
        GITHUB_REPOSITORY="joshlacal/CatbirdMLSCore" \
        RELEASE_TAG="ffi-w1-690d567b" \
        MOCK_RELEASE_TAGS="$release_tags" \
        bash "$step_script" >/dev/null 2>&1
    result_code=$?
    set -e

    case "$expected_result" in
        success)
            [[ "$result_code" -eq 0 ]] || fail "preflight rejected an absent tag/release"
            ;;
        failure)
            [[ "$result_code" -ne 0 ]] || fail "preflight accepted an existing draft release"
            ;;
        *)
            fail "unknown expected preflight result: $expected_result"
            ;;
    esac
}

assert_preflight_result success ""
assert_preflight_result failure $'ffi-w1-690d567b\n'

assert_manifest_generation() {
    local fixture_root
    fixture_root="$(mktemp -d)"
    local step_script="${fixture_root}/generate-manifest.sh"
    local manifest="${fixture_root}/Sources/CatbirdMLSFFI.xcframework/SHA256SUMS"
    mkdir -p \
        "${fixture_root}/Sources/CatbirdMLS" \
        "${fixture_root}/Sources/CatbirdMLSFFI.xcframework"
    printf 'fixture binding\n' > "${fixture_root}/Sources/CatbirdMLS/CatbirdMLS.swift"
    printf 'fixture framework\n' > "${fixture_root}/Sources/CatbirdMLSFFI.xcframework/Info.plist"
    printf 'stale manifest\n' > "$manifest"
    extract_step_script "Generate complete internal artifact manifest" "$step_script"

    (
        cd "$fixture_root"
        bash "$step_script"
        if grep -Fq 'Sources/CatbirdMLSFFI.xcframework/SHA256SUMS' "$manifest"; then
            fail "internal manifest contains an impossible self-hash"
        fi
        shasum -a 256 -c "$manifest" >/dev/null
        [[ "$(wc -l < "$manifest" | tr -d ' ')" == "2" ]] \
            || fail "internal manifest does not cover exactly the binding and framework payload"
    )
}

assert_manifest_generation

echo "PASS: build-ffi workflow is pinned and append-only"
