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

assert_before() {
    local first="$1"
    local second="$2"
    local first_line
    local second_line
    first_line="$(grep -nF -- "$first" "$WORKFLOW" | head -n 1 | cut -d: -f1)"
    second_line="$(grep -nF -- "$second" "$WORKFLOW" | head -n 1 | cut -d: -f1)"
    [[ -n "$first_line" ]] || fail "workflow is missing: $first"
    [[ -n "$second_line" ]] || fail "workflow is missing: $second"
    (( first_line < second_line )) \
        || fail "workflow must place '$first' before '$second'"
}

assert_contains "default: ffi-w2-bd2fe34d"
assert_contains "default: bd2fe34d9a87f7466b673043f76a03aeadc8e111"
assert_contains "default: 8b2f92e28097c0788492f2f82328f4ab5b032953"
assert_contains "default: 6cf0bbdc41ed57c8b8e9b55430a18ae143283f18"
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
assert_contains "name: Restore Cargo registry and build outputs"
assert_contains "id: restore-cargo-cache"
assert_contains "actions/cache/restore@0057852bfaa89a56745cba8c7296529d2fc39830 # v4.3.0"
assert_contains "name: Save Cargo registry and build outputs before cleanup"
assert_contains "if: steps.restore-cargo-cache.outputs.cache-hit != 'true'"
assert_contains "actions/cache/save@0057852bfaa89a56745cba8c7296529d2fc39830 # v4.3.0"
assert_contains "name: Release generated Rust build outputs before Swift validation"
assert_contains "cargo clean --manifest-path ../catbird-mls/Cargo.toml"
assert_contains "rm -rf ../catbird-mls/build"
assert_contains "run: swift build --jobs 2"
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
assert_absent "uses: actions/cache@"
assert_before \
    "name: Save Cargo registry and build outputs before cleanup" \
    "name: Release generated Rust build outputs before Swift validation"
assert_before \
    "name: Release generated Rust build outputs before Swift validation" \
    "name: Verify Swift package consumes generated FFI"

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
        RELEASE_TAG="ffi-w2-bd2fe34d" \
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
assert_preflight_result failure $'ffi-w2-bd2fe34d\n'

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

assert_rust_cleanup_preserves_copied_ffi() {
    local fixture_root
    fixture_root="$(mktemp -d)"
    local mock_bin="${fixture_root}/bin"
    local step_script="${fixture_root}/cleanup.sh"
    local core_root="${fixture_root}/CatbirdMLSCore"
    local rust_root="${fixture_root}/catbird-mls"
    mkdir -p \
        "$mock_bin" \
        "${core_root}/Sources/CatbirdMLS" \
        "${core_root}/Sources/CatbirdMLSFFI.xcframework" \
        "${rust_root}/target" \
        "${rust_root}/build"
    printf 'fixture binding\n' > "${core_root}/Sources/CatbirdMLS/CatbirdMLS.swift"
    printf 'fixture framework\n' > "${core_root}/Sources/CatbirdMLSFFI.xcframework/Info.plist"
    printf '[workspace]\n' > "${rust_root}/Cargo.toml"
    extract_step_script "Release generated Rust build outputs before Swift validation" "$step_script"

    cat > "${mock_bin}/cargo" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
[[ "$#" -eq 3 ]]
[[ "$1" == "clean" ]]
[[ "$2" == "--manifest-path" ]]
[[ "$3" == "../catbird-mls/Cargo.toml" ]]
rm -rf ../catbird-mls/target
MOCK
    chmod +x "${mock_bin}/cargo"

    (
        cd "$core_root"
        PATH="${mock_bin}:$PATH" bash "$step_script"
    )

    [[ ! -e "${rust_root}/target" ]] || fail "cleanup retained Rust target outputs"
    [[ ! -e "${rust_root}/build" ]] || fail "cleanup retained generated Rust build outputs"
    [[ -d "${core_root}/Sources/CatbirdMLSFFI.xcframework" ]] \
        || fail "cleanup removed the copied XCFramework"
    [[ -s "${core_root}/Sources/CatbirdMLS/CatbirdMLS.swift" ]] \
        || fail "cleanup removed the copied Swift binding"
    rm -rf "$fixture_root"
}

assert_rust_cleanup_preserves_copied_ffi

echo "PASS: build-ffi workflow is pinned and append-only"
