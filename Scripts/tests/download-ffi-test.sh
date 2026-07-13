#!/usr/bin/env bash
set -euo pipefail

SCRIPT_UNDER_TEST="$(cd "$(dirname "$0")/../.." && pwd)/Scripts/download-ffi.sh"
readonly SCRIPT_UNDER_TEST
readonly EXPECTED_DEFAULT_TAG="ffi-w1-690d567b"
readonly ASSET="CatbirdMLSFFI.xcframework.zip"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_line() {
    local expected="$1"
    local file="$2"
    grep -Fqx -- "$expected" "$file" || fail "missing line '$expected' in $file"
}

run_download() {
    local release_tag="${1-}"
    local unzip_mode="${2:-success}"
    local reject_find_quit="${3:-0}"
    local fixture_root
    fixture_root="$(mktemp -d)"
    local checkout="${fixture_root}/checkout"
    local mock_bin="${fixture_root}/bin"
    local curl_log="${fixture_root}/curl.log"
    mkdir -p "${checkout}/Scripts" "${checkout}/Sources/CatbirdMLS" "$mock_bin"
    cp "$SCRIPT_UNDER_TEST" "${checkout}/Scripts/download-ffi.sh"
    printf 'fixture binding\n' > "${checkout}/Sources/CatbirdMLS/CatbirdMLS.swift"
    install_mocks "$mock_bin"

    if [[ -n "$release_tag" ]]; then
        (
            cd "$checkout"
            MOCK_CURL_LOG="$curl_log" PATH="${mock_bin}:$PATH" \
                MOCK_UNZIP_MODE="$unzip_mode" \
                MOCK_FIND_REJECT_QUIT="$reject_find_quit" \
                FFI_RELEASE_TAG="$release_tag" ./Scripts/download-ffi.sh >/dev/null
        )
    else
        (
            cd "$checkout"
            MOCK_CURL_LOG="$curl_log" PATH="${mock_bin}:$PATH" \
                MOCK_UNZIP_MODE="$unzip_mode" \
                MOCK_FIND_REJECT_QUIT="$reject_find_quit" \
                env -u FFI_RELEASE_TAG ./Scripts/download-ffi.sh >/dev/null
        )
    fi

    printf '%s\n' "$curl_log"
}

install_mocks() {
    local mock_bin="$1"

    cat > "${mock_bin}/curl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
url="${!#}"
printf '%s\n' "$url" >> "$MOCK_CURL_LOG"
output="$(basename "$url")"
if [[ "$output" == *.sha256 ]]; then
    if [[ "${MOCK_ARCHIVE_CHECKSUM_MODE:-valid}" == "invalid" ]]; then
        printf '%064d  %s\n' 0 "${output%.sha256}" > "$output"
    else
        shasum -a 256 "${output%.sha256}" > "$output"
    fi
else
    : > "$output"
fi
MOCK

cat > "${mock_bin}/unzip" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "-Z1" ]]; then
    printf '%s\n' \
        'CatbirdMLSFFI.xcframework/' \
        'CatbirdMLSFFI.xcframework/Info.plist' \
        'CatbirdMLSFFI.xcframework/SHA256SUMS'
    case "${MOCK_UNZIP_MODE:-success}" in
        zip-traversal)
            printf '%s\n' '../escaped-from-staging'
            ;;
        symlink)
            printf '%s\n' 'CatbirdMLSFFI.xcframework/unmanifested-link'
            ;;
        unlisted-file)
            printf '%s\n' 'CatbirdMLSFFI.xcframework/unlisted.bin'
            ;;
    esac
    exit 0
fi
if [[ "${MOCK_UNZIP_MODE:-success}" == "fail" ]]; then
    exit 9
fi
destination=""
while (( $# > 0 )); do
    if [[ "$1" == "-d" ]]; then
        destination="$2"
        break
    fi
    shift
done
[[ -n "$destination" ]]
mkdir -p "${destination}/CatbirdMLSFFI.xcframework"
printf 'fixture framework\n' > "${destination}/CatbirdMLSFFI.xcframework/Info.plist"
append_binding_checksum() {
    shasum -a 256 "${PWD}/Sources/CatbirdMLS/CatbirdMLS.swift" \
        | sed "s#  ${PWD}/#  #" \
        >> "${destination}/CatbirdMLSFFI.xcframework/SHA256SUMS"
}
case "${MOCK_UNZIP_MODE:-success}" in
invalid-internal-checksum)
    printf '%064d  Sources/CatbirdMLSFFI.xcframework/Info.plist\n' 0 \
        > "${destination}/CatbirdMLSFFI.xcframework/SHA256SUMS"
    append_binding_checksum
    ;;
unsafe-parent-path)
    mkdir -p "${destination}/CatbirdMLSFFI.xcframework/subdir"
    shasum -a 256 \
        "${destination}/CatbirdMLSFFI.xcframework/Info.plist" \
        | sed "s#  ${destination}/CatbirdMLSFFI.xcframework/Info.plist#  Sources/CatbirdMLSFFI.xcframework/subdir/../Info.plist#" \
        > "${destination}/CatbirdMLSFFI.xcframework/SHA256SUMS"
    append_binding_checksum
    ;;
symlink)
    ln -s Info.plist "${destination}/CatbirdMLSFFI.xcframework/unmanifested-link"
    shasum -a 256 \
        "${destination}/CatbirdMLSFFI.xcframework/Info.plist" \
        | sed "s#  ${destination}/#  Sources/#" \
        > "${destination}/CatbirdMLSFFI.xcframework/SHA256SUMS"
    append_binding_checksum
    ;;
unlisted-file)
    printf 'not in manifest\n' > "${destination}/CatbirdMLSFFI.xcframework/unlisted.bin"
    shasum -a 256 \
        "${destination}/CatbirdMLSFFI.xcframework/Info.plist" \
        | sed "s#  ${destination}/#  Sources/#" \
        > "${destination}/CatbirdMLSFFI.xcframework/SHA256SUMS"
    append_binding_checksum
    ;;
*)
    shasum -a 256 \
        "${destination}/CatbirdMLSFFI.xcframework/Info.plist" \
        | sed "s#  ${destination}/#  Sources/#" \
        > "${destination}/CatbirdMLSFFI.xcframework/SHA256SUMS"
    append_binding_checksum
    ;;
esac
if [[ "${MOCK_UNZIP_MODE:-success}" == "no-trailing-newline" ]]; then
    manifest="${destination}/CatbirdMLSFFI.xcframework/SHA256SUMS"
    manifest_contents="$(cat "$manifest")"
    printf '%s' "$manifest_contents" > "$manifest"
fi
MOCK

cat > "${mock_bin}/find" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${MOCK_FIND_REJECT_QUIT:-0}" == "1" ]]; then
    for argument in "$@"; do
        if [[ "$argument" == "-quit" ]]; then
            echo "mock find rejects non-portable -quit" >&2
            exit 64
        fi
    done
fi
exec /usr/bin/find "$@"
MOCK

    chmod +x "${mock_bin}/curl" "${mock_bin}/unzip" "${mock_bin}/find"
}

assert_failure_preserves_installed_framework() {
    local failure_mode="$1"
    local archive_checksum_mode="${2:-valid}"
    local fixture_root
    fixture_root="$(mktemp -d)"
    local checkout="${fixture_root}/checkout"
    local mock_bin="${fixture_root}/bin"
    local curl_log="${fixture_root}/curl.log"
    local installed="${checkout}/Sources/CatbirdMLSFFI.xcframework"
    mkdir -p "${checkout}/Scripts" "${checkout}/Sources/CatbirdMLS" "$installed" "$mock_bin"
    cp "$SCRIPT_UNDER_TEST" "${checkout}/Scripts/download-ffi.sh"
    printf 'fixture binding\n' > "${checkout}/Sources/CatbirdMLS/CatbirdMLS.swift"
    printf 'known-good\n' > "${installed}/known-good.marker"
    install_mocks "$mock_bin"

    set +e
    (
        cd "$checkout"
        MOCK_CURL_LOG="$curl_log" PATH="${mock_bin}:$PATH" \
            MOCK_UNZIP_MODE="$failure_mode" \
            MOCK_ARCHIVE_CHECKSUM_MODE="$archive_checksum_mode" \
            ./Scripts/download-ffi.sh >/dev/null 2>&1
    )
    result_code=$?
    set -e

    [[ "$result_code" -ne 0 ]] \
        || fail "download unexpectedly succeeded for ${failure_mode}/${archive_checksum_mode}"
    [[ -f "${installed}/known-good.marker" ]] \
        || fail "existing framework was lost after ${failure_mode}/${archive_checksum_mode}"
}

default_log="$(run_download)"
assert_line \
    "https://github.com/joshlacal/CatbirdMLSCore/releases/download/${EXPECTED_DEFAULT_TAG}/${ASSET}" \
    "$default_log"
assert_line \
    "https://github.com/joshlacal/CatbirdMLSCore/releases/download/${EXPECTED_DEFAULT_TAG}/${ASSET}.sha256" \
    "$default_log"

override_tag="ffi-test-override"
override_log="$(run_download "$override_tag")"
assert_line \
    "https://github.com/joshlacal/CatbirdMLSCore/releases/download/${override_tag}/${ASSET}" \
    "$override_log"

run_download "" "no-trailing-newline" >/dev/null
run_download "" "success" "1" >/dev/null

forbidden_shasum_path="/usr/bin/"'shasum'
if grep -Fq -- "$forbidden_shasum_path" "$SCRIPT_UNDER_TEST" "$0"; then
    fail "download scripts must resolve shasum through PATH"
fi
assert_line \
    "https://github.com/joshlacal/CatbirdMLSCore/releases/download/${override_tag}/${ASSET}.sha256" \
    "$override_log"

assert_failure_preserves_installed_framework "fail"
assert_failure_preserves_installed_framework "invalid-internal-checksum"
assert_failure_preserves_installed_framework "success" "invalid"
assert_failure_preserves_installed_framework "unsafe-parent-path"
assert_failure_preserves_installed_framework "symlink"
assert_failure_preserves_installed_framework "unlisted-file"
assert_failure_preserves_installed_framework "zip-traversal"

echo "PASS: download-ffi immutable default and explicit override"
