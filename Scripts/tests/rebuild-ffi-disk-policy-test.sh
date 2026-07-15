#!/usr/bin/env bash
set -euo pipefail

SCRIPT_UNDER_TEST="$(cd "$(dirname "$0")/../.." && pwd)/Scripts/rebuild-ffi.sh"
readonly SCRIPT_UNDER_TEST

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

fixture_root="$(mktemp -d)"
trap 'rm -rf "$fixture_root"' EXIT
core_root="${fixture_root}/CatbirdMLSCore"
rust_root="${fixture_root}/catbird-mls"
mock_bin="${fixture_root}/bin"
mkdir -p "${core_root}/Scripts" "${core_root}/Sources/CatbirdMLS" "$rust_root" "$mock_bin"
cp "$SCRIPT_UNDER_TEST" "${core_root}/Scripts/rebuild-ffi.sh"

cat > "${mock_bin}/xcrun" <<'MOCK'
#!/usr/bin/env bash
exit 0
MOCK

cat > "${mock_bin}/cp" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"CatbirdMLSFFI.xcframework"* ]]; then
    [[ ! -e "${MOCK_RUST_ROOT}/build/libs" ]] || {
        echo "staging libraries still exist when package copy begins" >&2
        exit 91
    }
fi
exec /bin/cp "$@"
MOCK

cat > "${rust_root}/create-xcframework.sh" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
mkdir -p \
    CatbirdMLSFFI.xcframework/ios-arm64 \
    CatbirdMLSFFI.xcframework/ios-arm64_x86_64-simulator \
    CatbirdMLSFFI.xcframework/ios-arm64_x86_64-maccatalyst \
    CatbirdMLSFFI.xcframework/macos-arm64_x86_64 \
    build/bindings \
    build/libs/ios-arm64 \
    build/frameworks \
    target/cache
for library in \
    CatbirdMLSFFI.xcframework/ios-arm64/libCatbirdMLSFFI.a \
    CatbirdMLSFFI.xcframework/ios-arm64_x86_64-simulator/libCatbirdMLSFFI.a \
    CatbirdMLSFFI.xcframework/ios-arm64_x86_64-maccatalyst/libCatbirdMLSFFI.a \
    CatbirdMLSFFI.xcframework/macos-arm64_x86_64/libCatbirdMLSFFI.a; do
    printf 'verified native slice\n' > "$library"
done
printf 'generated Swift binding\n' > build/bindings/CatbirdMLS.swift
printf 'redundant staging library\n' > build/libs/ios-arm64/libCatbirdMLSFFI.a
printf 'redundant framework staging\n' > build/frameworks/staging.marker
printf 'preserve Cargo cache\n' > target/cache/cache.marker
printf 'preserve Rust source\n' > source.marker
MOCK

chmod +x \
    "${mock_bin}/xcrun" \
    "${mock_bin}/cp" \
    "${rust_root}/create-xcframework.sh" \
    "${core_root}/Scripts/rebuild-ffi.sh"

(
    cd "$core_root"
    PATH="${mock_bin}:$PATH" \
        MOCK_RUST_ROOT="$rust_root" \
        ./Scripts/rebuild-ffi.sh >/dev/null
)

for library in \
    ios-arm64/libCatbirdMLSFFI.a \
    ios-arm64_x86_64-simulator/libCatbirdMLSFFI.a \
    ios-arm64_x86_64-maccatalyst/libCatbirdMLSFFI.a \
    macos-arm64_x86_64/libCatbirdMLSFFI.a; do
    [[ -s "${core_root}/Sources/CatbirdMLSFFI.xcframework/${library}" ]] \
        || fail "installed XCFramework is missing ${library}"
done
[[ -s "${core_root}/Sources/CatbirdMLS/CatbirdMLS.swift" ]] \
    || fail "generated Swift binding was not installed"
[[ ! -e "${rust_root}/build/libs" ]] \
    || fail "redundant staging libraries were retained"
[[ ! -e "${rust_root}/build/frameworks" ]] \
    || fail "redundant framework staging was retained"
[[ -s "${rust_root}/target/cache/cache.marker" ]] \
    || fail "Cargo cache output was removed"
[[ -s "${rust_root}/source.marker" ]] \
    || fail "Rust source was removed"
[[ -s "${rust_root}/build/bindings/CatbirdMLS.swift" ]] \
    || fail "generated binding provenance was removed"

echo "PASS: rebuild-ffi releases only verified redundant staging before package copy"
