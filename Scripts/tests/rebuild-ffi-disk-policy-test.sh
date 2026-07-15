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
printf 'xcframework metadata\n' > CatbirdMLSFFI.xcframework/Info.plist
for slice in \
    ios-arm64 \
    ios-arm64_x86_64-simulator \
    ios-arm64_x86_64-maccatalyst \
    macos-arm64_x86_64; do
    mkdir -p "CatbirdMLSFFI.xcframework/${slice}/Headers"
    printf 'generated header\n' \
        > "CatbirdMLSFFI.xcframework/${slice}/Headers/CatbirdMLSFFI.h"
    printf 'generated module map\n' \
        > "CatbirdMLSFFI.xcframework/${slice}/Headers/module.modulemap"
done
if [[ "${MOCK_OMIT_COMPONENT:-0}" == 1 ]]; then
    rm -f CatbirdMLSFFI.xcframework/ios-arm64/Headers/module.modulemap
fi
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
[[ ! -e "${rust_root}/CatbirdMLSFFI.xcframework" ]] \
    || fail "completed XCFramework was duplicated instead of transferred"
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

grep -Fq 'source_stat.st_dev != parent_stat.st_dev' "$SCRIPT_UNDER_TEST" \
    || fail "same-filesystem transfer is not enforced"
grep -Fq 'os.replace(source, destination)' "$SCRIPT_UNDER_TEST" \
    || fail "XCFramework transfer is not a no-copy rename"
if grep -Eq 'cp[[:space:]]+-R[[:space:]]+CatbirdMLSFFI\.xcframework' "$SCRIPT_UNDER_TEST"; then
    fail "XCFramework transfer retains a copy fallback"
fi

printf 'preserve known-good destination\n' \
    > "${core_root}/Sources/CatbirdMLSFFI.xcframework/known-good.marker"
if (
    cd "$core_root"
    PATH="${mock_bin}:$PATH" \
        MOCK_RUST_ROOT="$rust_root" \
        MOCK_OMIT_COMPONENT=1 \
        ./Scripts/rebuild-ffi.sh >/dev/null 2>&1
); then
    fail "incomplete XCFramework was accepted"
fi
[[ -s "${core_root}/Sources/CatbirdMLSFFI.xcframework/known-good.marker" ]] \
    || fail "incomplete output replaced the known-good destination"

real_core_root="${fixture_root}/CatbirdMLSCore-real"
mv "$core_root" "$real_core_root"
ln -s "$real_core_root" "$core_root"
if (
    cd "$real_core_root"
    PATH="${mock_bin}:$PATH" \
        MOCK_RUST_ROOT="$rust_root" \
        ./Scripts/rebuild-ffi.sh >/dev/null 2>&1
); then
    fail "symlinked Core checkout was accepted"
fi
[[ -s "${real_core_root}/Sources/CatbirdMLSFFI.xcframework/known-good.marker" ]] \
    || fail "symlinked destination path modified the known-good artifact"

echo "PASS: rebuild-ffi transfers verified output without duplicating the XCFramework"
