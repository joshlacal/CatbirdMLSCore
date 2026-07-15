#!/usr/bin/env bash
set -euo pipefail

# Downloads the prebuilt Rust FFI binary target (Sources/CatbirdMLSFFI.xcframework)
# from GitHub Releases. The xcframework is gitignored (≈4 GB unzipped); CI and
# fresh clones fetch it with this script. To rebuild from Rust sources instead,
# use Scripts/rebuild-ffi.sh.
#
# Env:
#   FFI_RELEASE_TAG  immutable release tag to download
#                    (default: ffi-w2-bd2fe34d)

ASSET="CatbirdMLSFFI.xcframework.zip"
REPO="joshlacal/CatbirdMLSCore"
FFI_RELEASE_TAG="${FFI_RELEASE_TAG:-ffi-w2-bd2fe34d}"

BASE_URL="https://github.com/${REPO}/releases/download/${FFI_RELEASE_TAG}"

echo "📦 Downloading ${ASSET} from ${BASE_URL}"

cd "$(dirname "$0")/.."

TARGET="Sources/CatbirdMLSFFI.xcframework"
TEMP_ROOT=""
BACKUP_PATH=""

cleanup() {
    local exit_code=$?
    trap - EXIT

    if [[ -n "$BACKUP_PATH" && -e "$BACKUP_PATH" && ! -e "$TARGET" ]]; then
        mv "$BACKUP_PATH" "$TARGET" || true
    fi
    [[ -z "$TEMP_ROOT" ]] || rm -rf "$TEMP_ROOT"
    rm -f "$ASSET" "${ASSET}.sha256"
    exit "$exit_code"
}
trap cleanup EXIT

curl -fL --retry 3 -O "${BASE_URL}/${ASSET}"
curl -fL --retry 3 -O "${BASE_URL}/${ASSET}.sha256"

echo "🔐 Verifying checksum..."
shasum -a 256 -c "${ASSET}.sha256"

echo "📂 Extracting and validating ${TARGET}..."
TEMP_ROOT="$(mktemp -d "${PWD}/.ffi-download.XXXXXX")"
ARCHIVE_ENTRIES="${TEMP_ROOT}/archive-entries.txt"
unzip -Z1 "$ASSET" > "$ARCHIVE_ENTRIES"
[[ -s "$ARCHIVE_ENTRIES" ]] || { echo "❌ archive contains no entries"; exit 1; }
while IFS= read -r archive_entry || [[ -n "$archive_entry" ]]; do
    [[ -n "$archive_entry" ]] || { echo "❌ archive contains an empty entry"; exit 1; }
    [[ "$archive_entry" != *\\* ]] || { echo "❌ archive contains a backslash path"; exit 1; }
    if [[ "$archive_entry" == "CatbirdMLSFFI.xcframework/" ]]; then
        continue
    fi
    [[ "$archive_entry" == CatbirdMLSFFI.xcframework/* ]] \
        || { echo "❌ archive entry escapes the expected framework root"; exit 1; }
    case "/${archive_entry%/}/" in
        */../*|*/./*|*//*)
            echo "❌ archive contains an unsafe path component" >&2
            exit 1
            ;;
    esac
done < "$ARCHIVE_ENTRIES"
duplicate_entry="$(awk 'seen[$0]++ { print; exit }' "$ARCHIVE_ENTRIES")"
[[ -z "$duplicate_entry" ]] || { echo "❌ archive contains duplicate entries"; exit 1; }

mkdir -p "${TEMP_ROOT}/Sources"
# Zip was created with `ditto -c -k --keepParent`, so the archive root is
# CatbirdMLSFFI.xcframework/ itself.
unzip -q "${ASSET}" -d "${TEMP_ROOT}/Sources"

STAGED_FRAMEWORK="${TEMP_ROOT}/Sources/CatbirdMLSFFI.xcframework"
MANIFEST="${STAGED_FRAMEWORK}/SHA256SUMS"
MANIFEST_FILES="${TEMP_ROOT}/manifest-files.txt"
ACTUAL_FILES="${TEMP_ROOT}/actual-files.txt"
[[ -d "$STAGED_FRAMEWORK" ]] || { echo "❌ archive has no CatbirdMLSFFI.xcframework root"; exit 1; }
[[ -s "$MANIFEST" ]] || { echo "❌ archive has no non-empty internal SHA256SUMS"; exit 1; }

unexpected_root="$(find "${TEMP_ROOT}/Sources" -mindepth 1 -maxdepth 1 \
    ! -name CatbirdMLSFFI.xcframework -print | sed -n '1p')"
[[ -z "$unexpected_root" ]] || { echo "❌ archive contains an unexpected root"; exit 1; }
unexpected_type="$(find "$STAGED_FRAMEWORK" ! -type d ! -type f -print | sed -n '1p')"
[[ -z "$unexpected_type" ]] || { echo "❌ archive contains a symlink or unsupported file type"; exit 1; }

binding_is_manifested=false
: > "$MANIFEST_FILES"
while read -r checksum manifest_path || [[ -n "${checksum:-}" ]]; do
    manifest_path="${manifest_path#\*}"
    [[ "$checksum" =~ ^[0-9a-f]{64}$ ]] \
        || { echo "❌ malformed checksum in internal SHA256SUMS"; exit 1; }
    case "/${manifest_path}/" in
        */../*|*/./*|*//*)
            echo "❌ unsafe path in internal SHA256SUMS" >&2
            exit 1
            ;;
    esac
    case "$manifest_path" in
        Sources/CatbirdMLSFFI.xcframework/*)
            printf '%s\n' "$manifest_path" >> "$MANIFEST_FILES"
            ;;
        Sources/CatbirdMLS/CatbirdMLS.swift)
            binding_is_manifested=true
            ;;
        *)
            echo "❌ unsafe path in internal SHA256SUMS" >&2
            exit 1
            ;;
    esac
done < "$MANIFEST"
[[ "$binding_is_manifested" == true ]] \
    || { echo "❌ internal SHA256SUMS does not bind the generated Swift source"; exit 1; }

while IFS= read -r extracted_file; do
    printf 'Sources/%s\n' "${extracted_file#"${TEMP_ROOT}/Sources/"}"
done < <(find "$STAGED_FRAMEWORK" -type f ! -path "$MANIFEST" | LC_ALL=C sort) \
    > "$ACTUAL_FILES"
LC_ALL=C sort "$MANIFEST_FILES" -o "$MANIFEST_FILES"
cmp -s "$MANIFEST_FILES" "$ACTUAL_FILES" \
    || { echo "❌ internal SHA256SUMS does not cover every extracted framework file"; exit 1; }

# The artifact manifest also binds the generated Swift source committed in this
# checkout. A symlink lets shasum validate that entry without copying it.
ln -s "${PWD}/Sources/CatbirdMLS" "${TEMP_ROOT}/Sources/CatbirdMLS"
(
    cd "$TEMP_ROOT"
    shasum -a 256 -c "Sources/CatbirdMLSFFI.xcframework/SHA256SUMS"
)

BACKUP_PATH="${TEMP_ROOT}/previous-CatbirdMLSFFI.xcframework"
if [[ -e "$TARGET" ]]; then
    mv "$TARGET" "$BACKUP_PATH"
fi
mv "$STAGED_FRAMEWORK" "$TARGET"
rm -rf "$BACKUP_PATH"
BACKUP_PATH=""

echo "✅ ${TARGET} ready"
