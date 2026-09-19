#!/usr/bin/env python3
"""Create and verify the exact local Apple UniFFI artifact identity."""

import argparse
import hashlib
import json
from pathlib import Path
import plistlib
import subprocess


EXCLUDED_NATIVE_PARTS = {
    ".git",
    ".jj",
    "_trash",
    "build",
    "target",
    "CatbirdMLSFFI.xcframework",
}
NATIVE_SOURCE_SUFFIXES = {".rs", ".toml", ".lock"}


def sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def canonical_sha256(value):
    return sha256_bytes(canonical_bytes(value))


def regular_files(root):
    root = Path(root)
    return sorted(path for path in root.rglob("*") if path.is_file() and not path.is_symlink())


def file_map(root, paths=None):
    root = Path(root).resolve()
    paths = regular_files(root) if paths is None else sorted(Path(path) for path in paths)
    return {
        str(path.resolve().relative_to(root)): {
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
        for path in paths
    }


def native_source_files(native):
    native = Path(native).resolve()
    result = []
    for path in regular_files(native):
        relative = path.relative_to(native)
        if any(part in EXCLUDED_NATIVE_PARTS for part in relative.parts):
            continue
        if path.suffix in NATIVE_SOURCE_SUFFIXES or path.name == "build.rs":
            result.append(path)
    return result


def native_source_record(native):
    files = file_map(native, native_source_files(native))
    return {"sha256": canonical_sha256(files), "files": files}


def native_source_fingerprint(native):
    return native_source_record(native)["sha256"]


def source_bundle_record(native, dependencies=()):
    native = Path(native).resolve()
    dependency_records = {
        Path(dependency).resolve().name: native_source_record(dependency)
        for dependency in sorted((Path(path).resolve() for path in dependencies), key=str)
    }
    record = {
        "native": native_source_record(native),
        "local_path_dependencies": dependency_records,
    }
    record["sha256"] = canonical_sha256(record)
    return record


def source_bundle_fingerprint(native, dependencies=()):
    return source_bundle_record(native, dependencies)["sha256"]


def run_output(*arguments, cwd=None):
    result = subprocess.run(arguments, cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({' '.join(arguments)}):\n{result.stdout}{result.stderr}"
        )
    return (result.stdout + result.stderr).strip()


def jj_identity(path):
    path = Path(path).resolve()
    output = run_output(
        "jj",
        "log",
        "--no-graph",
        "-r",
        "@",
        "-T",
        'commit_id ++ "\\n" ++ change_id ++ "\\n"',
        cwd=path,
    ).splitlines()
    if len(output) != 2:
        raise RuntimeError(f"Could not read exact JJ identity for {path}")
    parent = run_output(
        "jj",
        "log",
        "--no-graph",
        "-r",
        "@-",
        "-T",
        "commit_id",
        cwd=path,
    )
    return {
        "jj_commit": output[0],
        "jj_change": output[1],
        "parent_jj_commit": parent,
    }


def repository_record(path, identity):
    return {"path": str(Path(path).resolve()), **identity}


def schema_record(petrel_catbird):
    root = Path(petrel_catbird).resolve() / "lexicons/blue/catbird/chat"
    if not root.is_dir():
        raise RuntimeError(f"Chat schema directory is absent: {root}")
    files = file_map(root, root.glob("*.json"))
    return {"root": str(root), "sha256": canonical_sha256(files), "files": files}


def framework_record(framework):
    framework = Path(framework).resolve()
    plist_path = framework / "Info.plist"
    if not plist_path.is_file():
        raise RuntimeError(f"XCFramework Info.plist is absent: {plist_path}")
    with plist_path.open("rb") as handle:
        plist = plistlib.load(handle)
    libraries = []
    for item in plist.get("AvailableLibraries", []):
        libraries.append(
            {
                "identifier": item["LibraryIdentifier"],
                "library_path": item["LibraryPath"],
                "headers_path": item.get("HeadersPath"),
                "architectures": sorted(item["SupportedArchitectures"]),
                "platform": item["SupportedPlatform"],
                "variant": item.get("SupportedPlatformVariant"),
            }
        )
    libraries.sort(key=lambda item: item["identifier"])
    files = file_map(framework)
    return {
        "path": str(framework),
        "sha256": canonical_sha256(files),
        "files": files,
        "libraries": libraries,
    }


def interface_record(core, framework):
    core = Path(core).resolve()
    framework = Path(framework).resolve()
    swift = core / "Sources/CatbirdMLS/CatbirdMLS.swift"
    if not swift.is_file():
        raise RuntimeError(f"Generated Swift binding is absent: {swift}")
    headers = {
        str(path.relative_to(framework)): sha256_file(path)
        for path in regular_files(framework)
        if path.suffix == ".h" or path.name == "module.modulemap"
    }
    record = {
        "generated_swift": {
            "path": str(swift),
            "sha256": sha256_file(swift),
            "size_bytes": swift.stat().st_size,
        },
        "native_headers": headers,
    }
    record["sha256"] = canonical_sha256(record)
    return record


def toolchain_record():
    commands = {
        "cargo": ("cargo", "--version"),
        "rustc": ("rustc", "--version"),
        "swift": ("swift", "--version"),
        "xcode": ("xcodebuild", "-version"),
    }
    return {name: run_output(*command) for name, command in commands.items()}


def build_manifest(
    *,
    core,
    native,
    atproto,
    petrel,
    petrel_catbird,
    selected_inputs,
    qualification,
    identities=None,
    toolchains=None,
):
    core = Path(core).resolve()
    native = Path(native).resolve()
    atproto = Path(atproto).resolve()
    petrel = Path(petrel).resolve()
    petrel_catbird = Path(petrel_catbird).resolve()
    selected_inputs = Path(selected_inputs).resolve()
    framework = core / "Sources/CatbirdMLSFFI.xcframework"
    if not selected_inputs.is_file():
        raise RuntimeError(f"Selected input manifest is absent: {selected_inputs}")
    repositories = {
        "catbird-mls": native,
        "catbird-atproto": atproto,
        "Petrel": petrel,
        "PetrelCatbird": petrel_catbird,
    }
    identities = identities or {name: jj_identity(path) for name, path in repositories.items()}
    source = native_source_record(native)
    atproto_source = native_source_record(atproto)
    manifest = {
        "schema_version": 1,
        "scope": "apple-native-uniffi",
        "qualification": qualification,
        "selected_inputs": {
            "path": str(selected_inputs),
            "sha256": sha256_file(selected_inputs),
        },
        "source": {
            "repositories": {
                name: repository_record(path, identities[name])
                for name, path in repositories.items()
            },
            "native_source_sha256": source["sha256"],
            "native_source_files": source["files"],
            "local_path_dependencies": {
                "catbird-atproto": atproto_source,
            },
            "cargo_lock_sha256": sha256_file(native / "Cargo.lock"),
            "schemas": schema_record(petrel_catbird),
        },
        "build": {
            "cargo_profile": "release",
            "default_features": True,
            "explicit_features": [],
            "apple_targets": [
                "aarch64-apple-darwin",
                "aarch64-apple-ios",
                "aarch64-apple-ios-macabi",
                "aarch64-apple-ios-sim",
                "x86_64-apple-darwin",
                "x86_64-apple-ios",
                "x86_64-apple-ios-macabi",
            ],
            "toolchains": toolchains or toolchain_record(),
        },
        "package": {
            "path": str(core),
            "manifest_sha256": sha256_file(core / "Package.swift"),
            "local_dependencies": {
                "Petrel": str(petrel),
                "PetrelCatbird": str(petrel_catbird),
                "CatbirdMLSFFI": str(framework),
            },
        },
        "interface": interface_record(core, framework),
        "framework": framework_record(framework),
    }
    manifest["identity"] = {
        "algorithm": "sha256",
        "canonical_sha256": canonical_sha256(manifest),
    }
    return manifest


def write_manifest(path, manifest):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    checksum = path.with_suffix(path.suffix + ".sha256")
    checksum.write_text(f"{sha256_file(path)}  {path.name}\n")


def require_equal(actual, expected, label):
    if actual != expected:
        raise RuntimeError(f"Apple artifact manifest mismatch: {label}")


def verify_manifest(path):
    path = Path(path).resolve()
    manifest = json.loads(path.read_text())
    identity = manifest.get("identity", {})
    payload = dict(manifest)
    payload.pop("identity", None)
    require_equal(
        identity.get("canonical_sha256"),
        canonical_sha256(payload),
        "canonical manifest identity",
    )
    require_equal(identity.get("algorithm"), "sha256", "identity algorithm")

    selected = Path(manifest["selected_inputs"]["path"])
    require_equal(sha256_file(selected), manifest["selected_inputs"]["sha256"], "selected inputs")
    native = Path(manifest["source"]["repositories"]["catbird-mls"]["path"])
    require_equal(
        native_source_fingerprint(native),
        manifest["source"]["native_source_sha256"],
        "native source fingerprint",
    )
    atproto_record = manifest["source"]["local_path_dependencies"]["catbird-atproto"]
    atproto = Path(manifest["source"]["repositories"]["catbird-atproto"]["path"])
    require_equal(
        native_source_fingerprint(atproto),
        atproto_record["sha256"],
        "catbird-atproto source fingerprint",
    )
    require_equal(
        native_source_record(atproto)["files"],
        atproto_record["files"],
        "catbird-atproto source file map",
    )
    require_equal(
        sha256_file(native / "Cargo.lock"),
        manifest["source"]["cargo_lock_sha256"],
        "Cargo.lock",
    )
    schema = manifest["source"]["schemas"]
    current_schema = file_map(Path(schema["root"]), Path(schema["root"]).glob("*.json"))
    require_equal(current_schema, schema["files"], "schema file map")

    package = manifest["package"]
    core = Path(package["path"])
    require_equal(sha256_file(core / "Package.swift"), package["manifest_sha256"], "Package.swift")
    for name, dependency in package["local_dependencies"].items():
        if not Path(dependency).exists():
            raise RuntimeError(f"Apple artifact manifest mismatch: missing local dependency {name}")

    framework = manifest["framework"]
    current_framework = framework_record(framework["path"])
    require_equal(current_framework["files"], framework["files"], "framework file map")
    require_equal(current_framework["libraries"], framework["libraries"], "framework architecture matrix")
    current_interface = interface_record(core, framework["path"])
    require_equal(current_interface, manifest["interface"], "generated interface")

    for name, record in manifest["source"]["repositories"].items():
        repository = Path(record["path"])
        if (repository / ".jj").exists():
            current = jj_identity(repository)
            expected_identity = {
                key: record[key]
                for key in ("jj_commit", "jj_change", "parent_jj_commit")
                if key in record
            }
            require_equal(current, expected_identity, f"{name} JJ identity")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    fingerprint = subparsers.add_parser("source-fingerprint")
    fingerprint.add_argument("--native", required=True, type=Path)
    fingerprint.add_argument("--dependency", action="append", default=[], type=Path)
    generate = subparsers.add_parser("generate")
    generate.add_argument("--core", required=True, type=Path)
    generate.add_argument("--native", required=True, type=Path)
    generate.add_argument("--atproto", required=True, type=Path)
    generate.add_argument("--petrel", required=True, type=Path)
    generate.add_argument("--petrel-catbird", required=True, type=Path)
    generate.add_argument("--selected-inputs", required=True, type=Path)
    generate.add_argument("--output", required=True, type=Path)
    generate.add_argument(
        "--qualification",
        choices=("unreviewed-candidate", "reviewed-source"),
        default="unreviewed-candidate",
    )
    verify = subparsers.add_parser("verify")
    verify.add_argument("manifest", type=Path)
    args = parser.parse_args()

    if args.command == "source-fingerprint":
        print(source_bundle_fingerprint(args.native, args.dependency))
    elif args.command == "generate":
        manifest = build_manifest(
            core=args.core,
            native=args.native,
            atproto=args.atproto,
            petrel=args.petrel,
            petrel_catbird=args.petrel_catbird,
            selected_inputs=args.selected_inputs,
            qualification=args.qualification,
        )
        write_manifest(args.output, manifest)
        print(manifest["identity"]["canonical_sha256"])
    else:
        manifest = verify_manifest(args.manifest)
        print(manifest["identity"]["canonical_sha256"])


if __name__ == "__main__":
    main()
