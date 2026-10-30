#!/usr/bin/env python3
"""Create and verify the exact local Apple UniFFI artifact identity."""

import argparse
import hashlib
import json
import os
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
    "CatbirdMLSFFISimulator.xcframework",
}
# Rust embeds JSON contracts with include_str!, and shell/Python scripts drive
# the native build and binding generation. A source receipt must detect changes
# to those inputs even when the Rust declarations and UniFFI ABI stay identical.
NATIVE_SOURCE_SUFFIXES = {".rs", ".toml", ".lock", ".json", ".sh", ".py"}

APPLE_TARGETS = {
    "apple-all-targets": [
        "aarch64-apple-darwin", "aarch64-apple-ios", "aarch64-apple-ios-macabi",
        "aarch64-apple-ios-sim", "x86_64-apple-darwin", "x86_64-apple-ios",
        "x86_64-apple-ios-macabi",
    ],
    "ios-arm64-simulator-only": ["aarch64-apple-ios-sim"],
}


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


def stdout_output(*arguments, cwd=None):
    result = subprocess.run(arguments, cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({' '.join(arguments)}):\n{result.stdout}{result.stderr}"
        )
    return result.stdout.strip()


def source_commit_identity(path):
    """Return the published commit whose tree is exactly this checkout.

    A JJ working-copy commit is local to one workspace and is never pushed, so
    it cannot identify a source to anyone else. The recorded identity is
    instead the single parent of an empty working copy, or a clean Git HEAD,
    which is the same commit in every checkout of that revision.
    """
    path = Path(path).resolve()
    if (path / ".jj").exists():
        lines = stdout_output(
            "jj",
            "log",
            "--no-graph",
            "-r",
            "@",
            "-T",
            'if(empty, "empty", "changed") ++ "\\n" ++ parents.map(|c| c.commit_id()).join(" ") ++ "\\n"',
            cwd=path,
        ).splitlines()
        if len(lines) != 2:
            raise RuntimeError(f"Could not read the JJ working copy of {path}")
        if lines[0] != "empty":
            raise RuntimeError(f"{path} has uncommitted changes; record only a committed source")
        parents = lines[1].split()
        if len(parents) != 1:
            raise RuntimeError(f"{path} working copy must have exactly one parent commit")
        return {"commit": parents[0]}
    if (path / ".git").exists():
        top = stdout_output("git", "rev-parse", "--show-toplevel", cwd=path)
        if Path(top).resolve() != path:
            raise RuntimeError(f"{path} is not the root of its Git checkout")
        if stdout_output("git", "status", "--porcelain", "--untracked-files=no", cwd=path):
            raise RuntimeError(f"{path} has modified tracked files; record only a committed source")
        return {"commit": stdout_output("git", "rev-parse", "HEAD", cwd=path)}
    raise RuntimeError(f"{path} is neither a JJ nor a Git checkout; its commit cannot be established")


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


def recorded_path(path, base=None):
    """Render a path relative to the manifest directory when one is given.

    Relative paths let the same manifest verify in any checkout that keeps
    the sibling layout (CatbirdMLSCore next to catbird-mls, catbird-atproto,
    Petrel and PetrelCatbird).
    """
    path = Path(path).resolve()
    if base is None:
        return str(path)
    return Path(os.path.relpath(path, Path(base).resolve())).as_posix()


def resolve_recorded(value, base):
    path = Path(value)
    if path.is_absolute():
        return path
    return Path(os.path.normpath(Path(base).resolve() / path))


def repository_record(path, identity, base=None):
    return {"path": recorded_path(path, base), **identity}


def schema_record(petrel_catbird, base=None):
    root = Path(petrel_catbird).resolve() / "lexicons"
    if not root.is_dir():
        raise RuntimeError(f"Overlay schema directory is absent: {root}")
    files = file_map(root, root.rglob("*.json"))
    return {"root": recorded_path(root, base), "sha256": canonical_sha256(files), "files": files}


def framework_record(framework, base=None):
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
        "path": recorded_path(framework, base),
        "sha256": canonical_sha256(files),
        "files": files,
        "libraries": libraries,
    }


def validate_framework_scope(framework, scope):
    actual = {
        (item["platform"], item["variant"], tuple(item["architectures"]))
        for item in framework["libraries"]
    }
    expected = {
        "apple-all-targets": {
            ("ios", None, ("arm64",)),
            ("ios", "simulator", ("arm64", "x86_64")),
            ("ios", "maccatalyst", ("arm64", "x86_64")),
            ("macos", None, ("arm64", "x86_64")),
        },
        "ios-arm64-simulator-only": {("ios", "simulator", ("arm64",))},
    }.get(scope)
    if expected is None or actual != expected or len(framework["libraries"]) != len(expected):
        raise RuntimeError("Native framework architecture matrix does not match artifact scope")


def interface_record(core, framework, base=None):
    core = Path(core).resolve()
    framework = Path(framework).resolve()
    swift = core / "Sources/CatbirdMLSSpaces/CatbirdMLS.swift"
    if not swift.is_file():
        raise RuntimeError(f"Generated Swift binding is absent: {swift}")
    headers = {
        str(path.relative_to(framework)): sha256_file(path)
        for path in regular_files(framework)
        if path.suffix == ".h" or path.name == "module.modulemap"
    }
    record = {
        "generated_swift": {
            "path": recorded_path(swift, base),
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
    framework=None,
    identities=None,
    toolchains=None,
    manifest_dir=None,
):
    core = Path(core).resolve()
    native = Path(native).resolve()
    atproto = Path(atproto).resolve()
    petrel = Path(petrel).resolve()
    petrel_catbird = Path(petrel_catbird).resolve()
    selected_inputs = Path(selected_inputs).resolve()
    framework = Path(framework).resolve() if framework else core / "Sources/CatbirdMLSFFI.xcframework"
    marker = json.loads((framework / "labyrinth-space-protocol.json").read_text())
    if marker.get("protocolVersion") != 20261023 or "labyrinth" not in marker.get("features", []):
        raise RuntimeError("Native artifact lacks the required Space protocol/feature marker")
    if marker.get("sourceFingerprint") != source_bundle_fingerprint(native, [atproto]):
        raise RuntimeError("Native artifact rebuild marker does not match current source inputs")
    base = Path(manifest_dir).resolve() if manifest_dir is not None else None
    interface = interface_record(core, framework, base)
    if marker.get("generatedSwiftSHA256") != interface["generated_swift"]["sha256"]:
        raise RuntimeError("Native artifact rebuild marker does not match the generated Swift binding")
    artifact_scope = marker.get("artifactScope")
    framework_identity = framework_record(framework, base)
    validate_framework_scope(framework_identity, artifact_scope)
    expected_name = "CatbirdMLSFFISimulator.xcframework" if artifact_scope == "ios-arm64-simulator-only" else "CatbirdMLSFFI.xcframework"
    if framework != core / "Sources" / expected_name:
        raise RuntimeError("Native framework path does not match artifact scope")
    if not selected_inputs.is_file():
        raise RuntimeError(f"Selected input manifest is absent: {selected_inputs}")
    repositories = {
        "catbird-mls": native,
        "catbird-atproto": atproto,
        "Petrel": petrel,
        "PetrelCatbird": petrel_catbird,
    }
    identities = identities or {name: source_commit_identity(path) for name, path in repositories.items()}
    source = native_source_record(native)
    atproto_source = native_source_record(atproto)
    manifest = {
        "schema_version": 1 if base is None else 2,
        "scope": "apple-native-uniffi-simulator-only" if artifact_scope == "ios-arm64-simulator-only" else "apple-native-uniffi",
        "qualification": qualification,
        "selected_inputs": {
            "path": recorded_path(selected_inputs, base),
            "sha256": sha256_file(selected_inputs),
        },
        "source": {
            "repositories": {
                name: repository_record(path, identities[name], base)
                for name, path in repositories.items()
            },
            "native_source_sha256": source["sha256"],
            "native_source_files": source["files"],
            "local_path_dependencies": {
                "catbird-atproto": atproto_source,
            },
            "cargo_lock_sha256": sha256_file(native / "Cargo.lock"),
            "schemas": schema_record(petrel_catbird, base),
        },
        "build": {
            "cargo_profile": "release",
            "default_features": True,
            "explicit_features": marker["features"],
            "space_protocol_version": marker["protocolVersion"],
            "apple_artifact_scope": artifact_scope,
            "apple_targets": APPLE_TARGETS[artifact_scope],
            "toolchains": toolchains or toolchain_record(),
        },
        "package": {
            "path": recorded_path(core, base),
            "manifest_sha256": sha256_file(core / "Package.swift"),
            "local_dependencies": {
                "Petrel": recorded_path(petrel, base),
                "PetrelCatbird": recorded_path(petrel_catbird, base),
                "CatbirdMLSFFI": recorded_path(framework, base),
            },
        },
        "interface": interface,
        "framework": framework_identity,
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

    # Schema 2 records every path relative to the manifest's own directory.
    # Absolute paths (schema 1) still resolve to themselves.
    base = path.parent
    relative = manifest.get("schema_version") == 2

    def located(value):
        return resolve_recorded(value, base)

    selected = located(manifest["selected_inputs"]["path"])
    require_equal(sha256_file(selected), manifest["selected_inputs"]["sha256"], "selected inputs")
    native = located(manifest["source"]["repositories"]["catbird-mls"]["path"])
    require_equal(
        native_source_fingerprint(native),
        manifest["source"]["native_source_sha256"],
        "native source fingerprint",
    )
    atproto_record = manifest["source"]["local_path_dependencies"]["catbird-atproto"]
    atproto = located(manifest["source"]["repositories"]["catbird-atproto"]["path"])
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
    schema_root = located(schema["root"])
    current_schema = file_map(schema_root, schema_root.rglob("*.json"))
    require_equal(current_schema, schema["files"], "schema file map")

    package = manifest["package"]
    core = located(package["path"])
    require_equal(sha256_file(core / "Package.swift"), package["manifest_sha256"], "Package.swift")
    for name, dependency in package["local_dependencies"].items():
        if not located(dependency).exists():
            raise RuntimeError(f"Apple artifact manifest mismatch: missing local dependency {name}")

    framework = manifest["framework"]
    framework_path = located(framework["path"])
    current_framework = framework_record(framework_path)
    validate_framework_scope(current_framework, manifest["build"]["apple_artifact_scope"])
    require_equal(manifest["build"]["apple_targets"], APPLE_TARGETS[manifest["build"]["apple_artifact_scope"]], "artifact target scope")
    require_equal(current_framework["files"], framework["files"], "framework file map")
    require_equal(current_framework["libraries"], framework["libraries"], "framework architecture matrix")
    current_interface = interface_record(core, framework_path, base if relative else None)
    require_equal(current_interface, manifest["interface"], "generated interface")
    marker = json.loads((framework_path / "labyrinth-space-protocol.json").read_text())
    require_equal(marker.get("generatedSwiftSHA256"), current_interface["generated_swift"]["sha256"], "rebuild marker generated interface")

    for name, record in manifest["source"]["repositories"].items():
        repository = located(record["path"])
        if "commit" in record:
            require_equal(source_commit_identity(repository), {"commit": record["commit"]}, f"{name} source commit")
        elif (repository / ".jj").exists():
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
    generate.add_argument("--framework", type=Path)
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
            framework=args.framework,
            manifest_dir=args.output.resolve().parent,
        )
        write_manifest(args.output, manifest)
        print(manifest["identity"]["canonical_sha256"])
    else:
        manifest = verify_manifest(args.manifest)
        print(manifest["identity"]["canonical_sha256"])


if __name__ == "__main__":
    main()
