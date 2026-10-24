#!/usr/bin/env python3
"""Resolve and record the exact Apple SDK siblings used by local Core builds."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


HERE = Path(__file__).resolve().parent
CORE = HERE.parent.resolve()
WORKSPACE = CORE.parent.resolve()
SELECTED_INPUTS = HERE / "spaces-selected-inputs.json"
APPLE_ARTIFACT_MANIFEST = HERE / "spaces-apple-artifact-manifest.json"
APPLE_ARTIFACT_TOOL = CORE / "Scripts" / "apple-artifact-manifest.py"


def run(*arguments, cwd=CORE, environment=None):
    result = subprocess.run(
        arguments,
        cwd=cwd,
        env=environment,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({' '.join(arguments)}):\n{result.stdout}{result.stderr}"
        )
    return result.stdout, result.stderr


def canonical(value):
    return Path(value).resolve()


def graph_nodes(graph):
    yield graph
    for dependency in graph.get("dependencies", []):
        yield from graph_nodes(dependency)


def validate_dependency_graph(graph, expected_paths):
    """Require each named package to resolve only at its exact local path."""
    resolved = {}
    for display_name, expected_path in expected_paths.items():
        identity = display_name.lower()
        matches = [node for node in graph_nodes(graph) if node.get("identity", "").lower() == identity]
        if not matches:
            raise RuntimeError(f"{display_name} is absent from the resolved dependency graph")
        paths = set()
        for match in matches:
            url = match.get("url", "")
            if match.get("location") or not match.get("path") or url.startswith(("http://", "https://", "ssh://", "git@")):
                raise RuntimeError(f"{display_name} resolved remotely instead of from the approved sibling")
            paths.add(canonical(match["path"]))
        expected = canonical(expected_path)
        if paths != {expected}:
            actual = ", ".join(sorted(str(path) for path in paths))
            raise RuntimeError(f"{display_name} resolved from wrong local path: {actual}; expected {expected}")
        resolved[display_name] = {"kind": "local", "path": str(expected)}
    return resolved


def write_swiftpm_config(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    # A file-URL source-control mirror requires a colocated `.git` directory
    # and therefore fails in a valid JJ-only workspace. Root path dependencies
    # already override PetrelCatbird's transitive Petrel identity; the resolved
    # graph validation below proves both edges select the exact sibling paths.
    payload = {"object": [], "version": 1}
    path = directory / "mirrors.json"
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text() != encoded:
        retained = directory / "_trash"
        retained.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        destination = retained / f"{timestamp}-{os.getpid()}-mirrors.json"
        if destination.exists():
            raise RuntimeError(f"Retained SwiftPM mirror destination already exists: {destination}")
        shutil.move(path, destination)
    path.write_text(encoded)
    return directory


def selected_records():
    if not SELECTED_INPUTS.is_file():
        raise RuntimeError(f"Selected input manifest is absent: {SELECTED_INPUTS}")
    manifest = json.loads(SELECTED_INPUTS.read_text())
    by_name = {item["repository"]: item for item in manifest["repos"]}
    expected = {
        "CatbirdMLSCore": CORE,
        "Petrel": WORKSPACE / "Petrel",
        "PetrelCatbird": WORKSPACE / "PetrelCatbird",
    }
    records = {}
    for name, path in expected.items():
        item = by_name.get(name)
        if item is None:
            raise RuntimeError(f"Selected input manifest has no {name} record")
        if canonical(item["workspace"]) != path.resolve():
            raise RuntimeError(f"Selected {name} workspace does not match exact sibling path")
        if not (path / "Package.swift").is_file() or path.is_symlink():
            raise RuntimeError(f"Approved {name} sibling is missing or symlinked: {path}")
        records[name] = item
    return records


def jj_identity(name, path, selected):
    root, _ = run("jj", "root", cwd=path)
    if canonical(root.strip()) != path.resolve():
        raise RuntimeError(f"{name} is not a JJ workspace rooted at {path}")
    current, _ = run("jj", "log", "--no-graph", "-r", "@", "-T", "commit_id ++ \"\\n\" ++ change_id ++ \"\\n\"", cwd=path)
    lines = current.splitlines()
    if len(lines) != 2:
        raise RuntimeError(f"Could not read current JJ identity for {name}")
    baseline = selected["source_jj_commit"]
    ancestor, _ = run("jj", "log", "--no-graph", "-r", f"{baseline} & ancestors(@)", "-T", "commit_id ++ \"\\n\"", cwd=path)
    if ancestor.strip() != baseline:
        raise RuntimeError(f"{name} no longer descends from selected input {baseline}")
    git_head = None
    git = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=path, text=True, capture_output=True
    )
    if git.returncode == 0:
        git_head = git.stdout.strip()
    return {
        "path": str(path.resolve()),
        "jj_commit": lines[0],
        "jj_change": lines[1],
        "selected_jj_commit": baseline,
        "selected_git_head": selected["source_git_head"],
        "git_head": git_head,
    }


def local_manifest(environment):
    output, _ = run("swift", "package", "dump-package", environment=environment)
    manifest = json.loads(output)
    locations = {}
    for dependency in manifest["dependencies"]:
        if "fileSystem" not in dependency:
            continue
        item = dependency["fileSystem"][0]
        location = item.get("path", item.get("location"))
        locations[item["identity"]] = canonical(location)
    expected = {
        "petrel": (WORKSPACE / "Petrel").resolve(),
        "petrelcatbird": (WORKSPACE / "PetrelCatbird").resolve(),
    }
    if locations != expected:
        raise RuntimeError(f"Core manifest did not select exact local SDK siblings: {locations}")
    return manifest


def resolved_sdk_graph(environment):
    """Resolve the real sibling manifests without downloading the large FFI artifact."""
    with tempfile.TemporaryDirectory(prefix="catbird-apple-resolution-") as directory:
        probe = Path(directory)
        config = write_swiftpm_config(probe / "swiftpm-config")
        source = probe / "Sources" / "Probe"
        source.mkdir(parents=True)
        (source / "Probe.swift").write_text("public enum Probe {}\n")
        petrel = json.dumps(str((WORKSPACE / "Petrel").resolve()))
        overlay = json.dumps(str((WORKSPACE / "PetrelCatbird").resolve()))
        (probe / "Package.swift").write_text(
            "// swift-tools-version: 6.0\n"
            "import PackageDescription\n"
            "let package = Package(name: \"AppleDependencyProbe\", dependencies: [\n"
            f"  .package(path: {petrel}),\n"
            f"  .package(path: {overlay}),\n"
            "], targets: [.target(name: \"Probe\", dependencies: [\n"
            "  .product(name: \"Petrel\", package: \"Petrel\"),\n"
            "  .product(name: \"PetrelCatbird\", package: \"PetrelCatbird\"),\n"
            "])])\n"
        )
        stdout, stderr = run(
            "swift",
            "package",
            "--config-path",
            str(config),
            "--scratch-path",
            str(probe / ".build"),
            "show-dependencies",
            "--format",
            "json",
            cwd=probe,
            environment=environment,
        )
        graph = json.loads(stdout)
        resolved = validate_dependency_graph(
            graph,
            {
                "Petrel": WORKSPACE / "Petrel",
                "PetrelCatbird": WORKSPACE / "PetrelCatbird",
            },
        )
        return graph, resolved


def hash_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def hash_tree(path):
    entries = {}
    for item in sorted(path.rglob("*")):
        if item.is_symlink():
            raise RuntimeError(f"FFI framework contains a symlink: {item}")
        if item.is_file():
            entries[item.relative_to(path).as_posix()] = hash_file(item)
    encoded = (json.dumps(entries, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return {"sha256": hashlib.sha256(encoded).hexdigest(), "files": entries}


def ffi_identity(manifest):
    target = next(item for item in manifest["targets"] if item["name"] == "CatbirdMLSFFI")
    if target.get("path"):
        path = canonical(target["path"])
        expected = {(CORE / "Sources" / name).resolve() for name in (
            "CatbirdMLSFFI.xcframework", "CatbirdMLSFFISimulator.xcframework")}
        if path not in expected or not path.is_dir():
            raise RuntimeError(f"Local FFI target resolved from wrong path: {path}")
        return {"kind": "local", "path": str(path), **hash_tree(path)}
    return {
        "kind": "remote",
        "url": target["url"],
        "checksum": target["checksum"],
    }


def verify_local_ffi_manifest(framework, manifest_path=None):
    framework = canonical(framework)
    manifest_path = canonical(manifest_path or (
        HERE / "spaces-simulator-artifact-manifest.json"
        if framework.name == "CatbirdMLSFFISimulator.xcframework" else APPLE_ARTIFACT_MANIFEST))
    if not framework.is_dir():
        return
    if not manifest_path.is_file():
        raise RuntimeError(
            f"Local FFI identity manifest is absent: {manifest_path}. "
            "Regenerate the native artifact and its canonical manifest together."
        )
    run(
        "python3",
        str(APPLE_ARTIFACT_TOOL),
        "verify",
        str(manifest_path),
    )


def collect():
    environment = os.environ.copy()
    environment["CATBIRD_MLS_LOCAL_INTEGRATION"] = "1"
    framework = canonical(environment.get("CATBIRD_MLS_FFI_PATH", CORE / "Sources" / "CatbirdMLSFFI.xcframework"))
    if "CATBIRD_MLS_FFI_PATH" in environment and not framework.is_dir():
        raise RuntimeError(f"Explicit native framework is absent: {framework}")
    if framework.is_dir():
        environment["CATBIRD_MLS_FFI_PATH"] = str(framework.resolve())
        verify_local_ffi_manifest(framework)
    else:
        environment.pop("CATBIRD_MLS_FFI_PATH", None)
    selected = selected_records()
    manifest = local_manifest(environment)
    _, resolved = resolved_sdk_graph(environment)
    identities = {
        name: jj_identity(name, Path(item["workspace"]), item)
        for name, item in selected.items()
    }
    for name, resolution in resolved.items():
        identities[name].update(resolution)
    return {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "selected_inputs": {
            "path": str(SELECTED_INPUTS),
            "sha256": hash_file(SELECTED_INPUTS),
        },
        "packages": identities,
        "ffi": ffi_identity(manifest),
        "generated_swift_sha256": hash_file(CORE / "Sources" / "CatbirdMLS" / "CatbirdMLS.swift"),
        "scope": "Apple local dependency and provenance prerequisite; no FFI generation or runtime qualification.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output")
    parser.add_argument(
        "--config-dir",
        default=str(CORE / ".build" / "LocalIntegration" / "swiftpm-config"),
    )
    args = parser.parse_args()
    write_swiftpm_config(Path(args.config_dir))
    provenance = collect()
    payload = json.dumps(provenance, indent=2, sort_keys=True) + "\n"
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists():
            raise RuntimeError(f"Refusing to replace existing provenance: {output}")
        output.write_text(payload)
    print(payload, end="")


if __name__ == "__main__":
    main()
