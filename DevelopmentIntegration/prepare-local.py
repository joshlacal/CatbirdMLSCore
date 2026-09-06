#!/usr/bin/env python3
"""Reconstruct/verify the sole approved local SDK; no dirty sibling input."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REVISION = "0e68066f0024d05d820be64c0e999771f829d3aa"
REMOTE = "https://github.com/joshlacal/PetrelCatbird.git"
EXPECTED_DIGEST = "c0e6f3bb032b0e10efbbaed5542c3ac660b1eea0f0148adc0cfbf408afdb40c8"
RELATIVE = ".build/LocalIntegration/PetrelCatbird-" + EXPECTED_DIGEST[:16]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def command(*args):
    return subprocess.check_output(args, stderr=subprocess.PIPE)


def verify(package, files):
    actual = {}
    for path in package.rglob("*"):
        if path.is_symlink():
            raise RuntimeError(f"SDK symlink is not allowed: {path}")
        if path.is_file():
            actual[path.relative_to(package).as_posix()] = sha(path.read_bytes())
    if actual != files:
        missing = sorted(set(files) - set(actual))
        extra = sorted(set(actual) - set(files))
        changed = sorted(k for k in set(actual) & set(files) if actual[k] != files[k])
        raise RuntimeError(f"SDK verification failed: missing={missing}, extra={extra}, changed={changed}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    manifest = json.loads((HERE / "petrel-sdk-manifest.json").read_text())
    files = manifest["files"]
    digest = sha((json.dumps(files, sort_keys=True, indent=2) + "\n").encode())
    if (digest != EXPECTED_DIGEST or manifest["baseline_revision"] != REVISION
            or manifest["repository_url"] != REMOTE or manifest["package_directory"] != RELATIVE):
        raise RuntimeError("Manifest does not identify the approved fixed SDK")
    for relative in files:
        path = PurePosixPath(relative)
        if path.is_absolute() or ".." in path.parts or not (relative == "Package.swift" or relative.startswith(("Sources/", "Tests/"))):
            raise RuntimeError("Invalid manifest source path")
    for relative, item in manifest["overlays"].items():
        payload = HERE / "Overlays" / relative
        if payload.is_symlink() or sha(payload.read_bytes()) != item["overlay_sha256"] or item["overlay_sha256"] != files[relative]:
            raise RuntimeError(f"Overlay hash mismatch: {relative}")
    target = ROOT / RELATIVE
    if target.exists():
        verify(target, files)
        print(f"Verified local SDK: {target} ({digest})")
        return
    if args.verify_only:
        raise RuntimeError("Verified SDK is absent; run prepare-local.py first")
    parent = target.parent
    parent.mkdir(parents=True, exist_ok=True)
    # This is a dependency download cache, not a source checkout or user repository.
    cache = parent / "PetrelCatbird-baseline.git"
    if not cache.exists():
        command("git", "init", "--bare", str(cache))
    try:
        actual = command("git", "--git-dir", str(cache), "rev-parse", REVISION + "^{commit}").decode().strip()
    except subprocess.CalledProcessError:
        command("git", "--git-dir", str(cache), "fetch", "--depth=1", REMOTE, REVISION)
        actual = command("git", "--git-dir", str(cache), "rev-parse", REVISION + "^{commit}").decode().strip()
    if actual != REVISION:
        raise RuntimeError("Downloaded SDK revision mismatch")
    names = command("git", "--git-dir", str(cache), "ls-tree", "-r", "--name-only", REVISION).decode().splitlines()
    selected = {p for p in names if p == "Package.swift" or p.startswith(("Sources/", "Tests/"))}
    if selected != set(files):
        raise RuntimeError("Pinned SDK file inventory mismatch")
    stage = Path(tempfile.mkdtemp(prefix=".petrel-sdk-", dir=parent))
    try:
        for relative in sorted(files):
            baseline = command("git", "--git-dir", str(cache), "show", REVISION + ":" + relative)
            if relative in manifest["overlays"]:
                if sha(baseline) != manifest["overlays"][relative]["baseline_sha256"]:
                    raise RuntimeError("Overlay preimage mismatch")
                payload = (HERE / "Overlays" / relative).read_bytes()
            else:
                payload = baseline
            if sha(payload) != files[relative]:
                raise RuntimeError(f"Reconstructed SDK hash mismatch: {relative}")
            destination = stage / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(payload)
            destination.chmod(0o444)
        verify(stage, files)
        if target.exists():
            verify(target, files)
        else:
            stage.rename(target)
        print(f"Prepared local SDK: {target} ({digest})")
    finally:
        if stage.exists():
            shutil.rmtree(stage)


if __name__ == "__main__":
    main()
