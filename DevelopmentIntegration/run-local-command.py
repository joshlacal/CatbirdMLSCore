#!/usr/bin/env python3
"""Run SwiftPM with local paths without allowing the remote lockfile to win."""

import argparse
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path
import shutil
import subprocess
import sys
import uuid


HERE = Path(__file__).resolve().parent
CORE = HERE.parent


def unique_retained_path(directory, name):
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return directory / f"{timestamp}-{uuid.uuid4().hex}-{name}"


def current_jj_identity(path):
    result = subprocess.run(
        [
            "jj", "log", "--no-graph", "-r", "@", "-T",
            'commit_id ++ "\\n" ++ change_id ++ "\\n"',
        ],
        cwd=path,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Could not read JJ identity for {path}:\n{result.stdout}{result.stderr}"
        )
    lines = result.stdout.splitlines()
    if len(lines) != 2:
        raise RuntimeError(f"Could not read exact JJ identity for {path}")
    return {"jj_commit": lines[0], "jj_change": lines[1]}


def changed_paths_from_commit(path, commit):
    result = subprocess.run(
        ["jj", "diff", "--from", commit, "--to", "@", "--name-only"],
        cwd=path,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Could not compare JJ identity for {path}:\n{result.stdout}{result.stderr}"
        )
    return [line for line in result.stdout.splitlines() if line]


def verify_package_identities(provenance_path):
    provenance = json.loads(Path(provenance_path).read_text())
    for name, record in provenance["packages"].items():
        current = current_jj_identity(record["path"])
        expected = {key: record[key] for key in ("jj_commit", "jj_change")}
        if current != expected:
            # This runner deliberately retains SwiftPM's local resolution in
            # Core's tracked _trash tree. That preservation write changes the
            # working-copy commit after the command even when every build input
            # stayed byte-identical. Accept only that exact runner-owned path;
            # source, manifest, artifact, and dependency changes still fail.
            retained_prefix = "_trash/local-integration-resolutions/"
            if (
                name == "CatbirdMLSCore"
                and current["jj_change"] == expected["jj_change"]
                and (changed := changed_paths_from_commit(record["path"], expected["jj_commit"]))
                and all(path.startswith(retained_prefix) for path in changed)
            ):
                continue
            raise RuntimeError(
                f"{name} changed during local integration; expected {expected}, found {current}"
            )


def run(command, config_dir, core=CORE, executable="swift", provenance_path=None):
    core = Path(core).resolve()
    lock_path = core / ".build/LocalIntegration/package-resolved.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    retained = core / "_trash/local-integration-resolutions"
    resolved = core / "Package.resolved"
    original = None
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if provenance_path is not None:
            verify_package_identities(provenance_path)
        if resolved.exists():
            original = unique_retained_path(retained, "remote-Package.resolved")
            shutil.move(resolved, original)
        try:
            result = subprocess.run(
                [
                    executable,
                    command[0],
                    "--config-path",
                    str(Path(config_dir).resolve()),
                    "--scratch-path",
                    str((core / ".build/LocalIntegration/scratch").resolve()),
                    *command[1:],
                ],
                cwd=core,
            )
            return_code = result.returncode
        finally:
            if resolved.exists():
                generated = unique_retained_path(retained, "local-Package.resolved")
                shutil.move(resolved, generated)
            if original is not None:
                shutil.move(original, resolved)
        if provenance_path is not None:
            verify_package_identities(provenance_path)
        return return_code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", required=True)
    parser.add_argument("--provenance")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.command:
        parser.error("a SwiftPM command is required")
    raise SystemExit(run(args.command, args.config_dir, provenance_path=args.provenance))


if __name__ == "__main__":
    main()
