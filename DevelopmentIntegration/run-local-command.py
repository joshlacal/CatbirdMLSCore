#!/usr/bin/env python3
"""Run SwiftPM with local paths without allowing the remote lockfile to win."""

import argparse
from datetime import datetime, timezone
import fcntl
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


def run(command, config_dir, core=CORE, executable="swift"):
    core = Path(core).resolve()
    lock_path = core / ".build/LocalIntegration/package-resolved.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    retained = core / "_trash/local-integration-resolutions"
    resolved = core / "Package.resolved"
    original = None
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
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
            return result.returncode
        finally:
            if resolved.exists():
                generated = unique_retained_path(retained, "local-Package.resolved")
                shutil.move(resolved, generated)
            if original is not None:
                shutil.move(original, resolved)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.command:
        parser.error("a SwiftPM command is required")
    raise SystemExit(run(args.command, args.config_dir))


if __name__ == "__main__":
    main()
