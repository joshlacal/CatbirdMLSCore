import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class RebuildPreservationTests(unittest.TestCase):
    def test_rebuild_retains_every_replaced_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            core = workspace / "CatbirdMLSCore"
            native = workspace / "catbird-mls"
            scripts = core / "Scripts"
            sources = core / "Sources" / "CatbirdMLS"
            framework = core / "Sources" / "CatbirdMLSFFI.xcframework"
            fake_bin = workspace / "bin"
            for path in (scripts, sources, framework, native, fake_bin):
                path.mkdir(parents=True, exist_ok=True)

            shutil.copy2(ROOT / "Scripts" / "rebuild-ffi.sh", scripts / "rebuild-ffi.sh")
            shutil.copy2(
                ROOT / "Scripts" / "apple-artifact-manifest.py",
                scripts / "apple-artifact-manifest.py",
            )
            (scripts / "rebuild-ffi.sh").chmod(0o755)
            (native / "Cargo.toml").write_text("[package]\nname = 'fixture'\n")
            (native / "Cargo.lock").write_text("version = 4\n")
            (native / "src").mkdir()
            (native / "src" / "lib.rs").write_text("pub fn fixture() {}\n")
            (native / "CatbirdMLSFFI.xcframework").mkdir()
            (native / "CatbirdMLSFFI.xcframework" / "old-native").write_text("old native framework")
            (native / "build" / "bindings").mkdir(parents=True)
            (native / "build" / "bindings" / "old-binding").write_text("old native binding")
            (framework / "old-binary").write_text("old framework")
            (sources / "CatbirdMLS.swift").write_text("old binding")
            (sources / "MLSFFI.swift").write_text("stale binding")

            builder = native / "create-xcframework.sh"
            builder.write_text(
                "#!/bin/sh\n"
                "set -eu\n"
                "mkdir -p CatbirdMLSFFI.xcframework build/bindings\n"
                "printf 'new framework' > CatbirdMLSFFI.xcframework/new-binary\n"
                "printf 'new binding' > build/bindings/CatbirdMLS.swift\n"
            )
            builder.chmod(0o755)
            xcrun = fake_bin / "xcrun"
            xcrun.write_text("#!/bin/sh\nexit 0\n")
            xcrun.chmod(0o755)

            environment = os.environ.copy()
            environment["PATH"] = str(fake_bin) + os.pathsep + environment["PATH"]
            environment["FFI_BACKUP_TIMESTAMP"] = "20260919T120000Z"
            result = subprocess.run(
                [str(scripts / "rebuild-ffi.sh")],
                cwd=core,
                env=environment,
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)

            backup = core / "_trash" / "ffi-rebuild" / "20260919T120000Z"
            self.assertEqual((backup / "CatbirdMLSFFI.xcframework" / "old-binary").read_text(), "old framework")
            self.assertEqual((backup / "CatbirdMLS.swift").read_text(), "old binding")
            self.assertEqual((backup / "MLSFFI.swift").read_text(), "stale binding")
            self.assertEqual(
                (backup / "native-CatbirdMLSFFI.xcframework" / "old-native").read_text(),
                "old native framework",
            )
            self.assertEqual(
                (backup / "native-build-bindings" / "old-binding").read_text(),
                "old native binding",
            )
            self.assertEqual((core / "Sources" / "CatbirdMLSFFI.xcframework" / "new-binary").read_text(), "new framework")
            self.assertEqual((sources / "CatbirdMLS.swift").read_text(), "new binding")
            self.assertFalse((sources / "MLSFFI.swift").exists())

    def test_rebuild_rejects_native_source_change_during_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            core = workspace / "CatbirdMLSCore"
            native = workspace / "catbird-mls"
            scripts = core / "Scripts"
            sources = core / "Sources" / "CatbirdMLS"
            framework = core / "Sources" / "CatbirdMLSFFI.xcframework"
            fake_bin = workspace / "bin"
            for path in (scripts, sources, framework, native / "src", fake_bin):
                path.mkdir(parents=True, exist_ok=True)

            shutil.copy2(ROOT / "Scripts" / "rebuild-ffi.sh", scripts / "rebuild-ffi.sh")
            shutil.copy2(
                ROOT / "Scripts" / "apple-artifact-manifest.py",
                scripts / "apple-artifact-manifest.py",
            )
            (scripts / "rebuild-ffi.sh").chmod(0o755)
            (framework / "old-binary").write_text("old framework")
            (sources / "CatbirdMLS.swift").write_text("old binding")
            (native / "Cargo.toml").write_text("[package]\nname = 'fixture'\n")
            (native / "Cargo.lock").write_text("version = 4\n")
            (native / "src" / "lib.rs").write_text("pub fn before() {}\n")

            builder = native / "create-xcframework.sh"
            builder.write_text(
                "#!/bin/sh\n"
                "set -eu\n"
                "printf 'pub fn after() {}\\n' > src/lib.rs\n"
                "mkdir -p CatbirdMLSFFI.xcframework build/bindings\n"
                "printf 'new framework' > CatbirdMLSFFI.xcframework/new-binary\n"
                "printf 'new binding' > build/bindings/CatbirdMLS.swift\n"
            )
            builder.chmod(0o755)
            xcrun = fake_bin / "xcrun"
            xcrun.write_text("#!/bin/sh\nexit 0\n")
            xcrun.chmod(0o755)

            environment = os.environ.copy()
            environment["PATH"] = str(fake_bin) + os.pathsep + environment["PATH"]
            result = subprocess.run(
                [str(scripts / "rebuild-ffi.sh")],
                cwd=core,
                env=environment,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Native source changed during FFI generation", result.stderr + result.stdout)
            self.assertEqual((framework / "old-binary").read_text(), "old framework")
            self.assertEqual((sources / "CatbirdMLS.swift").read_text(), "old binding")


if __name__ == "__main__":
    unittest.main()
