import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class RebuildPreservationTests(unittest.TestCase):
    def check_rebuild_retains_every_replaced_artifact(self, scope):
        framework_name = "CatbirdMLSFFISimulator.xcframework" if scope == "ios-arm64-simulator-only" else "CatbirdMLSFFI.xcframework"
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            core = workspace / "CatbirdMLSCore"
            native = workspace / "catbird-mls"
            atproto = workspace / "catbird-atproto"
            scripts = core / "Scripts"
            sources = core / "Sources" / "CatbirdMLSSpaces"
            framework = core / "Sources" / framework_name
            fake_bin = workspace / "bin"
            for path in (scripts, sources, framework, native, atproto / "src", fake_bin):
                path.mkdir(parents=True, exist_ok=True)

            shutil.copy2(ROOT / "Scripts" / "rebuild-ffi.sh", scripts / "rebuild-ffi.sh")
            shutil.copy2(
                ROOT / "Scripts" / "apple-artifact-manifest.py",
                scripts / "apple-artifact-manifest.py",
            )
            (scripts / "rebuild-ffi.sh").chmod(0o755)
            (native / "Cargo.toml").write_text("[package]\nname = 'fixture'\n")
            (native / "Cargo.lock").write_text("version = 4\n")
            (atproto / "Cargo.toml").write_text("[package]\nname = 'catbird-atproto'\n")
            (atproto / "src" / "lib.rs").write_text("pub fn fixture() {}\n")
            (native / "src").mkdir()
            (native / "src" / "lib.rs").write_text("pub fn fixture() {}\n")
            (native / framework_name).mkdir()
            (native / framework_name / "old-native").write_text("old native framework")
            (native / "build" / "bindings").mkdir(parents=True)
            (native / "build" / "bindings" / "old-binding").write_text("old native binding")
            (framework / "old-binary").write_text("old framework")
            (sources / "CatbirdMLS.swift").write_text("old binding")
            (sources / "MLSFFI.swift").write_text("stale binding")
            public_sources = core / "Sources" / "CatbirdMLS"
            public_sources.mkdir(parents=True)
            (public_sources / "CatbirdMLS.swift").write_text("public release binding")

            other = core / "Sources" / "Unselected.xcframework"
            other.mkdir()
            (other / "keep").write_text("unselected framework")

            builder = native / "create-xcframework.sh"
            builder.write_text(
                "#!/bin/sh\n"
                "set -eu\n"
                f"mkdir -p {framework_name} build/bindings\n"
                f"printf 'new framework' > {framework_name}/new-binary\n"
                "printf 'new binding' > build/bindings/CatbirdMLS.swift\n"
            )
            builder.chmod(0o755)
            xcrun = fake_bin / "xcrun"
            xcrun.write_text("#!/bin/sh\nexit 0\n")
            xcrun.chmod(0o755)

            environment = os.environ.copy()
            environment["PATH"] = str(fake_bin) + os.pathsep + environment["PATH"]
            environment["CATBIRD_MLS_APPLE_SCOPE"] = scope
            environment["FFI_BACKUP_TIMESTAMP"] = "20260919T120000Z"
            environment["CATBIRD_MLS_NATIVE_DIR"] = str(native)
            result = subprocess.run(
                [str(scripts / "rebuild-ffi.sh")],
                cwd=core,
                env=environment,
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)

            backup = core / "_trash" / "ffi-rebuild" / "20260919T120000Z"
            self.assertEqual((backup / framework_name / "old-binary").read_text(), "old framework")
            self.assertEqual((backup / "CatbirdMLSSpaces.swift").read_text(), "old binding")
            self.assertEqual((backup / "MLSFFISpaces.swift").read_text(), "stale binding")
            self.assertEqual(
                (backup / ("native-" + framework_name) / "old-native").read_text(),
                "old native framework",
            )
            self.assertEqual(
                (backup / "native-build-bindings" / "old-binding").read_text(),
                "old native binding",
            )
            self.assertEqual((core / "Sources" / framework_name / "new-binary").read_text(), "new framework")
            self.assertEqual((sources / "CatbirdMLS.swift").read_text(), "new binding")
            self.assertFalse((sources / "MLSFFI.swift").exists())
            self.assertEqual((public_sources / "CatbirdMLS.swift").read_text(), "public release binding")
            self.assertEqual((other / "keep").read_text(), "unselected framework")
            import json
            marker = json.loads((framework / "labyrinth-space-protocol.json").read_text())
            self.assertEqual(marker["artifactScope"], scope)

    def test_rebuild_retains_every_replaced_artifact(self):
        self.check_rebuild_retains_every_replaced_artifact("apple-all-targets")

    def test_simulator_rebuild_is_separate_and_marked_partial(self):
        self.check_rebuild_retains_every_replaced_artifact("ios-arm64-simulator-only")

    def test_rebuild_rejects_native_source_change_during_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            core = workspace / "CatbirdMLSCore"
            native = workspace / "catbird-mls"
            atproto = workspace / "catbird-atproto"
            scripts = core / "Scripts"
            sources = core / "Sources" / "CatbirdMLSSpaces"
            framework = core / "Sources" / "CatbirdMLSFFI.xcframework"
            fake_bin = workspace / "bin"
            for path in (scripts, sources, framework, native / "src", atproto / "src", fake_bin):
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
            (atproto / "Cargo.toml").write_text("[package]\nname = 'catbird-atproto'\n")
            (atproto / "src" / "lib.rs").write_text("pub fn dependency() {}\n")

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
            self.assertIn(
                "Native source or local path dependency changed during FFI generation",
                result.stderr + result.stdout,
            )
            self.assertEqual((framework / "old-binary").read_text(), "old framework")
            self.assertEqual((sources / "CatbirdMLS.swift").read_text(), "old binding")

    def test_rebuild_rejects_local_path_dependency_change_during_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            core = workspace / "CatbirdMLSCore"
            native = workspace / "catbird-mls"
            atproto = workspace / "catbird-atproto"
            scripts = core / "Scripts"
            sources = core / "Sources" / "CatbirdMLSSpaces"
            framework = core / "Sources" / "CatbirdMLSFFI.xcframework"
            fake_bin = workspace / "bin"
            for path in (scripts, sources, framework, native / "src", atproto / "src", fake_bin):
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
            (native / "src" / "lib.rs").write_text("pub fn native() {}\n")
            (atproto / "Cargo.toml").write_text("[package]\nname = 'catbird-atproto'\n")
            (atproto / "src" / "lib.rs").write_text("pub fn before() {}\n")

            builder = native / "create-xcframework.sh"
            builder.write_text(
                "#!/bin/sh\n"
                "set -eu\n"
                "printf 'pub fn after() {}\\n' > ../catbird-atproto/src/lib.rs\n"
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
            self.assertIn(
                "Native source or local path dependency changed during FFI generation",
                result.stderr + result.stdout,
            )
            self.assertEqual((framework / "old-binary").read_text(), "old framework")
            self.assertEqual((sources / "CatbirdMLS.swift").read_text(), "old binding")


if __name__ == "__main__":
    unittest.main()
