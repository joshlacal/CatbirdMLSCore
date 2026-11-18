import os
from pathlib import Path
import plistlib
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
            labyrinth = workspace / "labyrinth"
            scripts = core / "Scripts"
            sources = core / "Sources" / "CatbirdMLS"
            framework = core / "Sources" / framework_name
            fake_bin = workspace / "bin"
            for path in (scripts, sources, framework, native, atproto / "src", labyrinth / "labyrinth-protocol" / "src", fake_bin):
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
            (labyrinth / "Cargo.toml").write_text("[workspace]\nmembers = ['.', 'labyrinth-protocol']\n")
            (labyrinth / "labyrinth-protocol" / "Cargo.toml").write_text("[package]\nname = 'labyrinth-protocol'\n")
            (labyrinth / "labyrinth-protocol" / "src" / "lib.rs").write_text("pub fn fixture() {}\n")
            (native / "src").mkdir()
            (native / "src" / "lib.rs").write_text("pub fn fixture() {}\n")
            (native / framework_name).mkdir()
            (native / framework_name / "old-native").write_text("old native framework")
            (native / "build" / "bindings").mkdir(parents=True)
            (native / "build" / "bindings" / "old-binding").write_text("old native binding")
            (framework / "old-binary").write_text("old framework")
            (sources / "CatbirdMLS.swift").write_text("old binding")
            (sources / "MLSFFI.swift").write_text("stale binding")
            # Leftover of the retired split layout: the rebuild must neither
            # read nor replace it, because only Sources/CatbirdMLS is packaged.
            legacy_sources = core / "Sources" / "CatbirdMLSSpaces"
            legacy_sources.mkdir(parents=True)
            (legacy_sources / "CatbirdMLS.swift").write_text("legacy split binding")

            other = core / "Sources" / "Unselected.xcframework"
            other.mkdir()
            (other / "keep").write_text("unselected framework")

            # Use the same slice layout the rebuild hashes before replacing the package.
            # The simulator path also issues a source receipt; only the VCS/tool identities are fixtures.
            slice_id = "ios-arm64-simulator"
            slice_library = Path(slice_id) / "libCatbirdMLSFFI.a"
            with (workspace / "slice-info.plist").open("wb") as file:
                plistlib.dump({"AvailableLibraries": [{
                    "LibraryIdentifier": slice_id, "LibraryPath": "libCatbirdMLSFFI.a",
                    "SupportedPlatform": "ios", "SupportedPlatformVariant": "simulator",
                    "SupportedArchitectures": ["arm64"],
                }]}, file)
            if scope == "ios-arm64-simulator-only":
                (core / "DevelopmentIntegration").mkdir()
                (core / "DevelopmentIntegration/spaces-selected-inputs.json").write_text('{"repos":[]}')
                (core / "Package.swift").write_text("// fixture")
                (workspace / "PetrelCatbird/lexicons").mkdir(parents=True)
                for repository in (native, atproto, labyrinth, workspace / "Petrel", workspace / "PetrelCatbird"):
                    (repository / ".jj").mkdir(parents=True)
                jj = fake_bin / "jj"
                jj.write_text("#!/bin/sh\nset -eu\n[ \"$1\" = log ]\nprintf 'empty\\n%040d\\n' 1\n")
                jj.chmod(0o755)
                for name in ("swift", "rustc", "cargo", "xcodebuild"):
                    tool = fake_bin / name
                    tool.write_text("#!/bin/sh\necho fixture-toolchain\n")
                    tool.chmod(0o755)

            builder = native / "create-xcframework.sh"
            builder.write_text(
                "#!/bin/sh\n"
                "set -eu\n"
                f"mkdir -p {framework_name}/{slice_id} build/bindings\n"
                f"cp ../slice-info.plist {framework_name}/Info.plist\n"
                f"printf 'new framework' > {framework_name}/{slice_library}\n"
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
            self.assertEqual((framework / slice_library).read_text(), "new framework")
            self.assertEqual((sources / "CatbirdMLS.swift").read_text(), "new binding")
            self.assertFalse((sources / "MLSFFI.swift").exists())
            self.assertEqual((legacy_sources / "CatbirdMLS.swift").read_text(), "legacy split binding")
            self.assertEqual((other / "keep").read_text(), "unselected framework")
            import json
            marker = json.loads((framework / "labyrinth-space-protocol.json").read_text())
            self.assertEqual(marker["artifactScope"], scope)
            expected_fingerprint = subprocess.run(
                [
                    "python3",
                    str(scripts / "apple-artifact-manifest.py"),
                    "source-fingerprint",
                    "--native",
                    str(native),
                    "--dependency",
                    str(atproto),
                    "--dependency",
                    str(labyrinth),
                ],
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            self.assertEqual(marker["sourceFingerprint"], expected_fingerprint)
            import hashlib
            self.assertEqual(marker["generatedSwiftSHA256"], hashlib.sha256(b"new binding").hexdigest())

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
            labyrinth = workspace / "labyrinth"
            scripts = core / "Scripts"
            sources = core / "Sources" / "CatbirdMLS"
            framework = core / "Sources" / "CatbirdMLSFFI.xcframework"
            fake_bin = workspace / "bin"
            for path in (scripts, sources, framework, native / "src", atproto / "src", labyrinth / "labyrinth-protocol" / "src", fake_bin):
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
            (labyrinth / "Cargo.toml").write_text("[workspace]\nmembers = ['.', 'labyrinth-protocol']\n")
            (labyrinth / "labyrinth-protocol" / "Cargo.toml").write_text("[package]\nname = 'labyrinth-protocol'\n")
            (labyrinth / "labyrinth-protocol" / "src" / "lib.rs").write_text("pub fn dependency() {}\n")

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

    def test_rebuild_rejects_labyrinth_change_during_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            core = workspace / "CatbirdMLSCore"
            native = workspace / "catbird-mls"
            atproto = workspace / "catbird-atproto"
            labyrinth = workspace / "labyrinth"
            scripts = core / "Scripts"
            sources = core / "Sources" / "CatbirdMLS"
            framework = core / "Sources" / "CatbirdMLSFFI.xcframework"
            fake_bin = workspace / "bin"
            for path in (scripts, sources, framework, native / "src", atproto / "src", labyrinth / "labyrinth-protocol" / "src", fake_bin):
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
            (labyrinth / "Cargo.toml").write_text("[workspace]\nmembers = ['.', 'labyrinth-protocol']\n")
            (labyrinth / "labyrinth-protocol" / "Cargo.toml").write_text("[package]\nname = 'labyrinth-protocol'\n")
            (labyrinth / "labyrinth-protocol" / "src" / "lib.rs").write_text("pub fn before() {}\n")

            builder = native / "create-xcframework.sh"
            builder.write_text(
                "#!/bin/sh\n"
                "set -eu\n"
                "printf 'pub fn after() {}\\n' > ../labyrinth/labyrinth-protocol/src/lib.rs\n"
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
