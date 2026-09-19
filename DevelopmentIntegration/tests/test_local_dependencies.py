import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parent
SCRIPT = ROOT / "DevelopmentIntegration" / "verify-local-dependencies.py"
RUNNER = ROOT / "DevelopmentIntegration" / "run-local-command.py"


def load_verifier():
    spec = importlib.util.spec_from_file_location("local_dependencies", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_runner():
    spec = importlib.util.spec_from_file_location("local_dependency_runner", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def dump_package(local=False, extra_environment=None):
    environment = os.environ.copy()
    environment.pop("CATBIRD_MLS_LOCAL_INTEGRATION", None)
    environment.pop("CATBIRD_MLS_FFI_PATH", None)
    if local:
        environment["CATBIRD_MLS_LOCAL_INTEGRATION"] = "1"
    environment.update(extra_environment or {})
    result = subprocess.run(
        ["swift", "package", "dump-package"],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
    )
    return result, json.loads(result.stdout) if result.returncode == 0 else None


def dependency_locations(manifest):
    locations = {}
    for dependency in manifest["dependencies"]:
        if "fileSystem" in dependency:
            item = dependency["fileSystem"][0]
            locations[item["identity"]] = ("path", str(Path(item["path"] if "path" in item else item["location"]).resolve()))
        elif "sourceControl" in dependency:
            item = dependency["sourceControl"][0]
            remote = item["location"]["remote"][0]["urlString"]
            locations[item["identity"]] = ("remote", remote)
    return locations


class PackageSelectionTests(unittest.TestCase):
    def test_default_manifest_keeps_pinned_remote_sdks(self):
        result, manifest = dump_package()
        self.assertEqual(result.returncode, 0, result.stderr)
        locations = dependency_locations(manifest)
        self.assertEqual(locations["petrel"], ("remote", "https://github.com/joshlacal/Petrel.git"))
        self.assertEqual(locations["petrelcatbird"], ("remote", "https://github.com/joshlacal/PetrelCatbird.git"))

    def test_opt_in_manifest_selects_exact_workspace_siblings(self):
        result, manifest = dump_package(local=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        locations = dependency_locations(manifest)
        self.assertEqual(locations["petrel"], ("path", str((WORKSPACE / "Petrel").resolve())))
        self.assertEqual(locations["petrelcatbird"], ("path", str((WORKSPACE / "PetrelCatbird").resolve())))

    def test_manifest_rejects_ambiguous_local_selection_value(self):
        result, _ = dump_package(extra_environment={"CATBIRD_MLS_LOCAL_INTEGRATION": "yes"})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be exactly 0 or 1", result.stderr)


class ResolutionGuardTests(unittest.TestCase):
    def setUp(self):
        self.verifier = load_verifier()
        self.expected = {
            "CatbirdMLSCore": ROOT.resolve(),
            "Petrel": (WORKSPACE / "Petrel").resolve(),
            "PetrelCatbird": (WORKSPACE / "PetrelCatbird").resolve(),
        }

    def graph(self):
        return {
            "identity": "catbirdmlscore",
            "path": str(ROOT.resolve()),
            "dependencies": [
                {
                    "identity": "petrel",
                    "path": str((WORKSPACE / "Petrel").resolve()),
                    "url": str((WORKSPACE / "Petrel").resolve()),
                    "dependencies": [],
                },
                {
                    "identity": "petrelcatbird",
                    "path": str((WORKSPACE / "PetrelCatbird").resolve()),
                    "url": str((WORKSPACE / "PetrelCatbird").resolve()),
                    "dependencies": [
                        {
                            "identity": "petrel",
                            "path": str((WORKSPACE / "Petrel").resolve()),
                            "url": str((WORKSPACE / "Petrel").resolve()),
                            "dependencies": [],
                        }
                    ],
                },
            ],
        }

    def test_accepts_one_exact_local_identity_per_package(self):
        resolved = self.verifier.validate_dependency_graph(self.graph(), self.expected)
        self.assertEqual(resolved["Petrel"]["path"], str((WORKSPACE / "Petrel").resolve()))
        self.assertEqual(resolved["PetrelCatbird"]["kind"], "local")

    def test_rejects_remote_or_wrong_path_resolution(self):
        graph = self.graph()
        graph["dependencies"][0] = {
            "identity": "petrel",
            "url": "https://github.com/joshlacal/Petrel.git",
            "dependencies": [],
        }
        with self.assertRaisesRegex(RuntimeError, "Petrel resolved remotely"):
            self.verifier.validate_dependency_graph(graph, self.expected)

        graph = self.graph()
        graph["dependencies"][1]["path"] = str(WORKSPACE / "wrong-PetrelCatbird")
        with self.assertRaisesRegex(RuntimeError, "wrong local path"):
            self.verifier.validate_dependency_graph(graph, self.expected)

    def test_writes_empty_mirror_config_for_jj_only_siblings(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.verifier.write_swiftpm_config(Path(directory))
            mirrors = json.loads((config / "mirrors.json").read_text())
            self.assertEqual(
                mirrors,
                {"object": [], "version": 1},
            )

    def test_preserves_stale_mirror_config_before_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "config"
            config.mkdir()
            (config / "mirrors.json").write_text('{"object":[{"mirror":"stale"}],"version":1}\n')

            self.verifier.write_swiftpm_config(config)

            self.assertEqual(
                json.loads((config / "mirrors.json").read_text()),
                {"object": [], "version": 1},
            )
            retained = list((config / "_trash").glob("*-mirrors.json"))
            self.assertEqual(len(retained), 1)
            self.assertIn("stale", retained[0].read_text())

    def test_local_framework_requires_verified_artifact_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            framework = root / "CatbirdMLSFFI.xcframework"
            framework.mkdir()
            manifest = root / "apple-artifact-manifest.json"

            with self.assertRaisesRegex(RuntimeError, "identity manifest is absent"):
                self.verifier.verify_local_ffi_manifest(framework, manifest)


class LocalCommandTests(unittest.TestCase):
    def test_preserves_remote_lock_and_retains_local_resolution(self):
        runner = load_runner()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config"
            config.mkdir()
            executable = root / "fake-swift"
            executable.write_text(
                "#!/bin/sh\n"
                "printf 'local resolution' > Package.resolved\n"
                "exit 0\n"
            )
            executable.chmod(0o755)
            (root / "Package.resolved").write_text("remote resolution")

            code = runner.run(["test", "--filter", "Example"], config, root, str(executable))

            self.assertEqual(code, 0)
            self.assertEqual((root / "Package.resolved").read_text(), "remote resolution")
            retained = list((root / "_trash/local-integration-resolutions").glob("*local-Package.resolved"))
            self.assertEqual(len(retained), 1)
            self.assertEqual(retained[0].read_text(), "local resolution")


if __name__ == "__main__":
    unittest.main()
