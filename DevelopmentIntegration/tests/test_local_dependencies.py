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
        binding = next(target for target in manifest["targets"] if target["name"] == "CatbirdMLS")
        self.assertEqual(binding["path"], "Sources/CatbirdMLS")

    def test_opt_in_manifest_selects_exact_workspace_siblings(self):
        result, manifest = dump_package(local=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        locations = dependency_locations(manifest)
        self.assertEqual(locations["petrel"], ("path", str((WORKSPACE / "Petrel").resolve())))
        self.assertEqual(locations["petrelcatbird"], ("path", str((WORKSPACE / "PetrelCatbird").resolve())))

    def test_verified_framework_selects_matching_space_bindings(self):
        for name, scope in (
            ("CatbirdMLSFFI.xcframework", "apple-all-targets"),
            ("CatbirdMLSFFISimulator.xcframework", "ios-arm64-simulator-only"),
        ):
            with self.subTest(scope=scope), tempfile.TemporaryDirectory() as directory:
                workspace = Path(directory).resolve()
                package = workspace / "CatbirdMLSCore"
                package.mkdir()
                for sibling in ("Petrel", "PetrelCatbird"):
                    (workspace / sibling).mkdir()
                    (workspace / sibling / "Package.swift").write_text("// fixture manifest")
                (package / "Package.swift").write_bytes((ROOT / "Package.swift").read_bytes())
                framework = package / "Sources" / name
                framework.mkdir(parents=True)
                (framework / "labyrinth-space-protocol.json").write_text(json.dumps({
                    "protocolVersion": 20261023, "features": ["labyrinth"],
                    "artifactScope": scope, "sourceFingerprint": "0" * 64,
                }))
                environment = os.environ.copy()
                environment["CATBIRD_MLS_LOCAL_INTEGRATION"] = "1"
                environment["CATBIRD_MLS_FFI_PATH"] = str(framework)
                result = subprocess.run(
                    ["swift", "package", "--package-path", str(package), "dump-package"],
                    env=environment, text=True, capture_output=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                manifest = json.loads(result.stdout)
                binding = next(target for target in manifest["targets"] if target["name"] == "CatbirdMLS")
                self.assertEqual(binding["path"], "Sources/CatbirdMLSSpaces")

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

    def test_selected_inputs_name_siblings_relative_to_the_workspace(self):
        self.assertEqual(
            self.verifier.selected_path({"repository": "Petrel", "path": "Petrel"}),
            (WORKSPACE / "Petrel").resolve(),
        )
        for location in ("/absolute/Petrel", "../Petrel"):
            with self.subTest(location=location), self.assertRaisesRegex(RuntimeError, "sibling-relative"):
                self.verifier.selected_path({"repository": "Petrel", "path": location})

    def test_committed_selected_inputs_record_no_absolute_paths(self):
        selected = json.loads(self.verifier.SELECTED_INPUTS.read_text())
        for item in selected["repos"]:
            self.assertNotIn("workspace", item)
            if "path" in item:
                self.assertFalse(Path(item["path"]).is_absolute())

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

    def test_provenance_hashes_selected_binding_and_rejects_mixed_pair(self):
        with tempfile.TemporaryDirectory() as directory:
            core = Path(directory)
            for name, value in (("CatbirdMLS", "public"), ("CatbirdMLSSpaces", "local")):
                path = core / "Sources" / name / "CatbirdMLS.swift"
                path.parent.mkdir(parents=True)
                path.write_text(value)
            public = {"targets": [
                {"name": "CatbirdMLSFFI", "url": "https://example.test/public.zip"},
                {"name": "CatbirdMLS", "path": "Sources/CatbirdMLS"},
            ]}
            local = {"targets": [
                {"name": "CatbirdMLSFFI", "path": "Sources/CatbirdMLSFFISimulator.xcframework"},
                {"name": "CatbirdMLS", "path": "Sources/CatbirdMLSSpaces"},
            ]}
            public_record = self.verifier.generated_binding_record(public, core)
            local_record = self.verifier.generated_binding_record(local, core)
            self.assertNotEqual(public_record["sha256"], local_record["sha256"])
            self.assertTrue(local_record["path"].endswith("Sources/CatbirdMLSSpaces/CatbirdMLS.swift"))
            local["targets"][1]["path"] = "Sources/CatbirdMLS"
            with self.assertRaisesRegex(RuntimeError, "does not match"):
                self.verifier.generated_binding_record(local, core)

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

    def test_rejects_package_identity_change_from_recorded_provenance(self):
        runner = load_runner()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            provenance = root / "provenance.json"
            provenance.write_text(json.dumps({
                "packages": {
                    "Petrel": {
                        "path": str(root),
                        "jj_commit": "expected-commit",
                        "jj_change": "expected-change",
                    }
                }
            }))
            original = runner.current_jj_identity
            runner.current_jj_identity = lambda _: {
                "jj_commit": "different-commit",
                "jj_change": "expected-change",
            }
            try:
                with self.assertRaisesRegex(RuntimeError, "Petrel changed during local integration"):
                    runner.verify_package_identities(provenance)
            finally:
                runner.current_jj_identity = original

    def test_core_identity_guard_allows_only_runner_retained_resolution(self):
        runner = load_runner()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            provenance = root / "provenance.json"
            provenance.write_text(json.dumps({
                "packages": {
                    "CatbirdMLSCore": {
                        "path": str(root),
                        "jj_commit": "expected-commit",
                        "jj_change": "expected-change",
                    }
                }
            }))
            original_identity = runner.current_jj_identity
            original_paths = runner.changed_paths_from_commit
            runner.current_jj_identity = lambda _: {
                "jj_commit": "retention-commit",
                "jj_change": "expected-change",
            }
            runner.changed_paths_from_commit = lambda _path, _commit: [
                "_trash/local-integration-resolutions/run-local-Package.resolved"
            ]
            try:
                runner.verify_package_identities(provenance)
                runner.changed_paths_from_commit = lambda _path, _commit: [
                    "Sources/CatbirdMLSCore/Changed.swift"
                ]
                with self.assertRaisesRegex(RuntimeError, "CatbirdMLSCore changed"):
                    runner.verify_package_identities(provenance)
            finally:
                runner.current_jj_identity = original_identity
                runner.changed_paths_from_commit = original_paths


if __name__ == "__main__":
    unittest.main()
