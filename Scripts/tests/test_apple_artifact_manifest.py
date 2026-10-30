import importlib.util
import json
from pathlib import Path
import plistlib
import shutil
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "apple-artifact-manifest.py"


def load_script():
    spec = importlib.util.spec_from_file_location("apple_artifact_manifest", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write(path, contents):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


class AppleArtifactManifestTests(unittest.TestCase):
    def setUp(self):
        self.module = load_script()
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.native = self.root / "catbird-mls"
        self.core = self.root / "CatbirdMLSCore"
        self.petrel = self.root / "Petrel"
        self.petrel_catbird = self.root / "PetrelCatbird"
        self.atproto = self.root / "catbird-atproto"

        for directory in (
            self.native,
            self.core,
            self.petrel,
            self.petrel_catbird,
            self.atproto,
        ):
            directory.mkdir(parents=True)

        write(self.native / "Cargo.toml", "[package]\nname = 'catbird-mls'\n")
        write(self.native / "Cargo.lock", "version = 4\n")
        write(self.native / "uniffi.toml", "[bindings.swift]\n")
        write(self.native / "src/lib.rs", "pub fn binding_input() {}\n")
        write(self.native / "src/orchestrator/direct_request_types.rs", "pub enum RequestPreview {}\n")
        write(self.atproto / "Cargo.toml", "[package]\nname = 'catbird-atproto'\n")
        write(self.atproto / "src/lib.rs", "pub fn generated_contract() {}\n")
        write(self.core / "Package.swift", "// fixture\n")
        write(self.core / "Sources/CatbirdMLSSpaces/CatbirdMLS.swift", "public struct Generated {}\n")
        schema = self.petrel_catbird / "lexicons/blue/catbird/chat/blue.catbird.chat.prepareDirectRequest.json"
        write(schema, '{"lexicon":1}\n')
        write(self.petrel_catbird / "lexicons/blue/catbird/labyrinth/blue.catbird.labyrinth.defs.json", '{"lexicon":1}\n')

        framework = self.core / "Sources/CatbirdMLSFFI.xcframework"
        libraries = []
        for identifier, platform, variant, architectures in [
            ("ios-arm64", "ios", None, ["arm64"]),
            ("ios-arm64_x86_64-simulator", "ios", "simulator", ["arm64", "x86_64"]),
            ("ios-arm64_x86_64-maccatalyst", "ios", "maccatalyst", ["arm64", "x86_64"]),
            ("macos-arm64_x86_64", "macos", None, ["arm64", "x86_64"]),
        ]:
            folder = framework / identifier
            folder.mkdir(parents=True)
            (folder / "libCatbirdMLSFFI.a").write_bytes(b"archive")
            write(folder / "Headers/CatbirdMLSFFI.h", "void ffi(void);\n")
            write(folder / "Headers/module.modulemap", "module CatbirdMLSFFI {}\n")
            item = {
                "LibraryIdentifier": identifier,
                "LibraryPath": "libCatbirdMLSFFI.a",
                "HeadersPath": "Headers",
                "SupportedArchitectures": architectures,
                "SupportedPlatform": platform,
            }
            if variant:
                item["SupportedPlatformVariant"] = variant
            libraries.append(item)
        with (framework / "Info.plist").open("wb") as handle:
            plistlib.dump({"AvailableLibraries": libraries}, handle, sort_keys=True)

        self.selected = self.root / "selected-inputs.json"
        self.selected.write_text(json.dumps({"repos": []}) + "\n")
        self.identities = {
            name: {"jj_commit": f"{name}-commit", "jj_change": f"{name}-change"}
            for name in ("catbird-mls", "catbird-atproto", "Petrel", "PetrelCatbird")
        }
        self.refresh_marker()

    def refresh_marker(self):
        write(self.core / "Sources/CatbirdMLSFFI.xcframework/labyrinth-space-protocol.json", json.dumps({
            "protocolVersion": 20261023,
            "features": ["labyrinth"],
            "artifactScope": "apple-all-targets",
            "generatedSwiftSHA256": self.module.sha256_file(self.core / "Sources/CatbirdMLSSpaces/CatbirdMLS.swift"),
            "sourceFingerprint": self.module.source_bundle_fingerprint(self.native, [self.atproto]),
        }))

    def tearDown(self):
        self.temporary.cleanup()

    def build(self, framework=None):
        return self.module.build_manifest(
            core=self.core,
            native=self.native,
            atproto=self.atproto,
            petrel=self.petrel,
            petrel_catbird=self.petrel_catbird,
            selected_inputs=self.selected,
            identities=self.identities,
            qualification="unreviewed-candidate",
            framework=framework,
            toolchains={"rustc": "rustc fixture", "swift": "swift fixture"},
        )

    def test_build_records_exact_inputs_interfaces_and_architectures(self):
        manifest = self.build()

        self.assertEqual(manifest["qualification"], "unreviewed-candidate")
        self.assertEqual(
            manifest["source"]["repositories"]["catbird-mls"]["path"],
            str(self.native.resolve()),
        )
        self.assertEqual(
            manifest["framework"]["libraries"][0]["architectures"], ["arm64"]
        )
        self.assertEqual(manifest["build"]["explicit_features"], ["labyrinth"])
        self.assertEqual(manifest["build"]["default_features"], True)
        self.assertEqual(len(manifest["identity"]["canonical_sha256"]), 64)

    def commit_fixture_repositories(self):
        """Give each recorded source a Git commit, as a fresh checkout has."""
        for directory in (self.native, self.atproto, self.petrel, self.petrel_catbird):
            git = ("git", "-C", str(directory), "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid")
            subprocess.run((*git, "init", "--quiet"), check=True)
            write(directory / ".gitignore", "build/\ntarget/\n")
            write(directory / "README", "fixture\n")
            subprocess.run((*git, "add", "--all"), check=True)
            subprocess.run((*git, "commit", "--quiet", "--message", "fixture"), check=True)

    def relative_manifest(self):
        self.commit_fixture_repositories()
        directory = self.core / "DevelopmentIntegration"
        manifest = self.module.build_manifest(
            core=self.core,
            native=self.native,
            atproto=self.atproto,
            petrel=self.petrel,
            petrel_catbird=self.petrel_catbird,
            selected_inputs=self.selected,
            qualification="reviewed-source",
            toolchains={"rustc": "rustc fixture", "swift": "swift fixture"},
            manifest_dir=directory,
        )
        path = directory / "manifest.json"
        self.module.write_manifest(path, manifest)
        return manifest, path

    def test_relative_manifest_records_published_commits_and_no_absolute_paths(self):
        manifest, path = self.relative_manifest()

        self.assertEqual(manifest["schema_version"], 2)
        self.assertNotIn(str(self.root), path.read_text())
        self.assertNotIn(str(self.root.resolve()), path.read_text())
        record = manifest["source"]["repositories"]["catbird-mls"]
        self.assertEqual(record["path"], "../../catbird-mls")
        head = subprocess.run(
            ("git", "-C", str(self.native), "rev-parse", "HEAD"),
            check=True, text=True, capture_output=True,
        ).stdout.strip()
        self.assertEqual(record, {"path": "../../catbird-mls", "commit": head})
        self.assertEqual(manifest["package"]["path"], "..")
        self.assertEqual(manifest["framework"]["path"], "../Sources/CatbirdMLSFFI.xcframework")
        self.module.verify_manifest(path)

    def test_relative_manifest_verifies_in_a_relocated_checkout(self):
        _, path = self.relative_manifest()
        relocated = Path(self.temporary.name + "-relocated")
        self.addCleanup(shutil.rmtree, relocated, ignore_errors=True)
        shutil.copytree(self.root, relocated, symlinks=True)

        self.module.verify_manifest(relocated / path.relative_to(self.root))

    def test_relative_manifest_rejects_uncommitted_or_moved_source(self):
        _, path = self.relative_manifest()
        write(self.petrel / "README", "edited\n")
        with self.assertRaisesRegex(RuntimeError, "modified tracked files"):
            self.module.verify_manifest(path)

        subprocess.run(
            ("git", "-C", str(self.petrel), "-c", "user.name=fixture",
             "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "--all", "--message", "moved"),
            check=True,
        )
        with self.assertRaisesRegex(RuntimeError, "Petrel source commit"):
            self.module.verify_manifest(path)

    def test_commit_identity_requires_a_checkout(self):
        with self.assertRaisesRegex(RuntimeError, "neither a JJ nor a Git checkout"):
            self.module.source_commit_identity(self.petrel)

    def test_partial_matrix_cannot_claim_full_release_scope(self):
        framework = self.core / "Sources/CatbirdMLSFFI.xcframework"
        with (framework / "Info.plist").open("rb") as handle:
            value = plistlib.load(handle)
        value["AvailableLibraries"] = value["AvailableLibraries"][:1]
        with (framework / "Info.plist").open("wb") as handle:
            plistlib.dump(value, handle)
        with self.assertRaisesRegex(RuntimeError, "architecture matrix"):
            self.build()

    def test_simulator_artifact_has_distinct_scope_and_target_list(self):
        full = self.core / "Sources/CatbirdMLSFFI.xcframework"
        framework = self.core / "Sources/CatbirdMLSFFISimulator.xcframework"
        full.rename(framework)
        with (framework / "Info.plist").open("rb") as handle:
            value = plistlib.load(handle)
        simulator = next(item for item in value["AvailableLibraries"]
                         if item.get("SupportedPlatformVariant") == "simulator")
        simulator["SupportedArchitectures"] = ["arm64"]
        value["AvailableLibraries"] = [simulator]
        with (framework / "Info.plist").open("wb") as handle:
            plistlib.dump(value, handle)
        marker_path = framework / "labyrinth-space-protocol.json"
        marker = json.loads(marker_path.read_text())
        marker["artifactScope"] = "ios-arm64-simulator-only"
        marker_path.write_text(json.dumps(marker))
        manifest = self.build(framework)
        self.assertEqual(manifest["scope"], "apple-native-uniffi-simulator-only")
        self.assertEqual(manifest["build"]["apple_targets"], ["aarch64-apple-ios-sim"])
        path = self.root / "simulator-manifest.json"
        self.module.write_manifest(path, manifest)
        self.module.verify_manifest(path)

    def test_verify_rejects_changed_native_source(self):
        manifest = self.build()
        path = self.root / "manifest.json"
        self.module.write_manifest(path, manifest)
        write(self.native / "src/lib.rs", "pub fn changed() {}\n")

        with self.assertRaisesRegex(RuntimeError, "native source fingerprint"):
            self.module.verify_manifest(path)

    def test_verify_rejects_changed_embedded_contract_without_rust_changes(self):
        contract = self.native / "src/orchestrator/generated/blue.catbird.chat.defs.json"
        write(contract, '{"authorityDid":{"const":"did:web:chat.catbird.blue"}}\n')
        self.refresh_marker()
        manifest = self.build()
        path = self.root / "manifest.json"
        self.module.write_manifest(path, manifest)
        write(contract, '{"authorityDid":{"ref":"#bareDid"}}\n')

        with self.assertRaisesRegex(RuntimeError, "native source fingerprint"):
            self.module.verify_manifest(path)

    def test_source_fingerprint_includes_native_build_and_generator_scripts(self):
        for relative in ("create-xcframework.sh", "scripts/generate_chat_schema.py"):
            with self.subTest(relative=relative):
                script = self.native / relative
                write(script, "original\n")
                before = self.module.native_source_fingerprint(self.native)
                write(script, "changed\n")

                self.assertNotEqual(before, self.module.native_source_fingerprint(self.native))

    def test_verify_rejects_changed_framework_library(self):
        manifest = self.build()
        path = self.root / "manifest.json"
        self.module.write_manifest(path, manifest)
        (self.core / "Sources/CatbirdMLSFFI.xcframework/ios-arm64/libCatbirdMLSFFI.a").write_bytes(
            b"different"
        )

        with self.assertRaisesRegex(RuntimeError, "framework file map"):
            self.module.verify_manifest(path)

    def test_verify_rejects_changed_local_path_dependency(self):
        manifest = self.build()
        path = self.root / "manifest.json"
        self.module.write_manifest(path, manifest)
        write(self.atproto / "src/lib.rs", "pub fn changed_contract() {}\n")

        with self.assertRaisesRegex(RuntimeError, "catbird-atproto source fingerprint"):
            self.module.verify_manifest(path)

    def test_build_rejects_binding_replaced_before_manifest_creation(self):
        write(self.core / "Sources/CatbirdMLSSpaces/CatbirdMLS.swift", "public struct DifferentNativeMetadata {}\n")
        with self.assertRaisesRegex(RuntimeError, "generated Swift binding"):
            self.build()

    def test_build_rejects_marker_from_an_older_native_source(self):
        write(self.native / "src/lib.rs", "pub fn newer_source() {}\n")
        with self.assertRaisesRegex(RuntimeError, "rebuild marker"):
            self.build()

    def test_verify_rejects_changed_labyrinth_contract(self):
        manifest = self.build()
        path = self.root / "manifest.json"
        self.module.write_manifest(path, manifest)
        write(self.petrel_catbird / "lexicons/blue/catbird/labyrinth/blue.catbird.labyrinth.defs.json", '{"lexicon":2}\n')
        with self.assertRaisesRegex(RuntimeError, "schema file map"):
            self.module.verify_manifest(path)

    def test_verify_rejects_tampered_manifest_identity(self):
        manifest = self.build()
        manifest["build"]["cargo_profile"] = "debug"
        path = self.root / "manifest.json"
        path.write_text(json.dumps(manifest) + "\n")

        with self.assertRaisesRegex(RuntimeError, "canonical manifest identity"):
            self.module.verify_manifest(path)

    def test_source_fingerprint_excludes_build_outputs(self):
        before = self.module.native_source_fingerprint(self.native)
        write(self.native / "target/aarch64-apple-ios/release/output", "derived\n")
        write(self.native / "build/bindings/CatbirdMLS.swift", "derived\n")
        write(self.native / "_trash/previous-contract.json", "{}\n")
        write(self.native / "build/contract.json", "{}\n")

        self.assertEqual(before, self.module.native_source_fingerprint(self.native))

    def test_source_bundle_fingerprint_includes_local_path_dependencies(self):
        before = self.module.source_bundle_fingerprint(self.native, [self.atproto])
        write(self.atproto / "src/lib.rs", "pub fn changed_contract() {}\n")

        self.assertNotEqual(
            before,
            self.module.source_bundle_fingerprint(self.native, [self.atproto]),
        )


if __name__ == "__main__":
    unittest.main()
