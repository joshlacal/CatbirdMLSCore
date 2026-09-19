import importlib.util
import json
from pathlib import Path
import plistlib
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
        write(self.core / "Package.swift", "// fixture\n")
        write(self.core / "Sources/CatbirdMLS/CatbirdMLS.swift", "public struct Generated {}\n")
        schema = self.petrel_catbird / "lexicons/blue/catbird/chat/blue.catbird.chat.prepareDirectRequest.json"
        write(schema, '{"lexicon":1}\n')

        framework = self.core / "Sources/CatbirdMLSFFI.xcframework"
        library = framework / "ios-arm64/libCatbirdMLSFFI.a"
        header = framework / "ios-arm64/Headers/CatbirdMLSFFI.h"
        modulemap = framework / "ios-arm64/Headers/module.modulemap"
        library.parent.mkdir(parents=True, exist_ok=True)
        library.write_bytes(b"archive")
        write(header, "void ffi(void);\n")
        write(modulemap, "module CatbirdMLSFFI {}\n")
        with (framework / "Info.plist").open("wb") as handle:
            plistlib.dump(
                {
                    "AvailableLibraries": [
                        {
                            "LibraryIdentifier": "ios-arm64",
                            "LibraryPath": "libCatbirdMLSFFI.a",
                            "HeadersPath": "Headers",
                            "SupportedArchitectures": ["arm64"],
                            "SupportedPlatform": "ios",
                        }
                    ]
                },
                handle,
                sort_keys=True,
            )

        self.selected = self.root / "selected-inputs.json"
        self.selected.write_text(json.dumps({"repos": []}) + "\n")
        self.identities = {
            name: {"jj_commit": f"{name}-commit", "jj_change": f"{name}-change"}
            for name in ("catbird-mls", "catbird-atproto", "Petrel", "PetrelCatbird")
        }

    def tearDown(self):
        self.temporary.cleanup()

    def build(self):
        return self.module.build_manifest(
            core=self.core,
            native=self.native,
            atproto=self.atproto,
            petrel=self.petrel,
            petrel_catbird=self.petrel_catbird,
            selected_inputs=self.selected,
            identities=self.identities,
            qualification="unreviewed-candidate",
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
        self.assertEqual(manifest["build"]["explicit_features"], [])
        self.assertEqual(manifest["build"]["default_features"], True)
        self.assertEqual(len(manifest["identity"]["canonical_sha256"]), 64)

    def test_verify_rejects_changed_native_source(self):
        manifest = self.build()
        path = self.root / "manifest.json"
        self.module.write_manifest(path, manifest)
        write(self.native / "src/lib.rs", "pub fn changed() {}\n")

        with self.assertRaisesRegex(RuntimeError, "native source fingerprint"):
            self.module.verify_manifest(path)

    def test_verify_rejects_changed_framework_library(self):
        manifest = self.build()
        path = self.root / "manifest.json"
        self.module.write_manifest(path, manifest)
        (self.core / "Sources/CatbirdMLSFFI.xcframework/ios-arm64/libCatbirdMLSFFI.a").write_bytes(
            b"different"
        )

        with self.assertRaisesRegex(RuntimeError, "framework file map"):
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

        self.assertEqual(before, self.module.native_source_fingerprint(self.native))


if __name__ == "__main__":
    unittest.main()
