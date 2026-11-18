"""Exercise SwiftPM's actual binary selection before it can fetch an old release."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class PackageFFISelection(unittest.TestCase):
    def test_only_exact_artifact_is_selected(self):
        with tempfile.TemporaryDirectory() as temp:
            core = Path(temp) / "CatbirdMLSCore"
            core.mkdir()
            shutil.copyfile(ROOT / "Package.swift", core / "Package.swift")
            binding = core / "Sources/CatbirdMLS/CatbirdMLS.swift"
            binding.parent.mkdir(parents=True)
            binding.write_bytes(b"fixture binding")
            binding_digest = hashlib.sha256(binding.read_bytes()).hexdigest()
            framework = core / "Sources/CatbirdMLSFFI.xcframework"
            library = framework / "ios-arm64/libCatbirdMLSFFI.a"
            library.parent.mkdir(parents=True)
            library.write_bytes(b"qualified library")
            marker = framework / "labyrinth-space-protocol.json"
            marker.write_text(json.dumps({
                "protocolVersion": 20261023, "features": ["labyrinth"], "sourceFingerprint": "1" * 64,
                "artifactScope": "apple-all-targets", "generatedSwiftSHA256": binding_digest,
                "librarySHA256": {"ios-arm64/libCatbirdMLSFFI.a": hashlib.sha256(library.read_bytes()).hexdigest()}
            }))
            receipt = core / "DevelopmentIntegration/spaces-apple-artifact-manifest.json"
            receipt.parent.mkdir()
            selected = receipt.with_name("spaces-selected-inputs.json")
            selected.write_text("fixture source revision")
            receipt.write_text(json.dumps({
                "selected_inputs": {"sha256": hashlib.sha256(selected.read_bytes()).hexdigest()},
                "source": {"bundle_sha256": "1" * 64},
                "framework": {"files": {str(p.relative_to(framework)): {"sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                                        for p in (library, marker)}}
            }))
            def dump(force_remote=False):
                env = dict(os.environ)
                env.pop("CATBIRD_MLS_FFI_PATH", None)
                env["CATBIRD_MLS_LOCAL_INTEGRATION"] = "0" if force_remote else "1"
                return subprocess.run(["swift", "package", "--manifest-cache", "none", "dump-package"],
                                      cwd=core, env=env, capture_output=True, text=True, timeout=60)
            initial = dump()
            self.assertEqual(initial.returncode, 0, initial.stderr)
            targets = json.loads(initial.stdout)["targets"]
            self.assertEqual(next(t["path"] for t in targets if t["name"] == "CatbirdMLSFFI"),
                             "Sources/CatbirdMLSFFI.xcframework")
            selected.write_text("a subsequent native build")
            stale = dump()
            self.assertNotEqual(stale.returncode, 0)
            self.assertIn("Local XCFramework does not match", stale.stderr)
            selected.write_text("fixture source revision")
            library.write_bytes(b"different library")
            mismatch = dump()
            self.assertNotEqual(mismatch.returncode, 0)
            self.assertIn("Local XCFramework does not match", mismatch.stderr)
            published = core / "Scripts/ffi-release-inputs.json"
            published.parent.mkdir()
            shutil.copyfile(ROOT / "Scripts/ffi-release-inputs.json", published)
            missing_release = dump(force_remote=True)
            self.assertNotEqual(missing_release.returncode, 0)
            self.assertIn("No published CatbirdMLSFFI release matches", missing_release.stderr)


if __name__ == "__main__":
    unittest.main()
