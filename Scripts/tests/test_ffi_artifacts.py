import copy
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

spec = importlib.util.spec_from_file_location('ffi_artifacts', Path(__file__).parents[1] / 'ffi-artifacts.py')
artifact_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(artifact_module)
a = artifact_module


class ArtifactGuards(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.workspace = Path(self.temp.name)
        self.expected = {'source_run_id': 123, 'source_run_attempt': 1, 'release_tag': 'v1.5.18', 'artifact_name': 'catbird-mls-ffi',
                         'sources': {name: str(i) * 40 for i, name in enumerate(['CatbirdMLSCore', 'catbird-mls', 'catbird-atproto', 'Petrel', 'PetrelCatbird'], 1)}}
        self.run = {'id': 123, 'head_sha': '1' * 40, 'run_attempt': 1, 'status': 'completed', 'conclusion': 'success', 'path': '.github/workflows/build-ffi.yml'}
        self.artifact = {'id': 456, 'name': 'catbird-mls-ffi', 'expired': False, 'workflow_run': {'id': 123, 'head_sha': '1' * 40}}

    def digest(self, data):
        return hashlib.sha256(data).hexdigest()

    def zip_bytes(self, files):
        data = io.BytesIO()
        with zipfile.ZipFile(data, 'w') as z:
            for name, body in files.items():
                z.writestr(name, body)
        return data.getvalue()

    def build_fixture(self, mutate=None):
        framework = {'Info.plist': b'plist fixture'}
        paired = {'CatbirdMLS.swift': b'Swift fixture', 'CatbirdMLSFFI.h': b'header fixture', 'CatbirdMLSFFI.modulemap': b'module fixture'}
        binary = self.zip_bytes({'CatbirdMLSFFI.xcframework/' + k: v for k, v in framework.items()})
        resolved = json.dumps({'pins': [{'identity': 'dependency', 'state': {'revision': 'f' * 40}}]}).encode()
        libraries = [{'SupportedPlatform': p, 'SupportedPlatformVariant': v, 'SupportedArchitectures': sorted(archs)} for (p, v), archs in a.ffi.MATRIX.items()]
        proof = {'inputs': {'sources': self.expected['sources'], 'release_tag': 'v1.5.18', 'committed_swift_sha256': self.digest(paired['CatbirdMLS.swift'])},
                 'workflow_run': {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1', 'GITHUB_SHA': '1' * 40},
                 'archive': {'name': 'CatbirdMLSFFI.xcframework.zip', 'size_bytes': len(binary), 'sha256': self.digest(binary)},
                 'verification': {'libraries': libraries, 'framework_sha256': {k: self.digest(v) for k, v in framework.items()}, 'paired_files_sha256': {k: self.digest(v) for k, v in paired.items()}},
                 'resolved_swift_dependencies': {name: {'sha256': self.digest(resolved), 'pins': json.loads(resolved)['pins']} for name in ('CatbirdMLSCore', 'PetrelCatbird')}}
        if mutate:
            mutate(proof)
        files = {'CatbirdMLSFFI.xcframework.zip': binary, 'CatbirdMLSFFI.xcframework.zip.sha256': self.digest(binary) + '  CatbirdMLSFFI.xcframework.zip\n', 'release-support/ffi-provenance.json': json.dumps(proof)}
        files.update({'release-support/' + k: v for k, v in paired.items()})
        files.update({'release-support/' + name + '.Package.resolved': resolved for name in ('CatbirdMLSCore', 'PetrelCatbird')})
        outer = self.zip_bytes(files)
        (self.workspace / 'build-artifact.zip').write_bytes(outer)
        self.artifact['digest'] = 'sha256:' + self.digest(outer)
        return proof

    def verify(self):
        with contextlib.redirect_stdout(io.StringIO()):
            return a.verify(self.expected, self.run, self.artifact, self.workspace)

    def test_valid_pinned_artifact_emits_small_metadata(self):
        self.build_fixture()
        self.assertEqual(a.checked_run(self.expected, self.run, {'artifacts': [self.artifact]})['id'], 456)
        receipt = self.verify()
        self.assertEqual(receipt['artifact_id'], 456)
        self.assertFalse((self.workspace / 'metadata/CatbirdMLSFFI.xcframework.zip').exists())
        self.assertTrue((self.workspace / 'metadata/ffi-provenance.json').exists())

    def test_rejects_unsuccessful_or_wrong_run(self):
        self.build_fixture()
        for change in ({'conclusion': 'failure'}, {'head_sha': 'e' * 40}, {'run_attempt': 2}, {'path': 'other.yml'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                a.checked_run(self.expected, {**self.run, **change}, {'artifacts': [self.artifact]})

    def test_rejects_changed_outer_archive(self):
        self.build_fixture()
        with (self.workspace / 'build-artifact.zip').open('ab') as stream:
            stream.write(b'changed')
        with self.assertRaisesRegex(ValueError, 'artifact digest mismatch'):
            self.verify()

    def test_rejects_any_mismatched_source_revision(self):
        def mutate(proof):
            proof['inputs']['sources'] = {**proof['inputs']['sources'], 'catbird-atproto': 'e' * 40}
        self.build_fixture(mutate)
        with self.assertRaisesRegex(ValueError, 'source revisions mismatch'):
            self.verify()

    def test_rejects_wrong_framework_contents(self):
        self.build_fixture(lambda p: p['verification']['framework_sha256'].update({'Info.plist': 'f' * 64}))
        with self.assertRaisesRegex(ValueError, 'Archived framework differs'):
            self.verify()

    def test_rejects_archive_traversal(self):
        data = io.BytesIO(self.zip_bytes({'../escape': b'x'}))
        with zipfile.ZipFile(data) as z, self.assertRaisesRegex(ValueError, 'Unsafe archive path'):
            list(a.safe_members(z))

    def test_final_publication_requires_manifest_pair_and_existing_exact_tag(self):
        self.build_fixture()
        receipt = self.verify()
        root = self.workspace / 'final'
        binding = root / 'Sources/CatbirdMLS/CatbirdMLS.swift'
        binding.parent.mkdir(parents=True)
        binding.write_bytes(b'Swift fixture')
        manifest = '\n'.join([f'.package(url: "https://github.com/joshlacal/{name}.git", revision: "{self.expected["sources"][name]}")' for name in ('Petrel', 'PetrelCatbird')])
        manifest += '\n.binaryTarget(name: "CatbirdMLSFFI", url: "https://github.com/joshlacal/CatbirdMLSCore/releases/download/v1.5.18/CatbirdMLSFFI.xcframework.zip", checksum: "' + receipt['archive_sha256'] + '")'
        (root / 'Package.swift').write_text(manifest)
        final = 'a' * 40
        tag = {'ref': 'refs/tags/v1.5.18', 'object': {'type': 'commit', 'sha': final}}
        with patch.object(a.ffi, 'run', side_effect=[final, '']):
            a.publication(root, self.expected, receipt, final, tag)
        with self.assertRaisesRegex(ValueError, 'does not name final commit'):
            a.publication(root, self.expected, receipt, final, {**tag, 'object': {'type': 'commit', 'sha': 'b' * 40}})
        (root / 'Package.swift').write_text(manifest.replace(receipt['archive_sha256'], 'e' * 64))
        with patch.object(a.ffi, 'run', side_effect=[final, '']), self.assertRaisesRegex(ValueError, 'public binary target'):
            a.publication(root, self.expected, receipt, final, tag)


if __name__ == '__main__':
    unittest.main()
