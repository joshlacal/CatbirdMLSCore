import importlib.util
import json
import os
from pathlib import Path
import plistlib
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('ffi_release', Path(__file__).parents[1] / 'ffi-release.py')
ffi = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ffi)


class ReleaseGuards(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.parent = Path(self.temp.name)
        self.root = self.parent / 'CatbirdMLSCore'
        self.root.mkdir()
        self.revisions = {name: str(i) * 40 for i, name in enumerate(['CatbirdMLSCore', *ffi.REPOSITORIES], 1)}
        self.env = {'RELEASE_TAG': 'v1.5.18', 'GITHUB_SHA': self.revisions['CatbirdMLSCore']}
        self.env.update({env: self.revisions[name] for name, env in ffi.REPOSITORIES.items()})
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, self.env).start()
        def vcs(*args, cwd=None):
            if args[:2] == ('git', 'rev-parse'):
                return self.revisions[cwd.name]
            if args[:2] == ('git', 'status'):
                return ''
            if args[:2] == ('lipo', '-archs'):
                return 'arm64' if '/ios-arm64/' in args[2] else 'arm64 x86_64'
            raise AssertionError(args)
        patch.object(ffi, 'run', side_effect=vcs).start()
        for name in ffi.REPOSITORIES:
            (self.parent / name).mkdir()
        self.write(self.root / 'Package.swift', self.manifest())
        self.write(self.parent / 'PetrelCatbird/Package.swift', self.dependency('Petrel'))
        self.write(self.root / 'Sources/CatbirdMLS/CatbirdMLS.swift', 'reviewed Swift')
        self.write(self.root / '.github/workflows/build-ffi.yml', 'workflow')
        self.write(self.root / 'Scripts/rebuild-ffi.sh', 'rebuild')
        self.write(self.parent / 'catbird-mls/Cargo.lock', 'locked')
        self.write(self.parent / 'catbird-mls/create-xcframework.sh', 'native build')

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)

    def dependency(self, name):
        return f'.package(url: "https://github.com/joshlacal/{name}.git", revision: "{self.revisions[name]}")'

    def manifest(self):
        return '\n'.join([self.dependency('Petrel'), self.dependency('PetrelCatbird'),
            '.binaryTarget(name: "CatbirdMLSFFI", url: "https://example.invalid/prior.zip", checksum: "' + 'a' * 64 + '")'])

    def framework(self):
        native = self.parent / 'catbird-mls/build/bindings'
        for name, content in zip(ffi.PAIRED_FILES, ('reviewed Swift', 'header', 'module')):
            self.write(native / name, content)
        framework = self.root / 'Sources/CatbirdMLSFFI.xcframework'
        entries = []
        for (platform, variant), archs in ffi.MATRIX.items():
            identifier = platform + '-arm64' + ('-' + variant if variant else '')
            entries.append({'SupportedPlatform': platform, 'SupportedPlatformVariant': variant,
                            'SupportedArchitectures': sorted(archs), 'LibraryIdentifier': identifier,
                            'LibraryPath': 'libCatbirdMLSFFI.a', 'HeadersPath': 'Headers'})
            self.write(framework / identifier / 'libCatbirdMLSFFI.a', 'native')
            self.write(framework / identifier / 'Headers/CatbirdMLSFFI.h', 'header')
            self.write(framework / identifier / 'Headers/module.modulemap', 'module')
        (framework / 'Info.plist').write_bytes(plistlib.dumps({'AvailableLibraries': entries}))
        return framework

    def test_complete_pair_and_matrix(self):
        ffi.preflight(self.root)
        self.framework()
        ffi.verify(self.root)
        proof = json.loads((self.root / 'release-support/framework-verification.json').read_text())
        self.assertEqual(len(proof['libraries']), 4)
        self.assertEqual(len(proof['paired_files_sha256']), 3)

    def test_rejects_moving_ref(self):
        with patch.dict(os.environ, {'CATBIRD_MLS_REF': 'main'}):
            with self.assertRaisesRegex(ValueError, 'full commit SHA'):
                ffi.preflight(self.root)
        self.assertFalse((self.root / 'release-support').exists())

    def test_rejects_sdk_dependency_mismatch(self):
        self.write(self.parent / 'PetrelCatbird/Package.swift', self.dependency('Petrel').replace(self.revisions['Petrel'], 'f' * 40))
        with self.assertRaisesRegex(ValueError, 'SDK Petrel pin differs'):
            ffi.preflight(self.root)

    def test_rejects_local_only_manifest(self):
        self.write(self.root / 'Package.swift', ffi.local_manifest(self.manifest()))
        with self.assertRaisesRegex(ValueError, 'public URL'):
            ffi.preflight(self.root)

    def test_rejects_three_arm64_cached_variants(self):
        info = {'AvailableLibraries': [{'SupportedPlatform': platform, 'SupportedPlatformVariant': variant,
               'SupportedArchitectures': ['arm64']} for platform, variant in [('ios', ''), ('ios', 'simulator'), ('macos', '')]]}
        with self.assertRaisesRegex(ValueError, 'four XCFramework'):
            ffi.validate_matrix(info)

    def test_rejects_generated_binding_mismatch(self):
        ffi.preflight(self.root)
        self.framework()
        self.write(self.parent / 'catbird-mls/build/bindings/CatbirdMLS.swift', 'different ABI')
        with self.assertRaisesRegex(ValueError, 'Generated Swift differs'):
            ffi.verify(self.root)

    def test_rejects_changed_lock(self):
        ffi.preflight(self.root)
        self.framework()
        self.write(self.parent / 'catbird-mls/Cargo.lock', 'changed resolution')
        with self.assertRaisesRegex(ValueError, 'changed Cargo.lock'):
            ffi.verify(self.root)

    def test_rejects_unpaired_header(self):
        ffi.preflight(self.root)
        framework = self.framework()
        self.write(framework / 'ios-arm64/Headers/CatbirdMLSFFI.h', 'different ABI')
        with self.assertRaisesRegex(ValueError, 'Unpaired FFI header'):
            ffi.verify(self.root)

    def test_public_manifest_rewrite_preserves_dependency_pins(self):
        value = ffi.local_manifest(self.manifest())
        self.assertEqual(ffi.dependency_pin(value, 'PetrelCatbird'), self.revisions['PetrelCatbird'])
        self.assertIn('path: "Sources/CatbirdMLSFFI.xcframework"', value)

    def test_provenance_rejects_framework_mutation_after_build(self):
        ffi.preflight(self.root)
        framework = self.framework()
        ffi.verify(self.root)
        self.write(framework / 'ios-arm64/libCatbirdMLSFFI.a', 'changed after verification')
        with self.assertRaisesRegex(ValueError, 'Framework changed'):
            ffi.provenance(self.root)

    def test_provenance_rejects_paired_asset_mutation(self):
        ffi.preflight(self.root)
        self.framework()
        ffi.verify(self.root)
        self.write(self.root / 'release-support/CatbirdMLSFFI.h', 'changed after verification')
        with self.assertRaisesRegex(ValueError, 'Paired release files changed'):
            ffi.provenance(self.root)

    def test_provenance_captures_actual_transitive_resolution(self):
        ffi.preflight(self.root)
        self.framework()
        ffi.verify(self.root)
        pins = {'pins': [{'identity': 'grdb.swift', 'state': {'revision': 'b' * 40}}]}
        for directory in (self.root, self.parent / 'PetrelCatbird'):
            self.write(directory / 'Package.resolved', json.dumps(pins))
        asset = self.root / 'test.zip'
        self.write(asset, 'archive bytes')
        self.write(self.root / 'test.zip.sha256', ffi.sha(asset) + '  test.zip\n')
        with patch.dict(os.environ, {'ASSET_NAME': 'test.zip', 'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}), patch.object(ffi, 'run', return_value='recorded version'):
            ffi.provenance(self.root)
        proof = json.loads((self.root / 'release-support/ffi-provenance.json').read_text())
        self.assertEqual(proof['resolved_swift_dependencies']['CatbirdMLSCore']['pins'], pins['pins'])
        self.assertEqual(proof['archive']['sha256'], ffi.sha(asset))


if __name__ == '__main__':
    unittest.main()
