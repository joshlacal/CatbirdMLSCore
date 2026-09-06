#!/usr/bin/env python3
"""Guard paired FFI release inputs and emit artifact provenance."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import subprocess

REPOSITORIES = {
    'catbird-mls': 'CATBIRD_MLS_REF', 'catbird-atproto': 'CATBIRD_ATPROTO_REF',
    'Petrel': 'PETREL_REF', 'PetrelCatbird': 'PETREL_CATBIRD_REF',
}
MATRIX = {
    ('ios', ''): {'arm64'}, ('ios', 'simulator'): {'arm64', 'x86_64'},
    ('ios', 'maccatalyst'): {'arm64', 'x86_64'}, ('macos', ''): {'arm64', 'x86_64'},
}
PAIRED_FILES = ('CatbirdMLS.swift', 'CatbirdMLSFFI.h', 'CatbirdMLSFFI.modulemap')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def run(*args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def files(root):
    return {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob('*')) if p.is_file()}


def dependency_pin(manifest, repository):
    pattern = rf'url:\s*"https://github\.com/joshlacal/{repository}\.git",\s*revision:\s*"([0-9a-f]{{40}})"'
    matches = re.findall(pattern, manifest)
    require(len(matches) == 1, f'Expected one immutable {repository} dependency')
    return matches[0]


def local_manifest(manifest):
    patched, count = re.subn(
        r'\.binaryTarget\(\s*name: "CatbirdMLSFFI",\s*url: "[^"]+",\s*checksum: "[0-9a-f]{64}"\s*\)',
        '.binaryTarget(name: "CatbirdMLSFFI", path: "Sources/CatbirdMLSFFI.xcframework")', manifest,
    )
    require(count == 1, 'Expected one public URL binary target')
    return patched


def validate_matrix(info):
    entries = info.get('AvailableLibraries', [])
    require(len(entries) == len(MATRIX), 'Expected exactly four XCFramework library variants')
    actual = {}
    for entry in entries:
        key = (entry['SupportedPlatform'], entry.get('SupportedPlatformVariant', ''))
        require(key not in actual, f'Duplicate library variant: {key}')
        actual[key] = set(entry['SupportedArchitectures'])
    require(actual == MATRIX, f'Incomplete XCFramework platform matrix: {actual}')
    return entries


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def preflight(root):
    release = os.environ['RELEASE_TAG']
    require(re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', release), 'Use a new versioned release tag')
    sources = {}
    for name, env_name in {'CatbirdMLSCore': 'GITHUB_SHA', **REPOSITORIES}.items():
        expected = os.environ[env_name]
        require(re.fullmatch(r'[0-9a-f]{40}', expected), f'{env_name} must be a full commit SHA')
        repo = root if name == 'CatbirdMLSCore' else root.parent / name
        actual = run('git', 'rev-parse', 'HEAD', cwd=repo)
        require(actual == expected, f'{name} checkout differs from requested revision')
        require(not run('git', 'status', '--porcelain', '--untracked-files=no', cwd=repo), f'Dirty tracked input in {name}')
        sources[name] = actual
    manifest = (root / 'Package.swift').read_text()
    local_manifest(manifest)
    for name in ('Petrel', 'PetrelCatbird'):
        require(dependency_pin(manifest, name) == sources[name], f'Core {name} pin differs from workflow input')
    sdk_manifest = (root.parent / 'PetrelCatbird/Package.swift').read_text()
    require(dependency_pin(sdk_manifest, 'Petrel') == sources['Petrel'], 'SDK Petrel pin differs from Core')
    support = root / 'release-support'
    support.mkdir(exist_ok=False)
    save(support / 'inputs.json', {
        'sources': sources, 'release_tag': release,
        'committed_swift_sha256': sha(root / 'Sources/CatbirdMLS/CatbirdMLS.swift'),
        'package_manifest_sha256': sha(root / 'Package.swift'),
        'cargo_lock_sha256': sha(root.parent / 'catbird-mls/Cargo.lock'),
        'workflow_sha256': sha(root / '.github/workflows/build-ffi.yml'),
        'rebuild_script_sha256': sha(root / 'Scripts/rebuild-ffi.sh'),
        'native_build_script_sha256': sha(root.parent / 'catbird-mls/create-xcframework.sh'),
    })


def verify(root):
    support = root / 'release-support'
    inputs = json.loads((support / 'inputs.json').read_text())
    native = root.parent / 'catbird-mls'
    require(sha(native / 'Cargo.lock') == inputs['cargo_lock_sha256'], 'Native build changed Cargo.lock')
    require(sha(root / 'Package.swift') == inputs['package_manifest_sha256'], 'Package.swift changed during native build')
    binding = native / 'build/bindings/CatbirdMLS.swift'
    require(sha(binding) == inputs['committed_swift_sha256'], 'Generated Swift differs from committed paired bindings')
    require(sha(root / 'Sources/CatbirdMLS/CatbirdMLS.swift') == inputs['committed_swift_sha256'], 'Committed Swift was replaced')
    framework = root / 'Sources/CatbirdMLSFFI.xcframework'
    with (framework / 'Info.plist').open('rb') as stream:
        entries = validate_matrix(plistlib.load(stream))
    for entry in entries:
        variant = framework / entry['LibraryIdentifier']
        library = variant / entry['LibraryPath']
        require(set(run('lipo', '-archs', str(library)).split()) == set(entry['SupportedArchitectures']), 'Library architectures differ from plist')
        headers = variant / entry['HeadersPath']
        require(sha(headers / 'CatbirdMLSFFI.h') == sha(native / 'build/bindings/CatbirdMLSFFI.h'), 'Unpaired FFI header')
        require(sha(headers / 'module.modulemap') == sha(native / 'build/bindings/CatbirdMLSFFI.modulemap'), 'Unpaired FFI module map')
    for name in PAIRED_FILES:
        shutil.copyfile(native / 'build/bindings' / name, support / name)
    save(support / 'framework-verification.json', {
        'libraries': entries, 'framework_sha256': files(framework),
        'paired_files_sha256': {name: sha(support / name) for name in PAIRED_FILES},
    })


def provenance(root):
    support = root / 'release-support'
    inputs = json.loads((support / 'inputs.json').read_text())
    require(sha(root / 'Package.swift') == inputs['package_manifest_sha256'], 'Verification did not restore public Package.swift')
    verification = json.loads((support / 'framework-verification.json').read_text())
    require(files(root / 'Sources/CatbirdMLSFFI.xcframework') == verification['framework_sha256'], 'Framework changed during Swift verification')
    require({name: sha(support / name) for name in PAIRED_FILES} == verification['paired_files_sha256'], 'Paired release files changed during Swift verification')
    require(sha(root / 'Sources/CatbirdMLS/CatbirdMLS.swift') == inputs['committed_swift_sha256'], 'Swift bindings changed during package verification')
    resolved = {}
    for name, directory in (('CatbirdMLSCore', root), ('PetrelCatbird', root.parent / 'PetrelCatbird')):
        path = directory / 'Package.resolved'
        require(path.is_file(), f'{name} Package.resolved missing after verification')
        data = json.loads(path.read_text())
        require(isinstance(data.get('pins'), list) and data['pins'], f'{name} resolved dependency pins missing')
        destination = support / (name + '.Package.resolved')
        shutil.copyfile(path, destination)
        resolved[name] = {'sha256': sha(destination), 'pins': data['pins']}
    asset = root / os.environ['ASSET_NAME']
    require((root / (asset.name + '.sha256')).read_text().split()[0] == sha(asset), 'Archive checksum mismatch')
    save(support / 'ffi-provenance.json', {
        'inputs': inputs,
        'verification': verification,
        'resolved_swift_dependencies': resolved,
        'archive': {'name': asset.name, 'sha256': sha(asset), 'size_bytes': asset.stat().st_size},
        'toolchain': {name: run(*command) for name, command in {
            'rustc': ('rustc', '--version'), 'cargo': ('cargo', '--version'),
            'swift': ('swift', '--version'), 'xcode': ('xcodebuild', '-version'),
        }.items()},
        'workflow_run': {name: os.environ[name] for name in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_SHA')},
        'scope': 'Seven architectures across four Apple variants; generated Swift equals committed source. Xcode toolchain is recorded, not a claim of byte-identical reproduction of prior local binaries.',
    })


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('preflight', 'verify', 'local-manifest', 'provenance'))
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    if args.phase == 'local-manifest':
        path = root / 'Package.swift'
        path.write_text(local_manifest(path.read_text()))
    else:
        {'preflight': preflight, 'verify': verify, 'provenance': provenance}[args.phase](root)
