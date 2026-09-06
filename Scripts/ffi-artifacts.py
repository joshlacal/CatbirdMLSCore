#!/usr/bin/env python3
"""Verify a pinned completed FFI run without rebuilding or publishing its source."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('ffi_release', ROOT / 'Scripts/ffi-release.py')
ffi = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ffi)


def load(path):
    return json.loads(path.read_text())


def checked_run(expected, run, artifacts):
    ffi.require(run['id'] == expected['source_run_id'], 'Unexpected source run')
    ffi.require(run['head_sha'] == expected['sources']['CatbirdMLSCore'], 'Unexpected source head')
    ffi.require(run['run_attempt'] == expected['source_run_attempt'], 'Unexpected run attempt')
    ffi.require(run['status'] == 'completed' and run['conclusion'] == 'success', 'Source run has not succeeded')
    ffi.require(run['path'] == '.github/workflows/build-ffi.yml', 'Unexpected source workflow')
    matches = [a for a in artifacts['artifacts'] if a['name'] == expected['artifact_name']]
    ffi.require(len(matches) == 1 and not matches[0]['expired'], 'Expected one unexpired build artifact')
    artifact = matches[0]
    ffi.require(re.fullmatch(r'sha256:[0-9a-f]{64}', artifact.get('digest', '')), 'Missing artifact digest')
    ffi.require(artifact['workflow_run']['id'] == expected['source_run_id'], 'Artifact belongs to another run')
    ffi.require(artifact['workflow_run']['head_sha'] == expected['sources']['CatbirdMLSCore'], 'Artifact head mismatch')
    return artifact


def safe_members(archive):
    seen = set()
    for entry in archive.infolist():
        path = PurePosixPath(entry.filename)
        ffi.require(not path.is_absolute() and '..' not in path.parts and '\\' not in entry.filename, 'Unsafe archive path')
        ffi.require(entry.filename not in seen, 'Duplicate archive entry')
        ffi.require(not stat.S_ISLNK(entry.external_attr >> 16), 'Archive symlink is not accepted')
        seen.add(entry.filename)
        yield entry


def hash_stream(stream):
    h = hashlib.sha256()
    for block in iter(lambda: stream.read(1024 * 1024), b''):
        h.update(block)
    return h.hexdigest()


def verify(expected, run, artifact, workspace):
    outer = workspace / 'build-artifact.zip'
    ffi.require(ffi.sha(outer) == artifact['digest'].removeprefix('sha256:'), 'Downloaded artifact digest mismatch')
    unpacked = workspace / 'unpacked'
    unpacked.mkdir(exist_ok=False)
    with zipfile.ZipFile(outer) as archive:
        entries = list(safe_members(archive))
        ffi.require(sum(e.file_size for e in entries) < 8 * 1024**3, 'Unexpected outer artifact size')
        for entry in entries:
            archive.extract(entry, unpacked)
    support = unpacked / 'release-support'
    proof = load(support / 'ffi-provenance.json')
    ffi.require(proof['inputs']['sources'] == expected['sources'], 'Provenance source revisions mismatch')
    ffi.require(proof['inputs']['release_tag'] == expected['release_tag'], 'Provenance release tag mismatch')
    ffi.require(proof['workflow_run']['GITHUB_RUN_ID'] == str(expected['source_run_id']), 'Provenance run mismatch')
    ffi.require(proof['workflow_run']['GITHUB_RUN_ATTEMPT'] == str(expected['source_run_attempt']), 'Provenance attempt mismatch')
    ffi.require(proof['workflow_run']['GITHUB_SHA'] == expected['sources']['CatbirdMLSCore'], 'Provenance Core head mismatch')
    ffi.require(proof['archive']['name'] == 'CatbirdMLSFFI.xcframework.zip', 'Unexpected binary archive name')
    binary = unpacked / proof['archive']['name']
    ffi.require(binary.stat().st_size == proof['archive']['size_bytes'], 'Binary archive size mismatch')
    archive_sha = ffi.sha(binary)
    ffi.require(archive_sha == proof['archive']['sha256'], 'Binary archive digest mismatch')
    ffi.require((unpacked / (binary.name + '.sha256')).read_text().split()[0] == archive_sha, 'Checksum file mismatch')
    verification = proof['verification']
    ffi.validate_matrix({'AvailableLibraries': verification['libraries']})
    ffi.require(set(verification['paired_files_sha256']) == set(ffi.PAIRED_FILES), 'Unexpected paired files')
    for name, expected_hash in verification['paired_files_sha256'].items():
        ffi.require(ffi.sha(support / name) == expected_hash, 'Paired file mismatch: ' + name)
    ffi.require(verification['paired_files_sha256']['CatbirdMLS.swift'] == proof['inputs']['committed_swift_sha256'], 'Committed binding digest mismatch')
    actual_framework = {}
    with zipfile.ZipFile(binary) as archive:
        for entry in safe_members(archive):
            if entry.is_dir() or entry.filename.startswith('__MACOSX/'):
                continue
            path = PurePosixPath(entry.filename)
            ffi.require(path.parts[0] == 'CatbirdMLSFFI.xcframework', 'Unexpected binary archive root')
            with archive.open(entry) as stream:
                actual_framework[str(PurePosixPath(*path.parts[1:]))] = hash_stream(stream)
    ffi.require(actual_framework == verification['framework_sha256'], 'Archived framework differs from verified framework')
    ffi.require(set(proof['resolved_swift_dependencies']) == {'CatbirdMLSCore', 'PetrelCatbird'}, 'Incomplete resolved dependency provenance')
    for name, resolution in proof['resolved_swift_dependencies'].items():
        resolved = support / (name + '.Package.resolved')
        ffi.require(ffi.sha(resolved) == resolution['sha256'], 'Resolved dependency file mismatch')
        ffi.require(load(resolved)['pins'] == resolution['pins'], 'Resolved dependency revisions mismatch')
    metadata = workspace / 'metadata'
    metadata.mkdir(exist_ok=False)
    for name in (*ffi.PAIRED_FILES, 'ffi-provenance.json', 'CatbirdMLSCore.Package.resolved', 'PetrelCatbird.Package.resolved'):
        shutil.copyfile(support / name, metadata / name)
    shutil.copyfile(unpacked / (binary.name + '.sha256'), metadata / (binary.name + '.sha256'))
    receipt = {
        'source_run_id': expected['source_run_id'], 'source_run_attempt': expected['source_run_attempt'],
        'source_head_sha': expected['sources']['CatbirdMLSCore'], 'sources': expected['sources'],
        'artifact_id': artifact['id'], 'artifact_digest': artifact['digest'],
        'archive_sha256': archive_sha, 'archive_size_bytes': binary.stat().st_size,
        'release_tag': expected['release_tag'], 'metadata_files_sha256': ffi.files(metadata),
        'framework_file_count': len(actual_framework), 'scope': 'Verified remote build artifact; no native rebuild or publication.',
    }
    ffi.save(metadata / 'handoff-receipt.json', receipt)
    print(json.dumps(receipt, indent=2))
    return receipt


def publication(root, expected, receipt, final_sha, tag):
    ffi.require(re.fullmatch(r'[0-9a-f]{40}', final_sha), 'Final commit must be immutable')
    ffi.require(tag['object']['type'] == 'commit' and tag['object']['sha'] == final_sha, 'Existing release tag does not name final commit')
    ffi.require(tag['ref'] == 'refs/tags/' + expected['release_tag'], 'Unexpected release tag')
    ffi.require(ffi.run('git', 'rev-parse', 'HEAD', cwd=root) == final_sha, 'Final checkout mismatch')
    # The final package commit may update release metadata and dependency pins,
    # but the source and tests must remain exactly those compiled by the source run.
    ffi.run('git', 'diff', '--exit-code', expected['sources']['CatbirdMLSCore'], final_sha, '--', 'Sources', 'Tests', cwd=root)
    manifest = (root / 'Package.swift').read_text()
    url = f"https://github.com/joshlacal/CatbirdMLSCore/releases/download/{expected['release_tag']}/CatbirdMLSFFI.xcframework.zip"
    pattern = r'\.binaryTarget\(\s*name: "CatbirdMLSFFI",\s*url: "([^"]+)",\s*checksum: "([0-9a-f]{64})"\s*\)'
    ffi.require(re.findall(pattern, manifest) == [(url, receipt['archive_sha256'])], 'Final public binary target does not match artifact')
    for name in ('Petrel', 'PetrelCatbird'):
        ffi.require(ffi.dependency_pin(manifest, name) == expected['sources'][name], 'Final SDK dependency mismatch')
    ffi.require(ffi.sha(root / 'Sources/CatbirdMLS/CatbirdMLS.swift') == receipt['metadata_files_sha256']['CatbirdMLS.swift'], 'Final Swift binding mismatch')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('select', 'verify', 'publication'))
    args = parser.parse_args()
    expected = load(ROOT / 'Scripts/ffi-release-inputs.json')
    workspace = ROOT / '.build/ffi-artifact-work'
    run = load(workspace / 'source-run.json')
    artifact = checked_run(expected, run, load(workspace / 'source-artifacts.json'))
    if args.phase == 'select':
        print(artifact['id'])
    elif args.phase == 'verify':
        receipt = verify(expected, run, artifact, workspace)
        if os.environ.get('EXPECTED_ARTIFACT_ID'):
            ffi.require(str(receipt['artifact_id']) == os.environ['EXPECTED_ARTIFACT_ID'], 'Publication artifact ID mismatch')
            ffi.require(receipt['archive_sha256'] == os.environ['EXPECTED_ARCHIVE_SHA256'], 'Publication archive checksum mismatch')
    else:
        publication(ROOT, expected, load(workspace / 'metadata/handoff-receipt.json'), os.environ['FINAL_COMMIT_SHA'], load(workspace / 'release-tag.json'))
