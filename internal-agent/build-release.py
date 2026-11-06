#!/usr/bin/env python3
"""Build checksum-bound Linux agents from an exact committed source snapshot.

This script builds local artifacts only. It never connects to or updates a host.
Example: python internal-agent/build-release.py --source-commit <commit> --build-dir <new-local-directory> --go <go-executable>
"""
import argparse
import datetime
import hashlib
import io
import json
import os
import pathlib
import re
import struct
import subprocess
import tarfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source-commit', required=True)
parser.add_argument('--build-dir', required=True, type=pathlib.Path)
parser.add_argument('--go', default='go')
args = parser.parse_args()
kit = pathlib.Path(__file__).resolve().parent
repo = kit.parent
commit = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', '--verify', args.source_commit + '^{commit}'], text=True).strip()
if not re.fullmatch('[a-f0-9]{40}', commit):
    raise SystemExit('Expected a full verified source commit.')
build = args.build_dir.resolve()
if build.exists():
    raise SystemExit('Build directory must be new; existing directories are never deleted or overwritten.')
build.mkdir(parents=True)
source = build / 'source'
source.mkdir()
raw = subprocess.check_output(['git', '-C', str(repo), 'archive', commit + ':internal-agent/source'])
with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
    for member in archive.getmembers():
        if not (member.isdir() or member.isfile()):
            raise SystemExit('Source archive contains a link or unsupported file type.')
        relative = pathlib.PurePosixPath(member.name)
        target = (source / str(relative)).resolve()
        if target != source and source not in target.parents:
            raise SystemExit('Source archive escape.')
        if member.isdir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.extractfile(member).read())
match = re.search(r'const Version = "([^"]+)"', (source / 'pkg/buildinfo/version.go').read_text())
if not match or not re.search(r'-addnode\.\d+$', match.group(1)):
    raise SystemExit('The source must identify an Add Node release.')
version = match.group(1)
suffix = re.search(r'addnode\.\d+$', version).group(0)
stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
flags = '-s -w -X main.commit=' + commit + ' -X main.buildDate=' + stamp
checksums = {}
environment = os.environ.copy()
environment.update(CGO_ENABLED='0', GOOS='linux')
(kit / 'bin').mkdir(exist_ok=True)
for architecture, machine in [('amd64', 62), ('arm64', 183)]:
    environment['GOARCH'] = architecture
    binary = kit / 'bin' / ('tantor-agent-linux-' + architecture)
    subprocess.run([args.go, 'build', '-trimpath', '-ldflags', flags, '-o', str(binary), './cmd/agent'], cwd=source, env=environment, check=True)
    content = binary.read_bytes()
    if content[:4] != b'\x7fELF' or content[5] != 1 or struct.unpack_from('<H', content, 18)[0] != machine:
        raise SystemExit('Binary format/architecture mismatch: ' + architecture)
    metadata = subprocess.check_output([args.go, 'version', '-m', str(binary)], text=True)
    if commit.encode() not in content or version.encode() not in content or stamp.encode() not in content or 'GOOS=linux' not in metadata or 'GOARCH=' + architecture not in metadata:
        raise SystemExit('Binary source/build identity mismatch.')
    checksums[str(binary.relative_to(kit)).replace('\\', '/')] = hashlib.sha256(content).hexdigest()
archive_path = kit / ('source-' + suffix + '.tar.gz')
with tarfile.open(archive_path, 'w:gz') as archive:
    archive.add(source, arcname='source')
checksums[archive_path.name] = hashlib.sha256(archive_path.read_bytes()).hexdigest()
(kit / 'SHA256SUMS').write_text(''.join(digest + '  ' + name + '\n' for name, digest in checksums.items()), encoding='ascii', newline='\n')
manifest = {'version': version, 'source_commit': commit, 'build_date_utc': stamp, 'go_version': subprocess.check_output([args.go, 'version'], text=True).strip(), 'checksums': checksums,
            'deployment_status': 'HELD_NOT_INSTALLED', 'new_command': 'INSPECT_ADD_NODE_PROCESS', 'controller_execution_enabled': False, 'separate_process_execution_enabled': False}
(kit / 'BUILD-ADD-NODE.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8', newline='\n')
print(json.dumps(manifest))
