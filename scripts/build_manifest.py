#!/usr/bin/env python3
"""Package named build outputs with an unambiguous version and source commit."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def desktop_usage(files):
    suffixes = {Path(record['name']).suffix.lower() for record in files}
    instructions = ['This is the desktop application for normal investigation.']
    if '.deb' in suffixes:
        instructions.append('Linux .deb: install it with your package manager.')
    if '.appimage' in suffixes:
        instructions.append('Linux AppImage: make it executable (chmod +x) and open it. Without FUSE, run it with --appimage-extract-and-run.')
    if '.exe' in suffixes:
        instructions.append('Windows: open the .exe; Microsoft WebView2 Runtime is required.')
    return ' '.join(instructions)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', choices=('desktop-app', 'automation-cli'), required=True)
    parser.add_argument('--platform', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('files', type=Path, nargs='+')
    args = parser.parse_args()
    version = json.loads((ROOT / 'package.json').read_text())['version']
    commit = os.environ.get('GITHUB_SHA', '')
    if not re.fullmatch(r'[a-fA-F0-9]{40}', commit):
        commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    args.output.mkdir(parents=True, exist_ok=True)
    records = []
    for source in args.files:
        if not source.is_file():
            parser.error(f'Build output is missing: {source}')
        target = args.output / source.name
        if source.resolve() != target.resolve():
            shutil.copy2(source, target)
        with target.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        records.append({'name': target.name, 'bytes': target.stat().st_size, 'sha256': digest})
    manifest = {'version': version, 'commit': commit, 'kind': args.kind, 'platform': args.platform, 'files': records}
    (args.output / 'BUILD-INFO.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    (args.output / 'SHA256SUMS').write_text(''.join(f"{record['sha256']}  {record['name']}\n" for record in records), encoding='utf-8')
    usage = (desktop_usage(records)
             if args.kind == 'desktop-app' else
             f'This is the optional command-line tool for automation. For the graphical application, download the desktop artifact instead. See https://github.com/sumeshi/trivium/blob/{commit}/README.md#automation.')
    (args.output / 'README.txt').write_text(f'Trivium {version}\nSource commit: {commit}\nPlatform: {args.platform}\n\n{usage}\n\nSHA256SUMS identifies the files inside this artifact. GitHub displays a separate digest for its downloadable ZIP.\n', encoding='utf-8')
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        title = 'Desktop application' if args.kind == 'desktop-app' else 'Optional automation CLI'
        with Path(summary).open('a', encoding='utf-8') as output:
            output.write(f'### {title}: {version}\n\nCommit: `{commit}` · Platform: `{args.platform}`\n\n{usage}\n\n')
            for record in records:
                output.write(f"- `{record['name']}` — SHA-256 `{record['sha256']}`\n")
    print(f'Trivium {version} {args.kind} {args.platform} commit {commit}: {len(records)} file(s)')


if __name__ == '__main__':
    main()
