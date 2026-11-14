#!/usr/bin/env python3
"""Collect shipped dependency notices; refresh after updating either lockfile."""
import argparse
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'THIRD_PARTY_LICENSES.txt'
SYSTEM_MARKER = '\n=== Linux distribution components ===\n'


class LicenseHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def handle_starttag(self, tag, attrs):
        if tag in ('p', 'pre', 'li', 'h1', 'h2', 'h3', 'br'):
            self.parts.append('\n')


def lock_header():
    return '\n'.join(f'{name} SHA256: {hashlib.sha256((ROOT / name).read_text(encoding="utf-8").encode()).hexdigest()}'
                     for name in ('Cargo.lock', 'package-lock.json', 'rust-toolchain.toml'))


def license_files(directory):
    return sorted(path for path in directory.rglob('*') if path.is_file()
                  and re.match(r'^(?:licen[cs]e|copying|copyright|notice|unlicense)(?:[._-]|$)|^third[-_ ]?party[-_ ]?(?:notices?|licen[cs]es?)', path.name, re.I)
                  and 'node_modules' not in path.relative_to(directory).parts)


def cargo_packages():
    packages = {}
    for target in ('x86_64-unknown-linux-gnu', 'x86_64-pc-windows-msvc'):
        metadata = json.loads(subprocess.check_output([
            'cargo', 'metadata', '--locked', '--offline', '--format-version', '1',
            '--filter-platform', target], cwd=ROOT, text=True))
        nodes = {node['id']: node for node in metadata['resolve']['nodes']}
        pending = list(metadata['workspace_members'])
        seen = set()
        while pending:
            package = pending.pop()
            if package in seen:
                continue
            seen.add(package)
            pending.extend(dep['pkg'] for dep in nodes[package]['deps']
                           if any(kind['kind'] != 'dev' for kind in dep['dep_kinds']))
        packages.update({package['id']: package for package in metadata['packages']
                         if package['id'] in seen and package['source']})
    return sorted(packages.values(), key=lambda package: (package['name'], package['version']))


def generate():
    overrides = json.loads((ROOT / 'scripts/license_overrides.json').read_text(encoding='utf-8'))
    sections = ['Trivium third-party copyright notices and license terms', lock_header(),
                'Includes Rust dependencies for Linux/Windows (including build tools) and npm production dependencies.\n'
                'License alternatives are reproduced as supplied; inclusion does not imply that every component is used on every platform.\n'
                'Source archives are available from https://crates.io/crates/<name>/<version> and https://www.npmjs.com/package/<name>/v/<version>.']
    sysroot = Path(subprocess.check_output(['rustc', '--print', 'sysroot'], text=True).strip())
    rust_notice = sysroot / 'share/doc/rust/COPYRIGHT-library.html'
    if not rust_notice.is_file():
        raise RuntimeError('Install the rust-docs component to collect Rust standard library licenses')
    rust = LicenseHTML()
    rust.feed(rust_notice.read_text(encoding='utf-8'))
    sections.append('\n=== Rust Standard Library ===\n' + ''.join(rust.parts))
    for package in cargo_packages():
        name = f"{package['name']}@{package['version']}"
        directory = Path(package['manifest_path']).parent
        files = license_files(directory)
        text = f"\n=== Rust: {name} ===\nLicense: {package['license']}\nSource: https://crates.io/crates/{package['name']}/{package['version']}\n"
        if files:
            for file in files:
                text += f'\n--- {file.relative_to(directory)} ---\n' + file.read_text(encoding='utf-8', errors='replace') + '\n'
        else:
            notice = overrides.get(name)
            if not notice:
                raise RuntimeError(f'Missing license text for {name}; add its upstream license to scripts/license_overrides.json')
            text += f"\nUpstream license: {notice['source']}\n\n{notice['text']}\n"
        sections.append(text)
    lock = json.loads((ROOT / 'package-lock.json').read_text())
    for path, record in sorted(lock['packages'].items()):
        if not path or record.get('dev') or record.get('optional'):
            continue
        directory = ROOT / path
        package = json.loads((directory / 'package.json').read_text())
        files = license_files(directory)
        sections.append(f"\n=== npm: {package['name']}@{package['version']} ===\nLicense: {package.get('license', 'See license text')}\n")
        if not any(file.suffix != '.spdx' for file in files):
            notice = overrides.get(f"{package['name']}@{package['version']}")
            if not notice:
                raise RuntimeError(f'Missing npm license text: {path}')
            sections.append(f"\nUpstream license: {notice['source']}\n\n{notice['text']}\n")
        for file in files:
            sections.append(f'\n--- {file.relative_to(directory)} ---\n' + file.read_text(encoding='utf-8', errors='replace'))
    OUTPUT.write_text('\n'.join(sections) + '\n', encoding='utf-8')


def include_system():
    # AppImage carries system libraries as well as the application. Preserve the
    # distro's original copyright/license/source notices, including dlopen libs.
    # Include all installed notices conservatively; identical texts appear once.
    notices = {}
    for path in sorted(Path('/usr/share/doc').glob('*/copyright')):
        if path.is_file():
            contents = path.read_text(encoding='utf-8', errors='replace')
            notices.setdefault(contents, []).append(path.parent.name)
    if not notices:
        raise RuntimeError('No Linux distribution copyright notices found')
    text = OUTPUT.read_text(encoding='utf-8').split(SYSTEM_MARKER)[0]
    text += SYSTEM_MARKER
    packages = subprocess.check_output([
        'dpkg-query', '--show', '--showformat',
        '${binary:Package}\t${Version}\t${source:Package}\t${source:Version}\n'], text=True)
    # Preserve exact source versions for obtaining the corresponding library
    # sources, including distribution patches, rather than linking only upstream.
    ubuntu = 'ID=ubuntu' in Path('/etc/os-release').read_text()
    text += '\nInstalled package versions and corresponding source archives:\n'
    for record in packages.splitlines():
        binary, version, source, source_version = record.split('\t')
        source = source or binary.split(':')[0]
        source_version = source_version or version
        base = 'https://launchpad.net/ubuntu/+source/' if ubuntu else 'https://sources.debian.org/src/'
        text += f'{binary} {version}: {base}{quote(source, safe="")}/{quote(source_version, safe="")}\n'
    for contents, packages in notices.items():
        text += '\n--- ' + ', '.join(packages) + ' ---\n' + contents + '\n'
    OUTPUT.write_text(text, encoding='utf-8')
    print(f'Included {len(notices)} distinct Linux copyright/license notices')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--check', action='store_true', help='Check that the notice matches both dependency lockfiles')
    mode.add_argument('--include-system', action='store_true', help='Append original Linux runtime component notices before bundling')
    args = parser.parse_args()
    if args.check:
        if not OUTPUT.is_file() or lock_header() not in OUTPUT.read_text(encoding='utf-8'):
            parser.error('Dependency notices are missing or stale; run npm ci, cargo fetch, and python scripts/third_party_licenses.py')
    elif args.include_system:
        include_system()
    else:
        generate()
        print(f'Wrote {OUTPUT.name} ({OUTPUT.stat().st_size:,} bytes)')


if __name__ == '__main__':
    main()
