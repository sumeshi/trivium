#!/usr/bin/env python3
"""Collect binary outputs and their license notices for distribution."""
import argparse
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', choices=('desktop-app', 'automation-cli'), required=True)
    parser.add_argument('--platform', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('files', type=Path, nargs='+')
    args = parser.parse_args()
    sources = [*args.files, ROOT / 'LICENSE', ROOT / 'THIRD_PARTY_LICENSES.txt']
    for source in sources:
        if not source.is_file():
            parser.error(f'Build output or license notice is missing: {source}')
    args.output.mkdir(parents=True, exist_ok=True)
    for source in sources:
        target = args.output / source.name
        if source.resolve() != target.resolve():
            shutil.copy2(source, target)
    print(f'Packaged {args.kind} for {args.platform}: {len(args.files)} binary file(s) and license notices')


if __name__ == '__main__':
    main()
