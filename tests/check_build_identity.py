"""Keep app, portable CLI and package identities aligned without resolving dependencies."""
from pathlib import Path
import json
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[1]


class BuildIdentity(unittest.TestCase):
    def test_all_application_package_versions_match(self):
        version = json.loads((ROOT / 'package.json').read_text())['version']
        self.assertRegex(version, r'^\d+\.\d+\.\d+$')
        lock = json.loads((ROOT / 'package-lock.json').read_text())
        self.assertEqual(lock['version'], version)
        self.assertEqual(lock['packages']['']['version'], version)
        configuration = json.loads((ROOT / 'src-tauri/tauri.conf.json').read_text())
        self.assertEqual(configuration['version'], version)
        self.assertEqual(configuration['identifier'], 'com.trivium.app')
        for manifest in ('crates/engine/Cargo.toml', 'src-tauri/Cargo.toml'):
            self.assertEqual(tomllib.loads((ROOT / manifest).read_text())['package']['version'], version)
        packages = tomllib.loads((ROOT / 'Cargo.lock').read_text())['package']
        for name in ('trivium-engine', 'trivium-desktop'):
            self.assertEqual(next(package['version'] for package in packages if package['name'] == name), version)


if __name__ == '__main__':
    unittest.main()
