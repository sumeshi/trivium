"""The distributed checksum must describe the actual file, not the artifact ZIP."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class Manifest(unittest.TestCase):
    def test_copied_file_has_its_real_digest_and_explicit_app_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            source = directory / 'example.deb'
            source.write_bytes(b'abc')
            output = directory / 'artifact'
            environment = dict(os.environ, GITHUB_STEP_SUMMARY=str(directory / 'step-summary.md'))
            subprocess.run([sys.executable, str(ROOT / 'scripts/build_manifest.py'), '--kind', 'desktop-app',
                            '--platform', 'linux-x86_64', '--output', str(output), str(source)],
                           check=True, capture_output=True, text=True, env=environment)
            info = json.loads((output / 'BUILD-INFO.json').read_text())
            self.assertEqual(info['kind'], 'desktop-app')
            self.assertEqual(info['version'], json.loads((ROOT / 'package.json').read_text())['version'])
            self.assertRegex(info['commit'], r'^[a-fA-F0-9]{40}$')
            digest = 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'
            self.assertEqual(info['files'], [{'name': 'example.deb', 'bytes': 3, 'sha256': digest}])
            self.assertEqual((output / 'SHA256SUMS').read_text(), f'{digest}  example.deb\n')
            self.assertEqual((output / 'example.deb').read_bytes(), b'abc')
            self.assertIn('desktop application', (output / 'README.txt').read_text())
            summary = (directory / 'step-summary.md').read_text()
            self.assertIn('Desktop application', summary)
            self.assertIn(digest, summary)

    def test_desktop_instructions_identify_each_package_and_its_launch_method(self):
        for filename, platform, expected, absent in [
            ('Trivium_0.2.0_amd64.AppImage', 'linux-x86_64', ['chmod +x', '--appimage-extract-and-run'], ['package manager', 'Windows:']),
            ('Trivium-v0.2.0.exe', 'windows-x86_64', ['open the .exe', 'WebView2 Runtime'], ['chmod +x', 'package manager']),
            ('trivium_0.2.0_amd64.deb', 'linux-x86_64', ['package manager'], ['chmod +x', 'Windows:']),
        ]:
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / filename
                source.write_bytes(b'package')
                output = root / 'artifact'
                subprocess.run([sys.executable, str(ROOT / 'scripts/build_manifest.py'), '--kind', 'desktop-app',
                                '--platform', platform, '--output', str(output), str(source)],
                               check=True, capture_output=True, text=True)
                usage = (output / 'README.txt').read_text()
                for phrase in expected:
                    self.assertIn(phrase, usage)
                for phrase in absent:
                    self.assertNotIn(phrase, usage)


if __name__ == '__main__':
    unittest.main()
