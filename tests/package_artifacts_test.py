"""Every binary distribution includes the actual license notices."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class PackageArtifacts(unittest.TestCase):
    def test_binary_and_licenses_are_the_only_distribution_files(self):
        for kind, platform, filename in [
            ('desktop-app', 'linux-x86_64', 'Trivium.AppImage'),
            ('desktop-app', 'linux-x86_64', 'trivium.deb'),
            ('desktop-app', 'windows-x86_64', 'Trivium.exe'),
            ('automation-cli', 'linux-x86_64', 'trivium-cli'),
            ('automation-cli', 'windows-x86_64', 'trivium-cli.exe'),
        ]:
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as directory:
                directory = Path(directory)
                binary = directory / filename
                binary.write_bytes(b'example binary')
                binary.chmod(0o755)
                output = directory / 'artifact'
                subprocess.run([sys.executable, str(ROOT / 'scripts/package_artifacts.py'),
                                '--kind', kind, '--platform', platform, '--output', str(output), str(binary)],
                               check=True, capture_output=True, text=True)
                self.assertEqual({file.name for file in output.iterdir()},
                                 {filename, 'LICENSE', 'THIRD_PARTY_LICENSES.txt'})
                for source in (binary, ROOT / 'LICENSE', ROOT / 'THIRD_PARTY_LICENSES.txt'):
                    self.assertEqual((output / source.name).read_bytes(), source.read_bytes())
                self.assertEqual((output / filename).stat().st_mode, binary.stat().st_mode)

    def test_missing_binary_does_not_leave_an_incomplete_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            output = directory / 'artifact'
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/package_artifacts.py'),
                                     '--kind', 'desktop-app', '--platform', 'windows-x86_64',
                                     '--output', str(output), str(directory / 'missing.exe')],
                                    capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
