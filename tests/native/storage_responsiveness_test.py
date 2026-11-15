from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from storage_responsiveness_checks import _case_database


class StorageDatabaseLocation(unittest.TestCase):
    def test_linux_database_is_found_only_in_owned_temporary_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, owner = Path(temporary), str(uuid.uuid4())
            path = root / "data" / "com.trivium.native-tests" / "trivium" / "projects" / owner / "project.sqlite"
            path.parent.mkdir(parents=True)
            path.touch()
            self.assertEqual(_case_database(root, owner, platform="posix"), path)

    def test_windows_uses_known_folder_and_exact_automation_namespace(self):
        with tempfile.TemporaryDirectory() as temporary:
            root, owner = Path(temporary), str(uuid.uuid4())
            local = root / "actual-known-folder"
            path = local / "com.trivium.native-tests" / "trivium" / "projects" / owner / "project.sqlite"
            path.parent.mkdir(parents=True)
            path.touch()
            # A production case and an overridden temporary folder must never
            # be selected, even when their synthetic UUID happens to match.
            for base in [root / "overridden-localappdata", local / "com.trivium.app"]:
                decoy = base / "trivium" / "projects" / owner / "project.sqlite"
                decoy.parent.mkdir(parents=True)
                decoy.touch()
            with patch("storage_responsiveness_checks._windows_local_app_data", return_value=local):
                self.assertEqual(_case_database(root, owner, platform="nt"), path)
                path.unlink()
                with self.assertRaisesRegex(AssertionError, "owned Windows automation"):
                    _case_database(root, owner, platform="nt")


if __name__ == "__main__":
    unittest.main()
