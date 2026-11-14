"""Public CI annotations must not reveal native assertion payloads or paths."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from row_navigation import emit_navigation_failure, wait_for
from workspace_ui import emit_workspace_failure, main


class WorkspaceDiagnosticsTests(unittest.TestCase):
    def capture(self, stage, error, enabled="true"):
        output = io.StringIO()
        with patch.dict(os.environ, {"GITHUB_ACTIONS": enabled}), redirect_stdout(output):
            emit_workspace_failure(stage, error)
        return output.getvalue()

    def test_annotation_is_disabled_outside_github_actions(self):
        self.assertEqual(self.capture("startup", AssertionError("private"), "false"), "")

    def test_timeout_annotation_contains_native_location_without_values_or_paths(self):
        try:
            wait_for(lambda: "private-source-value", lambda value: False, "private-query", timeout=0)
        except AssertionError as error:
            annotation = self.capture("virtualized wide table and unchanged rows", error)
        self.assertRegex(annotation, r"^::error title=Native workspace::Native workspace failed: virtualized wide table and unchanged rows \(AssertionError at row_navigation\.py:[0-9]+\)\n$")
        self.assertNotIn("private", annotation)
        self.assertNotIn(str(Path(__file__).resolve().parent), annotation)

    def test_navigation_annotation_uses_fixed_stages_and_omits_private_payloads(self):
        try:
            wait_for(lambda: "private-source-value", lambda value: False, "private-query%\r\nsecret", timeout=0)
        except AssertionError as error:
            for label, expected in [("native import", "native import"),
                                    ("Go original #2,000,000", "Go original row"),
                                    ("filtered descending Go original #3,898", "filtered descending Go original row"),
                                    ("private-stage%\r\nsecret", "unknown checkpoint"),
                                    ({"private": "secret"}, "unknown checkpoint")]:
                for rows in [100, 5199, 2_000_000, 99, 2_000_001, True, "private-rows"]:
                    with self.subTest(label=label, rows=rows):
                        output = io.StringIO()
                        with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}), redirect_stdout(output):
                            emit_navigation_failure({"stage": label, "rows": rows}, error)
                        annotation = output.getvalue()
                        count = f"; rows={rows}" if type(rows) is int and 100 <= rows <= 2_000_000 else ""
                        prefix = f"::error title=Native navigation::Native navigation failed: {expected}{count} (AssertionError at row_navigation.py:"
                        self.assertTrue(annotation.startswith(prefix), annotation)
                        self.assertRegex(annotation[len(prefix):], r"^[0-9]+\)\n$")
                        self.assertNotIn("private", annotation)
                        self.assertNotIn("secret", annotation)
                        self.assertNotIn(str(Path(__file__).resolve().parent), annotation)
            output = io.StringIO()
            with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}), redirect_stdout(output):
                emit_navigation_failure({}, error)
            self.assertEqual(output.getvalue(), "")

    def test_startup_constructor_failure_is_annotated_and_keeps_its_private_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "test-binary"
            binary.touch()
            output = io.StringIO()
            with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}), \
                    patch("sys.argv", ["workspace_ui.py", "--binary", str(binary), "--output", str(root / "evidence")]), \
                    patch("workspace_ui.NativeApp", side_effect=RuntimeError("private-startup-value")), \
                    redirect_stdout(output), self.assertRaisesRegex(RuntimeError, "private-startup-value"):
                main()
            annotation = output.getvalue()
            self.assertIn("Native workspace failed: startup (RuntimeError at workspace_ui.py:", annotation)
            self.assertNotIn("private-startup-value", annotation)
            self.assertNotIn(str(root), annotation)
            reports = list((root / "evidence").glob("*/report.json"))
            self.assertEqual(len(reports), 1)
            report = json.loads(reports[0].read_text())
            self.assertEqual(report["error"], "private-startup-value")
            self.assertEqual(report["sessions"], [])


if __name__ == "__main__":
    unittest.main()
