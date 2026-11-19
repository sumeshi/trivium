"""Public CI annotations must not reveal native assertion payloads or paths."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch

from row_navigation import emit_navigation_failure, wait_for
from workspace_ui import emit_unexpected_ipc_errors, emit_workspace_failure, main


class WorkspaceDiagnosticsTests(unittest.TestCase):
    def capture(self, stage, error, enabled="true"):
        output = io.StringIO()
        with patch.dict(os.environ, {"GITHUB_ACTIONS": enabled}), redirect_stdout(output):
            emit_workspace_failure(stage, error)
        return output.getvalue()

    def test_annotation_is_disabled_outside_github_actions(self):
        self.assertEqual(self.capture("startup", AssertionError("private"), "false"), "")

    def ipc_annotations(self, entries, enabled="true"):
        output = io.StringIO()
        with patch.dict(os.environ, {"GITHUB_ACTIONS": enabled}), redirect_stdout(output):
            emit_unexpected_ipc_errors(entries)
        return output.getvalue()

    def test_unexpected_ipc_annotations_are_disabled_outside_ci_and_when_empty(self):
        entry = {"name": "query_project_rows", "error": "private error", "request": {"path": "private/path"}}
        self.assertEqual(self.ipc_annotations([entry], "false"), "")
        self.assertEqual(self.ipc_annotations([]), "")

    def test_unexpected_ipc_annotations_classify_fixed_errors_without_payloads(self):
        cases = [
            ("Project not found.", "project_missing"),
            ("Project data file missing.", "io_missing"),
            ("Project directory missing", "io_missing"),
            ("private/path: No such file or directory (os error 2)", "io_missing"),
            ("C:\\Users\\private\\case: The system cannot find the file specified. (os error 2)", "io_missing"),
            ("database is locked: private/path", "locked"),
            ("database table is locked: private-table", "locked"),
            ("Legacy annotation database is in use; private/path", "locked"),
            ("The operation queue is full. Wait for an operation to finish.", "capacity"),
            ("The storage queue is full. Wait for a save to finish.", "capacity"),
            ("Too many long-running operations. Wait for one to finish.", "capacity"),
            ("Cancelled.", "cancelled"),
            ("private unrecognized source error", "unknown"),
            ({"private": "error"}, "unknown"),
        ]
        for error, category in cases:
            with self.subTest(category=category, error=error):
                output = self.ipc_annotations([{"name": "query_project_rows", "error": error,
                    "requestId": "private-request-id", "request": {"projectId": "private-project-id",
                    "search": "private-search", "condition": {"column": "private-column"},
                    "sortKey": "private-sort", "path": "/home/private/case", "memo": "private memo"}}])
                prefix = "::error title=Unexpected native IPC::"
                self.assertTrue(output.startswith(prefix))
                self.assertNotIn("private", output)
                self.assertEqual(json.loads(output[len(prefix):]), {"command": "query_project_rows",
                    "category": category, "count": 1, "project": True, "owned": True, "filtered": True, "sorted": True})

    def test_unexpected_ipc_annotations_allowlist_commands_and_group_safe_flags_only(self):
        secret = "fixture.csv /home/alice/private source, memo, search, token=credential%\r\n::error title=Injected::bad"
        request = {"projectId": secret, "rowIndex": 123456, "path": secret, "memo": secret,
                   "search": secret, "sortKey": secret, "tags": [secret], "flagFilter": "all"}
        known = {"name": "get_cell", "error": secret, "requestId": secret, "request": request}
        hostile = {"name": secret, "error": secret, "request": request}
        output = self.ipc_annotations([known, known, hostile, {"name": {"secret": secret}, "error": secret}])
        self.assertNotIn(secret, output)
        for fragment in ["fixture.csv", "/home/", "alice", "credential", "123456", "Injected", "private", "token="]:
            self.assertNotIn(fragment, output)
        lines = output.splitlines()
        self.assertEqual(len(lines), 3)
        records = [json.loads(line.split("::", 2)[2]) for line in lines]
        self.assertEqual({record["command"] for record in records}, {"get_cell", "unknown"})
        self.assertEqual(next(record for record in records if record["command"] == "get_cell")["count"], 2)
        self.assertTrue(all(record["category"] == "unknown" for record in records))
        self.assertTrue(all(type(record[key]) is bool for record in records for key in ["project", "owned", "filtered", "sorted"]))

    def test_timeout_annotation_contains_native_location_without_values_or_paths(self):
        try:
            wait_for(lambda: "private-source-value", lambda value: False, "private-query", timeout=0)
        except AssertionError as error:
            annotation = self.capture("virtualized wide table and unchanged rows", error)
        self.assertRegex(annotation, r"^::error title=Native workspace::Native workspace failed: virtualized wide table and unchanged rows \(AssertionError at workspace_diagnostics_test\.py:[0-9]+\)\n$")
        self.assertNotIn("private", annotation)
        self.assertNotIn(str(Path(__file__).resolve().parent), annotation)

    def test_timeout_without_an_owned_caller_keeps_the_safe_helper_location(self):
        try:
            wait_for(lambda: "private-source", lambda value: False, "private-label", timeout=0)
        except AssertionError as error:
            error.__traceback__ = error.__traceback__.tb_next
            annotation = self.capture("startup", error)
        self.assertRegex(annotation, r"AssertionError at row_navigation\.py:[0-9]+")
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

    def test_incompatible_check_selections_fail_before_startup(self):
        command = [sys.executable, str(Path(__file__).with_name("workspace_ui.py")),
                   "--binary", "unused", "--memo-ime-only", "--table-presentation-only"]
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(command, cwd=directory, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2)
            self.assertIn("not allowed with argument", result.stderr)
            self.assertEqual(list(Path(directory).iterdir()), [])

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
