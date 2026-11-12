"""Diagnostic output must remain bounded and contain no private log/process data."""
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from startup_diagnostics import LOG_LIMIT, MAX_CHILDREN, log_diagnostics, process_diagnostics, sanitize_process_snapshot


class StartupDiagnosticsTests(unittest.TestCase):
    def test_log_tail_exposes_only_fixed_phase_panic_and_hresult_codes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'application.log'
            path.write_text('[trivium-native] before-builder\n' + 'private-before-tail' * LOG_LIMIT +
                            '\n[trivium-native] setup-start\n[trivium-native] state-ready-secret\n'
                            "thread 'main' panicked at private-path-and-values\n"
                            'HRESULT(0x80070002) HRESULT(-2147467259) https://user:secret@private.invalid\n', encoding='utf-8')
            result = log_diagnostics(path)
            self.assertEqual(result, {'phase': 'setup-start', 'panic': True, 'hresults': ['0x80070002', '0x80004005'],
                                     'log_bytes': path.stat().st_size, 'tail_truncated': True})
            self.assertNotIn('private', json.dumps(result))
            self.assertNotIn('secret', json.dumps(result))

    def test_snapshot_filters_unrelated_processes_and_private_fields(self):
        rows = [
            {'pid': 99, 'parent': 4, 'name': 'unrelated-secret.exe'},
            {'pid': 11, 'parent': 10, 'name': 'msedgewebview2.exe', 'commandLine': 'private-secret-command'},
            {'pid': 10, 'parent': 5, 'name': 'trivium-desktop.exe', 'environment': 'private-secret-environment'},
            {'pid': 12, 'parent': 11, 'name': 'C:\\private\\secret.exe'},
        ]
        value = sanitize_process_snapshot({'has_window': True, 'items': rows}, 10)
        self.assertEqual(value, {'has_window': True, 'count': 3, 'items': [
            {'pid': 10, 'parent': 5, 'name': 'trivium-desktop.exe'},
            {'pid': 11, 'parent': 10, 'name': 'msedgewebview2.exe'},
            {'pid': 12, 'parent': 11, 'name': 'unrecognized'},
        ]})
        self.assertNotIn('private', json.dumps(value))
        self.assertNotIn('unrelated', json.dumps(value))

    def test_snapshot_is_bounded_to_the_root_and_sixteen_children(self):
        rows = [{'pid': 100, 'parent': 0, 'name': 'trivium-desktop.exe'}]
        rows += [{'pid': child, 'parent': child - 1, 'name': 'msedgewebview2.exe'} for child in range(101, 140)]
        value = sanitize_process_snapshot({'has_window': False, 'items': rows}, 100)
        self.assertEqual(value['count'], MAX_CHILDREN + 1)
        self.assertEqual(value['items'][-1]['pid'], 100 + MAX_CHILDREN)

    def test_process_probe_is_timed_and_does_not_request_command_lines(self):
        process = SimpleNamespace(pid=100)
        result = SimpleNamespace(returncode=0, stdout=json.dumps({'has_window': False, 'items': [
            {'pid': 100, 'parent': 5, 'name': 'trivium-desktop.exe'},
        ]}))
        with patch('startup_diagnostics.subprocess.run', return_value=result) as run:
            value = process_diagnostics(process, platform='nt')
        self.assertEqual(value['count'], 1)
        self.assertEqual(run.call_args.kwargs['timeout'], 5)
        command = run.call_args.args[0]
        self.assertEqual(command[0], 'powershell.exe')
        self.assertNotIn('CommandLine', command[-1])
        self.assertNotIn('Environment', command[-1])

    def test_process_probe_failure_reports_only_an_error_class(self):
        with patch('startup_diagnostics.subprocess.run', side_effect=subprocess.TimeoutExpired('private-command', 5, output='private-output')):
            value = process_diagnostics(SimpleNamespace(pid=100), platform='nt')
        self.assertEqual(value, {'probe': 'TimeoutExpired'})
        self.assertNotIn('private', json.dumps(value))


if __name__ == '__main__':
    unittest.main()
