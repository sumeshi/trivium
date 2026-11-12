"""Test the WebView2 attach contract and owned process cleanup on any host."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import io
from contextlib import redirect_stdout
import os
from pathlib import Path
import re
import tempfile
from threading import Thread
import unittest
from unittest.mock import patch

from runtime import ApplicationSession, isolated_environment, debuggable_webview, emit_failure_annotation


class Process:
    def __init__(self, code=None):
        self.code = code

    def poll(self):
        return self.code


class RuntimeTests(unittest.TestCase):
    def test_linux_retains_the_tauri_driver_launch_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = isolated_environment(root)
            with patch('runtime.start_driver') as start:
                binary = root / 'trivium'
                app = ApplicationSession(binary, environment, root, platform='posix')
                self.assertEqual(app.capabilities, {'tauri:options': {'application': str(binary)}})
                app.close()
                start.assert_not_called()

    def test_windows_attaches_to_the_real_endpoint_and_owns_only_its_application(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = isolated_environment(root)
            environment['WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS'] = '--disable-features=Example'
            runtime = root / 'runtime'
            runtime.mkdir()
            (runtime / 'msedgewebview2.exe').touch()
            environment['TRIVIUM_WEBVIEW2_RUNTIME_DIRECTORY'] = str(runtime)
            original = dict(environment)
            process = Process()
            server = None
            child = {}

            class Handler(BaseHTTPRequestHandler):
                def do_GET(self):
                    payload = ({'Browser': 'Edg/145.1.2.3'}
                               if self.path == '/json/version' else [{'type': 'webview', 'url': 'http://tauri.localhost', 'webSocketDebuggerUrl': 'ws://127.0.0.1/devtools/page/test'}])
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(payload).encode())

                def log_message(self, *args):
                    pass

            def start(command, env, log):
                nonlocal server
                child.update(env)
                self.assertEqual(command, [str(root / 'debug' / 'trivium.exe')])
                port = int(re.search(r'--remote-debugging-port=(\d+)', env['WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS'])[1])
                server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
                server.daemon_threads = True
                Thread(target=server.serve_forever, daemon=True).start()
                return process

            def stop(owned):
                self.assertIs(owned, process)
                server.shutdown()
                server.server_close()

            with patch('runtime.start_driver', start), patch('runtime.stop_driver', side_effect=stop) as stop_owned:
                app = ApplicationSession(root / 'debug' / 'trivium.exe', environment, root / 'evidence', platform='nt')
                self.assertEqual(app.capabilities['browserName'], 'webview2')
                self.assertNotIn('tauri:options', app.capabilities)
                self.assertNotIn('binary', app.capabilities['ms:edgeOptions'])
                port = server.server_port
                self.assertEqual(app.capabilities['ms:edgeOptions']['debuggerAddress'], f'127.0.0.1:{port}')
                self.assertEqual(child['WEBVIEW2_BROWSER_EXECUTABLE_FOLDER'], str(runtime))
                self.assertEqual(child['WEBVIEW2_USER_DATA_FOLDER'], str(Path(environment['LOCALAPPDATA']) / 'native-webview-profile'))
                self.assertIn('--disable-features=Example', child['WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS'])
                self.assertEqual(environment, original)
                self.assertEqual(app.details, {'mode': 'webview2-attach', 'browser': 'Edg/145.1.2.3'})
                app.close()
                app.close()
                stop_owned.assert_called_once_with(process)

    def test_startup_exit_is_reported_and_the_owned_process_is_cleaned(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process = Process(17)
            with patch('runtime.start_driver', return_value=process), patch('runtime.stop_driver') as stop:
                with self.assertRaisesRegex(AssertionError, 'exited with code 17.*application.log'):
                    ApplicationSession(root / 'debug' / 'trivium.exe', isolated_environment(root), root, platform='nt')
                stop.assert_called_once_with(process)
                self.assertTrue((root / 'application.log').exists())

    def test_attach_needs_a_view_websocket_but_not_a_browser_websocket(self):
        version = {'Browser': 'Edg/145.1.2.3'}
        for kind in ('page', 'webview'):
            self.assertTrue(debuggable_webview(version, [{'type': kind, 'webSocketDebuggerUrl': 'ws://127.0.0.1/devtools/page/test'}]))
        self.assertFalse(debuggable_webview(version, [{'type': 'worker', 'webSocketDebuggerUrl': 'ws://127.0.0.1/devtools/worker/test'}]))
        self.assertFalse(debuggable_webview(version, [{'type': 'page'}]))
        self.assertFalse(debuggable_webview({}, [{'type': 'page', 'webSocketDebuggerUrl': 'ws://127.0.0.1/devtools/page/test'}]))

    def test_failure_annotation_is_bounded_escaped_and_omits_uncontrolled_logs(self):
        with patch.dict(os.environ, {'GITHUB_ACTIONS': 'true'}):
            output = io.StringIO()
            with redirect_stdout(output):
                emit_failure_annotation(AssertionError('Native app failed100%\r\nnext'))
            self.assertEqual(output.getvalue(), '::error title=Native smoke::Native app failed100%25%0D%0Anext\n')
            output = io.StringIO()
            with redirect_stdout(output):
                emit_failure_annotation(RuntimeError('private output that must not be published'))
            self.assertNotIn('private output', output.getvalue())

    def test_timeout_reports_only_safe_probe_metadata(self):
        app = object.__new__(ApplicationSession)
        app.process = Process()

        class Response(io.BytesIO):
            status = 200

        def endpoint(request, timeout):
            value = ({'Browser': 'Edg/145.1.2.3', 'secret': 'never-print-this'}
                     if request.endswith('/json/version') else
                     [{'type': 'webview', 'url': 'http://private.invalid/?secret=never-print-this'}])
            return Response(json.dumps(value).encode())

        with patch('runtime.urlopen', side_effect=endpoint), patch('runtime.time.monotonic', side_effect=[0, .01, 1]), patch('runtime.time.sleep'):
            with self.assertRaises(AssertionError) as failure:
                app._wait_for_debugging(12345, timeout=1)
        message = str(failure.exception)
        self.assertIn('"version_http":200', message)
        self.assertIn('"list_http":200', message)
        self.assertIn('"target_count":1', message)
        self.assertIn('"target_types":["webview"]', message)
        self.assertNotIn('private.invalid', message)
        self.assertNotIn('never-print-this', message)

    def test_windows_refuses_a_release_binary_before_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch('runtime.start_driver') as start:
                with self.assertRaisesRegex(AssertionError, 'debug test binary.*production executable'):
                    ApplicationSession(root / 'release' / 'trivium.exe', isolated_environment(root), root, platform='nt')
                start.assert_not_called()

    def test_runtime_pin_must_exist_before_spawning_the_application(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = isolated_environment(root)
            env['TRIVIUM_WEBVIEW2_RUNTIME_DIRECTORY'] = str(root / 'missing')
            with patch('runtime.start_driver') as start:
                with self.assertRaisesRegex(AssertionError, 'Selected WebView2 runtime is missing'):
                    ApplicationSession(root / 'debug' / 'trivium.exe', env, root, platform='nt')
                start.assert_not_called()


if __name__ == '__main__':
    unittest.main()
