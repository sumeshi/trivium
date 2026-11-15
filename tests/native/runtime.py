"""Isolated profiles and process cleanup shared by Linux and Windows GUI checks."""
from __future__ import annotations

import os
import json
import re
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import build_opener, ProxyHandler
from urllib.parse import urlsplit
from startup_diagnostics import startup_diagnostics


class NormalWindowClose:
    """An owned native window; WM_CLOSE/WM_DELETE_WINDOW follows normal app shutdown."""
    def __init__(self, application):
        self.process = application.process if application is not None else None
        if os.name == "nt":
            assert self.process is not None, "Windows normal close requires the owned app process"
            self.pid = self.process.pid
            self.window = _windows_main_window(self.pid)
        else:
            self.window, self.pid = _x11_window()

    def request(self) -> None:
        if os.name == "nt":
            from ctypes import byref
            from ctypes.wintypes import DWORD
            user, _ = _windows_user32()
            pid = DWORD()
            user.GetWindowThreadProcessId(self.window, byref(pid))
            assert pid.value == self.pid and not self.exited(), "WM_CLOSE must target the same owned process"
            assert user.PostMessageW(self.window, 0x0010, 0, 0), "WM_CLOSE must reach the owned app"
            return
        from ctypes import Structure, Union, byref, c_char, c_int, c_long, c_short, c_ulong, c_void_p
        class Data(Union):
            _fields_ = [("b", c_char * 20), ("s", c_short * 10), ("l", c_long * 5)]
        class Client(Structure):
            _fields_ = [("type", c_int), ("serial", c_ulong), ("send_event", c_int), ("display", c_void_p),
                        ("window", c_ulong), ("message_type", c_ulong), ("format", c_int), ("data", Data)]
        class Event(Union):
            _fields_ = [("client", Client), ("pad", c_long * 24)]
        x11, display = _x11_open()
        try:
            event = Event()
            event.client.type, event.client.display, event.client.window = 33, display, self.window
            event.client.message_type = x11.XInternAtom(display, b"WM_PROTOCOLS", 0)
            event.client.format = 32
            event.client.data.l[0] = x11.XInternAtom(display, b"WM_DELETE_WINDOW", 0)
            x11.XSendEvent.argtypes = [c_void_p, c_ulong, c_int, c_long, c_void_p]
            assert x11.XSendEvent(display, self.window, 0, 0, byref(event)), "WM_DELETE_WINDOW must reach the owned app"
            x11.XSync(display, 0)
        finally:
            x11.XCloseDisplay(display)

    def exited(self) -> bool:
        if self.process is not None:
            return self.process.poll() is not None
        try:
            # The driver's child can briefly remain a zombie before being reaped.
            stat = Path(f"/proc/{self.pid}/stat").read_text()
            return stat.rsplit(")", 1)[1].strip().startswith("Z")
        except FileNotFoundError:
            return True

    def open(self) -> bool:
        if self.exited():
            return False
        if os.name == "nt":
            user, _ = _windows_user32()
            return bool(user.IsWindow(self.window))
        x11, display = _x11_open()
        try:
            return self.window in _x11_children(x11, display)
        finally:
            x11.XCloseDisplay(display)


def _windows_user32():
    """Keep HWNDs and LPARAMs pointer-sized on the Windows x64 test runner."""
    from ctypes import POINTER, WinDLL, WINFUNCTYPE, c_ssize_t
    from ctypes.wintypes import BOOL, DWORD, HWND, INT, LPARAM, LPWSTR, RECT, UINT, WPARAM
    user = WinDLL("user32", use_last_error=True)
    callback = WINFUNCTYPE(BOOL, HWND, LPARAM)
    signatures = {
        "EnumWindows": ([callback, LPARAM], BOOL),
        "GetWindowThreadProcessId": ([HWND, POINTER(DWORD)], DWORD),
        "IsWindow": ([HWND], BOOL), "IsWindowVisible": ([HWND], BOOL),
        "GetAncestor": ([HWND, UINT], HWND), "GetWindow": ([HWND, UINT], HWND),
        "GetWindowLongPtrW": ([HWND, INT], c_ssize_t),
        "GetWindowTextLengthW": ([HWND], INT), "GetWindowTextW": ([HWND, LPWSTR, INT], INT),
        "GetClassNameW": ([HWND, LPWSTR, INT], INT), "GetClientRect": ([HWND, POINTER(RECT)], BOOL),
        "PostMessageW": ([HWND, UINT, WPARAM, LPARAM], BOOL),
    }
    for name, (arguments, result) in signatures.items():
        function = getattr(user, name)
        function.argtypes, function.restype = arguments, result
    return user, callback


def _select_windows_main_window(windows: list[dict], pid: int) -> int:
    owned = [window for window in windows if window["pid"] == pid]
    main = [window for window in owned if window["visible"] and window["is_root"] and not window["owned_popup"]
            and window["caption"] and not window["child"] and not window["tool"] and window["title_matches"]
            and window["class_name"] == "Tauri Window"
            and window["client_width"] >= 300 and window["client_height"] >= 200]
    if len(main) != 1:
        if os.environ.get("GITHUB_ACTIONS") == "true":
            # Fixed labels and numeric counts only: this annotation is public.
            counts = {"owned_total": len(owned), "owned_visible": sum(window["visible"] for window in owned),
                "titled_trivium": sum(window["title_matches"] for window in owned),
                "unowned": sum(not window["owned_popup"] for window in owned),
                "caption": sum(window["caption"] for window in owned),
                "class_matches": sum(window["class_name"] == "Tauri Window" for window in owned),
                "candidates": len(main)}
            print("::error title=Native window lookup::" + ", ".join(f"{key}={value}" for key, value in counts.items()), flush=True)
        # Never expose native window titles, which may contain document names.
        diagnostic = {"owned_total": len(owned), "owned_visible": sum(window["visible"] for window in owned),
            "matching_main": len(main), "windows": [{
                **{key: window[key] for key in ("visible", "is_root", "owned_popup", "caption", "child", "tool", "title_matches", "client_width", "client_height")},
                "class_name": window["class_name"] if re.fullmatch(r"[A-Za-z0-9_. -]{1,80}", window["class_name"]) else "unrecognized",
            } for window in owned[:16]]}
        raise AssertionError(f"Expected one owned native main window: {json.dumps(diagnostic, separators=(',', ':'))}")
    return main[0]["handle"]


def _windows_main_window(pid: int) -> int:
    from ctypes import byref, create_unicode_buffer, get_last_error
    from ctypes.wintypes import DWORD, RECT
    user, callback_type = _windows_user32()
    windows, errors = [], []
    @callback_type
    def visit(window, _):
        try:
            owner = DWORD()
            user.GetWindowThreadProcessId(window, byref(owner))
            if owner.value != pid:
                return True
            title = create_unicode_buffer(min(user.GetWindowTextLengthW(window) + 1, 4096))
            user.GetWindowTextW(window, title, len(title))
            classname = create_unicode_buffer(256)
            user.GetClassNameW(window, classname, len(classname))
            client = RECT()
            geometry = user.GetClientRect(window, byref(client))
            style, extended = user.GetWindowLongPtrW(window, -16), user.GetWindowLongPtrW(window, -20)
            windows.append({"handle": int(window), "pid": owner.value, "visible": bool(user.IsWindowVisible(window)),
                "is_root": user.GetAncestor(window, 2) == window, "owned_popup": bool(user.GetWindow(window, 4)),
                "caption": style & 0x00c00000 == 0x00c00000, "child": bool(style & 0x40000000), "tool": bool(extended & 0x80),
                "title_matches": title.value == "Trivium", "class_name": classname.value,
                "client_width": client.right - client.left if geometry else 0,
                "client_height": client.bottom - client.top if geometry else 0})
            return True
        except Exception as error:
            errors.append(type(error).__name__)
            return False
    enumerated = user.EnumWindows(visit, 0)
    assert not errors, f"Owned native window probe failed: {errors}"
    if not enumerated:
        raise OSError(get_last_error(), "Could not enumerate owned native windows")
    return _select_windows_main_window(windows, pid)


def _x11_open():
    from ctypes import CDLL, c_char_p, c_int, c_ulong, c_void_p
    x11 = CDLL("libX11.so.6")
    x11.XOpenDisplay.argtypes, x11.XOpenDisplay.restype = [c_char_p], c_void_p
    x11.XCloseDisplay.argtypes = [c_void_p]
    x11.XDefaultRootWindow.argtypes, x11.XDefaultRootWindow.restype = [c_void_p], c_ulong
    x11.XInternAtom.argtypes, x11.XInternAtom.restype = [c_void_p, c_char_p, c_int], c_ulong
    x11.XFree.argtypes = [c_void_p]
    x11.XSync.argtypes = [c_void_p, c_int]
    display = x11.XOpenDisplay(None)
    assert display, "Normal Linux close requires the owned Xvfb display"
    return x11, display


def _x11_children(x11, display) -> list[int]:
    from ctypes import POINTER, byref, c_int, c_uint, c_ulong, c_void_p
    parent, root, count, children = c_ulong(), c_ulong(), c_uint(), POINTER(c_ulong)()
    x11.XQueryTree.argtypes = [c_void_p, c_ulong, POINTER(c_ulong), POINTER(c_ulong), POINTER(POINTER(c_ulong)), POINTER(c_uint)]
    x11.XQueryTree.restype = c_int
    assert x11.XQueryTree(display, x11.XDefaultRootWindow(display), byref(root), byref(parent), byref(children), byref(count))
    try:
        return [int(children[index]) for index in range(count.value)]
    finally:
        if children:
            x11.XFree(children)


def _x11_window() -> tuple[int, int]:
    from ctypes import POINTER, byref, c_char_p, c_int, c_long, c_ubyte, c_ulong, c_void_p, cast
    x11, display = _x11_open()
    found = []
    try:
        x11.XFetchName.argtypes = [c_void_p, c_ulong, POINTER(c_void_p)]
        x11.XGetWindowProperty.argtypes = [c_void_p, c_ulong, c_ulong, c_long, c_long, c_int, c_ulong,
                                          POINTER(c_ulong), POINTER(c_int), POINTER(c_ulong), POINTER(c_ulong), POINTER(POINTER(c_ubyte))]
        pid_atom = x11.XInternAtom(display, b"_NET_WM_PID", 1)
        assert pid_atom, "The owned GTK window must identify its process"
        for window in _x11_children(x11, display):
            name = c_void_p()
            if not x11.XFetchName(display, window, byref(name)) or not name:
                continue
            try:
                if cast(name, c_char_p).value != b"Trivium":
                    continue
            finally:
                x11.XFree(name)
            data, actual, kind, count, remaining = POINTER(c_ubyte)(), c_ulong(), c_int(), c_ulong(), c_ulong()
            status = x11.XGetWindowProperty(display, window, pid_atom, 0, 1, 0, 0,
                byref(actual), byref(kind), byref(count), byref(remaining), byref(data))
            try:
                if status == 0 and kind.value == 32 and count.value == 1 and data:
                    pid = int(cast(data, POINTER(c_ulong))[0])
                    executable = Path(f"/proc/{pid}/exe").resolve().name
                    if executable.startswith("trivium-desktop"):
                        found.append((window, pid))
            finally:
                if data:
                    x11.XFree(data)
        assert len(found) == 1, f"Expected one owned Trivium window in Xvfb, found {len(found)}"
        return found[0]
    finally:
        x11.XCloseDisplay(display)


def native_driver_default() -> str:
    name = "msedgedriver" if os.name == "nt" else "WebKitWebDriver"
    return os.environ.get("TRIVIUM_NATIVE_DRIVER") or shutil.which(name) or name


def isolated_environment(root: Path) -> dict[str, str]:
    directories = {name: root / name for name in ("data", "config", "cache")}
    for directory in directories.values():
        directory.mkdir(parents=True, exist_ok=True)
    return dict(os.environ, XDG_DATA_HOME=str(directories["data"]),
                XDG_CONFIG_HOME=str(directories["config"]), XDG_CACHE_HOME=str(directories["cache"]),
                XDG_RUNTIME_DIR=str(root), APPDATA=str(directories["config"]),
                LOCALAPPDATA=str(directories["data"]),
                GSETTINGS_BACKEND="memory", NO_AT_BRIDGE="1")


def context_click(web, point: dict[str, int]) -> str:
    """Use real right-button input; WebKitWebDriver releases it as a left click."""
    if os.name == "nt":
        web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "native-context",
            "parameters": {"pointerType": "mouse"}, "actions": [
                {"type": "pointerMove", **point, "duration": 0, "origin": "viewport"},
                {"type": "pointerDown", "button": 2}, {"type": "pointerUp", "button": 2}]}]})
        return "WebDriver right-button input"
    # Linux checks run in bare Xvfb: window and content origins coincide.
    # XTest preserves the actual press/release buttons, without DOM dispatch.
    from ctypes import CDLL, c_char_p, c_int, c_ulong, c_void_p
    x11, xtest = CDLL("libX11.so.6"), CDLL("libXtst.so.6")
    x11.XOpenDisplay.argtypes, x11.XOpenDisplay.restype = [c_char_p], c_void_p
    x11.XSync.argtypes = [c_void_p, c_int]
    x11.XCloseDisplay.argtypes = [c_void_p]
    xtest.XTestFakeMotionEvent.argtypes = [c_void_p, c_int, c_int, c_int, c_ulong]
    xtest.XTestFakeButtonEvent.argtypes = [c_void_p, c_int, c_int, c_ulong]
    display = x11.XOpenDisplay(None)
    assert display, "The native context-click check requires Xvfb and libXtst6"
    try:
        rect = web.request("GET", web.route("window/rect"))
        assert xtest.XTestFakeMotionEvent(display, -1, rect["x"] + point["x"], rect["y"] + point["y"], 0)
        assert xtest.XTestFakeButtonEvent(display, 3, 1, 0)
        assert xtest.XTestFakeButtonEvent(display, 3, 0, 0)
        x11.XSync(display, 0)
    finally:
        x11.XCloseDisplay(display)
    return "XTest native right-button input"


def start_driver(command: list[str], environment: dict[str, str], log) -> subprocess.Popen:
    return subprocess.Popen(command, env=environment, stdout=log, stderr=log,
                            start_new_session=os.name != "nt",
                            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0)


def stop_driver(driver: subprocess.Popen) -> None:
    if driver.poll() is not None:
        return
    if os.name == "nt":
        # Only terminate the process tree created by this test, including its
        # own WebView2/browser subprocesses that may retain temporary files.
        subprocess.run(["taskkill", "/PID", str(driver.pid), "/T", "/F"],
                       check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        driver.wait(timeout=10)
        return
    os.killpg(driver.pid, signal.SIGTERM)
    try:
        driver.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(driver.pid, signal.SIGKILL)
        driver.wait(timeout=5)


class ApplicationSession:
    """Launch Windows explicitly and attach to its known localhost DevTools port.

    Microsoft documents this WebView2 attach mode. It avoids depending on a
    driver's implicit DevToolsActivePort/profile-directory discovery. Nothing is
    enabled in production: these variables exist only in this owned test app.
    Linux retains tauri-driver's WebKit launch capabilities.
    """
    def __init__(self, binary: Path, environment: dict[str, str], evidence: Path,
                 *, platform: str | None = None):
        self.process = None
        self.log = None
        self.log_path = None
        self.platform = platform or os.name
        self.details: dict = {"mode": "driver-launch"}
        self.capabilities = {"tauri:options": {"application": str(binary)}}
        if (platform or os.name) != "nt":
            return
        if binary.parent.name.casefold() != "debug":
            raise AssertionError("Windows native checks require a dedicated cargo debug test binary, not a production executable")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        app_environment = dict(environment)
        profile = Path(environment["LOCALAPPDATA"]) / "native-webview-profile"
        profile.mkdir(parents=True, exist_ok=True)
        # The dedicated debug binary applies both through WebviewWindowBuilder.
        # Host WebView2 overrides could otherwise change this endpoint/profile.
        app_environment.pop("WEBVIEW2_USER_DATA_FOLDER", None)
        app_environment.pop("WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS", None)
        app_environment["TRIVIUM_NATIVE_PROFILE"] = str(profile)
        app_environment["TRIVIUM_NATIVE_DEBUG_PORT"] = str(port)
        app_environment["TAURI_WEBVIEW_AUTOMATION"] = "true"
        runtime = app_environment.get("TRIVIUM_WEBVIEW2_RUNTIME_DIRECTORY")
        if runtime:
            if not (Path(runtime) / "msedgewebview2.exe").is_file():
                raise AssertionError("Selected WebView2 runtime is missing; inspect the driver installation step")
            app_environment["WEBVIEW2_BROWSER_EXECUTABLE_FOLDER"] = runtime
        evidence.mkdir(parents=True, exist_ok=True)
        self.log_path = evidence / "application.log"
        self.log = self.log_path.open("a", encoding="utf-8")
        try:
            self.process = start_driver([str(binary)], app_environment, self.log)
            version = self._wait_for_debugging(port)
            self.details = {"mode": "webview2-attach", "browser": version.get("Browser", "unknown")}
            self.capabilities = {"browserName": "webview2", "ms:edgeChromium": True,
                                 "ms:edgeOptions": {"debuggerAddress": f"127.0.0.1:{port}"}}
        except BaseException:
            self.close()
            raise

    def _probe(self, port: int, endpoint: str, diagnostic: dict):
        diagnostic[f"{endpoint}_http"] = None
        diagnostic.pop(f"{endpoint}_error_number", None)
        try:
            with _open_debugger(f"http://127.0.0.1:{port}/json/{endpoint}", timeout=2) as response:
                diagnostic[f"{endpoint}_http"] = response.status
                value = json.load(response)
            diagnostic.pop(f"{endpoint}_error", None)
            return value
        except HTTPError as error:
            diagnostic[f"{endpoint}_http"] = error.code
            diagnostic[f"{endpoint}_error"] = "HTTPError"
        except (OSError, URLError, ValueError) as error:
            reason = error.reason if isinstance(error, URLError) else error
            diagnostic[f"{endpoint}_error"] = type(reason).__name__
            number = getattr(reason, "winerror", None) or getattr(reason, "errno", None)
            if isinstance(number, int):
                diagnostic[f"{endpoint}_error_number"] = number
        return None

    def _wait_for_debugging(self, port: int, timeout: float = 45) -> dict:
        deadline = time.monotonic() + timeout
        diagnostic = {}
        while time.monotonic() < deadline:
            code = self.process.poll()
            if code is not None:
                raise AssertionError(f"Native app exited with code {code} before WebView2 was ready; inspect application.log")
            version = self._probe(port, "version", diagnostic)
            pages = self._probe(port, "list", diagnostic)
            if isinstance(version, dict):
                browser = version.get("Browser", "")
                diagnostic["browser"] = browser if isinstance(browser, str) and re.fullmatch(r"[A-Za-z0-9./ _-]{1,80}", browser) else "unrecognized"
            if isinstance(pages, list):
                diagnostic["target_count"] = len(pages)
                diagnostic["target_types"] = sorted({
                    target.get("type") if target.get("type") in {"page", "webview", "iframe", "worker", "service_worker", "background_page"} else "other"
                    for target in pages if isinstance(target, dict)
                })
            if debuggable_webview(version, pages):
                return version
            time.sleep(.05)
        details = json.dumps(diagnostic, sort_keys=True, separators=(",", ":"))
        startup = startup_diagnostics(self.process, getattr(self, "log_path", None),
            platform=getattr(self, "platform", None), debug_port=port)
        evidence = json.dumps(startup, separators=(",", ":"))
        raise AssertionError(f"Native app did not publish a debuggable WebView2 target within {timeout:g} seconds; probes={details}; startup={evidence}; inspect application.log")

    def close(self) -> None:
        try:
            if self.process is not None:
                stop_driver(self.process)
                self.process = None
        finally:
            if self.log is not None:
                self.log.close()
                self.log = None


def _open_debugger(url: str, *, timeout: float):
    # Only our owned loopback CDP endpoint uses this opener; do not alter the
    # process proxy configuration or external requests used by other tests.
    return build_opener(ProxyHandler({})).open(url, timeout=timeout)


def debuggable_webview(version, targets) -> bool:
    """Attach uses an actual debuggable view; browser-level WebSocket is optional."""
    if not isinstance(version, dict) or not isinstance(version.get("Browser"), str) or not version["Browser"]:
        return False
    if not isinstance(targets, list):
        return False
    return any(isinstance(target, dict) and target.get("type") in ("page", "webview")
               and isinstance(target.get("webSocketDebuggerUrl"), str)
               and urlsplit(target["webSocketDebuggerUrl"]).scheme in ("ws", "wss")
               for target in targets)


def emit_failure_annotation(error: Exception) -> None:
    """Expose bounded startup/protocol evidence in CI, never full native logs."""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    text = str(error)
    if text.startswith(("Native app ", "Windows native checks ", "Selected WebView2 runtime ")):
        message = text[:3000]
    elif text.startswith("WebDriver ") and ": {" in text:
        try:
            value = json.loads(text[text.index(": {") + 2:])["value"]
            message = f"Native WebDriver {value.get('error', 'failure')}: {value.get('message', '')}"[:1200]
        except (ValueError, KeyError, TypeError):
            message = "Native WebDriver request failed; inspect the native artifact report."
    else:
        message = f"Native smoke failed ({type(error).__name__}); inspect report.json and screenshots in the native artifact."
    message = re.sub(r"(https?://)[^/@\s]+:[^/@\s]+@", r"\1<redacted>@", message)
    message = re.sub(r"(?:github_pat_|gh[pousr]_)[A-Za-z0-9_]+", "<redacted>", message)
    message = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::error title=Native smoke::{message}", flush=True)


# Both are real Tauri transports. Windows WebView2 uses the HTTP host variant.
IPC_COMMAND_JS = r"""
            const raw=typeof input==='string'?input:input?.url??input?.href??'';
            let name='';
            try {
              const url=new URL(raw);
              if((url.protocol==='ipc:'&&url.hostname==='localhost')||
                 (url.protocol==='http:'&&url.hostname==='ipc.localhost'))
                name=decodeURIComponent(url.pathname.slice(1));
            } catch {}
"""
