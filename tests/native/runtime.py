"""Isolated profiles and process cleanup shared by Linux and Windows GUI checks."""
from __future__ import annotations

import os
import json
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
from urllib.error import URLError
from urllib.request import urlopen


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
        app_environment["WEBVIEW2_USER_DATA_FOLDER"] = str(profile)
        existing = app_environment.get("WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS", "")
        app_environment["WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS"] = (
            f"{existing} --remote-debugging-address=127.0.0.1 --remote-debugging-port={port}".strip())
        app_environment["TAURI_WEBVIEW_AUTOMATION"] = "true"
        runtime = app_environment.get("TRIVIUM_WEBVIEW2_RUNTIME_DIRECTORY")
        if runtime:
            if not (Path(runtime) / "msedgewebview2.exe").is_file():
                raise AssertionError("Selected WebView2 runtime is missing; inspect the driver installation step")
            app_environment["WEBVIEW2_BROWSER_EXECUTABLE_FOLDER"] = runtime
        evidence.mkdir(parents=True, exist_ok=True)
        self.log = (evidence / "application.log").open("a", encoding="utf-8")
        try:
            self.process = start_driver([str(binary)], app_environment, self.log)
            version = self._wait_for_debugging(port)
            self.details = {"mode": "webview2-attach", "browser": version.get("Browser", "unknown")}
            self.capabilities = {"browserName": "webview2", "ms:edgeChromium": True,
                                 "ms:edgeOptions": {"debuggerAddress": f"127.0.0.1:{port}"}}
        except BaseException:
            self.close()
            raise

    def _wait_for_debugging(self, port: int, timeout: float = 45) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            code = self.process.poll()
            if code is not None:
                raise AssertionError(f"Native app exited with code {code} before WebView2 was ready; inspect application.log")
            try:
                with urlopen(f"http://127.0.0.1:{port}/json/version", timeout=2) as response:
                    version = json.load(response)
                with urlopen(f"http://127.0.0.1:{port}/json/list", timeout=2) as response:
                    pages = json.load(response)
                if version.get("webSocketDebuggerUrl") and any(page.get("type") == "page" for page in pages):
                    return version
            except (OSError, URLError, ValueError):
                pass
            time.sleep(.05)
        raise AssertionError(f"Native app did not publish a WebView2 debugging page within {timeout:g} seconds; inspect application.log")

    def close(self) -> None:
        try:
            if self.process is not None:
                stop_driver(self.process)
                self.process = None
        finally:
            if self.log is not None:
                self.log.close()
                self.log = None


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
