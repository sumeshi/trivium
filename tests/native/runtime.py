"""Isolated profiles and process cleanup shared by Linux and Windows GUI checks."""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import signal
import subprocess


def native_driver_default() -> str:
    name = "msedgedriver" if os.name == "nt" else "WebKitWebDriver"
    return os.environ.get("TRIVIUM_NATIVE_DRIVER") or shutil.which(name) or name


def isolated_environment(root: Path) -> dict[str, str]:
    directories = {name: root / name for name in ("data", "config", "cache", "webview")}
    for directory in directories.values():
        directory.mkdir(parents=True, exist_ok=True)
    return dict(os.environ, XDG_DATA_HOME=str(directories["data"]),
                XDG_CONFIG_HOME=str(directories["config"]), XDG_CACHE_HOME=str(directories["cache"]),
                XDG_RUNTIME_DIR=str(root), APPDATA=str(directories["config"]),
                LOCALAPPDATA=str(directories["data"]), WEBVIEW2_USER_DATA_FOLDER=str(directories["webview"]),
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
