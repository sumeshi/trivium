"""External file drags use an owned GTK process and real XTest pointer input."""
from __future__ import annotations

from ctypes import CDLL, c_char_p, c_int, c_ulong, c_void_p
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

from feedback_checks import _settle
from fixture_cleanup import delete_fixture_project
from row_navigation import wait_for
from runtime import IPC_COMMAND_JS


class GtkFileSource:
    """One external gesture per process, including canceled GTK drags."""

    def __init__(self, paths: list[Path], evidence: Path, token: str):
        self.events: list[dict] = []
        self.log = (evidence / f"gtk-file-source-{token}.log").open("w")
        self.process = subprocess.Popen([sys.executable, str(Path(__file__).with_name("gtk_drag_source.py")),
            "--x", "960", "--y", "110", *[str(path) for path in paths]],
            stdout=subprocess.PIPE, stderr=self.log, text=True)

        def read():
            for line in self.process.stdout:
                try:
                    event = json.loads(line)
                    self.events.append(event)
                    self.log.write(json.dumps(event) + "\n")
                    self.log.flush()
                except json.JSONDecodeError:
                    self.events.append({"event": "invalid-helper-output"})

        self.reader = threading.Thread(target=read, daemon=True)
        self.reader.start()
        try:
            self.point = self.wait("ready")
        except Exception:
            self.close()
            raise

    def wait(self, event: str, after: int = 0) -> dict:
        def read():
            if self.process.poll() is not None:
                raise AssertionError(f"The owned GTK file source exited ({self.process.returncode}); inspect its log")
            return next((entry for entry in self.events[after:] if entry["event"] == event), None)
        return wait_for(read, bool, f"owned GTK source {event}", timeout=6)

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        self.reader.join(timeout=1)
        self.process.stdout.close()
        self.log.close()


class NativePointer:
    def __init__(self):
        self.x11, self.xtest = CDLL("libX11.so.6"), CDLL("libXtst.so.6")
        self.x11.XOpenDisplay.argtypes, self.x11.XOpenDisplay.restype = [c_char_p], c_void_p
        self.x11.XSync.argtypes, self.x11.XCloseDisplay.argtypes = [c_void_p, c_int], [c_void_p]
        self.xtest.XTestFakeMotionEvent.argtypes = [c_void_p, c_int, c_int, c_int, c_ulong]
        self.xtest.XTestFakeButtonEvent.argtypes = [c_void_p, c_int, c_int, c_ulong]
        self.display = self.x11.XOpenDisplay(None)
        assert self.display, "External file drag requires the owned Xvfb display"
        self.down = False

    def move(self, x: int, y: int):
        assert self.xtest.XTestFakeMotionEvent(self.display, -1, x, y, 0)
        self.x11.XSync(self.display, 0)

    def press(self):
        assert self.xtest.XTestFakeButtonEvent(self.display, 1, 1, 0)
        self.down = True
        self.x11.XSync(self.display, 0)

    def release(self):
        if self.down:
            assert self.xtest.XTestFakeButtonEvent(self.display, 1, 0, 0)
            self.down = False
            self.x11.XSync(self.display, 0)

    def enter(self, source: GtkFileSource, target: dict) -> int:
        checkpoint = len(source.events)
        x, y = source.point["x"], source.point["y"]
        self.move(x, y)
        self.press()
        time.sleep(.08)
        self.move(x - 20, y)
        source.wait("drag-begin", checkpoint)
        for step in range(1, 15):
            self.move(round(x + (target["x"] - x) * step / 14), round(y + (target["y"] - y) * step / 14))
            time.sleep(.025)
        return checkpoint

    def close(self):
        try:
            self.release()
        finally:
            self.x11.XCloseDisplay(self.display)


def _overlay(web) -> dict | None:
    return web.script("""
      const e=document.querySelector('.file-drop-overlay');if(!e)return null;
      const r=e.getBoundingClientRect(),s=getComputedStyle(e);return {text:e.textContent.trim(),role:e.getAttribute('role'),
        title:e.querySelector('.file-drop-title')?.textContent.trim(),
        width:r.width,height:r.height,background:s.backgroundColor,display:s.display,opacity:s.opacity};
    """)


def _enter(web, source: GtkFileSource, pointer: NativePointer, title: str = "Drop files here") -> int:
    rect = web.request("GET", web.route("window/rect"))
    target = web.script("return {x:Math.round(innerWidth*.75),y:Math.round(innerHeight*.45)};")
    target = {"x": rect["x"] + target["x"], "y": rect["y"] + target["y"]}
    checkpoint = pointer.enter(source, target)
    visible = wait_for(lambda: _overlay(web), bool, "external drag shows the overlay before release", timeout=8)
    assert visible["title"] == title and visible["role"] == "status", visible
    assert visible["width"] >= 850 and visible["height"] >= 600 and visible["display"] != "none", visible
    assert visible["background"] not in {"transparent", "rgba(0, 0, 0, 0)"}, visible
    return checkpoint


def _cancel(web, source: GtkFileSource, pointer: NativePointer, screenshot: Path | None = None) -> None:
    checkpoint = _enter(web, source, pointer)
    if screenshot is not None:
        _settle(web)
        web.screenshot(screenshot)
    pointer.move(source.point["x"], source.point["y"])
    wait_for(lambda: _overlay(web), lambda value: value is None, "leaving the app clears the file overlay", timeout=8)
    pointer.release()
    source.wait("drag-end", checkpoint)


def _drop(web, source: GtkFileSource, pointer: NativePointer, title: str = "Drop files here") -> None:
    checkpoint = _enter(web, source, pointer, title)
    pointer.release()
    source.wait("drag-end", checkpoint)
    sent = source.wait("selection-sent", checkpoint)
    assert sent["accepted"], sent
    wait_for(lambda: _overlay(web), lambda value: value is None, "dropping files clears the overlay", timeout=8)


def _import_preserves_note(web, path: Path, pointer: NativePointer, evidence: Path, created: list[dict]) -> dict:
    active = web.state()["active"]
    source = GtkFileSource([path], evidence, f"{path.stem}-note")
    try:
        _drop(web, source, pointer)
    finally:
        source.close()
    # The shared observer delays only the decoded response of a real import.
    web.script("window.__workspaceHold={name:'create_project',ready:false,release:null};")
    try:
        web.tap(web.node(".workspace-drawer .button.primary"))
        wait_for(lambda: web.script("return !!window.__workspaceHold?.ready;"), bool, "real import finishes while its UI response is held")
        meta = next(item["meta"] for item in web.command("list_projects") if item["meta"].get("source_file_name") == path.name)
        created.append(meta)
        web.named("Close workspace menu", ".workspace-drawer")
        wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "close the import drawer while the response is pending")
        web.named("Add note for row 1", ".data-row")
        draft = "Unsaved evidence entered while another file was importing"
        web.write('.memo-dialog textarea[aria-label="Note"]', draft)
        web.script("window.__importDraftEditor=document.querySelector('.memo-dialog textarea');")
        start = len(web.trace())
        web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
        wait_for(lambda: any(entry["name"] == "list_projects" and "result" in entry for entry in web.trace()[start:]),
                 bool, "the import batch completes its real project-list refresh")
        assert web.state()["active"] == active, "Import completion must preserve the case containing the unsaved note"
        assert web.script("return window.__importDraftEditor===document.querySelector('.memo-dialog textarea')&&window.__importDraftEditor.value===arguments[0];", draft), "Keep the original note editor and its unsaved text"
        assert web.script("return !!document.querySelector(arguments[0]);", f'#project-tab-{meta["id"]}'), "The imported case remains available in a background tab"
        web.screenshot(evidence / "native-import-preserves-note.png")
        web.named("Cancel", ".memo-dialog")
        wait_for(lambda: web.script("return !document.querySelector('.memo-dialog');"), bool, "explicitly discard only the test note draft")
        web.tab(meta["id"])
        web.results(2)
        web.tab(active.removeprefix("project-tab-"))
        web.results(2)
        return {"real_import_response_delayed": True, "same_note_dom_and_text_retained": True,
                "imported_background_tab_usable": True, "draft_explicitly_discarded": True}
    finally:
        web.script("window.__workspaceHold?.release?.();delete window.__workspaceHold;delete window.__importDraftEditor;")
        if web.script("return !!document.querySelector('.memo-dialog');"):
            web.named("Cancel", ".memo-dialog")


def verify_file_drop(web, data, root: Path, evidence: Path, report: dict) -> None:
    if os.name == "nt":
        report["file_drop"] = {"skipped": "The external source uses Linux GTK/XTest"}
        report["unverified"].append("Windows OS file drag and drop")
        return
    token = uuid.uuid4().hex[:8]
    csv = root / f"drop space {token}.csv"
    jsonl = root / f"ドロップ調査 {token}.jsonl"
    note_import = root / f"drop-note-{token}.csv"
    unsupported = root / f"drop unsupported {token}.txt"
    csv.write_text("host\tmessage\nalpha\tone\nbeta\ttwo\n", encoding="utf-8")
    jsonl.write_text('{"host":"gamma","message":"three"}\n{"host":"delta","message":"four"}\n', encoding="utf-8")
    note_import.write_text("host,message\nnew,one\nnew,two\n", encoding="utf-8")
    unsupported.write_text("Not a supported data file", encoding="utf-8")
    originals = {path: path.read_bytes() for path in [csv, jsonl, note_import, unsupported]}
    created: list[dict] = []
    pointer = source = None
    web.close_drawer()
    assert web.script("""
      const previous=window.fetch;window.__fileDropAudit={pickers:0,restore:()=>{window.fetch=previous;}};
      window.fetch=function(input,options,...rest){
    """ + IPC_COMMAND_JS + """
        if(name==='plugin:dialog|open')window.__fileDropAudit.pickers++;
        return previous.call(window,input,options,...rest);
      };return true;
    """), "Observe picker calls without substituting native drag events or selection data"
    try:
        web.request("POST", web.route("window/rect"), {"x": 0, "y": 0, "width": 930, "height": 700})
        _settle(web)
        pointer = NativePointer()
        source = GtkFileSource([csv, jsonl], evidence, token)
        start = len(web.trace())
        _cancel(web, source, pointer, evidence / "native-file-drop-overlay.png")
        assert not web.script("return !!document.querySelector('.workspace-drawer');")
        source.close()
        source = GtkFileSource([csv, jsonl], evidence, f"{token}-drop")
        _drop(web, source, pointer)
        source.close()
        source = None
        expected = [str(csv), str(jsonl)]
        wait_for(lambda: web.script("return [...document.querySelectorAll('.pending-files li')].map(e=>e.title);"),
            lambda value: value == expected, "native URI list supplies both exact full paths")
        assert web.script("return [...document.querySelectorAll('.pending-files li')].map(e=>e.textContent.trim());") == [csv.name, jsonl.name]
        web.choose('select[aria-label="CSV delimiter"]', "\t")
        source = GtkFileSource([csv, jsonl], evidence, f"{token}-cancel-options")
        _cancel(web, source, pointer)
        source.close()
        source = None
        assert web.script("return document.querySelector('[aria-label=\"CSV delimiter\"]').value;") == "\t"
        assert web.script("return [...document.querySelectorAll('.pending-files li')].map(e=>e.title);") == expected
        time.sleep(.35)
        assert not any(entry["name"] == "create_project" for entry in web.trace()[start:]), "File drop stages paths for review; it must not import automatically"
        assert web.script("return window.__fileDropAudit.pickers;") == 0
        web.tap(web.node(".workspace-drawer .button.primary"))
        wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"), bool, "trusted Import and open completes both dropped files")
        web.results(2)
        projects = web.command("list_projects")
        for path, hosts in [(csv, ["alpha", "beta"]), (jsonl, ["gamma", "delta"])]:
            meta = next(item["meta"] for item in projects if item["meta"].get("source_file_name") == path.name)
            created.append(meta)
            assert meta["source_path"] == str(path) and meta["total_records"] == 2, meta
            assert meta["source_sha256"] == hashlib.sha256(originals[path]).hexdigest(), meta
            assert web.script("return !!document.querySelector(arguments[0]);", f'#project-tab-{meta["id"]}')
            web.tab(meta["id"])
            web.results(2)
            detail = web.command("load_project", {"projectId": meta["id"]}, request=True)
            columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
            host_column = columns.get("host") or columns["/host"]
            rows = web.command("query_project_rows", {"projectId": meta["id"], "offset": 0, "limit": 10})["rows"]
            assert [row["cells"][host_column]["text"] for row in rows] == hosts, rows
        requests = [entry for entry in web.trace()[start:] if entry["name"] == "create_project"]
        assert [entry["request"]["path"] for entry in requests] == expected, requests
        assert [entry["request"]["delimiter"] for entry in requests] == ["\t", ""], requests
        note_guard = _import_preserves_note(web, note_import, pointer, evidence, created)
        unsupported_start = len(web.trace())
        source = GtkFileSource([unsupported], evidence, f"{token}-unsupported")
        _drop(web, source, pointer, "Unsupported file type")
        wait_for(lambda: web.view().get("toast", ""),
            lambda value: value == "Only CSV / JSON / JSONL / NDJSON files are supported.", "unsupported dropped files show a format notice", timeout=8)
        assert not web.script("return !!document.querySelector('.workspace-drawer');")
        assert not any(entry["name"] == "create_project" for entry in web.trace()[unsupported_start:])
        assert web.script("return window.__fileDropAudit.pickers;") == 0
        assert all(path.read_bytes() == original for path, original in originals.items())
        web.screenshot(evidence / "native-external-file-drop.png")
        report["file_drop"] = {"input": "Cross-process GTK text/uri-list with XTest pointer input",
            "overlay_before_release": True, "leave_clears_overlay": True, "cancel_preserves_tab_delimiter": True,
            "space_and_unicode_paths": True, "manual_import_only": True, "actual_imported_tabs": 2,
            "csv_tab_and_jsonl_rows_verified": True, "source_bytes_unchanged": True,
            "unsupported_file_rejected": True, "native_picker_calls": 0}
        report["file_drop"]["overlay_screenshot"] = "native-file-drop-overlay.png"
        report["file_drop"]["import_preserves_note"] = note_guard
    finally:
        try:
            if pointer:
                pointer.close()
        finally:
            if source:
                source.close()
        web.script("window.__fileDropAudit?.restore();delete window.__fileDropAudit;")
        if web.script("return !!document.querySelector('.workspace-drawer');"):
            web.named("Close workspace menu", ".workspace-drawer")
        web.request("POST", web.route("window/rect"), {"width": 1280, "height": 827})
        web.tab(data["id"])
        web.results(data["total"])
        for meta in created:
            if web.script("return !!document.querySelector(arguments[0]);", f'#project-tab-{meta["id"]}'):
                web.named(f'Close {meta["source_file_name"]}')
            delete_fixture_project(web, meta["id"])
