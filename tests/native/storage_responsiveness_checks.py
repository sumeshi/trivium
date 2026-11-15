"""Native window responsiveness while a real SQLite write lock is held."""
from __future__ import annotations

from pathlib import Path
import os
import sqlite3
import time
import uuid

from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for


def _windows_local_app_data() -> Path:
    """Match Tauri's Known Folder lookup, which ignores LOCALAPPDATA overrides."""
    import ctypes
    from ctypes import wintypes

    class Guid(ctypes.Structure):
        _fields_ = [("data1", wintypes.DWORD), ("data2", wintypes.WORD),
                    ("data3", wintypes.WORD), ("data4", ctypes.c_ubyte * 8)]

    folder = Guid.from_buffer_copy(uuid.UUID("f1b32785-6fba-4fcf-9d55-7b8e7f157091").bytes_le)
    path = ctypes.c_wchar_p()
    shell = ctypes.WinDLL("shell32")
    shell.SHGetKnownFolderPath.argtypes = [ctypes.POINTER(Guid), wintypes.DWORD,
                                         wintypes.HANDLE, ctypes.POINTER(ctypes.c_wchar_p)]
    shell.SHGetKnownFolderPath.restype = ctypes.c_long
    result = shell.SHGetKnownFolderPath(ctypes.byref(folder), 0, None, ctypes.byref(path))
    if result != 0:
        raise OSError(f"LocalAppData Known Folder lookup failed (HRESULT 0x{result & 0xffffffff:08x})")
    try:
        return Path(path.value)
    finally:
        ole = ctypes.WinDLL("ole32")
        ole.CoTaskMemFree.argtypes = [ctypes.c_void_p]
        ole.CoTaskMemFree.restype = None
        ole.CoTaskMemFree(ctypes.cast(path, ctypes.c_void_p))


def _case_database(root: Path, owner: str, *, platform: str | None = None) -> Path:
    owner = str(uuid.UUID(owner))
    if (platform or os.name) == "nt":
        # The owned debug binary rejects automation in any other namespace.
        # Never search production cases or infer this from overridden app env.
        path = (_windows_local_app_data() / "com.trivium.native-tests" / "trivium"
                / "projects" / owner / "project.sqlite")
        assert path.is_file(), "The owned Windows automation case database was not found"
        return path
    paths = list(root.glob(f"**/projects/{owner}/project.sqlite"))
    assert len(paths) == 1, paths
    return paths[0]


def verify_storage_responsiveness(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    outer_query, outer_total = web.query(), web.state()["total"]
    source = root / f"storage-lock-{uuid.uuid4().hex[:8]}.csv"
    source.write_text("source_id,message\nr-0000000,synthetic first last\n", encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    owner = case["meta"]["id"]
    database = None
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"])
        web.results(1)
        # Allow the newly mounted view's initial debounced save to settle.
        web.async_script("const done=arguments[arguments.length-1];setTimeout(()=>done(true),350);")
        database = sqlite3.connect(_case_database(root, owner), timeout=.1, isolation_level=None)
        database.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        checkpoint = web.script("return performance.now();")
        def writes(name: str, value: str) -> list[dict]:
            return [entry for entry in web.trace() if entry["name"] == name and entry["started"] >= checkpoint
                    and entry["request"].get("projectId") == owner
                    and (entry["request"].get("flag") if name == "update_flag" else entry["request"].get("state", {}).get("search")) == value]
        web.script("""
          const state=window.__storageProbe={frames:0,active:null};
          const frame=()=>{state.frames++;if(window.__storageProbe===state)requestAnimationFrame(frame);};
          requestAnimationFrame(frame);
          window.__TAURI_INTERNALS__.invoke('active_jobs').then(()=>state.active=true,error=>state.active=String(error));
        """)
        # Exercise the actual UI producers and their per-case queues. Raw
        # concurrent invokes have no defined arrival order at an async handler.
        web.named("Safe", ".data-row")
        web.named("Critical", ".data-row")
        web.write('input[aria-label="Search rows"]', "first")
        web.applied(projectId=owner, search="first"); web.results(1)
        wait_for(lambda: writes("save_view_state", "first"), bool,
                 "the UI's first debounced view save is dispatched while the flag write waits")
        web.write('input[aria-label="Search rows"]', "last")
        web.applied(projectId=owner, search="last"); web.results(1)
        # Native pointer input must still reach the window while DB writes wait.
        web.named("Workspace options")
        wait_for(lambda: web.script("return !!document.querySelector('[aria-label=\"Application version\"]');"),
                 bool, "trusted menu click remains responsive under SQLite lock", timeout=2)
        web.named("Workspace options")
        other = web.command("load_view_state", {"projectId": data["id"]}, request=True)
        assert other["status"] in {"ok", "missing"}, other
        web.async_script("const done=arguments[arguments.length-1];setTimeout(()=>done(true),250);")
        probe = web.script("return window.__storageProbe;")
        elapsed = time.monotonic() - started
        assert elapsed < 4, f"The window waited for SQLite's five-second busy timeout: {elapsed:.3f}s"
        assert probe["active"] is True and probe["frames"] > 0, probe
        first_flag, first_view = writes("update_flag", "safe"), writes("save_view_state", "first")
        assert len(first_flag) == len(first_view) == 1
        assert all("result" not in entry and "error" not in entry for entry in first_flag + first_view)
        assert not writes("update_flag", "critical") and not writes("save_view_state", "last"), "Later UI saves wait for their producer's earlier operation"
        database.execute("COMMIT")
        database.close()
        database = None
        requests = [("update_flag", "safe"), ("update_flag", "critical"), ("save_view_state", "first"), ("save_view_state", "last")]
        results = wait_for(lambda: [writes(name, value) for name, value in requests],
            lambda groups: all(group and "result" in group[-1] for group in groups),
            "actual UI flag and view queues finish after releasing the SQLite lock")
        assert all("error" not in entry for group in results for entry in group), results
        assert results[0][0]["result"]["revision"] < results[1][0]["result"]["revision"], results
        assert results[1][0]["started"] >= results[0][0]["decode_done"], results
        assert results[3][0]["started"] >= results[2][0]["decode_done"], results
        stored = web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 1})
        assert stored["rows"][0]["flag"] == "critical", stored
        view = web.command("load_view_state", {"projectId": owner}, request=True)
        assert view["state"]["search"] == "last", view
        report["storage_responsiveness"] = {"real_sqlite_write_lock": True,
            "trusted_menu_usable_while_locked": True, "native_main_thread_probe": True,
            "other_project_read_completed": True, "ui_frames_while_locked": probe["frames"],
            "responsive_phase_seconds": elapsed, "same_project_final_flag": "critical", "same_project_final_view": "last",
            "ui_flag_and_view_producers": True, "later_saves_dispatched_after_earlier_responses": True}
        web.screenshot(evidence / "storage-lock-responsive.png")
    finally:
        if database is not None:
            database.execute("ROLLBACK")
            database.close()
        web.script("delete window.__storageProbe;")
        web.close_drawer()
        open_project(web, data["name"])
        web.applied(**outer_query)
        web.results(outer_total)
        if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{owner}"):
            web.named(f"Close {source.name}")
        delete_fixture_project(web, owner)
