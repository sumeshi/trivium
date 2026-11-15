"""Native window responsiveness while a real SQLite write lock is held."""
from __future__ import annotations

from pathlib import Path
import os
import sqlite3
import time
import uuid

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
    source.write_text("source_id,message\nr-0000000,synthetic\n", encoding="utf-8")
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
        web.script("""
          const owner=arguments[0];
          const state=window.__storageProbe={results:[],frames:0,active:null};
          const run=(name,payload,label)=>window.__TAURI_INTERNALS__.invoke(name,{payload})
            .then(value=>state.results.push({label,ok:true,value}),error=>state.results.push({label,ok:false,error:String(error)}));
          run('update_flag',{projectId:owner,row_index:0,flag:'safe'},'safe');
          run('save_view_state',{projectId:owner,state:{version:1,search:'first'}},'view-first');
          run('update_flag',{projectId:owner,row_index:0,flag:'critical'},'critical');
          run('save_view_state',{projectId:owner,state:{version:1,search:'last'}},'view-last');
          const frame=()=>{state.frames++;if(window.__storageProbe===state)requestAnimationFrame(frame);};
          requestAnimationFrame(frame);
          window.__TAURI_INTERNALS__.invoke('active_jobs').then(()=>state.active=true,error=>state.active=String(error));
        """, owner)
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
        assert probe["active"] is True and probe["frames"] > 0 and probe["results"] == [], probe
        database.execute("COMMIT")
        database.close()
        database = None
        results = wait_for(lambda: web.script("return window.__storageProbe.results;"),
                           lambda rows: len(rows) == 4, "queued writes finish after releasing the SQLite lock")
        assert sorted(row["label"] for row in results) == ["critical", "safe", "view-first", "view-last"], results
        assert all(row["ok"] for row in results), results
        by_label = {row["label"]: row for row in results}
        assert by_label["safe"]["value"]["revision"] < by_label["critical"]["value"]["revision"], results
        stored = web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 1})
        assert stored["rows"][0]["flag"] == "critical", stored
        view = web.command("load_view_state", {"projectId": owner}, request=True)
        assert view["state"]["search"] == "last", view
        report["storage_responsiveness"] = {"real_sqlite_write_lock": True,
            "trusted_menu_usable_while_locked": True, "native_main_thread_probe": True,
            "other_project_read_completed": True, "ui_frames_while_locked": probe["frames"],
            "responsive_phase_seconds": elapsed, "same_project_final_flag": "critical", "same_project_final_view": "last",
            "reply_order": [row["label"] for row in results]}
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
        web.command("delete_project", {"projectId": owner}, request=True)
