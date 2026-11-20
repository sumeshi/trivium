"""Normal OS-window close must drain accepted work before the process exits.

Only selected save/export IPC dispatches and file-picker responses are held.
Every successful write/export and post-restart read uses the actual engine.
"""
from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import sqlite3
import sys
import uuid

from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS, NormalWindowClose
from storage_responsiveness_checks import _case_database


NOTE = "Pending close: <alice@example.org> <CommandLine>whoami</CommandLine>"


def _new_case(web, root: Path, label: str) -> tuple[dict, Path]:
    source = root / f"close-{label}-{uuid.uuid4().hex[:8]}.csv"
    source.write_text("source_id,message\nr-0000000,synthetic close\n", encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    return case, source


def _open_case(web, case: dict) -> None:
    open_project(web, case["meta"]["name"])
    # Every close fixture has one row. The outgoing case's matching count is
    # not evidence that this case's query or editable row has loaded.
    web.applied(projectId=case["meta"]["id"])
    web.results(1)


def _rows(web, owner: str) -> list[dict]:
    return web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 2})["rows"]


def _entry(root: Path, owner: str) -> dict:
    with sqlite3.connect(_case_database(root, owner), timeout=.2) as database:
        row = database.execute("SELECT entry FROM annotations WHERE row_id=0").fetchone()
        return json.loads(row[0]) if row else {}


def _install_transport(web, holds: list[tuple[str, str]]) -> None:
    web.script("""
      const previous=window.fetch;
      const state=window.__normalClose={holds:arguments[0].map(([name,owner])=>({name,owner,ready:false,release:null})),
        events:[],path:null,pickers:0,failNext:false,restore:()=>{window.fetch=previous;}};
      window.fetch=(input,options,...rest)=>{
    """ + IPC_COMMAND_JS + """
        if(name==='plugin:dialog|save'&&state.path!==null){
          const path=state.path;state.path=null;state.pickers++;
          const response=()=>new Response(JSON.stringify(path),
            {headers:{'Content-Type':'application/json','Tauri-Response':'ok'}});
          const picker=state.holds.find(hold=>hold.name===name&&!hold.ready);
          if(picker){picker.ready=true;return new Promise(resolve=>{picker.release=()=>{
            if(!picker.release)return;picker.release=null;resolve(response());
          };});}
          return Promise.resolve(response());
        }
        let args={};try{args=JSON.parse(typeof options?.body==='string'?options.body:new TextDecoder().decode(options.body));}catch{}
        const payload=args.payload??args.request??{};
        if(['update_flag','export_project'].includes(name))state.events.push({name,owner:payload.projectId});
        if(name==='update_flag'&&state.failNext){
          state.failNext=false;
          return Promise.resolve(new Response(JSON.stringify('Synthetic annotation write failure'),
            {headers:{'Content-Type':'application/json','Tauri-Response':'error'}}));
        }
        const held=state.holds.find(hold=>hold.name===name&&hold.owner===payload.projectId&&!hold.ready);
        if(held){held.ready=true;return new Promise(resolve=>{held.release=()=>{
          if(!held.release)return;held.release=null;resolve(previous.call(window,input,options,...rest));
        };});}
        return previous.call(window,input,options,...rest);
      };
    """, holds)


def _held(web, owner: str, name="update_flag", *, checkpoint: str) -> None:
    assert checkpoint in {"inactive flag", "active flag", "export", "picker"}
    assert name in {"update_flag", "export_project", "plugin:dialog|save"}
    print(f"CLOSE CHECK: {checkpoint} ({name})", flush=True)
    observation = {}

    def observed() -> dict:
        nonlocal observation
        observation = web.script("""
          const state=window.__normalClose,owner=arguments[0],name=arguments[1],
            matching=state?.holds.filter(h=>h.owner===owner&&h.name===name)??[];
          return {installed:!!state,held:matching.some(h=>h.ready&&!!h.release),
            matchingHolds:matching.length,readyHolds:matching.filter(h=>h.ready).length,
            selectedOwner:document.querySelector('[role=tab][aria-selected=true]')?.id==='project-tab-'+owner,
            loadedRows:document.querySelectorAll('.data-row:not(.loading)').length,
            exportDialogVisible:!!document.querySelector('.export-dialog')?.getClientRects().length,
            pickerPathPending:!!state&&state.path!==null,pickerCalls:state?.pickers??0,
            appInert:!!document.querySelector('.app-shell')?.inert,
            saveFailedToastPresent:/Failed to (save|export)|Export stopped|app is closing/.test(
              document.querySelector('.toast.error')?.textContent??''),
            matchingEvents:(state?.events??[]).filter(e=>e.owner===owner&&e.name===name).length};
        """, owner, name)
        return observation

    try:
        wait_for(observed, lambda state: state["held"],
                 f"{checkpoint}: accepted {name} is held before native dispatch")
    except AssertionError:
        if os.environ.get("GITHUB_ACTIONS") == "true":
            safe = {"checkpoint": checkpoint, "operation": name, **observation}
            print("::error title=Native close checkpoint::" + json.dumps(safe, sort_keys=True), flush=True)
        raise


def _release(web, owner: str, name="update_flag") -> None:
    web.script("window.__normalClose.holds.find(h=>h.owner===arguments[0]&&h.name===arguments[1])?.release?.();", owner, name)


def _submit_export(web) -> None:
    web.named("Export rows")
    # The modal is inserted by that click. Hit-test its laid-out button before
    # sending a real pointer action, then observe that submission took effect.
    button = wait_for(lambda: web.script("""
      const dialog=document.querySelector('.export-dialog');
      return dialog?.getClientRects().length
        ? [...dialog.querySelectorAll('button')].find(button=>button.textContent.trim()==='Export…'
            &&!button.disabled&&button.getClientRects().length)??null : null;
    """), bool, "the export dialog has a visible enabled submit button")
    web.tap(button)
    wait_for(lambda: web.script("return !document.querySelector('.export-dialog');"),
             bool, "the real export click submits and dismisses the dialog")


def _save_note(web, text=NOTE) -> None:
    web.named("Add note for row 1", ".data-row")
    web.write('.memo-dialog textarea[aria-label="Note"]', text)
    web.write('.memo-dialog .tag-input', "close-review")
    web.named("Save", ".memo-dialog")
    wait_for(lambda: web.script("return !!document.querySelector('.memo-dialog textarea')?.disabled;"),
             bool, "the explicitly submitted note waits on the held save")


def _closing(web) -> None:
    wait_for(lambda: web.script("return !!document.querySelector('.app-shell')?.inert;"),
             bool, "normal close freezes new UI operations while accepted saves drain")


def _restart(app, target: NormalWindowClose, diagnostic: dict):
    wait_for(target.exited, bool, "normal OS close exits after accepted work completes")
    # Driver cleanup follows the observed exit; it cannot substitute for it.
    app.end_session(captured_diagnostics=diagnostic)
    return app.start()


def _pending_tabs(app, data: dict, root: Path, report: dict) -> list[tuple[dict, Path]]:
    web = app.web
    first, first_source = _new_case(web, root, "inactive")
    second, second_source = _new_case(web, root, "note")
    a, b = first["meta"]["id"], second["meta"]["id"]
    _open_case(web, first)
    _install_transport(web, [("update_flag", a), ("update_flag", b)])
    web.named("Safe", ".data-row"); _held(web, a, checkpoint="inactive flag")
    web.named("Critical", ".data-row")
    web.named(f"Close {first_source.name}")
    assert not web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{a}")
    _open_case(web, second)
    web.named("Suspicious", ".data-row"); _held(web, b, checkpoint="active flag")
    _save_note(web)
    target = NormalWindowClose(app.application)
    target.request(); _closing(web)
    assert target.open(), "Pending writes must keep the native window alive"
    _release(web, a)
    wait_for(lambda: _entry(root, a).get("flag"), lambda value: value == "critical", "the closed tab's accepted flag saves complete")
    assert target.open(), "The other case's accepted note still blocks close"
    diagnostic = web.diagnostics()
    _release(web, b)
    web = _restart(app, target, diagnostic)
    _open_case(web, first)
    assert _rows(web, a)[0]["flag"] == "critical"
    _open_case(web, second)
    entry = _rows(web, b)[0]
    assert entry["flag"] == "suspicious" and entry["memo"] == NOTE and entry["tags"] == ["close-review"], entry
    report["normal_close_pending_tabs"] = {"closed_tab_saved": True, "active_flag_and_submitted_note_saved": True,
        "process_exited_normally": True, "same_profile_restart_verified": True}
    return [(first, first_source), (second, second_source)]


def _pending_export(app, root: Path, report: dict) -> tuple[dict, Path]:
    web = app.web
    case, source = _new_case(web, root, "export")
    owner = case["meta"]["id"]
    _open_case(web, case)
    destination = root / "normal-close-export.csv"
    _install_transport(web, [("export_project", owner)])
    web.script("window.__normalClose.path=arguments[0];", str(destination))
    _submit_export(web)
    _held(web, owner, "export_project", checkpoint="export")
    web.named("Critical", ".data-row")
    _save_note(web)
    target = NormalWindowClose(app.application)
    target.request(); _closing(web)
    assert target.open() and not destination.exists()
    assert web.script("return window.__normalClose.events.filter(e=>e.owner===arguments[0]).map(e=>e.name);", owner) == ["export_project"], "Later saves cannot overtake the accepted export"
    diagnostic = web.diagnostics()
    _release(web, owner, "export_project")
    web = _restart(app, target, diagnostic)
    with destination.open(encoding="utf-8", newline="") as handle:
        exported = list(csv.DictReader(handle))
    assert len(exported) == 1 and exported[0]["trivium-critical"] == "0" and exported[0]["trivium-memo"] == "", exported
    _open_case(web, case)
    entry = _rows(web, owner)[0]
    assert entry["flag"] == "critical" and entry["memo"] == NOTE, entry
    report["normal_close_pending_export"] = {"export_finished_before_later_saves": True,
        "export_keeps_its_ordered_snapshot": True, "later_flag_and_note_persisted": True,
        "process_exited_normally": True, "same_profile_restart_verified": True}
    return case, source


def _pending_picker(app, root: Path, report: dict) -> tuple[dict, Path]:
    web = app.web
    case, source = _new_case(web, root, "picker")
    owner = case["meta"]["id"]
    _open_case(web, case)
    destination = root / "normal-close-picker.csv"
    _install_transport(web, [("plugin:dialog|save", owner)])
    web.script("window.__normalClose.path=arguments[0];", str(destination))
    _submit_export(web)
    _held(web, owner, "plugin:dialog|save", checkpoint="picker")
    assert web.script("return window.__normalClose.events;") == []
    target = NormalWindowClose(app.application)
    target.request(); _closing(web)
    assert target.open() and not destination.exists()
    assert web.script("return window.__normalClose.events;") == [], "The accepted export is still awaiting the picker, before backend enqueue"
    diagnostic = web.diagnostics()
    _release(web, owner, "plugin:dialog|save")
    web = _restart(app, target, diagnostic)
    with destination.open(encoding="utf-8", newline="") as handle:
        exported = list(csv.DictReader(handle))
    assert len(exported) == 1 and exported[0]["source_id"] == "r-0000000" and exported[0]["trivium-critical"] == "0", exported
    _open_case(web, case)
    report["normal_close_pending_picker"] = {"accepted_export_picker_waited_before_backend_enqueue": True,
        "real_csv_written_after_picker_release": True, "process_exited_normally": True,
        "same_profile_restart_verified": True}
    return case, source


def _failed_save(app, root: Path, report: dict, *, sqlite_failure=False) -> tuple[dict, Path]:
    web = app.web
    case, source = _new_case(web, root, "sqlite-failure" if sqlite_failure else "failure")
    owner = case["meta"]["id"]
    web.command("update_flag", {"projectId": owner, "row_index": 0, "flag": "critical"})
    _open_case(web, case)
    database = None
    start = len(web.trace())
    try:
        if sqlite_failure:
            # Force a real failed native write, rather than only a frontend
            # rejection. This also exercises the native worker's error ledger.
            web.async_script("const done=arguments[arguments.length-1];setTimeout(()=>done(true),350);")
            database = sqlite3.connect(_case_database(root, owner), timeout=.1, isolation_level=None)
            database.execute("BEGIN IMMEDIATE")
        else:
            _install_transport(web, [])
            web.script("window.__normalClose.failNext=true;")
        web.named("Safe", ".data-row")
        wait_for(lambda: web.script("return document.querySelector('.toast.error')?.textContent??'';"),
                 lambda text: "Failed to save flag" in text, "the deliberately failed save is reported")
    finally:
        if database is not None:
            database.execute("ROLLBACK")
            database.close()
    if sqlite_failure:
        failed = [entry for entry in web.trace()[start:] if entry["name"] == "update_flag"
                  and entry["request"].get("projectId") == owner and entry.get("error")]
        assert len(failed) == 1 and any(word in failed[0]["error"].lower() for word in ["locked", "busy"]), failed
        report.setdefault("boundary_expected_errors", []).append({"name": failed[0]["name"],
            "request": failed[0]["request"], "error": failed[0]["error"]})
    target = NormalWindowClose(app.application)
    target.request()
    wait_for(lambda: web.script("return document.querySelector('#app-close-message')?.textContent??'';"),
             lambda text: "before closing again" in text,
             "a failed save keeps the native window open and asks the user to review")
    assert target.open() and web.script("return document.querySelector('.app-shell').inert;")
    web.named("Keep open", ".close-dialog")
    wait_for(lambda: web.script("return !document.querySelector('.close-dialog')&&!document.querySelector('.app-shell').inert;"),
             bool, "the user keeps the app open after reviewing the save failure")
    assert _rows(web, owner)[0]["flag"] == "critical"

    # A draft that was never submitted must not be auto-saved on close.
    if not sqlite_failure:
        web.named("Add note for row 1", ".data-row")
        draft = "Unsubmitted draft must remain in the editor"
        web.write('.memo-dialog textarea[aria-label="Note"]', draft)
        target.request()
        wait_for(lambda: web.script("return {inert:document.querySelector('.app-shell')?.inert,text:document.querySelector('.toast.error')?.textContent??''};"),
                 lambda state: not state["inert"] and "Save or cancel your note before closing." in state["text"],
                 "an unsubmitted note draft prevents close without being saved")
        assert target.open() and web.script("return document.querySelector('.memo-dialog textarea').value;") == draft
        assert not (_rows(web, owner)[0]["memo"] or "")
        web.named("Cancel", ".memo-dialog")
    diagnostic = web.diagnostics()
    target.request()
    web = _restart(app, target, diagnostic)
    _open_case(web, case)
    entry = _rows(web, owner)[0]
    assert entry["flag"] == "critical" and not (entry["memo"] or ""), entry
    report["normal_close_sqlite_failure" if sqlite_failure else "normal_close_failure"] = {
        "failed_save_keeps_window_open": True, "confirmed_flag_preserved": True,
        "unsubmitted_note_blocks_close_without_autosave": not sqlite_failure, "one_explicit_close_retry_succeeds": True,
        "failure_mode": "real SQLite busy timeout" if sqlite_failure else "one rejected annotation IPC response"}
    return case, source


def verify_graceful_close(app, data: dict, root: Path, evidence: Path, report: dict) -> None:
    outer_query, outer_total = app.web.query(), app.web.state()["total"]
    created = []
    try:
        created.extend(_pending_tabs(app, data, root, report))
        created.append(_pending_picker(app, root, report))
        created.append(_pending_export(app, root, report))
        created.append(_failed_save(app, root, report))
        created.append(_failed_save(app, root, report, sqlite_failure=True))
        app.web.screenshot(evidence / "normal-close-restarted.png")
    finally:
        failed = sys.exc_info()[0] is not None
        web = app.web
        # This is test cleanup only; measured normal closes already completed.
        try:
            web.script("if(window.__normalClose){for(const h of window.__normalClose.holds)h.release?.();window.__normalClose.restore();delete window.__normalClose;}")
        except (AssertionError, OSError):
            pass
        # Preserve the original failing observation instead of waiting on UI
        # controls that may already be frozen/closed. The outer harness owns
        # process cleanup and the isolated fixture directory on failure.
        if web.session and not failed:
            if web.script("return !!document.querySelector('.memo-dialog');"):
                web.named("Cancel", ".memo-dialog")
            web.close_drawer()
            open_project(web, data["name"])
            web.applied(**outer_query); web.results(outer_total)
            for case, source in created:
                owner = case["meta"]["id"]
                if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{owner}"):
                    web.named(f"Close {source.name}")
                delete_fixture_project(web, owner)
