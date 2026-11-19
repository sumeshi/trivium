"""Native IOC rejection and GUI export ordering across pending annotations.

The file picker response and selected rejected save response are substituted.
Successful saves, query membership, CSV creation and readback use real Rust IPC.
"""
from __future__ import annotations

import csv
from pathlib import Path
import uuid

from preset_ioc_checks import _open_iocs
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS


def _rows(web, owner: str) -> list[dict]:
    return web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 10})["rows"]


def _rejected(web, name: str, payload: dict, expected_errors: list[dict]) -> None:
    start = len(web.trace())
    response = web.async_script("""
      const done=arguments[arguments.length-1];
      window.__TAURI_INTERNALS__.invoke(arguments[0],{payload:arguments[1]})
        .then(()=>done({ok:true}),error=>done({ok:false,reason:String(error)}));
    """, name, payload)
    assert response["ok"] is False and response["reason"], (name, response)
    entries = [entry for entry in web.trace()[start:] if entry["name"] == name and entry.get("error") == response["reason"]]
    assert len(entries) == 1, (name, entries)
    expected_errors.append({"name": name, "request": entries[0]["request"], "error": response["reason"]})


def _ioc_queries(web, owner: str, root: Path, report: dict) -> None:
    saved = [{"flag": "critical", "tag": "review", "query": "message:match"}]
    web.command("save_iocs", {"projectId": owner, "entries": saved})
    # Direct setup writes do not update the active case's owned editor draft.
    # Reload through the UI before checking its saved baseline.
    start = len(web.trace())
    web.named("Workspace options")
    web.named("Reload saved project", ".workspace-options")
    wait_for(lambda: any(entry["name"] == "load_project" and entry["request"].get("projectId") == owner
                         and "result" in entry for entry in web.trace()[start:]),
             bool, "reload the saved IOC rules into the active case editor")
    web.results(2)
    initial = _rows(web, owner)
    preview_args = {"projectId": owner, "target": "filtered", "search": "message:match", "flag": "critical"}
    token = web.command("bulk_preview", preview_args)["previewToken"]
    expected_errors = report.setdefault("boundary_expected_errors", [])
    for index, query in enumerate(["|", "||", '""']):
        invalid = [{"flag": "critical", "tag": "broken", "query": query}]
        _rejected(web, "save_iocs", {"projectId": owner, "entries": invalid}, expected_errors)
        source = root / f"invalid-query-{index}.csv"
        with source.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["flag", "tag", "query"])
            writer.writerow(["critical", "broken", query])
        _rejected(web, "import_iocs", {"projectId": owner, "path": str(source)}, expected_errors)
        args = {**preview_args, "search": query}
        _rejected(web, "bulk_preview", args, expected_errors)
        _rejected(web, "bulk_update", {**args, "previewToken": token}, expected_errors)
        assert _rows(web, owner) == initial, query
        assert web.command("load_project", {"projectId": owner}, request=True)["iocs"] == saved
    # Unsaved invalid editor rows must be visibly invalid too.
    _open_iocs(web)
    for query in ["|", "||", '""']:
        web.named("Add rule", ".ioc-panel")
        index = web.script("return document.querySelectorAll('.ioc-row').length-1;")
        web.write('.ioc-row input[aria-label^="Rule query"]', query, index)
        invalid = web.script("""
          const row=document.querySelectorAll('.ioc-row')[arguments[0]];
          return {disabled:row.querySelector('.ioc-rule-actions button').disabled,
            invalid:row.querySelector('input[aria-label^="Rule query"]').getAttribute('aria-invalid'),
            message:row.querySelector('.ioc-query-error')?.textContent??''};
        """, index)
        assert invalid["disabled"] and invalid["invalid"] == "true" and invalid["message"], invalid
    web.named("Discard", ".ioc-panel")
    assert web.script("return document.querySelectorAll('.ioc-row').length;") == len(saved)
    web.close_drawer()
    assert _rows(web, owner) == initial
    assert len(web.command("query_project_rows", {"projectId": owner, "search": "", "offset": 0, "limit": 10})["rows"]) == 2
    report["invalid_ioc_queries"] = {"queries": ["|", "||", '""'], "annotations_unchanged": True,
                                     "saved_rules_unchanged": True, "gui_apply_disabled": True,
                                     "ordinary_empty_search_returns_all": True}


def _install_export_transport(web) -> None:
    web.script("""
      const previous=window.fetch;
      window.__boundaryTransport={path:null,pickers:0,failNext:false,restore:()=>{window.fetch=previous;}};
      window.fetch=(input,options,...rest)=>{
    """ + IPC_COMMAND_JS + """
        const state=window.__boundaryTransport;
        if(name==='plugin:dialog|save'&&state.path!==null){
          const path=state.path;state.path=null;state.pickers++;
          return Promise.resolve(new Response(JSON.stringify(path),
            {headers:{'Content-Type':'application/json','Tauri-Response':'ok'}}));
        }
        if(name==='update_flag'&&state.failNext){
          state.failNext=false;
          return Promise.resolve(new Response(JSON.stringify('Synthetic annotation write failure'),
            {headers:{'Content-Type':'application/json','Tauri-Response':'error'}}));
        }
        return previous.call(window,input,options,...rest);
      };
    """)


def _export(web, destination: Path) -> None:
    before = web.script("return window.__boundaryTransport.pickers;")
    web.script("window.__boundaryTransport.path=arguments[0];", str(destination))
    web.named("Export rows")
    web.named("Export…", ".export-dialog")
    wait_for(lambda: web.script("return window.__boundaryTransport.pickers;"),
             lambda value: value == before + 1, "the GUI Save picker returns its selected output path")


def _export_calls(web, owner: str, start: int) -> list[dict]:
    return [entry for entry in web.trace()[start:] if entry["name"] == "export_project" and entry["request"].get("projectId") == owner]


def _read_critical(destination: Path) -> list[dict]:
    with destination.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1 and rows[0]["trivium-critical"] == "1" and rows[0]["trivium-safe"] == "0", rows
    assert rows[0]["source_id"] == "r-0000000", rows
    return rows


def _ordered_export(web, owner: str, root: Path, report: dict) -> None:
    _install_export_transport(web)
    held = False
    try:
        web.script("window.__workspaceBeforeCommit={name:'update_flag',projectId:arguments[0],rowIndex:0,ready:false};", owner)
        held = True
        web.named("Safe", '.data-row[data-row-index="0"]')
        wait_for(lambda: web.script("return !!window.__workspaceBeforeCommit?.ready;"), bool, "Safe save is held before Rust storage")
        web.named("Critical", '.data-row[data-row-index="0"]')
        web.flags(["critical"])
        web.results(0)
        start = len(web.trace())
        destination = root / "ordered-export.csv"
        _export(web, destination)
        web.async_script("const done=arguments[arguments.length-1];setTimeout(()=>done(true),150);")
        assert not _export_calls(web, owner, start) and not destination.exists(), "Export must wait before resolving flag-filtered membership"
        web.script("window.__workspaceBeforeCommit.release();delete window.__workspaceBeforeCommit;")
        held = False
        wait_for(destination.exists, bool, "CSV is generated after both queued annotations commit")
        _read_critical(destination)
        entries = _export_calls(web, owner, start)
        assert len(entries) == 1 and entries[0]["request"]["flagFilter"] == "critical", entries
        web.applied(projectId=owner, flagFilter="critical")
        web.results(1)

        # A rejected save must not replace an existing output with old data.
        blocked = root / "blocked-export.csv"
        sentinel = b"existing output must survive\n"
        blocked.write_bytes(sentinel)
        web.script("window.__boundaryTransport.failNext=true;")
        web.named("Safe", '.data-row[data-row-index="0"]')
        wait_for(lambda: web.script("return !window.__boundaryTransport.failNext&&document.querySelector('.toast.error')?.textContent.includes('Failed to save flag.');"),
                 bool, "the rejected native annotation response is shown")
        start = len(web.trace())
        _export(web, blocked)
        wait_for(lambda: web.script("return document.querySelector('.toast.error')?.textContent??'';"),
                 lambda text: "Export stopped" in text and "annotation could not be saved" in text,
                 "export explicitly reports the earlier failed save")
        assert not _export_calls(web, owner, start) and blocked.read_bytes() == sentinel
        # The user explicitly checks the state and retries a new export.
        assert _rows(web, owner)[0]["flag"] == "critical"
        _export(web, blocked)
        wait_for(lambda: blocked.read_bytes(), lambda value: value != sentinel, "a fresh explicit export retry succeeds")
        _read_critical(blocked)
        assert len(_export_calls(web, owner, start)) == 1
        report["annotation_export_order"] = {"pending_saves_waited": 2, "filtered_membership_after_saves": True,
            "csv_critical": True, "failed_save_prevents_export_ipc": True, "existing_output_preserved": True,
            "explicit_retry_exports_confirmed_state": True,
            "substitutions": ["native Save picker response", "one rejected annotation IPC response"]}
    finally:
        if held:
            web.script("window.__workspaceBeforeCommit?.release?.();delete window.__workspaceBeforeCommit;")
        web.script("window.__boundaryTransport?.restore();delete window.__boundaryTransport;")


def verify_boundary_integrity(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    outer_query, outer_total = web.query(), web.state()["total"]
    source = root / f"boundary-integrity-{uuid.uuid4().hex[:8]}.csv"
    source.write_text("source_id,message\nr-0000000,match\nr-0000001,other\n", encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    owner = case["meta"]["id"]
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"])
        web.results(2)
        _ioc_queries(web, owner, root, report)
        _ordered_export(web, owner, root, report)
        web.screenshot(evidence / "boundary-export-integrity.png")
    finally:
        if web.script("return [...document.querySelectorAll('.ioc-panel .ioc-footer button')].some(e=>e.textContent.trim()==='Discard'&&!e.disabled);"):
            web.named("Discard", ".ioc-panel")
        web.close_drawer()
        if web.script("return !!document.querySelector('.export-dialog');"):
            web.named("Cancel", ".export-dialog")
        open_project(web, data["name"])
        web.applied(**outer_query)
        web.results(outer_total)
        if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{owner}"):
            web.named(f"Close {source.name}")
        delete_fixture_project(web, owner)
