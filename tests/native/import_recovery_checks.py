"""Explicit recovery uses the real importer, persisted diagnostics and table.

Only the file picker's selected path is supplied by the harness. All decoding,
strict rejection, recovery, source hashes, row queries and reopen use Rust IPC.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import uuid

from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS


def _install_picker(web) -> None:
    assert web.script("""
      const previous=window.fetch;
      window.__recoveryPicker={path:null,restore:()=>{window.fetch=previous;}};
      window.fetch=function(input,options,...rest){
    """ + IPC_COMMAND_JS + """
        if(name==='plugin:dialog|open'&&window.__recoveryPicker.path!==null){
          const path=window.__recoveryPicker.path;window.__recoveryPicker.path=null;
          return Promise.resolve(new Response(JSON.stringify(path),
            {headers:{'Tauri-Response':'ok','Content-Type':'application/json'}}));
        }
        return previous.call(window,input,options,...rest);
      };return true;
    """), "Only substitute the native file picker's result"


def _pick(web, path: Path, *, csv: bool) -> None:
    if not web.script("return !!document.querySelector('.workspace-drawer');"):
        web.named("Open project menu")
    web.script("window.__recoveryPicker.path=arguments[0];", str(path))
    web.click(web.node(".workspace-drawer .choose-file"))
    wait_for(lambda: web.script("return document.querySelector('.pending-file')?.textContent.trim();"),
             lambda value: value == path.name, "selected recovery fixture populates import form")
    if not web.script("return document.querySelector('.import-options').open;"):
        web.click(web.node(".import-options summary"))
    defaults = web.script("""
      return {encoding:document.querySelector('[aria-label="Import encoding"]').value,
        recover:document.querySelector('[aria-label="Recover readable records"]').checked,
        headers:document.querySelector('[aria-label="First row contains column names"]')?.checked??null};
    """)
    assert defaults == {"encoding": "auto", "recover": False, "headers": True if csv else None}, defaults
    if csv:
        web.choose('select[aria-label="CSV delimiter"]', "")


def _set_checkbox(web, label: str, checked: bool) -> None:
    selector = f'input[aria-label="{label}"]'
    if web.script("return document.querySelector(arguments[0]).checked;", selector) != checked:
        # Target the enclosing label, not the small browser checkbox glyph.
        web.tap(web.script("return document.querySelector(arguments[0]).closest('label');", selector))
    wait_for(lambda: web.script("return document.querySelector(arguments[0]).checked;", selector),
             lambda actual: actual == checked, f"{label} is explicitly selected")


def _import(web, path: Path, total: int, created: list[dict]) -> dict:
    web.named("Import and open", ".workspace-drawer")
    wait_for(lambda: web.script("return !document.querySelector('.workspace-drawer');"),
             bool, "real import opens its saved investigation")
    web.results(total)
    known = {meta["id"] for meta in created}
    meta = next(item["meta"] for item in web.command("list_projects")
                if item["meta"].get("source_file_name") == path.name and item["meta"]["id"] not in known)
    created.append(meta)
    assert meta["total_records"] == total
    assert meta["source_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest(), meta
    web.applied(projectId=meta["id"])
    return meta


def _values(web, meta: dict) -> list[list[str | None]]:
    response = web.command("query_project_rows", {"projectId": meta["id"], "offset": 0, "limit": 20})
    assert [row["row_index"] for row in response["rows"]] == list(range(meta["total_records"])), response
    values = [[row["cells"][column["id"]]["text"] for column in meta["columns"]]
              for row in response["rows"]]
    expected_first = [row[0] for row in values]
    wait_for(lambda: web.script("""
      return [...document.querySelectorAll('.data-row:not(.loading)')].map(row=>
        row.querySelector('[data-column-id="'+arguments[0]+'"] .cell-text')?.textContent.trim());
    """, meta["columns"][0]["id"]), lambda visible: visible == expected_first,
             "the real table displays every decoded first-column value")
    return values


def _show_report(web, expected: dict, screenshot: Path | None = None) -> None:
    web.named("Workspace options")
    web.named("Import report…", ".workspace-options")
    content = wait_for(lambda: web.script("return document.querySelector('.import-report-dialog')?.textContent;"),
                       bool, "persisted import diagnostics open from workspace options")
    assert "Import report" in content and expected["encoding"] in content, content
    if expected["issues"]:
        assert expected["issues"][0]["reason"] in content, content
    if screenshot is not None:
        web.screenshot(screenshot)
    web.named("Close import report", ".import-report-dialog")
    wait_for(lambda: web.script("return !document.querySelector('.import-report-dialog');"),
             bool, "import report closes")


def verify_import_recovery(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    token = uuid.uuid4().hex[:8]
    ragged = root / f"recovery-ragged-{token}.csv"
    headerless = root / f"recovery-headerless-{token}.csv"
    unicode_json = root / f"recovery-unicode-{token}.json"
    legacy_csv = root / f"recovery-legacy-{token}.csv"
    broken_lines = root / f"recovery-lines-{token}.jsonl"
    ragged.write_bytes(b"name,value\nalpha,one\nbeta\ncharlie,three,extra\n")
    headerless.write_bytes(b"alpha,one\nbeta,two\n")
    unicode_json.write_bytes(b"\xff\xfe" + '[{"message":"日本語"},{"message":"second"}]'.encode("utf-16le"))
    legacy_csv.write_bytes("message\ncafé\nsecond\n".encode("windows-1252"))
    broken_lines.write_bytes(b'{"message":"first"}\n{"broken":}\n{"message":"last"}\n')
    paths = [ragged, headerless, unicode_json, legacy_csv, broken_lines]
    originals = {path: path.read_bytes() for path in paths}
    created: list[dict] = []
    _install_picker(web)
    try:
        web.close_drawer()
        _pick(web, ragged, csv=True)
        start = web.checkpoint()
        web.named("Import and open", ".workspace-drawer")
        error = wait_for(lambda: next((entry for entry in web.trace(after=start)
            if entry["name"] == "create_project" and entry.get("request", {}).get("path") == str(ragged)
            and "error" in entry), None), bool, "strict CSV rejects an unequal record")
        wait_for(lambda: web.script("return document.querySelector('.import-failures')?.textContent??'';"),
                 lambda text: ragged.name in text and error["error"] in text, "strict import explains the failure")
        assert not error["request"]["importOptions"]["recover"], error
        assert not any(item["meta"].get("source_file_name") == ragged.name for item in web.command("list_projects"))
        report.setdefault("boundary_expected_errors", []).append(
            {key: error[key] for key in ("name", "request", "error")})

        _set_checkbox(web, "Recover readable records", True)
        repaired = _import(web, ragged, 3, created)
        assert [column["label"] for column in repaired["columns"]][:2] == ["name", "value"], repaired
        assert len(repaired["columns"]) == 3
        assert _values(web, repaired) == [["alpha", "one", None], ["beta", None, None], ["charlie", "three", "extra"]]
        diagnostics = repaired["import_report"]
        assert diagnostics["importedRecords"] == 3 and diagnostics["sourceRecords"] == 3, diagnostics
        assert diagnostics["generatedColumns"] == 1 and diagnostics["repairedRecords"] >= 2, diagnostics
        assert diagnostics["skippedRecords"] == 0 and not diagnostics["unreadableTail"], diagnostics
        assert web.script("return !!document.querySelector('.import-report-indicator');"), "Recovery must remain visible after the import menu closes"
        _show_report(web, diagnostics, evidence / "import-recovery-report.png")

        # Closing a tab and reopening through Recent must retain the report.
        web.tab(data["id"])
        web.named(f"Close {ragged.name}")
        open_project(web, repaired["name"])
        web.results(3)
        restored = web.command("load_project", {"projectId": repaired["id"]}, request=True)
        assert restored["project"]["meta"]["import_report"] == diagnostics, restored
        _show_report(web, diagnostics)

        _pick(web, headerless, csv=True)
        _set_checkbox(web, "First row contains column names", False)
        assert web.script("return document.querySelector('[aria-label=\"Restore Trivium annotations\"]').disabled;")
        no_headers = _import(web, headerless, 2, created)
        assert len(no_headers["columns"]) == 2 and all(column["label"] for column in no_headers["columns"])
        assert _values(web, no_headers) == [["alpha", "one"], ["beta", "two"]], "The first source record must remain data"
        assert no_headers["import_report"]["hasHeaders"] is False
        assert no_headers["import_report"]["generatedColumns"] == 2

        _pick(web, unicode_json, csv=False)
        unicode_case = _import(web, unicode_json, 2, created)
        assert _values(web, unicode_case) == [["日本語"], ["second"]]
        assert unicode_case["import_report"]["encoding"] == "utf-16le" and unicode_case["import_report"]["bom"]
        assert unicode_case["import_report"]["skippedRecords"] == 0

        _pick(web, legacy_csv, csv=True)
        web.choose('select[aria-label="Import encoding"]', "windows-1252")
        legacy_case = _import(web, legacy_csv, 2, created)
        assert _values(web, legacy_case) == [["café"], ["second"]]
        assert legacy_case["import_report"]["encoding"] == "windows-1252" and not legacy_case["import_report"]["bom"]

        _pick(web, broken_lines, csv=False)
        _set_checkbox(web, "Recover readable records", True)
        recovered = _import(web, broken_lines, 2, created)
        assert _values(web, recovered) == [["first"], ["last"]]
        recovered_report = recovered["import_report"]
        assert recovered_report["sourceRecords"] == 3 and recovered_report["importedRecords"] == 2
        assert recovered_report["skippedRecords"] == 1 and not recovered_report["unreadableTail"], recovered_report
        assert any(issue["sourceRecord"] == 2 and issue["action"] == "skipped"
                   for issue in recovered_report["issues"]), recovered_report
        _show_report(web, recovered_report)
        assert all(path.read_bytes() == before for path, before in originals.items()), "Source files remain byte-exact"
        report["import_recovery"] = {"strict_default_rejects_without_case": True,
            "opt_in_extra_columns_and_null_missing_cells": True, "first_headerless_record_kept": True,
            "report_survives_close_and_reopen": True, "unicode_bom_json": "utf-16le",
            "explicit_legacy_encoding": "windows-1252",
            "jsonl_skipped_physical_line": 2, "dense_accepted_row_ids": True, "source_hashes_match_original_bytes": True}
    finally:
        if web.script("return !!document.querySelector('.import-report-dialog');"):
            web.named("Close import report", ".import-report-dialog")
        web.script("window.__recoveryPicker?.restore();delete window.__recoveryPicker;")
        open_project(web, data["name"])
        web.results(data["total"])
        # Discover a case even if an assertion failed before _import recorded it.
        by_name = {path.name: path for path in paths}
        saved = [item["meta"] for item in web.command("list_projects") if item["meta"].get("source_file_name") in by_name]
        for meta in saved:
            if web.script("return !!document.querySelector(arguments[0]);", f'#project-tab-{meta["id"]}'):
                web.named(f'Close {meta["source_file_name"]}')
            delete_fixture_project(web, meta["id"])
        for path in paths:
            path.unlink(missing_ok=True)
