"""Real preview, persistent import settings, full diagnostics and encoded rescue.

Native picker paths alone are supplied by the harness; previews, importing,
reports and report writes use the actual Rust implementation.
"""
from __future__ import annotations

import json
from pathlib import Path
import uuid

from fixture_cleanup import delete_fixture_project
from import_recovery_checks import _install_picker, _pick, _set_checkbox, _import, _show_report, _values
from row_navigation import open_project, wait_for
from runtime import IPC_COMMAND_JS


def _open_report(web) -> dict:
    web.named("Workspace options")
    web.named("Import report…", ".workspace-options")
    wait_for(lambda: web.script("return !!document.querySelector('.import-report-dialog');"),
             bool, "saved import settings open")
    return web.script("""
      return Object.fromEntries([...document.querySelectorAll('.import-report-dialog dt')]
        .map(node=>[node.textContent,node.nextElementSibling.textContent]));
    """)


def _close_report(web) -> None:
    web.named("Close import report", ".import-report-dialog")
    wait_for(lambda: web.script("return !document.querySelector('.import-report-dialog');"),
             bool, "saved import settings close")


def _save_picker(web) -> None:
    web.script("""
      const previous=window.fetch;
      window.__refinementSave={path:null,restore:()=>{window.fetch=previous;}};
      window.fetch=function(input,options,...rest){
    """ + IPC_COMMAND_JS + """
        if(name==='plugin:dialog|save'&&window.__refinementSave.path!==null){
          const path=window.__refinementSave.path;window.__refinementSave.path=null;
          return Promise.resolve(new Response(JSON.stringify(path),
            {headers:{'Tauri-Response':'ok','Content-Type':'application/json'}}));
        }
        return previous.call(window,input,options,...rest);
      };
    """)


def verify_import_refinements(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    token = uuid.uuid4().hex[:8]
    tabbed = root / f"preview-tab-{token}.csv"
    broken = root / f"full-diagnostics-{token}.jsonl"
    unicode_lines = root / f"encoded-recovery-{token}.jsonl"
    exported = root / f"import-report-{token}.jsonl"
    tabbed.write_bytes(b"name\tvalue\nalpha\t00123\nbeta\t00045\n")
    broken.write_bytes(b'{"message":"first"}\n' + b"not-json\n" * 35 + b'{"message":"last"}\n')
    unicode_lines.write_bytes(b"\xff\xfe" + '{"message":"日本語"}\n'.encode("utf-16le")
                             + '{"message":"'.encode("utf-16le") + b"\x00\xd8"
                             + '"}\n{"message":"last"}\n'.encode("utf-16le"))
    paths = [tabbed, broken, unicode_lines]
    originals = {path: path.read_bytes() for path in paths}
    created: list[dict] = []
    _install_picker(web)
    _save_picker(web)
    try:
        web.close_drawer()
        start = len(web.trace())
        _pick(web, tabbed, csv=True)
        assert not web.script("return document.querySelector('[aria-label=\"Preview import\"]').checked;"), "Preview is opt-in"
        web.async_script("const done=arguments[arguments.length-1];requestAnimationFrame(()=>requestAnimationFrame(()=>done(true)));")
        assert not any(entry["name"] == "preview_import" for entry in web.trace()[start:]), "Disabled preview must not read a file"
        _set_checkbox(web, "Preview import", True)
        wait_for(lambda: web.script("return document.querySelector('.preview-table tbody')?.textContent;"),
                 bool, "enabled preview displays actual sampled values")
        wait_for(lambda: web.script("return [...document.querySelectorAll('.delimiter-suggestion')].map(node=>node.textContent.trim());"),
                 lambda labels: labels == ["Use tab delimiter"], "quoted CSV sample suggests tab without applying it")
        assert web.script("return document.querySelector('[aria-label=\"CSV delimiter\"]').value;") == ""
        web.named("Use tab delimiter", ".import-preview")
        wait_for(lambda: web.script("return document.querySelector('[aria-label=\"CSV delimiter\"]').value;"),
                 lambda value: value == "\t", "explicit suggestion click selects the delimiter")
        wait_for(lambda: web.script("return [...document.querySelectorAll('.preview-table thead th')].map(node=>node.textContent);"),
                 lambda labels: labels == ["name", "value"], "preview reflects the selected tab delimiter")
        web.screenshot(evidence / "import-preview-opt-in.png")
        previewed = _import(web, tabbed, 2, created)
        assert _values(web, previewed) == [["alpha", "00123"], ["beta", "00045"]]
        settings = previewed["import_report"]
        assert settings["delimiter"] == "\t" and settings["recover"] is False and settings["restoreAnnotations"] is False, settings
        fields = _open_report(web)
        assert fields["CSV delimiter"] == "Tab" and fields["Mode"] == "Strict" and fields["Restore annotations"] == "Off", fields
        _close_report(web)

        _pick(web, broken, csv=False)
        assert not web.script("return document.querySelector('[aria-label=\"Preview import\"]').checked;"), "A new drawer starts with preview disabled"
        _set_checkbox(web, "Recover readable records", True)
        recovered = _import(web, broken, 2, created)
        assert _values(web, recovered) == [["first"], ["last"]]
        diagnostics = recovered["import_report"]
        assert diagnostics["issueCount"] == 35 and len(diagnostics["issues"]) == 30, diagnostics
        assert diagnostics["fullDiagnostics"] is True and diagnostics["recover"] is True
        fields = _open_report(web)
        assert fields["Mode"] == "Recovery", fields
        assert web.script("return document.querySelectorAll('.import-report-dialog tbody tr').length;") == 30
        web.script("window.__refinementSave.path=arguments[0];", str(exported))
        web.named("Export JSONL", ".import-report-dialog")
        wait_for(exported.is_file, bool, "real engine exports every import diagnostic")
        wait_for(lambda: web.script("return document.querySelector('.import-report-dialog [role=status]')?.textContent;"),
                 lambda text: text == "Report exported.", "report export reports completion")
        entries = [json.loads(line) for line in exported.read_text(encoding="utf-8").splitlines()]
        summary, issues = entries[0], entries[1:]
        assert summary["type"] == "summary" and summary["diagnosticsComplete"] is True, summary
        assert summary["report"]["issues"] == [] and summary["report"]["issueCount"] == 35
        assert summary["report"]["recover"] is True and summary["project"]["sourceSha256"] == recovered["source_sha256"]
        assert len(issues) == 35 and [issue["sourceRecord"] for issue in issues] == list(range(2, 37)), issues
        assert all(issue["type"] == "issue" and issue["action"] == "skipped" and issue["reason"] for issue in issues)
        (evidence / "complete-import-report.jsonl").write_bytes(exported.read_bytes())
        _close_report(web)
        web.tab(data["id"])
        web.named(f"Close {broken.name}")
        open_project(web, recovered["name"])
        web.results(2)
        reopened = web.command("load_project", {"projectId": recovered["id"]}, request=True)
        assert reopened["project"]["meta"]["import_report"] == diagnostics
        _show_report(web, diagnostics)

        _pick(web, unicode_lines, csv=False)
        _set_checkbox(web, "Recover readable records", True)
        encoded = _import(web, unicode_lines, 2, created)
        assert _values(web, encoded) == [["日本語"], ["last"]]
        encoded_report = encoded["import_report"]
        assert encoded_report["encoding"] == "utf-16le" and encoded_report["bom"] is True
        assert encoded_report["skippedRecords"] == 1 and encoded_report["importedRecords"] == 2
        assert encoded_report["issues"][0]["sourceRecord"] == 2 and encoded_report["issues"][0]["action"] == "skipped", encoded_report
        assert all(path.read_bytes() == before for path, before in originals.items())
        report["import_refinements"] = {"preview_default_off_no_reads": True,
            "delimiter_suggestion_requires_explicit_click": True, "preview_uses_real_engine": True,
            "actual_import_settings_persist": True, "all_35_diagnostics_exported": True,
            "ui_examples_remain_bounded_at_30": True, "settings_and_report_survive_reopen": True,
            "utf16_bad_middle_line_recovers_valid_records": True, "original_bytes_and_hashes_preserved": True}
    finally:
        if web.script("return !!document.querySelector('.import-report-dialog');"):
            _close_report(web)
        web.close_drawer()
        web.script("window.__refinementSave?.restore();delete window.__refinementSave;window.__recoveryPicker?.restore();delete window.__recoveryPicker;")
        open_project(web, data["name"])
        web.results(data["total"])
        saved = [item["meta"] for item in web.command("list_projects")
                 if item["meta"].get("source_file_name") in {path.name for path in paths}]
        for meta in saved:
            if web.script("return !!document.querySelector(arguments[0]);", f'#project-tab-{meta["id"]}'):
                web.named(f'Close {meta["source_file_name"]}')
            delete_fixture_project(web, meta["id"])
        for path in paths + [exported]:
            path.unlink(missing_ok=True)
