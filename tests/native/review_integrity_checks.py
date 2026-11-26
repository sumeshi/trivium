"""CR-only source positions and report ownership use the real importer and UI."""
from __future__ import annotations

from pathlib import Path
import uuid

from fixture_cleanup import delete_fixture_project
from import_recovery_checks import _install_picker, _pick, _set_checkbox, _import
from import_refinements_checks import _open_report, _close_report
from row_navigation import open_project, wait_for


def verify_review_integrity(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    source = root / f"review-cr-{uuid.uuid4().hex[:8]}.csv"
    original = b'name,value\r\r"alpha\rinside",1\rbeta\rcharlie,3\r'
    source.write_bytes(original)
    created: list[dict] = []
    _install_picker(web)
    try:
        web.close_drawer()
        _pick(web, source, csv=True)
        _set_checkbox(web, "Recover readable records", True)
        meta = _import(web, source, 3, created)
        diagnostics = meta["import_report"]
        assert diagnostics["sourceRecords"] == diagnostics["importedRecords"] == 3
        assert diagnostics["repairedRecords"] == 1 and diagnostics["skippedRecords"] == 0, diagnostics
        assert [(issue["sourceRecord"], issue["action"]) for issue in diagnostics["issues"]] == [(5, "repaired")], diagnostics
        cell = web.command("get_cell", {"projectId": meta["id"], "rowIndex": 0,
            "columnId": meta["columns"][0]["id"]})
        assert cell["text"] == "alpha\rinside", cell
        page = web.command("query_project_rows", {"projectId": meta["id"], "offset": 0, "limit": 3})
        assert [row["row_index"] for row in page["rows"]] == [0, 1, 2]
        assert page["rows"][1]["cells"][meta["columns"][1]["id"]]["text"] is None

        start = web.checkpoint()
        _open_report(web)
        assert web.script("return document.querySelector('.import-report-dialog tbody td')?.textContent;") == "5"
        # Call the real tab handler to model background activation. This is
        # intentionally not a physical click through the modal overlay.
        assert web.script("""
          const tab=document.getElementById('project-tab-'+arguments[0]);
          if(!tab)return false;tab.click();return true;
        """, data["id"])
        web.applied(projectId=data["id"])
        wait_for(lambda: web.script("return !document.querySelector('.import-report-dialog');"),
                 bool, "switching cases dismisses the old import report")
        web.tab(meta["id"])
        web.results(3)
        assert not web.script("return !!document.querySelector('.import-report-dialog');"), "Returning must not reopen an old report"
        restored = web.command("load_project", {"projectId": meta["id"]}, request=True)
        assert restored["project"]["meta"]["import_report"] == diagnostics
        assert not any(entry["name"] == "export_import_report" for entry in web.trace(after=start))
        _open_report(web)
        web.screenshot(evidence / "review-cr-source-report.png")
        _close_report(web)
        assert source.read_bytes() == original
        report["review_integrity"] = {"cr_only_source_repair_line": 5,
            "quoted_cr_text_preserved": True, "dense_accepted_row_ids": True,
            "case_switch_closes_report": True, "return_does_not_reopen_report": True,
            "report_and_source_unchanged": True, "no_implicit_export": True}
    finally:
        if web.script("return !!document.querySelector('.import-report-dialog');"):
            _close_report(web)
        web.close_drawer()
        web.script("window.__recoveryPicker?.restore();delete window.__recoveryPicker;")
        open_project(web, data["name"])
        web.results(data["total"])
        for item in web.command("list_projects"):
            meta = item["meta"]
            if meta.get("source_file_name") == source.name:
                if web.script("return !!document.querySelector(arguments[0]);", f'#project-tab-{meta["id"]}'):
                    web.named(f"Close {source.name}")
                delete_fixture_project(web, meta["id"])
        source.unlink(missing_ok=True)
