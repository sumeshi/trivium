"""One bulk form: optional preview, atomic patch, exact tags and owned writes."""
from __future__ import annotations

from pathlib import Path
import uuid

from row_navigation import open_project, wait_for


def _rows(web, project_id: str) -> list[dict]:
    return web.command("query_project_rows", {"projectId": project_id, "offset": 0, "limit": 10})["rows"]


def _status(web, expected: str) -> None:
    wait_for(lambda: web.script("return document.querySelector('.bulk-form [role=status]')?.textContent;"),
        lambda text: text == expected, expected)


def _calls(web, start: int, project_id: str, name: str) -> list[dict]:
    return [entry for entry in web.trace()[start:] if entry["name"] == name and entry["request"].get("projectId") == project_id]


def verify_bulk_form(web, data, root: Path, evidence: Path, report: dict) -> None:
    source = root / f"bulk-form-{uuid.uuid4().hex[:8]}.csv"
    source.write_text("source_id,message\nfirst,match\nsecond,other\nthird,match\n", encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    for row, flag, note, tags, bookmarked in [(0, "critical", "human note", ["manual"], True), (1, "safe", "outside", [], False), (2, "suspicious", "keep note", [], False)]:
        web.command("update_flag", {"projectId": project_id, "row_index": row, "flag": flag, "memo": note, "tags": tags, "bookmarked": bookmarked})
    original = _rows(web, project_id)
    open_project(web, case["meta"]["name"])
    web.results(3)
    try:
        web.write('input[aria-label="Search rows"]', "match")
        web.applied(search="match"); web.results(2)
        web.drawer("Bulk annotation")
        assert web.script("return document.querySelector('[aria-label=\"Bulk flag\"]').value;") == "keep"
        assert web.script("return [...document.querySelectorAll('.bulk-actions button')].find(e=>e.textContent.trim()==='Apply').disabled;")
        web.write('input[aria-label="Bulk tag"]', "review, exact")
        start = len(web.trace())
        web.named("Apply", ".bulk-form")
        _status(web, "2 rows applied")
        assert len(_calls(web, start, project_id, "bulk_preview")) == len(_calls(web, start, project_id, "bulk_update")) == 1
        tag_only = _rows(web, project_id)
        for index in [0, 2]:
            assert tag_only[index]["flag"] == original[index]["flag"] and tag_only[index]["memo"] == original[index]["memo"]
            assert tag_only[index]["tags"] == original[index]["tags"] + ["review, exact"], tag_only[index]
            assert tag_only[index]["bookmarked"] == original[index]["bookmarked"]
        assert tag_only[1] == original[1]
        web.named("Apply", ".bulk-form"); _status(web, "0 rows applied")
        assert _rows(web, project_id) == tag_only, "Reapplying an identical patch changes no rows"
        web.named("Undo", ".bulk-form"); _status(web, "2 rows restored")
        assert _rows(web, project_id) == original

        web.choose('select[aria-label="Bulk flag"]', "critical")
        web.write('input[aria-label="Bulk note"]', "combined note")
        web.write('input[aria-label="Bulk tag"]', "before preview")
        web.named("Preview", ".bulk-form"); _status(web, "2 rows")
        assert _rows(web, project_id) == original
        web.write('input[aria-label="Bulk tag"]', "combined tag")
        assert not web.script("return !!document.querySelector('.bulk-form [role=status]');"), "A draft edit discards the visible preview"
        start = len(web.trace())
        web.named("Apply", ".bulk-form"); _status(web, "2 rows applied")
        updates = _calls(web, start, project_id, "bulk_update")
        assert len(updates) == 1 and updates[0]["request"]["flag"] == "critical" and updates[0]["request"]["memo"] == "combined note", updates
        assert updates[0]["request"]["add_tags"] == ["combined tag"]
        combined = _rows(web, project_id)
        for index in [0, 2]:
            assert combined[index]["flag"] == "critical" and combined[index]["memo"] == "combined note"
            assert combined[index]["tags"] == original[index]["tags"] + ["combined tag"]
            assert combined[index]["bookmarked"] == original[index]["bookmarked"]
        assert combined[1] == original[1]
        # Keep selection target selected after clearing the selection: Undo
        # still refers to the previous durable operation, not the form target.
        web.click(web.element('input[aria-label="Select visible rows"]'))
        wait_for(web.state, lambda state: state["selectionTotal"] == 2, "the two matching rows are selected")
        web.choose('select[aria-label="Bulk target"]', "selection")
        web.click(web.element('input[aria-label="Select visible rows"]'))
        wait_for(web.state, lambda state: state["selectionTotal"] == 0, "selection becomes empty")
        assert not web.script("return [...document.querySelectorAll('.bulk-actions button')].find(e=>e.textContent.trim()==='Undo').disabled;")
        web.named("Undo", ".bulk-form"); _status(web, "2 rows restored")
        assert _rows(web, project_id) == original

        web.choose('select[aria-label="Bulk target"]', "filtered")
        web.choose('select[aria-label="Bulk flag"]', "")
        web.click(web.element('.bulk-clear input'))
        web.write('input[aria-label="Bulk tag"]', "manual")
        web.choose('select[aria-label="Bulk tag action"]', "remove")
        web.named("Apply", ".bulk-form"); _status(web, "2 rows applied")
        cleared = _rows(web, project_id)
        for index in [0, 2]:
            assert cleared[index]["flag"] == "" and cleared[index]["memo"] is None and cleared[index]["tags"] == []
            assert cleared[index]["bookmarked"] == original[index]["bookmarked"]
        assert cleared[1] == original[1]
        web.named("Undo", ".bulk-form"); _status(web, "2 rows restored")
        assert _rows(web, project_id) == original

        web.choose('select[aria-label="Bulk flag"]', "keep")
        web.click(web.element('.bulk-clear input'))
        web.choose('select[aria-label="Bulk tag action"]', "add")
        web.write('input[aria-label="Bulk tag"]', "closed panel")
        web.script("window.__workspaceHold={name:'bulk_preview',projectId:arguments[0],ready:false};", project_id)
        start = len(web.trace())
        web.named("Apply", ".bulk-form")
        wait_for(lambda: web.script("return !!window.__workspaceHold?.ready;"), bool, "the direct Apply's actual internal preview response is held")
        web.close_drawer()
        web.write('input[aria-label="Search rows"]', "other")
        web.applied(search="other"); web.results(1)
        web.tab(data["id"]); web.results(data["total"])
        web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
        wait_for(lambda: _calls(web, start, project_id, "bulk_update"), lambda entries: bool(entries and "result" in entries[-1]), "the captured Apply completes after its panel closes")
        after_close = _rows(web, project_id)
        for index in [0, 2]:
            assert after_close[index]["tags"] == original[index]["tags"] + ["closed panel"]
            assert after_close[index]["flag"] == original[index]["flag"] and after_close[index]["memo"] == original[index]["memo"]
        assert after_close[1] == original[1], "Closing and changing the visible query cannot retarget a requested mutation"
        web.tab(project_id); web.applied(projectId=project_id, search="other"); web.results(1)
        web.drawer("Bulk annotation")
        web.named("Undo", ".bulk-form"); _status(web, "2 rows restored")
        assert _rows(web, project_id) == original

        web.write('input[aria-label="Bulk tag"]', "stale target")
        web.script("window.__workspaceHold={name:'bulk_preview',projectId:arguments[0],ready:false};", project_id)
        start = len(web.trace())
        web.named("Apply", ".bulk-form")
        wait_for(lambda: web.script("return !!window.__workspaceHold?.ready;"), bool, "a second direct preview is held while the panel remains open")
        web.write('input[aria-label="Search rows"]', "match")
        web.applied(search="match"); web.results(2)
        web.script("window.__workspaceHold.release();delete window.__workspaceHold;")
        wait_for(lambda: web.script("return document.querySelector('.bulk-form [role=alert]')?.textContent;"),
            lambda value: value == "Target changed. Apply again.", "an in-panel target change cancels the pending pre-commit Apply")
        assert not _calls(web, start, project_id, "bulk_update")
        assert _rows(web, project_id) == original
        web.screenshot(evidence / "bulk-unified-optional-preview.png")
        report["bulk_form"] = {"direct_apply": True, "default_flag_kept": True, "single_exact_tag": "review, exact",
            "combined_single_operation": True, "preview_optional_and_invalidated_on_edit": True,
            "identical_reapply_keeps_previous_undo": True,
            "undo_with_empty_selection": True, "clear_flag_note_remove_tag_atomic": True,
            "closed_panel_and_case_apply_keeps_captured_scope": True, "alive_target_change_aborts_precommit": True}
    finally:
        web.script("window.__workspaceHold?.release?.();delete window.__workspaceHold;")
        web.close_drawer()
        web.tab(data["id"]); web.results(data["total"])
        web.named(f"Close {source.name}")
        web.command("delete_project", {"projectId": project_id}, request=True)
