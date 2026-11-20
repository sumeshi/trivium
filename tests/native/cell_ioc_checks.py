"""Exact raw-cell IOC drafts, explicit application, and portable CSV rules."""
from __future__ import annotations

import csv
from pathlib import Path
import uuid

from fixture_cleanup import delete_fixture_project
from feedback_checks import _settle
from row_navigation import open_project, wait_for
from runtime import context_click


def _panel(web) -> None:
    web.drawer("IOC rules")
    wait_for(lambda: web.script("return !!document.querySelector('.ioc-panel');"), bool, "inline IOC panel opens")


def _notice(web, text: str) -> None:
    wait_for(lambda: web.script("return document.querySelector('.ioc-panel [role=status]')?.textContent;"),
             lambda value: value == text, text)


def _select_rule(web, index: int) -> None:
    button = web.node(f'.ioc-row [aria-label="Edit rule {index + 1}"]')
    web.script("arguments[0].scrollIntoView({block:'nearest'});", button)
    web.click(button)
    wait_for(lambda: web.script("return !!document.querySelector(arguments[0]);",
                               f'.ioc-editor [aria-label="Rule tag {index + 1}"]'),
             bool, f"rule {index + 1} is selected in the shared editor")


def _editor_layout(web) -> dict:
    _settle(web)
    return web.script("""
      const editor=document.querySelector('.ioc-editor'),list=document.querySelector('.ioc-table'),
        field=editor.querySelector('.ioc-cell-value,.ioc-query input'),e=editor.getBoundingClientRect(),f=field.getBoundingClientRect();
      return {top:e.top,height:e.height,left:f.left,width:f.width,listTop:list.getBoundingClientRect().top,
        overflow:editor.scrollWidth>editor.clientWidth};
    """)


def _verify_mixed_rule_roundtrip(web, owner: str, raw: str, ids: dict, original: list[dict],
                                root: Path, picker) -> dict:
    query = "cohort:keep -source_id:r1"
    cell_tag, query_tag = 'cell,"証拠"', 'query,"調査"'
    _select_rule(web, 0)
    web.write('[aria-label="Rule tag 1"]', cell_tag)
    web.click(web.node(".ioc-editor .flag-select-trigger"))
    web.click(web.node(".flag-select-menu .flag-option.flag-suspicious"))
    web.named("Add rule", ".ioc-panel")
    web.write('[aria-label="Rule tag 2"]', query_tag)
    web.write('[aria-label="Rule query 2"]', query)
    web.named("Save rules", ".ioc-panel")
    _notice(web, "Rules saved.")
    expected = [
        {"flag": "suspicious", "tag": cell_tag, "query": "", "cell": {"column": ids["message"], "value": raw}},
        {"flag": "critical", "tag": query_tag, "query": query},
    ]
    saved = lambda: web.command("load_project", {"projectId": owner}, request=True)["iocs"]
    assert saved() == expected and web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 10})["rows"] == original
    export_path = root / "mixed-query-cell-iocs.csv"
    picker(web, export_path)
    web.named("Export IOC rules", ".ioc-panel")
    _notice(web, "Rules exported.")
    exported = export_path.read_bytes()
    with export_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == ["flag", "tag", "query", "column", "value"]
        assert list(reader) == [
            {"flag": "suspicious", "tag": cell_tag, "query": "", "column": "message", "value": raw},
            {"flag": "critical", "tag": query_tag, "query": query, "column": "", "value": ""},
        ], "The mixed CSV preserves each rule kind and exact quoted, multiline Unicode content"

    # Make the saved set genuinely different before restoring it. Importing
    # the same file over unchanged definitions would miss a broken restore.
    _select_rule(web, 0)
    web.write('[aria-label="Rule value 1"]', "replaced source value")
    _select_rule(web, 1)
    web.write('[aria-label="Rule query 2"]', "cohort:hide")
    web.named("Save rules", ".ioc-panel")
    _notice(web, "Rules saved.")
    assert saved() != expected
    assert saved()[0]["cell"]["value"] == "replaced source value" and saved()[1]["query"] == "cohort:hide"
    picker(web, export_path)
    web.named("Import IOC rules", ".ioc-panel")
    _notice(web, "Rules imported.")
    assert saved() == expected
    visible_query = web.query()
    web.close_drawer()
    web.named("Workspace options")
    web.named("Reload saved project", ".workspace-options")
    web.results(1)
    _panel(web)
    assert saved() == expected and web.query() == visible_query
    assert web.script("return document.querySelectorAll('.ioc-row').length;") == 2
    for index, entry in enumerate(expected):
        _select_rule(web, index)
        assert web.script("return document.querySelector('.ioc-editor .flag-select').dataset.flag;") == entry["flag"]
        assert web.script("return document.querySelector(arguments[0]).value;", f'[aria-label="Rule tag {index + 1}"]') == entry["tag"]
        if entry.get("cell"):
            assert web.script("return document.querySelector(arguments[0]).value;", f'[aria-label="Rule value {index + 1}"]') == raw
            assert web.script("return document.querySelector(arguments[0]).value;", f'[aria-label="Rule column {index + 1}"]') == ids["message"]
        else:
            assert web.script("return document.querySelector(arguments[0]).value;", f'[aria-label="Rule query {index + 1}"]') == query
        before = len(web.trace())
        web.named("Apply", f'.ioc-row[data-rule-index="{index}"]')
        _notice(web, "2 rows annotated.")
        updates = [item for item in web.trace()[before:] if item["name"] == "bulk_update"]
        assert len(updates) == 1 and updates[0]["request"]["flag"] == entry["flag"]
        assert updates[0]["request"]["add_tags"] == [entry["tag"]]
        assert updates[0]["request"]["search"] == entry["query"]
        assert updates[0]["request"]["condition"] == ({"column": ids["message"], "op": "eq", "values": [raw], "negated": False} if entry.get("cell") else None)
        affected = [0, 3] if entry.get("cell") else [0, 2]
        rows = web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 10})["rows"]
        for row_index, row in enumerate(rows):
            if row_index not in affected:
                assert row == original[row_index]
                continue
            assert row["flag"] == entry["flag"] and row["tags"] == original[row_index]["tags"] + [entry["tag"]]
            assert row["memo"] == original[row_index]["memo"] and row["bookmarked"] == original[row_index]["bookmarked"]
        assert web.query() == visible_query, "Restored IOC rules keep their own all-row scope"
        web.named("Undo", ".ioc-footer")
        _notice(web, "2 rows restored.")
        assert web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 10})["rows"] == original
    assert saved() == expected and export_path.read_bytes() == exported
    return {"mixed_query_and_cell_rules": True, "special_characters_preserved": True,
            "replaced_saved_definitions_restored": True, "gui_reload_fields_verified": True,
            "restored_cell_source_rows": [0, 3], "restored_query_source_rows": [0, 2],
            "both_restored_rules_apply_and_undo": True}


def _context(web, column: str) -> None:
    _settle(web)
    target = web.node(f'.data-row[data-row-index="0"] [data-column-id="{column}"]')
    point = web.script("const e=arguments[0],r=e.getBoundingClientRect(),v=document.querySelector('.virtual-viewport').getBoundingClientRect(),x=Math.round((Math.max(r.left,v.left)+Math.min(r.right,v.right))/2),y=Math.round((r.top+r.bottom)/2),hit=document.elementFromPoint(x,y);return {x,y,hit:e.contains(hit),target:hit?.outerHTML.slice(0,200)};", target)
    assert point.pop("hit"), point
    point.pop("target")
    context_click(web, point)
    wait_for(lambda: web.script("return !!document.querySelector('.cell-context-menu');"), bool, "trusted cell context menu opens")


def verify_compact_ioc_list(web, owner: str, root: Path, evidence: Path, report: dict, picker) -> None:
    saved = web.command("load_project", {"projectId": owner}, request=True)["iocs"]
    before = web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 10})["rows"]
    source = root / "compact-ioc-list.csv"
    intended = [{"flag": flag, "tag": f"review-{index + 1}", "query": f"score:{index + 1}"}
                for index, flag in enumerate(["safe", "suspicious", "critical"] * 12)]
    with source.open("w", encoding="utf-8", newline="") as handle:
        out = csv.writer(handle)
        out.writerow(["flag", "tag", "query"])
        out.writerows((entry["flag"], entry["tag"], entry["query"]) for entry in intended)
    picker(web, source)
    web.named("Import IOC rules", ".ioc-panel")
    _notice(web, "Rules imported.")
    canonical = web.command("load_project", {"projectId": owner}, request=True)["iocs"]
    fields = lambda entries: sorted((entry["flag"], entry["tag"], entry["query"]) for entry in entries)
    assert fields(canonical) == fields(intended), "Import must preserve every rule even when its canonical order changes"
    layout = web.script("""
      const panel=document.querySelector('.ioc-panel'),editor=panel.querySelector('.ioc-editor'),list=panel.querySelector('.ioc-table'),
        rows=[...list.querySelectorAll('.ioc-row')],e=editor.getBoundingClientRect(),l=list.getBoundingClientRect();
      return {rows:rows.length,editors:panel.querySelectorAll('.ioc-editor').length,
        rowHeights:rows.map(row=>row.getBoundingClientRect().height),listHeight:l.height,editorHeight:e.height,
        listOverflow:getComputedStyle(list).overflowY,scrollable:list.scrollHeight>list.clientHeight,
        editorFraction:e.height/(e.height+l.height),listInputs:list.querySelectorAll('input,textarea,select').length,
        flags:rows.map(row=>row.querySelector('.ioc-list-flag').dataset.flag),
        queries:rows.map(row=>row.querySelector('.ioc-rule-text').textContent.trim())};
    """)
    assert layout["rows"] == 36 and layout["editors"] == 1 and layout["listInputs"] == 0, layout
    assert layout["scrollable"] and layout["listOverflow"] in ["auto", "scroll"], layout
    assert .1 <= layout["editorFraction"] <= .5 and max(layout["rowHeights"]) <= 64, layout
    assert layout["flags"] == [entry["flag"] for entry in canonical], layout
    assert layout["queries"] == [entry["query"] for entry in canonical], layout
    _select_rule(web, 35)
    assert web.script("return document.querySelector('[aria-label=\"Rule query 36\"]').value;") == canonical[-1]["query"]
    web.write('[aria-label="Rule tag 36"]', "edited without saving")
    _select_rule(web, 0)
    assert web.script("return document.querySelector('[aria-label=\"Rule query 1\"]').value;") == canonical[0]["query"]
    _select_rule(web, 35)
    assert web.script("return document.querySelector('[aria-label=\"Rule tag 36\"]').value;") == "edited without saving"
    assert web.script("return document.querySelector('.ioc-table').scrollTop;") > 0
    _settle(web)
    web.screenshot(evidence / "ioc-compact-many-rules.png")
    web.named("Discard", ".ioc-panel")
    assert web.script("return document.querySelectorAll('.ioc-row').length;") == 36
    _select_rule(web, 35)
    assert web.script("return document.querySelector('[aria-label=\"Rule tag 36\"]').value;") == canonical[-1]["tag"]
    assert web.command("load_project", {"projectId": owner}, request=True)["iocs"] == canonical
    assert web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 10})["rows"] == before
    web.command("save_iocs", {"projectId": owner, "entries": saved})
    web.close_drawer()
    web.named("Workspace options")
    web.named("Reload saved project", ".workspace-options")
    web.results(1)
    _panel(web)
    assert web.script("return document.querySelectorAll('.ioc-row').length;") == len(saved)
    report["compact_ioc_list"] = {**layout, "draft_survives_rule_switch": True,
        "discard_restores_definitions": True, "annotations_unchanged": True}


def verify_cell_iocs(web, data, root: Path, evidence: Path, report: dict, picker) -> None:
    raw = '  -literal:field | "quoted", C:\\Temp\\証拠\n' + "raw-evidence-" * 500 + "  "
    source = root / f"cell-ioc-{uuid.uuid4().hex[:8]}.csv"
    with source.open("w", encoding="utf-8", newline="") as handle:
        out = csv.writer(handle)
        out.writerow(["source_id", "message", "other", "cohort"])
        out.writerows([["r0", raw, "decoy", "keep"], ["r1", raw + " suffix", "decoy", "keep"],
                       ["r2", "ordinary", raw, "keep"], ["r3", raw, "decoy", "hide"]])
    case = web.command("create_project", {"path": str(source)})
    owner = case["meta"]["id"]
    ids = {column["source"]: column["id"] for column in case["meta"]["columns"]}
    rows = lambda: web.command("query_project_rows", {"projectId": owner, "offset": 0, "limit": 10})["rows"]
    saved_rules = lambda: web.command("load_project", {"projectId": owner}, request=True)["iocs"]
    selected = lambda: web.script("return [...document.querySelectorAll('.data-row input[aria-label^=\"Select row \"]:checked')].map(e=>e.getAttribute('aria-label'));")
    web.command("update_flag", {"projectId": owner, "row_index": 0, "flag": "safe", "memo": "<CommandLine>human note</CommandLine>", "tags": ["manual"], "bookmarked": True})
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"])
        web.results(4)
        original = rows()
        assert original[0]["cells"][ids["message"]]["truncated"], "The source action must resolve a truncated preview"
        web.write('input[aria-label="Search rows"]', "cohort:keep")
        web.applied(search="cohort:keep")
        web.results(3)
        _context(web, ids["source_id"])
        web.named("Filter in", '[aria-label="Cell actions"]')
        web.results(1)
        web.click(web.node('.data-row input[aria-label="Select row 1"]'))
        query, selection = web.query(), selected()
        checkpoint = len(web.trace())
        _context(web, ids["message"])
        web.named("Create IOC rule…", '[aria-label="Cell actions"]')
        wait_for(lambda: web.script("return document.querySelector('[aria-label=\"Rule value 1\"]')?.value;"),
                 lambda value: value == raw, "cell action resolves the complete original spelling")
        assert web.script("return document.querySelector('.ioc-editor .flag-select').dataset.flag;") == "critical"
        assert web.script("return document.querySelector('[aria-label=\"Rule column 1\"]').value;") == ids["message"]
        assert any(e["name"] == "get_cell" and e["request"].get("columnId") == ids["message"] for e in web.trace()[checkpoint:])
        assert saved_rules() == [] and rows() == original, "Creating a draft neither saves rules nor annotates"
        cell_layout = _editor_layout(web)
        for _ in range(2):
            web.named("Add rule", ".ioc-panel")
            wait_for(lambda: web.script("return !!document.querySelector('.ioc-query input');"), bool,
                     "adding a query rule selects the same fixed editor")
            query_layout = _editor_layout(web)
            assert not query_layout["overflow"]
            assert all(abs(query_layout[key] - cell_layout[key]) <= 1 for key in ["top", "height", "left", "width", "listTop"]), (cell_layout, query_layout)
        _select_rule(web, 0)
        assert web.script("return document.querySelector('[aria-label=\"Rule value 1\"]').value;") == raw
        assert _editor_layout(web) == cell_layout, "Switching back to source text keeps the editor and list geometry"
        web.named("Delete rule 3", ".ioc-panel")
        web.named("Delete rule 2", ".ioc-panel")
        web.write('[aria-label="Rule tag 1"]', "cell-review")
        web.close_drawer()
        _panel(web)
        assert web.script("return document.querySelector('[aria-label=\"Rule value 1\"]').value;") == raw, "Hiding the panel preserves its draft"
        web.close_drawer()
        web.named(f"Close {source.name}")
        wait_for(lambda: web.view().get("toast", ""), lambda text: "Save or discard your IOC rules" in text, "dirty IOC blocks closing its tab")
        assert web.state()["active"] == f"project-tab-{owner}"
        checkpoint = len(web.trace())
        web.named("Workspace options")
        web.named("Reload saved project", ".workspace-options")
        wait_for(lambda: web.view().get("toast", ""), lambda text: "Save or discard your IOC rules" in text, "dirty IOC blocks reload")
        assert not any(e["name"] == "load_project" for e in web.trace()[checkpoint:])
        _panel(web)
        web.screenshot(evidence / "ioc-cell-draft-panel.png")
        web.named("Save rules", ".ioc-panel")
        _notice(web, "Rules saved.")
        expected = [{"flag": "critical", "tag": "cell-review", "query": "", "cell": {"column": ids["message"], "value": raw}}]
        assert saved_rules() == expected and rows() == original
        assert web.query() == query and selected() == selection, "Saving definitions preserves filters and selection"
        checkpoint = len(web.trace())
        web.named("Apply", ".ioc-row")
        _notice(web, "2 rows annotated.")
        operations = [e for e in web.trace()[checkpoint:] if e["name"] in {"bulk_preview", "bulk_update"}]
        assert [e["name"] for e in operations] == ["bulk_preview", "bulk_update"]
        assert all(e["request"]["search"] == "" and e["request"]["condition"] == {"column": ids["message"], "op": "eq", "values": [raw], "negated": False} for e in operations)
        applied = rows()
        assert [row["flag"] for row in applied] == ["critical", "", "", "critical"], "Match exact values in the selected column across all source rows"
        assert applied[0]["memo"] == original[0]["memo"] and applied[0]["tags"] == ["manual", "cell-review"] and applied[0]["bookmarked"]
        web.named("Undo", ".ioc-footer")
        _notice(web, "2 rows restored.")
        assert rows() == original
        web.screenshot(evidence / "ioc-cell-inline-panel.png")
        exported = root / "exact-cell-iocs.csv"
        picker(web, exported)
        web.named("Export IOC rules", ".ioc-panel")
        _notice(web, "Rules exported.")
        with exported.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            assert reader.fieldnames == ["flag", "tag", "query", "column", "value"]
            records = list(reader)
        assert len(records) == 1 and records[0]["value"] == raw and records[0]["column"] == "message" and records[0]["query"] == ""
        web.write('[aria-label="Rule value 1"]', "temporary unsaved edit")
        picker(web, exported)
        web.named("Import IOC rules", ".ioc-panel")
        _notice(web, "Rules imported.")
        assert saved_rules() == expected and rows() == original
        assert web.script("return document.querySelector('[aria-label=\"Rule value 1\"]').value;") == raw
        web.close_drawer()
        web.click(web.node(f'.data-row[data-row-index="0"] [data-column-id="{ids["source_id"]}"]'))
        _settle(web)
        web.tap(web.node('.row-detail-dialog [aria-label="Create IOC rule for source_id"]'))
        wait_for(lambda: web.script("return document.querySelector('[aria-label=\"Rule value 2\"]')?.value;"), lambda value: value == "r0", "row detail creates a second Critical cell draft")
        assert not web.script("return !!document.querySelector('.row-detail-dialog');")
        assert web.script("return document.querySelector('.ioc-editor .flag-select').dataset.flag;") == "critical"
        assert web.script("return document.querySelectorAll('.ioc-row .ioc-list-flag')[1].dataset.flag;") == "critical"
        web.named("Discard", ".ioc-panel")
        assert web.script("return document.querySelectorAll('.ioc-row').length;") == 1
        assert saved_rules() == expected and rows() == original
        roundtrip = _verify_mixed_rule_roundtrip(web, owner, raw, ids, original, root, picker)
        report["cell_iocs"] = {"full_raw_value_characters": len(raw), "truncated_preview_resolved": True,
            "critical_draft_only": True, "save_keeps_query_and_selection": True, "exact_column_matching_rows": [0, 3],
            "manual_annotations_preserved": True, "undo_restores_original": True, "extended_csv_roundtrip": True,
            "hidden_draft_retained": True, "dirty_close_and_reload_blocked": True, "row_detail_flag_action": True}
        report["cell_iocs"]["mixed_rule_saved_csv_roundtrip"] = roundtrip
        report["cell_iocs"]["editor_geometry_stable_when_adding_and_switching_rules"] = True
    except Exception:
        web.screenshot(evidence / "cell-ioc-failure.png")
        raise
    finally:
        if web.script("return !!document.querySelector('.row-detail-dialog');"):
            web.named("Close", ".row-detail-dialog")
        _panel(web)
        if web.script("return ![...document.querySelectorAll('.ioc-footer button')].find(e=>e.textContent.trim()==='Discard').disabled;"):
            web.named("Discard", ".ioc-panel")
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        web.named(f"Close {source.name}")
        delete_fixture_project(web, owner)
