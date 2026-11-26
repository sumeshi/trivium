"""Flexible timestamp display and engine comparisons agree without rewriting evidence."""
from __future__ import annotations

import csv
import hashlib
from pathlib import Path
import uuid

from column_presentation_checks import _action, _value
from fixture_cleanup import delete_fixture_project
from import_recovery_checks import _install_picker, _pick, _import
from row_navigation import open_project, wait_for


def verify_datetime_flex(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    values = [
        ("2026/11/14 12:4:5,123456789 +0900", "2026-11-14T03:04:05.123456789Z"),
        ("Sat, 14 Nov 2026 3:4:5,123456790 GMT", "2026-11-14T03:04:05.12345679Z"),
        ("2026.11.14t3:4:5.123456789 UTC", "2026-11-14T03:04:05.123456789Z"),
        ("2026-11-14T03:04Z", "2026-11-14T03:04:00Z"),
        ("2026/1/4 3:4:5,000000001 UTC", "2026-01-04T03:04:05.000000001Z"),
        ("2026/11/14 03:05:05,123456790 UTC", "2026-11-14T03:05:05.12345679Z"),
        ("14 Nov 2026 12:15:05.123456791 +0900", "2026-11-14T03:15:05.123456791Z"),
    ]
    source = root / f"datetime-flex-{uuid.uuid4().hex[:8]}.csv"
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Timestamp", "Label"])
        writer.writerows((value, f"record-{index}") for index, (value, _) in enumerate(values))
    original = source.read_bytes()
    created: list[dict] = []
    _install_picker(web)
    try:
        web.close_drawer()
        _pick(web, source, csv=True)
        meta = _import(web, source, len(values), created)
        project_id, column = meta["id"], meta["columns"][0]["id"]
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        assert detail["column_kinds"][column] == "datetime", detail["column_kinds"]
        _action(web, column, "Show ISO datetime")
        wait_for(lambda: [_value(web, column, index) for index in range(len(values))],
                 lambda actual: actual == [iso for _, iso in values],
                 "real cells display every flexible timestamp at its exact UTC instant")
        for index, (value, _) in enumerate(values):
            cell = web.command("get_cell", {"projectId": project_id, "rowIndex": index, "columnId": column})
            assert cell["text"] == value, cell

        web.tap(web.node(f'.data-header [data-column-id="{column}"] .column-sort'))
        web.applied(sortKey=column, sortDirection="asc")
        expected_order = [4, 3, 0, 2, 1, 5, 6]
        wait_for(lambda: web.script("return [...document.querySelectorAll('.data-row:not(.loading)')].map(e=>Number(e.dataset.rowIndex));"),
                 lambda order: order == expected_order,
                 "table sorting compares instants and preserves source order for equal timestamps")
        ordered = web.command("query_project_rows", {**web.query(), "offset": 0, "limit": 20})
        assert [row["row_index"] for row in ordered["rows"]] == expected_order, ordered

        lower = "2026/11/14 3:4:5,123456789 UTC"
        upper = "14 Nov 2026 03:04:05.123456790 +0000"
        checked = web.command("validate_condition_values", {"kind": "datetime", "op": "ge", "values": [lower, upper]})
        assert checked["valid"] and all(value["valid"] for value in checked["values"]), checked
        assert int(checked["values"][1]["parsedNanos"]) - int(checked["values"][0]["parsedNanos"]) == 1, checked
        condition = {"kind": "and", "children": [
            {"column": column, "op": op, "values": [value], "negated": False}
            for op, value in [("ge", lower), ("lt", upper)]]}
        filtered = web.command("query_project_rows", {"projectId": project_id, "condition": condition,
            "sortKey": column, "sortDirection": "asc", "offset": 0, "limit": 20})
        assert [row["row_index"] for row in filtered["rows"]] == [0, 2], filtered
        assert [row["cells"][column]["text"] for row in ordered["rows"]] == [values[index][0] for index in expected_order]

        web.drawer("Time analysis")
        web.choose('select[aria-label="Time column"]', column)
        gaps = wait_for(lambda: web.script("return [...document.querySelectorAll('.time-section:first-of-type .span-row')].map(e=>({title:e.title,duration:e.querySelector('.span-measures').textContent.trim()}));"),
                        lambda rows: len(rows) == 3, "flexible source timestamps produce all three time gaps")
        assert all(row["duration"] != "—" for row in gaps), gaps
        assert [row["duration"] for row in gaps[1:]] == ["10m", "1m"], gaps
        assert values[5][0] in gaps[1]["title"] and values[6][0] in gaps[1]["title"], gaps
        web.click(web.script("return document.querySelectorAll('.time-section:first-of-type .span-row')[1];"))
        expected_range = {"kind": "and", "children": [
            {"column": column, "op": op, "values": [value], "negated": False}
            for op, value in [("regex", "\\S"), ("ge", "2026-11-14T03:05:05.123456790Z"),
                              ("le", "2026-11-14T03:15:05.123456791Z")]]}
        web.applied(condition=expected_range)
        web.results(2)
        matched = web.command("query_project_rows", {**web.query(), "offset": 0, "limit": 20})
        assert [row["row_index"] for row in matched["rows"]] == [5, 6], matched
        web.close_drawer()
        web.named("Clear all filters")
        web.results(len(values))

        web.write('input[aria-label="Search rows"]', "record-6")
        web.applied(search="record-6")
        web.results(1)
        web.drawer("Timeline")
        web.choose('select[aria-label="Timeline column"]', column)
        wait_for(lambda: web.script("return Number(document.querySelector('.timeline-chart')?.dataset.inWindow??-1);"),
                 lambda count: count == 1, "one timestamp produces a constant timeline")
        point = web.script("const r=document.querySelector('.timeline-svg').getBoundingClientRect();return {x:Math.round(r.left+1),to:Math.round(r.right-1),y:Math.round(r.top+r.height*.5)};")
        web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "datetime-constant-drag", "parameters": {"pointerType": "mouse"}, "actions": [
            {"type": "pointerMove", "x": point["x"], "y": point["y"], "duration": 0, "origin": "viewport"},
            {"type": "pointerDown", "button": 0},
            {"type": "pointerMove", "x": point["to"], "y": point["y"], "duration": 150, "origin": "viewport"},
            {"type": "pointerUp", "button": 0}]}]})
        instant = values[6][1].removesuffix("Z")
        web.applied(condition={"kind": "and", "children": [
            {"column": column, "op": op, "values": [value], "negated": False}
            for op, value in [("regex", "\\S"), ("ge", instant), ("le", instant)]]})
        web.results(1)
        web.close_drawer()
        web.named("Clear all filters")
        web.results(len(values))
        assert source.read_bytes() == original
        assert meta["source_sha256"] == hashlib.sha256(original).hexdigest()
        web.screenshot(evidence / "flexible-datetime-display.png")
        report["flexible_datetime"] = {"formats": len(values), "shared_datetime_kind": True,
            "iso_display_exact": True, "raw_cells_unchanged": True, "chronological_source_ids": expected_order,
            "one_nanosecond_range_source_ids": [0, 2], "source_hash_and_bytes_preserved": True,
            "engine_normalized_time_ranges": True, "longest_gap_source_ids": [5, 6], "gap_durations": gaps,
            "constant_timeline_actual_pointer_selection": True}
    finally:
        web.script("window.__recoveryPicker?.restore();delete window.__recoveryPicker;")
        web.close_drawer()
        open_project(web, data["name"])
        web.results(data["total"])
        for item in web.command("list_projects"):
            meta = item["meta"]
            if meta.get("source_file_name") == source.name:
                if web.script("return !!document.querySelector(arguments[0]);", f'#project-tab-{meta["id"]}'):
                    web.named(f'Close {source.name}')
                delete_fixture_project(web, meta["id"])
        source.unlink(missing_ok=True)
