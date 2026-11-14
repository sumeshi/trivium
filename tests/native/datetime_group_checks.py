"""Calendar grouping and exact time-range drill-down in the real native app."""
from __future__ import annotations

import json
from pathlib import Path
import uuid

from feedback_checks import _settle
from refinement_grouping import _row, _rows
from row_navigation import open_project, wait_for


def verify_datetime_groups(web, data, root: Path, evidence: Path, report: dict) -> None:
    records = [
        {"host": "alpha", "timestamp": "2026-11-14T23:59:59.999999999Z"},
        {"host": "alpha", "timestamp": "2026-11-15T08:59:59.999999999+09:00"},
        {"host": "alpha", "timestamp": "2026-11-15T00:00:00Z"},
        {"host": "beta", "timestamp": "2026-11-15T00:00:00.000000001Z"},
        {"host": "alpha", "timestamp": None},
        {"host": "alpha"},
        {"host": "alpha", "timestamp": ""},
        {"host": "alpha", "timestamp": " \t "},
        {"host": "beta", "timestamp": "2026/11/14 02:03:04.123456789"},
    ]
    source = root / f"datetime-groups-{uuid.uuid4().hex[:8]}.jsonl"
    source.write_text("".join(json.dumps(row) + "\n" for row in records), encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"])
        web.results(len(records))
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
        web.drawer("Groups")
        web.choose('select[aria-label="Group column"]', columns["timestamp"])
        web.named("Add level", ".grouping-panel")
        web.choose('select[aria-label="Group timestamp by"]', "day")

        def day_groups():
            return [row for row in _rows(web) if row["depth"] == 0]

        wait_for(day_groups, lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 9,
            "UTC day buckets include every row and preserve blank/null groups")
        daily = day_groups()
        assert {(row["label"], row["missing"]): row["count"] for row in daily} == {
            ("2026-11-14", False): 3, ("2026-11-15", False): 2,
            ("", True): 2, ("", False): 1, (" \t ", False): 1,
        }, daily
        web.click(web.script("return arguments[0].querySelector('.group-value');", _row(web, 0, "2026-11-14")))
        time_conditions = [{"column": columns["timestamp"], "op": op, "values": [value], "negated": False}
            for op, value in [("regex", "\\S"), ("ge", "2026-11-14T00:00:00Z"), ("lt", "2026-11-15T00:00:00Z")]]
        web.applied(projectId=project_id, condition={"kind": "and", "children": time_conditions})
        web.results(3)
        selected = web.command("query_project_rows", {**web.query(), "offset": 0, "limit": 20})
        assert [row["row_index"] for row in selected["rows"]] == [0, 1, 8], selected

        web.named("Clear all filters")
        web.results(9)
        wait_for(day_groups, lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 9, "all UTC days restored")
        web.choose('select[aria-label="Group column"]', columns["host"])
        web.named("Add level", ".grouping-panel")
        wait_for(lambda: web.script("return document.querySelectorAll('[aria-label=\"Group levels\"] li').length;"),
            lambda count: count == 2, "ordinary host nested below the day level")
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 9,
            "nested day aggregation completes")
        expand_day = wait_for(lambda: web.script("return [...document.querySelectorAll('.group-row')].find(e=>e.dataset.depth==='0'&&e.dataset.groupLabel==='2026-11-14')?.querySelector('.expand-group')??null;"),
            bool, "the completed day aggregation contains its nested host groups")
        web.click(expand_day)
        wait_for(lambda: _rows(web), lambda rows: any(row["depth"] == 1 and row["label"] == "alpha" and row["count"] == 2 for row in rows),
            "nested day/host counts respect normalized offset instants")
        web.click(web.script("return arguments[0].querySelector('.group-value');", _row(web, 1, "alpha")))
        expected = {"kind": "and", "children": time_conditions + [
            {"column": columns["host"], "op": "eq", "values": ["alpha"], "negated": False}]}
        web.applied(projectId=project_id, condition=expected)
        web.results(2)
        nested = web.command("query_project_rows", {**web.query(), "offset": 0, "limit": 20})
        assert [row["row_index"] for row in nested["rows"]] == [0, 1], nested
        web.named("Clear all filters")
        web.results(9)
        web.choose('select[aria-label="Group timestamp by"]', "hour")
        wait_for(day_groups, lambda rows: len(rows) == 6 and sum(row["count"] for row in rows) == 9, "UTC hour buckets")
        hourly = day_groups()
        assert {row["label"]: row["count"] for row in hourly if row["label"].startswith("2026-")} == {
            "2026-11-14T02:00Z": 1, "2026-11-14T23:00Z": 2, "2026-11-15T00:00Z": 2,
        }, hourly
        web.close_drawer()
        web.drawer("Groups")
        assert web.script("return document.querySelector('select[aria-label=\"Group timestamp by\"]').value;") == "hour"
        web.named("Move group host up", ".grouping-panel")
        wait_for(lambda: web.script("return [...document.querySelectorAll('[aria-label=\"Group levels\"] li')].map(e=>e.dataset.groupColumn);"),
            lambda ids: ids == [columns["host"], columns["timestamp"]], "time granularity follows its source column when levels reorder")
        assert web.script("return document.querySelector('select[aria-label=\"Group timestamp by\"]').value;") == "hour"
        wait_for(day_groups, lambda rows: len(rows) == 2 and sum(row["count"] for row in rows) == 9,
            "reordered host roots contain all rows")
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 0, "alpha")))
        wait_for(lambda: _rows(web), lambda rows: any(row["depth"] == 1 and row["label"] == "2026-11-14T23:00Z" and row["count"] == 2 for row in rows),
            "the hour granularity survives backend level reordering")
        original = web.command("query_project_rows", {"projectId": project_id, "offset": 0, "limit": 20})
        for index in (0, 1, 8):
            assert original["rows"][index]["cells"][columns["timestamp"]]["text"] == records[index]["timestamp"], original["rows"][index]
        _settle(web)
        web.screenshot(evidence / "datetime-granular-groups.png")
        report["datetime_groups"] = {"timezone": "UTC", "days": daily, "hours": hourly,
            "day_source_ids": [0, 1, 8], "nested_source_ids": [0, 1],
            "granularity_survives_drawer_and_reorder": True, "source_timestamp_precision_unchanged": True}
    finally:
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        web.named(f"Close {source.name}")
        web.command("delete_project", {"projectId": project_id}, request=True)
        open_project(web, data["name"])
        web.results(data["total"])
