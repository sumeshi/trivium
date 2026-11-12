"""Actual nested grouping counts and exact drilldown through the native app."""
from __future__ import annotations

import json
from pathlib import Path
import uuid

from row_navigation import open_project, wait_for
from feedback_checks import _settle


def _rows(web):
    return web.script("return [...document.querySelectorAll('.group-row')].map(e=>({depth:Number(e.dataset.depth),label:e.dataset.groupLabel,missing:e.dataset.groupMissing==='true',display:e.querySelector('.group-value-label').textContent,count:Number(e.querySelector('.group-count').textContent.replaceAll(',',''))}));")


def _row(web, depth: int, label: str, missing: bool = False):
    return wait_for(lambda: web.script("return [...document.querySelectorAll('.group-row')].find(e=>Number(e.dataset.depth)===arguments[0]&&e.dataset.groupLabel===arguments[1]&&(e.dataset.groupMissing==='true')===arguments[2])??null;", depth, label, missing), bool, f"group depth {depth}, label {label!r}, missing {missing}")


def _reset(web, total: int) -> None:
    if web.script("return !document.querySelector('[aria-label=\"Clear all filters\"]').disabled;"):
        web.named("Clear all filters")
    web.applied(search="", searchColumns=[], flagFilter="all", bookmarkedOnly=False, condition=None)
    web.results(total)
    wait_for(lambda: _rows(web), lambda rows: len([row for row in rows if row["depth"] == 0]) == 5
        and sum(row["count"] for row in rows if row["depth"] == 0) == total, "all five exact root groups restored")


def verify_grouping(web, data, root: Path, evidence: Path, report: dict) -> None:
    records = [
        {"level1": "alpha", "level2": "x", "level3": "red"},
        {"level1": "alpha", "level2": "x", "level3": "red"},
        {"level1": "alpha", "level2": "x", "level3": "blue"},
        {"level1": "alpha", "level2": "y", "level3": "red"},
        {"level1": "beta", "level2": "x", "level3": "red"},
        {"level1": "beta", "level2": "y", "level3": "blue"},
        {"level1": None, "level2": "x", "level3": "red"},
        {"level2": "x", "level3": "red"},
        {"level1": "", "level2": "x", "level3": "red"},
        {"level1": " ", "level2": "x", "level3": "red"},
    ]
    for index, record in enumerate(records):
        record["gate"] = "exclude" if index == 1 else "keep" if index % 2 == 0 else "other"
        record["large"] = 9007199254740993 if index < 2 else 9007199254740994
    source = root / f"nested-groups-{uuid.uuid4().hex[:8]}.jsonl"
    source.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    checks = []
    try:
        open_project(web, case["meta"]["name"])
        web.results(len(records))
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
        web.named("Actions for column level1")
        web.named("Group by this column", ".column-context-menu")
        wait_for(lambda: web.script("return document.querySelector('#tool-tab-groups')?.getAttribute('aria-selected');"), lambda value: value == "true", "header action opens Analysis Groups")
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 10, "first grouping contains every source row")
        initial = _rows(web)
        actual = {(row["missing"], row["label"]): row["count"] for row in initial}
        assert actual == {(False, "alpha"): 4, (False, "beta"): 2, (True, ""): 2, (False, ""): 1, (False, " "): 1}, actual
        assert next(row for row in initial if row["missing"])["display"] == "(missing / null)"
        assert next(row for row in initial if row["label"] == "" and not row["missing"])["display"] == "(empty string)"
        assert next(row for row in initial if row["label"] == " ")["display"] == '" "'

        for name in ("level2", "level3"):
            web.choose('select[aria-label="Group column"]', columns[name])
            web.named("Add level", ".grouping-panel")
            wait_for(lambda: web.script("return [...document.querySelectorAll('[aria-label=\"Group levels\"] li')].map(e=>e.dataset.groupColumn);"),
                lambda selected: selected == [columns[f"level{i}"] for i in range(1, int(name[-1]) + 1)], f"{name} added as nested level")
            wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 10, f"{name} aggregation completed")
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 0, "alpha")))
        wait_for(lambda: _rows(web), lambda rows: any(row["depth"] == 1 and row["label"] == "x" and row["count"] == 3 for row in rows), "second-level child counts")
        children = [row for row in _rows(web) if row["depth"] == 1]
        assert {(row["label"], row["count"]) for row in children} == {("x", 3), ("y", 1)}, children
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 1, "x")))
        wait_for(lambda: _rows(web), lambda rows: any(row["depth"] == 2 and row["label"] == "red" and row["count"] == 2 for row in rows), "third-level child counts")
        grandchildren = [row for row in _rows(web) if row["depth"] == 2]
        assert {(row["label"], row["count"]) for row in grandchildren} == {("red", 2), ("blue", 1)}, grandchildren
        _settle(web)
        web.screenshot(evidence / "refinement-nested-groups.png")
        web.named("Collapse all", ".grouping-panel")
        assert all(row["depth"] == 0 for row in _rows(web)), "Collapsed child groups must leave the rendered tree"
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 0, "alpha")))
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 1, "x")))
        web.click(web.script("return arguments[0].querySelector('.group-value');", _row(web, 2, "red")))
        expected = {"kind": "and", "children": [{"column": columns[name], "op": "eq", "values": [value], "negated": False}
            for name, value in (("level1", "alpha"), ("level2", "x"), ("level3", "red"))]}
        web.applied(projectId=project_id, condition=expected)
        web.results(2)
        selected = web.command("query_project_rows", {"projectId": project_id, **{key: value for key, value in web.query().items() if key != "projectId"}, "offset": 0, "limit": 10})
        assert [row["row_index"] for row in selected["rows"]] == [0, 1], selected
        checks.append({"path": ["alpha", "x", "red"], "count": 2, "source_rows": [0, 1]})

        for missing, label, ids in ((True, "", [6, 7]), (False, "", [8]), (False, " ", [9])):
            _reset(web, len(records))
            web.click(web.script("return arguments[0].querySelector('.group-value');", _row(web, 0, label, missing)))
            web.results(len(ids))
            applied = web.query()
            selected = web.command("query_project_rows", {**applied, "offset": 0, "limit": 10})
            assert [row["row_index"] for row in selected["rows"]] == ids, (label, missing, selected)
            checks.append({"missing": missing, "exact_value": label, "source_rows": ids, "count": len(ids)})
        _reset(web, len(records))
        web.named("Move group level2 up", ".grouping-panel")
        wait_for(lambda: web.script("return [...document.querySelectorAll('[aria-label=\"Group levels\"] li')].map(e=>e.dataset.groupColumn);"),
            lambda selected: selected == [columns["level2"], columns["level1"], columns["level3"]], "reordered group levels")
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 2 and sum(row["count"] for row in rows) == 10, "reordered root counts")
        web.named("Move group level2 down", ".grouping-panel")
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 10, "restored root order")
        web.named("Remove group level3", ".grouping-panel")
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 10, "removed level aggregation completes")
        web.choose('select[aria-label="Group column"]', columns["large"])
        web.named("Add level", ".grouping-panel")
        wait_for(lambda: web.script("return [...document.querySelectorAll('[aria-label=\"Group levels\"] li')].map(e=>e.dataset.groupColumn);"),
            lambda selected: selected == [columns["level1"], columns["level2"], columns["large"]], "removed and replaced nesting level")
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 10, "replacement level aggregation completes")

        # Build a real OR in the editor; one matching group member is excluded.
        web.close_drawer()
        web.drawer("Conditions")
        web.choose('select[aria-label="Combine conditions"]', "or")
        for index, value in enumerate(("keep", "other")):
            web.named("Add condition", ".condition-editor")
            web.choose('.condition-row select[aria-label="Column"]', columns["gate"], index)
            web.write('.condition-row input[aria-label="Value"]', value, index)
            web.choose('.condition-row select[aria-label="Operator"]', "eq", index)
        original_or = {"kind": "or", "children": [{"column": columns["gate"], "op": "eq", "values": [value], "negated": False} for value in ("keep", "other")]}
        web.applied(projectId=project_id, condition=original_or)
        web.results(9)
        web.close_drawer()
        web.named("Open Analysis")
        web.click(web.element("#tool-tab-groups"))
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 9, "groups respect the existing OR filter")
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 0, "alpha")))
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 1, "x")))
        large = _row(web, 2, "9007199254740993")
        assert web.script("return arguments[0].querySelector('.group-count').textContent;", large) == "1"
        web.click(web.script("return arguments[0].querySelector('.group-value');", large))
        expected = {"kind": "and", "children": [original_or] + [{"column": columns[name], "op": "eq", "values": [value], "negated": False}
            for name, value in (("level1", "alpha"), ("level2", "x"), ("large", "9007199254740993"))]}
        web.applied(projectId=project_id, condition=expected)
        web.results(1)
        selected = web.command("query_project_rows", {**web.query(), "offset": 0, "limit": 10})
        assert [row["row_index"] for row in selected["rows"]] == [0], selected
        checks.append({"path": ["alpha", "x", "9007199254740993"], "count": 1, "source_rows": [0], "existing_or_preserved": True})
        report["nested_groups"] = {"header_action_opens_groups": True, "levels": 3,
            "reorder_remove_replace": True, "large_integer_exact": True, "existing_or_preserved": True,
            "root_groups": initial, "second_level": children, "third_level": grandchildren, "drilldowns": checks}
    finally:
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        web.named(f"Close {source.name}")
        web.command("delete_project", {"projectId": project_id}, request=True)
        open_project(web, data["name"])
        web.results(data["total"])
