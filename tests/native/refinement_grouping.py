"""Actual nested grouping counts and exact drilldown through the native app."""
from __future__ import annotations

import json
from pathlib import Path
import uuid

from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for
from feedback_checks import _settle


def _rows(web):
    return web.script("return [...document.querySelectorAll('.group-row')].map(e=>({depth:Number(e.dataset.depth),label:e.dataset.groupLabel,missing:e.dataset.groupMissing==='true',column:e.querySelector('.group-value-column').textContent.trim(),display:e.querySelector('.group-value-label').textContent,count:Number(e.querySelector('.group-count').textContent.replaceAll(',',''))}));")


def _row(web, depth: int, label: str, missing: bool = False):
    return wait_for(lambda: web.script("return [...document.querySelectorAll('.group-row')].find(e=>Number(e.dataset.depth)===arguments[0]&&e.dataset.groupLabel===arguments[1]&&(e.dataset.groupMissing==='true')===arguments[2])??null;", depth, label, missing), bool, f"group depth {depth}, label {label!r}, missing {missing}")


def _levels(web):
    return web.script("return [...document.querySelectorAll('[aria-label=\"Group levels\"] li')].map(e=>e.dataset.groupColumn);")


def _condition(columns: dict[str, str], path, base=None):
    children = ([] if base is None else [base]) + [
        {"column": columns[name], "op": "eq", "values": [value], "negated": False}
        for name, value in path
    ]
    return None if not children else children[0] if len(children) == 1 else {"kind": "and", "children": children}


def _select_path(web, labels) -> None:
    for depth, label in enumerate(labels):
        row = _row(web, depth, label)
        if depth + 1 == len(labels):
            web.click(web.script("return arguments[0].querySelector('.group-value');", row))
        elif web.script("return arguments[0].querySelector('.expand-group').getAttribute('aria-expanded');", row) != "true":
            web.click(web.script("return arguments[0].querySelector('.expand-group');", row))


def _sort(web, by: str, direction: str) -> None:
    selector = 'select[aria-label="Sort groups by"]'
    if web.script("return document.querySelector(arguments[0])?.value;", selector) != by:
        web.choose(selector, by)
    action = f"Sort groups {direction}ending"
    if web.script("return !!document.querySelector(arguments[0]);", f'[aria-label="{action}"]'):
        web.named(action, ".grouping-panel")
    _settle(web)
    opposite = "descending" if direction == "asc" else "ascending"
    assert web.script("return !!document.querySelector(arguments[0]);", f'[aria-label="Sort groups {opposite}"]')


def _verify_sorting(web) -> None:
    before = len([entry for entry in web.trace() if entry["name"] == "group_tree"])
    initial = {(row["depth"], row["missing"], row["label"]): row["count"] for row in _rows(web)}
    for by, direction, roots, children, grandchildren in [
        ("count", "asc", [1, 1, 2, 2, 4], ["y", "x"], ["blue", "red"]),
        ("name", "asc", ["alpha", "beta"], ["x", "y"], ["blue", "red"]),
        ("name", "desc", ["beta", "alpha"], ["y", "x"], ["red", "blue"]),
        ("count", "desc", [4, 2, 2, 1, 1], ["x", "y"], ["red", "blue"]),
    ]:
        _sort(web, by, direction)
        rows = _rows(web)
        assert {(row["depth"], row["missing"], row["label"]): row["count"] for row in rows} == initial
        root_rows = [row for row in rows if row["depth"] == 0]
        actual = [row["count"] for row in root_rows] if by == "count" else [
            row["label"] for row in root_rows if row["label"] in {"alpha", "beta"}]
        assert actual == roots, (by, direction, rows)
        assert [row["label"] for row in rows if row["depth"] == 1] == children, (by, direction, rows)
        assert [row["label"] for row in rows if row["depth"] == 2] == grandchildren, (by, direction, rows)
    assert len([entry for entry in web.trace() if entry["name"] == "group_tree"]) == before, "Changing group order must not rescan the source data"


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
        # Navigation is available before any group or managed rule exists.
        original_query, checkpoint = web.query(), len(web.trace())
        web.drawer("Conditions")
        assert not web.script("return [...document.querySelectorAll('.condition-editor button')].find(e=>e.textContent.trim()==='Open Groups')?.disabled??true;"), "Open Groups is enabled without a selected path"
        web.named("Open Groups", ".condition-editor")
        wait_for(lambda: web.script("return document.querySelector('#tool-tab-groups')?.getAttribute('aria-selected');"),
                 lambda value: value == "true", "empty Rules opens Groups")
        assert _levels(web) == [] and not _rows(web)
        assert not web.script("return [...document.querySelectorAll('.grouping-panel button')].find(e=>e.textContent.trim()==='Open Filters')?.disabled??true;"), "Open Filters is enabled without configured levels"
        web.named("Open Filters", ".grouping-panel")
        wait_for(lambda: web.script("return document.querySelector('#tool-tab-conditions')?.getAttribute('aria-selected');"),
                 lambda value: value == "true", "empty Groups opens Filter Rules")
        _settle(web)
        assert web.query() == original_query, "Empty navigation changes no query settings"
        assert not [entry for entry in web.trace()[checkpoint:] if entry["name"] in {"query_project_rows", "group_tree"}], "Empty navigation needs no result scan or grouping request"
        web.close_drawer()
        assert web.script("return document.activeElement?.id;") == "tool-group-filters", "Closing returns focus to the destination Filter control"
        web.named("Actions for column level1")
        web.named("Group by this column", ".column-context-menu")
        wait_for(lambda: web.script("return document.querySelector('#tool-tab-groups')?.getAttribute('aria-selected');"), lambda value: value == "true", "header action opens Analysis Groups")
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 10, "first grouping contains every source row")
        initial = _rows(web)
        actual = {(row["missing"], row["label"]): row["count"] for row in initial}
        assert actual == {(False, "alpha"): 4, (False, "beta"): 2, (True, ""): 2, (False, ""): 1, (False, " "): 1}, actual
        assert [row["count"] for row in initial] == [4, 2, 2, 1, 1], "New groupings show the largest groups first"
        assert web.script("return document.querySelector('select[aria-label=\"Sort groups by\"]').value;") == "count"
        assert web.script("return !!document.querySelector('[aria-label=\"Sort groups ascending\"]');"), "Count defaults to descending"
        assert all(row["column"] == "level1" for row in initial), "Each result identifies its source column beside its value"
        assert next(row for row in initial if row["missing"])["display"] == "(missing / null)"
        assert next(row for row in initial if row["label"] == "" and not row["missing"])["display"] == "(empty string)"
        assert next(row for row in initial if row["label"] == " ")["display"] == '" "'
        assert not web.script("return !!document.querySelector('.group-granularity');"), "Text groups have no datetime granularity selector"
        assert web.script("return document.querySelector('[aria-label=\"Group level 1 column\"]').value;") == columns["level1"]
        assert web.script("return [...document.querySelectorAll('.group-result-heading span')].map(element=>element.textContent.trim());") == ["Value", "Count"]
        assert web.script("return document.querySelector('.group-summary .section-label')?.textContent;") == "Values"

        web.click(web.script("return arguments[0].querySelector('.group-value');", _row(web, 0, "alpha")))
        web.results(4)
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 1 and rows[0]["label"] == "alpha" and rows[0]["count"] == 4,
                 "a selected group remains one group in the filtered results")
        selected_query, checkpoint = web.query(), len(web.trace())
        for _ in range(3):
            web.click(web.script("return arguments[0].querySelector('.group-value');", _row(web, 0, "alpha")))
            _settle(web)
        assert web.query() == selected_query, "Repeated clicks must not duplicate a group filter"
        assert not [entry for entry in web.trace()[checkpoint:] if entry["name"] in {"query_project_rows", "group_tree"}], "Repeated selection must not reload the same results"
        web.named("Ungroup last level", ".grouping-panel")
        web.applied(projectId=project_id, condition=None)
        web.results(len(records))
        assert _levels(web) == [], "Ungroup removes the only configured level and its filter"
        web.choose('select[aria-label="Group column"]', columns["level1"])
        web.named("Add level", ".grouping-panel")
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 10,
                 "Ungroup removes its own filter before rebuilding the hierarchy")

        for name in ("level2", "level3"):
            web.choose('select[aria-label="Group column"]', columns[name])
            web.named("Add level", ".grouping-panel")
            wait_for(lambda: web.script("return [...document.querySelectorAll('[aria-label=\"Group levels\"] li')].map(e=>e.dataset.groupColumn);"),
                lambda selected: selected == [columns[f"level{i}"] for i in range(1, int(name[-1]) + 1)], f"{name} added as nested level")
            wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 10, f"{name} aggregation completed")
        assert _levels(web) == [columns[name] for name in ("level1", "level2", "level3")]
        assert not web.script("return [...document.querySelectorAll('.grouping-panel label')].some(element=>['Group by','Then by'].includes(element.textContent.trim()));"), "Hierarchy controls name columns without repetitive Group by/Then by captions"
        options = web.script("return [...document.querySelector('[aria-label=\"Group level 2 column\"]').options].map(option=>option.value);")
        assert columns["level2"] in options and not {columns["level1"], columns["level3"]}.intersection(options), "A nested level cannot duplicate another selected column"
        for replacement in ("large", "level3"):
            start = len(web.trace())
            web.choose('select[aria-label="Group level 3 column"]', columns[replacement])
            expected_columns = [columns["level1"], columns["level2"], columns[replacement]]
            wait_for(lambda: [entry for entry in web.trace()[start:] if entry["name"] == "group_tree"
                     and entry["request"].get("groupColumns") == expected_columns and "result" in entry],
                     bool, "editing a nested level aggregates the chosen source column")
            wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 10,
                     "nested replacement preserves complete root counts")
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 0, "alpha")))
        wait_for(lambda: _rows(web), lambda rows: any(row["depth"] == 1 and row["label"] == "x" and row["count"] == 3 for row in rows), "second-level child counts")
        children = [row for row in _rows(web) if row["depth"] == 1]
        assert {(row["label"], row["count"]) for row in children} == {("x", 3), ("y", 1)}, children
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 1, "x")))
        wait_for(lambda: _rows(web), lambda rows: any(row["depth"] == 2 and row["label"] == "red" and row["count"] == 2 for row in rows), "third-level child counts")
        grandchildren = [row for row in _rows(web) if row["depth"] == 2]
        assert {(row["label"], row["count"]) for row in grandchildren} == {("red", 2), ("blue", 1)}, grandchildren
        assert all(row["column"] == f"level{row['depth'] + 1}" for row in _rows(web)), "Nested values retain their column context"
        _verify_sorting(web)
        _settle(web)
        clipped_columns = web.script("return [...document.querySelectorAll('.group-value-column')].filter(e=>e.scrollWidth>e.clientWidth+1).map(e=>e.textContent);")
        assert not clipped_columns, f"Short nested column names must remain readable: {clipped_columns}"
        web.screenshot(evidence / "refinement-nested-groups.png")
        web.named("Collapse all", ".grouping-panel")
        assert all(row["depth"] == 0 for row in _rows(web)), "Collapsed child groups must leave the rendered tree"
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 0, "alpha")))
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 1, "x")))
        web.click(web.script("return arguments[0].querySelector('.group-value');", _row(web, 2, "red")))
        expected = _condition(columns, (("level1", "alpha"), ("level2", "x"), ("level3", "red")))
        web.applied(projectId=project_id, condition=expected)
        web.results(2)
        selected = web.command("query_project_rows", {"projectId": project_id, **{key: value for key, value in web.query().items() if key != "projectId"}, "offset": 0, "limit": 10})
        assert [row["row_index"] for row in selected["rows"]] == [0, 1], selected
        checks.append({"path": ["alpha", "x", "red"], "count": 2, "source_rows": [0, 1]})
        web.named("All groups", ".grouping-panel")
        web.applied(projectId=project_id, condition=None)
        web.results(len(records))
        assert len(_levels(web)) == 3, "All groups clears only its drilldown and keeps the configured hierarchy"

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
        assert not web.script("return !!document.querySelector('.group-granularity');"), "Text and integer groups use exact values without a redundant selector"

        # Build a real OR in the editor; one matching group member is excluded.
        web.named("Open Filters", ".grouping-panel")
        wait_for(lambda: web.script("return document.querySelector('#tool-tab-conditions')?.getAttribute('aria-selected');"),
                 lambda value: value == "true", "configured Groups opens Filter Rules without a selected path")
        assert web.query()["condition"] is None
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
        web.named("Open Analyze")
        web.click(web.element("#tool-tab-groups"))
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 9, "groups respect the existing OR filter")
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 0, "alpha")))
        web.click(web.script("return arguments[0].querySelector('.expand-group');", _row(web, 1, "x")))
        large = _row(web, 2, "9007199254740993")
        assert web.script("return arguments[0].querySelector('.group-count').textContent;", large) == "1"
        web.click(web.script("return arguments[0].querySelector('.group-value');", large))
        path = (("level1", "alpha"), ("level2", "x"), ("large", "9007199254740993"))
        expected = _condition(columns, path, original_or)
        web.applied(projectId=project_id, condition=expected)
        web.results(1)
        selected = web.command("query_project_rows", {**web.query(), "offset": 0, "limit": 10})
        assert [row["row_index"] for row in selected["rows"]] == [0], selected
        checks.append({"path": ["alpha", "x", "9007199254740993"], "count": 1, "source_rows": [0], "existing_or_preserved": True})
        assert web.script("return [...document.querySelectorAll('nav[aria-label=\"Selected group path\"] .path-part')].map(e=>e.title);") == [
            f"{name}: {value}" for name, value in path], "The selected path exposes each column and exact value"
        saved_query = web.query()
        web.tab(data["id"])
        web.results(data["total"])
        checkpoint = len(web.trace())
        web.tab(project_id)
        web.applied(projectId=project_id, condition=expected)
        web.results(1)
        wait_for(lambda: web.script("return document.querySelector('#tool-tab-groups')?.getAttribute('aria-selected');"),
                 lambda selected: selected == "true", "returning to the source tab restores its Groups drawer")
        assert _levels(web) == [columns[name] for name, _ in path], "Tab switching keeps the editable group hierarchy"
        wait_for(lambda: web.script("return [...document.querySelectorAll('nav[aria-label=\"Selected group path\"] .path-part')].map(e=>e.title);"),
                 lambda titles: titles == [f"{name}: {value}" for name, value in path],
                 "returning to the source tab restores its selected group path")
        assert web.query() == saved_query, "Tab switching must not replace the investigation's selected group filter"
        returned_group_reads = wait_for(lambda: [entry for entry in web.trace()[checkpoint:]
            if entry["name"] == "group_tree" and entry["request"].get("projectId") == project_id],
            lambda entries: bool(entries and "result" in entries[0]), "the restored Groups drawer loads its saved selection")
        assert returned_group_reads[0]["request"].get("condition") == expected, "The first restored group request must use the saved filter"
        _settle(web)
        web.screenshot(evidence / "group-selected-path.png")
        web.named("Back", ".history-buttons")
        web.applied(projectId=project_id, condition=original_or)
        web.results(9)
        assert not web.script("return !!document.querySelector('nav[aria-label=\"Selected group path\"]');"), "Rules history Back removes the selected group path"
        web.named("Forward", ".history-buttons")
        web.applied(projectId=project_id, condition=expected)
        web.results(1)
        wait_for(lambda: web.script("return [...document.querySelectorAll('nav[aria-label=\"Selected group path\"] .path-part')].map(e=>e.title);"),
                 lambda titles: titles == [f"{name}: {value}" for name, value in path],
                 "Rules history Forward restores verified group ownership and its breadcrumb")
        for depth, count in ((2, 2), (1, 3), (0, 9)):
            web.named("Back one group", ".grouping-panel")
            web.applied(projectId=project_id, condition=_condition(columns, path[:depth], original_or))
            web.results(count)
            assert len(_levels(web)) == 3, "Back changes the selected group path without removing configured levels"
        assert not web.script("return !!document.querySelector('[aria-label=\"Back one group\"]:not(:disabled)');"), "There is no unrelated Rules-history fallback after returning to all groups"
        before_clear = web.query()
        _select_path(web, [value for _, value in path])
        web.applied(projectId=project_id, condition=expected)
        web.results(1)
        checkpoint = len(web.trace())
        web.click(web.node('nav[aria-label="Selected group path"] [aria-current="step"]'))
        _settle(web)
        assert web.query()["condition"] == expected
        assert not [entry for entry in web.trace()[checkpoint:] if entry["name"] in {"query_project_rows", "group_tree"}], "Selecting the current breadcrumb must not duplicate filters or reload"
        web.click(web.node('nav[aria-label="Selected group path"] .path-part'))
        web.applied(projectId=project_id, condition=_condition(columns, path[:1], original_or))
        web.results(3)
        web.named("All groups", ".grouping-panel")
        web.applied(projectId=project_id, condition=original_or)
        web.results(9)
        assert len(_levels(web)) == 3, "The All groups breadcrumb keeps the hierarchy available for the next drilldown"
        _select_path(web, [value for _, value in path])
        web.applied(projectId=project_id, condition=expected)
        web.results(1)
        ungroup_steps = []
        for depth, count in ((2, 2), (1, 3), (0, 9)):
            web.named("Ungroup last level", ".grouping-panel")
            web.applied(projectId=project_id, condition=_condition(columns, path[:depth], original_or))
            web.results(count)
            wait_for(lambda: _levels(web), lambda selected: selected == [columns[name] for name, _ in path[:depth]],
                     f"Ungroup removes only level {depth + 1} and its corresponding filter")
            ungroup_steps.append({"levels_remaining": depth, "rows": count})
        assert not _rows(web), "Removing the last level removes the aggregate list"
        assert web.query() == before_clear, "Ungroup removes its drilldown while preserving investigation Rules"
        web.choose('select[aria-label="Group column"]', columns["level1"])
        web.named("Add level", ".grouping-panel")
        wait_for(lambda: _rows(web), lambda rows: len(rows) == 5 and sum(row["count"] for row in rows) == 9,
                 "grouping can be rebuilt after clearing without losing prior Rules")
        _select_path(web, ["alpha"])
        web.applied(projectId=project_id, condition=_condition(columns, path[:1], original_or))
        web.results(3)
        web.named("Open Filters", ".grouping-panel")
        wait_for(lambda: web.script("return document.querySelector('#tool-tab-conditions')?.getAttribute('aria-selected');"),
                 lambda value: value == "true", "the selected Groups path opens its Filter Rules")
        assert web.query()["condition"] == _condition(columns, path[:1], original_or)
        managed = web.script("return [...document.querySelectorAll('.managed-rule')].map(e=>({text:e.textContent,controls:e.querySelectorAll('input,select,button').length}));")
        assert len(managed) == 1 and managed[0]["controls"] == 0, "Group predicates are identified and read-only until converted"
        assert "Group" in managed[0]["text"] and "alpha" in managed[0]["text"], managed
        web.screenshot(evidence / "group-managed-rules.png")
        connectors = web.script("const control=document.querySelector('.condition-row input[aria-label=\"Value\"]');return [...document.querySelectorAll('.condition-connector')].map(e=>({font:parseFloat(getComputedStyle(e).fontSize),control:parseFloat(getComputedStyle(control).fontSize),weight:Number(getComputedStyle(e).fontWeight)}));")
        assert connectors and all(item["font"] <= item["control"] and item["weight"] <= 500 for item in connectors), connectors
        web.named("Open Groups", ".condition-editor")
        wait_for(lambda: _levels(web), lambda levels: levels == [columns["level1"]], "Rules links to the active group hierarchy")
        assert web.query()["condition"] == _condition(columns, path[:1], original_or)
        assert web.script("return !!document.querySelector('nav[aria-label=\"Selected group path\"]');")
        web.named("Open Filters", ".grouping-panel")
        wait_for(lambda: web.script("return document.querySelector('#tool-tab-conditions')?.getAttribute('aria-selected');"),
                 lambda value: value == "true", "reciprocal navigation returns to the unchanged group Rules")
        checkpoint = len(web.trace())
        web.named("Make rule", ".condition-editor")
        _settle(web)
        assert not web.script("return !!document.querySelector('.managed-rule,.managed-group-actions');")
        assert web.query()["condition"] == _condition(columns, path[:1], original_or), "Conversion changes ownership, not matching rows"
        assert not [entry for entry in web.trace()[checkpoint:] if entry["name"] == "query_project_rows"], "Converting the same query needs no new scan"
        value_index = web.script("return [...document.querySelectorAll('.condition-row')].findIndex(e=>e.querySelector('select[aria-label=\"Column\"]').value===arguments[0]);", columns["level1"])
        assert value_index >= 0
        web.write('.condition-row input[aria-label="Value"]', "beta", value_index)
        manually_edited = _condition(columns, (("level1", "beta"),), original_or)
        web.applied(projectId=project_id, condition=manually_edited)
        web.results(2)
        web.close_drawer()
        web.drawer("Groups")
        web.named("Ungroup last level", ".grouping-panel")
        _settle(web)
        assert _levels(web) == []
        assert web.query()["condition"] == manually_edited, "Ungroup must not remove a group constraint after the user edits it in Rules"
        web.results(2)
        report["nested_groups"] = {"header_action_opens_groups": True, "levels": 3,
            "reorder_remove_replace": True, "large_integer_exact": True, "existing_or_preserved": True,
            "default_count_descending": True, "values_identify_source_column": True,
            "name_and_count_sort_both_directions": True, "nested_sort_preserves_expansion_and_values": True,
            "nested_columns_editable_without_duplicates": True, "local_back_preserves_previous_rules": True,
            "group_hierarchy_path_and_ownership_survive_tab_switch": True,
            "global_rules_back_forward_restores_group_ownership": True,
            "breadcrumb_ancestor_shortens_filter_path": True, "current_breadcrumb_does_not_reload": True,
            "all_groups_keeps_configured_hierarchy": True, "manually_edited_rule_survives_ungroup": True,
            "group_rules_readonly_until_converted": True, "open_groups_from_rules": True,
            "navigation_permanent_in_empty_state_without_scan": True, "selected_group_rules_roundtrip": True,
            "make_rule_preserves_query_without_rescan": True,
            "ungroup_removes_one_level_and_filter_at_a_time": ungroup_steps,
            "repeated_single_group_does_not_duplicate_or_reload": True, "ungroup_removes_only_group_filter": True,
            "sort_does_not_rescan": True, "non_datetime_granularity_hidden": True,
            "root_groups": initial, "second_level": children, "third_level": grandchildren, "drilldowns": checks}
    finally:
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        web.named(f"Close {source.name}")
        delete_fixture_project(web, project_id)
        open_project(web, data["name"])
        web.results(data["total"])
