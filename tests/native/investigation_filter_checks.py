"""Mixed datetime analysis and drilldown preserve an investigation's Rules."""
from __future__ import annotations

from pathlib import Path
import uuid

from column_pin_checks import _key
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for


def _leaves(node: dict | None) -> list[dict]:
    if not node:
        return []
    if "children" not in node:
        return [node]
    return [leaf for child in node["children"] for leaf in _leaves(child)]


def _has_subtree(node: dict | None, expected: dict) -> bool:
    return node == expected or any(_has_subtree(child, expected) for child in (node or {}).get("children", []))


def _query_condition(web, project_id: str, predicate, label: str) -> dict:
    def latest():
        query = web.query()
        return query if query["projectId"] == project_id and predicate(query.get("condition")) else None
    return wait_for(latest, bool, label)["condition"]


def _range_from_first_bucket(web, project_id: str, column: str, expected_leaves: int) -> dict:
    assert web.script("return document.querySelector('[aria-label=\"Timeline column\"]').value;") == column, "Timeline keeps the chosen datetime interpretation across drawer switches"
    active_condition = web.query()["condition"]
    wait_for(lambda: next((entry for entry in reversed(web.trace())
             if entry["name"] == "histogram" and "result" in entry
             and entry["request"].get("projectId") == project_id
             and entry["request"].get("column") == column
             and entry["request"].get("asDatetime") is True
             and entry["request"].get("buckets") == 60
             and entry["request"].get("condition") == active_condition), None),
             bool, "timeline histogram uses the actual applied Rules")
    wait_for(lambda: web.script("return document.querySelector('.timeline-summary')?.textContent??'';"),
             lambda text: "In window" in text, "timeline for the applied Rules")
    before = len(web.trace())
    web.script("document.querySelector('.timeline-svg').focus();")
    _key(web, "\ue011")  # Home
    _key(web, "\ue007")  # Enter selects the exact bucket interval.
    wait_for(lambda: next((entry for entry in reversed(web.trace()[before:])
             if entry["name"] == "query_project_rows" and "result" in entry
             and "searchColumns" in entry["request"]), None), bool, "new timeline range query completes")
    condition = _query_condition(web, project_id,
        lambda node: len(_leaves(node)) == expected_leaves and any(leaf["op"] == "ge" for leaf in _leaves(node)),
        "timeline appends guarded bounds without replacing Rules")
    web.results(1)
    return condition


def verify_investigation_filters(web, data: dict, root: Path, evidence: Path, report: dict) -> None:
    outer_query, outer_total = web.query(), web.state()["total"]
    source = root / f"mixed-datetime-{uuid.uuid4().hex[:8]}.csv"
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "mixed-datetime.csv"
    source.write_bytes(fixture.read_bytes())
    original = source.read_bytes()
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"])
        web.results(5)
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        ids = {column["label"]: column["id"] for column in detail["columns_meta"]}
        timestamp = ids["Timestamp"]
        assert detail["column_kinds"][timestamp] == "string"

        web.drawer("Timeline")
        assert web.script("return document.querySelector('[aria-label=\"Timeline column\"]').value;") == ""
        web.choose('select[aria-label="Timeline column"]', timestamp)
        wait_for(lambda: web.script("return document.querySelector('.timeline-summary')?.textContent??'';"),
                 lambda text: "In window 3 / 5" in text and "Missing 1" in text and "Unparseable 1" in text,
                 "manual datetime timeline counts valid, blank, and invalid values")
        web.drawer("Time analysis")
        web.choose('select[aria-label="Time column"]', timestamp)
        wait_for(lambda: web.script("return document.querySelector('.time-panel')?.textContent??'';"),
                 lambda text: "Missing 1" in text and "Unparseable 1" in text, "manual datetime time analysis")
        web.drawer("Column stats")
        web.choose('select[aria-label="Stats column"]', timestamp)
        web.choose('select[aria-label="Histogram interpretation"]', "datetime")
        wait_for(lambda: web.script("return document.querySelector('.histogram-section')?.textContent??'';"),
                 lambda text: "3 values" in text and "Missing 1" in text and "Unparseable 1" in text,
                 "manual datetime stats histogram")
        web.drawer("Time analysis")
        assert web.script("return document.querySelector('[aria-label=\"Time column\"]').value;") == timestamp
        web.drawer("Column stats")
        assert web.script("return document.querySelector('[aria-label=\"Stats column\"]').value;") == timestamp
        assert web.script("return document.querySelector('[aria-label=\"Histogram interpretation\"]').value;") == "datetime"

        checked = []
        for kind in ("and", "or"):
            web.close_drawer()
            if web.script("return !document.querySelector('[aria-label=\"Clear all filters\"]').disabled;"):
                web.named("Clear all filters")
                web.applied(projectId=project_id, condition=None)
                web.results(5)
            web.drawer("Conditions")
            web.choose('select[aria-label="Combine conditions"]', kind)
            rules = []
            for index, (column, value) in enumerate([(ids["Host"], "WS01"), (ids["EventID"], "4624" if kind == "and" else "4625")]):
                web.named("Add condition", ".condition-editor")
                web.choose('.condition-row select[aria-label="Column"]', column, index)
                web.choose('.condition-row select[aria-label="Operator"]', "eq", index)
                web.write('.condition-row input[aria-label="Value"]', value, index)
                rules.append({"column": column, "op": "eq", "values": [value], "negated": False})
            original_rules = {"kind": kind, "children": rules}
            web.applied(projectId=project_id, condition=original_rules)
            web.results(3 if kind == "and" else 4)
            web.drawer("Timeline")
            condition = _range_from_first_bucket(web, project_id, timestamp, 5)
            assert all(rule in _leaves(condition) for rule in rules), condition
            if kind == "or":
                assert _has_subtree(condition, original_rules), condition
            assert any(leaf["column"] == timestamp and leaf["op"] == "regex" and leaf["values"] == [r"\S"] for leaf in _leaves(condition)), condition
            # Re-selecting a range updates that timeline's existing group.
            condition = _range_from_first_bucket(web, project_id, timestamp, 5)
            web.drawer("Column stats")
            web.choose('select[aria-label="Stats column"]', ids["Host"])
            value = wait_for(lambda: web.script("return [...document.querySelectorAll('.value-count-row')].find(e=>e.querySelector('.value-label')?.textContent==='WS01')??null;"),
                             bool, "scoped Host stats value")
            web.click(value)
            after_stats = _query_condition(web, project_id, lambda node: len(_leaves(node)) == 6,
                                          "column stats appends to both manual and timeline Rules")
            web.results(1)
            assert all(rule in _leaves(after_stats) for rule in rules), after_stats
            assert all(rule in _leaves(after_stats) for rule in _leaves(condition)), after_stats
            if kind == "or":
                assert _has_subtree(after_stats, original_rules), after_stats
            rows = web.command("query_project_rows", {**web.query(), "offset": 0, "limit": 5})["rows"]
            assert [row["row_index"] for row in rows] == [0], rows
            checked.append({"root": kind, "manual_rules_preserved": True, "range_rules": 3,
                            "repeated_range_does_not_accumulate": True, "stats_preserves_rules": True, "source_rows": [0]})
        assert source.read_bytes() == original
        assert web.command("load_project", {"projectId": project_id}, request=True)["column_kinds"][timestamp] == "string"
        web.screenshot(evidence / "investigation-rules-preserved.png")
        report["investigation_filters"] = {"manual_datetime": {"valid": 3, "missing": 1, "unparseable": 1}, "checks": checked}
    finally:
        web.close_drawer()
        open_project(web, data["name"])
        web.applied(**outer_query)
        web.results(outer_total)
        if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{project_id}"):
            web.named(f"Close {source.name}")
        delete_fixture_project(web, project_id)
