"""Decimal comparisons and advisory validation through the actual Rules UI."""
from __future__ import annotations

from pathlib import Path
import uuid

from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for


def verify_decimal_filters(web, data: dict, root: Path, report: dict) -> None:
    outer_query = web.query()
    outer_total = web.state()["total"]
    source = root / f"decimal-filter-{uuid.uuid4().hex[:8]}.csv"
    original = "n\n0.1\n1.5\n2.5\n"
    source.write_text(original, encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    owner = case["meta"]["id"]
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"])
        web.results(3)
        detail = web.command("load_project", {"projectId": owner}, request=True)
        column = detail["columns_meta"][0]["id"]
        assert detail["column_kinds"][column] == "float"
        web.drawer("Conditions")
        web.named("Add condition", "#analysis-drawer")
        web.script("const e=document.querySelector('input[aria-label=\"Value\"]');e.value='1.5';e.dispatchEvent(new Event('input',{bubbles:true}));")
        checked = []
        for op, expected in [("ge", [1, 2]), ("gt", [2]), ("lt", [0]), ("le", [0, 1])]:
            web.choose('select[aria-label="Operator"]', op)
            rule = {"column": column, "op": op, "values": ["1.5"], "negated": False}
            web.applied(projectId=owner, condition=rule)
            web.results(len(expected))
            wait_for(lambda: web.script("return document.querySelector('.value-check')?.textContent??'';"),
                     lambda text: "Number format accepted." in text,
                     "decimal comparison passes the actual input validator")
            assert not web.script("return document.querySelector('input[aria-label=\"Value\"]').getAttribute('aria-invalid')==='true';")
            rows = web.command("query_project_rows", {**web.query(), "offset": 0, "limit": 10})["rows"]
            assert [row["row_index"] for row in rows] == expected, (op, rows)
            assert [row["cells"][column]["text"] for row in rows] == [
                ["0.1", "1.5", "2.5"][index] for index in expected
            ]
            checked.append({"operator": op, "rows": expected})
        assert source.read_text(encoding="utf-8") == original
        report["decimal_filters"] = {"comparisons": checked, "format_check_accepted": True,
                                     "source_unchanged": True}
    finally:
        web.close_drawer()
        open_project(web, data["name"])
        web.applied(**outer_query)
        web.results(outer_total)
        if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{owner}"):
            web.named(f"Close {source.name}")
        delete_fixture_project(web, owner)
