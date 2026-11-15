"""Restore a deep table position after moving through rule history."""
from __future__ import annotations

from pathlib import Path
import uuid

from column_pin_checks import _key
from feedback_checks import _settle
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project


def verify_history_scroll(web, data: dict, root: Path, report: dict) -> None:
    outer_query = web.query()
    outer_total = web.state()["total"]
    source = root / f"history-scroll-{uuid.uuid4().hex[:8]}.csv"
    source.write_text("source_id,cohort\n" + "".join(
        f"r-{index + 1:07d},{'keep' if index % 52 == 0 else 'drop'}\n" for index in range(5199)
    ), encoding="utf-8")
    case = web.command("create_project", {"path": str(source)})
    owner = case["meta"]["id"]
    try:
        web.close_drawer()
        open_project(web, case["meta"]["name"])
        web.results(5199)
        detail = web.command("load_project", {"projectId": owner}, request=True)
        cohort = next(column["id"] for column in detail["columns_meta"] if column["label"] == "cohort")
        web.write('input[aria-label="Original row number"]', "4001")
        _key(web, "\ue007")
        web.result(5199, 4001)
        web.drawer("Conditions")
        _settle(web)
        saved_row = int(web.state()["scrollTop"] // 32)
        assert saved_row > 3900
        web.named("Add condition", "#analysis-drawer")
        web.choose('select[aria-label="Column"]', cohort)
        # One input event represents pasting a complete rule value. The Back
        # assertion should cover query changes, rather than each typed letter.
        web.script("const e=document.querySelector('input[aria-label=\"Value\"]');e.value='keep';e.dispatchEvent(new Event('input',{bubbles:true}));")
        rule = {"column": cohort, "op": "contains", "values": ["keep"], "negated": False}
        web.applied(projectId=owner, condition=rule)
        web.results(100)
        for _ in range(2):
            web.named("Back", ".history-buttons")
            web.applied(projectId=owner, condition=None)
            restored = web.result(5199, 4001)
            assert int(restored["scrollTop"] // 32) == saved_row, (saved_row, restored)
            web.named("Forward", ".history-buttons")
            web.applied(projectId=owner, condition=rule)
            narrowed = web.results(100)
            assert narrowed["scrollTop"] < 32, narrowed
        report["history_scroll"] = {"source_rows": 5199, "filtered_rows": 100,
            "restored_logical_row": saved_row, "back_forward_roundtrips": 2}
    finally:
        web.close_drawer()
        open_project(web, data["name"])
        web.applied(**outer_query)
        web.results(outer_total)
        if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{owner}"):
            web.named(f"Close {source.name}")
        delete_fixture_project(web, owner)
