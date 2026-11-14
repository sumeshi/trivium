"""Tags remain visible without notes and filter every investigation operation."""
from __future__ import annotations

import csv
from pathlib import Path
import uuid

from column_pin_checks import _key
from feedback_checks import _check_sticky, _settle
from refinement_checks import _scope
from refinement_grouping import _rows
from row_navigation import open_project, wait_for


def _badge(web, row: int):
    return web.node(f'.data-row[data-row-index="{row}"] .row-tags-button')


def verify_tag_usage(web, data, root: Path, evidence: Path, report: dict) -> None:
    source = root / f"tag-usage-{uuid.uuid4().hex[:8]}.csv"
    records = [("r1", "beacon", "other"), ("r2", "normal", "review"),
               ("r3", "beacon", "review"), ("r4", "normal", "other")]
    records += [(f"r{i}", "filler", "other") for i in range(5, 101)]
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "message", "tag", *[f"extra_{i}" for i in range(24)]])
        writer.writerows([*record, *[f"value {i}" for i in range(24)]] for record in records)
    original = source.read_bytes()
    case = web.command("create_project", {"path": str(source)})
    owner = case["meta"]["id"]
    for row, flag, memo, tags in [
        (0, "", "", ["review", "needs triage"]),
        (1, "critical", "existing", ["review"]),
        (2, "safe", "", ['say "hi"', "a|b:blue", "100%"]),
    ]:
        web.command("update_flag", {"projectId": owner, "row_index": row, "flag": flag, "memo": memo, "tags": tags})
    try:
        open_project(web, case["meta"]["name"])
        web.results(100)
        detail = web.command("load_project", {"projectId": owner}, request=True)
        columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
        first = web.script("""
          const r=document.querySelector('.data-row[data-row-index="0"]');
          return {note:r.querySelector('.memo-button').textContent.trim(),
            tags:r.querySelector('.row-tags-button').textContent.trim(),height:r.getBoundingClientRect().height};
        """)
        assert first["note"] == "Add note" and first["tags"] == "2" and first["height"] == 32, first
        assert not web.script("return !!document.querySelector('.data-row[data-row-index=\"3\"] .row-tags-button');")
        web.script("document.querySelector('.virtual-viewport').scrollLeft=700;")
        _settle(web)
        sticky = _check_sticky(web, '.data-row[data-row-index="0"]', "tag-only sticky note cell")
        web.script("document.querySelector('.virtual-viewport').scrollLeft=0;")
        _settle(web)

        web.script("document.querySelector('.data-row[data-row-index=\"0\"] .memo-button').focus();")
        _key(web, "\ue004")
        assert web.script("return document.activeElement.classList.contains('row-tags-button');")
        _key(web, "\ue00d")
        wait_for(lambda: web.script("return !!document.querySelector('.row-tags-menu');"), bool, "Space opens the row's tags")
        _key(web, "\ue00c")
        wait_for(lambda: web.script("return !document.querySelector('.row-tags-menu');"), bool, "Escape closes tags")
        assert web.script("return document.activeElement.classList.contains('row-tags-button');")
        _key(web, "\ue004")
        wait_for(lambda: web.script("return document.activeElement.dataset.columnId;"),
                 lambda value: value == columns["id"], "Tab after tags reaches the first logical data column")

        web.named("Choose visible columns")
        web.named("Deselect all", "#visible-column-menu")
        wait_for(lambda: web.state()["headers"], lambda value: not value, "all data columns hidden")
        web.named("Done", "#visible-column-menu")
        web.tap(_badge(web, 0))
        _key(web, "\ue004")
        wait_for(lambda: web.script("return !document.querySelector('.row-tags-menu')&&document.activeElement.classList.contains('row-tags-button');"),
                 bool, "Tab exits the tag menu without losing focus when every data column is hidden")
        web.named("Choose visible columns")
        web.named("Select all", "#visible-column-menu")
        web.named("Done", "#visible-column-menu")

        _scope(web, "id")  # Annotation terms do not depend on source search columns.
        before = web.query()
        web.tap(_badge(web, 0))
        _settle(web)
        assert web.query() == before, "Opening tags must not change the result"
        web.named("Search tag review", ".row-tags-menu")
        web.applied(search='@tag:"review"')
        web.results(2)
        query = web.query()
        tagged = web.command("query_project_rows", {**query, "offset": 0, "limit": 10})
        assert [row["row_index"] for row in tagged["rows"]] == [0, 1]
        web.drawer("Groups")
        web.choose('select[aria-label="Group column"]', columns["message"])
        web.named("Add level", ".grouping-panel")
        groups = wait_for(lambda: _rows(web), lambda rows: len(rows) == 2 and sum(row["count"] for row in rows) == 2,
                          "Analysis uses the annotation-tag result")
        web.close_drawer()
        exported = root / "tagged-result.csv"
        web.command("export_project", {**query, "scope": "filtered", "destination": str(exported), "includeAnnotations": True})
        with exported.open(encoding="utf-8", newline="") as handle:
            output = list(csv.DictReader(handle))
        assert [row["id"] for row in output] == ["r1", "r2"], output
        assert "trivium-tags" in output[0] and all("review" in row["trivium-tags"] for row in output)

        _scope(web, "id")
        web.applied(search='@tag:"review"', searchColumns=[])
        web.write('input[aria-label="Search rows"]', "tag:review")
        web.applied(search="tag:review")
        web.results(2)
        ordinary = web.command("query_project_rows", {**web.query(), "offset": 0, "limit": 10})
        assert [row["row_index"] for row in ordinary["rows"]] == [1, 2], ordinary

        web.write('input[aria-label="Search rows"]', "")
        web.applied(search="")
        web.results(100)
        web.tap(_badge(web, 2))
        web.named('Search tag say "hi"', ".row-tags-menu")
        web.applied(search="@tag:say%20%22hi%22")
        assert [row["id"] for row in web.results(1)["rows"]] == [3]
        web.write('input[aria-label="Search rows"]', "@tag:*")
        web.applied(search="@tag:*")
        web.results(3)
        web.drawer("Bulk annotation")
        web.choose('select[aria-label="Bulk tag action"]', "remove")
        web.write('input[aria-label="Bulk tag"]', "review")
        web.named("Apply", ".bulk-form")
        wait_for(lambda: web.script("return document.querySelector('.bulk-form [role=status]')?.textContent;"),
                 lambda text: bool(text and "applied" in text), "direct tag removal completes")
        web.results(2)
        web.write('input[aria-label="Search rows"]', '@tag:"review"')
        web.applied(search='@tag:"review"')
        web.results(0)
        web.named("Undo", ".bulk-form")
        web.results(2)
        web.close_drawer()
        web.write('input[aria-label="Search rows"]', "")
        web.applied(search="")
        web.results(100)
        web.tap(_badge(web, 0))
        web.script("document.querySelector('.virtual-viewport').scrollTop=1600;")
        _settle(web)
        assert not web.script("return !!document.querySelector('.row-tags-menu');"), "Scrolling/recycling must close the old row's tags"
        assert web.query()["search"] == ""
        assert source.read_bytes() == original
        report["tag_usage"] = {"tag_only_row": first, "sticky": sticky, "metadata_rows": [0, 1],
                              "ordinary_tag_column_rows": [1, 2], "analysis": groups,
                              "quoted_label_search": True, "cached_membership_after_remove_and_undo": True,
                              "keyboard": True, "source_unchanged": True}
        web.screenshot(evidence / "tag-usage.png")
    finally:
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        web.named(f"Close {source.name}")
        web.command("delete_project", {"projectId": owner}, request=True)
