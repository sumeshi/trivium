"""Real search failures, negated phrases, note sizing and record navigation."""
from __future__ import annotations

import csv
from pathlib import Path
import time
import uuid

from column_pin_checks import _key, _saved
from feedback_checks import _horizontal_to, _settle
from fixture_cleanup import delete_fixture_project
from row_navigation import open_project, wait_for


def _ids(web) -> list[int]:
    return web.script("return [...document.querySelectorAll('.data-row:not(.loading)')].map(e=>Number(e.dataset.rowIndex));")


def _search(web, query: str, expected: list[int]) -> None:
    web.write('input[aria-label="Search rows"]', query)
    _key(web, "\ue007")
    web.applied(search=query)
    web.results(len(expected))
    wait_for(lambda: _ids(web), lambda value: value == expected, f"actual table rows for {query!r}")


def _query_failure(web, project_id: str, report: dict) -> None:
    checkpoint = len(web.trace())
    web.write('input[aria-label="Search rows"]', "|")
    _key(web, "\ue007")

    def failed():
        return web.script("""
          const error=document.querySelector('.table-query-state[role="alert"]');
          return {message:error?.textContent??'',meta:document.querySelector('.table-meta-left')?.textContent??'',
            flagged:document.querySelector('.table-meta-right')?.textContent??'',
            empty:!!document.querySelector('.empty-rows'),cells:document.querySelectorAll('.data-row:not(.loading) .cell-text').length,
            query:document.querySelector('[aria-label="Search rows"]').value};
        """)

    first = wait_for(failed, lambda value: "Could not load rows." in value["message"], "invalid query is a visible failure")
    assert "Invalid search query" in first["message"] and "Retry" in first["message"], first
    assert "Result unavailable" in first["meta"] and "8 source rows" in first["meta"], first
    assert not first["empty"] and first["cells"] == 0 and first["query"] == "|", first
    assert "Flagged —" in first["flagged"], first
    web.drawer("Conditions")
    web.tap(web.node('.analysis-drawer [aria-label="Add condition"]'))
    wait_for(lambda: web.script("return document.querySelectorAll('.condition-row').length;"),
        lambda count: count == 1, "Rules remain clickable above the query-error layer")
    web.close_drawer()
    before_retry = len(web.trace())
    web.named("Retry", '.table-query-state[role="alert"]')
    wait_for(lambda: [entry for entry in web.trace()[before_retry:] if entry["name"] == "query_project_rows"
        and entry["request"].get("projectId") == project_id and entry["request"].get("search") == "|"],
        lambda entries: bool(entries and entries[-1].get("error")), "Retry repeats the same rejected query")
    retry = wait_for(failed, lambda value: "Could not load rows." in value["message"], "retry failure remains explicit")
    assert retry["query"] == "|" and not retry["empty"] and retry["cells"] == 0, retry
    errors = [entry for entry in web.trace()[checkpoint:] if entry["name"] == "query_project_rows"
        and entry["request"].get("projectId") == project_id and entry["request"].get("search") == "|"
        and "error" in entry]
    assert len(errors) >= 2 and all("Invalid search query" in entry["error"] for entry in errors), errors
    report.setdefault("boundary_expected_errors", []).extend(
        {key: entry[key] for key in ("name", "request", "error")} for entry in errors)
    _search(web, '-"powershell -enc"', [1, 3, 4, 5, 7])
    assert not web.script("return !!document.querySelector('.table-query-state[role=alert]');")
    report["review_query_failure"] = {"explicit_failure": True, "same_query_retry": True,
        "no_false_zero_or_stale_rows": True, "recovered": True}


def _note_geometry(web) -> dict:
    return web.script("""
      const viewport=document.querySelector('.virtual-viewport'),v=viewport.getBoundingClientRect(),
        sample=e=>{const r=e.getBoundingClientRect(),s=getComputedStyle(e);return {
          left:r.left,width:r.width,position:s.position,cssLeft:parseFloat(s.left)};};
      return {scroll:viewport.scrollLeft,viewportLeft:v.left,
        header:sample(document.querySelector('.data-header .sticky-memo')),
        cells:[...document.querySelectorAll('.data-row:not(.loading) .sticky-memo')].map(sample),
        data:Object.fromEntries([...document.querySelectorAll('.data-header>[data-column-id]')]
          .map(e=>[e.dataset.columnId,e.getBoundingClientRect().width])),
        value:Number(document.querySelector('[aria-label="Resize column Note"]').getAttribute('aria-valuenow'))};
    """)


def _note_width(web, width: int) -> dict:
    value = wait_for(lambda: _note_geometry(web), lambda state: state["value"] == width
        and abs(state["header"]["width"] - width) <= 1
        and bool(state["cells"]) and all(abs(cell["width"] - width) <= 1 for cell in state["cells"]),
        f"note header and cells resize to {width} pixels")
    assert all(cell["position"] == "sticky" and abs(cell["left"] - value["header"]["left"]) <= 1
        and abs(cell["cssLeft"] - value["header"]["cssLeft"]) <= 1 for cell in value["cells"]), value
    return value


def _resize_note(web, case: dict, source: Path, report: dict) -> None:
    _search(web, "", list(range(8)))
    _horizontal_to(web, 0)
    query, before, checkpoint = web.query(), _note_width(web, 200), len(web.trace())
    slider = web.node('[aria-label="Resize column Note"]')
    assert web.script("return [arguments[0].getAttribute('aria-valuemin'),arguments[0].getAttribute('aria-valuemax'),arguments[0].tabIndex];", slider) == ["120", "600", 0]
    web.script("arguments[0].focus({preventScroll:true});", slider)
    checks = []
    for key, width in [("\ue012", 190), ("\ue011", 120), ("\ue014", 130),
                       ("\ue010", 600), ("\ue011", 120), ("\ue014", 130),
                       ("\ue014", 140), ("\ue014", 150)]:
        _key(web, key)
        current = _note_width(web, width)
        assert all(abs(current["data"][column] - value) <= 1 for column, value in before["data"].items()
            if column in current["data"]), (before, current)
        checks.append(width)
    time.sleep(.35)  # Beyond filter debounce: presentation changes must not query.
    assert web.query() == query, (query, web.query())
    assert not any(entry["name"] in {"query_project_rows", "locate_project_row"}
        for entry in web.trace()[checkpoint:]), "Resizing Note must not reload or re-sort rows"
    _horizontal_to(web, 300)
    panned = _note_width(web, 150)
    assert panned["scroll"] == 300 and abs(panned["header"]["left"] - before["header"]["left"]) <= 1, panned
    _saved(web, case["meta"]["id"], lambda state: state.get("noteColumnWidth") == 150, "Note width is saved")
    web.named(f"Close {source.name}")
    open_project(web, case["meta"]["name"])
    web.results(8)
    _note_width(web, 150)
    assert web.query() == query, (query, web.query())
    _horizontal_to(web, 0)
    report["review_note_width"] = {"keyboard_widths": checks, "query_unchanged": True,
        "data_widths_unchanged": True, "sticky_alignment_after_horizontal_scroll": True, "reopened_width": 150}


def _detail_navigation(web, columns: dict[str, str], report: dict) -> None:
    _search(web, "cohort:keep", [0, 2, 4, 6])
    for direction in ["asc", "desc"]:
        web.tap(web.node(f'.data-header [data-column-id="{columns["score"]}"] .column-sort'))
        web.applied(sortKey=columns["score"], sortDirection=direction)
        web.results(4)
    wait_for(lambda: _ids(web), lambda value: value == [6, 4, 2, 0], "filtered descending table order")
    query = web.query()
    web.tap(web.node('.data-row:not(.loading) .stickyData'))

    def at(original: int, position: int):
        return wait_for(lambda: web.script("""
          const d=document.querySelector('.row-detail-dialog');return d?{label:d.getAttribute('aria-label'),
            position:d.querySelector('.row-detail-position')?.textContent??'',
            previous:d.querySelector('[aria-label="Previous row"]').disabled,
            next:d.querySelector('[aria-label="Next row"]').disabled}:null;
        """), lambda value: value is not None and value["label"] == f"Row {original}"
            and value["position"] == f"{position} / 4" and value["previous"] == (position == 1)
            and value["next"] == (position == 4), f"inspector original row {original} at result position {position}")

    at(7, 1)
    for original, position in [(5, 2), (3, 3), (1, 4)]:
        web.named("Next row", ".row-detail-dialog")
        at(original, position)
    web.named("Previous row", ".row-detail-dialog")
    at(3, 3)
    assert web.query() == query, (query, web.query())
    web.named("Close", ".row-detail-dialog")
    wait_for(lambda: web.script("return !document.querySelector('.row-detail-dialog');"), bool, "the inspector closes after navigation")
    assert _ids(web) == [6, 4, 2, 0]
    report["review_detail_navigation"] = {"filtered_descending_original_rows": [7, 5, 3, 1],
        "previous_returns_to_original_row": 3, "query_unchanged": True, "boundaries_disabled": True}


def verify_review_fixes(web, data, root: Path, evidence: Path, report: dict) -> None:
    web.close_drawer()
    source = root / f"review-fixes-{uuid.uuid4().hex[:8]}.csv"
    commands = ["powershell -enc AAA", "powershell -nop", "notepad", "-powershell",
                "cmd.exe", "powershell.exe -enc", "C:\\Windows\\powershell -enc", "blocked|literal"]
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["command", "detail", "score", "cohort"] + [f"extra_{i}" for i in range(6)])
        writer.writerows([command, "powershell -enc" if index == 2 else "allow", index + 1,
                          "keep" if index % 2 == 0 else "drop"] + [f"extra-{i}-{index}" for i in range(6)]
                         for index, command in enumerate(commands))
    original = source.read_bytes()
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    try:
        open_project(web, case["meta"]["name"])
        web.results(8)
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
        for query, expected in [('-"powershell -enc"', [1, 3, 4, 5, 7]),
                                ('"-powershell"', [3]),
                                ('-command:"powershell -enc"', [1, 2, 3, 4, 5, 7]),
                                ('command:notepad|-"powershell -enc"', [1, 2, 3, 4, 5, 7])]:
            _search(web, query, expected)
        report["review_negated_phrases"] = {"unscoped": True, "quoted_minus_is_literal": True,
            "column_scope_preserved": True, "or_column_carry": True}
        _query_failure(web, project_id, report)
        _resize_note(web, case, source, report)
        _detail_navigation(web, columns, report)
        assert source.read_bytes() == original
        _settle(web)
        web.screenshot(evidence / "review-fixes.png")
    finally:
        if web.script("return !!document.querySelector('.row-detail-dialog');"):
            web.named("Close", ".row-detail-dialog")
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{project_id}"):
            web.named(f"Close {source.name}")
        delete_fixture_project(web, project_id)
