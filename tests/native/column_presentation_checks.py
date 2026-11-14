"""Native column arrangement and datetime presentation preserve source evidence."""
from __future__ import annotations

import csv
from pathlib import Path
import time
import uuid

from column_pin_checks import _key, _prefix, _saved
from feedback_checks import _horizontal_to, _settle
from row_navigation import open_project, wait_for


def _action(web, column: str, command: str) -> None:
    header = web.script("return document.querySelector('.data-header [data-column-id=\"'+arguments[0]+'\"] .column-sort');", column)
    assert header, column
    web.script("arguments[0].focus({preventScroll:true});", header)
    _key(web, "\ue015")
    web.named(command, ".column-context-menu")
    _settle(web)


def _order(web) -> list[str]:
    return web.script("return [...document.querySelectorAll('.data-header>[data-column-id]')].map(e=>e.dataset.columnId);")


def _width(web, column: str) -> dict:
    return web.script("""
      const id=arguments[0],header=document.querySelector('.data-header [data-column-id="'+id+'"]'),
        cell=document.querySelector('.data-row [data-column-id="'+id+'"]');
      return {header:header.getBoundingClientRect().width,cell:cell.getBoundingClientRect().width};
    """, column)


def _value(web, column: str, row: int = 0) -> str:
    return web.script("return document.querySelector('.data-row[data-row-index=\"'+arguments[1]+'\"] [data-column-id=\"'+arguments[0]+'\"] .cell-text').textContent.trim();", column, row)


def _unchanged(web, query: dict) -> None:
    time.sleep(.35)  # Beyond the real filter debounce, including accidental sort clicks.
    assert web.query() == query, (query, web.query())
    state = web.state()
    assert state["selectionTotal"] == 1 and state["selectedIds"] == [2], state


def _drag(web, source: str, target: str) -> dict:
    _horizontal_to(web, 0)
    points = web.script("""
      const a=document.querySelector('.data-header [data-column-id="'+arguments[0]+'"] .column-sort'),
        b=document.querySelector('.data-header [data-column-id="'+arguments[1]+'"] .column-sort'),
        r=a.getBoundingClientRect(),t=b.getBoundingClientRect();
      const start={x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)},
        end={x:Math.round(t.left+t.width/4),y:Math.round(t.top+t.height/2)};
      return {start,end,reachable:a.contains(document.elementFromPoint(start.x,start.y))&&b.contains(document.elementFromPoint(end.x,end.y))};
    """, source, target)
    assert points["reachable"], points
    web.script("""
      window.__columnDrag=[];
      window.__columnDragAudit=e=>window.__columnDrag.push({type:e.type,trusted:e.isTrusted});
      for(const name of ['dragstart','drop','dragend'])document.addEventListener(name,window.__columnDragAudit,true);
    """)
    start, end = points["start"], points["end"]
    try:
        web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "presentation-drag",
            "parameters": {"pointerType": "mouse"}, "actions": [
                {"type": "pointerMove", **start, "origin": "viewport", "duration": 0},
                {"type": "pointerDown", "button": 0},
                {"type": "pointerMove", "x": start["x"] - 12, "y": start["y"], "origin": "viewport", "duration": 150},
                {"type": "pointerMove", **end, "origin": "viewport", "duration": 350},
                {"type": "pause", "duration": 150}, {"type": "pointerUp", "button": 0},
            ]}]})
        _settle(web)
        events = web.script("return window.__columnDrag;")
        return {"events": events, "delivered": any(event["type"] == "drop" and event["trusted"] for event in events)}
    finally:
        web.request("DELETE", web.route("actions"))
        web.script("for(const name of ['dragstart','drop','dragend'])document.removeEventListener(name,window.__columnDragAudit,true);delete window.__columnDragAudit;delete window.__columnDrag;")


def verify_column_presentation(web, data, root: Path, evidence: Path, report: dict) -> None:
    source = root / f"column-presentation-{uuid.uuid4().hex[:8]}.csv"
    timestamps = ["2024/04/01 09:02:03.123456789+09:00", "2024-04-01 00:02:04.000000001Z", "2024-04-01T00:03:00Z"]
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["record", "created", "message", "not_time"])
        writer.writerows(zip(["r1", "r2", "r3"], timestamps, ["alpha", "beta", "gamma"], ["not-a-timestamp", "42", "wrong"]))
    original = source.read_bytes()
    case = web.command("create_project", {"path": str(source)})
    project_id = case["meta"]["id"]
    try:
        open_project(web, case["meta"]["name"])
        web.results(3)
        detail = web.command("load_project", {"projectId": project_id}, request=True)
        columns = {column["label"]: column["id"] for column in detail["columns_meta"]}
        web.write('input[aria-label="Search rows"]', "alpha | beta")
        query = web.applied(projectId=project_id, search="alpha | beta")
        web.results(2)
        query = web.query()
        web.click(web.element('input[aria-label="Select row 2"]'))

        drag = _drag(web, columns["message"], columns["created"])
        moved = [columns[name] for name in ["record", "message", "created", "not_time"]]
        if drag["delivered"]:
            assert _order(web) == moved, (drag, _order(web))
        else:
            drag["unverified"] = "The native driver did not deliver an HTML5 drop; menu reordering is checked below."
            report.setdefault("unverified", []).append("Native HTML5 column drag (the driver did not deliver a drop event)")
            _action(web, columns["message"], "Move left")
        assert _order(web) == moved, _order(web)
        _unchanged(web, query)
        _action(web, columns["created"], "Move left")
        _action(web, columns["created"], "Move left")
        desired = [columns[name] for name in ["created", "record", "message", "not_time"]]
        assert _order(web) == desired, _order(web)
        _action(web, columns["created"], "Move right")
        _action(web, columns["created"], "Move left")
        assert _order(web) == desired, _order(web)

        initial_width = _width(web, columns["created"])["header"]
        handle = web.script("const e=document.querySelector('[aria-label=\"Resize column created\"]'),r=e.getBoundingClientRect();return {x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2),hit:e.contains(document.elementFromPoint(r.left+r.width/2,r.top+r.height/2))};")
        assert handle.pop("hit"), handle
        web.request("POST", web.route("actions"), {"actions": [{"type": "pointer", "id": "presentation-resize",
            "parameters": {"pointerType": "mouse"}, "actions": [
                {"type": "pointerMove", **handle, "origin": "viewport", "duration": 0}, {"type": "pointerDown", "button": 0},
                {"type": "pointerMove", "x": handle["x"] + 64, "y": handle["y"], "origin": "viewport", "duration": 200},
                {"type": "pointerUp", "button": 0}]}]})
        wait_for(lambda: _width(web, columns["created"]), lambda value: abs(value["header"] - initial_width - 64) <= 1, "drag resizes the source column")
        _action(web, columns["created"], "Resize column")
        assert web.script("return document.activeElement.getAttribute('aria-label');") == "Resize column created"
        _key(web, "\ue014")
        final_width = initial_width + 74
        widths = wait_for(lambda: _width(web, columns["created"]),
            lambda value: all(abs(width - final_width) <= 1 for width in value.values()), "header and body share keyboard-adjusted column width")
        assert _order(web) == desired, _order(web)
        _action(web, columns["record"], "Freeze columns through this column")
        frozen = _prefix(web, desired[:2], "frozen prefix follows the display order")
        _unchanged(web, query)

        _action(web, columns["created"], "Show ISO datetime")
        wait_for(lambda: _value(web, columns["created"]), lambda value: value == "2024-04-01T00:02:03.123456789Z", "ISO display preserves nine fractional digits and normalizes the offset")
        assert _value(web, columns["created"], 1) == "2024-04-01T00:02:04.000000001Z"
        raw = web.command("get_cell", {"projectId": project_id, "rowIndex": 0, "columnId": columns["created"]})
        assert raw["text"] == timestamps[0], raw
        exported = root / "column-presentation-source.csv"
        result = web.command("export_project", {**query, "destination": str(exported), "scope": "filtered", "columns": desired, "includeAnnotations": False})
        with exported.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert result["rows"] == 2 and [row["created"] for row in rows] == timestamps[:2], rows
        assert list(rows[0]) == ["created", "record", "message", "not_time"], rows[0]
        _action(web, columns["created"], "Show original values")
        wait_for(lambda: _value(web, columns["created"]), lambda value: value == timestamps[0], "original display can be restored")
        _action(web, columns["created"], "Show ISO datetime")
        _action(web, columns["not_time"], "Show ISO datetime")
        wait_for(lambda: web.view()["toast"], lambda value: "not_time" in value and "Unrecognized timestamp" in value, "invalid timestamp display is rejected visibly")
        saved = _saved(web, project_id, lambda state: state.get("columnOrder") == desired
            and abs(state.get("columnWidths", {}).get(columns["created"], 0) - final_width) <= 1
            and state.get("columnFormats", {}).get(columns["created"]) == "iso", "column presentation saved")
        assert columns["not_time"] not in saved.get("columnFormats", {}), saved
        _unchanged(web, query)

        web.tab(data["id"])
        web.results(data["total"])
        web.tab(project_id)
        web.results(2)
        _unchanged(web, query)
        web.named(f"Close {source.name}")
        open_project(web, case["meta"]["name"])
        web.results(2)
        _settle(web)
        assert _order(web) == desired and web.query() == query, (_order(web), web.query())
        assert all(abs(width - final_width) <= 1 for width in _width(web, columns["created"]).values())
        assert _value(web, columns["created"]) == "2024-04-01T00:02:03.123456789Z"
        _prefix(web, desired[:2], "saved reordered frozen prefix reopens")
        assert source.read_bytes() == original
        web.screenshot(evidence / "column-presentation.png")
        report["column_presentation"] = {"drag": drag, "display_order": desired, "widths": widths,
            "frozen_prefix": [item["id"] for item in frozen["headers"] if item["pinned"]],
            "iso_nanoseconds_and_offset": True, "raw_get_cell_and_export_preserved": True,
            "invalid_timestamp_rejected": True, "query_and_selection_unchanged": True,
            "saved_order_width_format_reopened": True}
    finally:
        web.close_drawer()
        web.tab(data["id"])
        web.results(data["total"])
        if web.script("return !!document.querySelector(arguments[0]);", f"#project-tab-{project_id}"):
            web.named(f"Close {source.name}")
        web.command("delete_project", {"projectId": project_id}, request=True)
